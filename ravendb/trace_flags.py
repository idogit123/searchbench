#!/usr/bin/env python3
"""Per-trace service flags for the join queries (Q84-Q92).

Each of those nine asks: how many distinct traces have a log matching this text
predicate, and also a log from service X -- where X is only ever payment,
frontend or cart. So each trace needs three booleans, and every log needs to know
its own trace's three booleans.

This module does one streaming pass over the parquet and returns the flags as
Arrow arrays. ingest.py then writes them onto each log document.

WHY ON THE DOCUMENT, AND NOT VIA LoadDocument
The obvious RavenDB spelling is a traces/<TraceId> document per trace, pulled
into the index with LoadDocument. That works, and it was the first design here,
but LoadDocument registers an index REFERENCE: RavenDB records, per referencing
document, what it loaded, and per referenced document, who loaded it, so that
editing a trace re-indexes its logs. Keeping that promise costs real storage, and
at this shape -- every one of 1M logs referencing one of ~102k traces by a
32-character random hex id -- it was measured at 383 MB, against 8.2 MB of trace
documents and 25 MB for the three boolean fields themselves. More than every log
document in the database, to track the join.

It is not a RavenDB defect; it is what makes incremental indexing correct, and
there is deliberately no switch for it (an index with stale LoadDocument results
looks perfectly healthy and answers wrong). It is simply a capability this
workload never uses: the corpus loads once and no trace is ever updated.

So the flags go on the log document at write time and the map reads them from the
document it is already holding. The scan below is the same one that used to build
the traces/ collection -- we just stop making the server rediscover its result a
million times. Consequence to be aware of: the flags are a denormalised copy, so
they would NOT self-heal if a trace changed. Nothing in this benchmark changes
one. Elasticsearch precomputes the same three flags for the same three services
in elastic/build_lookup.py.
"""

import argparse
import glob
import os
import sys
import time

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

# B-side services, and only these three: across all nine join queries the
# b.ServiceName predicate is payment (Q84-86, Q91), frontend (Q87, Q89, Q90,
# Q92) or cart (Q88).
FLAG_SERVICES = ["payment", "frontend", "cart"]
FLAG_FIELDS = ["HasPayment", "HasFrontend", "HasCart"]


def _column(batch, *candidates):
    """Parquet ships these columns lowercase in some parts, CamelCase in others.

    victorialogs/ingest.py carries the same rename map defensively; this does the
    same for the two columns we read.
    """
    names = set(batch.schema.names)
    for c in candidates:
        if c in names:
            return batch.column(c)
    raise KeyError(f"none of {candidates} in parquet schema {sorted(names)}")


def scan(files):
    """tid -> bitmask over FLAG_SERVICES.

    Only flag-service rows materialise to Python, so the work scales with the
    relevant subset rather than the whole corpus.
    """
    bit = {s: 1 << i for i, s in enumerate(FLAG_SERVICES)}
    flags = {}
    for path in files:
        pf = pq.ParquetFile(path)
        cols = [c for c in pf.schema_arrow.names if c.lower() in ("traceid", "servicename")]
        for batch in pf.iter_batches(columns=cols, batch_size=1_000_000):
            svc = _column(batch, "ServiceName", "servicename")
            tid = _column(batch, "TraceId", "traceid")
            for s, b in bit.items():
                sel = pc.equal(svc, s)
                if not pc.any(sel).as_py():
                    continue
                for t in pc.unique(pc.filter(tid, sel)).to_pylist():
                    if t:                      # skip the empty-TraceId bucket
                        flags[t] = flags.get(t, 0) | b
    return flags


def build(files):
    """-> (trace ids, {field: booleans}), as Arrow arrays aligned by position.

    Arrow rather than the dict itself because ingest.py looks these up once per
    log: pc.index_in over a string array is vectorised and holds the ids in
    Arrow's buffers instead of a Python object per trace. ingest.py builds this
    once before forking its worker pool, so the arrays are shared, not copied.

    Only traces touching at least one flag service get an entry. A log whose
    trace is absent reads false for all three and can never satisfy the b-side,
    which is also why the SQL's `a.TraceId <> ''` guard needs no equivalent: an
    empty TraceId is never a key here.
    """
    flags = scan(files)
    ids = pa.array(list(flags.keys()), type=pa.string())
    masks = list(flags.values())
    cols = {f: pa.array([bool(m & (1 << i)) for m in masks], type=pa.bool_())
            for i, f in enumerate(FLAG_FIELDS)}
    return ids, cols


def main():
    p = argparse.ArgumentParser(description="Report the flag scan without ingesting.")
    p.add_argument("--local-dir", required=True)
    args = p.parse_args()

    files = sorted(glob.glob(os.path.join(args.local_dir, "part_*.parquet")))
    if not files:
        sys.exit(f"no part_*.parquet in {args.local_dir}")

    t0 = time.monotonic()
    ids, cols = build(files)
    print(f"[flags] {len(ids):,} traces touch a flag service "
          f"({time.monotonic() - t0:.1f}s)")
    for f, col in cols.items():
        print(f"[flags]   {f}: {pc.sum(col).as_py():,}")


if __name__ == "__main__":
    main()
