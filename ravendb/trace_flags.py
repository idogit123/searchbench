#!/usr/bin/env python3
"""Per-trace service flags for the join queries (Q84-Q92).

Each of those nine asks: how many distinct traces have a log matching this text
predicate, and also a log from service X -- where X is only ever payment,
frontend or cart. So each trace needs three booleans, and every log needs to know
its own trace's three booleans.

This module does one streaming pass over the parquet and returns the flags as a
dict, trace id -> bitmask. ingest.py then writes them onto each log document.

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

WHY A PLAIN DICT
ingest.py looks every log's trace id up in this table. The vectorised-looking
way is Arrow's pc.index_in against an array of the flagged ids, and that was the
first version here. But Arrow rebuilds its hash table from the whole value set on
EVERY call, and ingest.py calls it once per 50,000-row batch. Measured, per
batch: 12 ms with 100k flagged traces, 170 ms with 1M, 736 ms with 4M -- linear in
the size of the set. Invisible at 1M rows (102k traces); at 1B rows, if the
flagged-trace count grows with the rows to ~100M, about 18 s per batch over
20,000 batches, i.e. ~100 CPU-hours, plus a 100M-entry table rebuilt in every
worker. A dict lookup costs ~100 ns per row at any size. What it costs instead is
memory, an estimated 15-25 GB at 1B; ingest.py builds it before forking its
workers so they share it rather than copy it.
"""

import argparse
import glob
import os
import sys
import time

import pyarrow.compute as pc
import pyarrow.parquet as pq

# B-side services, and only these three: across all nine join queries the
# b.ServiceName predicate is payment (Q84-86, Q91), frontend (Q87, Q89, Q90,
# Q92) or cart (Q88).
FLAG_SERVICES = ["payment", "frontend", "cart"]
FLAG_BITS = [1 << i for i in range(len(FLAG_SERVICES))]
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


def build(files):
    """tid -> bitmask over FLAG_SERVICES (bit i set: a log from FLAG_SERVICES[i]).

    Only flag-service rows materialise to Python, so the work scales with the
    relevant subset rather than the whole corpus.

    Only traces touching at least one flag service get an entry. A log whose
    trace is absent reads false for all three and can never satisfy the b-side,
    which is also why the SQL's `a.TraceId <> ''` guard needs no equivalent: an
    empty TraceId is never a key here.
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


def main():
    p = argparse.ArgumentParser(description="Report the flag scan without ingesting.")
    p.add_argument("--local-dir", required=True)
    args = p.parse_args()

    files = sorted(glob.glob(os.path.join(args.local_dir, "part_*.parquet")))
    if not files:
        sys.exit(f"no part_*.parquet in {args.local_dir}")

    t0 = time.monotonic()
    flags = build(files)
    print(f"[flags] {len(flags):,} traces touch a flag service "
          f"({time.monotonic() - t0:.1f}s)")
    for f, bit in zip(FLAG_FIELDS, FLAG_BITS):
        print(f"[flags]   {f}: {sum(1 for m in flags.values() if m & bit):,}")


if __name__ == "__main__":
    main()
