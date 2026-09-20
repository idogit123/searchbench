#!/usr/bin/env python3
"""Build the `Traces` collection the join queries (Q84-Q92) need.

One document per TraceId, `traces/<TraceId>`, carrying three booleans:

    { "HasPayment": true, "HasFrontend": false, "HasCart": true }

config/index.json pulls these into the log index with LoadDocument, which turns
each self-join into a flat predicate:

    where ServiceName = 'frontend' and search(Body,'failed') and HasPayment = true
    select distinct TraceId limit 0, 0

Directly modelled on this repo's elastic/build_lookup.py, which builds the same
three flags into a `trace_lookup` dimension index for ES|QL LOOKUP JOIN -- same
reason, same B-side services, same one-streamed-pass-over-parquet approach.

Correctness note, inherited from build_lookup.py: every join query filters
Has<B> = true, so a trace touching NONE of payment/frontend/cart can never
survive. We therefore emit a document only for traces containing at least one
flag service. A log whose trace has no document loads null, all three flags read
false, and it drops -- which is the same outcome, for far fewer documents. This
also makes the SQL's `a.TraceId <> ''` guard unnecessary: an empty TraceId has
no document either.
"""

import argparse
import glob
import os
import sys
import time

import pyarrow.compute as pc
import pyarrow.parquet as pq
import requests

from rvn_bulk import bulk_insert

# B-side services, and only these three: across all nine join queries the
# b.ServiceName predicate is payment (Q84-86, Q91), frontend (Q87, Q89, Q90,
# Q92) or cart (Q88).
FLAG_SERVICES = ["payment", "frontend", "cart"]
_FLAG_FIELD = {"payment": "HasPayment", "frontend": "HasFrontend", "cart": "HasCart"}


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


def scan_flags(files):
    """tid -> bitmask over FLAG_SERVICES. Only flag-service rows materialise to
    Python, so the work scales with the relevant subset, not the whole corpus."""
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


def _documents(flags):
    for tid, mask in flags.items():
        doc = {}
        for i, s in enumerate(FLAG_SERVICES):
            doc[_FLAG_FIELD[s]] = bool(mask & (1 << i))
        yield f"traces/{tid}", doc


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--local-dir", required=True)
    p.add_argument("--url", default=os.environ.get("RVN_URL", "http://127.0.0.1:8080"))
    p.add_argument("--database", default=os.environ.get("RVN_DATABASE", "searchbench"))
    args = p.parse_args()

    files = sorted(glob.glob(os.path.join(args.local_dir, "part_*.parquet")))
    if not files:
        sys.exit(f"no part_*.parquet in {args.local_dir}")

    t0 = time.monotonic()
    flags = scan_flags(files)
    scanned = time.monotonic() - t0
    print(f"[traces] {len(flags):,} traces touch a flag service "
          f"(scan {scanned:.1f}s)", flush=True)

    session = requests.Session()
    bulk_insert(session, args.url, args.database, "Traces", _documents(flags))

    elapsed = time.monotonic() - t0
    print(f"[traces] {len(flags):,} documents written in {elapsed:.1f}s", flush=True)


if __name__ == "__main__":
    main()
