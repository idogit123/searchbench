#!/usr/bin/env python3
"""High-throughput Parquet -> RavenDB ingest for the Logs collection.

Structure follows this repo's victorialogs/ingest.py (itself from opensearch/,
itself from ClickHouse/TextBench, Apache-2.0): row groups are split across
processes, each process converts RecordBatches and streams them out. What
changes is the destination -- RavenDB's bulk_insert protocol, spoken directly
via rvn_bulk (see that module for why no client library is used).

Unlike the VictoriaLogs adapter there is no lowercased Body copy: RavenDB has a
real analyzer stage, and config/analyzer.cs does the lowercasing at index time
the way every other engine's analyzer does.

Each log also carries the three trace flags Q84-Q92 join on, written here
rather than resolved by the index with LoadDocument -- see trace_flags.py for
the measurement that motivated that. The indexes are created afterwards by
./load, so there is exactly one indexing pass over the finished documents.
"""

import argparse
import os
import queue
import threading
import time
from multiprocessing import Pool

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import requests

import trace_flags
from rvn_bulk import bulk_insert

RVN_URL = os.environ.get("RVN_URL", "http://127.0.0.1:8080")
RVN_DATABASE = os.environ.get("RVN_DATABASE", "searchbench")

# Map Parquet lowercase OTel names -> CamelCase; unmapped columns DROPPED.
# "timestamptime" (the redundant millisecond twin of Timestamp) is omitted,
# matching every other adapter -- keeping it would make data_size incomparable.
_FIELD_RENAME = {
    "timestamp": "Timestamp",
    "traceid": "TraceId",
    "spanid": "SpanId",
    "traceflags": "TraceFlags",
    "severitytext": "SeverityText",
    "severitynumber": "SeverityNumber",
    "servicename": "ServiceName",
    "body": "Body",
    "resourceschemaurl": "ResourceSchemaUrl",
    "resourceattributes": "ResourceAttributes",
    "scopeschemaurl": "ScopeSchemaUrl",
    "scopename": "ScopeName",
    "scopeversion": "ScopeVersion",
    "scopeattributes": "ScopeAttributes",
    "logattributes": "LogAttributes",
}
_FIELD_TARGETS = set(_FIELD_RENAME.values())

# Per-trace flags, built once in main() BEFORE the worker Pool forks, so every
# worker inherits the same Arrow buffers instead of building or copying its own.
_FLAG_IDS = None
_FLAG_COLS = {}


def _canonical_field(name: str):
    if name in _FIELD_RENAME:
        return _FIELD_RENAME[name]
    if name in _FIELD_TARGETS:
        return name
    return None


def _ts_iso(col):
    """timestamp[*] -> RavenDB date strings, 'YYYY-MM-DDTHH:MM:SS.fffffffZ'.

    RavenDB stores dates as ISO-8601 strings and recognises them as dates, which
    is what lets config/index.json cast (DateTime)log.Timestamp and index a real
    date for the range windows, the ORDER BY and the minute histogram.

    Done vectorised in Arrow rather than per-row in Python: at a billion rows a
    datetime.strftime per document would dominate the whole load. The seconds
    part comes from strftime; the sub-second part is decomposed arithmetically
    and zero-padded to .NET's 7-digit tick resolution.

    UNVERIFIED -- this machine has no pyarrow, so the exact compute-kernel
    spellings below have not been executed. It is the first thing to check on
    the smoke run: one wrong kernel name here fails loudly at the first batch.
    """
    i64 = col.cast(pa.int64())
    unit = col.type.unit
    to_ns = {"ns": 1, "us": 1_000, "ms": 1_000_000, "s": 1_000_000_000}[unit]
    if to_ns != 1:
        i64 = pc.multiply(i64, to_ns)

    seconds = pc.divide(i64, 1_000_000_000)
    ticks = pc.divide(pc.subtract(i64, pc.multiply(seconds, 1_000_000_000)), 100)

    date_part = pc.strftime(seconds.cast(pa.timestamp("s")), format="%Y-%m-%dT%H:%M:%S")
    frac_part = pc.utf8_lpad(ticks.cast(pa.string()), 7, "0")
    return pc.binary_join_element_wise(
        pc.binary_join_element_wise(date_part, frac_part, "."), "Z", ""
    ).to_pylist()


def _trace_flags(trace_col, n):
    """The log's own trace flags, one value per row, in batch order.

    pc.index_in, not a join: a hash join does not promise to preserve row order,
    and these columns have to stay aligned with the rest of the batch. A trace
    touching none of the three services is absent from the lookup, so index_in
    returns null and fills to false -- the same outcome the missing traces/
    document used to produce.
    """
    if trace_col is None or _FLAG_IDS is None or len(_FLAG_IDS) == 0:
        return {f: [False] * n for f in trace_flags.FLAG_FIELDS}
    idx = pc.index_in(trace_col, value_set=_FLAG_IDS)
    return {f: pc.fill_null(pc.take(col, idx), False).to_pylist()
            for f, col in _FLAG_COLS.items()}


def batch_to_documents(batch: pa.RecordBatch, id_prefix: str):
    """RecordBatch -> (document_id, dict) pairs for rvn_bulk."""
    cols = {}
    trace_col = None
    for name in batch.schema.names:
        out = _canonical_field(name)
        if out is None:
            continue
        col = batch.column(name)
        if out == "TraceId":
            trace_col = col
        if pa.types.is_timestamp(col.type):
            cols[out] = _ts_iso(col)
        elif pa.types.is_map(col.type):
            cols[out] = [dict(v) if v is not None else None for v in col.to_pylist()]
        else:
            cols[out] = col.to_pylist()

    cols.update(_trace_flags(trace_col, batch.num_rows))

    names = list(cols.keys())
    values = list(cols.values())
    for i, row in enumerate(zip(*values)):
        yield f"{id_prefix}{i}", dict(zip(names, row))


def ingest_segment(args: tuple) -> dict:
    """Worker: read row groups [rg_start, rg_end), stream them via `writers`
    concurrent bulk_insert requests."""
    file_path, file_no, rg_start, rg_end, batch_size, writers = args
    tag = f"[rg {rg_start:04d}-{rg_end:04d}]"
    pf = pq.ParquetFile(file_path)

    work: queue.Queue = queue.Queue(maxsize=writers * 2)
    stats = {"indexed": 0}
    lock = threading.Lock()
    errors = []

    def writer():
        session = requests.Session()
        while True:
            item = work.get()
            if item is None:
                work.task_done()
                break
            docs, n = item
            try:
                bulk_insert(session, RVN_URL, RVN_DATABASE, "Logs", iter(docs))
                with lock:
                    stats["indexed"] += n
            except Exception as e:                      # noqa: BLE001
                with lock:
                    errors.append(e)
            work.task_done()

    threads = [threading.Thread(target=writer, daemon=True) for _ in range(writers)]
    for t in threads:
        t.start()

    seq = 0
    for rg_idx in range(rg_start, rg_end):
        for batch in pf.read_row_group(rg_idx).to_batches(max_chunksize=batch_size):
            # Document ids must be unique across every process. file/row-group/
            # sequence is disjoint by construction, so workers never collide.
            prefix = f"logs/{file_no:03d}-{rg_idx:05d}-{seq:04d}-"
            docs = list(batch_to_documents(batch, prefix))
            work.put((docs, len(docs)))
            seq += 1

    for _ in threads:
        work.put(None)
    for t in threads:
        t.join()

    if errors:
        raise errors[0]

    print(f"{tag} done - {stats['indexed']:,} docs", flush=True)
    return stats


def process_file(file_path: str, file_no: int, batch_size: int,
                 num_processes: int, writers: int) -> int:
    pf = pq.ParquetFile(file_path)
    total_rows, total_rg = pf.metadata.num_rows, pf.metadata.num_row_groups
    print(f"{file_path}: {total_rows:,} rows / {total_rg} row groups -> "
          f"{num_processes} processes x {writers} writers", flush=True)

    rg_per_proc = (total_rg + num_processes - 1) // num_processes
    segments = [
        (file_path, file_no, i * rg_per_proc,
         min((i + 1) * rg_per_proc, total_rg), batch_size, writers)
        for i in range(num_processes)
        if i * rg_per_proc < total_rg
    ]

    t0 = time.monotonic()
    with Pool(processes=len(segments)) as pool:
        results = pool.map(ingest_segment, segments)
    elapsed = time.monotonic() - t0
    total = sum(r["indexed"] for r in results)
    print(f"File done: {total:,} docs in {elapsed:.1f}s  ({total/elapsed:,.0f} docs/s)",
          flush=True)
    return total


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--files", type=int, required=True)
    p.add_argument("--start-file", type=int, default=0)
    p.add_argument("--processes", type=int,
                   default=int(os.environ.get("SEARCHBENCH_LOAD_PROCESSES",
                                              max(1, (os.cpu_count() or 4) // 2))),
                   help="Parallel converter processes per file (default: cpu/2)")
    p.add_argument("--writers", type=int, default=4,
                   help="Concurrent bulk_insert requests per process")
    p.add_argument("--batch-size", type=int, default=50000)
    p.add_argument("--local-dir", default="/tmp")
    args = p.parse_args()

    paths = [os.path.join(args.local_dir, f"part_{fn:03d}.parquet")
             for fn in range(args.start_file, args.start_file + args.files)]

    t0 = time.monotonic()

    # Before any Pool exists: these globals have to be in place at fork time.
    global _FLAG_IDS, _FLAG_COLS
    _FLAG_IDS, _FLAG_COLS = trace_flags.build(paths)
    print(f"[flags] {len(_FLAG_IDS):,} traces touch payment/frontend/cart "
          f"({time.monotonic() - t0:.1f}s)", flush=True)

    grand = 0
    for fn, path in enumerate(paths, start=args.start_file):
        grand += process_file(path, fn, args.batch_size, args.processes, args.writers)
    elapsed = time.monotonic() - t0
    print(f"\nGrand total: {grand:,} docs in {elapsed:.1f}s  ({grand/elapsed:,.0f} docs/s)")


if __name__ == "__main__":
    main()
