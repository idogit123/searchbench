#!/usr/bin/env python3
"""RavenDB bulk insert over its HTTP protocol, streamed.

    GET  /databases/<db>/operations/next-operation-id   -> {"Id": n}
    POST /databases/<db>/bulk_insert?id=<n>             <- one streamed JSON array

Array elements are:

    {"Id":"logs/1","Type":"PUT","Document":{ ...fields..., "@metadata":{"@collection":"Logs"} }}

This is precisely what the official clients do internally -- verified against
ravendb-nodejs-client's BulkInsertOperation.ts (the URL at :997, the element
shape in _writeToStream at :673) and the Python client's
bulk_insert_operation.py. Speaking it directly rather than through a client is
deliberate:

  * The npm client would add package.json and node_modules to a repo where no
    adapter uses Node, and would need a pure-JS parquet reader in place of
    pyarrow.
  * The official Python client pins requests~=2.32.0 against this repo's
    requests==2.34.2 -- a hard conflict -- and its per-document path takes a
    mutex, runs a reflection-based entity-to-dict conversion, serializes with
    stdlib json rather than orjson, and makes a redundant bytearray copy.

So this module adds no dependency beyond the pinned pyarrow/orjson/requests that
every other adapter's ingest already uses.
"""

import orjson
import requests

# Bytes buffered before a chunk is handed to the HTTP layer. The request body is
# a generator, so without buffering every document would become its own chunked
# transfer-encoding frame.
DEFAULT_CHUNK_BYTES = 1 << 20


def next_operation_id(session: requests.Session, url: str, database: str) -> int:
    r = session.get(f"{url}/databases/{database}/operations/next-operation-id", timeout=60)
    r.raise_for_status()
    return r.json()["Id"]


def _stream_body(docs, collection: str, chunk_bytes: int):
    """Yield the JSON array incrementally: [ {..}, {..}, ... ]

    `docs` is an iterable of (document_id, dict). The metadata is attached here
    rather than by the caller so the collection name is stated once.
    """
    meta = {"@collection": collection}
    buf = bytearray(b"[")
    first = True

    for doc_id, doc in docs:
        if first:
            first = False
        else:
            buf += b","
        doc["@metadata"] = meta
        buf += b'{"Id":'
        buf += orjson.dumps(doc_id)
        buf += b',"Type":"PUT","Document":'
        buf += orjson.dumps(doc)
        buf += b"}"

        if len(buf) >= chunk_bytes:
            yield bytes(buf)
            buf = bytearray()

    buf += b"]"
    yield bytes(buf)


def bulk_insert(session: requests.Session, url: str, database: str, collection: str,
                docs, chunk_bytes: int = DEFAULT_CHUNK_BYTES, timeout: int = 3600) -> None:
    """Stream `docs` into `collection` as one long-lived request.

    One operation id per call. Callers that want parallelism run several of
    these concurrently -- each gets its own id and its own request, which is
    also how the official clients parallelise.
    """
    op_id = next_operation_id(session, url, database)
    resp = session.post(
        f"{url}/databases/{database}/bulk_insert?id={op_id}&skipOverwriteIfUnchanged=false",
        data=_stream_body(docs, collection, chunk_bytes),
        headers={"Content-Type": "application/json"},
        timeout=timeout,
    )
    # A failed bulk insert answers with a JSON error body; surface it rather
    # than letting a truncated load look like a successful one.
    if resp.status_code >= 400:
        raise RuntimeError(
            f"bulk_insert into {collection} failed: HTTP {resp.status_code} {resp.text[:2000]}"
        )
