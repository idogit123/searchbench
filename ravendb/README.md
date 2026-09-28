# RavenDB adapter

RQL over HTTP against a RavenDB server in Docker. All 92 queries run; nothing is
marked `UNSUPPORTED`.

```bash
SEARCHBENCH_DATA_DIR=/path/to/data SEARCHBENCH_DATASET=otel_logs_1m ./benchmark.sh --index
```

## Requirements

`curl`, `jq`, `docker`, and a `python3` with the repo's pinned
`lib/requirements.txt` (`pyarrow`, `orjson`, `requests`). The adapter adds **no**
dependency beyond those — see "Why no RavenDB client library" below. Point
`SEARCHBENCH_PYTHON` at a venv if those modules live in one.

## Environment

| Variable | Default | Meaning |
|---|---|---|
| `RVN_PORT` | `8080` | server port (`--network host`, so no NAT in the timing path) |
| `RVN_DATABASE` | `searchbench` | target database |
| `RVN_IMAGE` | `ravendb/ravendb:7.2.6-ubuntu.24.04-x64` | pinned, so the wire protocol and the server move together |
| `RVN_CONTAINER` | `searchbench-ravendb` | container name |
| `RVN_DATA_DIR` | `./raven-data` | bind mount, not a named volume (named volumes live on the small root disk and overflow at scale) |
| `RVN_LICENSE_FILE` | `./license.json` | optional; see below |

## License

Drop a **Developer** license at `ravendb/license.json` (gitignored). It is free
and full-featured, so RavenDB runs without the Community edition's 3-core / 6 GB
cap and competes on the same hardware as every other row. Without it the server
starts capped and `./start` says so on stderr.

The licence also decides the **default** search engine, which is why this adapter
never relies on that default — see below.

## Two indexes, two search engines

`Indexing.Static.SearchEngineType` is a per-index setting, so the adapter builds
both engines rather than picking one:

| Index | Engine | Serves |
|---|---|---|
| `Logs/Search` | Corax | 84 queries, including all nine joins |
| `Logs/Fuzzy` | Lucene | Q13, Q22–Q24, Q48, Q49, Q59 (fuzzy/proximity) and Q53 |

`CoraxQueryBuilder.cs` throws `NotSupportedInCoraxException` for proximity and
for `fuzzy()`. Everything else the workload needs — `Search`, `Regex`,
`StartsWith`, `EndsWith`, `Boost`, `Exact` — Corax handles, and it executes the
distinct-count joins far better than Lucene, whose path materialises the whole
match set in a `GatherAllCollector` before deduping, scores hits it then
discards, and projects through a stored-fields fetch per match.

**Honest cost:** measured at 1M, `Logs/Fuzzy` uses 210 MB against `Logs/Search`’s
733 MB — about +29% on the index footprint, since it carries four fields rather
than eleven plus the trace flags. That is corpus-proportional, it is
charged to `load_time` and `data_size`, and it buys seven queries that would
otherwise be blank.

Routing queries across two indexes inside one adapter is the established pattern
here, not a deviation: `tiger` picks between BM25, GIN and `~*` per query;
`elastic` runs DSL for Q01–Q83, ES|QL for the joins, and ships a second
`trace_lookup` index.

**The engine is pinned explicitly on both indexes.** The default follows the
*licence*, not the version — None/Community/Developer get Corax,
Professional/Enterprise get Lucene — so developing on a dev licence and
publishing from an enterprise one would flip both engines silently, and the
joins are where they diverge most.

## Custom analyzer

`config/analyzer.cs` is compiled server-side via `PUT /admin/analyzers`. It
splits on non-alphanumeric characters and lowercases: a `CharTokenizer` whose
`IsTokenChar` is `char.IsLetterOrDigit` and whose `Normalize` is
`char.ToLowerInvariant`.

None of RavenDB's eight built-ins matches what every other adapter does.
`RavenStandardAnalyzer` is `StandardTokenizer` + `RavenStandardFilter`, and that
filter carries `StopAnalyzer.ENGLISH_STOP_WORDS_SET` — it drops the `to` out of
`"failed to place order"`, the phrase behind Q11, Q12, Q14 and Q15, which are
`task=count` and compared as exact integers. `SimpleAnalyzer`'s
`LetterTokenizer` drops digits, breaking Q12's `"…expected 200 got 500"`. The
keyword and whitespace analyzers do not split on punctuation at all.


### Wildcards

Trailing wildcards go through `search()`: `ts_regexp('charg.*')`,
`ts_starts_with('charg')` and `ts_like('charg%')` all mean "a token beginning
charg", and all map to `search(Body, 'charg*')`, which RavenDB turns into a
prefix query *before* analysis. Around 2 ms at 1M rows.

Leading and mid wildcards must not use `search()`. The analyzer's `IsTokenChar`
is `char.IsLetterOrDigit`, so it strips `*` and `?` — measured at 1M,
`search(Body,'*tion')` returns exactly what `search(Body,'tion')` returns, which
is 0, on *both* engines. So this is the analyzer, not a Corax limit. Instead:

| spec | RQL | result at 1M |
|---|---|---|
| `%tion` | `endsWith(Body, 'tion')` | 113,086 |
| `%nnec%` | `regex(Body, 'nnec')` | 30,127 |
| `c.che` | `regex(Body, '^c.che$')` | 57,206 |

On a `Search`-indexed field these match **terms**, not the raw field value:
`regex(Body,'^c.che$')` returns exactly the count of the token `cache`, where a
field-level match would need the whole log line to be "cache" and return ~0.
Unanchored `regex(Body,'c.che')` gives 59,089 — `cache` plus `cached` — which is
why Q19 is anchored. Both cost ~110 ms at 1M: a suffix or infix match has to walk
the term dictionary, as it does in any engine.

### Fuzzy thresholds

`fuzzy()` takes a *similarity*, not an edit distance. Lucene's similarity is
`1 - distance/min(len)`, and `connection` is 10 characters, so distance ≤1 is 0.9
and distance ≤2 is 0.8. The comparison is **strictly greater than**, so passing
the boundary value excludes the very distance it names: measured at 1M,
`fuzzy('connektion', 0.9)` returns 0 while `0.89` returns 30,112, and
`connektion` is distance 1 from `connection`. Hence 0.89 and 0.79.
## Joins (Q84–Q92)

`count(DISTINCT TraceId)` over a self-join, expressed in two halves.

**The join half** is three booleans on each log document. `trace_flags.py` makes
one streaming pass over the parquet, collecting for every trace whether it
touches `payment`, `frontend` or `cart` — the only three b-side services across
the nine queries — and `ingest.py` writes `HasPayment` / `HasFrontend` /
`HasCart` onto each log as it goes, flattening the join into an ordinary
predicate. Same move `elastic/build_lookup.py` makes for the same three
services, and like Elastic we charge that work to `load_time`.

Only traces touching at least one flag service get an entry: every join filters
`Has<B> = true`, so a trace touching none of them can never survive. This also
makes the SQL's `a.TraceId <> ''` guard unnecessary — an empty TraceId is never
a key in the scan, so all three flags read false.

### Why not `LoadDocument`

Because it costs 383 MB at 1M rows, measured.

The idiomatic spelling is a `traces/<TraceId>` document per trace pulled into the
index with `LoadDocument`, and that was the first design here. But `LoadDocument`
registers an index **reference**: RavenDB records, per referencing document, what
it loaded, and per referenced document, who loaded it, so that editing a trace
re-indexes every log that read it. On this shape — 1M logs each referencing one
of ~102k traces by a 32-character random hex id — that bookkeeping was 383 MB,
against 25 MB for the three boolean fields themselves and 8.2 MB for the whole
`Traces` collection. More than every log document in the database.

Isolated by building both indexes over the same loaded corpus: identical map,
identical three booleans, derived locally in one and through `LoadDocument` in
the other. 164.1 MB against 547.3 MB.

There is no setting to disable it, and there should not be — an index with stale
`LoadDocument` results looks perfectly healthy and answers wrong. It is what
makes incremental indexing correct on a corpus that changes. This one loads once
and never touches a trace again, so the tree is paid for and never used.

The trade is explicit and worth stating: the flags are a denormalised copy and
would not self-heal if a trace changed.

**The counting half** is a distinct projection with page size zero:

```
from index 'Logs/Search'
where ServiceName = 'frontend' and search(Body, 'failed') and HasPayment = true
select distinct TraceId
limit 0, 0
```

`PageSize == 0` together with `distinct` takes a dedicated branch in both engines
(`isDistinctCount`, `CoraxIndexReadOperation.cs:641` and
`LuceneIndexReadOperation.cs:110`). The server scans the match set, dedups
server-side, returns zero rows, and puts the distinct count in `TotalResults`.
One request, one scalar, no client-side paging inside the measured window. Corax
already applies `totalResults -= skippedResults` itself, so `./query` prints
`TotalResults` and does not subtract again. `TraceId` is stored in the index so
the projection is served from the index entry rather than loading a document per
matching log.

No engine in this benchmark precomputes the distinct — ClickHouse, Postgres,
tiger, ParadeDB, ArangoDB and SereneDB all build it at query time. Elasticsearch
precomputes only the join half, which is exactly what `trace_flags.py` does
here.

**Considered and rejected:** a trace-grain term index, fanning the map out one
entry per token grouped by `(TraceId, ServiceName, Term)`. It works for some of
the nine and its filter would legally sit on the group-by key, but at 1B logs it
yields on the order of 10B reduce entries — an index larger than the corpus,
charged to `data_size` and `load_time` for all 92 queries — and it still cannot
express Q86, Q88 or Q90, because reducing to trace grain loses same-log token
co-occurrence.

## Aggregations use facets

RQL's `group by` builds an *auto map-reduce* index and is not available over a
static index. The static-index equivalent is `select facet(Field)`, which counts
unique terms under an arbitrary `where`. Facet options (`CountDesc`, `PageSize:
20`) have no inline RQL form, so `./query` declares them once as query
parameters and `queries.dsl` references `$top20`.

Two computed fields exist for this:

- **`TimestampMinute`** — the minute bucket for the histograms Q61–Q65, which
  other engines derive at query time via `date_trunc` / `date_histogram`.
- **`SeverityScope`** (`SeverityText|ScopeName`) — because two facets return two
  *independent* lists, not the key *pairs* Q67 asks for.

Both are per-document derived values, not pre-computed aggregates: the counting
still happens at query time over whatever the filter matched. It is nonetheless
a real, if small, shift of work from query time to index time, and it is stated
here rather than left to be discovered.

## Why no RavenDB client library

Bulk insert is a three-call HTTP protocol — `GET …/operations/next-operation-id`,
then one streamed JSON array `POST …/bulk_insert?id=<n>` — and `rvn_bulk.py`
speaks it directly. That is exactly what the official clients do internally.

The npm client would add `package.json` and `node_modules` to a repo where no
adapter uses Node, and would need a pure-JS parquet reader in place of pyarrow.
The official Python client pins `requests~=2.32.0` against this repo's
`requests==2.34.2` — a hard conflict — plus five further dependencies, and its
per-document path takes a mutex, runs a reflection-based entity conversion,
serialises with stdlib `json` rather than `orjson`, and makes a redundant
`bytearray` copy.

## Load order

```
wipe → start → analyzer → compression → logs → INDEXES → wait until non-stale
```

Indexes are created **last**, after every document is in, so there is exactly one
indexing pass over finished documents rather than two over moving ones. Creating
them first would race the ingest.

`./load` blocks until no index is stale. RavenDB indexes asynchronously, so
returning early would both understate `load_time` and let the first queries race
the indexer.

## Timing and caching

`./query` reports curl's `%{time_total}` — the client round-trip — as the last
bare-numeric line on stderr, alongside a non-numeric `SEARCHBENCH_ROWS=<n>`.
RavenDB's server-side `DurationInMs` is deliberately not emitted: it excludes
response delivery and is not what the other engines report.

Nothing is cached between invocations. RavenDB's query cache is client-side
(ETag + 304 revalidation); each `./query` is a fresh curl that stores no response
and never sends `If-None-Match`, so the server has no ETag to match and computes
every query in full. There is no server-side result cache, and the `DisableCaching`
flag is client-only — the server never reads it, so sending it would be a no-op.

## `data-size`

`du` over the data directory, excluding journals. `GetSizeOnDisk()` sums
`DataFileInBytes + JournalsInBytes` across every storage environment
(`DocumentDatabase.cs:1986`), i.e. it includes the write-ahead journals — and no
other adapter charges its WAL (`serenedb/data-size` passes `--exclude=wal`;
Postgres uses `pg_total_relation_size`). Counted: the documents store and both
index environments. The joins need no auxiliary collection — their three flags
are fields on the log documents, already counted.
