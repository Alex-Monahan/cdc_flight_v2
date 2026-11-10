# Local read throughput at `lineitem` width (PLAN §2 A 5, part 1)

> **These are indications from a development laptop, not performance results.** They come from a shared machine
> (Apple silicon, 10 cores, 16 GB, load average 3–12 from other work during the runs), with source and client on the
> same host. SPEC: performance claims come only from Flight runs (3.1, 5.1 are *(Flight)* items). The numbers here
> choose the backfill reader's design; they are never quoted as what the product achieves.

Measured 2026-11-10 (UTC). Postgres 18.6 (Homebrew), private cluster of this spike (primary 55500, replica 55501,
`tools/localpg` defaults: `shared_buffers` 128 MB, TLS `verify-full` on every connection, `hot_standby_feedback` on,
`max_standby_streaming_delay` 30 s). Clients: Python 3.13, psycopg 3.3.8 (C implementation), pyarrow 26.0.1, DuckDB
1.5.6 with its `postgres_scanner` extension (build `318dabb`). Every read goes to the **replica** (SPEC R17).
Raw results: `evidence/p2/local_throughput/` (JSON lines, CSV summaries).

## 1. Data and method

- **Data**: TPC-H `lineitem` from DuckDB's `tpch` extension, `CALL dbgen(sf := 2)`: **11,997,996 rows**, 16 columns,
  Postgres-native types (`bigint` keys, `integer` line number, `numeric(15,2)`, `char(1)`, `date`, `char(25)`,
  `char(10)`, `varchar(44)`), primary key `(l_orderkey, l_linenumber)`, loaded in key order, `VACUUM ANALYZE`d.
  Heap 1.99 GB (242,904 pages, 166 B/row), 2.37 GB with the index. Wire size of a full read: binary COPY 2.55 GB
  (213 B/row), text 1.78 GB, CSV 1.78 GB. Load: 5 s generate, 40 s load (DuckDB's Postgres writer), 9 s
  `VACUUM ANALYZE`, replica caught up 6 s later (`setup.json`).
- The table (2 GB per node) fits in the OS page cache, so these are **CPU and protocol rates with a hot cache**, not
  disk rates. A warm-up read precedes the matrix.
- **Every measurement runs in a fresh process**, three times, interleaved (repeat 1 of every variant, then repeat 2,
  …, shuffled with a fixed seed). Tables give the median and the spread (max − min)/median. The 1-minute load average
  is recorded next to every run (`load_avg` in `reads.jsonl`; it includes the previous run of this spike).
- **Client CPU** is `getrusage` of the measuring processes. **Postgres backend CPU** is read with `psutil` from the
  backend processes (local processes): exactly per COPY worker (read just before it disconnects), by 0.2 s sampling for
  DuckDB's connections. **Peak RSS** is `ru_maxrss` of each process (summed over workers).
- "cores" = CPU seconds / wall seconds.

Routes (`spikes/local_throughput/`):

| Script | What it measures |
|---|---|
| `bench_copy.py` | Our own reader: N worker processes, one connection each, `REPEATABLE READ READ ONLY`, pulling 32 chunks from a queue; `COPY (SELECT <16 columns> FROM lineitem WHERE <range>) TO STDOUT (FORMAT text\|csv\|binary)`. Ranges by `ctid` (TID range scan, `ctid >= '(a,0)' AND ctid < '(b,0)'`) or by primary key (`l_orderkey` ranges). Sinks: `count` (bytes counted, nothing else), `arrow` (each chunk parsed into an Arrow table by `pyarrow.csv`, one thread per worker), `rows` (psycopg's C binary loaders decode every value). `--snapshot`: workers import one exported snapshot |
| `bench_duckdb.py` | DuckDB's Postgres reader, `ATTACH … (TYPE postgres, READ_ONLY)`. Sinks: `hash` = `SELECT count(*), sum(hash(<all 16 columns>))` (every column materialised, sink discarded); `parquet` = `COPY (SELECT *) TO '<local>.parquet'` |
| `run_reads.py` | The matrix below (159 runs) and its summary |
| `snapshot_facts.py` | §4 |
| `e2e_md.py` | §5–§6 (MotherDuck database `cdc_v2_spike_tp`, dropped at the end) |
| `hazards.py` | §8 |

## 2. Read rate with the sink discarded or parsed (whole table, 12.0 M rows)

Full table with all 53 variants: `evidence/p2/local_throughput/reads_summary.csv`. Medians of 3 runs; spread ≤ 10 %
unless noted.

### 2.1 Our own parallel COPY, `ctid` ranges

| Format, sink | N=1 | N=2 | N=4 | N=8 | client CPU s | backend CPU s | peak RSS (all workers) |
|---|---|---|---|---|---|---|---|
| binary, count | 1.07 M/s | 2.04 M/s | 3.64 M/s | 3.99 M/s | 10.3–11.5 | 10.4–12.6 | 60 MB per worker |
| text, count | 1.13 M/s | 2.23 M/s | 4.08 M/s | 4.37 M/s | 9.8–10.5 | 9.8–11.4 | 60 MB per worker |
| csv, count | 1.06 M/s | 2.09 M/s | 3.79 M/s | 4.09 M/s | 10.1–10.9 | 10.4–12.0 | 60 MB per worker |
| text → Arrow | 0.72 M/s | 1.39 M/s | 2.57 M/s | 3.12 M/s (18 %) | 16.1–18.0 | 9.7–11.7 | **1.96 / 2.21 / 2.72 / 3.26 GB** |
| csv → Arrow | 0.67 M/s | 1.31 M/s | 2.47 M/s | 2.89 M/s | 16.8–18.4 | 10.6–12.6 | **1.44 / 2.15 / 2.71 / 3.21 GB** |
| binary → psycopg row decode | 0.24 M/s | 0.47 M/s | 0.93 M/s | 1.51 M/s | 49.7–52.7 | 10.2–14.5 | 60 MB per worker |

MB/s at N=4: binary 774, text 606, csv 564 (wire bytes). A shared exported snapshot costs nothing measurable
(binary N=2: 2.04 vs 2.05 M/s; csv → Arrow N=2: 1.31 vs 1.34 M/s).

### 2.2 Our own parallel COPY, primary-key ranges

| Format, sink | N=1 | N=2 | N=4 | N=8 | backend CPU s |
|---|---|---|---|---|---|
| binary, count | 0.48 M/s | 0.84 M/s | 1.07 M/s | 1.23 M/s | 21.6–28.9 |
| text, count | 0.50 M/s | 0.88 M/s | 1.14 M/s | 1.28 M/s | 20.7–27.7 |
| csv, count | 0.49 M/s | 0.87 M/s | 1.08 M/s | 1.23 M/s | 21.3–28.6 |

Key ranges are read through the primary-key index: **twice the backend CPU per row** and a plateau near 1.2 M/s from
N=4 on, although the table was loaded in key order (the friendliest case for an index scan).

### 2.3 DuckDB's Postgres reader

| Variant | threads 1 | 2 | 4 | 8 | client CPU s | backend CPU s | peak RSS |
|---|---|---|---|---|---|---|---|
| hash (sink discarded) | 1.05 M/s | 1.94 M/s | 3.73 M/s | 4.78 M/s (15 %) | 6.5–8.1 | 10.5–12.2 | 75–81 MB |
| → local Parquet | 0.84 M/s | 1.60 M/s | 2.97 M/s | 3.80 M/s | 9.5–11.3 | 10.6–12.7 | 174 / 264 / 361 / 598 MB |

Settings at threads 4, hash sink (defaults: `pg_pages_per_task` 1000, `pg_use_binary_copy` true, `pg_use_ctid_scan`
true, `pg_pool_max_connections` 15):

| Setting changed | rows/s | Note |
|---|---|---|
| none | 3.73 M/s | |
| `pg_pages_per_task` 100 / 10000 | 3.49 / 3.53 M/s | small effect at this size (243 vs 2,429 vs 24 tasks) |
| `pg_use_binary_copy = false` | 3.72 M/s | text COPY is as fast here |
| `pg_use_ctid_scan = false` | 1.07 M/s | **one connection only**: no parallelism at all |
| `pg_pool_max_connections = 2` (threads 8) | 1.95 M/s | **caps the parallel connections** (same as threads 2) |

Which settings matter (current list from `duckdb_settings()` of this build and the
[extension documentation](https://duckdb.org/docs/2.0/core_extensions/postgres/overview.html)): the scan's
parallelism is `min(threads, pg_pool_max_connections, pages / pg_pages_per_task)`, and only with `pg_use_ctid_scan`.
`pg_connection_limit` and `pg_connection_cache` are deprecated in favour of `pg_pool_max_connections`
(`pg_pool_acquire_mode` 'force' did *not* lift the cap in our test). `pg_use_binary_copy` and `pg_use_text_protocol`
change the wire format (no speed difference measured for binary vs text COPY). `pg_statement_timeout_millis` and
`pg_idle_in_transaction_timeout_millis` set the session timeouts SPEC 4.5 asks for on scan connections. The ATTACH
option `ISOLATION_LEVEL` (default `REPEATABLE READ`) decides whether a snapshot is shared (§4). The 2.0 documentation
also lists `pg_query_mode`, which build `318dabb` (shipped for 1.5.6) does not have.

### 2.4 What limits each read route

Per-row costs (medians, this machine):

| | client CPU per row | Postgres backend CPU per row |
|---|---|---|
| COPY, bytes counted (psycopg, TLS) | 0.82–0.96 µs | 0.82–1.05 µs (ctid) · 1.7–2.4 µs (pk) |
| COPY → Arrow (pyarrow CSV parser) | 1.35–1.53 µs | 0.81–1.05 µs |
| COPY binary → psycopg row decode | 4.1–4.4 µs | 0.85–1.2 µs |
| DuckDB, sink discarded | 0.54–0.67 µs | 0.87–1.02 µs |
| DuckDB → Parquet | 0.79–0.94 µs | 0.89–1.06 µs |

- **The Postgres backend limits a single connection**: one backend process uses one core and delivers
  ≈ 1.0–1.1 M rows/s with a TID range scan, whatever the COPY format (backend CPU is the same for text, CSV and
  binary). Everything scales with the number of connections until this 10-core machine is saturated (N=8: client
  and backends together use 8–9 cores next to the background load).
- **With Arrow parsing in Python, the client becomes the limit** (0.93–0.96 client cores per worker vs 0.58 backend
  cores); per-row decoding in Python (psycopg `rows()`) is 3–4× worse still and is not an option.
- **DuckDB is the cheapest client** (0.6 µs/row discarded, 0.9 µs/row into Parquet, with C++ binary decoding), so with
  DuckDB the backend is always the limit.
- **Memory**: COPY workers that stream bytes stay at 60 MB each. Buffering a whole chunk and parsing it (32 chunks =
  62 MB of heap, ≈ 55 MB of CSV each) peaks at **1.4–2.0 GB per worker** (allocator retention plus buffer plus Arrow
  table): chunk size or a streaming parser must bound it. DuckDB stays at 75 MB (discarded) to 600 MB (Parquet, 8
  threads) and respects `memory_limit` (1 GB in the F16-shaped runs).

## 3. The Flight's shape (item 4)

An F16 Flight has a cgroup quota of 2 CPUs and 16 GiB (`docs/spike/flight_platform.md`; x86-64 AMD EPYC 7571).
Locally, with DuckDB `threads=2` and `memory_limit=1GB`, or 2 COPY worker processes:

| Variant (local, 2 client workers) | rows/s | client cores | backend cores | peak RSS |
|---|---|---|---|---|
| DuckDB threads 2, memory 1 GB, sink discarded | 1.95 M/s | 1.12 | 1.83 | 76 MB |
| DuckDB threads 2, memory 1 GB, → Parquet | 1.58 M/s | 1.33 | 1.46 | 271 MB |
| COPY binary, 2 workers, shared snapshot, count | 2.05 M/s | 1.82 | 1.87 | 121 MB |
| COPY csv → Arrow, 2 workers, shared snapshot | 1.34 M/s | 1.88 | 1.20 | 2.2 GB |

**Estimate (not a measurement)**: on a Flight the backends run on the managed replica, so the Flight's 2 vCPUs carry
only the client. With the per-row client costs of §2.4 and an assumed per-core slowdown of 1.5–2× for an EPYC 7571
core against this laptop's cores, 2 vCPUs allow roughly **1.0–1.5 M rows/s with DuckDB into Parquet** and
**0.65–0.95 M rows/s with COPY → Arrow** before the client is the limit. On the source side, ≈ 1 M rows/s per backend
core here means 2 connections need 2 replica cores to keep up; a smaller or busier managed replica makes **the source
the limit**, and the network carries 213 B/row in binary (≈ 150 B/row in text): 100 K rows/s is ≈ 21 MB/s, 1 M rows/s
≈ 210 MB/s. Either way **reading is 6–15× above the 100 K rows/s target of R5**; the end-to-end limit is the load into
MotherDuck (§5).

## 4. Snapshot facts (item 2)

From `snapshot_facts.json` (Postgres 18.6, hot standby):

| Question | Answer |
|---|---|
| `pg_export_snapshot()` on the hot standby | **works** (in `REPEATABLE READ READ ONLY`; also allowed in `READ COMMITTED`) |
| N readers attach with `SET TRANSACTION SNAPSHOT` | **works**: 4 importers on the standby saw the exported state; a row committed on the primary after the export (and replayed) was invisible to all 4 and visible to a fresh transaction |
| Import after the exporting transaction ended | fails: `snapshot "<id>" does not exist` — the exporter must stay open for the whole backfill |
| `SERIALIZABLE` on the standby | refused: `cannot use serializable mode in a hot standby` (HINT: use `REPEATABLE READ`) |
| Does DuckDB give its parallel connections one snapshot on a standby? | **Yes, on this version.** Source ([`postgres_scanner.cpp`](https://github.com/duckdb/duckdb-postgres/blob/main/src/postgres_scanner.cpp), `PostgresGetSnapshot`, `PostgresScanConnect`) and `pg_debug_show_queries` agree: the main connection runs `BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY; SELECT pg_export_snapshot()`, every further scan connection runs `BEGIN … REPEATABLE READ READ ONLY` + `SET TRANSACTION SNAPSHOT '<id>'`, then `COPY (SELECT … WHERE ctid BETWEEN '(a,0)'::tid AND '(b,0)'::tid) TO STDOUT (FORMAT "binary")` |
| How it can still go wrong in DuckDB | the export is a `TryQuery`: if `pg_export_snapshot()` fails (e.g. behind a transaction-mode pooler, or on a provider that refuses it) the scan **silently** runs with one snapshot per connection; with `ISOLATION_LEVEL 'READ COMMITTED'` it never shares one; with one task (`pages / pg_pages_per_task ≤ 1`) it uses one connection |

**Mixed-state test** (3 M-row table `snap_t`, `sum(bal) = 0` invariant; on the primary 4 writer threads moved one unit
between two random rows per transaction — 43,134 transactions — and a fifth rewrote 1 % of the rows every 0.2 s so
that tuples moved to other pages; parallel readers on the replica checked `count(*)`, `count(DISTINCT id)` and
`sum(bal)` while this ran, 8 rounds each):

| Reader on the replica | Mixed state seen |
|---|---|
| DuckDB, threads 8, `pg_pages_per_task` 200 (columns `id`, `bal`, 0.3 s per scan) | 0 of 8 |
| DuckDB, threads 2, `pg_pages_per_task` 50, every column (1.0–1.9 s per scan) | 0 of 8 |
| our COPY, 8 workers, shared exported snapshot (2.2–2.5 s, throttled) | 0 of 8 |
| our COPY, 8 workers, **one snapshot each** (negative control) | **3 of 8** (rows seen twice or missed, sum ≠ 0) |

The negative control shows that the test detects a mixed state; the shared-snapshot readers never showed one.

## 5. End to end, replica → MotherDuck staging table (item 3)

All routes load the full 12.0 M rows into a table of `cdc_v2_spike_tp` (row count verified every time); 3–4 runs
each, interleaved; `e2e.jsonl`, `e2e_summary.csv`. "laptop" = DuckDB threads 8, 4 COPY workers; "F16 shape" = DuckDB
threads 2, 2 COPY workers, memory 1–2 GB per process.

| Shape | Route | rows/s median (min–max) | wall s | phases |
|---|---|---|---|---|
| laptop | **upload bound**: `CREATE TABLE AS SELECT * FROM read_parquet(<one local 421 MB file>)` | 586 K (265 K–617 K) | 20.5 | — |
| laptop | (i) one DuckDB process, `CREATE TABLE stage AS SELECT * FROM pg.public.lineitem` | 555 K (208 K–564 K) | 21.6 | read and upload overlap |
| laptop | (ii) parallel COPY → Arrow → 32 local Parquet chunks, then one `CREATE TABLE AS … read_parquet(<chunks>)` | 429 K (97 K–482 K) | 28.0 | read 7.2 s + load 20.8 s |
| laptop | (iii) K loaders, one `INSERT … read_parquet(<chunk>)` per chunk (= per transaction), K=1 / 2 / 4 | 190 K / 378 K / 474 K | 63 / 32 / 25 | 1.7–2.4 s per 375 K-row INSERT |
| F16 shape | upload bound | 444 K (324 K–653 K) | 27.8 | — |
| F16 shape | (i) one DuckDB process | 510 K (403 K–632 K) | 23.5 | |
| F16 shape | (ii) COPY → Parquet chunks → one load | 365 K (126 K–389 K) | 32.9 | read 12.7 s + load 20.1 s |
| F16 shape | (iii) K loaders, K=1 / 2 | 207 K / 345 K | 58 / 35 | 1.6–1.7 s per INSERT |

What limits each route:

- **The upload to MotherDuck limits every route here.** The read side does 1.3–4 M rows/s (§2); every end-to-end
  route stays at or below the plain upload bound (≈ 450–600 K rows/s, ≈ 20–28 s for 421 MB of Parquet). The uplink
  itself varied by 2.5× between runs of the same file (19–50 s; one diagnostic pair: the same data uploaded in 26 s
  and 50 s minutes apart), which is the large min–max above. **This uplink is not a Flight's network**: these numbers
  bound nothing on a Flight.
- (i) overlaps reading and uploading in one statement and lands on the upload bound with the least machinery.
- (ii) adds the read phase to the upload instead of overlapping it (7–13 s for 12 M rows).
- (iii) **Each `INSERT` into MotherDuck has a large fixed cost**: 1.6–2.4 s for a 375 K-row (≈ 13 MB) chunk, against
  ≈ 20 s for 12 M rows in one statement. One loader with 32 chunk-sized transactions reaches only ≈ 200 K rows/s;
  K parallel loaders on the same table recover it (K=4 ≈ the upload bound). Concurrent appends from 4 connections to
  one table worked without conflicts.

## 6. Recommendation for the backfill reader

1. **Read with our own parallel `COPY` over `ctid` ranges (TID range scans), from the replica**, not with
   primary-key ranges (2× backend CPU, plateaus) and not with Debezium's snapshot (R3). Keep DuckDB's Postgres reader
   as the measured fallback and as the reference for cost: it is the cheapest client and it does share one snapshot on
   a standby, but it gives us no per-chunk control. The backfill needs exactly that control: per-chunk progress for
   resume (SPEC 3.7), per-chunk retries, per-chunk short transactions (the hazard in §8 — DuckDB holds every scan
   connection in one transaction for the whole scan), session timeouts, and a hard failure instead of DuckDB's silent
   fallback to unshared snapshots.
2. **Snapshot**: one exporter connection on the replica (`REPEATABLE READ READ ONLY`, `pg_export_snapshot()`) that
   **never reads the table** and stays open for the table's backfill; each chunk is read in its **own short
   transaction** that begins with `SET TRANSACTION SNAPSHOT`. Measured: no recovery conflict and ≤ 0.22 s replay lag
   under an `ACCESS EXCLUSIVE` lock on the primary (§8, `lock_chunked`). The exporter's snapshot is the backfill's
   consistent point; how it is tied to the stream position for the handoff is P3's design (not examined here).
3. **Connections on F16: 2** (one per vCPU; locally 2 connections give 1.3–2.0 M rows/s). More connections help only
   if the client has idle CPU, which a 2-vCPU Flight will not; the measured target is 100 K rows/s.
4. **Format: CSV** (or text) parsed into Arrow by a native parser, column types driven by the type mapping: same
   backend cost as binary, the cheapest robust parser available from Python (pyarrow), quoting handles any value.
   Binary needs a native decoder (DuckDB has one; psycopg's per-row path is 4× too slow). If client CPU on a Flight
   turns out to be the limit, the alternative is to let DuckDB decode (binary) per chunk; that is an open question
   (below), not a measured route.
5. **Chunking**: ranges of ≈ 2,000–8,000 pages (16–64 MB of heap, ≈ 100–400 K rows at `lineitem` width; a chunk
   takes well under a second per connection locally). Small enough that (a) a chunk transaction is short against
   `max_standby_streaming_delay` (it bounds how long a conflicting lock on the primary stalls replay — §8),
   (b) the parse stays inside a memory budget (whole-chunk buffering of 55 MB cost 1.4–2 GB RSS per worker: parse in
   bounded batches or keep chunks small), (c) a lost chunk is cheap to repeat (3.7: ≤ 1.1× re-read).
6. **Loading**: the per-statement cost of MotherDuck appends (≈ 2 s here) dominates small chunks. Read chunks are not
   load units: accumulate several chunks (≥ 100 MB of Parquet or ≥ 2–3 M rows) per `INSERT`, and/or run 2–4 loader
   connections appending to the staging table in parallel (both measured to help), recording chunk completion in the
   same MotherDuck transaction as the data. The bulk-load rate itself is §2 A 3's subject (MotherDuck semantics spike)
   and must be measured from a Flight.

## 7. Open questions for the same measurement on a Flight against a managed replica (stage 2)

1. Read rate per connection against a PlanetScale replica (replica cores, network RTT and bandwidth from the Flight's
   region, TLS cost on EPYC cores) and the number of connections at which the replica, not the Flight, is the limit.
2. Client CPU per row on a Flight core for COPY → Arrow and for DuckDB, to replace the 1.5–2× assumption of §3.
3. MotherDuck append rate from a Flight (same region): fixed cost per `INSERT`, rows/s per loader connection, the
   best K, and whether read and load overlap well enough to reach ≥ 100 K rows/s end to end (3.1).
4. Whether the provider allows `pg_export_snapshot()` / `SET TRANSACTION SNAPSHOT` on its replica (PlanetScale
   connections may pass through a proxy) and what `max_standby_streaming_delay` and `hot_standby_feedback` are set to
   (they decide §8's hazards). A pooler in transaction mode would break exported snapshots.
5. Whether DuckDB can read one chunk inside a transaction that imported our snapshot (e.g. through
   `postgres_execute` before the scan, threads 1), which would combine DuckDB's cheap decoding with our chunk control.
6. Replay lag on the managed replica under a full-speed backfill plus the 2,000 TPS workload of 5.3.

## 8. Hazards found

Setup: `hazards.py`, a 4 M-row copy of `lineitem` (`hz_li`). On the primary a modest write load (≈ 4.8 transactions/s,
each 200 inserts into another table plus an update of the line items of 40 orders of `hz_li`, ≈ 160 rows —
≈ 1,700 rows/s); on the replica an **active reader**
(one `REPEATABLE READ` transaction COPYing `hz_li` in a loop for 60–75 s, like a backfill worker that keeps its
snapshot) and an **idle reader** (same transaction kind, read once, then idle in transaction, like a worker between
chunks). Action on the primary at t = 5 s. Settings: `hot_standby_feedback = on`, `max_standby_streaming_delay = 30s`,
`vacuum_truncate = on` (`hazards.json`, `hazards_vacuum_truncate.json`, `hazards_lock_chunked.json`).

| Scenario | Active reader | Idle-in-transaction reader | Replay lag max |
|---|---|---|---|
| baseline (write load only) | finished (20 passes, 17 GB) | finished | 0.24 s, 0 B |
| `VACUUM hz_li` at 5 s and 30 s, with the update load | finished | finished | 0.27 s |
| `LOCK TABLE hz_li IN ACCESS EXCLUSIVE MODE` (and `COMMIT`) | **cancelled after 30.0 s** | **terminated** | **29.8 s**, 13.7 MB |
| `ALTER TABLE hz_li ADD COLUMN hz_extra int` | **cancelled after 30.0 s** | **terminated** | **30.1 s**, 7.9 MB |
| `VACUUM` that truncates the tail (`truncated 80993 to 72893 pages`) | **cancelled after 30.2 s** | **terminated** | **29.8 s**, 4.9 MB |
| `LOCK TABLE …` with the chunked reader (exporter never touches the table; one short transaction per chunk, 625 chunk transactions, max 0.15 s) | **finished, no error** | — | **0.22 s** |

Exact errors (psycopg `SerializationFailure`, SQLSTATE **40001**, `pg_stat_database_conflicts.confl_lock` +2 per
scenario):

- active reader: `canceling statement due to conflict with recovery` — DETAIL `User was holding a relation lock for
  too long.`, exactly `max_standby_streaming_delay` (30 s) after the lock was taken on the primary;
- idle reader: `terminating connection due to conflict with recovery` — same DETAIL; the connection is killed at the
  same moment, the client learns it at its next statement.

Hazards for the backfill (and for the stream):

1. **Any `ACCESS EXCLUSIVE` lock on a table on the primary cancels every replica transaction that holds a lock on
   that table after `max_standby_streaming_delay`, and stalls all replay on the replica until then.** Sources of such
   locks are ordinary: most `ALTER TABLE` forms (even a metadata-only `ADD COLUMN`), `LOCK TABLE`, `TRUNCATE`,
   `DROP`, `REINDEX` without `CONCURRENTLY`, `CLUSTER`, `VACUUM FULL`, and **plain (auto)vacuum when it truncates
   empty pages at the end of a table** (any table whose tail rows were deleted, such as a queue). A backfill that holds
   one long transaction per worker (the shape of DuckDB's reader and of the naive parallel `COPY`) is cancelled by
   any of these; a 60 M-row backfill takes minutes, so this will happen. Prevention: per-chunk transactions under an
   exporter that holds no relation lock (measured to avoid the conflict) and a per-chunk retry; chunk transactions
   much shorter than `max_standby_streaming_delay`.
2. **Replay stalls up to `max_standby_streaming_delay` while a replica query holds a conflicting lock.** In replica
   mode the logical slot lives on that replica, so a long backfill read can delay *every* change by up to 30 s (the
   default): this alone threatens 5.2 (p99 ≤ 30 s, max ≤ 60 s), and a provider setting of `-1` would make the stall
   unbounded. Per-chunk transactions bound it by the chunk time (measured ≤ 0.22 s lag).
3. **`hot_standby_feedback` holds back cleanup on the primary for as long as a replica snapshot lives** (observed: rows
   deleted on the primary stayed "dead but not yet removable" until the replica's next feedback; with a long exporter
   transaction that is the whole backfill). That is the protection that prevents snapshot conflicts (no cancellation
   in the vacuum scenario), and its cost is bloat on the primary during a long backfill. It also means a VACUUM cannot
   truncate the tail while the snapshot is older than the deletion, so the truncation hazard of point 1 needs tail
   rows that were already dead before the backfill's snapshot.
4. Without `hot_standby_feedback` (not tested here; a provider may turn it off) cleanup on the primary would cancel
   replica snapshots (`confl_snapshot`) after the same 30 s; the exporter itself would then be at risk.
5. Importing a snapshot fails once the exporter's transaction has ended (`snapshot "<id>" does not exist`): the
   exporter connection is a single point of failure for a table's backfill (it must be supervised, kept alive with
   timeouts that do not kill it, and on loss the table's backfill restarts from a new snapshot).
6. A table rewrite or `TRUNCATE` on the primary during a chunked backfill is not MVCC-safe in Postgres: later chunks
   read under the old snapshot would see the rewritten (or empty) relation. Not tested here; the backfill should
   compare `pg_relation_filenode()` per chunk and restart the table when it changes (P4 test).

## 9. Reproduce

```sh
tools/with-env uv run --group spike python spikes/local_throughput/setup_data.py --sf 2   # cluster up + data (≈ 1 min)
tools/with-env uv run --group spike python spikes/local_throughput/run_reads.py            # 159 runs, ≈ 30 min
tools/with-env uv run --group spike python spikes/local_throughput/run_reads.py --summarize
tools/with-env uv run --group spike python spikes/local_throughput/snapshot_facts.py
tools/with-env uv run --group spike python spikes/local_throughput/e2e_md.py --label laptop --repeats 3
tools/with-env uv run --group spike python spikes/local_throughput/e2e_md.py --label f16_shape --threads 2 --workers 2 --k 1,2 --repeats 3
tools/with-env uv run --group spike python spikes/local_throughput/e2e_md.py --summarize
tools/with-env uv run --group spike python spikes/local_throughput/hazards.py              # ≈ 7 min
tools/with-env uv run --group spike python spikes/local_throughput/e2e_md.py --drop        # drop cdc_v2_spike_tp
spikes/local_throughput/localpg down
```

`spikes/local_throughput/localpg` is `tools/localpg` with this spike's home (`~/.cache/cdc_flight_v2/pg-spike-tp`)
and port base 55500. The cluster is left stopped with `lineitem` loaded (6.4 GB on disk with WAL) for part 2 of
§2 A 5. Notes on the runs: the e2e routes of the first repeat had a slow uplink; e2e repeats between 03:19 and 03:26
UTC overlapped with the hazard runs (light load on the replica); the first `vacuum_truncate` attempt in
`hazards.json` did not truncate (the update load appended new tuple versions behind the deleted tail, and
`hot_standby_feedback` delays removal by up to `wal_receiver_status_interval`); the repeated run in
`hazards_vacuum_truncate.json` excludes both and truncated.
