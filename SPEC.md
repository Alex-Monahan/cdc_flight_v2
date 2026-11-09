# Postgres → MotherDuck CDC Flight — Build Spec v4 (DRAFT for refinement)

Status: **draft, not yet approved**. Nothing is built until the owner signs off on this document and `PLAN.md`.
Items marked **[OPEN Dn]** are decisions still to be confirmed; each has a proposed default that applies if not changed.

## 0. Sources and precedence

1. `spec/cdc_tool_decision_matrix_v3_for_swarm.md` — the 43-item rubric (215 points), copied verbatim. It is the scoring
   authority. It mentions review files and "fix rounds" that do not exist in this repository; those references are
   historical, carry no requirements, and must not be searched for.
2. This document — the owner's rulings plus measurable acceptance criteria. Where it is more specific than the rubric,
   this document wins. Where they conflict, stop and ask; do not pick.
3. `PLAN.md` — how the work is organised. It never overrides 1 or 2.

Target score: **214 / 215** (item 7.3 is capped at 4 by ruling R10). Every other item targets its 5 band.

## 1. Owner rulings (binding)

| # | Ruling | Affects |
|---|---|---|
| R1 | Clean-slate implementation. The only reused artifact is the rubric file. | all |
| R2 | **Stock Debezium**: unmodified released Debezium artifacts, version pinned. No forks, no patched or shadowed classes. Configuration and our own code around it are allowed. | 7.1, all streaming |
| R3 | Debezium is required for the streaming (WAL) path. Snapshots/backfills use **another approach** whenever Debezium's snapshot cannot reach the R5 target (expected: direct parallel reads from Postgres). | 3.x, 1.6 |
| R4 | Runs as a **MotherDuck Flight**. The Flight's `main.py` is a small bootstrap that **clones this git repo** at a pinned commit and runs it (avoids the Flight source-size limit). | packaging |
| R5 | Performance target for 3.1 and 5.1: **100K rows per second** at **TPC-H `lineitem` row width** (16 columns). **[OPEN D1]** the requested "F8" size does not exist (sizes are F4 = 0.5 vCPU/4 GB, F16 = 2 vCPU/16 GB, F32 = 4 vCPU/32 GB); proposed: grade on **F16**, also report F4. **[OPEN D2]** "100K rows" is read as rows/second. | 3.1, 5.1 |
| R6 | Transaction atomicity (1.3) **must hold at benchmark rate**. The benchmark may use one table; multiple tables replicating in parallel must be supported and tested. | 1.3, 5.1, 5.3 |
| R7 | 3.2: a staged load followed by drop-and-rename inside one `BEGIN`/`COMMIT` scores 5. | 3.2 |
| R8 | A heartbeat and a signal table on the primary are acceptable and count as "light". | 4.4, 7.2 |
| R9 | 2.5: when the old and new type can be stored losslessly in one type, **upgrade in place** (integer → bigint). When they cannot (float vs bigint), the column becomes a **MotherDuck `UNION`** of both. | 2.5 |
| R10 | DuckLake is **out of scope**. 7.3 targets 4. | 7.3 |
| R11 | 2.6 and 3.3 contain subjective wording ("efficiently", "simple and elegant"). They are scored by an **independent reviewer subagent** with fresh context, against the checklists in §3. | 2.6, 3.3 |
| R12 | 4.3: the problematic-WAL cases must be **discovered up front and prevented**, not merely recovered from. | 4.3 |
| R13 | 6.2: alerts go to **a Slack channel and a MotherDuck table**. | 6.2 |
| R14 | Implementer decides the SCD2 shape, PII config surface and salt source (see 8.2, 8.3). | 8.x |
| R15 | Test source is the owner's **PlanetScale Postgres** instance, plus a local Postgres for hermetic fault injection. | testing |
| R16 | Rubric clarifications already in the file stay binding: `money`/`inet` (and comparably obscure types) delivered losslessly **as text** is 5/5 and **refusing** to deliver a value is never acceptable (2.4); rename-swap is 5 and no complexity is added just to reach `CREATE OR REPLACE` (3.2); heartbeat writes go to the primary on a separate connection while CDC reads from the replica (7.2). | 2.4, 3.2, 7.2 |

## 2. Platform facts the design must respect

- Flight sizes: F4, F16, F32 only. Maximum run time is 8 hours (Business/Enterprise) or 1 hour (Lite); a continuous
  pipeline therefore **must hand over between runs** without violating 5.2 more than the declared restart window.
- Flight source is one `main.py` (≤ 200 KB) plus requirements (≤ 20 KB). Only `git` is preinstalled: no JVM. Local disk
  is not durable across runs. Cron granularity is one minute. Outbound connections only, from fixed egress IPs.
- Secrets arrive as environment variables. No secret value may appear in source, config, arguments, logs, alerts or
  MotherDuck tables.
- Loading MotherDuck row by row is not acceptable; all writes are set-based batches.
- Local emulation is never proof of Flight behaviour. Every performance number and every item marked *(Flight)* below
  is measured in a real Flight run.

## 3. Acceptance criteria per rubric item

Conventions: *source commit* = the moment Postgres acknowledges `COMMIT`; *visible* = returned by a query on a separate
MotherDuck connection. *Oracle* = an automated comparison of source and destination (row counts plus order-independent
checksums per table, taken at a quiesced LSN). Every item needs at least one automated test and a stored evidence
artifact; "it worked when I tried it" is not evidence.

### §1 Delivery guarantees and correctness

**1.1 / 1.2 Exactly-once, with and without primary key (5).** The applied position is committed in the *same*
MotherDuck transaction as the data it describes; the slot is acknowledged only afterwards; replays are discarded by
position. Tables without a key are exactly-once by WAL position: each change is applied once, and with
`REPLICA IDENTITY FULL` a delete or update of one of several identical rows affects exactly one row. Verified by the
oracle after every fault-injection run (1.7), including tables with duplicate rows.

**1.3 Multi-table transactional batches (5).** All changes of one Postgres transaction, across all tables, become
visible in one MotherDuck transaction. A source transaction is never split across destination commits, **with no size
exemption** (R6). Test: a consumer loop checks cross-table invariants (e.g. debits = credits, parent/child counts)
continuously during load, including a single ≥ 10M-row transaction, and never sees a violation. A table that is still
in its initial backfill joins the guarantee when it reaches the streaming state; this is documented, visible in the
table-status log, and the only exception.

**1.4 Primary-key update (5).** Old key removed and new key present, atomically. Tests include chains (A→B, B→C) and
swaps inside one transaction, under default and full replica identity, and with history mode on.

**1.5 Truncate and drop (5).** `TRUNCATE` (including multi-table and `CASCADE`) empties the destination tables in
commit order within the same destination transaction as neighbouring changes. `DROP TABLE` drops the destination table;
drop-and-recreate under the same name yields a fresh table. **[OPEN D11]** proposed default mirrors Postgres exactly
(destructive); a per-table `on_source_drop: retain` option renames instead of dropping.

**1.6 Snapshot consistent with CDC (5).** A backfill taken under concurrent writes converges to oracle equality with
no lost and no duplicated change, for keyed and keyless tables, for initial snapshots, single-table re-backfills and
resumed backfills.

**1.7 Robust failure injection (5).** A named crash point exists at every step of the commit/acknowledge protocol,
backfill chunk commit, schema change and table swap. The suite kills the process (`SIGKILL`) at each crash point with
≥ 3 seeds, injects network faults through a TCP proxy (drop, blackhole, reset, latency), destination errors, source
backend termination, slot loss and disk-full on the spill area, and runs a randomized soak of ≥ 1 hour with ≥ 200
faults. Pass = oracle equality after every run, zero divergences.

**1.8 Slot advanced externally (5).** On **every slot acquisition** (and periodically while streaming) the slot's
position and identity are compared with the durably applied position. Slot ahead of applied position, slot missing or
recreated, or slot invalidated ⇒ alert plus **automatic backfill** of the affected tables; the Flight does not exit.
Tests: `pg_replication_slot_advance`, drop-and-recreate, changes consumed by another session, slot invalidated by
`max_slot_wal_keep_size`.

**1.9 State machines (5).** Consistency-relevant state is held in several small explicit state machines, each with an
enumerated state set and a transition table, persisted transactionally with the data they govern, changed only through
one validated transition function, and logged on every transition. Expected machines: leader/run lifecycle, slot and
stream, per-table replication lifecycle, per-table backfill progress, schema-change application, batch commit/ack,
quarantine. Tests enumerate every legal transition and assert illegal ones raise.

### §2 Schema evolution and types

**2.1 Add/drop column (5).** Destination follows without restart or backfill. `ADD COLUMN … DEFAULT` must leave
existing destination rows equal to the source (Postgres emits no changes for them) — oracle-checked.

**2.2 Rename (5).** Column rename is applied in place: same data, no tombstone column, no backfill. Table rename is
handled the same way.

**2.3 Discovery of new tables and schemas (5).** **[OPEN D5]** proposed: the source catalog is polled every 30 s
(configurable 5–300 s, on the replica when one is configured). A table matching the include/exclude patterns (default:
all non-system schemas) exists in MotherDuck ≤ 60 s after its `CREATE TABLE` commits, and rows committed after
discovery meet the 5.2 latency bound. Rows that existed before the table joined the publication are loaded by an
automatic per-table snapshot. New schemas are created automatically. The Flight maintains the publication itself.
Global PII rules (8.3) apply to newly discovered tables before their first row lands.

**2.4 Types (5).** A complete mapping table is a deliverable (`docs/type_mapping.md`). Core scalars, arrays, composites,
JSON, ranges, enums, UUID, bytea, temporal and interval types map to native MotherDuck types; NaN and ±infinity
round-trip for every type where DuckDB supports them; obscure types arrive as lossless text (R16). **No value is ever
refused**: a value the native type cannot hold is still delivered losslessly (by the R9 mechanism or as text).
**[OPEN D10]** unconstrained `numeric` (and precision > 38, NaN, ±Infinity) — proposed: `DECIMAL(38, s)` while values
fit, automatically becoming `UNION(DECIMAL, VARCHAR)` on the first value that does not.

**2.5 Type changes (5).** Per R9. Lossless widenings are applied in place. Otherwise the column becomes
`UNION(old, new)`: existing values keep their original member, new values use the new one; further changes add members.
Tests cover int→bigint, float4→float8, varchar(n)→text, numeric growth, bigint↔float8, text→integer, and a second
change on an already-union column.

**2.6 TOAST handled efficiently (5; reviewer-judged, R11).** Checklist: unchanged TOASTed values are neither re-read
from the source nor rewritten in the destination; `REPLICA IDENTITY FULL` is not required; no per-row statements; a
bounded number of statement shapes per batch regardless of which columns are unchanged; correct for insert-then-update
in one batch, consecutive updates, and updates overlapping a backfill; memory stays bounded with 100 MB values.

### §3 Backfill and refresh modes

**3.1 Fast, parallel backfill (5) *(Flight)*.** ≥ 100K rows/s sustained, measured from backfill start to the swapped
table being visible, on a `lineitem`-width table of ≥ 60M rows (TPC-H SF10), with memory bounded per 5.4. Parallel
within a table and across tables. The report states the Flight size, the source instance size, and — if the source is
the limit — the source-limited rate next to the destination-only rate. Not using Debezium's snapshot is expected (R3).

**3.2 Atomic backfill (5).** Per R7/R16: load into a staging table, then swap inside one transaction. A consumer loop
on a separate connection polls count and checksum throughout and only ever observes the complete old version or the
complete new version: never a missing, empty or partial table.

**3.3 CDC continues during snapshots (5; reviewer-judged, R11).** While any table is being snapshotted, every table
already streaming keeps meeting 5.2. Reviewer checklist for "simple and elegant": one mechanism serves initial
snapshot, newly discovered table, operator re-backfill, automatic re-backfill and resume; no duplicated code paths; the
interaction between snapshot and stream is expressed in one small state machine; no stop-the-world phase.

**3.4 Arbitrary table sets (5).** An operator can request a backfill of any subset of tables (control table in
MotherDuck, or a Postgres signal table) while all other tables keep streaming.

**3.5 Per-table mode: CDC, scheduled full refresh, scheduled incremental refresh (5).** **[OPEN D6]** proposed:
`mode: cdc | full_refresh | incremental` per table, with a cron expression for the two scheduled modes (one-minute
floor). Full refresh uses the 3.2 staging + swap. Incremental = `cursor_column` high-watermark query with a
configurable lookback window, upserting by primary key (append-only if the table has no key), plus an optional periodic
full reconcile to catch hard deletes, which a cursor query cannot see. Schedules run inside the long-lived Flight; a
deployment with no CDC tables may run on Flight cron instead. Changing a table's mode triggers a re-snapshot.

**3.6 Backfill when CDC falls too far behind (5).** **[OPEN D7]** proposed: two configurable thresholds — size (WAL
bytes between applied position and current WAL position; default 50 % of `max_slot_wal_keep_size`) and time (now −
commit time of the last applied transaction; default 6 h). A backfill is triggered only when a threshold is exceeded
**and** lag has not been shrinking for 10 minutes, with a cooldown to prevent flapping. Where possible only the tables
responsible are shed and backfilled while the rest keep streaming. Any resulting gap in a history table (8.2) is
recorded explicitly.

**3.7 Resume mid-backfill (5).** Progress is recorded per table and per chunk in MotherDuck (local disk is not
durable). After a crash or a run handover, completed chunks are not read again: total source rows read ≤ 1.1 × table
size for a kill at 50 %, and the oracle passes.

### §4 Failure detection and recovery

**4.0 Blast-radius containment (5).** A row or table that can never be processed is moved automatically to a durable,
queryable quarantine after a bounded number of retries (default 3), with an alert. Every healthy table continues to be
created and to receive data, and the slot keeps advancing so WAL retention stays bounded. Quarantined tables are
retried on a slow schedule and on operator request. Given R16, quarantine is a last resort, not a substitute for
delivering awkward values.

**4.1 Slot failure and failover (5).** The slot is created with `failover = true` where the server supports it, and
this is verified at every acquisition. After a primary failover the Flight resumes from the synced slot with no
backfill once position continuity is proven; if the slot is absent, unsynced or invalid, it backfills automatically.
Tested with a real failover on PlanetScale *(Flight)* and with a local primary/standby pair.
**[OPEN D9]** the default topology on PlanetScale: slot on the primary with failover (4.1) versus decoding from a
replica (7.2). Both configurations must work; proposed default is primary + failover slot with snapshot reads sent to
a replica.

**4.2 Concurrent Flights (5).** **[OPEN D8]** proposed: a lease with a monotonically increasing fencing epoch lives
in MotherDuck and is checked inside every destination transaction. Exactly one run writes at any time. A newcomer
detects a live leader before touching the slot or any DDL and exits within 10 s: exit 0 and no alert if it is a
scheduled run of the same Flight (the keep-alive case); non-zero exit and an alert if a different Flight is contending
for the same slot or destination. Takeover happens only after lease expiry (≤ 90 s). A paused-then-resumed old leader
has every write rejected. Tests: 3 simultaneous starts, `SIGSTOP`/`SIGCONT` of the leader across a takeover.

**4.3 Problematic WAL messages (5).** Per R12 the deliverable is `docs/wal_hazards.md`: a researched catalog of every
class of WAL content or stream behaviour that can stall, crash or corrupt the pipeline, each with a reproduction, a
**prevention**, and a test. Minimum classes: oversized transactions; oversized rows and values; unchanged-TOAST
placeholders; schema changes mid-stream, including DDL and DML in one transaction and drop/recreate with a new OID;
values that cannot be decoded or represented; replica-identity problems; truncate variants; logical messages;
origin, two-phase and streamed-transaction messages; partition attach/detach; generated and identity columns,
publication column lists and row filters; keepalive and sender timeouts during long applies; redelivery after
restart and empty-transaction floods; rename versus drop+add ambiguity; timeline changes after failover. A randomized
DDL+DML fuzzer runs against the oracle. As the last line of defence, a message that still fails repeatedly at the same
position is skipped, its table is backfilled automatically, and 4.0 applies if that fails too. The pipeline never
hangs and never needs a restart to recover.

**4.4 Idle-slot heartbeat (5).** A periodic lightweight write on the primary (default every 10 s) keeps the slot
advancing when replicated tables are quiet and unreplicated tables are busy. Test: retained WAL stays bounded for 30
minutes under exactly that workload.

**4.5 No hangs or locks (5).** Every blocking call has a deadline. Each component (stream reader, applier, backfill
workers, any child process) reports progress to an in-process supervisor, which restarts a component that is silent
beyond its deadline (default 120 s). Source sessions set statement, lock and idle-in-transaction timeouts; no source
lock is held across a network wait; recovery never requires killing a Postgres backend by hand. Tests: blackholed
source mid-stream, blackholed destination mid-commit, frozen child process, a conflicting exclusive lock during a
snapshot.

**4.6 Silently dead connections (5).** TCP keepalives (detection ≈ 2 min) plus an end-to-end heartbeat: a marker
written on the primary must be observed on the stream within 60 s, otherwise the connection is torn down and rebuilt.

**4.7 Self-healing (5).** `docs/failure_modes.md` lists every known failure with its automatic recovery and its test.
No failure requires a human to restore consistency. When the cause is external and only a human can fix it (revoked
credentials, exhausted quota, source deleted) the Flight alerts, keeps retrying with backoff, and resumes by itself
once the cause is removed.

### §5 Performance, latency and scale

**5.1 Large changes (5) *(Flight)*.** Single transactions of ≥ 10M rows (insert, update, delete) are applied
correctly and atomically (R6) with bounded memory. Target apply rate 100K rows/s at `lineitem` width (R5).
**[OPEN D3]** if the measured ceiling of stock Debezium streaming on the chosen Flight size is below the target, the
choice between a larger Flight, shedding oversized changes to the backfill path (3.6), or accepting the measured
number returns to the owner with data.

**5.2 Low latency (5) *(Flight)*.** **[OPEN D4]** proposed definition: latency = source commit → visible. A canary
row is written every 5 s and its latency is logged in MotherDuck. Pass = **p99 ≤ 30 s and max ≤ 60 s over 24 h**,
both when otherwise idle and with a 2,000 TPS background load, with p50 ≤ 5 s as a goal. Declared windows (run
handover, failover, the affected table's own backfill) are excluded from the percentile but reported, and must return
under 30 s within 120 s.

**5.3 High TPS (5) *(Flight)*.** ≥ 2,000 small transactions per second sustained for 30 minutes with 5.2 holding and
1.3 verified, on one table and on 8 tables in parallel; the breaking point is reported.

**5.4 Memory (5).** All buffers have configured byte limits with back-pressure to the reader; staging spills to disk
under a quota; any JVM heap is capped. Tests (50M-row transaction; 1 GB of TOAST values) keep peak memory ≤ 70 % of the
instance and never crash on a full spill area.

### §6 Observability and alerting

**6.1 Logs in MotherDuck (5).** An operations schema in MotherDuck (**[OPEN D12]** proposed: schema `_cdc` in the
destination database) holds structured logs, per-interval lag metrics (WAL bytes behind, time behind, slot positions,
rows/s, commit latency), per-table status and state transitions, batches, backfills, schema changes, quarantine,
canary latency and alerts. Log writes are batched and never delay data.

**6.2 Alerts (5).** Built-in rules (lag, slot health, backfill triggered, quarantine, takeover, schema-change
failure, heartbeat loss, memory/disk pressure, latency breach) write to an alerts table and post to Slack (R13) with
de-duplication, rate limiting and a "resolved" message. Slack being down never blocks replication; undelivered alerts
are retried from the table. A dead leader is reported by the next scheduled run that takes over.

### §7 Postgres features

**7.1 pgoutput (5).** Stock Debezium with `pgoutput`; nothing is installed on the source.

**7.2 Replica (5).** Streaming and snapshot reads can be served by a replica; the primary only sees the heartbeat, the
optional signal table and publication maintenance (R8, R16). See D9 for how this combines with 4.1.

**7.3 Partitioned tables (target 4, R10).** Per-partition tables or one combined table, selectable per partitioned
table; attach, detach and drop of partitions are handled without a restart.

**7.4 Logical messages (5).** `pg_logical_emit_message` messages matching configured prefixes, transactional and
non-transactional, land in a MotherDuck table in commit order; transactional ones are atomic with their transaction.

### §8 CDC features

**8.1 Deletes (5).** Per table `delete_mode: hard | soft`. Soft adds `_cdc_deleted` and `_cdc_deleted_at`; truncate
marks all rows.

**8.2 History / SCD2 (5).** Per table `history: off | only | both`. `both` keeps the current-state table and
`<table>__history`; `only` keeps just the history. The history table is one row per row version: all source columns
plus `_cdc_op`, `_cdc_lsn`, `_cdc_commit_ts`, `_cdc_xid`, `_cdc_seq`, `_cdc_valid_from`, `_cdc_valid_to`,
`_cdc_is_current`. Closing the previous version happens in the same transaction as the batch. Keyless tables get a
plain changelog. Versions that could not be observed (after a re-backfill) are recorded as gaps, never invented.

**8.3 PII (5).** Ordered rules in the config file: `match` (regexes on schema, table, column) → `action`:
`exclude`, `mask` (constant, keep-first/last-n, or value regex + replacement), `hash` (HMAC-SHA-256, hex), `truncate`
(length for text; unit for dates/timestamps; precision for numbers). Rules run inside the Flight before a value reaches
any staging file, table, history, quarantine, log or alert, on both the snapshot and streaming paths, and apply to
newly discovered tables. The salt is a Flight secret; only a fingerprint is stored, so a changed salt is detected and
blocks mixing pseudonyms until the affected tables are re-backfilled. Hashing a key column is allowed (deterministic);
excluding or masking one is a config validation error.

## 4. Evidence and grading

- Each item: an automated test, an evidence file under `evidence/<item>/`, and a one-paragraph claim.
- Grading is done by subagents with fresh context that did not write the code, against this document. Reviewer-judged
  items (R11) use a separate reviewer.
- *(Flight)* items are graded only on real Flight runs against PlanetScale.

## 5. Open decisions

| # | Decision | Proposed default |
|---|---|---|
| D1 | Flight size for grading ("F8" does not exist) | F16; F4 also reported |
| D2 | Meaning of "100K rows" | rows per second, for backfill and for CDC apply |
| D3 | Stock Debezium streaming measured below 100K rows/s | decide with spike data |
| D4 | Definition of "consistently under 30 seconds" | p99 ≤ 30 s, max ≤ 60 s over 24 h, declared windows excluded |
| D5 | "Short interval" for discovery | 30 s poll, table present ≤ 60 s, auto-include by pattern |
| D6 | Meaning of "incremental refresh" | cursor column + lookback + optional reconcile |
| D7 | Lag thresholds and scope | 50 % of WAL cap or 6 h, not shrinking for 10 min, shed per table |
| D8 | Concurrent-Flight behaviour | lease + fencing; first wins; takeover on expiry |
| D9 | Default topology on PlanetScale | primary + failover slot; replica mode also supported |
| D10 | Unconstrained `numeric` | DECIMAL while it fits, else UNION with text |
| D11 | Mirroring destructive DDL | mirror by default; `retain` option |
| D12 | Location of the operations schema | `_cdc` schema in the destination database |
| D13 | MotherDuck plan (sets the maximum run time) | assume 8 h; hand over at 7 h 30 min |
