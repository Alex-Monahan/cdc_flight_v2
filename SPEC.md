# Postgres → MotherDuck CDC Flight — Build Spec v4 (DRAFT for refinement)

Status: **draft, revision 2 (2026-11-09)** — the owner's decisions on D1 and D4–D13 are folded in. Nothing is built
until the owner signs off on this document and `PLAN.md`. Items marked **[OPEN Dn]** are still to be confirmed; each
has a proposed default that applies if not changed.

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
| R5 | Performance target for 3.1 and 5.1: **100K rows per second** at **TPC-H `lineitem` row width** (16 columns), graded on an **F16** Flight (2 vCPU / 16 GB). **[OPEN D2]** "100K rows" is read as rows/second. | 3.1, 5.1 |
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
| R17 | **Replica first.** By default the pipeline loads the primary as little as possible: WAL decoding, snapshot and backfill reads and catalog polling run against a replica. The primary only receives the light heartbeat, the optional signal table and publication maintenance. Decoding from the primary is a supported, non-default mode. | 7.2, 4.1, 4.4, 2.3 |
| R18 | **Concurrency guard.** A starting run checks for a healthy heartbeat from an existing run. If there is one, it does not start. If there is none, it starts, kills the existing run and becomes the leader. The Flight is scheduled every minute, and this same check performs the hand-over when a run ends at the platform's run-time limit. | 4.2, 5.2 |
| R19 | Latency: p99 ≤ 30 s and max ≤ 60 s. Run hand-over, failover and a table's own backfill are excluded from the statistic but reported. | 5.2 |
| R20 | New tables replicate automatically, with global PII rules applied first. A configuration switch turns automatic replication of new tables off. | 2.3, 8.3 |
| R21 | The pipeline may be at most **2 hours** behind: the lag trigger is 2 hours of overall WAL lag time (not per-table lag) that is not shrinking. No per-table shedding. | 3.6 |
| R22 | The solution must be battle-tested and extremely robust: extensive testing and extensive fuzzing, with the quantitative minimums of the robustness bar in §4. | 4.3, 1.7, all |

## 2. Platform facts the design must respect

- Flight sizes are F4, F16 and F32; this project is graded on **F16**. Maximum run time is 8 hours
  (Business/Enterprise) or 1 hour (Lite). The Flight is therefore scheduled **every minute**: the 4.2 guard makes every
  run but the leader exit at once, and performs the hand-over when a run ends (R18).
- Flight source is one `main.py` (≤ 200 KB) plus requirements (≤ 20 KB). Only `git` is preinstalled: no JVM. Local disk
  is not durable across runs. Cron granularity is one minute. Outbound connections only, from fixed egress IPs.
- A Flight runs as the account that created it. This project's Flights are created by a dedicated MotherDuck service
  account that can see only this project's databases.
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
drop-and-recreate under the same name yields a fresh table. Mirroring Postgres exactly is the default; a per-table
`on_source_drop: retain` option renames the destination table instead of dropping it.

**1.6 Snapshot consistent with CDC (5).** A backfill taken under concurrent writes converges to oracle equality with
no lost and no duplicated change, for keyed and keyless tables, for initial snapshots, single-table re-backfills and
resumed backfills.

**1.7 Robust failure injection (5).** A named crash point exists at every step of the commit/acknowledge protocol,
backfill chunk commit, schema change and table swap. The suite kills the process (`SIGKILL`) at each crash point with
≥ 3 seeds, injects network faults through a TCP proxy (drop, blackhole, reset, latency), destination errors, source
backend termination, slot loss and disk-full on the spill area, and runs a randomized soak of ≥ 1 hour with ≥ 200
faults. Pass = oracle equality after every run, zero divergences. The robustness bar in §4 (R22) extends this suite.

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

**2.3 Discovery of new tables and schemas (5).** The source catalog is polled every 30 s (configurable 5–300 s, on the
replica). A table matching the include/exclude patterns (default: all non-system schemas) exists in MotherDuck ≤ 60 s
after its `CREATE TABLE` commits, and rows committed after discovery meet the 5.2 latency bound. Rows that existed
before the table joined the publication are loaded by an automatic per-table snapshot. New schemas are created
automatically. The Flight maintains the publication itself (an occasional light statement on the primary). Global PII
rules (8.3) are applied to a newly discovered table before its first row lands. For deployments where PII is critical,
`discovery.auto_replicate: false` turns automatic replication off: new tables are then detected, logged and alerted
on, but replicate only after an operator adds them to the configuration.

**2.4 Types (5).** A complete mapping table is a deliverable (`docs/type_mapping.md`). Core scalars, arrays, composites,
JSON, ranges, enums, UUID, bytea, temporal and interval types map to native MotherDuck types; NaN and ±infinity
round-trip for every type where DuckDB supports them; obscure types arrive as lossless text (R16). **No value is ever
refused**: a value the native type cannot hold is still delivered losslessly (by the R9 mechanism or as text).
Unconstrained `numeric` (and precision > 38, NaN, ±Infinity) is stored as `DECIMAL(38, s)` while values fit and
automatically becomes `UNION(DECIMAL, VARCHAR)` on the first value that does not.

**2.5 Type changes (5).** Per R9. Lossless widenings are applied in place. Otherwise the column becomes
`UNION(old, new)`: existing values keep their original member, new values use the new one; further changes add members.
Tests cover int→bigint, float4→float8, varchar(n)→text, numeric growth, bigint↔float8, text→integer, and a second
change on an already-union column.

**2.6 TOAST handled efficiently (5; reviewer-judged, R11).** Checklist: unchanged TOASTed values are neither re-read
from the source nor rewritten in the destination; `REPLICA IDENTITY FULL` is not required; no per-row statements; a
bounded number of statement shapes per batch regardless of which columns are unchanged; correct for insert-then-update
in one batch, consecutive updates, and updates overlapping a backfill; memory stays bounded with 100 MB values.

### §3 Backfill and refresh modes

**3.1 Fast, parallel backfill (5) *(Flight)*.** ≥ 100K rows/s sustained on F16, measured from backfill start to the swapped
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

**3.5 Per-table mode: CDC, scheduled full refresh, scheduled incremental refresh (5).**
`mode: cdc | full_refresh | incremental` per table, with a cron expression for the two scheduled modes (one-minute
floor). Full refresh uses the 3.2 staging + swap. Incremental = `cursor_column` high-watermark query with a
configurable lookback window, upserting by primary key (append-only if the table has no key), plus an optional periodic
full reconcile to catch hard deletes, which a cursor query cannot see. Refresh queries read from the replica (R17).
Schedules run inside the long-lived Flight. Changing a table's mode triggers a re-snapshot.

**3.6 Backfill when CDC falls too far behind (5).** Per R21, lag is measured for the **whole stream**, never per table.
*Time lag* = now − source commit time of the last applied transaction (the 4.4 heartbeat keeps this meaningful when
tables are idle). *Size lag* = WAL bytes between the applied position and the current WAL position. Triggers, both
configurable: time lag > **2 hours**, or size lag > 50 % of the WAL retention cap on the server that holds the slot —
in either case only when lag has **not been shrinking for 10 minutes**. When triggered, the backlog is abandoned, the
stream restarts from the current WAL position and all CDC tables are re-backfilled through the normal mechanism (3.2,
3.3), so consumers keep the last complete version of each table until its swap. An alert is raised, and no new
lag-triggered backfill starts while one is running or within 30 minutes of its completion. There is no per-table
shedding. Any resulting gap in a history table (8.2) is recorded explicitly. Note: lag may exceed 2 hours for as long
as it is still shrinking.

**3.7 Resume mid-backfill (5).** Progress is recorded per table and per chunk in MotherDuck (local disk is not
durable). After a crash or a run handover, completed chunks are not read again: total source rows read ≤ 1.1 × table
size for a kill at 50 %, and the oracle passes.

### §4 Failure detection and recovery

**4.0 Blast-radius containment (5).** A row or table that can never be processed is moved automatically to a durable,
queryable quarantine after a bounded number of retries (default 3), with an alert. Every healthy table continues to be
created and to receive data, and the slot keeps advancing so WAL retention stays bounded. Quarantined tables are
retried on a slow schedule and on operator request. Given R16, quarantine is a last resort, not a substitute for
delivering awkward values.

**4.1 Slot failure and failover (5).** *Default (replica mode, R17):* the slot lives on a replica. It survives a
restart of that replica and a promotion of that replica, and the Flight then resumes with no backfill once position
continuity is proven. If the replica or its slot is lost or invalidated (replica replaced, recovery conflict, WAL cap
exceeded), the Flight attaches to another configured replica, creates a new slot and **backfills automatically**.
*Primary mode (non-default):* the slot is created with `failover = true`, verified at every acquisition; after a
primary failover the Flight resumes from the synced slot with no backfill once continuity is proven, and backfills
automatically if the slot is absent, unsynced or invalid. Both modes are tested with a real failover on PlanetScale
*(Flight)* and with a local primary/standby pair. If no replica is configured the Flight runs in primary mode and says
so in its status. **[OPEN D15]** all replicas unavailable: proposed — keep retrying and alerting for 15 minutes
(configurable, or never), then fall back to primary mode until a replica returns. **[OPEN D17]** if the provider does
not permit logical decoding on a replica, primary mode with snapshot reads on the replica becomes the default there.

**4.2 Concurrent Flights (5).** Per R18. A *leader record* in MotherDuck holds the run id, Flight id, a monotonically
increasing fencing epoch, a heartbeat time and a state. The leader refreshes the heartbeat every 10 s, and only while
its supervisor (4.5) sees every component making progress; a heartbeat older than 30 s is *not healthy*. The Flight is
scheduled every minute, and every run begins with the guard, before it clones, starts a JVM, touches the slot or runs
any DDL:
- **Healthy heartbeat** ⇒ the newcomer exits within 10 s: exit 0 and no alert when it is a scheduled run of the same
  Flight; non-zero exit and an alert when a *different* Flight contends for the same slot or destination.
- **No healthy heartbeat** (stale, released or absent) ⇒ the newcomer takes over: it raises the fencing epoch in one
  transaction (exactly one winner among simultaneous starters), **kills the previous run** — cancels it through the
  MotherDuck API where available and terminates its replication session on the source — then becomes leader. A
  takeover raises an alert unless the previous leader had released cleanly.

The epoch is checked inside every destination transaction, so a paused-then-resumed old leader has every write
rejected and shuts itself down when it sees a newer epoch. *Planned hand-over:* before the platform's run-time limit
(`run.max_runtime`, default 7 h 30 min; 50 min on a Lite plan) the leader commits, marks the record `released` and
exits just before a minute boundary; the next scheduled run takes over through the same guard. A dead or hung leader
is replaced within 90 s plus start-up. Tests: 3 simultaneous starts; `SIGSTOP`/`SIGCONT` of the leader across a
takeover; kill of the leader; a hung component with a live process; 100 consecutive hand-overs under load with the
oracle. **[OPEN D14]** the spike must confirm that the platform starts a scheduled run while another run of the same
Flight is active, and what 1,440 short runs a day cost; fallback: a small F4 watchdog Flight performs the same check
every minute and starts the main Flight on demand.

**4.3 Problematic WAL messages (5).** Per R12 the deliverable is `docs/wal_hazards.md`: a researched catalog of every
class of WAL content or stream behaviour that can stall, crash or corrupt the pipeline, each with a reproduction, a
**prevention**, and a test. Minimum classes: oversized transactions; oversized rows and values; unchanged-TOAST
placeholders; schema changes mid-stream, including DDL and DML in one transaction and drop/recreate with a new OID;
values that cannot be decoded or represented; replica-identity problems; truncate variants; logical messages;
origin, two-phase and streamed-transaction messages; partition attach/detach; generated and identity columns,
publication column lists and row filters; keepalive and sender timeouts during long applies; redelivery after
restart and empty-transaction floods; rename versus drop+add ambiguity; timeline changes after failover. Classes
specific to decoding on a replica (R17): slot creation that waits on an idle primary; slot invalidation by recovery
conflict or by the WAL cap; replay lag and replay pauses; promotion and timeline switch; the slot's catalog horizon
holding back vacuum on the primary through standby feedback. Fuzzing and soak follow the robustness bar in §4 (R22). As the last line of defence, a message that still fails repeatedly at the same
position is skipped, its table is backfilled automatically, and 4.0 applies if that fails too. The pipeline never
hangs and never needs a restart to recover.

**4.4 Idle-slot heartbeat (5).** A periodic lightweight write on the primary (default every 10 s, on its own
connection) keeps the slot advancing when replicated tables are quiet and unreplicated tables are busy; in replica
mode it reaches the slot through physical replication. Test: retained WAL on the server holding the slot stays bounded
for 30 minutes under exactly that workload, in both modes.

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
correctly and atomically (R6) with bounded memory. Target apply rate 100K rows/s at `lineitem` width on F16 (R5).
**[OPEN D3]** if the measured ceiling of stock Debezium streaming on F16 is below the target, the choice between a
larger Flight (F32) and accepting the measured number returns to the owner with data.

**5.2 Low latency (5) *(Flight)*.** Per R19. Latency = source commit on the primary → visible, so replica replay lag
is included. A canary row is written every 5 s and its latency is logged in MotherDuck. Pass = **p99 ≤ 30 s and max
≤ 60 s over 24 h**, both when otherwise idle and with a 2,000 TPS background load; p50 ≤ 5 s is a goal. Three windows
are excluded from the statistic but logged and reported: run hand-over, failover, and the affected table's own
backfill. After a hand-over or failover, latency must be back under 30 s within 120 s.

**5.3 High TPS (5) *(Flight)*.** ≥ 2,000 small transactions per second sustained for 30 minutes with 5.2 holding and
1.3 verified, on one table and on 8 tables in parallel; the breaking point is reported.

**5.4 Memory (5).** All buffers have configured byte limits with back-pressure to the reader; staging spills to disk
under a quota; any JVM heap is capped. Tests (50M-row transaction; 1 GB of TOAST values) keep peak memory ≤ 70 % of the
instance and never crash on a full spill area.

### §6 Observability and alerting

**6.1 Logs in MotherDuck (5).** The operations schema `_cdc` in the destination database holds structured logs,
per-interval lag metrics (WAL bytes behind, time behind, replica replay lag, slot positions, rows/s, commit latency),
the leader record and its heartbeat, per-table status and state transitions, batches, backfills, schema changes,
quarantine, canary latency and alerts. Log writes are batched and never delay data.

**6.2 Alerts (5).** Built-in rules (lag, slot health, backfill triggered, quarantine, takeover, contending Flight,
replica lost or fallback to primary, new table awaiting approval, schema-change failure, heartbeat loss, memory/disk
pressure, latency breach) write to an alerts table and post to Slack (R13) with
de-duplication, rate limiting and a "resolved" message. Slack being down never blocks replication; undelivered alerts
are retried from the table. A dead leader is reported by the next scheduled run that takes over.

### §7 Postgres features

**7.1 pgoutput (5).** Stock Debezium with `pgoutput`; nothing is installed on the source.

**7.2 Replica (5).** Per R17 the default is replica mode: WAL decoding (the slot), snapshot and backfill reads,
scheduled refresh queries and catalog polling all use a replica. The primary receives only the heartbeat (4.4), the
optional signal table and publication maintenance, each on a separate light connection (R8, R16). Test: under a full
backfill plus streaming, the primary sees no session of the Flight other than those, and their measured cost is
reported. Primary mode is selected by configuration (see 4.1).

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
- *(Flight)* items are graded only on real Flight runs against PlanetScale, on F16.

### Robustness bar (R22)

The build is not finished until all of the following hold. The quantities are proposed minimums (**[OPEN D16]**).

- **Workload fuzzer**: randomized DDL, DML and transaction shapes (sizes, interleavings, savepoints and rollbacks,
  multi-table transactions) checked against the oracle: ≥ 1,000 seeds and ≥ 100 cumulative hours, on local Postgres
  and on PlanetScale, in replica mode and in primary mode.
- **Fault fuzzer**: process kills at crash points, network faults, source and destination errors, slot loss, replica
  restart, replica loss and promotion, and a full spill area, injected at random times under the workload fuzzer:
  ≥ 500 seeds, ≥ 100 cumulative hours, ≥ 10,000 injected faults, zero divergences.
- **Value fuzzer**: property-based round trip of generated values for every supported type family (boundaries, NaN and
  infinity, NULLs inside nested types, values the native destination type cannot hold, unusual encodings):
  ≥ 1,000,000 values per family; every value is delivered (R16) and equal under the documented mapping.
- **Stream fuzzer**: disconnects, stalls and duplicate delivery at random offsets between the Debezium process and
  the applier; truncated or corrupted input is detected and contained, never applied.
- **Leadership fuzzer**: random starts, pauses, kills and hand-overs of ≥ 3 competing runs: never two writers, never a
  lost or duplicated change; ≥ 1,000 rounds.
- **Soak**: 72 hours on a real Flight against PlanetScale with scheduled hand-overs, ≥ 2 forced failovers, one
  lag-triggered backfill, continuous oracle checks and the 5.2 canary.
- Every failure found becomes a seeded regression test. After a fix to correctness code, the counters of the affected
  fuzzer restart from zero.
- Fresh-context graders must attack with cases that are not in the hazard catalog; any finding reopens the item.

## 5. Decisions

### Still open

| # | Decision | Proposed default |
|---|---|---|
| D2 | Meaning of "100K rows" | rows per second, for backfill and for CDC apply |
| D3 | Stock Debezium streaming measured below 100K rows/s on F16 | decide with spike data: F32, or accept the measured number |
| D14 | Platform behaviour and cost of a run scheduled every minute | use it if the spike confirms it; otherwise a small F4 watchdog Flight does the check |
| D15 | All replicas unavailable | retry and alert for 15 min, then fall back to primary mode until a replica returns |
| D16 | Quantities of the robustness bar (§4) | as listed |
| D17 | Provider does not permit logical decoding on a replica | primary mode + failover slot, snapshot reads on the replica |

### Resolved by the owner (2026-11-09)

| # | Decision | Resolution |
|---|---|---|
| D1 | Flight size for grading | F16 (R5) |
| D4 | "Consistently under 30 seconds" | p99 ≤ 30 s, max ≤ 60 s over 24 h; hand-over, failover and own backfill excluded but reported (R19) |
| D5 | "Short interval" for discovery | 30 s poll, table present ≤ 60 s, automatic by pattern, with an off switch (R20) |
| D6 | "Incremental refresh" | cursor column + lookback + optional reconcile (3.5) |
| D7 | Lag trigger | 2 h overall lag time or 50 % of the WAL cap, not shrinking for 10 min; whole stream; no shedding (R21) |
| D8 | Concurrent Flights | heartbeat guard: healthy leader ⇒ newcomer exits; otherwise newcomer kills it and leads; fencing epoch (R18) |
| D9 | Default topology | replica first; primary mode with a failover slot is the non-default option (R17) |
| D10 | Unconstrained `numeric` | `DECIMAL(38, s)` while it fits, else `UNION(DECIMAL, VARCHAR)` (2.4) |
| D11 | Destructive DDL | mirrored by default; per-table `on_source_drop: retain` (1.5) |
| D12 | Operations schema | `_cdc` in the destination database (6.1) |
| D13 | Run-time limit | every-minute schedule; the leader releases before `run.max_runtime`; the next run takes over through the 4.2 guard (R18) |
