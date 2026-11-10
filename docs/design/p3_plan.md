# P3 Core — work plan

Status: written by the orchestrator at the P2 gate (2026-11-10). It splits `docs/design/core.md` into packages that
can be built in parallel, and fixes the interfaces between them. Gate of the phase (PLAN §1): the tests of SPEC
1.1–1.9 and 4.2 are green on T1. Revised after the coordinator's review of this plan (section 6).

Parts of those items that need a feature of a P4 wave are not in the P3 gate; they are carried to the wave named
here and listed in STATUS.md at the gate:

| Part | Needs | Carried to |
|---|---|---|
| 1.4 "with history mode on" | history tables (SPEC 8.1) | P4, CDC features |
| 1.7 crash points at every step of a schema change | schema changes (SPEC 2.x) | P4, schema and types |
| 1.9 schema-change machine; the full quarantine machine (stored rows, slow retries; P3 has the exit to `quarantined` only) | SPEC 2.x, 4.0 | P4, schema and types; failure handling |
| 1.7 soak of at least 1 h with at least 200 faults | all fuzzers and the whole pipeline | P3 runs a short soak at the gate; the full one is the P6 campaign |

An interface below is a **contract**. A worker who needs a contract changed says so in its report or asks the
orchestrator; it does not change it alone, because another worker is building against it.

## 1. Packages

The data path, and which package owns each stage:

```
slot → engine (E) → frames → transform (Y) → transactions, groups (S) → apply (A) → MotherDuck, through a lane (D)
table → chunk readers (B) → transform (Y) → load, pending log, swap (B, A)
```

| # | Package | Design sections | Depends on | Worker |
|---|---|---|---|---|
| O | Oracle and T1 harness | SPEC §4 evidence, 1.6, 1.7 | — | Opus |
| H | Crash-point and fault harness | SPEC 1.7 | — | merged |
| X | Cut: where a snapshot lies in the stream | 7.2, open points 1, 12 | — | Fable |
| E | Engine: the JVM program, its frames, the Python side that starts it and turns frames into Arrow | 1, 3 steps 7–8, 6.1 | H (crash points in Java) | Opus |
| D | Destination basics: connections, lanes, fence, clock, deadlines; `_cdc` schema; transition function; alert records | 3 (fence, unknown outcome), 4.1–4.2, 9 | H | Fable |
| A | Apply: the statements of 6.2, 6.3 and 6.5 over a change set | 6.1–6.3, 6.5, open points 2, 3, 9 | O (canonical form) | Opus |
| P | Source: connections with deadlines, catalog and catalog observation (H-008), publication, slot on a standby and on a primary, slot facts, timeline history, primary heartbeat | 8 (facts only), SPEC 4.4 | — | Opus |
| L | Leadership: guard, take-over, heartbeat, planned hand-over, release; bootstrap; platform interface and local simulator; the leader machine | 4, 5 | D, H | Fable |
| Y | Transform: destination type per source type; conversion of the engine's transport form and of the backfill's text form into destination-typed Arrow, inside the process; baseline type families | 6.1, SPEC 2.4 (baseline), 8.3 (place only) | E, P | Opus |
| S | Stream: transaction assembly, groups, the commit and acknowledge protocol with its crash points, unknown commit outcome, the oversized transaction, supervisor, pipeline process, configuration file | 2, 3, 6.4 | E, D, A, K; then Y, P, L | Fable |
| V | Slot continuity: the attach step and its decision, the periodic check, the finding enacted (alert, position, backfill requests) | 8, SPEC 1.8, 4.1 | P, D, E; completes after B | Fable |
| F | Fuzzers and the campaign ledger: workload, fault, stream, leadership (value fuzzer in the P4 type wave); the consumer loop of SPEC 1.3 and 3.2; hand-overs under load and the hung component of SPEC 4.2 | SPEC §4, PLAN §6 | O, H; attacks S and L as they appear | Opus |
| B | Backfill and hand-off: chunk planning and readers, pending log, catch-up with repair reads, swap; table and backfill machines | 7, 9 | X, S, A, P, Y | Fable |
| T | Truncate, drop, drop-and-recreate end to end; remaining machines and the enumeration tests of 1.9 | 6.5, 9, SPEC 1.5 | S, B | Opus |
| K | Key rule: the catalog reads deferrability, the engine is told which tables are without a key, what Debezium calls a key is measured (added 2026-11-10 after the second review of A) | 6 (rule K), H-015 | E, P | Opus |

Order: E, D, A and P start as writer slots become free (at most four writers at once). Then L (after D), Y (after
E and P), S (after E, D, A and K), V (after P and D), F (after O and H), B (after X and S), T last. K (after E and P)
is small and runs when a writer slot is free. The gate tests of
1.1–1.9 and 4.2 are written by the package that owns the behaviour and run by the T1 harness of package O.

Not in P3, but the contracts leave room for it: history tables and soft deletes (SPEC 8.1, 8.2: P4 builds them
from the same change set, which is why a change set is never reduced before it reaches apply and carries the
transaction of every row); PII rules (SPEC 8.3: P4 adds them to the transform, which is why the transform runs
inside the process before a value is buffered on disk or sent anywhere); the long tail of types (P4 type wave).

## 2. Contracts

### C0 Common

- A **position** is a WAL location (LSN) as a Python `int`. Text form `X/Y` only at the edges, through
  `cdc_v2.lsn.parse` and `cdc_v2.lsn.format`. In MotherDuck a position is a `UBIGINT` column.
- Every package takes its settings as a small frozen dataclass of its own, with the defaults of PLAN §7 and of the
  core design. Package S builds the loader of the one configuration file that fills them.
- Time: durations are measured on the monotonic clock of the process; every comparison of times across processes
  uses MotherDuck's clock (design 4.1). No code compares a local wall clock with a stored time.
- Every blocking call takes a deadline (SPEC 4.5): a `cdc_v2.deadline.Deadline` (monotonic; `Deadline.after`,
  `remaining`, `check`, `within`, `sleep`). A package offers no call that can wait for ever. A deadline that passes
  raises `cdc_v2.deadline.DeadlineExceeded`, the one class for this in every package.
- Evidence files are written through `cdc_v2.testing.harness_evidence.write_evidence` only (it scrubs paths and
  the user name, then refuses what is left).
- Logs go through `logging` with structured fields; no value of a source row is ever logged (SPEC 8.3).
- Every persisted change row is ordered by (commit position, `seq`); `seq` alone orders rows only inside one
  transaction (it starts at 0 again with every engine instance).
- **Destination names.** The source table `s.t` is the table `s.t` of the destination database: the same schema
  name and the same table name, always as quoted identifiers (Postgres `public` is the destination schema
  `public`; nothing is put into `main`). Everything of ours lives in schemas whose names start with `_cdc`.
  DuckDB compares identifiers without regard to case: a source schema whose name starts with `_cdc` in any
  case, and schemas or tables whose names differ only by case, are not replicated in P3 and are reported by the
  code that meets them; the discovery wave of P4 decides their rule.

### C1 Crash points and fault sites (package H)

Delivered; the contract is `docs/testing/harness.md`.

- `cdc_v2.crashpoints`: `declare(name, description, once=False)` at module level, `crash_point(name)` at the place.
  The Java side implements the same environment contract (`CDC_V2_CRASH_*`); the pipeline passes those variables to
  the JVM unchanged.
- Names are `<area>.<step>`. The names of section 3 of the core design are used as written there. Every package
  puts a point after every step that changes durable state (destination, source slot, local spill that a restart
  reads) and immediately before every blocking commit. The second kind is run with the action `kill_later`, which
  is the only way to produce "commit in flight when the process died".
- A point that is declared and never reached by the crash runner is a failure of the suite, not a pass.
- `cdc_v2.faultsites`: `declare_site(name, description)`, `fault_site(name)` immediately before a call to an
  outside service that the TCP proxy cannot reach (MotherDuck). Package D owns the `md.*` sites.
- Process control, the crash runner, the TCP fault proxy and the byte-stream relay are in `cdc_v2.testing`; a
  package's crash and fault tests use them and do not build their own.

### C2 Engine frames (package E)

The engine writes frames to file descriptor 3 and reads commands on standard input. Standard output and standard
error are logs only.

Every frame: magic, frame type, sequence number (starts at 0, no gaps), payload length, payload, CRC-32 (the one of
`zlib` and `java.util.zip.CRC32`, so that neither side needs a library) of everything before it. The reader rejects a frame with a wrong magic, a gap, a bad checksum or a length above the
configured maximum, and treats the stream as broken: nothing from a broken stream is applied (SPEC §4, stream
fuzzer).

| Frame | Content |
|---|---|
| `HELLO` | protocol version, Debezium version, slot, the position the engine starts from, and what identifies this engine's replication session among the server's backends (attach rule below) |
| `SCHEMA` | table, schema version (counter per table), per column: position, source name, Debezium type and semantic name, source type name with length and scale, nullable, part of the key, whether its values can be sent as "unchanged" |
| `ROWS` | table, schema version, row count, columns as buffers (below) |
| `BEGIN` / `END` | transaction id, commit time, all position fields of the record's offset and its `ack_position` (below); `END` also the event count in total and per table |
| `TRUNCATE` | one table and its `seq`; a truncate of several tables arrives as consecutive frames |
| `MESSAGE` | a logical decoding message: prefix, transactional or not, content bytes, `seq`, position fields (SPEC 7.4 consumes it in P4; the frame exists from the start) |
| `HEARTBEAT` | position fields of a heartbeat record, for information only: the frame shows that the engine is alive; it has no `ack_position` and is never acknowledged |
| `BATCH_END` | end of one engine batch |
| `FATAL` | the engine stops: class, message and cause chain of the error (filtered: never a source value), the position fields of the last record handed over, plus `failing_position` (the `lsn` and `txId` of the event Debezium names when a record fails to decode; absent otherwise); schema and table only when the failure is in our handler |

Every record gets a sequence number `seq` in arrival order: commit order, then order inside the transaction. Rows of
several tables travel in separate `ROWS` frames; `seq` restores their order.

`ack_position` of an `END` frame is exactly the position that the source will be told is confirmed if that record is
the newest one acknowledged: the transaction's commit end position, which is the applied position of the core
design, section 2. Over the life of one engine it never decreases; the engine checks that and stops with `FATAL` if
it would.

**Only `END` records are acknowledged.** A heartbeat record is not: Debezium dispatches a heartbeat that already
carries a transaction's commit position before it creates that transaction's END record, and one that carries the
position of a non-transactional message before that message. A pipeline that adopted such a position lost
transactions (second review of package E: 80 of 20,000 in one run). The engine therefore never holds or marks a
heartbeat record, a `HEARTBEAT` frame may arrive anywhere (also between `BEGIN` and `END`) and never moves the
applied position, and the pipeline sends `DURABLE` only for the commit end of an `END` it has applied. On an idle
source the position moves with the transaction of the source-side heartbeat write (package P, every 10 s, in a
published table). A non-transactional `MESSAGE` is not acknowledged either; SPEC 7.4 (P4) decides its rule: the
server sends such a message again after every restart, from the last confirmed `END` on, so P4 needs a position of
its own (the position of the last applied message) to discard repeats. An `END` whose offset has no `lsn_commit`
stops the engine with `FATAL`; the engine never falls back to another field for an `END`.

The engine holds a bounded number of unacknowledged `END` records. When it must drop some, a `DURABLE` confirms a
position at or below the durable one, never above it, and the distance stays within a bound that
`docs/design/engine.md` states and a test shows (the first version could stop following altogether while the
pipeline was more than about 24,000 transactions behind).

`ROWS` columns are column-oriented buffers in little-endian Arrow layout, written by our own Java code (no Arrow
library in the JVM), so that Python wraps them with `pyarrow.Array.from_buffers` without touching single values:
fixed-width values as one buffer; text and bytes as 32-bit offsets plus data; a validity bitmap where a column has
NULLs. Physical types: boolean, 16/32/64-bit integer, 32/64-bit float, text, bytes. Every other Debezium value
travels as text or bytes in the form Debezium produces with the settings of `docs/research/debezium.md` §7; the
`SCHEMA` frame says which form. Conversion to the destination type happens in SQL in the pipeline, in one place for
the stream and the backfill.

Column sets of a `ROWS` frame, each column named by its position `i` in the schema version:

| Set | Present | Meaning |
|---|---|---|
| `seq`, `op` | always | `op` is `c`, `u` or `d` |
| `a<i>` | always | new image; for a delete of a table with a key: the key columns |
| `b<i>` | tables without a key (`REPLICA IDENTITY FULL`) | old image |
| `u<i>` | for every column whose values can be sent as "unchanged" | true where the value in `a<i>` is absent because it did not change |
| `k<i>` | key columns, tables with a key | old key of the row, set on the create record that follows a key change |

"Unchanged" must never be confused with a real value. The rule is the prevention of hazard H-003 in
`docs/wal_hazards.md` (a placeholder that contains a zero byte, which no Postgres text value can contain; for
`bytea` and arrays a mark that does not travel inside the value). The engine turns it into `u<i>`; after the
engine, no code compares a value with a placeholder. The H-003 tests are part of package E.

**Attach rule.** The pipeline decides slot continuity only while its own engine holds the slot (design section 8),
so it must know when that is the case and which backend it is. Stock Debezium names every replication connection
`Debezium Streaming`, so the engine cannot carry a unique application name (found by package E). The engine sends
`HELLO` at its first `handleBatch` call — records exist only after `START_REPLICATION` has succeeded, and on an idle
source Debezium's heartbeat record makes that call within `heartbeat.interval.ms` — and `HELLO` names the slot's
holder as one SQL query reads it at that moment: `active_pid` and that backend's `backend_start` (µs, UTC). A holder
is identified by this pair, never by the process id alone. If the probe fails or the slot has no holder, the engine
sends `FATAL`. The engine never attaches a second time by itself: `errors.max.retries=0`, `slot.max.retries=0`, and
any loss of the replication connection ends in `FATAL`.

The engine checks its effective configuration at start and refuses (`FATAL`) a value that would weaken a guarantee:
among others `skipped.operations`, the offset storage and flush interval, `lsn.flush.mode`, the retry settings, the
transaction metadata, the snapshot mode, the publication mode, the converter, tombstones and a heartbeat interval
outside 0 < interval ≤ 10 s. Nothing that reaches the engine as "extra" properties can override them.

`END` carries the exact position the engine flushes when that record is marked processed. The
package document states which records Debezium's event counts include, proven by test for a key change, a
TRUNCATE and a transactional message.

Commands to the engine, one per line on standard input:

- `CONFIG <json>` — the first line, once: the Debezium properties, with the source password. Secrets reach the JVM
  only this way: never in arguments, in the environment, in a file or in a log line.
- `DURABLE <position>` — the destination commit at this position is durable. The engine marks as processed the
  newest `END` record that it holds whose `ack_position` is at or below it, and then finishes the batch
  bookkeeping. It acknowledges nothing else, ever. The pipeline sends `DURABLE p` only for p ≤ an applied
  position it has seen committed, none before the attach decision (design 8), and none below the applied position
  of that decision. The engine sends the server no flush position before its first `DURABLE`, so it never moves
  the slot backwards, also on a server that would allow it (design 8, open point 14).
- `STOP` — stop reading the slot, close the engine, exit 0.
- End of input means the parent is gone: the engine halts at once without acknowledging anything.

Back-pressure: the pipeline stops reading file descriptor 3; the engine blocks in its write; Debezium's queue fills;
the slot read stops. Nothing between the slot and the applier is unbounded. The two pipes must not be able to
block each other: the engine reads standard input on a dedicated thread at all times (also while a write to file
descriptor 3 is blocked), and `Engine.durable()` never blocks its caller.

Python side (`cdc_v2.stream.engine`):

```python
engine = Engine(config, start_position, artifacts)      # starts the JVM as a child, in the pipeline's process group
for frame in engine.frames(deadline=...):               # Schema | Rows(table, version, RecordBatch) | Begin | End |
    ...                                                 # Truncate | Heartbeat | BatchEnd; raises EngineBroken, EngineFatal
engine.durable(position)
engine.stop(deadline)        # STOP, wait, then SIGKILL of the JVM; engine.kill() at any time
```

The JVM stays in the process group of the pipeline, so that the bootstrap's kill of that group (design section 5)
and a freeze of the whole tree reach it. The pipeline kills the JVM by its process id; the JVM starts no lasting
child of its own.

### C3 Transform (package Y)

```python
plan = transform.plan(source_table, schema_frame=None, *, server_version_num) -> TablePlan
                                        # per column position: destination name and type; `plan.ambiguous` (below)
transform.stream(plan, rows: pyarrow.RecordBatch) -> pyarrow.RecordBatch      # engine transport form → destination types
transform.backfill(plan, rows: pyarrow.RecordBatch) -> pyarrow.RecordBatch    # text as Postgres prints it → destination types
transform.ambiguous(plan, rows: pyarrow.RecordBatch) -> mask or None          # rows whose zero interval is ambiguous
```

The transform is row-local and keeps the column names (`a<i>`, `b<i>`, `k<i>`; `seq`, `op` and `u<i>` pass through).
It runs in a local DuckDB connection that is not attached to MotherDuck, and it runs on every batch as it arrives,
before the batch is buffered, spilled to disk or sent anywhere. So nothing downstream ever holds an unconverted
value, and the PII rules of SPEC 8.3 get one place (P4). Both paths must give the same destination value for the
same source value; a test compares them through a local Postgres for every baseline type family. A value that
cannot be converted raises `ConversionError` with table, column position and `seq`, never the value; it is never
turned into NULL silently.

The backfill's text form is what Postgres prints under the session settings that package P fixes for every read
connection (C7; `docs/design/source.md` section 1). The stream's form of every type is the table in section 3 of
`docs/design/engine.md`; the engine's walsender session has settings of its own (`IntervalStyle=postgres`,
`DateStyle=ISO`). These are two transport forms on purpose, and the transform is the only code that knows either.

**Ambiguous zero interval.** On a source of Postgres 17 or later, `interval 'infinity'` and `'-infinity'` in a
scalar `interval` column reach the engine as a zero interval: the Postgres driver parses them into nothing, before
any converter of ours runs (`docs/design/engine.md` section 10; no configuration of stock Debezium changes it). An
interval array keeps them, and the backfill reads the true value. So on such a source a zero in a scalar interval
column of the stream is *ambiguous*: it is a real zero, `infinity` or `-infinity`.

- `TablePlan.ambiguous` lists those column positions (empty on a source below 17), and
  `transform.ambiguous(plan, rows) -> mask` marks the rows of a stream batch whose new image holds one. The change
  set (C3a) is unchanged.
- The rule (built in the type wave of P4, on the seam that package S leaves): before a group with marked rows is
  applied, the stream closes it on a cut (C9) — it takes a cut at or after the group's end and reads on until the
  stream has passed it, so that the group holds exactly the transactions that the cut's snapshot contains. For
  each key whose last change in the group is marked it reads the row in that snapshot (one statement per table:
  key and ambiguous columns, `WHERE key = ANY(…)`) and takes the value found. The group is then applied in one
  destination transaction as always. The value is the source's value at the group's end, so the destination never
  shows a row that the source did not have (SPEC 1.3), and no value is refused (SPEC 2.4).
- A shortcut that stays exact: under any snapshot at or after the group's end, a row whose `xmin` equals the
  transaction id of the key's last change in the group is that very version. (A row written in a subtransaction
  has another `xmin` and takes the full way.)
- Not taken: "apply whatever the row holds now, and count it when a later transaction has changed it". It shows,
  for one group, a row that mixes two source transactions.
- A table without a key that has a scalar interval column on such a source is not streamed: mode `full_refresh`
  with an alert (SPEC 3.5), until P4 decides more.
- An infinite value puts the column into its wide form, `UNION(INTERVAL, VARCHAR)` (R9, SPEC 2.4), on both paths.
- P3: the transform gives the list and the mask; the stream counts marked rows per table (`interval_ambiguous`),
  alerts once per table, and applies the delivered zero. Until the repair exists, an infinite interval in a
  streamed scalar column of a source ≥ 17 arrives as zero — a known gap with an alert, on the list of parts
  carried to P4. No P3 gate workload holds an interval.

### C3a Change set (transform → stream → apply; pending log → apply)

Per destination table and segment, in one of two forms:

**Local form** (a group in memory):

- `rows`: an Arrow table with the columns of C2 `ROWS` in destination types (`seq`, `op`, `a<i>`, `b<i>`, `u<i>`,
  `k<i>`) plus `txn` (index into `transactions`, in commit order). It contains only whole source transactions,
  its order is (`txn`, `seq`), and it is **not reduced**: every change is there.
- `transactions`: per `txn`: `xid` (the transaction id as an integer), `commit_lsn`, `commit_ts`.

**Relation form** (rows that are already in MotherDuck: the staging of an oversized transaction, a pending log):

- a `SELECT` (text and parameters) over a table with the same row columns, in which `txn` is replaced by the
  columns `commit_lsn UBIGINT`, `xid UBIGINT`, `commit_ts TIMESTAMPTZ`. Its order is (`commit_lsn`, `seq`).
- A pending log may also hold rows with `op = 't'` (a truncate of the table at that place). They cut the log into
  segments; the owner of the log gives apply one segment at a time and calls `truncate` between them. A `'t'` row
  never reaches `apply_group`.

Both forms carry `meta`: `TableMeta` — per column position the destination name and type, the key flag and the
"unchanged possible" flag; whether the table has a key.

The names of source columns never appear as Arrow or SQL identifiers on the way, so a source column name can never
collide with a name of ours.

```python
apply_group(tx, target, change_set, mode="stream") -> ApplyCounts      # mode: "stream" | "catch_up"
truncate(tx, target)
```

`tx` is an open lane transaction (C4). `apply_group` executes the statements of 6.2 or 6.3; it never commits.

Unchanged values (tables with a key). A record that carries "unchanged" for a column needs the value from
somewhere. The order is: (1) the nearest earlier record of the same row in the change set, following key changes;
(2) the destination row that has the key the row had before the change set; (3) in mode `catch_up` only: the
destination row with the row's new key, if no earlier record of the change set addressed that key. If none of
them gives the value, the key is **unresolved**:

- mode `stream`: `ApplyInvariantError` (the destination has diverged from the source); nothing is ignored and no
  NULL is invented. The caller rolls back.
- mode `catch_up` (a log replayed onto a table that was loaded separately, design 7.3): the records of unresolved
  keys are not applied, everything else is, and the keys are returned in `ApplyCounts.unresolved` for the repair
  read.

Applying the same change set twice in mode `catch_up` gives the same table, except for keys returned as
unresolved. Mode `stream` makes no such promise and needs none: a group is applied exactly once because of the
position check of the run-again rule (C4).

`ApplyInvariantError` is also raised, in both modes, when a delete or update on a table without a key finds fewer
rows than it must remove.

### C4 Destination lanes (package D)

```python
conn = md.connect(role)                       # session time zone UTC, marker, server connection id known
lane = md.Lane(conn, name, epoch)             # name: 'stream' | 'control' | 'backfill.0' … 'backfill.15'
with lane.transaction(deadline) as tx:
    tx.execute(sql, params, deadline=None)
    ...
# leaving the block: fence statement, exactly one row changed, COMMIT
md.clock_sql                                   # the expression for MotherDuck's clock (4.1)
```

Errors (all in `cdc_v2.md.errors`, all subclasses of `DestinationError` except the deadline):

| Error | Meaning | What the caller may do |
|---|---|---|
| `FenceLost` | another run leads; the transaction was rolled back | stop every lane; the pipeline exits |
| `CommitUnknown` | the connection was lost, or the deadline passed, after `COMMIT` was sent | read the state in a new transaction and continue from what it finds; never assume either outcome |
| `Conflict` | the transaction lost a write-write conflict and was rolled back; nothing was applied | run it again from the start |
| `ConnectionLost` | the connection was lost before `COMMIT` was sent; nothing was applied | run it again from the start; the lane has replaced the connection |
| `cdc_v2.deadline.DeadlineExceeded` | a statement before `COMMIT` was interrupted; rolled back; the connection is closed and replaced | as the caller's own deadline rule says |

Any other error of the destination (a statement that is wrong, a value that does not fit) is passed on unchanged
after the roll-back; it is not a transport error and is never retried blindly.

**Run-again rule.** A sent `COMMIT` can become durable up to about 2 s after its client is gone, and a dead
client's open transaction can hold its rows for about 30 s (`docs/spike/md_semantics.md`). So every transaction
that a caller may run again (after `CommitUnknown`, `Conflict`, `ConnectionLost` or a restart) must (1) read the
state it starts from inside the new transaction, never reuse an earlier read, and (2) change that state with a
condition on the value it read (the applied position, the chunk's state, the machine's state), checking that
exactly one row changed. Then a late first copy and the second copy can never both commit. The lane offers
`tx.execute_one(sql, params)` for such a statement: it raises `Conflict` when the statement changed no row.

Crash points of a lane, declared by package D for each lane name: `<lane>.fenced` (fence statement executed),
`<lane>.commit_sent` (immediately before `COMMIT`; run with `kill_later`), `<lane>.committed` (`COMMIT` returned).
Fault sites: `md.connect`, `md.execute`, `md.fence`, `md.commit`.

Every call has a deadline; none can wait for ever (SPEC 4.5). No other module issues `BEGIN`, `COMMIT` or the fence
statement.

Lanes and their fence rows are a fixed set, created with the leader tables at initialisation (`stream`, `control`,
`backfill.0` … `backfill.15`); no fence row is ever added later, because a row added during a take-over would
keep the old epoch.

Connections. Every connection of a lane appends (epoch, lane, server connection id) to `_cdc.connections` before
its first transaction, in one autocommit statement that inserts only if `_cdc.leader` still holds that epoch (no
row inserted ⇒ `FenceLost`). A take-over interrupts every listed connection of earlier epochs and gives up after
its deadline (design 4.5). A lane that replaces a connection first interrupts the old server connection by id,
so that an abandoned transaction cannot keep holding a fence row.

Staging appends (design 6.1) are ordinary `stream`-lane transactions that move a staging progress row with
`execute_one`.

The three leader tables (`_cdc.leader`, `_cdc.fence`, `_cdc.heartbeat`) and `_cdc.connections` have a frozen
shape: their statements are constants in `cdc_v2.md.schema`, and the bootstrap carries a copy that a T0 test
compares (design section 4). Every other `_cdc` table is created by migration steps, one ordered list per
package, run only by a leader after its take-over, in fenced `control`-lane transactions.

`_cdc` schema: created and migrated by `cdc_v2.md.schema` only; every package adds its tables there.

Alerts: `alerts.emit(tx, kind, severity, fields)` records an alert in `_cdc.alerts` inside the caller's transaction
(delivery to Slack is built in the operations wave and reads that table). Packages raise alerts only this way.

### C5 Transition function (package D)

```python
state.transition(tx, machine, key, from_state, to_state, cause)   # inside the caller's transaction
state.read(conn_or_tx, machine, key) -> state
```

Each machine is declared once as a state set and a transition table, by the package that owns it, with
`state.declare(machine)`; the function refuses a pair that is not in the table, refuses when the stored state is
not `from_state`, and appends to `_cdc.transitions`. No other code writes a state column. Package D also delivers
the test helper that, for a declared machine, tries every pair of states and asserts that exactly the declared
transitions are accepted (SPEC 1.9); every owning package calls it for its machines.

One exception to "no other code": the leader machine is written by `main.py`, which cannot import the package
before the clone. It carries its own copy of that machine (states `leading`, `released`; why a run ended is the
cause of the transition), and a T0 test compares the copy with the declaration.

### C6 Platform (package L)

What the leader code needs from the Flight platform, behind one interface with two implementations:

```python
platform.identity() -> (flight_id, run_number, run_id)
platform.run_state(flight_id, run_number) -> 'RUNNING' | 'PENDING' | terminal | None
platform.cancel(flight_id, run_number)
```

`MotherDuckPlatform` uses the Flight SQL functions. `LocalPlatform` is the simulator for T1: runs are local process
groups started from `flights/cdc_v2/main.py`; it starts runs on a schedule and on demand, lets starts overlap,
cancels as the real platform does (the record says cancelled first, the processes are killed about 2.5 s later,
without a signal), enforces a maximum run time late, and ends a run when its main process exits. The leadership
fuzzer drives it. Its behaviour follows `docs/spike/run_semantics.md`; each imitated behaviour cites the finding.

- LocalPlatform kills every process a run started, in any session or process group (the real platform destroys
  the container), and a test proves it for a pipeline that started its own session.
- `main.py` accepts a local source path instead of the pinned clone.
- Every leader timing is a setting, with the order heartbeat interval < stale age / 3 and stale age < hard-exit
  age checked when the settings are built.

### C7 Source (package P)

```python
pg.connect(target, deadline, replication=False)    # target: 'primary' | 'replica' | a configured name; TLS verify-full;
                                                   # fixed session settings (time zone, date, interval, float, bytea output)
catalog.tables(conn) -> [SourceTable]              # columns, types, key, replica identity, publication membership
catalog.observe(node) -> CatalogObservation        # the catalog facts of H-008 with the replay bracket (b, a)
catalog.diff(old, new)                             # appeared, dropped, recreated (new OID), renamed, columns, rewritten
publication.ensure(primary, tables)                # only ever adds tables
publication.state(conn)                            # what exists, to compare with the configuration
replica_identity.set_full(primary, table, deadline)
slot.create(node, name, deadline) ; slot.drop(...)
slot.facts(node, name) -> SlotFacts                # everything section 8 compares, read in one statement, with the
                                                   # holder: active_pid, that backend's start time and application
                                                   # name, and pg_is_in_recovery()
slot.timeline_history(node) -> [...]               # over a replication connection
timeline.contains(history, position, timeline)     # is a position on the history of the current timeline
slot.terminate_holder(node, name)                  # our slot name only
heartbeat.beat(primary)                            # the source-side heartbeat write of SPEC 4.4
```

As delivered, `docs/design/source.md` is the full account (signatures, the role split, measurements); where it
differs from the sketch above, the document holds. The differences that other packages build on:
`heartbeat.beat(primary, deadline, slot=…)` returns the position after its own commit, and an `END` with
`ack_position ≥` that position proves that the beat was decoded; `heartbeat.ensure` (setup role) creates the
published table `cdc_v2.heartbeat` and `heartbeat.check` (runtime role) verifies it before streaming;
`slot.terminate_holder(node, name, deadline, expect=…)` takes the holder pair of C2; `SlotFacts.invalid` is true
when `wal_status = 'lost'`, or `invalidation_reason` is set, or `conflicting` is true (Postgres 16 has no
`invalidation_reason` and shows a slot invalidated by the WAL cap as `conflicting`); a standby's timeline comes
from `slot.timeline_history` (`IDENTIFY_SYSTEM`), never from `pg_control_checkpoint()`, which can lag behind a
switch; no fact identifies one incarnation of a slot; `pg.connect` puts the session settings into the startup
packet and reads them back.

Every call has a deadline. A slot on a standby is created as `docs/localpg.md` describes (standby snapshot record
requested on the primary; without that grant the runtime role's own write, and a running-transactions record
within about 25 s). Slot facts are used for a continuity decision only as the attach step of design
section 8 says. Nothing on the source is fenced by the leader epoch, so every source-side write must be harmless
when a deposed run repeats it: `publication.ensure` only adds, the heartbeat write is idempotent, and a slot is
dropped only by an explicit operator action or by the enactment of a slot finding.

### C8 Oracle and T1 harness (package O)

Details in `docs/oracle.md`. What binds every package that writes a gate test:

- Every gate test decides equality only with `cdc_v2.oracle.compare(source, destination, config, position=Q)`.
  `Q` comes from `Workload.quiesce()` or `Workload.pause()`. `compare` refuses a source snapshot or a destination
  state that is not at or past `Q`, and it refuses an empty comparison. A comparison without a position must be
  asked for by name and is never a gate result.
- A gate test passes only if `result.equal` holds and every table's `fallback_columns` is empty or declared by
  the test.
- The compared tables are listed explicitly (`OracleConfig.tables`, `table_map`). Heartbeat, canary and `_cdc*`
  objects are never compared. The oracle does not enforce the list: without one it compares every table of the
  source, the heartbeat table too, which changes every 10 s and gives a false difference.
- A comparison that reads the source from a standby can lose its connection to a recovery conflict. The oracle
  starts again only on SQLSTATE 40001; a connection that the standby ends while it is idle in its transaction
  can surface as a bare connection loss, which is not retried. Such a test retries the comparison itself.
- The applied-position lookup that `wait_for_applied` and `compare` use is provided by package S (by slot name);
  until then the `_cdc.stream_state` reader of the oracle is a stand-in.
- Waiting takes a `cdc_v2.deadline.Deadline`; evidence is written only through
  `cdc_v2.testing.harness_evidence.write_evidence` (C0).
- T1 sources and destinations come only from the fixtures in `tests/conftest.py` (section 4 says which cluster
  and which database prefix they use). Destructive drills use the fixture `private_cluster`.
- Workloads come from `cdc_v2.testing.workload`, with a seed and a replay log. Its transfers observer detects
  splits of transfer transactions only; other invariants across tables are package F's.
- The oracle shares no code with Y or A, and the pipeline never imports `cdc_v2.oracle` or `cdc_v2.testing`.
- Canonical comparison (design open point 3, settled here): floats compare by bits with all NaNs equal, so `-0`
  and `0` differ; intervals compare by months, days and microseconds, so `1 mon` and `30 days` differ; a UNION
  column compares by member and by the rule of the member's source family; a source NULL is a real NULL in the
  destination, never a UNION member that holds NULL.

### C9 Cut (package X)

As delivered by package X; its report defines the interface, and this section is completed when it merges.

### C10 Stream and backfill (packages S, B, V, T)

- S keeps a routing map: table → `streaming` | `backfilling(id)` with the flag "also apply to the visible table"
  (design 7.1, a backfill without a gap) | `skip`. It is stored in `_cdc` and changed only by stream-lane
  transactions.
- Every backfill request (operator, V, discovery, T, a quarantine exit) is a row in a request table, written by
  whoever asks, in any lane. It is enacted inside a stream-lane group; `P0` is that group's end.
- Before the position step, S calls `backfill.on_group(tx, group_end, change_sets) -> routing changes`. It enacts
  requests, appends to pending logs (relation form of C3a), and performs the swap, all in `tx`. S applies to
  visible tables what the routing says, in the same `tx`.
- The backfill lane moves `catching_up → swap_ready` and then stops writing that backfill's row and its new
  table; the stream lane moves `swap_ready → done`, or back to `catching_up` when the rest of the log has an
  unresolved key.
- A slot finding (V) sets the applied position to the slot's `confirmed_flush_lsn` (or the new slot's consistent
  point) and enacts a backfill of every CDC table, in one stream-lane transaction (design section 8). The
  function that does it belongs to S, because S owns `_cdc.stream_state`:
  `stream.reposition(tx, position, tables, cause)` — the position set (in any direction), a backfill request
  enacted for each table with `P0` = that position (after a gap), the alert, all in `tx`. V calls it for a
  finding; S calls it for a message that fails again and again at one position (SPEC 4.3, last line); P4 calls it
  for the lag rule of SPEC 3.6. Every `stream.reposition` is followed by a new engine: an engine keeps the highest
  `DURABLE` of its life, so one that outlived a move backwards would still confirm the old, higher position.
  Restarts that look for the table behind a repeated `FATAL` (engine started with candidate tables left out of
  its include list) apply nothing and send no `DURABLE`.
- The stream component starts its engine only through the attach step (design section 8):
  `attach(ctx, deadline) -> Attached(engine, facts, finding)`. V builds it. Until V is merged, S ships a
  minimal one behind the same signature: fence row read, slot facts read, a holder of our slot terminated,
  engine started at the applied position, and any finding other than "continuous" stops the stream with an alert
  instead of being enacted.
- A table whose `SCHEMA` frame no longer agrees with its destination table (a schema change at the source) is
  sent through a backfill in P3: from the first transaction that carries the new version, its changes are not
  applied to the visible table, and a backfill request (after a gap) is enacted in that group. The schema wave of
  P4 replaces this by a change in place (design 6.5).
- **Catalog gate.** Debezium's records name a table by schema and name only, and a drop and recreate, or a dropped
  and added column of the same name and type, can leave the `SCHEMA` frame unchanged (H-007, H-008). So before a
  group is applied, S calls `tables.confirm(group_end, tables, deadline) -> {table: same | changed}`. Package T
  builds it on `catalog.observe` (C7): an observation whose lower bracket `b` is ≥ `group_end` must show, for each
  table of the group, the identity that the destination table was built for (OID; per column `attnum`, name, type;
  the key columns). A table that is not `same` leaves the visible path in that group: its rows are not applied. What
  follows is enacted by T in `tables.on_group(tx, group_end)`, which S calls in every group and every idle position
  step, immediately before `backfill.on_group`: in the first group after the stream has passed the observation (its
  end is greater than the upper bracket `a`, design 7.2). The cost is one catalog round trip per group. Until T is
  merged, S's stand-ins answer `same` and do nothing.
- **The destination follows the source's names** (P3 rule). For each replicated name: absent at the source → the
  destination table is dropped, or renamed under `on_source_drop: retain`; present with another OID → the old
  destination table is dropped (or renamed) and a backfill of the new table is requested, in the same group; the
  same OID with other columns → the schema rule above. A backfill that follows such a finding is entered only past
  `a`, so that its pending log never holds a row of the earlier table. A rename is therefore a drop of the old name
  and, if the new name is replicated, a fresh table; the rename in place of SPEC 2.2 is P4.
- S runs a group again after `Conflict`, a bounded number of times, building its statements again when the
  conflict came from a DDL (a swap changes the catalog).
- A table that fails to apply `apply.max_table_attempts` times (default 3) leaves the stream: state `quarantined`,
  routing `skip`, alert, in a stream-lane transaction; the group is then applied without it (design section 3).

### C11 Supervisor and heartbeat (packages S, L)

- The heartbeat's only input is `supervisor.progress() -> (ok, phase, progress_at)`.
- A component inside `with supervisor.waiting(component, reason, deadline):` counts as progressing until that
  deadline, and not after it. Slot creation, the attach step after a take-over, a commit in flight and the wait
  for a replica to reach `P0` are declared this way.
- Until package S delivers the supervisor, package L builds against this interface with a stand-in of its own.

### C12 Pipeline process (packages S, L)

- The bootstrap starts the pipeline as `python -m cdc_v2.pipeline <configuration file>` in its own session
  (design section 5). The environment carries `CDC_V2_EPOCH`, `CDC_V2_RUN_ID`, `CDC_V2_FLIGHT_ID`,
  `CDC_V2_RUN_NUMBER`, `CDC_V2_TICK_FD` (an inherited descriptor) and the secrets of the run. No secret is an
  argument.
- Tick: after every successful heartbeat the pipeline writes one byte to the tick descriptor without blocking; a
  full pipe is not an error. The bootstrap needs nothing else from the pipeline while it runs.
- Exit status of the pipeline, passed on by the bootstrap: `0` only after a committed release (planned
  hand-over); `10` when the run was superseded (`FenceLost`, or a heartbeat that changed no row); any other
  value is a failure.
- Inside the pipeline, `cdc_v2.leader` (L) owns the heartbeat thread, the tick, the one reaction to `FenceLost`
  and the hand-over clock; `cdc_v2.supervisor` (S) owns progress and the restart of components; the stream
  component (S) offers `finish_current_group(deadline)` for the hand-over. `cdc_v2.pipeline` (S) is the process
  that wires them, runs the migrations as leader and starts the components.
- Whichever of S and L is merged second joins the two in a short follow-up round, with a test in which the real
  bootstrap starts the real pipeline under `LocalPlatform`.

## 3. Test tiers used in P3

- **T0**: hermetic (`pytest` default): no network, no Postgres, no JVM. Local DuckDB stands in for MotherDuck where
  only SQL semantics matter; the same tests run again on MotherDuck under the marker `md`.
- **T1**: local primary and standby (`tools/localpg`, private home and ports per worker), real MotherDuck
  (`cdc_v2_t_*` databases), local JVM. Markers `localpg`, `md`, `jvm`, `t1`.
- Everything a test creates outside the repository is removed by the test, also when it fails.

## 4. Private resources per package

No two workers share a Postgres cluster, a port or a MotherDuck database. The owner's Postgres on port 5432 is
never touched; every command names its port.

| Package | `tools/localpg` home under `~/.cache/cdc_flight_v2/` and port base | MotherDuck databases |
|---|---|---|
| E | `pg-e`, 55620 | none |
| D | none | `cdc_v2_t_d_*` |
| A | none | `cdc_v2_t_a_*` |
| P | `pg-p`, 55640 (a second home `pg-p2`, 55648) | none |
| L | none | `cdc_v2_t_l_*` |
| Y | `pg-y`, 55656 | none |
| S | `pg-s`, 55664 (`pg-s2`, 55672) | `cdc_v2_t_s_*` |
| V | `pg-v`, 55680 (`pg-v2`, 55688) | `cdc_v2_t_v_*` |
| F | `pg-f<n>`, from 55700 in steps of 8, up to 55795 | `cdc_v2_t_f_*` |
| B | `pg-b`, 55800 (`pg-b2`, 55808) | `cdc_v2_t_b_*` |
| T | `pg-t`, 55816 | `cdc_v2_t_t_*` |
| K | the homes of E (`pg-e`, 55620) and P (`pg-p`, 55640; `pg-p2`, 55648), whose workers have finished | none |

TCP fault proxies listen on 55440–55459 (package H); a worker that needs proxies of its own takes ports from the
upper half of its own range.

**The shared T1 fixtures** (`tests/conftest.py`, package O) follow the same rule through the environment:

| Variable | Meaning | A package worker sets it to |
|---|---|---|
| `CDC_V2_PG_HOME`, `CDC_V2_PG_PORT_BASE` | the cluster on which the fixtures create one private database (`cdc_v2_t_<random>`) per test, for tests that do not disturb the cluster | its own home and port base from the table |
| `CDC_V2_MD_PREFIX` | prefix of the MotherDuck databases the fixtures create (must start with `cdc_v2_t_`) | its own prefix, for example `cdc_v2_t_s_` |
| `CDC_V2_T1_SHARED=1` | allows the default home (ports 55432–55439) | never: the default home is for the orchestrator's gate runs |

Without a named home and without `CDC_V2_T1_SHARED=1` the fixtures fail with a message that says what to set.
Tests that kill, promote, reconfigure or fill a cluster take the fixture `private_cluster`: a primary and a
standby of their own on a block of 8 ports leased under a file lock from 55840–56095 (32 blocks), removed when
the test ends. Every worker may use these leased blocks; nothing else listens there.

Why no cluster is shared between workers even for harmless tests: a replayed `ACCESS EXCLUSIVE` lock stalls the
replay of a standby for every stream that decodes there, one worker's WAL load slows every other slot, and the
limits on slots and senders are per cluster.

Fixed ranges, for reference (a default-run test of package O asserts that the leased range misses all of them):
55432–55439 default home, 55440–55459 TCP proxies, 55460–55467 `localpg` self-tests, 55500–55509 throughput
spike, 55600–55607 cut stress, 55620–55823 the homes of the table above.

## 5. Reviews

Every package is reviewed before it is merged: by the P3 coordinator (Opus, fresh context, did not write the code)
against SPEC, PLAN, the core design and this plan; the packages D, L, S, V and B additionally by a second reviewer
who attacks the protocol with cases of its own, and so is the acknowledgement path of package E. The
orchestrator starts the workers and merges; the coordinator reviews and does not write code.

## 6. The coordinator's review of this plan (2026-11-10)

The coordinator reviewed the design and this plan before most packages started. What it found is folded into
`docs/design/core.md` (sections 2–4, 6–9, open points 14–17) and into the contracts above. This section keeps
what is not a contract yet, for the task files that are still to be written.

### 6.1 Who owns which gate test

| SPEC | Owner | With |
|---|---|---|
| 1.1 exactly-once | S | A, D |
| 1.2 ordering and invariants | S | A (an `ApplyInvariantError` in the stream ends in the quarantine exit of C10) |
| 1.3 one source transaction, one destination transaction | S | F: the consumer loop on cross-table invariants, on O's harness; B: the table-status log |
| 1.4 key changes | A (T0, `md`) | S end to end; the history variant is P4 |
| 1.5 truncate, drop, drop and recreate | T | P (`catalog.observe`) |
| 1.6 snapshot and stream hand-off | B | X (the cut) |
| 1.7 crash and fault suite | F (suite and ledger) | H (runner); every package declares and reaches its own points |
| 1.8 slot continuity | V | B; plus drop-and-recreate between the first read and the attach |
| 1.9 state machines | T (document and enumeration over all machines) | every owner, through D's helper |
| 4.2 one leader | L | F: 100 hand-overs under load and the hung component, after S and L are merged |

### 6.2 Carried into the task files of later packages

**B (backfill).** The repair snapshot needs `R0 ≥ caught_up_lsn`. The last chunk is open-ended. The wait for
`R0 ≥ P0` has a deadline and is a declared wait. `swap_ready` and who writes when (C10). Parallel loaders use
lanes `backfill.<i>`. Open point 5 (appends by the stream lane and reads by a backfill lane on one pending log)
is tested first. The swap group uses mode `catch_up`; an unresolved key sends the backfill back.

**V and S (attach).** The attach step of design section 8, with its loop and a deadline of at least 75 s. The
pipeline reads its fence row before every engine start. A restart of the stream component always starts a new
engine. `slot.terminate_holder` belongs to the attach step, not to `main.py`. Open points 6 and 14. Whether a
slot's identity can be read at all; if not, continuity is: same system identifier, applied position on the
timeline history, a valid slot of our plugin and database, `confirmed_flush_lsn` ≤ applied — and a recreated
slot that is not ahead gets an alert where it can be seen, and no backfill.

**S (stream).** A mismatch of the `END` count at the same position: bounded restarts, then the skip-and-backfill
path of SPEC 4.3. The quarantine exit of C10. The spill area: DuckDB's temporary directory of the local
transform connection under `max_temp_directory_size`, with a fault site for "no space left"; the stream waits
and alerts, it never crashes on it (SPEC 5.4, 1.7). The batch commit/ack machine as one row per slot (design
section 9). Staging is fenced, with a progress row, in one schema, truncated at every start of the stream
component. After `CommitUnknown`: read the position in a new transaction and continue from it. Only an `END`'s
`ack_position` becomes the applied position, in a committed group that contains every transaction framed before
it; a `HEARTBEAT` frame never moves it.

**L (leadership).** Two states. Take-over deadline (default 45 s), then exit non-zero. A take-over whose commit
outcome is unknown: read again after more than 2 s and decide by `run_id`. A planned hand-over abandons an
oversized transaction that is still arriving. Leader tables created by simultaneous first runs (open point 16).
Clean-up of `_cdc.connections`. The measurement point of "a newcomer exits within 10 s" (open point 17).

**T (truncate and drop).** With `catalog.observe` and its bracket (b, a): a drop is enacted in the first group
whose end is ≥ a; a table whose OID changed is a fresh table and goes through B; changes that arrive inside the
bracket for a table whose identity is in doubt are held, never applied to the wrong incarnation (H-007).

**X (cut), as input only.** A proposal for the primary: after `pg_current_snapshot()`, read `pg_xact_status` for
every transaction id in the in-progress list; if one is already `committed`, take a new snapshot. A transaction
is inside iff the snapshot sees it by its 64-bit id. "The stream has passed" = applied position >
`pg_current_wal_insert_lsn()` read after the snapshot. To be proven with a stopped synchronous standby and a
negative control.

**Rule K (core design section 6), for S, Y, B, T and the P4 waves.** S: at every `SCHEMA` frame the key flag and
the key positions must equal the catalog's (Y's `plan` makes the comparison); a difference is a typed error, the
stream reads the catalog again and restarts the engine with a fresh list of tables without a key; it never applies
by a key the catalog does not confirm. S and B: a T1 case for a key that changes only its form (`float8` `-0` to
`0`: the engine sends a key change). T and the schema wave of P4: a change of the key facts of a table (key
columns, deferrability, replica identity) is a schema change; they are part of the table's shape. Type wave and
PII rules of P4: the mapping of a key column is injective up to the source's equality; a rule that masks or
truncates a key column is refused. History of P4: a table with a deferrable primary key gets the plain changelog
of a table without a key.

### 6.3 Simplifications taken

Two-state leader machine; one fenced staging schema; a TRUNCATE or schema change cuts segments and does not close
the group; the batch/ack machine is one row per slot. The slot identity is dropped only if package V finds that
Postgres does not expose one (6.2).
