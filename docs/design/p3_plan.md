# P3 Core — work plan

Status: written by the orchestrator at the P2 gate (2026-11-10). It splits `docs/design/core.md` into packages that
can be built in parallel, and fixes the interfaces between them. Gate of the phase (PLAN §1): the tests of SPEC
1.1–1.9 and 4.2 are green on T1.

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
| O | Oracle and T1 harness | SPEC §4 evidence, 1.6, 1.7 | — | running |
| H | Crash-point and fault harness | SPEC 1.7 | — | running |
| X | Cut: where a snapshot lies in the stream | 7.2, open points 1, 12 | — | running (Fable) |
| E | Engine: the JVM program, its frames, the Python side that starts it and turns frames into Arrow | 1, 3 steps 7–8, 6.1 | H (crash points in Java) | Opus |
| D | Destination basics: connections, lanes, fence, clock, deadlines; `_cdc` schema; transition function; alert records | 3 (fence, unknown outcome), 4.1–4.2, 9 | H | Fable |
| A | Apply: the statements of 6.2, 6.3 and 6.5 over a change set | 6.1–6.3, 6.5, open points 2, 3, 9 | O (canonical form) | Opus |
| P | Source: connections with deadlines, catalog, publication, slot on a standby and on a primary, slot facts, timeline history, primary heartbeat | 8 (facts only), SPEC 4.4 | — | Opus |
| L | Leadership: guard, take-over, heartbeat, planned hand-over, release; bootstrap; platform interface and local simulator; the leader machine | 4, 5 | D, H | Fable |
| Y | Transform: destination type per source type; conversion of the engine's transport form and of the backfill's text form into destination-typed Arrow, inside the process; baseline type families | 6.1, SPEC 2.4 (baseline), 8.3 (place only) | E, P | Opus |
| S | Stream: transaction assembly, groups, the commit and acknowledge protocol with its crash points, unknown commit outcome, the oversized transaction, supervisor, pipeline process, configuration file | 2, 3, 6.4 | E, D, A; then Y, P, L | Fable |
| V | Slot continuity: the decision at every slot acquisition and periodically; alert and automatic backfill request | 8, SPEC 1.8, 4.1 | P, D | Fable |
| F | Fuzzers and the campaign ledger: workload, fault, stream, leadership (value fuzzer in the P4 type wave) | SPEC §4, PLAN §6 | O, H; attacks S and L as they appear | Opus |
| B | Backfill and hand-off: chunk planning and readers, pending log, catch-up with repair reads, swap; table and backfill machines | 7, 9 | X, S, A, P, Y | Fable |
| T | Truncate, drop, drop-and-recreate end to end; remaining machines and the enumeration tests of 1.9 | 6.5, 9, SPEC 1.5 | S, B | Opus |

Order: E, D, A and P start as writer slots become free (at most four writers at once). Then L (after D), Y (after
E and P), S (after E, D and A), V (after P and D), F (after O and H), B (after X and S), T last. The gate tests of
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
| `HELLO` | protocol version, Debezium version, slot, the position the engine starts from |
| `SCHEMA` | table, schema version (counter per table), per column: position, source name, Debezium type and semantic name, source type name with length and scale, nullable, part of the key, whether its values can be sent as "unchanged" |
| `ROWS` | table, schema version, row count, columns as buffers (below) |
| `BEGIN` / `END` | transaction id, commit time, all position fields of the record's offset and its `ack_position` (below); `END` also the event count in total and per table |
| `TRUNCATE` | one table and its `seq`; a truncate of several tables arrives as consecutive frames |
| `MESSAGE` | a logical decoding message: prefix, transactional or not, content bytes, `seq`, position fields (SPEC 7.4 consumes it in P4; the frame exists from the start) |
| `HEARTBEAT` | position fields of a heartbeat record and its `ack_position` (the position moved without data) |
| `BATCH_END` | end of one engine batch |
| `FATAL` | the engine stops: class, message and cause chain of the error |

Every record gets a sequence number `seq` in arrival order: commit order, then order inside the transaction. Rows of
several tables travel in separate `ROWS` frames; `seq` restores their order.

`ack_position` of an `END` or `HEARTBEAT` frame is exactly the position that the source will be told is confirmed
if that record is the newest one acknowledged (`docs/research/debezium.md` 3.1: the commit position of the offset
if it has one, else the processed position). For an `END` it is the transaction's commit end position, which is
the applied position of the core design, section 2. Over the life of one engine it never decreases; the engine
checks that and stops with `FATAL` if it would.

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

Commands to the engine, one per line on standard input:

- `CONFIG <json>` — the first line, once: the Debezium properties, with the source password. Secrets reach the JVM
  only this way: never in arguments, in the environment, in a file or in a log line.
- `DURABLE <position>` — the destination commit at this position is durable. The engine marks as processed the
  newest `END` or `HEARTBEAT` record whose `ack_position` is at or below it, and then finishes the batch
  bookkeeping. It acknowledges nothing else, ever.
- `STOP` — stop reading the slot, close the engine, exit 0.
- End of input means the parent is gone: the engine halts at once without acknowledging anything.

Back-pressure: the pipeline stops reading file descriptor 3; the engine blocks in its write; Debezium's queue fills;
the slot read stops. Nothing between the slot and the applier is unbounded.

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
plan = transform.plan(source_table, schema_frame=None) -> TablePlan     # per column position: destination name and type
transform.stream(plan, rows: pyarrow.RecordBatch) -> pyarrow.RecordBatch      # engine transport form → destination types
transform.backfill(plan, rows: pyarrow.RecordBatch) -> pyarrow.RecordBatch    # text as Postgres prints it → destination types
```

The transform is row-local and keeps the column names (`a<i>`, `b<i>`, `k<i>`; `seq`, `op` and `u<i>` pass through).
It runs in a local DuckDB connection that is not attached to MotherDuck, and it runs on every batch as it arrives,
before the batch is buffered, spilled to disk or sent anywhere. So nothing downstream ever holds an unconverted
value, and the PII rules of SPEC 8.3 get one place (P4). Both paths must give the same destination value for the
same source value; a test compares them through a local Postgres for every baseline type family. A value that
cannot be converted raises `ConversionError` with table, column position and `seq`, never the value; it is never
turned into NULL silently.

The backfill's text form is what Postgres prints under the session settings that package P fixes for every read
connection (C7).

### C3a Change set (transform → stream → apply; pending log → apply)

Per destination table and segment:

- `rows`: an Arrow table with the columns of C2 `ROWS` in destination types (`seq`, `op`, `a<i>`, `b<i>`, `u<i>`,
  `k<i>`) plus `txn` (index into `transactions`). It contains only whole source transactions, in `seq` order, and
  it is **not reduced**: every change is there.
- `transactions`: per `txn`: transaction id, commit position, commit time.
- `meta`: `TableMeta` — per column position the destination name and type, the key flag and the "unchanged
  possible" flag; whether the table has a key.

The names of source columns never appear as Arrow or SQL identifiers on the way, so a source column name can never
collide with a name of ours.

```python
apply_group(tx, target: str, change_set) -> ApplyCounts
truncate(tx, target: str)
```

`tx` is an open lane transaction (C4). `change_set.rows` is local Arrow data, or the name of a relation in
MotherDuck with the same columns (6.4, pending log). `apply_group` executes the statements of 6.2 or 6.3; it never
commits. Applying the same change set twice to a table with a key gives the same table. It raises
`ApplyInvariantError` when a delete or update on a table without a key finds fewer rows than it must remove.

### C4 Destination lanes (package D)

```python
conn = md.connect(role)                       # session time zone UTC, marker, client connection id recorded
lane = md.Lane(conn, name, epoch)             # name: 'stream' | 'backfill' | 'control'
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

### C7 Source (package P)

```python
pg.connect(target, deadline)                       # target: 'primary' | 'replica' | a configured name; TLS verify-full;
                                                   # fixed session settings (time zone, date, interval, float, bytea output)
catalog.tables(conn) -> [SourceTable]              # columns, types, key, replica identity, publication membership
publication.ensure(primary, tables) ; slot.create(node, name, deadline) ; slot.drop(...)
slot.facts(node, name) -> SlotFacts                # everything section 8 compares, read in one statement
slot.timeline_history(node) -> [...]               # over a replication connection
slot.terminate_holder(node, name)                  # our slot name only
heartbeat.beat(primary)                            # the source-side heartbeat write of SPEC 4.4
```

Every call has a deadline. A slot on a standby is created as `docs/localpg.md` describes (standby snapshot record
requested on the primary).

### C8 Oracle and T1 harness (package O), C9 Cut (package X)

As delivered by those packages; their reports define the interfaces, and this section is completed when they merge.

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

TCP fault proxies listen on 55440–55459 (package H); a worker that needs proxies of its own takes ports from the
upper half of its own range.

## 5. Reviews

Every package is reviewed before it is merged: by the P3 coordinator (Opus, fresh context, did not write the code)
against SPEC, PLAN, the core design and this plan; the packages D, L, S, V and B additionally by a second reviewer
who attacks the protocol with cases of its own.
