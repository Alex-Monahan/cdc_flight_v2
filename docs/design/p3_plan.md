# P3 Core — work plan

Status: written by the orchestrator at the P2 gate (2026-11-10). It splits `docs/design/core.md` into packages that
can be built in parallel, and fixes the interfaces between them. Gate of the phase (PLAN §1): the tests of SPEC
1.1–1.9 and 4.2 are green on T1.

An interface below is a **contract**. A worker who needs a contract changed says so in its report or asks the
orchestrator; it does not change it alone, because another worker is building against it.

## 1. Packages

| # | Package | Design sections | Depends on | Worker |
|---|---|---|---|---|
| O | Oracle and T1 harness | SPEC §4 evidence, 1.6, 1.7 | — | running |
| H | Crash-point and fault harness | SPEC 1.7 | — | running |
| X | Cut: where a snapshot lies in the stream | 7.2, open points 1, 12 | — | running (Fable) |
| E | Engine: the JVM program, its frames, the Python side that starts it and turns frames into Arrow | 1, 3 steps 7–8, 6.1 | H (crash points in Java) | Opus |
| M | Destination basics and leadership: connections, lanes, fence, clock, deadlines; `_cdc` schema; transition function and the leader machine; guard, take-over, heartbeat, release; bootstrap; local platform simulator | 4, 5, 9 | H | Fable |
| A | Apply: the statements of 6.2, 6.3 and 6.5 over a group | 6.1–6.3, 6.5, open points 2, 3, 9 | O (canonical form) | Opus |
| P | Source: connections with deadlines, catalog, publication, slot on a standby and on a primary, slot facts, timeline history, primary heartbeat | 8 (facts only), SPEC 4.4 | — | Opus |
| S | Stream: transaction assembly, groups, the commit and acknowledge protocol with its crash points, unknown commit outcome, supervisor, pipeline process; slot continuity decisions | 2, 3, 6.4, 8 | E, M, A, P, H | Fable |
| F | Fuzzers and the campaign ledger: workload, fault, stream, leadership (value fuzzer in the P4 type wave) | SPEC §4, PLAN §6 | O, H; attacks S and M as they appear | Opus |
| B | Backfill and hand-off: chunk planning and readers, pending log, catch-up with repair reads, swap; table and backfill machines | 7, 9 | X, S, A, P | Fable |
| T | Truncate, drop, drop-and-recreate; remaining machines and the enumeration tests of 1.9 | 6.5, 9, SPEC 1.5 | S, B | Opus |

Order: E, M, A and P start as writer slots become free (at most four writers at once). S starts when E, M and A are
merged; F when O and H are merged; B when X and S are merged; T last. The gate tests of 1.1–1.9 and 4.2 are written
by the package that owns the behaviour and run by the T1 harness of package O.

## 2. Contracts

### C1 Crash points and fault sites (package H)

`cdc_v2.crashpoints`: `declare(name, description)` once, `crash_point(name)` at the place. The Java side implements
the same environment contract. The names of section 3 of the core design are used as written there. Every package
declares its points; a test fails if a declared point is never reached by the crash runner.

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
| `BEGIN` / `END` | transaction id and all position fields of the record's offset; `END` also the event count in total and per table |
| `TRUNCATE` | the tables of one truncate, in the order received |
| `HEARTBEAT` | position fields of a heartbeat record (the position moved without data) |
| `BATCH_END` | end of one engine batch |
| `FATAL` | the engine stops: class, message and cause chain of the error |

Every record gets a sequence number `seq` in arrival order: commit order, then order inside the transaction. Rows of
several tables travel in separate `ROWS` frames; `seq` restores their order.

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

Commands to the engine: `DURABLE <position>` (the destination commit at this position is durable: the engine
acknowledges the newest `END` at or below it), `STOP`. End of input means the parent is gone: the engine halts at
once without acknowledging anything.

Back-pressure: the pipeline stops reading file descriptor 3; the engine blocks in its write; Debezium's queue fills;
the slot read stops. Nothing between the slot and the applier is unbounded.

Python side (`cdc_v2.stream.engine`):

```python
engine = Engine(config, start_position, artifacts)      # starts the JVM in its own process group
for frame in engine.frames(deadline=...):               # Schema | Rows(table, version, RecordBatch) | Begin | End |
    ...                                                 # Truncate | Heartbeat | BatchEnd; raises EngineBroken, EngineFatal
engine.durable(position)
engine.stop(deadline)        # then kill of the process group; engine.kill() at any time
```

### C3 Change batch (stream → apply, pending log → apply)

One Arrow table per destination table and segment: the columns of C2 `ROWS` (`seq`, `op`, `a<i>`, `b<i>`, `u<i>`,
`k<i>`), containing only whole source transactions. With it travels the table's description for that schema version
(`TableMeta`: per position the source name, destination name, transport form, destination type, key flag,
"unchanged possible" flag). The names of source columns never appear as Arrow or SQL identifiers in transport, so a
source column name can never collide with a name of ours.

```python
apply_group(tx, meta: TableMeta, target: str, changes: pyarrow.Table | str) -> ApplyCounts
truncate(tx, target)
```

`tx` is an open lane transaction (C4). `changes` is local Arrow data, or the name of a relation in MotherDuck with
the same columns (6.4, pending log). `apply_group` executes the statements of 6.2 or 6.3; it never commits. It
raises `ApplyInvariantError` when a keyless delete finds fewer rows than it must remove.

### C4 Destination lanes (package M)

```python
conn = md.connect(role)                       # session time zone UTC, marker, client connection id recorded
lane = md.Lane(conn, name, epoch)             # name: 'stream' | 'backfill' | 'control'
with lane.transaction(deadline) as tx:
    tx.execute(sql, params, deadline=None)
    ...
# leaving the block: fence statement, exactly one row changed, COMMIT
md.clock_sql                                   # the expression for MotherDuck's clock (4.1)
```

Errors: `FenceLost` (another run leads: the caller stops every lane and the pipeline exits), `CommitUnknown` (the
connection was lost during `COMMIT`: the caller reads the state and continues from what it finds), `DeadlineExceeded`
(the statement was interrupted; the connection is closed and replaced). Every call has a deadline; none can wait for
ever (SPEC 4.5). No other module issues `BEGIN`, `COMMIT` or the fence statement.

`_cdc` schema: created and migrated by `cdc_v2.md.schema` only; every package adds its tables there.

### C5 Transition function (package M)

```python
state.transition(tx, machine, key, from_state, to_state, cause)   # inside the caller's transaction
state.read(conn_or_tx, machine, key) -> state
```

Each machine is declared once as a state set and a transition table (`cdc_v2.state.machines`); the function refuses
a pair that is not in the table, refuses when the stored state is not `from_state`, and appends to
`_cdc.transitions`. No other code writes a state column.

### C6 Platform (package M)

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
pg.connect(target, deadline)                       # target: 'primary' | 'replica' | a configured name; TLS verify-full
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
| M | none | `cdc_v2_t_m_*` |
| A | none | `cdc_v2_t_a_*` |
| P | `pg-p`, 55640 (a second home `pg-p2`, 55648) | none |
| S | `pg-s`, 55660 (`pg-s2`, 55668) | `cdc_v2_t_s_*` |
| B | `pg-b`, 55680 (`pg-b2`, 55688) | `cdc_v2_t_b_*` |
| F | `pg-f<n>`, from 55700 in steps of 8, up to 55795 | `cdc_v2_t_f_*` |
| T | `pg-t`, 55800 | `cdc_v2_t_t_*` |

TCP fault proxies listen on 55440–55459 (package H); a worker that needs proxies of its own takes ports from the
upper half of its own range.

## 5. Reviews

Every package is reviewed before it is merged: by the P3 coordinator (Opus, fresh context, did not write the code)
against SPEC, PLAN, the core design and this plan; the packages M, S and B additionally by a second reviewer who
attacks the protocol with cases of its own.
