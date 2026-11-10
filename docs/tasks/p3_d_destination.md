# Task: package D — destination basics

Read `docs/tasks/README.md` first; its rules bind you.

You build the layer through which every other package talks to MotherDuck: connections, lanes with the fence,
deadlines on every statement, the error taxonomy, the `_cdc` schema mechanism, the one transition function for all
state machines, and alert records. Contract: `docs/design/p3_plan.md` C0, C1, C4, C5. This is a core-correctness
package: the fence and the commit rules here are what makes two runs at once harmless (SPEC 4.2) and what makes
exactly-once hold across lost connections (SPEC 1.1, 1.7).

## Read as well

- `docs/design/core.md` sections 2, 3, 4 (all of it), 9 and 11; `docs/adr/0001-architecture.md` decision 2.
- `docs/spike/md_semantics.md` completely (what MotherDuck really does: the clock, conflicts, late commits, frozen
  and dead clients, interrupting server connections) and `docs/spike/run_semantics.md` findings on take-over.
  The spike programs under `tools/spike/` and `flights/spike_md/` show working calls.
- `docs/testing/harness.md` (crash points, fault sites, process control, crash runner).

## Resources

MotherDuck through `tools/with-env` only; databases named `cdc_v2_t_d_*` only, created and dropped by your tests.
The account is shared with other workers: keep data small, never run a heavy query. No Postgres. No Flight.

## What to build

### 1. `src/cdc_v2/md/` — connections and lanes

- **Settings**: a frozen dataclass (destination database name, deadlines, retry limits, the marker). The token comes
  from the environment only; it is never an attribute that can be printed, logged or put into an error message.
  A destination that is a local DuckDB file or `:memory:` must work with the same code (tier T0; later packages
  test on it).
- **`md.connect(role)`**: a connection with the session time zone UTC, a marker that identifies this pipeline's
  connections (`custom_user_agent` or what the spike used), and its server-side connection id read and kept.
  Connecting has a deadline.
- **Connection registry** (design section 4, C4). Before its first transaction, every connection of a lane adds
  (epoch, lane, server connection id, MotherDuck time) to `_cdc.connections`, in one autocommit statement that
  inserts only if `_cdc.leader` still holds the lane's epoch; no row inserted ⇒ `FenceLost`. You provide the
  function a take-over calls: interrupt every listed connection of earlier epochs (ids from this table only,
  never from a list of all connections), and the clean-up of rows older than the previous epoch. When a lane
  replaces a connection (deadline, lost connection), it first interrupts the old server connection by id, so that
  an abandoned transaction cannot go on holding a fence row; show with a test on MotherDuck that a transaction
  left open on an abandoned connection blocks a take-over without this, and does not with it.
- **`md.Lane(conn, name, epoch)`** with `lane.transaction(deadline)` as C4. Lane names are `stream`, `control`
  and `backfill.0` … `backfill.15`; any other name is refused.
  - `BEGIN`; the caller's statements through `tx.execute(sql, params, deadline=None)`, `tx.execute_one(...)`
    (must change exactly one row, else `Conflict`) and what else the callers need (fetching rows, registering
    local Arrow data, row counts of a statement); leaving the block runs the fence statement of design 4.2,
    requires exactly one changed row (else roll back and `FenceLost`), then `COMMIT`.
  - An exception inside the block rolls back; the roll-back itself has a deadline; a connection that cannot roll
    back in time is abandoned and replaced.
  - Crash points `<lane>.fenced`, `<lane>.commit_sent`, `<lane>.committed` and fault sites `md.connect`,
    `md.execute`, `md.fence`, `md.commit`, declared here.
  - A transaction object cannot be used after its block ended, from another thread, or nested.
- **Deadlines.** A statement that passes its deadline is interrupted from a watchdog thread; if it does not return
  within a short grace period the connection is abandoned (closed in the background) and the lane gets a new
  one. A blocked statement must never be able to block the caller beyond deadline plus grace. Show this with a
  statement that really hangs (a fault site that blocks, and a real lock wait or long query on MotherDuck).
- **Errors** exactly as the table in C4. Mapping the client library's exceptions onto them is the hard part: find
  out, with real MotherDuck and with fault sites, what a write-write conflict, a lost connection before `COMMIT`,
  a lost connection during `COMMIT`, an interrupted statement and a take-over between two statements look like,
  and write the mapping down with the evidence. An error that cannot be classified is never treated as "nothing
  happened": after `COMMIT` was sent it is `CommitUnknown`.
- **`md.clock_sql`** (design 4.1) and a helper that reads MotherDuck's clock. No code of this package compares a
  local wall clock with a stored time.
- **The run-again rule** of C4 is yours to prove: build the test that shows why it is needed (a first copy whose
  `COMMIT` lands late, a second copy started after a read that did not see it) and that with the rule the two
  copies never both commit, whatever the timing. Use crash points with `kill_later`, SIGKILL and SIGSTOP of a
  child process, and a successor process.

### 2. `src/cdc_v2/md/schema.py` — the `_cdc` schema

Two parts.

**The leader tables** — `_cdc.leader`, `_cdc.fence`, `_cdc.heartbeat` (design section 4) and `_cdc.connections`.
Their shape is frozen from now on, because the bootstrap (`flights/cdc_v2/main.py`, package L) must create and
use them before it can import this package. You write their statements as constants (plain SQL text, nothing
computed at run time), and `ensure_leader_tables(conn)` that executes them. Requirements:

- every row these tables will ever need exists after initialisation: one leader row, one heartbeat row, and the
  whole fixed set of fence rows (`stream`, `control`, `backfill.0` … `backfill.15`). Nothing is inserted later;
- several processes that run the initialisation at the same moment on a database that has none of it end with
  exactly one set of rows, each of them either succeeding or failing in a way the caller can simply repeat. Find
  out what MotherDuck offers for this (table and rows created in one statement; a primary key; a conflict on the
  catalog) and prove the result with at least 3 simultaneous starters, many repetitions, on MotherDuck;
- rows are only ever updated, never deleted and inserted again (finding 7 of the spike); give the code a helper
  that makes that mistake hard;
- a T0 test helper that compares a second copy of the statements (the one package L will put into `main.py`)
  with yours, character for character.

**Everything else in `_cdc`**: a `_cdc.schema_version` table and ordered migration steps, one list per package,
registered by that package. Steps run only in a leader, after its take-over, each in a fenced `control`-lane
transaction; running a step twice, or from two processes at once, is safe; a step that fails leaves nothing
half-done. You add your own tables this way: `_cdc.transitions`, `_cdc.alerts`.

### 3. `src/cdc_v2/state/` — the transition function

- `state.declare(Machine(name, states, transitions, table, key_columns, state_column))`, once per machine, by the
  package that owns it. The owning package creates the table through the schema mechanism.
- `state.transition(tx, machine, key, from_state, to_state, cause)`: inside the caller's lane transaction;
  refuses a pair that is not declared (before touching the database), updates the state with a condition on
  `from_state` and requires exactly one changed row (else `IllegalTransition` with the stored state), appends to
  `_cdc.transitions` (machine, key, from, to, cause, epoch, MotherDuck time). A first state is set by
  `state.create(tx, machine, key, state, cause)`, which fails if the key exists.
- `state.read(conn_or_tx, machine, key)`.
- No other code writes a state column: add a static test that searches `src/` for writes to declared state columns
  outside this module.
- The test helper for SPEC 1.9: for a declared machine, try every ordered pair of states (and unknown states)
  from every stored state and assert that exactly the declared transitions succeed and every other one raises and
  changes nothing. Deliver it in `cdc_v2.testing` with two example machines of your own; the real machines come
  with their packages.
- `docs/state_machines.md`: the frame of the document (what a machine is, how to read the tables), generated from
  the declarations so that it cannot drift; the owning packages add their machines by declaring them.

### 4. `src/cdc_v2/alerts.py` — alert records

`alerts.emit(tx, kind, severity, fields)` writes one row into `_cdc.alerts` inside the caller's transaction, and
`alerts.emit_now(lane, ...)` in a transaction of its own for alerts that have no data transaction. The table must
carry what SPEC 6.2 needs later without a change of shape: kind, severity, a de-duplication key, first and last
time (MotherDuck's clock), count, state (open, resolved), delivery state and attempts, and `fields` as JSON.
`fields` may hold identifiers, positions, counts and durations only; a value that is not one of these types, or a
string above a small length, is refused in code, so that a row value can never reach an alert (SPEC 8.3).
Delivery to Slack is not part of this package.

## Tests

Tier T0 (default run; local DuckDB in place of MotherDuck, a few seconds):

- Lanes: commit with fence; stale epoch never commits and raises `FenceLost`; a fence row changed between `BEGIN`
  and the fence statement; exactly-one-row rule; roll-back on exception; misuse (nested, reuse, wrong thread).
- Connection registry: registration with a current and with a stale epoch; replacement interrupts the old id.
- Leader tables: initialisation from threads at once; the comparison helper.
- Deadlines with blocking fault sites; connection replacement; no thread and no connection left behind.
- Transition function and the enumeration helper; alerts; schema mechanism (fresh, repeated, concurrent in
  threads, a failing step).

Marker `md` (real MotherDuck, databases `cdc_v2_t_d_*`):

- Everything above that makes sense there, again.
- Two processes, same lane, different epochs and the same epoch: all interleavings that crash points allow;
  never two commits where the design allows one.
- Take-over in the middle of an open transaction of the old run (the old one frozen with SIGSTOP, dead by SIGKILL,
  or slow): what the newcomer sees, how long it takes, which error the old one gets when it continues.
- Commit outcome unknown: kill at `<lane>.commit_sent` with `kill_later` and delays from 0 to a few hundred
  milliseconds, many repetitions; the successor follows the run-again rule; the checked result is exactly-once
  for a counter-and-position table, in every repetition. Record how many of the killed commits landed.
- Lost connection: before `COMMIT`, during `COMMIT`, during a statement (fault sites; if you find a way to break
  the real connection of the client library from the test, use that too and say how).
- The crash runner of the harness over a small system under test of yours that uses a lane to move a position and
  data together: every lane crash point reached, 3 seeds, random kills, freeze with a second instance.
- Timings as indications: fixed cost of an empty lane transaction, of the fence statement, of connect.

## Deliver

- The code and tests above; `docs/design/destination.md` (the API as built, the error mapping with evidence, the
  deadline mechanism, contract notes); evidence under `evidence/p3/destination/`.
- In your report: every behaviour of MotherDuck or of the client library that differs from the spike report or
  from the design, and what the callers (packages A, L, S, V, B) must know that the contract does not say yet.

The leader machine itself, the take-over and the heartbeat are package L, not yours; you deliver what they stand
on. Build your two-process tests with a small take-over of your own that follows design 4.5 (raise the epoch in
the leader row and in every fence row in one transaction).
