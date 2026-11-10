# Destination layer (package D)

The one module that talks to MotherDuck with a transaction: connections with deadlines, lanes with the fence
statement, the leader tables, the connection registry, the migrations of the `_cdc` schema, the transition
function for state machines, and alerts. It is built to contracts C0, C1, C4 and C5 of `docs/design/p3_plan.md`
and to section 4 of `docs/design/core.md`. This document describes the layer **as built**, the mapping of errors
with its evidence, the deadline mechanism, and every place where the contract was open or the design was wrong
(sections 13 and 14).

| File | What |
|---|---|
| `src/cdc_v2/md/settings.py` | `Settings`, the fixed lane names `LANES` |
| `src/cdc_v2/md/connection.py` | `connect`, `Connection`: one worker thread per connection, deadlines, interrupt, replacement |
| `src/cdc_v2/md/lane.py` | `Lane`, `Transaction`, the fence statement, the lane crash points |
| `src/cdc_v2/md/errors.py` | the errors of C4 and the mapping of the client's exceptions onto them |
| `src/cdc_v2/md/registry.py` | `_cdc.connections`: register, interrupt the connections of earlier epochs, clean up |
| `src/cdc_v2/md/schema.py` | the leader tables (constant text), the migrations |
| `src/cdc_v2/state/` | `declare`, `transition`, `read`, `create`; `python -m cdc_v2.state` writes `docs/state_machines.md` |
| `src/cdc_v2/alerts.py` | `emit`, `emit_now`, `resolve` |
| `src/cdc_v2/testing/destination.py` | a destination for one test (local or MotherDuck), a small take-over (design 4.5) |
| `src/cdc_v2/testing/machines.py` | `check_machine` (every pair of states of a declared machine), the search for state writes |
| `tests/destination/` | the tests; `sut.py` is a small system under test, `connect_relay.py` a CONNECT relay |
| `evidence/p3/destination/` | the evidence that this document points to |

## 1. Words

- **Destination**: the MotherDuck database a Flight writes to. For the default test run it is a local DuckDB
  database in memory; the layer is the same code for both.
- **Lane**: one connection that commits data or state (design 4.2). The names are fixed: `stream`, `control`,
  `backfill.0` … `backfill.15`.
- **Epoch**: the number in `_cdc.leader` that a take-over raises by one. A lane is built for one epoch.
- **Fence statement**: the last statement before every `COMMIT` of a lane. It changes exactly one row only while
  the lane's epoch still leads.
- **Leader tables**: `_cdc.leader`, `_cdc.fence`, `_cdc.heartbeat` (design 4) and `_cdc.connections`.
- **Server connection id**: the id MotherDuck gives a client connection (`md_current_client_connection_id()`).
  `md_interrupt_server_connection(id)` ends what that connection is doing on the server.
- **Old run, newcomer**: in a take-over, the run that led before and the run that takes over.
- **Retire** a connection: never use it again; a background thread interrupts it once more, closes it and ends
  its worker thread.
- **T0**: tests without network, in the default run. **md**: tests against real MotherDuck, opt-in.

## 2. The API as built

### 2.1 Settings and connections

```python
from cdc_v2 import md

md.configure(md.Settings(database="my_db"))          # once per process
conn = md.connect("stream")                          # role: a short lower-case name for logs
conn.fetchall(sql, params=None, deadline=None)       # autocommit; also fetchone, execute, clock_us
```

`md.Settings(database, marker="cdc_v2", connect_deadline_s=30.0, connect_attempts=3, connect_retry_pause_s=0.5,
statement_deadline_s=60.0, transaction_deadline_s=300.0, rollback_deadline_s=5.0, grace_s=2.0,
interrupt_deadline_s=5.0)` is a frozen dataclass.

- `database` is a plain MotherDuck database name. The layer opens it alone (`attach_mode=single`), so no other
  database of the account is attached. `:memory:` or `:memory:<name>` gives a local database (tier T0); a file
  path gives a local file. A connection string (`md:…`, a `?`) is refused: the token is never a setting; the
  client reads it from the environment itself.
- `marker` is sent as `custom_user_agent`.
- `md.connect(role, settings=None, deadline=None)` sets the session time zone to UTC and keeps the server
  connection id. It tries `connect_attempts` times. Errors: `ConnectionLost`, `DeadlineExceeded`.
  Test: `test_md.py::test_connect_keeps_the_server_id_and_the_session_is_utc`,
  `test_lane.py::test_connect_has_a_deadline`.
- A deadline is always a `cdc_v2.deadline.Deadline`. `None` means the default of the settings. A number of
  seconds is refused with `TypeError`, in every call of the layer
  (`test_lane.py::test_arguments_are_checked_and_a_failed_connect_hides_the_path`; the same test covers the
  refused database names and roles).

A `Connection` has `role`, `settings`, `generation` (counts the client connections it has used), `is_motherduck`,
`usable`, `server_id`, and:

| Method | What |
|---|---|
| `ensure(deadline)` | take a client connection now if there is none |
| `execute`, `fetchall`, `fetchone(sql, params=None, deadline=None)` | one statement in autocommit |
| `clock_us(deadline=None)` | MotherDuck's clock as integer microseconds (section 10) |
| `interrupt_server_connection(server_id, deadline=None)` | interrupt another connection by id; refuses its own id |
| `abort()` | from any thread: end the call in flight now (section 4) |
| `close(deadline=None)` | close; also a context manager |

Errors of the autocommit methods: `Conflict` (nothing applied), `ConnectionLost` and `DeadlineExceeded` for a
read, `CommitUnknown` for a write whose outcome is unknown (a single statement in autocommit is its own
commit). A write that did complete but late returns its result; only the connection is replaced.
Evidence: `deadlines.json`, key `long_statement_on_the_server` (read: `DeadlineExceeded`, write: `CommitUnknown`).

Every statement of a caller is checked (`check_sql`): exactly one statement; never `BEGIN`, `COMMIT` or
`ROLLBACK`; never a statement that deletes, inserts, truncates, merges into, drops, alters or re-creates
`_cdc.leader`, `_cdc.fence` or `_cdc.heartbeat`, and never `DROP SCHEMA _cdc`. A refused statement raises
`LaneMisuse` before anything is sent. Tests: `test_lane.py::test_misuse_is_refused`,
`test_lane.py::test_leader_rows_cannot_be_deleted_or_inserted`.

`md.open_connections()`, `md.background_threads()` and `md.wait_idle(deadline)` let a test check that nothing is
left (`test_lane.py::test_no_connection_and_no_thread_is_left`).

### 2.2 Lanes and transactions

```python
lane = md.Lane(conn, "stream", epoch)                # one lane per connection; epoch is an integer >= 1
with lane.transaction(deadline) as tx:               # deadline: a cdc_v2.deadline.Deadline, or None for the default
    pos = tx.fetchone("SELECT pos FROM ... WHERE ...")[0]
    tx.execute("INSERT INTO ...", params)
    tx.execute_one("UPDATE ... SET pos = ? WHERE ... AND pos = ?", [new, pos])   # exactly one row, else Conflict
# leaving the block: fence statement, exactly one row changed, COMMIT
```

A `Transaction` has `lane`, `epoch`, `deadline`, `is_motherduck`, and:

| Method | What |
|---|---|
| `execute(sql, params=None, deadline=None)` | run a statement; returns the number of changed rows when the statement reports one |
| `execute_one(sql, params=None, deadline=None)` | the statement must change exactly one row. Any other count rolls back and raises `Conflict`. A statement that reports no count is `LaneMisuse` |
| `fetchall`, `fetchone`, `fetch_arrow` | read; `fetch_arrow` returns a `pyarrow.Table` |
| `clock_us(deadline=None)` | MotherDuck's clock inside the transaction |
| `register(name, data, deadline=None)` | make local Arrow data visible as a view until the block ends |
| `fail(error)` | roll back now and remember `error`; leaving the block then raises it even if the caller swallowed it |

Rules that the object enforces (`LaneMisuse` otherwise; `test_lane.py::test_misuse_is_refused`): one block per
`Transaction` object; no nested block on one lane; only the thread that opened the block uses it; no use after
the block. An error that the caller catches inside the block and does not raise again is raised when the block
ends: a failed transaction cannot end quietly (`test_lane.py::test_a_failed_transaction_cannot_end_quietly`).

The deadline of one statement is the smaller of `statement_deadline_s`, the statement's own `deadline` and what
is left of the block's deadline.

`md.crash_points(lane)` gives the three crash points of a lane. `md.clock_sql` is the expression of design 4.1.
`md.FENCE_SQL` is the fence statement.

### 2.3 Registry

`cdc_v2.md.registry` keeps `_cdc.connections (epoch, lane, connection_id, registered_at)`.

- `register(conn, lane, epoch, deadline=None)` returns the server connection id. The lane calls it itself
  before the first `BEGIN` on every new client connection. One statement inserts the row only if `_cdc.leader`
  still holds the epoch; no row inserted is `FenceLost`
  (`test_lane.py::test_new_connection_of_a_stale_epoch_is_refused_at_registration`).
- `md.interrupt_earlier(conn, epoch, deadline=None)` returns `(listed, interrupted)`: the rows with an epoch
  **below** `epoch`, and how many of them the server interrupted. The caller's own connection is left out.
- `registry.cleanup(conn_or_tx, epoch, deadline=None)` deletes the rows of epochs before `epoch - 1`.

A connection that writes for an epoch without being a lane (the heartbeat connection of package L) calls
`register` itself, with one of the fixed lane names
(`test_lane.py::test_a_connection_that_is_not_a_lane_registers_under_a_lane_name`).

Section 7 says what the server lists and what an interrupt answers.

### 2.4 Leader tables and migrations

`cdc_v2.md.schema` holds the statements of the leader tables as constant text
(`test_schema.py::test_the_leader_table_statements_are_constant_text`):

| Table | Created as | Rows |
|---|---|---|
| `_cdc.leader` | `epoch 0, flight_id NULL, run_number NULL, run_id NULL, state 'released', taken_at NULL` | 1 |
| `_cdc.fence` | `lane, epoch 0, n 0` for each of the 18 lanes | 18 |
| `_cdc.heartbeat` | `epoch 0, beat_at NULL, progress_at NULL, phase 'none'` | 1 |
| `_cdc.connections` | empty | 0 |

Each table that has rows is created **with** its rows in one `CREATE TABLE IF NOT EXISTS … AS SELECT`, so there
is no moment in which the table exists without them.

- `schema.ensure_leader_tables(conn, deadline=None)` runs the statements and checks the rows. It repeats after
  `Conflict`, `CommitUnknown` and `ConnectionLost`, and returns the number of repeats.
- `schema.check_leader_tables(conn)` raises `SchemaError` when a row is missing. It never repairs
  (`test_schema.py::test_a_missing_row_is_reported_not_repaired`).
- `schema.NAMED_STATEMENTS` is every statement of which the bootstrap needs a copy, by name;
  `schema.compare_statements(copy, required=…)` compares a copy character for character
  (`test_schema.py::test_compare_statements_finds_every_difference`).

Starters at once: on an empty MotherDuck database, 4 rounds of 3 processes and 12 rounds of 4 threads all ended
with exactly `(1, 18, 18, 1)` rows; every process exited 0 after 0 or 1 repeats
(`leader_tables_concurrent.json`; `test_md.py::test_starters_at_once_end_with_exactly_one_row_set`; T0:
`test_schema.py::test_starters_at_once_end_with_exactly_one_row_set`).

Migrations. Every other `_cdc` table is made by migration steps.

```python
from cdc_v2.md import schema
schema.register_migrations("apply", [schema.Step("applied_table", ["CREATE TABLE _cdc.applied (...)"]), ...])
done = md.migrate(control_lane)          # [(package, version, step name), ...] of the steps it ran
```

- A package registers one ordered list of steps. A step has a name and statements. The list only grows at its
  end.
- `md.migrate(lane, deadline=None)` runs only on the `control` lane, so only a run that leads migrates
  (`test_schema.py::test_only_the_control_lane_migrates`,
  `test_schema.py::test_a_run_that_no_longer_leads_migrates_nothing`).
- It first looks without writing. When nothing is pending it writes nothing
  (`test_schema.py::test_running_again_does_nothing_and_writes_nothing`).
- Each step runs in its own fenced transaction together with the move of its version row in
  `_cdc.schema_version (package, version, step, epoch, updated_at)`. The move follows the run-again rule
  (`execute_one` with a condition on the version read). So a step is applied exactly once, also when two leaders
  migrate at once (`test_schema.py::test_leaders_at_once_apply_every_step_exactly_once`) and when the answer
  of a `COMMIT` is lost (`test_schema.py::test_a_step_whose_commit_has_no_answer_is_applied_once`: after
  `CommitUnknown` it waits 2.5 s and reads again).
- A database that is newer than the code, or whose recorded step names differ, is refused with `SchemaError`
  (`test_schema.py::test_a_database_that_does_not_fit_the_code_is_refused`).
- The lists `state` and `alerts` of this package run first, the others in the order of their package names.

### 2.5 State machines (contract C5)

```python
from cdc_v2 import state

CHUNK = state.declare(state.Machine(
    name="chunk", states=("planned", "loading", "done"),
    transitions={("planned", "loading"), ("loading", "done"), ("loading", "planned")},
    table="_cdc.chunks", key_columns=("table_id", "chunk_no"), state_column="state",
    initial=("planned",), description="one chunk of a backfill"))

state.create(tx, CHUNK, (table_id, 0), "planned", "plan", values={"created_at": state.NOW})
state.transition(tx, CHUNK, (table_id, 0), "planned", "loading", "loader 3 took it")
state.read(conn_or_tx, CHUNK, (table_id, 0))        # the stored state, or None
```

- `transition` refuses a pair that is not declared **before** it touches the database
  (`IllegalTransition`, `declared=False`).
- It changes the row with a condition on `from_state`. No row changed: `IllegalTransition` with the state that
  is stored; the caller's transaction stays usable. More than one row for a key: `StateError`, and the
  transaction fails.
- Every transition and every `create` appends to `_cdc.transitions (machine, key, from_state, to_state, cause,
  epoch, changed_at)` in the same transaction (`test_state.py::test_a_transition_is_part_of_the_callers_transaction`).
- Two transactions cannot both make one transition
  (`test_state.py::test_two_transactions_cannot_both_make_one_transition`).
- `cdc_v2.testing.machines.check_machine(lane, machine, key=…)` tries every pair of states and asserts that
  exactly the declared ones are accepted (SPEC 1.9). `test_state.py::test_the_enumeration_catches_a_wrong_transition_function`
  shows that it fails for a transition function that is wrong in one of three ways.
- `python -m cdc_v2.state` writes `docs/state_machines.md` from the declarations; a T0 test fails when the file
  is not what the declarations give (`test_state.py::test_the_document_is_generated_from_the_declarations`).
  **Whoever merges a package that declares a machine must run it again.**

### 2.6 Alerts

```python
from cdc_v2 import alerts
alerts.emit(tx, "table_quarantined", "warning", {"table_id": 16385, "lsn": "0/16B3748"}, key=("table_id",))
alerts.emit_now(lane, "takeover", "info", {"previous_run": 41})    # a transaction of its own
alerts.resolve(tx, alerts.dedup_key("table_quarantined", {"table_id": 16385}, ("table_id",)), "table released")
```

- `_cdc.alerts` is made by the migration list `alerts`. An alert is one inserted row with state `open`; nothing
  updates a row except the declared machine `alert` (`open` → `resolved`) and, later, the deliverer's own
  delivery column.
- `fields` holds only identifiers, positions, counts and durations (SPEC 8.3): at most 24 fields; a value is an
  integer of 64 bits (not a boolean), a finite float, or a string of at most 128 printable characters. Anything
  else raises `AlertError` and nothing is written
  (`test_alerts.py::test_a_value_that_is_not_an_identifier_position_count_or_duration_is_refused`).
- `key` names the fields that, with the kind, tell one condition from another. `dedup_key` is stored so that
  the deliverer can send one message per condition.
- `emit_now` tries again after `Conflict` and `ConnectionLost` until its deadline. It passes on `FenceLost` and
  `CommitUnknown` (`test_alerts.py::test_emit_now_passes_on_what_it_cannot_decide`).

### 2.7 Test support

`cdc_v2.testing.destination`:

- `local_destination()` and `motherduck_destination(prefix)` give a private destination with the leader
  tables and remove it at the end. `Destination.lead()` takes over, migrates and returns the epoch.
- `take_over(settings, …)` is a stand-in for the bootstrap of package L. It follows design 4.5: in one
  transaction it raises the epoch in the leader row, in every fence row and in the heartbeat row. On a conflict
  it interrupts the connections of earlier epochs and tries again until its time is over (`TakeoverFailed`).
  After an unknown commit outcome it waits 2.5 s, reads the record and decides by `run_id`.
- `create_database(name)`, `drop_database(name)` and `leftover_databases(prefix)` create and drop a MotherDuck
  test database through `cdc_v2.testing.md` (package O), the one place that does this. It checks the name, has
  a deadline, and writes a **ledger** entry (an empty file named like the database, under
  `~/.cache/cdc_flight_v2/md_ledger/`) before it creates and removes it after the drop. So a database that a
  killed test left behind is found by its name, without listing the account
  (`test_sut.py::test_a_test_database_is_in_the_ledger_from_before_it_is_created_until_it_is_dropped`;
  on MotherDuck `test_md.py::test_a_test_database_is_in_the_ledger_until_it_is_dropped`).

## 3. How a lane transaction runs

| Step | What the layer does | Fault site | Crash point |
|---|---|---|---|
| 1 | On a new client connection: connect; interrupt the server connection it replaces, by id; register in `_cdc.connections` | `md.connect`, `md.execute` | |
| 2 | `BEGIN` | `md.execute` | |
| 3 | The caller's statements | `md.execute` | |
| 4 | The fence statement; it must change exactly one row | `md.fence` | `<lane>.fenced` after it |
| 5 | The block's deadline is checked one last time | | `<lane>.commit_sent` after it |
| 6 | `COMMIT` | `md.commit` before it is sent, `md.commit_reply` after the destination has executed it | `<lane>.committed` after it returned |

- An empty block still runs steps 4 to 6 (`test_lane.py::test_empty_transaction_still_runs_the_fence`).
- An exception in step 3 rolls back and is passed on (`test_lane.py::test_an_exception_rolls_back_and_is_passed_on`).
- The roll-back has its own short deadline (`rollback_deadline_s`). A roll-back that does not answer retires the
  connection; the caller still gets its own exception (`deadlines.json`, key `roll_back_blocked`: after 3.17 s
  with a roll-back deadline of 1.0 s and a grace of 2.0 s).
- Every lane has its three crash points, and each is reached once per transaction
  (`test_lane.py::test_every_lane_has_its_three_crash_points`).

## 4. Deadlines

**Rule.** No call of this layer waits longer than its deadline plus the grace period (`grace_s`, default 2 s).

**Why a mechanism is needed.** The client library has no time limit for a statement. Its own `interrupt()` does
not end a statement that runs on MotherDuck: an `UPDATE` that sleeps 4 s on the server and is interrupted by the
client after 0.8 s returned after 4.17 s (`error_mapping.json`, key `raw_texts`, case
`client_interrupt_while_the_statement_runs_on_the_server`, `stopped_early: false`).

**Mechanism.**

1. Every connection has one worker thread. The caller hands a statement to it and waits, at most until the
   deadline.
2. At the deadline a watchdog thread interrupts: the client's `interrupt()`, and on MotherDuck
   `md_interrupt_server_connection(<the connection's own id>)` sent through a second, short-lived connection.
3. The caller waits the grace period for the statement to end.
4. If the statement ended: the outcome decides (section 5). If it did not, or if it ended late: the connection
   is retired. A reaper thread interrupts it once more, closes it in the background and ends the worker. The
   caller never calls `close()` on a connection that may hang.
5. The `Connection` object takes a new client connection at its **next** call, not at once. Before the new one
   is used, it interrupts the old server connection by id, so that an abandoned transaction cannot keep holding
   a fence row (C4). Test: `test_lane.py::test_a_replaced_connection_is_interrupted_by_id_before_the_new_one_is_used`,
   `test_lane.py::test_the_interrupt_of_a_replaced_connection_runs_on_the_new_connection`; on MotherDuck
   `registry_takeover.json`, key `replaced_connection_interrupted_by_id` (the interrupt statement took 76 ms; the
   next transaction with connect, interrupt and registration took 488 ms).

`Connection.abort()` does steps 2 to 4 at once from any thread; the thread that is using the connection gets
`ConnectionLost`, or `CommitUnknown` inside `COMMIT` (`test_lane.py::test_abort_from_another_thread_ends_the_call_at_once`).

**Evidence on MotherDuck** (`deadlines.json`; grace 2.0 s):

| Case | Deadline | The caller got its error after | Error | Committed |
|---|---|---|---|---|
| a statement blocks at `md.execute` (first or second statement) | 2.0 s | 4.01 s, 4.02 s | `DeadlineExceeded` | no |
| the fence statement blocks (`md.fence`) | 2.0 s | 4.00 s | `DeadlineExceeded` | no |
| `COMMIT` blocks before it is sent (`md.commit`) | 2.0 s | 4.02 s | `CommitUnknown` | no |
| the answer of `COMMIT` blocks (`md.commit_reply`) | 2.0 s | 4.00 s | `CommitUnknown` | yes |
| an `UPDATE` that sleeps 30 s on the server, in a lane transaction | 1.0 s | 1.12 s | `DeadlineExceeded` | no |
| the same as a read / as a write outside a transaction | 1.0 s | 1.21 s / 1.21 s | `DeadlineExceeded` / `CommitUnknown` | no |

After the statement that slept on the server was ended, another lane updated the same row 334 ms later: the
interrupt on the server released the row. After each case the next transaction of the lane ran on a new
connection after 475 to 509 ms. The same cases run at T0 with shorter times
(`test_lane.py::test_a_blocked_step_never_holds_the_caller_beyond_deadline_plus_grace`,
`test_lane.py::test_a_long_query_is_interrupted_at_its_deadline`).

A block whose deadline has passed when it ends is rolled back before `COMMIT`: `DeadlineExceeded`, nothing
committed (`test_lane.py::test_an_expired_deadline_stops_the_block_before_commit`, and the same test in
`test_md.py`).

## 5. Error mapping

The rules, in the order they are applied (`src/cdc_v2/md/errors.py`):

1. A deadline that passed decides first: before `COMMIT` `DeadlineExceeded`, from `COMMIT` on `CommitUnknown`.
2. A **transport** error: the client's classes `ConnectionException`, `IOException`, `HTTPException`,
   `InterruptException`; Python's `ConnectionError`, `TimeoutError`, `OSError`; and every client error whose text
   holds one of the phrases `Connection Error`, `interrupted by user`, `lease expired`, `transaction already
   rolled back`, `Request failed`, `Could not connect`, `UNAVAILABLE`, `DEADLINE_EXCEEDED`, `Connection already
   closed`, `No open result set`, `connection … closed`. The phrases are needed because MotherDuck reports a
   server interrupt in a transaction as `TransactionException`, not as a connection error.
3. A **conflict**: a client error whose text says `Conflict on …` or `write-write conflict`.
4. Everything else is passed on unchanged before `COMMIT`, and is `CommitUnknown` from `COMMIT` on.

The text decides nothing for an error about the data of a statement (the client's `DataError` and
`IntegrityError` families: a conversion error, a constraint error). Such a text quotes a value of a row. Found
while this document was written: `Could not convert string 'Conflict on update' to INT32` was sorted as a
conflict, and a value with `Connection Error` in it as a lost connection; a caller answers both by running the
transaction again. Seeded regression tests:
`test_errors.py::test_a_value_that_holds_a_phrase_does_not_look_like_a_conflict_or_a_lost_connection` and the
table in `test_errors.py::test_how_an_exception_is_sorted`. Limit: a `TransactionException` whose text quotes a
key value (`Failed to commit: PRIMARY KEY or UNIQUE constraint violation: duplicate key "…"`) is still sorted
by its text. At `COMMIT` that is `CommitUnknown` or, with a phrase in the key, `Conflict`; both make the caller
read again.

| Where | Transport error | Conflict | Other error | Deadline |
|---|---|---|---|---|
| connect | `ConnectionLost` after the last attempt | | every other error of the client is `ConnectionLost` too | `DeadlineExceeded` |
| registration (one insert in autocommit) | `ConnectionLost`, also when its outcome is unknown | `Conflict` | passed on | `DeadlineExceeded` |
| registration inserted no row | `FenceLost` | | | |
| `BEGIN`, a statement | `ConnectionLost`; the session is dropped | `Conflict` after roll-back | passed on after roll-back | `DeadlineExceeded` |
| the fence statement | `ConnectionLost` | `FenceLost` if the fence row holds another epoch, else `Conflict` | passed on after roll-back | `DeadlineExceeded` |
| the fence statement changed no row | `FenceLost` after roll-back | | | |
| `COMMIT` | `CommitUnknown` | `Conflict` (the destination refused the commit; nothing applied) | `CommitUnknown` | `CommitUnknown` |

The `Conflict` of the row `COMMIT` was never seen for real: no run on MotherDuck or on local DuckDB answered a
`COMMIT` with a write-write conflict. The rule is tested with a fault site only
(`test_lane.py::test_a_conflict_that_commit_reports_is_conflict`).

A message of these errors never repeats the client's text of a conflict (it can hold a key value, SPEC 8.3). For
a transport error it holds the first line of the client's text with tokens, paths, host names and addresses
replaced (`errors.safe_text`; `test_errors.py::test_a_message_never_holds_a_token_a_path_a_host_or_an_address`).

**Evidence on MotherDuck** (`error_mapping.json`; tests in `test_md.py`):

| Case | The client's text (shortened) | Raised | Committed |
|---|---|---|---|
| a second lane updates a row that an open transaction has updated | `Conflict on update!` | `Conflict`; the connection is kept | no |
| a statement in flight is interrupted on the server | `ConnectionException: Connection Error: interrupted by user`, 104 ms after the interrupt | `ConnectionLost` | no |
| an open transaction is interrupted on the server, then a statement | `TransactionException: … transaction already rolled back: Connection Error: interrupted by user` | `ConnectionLost` | no |
| the same, then the block ends (fence statement) | the same text | `ConnectionLost` | no |
| the connection is interrupted between the fence statement and `COMMIT` | `TransactionException: … Failed to commit: MotherDuck transaction statement failed: Connection Error: interrupted by user` | `CommitUnknown` | no |
| fault site `md.execute` raises `ConnectionError` | | `ConnectionLost` | no |
| fault site `md.fence` raises `ConnectionError` | | `ConnectionLost` | no |
| fault site `md.commit` raises `ConnectionError` | | `CommitUnknown` | no |
| fault site `md.commit_reply` raises `ConnectionError` | | `CommitUnknown` | **yes** |

The key `raw_texts` of the same file lists, for plain client connections, the statements and answers of each
case. Three answers of the client that the layer has to know:

- **`COMMIT` after a failed statement answers without an error and commits nothing.** The layer therefore never
  sends `COMMIT` after a failure; it rolls back.
- After a server interrupt the same client connection can be used again. The layer still retires it: it cannot
  know what else the interrupt ended.
- Two transactions that create the same table conflict at the statement.

The same mapping is tested at T0 with fault sites for every step
(`test_lane.py::test_a_lost_connection_is_reported_by_where_it_happened`,
`test_lane.py::test_an_unclassified_error_after_commit_was_sent_is_commit_unknown`).

## 6. The fence statement, and conflicts per column

**Finding (contradicts design 4.2).** A write-write conflict is detected **per column**, on local DuckDB 1.5.6
and on MotherDuck. Two open transactions that update different columns of one row do not conflict, and both
commit.

Design 4.2 gives the fence statement as `UPDATE _cdc.fence SET n = n + 1 WHERE lane = :lane AND epoch = :epoch`.
Design 4.5 gives the take-over as `UPDATE _cdc.fence SET epoch = :seen + 1`. One writes `n`, the other writes
`epoch`. With these two texts the old transaction **commits after the take-over committed**, in two of three
orders, 5 of 5 rounds each, locally and on MotherDuck (`fence_columns.json`, rows `n_only (design 4.2)` with
`epoch_only (design 4.5)`):

| Order | What happened |
|---|---|
| the take-over commits first, then the old fence statement runs (the old transaction's snapshot is older) | the old fence statement changed 1 row; the old `COMMIT` committed |
| the old fence statement runs, then the take-over commits, then the old `COMMIT` | the take-over did not conflict; the old `COMMIT` committed |

**As built.** The fence statement also writes the column that its condition reads:

```sql
UPDATE _cdc.fence SET n = n + 1, epoch = ? WHERE lane = ? AND epoch = ?      -- epoch, lane, epoch
```

With it, every order ends with exactly one of the two committed, with either take-over text, 5 of 5 rounds
each, locally and on MotherDuck (same file, rows `n_and_epoch_parameter (as built)`). The seeded regression
test is `test_lane.py::test_conflicts_are_per_column_which_is_why_the_fence_statement_writes_the_epoch`; it
also keeps the failing pair as a negative control. The process tests of section 8 run every interleaving that
the crash points allow against it.

**Rule for every caller.** A statement that must conflict with another writer **writes the columns that its
condition reads** (`SET state = 'b' … WHERE state = 'a'` does; `SET pos = ? … WHERE pos = ?` does;
`SET n = n + 1 … WHERE epoch = ?` does not). `state.transition` and the version row of the migrations follow
it.

The same rule applies to the leader row and the heartbeat row of package L. This package did not test those
statements. By the finding, the heartbeat statement of design 4.3 (it writes `beat_at`, `progress_at`, `phase`
with a condition on `epoch`) conflicts with the take-over of design 4.5 only because that take-over also writes
`beat_at` and `phase` of the heartbeat row. Package L must test its own statements against its own take-over,
in every order, as `tests/destination/probes/probe_fence_columns.py` does for the fence row.

## 7. Connections on the server

What `md_active_server_connections()` lists and what `md_interrupt_server_connection(id)` answers, measured with
connections of the test itself, always filtered by their own ids (`registry_takeover.json`, key
`what_the_server_answers`; `test_md.py::test_what_the_server_lists_and_what_an_interrupt_answers`):

- `BEGIN` is not sent to the server (0.4 ms). A connection that only said `BEGIN` is not listed, and an
  interrupt of it answers false.
- The server lists a connection only while it has a statement in flight, or a transaction that has run at
  least one statement. A transaction that so far only **read** is listed too (`two_process.json`, key
  `different_epochs`, point `sut.begun`: the newcomer interrupted the old run, which then got `ConnectionLost`).
- An interrupt answers true in exactly those states. So `interrupted` in the result of `interrupt_earlier`
  counts connections that held something, not connections that are alive. An idle registered connection cannot
  be told from one that is gone.
- The interrupt answers before the server has finished. Right after it the server still listed the transaction
  (5 of 5 rounds) and a second interrupt answered true in 4 of 5 rounds. One second later the transaction was no
  longer listed (0 of 5), although the client had sent nothing. The client learns of it at its next statement,
  which fails with `transaction already rolled back: Connection Error: interrupted by user`. The test records
  these counts and does not assert them: an earlier version asserted that the second interrupt answers true, and
  failed once on 2026-11-10.
- A statement cannot interrupt its own connection: the statement itself fails. So `interrupt_earlier` leaves
  the caller's own id out, and `Connection.interrupt_server_connection` refuses it.
- A statement in flight ends 0.1 to 0.32 s after the interrupt.
- **The rows of an interrupted transaction stay held for a moment after the interrupt answered true.** A writer
  that comes right after the interrupt can still get a conflict. It must try again. Measured: a lane that
  wrote the row right after `interrupt_earlier` had returned got 3 conflicts, and committed 1.28 s after the
  interrupt (`registry_takeover.json`, key `open_transaction_holds_only_data_rows`). A take-over that
  interrupts needs 1 or 2 further attempts (the table below, and section 8).

What a take-over meets (`registry_takeover.json`; one process, lanes as the old run; the old run's client is
alive, so nothing ends its transaction but the interrupt):

| The old run's open transaction | Take-over with the interrupt | Without the interrupt |
|---|---|---|
| has updated the heartbeat row | won at the 3rd attempt after 2 conflicts, 1.93 s | `TakeoverFailed` after 10 conflicts in 6.5 s |
| has run its fence statement and stands before `COMMIT` | won at the 2nd attempt after 1 conflict, 1.07 s; the old run gets `CommitUnknown`; nothing committed | `TakeoverFailed` after 9 conflicts in 5.3 s |
| holds only data rows | won at once, 0.38 s; nothing listed, nothing interrupted | the same |

The last row matters: **a take-over that meets no conflict interrupts nothing**, and the old run's open
transaction keeps holding its data rows. The new run then gets `Conflict` on those rows until somebody
interrupts the old connection. So the new leader calls `md.interrupt_earlier(conn, new_epoch)` once more
**after** it won (`test_md.py::test_an_open_transaction_that_holds_only_data_rows_does_not_block_the_take_over_but_the_new_run`).
The small take-over of the test support does not do it; the system under test of section 8 does.

These three tests asserted at first that the take-over wins at its second attempt and that the row is free at
once after the interrupt. Both failed in one run of 2026-11-10 (2 conflicts instead of 1; a `Conflict`
right after the interrupt). They now assert what the layer can promise: the take-over wins after conflicts
only, and the row is free within 10 s (`FREE_WITHIN_S`); the counts are recorded.

## 8. Two processes on one lane

`tests/destination/sut.py` is a small system under test (**SUT**). A process of it is a pipeline in miniature:
it takes over (design 4.5, with `cdc_v2.testing.destination.take_over`), calls `md.interrupt_earlier` once
more, migrates, and then applies numbered events from a file, one thread per lane. One lane transaction reads
the position, inserts up to three rows into `sut.data` and moves the position row with `execute_one` and a
condition on the position it read (the run-again rule of C4). The position row also counts the rows (`n`).

The **check** reads positions and rows in one transaction and asks: is every event there at most once
(`doubles` is 0), and is the counter equal to the position and to the number of rows.

The SUT has crash points of its own inside a data transaction: `sut.begun` (the position is read), `sut.written`
(rows inserted), `sut.moved` (position moved); then come the lane's `stream.fenced`, `stream.commit_sent`,
`stream.committed`. A test freezes a process at a point with SIGSTOP, lets it go on with SIGCONT, or kills it
with SIGKILL. All tests of this section are in `tests/destination/test_md_procs.py` and run real processes
against MotherDuck. In every case of this section the check passed: 0 doubles, counter equal to position.

### 8.1 Different epochs: the old run frozen at every point

`test_two_runs_with_different_epochs_at_every_point`; `two_process.json`, key `different_epochs`. The old run
is frozen at a point of its second transaction for 6.5 to 8.6 s. A newcomer starts, takes over, applies
everything and ends. Then the old run goes on.

| Old run frozen at | Take-over | Newcomer before its first commit | Old run, when it goes on |
|---|---|---|---|
| `sut.begun` (it only read) | at once, 0.41 s; the call after the take-over interrupted 1 | no error | `ConnectionLost`, then `FenceLost` |
| `sut.written` | at once, 0.39 s; the call after the take-over interrupted 1 | no error | `ConnectionLost`, then `FenceLost` |
| `sut.moved` | at once; the call after the take-over interrupted 1 | 1 `Conflict` | `ConnectionLost`, then `FenceLost` |
| `stream.fenced` | 3rd attempt after 2 conflicts, 1.84 s | no error | `CommitUnknown`, then `FenceLost` |
| `stream.commit_sent` | 3rd attempt after 2 conflicts, 1.74 s | no error | `CommitUnknown`, then `FenceLost` |
| `stream.committed` | at once, 0.39 s | no error | its commit stands; `FenceLost` at its next transaction |

- The old run never committed after the take-over. Its transaction at the first five points left no row.
- The old run ended with exit code 3 (`FenceLost`) 0.56 to 3.05 s after SIGCONT; 2.98 and 3.05 s in the two
  `CommitUnknown` cases, in which the SUT waits 2.5 s before it reads again.
- At `sut.moved` the newcomer got one `Conflict` although the interrupt had answered: the rows of an
  interrupted transaction stay held for a moment (section 7).
- At `stream.fenced` and `stream.commit_sent` the old run's `COMMIT` is the first thing it sends after the
  interrupt. So the layer reports `CommitUnknown` although nothing was committed; the SUT reads again and then
  gets `FenceLost`.

### 8.2 The same epoch twice

Two processes that use one lane with one epoch are a programming error. The fence row is the same row for
both, and the run-again rule holds as well. `test_two_processes_with_the_same_epoch`; `two_process.json`, key
`same_epoch`.

- Both free, 30 events: one process made 8 data transactions (24 rows, 1 `Conflict`), the other 2 (6 rows,
  6 `Conflict`s).
- The first frozen at `sut.begun` or `sut.written` of its second transaction: the second applied everything.
  The first got `Conflict` when it went on.
- The first frozen at `sut.moved`, `stream.fenced` or `stream.commit_sent` (it holds the position row): the
  second got 3 `Conflict`s and committed nothing while the first was frozen. The first committed when it went
  on. Without a take-over nobody interrupts the first.
- The first frozen at `stream.committed`: no error.

At T0 the same is `test_lane.py::test_same_lane_same_epoch_twice_never_commits_both` and
`tests/destination/test_sut.py` (two SUT processes on a local file).

### 8.3 Take-over in the middle of an open transaction

`test_takeover_in_the_middle_of_an_open_transaction`; `takeover_open_tx.json`. The old run holds a row in an
open transaction: its **position row** (it stands at `sut.moved`) or its **fence row** (it stands at
`stream.fenced`). It is **frozen** (SIGSTOP), **killed** (SIGKILL) or **slow** (alive; it keeps the transaction
open for 15 s). The newcomer's first data commit is the moment at which the new run really works.

With the interrupt (key `with_interrupt`):

| Old run holds | Old run is | Take-over | Newcomer's first commit after the take-over | Old run gets |
|---|---|---|---|---|
| fence row | frozen | 3rd attempt, 2 conflicts, 1.74 s | 0.94 s | `CommitUnknown`, then `FenceLost` |
| fence row | killed | 3rd attempt, 2 conflicts, 1.83 s | 0.93 s | |
| fence row | slow | 3rd attempt, 2 conflicts, 1.83 s | 1.01 s | `CommitUnknown`, then `FenceLost` |
| position row | frozen | at once, 0.39 s; the call after it interrupted 1 | 0.90 s | `ConnectionLost`, then `FenceLost` |
| position row | killed | at once, 0.40 s; the call after it interrupted 1 | 0.96 s | |
| position row | slow | at once, 0.40 s; the call after it interrupted 1 | 0.91 s | `ConnectionLost`, then `FenceLost` |

No transaction of the old run committed (0 rows) in the six cases.

Without the interrupt (key `without_interrupt`; the newcomer never calls `interrupt_earlier`). This is the
control: it shows what the interrupt is for.

| Old run holds | Old run is | Take-over | Newcomer's first commit after the take-over | Old run gets |
|---|---|---|---|---|
| fence row | frozen | 49th attempt, 48 conflicts, **28.98 s** | 0.77 s | `DeadlineExceeded` (its block deadline had passed), then `FenceLost` |
| fence row | killed | 50th attempt, 49 conflicts, **29.32 s** | 0.75 s | |
| fence row | slow | 24th attempt, 23 conflicts, 14.41 s | nothing was left to apply | it committed two more transactions (4 rows) before the take-over won, then `FenceLost` |
| position row | frozen | at once, 0.39 s | **28.66 s**, after 50 `Conflict`s | `DeadlineExceeded`, then `FenceLost` |
| position row | killed | at once, 0.40 s | **28.97 s**, after 48 `Conflict`s | |
| position row | slow | at once, 0.38 s | 12.99 s, after 22 `Conflict`s | `FenceLost` when its block ended; nothing committed |

What this shows:

- **MotherDuck ends the open transaction of a frozen or killed client after about 29 s.** This agrees with
  `docs/spike/md_semantics.md` (lease 32 s for a paused client; rows locked 29.4 to 29.9 s after SIGKILL).
- With the interrupt a take-over takes 0.4 s when it meets no conflict and about 1.8 s when it does. The
  newcomer's first commit comes about 0.9 to 1.0 s after the take-over.
- A take-over that meets no conflict interrupts nothing (section 7). Without the call after the take-over the
  new leader's lanes are blocked on the old run's rows for up to 29 s. They are never wrong: the lanes get
  `Conflict` and try again.
- A slow old run that is alive and stands before its `COMMIT` goes on committing when nobody interrupts it.
  The take-over conflicts until it gets in between two transactions. Every commit of the old run came before
  the take-over; after it the old run got `FenceLost`.

### 8.4 Commit outcome unknown

`test_commit_outcome_unknown_kill_at_commit_sent`; `commit_unknown.json`. A process is killed with SIGKILL a
chosen time after it passed `stream.commit_sent` (`kill_later` of the crash points): 9 delays, 4 repetitions
each, 36 victims. After every victim the check ran, and a successor that follows the run-again rule went on.

| Delay after `commit_sent` | Killed commits that landed | Of them, the client had not seen the answer |
|---|---|---|
| 0 ms | 0 of 4 | |
| 10, 20, 40, 60 ms | 4 of 4 each | 4 of 4 each |
| 80 ms | 4 of 4 | 1 of 4 |
| 100, 150, 300 ms | 4 of 4 each | 0 of 4 each |

**32 of 36 killed commits landed, 4 did not.** 17 landed without the client having seen the answer: these
are the cases in which a run cannot know. The result was exactly-once after every one of the 36 victims; at
the end the counter was 108 with 0 doubles.

The edge lies near one round trip (about 80 ms, section 11) and moves a little from run to run: the run
before this one had 0 of 4 at 80 ms, 16 in all (the earlier version of `commit_unknown.json` in the
history of the repository).

A real lost answer (section 9, case `commit_answer_lost`) gives the same: the layer raises `CommitUnknown`, the
commit had landed, the SUT read again and applied nothing twice.

### 8.5 The run-again rule, and a control that breaks it

`test_run_again_rule_with_the_naive_control`; `run_again_rule.json`. A second copy reads the position outside
any transaction. Then a first copy commits three events: frozen before its `COMMIT` and let go; killed right
after the answer; or killed 60 ms after `commit_sent`. In all three the commit landed. Then the second copy
goes on.

| Second copy | Result in each of the three cases |
|---|---|
| follows the rule (reads the position inside the transaction, `execute_one` with a condition on it) | 0 doubles; counter equal to position |
| naive control (`--naive`: uses the position it read before, no condition) | **3 events applied twice**; counter 12, position 9 |

The control shows that the tests see a broken rule, and that the fence does not replace the rule: both copies
had the leading epoch or a newer one.

**After `CommitUnknown` a caller waits before it reads again.** A `COMMIT` that is on its way can land after
the error. The layer waits for nothing; the SUT, the migrations and the take-over of the test support wait
2.5 s (`LATE_COMMIT_WAIT_S`; design 4.5 says "more than 2 s") and then read inside a new transaction. The wait
alone proves nothing: a `COMMIT` held for 4 s still landed (section 9.2, case `commit_delivered_late`). What
keeps the result right is the run-again rule: a transaction that read the position and then meets another
commit of the position row gets `Conflict` (section 8.2, the first process frozen at `sut.begun`).

## 9. A lost connection, and the crash runner

### 9.1 Fault sites in a child process

`test_lost_connection_in_a_child_by_fault_sites`; `connection_break.json`, key `fault_sites_in_a_child`. A SUT
process gets a `ConnectionError` at one fault site, once.

| Site | The SUT got | The transaction was committed |
|---|---|---|
| `md.execute` (during a statement) | `ConnectionLost` | no |
| `md.fence` (before `COMMIT`) | `ConnectionLost` | no |
| `md.commit` (`COMMIT` not sent) | `CommitUnknown` | no |
| `md.commit_reply` (`COMMIT` executed, answer lost) | `CommitUnknown` | yes |

After each the process went on by itself and the check passed.

### 9.2 A real break of the client's connection

**How.** The MotherDuck client obeys the proxy variables of its process. The test gives the child
`https_proxy` and `grpc_proxy` that point to the harness `NetProxy`; behind it stands a small CONNECT relay
(`tests/destination/connect_relay.py`) that opens the tunnel to MotherDuck. The path is: client → `NetProxy` →
relay → MotherDuck. TLS stays end to end; the proxy sees only bytes. The test can then reset the client's TCP
connections or stop passing bytes, in the middle of a transaction, while the child is frozen at a crash
point. Only the child uses the proxy; nothing else on the machine is touched.

`test_a_real_connection_break_in_a_child`; `connection_break.json`, key `real_break`. Child: statement deadline
4 s, transaction deadline 8 s, grace 1 s.

| Case | What the proxy does | The SUT got | Committed |
|---|---|---|---|
| `reset` | resets every TCP connection of the child while its transaction is open (at `sut.written`), then passes again | **no error**; the transaction committed normally | yes |
| `blackhole_statement` | passes nothing from the next statement on; passes again 8 s later | `DeadlineExceeded` | no |
| `commit_never_arrives` | holds the `COMMIT`; after the child's error the connections are reset | `CommitUnknown` | no |
| `commit_answer_lost` | lets the `COMMIT` through, passes nothing back | `CommitUnknown` | **yes** |
| `commit_delivered_late` | holds the `COMMIT`; 4 s after the child's error it delivers it, and the statements of the child's next attempt behind it | `CommitUnknown` | **yes**, late |

In all five the child recovered by itself and ended with exit code 0, 4.5 to 6.9 s after the fault was
lifted. 4 s of that is the time the SUT waits for new events before it ends. The check passed (0 doubles).

What this shows:

- **A TCP reset does not end the transaction.** In the `reset` case the proxy reset 2 live connections of the
  child (`proxy_connections_reset`; the test asserts at least one) and accepted 3 in all, so the client opened
  a new one. The open transaction went on and committed; the caller saw nothing. The session on MotherDuck is
  not tied to one TCP connection.
- A connection that passes nothing gives no error by itself within the times tested; the layer's deadline
  ends the wait (`DeadlineExceeded` before `COMMIT`, `CommitUnknown` from `COMMIT` on).
- A `COMMIT` can land after the caller got `CommitUnknown` (case `commit_delivered_late`). The run-again rule
  kept the result exactly-once.

### 9.3 The crash runner of the harness over the SUT

`test_crash_runner_over_the_sut`; `crash_runner.json`, `crash_summary.json`, `crash_ledger.jsonl`. The harness
`cdc_v2.testing.crash.CrashRunner` runs the SUT with three lanes (`stream`, `control`, `backfill.0`) on
MotherDuck. After every scenario a fresh process catches up and the check compares the destination with the
source.

| Mode | Scenarios | Passed |
|---|---|---|
| kill at every lane crash point (9 points × 3 seeds; the seed chooses the hit) | 27 | 27 |
| kill 40 ms after every `commit_sent` point (3 points × 3 seeds) | 9 | 9 |
| random kills (3 seeds, 3 kills each) | 3 | 3 |
| freeze with SIGSTOP and a second instance that takes over (3 seeds) | 3 | 3 |

**42 of 42 passed; 9 of 9 lane crash points were reached.** The run took 234 s and created and dropped 42
databases. At T0 `tests/destination/test_sut.py` runs the SUT on a local file.

## 10. The clock

`md.clock_sql` is `uuid_extract_timestamp(uuidv7())` (design 4.1). Findings (`clock.json`,
`test_md.py::test_where_the_clock_expression_is_evaluated`):

- **A bare `SELECT uuid_extract_timestamp(uuidv7())` is answered by the client**, without a round trip (median
  0.8 ms; plan `PROJECTION (L)`; 30 of 30 values lie inside the client's own send-and-receive window). It is the
  client machine's clock. Design 4.1 and the spike report say the expression is evaluated by MotherDuck; that is
  true only where the statement runs on MotherDuck.
- The expression runs on MotherDuck in every statement that reads or writes a MotherDuck table: `UPDATE … SET`,
  `INSERT … SELECT`, `INSERT … VALUES`, and a `SELECT … FROM` a MotherDuck table (the plans in `clock.json`).
- So the layer reads the clock with `md.CLOCK_US_SQL`, `SELECT epoch_us(uuid_extract_timestamp(uuidv7())) FROM
  _cdc.leader` (median 74 ms). Use `conn.clock_us()` or `tx.clock_us()`, or write the expression inside DML.
  **Never read the clock with a bare `SELECT`.**
- The clock counts whole milliseconds (30 of 30 values).
- The client cannot fetch a `TIMESTAMPTZ` value without the module `pytz`, which is not a dependency. The layer
  therefore reads times as integer microseconds (`epoch_us`). A caller that selects a `TIMESTAMPTZ` column does
  the same or casts it.

## 11. Timings on MotherDuck

Indications from a laptop, one process, 25 repetitions each, other workers running on the same machine
(`timings.json`):

| What | Median | p90 |
|---|---|---|
| `md.connect()` to a database that the process has open | 76 to 82 ms (5 calls) | |
| `BEGIN` | 0.2 ms | 0.2 ms |
| the fence statement | 76.3 ms | 77.5 ms |
| `COMMIT` after the fence statement | 80.5 ms | 82.4 ms |
| an empty lane transaction | 155.3 ms | 157.7 ms |
| a lane transaction with one `UPDATE` | 234.9 ms | 240.1 ms |
| reading the clock | 79.1 ms | 85.4 ms |

One statement is one round trip of about 75 to 80 ms from this machine. The fence costs one round trip per
transaction. `docs/spike/md_semantics.md` measured 8 ms per round trip from a Flight.

## 12. Static tests

`tests/destination/test_static.py` reads every Python file under `src/cdc_v2`. The bootstrap
`flights/cdc_v2/main.py` (package L) is outside that tree; no test of this package reads it.

| Test | What it proves | Its limit |
|---|---|---|
| `test_no_code_outside_the_state_module_writes_a_state_column` | no `UPDATE`, `INSERT` or `MERGE` text outside `cdc_v2.state` names the state column of a declared machine | it sees SQL text in string literals of Python files; a statement built from parts at run time is seen only as far as its literals show it |
| `test_the_search_finds_seeded_writes_and_nothing_else` | the search finds seeded writes of each kind and no false ones | |
| `test_the_search_walks_a_tree_and_honours_the_exclusions` | the search reads every Python file of a tree and leaves out only what it is told to | |
| `test_only_the_layer_opens_a_duckdb_connection` | `duckdb.connect` is called only in `cdc_v2/md`, `cdc_v2/testing`, `flightctl.py` and `cdc_v2/oracle` | a call through another name is not seen |
| `test_the_oracle_only_reads` | no SQL text of `cdc_v2/oracle` writes or commits; that is why the oracle may open a connection of its own | as the first |
| `test_only_the_layer_holds_the_fence_statement` | the fence statement exists once | |
| `test_no_text_deletes_or_inserts_leader_rows` | no SQL text deletes or inserts rows of the three leader tables | as the first |
| `test_this_package_reads_no_local_wall_clock` | the layer never compares with the local wall clock | only this package's files |

## 13. Contract notes

Where contract C4 or C5, or the design, left something open, the simplest reading was built. Each note names
the test that holds it. Tests without a file name are in `tests/destination/test_lane.py`.

**Connections and deadlines**

1. **A deadline is a `Deadline` object.** C4 writes `deadline` without a type. `None` is the default of the
   settings; a number of seconds raises `TypeError`
   (`test_arguments_are_checked_and_a_failed_connect_hides_the_path`).
2. **Where the database comes from.** C4 writes `md.connect(role)`. Built: `md.configure(settings)` once per
   process, or `md.connect(role, settings)`. `Settings.database` is a name, never a connection string (same
   test).
3. **Every error of the client at connect is `ConnectionLost`**, also one that is no transport error. Its message
   repeats no local path (same test; `test_connect_has_a_deadline`).
4. **A connection that was given up is replaced at the next call**, not at once. C4 says only that the old server
   connection is interrupted "first". Built: the new client connection sends the interrupt, by the stored id,
   before anything else (`test_a_replaced_connection_is_interrupted_by_id_before_the_new_one_is_used`,
   `test_the_interrupt_of_a_replaced_connection_runs_on_the_new_connection`).
5. **`Connection.abort()` is not in C4.** It lets another thread end a call in flight
   (`test_abort_from_another_thread_ends_the_call_at_once`).
6. **`LaneMisuse` is not in C4** and is not a `DestinationError`; it is a `RuntimeError`. It marks a programming
   error (a nested block, a second thread, a statement that `check_sql` refuses), which a handler for errors of
   the destination must not catch (`test_misuse_is_refused`, `test_leader_rows_cannot_be_deleted_or_inserted`).

**Lanes and errors**

7. **The epoch of a lane is an integer of at least 1, and one connection carries one lane**
   (`test_misuse_is_refused`).
8. **`execute_one`.** C4 says it raises `Conflict` "when the statement changed no row". Built: every count other
   than 1 is `Conflict`; a statement that reports no count is `LaneMisuse`
   (`test_execute_one_needs_exactly_one_row`).
9. **`Transaction` has more methods than C4 names**: `fetchall`, `fetchone`, `fetch_arrow`, `register`,
   `clock_us`, `fail` (`test_arrow_data_and_row_counts`, `test_a_failed_transaction_cannot_end_quietly`,
   `test_state.py::test_two_rows_for_one_key_fail_the_transaction`).
10. **The fence statement writes `epoch` too.** Its text differs from design 4.2 (section 6).
11. **`CommitUnknown` starts when the layer hands `COMMIT` to the client**, at the fault site `md.commit`. The
    layer cannot tell a `COMMIT` that was never sent from one whose answer was lost
    (`test_a_lost_connection_is_reported_by_where_it_happened`).
12. **Errors of `COMMIT`.** C4 names `Conflict` for a statement, and says that any other error "is passed on
    unchanged after the roll-back". Built: a write-write conflict that `COMMIT` itself answers is `Conflict`
    (the destination refused the commit; the connection is kept). Every other error of `COMMIT` is
    `CommitUnknown`, because no roll-back can make its outcome certain
    (`test_a_conflict_that_commit_reports_is_conflict`,
    `test_an_unclassified_error_after_commit_was_sent_is_commit_unknown`).
13. **A fifth fault site, `md.commit_reply`.** C1 and C4 list four. It fires after the destination has executed
    `COMMIT` and before the caller sees the answer. Without it "committed, answer lost" cannot be tested at T0
    (`test_a_lost_connection_is_reported_by_where_it_happened`; section 9.1).
14. **An error about the data of a statement is never sorted by its text** (section 5;
    `test_errors.py::test_a_value_that_holds_a_phrase_does_not_look_like_a_conflict_or_a_lost_connection`).

**Registry and leader tables**

15. **A connection that is not a lane registers under a lane name.** C4 speaks of "every connection of a lane".
    The heartbeat connection of package L writes for an epoch too, so it calls
    `registry.register(conn, <a lane name>, epoch)` itself
    (`test_a_connection_that_is_not_a_lane_registers_under_a_lane_name`).
16. **`md.interrupt_earlier(conn, epoch)`** means the epochs below `epoch`. It leaves the caller's own
    connection out and returns `(listed, interrupted)` (`test_interrupt_earlier_and_cleanup`).
17. **`registry.cleanup(conn_or_tx, epoch)`** deletes the rows of epochs below `epoch - 1`, as design 4 says
    ("rows of epochs before the previous one"; same test).
18. **No column `connection_ids` in `_cdc.leader`.** The take-over text of design 4.5 sets one; C4 and the text
    above it in design 4 keep the ids in `_cdc.connections`. Built as C4 says
    (`test_schema.py::test_initialisation_creates_every_row_with_the_frozen_shape`).
19. **The take-over statements are not in `schema.NAMED_STATEMENTS`.** The take-over is package L's. A copy for
    tests is in `cdc_v2.testing.destination` (`take_over`).

**Migrations**

20. **Order.** The lists `state` and `alerts` run first, the other packages in the order of their names. There is
    no way to say that one package's step needs another package's table
    (`test_schema.py::test_steps_run_in_order_each_in_its_own_transaction`).
21. **`_cdc.schema_version` holds one row per package**
    (`test_schema.py::test_a_fresh_database_gets_the_tables_of_this_package`).
22. **A package registers its whole list once per process.** The same list again is accepted; a different one is
    refused (`test_schema.py::test_registering_lists`).

**State machines and alerts**

23. **`state.create` is not in C5.** C5 has no call that makes the first row of a key. Built:
    `state.create(tx, machine, key, state, cause, values=…)`; `Machine` has the optional fields `initial` (the
    states a new row may have) and `description` (`test_state.py::test_create_transition_and_read`,
    `test_state.py::test_create_fails_when_the_key_exists_or_the_state_is_not_initial`).
24. **This package declares no machine but `alert`.** The machines of the tests live in a schema `_cdc_example`
    and are declared only while a test runs
    (`test_state.py::test_the_example_machines_are_declared_only_inside_their_block`). The leader machine is
    package L's (C5).
25. **Alerts.** C4 gives `alerts.emit(tx, kind, severity, fields)`. Built with one more argument, `key`, and with
    `alerts.dedup_key` and `alerts.resolve`. The severities are `info`, `warning`, `critical`. The column
    `delivery_state` starts as `pending`; its other values are the deliverer's
    (`test_alerts.py::test_dedup_key`, `test_alerts.py::test_resolve`,
    `test_alerts.py::test_a_wrong_kind_or_severity_is_refused`,
    `test_alerts.py::test_the_table_carries_what_delivery_needs`).

**Tests**

26. **"No other module issues `BEGIN`, `COMMIT` or the fence statement" (C4)** is read as "no other module
    commits". The oracle of package O opens a connection of its own and a read transaction on it; it must not
    read through the layer it judges. The static tests allow that as long as no SQL text of the oracle writes
    or commits (`test_static.py::test_the_oracle_only_reads`).
27. **Proxy ports.** The plan gives package D no port range. The test of section 9.2 lets the harness `NetProxy`
    and the relay take free ports of the loopback interface.
28. **Test databases** are named `cdc_v2_t_d_<random>` and are created and dropped through `cdc_v2.testing.md`
    (section 2.7). The tests run with `CDC_V2_MD_PREFIX` unset or set to `cdc_v2_t_d_`.

## 14. Findings that differ from the design or the spike reports

Each finding names what the design (`docs/design/core.md`) or the spike report (`docs/spike/md_semantics.md`)
says, what was measured, and where the evidence is.

**The design is wrong or incomplete**

1. **Conflicts are per column** (section 6; `fence_columns.json`). With the fence statement of design 4.2 and
   the take-over of design 4.5 an old run commits after the take-over. The fence statement as built writes
   `epoch` too. The spike could not see this: in `spikes/md_semantics/leader.py` the take-over and the fence
   statement both write the column `hb`, so they conflicted. The spike's conflict tests (its question 4) updated
   the same column of one row.
2. **A take-over that meets no conflict interrupts nothing** (sections 7 and 8.3). Design 4.5 interrupts the
   connections of earlier epochs only "on a conflict". An old run that holds only data rows then blocks the new
   run's lanes until MotherDuck ends its transaction: 28.66 and 28.97 s for a frozen and a killed old run
   (`takeover_open_tx.json`, key `without_interrupt`). With one more call of `md.interrupt_earlier` after the
   take-over the new run's first commit came 0.90 to 0.96 s after it.
3. **A bare `SELECT` of the clock expression is answered by the client** (section 10; `clock.json`). Design 4.1
   and the spike report say that `uuid_extract_timestamp(uuidv7())` is evaluated by MotherDuck. That holds for
   a statement that reads or writes a MotherDuck table; every statement of the spike did.
4. **The wait of "more than 2 s" after an unknown outcome (design 4.5) is no upper bound.** A `COMMIT` that the
   network held for 4 s still landed after the caller had its `CommitUnknown` (section 9.2, case
   `commit_delivered_late`; `connection_break.json`). The spike saw a dead client's `COMMIT` land up to 1.95 s
   after the kill. What keeps the result right is the run-again rule, not the wait (section 8.5).
5. **No column `connection_ids`** (contract note 18): the take-over text of design 4.5 and contract C4 differ.
6. **An error of `COMMIT` cannot be "passed on unchanged after the roll-back"** as C4 says for other errors
   (contract note 12): after `COMMIT` was sent a roll-back decides nothing.

**MotherDuck or the client library behaves differently from what the spike report says, or the report is silent**

7. **An interrupt does not free the rows at once** (section 7; `registry_takeover.json`). The spike report says
   the interrupt "ends the old transaction at once". Measured: the interrupt answers true, the transaction is
   still listed right after it (5 of 5), a writer got 3 conflicts and committed 1.28 s after the interrupt, and
   a take-over needs 1 or 2 further attempts. The take-over times agree with the report (1.07 to 1.93 s here,
   0.5 to 2.2 s there). Three tests of this package had asserted "at once" and failed in one run.
8. **`BEGIN` is not sent to the server, and the server lists a connection only while it holds something**
   (section 7). An idle connection of an old run is not listed and an interrupt of it answers false. So the
   count of interrupted connections says nothing about which runs are alive.
9. **A statement cannot interrupt its own connection** (section 7).
10. **The client's `interrupt()` does not end a statement that runs on MotherDuck** (section 4;
    `error_mapping.json`, key `raw_texts`). A deadline therefore needs the server interrupt through a second
    connection.
11. **`COMMIT` after a failed statement answers without an error and commits nothing** (section 5).
12. **An interrupted client connection stays usable** (section 5). The layer retires it all the same.
13. **A reset of the TCP connections does not end the transaction; a connection that passes nothing gives no
    error within the times tested** (section 9.2; `connection_break.json`, key `real_break`).
14. **Kill at `commit_sent`**: 0 ms after it 0 of 4 commits landed; 10 to 60 ms after it 4 of 4 landed and the
    client had not seen the answer (section 8.4; `commit_unknown.json`).
15. **The text of a conversion or constraint error quotes a value of a row**, and the text of a failed connect
    can hold a local path (section 5; contract note 3). Both are kept out of the errors of this layer.
16. **The client cannot hand out a `TIMESTAMPTZ` value without `pytz`**
    (`test_lane.py::test_times_are_read_as_integer_microseconds`).
17. **The clock counts whole milliseconds** (section 10).

**Agreement with the spike report**

- MotherDuck ends the open transaction of a frozen or killed client after about 29 s (28.66 to 29.32 s here;
  29.4 to 29.9 s and a lease of 32 s there).
- One round trip takes about 75 to 80 ms from a laptop (75 ms there; 8 ms from a Flight).

**A bug of this package that only the MotherDuck run found.** The interrupt of a replaced connection called
itself and never returned. It needs a destination that has `md_interrupt_server_connection`, so no T0 test had
reached it. Seeded regression test:
`test_lane.py::test_the_interrupt_of_a_replaced_connection_runs_on_the_new_connection`.

## 15. What the callers must know

For packages A, L, S, V, B and the integrator. Each line is backed by the section it names.

**Every caller**

- A deadline is a `cdc_v2.deadline.Deadline`, not a number (contract note 1).
- **Write the columns your condition reads** when the statement must conflict with another writer (section 6).
- Follow the run-again rule of C4: read what the transaction depends on inside the transaction, and move it
  with `execute_one` and a condition on what was read. Section 8.5 shows what happens without it.
- After `CommitUnknown`: wait about 2.5 s, then read inside a new transaction and decide by what is there. Never
  assume either outcome. The wait is no proof (finding 4); the condition of the next write is.
- `Conflict` can come again. After an interrupt the row is free after about 1 s; without one, after about 29 s
  (sections 7 and 8.3). Try again with a pause and a limit.
- An error about data (a conversion error, a constraint error) is passed on unchanged. Its text can quote a
  value of a row, so do not log it as it is (section 5; SPEC 8.3).
- A block whose deadline has passed gets `DeadlineExceeded` before `COMMIT` and commits nothing (section 4). A
  run that was frozen for longer than its block deadline meets this first, before `FenceLost`
  (`takeover_open_tx.json`, key `without_interrupt`).
- Never read the clock with a bare `SELECT`. Use `tx.clock_us()` or `conn.clock_us()`, or write `md.clock_sql`
  inside a statement on a MotherDuck table (section 10).
- Read a `TIMESTAMPTZ` column with `epoch_us(…)` or a cast (section 10).
- An empty lane transaction costs two round trips, about 155 ms from a laptop (section 11).
- `LaneMisuse` is a programming error. Do not catch it together with `DestinationError` (contract note 6).
- `alerts.emit_now` passes on `FenceLost` and `CommitUnknown`
  (`test_alerts.py::test_emit_now_passes_on_what_it_cannot_decide`).
- A package registers its migrations as one list (`md.register_migrations`); the lists run in the order of the
  package names, after `state` and `alerts` (contract notes 20 to 22).
- When a package declares a state machine, write the document again:
  `uv run python -m cdc_v2.state > docs/state_machines.md`. A test compares the two
  (`test_state.py::test_the_document_is_generated_from_the_declarations`).

**Package L (leader, take-over, heartbeat)**

- The take-over must try again after a conflict that follows an interrupt: 1 or 2 further attempts were seen
  (section 7).
- Call `md.interrupt_earlier(conn, new_epoch)` once more **after** the take-over won (finding 2).
- `interrupted` counts connections that held something. It does not say whether the old run is alive
  (finding 8).
- Test the heartbeat statement and the leader statements against the take-over in every order, as
  `tests/destination/probes/probe_fence_columns.py` does for the fence row (section 6). This package tested only
  the fence row.
- The heartbeat connection is no lane. Register it with `registry.register(conn, <a lane name>, epoch)` before
  its first write (contract note 15).
- `Connection.abort()` ends a call in flight from another thread (section 4).
- The static tests read `src/cdc_v2` (section 12). There, a text that writes `_cdc.fence` outside `cdc_v2.md`
  is flagged by `test_only_the_layer_holds_the_fence_statement`, and a write of the state column of a
  declared machine outside `cdc_v2.state` by `test_no_code_outside_the_state_module_writes_a_state_column`.
  The allow-lists are in `tests/destination/test_static.py`; an entry needs a reason. The bootstrap
  `flights/cdc_v2/main.py` is outside the tree these tests read: package L needs a check of its own that the
  bootstrap holds the take-over and nothing else that commits.
- The view `_cdc.leader_record` and the leader machine are not built here.

**Package O and the coordinator**

- The oracle opens its own read-only connection; `test_static.py::test_the_oracle_only_reads` holds it to that
  (contract note 26).
- By finding 10, a deadline that relies on the client's `interrupt()` alone does not end a statement that runs
  on MotherDuck. Whether the oracle's own guard is affected was not tested here; it is an inference.
- Test databases go through `cdc_v2.testing.md` and its ledger (section 2.7).

## 16. How to run the suites

**The default run** needs no network: `uv run -q pytest -q`. On 2026-11-10, on this branch with `origin/main`
merged: 567 passed, 1 skipped, 263 deselected in 61 s. The tests of this package alone
(`uv run -q pytest -q tests/destination`): 200 passed, 96 deselected in 13 s.

**The MotherDuck tests** (marker `md`) need the token that `tools/with-env` supplies. They create and drop only
databases named `cdc_v2_t_d_<random>`. Leave `CDC_V2_MD_PREFIX` unset or set it to `cdc_v2_t_d_`. The command
that the evidence files record is `tools/with-env uv run -q pytest -q -m md tests/destination`.
`pyproject.toml` already passes `-q` to pytest, so that command prints no line with the counts; the commands
below leave the second `-q` out.

| Command | Tests | Last run (2026-11-10) |
|---|---|---|
| `tools/with-env uv run -q pytest -m md tests/destination/test_md.py` | 31: what MotherDuck answers, deadlines, the registry, the clock, timings | 31 passed in 310 s |
| `tools/with-env uv run -q pytest -m md tests/destination/test_md_again.py` | 55: the T0 tests of lanes, leader tables, migrations, state and alerts, with a MotherDuck database | 55 passed in 462 s |
| `tools/with-env uv run -q pytest -m md tests/destination/test_md_procs.py` | 10: real processes (sections 8 and 9) | 10 passed in 1079 s, as six runs chosen with `-k` (95 to 308 s each) |

All 96 passed. The whole of `test_md_procs.py` takes about 18 minutes in one run. The six runs were, by `-k`:
`different_epochs or same_epoch`, `middle_of_an_open_transaction`, `commit_outcome_unknown`,
`run_again_rule or by_fault_sites`, `real_connection_break`, `crash_runner`.

**After a run** no test database may be left. This reads only the ledger (section 2.7) and prints `[]`:

    uv run python -c "from cdc_v2.testing import destination as td; print(td.leftover_databases('cdc_v2_t_d_'))"

A name that it prints belongs to a run that was killed. Drop it with
`tools/with-env uv run python -c "from cdc_v2.testing import destination as td; td.drop_database('<name>')"`.

**The probe** of section 6: `tools/with-env uv run python tests/destination/probes/probe_fence_columns.py`. It
writes `fence_columns.json`.

**The document of the state machines**: `uv run python -m cdc_v2.state > docs/state_machines.md`.

**The numbers in this document** are the ones in `evidence/p3/destination/`, each file with the date of its
run. The md tests write these files again at every run. Times and the counts of conflicts move from run to
run; the tests assert only what the layer promises and record the rest. The last run named in the table above
passed with counts in the same ranges; its files were not kept, except `commit_unknown.json` (section 8.4).
