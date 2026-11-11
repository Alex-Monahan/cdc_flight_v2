# Destination layer (package D)

The one module that talks to MotherDuck with a transaction: connections with deadlines, lanes with the fence
statement, the leader tables, the connection registry, the migrations of the `_cdc` schema, the transition
function for state machines, and alerts. It is built to contracts C0, C1, C4 and C5 of `docs/design/p3_plan.md`
and to section 4 of `docs/design/core.md`. This document describes the layer **as built**, the mapping of errors
with its evidence, the deadline mechanism, and every place where the contract was open or the design was wrong
(sections 13 and 14). Section 14.3 says what the fix round after the two reviews of this package changed.

| File | What |
|---|---|
| `src/cdc_v2/md/settings.py` | `Settings`, the fixed lane names `LANES` |
| `src/cdc_v2/md/connection.py` | `connect`, `Connection`: one worker thread per connection, deadlines, interrupt, replacement |
| `src/cdc_v2/md/lane.py` | `Lane`, `Transaction`, the fence statement, the lane crash points |
| `src/cdc_v2/md/errors.py` | the errors of C4, `StatementError`, the mapping of the client's exceptions onto them, and what a message may repeat of the client's text |
| `src/cdc_v2/md/registry.py` | `_cdc.connections`: register, interrupt the connections of earlier epochs, clean up |
| `src/cdc_v2/md/schema.py` | the leader tables (constant text), the migrations |
| `src/cdc_v2/state/` | `declare`, `transition`, `read`, `create`; `python -m cdc_v2.state` writes `docs/state_machines.md` |
| `src/cdc_v2/alerts.py` | `emit`, `emit_now`, `resolve` |
| `src/cdc_v2/testing/destination.py` | a destination for one test (local or MotherDuck), a small take-over (design 4.5) |
| `src/cdc_v2/testing/machines.py` | `check_machine` (every pair of states of a declared machine), the search for state writes |
| `tests/destination/` | the tests; `sut.py` is a small system under test, `connect_relay.py` a CONNECT relay; `test_from_apply.py` holds the tests of the seven points from the review of apply (section 14.1), `test_review.py` the seeded tests from the review of this package (section 14.2), `test_fix_round.py` and `test_md_fix_round.py` those of the fix round (section 14.3), with `review_cases.py` and `data/misleading_texts.json` (cases and texts of the second reviewer's programs), `fence_campaign.py` (the take-over at every point, many rounds) and `blackhole_child.py` (a lane on a black-holed path) |
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
interrupt_deadline_s=5.0, max_waiting_calls=2)` is a frozen dataclass. `max_waiting_calls` bounds, per
connection, the calls that it gave up and that have not returned (section 4).

- `database` is a plain MotherDuck database name. The layer opens it alone (`attach_mode=single`), so no other
  database of the account is attached. `:memory:` or `:memory:<name>` gives a local database (tier T0); a file
  path gives a local file. A connection string (`md:…`, a `?`) is refused: the token is never a setting; the
  client reads it from the environment itself.
- `marker` is sent as `custom_user_agent`.
- `md.connect(role, settings=None, deadline=None)` keeps the server connection id. It tries
  `connect_attempts` times. Errors: `ConnectionLost`, `DeadlineExceeded`. A connect that answers after its
  deadline is `DeadlineExceeded` too, and nothing stays open. Tests:
  `test_md.py::test_connect_keeps_the_server_id_and_the_session_is_utc`,
  `test_lane.py::test_connect_has_a_deadline`,
  `test_review.py::test_a_connect_that_answers_after_its_deadline_is_not_handed_out`.
- **Two settings on every client connection of the layer**, before its first statement, also on a connection
  that replaces one that was given up: `SET SESSION TimeZone = 'UTC'` and
  `SET SESSION arrow_lossless_conversion = true` (C4, from the review of apply). With the first, Arrow data
  with a `TIMESTAMPTZ` column needs no `pytz`. With the second, `UUID`, `JSON`, `TIMETZ` and `HUGEINT` keep
  their type in Arrow data (section 2.2). Both are set for the session only. A plain `SET` of the second one on
  one connection changes what the other connections of the process to the same database read; `SET SESSION`
  does not (local DuckDB 1.5.6:
  `test_from_apply.py::test_a_plain_set_of_the_lossless_setting_reaches_the_other_connections_of_the_process`).
  So another connection keeps its own values and cannot change those of the layer. Tests:
  `test_from_apply.py::test_every_connection_of_the_layer_has_the_two_settings`,
  `test_from_apply.py::test_the_two_settings_belong_to_the_connections_of_the_layer_only`; on MotherDuck
  `from_apply.json`, key `settings_scope`.
- A deadline is always a `cdc_v2.deadline.Deadline`. `None` means the default of the settings. A number of
  seconds is refused with `TypeError`, in every call of the layer
  (`test_lane.py::test_arguments_are_checked_and_a_failed_connect_hides_the_path`; the same test covers the
  refused database names and roles).

A `Connection` has `role`, `settings`, `generation` (counts the client connections it has used), `is_motherduck`,
`usable`, `server_id`, and:

| Method | What |
|---|---|
| `ensure(deadline)` | take a client connection now if there is none |
| `execute`, `fetchall`, `fetchone(sql, params=None, deadline=None)` | one statement **that only reads**, in autocommit; `execute` returns `None`. A statement that writes is `LaneMisuse` (fix round 2, N2): outside a lane transaction it would carry no fence, and after a take-over an old run's insert, its update of its fence row and of the leader row all committed (the second reviewer's case; `test_fix_round.py::test_an_old_runs_plain_connection_cannot_write_after_a_take_over`) |
| `clock_us(deadline=None)` | MotherDuck's clock as integer microseconds (section 10) |
| `interrupt_server_connection(server_id, deadline=None)` | interrupt another connection by id; refuses its own id |
| `abort()` | from any thread: end the call in flight now (section 4) |
| `close(deadline=None)` | close; also a context manager |

Errors of the autocommit methods: `Conflict` (nothing applied), `StatementError` for a statement that the
destination refused (section 5), `ConnectionLost` and `DeadlineExceeded`, and `LaneMisuse` for a statement that
writes. The layer's own statements that write in autocommit (the creation of the leader tables, the clean-up of
`_cdc.connections`, `md.schema.heartbeat`) give `CommitUnknown` when their outcome is unknown (a single statement
in autocommit is its own commit). Evidence: `deadlines.json`, key `long_statement_on_the_server`.

Every statement of a caller is checked (`check_sql`): exactly one statement; never `BEGIN`, `COMMIT` or
`ROLLBACK`; never a statement that deletes, inserts, truncates, merges into, drops, alters or re-creates
`_cdc.leader`, `_cdc.fence` or `_cdc.heartbeat`, and never `DROP SCHEMA _cdc`. A refused statement raises
`LaneMisuse` before anything is sent. Tests: `test_lane.py::test_misuse_is_refused`,
`test_lane.py::test_leader_rows_cannot_be_deleted_or_inserted`.

The check reads the statement with the parser of the client library and sends nothing. Text that the parser
refuses is `StatementError` with the kind `UNPARSED`. `md.statement_kind(sql)` gives the name the parser has for
a statement: `INSERT`, `UPDATE`, `DELETE`, `SELECT`, `CREATE`, `DROP`, `MERGE_INTO` and so on
(`test_from_apply.py::test_the_kind_of_a_statement_comes_from_the_parser_of_the_client`). A statement on a
scratch schema of apply (`_cdc_apply_<random>`, section 2.4) is not taken for one on the leader tables, also
when its table is named `leader`, `fence` or `heartbeat`
(`test_from_apply.py::test_a_statement_on_a_scratch_schema_is_not_taken_for_one_on_the_leader_tables`,
`test_from_apply.py::test_the_leader_tables_stay_protected_next_to_scratch_schemas`).

`md.open_connections()`, `md.background_threads()` and `md.wait_idle(deadline)` let a test check that nothing is
left (`test_lane.py::test_no_connection_and_no_thread_is_left`). `md.waiting_calls()` counts the calls of the
process that were given up and still wait for an answer; a supervisor ends the process when it passes its bound
(section 4).

- `open_connections()`: every client connection of the layer, from the moment the client returned it (also one
  whose connect never finished its first statement on the server), and the short-lived connections that
  interrupt a connection by id.
- `background_threads()`: watchdog, reaper and close threads, and the worker thread of every retired
  connection.
- `waiting_calls()`: a statement or a connect of a retired connection that has not returned, and an interrupt
  through a short-lived connection that has not returned. `Connection.waiting_calls()` counts those of one
  connection.

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
| `execute(sql, params=None, deadline=None)` | run one statement. Returns the number of rows an `INSERT`, `UPDATE` or `DELETE` changed, else `None` |
| `execute_one(sql, params=None, deadline=None)` | the statement must change exactly one row. Any other count rolls back and raises `Conflict`. A statement that reports no count (a `SELECT`, a statement with `RETURNING`) rolls back and is `LaneMisuse` |
| `fetchall(sql, params=None, deadline=None)`, `fetchone` | read rows: a list of tuples, or one tuple or `None` |
| `fetch_arrow(sql, params=None, deadline=None)` | read a result as a `pyarrow.Table` that is complete in memory when the call returns |
| `clock_us(deadline=None)` | MotherDuck's clock inside the transaction |
| `register(name, data, deadline=None)` | make a `pyarrow.Table`, a record batch or a list of record batches readable under `name` until the block ends |
| `fail(error)` | roll back now and remember `error`; leaving the block then raises it even if the caller swallowed it |

Rules that the object enforces (`LaneMisuse` otherwise; `test_lane.py::test_misuse_is_refused`): one block per
`Transaction` object; no nested block on one lane; only the thread that opened the block uses it; no use after
the block. An error that the caller catches inside the block and does not raise again is raised when the block
ends: a failed transaction cannot end quietly (`test_lane.py::test_a_failed_transaction_cannot_end_quietly`).

The deadline of one statement is the smaller of `statement_deadline_s`, the statement's own `deadline` and what
is left of the block's deadline.

`execute`, `fetchall`, `fetch_arrow` and `register` have the shape that contract C4 got from the review of
apply (section 14.1). The tests named here without a file are in `tests/destination/test_from_apply.py`.

- **Counts.** `execute` returns a count only for a statement that the client's parser calls `INSERT`, `UPDATE`
  or `DELETE` and that has no `RETURNING` clause. `TRUNCATE` is a `DELETE` to the parser and returns the number
  of rows it removed. `MERGE INTO`, `CREATE TABLE … AS`, `COPY` and every statement with `RETURNING` return
  `None` (`COPY` by its kind only; no test of this package runs one). The rows of `RETURNING` are read with `fetchall`. The layer looks for the `RETURNING` clause itself,
  in the tokens of the client's tokenizer, because the client describes the result of `… RETURNING k AS "Count"`
  like a count (finding 19). Two texts are taken for a `RETURNING` clause that are none: a column label
  `AS returning` and a parameter named `$returning`. They give `None` where a count was possible, never a wrong
  number. A statement with `RETURNING` is not read as a count at all: `… RETURNING b AS "Count"` with a text
  value was a `StatementError(ValueError)` and rolled the transaction back (fix round, F8;
  `test_fix_round.py::test_a_returning_column_named_count_that_holds_text_is_no_count`). Tests: `test_execute_returns_a_count_for_insert_update_and_delete_only`,
  `test_merge_reports_no_count_and_truncate_counts_as_a_delete`,
  `test_a_returning_clause_is_found_by_the_tokenizer_of_the_client`,
  `test_execute_one_never_takes_a_returned_value_for_a_count`.
- **`fetch_arrow`** reads the whole result (`to_arrow_table()`). The client's `.arrow()` gives a lazy reader in
  DuckDB 1.5.6; C4 says that registering such a reader on the connection that produced it and querying it
  deadlocks (measured by package A, not measured again here). The layer never hands the reader out
  (`test_static.py::test_the_layer_never_hands_out_the_lazy_reader_of_the_client`,
  `test_fetch_arrow_gives_a_table_that_the_same_transaction_can_register`).
- **`register`** takes a `pyarrow.Table`, a `pyarrow.RecordBatch`, or a list or tuple of record batches with one
  schema. A `RecordBatchReader`, an empty list and anything else are `LaneMisuse`. The name must match
  `^[a-z_][a-z0-9_]{0,62}$` from its first to its last character, else `LaneMisuse`. The name is removed when
  the block ends, also when it ends by an error or a deadline; when the connection was given up, the name went
  with it. Data that the client cannot register is `StatementError` with the kind `REGISTER`. Tests:
  `test_register_takes_a_table_a_record_batch_or_a_list_of_batches`, `test_a_registered_name_is_checked`,
  `test_a_registered_name_is_gone_when_the_block_ends_however_it_ends`,
  `test_a_registered_name_is_gone_after_a_deadline_gave_the_connection_up`,
  `test_a_value_in_local_data_that_cannot_be_registered_does_not_travel_on`.
- **Types in Arrow data.** With the two settings of section 2.1, a table that `fetch_arrow` read and `register`
  made readable again holds the same values and types: `UUID`, `JSON`, `TIME WITH TIME ZONE`,
  `TIMESTAMP WITH TIME ZONE`, `HUGEINT`. On a connection without `arrow_lossless_conversion` the first two arrive
  as `string`, `TIMETZ` as `time64[us]` without its offset and `HUGEINT` as `decimal128(38, 0)`
  (`from_apply.json`, key `arrow_types`; `test_arrow_data_keeps_uuid_json_and_time_zones`).

`md.crash_points(lane)` gives the three crash points of a lane. `md.clock_sql` is the expression of design 4.1.
`md.FENCE_SQL` is the fence statement.

### 2.3 Registry

`cdc_v2.md.registry` keeps `_cdc.connections (epoch, lane, connection_id, registered_at)`.

- `register(conn, lane, epoch, deadline=None, retry_conflict=False)` returns the server connection id. The lane
  calls it itself before the first `BEGIN` on every new client connection, with the statement deadline. It is a
  transaction of its own (fix round, item 2):

  ```sql
  BEGIN;
  INSERT INTO _cdc.connections (epoch, lane, connection_id, registered_at) VALUES (?, ?, ?::UUID, <md clock>);
  UPDATE _cdc.fence SET n = n + 1 WHERE lane = ? AND epoch = ?;          -- exactly one row, else FenceLost
  COMMIT;
  ```

  The `UPDATE` writes the counter `n` with a changed value, and a take-over writes `n` too (section 14.4), so a
  registration and a take-over cannot both commit. The counter only grows: until fix round 2 the registration
  put it back (`n - 1`) in the same transaction, a net-zero write that no documented DuckDB rule keeps
  recorded (`test_fix_round.py::test_the_take_over_and_the_registration_never_put_the_counter_back`): a registration whose transaction began before the take-over committed gets `Conflict`, one that
  begins after it gets `FenceLost`, and one that commits first is in the list when the new leader interrupts
  the earlier epochs. Until the fix round the registration was one `INSERT … SELECT … FROM _cdc.leader WHERE
  epoch = ?` in autocommit: it read the leader row and wrote only `_cdc.connections`, so on MotherDuck one that
  read the old epoch could commit after the take-over and after the new leader's interrupt had read the list.
  Tests: `test_lane.py::test_new_connection_of_a_stale_epoch_is_refused_at_registration`,
  `test_fix_round.py::test_a_registration_while_a_take_over_is_open_does_not_commit`,
  `test_fix_round.py::test_a_registration_frozen_between_its_check_and_its_commit_and_a_take_over_in_between`
  (both also on MotherDuck). The statements run on the client connection whose id they write. A connection
  that is replaced while it registers is `ConnectionLost`, and so is a lost answer of its `COMMIT`; the
  connection is then given up, so the next call registers a new one
  (`test_review.py::test_registration_writes_the_id_of_the_connection_that_runs_it`). The `COMMIT` passes the
  fault sites `md.commit` and `md.commit_reply` like every `COMMIT`.
- The first form of the fix round wrote `SET epoch = epoch`. A write of an unchanged value is not recorded
  after a checkpoint (section 14.4), so that form still let a registration commit after a take-over
  (`test_fix_round.py::test_a_registration_right_after_a_checkpoint_cannot_commit_after_a_take_over`, with the
  negative control).
- A registration holds the fence row of its lane until it commits. A take-over meets it as a conflict and
  cannot interrupt it (its row is not in the list yet); a registration that is frozen there delays a take-over
  until MotherDuck ends its transaction (about 29 s, section 8.3). And a registration that commits while a
  transaction of the same lane name is open makes that transaction's fence statement conflict (`Conflict`).
  Neither happens to a lane, which registers before its own transactions.
- `md.interrupt_earlier(conn, epoch, deadline=None)` returns `(listed, interrupted)`: the rows with an epoch
  **below** `epoch`, and how many of them the server interrupted. The caller's own connection is left out.
- `registry.cleanup(conn_or_tx, epoch, deadline=None)` deletes the rows of epochs before `epoch - 1`.

A connection that writes for an epoch without being a lane (the heartbeat connection of package L) calls
`register(conn, "control", epoch, retry_conflict=True)` itself
(`test_lane.py::test_a_connection_that_is_not_a_lane_registers_under_a_lane_name`). Its registration writes the
fence row of `control`, which a transaction of the control lane holds between its fence statement and its
`COMMIT`: that rare `Conflict` it tries again until its deadline
(`test_fix_round.py::test_a_connection_that_is_not_a_lane_registers_under_control_and_retries_a_conflict`). Its
registration can also make an open transaction of the control lane end with `Conflict` (the bullet above); a
control-lane caller runs it again, as the migrations do.

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

One exception (C4, from the review of apply): a **scratch schema** named `_cdc_apply_<random>`. Apply creates
it and drops it inside one lane transaction, so it never exists outside that transaction. The check of the
leader tables, their initialisation and the migrations look at the schema `_cdc` only: they report nothing
about a scratch schema and do not touch it, and no code of this package names or drops one
(`test_from_apply.py::test_the_schema_check_and_the_migrations_leave_a_scratch_schema_alone`,
`test_static.py::test_nothing_of_this_package_drops_a_schema_or_names_a_scratch_schema_of_apply`). Whoever
drops what a transaction created drops the tables one by one and then the schema: `DROP SCHEMA … CASCADE` of
tables made in the same transaction fails at `COMMIT` (finding 20;
`test_from_apply.py::test_a_scratch_schema_of_apply_lives_inside_one_transaction`). No migration step of this
package drops anything.

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
  `_cdc.schema_version (package, version, step, applied, epoch, updated_at)`. The move follows the run-again rule
  (`execute_one` with a condition on the version read). So a step is applied exactly once, also when two leaders
  migrate at once (`test_schema.py::test_leaders_at_once_apply_every_step_exactly_once`) and when the answer
  of a `COMMIT` is lost (`test_schema.py::test_a_step_whose_commit_has_no_answer_is_applied_once`: after
  `CommitUnknown` it waits 2.5 s and reads again).
- A database that is newer than the code, or whose applied steps differ from the first steps of the registered
  list, is refused with `SchemaError` before anything runs
  (`test_schema.py::test_a_database_that_does_not_fit_the_code_is_refused`). The version row keeps, in the column
  `applied`, the name and a hash (SHA-256 of the name and the statements, `schema.step_hash`) of every applied
  step, and `migrate` compares all of them: a step changed under its name, a renamed, removed or inserted step
  are refused (fix round, F5; until then only the last step's name was compared;
  `test_fix_round.py::test_a_changed_or_renamed_applied_step_refuses_to_start`). `schema.applied_steps(conn,
  package)` reads them.
- The lists `state` and `alerts` of this package run first, the others in the order of their package names.

### 2.5 State machines (contract C5)

```python
from cdc_v2 import state

CHUNK = state.declare(state.Machine(
    name="chunk", states=("planned", "loading", "done"),
    transitions={("planned", "loading"), ("loading", "done"), ("loading", "planned")},
    table="_cdc.chunks", key_columns=("table_id", "chunk_no"), state_column="state",
    creators=tuple("backfill.%d" % i for i in range(16)),
    initial=("planned",), description="one chunk of a backfill"))

state.create(tx, CHUNK, (table_id, 0), "planned", "plan", values={"created_at": state.NOW})
state.transition(tx, CHUNK, (table_id, 0), "planned", "loading", "loader 3 took it",
                 values={"rows": 0}, expect={"loader": None}, detail={"loader": 3})
state.read(conn_or_tx, CHUNK, (table_id, 0))        # the stored state, or None
```

- `transition` and `create` write only inside an `md.Transaction`; anything else (a plain `Connection`) is
  `LaneMisuse`, and so for `alerts.emit`. After a take-over the old run had changed a state and created a row
  through a plain connection, without the fence, and the row of `_cdc.transitions` had no epoch (fix round,
  F4; `test_fix_round.py::test_the_state_module_and_alerts_write_only_inside_a_lane_transaction`). `read` still
  takes a connection.
- **Who creates the keys of a machine** (`creators`, required): lane names from the fixed set, or
  `state.FRESH_IDS` for keys that are new random ids (alerts, requests) and that any lane may create. `create`
  refuses any other lane with `LaneMisuse` before it touches the database. A machine whose rows are created
  from several lanes (the chunks of a backfill, from `backfill.<i>`) creates each child row together with a
  transition of its parent row (the backfill's `planned` → `reading`), which every planner writes; two planners
  then conflict on the parent's state column and only one commits (fix round, item 3;
  `test_state.py::test_a_key_is_created_only_by_the_lanes_that_the_machine_names`,
  `test_state.py::test_a_child_row_created_from_two_lanes_is_created_with_a_transition_of_its_parent`).
- **The facts of a group** (fix round, item 1): `transition(…, cause, *, detail=None, values=None,
  expect=None)` and `create(…, values=None, detail=None)`. `values` sets more columns in the same conditional
  `UPDATE` (`state.NOW` for MotherDuck's clock). `expect` adds `AND col IS NOT DISTINCT FROM ?` for each column,
  and every expected column must be in `values` with its new value; a transition from a state to itself needs
  `values` too. Both are refused with `LaneMisuse` otherwise (fix round 2, items 2 and 6): a write of an
  unchanged value is not recorded after a checkpoint (section 14.4), so neither excluded a concurrent writer
  (the second reviewer's N3: both committed, 5 of 5 rounds each, across two lanes of one epoch;
  `test_fix_round.py::test_a_condition_on_a_column_the_transition_does_not_change_is_refused`). The caller
  puts into `values` a column whose value it changes (the stream's position always changes). With `expect` a
  count of 0 rolls the transaction back and raises `Conflict` (the run-again rule of C4). `detail` follows the value rules of alert fields (`state.check_fields`; identifiers, positions,
  counts and durations only) and is stored as JSON. The stream's group is then two statements: the `UPDATE` of
  its position with `expect` on the previous value, and the `INSERT` of the log row
  (`test_fix_round.py::test_values_and_expect_in_the_same_conditional_update`).

- `transition` refuses a pair that is not declared **before** it touches the database
  (`IllegalTransition`, `declared=False`).
- It changes the row with a condition on `from_state`. No row changed: `IllegalTransition` with the state that
  is stored; the caller's transaction stays usable. More than one row for a key: `StateError`, and the
  transaction fails.
- Every transition and every `create` appends to `_cdc.transitions (machine, key, from_state, to_state, cause,
  epoch, lane, seq, detail, changed_at)` in the same transaction
  (`test_state.py::test_a_transition_is_part_of_the_callers_transaction`). `seq` (`UBIGINT`) is written in the
  same `INSERT` as `(SELECT n + 1 FROM _cdc.fence WHERE lane = ?)`: the lane's fence counter after this
  transaction's fence statement, which is the last statement before `COMMIT`. Two transactions of one lane that
  read the same `n` conflict at the fence row, so `seq` orders the transactions of a lane exactly as they
  committed, and a duplicate (lane, `seq`) between two transactions shows a second writer. Every row of one
  transaction carries the same `seq`. `seq` is **strictly increasing per lane, with gaps**: a take-over and the
  registration of a new connection raise `n` too (the counter only grows, fix round 2), and so does a
  transaction that writes no row
  (`test_fix_round.py::test_a_transition_row_carries_its_lane_epoch_seq_and_detail`,
  `::test_seq_is_strictly_increasing_per_lane_with_gaps_only_at_take_overs_and_registrations`,
  `::test_a_rolled_back_transaction_leaves_no_transition_row`,
  `::test_a_run_that_bypasses_the_fence_shows_a_duplicate_seq`).
- **`changed_at` does not give the order of commit.** It is MotherDuck's clock when the `INSERT` ran, not when
  the transaction committed, in whole milliseconds (section 10). Two writers can run their statements in one
  order and commit in the other, and rows of one millisecond have equal times. Order by (lane, `seq`).
- Two transactions cannot both make one transition
  (`test_state.py::test_two_transactions_cannot_both_make_one_transition`).
- The log line `state transition` (logger `cdc_v2.state`) is written inside the caller's open transaction. Its
  record says so: `committed: False`. `_cdc.transitions` is the record of what holds
  (`test_review.py::test_a_log_line_written_inside_a_transaction_says_that_nothing_is_committed_yet`).
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
  (`test_alerts.py::test_a_value_that_is_not_an_identifier_position_count_or_duration_is_refused`). The rules are
  `state.check_fields`, which the detail of a transition follows too; `AlertError` is a `state.FieldError`.
- The machine `alert` has fresh ids (`creators=state.FRESH_IDS`): every lane records alerts. `emit` takes an
  `md.Transaction` only (section 2.5).
- `key` names the fields that, with the kind, tell one condition from another. `dedup_key` is stored so that
  the deliverer can send one message per condition.
- `emit_now` tries again after `Conflict` and `ConnectionLost` until its deadline. It passes on `FenceLost` and
  `CommitUnknown` (`test_alerts.py::test_emit_now_passes_on_what_it_cannot_decide`).
- Before the migrations ran the table does not exist: an emit fails with `md.StatementError` and writes nothing
  (`test_alerts.py::test_an_alert_before_the_migrations_ran_fails_and_writes_nothing`).
- The log line `alert` (logger `cdc_v2.alerts`) is written inside the open transaction and carries
  `committed: False`, like the line of a state transition (section 2.5).

### 2.7 Test support

`cdc_v2.testing.destination`:

- `local_destination()` and `motherduck_destination(prefix)` give a private destination with the leader
  tables and remove it at the end. `Destination.lead()` takes over, migrates and returns the epoch.
- `take_over(settings, …)` is a stand-in for the bootstrap of package L. It follows design 4.5: in one
  transaction it raises the epoch in the leader row, in every fence row and in the heartbeat row. It decides
  once from which epoch it takes over (`seen`, read at its first attempt). After a conflict it reads the leader
  row again: if the row moved past `seen` under another run's id, another starter won, and it raises
  `AnotherRunLeads` (a `TakeoverFailed`; design 4.5: the losers re-read and exit). Otherwise it interrupts the
  connections of earlier epochs and tries again until its time is over (`TakeoverFailed`). After an unknown
  commit outcome it waits 2.5 s, reads the record and decides by `run_id`. Until the fix round a loser re-read
  `seen` at every attempt and took over again from the winner: with three simultaneous starters all three were
  returned as leader, 5 rounds of 5 (F6; `test_fix_round.py::test_three_simultaneous_starters_have_exactly_one_leader`,
  also on MotherDuck). Package L copies this stand-in.
- `Destination.run(sql, params=None)` sets up and inspects the test's database on a plain client connection,
  outside the layer (whose connections take reads only outside a lane transaction since fix round 2).
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
| 1 | On a new client connection: connect and set the two session settings; interrupt the server connection it replaces, by id; register in `_cdc.connections` (a transaction of its own: `BEGIN`, the `INSERT`, the `UPDATE` of the lane's fence row, `COMMIT`; section 2.3) | `md.connect`, `md.execute`, `md.commit`, `md.commit_reply` | |
| 2 | `BEGIN` | `md.execute` | |
| 3 | The caller's statements | `md.execute` | |
| 4 | The fence statement; it must change exactly one row. Its deadline is the statement deadline (and never more than what is left of the block) | `md.fence` | `<lane>.fenced` after it |
| 5 | The block's deadline is checked one last time | | `<lane>.commit_sent` after it |
| 6 | `COMMIT` | `md.commit` before it is sent, `md.commit_reply` after the destination has executed it | `<lane>.committed` after it returned |

- An empty block still runs steps 4 to 6 (`test_lane.py::test_empty_transaction_still_runs_the_fence`).
- An exception that the caller's own code raises in step 3 rolls back and is passed on. A statement that the
  destination refuses rolls back and raises `StatementError` (section 5). Both:
  `test_lane.py::test_an_exception_rolls_back_and_is_passed_on`.
- What the fault site `md.execute` sees before the caller's statements. On a lane's first connection: the
  registration's `BEGIN`, `INSERT` and `UPDATE` (its `COMMIT` passes `md.commit`), then `BEGIN`. On a connection
  that replaces one that was given up, locally the same; on MotherDuck the interrupt of the old id comes first,
  as a statement of its own. On a registered connection: `BEGIN` only. (Locally the interrupt is a call inside
  the process and passes no fault site.)
- The clean-up after a step that failed has one short deadline (`rollback_deadline_s`) for all of it: the
  roll-back, the read of the fence row after a conflict in the fence statement, and the removal of registered
  data. A roll-back that does not answer retires the connection; the caller still gets its own exception
  (`deadlines.json`, key `roll_back_blocked`: after 3.17 s with a roll-back deadline of 1.0 s and a grace of
  2.0 s; `test_review.py::test_the_clean_up_after_a_failed_step_has_one_time_budget`).
- Every lane has its three crash points, and each is reached once per transaction
  (`test_lane.py::test_every_lane_has_its_three_crash_points`).

## 4. Deadlines

**Rule.** No statement of this layer holds its caller longer than its deadline plus the grace period (`grace_s`,
default 2 s). A lane call that fails adds its clean-up: it returns at the latest `rollback_deadline_s` (default
5 s) plus the grace period after the step that failed (section 3). The deadline of a statement, of the fence
statement and of the registration is the statement deadline (`statement_deadline_s`, default 60 s), and never
more than what is left of the block; only `COMMIT` has what is left of the block, because a commit has no
progress signal and the caller sets the block's deadline by the size of the group (design 3). Until the fix
round the fence statement and the registration waited for what was left of the block, up to 300 s with the
defaults, above the 120 s of SPEC 4.5 (F7;
`test_fix_round.py::test_the_fence_statement_and_the_registration_wait_the_statement_deadline_not_the_blocks`).

**The worst case with the defaults**, on a network that answers nothing, for one call of a lane: connect 30 s
(`connect_deadline_s`) plus 2 s; the interrupt of an old id 5 s plus 2 s; the registration 60 s plus 2 s; each
statement and the fence statement 60 s plus 2 s; `COMMIT` what is left of the block (default 300 s) plus 2 s.
A connect that the destination refuses is tried `connect_attempts` (3) times. Once a connection holds
`max_waiting_calls` (2) calls that it gave up, its next call fails at once with `ConnectionLost` and starts
nothing. Measured with short settings on a black-holed path (section 9.4).

**Calls that were given up.** The worker thread, the reaper thread and the client connection of a retired
connection stay until the statement in flight returns; the client library offers no way to end them earlier
(the next paragraph). On a network that answers nothing that is until the network answers again (section 9.4:
all were freed within seconds of the path passing again). So they are counted and bounded (fix round, items 6
and 15):

- `md.waiting_calls()` counts them (the statement or connect of a retired connection, and an interrupt through a
  short-lived connection, that have not returned). A supervisor that sees more than its bound ends the process
  (package L, contract C11); the fuzzers measure it.
- `md.background_threads()` counts every worker of a retired connection and every helper thread;
  `md.open_connections()` every client connection of the layer, from the moment the client returned it, and the
  short-lived connections that interrupt. Until the fix round a connect that never finished its first statement
  and the workers were not counted (the second review measured 1 and 13 where 26 threads were left).
- Per connection at most `max_waiting_calls` (default 2) such calls: the call that would need one more fails at
  once with `ConnectionLost` (`… calls that were given up still wait for the destination`) and starts nothing.
  Per such call at most three threads stay (worker, watchdog, reaper) and at most one interrupt through a
  short-lived connection is in flight. Until the fix round every attempt on a black-holed path left two more
  threads, without a bound (2 to 26 after 12 attempts). Test:
  `test_fix_round.py::test_the_calls_that_were_given_up_are_counted_and_capped_per_connection`.

What the rule does not cover:
- **A process that cannot start a thread.** The watchdog thread is started at the deadline. When that fails,
  the failure is logged (`md background thread not started`), the caller waits to the end of the grace period,
  the connection is given up and the block fails
  (`test_review.py::test_a_deadline_ends_the_transaction_also_when_no_watchdog_thread_can_start`).
- **A wait that is broken off** by an exception in the caller's thread (`KeyboardInterrupt`) gives the
  connection up and fails the transaction before the exception is passed on. Nothing is committed after it, also
  when the caller swallows it
  (`test_review.py::test_a_wait_that_is_broken_off_never_leaves_a_transaction_that_commits`).

**Why a mechanism is needed.** The client library has no time limit for a statement. Its own `interrupt()` does
not end a statement that runs on MotherDuck: an `UPDATE` that sleeps 4 s on the server and is interrupted by the
client after 0.8 s returned after 4.16 s (`error_mapping.json`, key `raw_texts`, case
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
   a fence row (C4). An interrupt that the destination refuses fails that one call with `StatementError` (kind
   `INTERRUPT`) and the id is dropped, so that it cannot block the lane for ever; an interrupt that is lost or
   late is tried again at the next call
   (`test_review.py::test_an_old_connection_id_that_the_destination_refuses_does_not_block_the_lane_for_ever`).
   Test: `test_lane.py::test_a_replaced_connection_is_interrupted_by_id_before_the_new_one_is_used`,
   `test_lane.py::test_the_interrupt_of_a_replaced_connection_runs_on_the_new_connection`; on MotherDuck
   `registry_takeover.json`, key `replaced_connection_interrupted_by_id` (the interrupt statement took 76 to 87 ms;
   the next transaction with connect, interrupt and registration took 488 ms on `88e0cf1` and 777 ms on
   `7193b39`, where the registration is a transaction of its own of three statements).

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
| the same as a read outside a transaction | 1.0 s | 1.19 to 1.21 s | `DeadlineExceeded` | no |
| the same as a write outside a transaction | | refused before it is sent since fix round 2 (until then 1.21 to 1.22 s, `CommitUnknown`) | `LaneMisuse` | no |

After the statement that slept on the server was ended, another lane updated the same row 334 ms later: the
interrupt on the server released the row. After each case the next transaction of the lane ran on a new
connection after 475 to 509 ms. The same cases run at T0 with shorter times
(`test_lane.py::test_a_blocked_step_never_holds_the_caller_beyond_deadline_plus_grace`,
`test_lane.py::test_a_long_query_is_interrupted_at_its_deadline`).

A block whose deadline has passed when it ends is rolled back before `COMMIT`: `DeadlineExceeded`, nothing
committed (`test_lane.py::test_an_expired_deadline_stops_the_block_before_commit`, and the same test in
`test_md.py`).

## 5. Error mapping

The rules, in the order they are applied (`src/cdc_v2/md/errors.py`; changed in the fix round, F1 to F3):

1. A deadline that passed decides first: before `COMMIT` `DeadlineExceeded`, from `COMMIT` on `CommitUnknown`.
2. The **class** decides for three families, whatever the text says. An error about the data of a statement
   (the client's `DataError` and `IntegrityError`) and an error of DuckDB's programming family
   (`ProgrammingError`: parser, syntax, binder, catalog, invalid input, invalid type) is rule 5: their text
   repeats values and names of the statement without quotation marks. The client's `ConnectionException` and
   `InterruptException` and Python's `ConnectionError`, `TimeoutError` and `OSError` are **transport** errors.
3. A **transport** error by its text: any other client error whose head holds one of the phrases `Connection
   Error`, `interrupted by user`, `lease expired`, `transaction already rolled back`, `Request failed`, `Could
   not connect`, `Connection already closed`, `No open result set`, `connection … closed`, `Service
   Unavailable`, `Bad Gateway`, `Gateway Timeout` (whole words, any case), or the gRPC codes `UNAVAILABLE` and
   `DEADLINE_EXCEEDED` (whole words, capitals only). The phrases are needed because MotherDuck reports a server
   interrupt in a transaction as `TransactionException`, not as a connection error. An `IOException` or
   `HTTPException` is a transport error by this rule only: a file that does not exist is an `IOException` too.
4. A **conflict**: any other client error whose head says `Conflict on …`, `write-write conflict` or
   `Transaction conflict` (DDL on a table while another transaction writes it).
5. Everything else that a statement raises is `md.StatementError` before `COMMIT` was sent, and `CommitUnknown`
   from `COMMIT` on. Inside a lane transaction the transaction was rolled back. A statement in autocommit was
   refused as a whole. An exception of the caller's own code inside a block is no error of a statement: it is
   passed on unchanged after the roll-back.

What the fix round changed, from the second review (seeded tests in `test_errors.py` and `test_fix_round.py`,
the texts in `tests/destination/data/misleading_texts.json`):

- **F1.** `Transaction conflict: attempting to insert into/update table … altered by a different transaction`
  was a `StatementError` (8 of 40 two-lane shapes: `ADD COLUMN`, `ALTER TYPE`, `DROP` and the swap of SPEC 3.2
  on one lane while another lane writes; 7 of the 8 succeed when run again). The stream counts a
  `StatementError` as a failure of the table, so tables were quarantined for nothing. It is `Conflict` now,
  also as the answer of `COMMIT` (`Failed to commit: … Transaction conflict:`). After a `DROP` the next attempt
  meets `CatalogException`, a `StatementError`
  (`test_fix_round.py::test_ddl_on_one_lane_while_another_lane_writes_is_a_conflict_and_runs_again`, also on
  MotherDuck).
- **F2.** The transport rule was case-insensitive and had no word boundaries:
  `Catalog Error: Table with name stock_unavailable does not exist!` was `ConnectionLost` and was retried for
  ever; 48 of 168 MotherDuck texts of the second review were sorted wrongly, all of them `CatalogException`
  and `InvalidInputException`. Now the programming family is decided by its class and the gRPC codes count in
  capitals as whole words; all 168 texts are seeded tests
  (`test_errors.py::test_every_misleading_text_that_motherduck_gave_is_refused_not_retried`; the 168 statements
  locally and through the layer: `test_errors.py::test_the_misleading_statements_on_local_duckdb_are_refused_not_retried`,
  `test_fix_round.py::test_the_misleading_statements_through_the_layer_are_refused`, also on MotherDuck).
- **F3.** `IOException` was a transport error by its class: a missing file gave `ConnectionLost` 5 of 5 times
  with a new connection each time, and an autocommit `INSERT` from it `CommitUnknown` with nothing applied. Now
  an IO or HTTP error is a transport error only when a transport phrase decides
  (`test_fix_round.py::test_a_missing_file_is_refused_and_not_a_lost_connection`, also on MotherDuck).

**`md.StatementError(where, error_class, statement)`** is a `DestinationError`. Its message is
`<where>: the destination refused a statement (<error_class> in <statement>)`, for example
`lane stream: the destination refused a statement (ConversionException in INSERT)`. `error_class` is the name
of the client's exception class. `statement` is the kind of the statement: the name the client's parser gives
it, or `UNPARSED` (the parser refused the text), `BEGIN`, `FENCE` (the lane's own statements), `REGISTER` (data
handed to `register`), `INTERRUPT` (the interrupt of an old connection id). Both come from fixed lists. Running
the same statement again gives the same answer, so a caller does not try again; it reads the two attributes and
decides (section 15).

**What a message keeps of the client's text** (SPEC 8.3; C4, from the review of apply). The client names the
value it could not use: with quotation marks (`Could not convert string '…'`, `duplicate key "…"`), without
them (`Type INT64 with value 3000000000 can't be cast …`), or as a piece of the SQL text (a parser error, a
table name in a catalog error). On MotherDuck 7 of 7 planted values came back in the client's text
(`from_apply.json`, key `planted_values`). So no rule about the text makes it safe to repeat, and the errors of
this layer repeat none of it:

- `StatementError` holds the class and the kind of statement.
- `ConnectionLost` and `CommitUnknown` hold a **label**: the class name and, for a client error, the transport
  phrase that decided, taken from the list of rule 2 (`errors.label`; for example
  `TransactionException (lease expired)`).
- `Conflict` and `FenceLost` hold the lane and the step.
- The client's exception is not attached (no `__cause__`, no `__context__`): a traceback in a log would print
  its whole text.
- The raw text is never logged and never written to an alert or to `_cdc`.
- One exception: a failed **connect**. Nothing was sent, so its text holds no value of a row, and it says why
  the connect failed. It is kept as the class name and the first line, with tokens, paths, host names and
  addresses replaced (`errors.safe_text`).

Tests: `test_from_apply.py::test_a_value_in_a_destination_error_never_travels_on` plants a marker text and a
number in seven statements (conversion, constraint, parser and catalog errors), inside a lane transaction and
in autocommit. It looks for them in the message, `repr`, arguments, attributes and formatted traceback of the
exception, in every captured log line, in the alert a caller writes from the error, and in every row of every
`_cdc` table. Also `test_review.py::test_a_key_value_that_commit_reports_does_not_travel_on`,
`test_errors.py::test_a_label_is_a_class_name_and_at_most_a_phrase_from_the_list`,
`test_errors.py::test_refused_names_the_class_and_the_kind_of_statement_and_nothing_else`,
`test_errors.py::test_a_message_never_holds_a_token_a_path_a_host_or_an_address`,
`test_errors.py::test_a_message_keeps_what_a_reader_needs`.

**Where a text decides (rules 2 and 3), only its head does**: the first line up to the first quotation mark.
The client quotes most values it repeats, so a quoted value that holds one of the phrases decides nothing
(`test_errors.py::test_the_head_of_a_text_ends_at_the_first_quotation_mark`). And the text decides nothing at
all for an error about the data of a statement (the client's `DataError` and `IntegrityError` families: a
conversion error, a constraint error). Found while this document was written:
`Could not convert string 'Conflict on update' to INT32` was sorted as a conflict, and a value with
`Connection Error` in it as a lost connection; a caller answers both by running the transaction again. Seeded
regression tests:
`test_errors.py::test_a_value_that_holds_a_phrase_does_not_look_like_a_conflict_or_a_lost_connection` and the
table in `test_errors.py::test_how_an_exception_is_sorted`. The review of this package found the same for a
`TransactionException` whose text quotes a key value
(`Failed to commit: PRIMARY KEY or UNIQUE constraint violation: duplicate key "…"`); the head rule is the answer
to it. Every text that MotherDuck gave in the runs of this package has its phrase in the head
(`test_errors.py::test_no_text_that_motherduck_gave_has_its_phrase_only_inside_a_quoted_value`).

Limit of the head rule: a value that the client repeats **without** quotation marks, in an error that is
neither about data nor of the programming family, is part of the head and can decide. No such text was seen; the
catalog and binder errors, which repeat names without quotation marks, are decided by their class since the fix
round (`test_errors.py::test_the_known_limit_of_the_head_rule`). The caller would run the transaction again and
meet the same error: a loop of retries that the caller's limit ends, not wrong data.

| Where | Transport error | Conflict | Other error | Deadline |
|---|---|---|---|---|
| connect | `ConnectionLost` after the last attempt | | every other error of the client is `ConnectionLost` too | `DeadlineExceeded` |
| the interrupt of an old connection id | `ConnectionLost`; tried again at the next call | | `StatementError`, once; the id is dropped | `DeadlineExceeded`; tried again at the next call |
| registration (a transaction of its own, section 2.3) | `ConnectionLost`, also when the outcome of its `COMMIT` is unknown | `Conflict` (a take-over is open, or committed after it began) | `StatementError` | `DeadlineExceeded` |
| registration: the fence row holds another epoch | `FenceLost` | | | |
| `BEGIN`, a statement | `ConnectionLost`; the session is dropped | `Conflict` after roll-back | `StatementError` after roll-back | `DeadlineExceeded` |
| the fence statement | `ConnectionLost` | `FenceLost` if the fence row holds another epoch, else `Conflict` | `StatementError` after roll-back | `DeadlineExceeded` |
| the fence statement changed no row | `FenceLost` after roll-back | | | |
| `COMMIT` | `CommitUnknown` | `Conflict` (the destination refused the commit; nothing applied) | `CommitUnknown` | `CommitUnknown` |

The `Conflict` of the row `COMMIT` was never seen for real: no run on MotherDuck or on local DuckDB answered a
`COMMIT` with a write-write conflict. The rule is tested with a fault site only
(`test_lane.py::test_a_conflict_that_commit_reports_is_conflict`).

**Evidence on MotherDuck** (`error_mapping.json`; tests in `test_md.py`). The second column is the label in
the layer's message.

| Case | Class of the client's error and the phrase that decided | Raised | Committed |
|---|---|---|---|
| a second lane updates a row that an open transaction has updated | `TransactionException`; the text says `Conflict on update!` | `Conflict`; the connection is kept | no |
| a statement in flight is interrupted on the server | `ConnectionException (Connection Error)`, 100 ms after the interrupt | `ConnectionLost` | no |
| an open transaction is interrupted on the server, then a statement | `ConnectionException (Connection Error)`; in an earlier run `TransactionException` with `transaction already rolled back` (finding 18) | `ConnectionLost` | no |
| the same, then the block ends (fence statement) | the same two | `ConnectionLost` | no |
| the connection is interrupted between the fence statement and `COMMIT` | `TransactionException (Connection Error)`; the text is `… Failed to commit: MotherDuck transaction statement failed: Connection Error: interrupted by user` | `CommitUnknown` | no |
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
each, locally and on MotherDuck (same file, rows `n_and_epoch_parameter (as built)`). **That was not enough**:
right after a checkpoint the `epoch` that the fence statement writes is an unchanged value, which is not
recorded, and a take-over that writes the epoch alone commits next to the old transaction (F0, section 14.4).
Since the fix round the take-over also writes the counter `n` of every fence row (`n + 1`, never put back),
which the fence statement changes; the two meet on `n` in every order. The seeded regression
test is `test_lane.py::test_conflicts_are_per_column_which_is_why_the_fence_statement_writes_the_epoch`; it
also keeps the failing pair as a negative control. The process tests of section 8 run every interleaving that
the crash points allow against it.

**Rule for every caller.** A statement that must conflict with another writer **writes, with a changed value,
a column that the other writer also writes with a changed value** (`SET state = 'b' … WHERE state = 'a'` does;
`SET pos = ? … WHERE pos = ?` does; `SET n = n + 1 … WHERE epoch = ?` does only against a writer of `n`). A
write of an unchanged value (`SET epoch = epoch`, `SET epoch = ?` with the value it has, a transition from a
state to itself) is **not recorded** by DuckDB 1.5.6 or MotherDuck when the column has had no update since the
table was written or checkpointed, and another writer then does not conflict with it (fix round, F0; section
14.4). `state.transition` changes its state column, the migrations change their version, and the stream moves
its position, so they follow the rule.

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
  wrote the row right after `interrupt_earlier` had returned got 1 to 3 conflicts, and committed 0.65 to 1.28 s
  after the interrupt (`registry_takeover.json`, key `open_transaction_holds_only_data_rows`; the ranges are
  over the runs of 2026-11-10 on `88e0cf1` and on `7193b39`). A take-over that
  interrupts needs 1 or 2 further attempts (the table below, and section 8).

What a take-over meets (`registry_takeover.json`; one process, lanes as the old run; the old run's client is
alive, so nothing ends its transaction but the interrupt; ranges over the runs on `88e0cf1` and `7193b39`):

| The old run's open transaction | Take-over with the interrupt | Without the interrupt |
|---|---|---|
| has updated the heartbeat row | won at the 2nd or 3rd attempt after 1 or 2 conflicts, 1.47 to 1.93 s | `TakeoverFailed` after 8 to 10 conflicts in 6.5 to 6.7 s |
| has run its fence statement and stands before `COMMIT` | won at the 2nd attempt after 1 conflict, 1.07 to 1.35 s; the old run gets `CommitUnknown`; nothing committed | `TakeoverFailed` after 8 or 9 conflicts in 5.3 to 5.5 s |
| holds only data rows | won at once, 0.38 to 0.57 s; nothing listed, nothing interrupted | the same |

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
everything and ends. Then the old run goes on. Numbers are ranges over the recorded runs on `88e0cf1` and
`7193b39`.

| Old run frozen at | Take-over | Newcomer before its first commit | Old run, when it goes on |
|---|---|---|---|
| `sut.begun` (it only read) | at once, 0.41 to 0.51 s; the call after the take-over interrupted 1 | no error | `ConnectionLost`, then `FenceLost` |
| `sut.written` | at once, 0.39 to 0.50 s; the call after the take-over interrupted 1 | no error | `ConnectionLost`, then `FenceLost` |
| `sut.moved` | at once; the call after the take-over interrupted 1 | 0 or 1 `Conflict` | `ConnectionLost`, then `FenceLost` |
| `stream.fenced` | 3rd attempt after 2 conflicts, 1.84 to 2.19 s | no error | `CommitUnknown`, then `FenceLost` |
| `stream.commit_sent` | 3rd attempt after 2 conflicts, 1.74 to 2.24 s | no error | `CommitUnknown`, then `FenceLost` |
| `stream.committed` | at once, 0.39 to 0.50 s | no error | its commit stands; `FenceLost` at its next transaction |

- The old run never committed after the take-over. Its transaction at the first five points left no row.
- The old run ended with exit code 3 (`FenceLost`) 0.56 to 3.05 s after SIGCONT; 2.98 and 3.05 s in the two
  `CommitUnknown` cases, in which the SUT waits 2.5 s before it reads again.
- At `sut.moved` the newcomer got one `Conflict` (in one of the two runs) although the interrupt had answered: the rows of an
  interrupted transaction stay held for a moment (section 7).
- At `stream.fenced` and `stream.commit_sent` the old run's `COMMIT` is the first thing it sends after the
  interrupt. So the layer reports `CommitUnknown` although nothing was committed; the SUT reads again and then
  gets `FenceLost`.

### 8.2 The same epoch twice

Two processes that use one lane with one epoch are a programming error. The fence row is the same row for
both, and the run-again rule holds as well. `test_two_processes_with_the_same_epoch`; `two_process.json`, key
`same_epoch`.

- Both free, 30 events: one process made 8 data transactions (24 rows, 1 or 2 `Conflict`s), the other 2 (6
  rows, 6 `Conflict`s); which one was which changed between the runs.
- The first frozen at `sut.begun` or `sut.written` of its second transaction: the second applied everything.
  The first got `Conflict` when it went on.
- The first frozen at `sut.moved`, `stream.fenced` or `stream.commit_sent` (it holds the position row): the
  second got 3 or 4 `Conflict`s and committed nothing while the first was frozen. The first committed when it
  went on, in the runs on `88e0cf1` and `7193b39`. In the run on `e671405`, under the load of two campaigns at
  once, the second process applied the events at `sut.moved` and the first got an error when it went on:
  MotherDuck had ended its frozen transaction (the lease of section 8.3); exactly-once held. The test accepts
  both. Without a take-over nobody interrupts the first.
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
| fence row | frozen | 3rd attempt, 2 conflicts, 1.74 to 2.71 s | 0.94 to 1.68 s | `CommitUnknown`, then `FenceLost` |
| fence row | killed | 2nd or 3rd attempt, 1 or 2 conflicts, 1.39 to 1.83 s | 0.93 to 1.21 s | |
| fence row | slow | 3rd attempt, 2 conflicts, 1.83 to 2.73 s | 1.01 to 1.59 s | `CommitUnknown`, then `FenceLost` |
| position row | frozen | at once, 0.39 to 0.58 s; the call after it interrupted 1 | 0.90 to 1.32 s | `ConnectionLost`, then `FenceLost` |
| position row | killed | at once, 0.40 to 0.49 s; the call after it interrupted 1 | 0.96 to 1.22 s | |
| position row | slow | at once, 0.40 to 0.66 s; the call after it interrupted 1 | 0.91 to 1.38 s | `ConnectionLost`, then `FenceLost` |

No transaction of the old run committed (0 rows) in the six cases.

Without the interrupt (key `without_interrupt`; the newcomer never calls `interrupt_earlier`). This is the
control: it shows what the interrupt is for.

| Old run holds | Old run is | Take-over | Newcomer's first commit after the take-over | Old run gets |
|---|---|---|---|---|
| fence row | frozen | 41st to 49th attempt, 40 to 48 conflicts, **28.93 to 28.98 s** | 0.77 to 1.11 s | `DeadlineExceeded` (its block deadline had passed), then `FenceLost` |
| fence row | killed | 40th to 50th attempt, 39 to 49 conflicts, **28.67 to 29.32 s** | 0.75 to 1.11 s | |
| fence row | slow | 19th to 24th attempt, 18 to 23 conflicts, 14.16 to 14.41 s | nothing was left to apply | it committed two more transactions (4 rows) before the take-over won, then `FenceLost` |
| position row | frozen | at once, 0.39 to 0.47 s | **28.59 to 28.66 s**, after 47 to 50 `Conflict`s | `DeadlineExceeded`, then `FenceLost` |
| position row | killed | at once, 0.40 to 0.49 s | **28.00 to 28.97 s**, after 45 to 48 `Conflict`s | |
| position row | slow | at once, 0.38 to 0.52 s | 12.46 to 12.99 s, after 20 to 22 `Conflict`s | `FenceLost` when its block ended; nothing committed |

What this shows:

- **MotherDuck ends the open transaction of a frozen or killed client after about 29 s.** This agrees with
  `docs/spike/md_semantics.md` (lease 32 s for a paused client; rows locked 29.4 to 29.9 s after SIGKILL).
- With the interrupt a take-over takes 0.4 to 0.7 s when it meets no conflict and 1.4 to 2.7 s when it does.
  The newcomer's first commit comes 0.9 to 1.7 s after the take-over.
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
| 80 ms | 4 of 4 | 0 or 1 of 4 |
| 100 ms | 4 of 4 | 0 or 1 of 4 |
| 150, 300 ms | 4 of 4 each | 0 of 4 each |

**32 of 36 killed commits landed, 4 did not**, in every run. **16 to 18** landed without the client having seen
the answer: these are the cases in which a run cannot know. The result was exactly-once after every one of the
36 victims; at the end the counter was 108 with 0 doubles.

The edge lies near one round trip (about 80 ms, section 11) and moves a little from run to run. The counts over
the recorded runs: 16 (two runs on `88e0cf1` and before it, 0 of 4 at 80 ms), 17 (1 of 4 at 80 ms) and 18 on
`7193b39` (1 of 4 at 80 ms and 1 of 4 at 100 ms; `commit_unknown.json`; the earlier files are in the history
of the repository and in `final_runs.json`).

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
| `commit_never_arrives` | holds the `COMMIT`; after the child's error the connections are reset | `CommitUnknown` | no in the runs on `88e0cf1` and before; **yes** in the runs on `e671405` and `7193b39`: the client delivered the held `COMMIT` on a new connection after the reset. Either outcome is right for `CommitUnknown`; the test asserts the error only |
| `commit_answer_lost` | lets the `COMMIT` through, passes nothing back | `CommitUnknown` | **yes** |
| `commit_delivered_late` | holds the `COMMIT`; 4 s after the child's error it delivers it, and the statements of the child's next attempt behind it | `CommitUnknown` | **yes**, late |

In all five the child recovered by itself and ended with exit code 0, 4.5 to 7.0 s after the fault was
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

**42 of 42 passed; 9 of 9 lane crash points were reached.** The run took 234 to 268 s and created and dropped 42
databases. At T0 `tests/destination/test_sut.py` runs the SUT on a local file.

### 9.4 A network path that answers nothing (fix round, items 6 and 15)

`test_md_fix_round.py::test_a_black_holed_path_bounds_every_call_and_the_calls_left_waiting`; `fix_round.json`, key
`black_hole` (`blackhole.json` points there). A child (`blackhole_child.py`) reaches MotherDuck only through the
harness `NetProxy` and the relay of section 9.2; the proxy passes nothing in either direction from a chosen
moment on. Child settings: statement 2 s, block 4 s, grace 0.5 s, connect 3 s (one attempt), roll-back 1 s,
`max_waiting_calls` 2. Copied from the second reviewer's `e3_blackhole.py`, whose numbers on `9e389cc` are next to
these.

| Step that meets the black hole | The caller got after (7193b39) | On 9e389cc | Error |
|---|---|---|---|
| a statement | 2.52 s | 2.52 s | `DeadlineExceeded` |
| the fence statement | 2.60 s | 4.50 s (the block's deadline) | `DeadlineExceeded` |
| `COMMIT` | 4.51 s (the block's deadline, by design) | 4.51 s | `CommitUnknown` |
| connect | 3.51 s | 3.51 s | `DeadlineExceeded` |
| the registration | 2.52 s | 4.51 s (the block's deadline) | `DeadlineExceeded` |
| 12 more attempts of the lane, the path still black-holed | the first 2: 2.52 s and 3.52 s (`DeadlineExceeded`); the other 10: at once, 0.0 s (`ConnectionLost`, "2 calls that were given up still wait") | 3.5 s each, `DeadlineExceeded` every time | |

- Threads of the child during the 12 attempts: 4, then 6, then 6 for the last 10 (on `9e389cc`: 4 growing by 2
  per attempt to 26). `md.waiting_calls()` 2, then 3 (two calls and one interrupt through a short-lived
  connection); `md.open_connections()` counted 2 to 3 (on `9e389cc` it showed 1 throughout).
- After the path passed again everything was freed within about 1 s (on `9e389cc` within 10 s); the next
  transaction of the lane worked; at the end no thread and no connection of the layer was left.
- Every call returned within its bound plus at most 0.1 s.

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
- The client cannot fetch a `TIMESTAMPTZ` value as a Python object without the module `pytz`, which is not a
  dependency. The layer therefore reads times as integer microseconds (`epoch_us`). A caller that selects a
  `TIMESTAMPTZ` column with `fetchall` or `fetchone` does the same or casts it. Arrow data is different: with
  the session time zone UTC (section 2.1) a `TIMESTAMPTZ` column goes out through `fetch_arrow` and in through
  `register` without `pytz` (`test_from_apply.py::test_arrow_data_keeps_uuid_json_and_time_zones`).

## 11. Timings on MotherDuck

Indications from a laptop, one process, 25 repetitions each, other workers running on the same machine
(`timings.json`; ranges over the runs on `88e0cf1` and `7193b39`). Since the fix round the first transaction of a
new client connection costs three more round trips (the registration is a transaction of its own, section 2.3);
later transactions of the connection cost the same as before:

| What | Median | p90 |
|---|---|---|
| `md.connect()` to a database that the process has open | 76 to 106 ms (5 calls a run) | |
| `BEGIN` | 0.2 ms | 0.2 ms |
| the fence statement | 76.3 to 78.0 ms | 77.5 to 83.9 ms |
| `COMMIT` after the fence statement | 79.9 to 80.5 ms | 82.4 to 84.5 ms |
| an empty lane transaction | 155.3 to 160.9 ms | 157.7 to 171.2 ms |
| a lane transaction with one `UPDATE` | 234.9 to 238.9 ms | 240.1 to 258.7 ms |
| reading the clock | 77.9 to 79.1 ms | 82.2 to 85.4 ms |

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
| `test_only_the_layer_opens_a_duckdb_connection` | outside `cdc_v2/md`, `cdc_v2/testing`, `flightctl.py` and `cdc_v2/oracle`, `duckdb.connect` is called only for a database in the memory of the process, written as the literal `":memory:"` (contract note 37) | a call through another name is not seen |
| `test_no_code_outside_the_layer_holds_text_that_reaches_the_destination` | outside the same places no SQL text attaches a database, names `md:` or loads the MotherDuck extension, so a database in memory stays one | as the first |
| `test_the_oracle_only_reads` | no SQL text of `cdc_v2/oracle` writes or commits; that is why the oracle may open a connection of its own | as the first |
| `test_only_the_layer_holds_the_fence_statement` | the fence statement exists once | |
| `test_no_text_deletes_or_inserts_leader_rows` | no SQL text deletes or inserts rows of the three leader tables | as the first |
| `test_this_package_reads_no_local_wall_clock` | the layer never compares with the local wall clock | only this package's files |
| `test_nothing_of_this_package_drops_a_schema_or_names_a_scratch_schema_of_apply` | no SQL text of this package holds `DROP SCHEMA`, `CASCADE` or the prefix of apply's scratch schemas, so nothing of it cleans one up or drops with `CASCADE` | only this package's files; SQL text as in the first row |
| `test_the_layer_never_hands_out_the_lazy_reader_of_the_client` | no file of the layer uses the client's `.arrow`, `fetch_record_batch`, `fetch_arrow_reader` or `to_arrow_reader`; Arrow results are read in one place, with `to_arrow_table`, in `md/connection.py` | only this package's files; a call through another name is not seen |

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
   repeats no local path (same test; `test_connect_has_a_deadline`). It is the one message of the layer that
   holds text of the client (section 5).
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
   (`test_execute_one_needs_exactly_one_row`). A statement with `RETURNING` reports no count (note 30).
9. **`Transaction` has more methods than C4 names**: `fetchone`, `clock_us`, `fail`
   (`test_arrow_data_and_row_counts`, `test_a_failed_transaction_cannot_end_quietly`,
   `test_state.py::test_two_rows_for_one_key_fail_the_transaction`). `fetchall`, `fetch_arrow` and `register`
   came into C4 with the review of apply (notes 29 to 33).
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
14. **An error about the data of a statement is never sorted by its text**, and where a text decides, only its
    head does (section 5;
    `test_errors.py::test_a_value_that_holds_a_phrase_does_not_look_like_a_conflict_or_a_lost_connection`,
    `test_errors.py::test_the_known_limit_of_the_head_rule`).

**Registry and leader tables**

15. **A connection that is not a lane registers under a lane name.** C4 speaks of "every connection of a lane".
    The heartbeat connection of package L writes for an epoch too, so it calls
    `registry.register(conn, "control", epoch, retry_conflict=True)` itself
    (`test_a_connection_that_is_not_a_lane_registers_under_a_lane_name`; section 2.3).
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

**Added with the review of apply and the review of this package** (tests without a file name are in
`tests/destination/test_from_apply.py`)

29. **`md.StatementError` is not in C4; it is a proposal for it.** C4 said that an error which is none of its
    four "is passed on unchanged after the roll-back". The review of apply asks for a destination error type
    whose message keeps "the error class and the statement shape, never a value", and leaves its name open.
    Built: `md.StatementError(where, error_class, statement)`, a subclass of `DestinationError` (section 5). The
    "statement shape" is read as the kind of the statement: the name the client's parser gives it. Nothing
    finer was built, because any piece of the SQL text can hold a value
    (`test_a_value_in_a_destination_error_never_travels_on`).
30. **What `execute` counts.** The review of apply says: the number of rows an `INSERT`, `UPDATE` or `DELETE`
    changed, else `None`. Open were `TRUNCATE`, `MERGE`, `COPY`, `CREATE TABLE … AS` and `RETURNING`. Built:
    the kind comes from the client's parser, so `TRUNCATE` counts (the parser calls it `DELETE`) and the others
    return `None`; a statement with `RETURNING` returns `None` and is `LaneMisuse` in `execute_one` (section
    2.2). `Connection.execute` follows the same rule.
31. **What `register` takes.** The review of apply says "a `pyarrow.Table` or record batches". Built: a table,
    one record batch, or a list or tuple of record batches with one schema. A `RecordBatchReader` is refused:
    the layer cannot tell whether the connection itself produced it, and that case deadlocks. A name is checked
    as a whole (`fullmatch`), so a name with a line break after it is refused (`test_a_registered_name_is_checked`).
32. **The two settings are set with `SET SESSION`.** The review of apply says "sets". A plain `SET` reaches the
    other connections of the process (section 2.1).
33. **Scratch schemas of apply.** `check_sql` does not take a statement on `_cdc_apply_<random>` for one on the
    leader tables, and still refuses the same statements on `_cdc` (section 2.1). The check of the leader tables and the migrations
    never look outside `_cdc` (section 2.4).
34. **Who creates the keys of a machine** (decided by the coordinator in the fix round, item 3). C5 did not say
    what happens when two transactions create one key; on two lanes both committed. Built:
    `Machine(creators=(lane, …) | state.FRESH_IDS)`, required, and `create` refuses any other lane with
    `LaneMisuse`; a child row is created together with a transition of its parent row (section 2.5).
35. **Log lines of alerts and state transitions carry `committed: False`** (sections 2.5 and 2.6). C4 and C5 name
    no log lines.
36. **The bound of a lane call that fails** is `rollback_deadline_s` plus the grace period after the failed
    step, for the whole clean-up (section 4). C4 says only that the roll-back has a short deadline.

**Added with the fix round** (section 14.3; the coordinator's replacement sentences for C4 and C5 say the same)

38. **`state.transition(tx, machine, key, from_state, to_state, cause, *, detail=None, values=None,
    expect=None)`** and **`state.create(…, values=None, detail=None)`**; every row of `_cdc.transitions` carries
    lane, epoch, `seq` and `detail` (section 2.5; item 1).
39. **`state.transition`, `state.create` and `alerts.emit` take an `md.Transaction` only**; `state.read` takes a
    connection too (item 12).
40. **The registration is a transaction of its own** that writes the epoch of its lane's fence row (section
    2.3; item 2). Its `COMMIT` passes `md.commit` and `md.commit_reply`. `registry.register` has
    `retry_conflict` for the heartbeat connection.
41. **`md.waiting_calls()` and `Settings.max_waiting_calls`** (section 4; items 6 and 15). The call past the cap
    is `ConnectionLost` at once.
42. **The deadline of the fence statement and of the registration is the statement deadline** (section 4;
    item 15).
43. **`_cdc.schema_version.applied`** keeps the name and hash of every applied step; a difference is
    `SchemaError` (section 2.4; item 13).
44. **`cdc_v2.testing.destination.AnotherRunLeads`**, a `TakeoverFailed`, for the loser of simultaneous
    starters (section 2.7; item 14).
45. **Marker `d_full`** for the rest of the full test matrices of this package (section 16; item 7). It is
    registered in `tests/destination/conftest.py`, because this package does not edit `pyproject.toml`.
46. **The take-over raises the fence rows' counter `n`** by one, and the registration raises `n` of its lane's
    fence row (F0, section 14.4); neither puts it back (fix round 2). Design 4.5 writes only the epoch; the
    coordinator's replacement sentences for core 4.2, 4.5 and C4 say it.
47. **Outside a lane transaction only reads run** (fix round 2, N2): `Connection.execute`, `fetchall` and
    `fetchone` refuse a statement that writes with `LaneMisuse`; the layer's own statements (the creation of
    the leader tables, the clean-up of `_cdc.connections`, the heartbeat) use an internal path. No caller may
    `UPDATE` `_cdc.leader`, `_cdc.fence` or `_cdc.heartbeat`, in a lane or outside one.
48. **`md.schema.heartbeat(conn, epoch, phase, progress_age_us=0)` and `md.schema.release(tx)`** are package L's
    heartbeat (design 4.3, in autocommit) and release (design 4.6, in a control-lane transaction), statements
    of the layer since callers may not update the leader tables. Both raise `FenceLost` when the epoch no longer
    leads. They are in `schema.NAMED_STATEMENTS` (`heartbeat`, `release`).
49. **`state.transition` refuses `expect` on a column that is not in `values`, and a transition from a state to
    itself without `values`** (`LaneMisuse`; fix round 2, items 2 and 6). `check_machine` takes `self_values`
    for a machine with a self-transition.
50. **The texts of the `md.*` fault sites** are those of the toy system of package F (`tests/fuzz/toy/toy.py`),
    which declares the same sites; `cdc_v2.faultsites` refuses one site with two texts (found when `origin/main`
    with package F was merged).
51. **`cdc_v2.testing.destination.Destination.run(sql, params)`** sets up and inspects a test's database on a
    plain client connection, outside the layer.

**Added with the merge of the transform (package Y)**

37. **"Only the layer opens a DuckDB connection" is read as "only the layer opens a connection to the
    destination".** C4 names `md.connect` as the way to the destination. It does not speak of a DuckDB database
    that a module uses to compute, and the plan names two of them: "the transform connection" and "apply's
    planning database" (`docs/design/p3_plan.md`, section 6.2, "From the review of F"). Built in the static
    tests (section 12): outside the layer, its test helpers, `flightctl.py` and the oracle, a module may open a
    database in the memory of its process, written as `duckdb.connect(":memory:", …)` with that literal, and no
    SQL text there attaches a database, names `md:` or loads the MotherDuck extension. Until the transform was
    merged the test refused every `duckdb.connect` outside those places (finding 23).

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
24. **(Fix round, F0) The fence of design 4.2 with the take-over of design 4.5 lets two runs commit.** DuckDB
    1.5.6 and MotherDuck do not record a write of an unchanged value into a column that has had no update
    since the table was written or checkpointed. After a checkpoint the fence statement's write of the unchanged
    epoch is such a write, so a take-over that writes the epoch alone does not conflict with an old run's open
    transaction, and both commit, data included (section 14.4). The take-over must also write the fence rows'
    counter `n` with a changed value (`n + 1`, never put back), and every exclusion must rest on a changed value
    (section 6). This changes the take-over text of design 4.5 (package L) and the rule "write the columns your
    condition reads" of finding 1.

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
15. **The text of a client error names a value of a row**: with quotation marks, without them, or as a piece of
    the SQL text (section 5; `from_apply.json`, key `planted_values`). The text of a failed connect can hold a
    local path (contract note 3). Both are kept out of the errors of this layer.
16. **The client cannot hand out a `TIMESTAMPTZ` value as a Python object without `pytz`**
    (`test_lane.py::test_times_are_read_as_integer_microseconds`). Arrow data with the session time zone UTC
    needs no `pytz` (section 10).
17. **The clock counts whole milliseconds** (section 10).
18. **The class of the client's error after a server interrupt of an open transaction is not always the
    same.** In one test (`test_md.py::test_an_open_transaction_interrupted_on_the_server_is_connection_lost`) the
    first statement that the lane sent after the interrupt was answered with `TransactionException` (`…
    transaction already rolled back: Connection Error: interrupted by user`) in the run of 08:13 UTC and with
    `ConnectionException` in the run of 11:46 UTC (both 2026-11-10; the file of the first run is in the git
    history of `error_mapping.json`). A plain client connection got `ConnectionException` for the first
    statement after the interrupt and `TransactionException` for the next one, in both runs (`error_mapping.json`,
    key `raw_texts`, case `idle_open_transaction_interrupted_on_the_server_then_a_statement`). The code of the
    layer changed between the two runs, so they do not show why the lane saw the other class the first time.
    Both classes are transport errors by rule 2 of section 5, and the tests assert `ConnectionLost`, not the
    class.
19. **The client describes the result of a statement with `RETURNING` like a count when a returned column is
    named `Count`.** `INSERT … RETURNING k AS "Count"` came back from `execute` as 41, the first returned value,
    and `execute_one` took a returned 1 for "one row changed" although two rows had changed. Found while the
    seven points of section 14.1 were tested, fixed (section 2.2), with seeded tests that fail on the code
    before the fix: `test_from_apply.py::test_execute_returns_a_count_for_insert_update_and_delete_only`,
    `test_from_apply.py::test_execute_one_never_takes_a_returned_value_for_a_count`. Both also pass on MotherDuck.
20. **`DROP SCHEMA … CASCADE` of tables made in the same transaction is accepted, and then `COMMIT` fails**, on
    MotherDuck (measured first by package A) and on local DuckDB 1.5.6. To the lane it is an error after
    `COMMIT` was sent: `CommitUnknown` with the label `TransactionException`, and the connection is given up.
    Nothing was committed and no schema was left (`from_apply.json`, key `drop_schema_cascade`;
    `test_from_apply.py::test_drop_schema_cascade_of_tables_made_in_the_same_transaction_fails_at_commit`).
21. **A plain `SET arrow_lossless_conversion` on one connection changes what the other connections of the
    process to the same database read** (local DuckDB 1.5.6; section 2.1). It was not measured on MotherDuck.
    There, after another connection had set both settings globally to other values, the connections of the
    layer still read their own (`from_apply.json`, key `settings_scope`). Package A sets both settings with a
    plain `SET` on local DuckDB databases of its own (`src/cdc_v2/apply/backend.py` and `core.py` on its
    branch); those are not connections of the layer and not connections to the destination.
22. **`MERGE INTO` reports no count through the layer, and `TRUNCATE` reports the rows it removed**
    (`from_apply.json`, key `merge_and_truncate`: `None`, and 3 for a table of 3 rows).
23. **A static test of this package refused the transform.**
    `test_static.py::test_only_the_layer_opens_a_duckdb_connection` refused every `duckdb.connect` outside the
    layer. `src/cdc_v2/transform/convert.py` (package Y) opens a database in memory to convert values. The test
    failed in the default run when `origin/main` with the transform was merged into this branch on 2026-11-10
    (1 failed: `('transform/convert.py', 79)`). The rule of the test was wider than C4, and the plan names such
    databases; the transform is not wrong (contract note 37). The test now refuses every connection outside
    the layer but the literal `":memory:"`, with seeded cases for every other way of writing one. A second test,
    `test_no_code_outside_the_layer_holds_text_that_reaches_the_destination`, refuses the SQL text with which
    such a database could reach MotherDuck. The two rules were also run by hand over the files of package A on
    its branch (commit `68df804`, read only): 3 connections in `apply/backend.py` and `apply/core.py`, all in
    that form, and no such text. The test run itself reads only this branch.

**Agreement with the spike report**

- MotherDuck ends the open transaction of a frozen or killed client after about 29 s (28.66 to 29.32 s here;
  29.4 to 29.9 s and a lease of 32 s there).
- One round trip takes about 75 to 80 ms from a laptop (75 ms there; 8 ms from a Flight).

**A bug of this package that only the MotherDuck run found.** The interrupt of a replaced connection called
itself and never returned. It needs a destination that has `md_interrupt_server_connection`, so no T0 test had
reached it. Seeded regression test:
`test_lane.py::test_the_interrupt_of_a_replaced_connection_runs_on_the_new_connection`.

**A wrong assumption of a test that only the MotherDuck run found.**
`test_lane.py::test_an_unclassified_error_after_commit_was_sent_is_commit_unknown` assumed that a connection
which replaces another one sends one statement before `BEGIN`. On MotherDuck it sends two: the interrupt of the
old id, then the registering `INSERT` (section 3). The test failed in the md run of 2026-11-10 (1 of 71). It
now walks the statements of a lane's first connection, which are the same in both places. It asserts the same
classes and kinds as before.

### 14.1 From the review of apply

The review of package A added seven points to contract C4. All seven are built, and none of them is wrong on
MotherDuck. The tests are in `tests/destination/test_from_apply.py` (85 test cases in the default run) and two
are in `tests/destination/test_static.py`. Fifteen of the tests run again against MotherDuck through
`test_md_again.py`; they are marked (md). What they measured there is in `from_apply.json`.

| Point of C4 | Built | Tests | Measured on MotherDuck |
|---|---|---|---|
| 1. `tx.execute(sql, params=None, deadline=None) -> int \| None`: the rows an `INSERT`, `UPDATE` or `DELETE` changed, else `None` | section 2.2, "Counts"; contract note 30 | `test_execute_returns_a_count_for_insert_update_and_delete_only` (md), `test_merge_reports_no_count_and_truncate_counts_as_a_delete` (md), `test_a_returning_clause_is_found_by_the_tokenizer_of_the_client`, `test_execute_one_never_takes_a_returned_value_for_a_count` (md), `test_the_kind_of_a_statement_comes_from_the_parser_of_the_client` | `MERGE` returns `None`; `TRUNCATE` of 3 rows returns 3 (key `merge_and_truncate`) |
| 2. `tx.fetchall(sql, params=None, deadline=None) -> list` | section 2.2 | `test_fetch_arrow_gives_a_table_that_the_same_transaction_can_register` (md) reads with both | |
| 3. `tx.fetch_arrow(…) -> pyarrow.Table`, materialised; never a lazy reader | section 2.2 | the same test (md); `test_static.py::test_the_layer_never_hands_out_the_lazy_reader_of_the_client` | a table read with `fetch_arrow` was registered and read again in the same transaction |
| 4. `tx.register(name, data, deadline=None)`; the name rule; gone when the block ends, also by an error or a deadline | section 2.2; contract note 31 | `test_register_takes_a_table_a_record_batch_or_a_list_of_batches` (md), `test_a_registered_name_is_checked` (md), `test_a_registered_name_is_gone_when_the_block_ends_however_it_ends` (md), `test_a_registered_name_is_gone_after_a_deadline_gave_the_connection_up` (md) | the same results as locally |
| 5. `TimeZone = 'UTC'` and `arrow_lossless_conversion = true` on every lane connection before its first statement | section 2.1; contract note 32 | `test_arrow_data_keeps_uuid_json_and_time_zones` (md), `test_every_connection_of_the_layer_has_the_two_settings` (md), `test_the_two_settings_belong_to_the_connections_of_the_layer_only` (md), `test_a_plain_set_of_the_lossless_setting_reaches_the_other_connections_of_the_process` (local only) | with the setting: `extension<arrow.uuid>`, `extension<arrow.json>`, `time_tz` and `hugeint` as `arrow.opaque`, `timestamp[us, tz=UTC]`; without: `string`, `string`, `time64[us]`, `decimal128(38, 0)` (key `arrow_types`). After another connection set both globally to other values, the lane and a new connection of the layer still read `UTC` and `true` (key `settings_scope`) |
| 6. A destination error keeps the error class and the statement shape, never a value; the raw text is never logged, never in an alert or in `_cdc`; a test with a planted value | section 5; contract note 29 | `test_a_value_in_a_destination_error_never_travels_on` (md), `test_a_value_in_local_data_that_cannot_be_registered_does_not_travel_on`, `test_review.py::test_a_key_value_that_commit_reports_does_not_travel_on` | 7 of 7 client texts named the planted value; none of the layer's texts, log lines, alert records or `_cdc` rows did (key `planted_values`) |
| 7. `DROP SCHEMA … CASCADE` of tables created in the same transaction fails at `COMMIT`; scratch schemas `_cdc_apply_<random>` are not reported and not cleaned up | sections 2.1 and 2.4; contract note 33; finding 20 | `test_a_scratch_schema_of_apply_lives_inside_one_transaction` (md), `test_drop_schema_cascade_of_tables_made_in_the_same_transaction_fails_at_commit` (md), `test_a_statement_on_a_scratch_schema_is_not_taken_for_one_on_the_leader_tables`, `test_the_leader_tables_stay_protected_next_to_scratch_schemas`, `test_the_schema_check_and_the_migrations_leave_a_scratch_schema_alone` (md), `test_static.py::test_nothing_of_this_package_drops_a_schema_or_names_a_scratch_schema_of_apply` | `CommitUnknown`, label `TransactionException`, nothing committed, 0 scratch schemas left (key `drop_schema_cascade`) |

What goes beyond the seven points, or reads one of them:

- The name of the error type of point 6, `md.StatementError`, and the reading of "statement shape" as the kind
  of the statement are a proposal (contract note 29).
- Point 1 does not say what a statement with `RETURNING` returns. It returns `None` (finding 19).
- Point 5: the settings are set with `SET SESSION` (finding 21).
- Point 7: no migration step and no staging of this package drops what it created in the same transaction, so
  there was nothing to change. A caller that sends `DROP SCHEMA … CASCADE` in that case gets `CommitUnknown`
  and a new connection, not a refusal before the statement is sent (finding 20).
- This package read `src/cdc_v2/apply/backend.py` of the branch of package A (read only). It calls
  `tx.execute`, `tx.fetch_arrow` and `tx.register`, with the arguments and results as built here. It drops its
  scratch tables one by one and then the schema. No test of this package runs the code of package A.

### 14.2 What the review of this package found

One read-only reviewer with a fresh context (model `opus`) read the code of this package, as
`docs/tasks/README.md` allows. The table gives its results by topic. Each seeded test fails on the code as it
was before the fix; it was run against a copy of that code.

| Topic | What was wrong | What was done | Seeded test |
|---|---|---|---|
| A wait that raises | The wait for a statement starts a watchdog thread at the deadline. When no thread could start, `RuntimeError` came out of the wait; a caller that swallowed it left the block normally, and the block committed a transaction whose statement had no answer. Any other exception in the waiting thread left the connection as it was | section 4, "What the rule does not cover" | `test_review.py::test_a_deadline_ends_the_transaction_also_when_no_watchdog_thread_can_start`, `test_review.py::test_a_wait_that_is_broken_off_never_leaves_a_transaction_that_commits` |
| The bound of a call that fails | The roll-back and the read of the fence row each had `rollback_deadline_s` plus the grace period, twice the documented bound | one budget for the clean-up (section 3) | `test_review.py::test_the_clean_up_after_a_failed_step_has_one_time_budget` |
| Values in the texts of the client | The errors of the layer carried the first line of the client's text and the client's exception as their cause; a text was sorted by a phrase anywhere in it | labels, no cause, the head rule (section 5) | `test_review.py::test_a_key_value_that_commit_reports_does_not_travel_on`; in `test_errors.py` the rows marked "seeded (review)" of `test_how_an_exception_is_sorted`, `test_the_head_of_a_text_ends_at_the_first_quotation_mark`, `test_the_known_limit_of_the_head_rule` |
| A connect that answers late | A connect that answered inside the grace period, after its deadline, was handed to the caller although it was being closed | `DeadlineExceeded`; nothing stays open (section 2.1) | `test_review.py::test_a_connect_that_answers_after_its_deadline_is_not_handed_out` |
| The id that registration writes | Registration read the id and then ran its statement through the ordinary path, which replaces a connection that was given up meanwhile. The row then held the id of the old connection, and a take-over would have interrupted the wrong one | the statement runs on the connection whose id it writes (section 2.3) | `test_review.py::test_registration_writes_the_id_of_the_connection_that_runs_it` |
| An old id that the destination refuses to interrupt | The id stayed first in the list, so every later call of the lane failed in the same way | the call fails once and the id is dropped (section 4) | `test_review.py::test_an_old_connection_id_that_the_destination_refuses_does_not_block_the_lane_for_ever` |
| Log lines before the commit | The lines `alert` and `state transition` read like facts and are written inside an open transaction that can still roll back | `committed: False` in the record (sections 2.5 and 2.6) | `test_review.py::test_a_log_line_written_inside_a_transaction_says_that_nothing_is_committed_yet` |
| Two creators of one key | On two lanes both commit and the key has two rows | **not solved**; the damage is seen, and the rule is the callers' (contract note 34) | `test_state.py::test_two_creators_of_one_key_meet_only_through_the_fence_row_of_their_lane` |

Open after the review, and what became of it in the fix round (section 14.3):

- **Two creators of one key on two lanes**: closed by `Machine.creators` (item 3).
- **Threads of a retired connection on a network that answers nothing**: bounded and counted (items 6 and 15;
  section 4). The client library still gives no way to end a call that waits for the server.
- **A connection of an old run that registers late**: closed; the registration writes the fence row (item 2).
- **The limit of the head rule** (section 5): narrowed to errors outside the data and programming families.
  The two texts that the `RETURNING` rule takes for a clause (section 2.2) stay; they err toward no count.

### 14.3 The fix round

Two reviews of the delivery `9e389cc`: the coordinator's ("merge after named fixes", items 1 to 7) and a second
reviewer's, who attacked the layer with programs of its own (items 8 to 16, its findings F0 to F8). Every item
has a test that fails on the code of `9e389cc`: the new tests were run against that code (the tree of the test
commit `7193b39` with `src` of `9e389cc`; `fix_round_old_code.json`). 50 of the 59 new tests and seeded cases
failed there. The 9 that passed: the take-over at every point in a destination in memory (4 cases: there the
fence row always has updates, so F0 cannot arise; F0 shows on MotherDuck and after a checkpoint), the test of
DuckDB's own behaviour that causes F0, 3 seeded cases of `test_how_an_exception_is_sorted` that guard texts the
old rules already sorted right, and `test_a_child_row_created_from_two_lanes_is_created_with_a_transition_of_its_parent`,
which shows the callers' rule of item 3 (it holds on the old code too). The shape of F0 itself, a take-over at
`stream.fenced` right after a checkpoint on a local file, committed the old block after the take-over in 5 of 5
rounds on `9e389cc` and in 0 of 5 on `7193b39` (same file, key `f0_on_the_old_code_after_a_checkpoint`). The tests without a file are in `test_fix_round.py`.

| Item | What was wrong | What was done | Tests |
|---|---|---|---|
| 1 | A row of `_cdc.transitions` had no place for the facts of a group and no order of commit | columns `lane`, `seq`, `detail`; `values`, `expect`, `detail` (section 2.5) | `test_a_transition_row_carries_its_lane_epoch_seq_and_detail`, `test_seq_has_no_gaps_and_equals_the_fence_counter_on_two_lanes_and_two_epochs`, `test_a_rolled_back_transaction_leaves_no_transition_row`, `test_a_run_that_bypasses_the_fence_shows_a_duplicate_seq`, `test_values_and_expect_in_the_same_conditional_update` |
| 2 | The registration read `_cdc.leader` and wrote only `_cdc.connections`: one that read the old epoch could commit after the take-over | a transaction that writes the lane's fence row (section 2.3); the heartbeat under `control` with `retry_conflict` | `test_a_registration_while_a_take_over_is_open_does_not_commit`, `test_a_registration_frozen_between_its_check_and_its_commit_and_a_take_over_in_between`, `test_a_connection_that_is_not_a_lane_registers_under_control_and_retries_a_conflict` |
| 3 | Nothing said who creates the keys of a machine | `Machine.creators` (section 2.5) | `test_state.py::test_a_key_is_created_only_by_the_lanes_that_the_machine_names`, `test_state.py::test_a_child_row_created_from_two_lanes_is_created_with_a_transition_of_its_parent`, `test_state.py::test_a_wrong_declaration_is_refused` |
| 4 | Section 15 said "do not retry `StatementError`", against the stream's rule for a failed table | the sentence of section 15 | |
| 5 | The evidence files named a command that had not run and no commit | each result names its command and commit (`conftest.run_command`, `run_commit`); one recorded run per MotherDuck file (section 16) | |
| 6 | The threads of a retired connection could not be counted from outside | `md.waiting_calls()` (section 4) | with item 15 |
| 7 | The full matrices made the default run slow | marker `d_full` (section 16) | |
| 8 (F0) | In 1 of 30 runs a take-over at `stream.fenced` returned and the old empty block then reported a successful `COMMIT` | **a breach of SPEC 4.2**: both committed. Found by a campaign (section 14.4), caused by DuckDB not recording a write of an unchanged value after a checkpoint; fixed by a take-over and a registration that write the fence counter `n` | `test_duckdb_does_not_record_a_write_of_an_unchanged_value_after_a_checkpoint`, `test_a_take_over_right_after_a_checkpoint_keeps_the_fence`, `test_a_registration_right_after_a_checkpoint_cannot_commit_after_a_take_over`, `test_a_take_over_at_every_point_of_a_lane_transaction_keeps_the_fence`, `test_md_fix_round.py::test_a_take_over_at_every_point_of_a_lane_transaction_on_motherduck`, `fence_campaign.py` |
| 9 (F1) | `Transaction conflict: …` was `StatementError` | `Conflict` (section 5) | `test_errors.py::test_every_transaction_conflict_text_that_motherduck_gave_is_a_conflict`, `test_ddl_on_one_lane_while_another_lane_writes_is_a_conflict_and_runs_again` |
| 10 (F2) | Transport words inside other errors (48 of 168 texts) | the programming family by class; gRPC codes in capitals, whole words (section 5) | `test_errors.py::test_every_misleading_text_that_motherduck_gave_is_refused_not_retried`, `test_errors.py::test_the_misleading_statements_on_local_duckdb_are_refused_not_retried`, `test_the_misleading_statements_through_the_layer_are_refused` |
| 11 (F3) | `IOException` was transport by its class | transport only by a phrase (section 5) | `test_a_missing_file_is_refused_and_not_a_lost_connection` |
| 12 (F4) | The state module accepted a plain `Connection` | `LaneMisuse` (section 2.5) | `test_the_state_module_and_alerts_write_only_inside_a_lane_transaction` |
| 13 (F5) | Migrations compared only the last step's name | the name and hash of every step (section 2.4) | `test_a_changed_or_renamed_applied_step_refuses_to_start` |
| 14 (F6) | The stand-in take-over named three leaders | `AnotherRunLeads` (section 2.7) | `test_three_simultaneous_starters_have_exactly_one_leader` |
| 15 (F7) | Threads grew without a cap on a black-holed path; the counters undercounted; the fence statement and the registration waited for the block's deadline | counted, capped, the statement deadline (section 4) | `test_the_calls_that_were_given_up_are_counted_and_capped_per_connection`, `test_the_fence_statement_and_the_registration_wait_the_statement_deadline_not_the_blocks`, `test_md_fix_round.py::test_a_black_holed_path_bounds_every_call_and_the_calls_left_waiting` |
| 16 (F8) | `RETURNING b AS "Count"` with text was `StatementError` | never read as a count (section 2.2) | `test_a_returning_column_named_count_that_holds_text_is_no_count` |

Changes that the fix round made to tests of the first delivery: the tests that counted the statements of a
lane's first transaction now count the registration as `BEGIN`, `INSERT`, `UPDATE` and `COMMIT`
(`test_lane.py::test_an_unclassified_error_after_commit_was_sent_is_commit_unknown`, the lost-connection matrix,
`test_alerts.py::test_emit_now_passes_on_what_it_cannot_decide`,
`test_review.py::test_registration_writes_the_id_of_the_connection_that_runs_it`, and in
`test_md_procs.py` the slow commit of `takeover_case`, which now skips the registration's `COMMIT`); in
`test_md_procs.py` two assertions of outcomes the layer does not promise, which a run under load contradicted
(section 14.4); `test_lane.py::test_conflicts_are_per_column_which_is_why_the_fence_statement_writes_the_epoch`
uses the take-over as design 4.5 writes it for its negative control (`as_designed=True`); the test of
two creators on two lanes became the two tests of item 3; the known limit of the head rule moved to a class
outside the programming family; `InvalidInputException("Connection already closed")`, which no client path
raises (a closed connection is `ConnectionException`), is no longer sorted as transport.

Not this package's, for information (the second reviewer's E1): a swap (`DROP` and rename) on one lane and a
committed `INSERT` of another lane into the dropped table both commit, and the insert is lost with the table.
A DDL that drops a table does not conflict with a committed write of another lane to it (section 15).

### 14.4 F0: a take-over and an old run's transaction both committed

**What the second reviewer saw.** In 1 of 30 rounds of `e1d_shapes.py` (an empty block, a take-over at
`stream.fenced`) the take-over returned and the old block's `COMMIT` then returned success. The fence row was not
recorded, so it could not say whether both had committed or MotherDuck had answered OK for nothing.

**How it was told apart** (`tests/destination/fence_campaign.py`; every run is in `fence_campaign.json`). The fence row's counter `n` grows only when
a transaction of the lane commits, and the take-over does not touch it in design 4.5. So between a read before the
old block and a read after it, `n` grows by one exactly when the old block committed. Every round records the
fence row, every fence epoch, the leader row, the heartbeat's epoch, the old block's own trace (a row, a
transition) and both answers, and checks them (the module text). A diagnostic copy also read the committed fence
row and the server's listing of the old connection at the crash point, before the take-over started.

| Run (MotherDuck) | Code | Rounds | of them the reviewer's case (`nothing@fenced`) | Breaches |
|---|---|---|---|---|
| 1 (stopped at the pause) | `b7b02dd` (the fix round's first code; the take-over as designed) | 393 | 197 | 0 |
| diagnostic | `e671405` (as designed) | 235 | 235 | 1 |
| 2 (stopped when the cause was found) | `e671405` (as designed) | 681 | 341 | 8 (3 `nothing@fenced`, 3 `registered_relation@fenced`, 1 `transition@fenced`, 1 `row@fenced`) |
| 3 (stopped at round 1001 of 1040 for a pause) | `7193b39` (the fix) | 1001 | 501 | **0** |
| 4 (fix round 2: the counter only grows) | `50229d7` | RUN4_ROUNDS | RUN4_MAIN | RUN4_BREACHES |

**What it is: both committed.** In the diagnostic breach, at the crash point the committed `n` was the old value
and the server listed the old connection with its transaction open; the take-over then met **no** conflict (1
attempt, 0.37 s) and committed; the old block's `COMMIT` returned OK and `n` grew. Every breach of runs 2 and the
diagnostic had a take-over with 0 conflicts, and every round that passed at `fenced` or `commit_sent` had at least
one. In 5 of the 8 breaches of run 2 the old block had written data (a row, a transition, a registered relation),
and that data was committed after the take-over. This is a breach of SPEC 4.2. (Answer OK for a transaction that
committed nothing was never seen.)

**Why.** DuckDB 1.5.6 does not record a write of an **unchanged value** into a column that has had no update
since the table was written or checkpointed; another transaction that then writes the column does not conflict
with it. With a changed value, or after an earlier update of the column, the conflict is detected. Local DuckDB
shows it deterministically
(`test_fix_round.py::test_duckdb_does_not_record_a_write_of_an_unchanged_value_after_a_checkpoint`), and so does
MotherDuck on a fresh table: the fence statement against the take-over as designed, 6 of 6 both committed; the
registration of the first form of item 2 (`SET epoch = epoch`) against it, 6 of 6; the control with a changed
value, 0 of 6 (`tests/destination/probes/probe_unchanged_write.py`, `unchanged_write.json`, local and MotherDuck
alike). The fence statement writes the unchanged `epoch` on purpose (section 6), and the
take-over of design 4.5 writes `epoch` alone. The fence row normally has updates (every take-over and every
lane transaction writes it), so the hole opens only after MotherDuck checkpoints: about 1 round in 100 in run 2,
under the load of other tests; run 1 had none in 393.

**The rule the design rests on** (the second reviewer's map, `m1b_map.py` under `review_d2/progs/`: 768 cases
locally, 672 on MotherDuck). Transaction A writes and stays open; then transaction B writes the same column of
the same row with a changed value. "Recorded" means that B conflicts.

| A's write | The column has had no update since the table was written or checkpointed | The column was updated since (any row, committed or rolled back) |
|---|---|---|
| an unchanged value (`= literal`, `c = c`, `c + 0`), also inside a statement of many rows or one that changes other rows | **not recorded: both commit** | recorded |
| a changed value | recorded | recorded |
| changed and changed back (`n + 1`, then `n - 1`) | recorded | recorded |
| unchanged, after a changed write in the same transaction | recorded | recorded |
| only another column of the row | not recorded (conflicts are per column) | not recorded |
| `DELETE` plus `INSERT` of the row | never conflicts, in any order | never |
| any of these when B wrote first (still open, or committed after A's snapshot) | A fails | A fails |

- A `CHECKPOINT` cannot fall between the two writes locally (it is refused while a write transaction or an older
  snapshot is open). An explicit `CHECKPOINT` on MotherDuck answers OK and resets nothing (the only difference of
  the two maps). MotherDuck's own background checkpoint came about every 62 s, never while a write transaction
  or an older snapshot was open, and after one the take-over as designed committed next to an old fence 3 of 3
  times while the take-over as built excluded it 3 of 3 (`m3_md_bgckpt.py`).
- **So two statements exclude each other only through a column that both change.** The design never relies on an
  unchanged write, nor on changed-and-changed-back (it is recorded today, but no documented DuckDB rule keeps it
  so; fix round 2 removed the one place that used it). DuckDB's concurrency documentation does not describe this
  behaviour; whether to report it upstream is the owner's decision.

**The fix.** The take-over also raises the counter `n` of every fence row (`UPDATE _cdc.fence SET epoch = :seen + 1,
n = n + 1`; `cdc_v2.testing.destination.TAKEOVER_FENCE_SQL`). Until fix round 2 it put `n` back with a second
statement (`n - 1`); that net-zero write is recorded today, but no documented DuckDB rule keeps it so, and a
future optimisation of the same kind could drop it, so the counter now only grows. The fence statement always
changes `n`, so the two meet on `n` in every order and after any checkpoint: on MotherDuck on a fresh table 0 of 6
rounds with both committed, in both orders, and the same for the registration (`unchanged_write.json`); locally after a checkpoint 5 of 5, against 5 of 5 breaches with the code of
`9e389cc` (`fix_round_old_code.json`). The registration raises `n` the same way (section 2.3). `seq` therefore
has gaps at take-overs and registrations (section 2.5). Tests: the two "right after a checkpoint" tests of
`test_fix_round.py`, each with its negative control; the campaign's runs 3 and 4 above.

**What it means for the rest of the design** (finding 24): every exclusion by a write-write conflict must rest on a
changed value (section 6). Package L's take-over must write `n` as above (design 4.5 does not), its heartbeat and
release write changed values already. A transition from a state to itself, and an `expect` on a column the
transition does not change, do not exclude a concurrent writer after a checkpoint (section 2.5). The leadership
fuzzer should take over at `<lane>.fenced` and right after a `CHECKPOINT`.

### 14.5 Fix round 2

The coordinator's re-check of `161d847` and a second attack by the second reviewer on the fence after the fix
(1,440 cases of the map above, 620 campaign rounds on MotherDuck and 400 locally: 0 breaches with the fix, the
control of design 4.5 breached every time). Six items; every one has a test that fails on the code of `161d847`
(run against that code with the new test support, as in section 14.3).

| Item | What was wrong | What was done | Tests |
|---|---|---|---|
| 1 | The take-over and the registration put `n` back (`n - 1`): a net-zero write that no documented rule keeps recorded | the counter only grows; `seq` is strictly increasing per lane with gaps at take-overs and registrations (sections 2.3, 2.5, 14.4) | `test_fix_round.py::test_the_take_over_and_the_registration_never_put_the_counter_back`, `::test_seq_is_strictly_increasing_per_lane_with_gaps_only_at_take_overs_and_registrations` |
| 2, 6 (N3) | `expect` on a column not in `values` (written `col = col`) and a self-transition without `values` excluded nothing (5 of 5 both committed, across two lanes of one epoch) | refused with `LaneMisuse` (section 2.5); `check_machine` takes `self_values` | `::test_a_condition_on_a_column_the_transition_does_not_change_is_refused` |
| 3 | `test_md_procs.py` accepted either outcome for a transaction that MotherDuck may end while it is frozen | the time MotherDuck keeps a frozen or killed client's open transaction is recorded and asserted below the take-over's deadline of 45 s (`takeover_open_tx.json`, key `without_interrupt`, `frozen_transaction_kept_s`); the same-epoch case accepts the lost transaction only after a freeze of at least 25 s | `test_md_procs.py::test_takeover_in_the_middle_of_an_open_transaction`, `::test_two_processes_with_the_same_epoch` |
| 4 (N1) | The local stand-in of the server's interrupt swallowed an interrupted `ROLLBACK` and marked the transaction rolled back; it stayed open and two simultaneous take-overs both failed (9 of 30 local rounds) | one interrupt and its `ROLLBACK` at a time per connection; an interrupted `ROLLBACK` is tried again; the transaction is marked rolled back only after it succeeded | `::test_an_interrupt_that_lands_in_the_local_roll_back_does_not_leave_the_transaction_open`; the reviewer's shape with `-m d_full`: `::test_two_simultaneous_take_overs_while_an_old_run_stands_at_its_fence_have_one_winner` |
| 5 (N2) | Autocommit writes carried no fence: after a take-over an old run's plain connection inserted data and set its fence row and the leader row back, and all committed | outside a lane only reads run; no caller may `UPDATE` the leader tables; package L's heartbeat and release are statements of the layer (section 2.1, contract notes 47 and 48) | `::test_an_old_runs_plain_connection_cannot_write_after_a_take_over`, `::test_the_heartbeat_and_the_release_are_statements_of_the_layer`, `test_lane.py::test_leader_rows_cannot_be_deleted_or_inserted`, `test_static.py::test_no_text_outside_the_layer_updates_the_leader_or_heartbeat_row` (a guard: no such text existed) |

The second reviewer's liveness note stands: a registration held between its `UPDATE` and its `COMMIT` cannot be
interrupted (its id is not committed yet); a take-over waits for it, bounded for a frozen client by the time
MotherDuck keeps its transaction (item 3).

## 15. What the callers must know

For packages A, L, S, V, B and the integrator. Each line is backed by the section it names.

**Every caller**

- A deadline is a `cdc_v2.deadline.Deadline`, not a number (contract note 1).
- A DuckDB database that you need to compute with is opened as `duckdb.connect(":memory:", …)`, with that
  literal. Any other `duckdb.connect` outside the layer, and any SQL text there that attaches a database or
  names `md:`, fails the static tests (contract note 37, finding 23). The destination is reached with
  `md.connect` only.
- **Write the columns your condition reads, with a changed value,** when the statement must conflict with
  another writer; a write of an unchanged value is not seen by the other writer after a checkpoint (section 6,
  section 14.4).
- Follow the run-again rule of C4: read what the transaction depends on inside the transaction, and move it
  with `execute_one` and a condition on what was read. Section 8.5 shows what happens without it.
- After `CommitUnknown`: wait about 2.5 s, then read inside a new transaction and decide by what is there. Never
  assume either outcome. The wait is no proof (finding 4); the condition of the next write is.
- `Conflict` can come again. After an interrupt the row is free after about 1 s; without one, after about 29 s
  (sections 7 and 8.3). Try again with a pause and a limit.
- A statement that the destination refuses (a conversion error, a constraint error, a name that does not
  exist, text that does not parse, a file that does not exist) raises `md.StatementError`, a
  `DestinationError`. The transaction was rolled back. Never put it into the transport retry loop with
  `Conflict` and `ConnectionLost`; the stream counts it as a failure of the table, and runs the group again up
  to `apply.max_table_attempts` before the table leaves the stream (design 3). Read `error_class` and
  `statement`; both are safe to log and to put into an alert. The client's text is not available, on purpose
  (section 5; SPEC 8.3).
- **To exclude another writer, change a column that it also changes.** `state.transition` refuses `expect` on a
  column that is not in `values` and a transition from a state to itself without `values` (fix round 2, N3);
  put into `values` a column whose value the transition changes. A write of an unchanged value excludes nothing
  after a checkpoint (section 14.4).
- **Write only in a lane transaction.** Outside one, `Connection.execute`, `fetchall` and `fetchone` take reads
  only; a statement that writes is `LaneMisuse` (fix round 2, N2). Never `UPDATE` `_cdc.leader`, `_cdc.fence`
  or `_cdc.heartbeat`.
- `seq` in `_cdc.transitions` is strictly increasing per lane, with gaps at take-overs and registrations; check
  uniqueness and order, not the absence of gaps.
- `Conflict` includes DDL on a table that another transaction writes (`Transaction conflict: …`, section 5).
  After it, read the table's schema again before the statements are built again: a column may have been added
  or changed (the spike's question 4).
- **A DDL that drops a table does not conflict with a committed write of another lane to it.** A swap (`DROP`
  and rename, SPEC 3.2) on one lane and an `INSERT` of another lane into the old table, committed first, both
  commit, and the inserted rows are lost with the dropped table (the second reviewer's E1). Packages B and S
  need a rule for it (serialise the swap with the table's stream writes); the layer does not prevent it.
- A block can fail before the caller's first statement: its connection's registration (section 2.3) gives
  `FenceLost` after a take-over, `Conflict` while a take-over is open, `ConnectionLost` or `DeadlineExceeded` on
  a bad network. Treat them as for any other step.
- On a network that answers nothing, a connection gives up at most `max_waiting_calls` calls; after that every
  call fails at once with `ConnectionLost` until one returns (section 4). Retry with a pause, as for any
  `ConnectionLost`.
- `tx.execute` returns a count only for `INSERT`, `UPDATE`, `DELETE` (and `TRUNCATE`) without `RETURNING`. It
  returns `None` for `MERGE`, `CREATE TABLE … AS`, `COPY` and for every statement with `RETURNING`; read the rows
  of `RETURNING` with `tx.fetchall`. `tx.execute_one` with `RETURNING` is `LaneMisuse` (section 2.2).
- `tx.fetch_arrow` gives a complete table; a large result is in memory. `tx.register` takes a table or record
  batches, never a reader, and a name of lower-case letters, digits and `_` of at most 63 characters. The name
  is gone when the block ends (section 2.2).
- The layer sets `TimeZone = 'UTC'` and `arrow_lossless_conversion = true` on its connections, for the session.
  Do not set them on a lane. On a connection of your own to the same database use `SET SESSION`: a plain `SET`
  reaches the other connections of the process to that database (finding 21).
- Drop what a transaction created table by table, then the schema. `DROP SCHEMA … CASCADE` in that case ends as
  `CommitUnknown`, and nothing is committed (finding 20).
- Declare who creates the rows of a machine (`creators`: lane names, or `state.FRESH_IDS` for fresh random
  ids). A machine whose rows are created from several lanes creates each child row together with a transition
  of its parent row, which every creator writes (section 2.5). Change a state only inside a lane transaction.
- `state.transition(…, values=…, expect=…, detail=…)`: set the data of the row in the same `UPDATE`, put the
  values the transaction read into `expect` (a changed value is `Conflict`, the run-again rule), and keep
  `detail` to identifiers, positions, counts and durations. Order the rows of `_cdc.transitions` of a lane by
  `seq`; `changed_at` is not the order of commit (section 2.5).
- A log line `alert` or `state transition` with `committed: False` says what a transaction wrote, not what
  holds. The tables say what holds (sections 2.5 and 2.6).
- A block whose deadline has passed gets `DeadlineExceeded` before `COMMIT` and commits nothing (section 4). A
  run that was frozen for longer than its block deadline meets this first, before `FenceLost`
  (`takeover_open_tx.json`, key `without_interrupt`).
- Never read the clock with a bare `SELECT`. Use `tx.clock_us()` or `conn.clock_us()`, or write `md.clock_sql`
  inside a statement on a MotherDuck table (section 10).
- Read a `TIMESTAMPTZ` column with `epoch_us(…)` or a cast when you read rows; Arrow data needs neither
  (section 10).
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

- **The take-over must raise the counter `n` of every fence row** and never put it back:
  `UPDATE _cdc.fence SET epoch = :seen + 1, n = n + 1` (section 14.4; `cdc_v2.testing.destination.TAKEOVER_FENCE_SQL`).
  With the text of design 4.5 (`SET epoch = :seen + 1` alone) an old run's transaction commits after the
  take-over whenever MotherDuck has just checkpointed the fence table. The bootstrap's copy of the statements
  needs the names `register_fence`, `heartbeat` and `release` too (`schema.NAMED_STATEMENTS`).
- **Heartbeat and release are the layer's** (`md.schema.heartbeat`, `md.schema.release`; contract note 48):
  callers may not update the leader tables (fix round 2, N2). The coordinator's re-check adds that release
  and take-over must change one common column (a counter `v` in the leader row is L's change of the frozen
  shape): today the take-over writes `state = 'leading'` over `leading`, an unchanged value.
- The take-over must try again after a conflict that follows an interrupt: 1 or 2 further attempts were seen
  (section 7).
- Call `md.interrupt_earlier(conn, new_epoch)` once more **after** the take-over won (finding 2).
- `interrupted` counts connections that held something. It does not say whether the old run is alive
  (finding 8).
- Test the heartbeat statement and the leader statements against the take-over in every order, as
  `tests/destination/probes/probe_fence_columns.py` does for the fence row (section 6). This package tested only
  the fence row.
- The heartbeat connection is no lane. Register it with `registry.register(conn, "control", epoch,
  retry_conflict=True)` before its first write and after every change of `conn.generation` (contract note 15;
  section 2.3). Its registration can make an open control-lane transaction end with `Conflict`.
- The take-over decides once from which epoch it takes over, and a loser exits when the leader row moved past
  that epoch under another run's id (`AnotherRunLeads` in the stand-in, section 2.7). Copy that rule.
- The supervisor (contract C11) reads `md.waiting_calls()` and ends the process past its bound (section 4).
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

**The default run** needs no network: `uv run -q pytest -q`. On 2026-11-11, on commit `50229d7` (fix round 2, with
`origin/main` merged): 1415 passed, 4 skipped, 742 deselected, 1 xfailed in 106 s. The tests of this package alone
(`uv run -q pytest -q tests/destination`): DEFAULT_D_COUNT. Both are in `final_runs.json`.

**The rest of the full matrices** (marker `d_full`, fix round item 7): the default run keeps one case per fault
site of the deadline matrix of `test_lane.py` (the case of `BEGIN` is left out), the door of
`test_state.py::test_every_pair_of_the_example_machines` (the job is left out), and the empty block at every point
of `test_fix_round.py::test_a_take_over_at_every_point_of_a_lane_transaction_keeps_the_fence` (the other six
shapes are left out). They run with `uv run -q pytest -q -m d_full tests/destination` (8 tests, 3 s). A
MotherDuck run (`-m md`) keeps them. The marker is registered in `tests/destination/conftest.py`.

**The MotherDuck tests** (marker `md`) need the token that `tools/with-env` supplies. They create and drop only
databases named `cdc_v2_t_d_<random>`. Leave `CDC_V2_MD_PREFIX` unset or set it to `cdc_v2_t_d_`. Every result
in an evidence file names the command that wrote it and the commit (`commit`, with `+changes` when the files the
tests use differ from it; fix round, item 5). One recorded run per file, without `-k` (fix round 2):

| Command | Tests | Recorded run (2026-11-11 UTC) | Earlier recorded runs |
|---|---|---|---|
| `tools/with-env uv run -q pytest -m md tests/destination/test_md.py` | 31: what MotherDuck answers, deadlines, the registry, the clock, timings | MD_RUN_TEST_MD | 31 passed in 358 s (`7193b39`) |
| `tools/with-env uv run -q pytest -m md tests/destination/test_md_again.py` | 92: the T0 tests of lanes, leader tables, migrations, state, alerts, the points from the review of apply and both fix rounds, with a MotherDuck database | 92 passed in 865 s (`50229d7`) | 88 passed in 893 s (`7193b39`) |
| `tools/with-env uv run -q pytest -m md tests/destination/test_md_fix_round.py` | 2: the black-holed path (section 9.4), the take-over at every point (14 rounds) | 2 passed in 103 s (`50229d7`) | 2 passed in 113 s (`7193b39`) |
| `tools/with-env uv run -q pytest -m md tests/destination/test_md_procs.py` | 10: real processes (sections 8 and 9) | MD_RUN_PROCS | 10 passed in 1212 s (`7193b39`) |

MD_RUN_NOTE

**The campaign of item 8**: `tools/with-env uv run python tests/destination/fence_campaign.py --rounds 1040 --log
<file>` (about 3 to 5 s a round; its summary goes to `fence_campaign.json`; SIGTERM stops it and drops its
database). **The probe of its cause**: `tools/with-env uv run python tests/destination/probes/probe_unchanged_write.py`
writes `unchanged_write.json`.

**Numbers that vary.** Times, the counts of conflicts, attempts and interrupts, which process won a race, and how
many killed commits landed without an answer move from run to run; the tests assert only what the layer
promises and record the rest. Where this document gives such a number from more than one run, it gives the
range and names the runs.

**After a run** no test database may be left. This reads only the ledger (section 2.7) and prints `[]`:

    uv run python -c "from cdc_v2.testing import destination as td; print(td.leftover_databases('cdc_v2_t_d_'))"

A name that it prints belongs to a run that was killed. Drop it with
`tools/with-env uv run python -c "from cdc_v2.testing import destination as td; td.drop_database('<name>')"`.

**The probe** of section 6: `tools/with-env uv run python tests/destination/probes/probe_fence_columns.py`. It
writes `fence_columns.json`.

**The document of the state machines**: `uv run python -m cdc_v2.state > docs/state_machines.md`.
