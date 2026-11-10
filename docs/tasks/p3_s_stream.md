# Task: package S — stream

Read `docs/tasks/README.md` first; its rules bind you.

You build the part that carries the promise of the whole product: every committed source transaction reaches the
destination exactly once, whole, in commit order, whatever is killed and whenever. You take frames from the
engine (package E), turn them into transactions and groups, apply each group through package A inside one
`stream`-lane transaction of package D that also writes the applied position, and only then tell the engine that
the position is durable. Around that you build the supervisor, the pipeline process and the loader of the one
configuration file.

Contracts: `docs/design/p3_plan.md` C0, C1, C2, C3, C3a, C4, C5, C7, C8, C10, C11, C12, and section 6.2 "S" and
"V and S". Design: `docs/design/core.md` sections 1, 2, 3, 4.3, 4.6, 6.1, 6.4, 6.5, 8 (the attach step), 9 and
open points on your path. SPEC items you own the gate tests of: **1.1, 1.2, 1.3**. SPEC items your code decides
and must not make impossible later: 1.4 (end to end), 4.0, 4.3, 4.4, 4.5, 4.6, 5.1, 5.2, 5.4.

A second reviewer reads everything you write on the commit and acknowledge path. Write it so that a reader can
check each rule of design section 3 against one place in the code.

## Read as well

- The documents of the packages you build on, as delivered: `docs/design/engine.md` (frames as built, the
  `event_count` rule, what the engine does on back-pressure and on end of input), `docs/design/destination.md`
  (lanes, errors, the run-again rule, migrations, alerts, the transition helper), `docs/design/apply.md`
  (`apply_group`, `ApplyCounts`, `unresolved`, `ApplyInvariantError`), `docs/design/source.md` (connections,
  `slot.*`, `publication.*`, `heartbeat.beat`, the role split). Where one of them differs from the contract, the
  document of the merged package is what exists; say so in your report.
- `docs/oracle.md` and `docs/testing/harness.md` (crash runner, `kill_later`, fault sites, the TCP fault proxy,
  the byte-stream relay).
- `docs/spike/md_semantics.md`: the late commit (up to about 2 s), the open transaction of a dead client (about
  30 s), write-write conflicts and which are not detected, the cost of the apply shapes on an F16.
- `docs/research/debezium.md`: restart and re-send of whole transactions, the offset rules, the heartbeat.
- `docs/wal_hazards.md`: every hazard whose owner is the stream (search for "stream" and "ack").

## From the engine as built — build on it

Package E is built and reviewed. `docs/design/engine.md` is the full account; where it differs from what this task
says elsewhere, these points and that document hold.

- A transaction is the frames from `BEGIN` to the next `END`, in stream order. Never use Debezium's transaction id:
  it differs on `BEGIN`, on every row and on `END`. `positions.txId` is the 32-bit xid and is used only by the cut
  on a primary.
- `seq` numbers every record, `BEGIN`, `END` and `HEARTBEAT` included, from 0 for every engine life.
- `HEARTBEAT` frames may arrive anywhere, also inside a transaction; they are not boundaries and they never move the
  applied position (plan C2, design section 2: a heartbeat can carry the commit position of a transaction whose
  `END` is not framed yet; a pipeline that adopted heartbeat positions lost transactions in the second review of
  package E). The applied position is always the `ack_position` of an `END` you have applied, and `DURABLE` is sent
  only for such a position. Show it by test: a stream whose `HEARTBEAT` carries a position above the last `END`
  changes nothing.
- `END.event_count` = rows in `ROWS` frames + `TRUNCATE` frames + transactional `MESSAGE` frames between `BEGIN` and
  `END`. A key change counts two (a delete and a create). The per-collection counts name a message collection by a
  text with an unstable ending; match those by prefix. An `END` with count 0 is a legal empty transaction (for
  example one that touched only tables outside the include list), and its `ack_position` may move the applied
  position.
- Send no `DURABLE` before the attach decision; after it, send `DURABLE(applied position)` at once. The engine keeps
  the position and applies it to records that the server delivers again.
- `HELLO` comes with the engine's first batch. On an idle source that batch is Debezium's own heartbeat record,
  within `heartbeat.interval.ms` of the start of streaming, also while the server decodes a large unconfirmed
  transaction again. The wait for `HELLO` is part of the attach step's declared wait.
- After a restart the server may decode an unconfirmed large transaction again for minutes before the first row
  arrives (36 s for 10M rows locally, `docs/spike/local_streaming.md` section 5). That is a declared wait, not a
  fault, and no silence rule may end the engine for it.
- Frames prove that the engine is alive, and for a short while that its connection is: in the black-hole test
  `HEARTBEAT` frames kept arriving for 8.2 s after the hole opened and then stopped; neither Debezium nor the driver
  ended within 150 s; the server ended the walsender after `wal_sender_timeout` (59.8 s). Three rules:
  1. While you are reading and no commit or declared wait is in progress, no frame for `stream.frame_silence`
     (default 30 s) tears the engine down. This is an early signal only, since the 8 s depend on socket buffers.
     During a re-decode after a restart `HEARTBEAT` frames keep coming, so this rule does not fire there.
  2. Package V's periodic check finds a holder that is not our `HELLO` pair, within `wal_sender_timeout` +
     `slot.verify_interval`.
  3. The marker rule of SPEC 4.6, counted **only while no transaction is open and after the first `BEGIN` or the
     first marker of an engine life**: a marker committed behind a 10M-row transaction cannot arrive within 60 s,
     and tearing the connection down there would force the same decode again, for ever.

  State the bound of each case (idle; inside a transaction) and test with package H's proxy.
- `frames(deadline)` ends at the deadline without an exception; `next_frame` raises `DeadlineExceeded`;
  `EngineExited` is an `EngineBroken`; `start_position` is only echoed in `HELLO` (Debezium always starts from the
  slot's confirmed position).
- A 10M-row transaction arrives as about 1,200 `ROWS` frames of at most one Debezium batch each.
- `FATAL` carries the position of the last record the engine handed over, never a source value, and, when Debezium
  names the event that failed to decode, `failing_position`: the `lsn` and `txId` of that event, the same on every
  attempt. It names a schema and table only when the failure is in our handler. "Fails again and again at one
  position" (section 7) compares `failing_position` when it is there, else the position of the last record.
- A value that the Postgres driver cannot parse never reaches our converter (`docs/design/engine.md`, section
  10). An interval whose hour part exceeds 2^31 ends the engine with `FATAL` in Debezium's decoding thread, at the
  same transaction on every start; nothing of that transaction beyond its `BEGIN` is framed. After
  `stream.max_attempts_at_position` failures at one failing position: the candidates are the streamed tables with
  a column of a type that section lists (now `interval`). Restart the engine with all candidates left out of
  `table.include.list`; if the failing transaction then passes (`BEGIN`/`END` with count 0), halve the
  candidate set with one restart per step. Probing restarts apply nothing and send no `DURABLE`, so no table loses
  anything. Then run with only the culprit left out: quarantine it (routing `skip`, a backfill request after a gap,
  the slow retries of SPEC 4.0). With no candidate, or if leaving them out does not help, take the last line of
  section 7 (move past the place, backfill every table, alert). Test both with a real value on Postgres 18.
- An infinite interval (`'infinity'`, `'-infinity'`; Postgres 17 and later) does not fail: in a scalar `interval`
  column it arrives as a zero interval, with nothing that tells it from a real zero (an interval array keeps it).
  The rule is in plan C3 ("Ambiguous zero interval"). Your part in P3: for every batch of a table whose plan has
  ambiguous columns, count the rows that `transform.ambiguous(plan, rows)` marks (counter `interval_ambiguous` per
  table in `_cdc.stream_state` or beside it), alert once per table and engine life, and apply the delivered value.
  The repair is built in P4 on your group loop; what it needs from you now is in section 9 ("Seams for others").

## From the source package as built — build on it

Package P is built and reviewed. `docs/design/source.md` is the full account.

- `heartbeat.beat(primary, deadline, slot=…)` returns the position after its own commit. A beat has been decoded
  once an `END` with `ack_position ≥` that position has arrived. Run `heartbeat.check` (runtime role) before
  streaming; `heartbeat.ensure` needs the setup role and belongs to deployment, like `publication.ensure`.
- `cdc_v2.heartbeat` is a published source table. Route it to `skip`: it is never written to the destination, and
  only the position of its transactions is used.
- The slot holder that `slot.facts` reports and `slot.terminate_holder(node, name, expect=…)` takes is the same pair
  (process id, backend start in microseconds) as the engine's `HELLO`.
- The replication connection of the engine and the read connections of package P are two sessions. Both pin the
  same text-output settings (`IntervalStyle=postgres`, `DateStyle=ISO`; plan C3), so a value that travels as
  Postgres's text is the same text on both paths. The transform takes the stream's form from the engine's document
  and the backfill's form from the source's; you pass both through untouched.
- The slot-creation nudge is a non-transactional logical message with prefix `cdc_v2.nudge`. It lies outside the
  engine's default `message.prefix.include.list` (`cdc_v2_.*`) on purpose: it must never reach you.
- In primary mode, hand the `GapMark` at `group_end` (C9) to `tables.confirm`: on a primary a catalog observation
  is usable for a group only under the rule in package T's task.

## From the transform as built — build on it

Package Y is built and reviewed. `docs/design/transform.md` is the full account (section 1: the interface as
built; section 2: `WideningNeeded`; section 5: contract notes).

- The plan is built from the catalog alone. A `SCHEMA` frame is only checked against it: column count, position,
  name, transport form, **key flag and key positions**. `PlanMismatch.differences` lists each difference as
  (position, field, catalog side, frame side). The backfill's plan and the stream's plan of one table are the same
  value with the same hash; the hash holds the server's major version.
- A `Transformer` owns one DuckDB connection and belongs to one thread. Its spill area (`temp_directory`,
  `max_temp_directory_size`) is given at construction and cannot be changed later; `threads` and `memory_limit`
  come from your settings.
- **Concatenate before the transform** (plan C3). Many small batches cost one statement each for some type
  families (about 1 ms per batch for a table with an `interval`, `uuid`, `timetz`, unconstrained numeric or money
  column). Concatenate the consecutive `ROWS` batches of one table and schema version within a group, in memory
  only, in stream order, bounded by `apply.max_group_bytes`, and transform the result once. The transform still
  runs before anything is staged, spilled or sent. Show the cost per transaction at 2,000 small transactions per
  second (SPEC 5.3) with and without it.
- **Tables that are not streamed** (plan C3, C10). A table whose plan has an `ambiguous_key`, and a table without
  a key whose plan has an `ambiguous` column, get routing `skip` and one alert per table that names the reason;
  their rows are counted and dropped. Decide it where a table is first routed to `streaming` and again whenever
  its plan changes. The mode `full_refresh` that will carry such a table is P4.
- `transform.ambiguous(plan, rows)` takes the batch in transport form, before `transform.stream`.
- JSON and enums arrive as `VARCHAR` in P3; nothing of yours depends on it.

## From the key rule (package K) — build on it

Rule K (`docs/design/core.md`, start of section 6): a table has a key when its primary key is not deferrable and
its replica identity is `DEFAULT`, `FULL`, or `USING INDEX` on the primary key's own index. Every other
publishable table is a table without a key. The catalog is the one authority, and the engine is told which
tables are without a key (`docs/design/source.md` section 3 and `docs/design/engine.md`, as package K delivered
them; take the names of the fields from there).

- At every engine start you read the catalog and give the engine the list of tables without a key.
- At every `SCHEMA` frame the key flag and the key positions must equal the catalog's; `transform.plan` makes
  the comparison. A difference is never applied: nothing of that table is applied by a key that the catalog does
  not confirm. The stream reads the catalog again and starts a new engine with a fresh list (a table that
  appeared after the start, SPEC R20, or a key that became deferrable while the engine ran). Since no `DURABLE`
  was sent for the group, the new engine delivers it again. If the frame and the catalog still differ after
  `stream.max_key_restarts` (default 2) at one position, it is a failure of that table (section 7).
- A table that the catalog refuses (`identity_problem`: for example a deferrable primary key without `REPLICA
  IDENTITY FULL`) is not streamed: routing `skip`, alert with the catalog's message. So is a table with a
  `name_problem` (a `"` in its schema or table name, H-051); it is never in the publication.
- Restart the engine with a fresh list also on package T's `key_changed`: a key made deferrable is a drop and
  re-add of the key, and whether Debezium then sends a new `SCHEMA` frame was not measured.
- **A FATAL that names a table** (`fatal.schema`, `fatal.table`; for example an update of a table without a key
  that arrived without an old image, after somebody changed its replica identity) is a failure of that table, not
  of the stream: routing `skip`, one alert and quarantine at once, with no loop of restarts; start the engine
  again without the table in its include list (`table.exclude.list` is a protected setting); the table comes back
  through a backfill after a gap once the catalog's `replica_identity_problem` is `None` again. Test it with a
  live change of replica identity while the stream runs.
- Gate workload (SPEC 1.4 names "swaps inside one transaction, under default and full replica identity"): one
  table with a deferrable primary key and `REPLICA IDENTITY FULL`, with a swap of two keys in one statement and
  with "insert a duplicate, then delete the old row" (`tests/pg_source/test_pg_keys.py` has both). It takes the
  path of a table without a key and must come out equal under the oracle. And one keyed table with a key that
  changes only its form (`float8` `-0` to `0`: the engine sends a key change).

## From the cut as built (package X) — build on it

Contract C9 (`docs/design/cut.md` 13.4). In primary mode the stream lane keeps the tracker: at each engine start
`GapTracker(unknown_below=next_xid_marker(primary))`; for each applied `END` `delivered_xid32(txId)`; once per group
`probe(probe_snapshot(conn))` on a primary connection of the stream lane (one statement); hand `mark()` to the
backfill (`backfill.on_group`) at every `P0` and `caught_up_lsn`, and to `tables.confirm` at `group_end`. The source
heartbeat write is also what settles a restarted standby (a standby that has just started can hide a commit until
it replays the next one; the takers refuse a cut until then). If the decoding replica is promoted, the stream
switches to the primary rule and the tracker starts again with `unknown_below`.

## What may be missing when you start

Packages E, D, A, P, Y, K and X are merged. Others are not. Build to their contracts and join when they arrive
(`git merge origin/main` into your branch):

- **L (leadership, C12).** If `cdc_v2.leader` is not on `main`, your tests take the epoch from a helper that
  performs the take-over statements of design 4.5 through package D, and the pipeline runs without the heartbeat
  thread. Whichever of you and L is merged second writes the join and its test (C12, last point).
- **V (slot continuity) and B (backfill)** come after you. You ship the two seams they fill: the attach step
  behind V's signature (C10) and the hook `backfill.on_group` with a stand-in (below).

## Resources

- Postgres: `tools/localpg --home ~/.cache/cdc_flight_v2/pg-s --port-base 55664`; a second pair
  `--home ~/.cache/cdc_flight_v2/pg-s2 --port-base 55672` for a second major version or a cluster you break.
  For the shared fixtures: `CDC_V2_PG_HOME`, `CDC_V2_PG_PORT_BASE`, `CDC_V2_MD_PREFIX=cdc_v2_t_s_`.
- MotherDuck through `tools/with-env` only; databases `cdc_v2_t_s_*` only.
- The engine with its pinned JRE and jars, as package E starts it. Heap as E's settings cap it.

## What to build

### 1. Configuration (`src/cdc_v2/config.py`)

One file configures a deployment. `config.load(path, environ) -> Config` fills the frozen settings dataclass of
every package (C0) and nothing else reads the file.

- Format: TOML, read with the standard library. One table per package's settings, named as the settings are
  named in the design (`apply.max_group_delay`, `leader.takeover_timeout`, `run.max_runtime`, …).
- No secret is in the file. A secret is named by the environment variable that holds it; the loader hands the
  packages a callable, never the value as an attribute (C7 has the same rule for the Postgres password).
- An unknown key, a wrong type, a value out of range, or an order that the design demands and the file breaks
  (heartbeat interval < stale age / 3, …; the owning package's dataclass checks it) is refused at load with a
  message that names the key and never prints a value of a secret.
- Defaults are the values of PLAN §7 and of the design. `docs/configuration.md` lists every key with its default,
  its unit and the SPEC item or design section it serves; a default-run test fails when a settings field exists
  that the document does not list.
- Durations and sizes have one spelling each (say which); a test reads an example file,
  `config/example.toml`, that sets every key.

### 2. Supervisor (`src/cdc_v2/supervisor.py`, C11)

- Components register with a name and report progress with a phase. `supervisor.progress() -> (ok, phase,
  progress_at)` is the only thing the leader's heartbeat reads: `ok` is false when the stream component has not
  reported within its deadline or a component is past its restart limit.
- `with supervisor.waiting(component, reason, deadline):` — a declared wait counts as progress until its
  deadline and not after it. Slot creation, the attach step, a commit in flight (deadline scaled by size, design
  section 3) and every retry loop against a source or destination that cannot be reached are declared waits
  with a reason that an operator can read in `_cdc.heartbeat.phase`. Without the last one, an unreachable source
  would stop the heartbeat and runs would replace each other every minute for nothing; say in your document how
  long such a wait may last and what ends it.
- A component that is silent beyond `supervisor.restart_after` (default 120 s, SPEC 4.5) is restarted: its
  resources are closed from outside (`engine.kill()`, the lane's connection interrupted and closed, the source
  connection closed), its thread is given a bounded time to return, and a new instance starts. A restart of the
  stream component always starts a new engine. A thread that does not return, or a component restarted more
  than `supervisor.max_restarts` times within `supervisor.restart_window`, puts the supervisor past its limit:
  `ok` becomes false, the heartbeat stops, the bootstrap's hard exit ends the run and the next run takes over.
  The pipeline never waits for ever and never needs an operator to restart it.
- Every blocking call in your code takes a `Deadline`. A default-run test walks your modules and fails on a call
  to a known blocking function without one (say how you find them).

### 3. Stream state (`_cdc` tables of your migration list)

- `_cdc.stream_state`: one row per slot name: `applied_lsn UBIGINT`, the commit time and transaction id of the
  last applied source transaction, the engine instance, counters, and what the rules below need. This row is
  the batch commit and acknowledge machine (plan 6.3): no second table.
- The routing map and the backfill request table of C10. A table row carries the source identity (schema, name,
  OID), the hash of the transform plan the destination table was built for, the routing, and the state of the
  table machine. Declare the table machine with `state.declare` in one module with the states you enact
  (`streaming`, `backfilling`, `quarantined`); package B extends that same declaration.
- The staging progress row and the tables of schema `_cdc_stage` (design 6.1, 6.4).
- The attempt counter of section 7 below.
- `stream.applied_position(conn_or_tx, slot_name) -> int | None` is the one reader of the applied position; the
  oracle's stand-in is replaced by it (C8). Nothing else reads the column by SQL of its own.
- First start (no row for the slot): decide and write down the order of "create the slot" and "insert the row
  with the slot's consistent point", with a crash point after each durable step, such that a crash between them
  leaves nothing that a later start gets wrong. No table can be `streaming` before the row exists.

### 4. From frames to groups (`src/cdc_v2/stream/`)

- **Assembly.** `BEGIN … END` frames become transactions. A transaction whose commit position is ≤ the applied
  position is discarded as a whole (design section 2); the discard is decided once, by position, in one function.
  The `END` count is compared with the records received, in total and per table as far as package E's frames
  allow; a mismatch is a protocol error (section 7). Records of one transaction stay in (`txn`, `seq`) order and
  are never reduced by you (C3a).
- **Transform on arrival.** Every `ROWS` batch goes through `transform.stream(plan, rows)` when it arrives, before
  it is buffered, staged or counted against a byte limit (C3; SPEC 8.3 depends on this place). A `SCHEMA` frame
  gives a plan through `transform.plan(source_table, schema_frame, server_version_num=…)`; the source table comes
  from package P's catalog. A plan whose hash differs from the one stored for the table is the schema change of C10:
  the table leaves the visible path in that group, with a backfill request. `WideningNeeded` is treated the same way
  in P3. `ConversionError` and `PlanMismatch` are failures of one table (section 7), never of the stream.
- **Groups.** A group is one or more whole transactions. It closes on the first of `apply.max_group_delay`
  since its first record, `apply.max_group_bytes`, a transaction that is larger than a group on its own, a
  planned hand-over, or an idle position step (below). `TRUNCATE` cuts a table's rows into segments inside the
  group (design 6.5) and reaches the destination through `apply.truncate` in order; the full behaviour of 1.5 is
  package T's, your part is that the order is right and nothing is lost.
- **Routing.** For each table of the group the routing map says: apply to the visible table, hand to the
  backfill (pending log), both, or skip. A table that is not in the map is not replicated; its rows are counted
  and dropped, with an alert once per table and start (the discovery wave of P4 replaces this).
- **Bounded memory (SPEC 5.4).** Every buffer has a byte limit. When the limit is reached you stop reading the
  engine's pipe; the engine's queue fills and the slot read stops (C2). Arrow data reaches DuckDB in bounded
  record batches. The local DuckDB of the transform and of the client-side reduction has `memory_limit`,
  a temporary directory inside the run's working directory and `max_temp_directory_size`; the fault site
  `stream.spill_full` stands for "no space left": the stream waits (a declared wait), alerts once, and goes on
  when space returns. It never crashes on it and never drops a record.

### 5. Commit and acknowledge (design section 3)

The eight steps exactly as the design numbers them, in one function per group that can run again from the start
(the run-again rule of C4): it reads `applied_lsn` inside the transaction, checks that it is the position the
group was built on, applies, moves the position with `execute_one … WHERE applied_lsn = :prev`, and leaves the
block (fence, `COMMIT`: package D).

- Crash points, declared by you: `stream.staged`, `stream.begun`, `stream.applied`,
  `stream.position_written`, `stream.ack_sent`; the lane's own (`stream.fenced`, `stream.commit_sent`,
  `stream.committed`) are package D's and `engine.acked` is package E's. Add a point after every other durable
  step you introduce (staging chunk, first start, reposition, quarantine, attempt counter) by the rule of C1.
- **Durable means seen.** `engine.durable(position)` is called only with a position whose commit you saw
  return, or that you read back as the stored applied position. Nothing else may reach that call; make it one
  call site.
- **`CommitUnknown`**: read the applied position in a new transaction after the wait that the lane's document
  prescribes for a late commit, and continue from what is there. Equal to the group's end: the group is done
  (go on with step 7). Equal to the previous position: run the group again. Anything else is a stop with an
  alert. Never assume.
- **`Conflict` and `ConnectionLost`**: run the group again from the start, at most
  `apply.max_group_attempts` times, building the statements again (a swap by package B changes the catalog).
- **`FenceLost`**: nothing is retried. The stream stops every lane it owns and the pipeline ends with the exit
  status of C12. There is one reaction to `FenceLost` in the process (package L owns it); you raise to it.
- **A failure of one table** (`ApplyInvariantError`, a value the destination refuses, `ConversionError`): the
  group is rolled back and run again; after `apply.max_table_attempts` (default 3) the table leaves the stream in
  a stream-lane transaction (state `quarantined`, routing `skip`, alert) and the group is applied without it
  (C10). Healthy tables never wait for it longer than those attempts take (SPEC 4.0). The stored rows and the
  slow retries of the full quarantine are P4.
- **Idle.** When the replicated tables are idle, the only transactions are those of the source-side heartbeat write
  (package P: one row in a published table of ours on the primary, every 10 s). Each arrives as `BEGIN` … `END`; its
  `END` moves the applied position in a group like any other, so the slot's `confirmed_flush_lsn` moves on an idle
  source and the marker rule of SPEC 4.6 has its marker. Decide with package P's interface whether the heartbeat
  table itself is written to the destination or only its position is used, and keep the cost of an idle hour small
  (one small destination commit per `stream.idle_position_interval` is the bound; several heartbeat transactions may
  share one group). Recognise the marker by position — an `END` whose `ack_position` is at or past the position
  `heartbeat.beat` returned — so that it is seen even when the heartbeat table is outside `table.include.list` (its
  transaction then arrives as an `END` with count 0).
- **Planned hand-over.** `finish_current_group(deadline)`: the group in flight is committed and acknowledged, no
  new group starts, and a transaction larger than a group that is still arriving is abandoned (its staging is
  left to the truncate of the next start). Returns the applied position.

### 6. A transaction larger than a group (design 6.4)

- Its rows go to `_cdc_stage` in chunks while they arrive. Each chunk is a `stream`-lane transaction that moves
  the staging progress row with `execute_one` (engine instance, source transaction, chunk number), so a chunk
  whose commit outcome was unknown is never added twice and a deposed run meets the fence at its next chunk.
- At its `END`: one destination transaction applies everything through package A's relation form of C3a, writes
  the position, fences and commits. Nothing of an unfinished source transaction is ever visible, the applied
  position does not move before that commit, and no later transaction overtakes it.
- Every start of the stream component truncates the staging tables and resets the progress row, in a fenced
  transaction, before the engine starts.
- Memory of the pipeline and of the JVM stays bounded while it arrives; show it with a measurement.
- The commit deadline scales with the rows (design section 3) and is a declared wait.

### 7. Never stuck at one position (SPEC 4.3, 4.0)

- An attempt counter, stored in `_cdc` and keyed by the applied position, is raised in a committed transaction
  at every start of the stream component and is reset by the first group that moves the position. It survives
  process and run.
- Causes that belong to one table take that table out (quarantine exit above), with a backfill request.
- A cause that belongs to no table (an `END` count mismatch, `EngineFatal` or a frame that cannot be read, again
  and again at the same applied position): after `stream.max_attempts_at_position` starts, the last line of
  defence. The engine is stopped, the slot is moved past the place (decide how — advancing our slot to the
  node's current position is the candidate; prove on Postgres 16 and 18 that the runtime role can do it on a
  standby and on a primary, or report what is needed), and `stream.reposition` (C10) sets the applied position
  to the slot's new `confirmed_flush_lsn` and requests a backfill of every table, in one stream-lane
  transaction, with an alert that says what was skipped. The stream then goes on. It never restarts for ever.
- `stream.reposition(tx, position, tables, cause)` is also what package V calls for a slot finding. It is the
  only function that moves the applied position backwards.

### 8. Attach, source heartbeat, start and stop

- **Attach** (C10; plan 6.2 "V and S"): `attach(ctx, deadline) -> Attached(engine, facts, finding)` in a module
  of its own. Your minimal version: read the fence row (a deposed run starts no engine), read
  `slot.facts`, terminate a holder of our slot name with `slot.terminate_holder`, start the engine at the
  applied position, wait for `HELLO`, repeat until our engine holds the slot or the deadline (at least 75 s
  after a take-over) passes; a declared wait. It checks the invariant `confirmed_flush_lsn ≤ applied position`
  and treats anything else, and any missing or invalid slot, as a finding that stops the stream with an alert.
  Package V replaces the body, not the callers.
- **Source heartbeat** (SPEC 4.4): a component of its own writes `heartbeat.beat` on the primary every 10 s on
  its own connection. The stream component checks that a beat written at time `t` is seen in the frames within
  `stream.marker_timeout` (default 60 s, SPEC 4.6); if not, the stream connection is torn down and built again
  through the attach step, and the slot and stream machine shows `stalled` meanwhile. In replica mode a standby
  that has stopped replaying produces the same silence: say what your code does then, and what it tells the
  operator. Two cases are findings with an alert and not a reason to reconnect: the heartbeat table is not in the
  publication (checked at attach with package P's check; reconnecting every 60 s would never end), and the beat
  itself cannot be written (privilege revoked, primary read-only or unreachable). In the second case only beats
  that were written count for the marker rule, the stream goes on, and the alert says that the slot's position
  stands still while the replicated tables are idle and that kept WAL grows with other traffic (SPEC 4.4).
- **`python -m cdc_v2.pipeline <configuration file>`** (C12): reads the environment of C12, opens the lanes with
  the epoch, runs every package's migration list as leader in fenced `control`-lane transactions, makes sure
  the publication exists as package P's document says (it never changes which tables are in it), starts the
  supervisor and the components, and ends with the exit status of C12. A `SIGTERM` is a planned stop of the
  group in flight within a deadline; every other stop is a crash, and correctness never depends on a clean
  stop (design section 1).
- No source row value is ever logged (C0): log lines and alerts carry tables, positions, counts and error
  classes. A default-run test plants a marker value and searches the logs, the alerts and the exception texts.

### 9. Seams for others

- `backfill.on_group(tx, group_end, change_sets) -> routing changes` (C10) is called before the position step.
  Your stand-in enacts a request by setting the table's routing to `skip` and leaving the request row pending;
  package B replaces the stand-in.
- `tables.confirm(group_end, tables, deadline) -> {table: same | changed}` (C10, the catalog gate) is called
  for every group before it is applied, at a point where the answer can still take a table out of the group. It
  runs beside the client-side work of the group, not after it, so that it adds one round trip to the source and
  no more; it is a blocking call with a deadline like any other. Your stand-in answers `same`; package T builds
  the real one. `tables.on_group(tx, group_end)` (C10) is called in every group and every idle position step,
  immediately before `backfill.on_group`; your stand-in does nothing.
- **Closing a group on a cut** (for the interval repair of plan C3, built in P4, and for nothing else yet): your
  group loop can be asked to end the group in flight exactly at a cut of package X — it goes on reading until the
  stream has passed the cut (it asks for a beat when the source is idle, so the wait is bounded by one beat and
  the stream's delay), takes every transaction the cut contains into the group and keeps the others for the next
  one — and to call a hook `values.on_group(group, cut) -> replaced values` before the apply. Build the loop so
  that this is a small addition: say in `docs/design/stream.md` where the hook goes and what the loop must
  already be able to do (read ahead of a group's end; split what it has read at a transaction boundary). You do
  not build the hook in P3.
- A test helper (under `tests/`, importable by packages F, V, B and T) that starts the whole local path for a
  set of tables: publication, slot, destination tables built from the transform plans, routing `streaming`, the
  pipeline as a process group, and handles to crash it at a named point, freeze it, and wait for a position.
  Until package B exists it registers only tables that were created empty before the slot.

### 10. What the fuzzers of round 2 need from you (plan 6.2, "From the review of F")

Package F points its fuzzers at your pipeline after you are merged. It must be able to see, from outside:

- **Never two writers.** The row that the batch machine appends to `_cdc.transitions` for every committed group
  carries the facts: epoch, run id, previous and new applied position, first and last commit position of the
  group's source transactions and their count, and a sequence that gives the order of commit. It is part of the
  group's own transaction: no extra statement and no extra round trip per group (SPEC 6.1). If package D's
  `state.transition` has no place for such facts, say so at once; do not build a second log.
- The applied position only through `stream.applied_position` (C8).
- **The spill area can be made tiny by a setting** (the transform connection's and apply's planning database's
  `max_temp_directory_size`), and "spill area full" is an alert through `alerts.emit`: the alert row is the fuzzer's
  proof that the fault had an effect. `ApplySpillFull` (apply's part): roll the group back. It is not a table failure:
  no attempt is counted and the table is never quarantined. Alert once. If the spill area holds nothing else, stage
  that table's change set and apply it in the relation form; otherwise wait (a declared wait) and run the group again.
  The transform connection and each planning thread get shares of one `max_temp_directory_size`.
- **Every source address is a setting, per node** (primary, replica, the engine's replication connection), so
  that each link can pass through package H's proxy. MotherDuck is reached only through the `md.*` fault sites.
- **Crash points** for every durable step (section 5), declared through `cdc_v2.crashpoints`; a point that is
  reached only on some paths says so in its declaration.
- Every SQL session of the pipeline at the source has an `application_name` with the prefix `cdc_v2_`; the
  replication session is found through the `active_pid` of `HELLO`.
- The quarantine exit and `ApplyInvariantError` are alert rows (`alerts.emit`), not only log lines.

The stream has no end frame (plan C2): only your own `STOP` ends a stream cleanly. An end at a frame boundary
that you did not ask for is `EngineExited`, and a transaction without its `END` is never applied. A new engine
life (`HELLO`, sequence 0) may repeat transactions that are applied; the discard by position is the one defence.

## Tests

**Default run (hermetic).** With a scripted engine (frames from a list) and package D's lanes on a local DuckDB:
assembly, discard by position, the `END` count, group closing, segments, routing, the three outcomes after
`CommitUnknown`, bounded attempts, the idle step, the hand-over, the attempt counter, `reposition`, the
configuration loader, the supervisor's rules with a fake clock, the transition enumeration for your machines
(D's helper), and one walk over **every declared crash point of the stream path**: crash there, start again,
and the destination equals the run without a crash, with a keyless table in the workload so that a duplicate
shows.

**Marker `md`.** The same crash walk on MotherDuck, with `stream.commit_sent` under `kill_later`; `CommitUnknown`
through the fault site `md.commit` for both real outcomes; a second run that takes the epoch in the middle of a
group (nothing of the deposed run commits, exit status 10); a concurrent writer that forces `Conflict`; a
staging chunk committed twice in the attempt.

**Marker `t1` (the gate tests; Postgres, engine, MotherDuck, the oracle).**

1. **SPEC 1.1 and 1.2.** A seeded workload from `cdc_v2.testing.workload` on keyed and keyless tables, with
   unchanged TOAST values, key changes, transactions over several tables, deletes and re-inserts of the same
   key, and empty transactions. The pipeline is killed at every declared crash point, each at least 20 times
   over the campaign, at random moments as well, and with the JVM killed alone. After every run:
   `cdc_v2.oracle.compare(..., position=Q)` is equal, the slot's `confirmed_flush_lsn` ≤ the applied position
   at every start (checked by the attach step and recorded), and the applied position never moved backwards
   outside `reposition`. The evidence counts runs, kills per point, transactions and rows.
2. **SPEC 1.3.** While the workload runs, an observer reads the destination again and again (the transfers
   observer of package O): no partly applied source transaction is ever visible, also across tables, also while
   the pipeline is being killed. One transaction larger than a group, with limits set small so that it has many
   staging chunks, is walked through every crash point of section 6. One real transaction of at least 10
   million rows runs once end to end, time-boxed, with the peak memory of the pipeline and of the JVM and the
   time of each phase in the evidence. Package F repeats this in its consumer loop; yours is the proof that the
   path works.
3. **Robustness on your path** (SPEC 4.5, 1.7): the destination black-holed in the middle of a commit and the
   source black-holed in the middle of the stream (package D's and package H's means); the JVM frozen with
   `SIGSTOP`; the spill fault site; the engine's pipe closed. Each ends in a stream that runs again by itself
   within the stated bounds, with the oracle equal.
4. **Never stuck**: a table whose apply always fails leaves the stream after three attempts while the others go
   on and the slot keeps advancing; a scripted failure that belongs to no table ends in the reposition path.
5. **Idle**: with no writes on published tables the slot's `confirmed_flush_lsn` moves within the bound you
   state; with a busy unpublished table too.

**Negative controls**, kept as tests that must fail the check they attack: the discard by position switched off;
`durable` sent before the commit; the position written in a transaction of its own; the fence left out of a
staging chunk; a group that splits a source transaction. A harness that cannot fail proves nothing (SPEC §4).

**Speed.** Measure the whole local path (rows per second and where the time goes) for an insert-only load and
for the 10-million-row transaction, and name every place where Python touches a row or a value: there must be
none on the steady path (SPEC 5.1 asks for 100,000 rows per second on an F16 later).

## Deliver

- `src/cdc_v2/config.py`, `src/cdc_v2/supervisor.py`, `src/cdc_v2/stream/`, `src/cdc_v2/pipeline.py`, your
  migration list, `config/example.toml`, the tests and the helper of section 9.
- `docs/design/stream.md`: the code path of each step of design section 3 (file and function), every crash
  point and fault site with what a crash there leaves behind, the tables you created, the bounds you state
  (restart, attach, marker, idle step, attempts), contract notes. `docs/configuration.md`.
- Evidence under `evidence/p3/stream/`.
- In your report: what design section 3 or a contract got wrong or left open; the numbers; every difference
  between the documents of E, D, A, P and what you needed from them; what packages L, V, B, T and F must know.
