# Core design

Status: draft written during P2, before the architecture decision record. It fixes the contracts that the P3 workers
build against. Every statement marked **[test]** is an assumption that a P3 test must confirm before code depends on
it; every number comes from `docs/spike/` or `docs/research/` and is cited there.

Sections: 1 processes · 2 position · 3 commit and acknowledge · 4 leader, fence, heartbeat · 5 bootstrap ·
6 apply · 7 snapshot and stream · 8 slot continuity · 9 state machines · 10 modules · 11 open points.

## 1. Processes

One Flight run is one process tree:

```
main.py (bootstrap, standard library only until the guard has decided)
└── pipeline (Python, own session and process group)
    ├── engine (JVM child: stock Debezium embedded engine + Postgres connector, our consumer)
    └── backfill readers (threads or child processes, read from the replica)
```

- The **bootstrap** is the only code stored in the Flight. It runs the leader guard, clones the repository at the
  pinned commit, fetches the pinned JRE and jars, starts the pipeline and then does nothing but watch it (section 5).
- The **pipeline** owns every MotherDuck connection that writes data, every state machine and the supervisor.
- The **engine** reads the slot and writes change records to the pipeline over a pipe. It never talks to MotherDuck.
  It acknowledges the slot only when the pipeline tells it a commit position is durable (section 3).
- The engine and every other child of the pipeline stay in the pipeline's process group, so that one signal to
  the group (the bootstrap's kill in section 5, a freeze of the whole tree in a test) reaches all of them. The
  pipeline kills a single child by its process id.
- An out-of-memory kill in any child ends the whole run (`docs/spike/run_semantics.md`, question 6). Memory limits are
  therefore set for the sum of all processes, and nothing relies on restarting a child after an out-of-memory kill.

Every stop that the platform causes (cancel, maximum run time, out-of-memory, delete) arrives without a signal. All of
them are treated as a crash: correctness never depends on a shutdown handler having run.

## 2. Position

- The **applied position** is the commit end LSN (`lsn_commit` of the END record) of the last source transaction
  whose effects are committed in MotherDuck. It is always a transaction boundary, and it only ever moves in a
  committed group that contains every transaction framed before it. A heartbeat record of Debezium never moves it:
  Debezium dispatches a heartbeat that already carries a transaction's commit position before it creates that
  transaction's END record, so such a position can be ahead of what has been delivered (second review of package E;
  with heartbeat positions adopted, transactions were lost in a reproduction). On an idle source the position moves
  with the transaction of the source-side heartbeat write (SPEC 4.6, package P), which the publication carries.
- Debezium re-sends whole transactions in commit order after a restart. A transaction whose commit LSN is ≤ the
  applied position is discarded as a whole; one whose commit LSN is greater is applied as a whole. No row-level
  position is needed for the stream, and the 32-bit transaction id is never used as a key.
- The records of one transaction are found by their **order** in the stream: BEGIN, the changes and END are
  contiguous. Debezium's transaction `id` is not a key: it is `<xid>:<the record's own LSN>` and differs from record
  to record (`docs/spike/local_streaming.md` §4: equal in 0 of 1,222 transactions). A change record does not carry
  its own transaction's commit end LSN (its `lsn_commit` is the previous transaction's); only END does. The commit
  record's start LSN is not available from Debezium and is not needed.
- The END record's `event_count` is compared with the number of change records received for that transaction (which
  records Debezium counts is established by test in package E; against the server's own stream it was exact for
  1,222 of 1,222 transactions), and each change's `total_order` with its place in the transaction. A mismatch is a
  protocol error: the batch is not committed and the stream component restarts. A mismatch at the same position
  after the configured number of restarts takes the skip-and-backfill path of SPEC 4.3; it never restarts for ever.
- A change row that is stored (pending log, staging) is ordered by (commit position, `seq`). `seq` alone orders
  rows only inside one engine instance: it starts at 0 again with every engine start.
- The applied position is stored in `_cdc.stream_state` (one row per slot; `applied_lsn UBIGINT`, plan C0) and is
  written in the same MotherDuck transaction as the data it describes.
- Invariant, checked by the oracle and at every slot acquisition: slot `confirmed_flush_lsn` ≤ applied position.

## 3. Commit and acknowledge

One destination commit applies a **group** of one or more whole source transactions. The group closes on the first
of: `apply.max_group_delay` (default 1 s) since its first record, `apply.max_group_bytes`, or a transaction that
exceeds the byte limit on its own (section 6.4). A TRUNCATE or a schema change does not close the group; it cuts
the group into segments (6.5), so there is one code path for it.

Steps, each with a named crash point (`crashpoints.py`):

| # | Step | Crash point |
|---|---|---|
| 1 | Records of the group are staged (section 6.1) | `stream.staged` |
| 2 | `BEGIN` on the stream connection | `stream.begun` |
| 3 | Apply statements for every segment of the group, in commit order | `stream.applied` |
| 4 | `UPDATE _cdc.stream_state SET applied_lsn = :group_end, … WHERE applied_lsn = :prev`; must change exactly one row (`:prev` was read in this transaction or is the position this process committed last) | `stream.position_written` |
| 5 | Fence statement for lane `stream` (section 4.2); must change exactly one row | `stream.fenced` |
| 6 | `COMMIT` | `stream.commit_sent`, `stream.committed` |
| 7 | Tell the engine "durable up to `:group_end`" | `stream.ack_sent` |
| 8 | Engine: `markProcessed(record)` for the newest END record whose `ack_position` ≤ `:group_end`, then `markBatchFinished()` | `engine.acked` |

Rules:

- A crash before the commit is durable leaves the destination at the previous position; the group is re-sent and
  applied. A crash after it leaves the slot behind the destination; the re-sent transactions are discarded by
  position. Step 6 has two crash points because a commit can become durable after the client has died
  (`docs/spike/md_semantics.md`: up to 1.95 s later). `stream.commit_sent` lies immediately before the blocking
  `COMMIT`; the crash tests run it with the action `kill_later` (`docs/testing/harness.md`), which kills the
  process while the `COMMIT` is in flight. With a plain kill that point would only repeat "nothing sent".
- Every other step of the design that changes durable state has crash points by the same rule
  (`docs/design/p3_plan.md` C1): take-over, release, heartbeat, backfill chunk, pending log, swap, schema change,
  slot creation. The package that builds the step names and declares them.
- **Unknown commit outcome** (connection lost during `COMMIT`): the stream component reconnects, reads
  `applied_lsn` and the fence, and continues from what it finds. It never assumes either outcome.
- The engine acknowledges nothing on its own: `offset.flush.interval.ms=0`, memory offset store, and the consumer
  calls `markProcessed` only for END records at or below the durable position. The pipeline says
  "durable" only for a position whose commit it has seen return, or has read back as the stored applied position.
- `offset.flush.interval.ms=0` together with `heartbeat.interval.ms` is **required**, not a tuning choice: with a
  flush interval and no heartbeat, Debezium never confirms the last transaction before an idle period or a stop,
  and every restart receives it again (`docs/spike/local_streaming.md` §6). Package E proves by test that the slot's
  `confirmed_flush_lsn` reaches the last applied transaction within a bound while the source is idle. Debezium's
  heartbeat records serve this only by causing the engine's batch calls on an idle source, in which a `DURABLE`
  takes effect and is flushed; they are never marked themselves (section 2).
- Nothing inside a transaction can be confirmed. An engine start after a large transaction that is not yet
  confirmed makes the server decode and spill it again before it sends anything (10M rows: 36 s and 2.6 GB on the
  node that holds the slot, measured locally). This time is part of the recovery time of SPEC 5.2.
- The commit deadline scales with the size of the group: `apply.commit_timeout_base` plus
  `apply.commit_timeout_per_million_rows` × rows. A commit has no progress signal, so the supervisor uses this
  deadline instead of the 120 s silence rule while a commit is in flight.
- Staging rows of a large transaction (section 6.4) are written by fenced stream-lane transactions and are
  removed by the transaction that applies them, or by the truncate of the staging tables that every start of the
  stream component performs (a restart always starts the engine again, and the slot re-sends the transaction).
- A table whose apply fails for a reason that a retry cannot cure (`ApplyInvariantError`, a value the destination
  refuses) must not stop the stream: after the configured number of attempts the table leaves the stream (state
  `quarantined`, routing `skip`, alert) in a stream-lane transaction, and the group is applied without it. The
  full quarantine of SPEC 4.0 (stored rows, slow retries) is built on this in P4.

## 4. Leader, fence, heartbeat

SPEC 4.2 describes one leader record. It is stored as three small tables, because they are written at different
rates by different connections, and a row that a long commit holds must not be the row the heartbeat needs
(`docs/spike/md_semantics.md`: the second writer of a row fails at once; a commit of 10M rows takes 8.9 s, of 50M
rows 38 s). The view `_cdc.leader_record` joins them into the record that SPEC 4.2 and 6.1 name.

| Table | Rows | Written by | Content |
|---|---|---|---|
| `_cdc.leader` | 1 | take-over and release only | `epoch`, `flight_id`, `run_number`, `run_id`, `state` (`leading`, `released`), `taken_at` |
| `_cdc.fence` | 1 per lane | every data transaction of that lane | `lane`, `epoch`, `n` |
| `_cdc.heartbeat` | 1 | heartbeat connection | `epoch`, `beat_at`, `progress_at`, `phase` |

All three are created once, with all their rows, and rows are only ever updated, never deleted and inserted again
(a delete next to an update is not detected as a conflict; a row inserted later would miss a concurrent
take-over). They are created by the bootstrap, in a way that leaves exactly one row set when several first runs
start at once (table and rows in one statement, or a key; proven on MotherDuck with simultaneous starters). Their
shape is frozen: the bootstrap cannot import the package before the clone, so it carries a copy of the statements
that `cdc_v2.md.schema` holds, and a T0 test compares the two.

`_cdc.connections` lists the server connection ids of every run: `epoch`, `lane`, connection id, time. A
connection adds its row before its first transaction, in one statement that inserts only if `_cdc.leader` still
holds the connection's epoch (so the epoch is checked there too, without holding a row). A take-over interrupts
the connections of earlier epochs from this list; the leader removes rows of epochs before the previous one.

### 4.1 Time

Every timestamp that is compared with another run's timestamp is MotherDuck's clock:
`uuid_extract_timestamp(uuidv7())` evaluated in the statement that writes or reads it. `now()` through the DuckDB
client is the container's clock and is never used for the leader protocol.

### 4.2 Fence

Lanes: `stream`, `control`, and `backfill.0` … `backfill.15` (one per parallel loader; the set is fixed so that no
fence row is ever added later). A lane is one connection that commits data or state. The last statement before
every `COMMIT` in a lane is

```sql
UPDATE _cdc.fence SET n = n + 1 WHERE lane = :lane AND epoch = :my_epoch;
```

and the transaction commits only if exactly one row changed. A take-over updates every fence row, so a transaction
of the old leader either committed before the take-over or fails (zero rows changed, or a write conflict on the
row). One row per lane lets a backfill chunk commit while a large stream commit is in flight.

### 4.3 Heartbeat

A dedicated connection runs every 10 s:

```sql
UPDATE _cdc.heartbeat SET beat_at = <md clock>, progress_at = :p, phase = :phase WHERE epoch = :my_epoch;
```

- The supervisor sends the heartbeat only while it sees progress (SPEC 4.2, 4.5): the stream component has reported
  within its deadline, and no component is past its restart limit. A commit in flight counts as progress until its
  size-scaled deadline (section 3). So does every other wait that a component declares with a reason and a
  deadline (slot creation on a standby can wait for a long transaction on the primary; a take-over can wait for
  the old engine to let go of the slot): without this rule a slow start-up would stop the heartbeat, the next
  minute's run would take over, and no run would ever finish starting. The heartbeat's only input is the
  supervisor's progress report (plan C11).
- Zero rows changed means another run has taken over: the pipeline stops all lanes and exits non-zero.
- After each successful heartbeat the pipeline sends a tick to the bootstrap (section 5).
- Healthy = `state = 'leading'` and `beat_at` newer than 30 s by MotherDuck's clock.
- The interval (10 s), the stale age (30 s), the hard-exit age of section 5 (60 s) and the start-up deadline are
  settings, so that tests can run the protocol in seconds. Their order is checked when the settings are built:
  interval < stale age / 3, and stale age < hard-exit age.

### 4.4 Guard decision

Run by the bootstrap before the clone, the JVM, the slot or any DDL.

1. Read `_cdc.leader_record`.
2. Healthy ⇒ exit within 10 s. Same Flight: exit 0. Different Flight: alert `contending_flight`, exit non-zero.
3. `released`, or not healthy ⇒ take over (4.5).

The first guard uses the `duckdb` client. `docs/spike/run_semantics.md` measured a guard without requirements over
MotherDuck's Postgres endpoint at half the run time; it is kept as an optimisation and is not on the P3 path.

### 4.5 Take-over

```sql
BEGIN;
UPDATE _cdc.leader    SET epoch = epoch + 1, flight_id = :f, run_number = :r, run_id = :id,
                          state = 'leading', taken_at = <md clock>, connection_ids = :mine
                      WHERE epoch = :seen_epoch;              -- must change exactly 1 row
UPDATE _cdc.fence     SET epoch = :seen_epoch + 1;            -- every lane
UPDATE _cdc.heartbeat SET epoch = :seen_epoch + 1, beat_at = <md clock>, phase = 'starting';
COMMIT;
```

- Exactly one of several simultaneous starters commits; the others see zero rows changed or a write conflict,
  re-read the record and exit by rule 2 of 4.4.
- If the statement conflicts with an open transaction of the previous leader (a hung commit holds a fence row), the
  newcomer calls `md_interrupt_server_connection` for the connection ids of earlier epochs in `_cdc.connections`
  and retries, until it wins, a healthy heartbeat appears, or `leader.takeover_timeout` (default 45 s) has passed;
  then it exits non-zero and the next minute's run tries again. It only ever interrupts ids read from that table.
  A lane that replaces its connection interrupts the old server connection first, so that no transaction of its
  own is left holding a fence row.
- A take-over whose commit outcome is unknown is not repeated blindly: the newcomer reads the record again after
  more than 2 s (a commit can land that late) and decides by `run_id` whether it leads.
- After winning: cancel the stored `run_number` through the MotherDuck API if `MD_GET_FLIGHT_RUN` shows it running
  (never "all other runs"); continue with start-up. The replication session that holds the slot is terminated
  later, by the pipeline's attach step (section 8), because the bootstrap has no Postgres client before the
  clone: `pg_terminate_backend(active_pid)` for our slot name only, repeated together with the engine start until
  our engine holds the slot or a deadline of at least 75 s has passed (the old run can re-take the slot until its
  hard exit, up to 60 s after it froze).
- A cancelled run keeps running for up to 2.5 s after the cancel. The fence, not the cancel, is what makes its late
  commits fail.
- The alert `takeover` records the previous run, the age of its heartbeat and the reason.

### 4.6 Planned hand-over

At `run.max_runtime` (default 7 h 30 min) the leader finishes the group in flight, commits, sets
`state = 'released'` in a fenced transaction, closes the slot connection and exits 0, timed to finish about 5 s
before a minute boundary. A transaction larger than a group that is still arriving is not waited for: it is
abandoned, and the slot sends it again to the next run. The next scheduled run takes over by 4.5 without interrupting or cancelling anything.
`max_runtime_sec` of the Flight is set above `run.max_runtime` as a coarse backstop only.

## 5. Bootstrap (`main.py`)

1. Guard decision (4.4). If the run is not to lead, exit.
2. Take over (4.5). Start a start-up heartbeat thread that writes `phase = 'starting'` every 10 s, with a start-up
   deadline (`run.startup_timeout`, default 180 s); past the deadline the bootstrap exits non-zero.
3. Clone the repository at the pinned commit; fetch and verify the pinned JRE and jars (`src/cdc_v2/artifacts.py`).
4. Start the pipeline with `start_new_session=True`, passing the epoch and a tick pipe. Stop the start-up
   heartbeat thread once the pipeline has sent its first tick.
5. Watch: the bootstrap reads ticks and uses no network and no MotherDuck connection. No tick for 60 s ⇒
   `SIGKILL` to the pipeline's process group, then `os._exit(1)`. Pipeline exit ⇒ exit with its status.

For local tiers the bootstrap takes a local source path in place of the pinned clone, and the local platform
(plan C6) does what the real one does at the end of a run: it kills every process the run started, in any
session or process group.

The run ends when `main.py` exits, and the platform then destroys everything in it; measured, a frozen pipeline's
run ended 65 s after the freeze. A frozen bootstrap is bounded by the next run's guard (30 s stale heartbeat, then
take-over and cancel).

## 6. Apply

### 6.1 Staging

The engine sends change records as Arrow record batches, one stream per table. The names and exact column sets
are fixed in the plan (`docs/design/p3_plan.md` C2, C3a); in meaning, each row carries:

| Column | Meaning |
|---|---|
| `_seq` | order of arrival; commit order, then order inside the transaction |
| `_commit_lsn` | commit end LSN of the source transaction |
| `_op` | `c`, `u`, `d` (TRUNCATE and schema changes travel on the control stream, section 6.5) |
| key columns | key of the row the record addresses; for a key change Debezium sends a delete and a create |
| value columns | new image |
| before columns | old image, only for tables without a key (`REPLICA IDENTITY FULL`) |
| `_unchanged` | list of columns whose value is an unchanged TOAST placeholder |

A group is held in memory as Arrow tables and applied **directly**: the statements of 6.2–6.3 read the local Arrow
data through the client (a registered relation, or a TEMP table, which also lives in the client). Measured on an
F16 Flight (`docs/spike/md_semantics.md`, "On an F16 Flight", run 4): 0.066 s for a group of 1–100 rows, 212K
rows/s at 100K rows, 342–355K rows/s at 1M rows. A staging table in MotherDuck was slower at every size and more
sensitive to load on the server, so it is used only for a transaction larger than a group (6.4).

Rules that follow from the measurements:

- Arrow data is handed to DuckDB in bounded record batches, never as one reader over an unbounded stream (a
  reader over 12M rows grew to 2 GB of client memory).
- `net` (6.2) and `delta` (6.3) are computed once per table and group on the client; the statements that follow
  read that result, so the rows are uploaded once per statement that needs them.
- The memory a group may hold is `apply.max_group_bytes`. The buffer in front of it has a byte limit; when it is
  full the pipeline stops reading the engine's pipe, which stops the engine's queue and then the slot read
  (back-pressure, SPEC 5.4).

Staging tables for 6.4 live in one schema, `_cdc_stage`. Every append is a stream-lane transaction like any
other: it ends with the fence, and it moves a staging progress row (engine instance, source transaction, number
of chunks) with a condition on the value it read, so that an append whose commit outcome was unknown is never
added twice, and a deposed leader meets the fence at its next chunk instead of writing for minutes. The staging
tables are truncated by every start of the stream component and by the transaction that applies them. The
statements of 6.2–6.3 are the same for both inputs; only the input relation differs.

### 6.2 Tables with a key

For one table and one segment, with `net` = the last record per key by `_seq`:

1. `DELETE FROM t USING net WHERE t.key = net.key` for keys whose net record is a delete or a complete image.
2. `INSERT INTO t SELECT … FROM net` for complete images.
3. Partial images (some column unchanged) are applied with `UPDATE … FROM net`, never by delete and insert, and
   never write a column whose value is unchanged. A column that is unchanged in the last record takes the newest
   changed value from earlier records of the same key in the group, if there is one.

Chains and swaps of key values (SPEC 1.4): the delete and the create of each key change are ordinary records, and
the last record per key decides. One case needs more. The create of a key change can be a partial image; its
unchanged values belong to the row that had the old key. The engine therefore copies the old key onto the create
record (Debezium links the two records with the headers `__debezium.oldkey` and `__debezium.newkey` **[test]**),
and the applier resolves a chain of key changes inside a group back to the key the row had before the group, and
takes the unchanged values from that row before it deletes it. The destination has no keys or indexes, so no intermediate
state can violate a constraint.

An unchanged value is never lost silently (H-003). It is resolved in this order: from an earlier record of the
same row in the change set; from the destination row that has the key the row had before the change set; and,
only when a log is replayed onto a table that was loaded separately (catch-up, 7.3), from the destination row
with the new key if no earlier record of the set addressed that key. A value that is still unresolved is an
invariant violation in the stream (`ApplyInvariantError`: the destination has diverged), and a key for a repair
read in catch-up (7.3). Applying a change set a second time gives the same table, except for keys reported as
unresolved.

DuckDB does not write an `UPDATE` whose new value compares equal to the stored one, and keeps the old
representation: after `SET x = 0` a stored `-0` is still `-0`, and after `SET i = INTERVAL '30 days'` a stored `1
mon` is still `1 mon`, also through `UPDATE … FROM`, locally and on MotherDuck (package O,
`evidence/p3/oracle/duckdb_equal_update.json`). The source and the oracle tell these values apart. For columns whose
types have such values (real, double precision, interval, and what package A's list adds), step 3 must therefore use
a shape that always writes the changed column; the rule that an unchanged column is never written stays. Package A
chooses the shape and proves it against the oracle.

The number of statement shapes per table depends on the table's columns, not on the data (SPEC 2.6).

### 6.3 Tables without a key

Every record carries the full old and new image. For one table and one segment, `delta` = per distinct row image
the sum of +1 for every new image and −1 for every old image (`GROUP BY` all columns, which treats NULLs as equal).

1. Images with a negative sum: delete exactly `-sum` rows among the identical rows, chosen by `rowid` through
   `row_number() OVER (PARTITION BY <all columns>)`.
2. Images with a positive sum: insert `sum` copies.

A delete that finds fewer rows than it must remove is an invariant violation: the transaction is rolled back and
the table goes to quarantine handling (SPEC 4.0); it is never ignored.

The grouping key must keep apart every pair of values that the source tells apart. DuckDB's `GROUP BY`, `PARTITION
BY`, `DISTINCT` and `=` do not: `-0` and `0` form one group, NaN and −NaN one, and `1 mon`, `30 days` and `720
hours` one; UNION values of different members compare unequal although they can stand for the same source value
(package O, `docs/oracle.md` 1.5). The delta and the choice of rows to delete therefore use an exact key for columns
of such types — the canonical form the oracle compares by (`docs/oracle.md` 1.2: a UNION value by its member and
that member's rule, NaN equal to NaN, `-0` different from `0`, an interval by its three fields) — and the value
itself elsewhere. Package A writes down the full list of destination types that need it. A source NULL in a UNION
column is written as a real NULL: a UNION whose member holds NULL is not NULL in DuckDB. This settles open point 3.

The stream path and the backfill path must produce identical destination values for identical source values; the
type fuzzer checks this for every type, because a keyless delete can only find a row that was written the same way.

### 6.4 A transaction larger than a group

Its records are appended to staging tables in MotherDuck (6.1) in fenced, committed chunks while they arrive, so
memory and local disk stay bounded, and the upload is finished before the END record arrives. At its END record one
destination transaction runs the statements of 6.2–6.3 over all of its staged rows, writes the position, fences
and commits. The applied position does not move, and no later source transaction
becomes visible, until that commit. Nothing is ever applied from an unfinished source transaction.

### 6.5 Segments

A TRUNCATE (all relations named in the message, so CASCADE needs nothing extra) or a schema change cuts the group
into segments. Inside the one destination transaction: apply segment 1, execute the truncate or the DDL, apply
segment 2, and so on, in commit order. DDL in MotherDuck is transactional, so a rolled-back group leaves no
half-changed schema.

## 7. Snapshot and stream

One mechanism serves the initial snapshot, a newly discovered table, an operator's re-backfill, an automatic
re-backfill and a resumed backfill (SPEC 3.3). The stream never stops for it.

### 7.1 Life of one backfill

1. **Enter.** Every request for a backfill (first start, discovery, operator, slot finding, lag, drop and
   recreate) is a row in a request table; the stream lane enacts it inside a group: that group commits the
   table's transition to `backfilling` at stream position `P0` = the group's end. From that commit on, the
   table's changes are appended to the table's **pending log** in the same group transaction that moves the
   position. What happens to the visible table depends on why the backfill runs (the table's **routing**, kept
   by the stream lane, plan C10):
   - *after a gap* (first backfill, slot finding, lag-triggered re-backfill, quarantine, drop and recreate): the
     changes go to the pending log only. Consumers keep the last complete version, or no table yet (SPEC 3.6);
   - *without a gap* (operator re-backfill of a table that is streaming, change of mode): the changes are still
     applied to the visible table as well, in the same group as every other table, until the swap. The table
     never leaves the guarantee of SPEC 1.3, whose only exception is a table that has not reached streaming.
2. **Read.** The backfill lane reads chunks from the source into the **new table** (same schema as the target, a
   reserved name, so that the swap can be a rename). Chunks are `ctid` ranges, read with `COPY` by parallel
   readers (2 on F16); R3: Debezium takes no snapshot. A load transaction covers one or more chunks; it is fenced
   in the loader's own lane `backfill.<i>` and records its chunks as done in the same transaction (crash point
   `backfill.chunk_committed`). The last chunk of a table has no upper bound: the table can have grown between
   planning and the chunk's snapshot. Rules for the source side, from `docs/spike/local_throughput.md` section 8:
   - every chunk is read in its own short transaction, far shorter than `max_standby_streaming_delay`. A long
     read transaction on a standby is cancelled by any `ACCESS EXCLUSIVE` lock taken on the primary (also by a
     vacuum that truncates the table's tail) and stalls replay, and with it the stream, until then;
   - a table with a key: every chunk transaction takes its own snapshot and its own cut (7.2, 7.3). Nothing is
     held between chunks, so the backfill holds back no cleanup on the primary and has no connection whose loss
     ends it; a resume is nothing special;
   - a table without a key needs one snapshot for the whole table (7.4). It is held by an **exporter** connection
     that never touches the table (it holds no relation lock); readers import it per chunk with
     `SET TRANSACTION SNAPSHOT`. While it lives it holds back cleanup on the primary through
     `hot_standby_feedback`; its age is reported and has an alert threshold;
   - a chunk that fails with a recovery conflict (SQLSTATE 40001) is read again.
3. **Catch up.** The backfill lane applies pending changes to the new table in rounds (statements of section 6,
   mode `catch_up`), recording `caught_up_lsn` each time, until the rest is small. Then it moves the backfill to
   `swap_ready` and writes nothing more for this table: from here on only the stream lane touches the new table
   and the backfill's row, so the two lanes never write the same row at the same time.
4. **Swap.** A stream-lane group applies the rest of the pending log, drops the old table, renames the new one,
   and moves the table to `streaming`, in the same transaction as the group's position and fence (R7, SPEC 3.2).
   If the rest holds a key that cannot be resolved (6.2), nothing is swapped: the backfill goes back to
   `catching_up` for a repair read (7.3). From the next group on, the table's changes are applied directly. This
   is also the moment a table "reaches streaming" for SPEC 1.3.

The stream component calls one function of the backfill package inside every group, before the position step
(plan C10); enter, the pending-log appends and the swap all happen there.

A crash anywhere leaves either the old version with a resumable backfill, or the new version in `streaming`.

### 7.2 Where a snapshot lies in the stream

Each snapshot has a **cut**: a function that answers, for a source transaction, `inside`, `outside` or `unknown`.
The stream identifies a transaction by the end of its commit record (`E`, section 2), so the cut is stated in `E`.

**On a standby** (replica mode) the cut is a pair of replay positions around the moment the snapshot is taken:
`R0` = `pg_last_wal_replay_lsn()` read by a statement that has finished before the snapshot is taken, and `R1` =
the same function read after the snapshot exists. Two ways to get them:

- a reader's own snapshot: `SELECT pg_last_wal_replay_lsn()` in autocommit (`R0`), then
  `BEGIN ISOLATION LEVEL REPEATABLE READ`, whose first statement takes the snapshot and reads `R1`;
- an exported snapshot: one read-committed transaction with three statements: `R0`, `pg_export_snapshot()`, `R1`.

A value read by the statement that takes the snapshot is an upper bound only, never `R0`.

Replay is serial, and a commit becomes visible to snapshots during the replay of its commit record, before the
replay position moves past that record. Therefore, with `E1` = the `E` of the first transaction in the stream with
`E > R1`:

- `E ≤ R0` ⇒ `inside`;
- `E > E1` ⇒ `outside`;
- otherwise `unknown`: the commits in `(R0, R1]`, and the one at `E1`, whose record may have been in replay while
  the snapshot was taken.

The stream **has passed** a snapshot when its applied position is greater than `R1`; the transaction at `E1` is
then applied. The heartbeat write guarantees that such a transaction arrives.

A snapshot is used only if `R0 ≥ P0`: everything the stream applied before the table entered `backfilling` is then
inside. This matters when backfill reads use a different replica than the slot. The wait for it has a deadline
(the replica may be stalled); past it the backfill reports and keeps waiting as a declared wait (4.3), it does
not spin and does not read.

`pg_current_snapshot()` cannot give the answer on a standby: its in-progress list is empty there. Its `xmin` and
`xmax` can still turn `unknown` into an answer (transaction id below `xmin` ⇒ `inside`, at or above `xmax` ⇒
`outside`), which helps 7.4.

**On a primary** (primary mode, or reads from the primary) there is no such pair of positions: a commit record is
written, and can be decoded by the slot, before the transaction becomes visible to snapshots; with synchronous
replication that gap can be long. The cut is `pg_current_snapshot()` together with the transaction id of each
stream transaction, and it never answers `unknown`. What replaces `R0 ≥ P0` there, so that no transaction the
stream applied before `P0` is still invisible to the snapshot, is open point 12.

**[test]** P3 proves all of 7.2 with a stress test before anything depends on it (`docs/design/cut.md`): very many
cuts while writers commit counted rows; the rows visible in each snapshot must equal the commits `inside`, plus
some of the `unknown` ones, and none of the `outside` ones; a deliberately wrong rule must be caught.

### 7.3 Tables with a key

Chunks may be read under different snapshots, which is what makes a resumed backfill possible without reading
completed chunks again (SPEC 3.7). The pending log is replayed from `replay_from` = the smallest `R0` of the
table's snapshots; `unknown` counts as `outside`. Catch-up starts only after all chunks are loaded.

A row can move between `ctid` ranges between two snapshots, but only through an `UPDATE`, and that change is in
the replayed log. So after loading, for a key that does not occur in the log the new table holds exactly one
correct row, and for a key that occurs in the log it may hold **no row, one row or several versions**. Catch-up
therefore treats the keys of the log like this, before the statements of 6.2 run:

1. Several versions of a key are reduced to one; which one does not matter, because every column that differs
   between them was changed by a record in the log, and the log supplies that value.
2. If the last record of a key is a partial image (unchanged TOAST values) and a column is still unresolved after
   looking at the earlier records of the key in the log, the value is taken from the row in the new table. If the
   new table has no row for the key, the key goes to a **repair read**: one statement per round reads the missing
   rows by key from the source under a new snapshot (`WHERE key = ANY(…)`; no temporary table, the standby is
   read-only), and they are loaded like a chunk. The repair snapshot must have `R0 ≥ caught_up_lsn`, so that a
   repaired row is never older than what catch-up has already applied to the new table. This is a backfill read, not a re-read of an unchanged value in
   the stream (SPEC 2.6); it counts towards the row bound of SPEC 3.7.
3. Then every key of the log ends as exactly one row, or none after a delete.

The swap waits until the stream position has passed the upper bound of every snapshot the table used, repair
reads included: the stream has applied a transaction whose commit ends after that snapshot's `R1` (7.2). Until
then the new table may hold values that are newer than the stream position; consumers never see it in that state.

Rows also move without any change record when the table is rewritten (`VACUUM FULL`, `CLUSTER`, a rewriting
`ALTER TABLE`, `TRUNCATE`). Every chunk therefore reads `pg_relation_filenode` under its own snapshot; if it
differs from the value of the table's first chunk, the loaded chunks are discarded and the backfill starts again.

### 7.4 Tables without a key

A change cannot be attributed to a `ctid` range, and replaying a change twice changes a row count. So the whole
table is read under **one** snapshot whose cut has no `unknown` transaction that touches the table. The backfill
opens a snapshot, waits until the stream has passed `R1`, and checks; if a touching transaction is `unknown`, it
opens another snapshot before reading anything. The pending log is replayed from that exact cut.

If the snapshot is lost (crash, hand-over, standby conflict), the chunks already loaded are discarded and the
table's backfill starts again. Consequence: the row bound of SPEC 3.7 holds for tables with a key; a table without
a key is consistent after a resume (SPEC 1.6) but is read again. This is reported to the owner as a known limit.

### 7.5 A table that joins the publication

Changes written before the publication change are not published, even if their transaction commits later. After
adding a table to the publication, the pipeline notes the primary's next transaction id `X`, and the table's first
snapshot must have `xmin ≥ X`: every transaction that wrote to the table before the change has then finished and
is `inside`. **[test]** in the discovery work of P4, including a transaction that stays open across the change.

## 8. Slot continuity

Checked at every slot acquisition and every `slot.verify_interval` (default 30 s) while streaming (SPEC 1.8,
4.1). `_cdc.stream_state` stores, next to the applied position: slot name, the server's system identifier, the
timeline, and what identifies the slot (**[test]** which catalog values identify a recreated slot of the same
name on Postgres 17 and 18; if there is none, a recreated slot is recognised by its positions only).

A check that reads the slot and then starts the engine proves nothing: the slot can be advanced, dropped or
recreated in between. The **attach step** therefore is:

1. Read the fence row (a deposed run must not touch the slot). Read the slot facts. If another session holds the
   slot (the previous run's engine), terminate that backend (our slot name only) and read again.
2. Start the engine at the applied position. The engine reports that it holds the slot only when replication has
   really started (plan C2), and never attaches a second time by itself.
3. Read the slot facts again. They must show our engine's backend as the holder and must agree with step 1 in
   everything the decision uses. If not, kill the engine and start at 1. The whole loop has a deadline of at least
   75 s after a take-over (the previous run can take the slot back until its hard exit) and counts as a declared
   wait (4.3).
4. Decide by the table below on the facts of step 1, now known to describe the slot that our engine holds. No
   frame is applied before the decision. While our engine holds the slot, nobody else can advance or drop it; if
   the engine loses it, the stream component restarts and attaches again.

A restart of the stream component always kills the engine and starts a new one. **[test]** whether a status
update that carries a lower flush position can move `confirmed_flush_lsn` backwards on Postgres 16, 17 and 18:
if it can, comparing only after the attach would hide a slot that was ahead, which is why step 1 reads first.

| Finding | Meaning | Action |
|---|---|---|
| slot `confirmed_flush_lsn` ≤ applied position, same identity, applied position on the server's timeline history | continuous | stream from the slot; discard by position |
| slot ahead of the applied position | changes were consumed elsewhere or the slot was advanced | alert, automatic backfill of all CDC tables, stream from the slot's position |
| slot missing, recreated, or `wal_status = 'lost'` / invalidated | history is gone | alert, new slot, automatic backfill |
| different system identifier, or applied position not on the timeline history | different server or diverged history (failover) | as the replica rules of SPEC 4.1 decide: continue if the position is proven to be on the new history, else alert and backfill |

A finding other than "continuous" is enacted in **one** stream-lane transaction: the alert, the applied position
set to the slot's `confirmed_flush_lsn` (for a new slot: its consistent point), and a backfill request enacted
for every CDC table with `P0` = that position (7.1, after a gap). After that commit the same check finds
"continuous", so a finding cannot repeat for ever. A slot that was recreated but is not ahead of the applied
position has lost nothing; it needs an alert where the recreation can be seen, and no backfill.

The Flight never exits because of a slot finding. The timeline history is read over a replication connection
(`TIMELINE_HISTORY`); **[test]** that the role and the PlanetScale endpoint allow it, otherwise a failover is
always treated as a break.

## 9. State machines

Each machine is one module with an enumerated state set and one transition table; `transition(conn, machine, key,
from, to, cause)` is the only function that writes a state. It runs inside the caller's transaction, fails if the
stored state is not `from` or the pair is not in the table, and appends a row to `_cdc.transitions`. States live in
`_cdc` tables and change in the same transaction as the data they govern.

| Machine | Key | States |
|---|---|---|
| leader | the one leader row | `released` → `leading`; `leading` → `leading` (take-over, epoch + 1); `leading` → `released`. Why a run ended (superseded, failed, released) is the cause in `_cdc.transitions`. Written by the bootstrap with its own copy of the machine (section 4) |
| slot and stream | slot | `unverified` → `verified` → `streaming` ⇄ `stalled`; `broken` → `rebuilding` → `verified` |
| table replication | table | `discovered` → `awaiting_approval` / `backfilling` → `streaming`; `streaming` → `backfilling`; `refresh_only`; `quarantined`; `retained`; `dropped` |
| backfill progress | table, backfill id | `planned` → `reading` → `catching_up` ⇄ `swap_ready` → `done`; `abandoned` |
| schema change | table, change id | `detected` → `applying` → `applied`; `failed` |
| batch commit / ack | slot | one row per slot in `_cdc.stream_state`: the applied position and the last position sent as durable; "acknowledged" is read from the slot's `confirmed_flush_lsn`. Legal moves: applied position forward only (or set by a slot finding, section 8); durable ≤ applied |
| quarantine | table | `healthy` → `retrying(n)` → `quarantined` → `healthy` |

The tables above are the starting point; the worker who builds a machine fixes its final transition table in code
and in `docs/state_machines.md`, and the T0 tests enumerate every pair.

## 10. Modules

```
flights/cdc_v2/main.py      bootstrap (section 5)
jvm/                        engine: embedded Debezium, consumer, Arrow writer, ack reader
src/cdc_v2/config.py        configuration model and validation
src/cdc_v2/md/              MotherDuck connections, lanes, fence, clock, deadlines
src/cdc_v2/pg/              source connections, catalog, slot and publication, cut (7.2)
src/cdc_v2/state/           transition function and the machines of section 9
src/cdc_v2/leader/          guard, take-over, heartbeat, release
src/cdc_v2/stream/          engine process, record decoding, grouping, commit/ack protocol
src/cdc_v2/apply/           staging and the statements of section 6
src/cdc_v2/backfill/        chunk planning, readers, pending log, catch-up, swap
src/cdc_v2/supervisor.py    component deadlines, restarts, progress for the heartbeat
src/cdc_v2/crashpoints.py   named crash points, switched on by environment for the harness
src/cdc_v2/oracle/          source/destination comparison used by every test tier
```

## 11. Open points

| # | Point | Decided by |
|---|---|---|
| 1 | Cut on a standby (7.2) is exact as described | P3 stress test |
| 2 | `rowid` is usable to delete exactly k identical rows (6.3) | P3 test on MotherDuck |
| 3 | Canonical comparison form for UNION and NaN (6.3) | settled by the oracle (package O, `docs/oracle.md` 1.2); the keyless grouping key uses the same form (6.3) |
| 4 | Staging tables versus direct apply (6.1) | decided by the Flight measurement in P2: direct for groups, staging tables only for 6.4 |
| 5 | Appends by the stream lane and reads by the backfill lane on one pending log do not conflict (7.1) | P3 test on MotherDuck |
| 6 | Identity of a recreated slot; reading the timeline history (8) | P3, stage 2 for PlanetScale |
| 7 | Publication join rule (7.5) | P4 discovery work |
| 8 | Guard over the Postgres endpoint; hand-over that starts the new leader before the old one stops | after P6, if the numbers ask for it |
| 9 | Key change with unchanged TOAST values: old key on the create record, chains resolved (6.2) | P3 (1.4) and P4 (2.6) tests |
| 10 | Several versions and missing rows after loading under several snapshots; repair read (7.3) | P3 handoff tests, workload fuzzer with TOAST values |
| 11 | A schema change of a table during its own backfill | P4 schema wave; first rule: the table's backfill starts again |
| 12 | Cut on a primary: a transaction decoded before `P0` that a later snapshot does not see yet (7.2) | P3 cut stress test proposes and proves the rule |
| 13 | Catch-up that never converges (changes with unchanged values arrive faster than repair reads resolve them) | P4: bound the rounds, alert; P6 fuzzers |
| 14 | Can a status update with a lower flush position move `confirmed_flush_lsn` backwards (8)? | P3, packages E and V. Not on 18.6 (E's test, repeated by the second review). 16 is repeated once `tools/localpg` runs the 16 binaries; 17 where a 17 server is available |
| 15 | SPEC 1.3 during a re-backfill: a table that streams stays in the guarantee (7.1, "without a gap"); after a gap each table swaps on its own (SPEC 3.6) and is outside the guarantee until then, like a first backfill | decided here; reported to the owner |
| 16 | Leader tables created by several first runs at once end as one row set (4) | P3, package L, on MotherDuck |
| 17 | SPEC 4.2 "a newcomer exits within 10 s": measured from the first line of `main.py`; the platform's own start time varies (p95 7 s, max 21 s in the spike) and is outside our control | reported to the owner |
