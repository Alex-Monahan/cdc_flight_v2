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
- An out-of-memory kill in any child ends the whole run (`docs/spike/run_semantics.md`, question 6). Memory limits are
  therefore set for the sum of all processes, and nothing relies on restarting a child after an out-of-memory kill.

Every stop that the platform causes (cancel, maximum run time, out-of-memory, delete) arrives without a signal. All of
them are treated as a crash: correctness never depends on a shutdown handler having run.

## 2. Position

- The **applied position** is the commit end LSN (`lsn_commit` of the END record) of the last source transaction
  whose effects are committed in MotherDuck. It is always a transaction boundary.
- Debezium re-sends whole transactions in commit order after a restart. A transaction whose commit LSN is ≤ the
  applied position is discarded as a whole; one whose commit LSN is greater is applied as a whole. No row-level
  position is needed for the stream, and the 32-bit transaction id is never used as a key.
- The END record's `event_count` is compared with the number of change records received for that transaction. A
  mismatch is a protocol error: the batch is not committed and the stream component restarts.
- The applied position is stored in `_cdc.stream_state` (one row per slot) and is written in the same MotherDuck
  transaction as the data it describes.
- Invariant, checked by the oracle and at every slot acquisition: slot `confirmed_flush_lsn` ≤ applied position.

## 3. Commit and acknowledge

One destination commit applies a **group** of one or more whole source transactions. The group closes on the first
of: `apply.max_group_delay` (default 1 s) since its first record, `apply.max_group_bytes`, a transaction that
exceeds the byte limit on its own (section 6.4), or a record that must start a new segment (TRUNCATE, schema change).

Steps, each with a named crash point (`crashpoints.py`):

| # | Step | Crash point |
|---|---|---|
| 1 | Records of the group are staged (section 6.1) | `stream.staged` |
| 2 | `BEGIN` on the stream connection | `stream.begun` |
| 3 | Apply statements for every segment of the group, in commit order | `stream.applied` |
| 4 | `UPDATE _cdc.stream_state SET applied_lsn = :group_end, …` | `stream.position_written` |
| 5 | Fence statement for lane `stream` (section 4.2); must change exactly one row | `stream.fenced` |
| 6 | `COMMIT` | `stream.commit_sent`, `stream.committed` |
| 7 | Tell the engine "durable up to `:group_end`" | `stream.ack_sent` |
| 8 | Engine: `markProcessed(endRecord)` for the newest END ≤ `:group_end`, then `markBatchFinished()` | `engine.acked` |

Rules:

- A crash before the commit is durable leaves the destination at the previous position; the group is re-sent and
  applied. A crash after it leaves the slot behind the destination; the re-sent transactions are discarded by
  position. Step 6 has two crash points because a commit can become durable after the client has died
  (`docs/spike/md_semantics.md`: up to 1.95 s later).
- **Unknown commit outcome** (connection lost during `COMMIT`): the stream component reconnects, reads
  `applied_lsn` and the fence, and continues from what it finds. It never assumes either outcome.
- The engine acknowledges nothing on its own: `offset.flush.interval.ms=0`, memory offset store, and the consumer
  calls `markProcessed` only for END records at or below the durable position.
- The commit deadline scales with the size of the group: `apply.commit_timeout_base` plus
  `apply.commit_timeout_per_million_rows` × rows. A commit has no progress signal, so the supervisor uses this
  deadline instead of the 120 s silence rule while a commit is in flight.
- Staging rows of a large transaction (section 6.4) are deleted after its commit, by the next group's transaction
  or by a truncate of the staging table between groups, never in a way that can lose rows of a transaction that
  is not committed.

## 4. Leader, fence, heartbeat

SPEC 4.2 describes one leader record. It is stored as three small tables, because they are written at different
rates by different connections, and a row that a long commit holds must not be the row the heartbeat needs
(`docs/spike/md_semantics.md`: the second writer of a row fails at once; a commit of 10M rows takes 8.9 s, of 50M
rows 38 s). The view `_cdc.leader_record` joins them into the record that SPEC 4.2 and 6.1 name.

| Table | Rows | Written by | Content |
|---|---|---|---|
| `_cdc.leader` | 1 | take-over and release only | `epoch`, `flight_id`, `run_number`, `run_id`, `state` (`leading`, `released`), `taken_at`, connection ids of the run's MotherDuck connections |
| `_cdc.fence` | 1 per lane | every data transaction of that lane | `lane`, `epoch`, `n` |
| `_cdc.heartbeat` | 1 | heartbeat connection | `epoch`, `beat_at`, `progress_at`, `phase` |

All three are created once, and rows are only ever updated, never deleted and inserted again (a delete next to an
update is not detected as a conflict).

### 4.1 Time

Every timestamp that is compared with another run's timestamp is MotherDuck's clock:
`uuid_extract_timestamp(uuidv7())` evaluated in the statement that writes or reads it. `now()` through the DuckDB
client is the container's clock and is never used for the leader protocol.

### 4.2 Fence

Lanes: `stream`, `backfill`, `control`. A lane is one connection that commits data or state. The last statement
before every `COMMIT` in a lane is

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
  size-scaled deadline (section 3).
- Zero rows changed means another run has taken over: the pipeline stops all lanes and exits non-zero.
- After each successful heartbeat the pipeline sends a tick to the bootstrap (section 5).
- Healthy = `state = 'leading'` and `beat_at` newer than 30 s by MotherDuck's clock.

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
  newcomer calls `md_interrupt_server_connection` for the connection ids stored in `_cdc.leader` and retries, until
  it wins or a healthy heartbeat appears. It only ever interrupts ids read from the record.
- After winning, in this order: cancel the stored `run_number` through the MotherDuck API if `MD_GET_FLIGHT_RUN`
  shows it running (never "all other runs"); terminate the replication session that holds the slot
  (`pg_terminate_backend(active_pid)` for our slot name only); read the applied position; continue with start-up.
- A cancelled run keeps running for up to 2.5 s after the cancel. The fence, not the cancel, is what makes its late
  commits fail.
- The alert `takeover` records the previous run, the age of its heartbeat and the reason.

### 4.6 Planned hand-over

At `run.max_runtime` (default 7 h 30 min) the leader finishes the group in flight, commits, sets
`state = 'released'` in a fenced transaction, closes the slot connection and exits 0, timed to finish about 5 s
before a minute boundary. The next scheduled run takes over by 4.5 without interrupting or cancelling anything.
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

The run ends when `main.py` exits, and the platform then destroys everything in it; measured, a frozen pipeline's
run ended 65 s after the freeze. A frozen bootstrap is bounded by the next run's guard (30 s stale heartbeat, then
take-over and cancel).

## 6. Apply

### 6.1 Staging

The engine sends change records as Arrow record batches, one stream per table. Each row carries:

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

Staging tables for 6.4 live in a schema that is private to the leader epoch (`_cdc_stage_<epoch>`; schemas of
older epochs are dropped during start-up). Appends to them are committed on their own and are not fenced: they
are invisible to consumers, and a stale leader can only write into its own epoch's schema. The statements of
6.2–6.3 are the same for both inputs; only the name of the input relation differs.

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

The number of statement shapes per table depends on the table's columns, not on the data (SPEC 2.6).

### 6.3 Tables without a key

Every record carries the full old and new image. For one table and one segment, `delta` = per distinct row image
the sum of +1 for every new image and −1 for every old image (`GROUP BY` all columns, which treats NULLs as equal).

1. Images with a negative sum: delete exactly `-sum` rows among the identical rows, chosen by `rowid` through
   `row_number() OVER (PARTITION BY <all columns>)`.
2. Images with a positive sum: insert `sum` copies.

A delete that finds fewer rows than it must remove is an invariant violation: the transaction is rolled back and
the table goes to quarantine handling (SPEC 4.0); it is never ignored.

Comparison uses a canonical form for types whose values do not compare equal to themselves in MotherDuck (UNION
members, NaN): **[test]** decide the canonical form in P3 and use the same one in the oracle.

The stream path and the backfill path must produce identical destination values for identical source values; the
type fuzzer checks this for every type, because a keyless delete can only find a row that was written the same way.

### 6.4 A transaction larger than a group

Its records are appended to staging tables in MotherDuck (6.1) in committed chunks while they arrive, so memory and
local disk stay bounded, and the upload is finished before the END record arrives. At its END record one
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

1. **Enter.** A stream-lane group commits the table's transition to `backfilling` at stream position `P0`. From
   that commit on, the table's changes are not applied to the visible table; they are appended to the table's
   **pending log** in the same group transaction that moves the position. Consumers keep the old version.
2. **Read.** The backfill lane reads chunks from the source into the **new table** (same schema as the target, a
   reserved name, so that the swap can be a rename). Chunks are `ctid` ranges, read with `COPY` by parallel
   readers (2 on F16); R3: Debezium takes no snapshot. A load transaction covers one or more chunks; it is fenced
   in lane `backfill` and records its chunks as done in the same transaction (crash point
   `backfill.chunk_committed`). Rules for the source side, from `docs/spike/local_throughput.md` section 8:
   - every chunk is read in its own short transaction, far shorter than `max_standby_streaming_delay`. A long
     read transaction on a standby is cancelled by any `ACCESS EXCLUSIVE` lock taken on the primary (also by a
     vacuum that truncates the table's tail) and stalls replay, and with it the stream, until then;
   - a snapshot that several chunks share is held by an **exporter** connection that never touches the table (it
     holds no relation lock); readers import it with `SET TRANSACTION SNAPSHOT`;
   - a snapshot is kept for at most `backfill.snapshot_max_age` (60 s), because it holds back cleanup on the
     primary through `hot_standby_feedback`. After that, or when it is lost, the table continues under a new
     snapshot (7.3). A resume is the same step;
   - a chunk that fails with a recovery conflict (SQLSTATE 40001) is read again.
3. **Catch up.** The backfill lane applies pending changes to the new table in rounds (statements of section 6),
   recording `caught_up_lsn` each time, until the rest is small.
4. **Swap.** A stream-lane group applies the rest of the pending log, drops the old table, renames the new one,
   and moves the table to `streaming`, in the same transaction as the group's position and fence (R7, SPEC 3.2).
   From the next group on, the table's changes are applied directly. This is also the moment a table "reaches
   streaming" for SPEC 1.3.

A crash anywhere leaves either the old version with a resumable backfill, or the new version in `streaming`.

### 7.2 Where a snapshot lies in the stream

Each snapshot has a **cut**: a function that answers, for a source transaction, `inside`, `outside` or `unknown`.

On a standby (replica mode), one multi-statement message in a read-committed transaction reads
`pg_last_wal_replay_lsn()` (`R0`), then exports the snapshot and reads `pg_last_wal_replay_lsn()` again (`R1`).
Replay is serial, and a commit becomes visible to snapshots during the replay of its commit record, before the
replay position moves past that record. Therefore:

- commit record ends at or before `R0` ⇒ `inside`;
- commit record starts after the first record that follows `R1` ⇒ `outside`;
- otherwise `unknown` (at most the commits between `R0` and `R1`, plus the one record in flight).

**[test]** P3 proves this with a stress test on a hot table before anything depends on it: thousands of snapshots
while a writer commits one counted row per transaction; the rows visible in each snapshot must equal the commits
`inside`, plus some prefix of the `unknown` ones. `pg_current_snapshot()` cannot give the answer on a standby: there
its in-progress list is empty **[test]**.

On a primary (primary mode), the cut is `pg_current_snapshot()` and the transaction id of each stream transaction;
it never answers `unknown`.

A snapshot is used only if `R0 ≥ P0`. This matters when backfill reads use a different replica than the slot.
Parallel readers import the exported snapshot (`SET TRANSACTION SNAPSHOT`, measured on the standby: consistent;
readers with their own snapshots: mixed states).

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
   read-only), and they are loaded like a chunk. This is a backfill read, not a re-read of an unchanged value in
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

Checked at every slot acquisition and every `slot.verify_interval` (default 30 s) while streaming, before the
engine may read (SPEC 1.8, 4.1). `_cdc.stream_state` stores, next to the applied position: slot name, the server's
system identifier, the timeline, and the slot's creation identity (**[test]** which catalog values identify a
recreated slot of the same name on Postgres 17 and 18).

| Finding | Meaning | Action |
|---|---|---|
| slot `confirmed_flush_lsn` ≤ applied position, same identity, applied position on the server's timeline history | continuous | stream from the slot; discard by position |
| slot ahead of the applied position | changes were consumed elsewhere or the slot was advanced | alert, automatic backfill of all CDC tables, stream from the slot's position |
| slot missing, recreated, or `wal_status = 'lost'` / invalidated | history is gone | alert, new slot, automatic backfill |
| different system identifier, or applied position not on the timeline history | different server or diverged history (failover) | as the replica rules of SPEC 4.1 decide: continue if the position is proven to be on the new history, else alert and backfill |

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
| leader / run | epoch | `starting` → `leading` → `releasing` → `released`; `superseded`; `failed` |
| slot and stream | slot | `unverified` → `verified` → `streaming` ⇄ `stalled`; `broken` → `rebuilding` → `verified` |
| table replication | table | `discovered` → `awaiting_approval` / `backfilling` → `streaming`; `streaming` → `backfilling`; `refresh_only`; `quarantined`; `retained`; `dropped` |
| backfill progress | table, backfill id | `planned` → `reading` → `catching_up` → `swapping` → `done`; `abandoned` |
| schema change | table, change id | `detected` → `applying` → `applied`; `failed` |
| batch commit / ack | group | `staged` → `committed` → `acknowledged`; `discarded` |
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
| 3 | Canonical comparison form for UNION and NaN (6.3) | P3, with the oracle |
| 4 | Staging tables versus direct apply (6.1) | decided by the Flight measurement in P2: direct for groups, staging tables only for 6.4 |
| 5 | Appends by the stream lane and reads by the backfill lane on one pending log do not conflict (7.1) | P3 test on MotherDuck |
| 6 | Identity of a recreated slot; reading the timeline history (8) | P3, stage 2 for PlanetScale |
| 7 | Publication join rule (7.5) | P4 discovery work |
| 8 | Guard over the Postgres endpoint; hand-over that starts the new leader before the old one stops | after P6, if the numbers ask for it |
| 9 | Key change with unchanged TOAST values: old key on the create record, chains resolved (6.2) | P3 (1.4) and P4 (2.6) tests |
| 10 | Several versions and missing rows after loading under several snapshots; repair read (7.3) | P3 handoff tests, workload fuzzer with TOAST values |
| 11 | A schema change of a table during its own backfill | P4 schema wave; first rule: the table's backfill starts again |
