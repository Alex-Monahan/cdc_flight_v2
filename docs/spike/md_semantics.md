# Spike: MotherDuck semantics the design depends on (PLAN P2, §2 A item 3)

Date: 2026-11-09/10. Client: Python `duckdb` 1.5.6 (the pinned version), `duckdb.connect("md:<db>?attach_mode=single")`,
token from `tools/with-env`. Every concurrency test uses **separate processes, each with its own connection**
(`multiprocessing` with the `spawn` start method); no test relies on two cursors of one connection. All databases were
named `cdc_v2_spike_md_*` and every script drops its database at the end.

Scripts: `spikes/md_semantics/`. Evidence: `evidence/p2/md_semantics/` (one `.txt` log and one `.json` with the raw
numbers per script). Every script is re-runnable: `tools/with-env uv run [--with pyarrow --with pandas] python
spikes/md_semantics/<script>.py`.

**All timings were taken from a laptop over the public internet** (round trip to MotherDuck about 75-80 ms for the
simplest remote query). They are indications only. Server-side work is real; anything that crosses the network is
bounded by this machine's round-trip time and uplink. The numbers that count come later, from a Flight.

Public documentation read first: MotherDuck connection-string parameters and attach modes, the MotherDuck
architecture and limitations page, DuckDB's concurrency page (MVCC with optimistic concurrency control: appends never
conflict; concurrent updates or deletes of the same row conflict; the losing transaction must be retried). None of
them describes transactional DDL, where clock functions are evaluated, or what happens to a client's open
transaction when the client dies, so all of that was measured.

## Findings that contradict SPEC or force a design choice

1. **`now()`, `current_timestamp`, `get_current_timestamp()`, `transaction_timestamp()` are evaluated by the client,
   not by MotherDuck** (SPEC 4.2, single clock). The client planner folds them into literals before the plan is sent,
   even inside remote operators, and inside a transaction they all return the client's transaction start time. Column
   `DEFAULT now()` behaves the same. Heartbeat age must use `uuid_extract_timestamp(uuidv7())`, which is evaluated by
   the server (proof and skew in Q6).
2. **A column with a PRIMARY KEY, UNIQUE constraint or index cannot change type, and a UNION column cannot be a key
   or be indexed** (SPEC 2.5, R9, 2.4). Consequence: destination tables carry no PK/UNIQUE constraints and no indexes;
   keys are enforced by the applier. `INSERT OR REPLACE` (needs a PK) is therefore not an apply option; `MERGE` and
   `DELETE … USING` + `INSERT` work without one and work on UNION keys.
3. **A frozen (SIGSTOPped) leader in the middle of a transaction blocks the takeover for about 30 s**, because
   MotherDuck keeps that transaction's row versions until a server-side lease expires ("time-based lease expired",
   measured 32 s). The takeover must call `md_interrupt_server_connection(<old leader's client_connection_id>)`:
   that ends the old transaction at once and the takeover then succeeds within about 0.2-2 s (SPEC 4.2). The leader
   record therefore stores the leader's `client_connection_id`.
4. **A dead client's COMMIT can land after the client is gone** (up to 1.95 s after SIGKILL in these trials), and is
   then fully applied — never partially (SPEC 1.1, 1.7). **A dead client's open transaction keeps its rows locked for
   about 30 s** (SIGKILL: 29.4-29.9 s, 10/10 trials), so a run restarting after a crash conflicts on the leader or
   state row for up to 30 s unless it interrupts the dead connection (finding 3). A restarting run must read its position from MotherDuck, never
   assume the last unacknowledged batch failed. (Exactly-once by position, SPEC 1.1, already requires this.)
5. **One shared state row updated by parallel apply transactions makes them conflict** (SPEC 1.3, 5.3, 3.3). Parallel
   appliers need separate state rows (one per stream or per table group), or must be serialised.
6. **The fixed cost of a destination transaction is a few round trips** (3.8 commits/s per connection from this
   laptop; 4 connections gave 15/s). SPEC 5.3 (2,000 source TPS) is only reachable by **grouping many source
   transactions into one destination transaction** (allowed by 1.3: a source transaction is never split, but several
   may share one commit). Statements in one transaction are sent one round trip each; a multi-statement string does
   not save round trips.
7. **DuckDB does not detect a delete/update conflict on the same row** (only update/update and delete/delete). Never
   use DELETE as part of the fencing protocol; the leader row is only ever UPDATEd.
8. `ALTER COLUMN … TYPE TIMESTAMPTZ` from `TIMESTAMP` interprets the old values in the session `TimeZone`, which the
   client sends to the server: the applier must run with `SET TimeZone = 'UTC'`.

## Q1 Atomic swap (SPEC 3.2, R7) — `swap.py`, evidence `swap.txt`, `swap_long_scan.txt`

A writer process swapped a new version of `t` in with `BEGIN; DROP TABLE t; ALTER TABLE t_staging RENAME TO t;
COMMIT` **1,000 times** while two reader processes polled `count(*)`, `sum(id)`, `min(ver)`, `max(ver)` as fast as
they could (7,914 observations, every one of the 1,001 versions seen). **Zero anomalies**: never a missing table,
an empty table, a partial or mixed version or an error, and a reader never saw an older version after a newer one.
`CREATE OR REPLACE TABLE t AS SELECT * FROM t_staging` inside a transaction (300 swaps), rename-old-away-then-rename-
new-in (300) and swaps that change the table's schema on every other version (300) behaved identically: zero
anomalies. The control — the same two statements without a transaction — produced 647 "table does not exist"
observations in 300 swaps, which shows the detector works. A reader inside its own transaction keeps reading the
version it first saw (18 of 18 transactions, each spanning 2-3 swaps; no error at its next read or at COMMIT).
Long scans (about 1.1 s, 40M rows processed per scan) across 20 swaps of a 5M-row table: 96 of 96 consistent.
DDL is fully transactional: for drop+rename, `CREATE OR REPLACE`, `ADD COLUMN` with DML, `ALTER TYPE` with DML,
`RENAME COLUMN` and `CREATE TABLE` with DML, a ROLLBACK restored exactly the previous catalog and data, and a second
connection saw nothing of the open transaction. Swap transaction duration p50 0.35 s from this laptop (4 round
trips). **Design:** R7 holds as written; use drop+rename (simplest). Note from Q4: a writer that inserts into `t`
while a swap of `t` commits fails ("has been dropped by a different transaction"), so CDC apply to a table must not
run concurrently with that table's swap — the 3.3 handoff state machine serialises them.

## Q2 Position and data in one transaction (SPEC 1.1, 1.3) — `atomic_multi.py`, evidence `atomic_multi.txt`, `atomic_multi_c.txt`

Four writer processes, each owning two data tables and a state row, committed **6,000 transactions** (each: paired
inserts into both tables, set-based updates of a varying subset of both, state update). Two reader processes checked
the cross-table invariant (count(a) = count(b) = state.rows, sum(a) + sum(b) = 0, max id + 1 = state.rows) in one
statement 16,820 times, and a third checked it with six separate statements inside one transaction 1,333 times:
**zero violations**. **Visibility:** a commit is visible to another process as soon as COMMIT returns: in 9,198 polls
no poll that started after COMMIT returned saw the older position (zero stale reads); first-seen minus
COMMIT-returned p50 0.002 s, p99 0.094 s, max 0.146 s (bounded by the 80 ms poll interval). **Crash:** a client was
SIGKILLed inside a 600,000-row transaction (two tables + state row). Killed mid-transaction: nothing applied (10/10; rerun 10/10).
Killed after sending COMMIT (random delay 0-1.5 s): every outcome was all-or-nothing and the invariant always held;
in 9 of 30 (first run) and 3 of 10 (rerun) trials the commit landed **after** the kill (0.17-1.95 s later), i.e. the
client could not know whether it committed. **Killed while holding an open transaction** after all its writes
(rerun, `atomic_multi_c.txt`): nothing was applied, but the dead client's server-side transaction kept the state row
locked for **29.4-29.9 s** (10 of 10 trials; another client's UPDATE of that row got "Conflict on update!" until
then) — the same ~30 s lease as for a frozen client in Q5. (In the first run one trial's client could not even
start: its state-row UPDATE conflicted with the transaction of the client killed 20 s earlier; that trial is the one
"during_commit applied=False".) **Design:** the position-in-the-same-transaction protocol (1.1) is sound on MotherDuck; on
restart, read the position from MotherDuck; expect the fencing UPDATE of a restarted run to conflict until the dead
run's server transaction has ended, and end it with `md_interrupt_server_connection` (Q5).

## Q3 Transaction size and apply shapes (SPEC 1.3, 5.1, 5.2, 5.3, 5.4) — `txn_size.py`, evidence `txn_size.txt`

One transaction inserting 10M lineitem-width rows (server-side generator), then updating all 10M, then deleting all
10M, committed in 21.4 s (insert 20.8 s — dominated by the generator's per-row string work —, update 0.24 s,
delete 0.28 s, commit 0.08 s); no limit, error or timeout; client peak RSS 88 MB (the client holds nothing: the
transaction lives on the server). The same with 10M rows that survive: insert 20.1 s, update 0.14 s, **commit
8.9 s**. **50M rows** (`txn_size_1.txt`): insert+update+delete in one transaction 102.7 s (insert 101 s, update
0.8 s, delete 0.7 s, commit 0.1 s); 50M surviving rows: insert 101 s, update 0.2 s, **commit 38.4 s**; no limit or
timeout, client RSS 89 MB. (Updates and deletes in Part 1 hit rows inserted by the same transaction; updates and
deletes of committed rows are measured in Part 2.) **Apply shapes** from a staged table already on the server, target 10M rows, one transaction each:

| shape | 10K rows | 1M rows | 10M rows |
|---|---|---|---|
| INSERT … SELECT | 46K/s | 1.5M/s | 7.5M/s |
| DELETE … USING | 39K/s | 2.2M/s | 5.0M/s |
| UPDATE … FROM | 57K/s | 1.4M/s | 10.2M/s |
| MERGE (half update, half insert) | 42K/s | 170K/s; repeat 190K/s | 4.7M/s |
| DELETE … USING + INSERT (upsert) | 36K/s | 1.1M/s | 3.3M/s |
| INSERT OR REPLACE (needs a PK) | 33K/s | 0.6M/s | 1.2M/s |

At 10K rows every shape costs about 0.2-0.3 s, i.e. round trips; from 1M rows up the server work dominates and all
shapes except INSERT OR REPLACE are far above 100K rows/s. MERGE at 1M rows was slow twice (5.9 s and 5.3 s,
`txn_size_2.txt` is the repeat) although it took 2.1 s at 10M rows: MERGE performance is erratic, `DELETE … USING` +
`INSERT` is consistently fast. **Small transactions** sent from the client (one
INSERT … VALUES, one state UPDATE, COMMIT): 3.8 commits/s with 1-row batches and 3.1/s with 100-row batches on one
connection (p50 0.26 s / 0.32 s ≈ 3-4 round trips), 15.1/s and 11.9/s on four connections. **Design:** apply with
set-based statements from a staged table (`DELETE … USING` + `INSERT`, or `MERGE`); avoid PK-dependent shapes
(finding 2); group many source transactions per destination commit (finding 6); 5.2 latency on a Flight is a few
round trips plus upload time, far below 30 s; a single 10M-row source transaction fits in one destination
transaction with bounded client memory (5.1, 5.4).

## Q4 Concurrent writers (SPEC 4.2, 3.3) — `concurrent_writers.py`, evidence `concurrent_writers.txt`

Two processes, exact interleavings, each case in two orders ("overlap": both statements before either commits;
"stale": the second transaction started before the first committed and writes after it). **No conflict:** different
tables; append/append on one table; updates of different rows (also two set-based updates of disjoint halves);
update/append; DDL on one table with DML or DDL on another; two `CREATE TABLE`s. **Conflict, at statement time, the
second writer fails immediately, without waiting** (first writer wins, even while the first has not committed):
update/update of the same row and delete/delete ("Conflict on update!", "Conflict on tuple deletion!"); two
transactions updating one shared state row; DDL then DML on the same table ("Transaction conflict: attempting to
insert into / update table "t1" but it has been altered by a different transaction"); DDL/DDL on one table
("Catalog write-write conflict on alter"); `ALTER TYPE` vs INSERT; swap vs a later INSERT into the swapped table.
**Conflict at COMMIT:** an INSERT whose table was altered (or dropped by a swap) by a transaction that committed
first ("Failed to commit: … has been altered/dropped by a different transaction"). **Not detected:** delete vs update
of the same row — the update reports 1 row and is silently lost (finding 7). All error classes are
`TransactionException` (a `ConstraintException` for primary keys). **Retry:** re-running the whole losing transaction
succeeded in every case, except where the other side changed the table's schema (`ADD COLUMN`): the retried
statement then fails to bind, so after a DDL conflict the applier must re-read the schema and rebuild its
statements. **Design:** one applier writes each table; parallel appliers use disjoint tables and separate state rows;
conflicts are retried as a whole transaction after re-planning.

## Q5 Leader record as a compare-and-swap (SPEC 4.2) — `leader.py`, evidence `leader.txt`

Four processes met at a barrier and raised the epoch at the same instant. **Exactly one winner in every round**:
autocommit `UPDATE leader SET epoch = epoch + 1, owner = ? WHERE epoch = ?` 1,000/1,000 rounds; the same inside
BEGIN/COMMIT 300/300; read-then-update in one transaction 300/300; INSERT of epoch+1 into a table keyed on the epoch
300/300. Losers observe either **zero rows changed** (2,633 times; the winner had already committed) or **"Conflict
on update!"** (367 times; the winner's transaction was still open) for the autocommit form, always the conflict error
for the transactional forms, and "Duplicate key … violates primary key constraint" (once at COMMIT) for the insert
form. A loser must treat both outcomes as "lost". **Fencing:** every data transaction contains
`UPDATE leader SET hb = uuid_extract_timestamp(uuidv7()) WHERE epoch = :mine AND owner = :me` and aborts unless it
changed exactly one row. 12 SIGSTOP/SIGCONT trials (fence as first or last statement, leader paused inside a
transaction or between transactions, with and without a shared data row both leaders write) **all passed**: after the
newcomer's takeover no write of the old leader from the paused transaction on was committed, the newcomer's three
fenced transactions committed, and the resumed old leader saw its COMMIT fail ("interrupted by user" or "Conflict on
update!") or its next fence update change zero rows, and stepped down. With the fence as the first statement the
paused leader holds the leader row, so the takeover first gets a conflict and must interrupt the old connection
(takeover 0.5-2.2 s); with the fence as the **last** statement the takeover succeeded at once (0.16-0.23 s) unless a
data row was shared. Without the interrupt, MotherDuck itself ended the paused transaction after **32 s** ("time-based
lease expired"). Checking the epoch costs one statement (one round trip) per transaction. **Design:** fence as the
last statement before COMMIT; store the leader's `client_connection_id` (a connection finds its own id with
`SELECT client_connection_id FROM md_active_server_connections() WHERE client_query LIKE '%<fresh marker>%'`) and
interrupt it on takeover. Caution: `md_active_server_connections()` lists all connections of the account; the code
must only ever filter it by its own marker or a stored id.

## Q6 A single clock (SPEC 4.2) — `clock.py`, evidence `clock.txt`

MotherDuck's EXPLAIN marks every operator local (L) or remote (R). For `SELECT <expr> FROM <md table>`,
`now()`, `current_timestamp`, `get_current_timestamp()`, `transaction_timestamp()`, `current_localtimestamp()` and
`epoch_ms(now())` appear in the remote projection as **timestamp literals computed by the client planner**; in
`UPDATE … SET x = epoch_ms(now())` and `INSERT … SELECT now()` too. Inside a client transaction opened 5 s earlier,
`now()` and `current_timestamp` returned exactly the client's BEGIN time (0 ms difference) and
`get_current_timestamp()` also returned that time (5.08 s old); column defaults `DEFAULT now()` and
`DEFAULT get_current_timestamp()` stored the client's transaction start time as well. `uuidv7()` is not folded: it
appears as a function call in the remote projection, and `uuid_extract_timestamp(uuidv7())` returned the current
time. Proof that it comes from a **different clock**: in 277 of 300 round trips the server timestamp lay outside the
client's own [send, receive] window — impossible if one clock produced both. Skew: server ahead of this laptop by
**88 ms ± 37 ms** (from the fastest 10 % of round trips, rtt 75 ms). A heartbeat written and read with the server
clock showed an age of 2.19 s after a 2 s sleep. The session `TimeZone` is applied on the server too (a +05:45
setting showed in server-rendered text). **Design:** write the heartbeat as `uuid_extract_timestamp(uuidv7())` and
compute its age in the same SQL expression on the server; never use `now()` for anything compared across runs.

## Q7 Type machinery (SPEC 2.4, 2.5, R9) — `types.py`, evidence `types.txt`

Works on MotherDuck, inside one transaction together with INSERT and UPDATE, and rolls back cleanly: INTEGER → BIGINT,
SMALLINT → INTEGER, FLOAT → DOUBLE, DECIMAL(10,2) → DECIMAL(38,2), DECIMAL(18,4) → DECIMAL(38,10), VARCHAR(5) →
VARCHAR, TIMESTAMP → TIMESTAMPTZ (finding 8); BIGINT → `UNION(i BIGINT, f DOUBLE)` (implicit, and with
`USING union_value(i := v)`), DOUBLE → `UNION(f DOUBLE, i BIGINT)`, VARCHAR → `UNION(s VARCHAR, i INTEGER)`,
DECIMAL(38,10) → `UNION(n DECIMAL(38,10), s VARCHAR)` holding 'NaN' and '1e400' as text, and a second change adding a
member (`UNION(i, f)` → `UNION(i, f, s)`). Existing values keep their member. On a 10M-row table each change took
0.4-0.7 s including a 1,000-row insert in the same transaction (VARCHAR → UNION 1.5 s). UNION values work in
INSERT … SELECT from a plain staged column (the member is chosen by type), in UPDATE, in joins, in MERGE and in
DELETE … USING. **Unsupported (loud):** changing the type of a PK/UNIQUE column ("Cannot change the type of a column
that has a UNIQUE or PRIMARY KEY constraint specified") or of an indexed column; a PK on a UNION column ("Invalid type
for index key") — finding 2. **Semantics to design for:** values of different members never compare equal
(`union(i:1) <> union(f:1.0)`; `WHERE k = 1` matches only the BIGINT member), ORDER BY sorts by member first,
`count(DISTINCT)` counts 1 and 1.0 separately, and `hash()` of a UNION value differs from `hash()` of the plain value
(hashes are equal on client and server). The oracle checksum must therefore canonicalise, e.g. hash `k::VARCHAR`.
**Arrow:** a UNION column arrives as an Arrow `sparse_union` and an Arrow union inserts back correctly. **Parquet:**
COPY writes a UNION as `STRUCT(tag UTINYINT, i, f, s)`; inserting that struct back into the UNION column restores the
values.

## Q8 Bulk load at lineitem width (SPEC 3.1), first indication only — `bulk_load.py`, evidence `bulk_load.txt`, `bulk_load_tpch.txt`

**This machine's uplink limits every client-side number below; the numbers that count are measured by running the
same script inside a Flight** (it takes everything from environment variables and writes only to a temporary
directory). Synthetic lineitem-shaped data (16 B/row as zstd Parquet — it compresses better than real TPC-H data):

| method | 1M rows | 10M rows |
|---|---|---|
| INSERT … SELECT FROM read_parquet (local files) | 209K/s | 655K/s |
| COPY t FROM local Parquet | 338K/s | 646K/s |
| Arrow RecordBatchReader stream | 333K/s | 793K/s |
| read_csv | 391K/s | 675K/s |
| 4 processes, Parquet, one table | (1 file) 311K/s | 822K/s |
| server: INSERT … SELECT generator | 390K/s | 465K/s (generator-bound) |
| server: INSERT … SELECT from another table | 1.9M/s | 7.2M/s |

The appender path (`DuckDBPyConnection.append` of a pandas frame) failed on DECIMAL columns (pandas holds them as
Python objects and the inferred type was DECIMAL(3,2)); the script now sends them as float64. Real TPC-H lineitem
(`dbgen` sf 1, 6.0M rows, 25 B/row as zstd Parquet, `bulk_load_tpch.txt`): COPY from Parquet 497K rows/s, Arrow
stream 548K rows/s, pandas `append` 255K rows/s, 4 processes 710K rows/s (17.7 MB/s of Parquet). **Design:** a 1M-row batch takes ~1.5-3 s to upload even from a laptop, so the staged-load path
(Arrow stream or Parquet into a staging table, then server-side INSERT … SELECT / swap) has headroom above 100K rows/s;
the server-side copy rate (7M rows/s) is not the bottleneck.

## Unfinished or limited

- Long scans across swaps lasted about 1.1 s each (40M rows processed per scan), not tens of seconds; longer scans were
  not tried.
- The SIGKILL "during COMMIT" trials could not separate "COMMIT not yet received by the server" from "received":
  outcomes are reported per trial; none was partial.
- Server-side fixed cost of a commit could not be separated from the laptop's round trips; Part 3 of Q3 must be
  repeated on a Flight (same script) to bound SPEC 5.2/5.3 properly.
- Bulk-load numbers are laptop-uplink numbers; `bulk_load.py` is ready to run unchanged in a Flight
  (`CDC_V2_SPIKE_TMP`, `CDC_V2_SPIKE_EVIDENCE`, `CDC_V2_BULK_*` from the environment).
- The idle-transaction lease (~30 s) was measured, not documented by MotherDuck; whether it is configurable was not
  found in the public documentation.
