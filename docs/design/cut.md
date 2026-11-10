# The cut: where a snapshot lies in the stream

Status: result of the P3 cut work. This document states the rule, argues it from the Postgres source, reports
what was measured, and proposes text for `docs/design/core.md` 7.2 and its open points 1 and 12.

Code: `src/cdc_v2/pg/cut.py` (the library the engine uses), `src/cdc_v2/testing/pgoutput.py` (decoder for
tests), `src/cdc_v2/testing/cutstress.py`, `cutprimary.py`, `cutmodel.py` and `tools/cutstress` (the tests).
Evidence: `evidence/p3/cut/` (index in its `README.md`). Every number below names its evidence file.

## 1. Terms

- **WAL**: the write-ahead log of Postgres. A **position** (LSN) is a byte offset in it.
- **Commit record**: the WAL record that commits a transaction. `S` is the position where it starts, `E` the
  position where it ends. The stream identifies a transaction by `E` (core design, section 2).
- **Stream**: what the logical replication slot delivers through `pgoutput`: whole transactions, each with its
  transaction id and its `E`.
- **Snapshot**: what one statement or one repeatable-read transaction can see. A transaction is **visible** to
  a snapshot when the snapshot sees its effects.
- **Replay**: a standby applies WAL records one after another. The **replay position**,
  `pg_last_wal_replay_lsn()`, is the end of the last record whose replay is complete.
- **Cut**: a function that answers `inside`, `outside` or `unknown` for a transaction of the stream.
  `inside` promises that the snapshot sees the transaction. `outside` promises that it does not. `unknown`
  promises nothing.
- **xid**: transaction id. The stream carries the 32-bit id of the top-level transaction. `pg_current_snapshot()`
  prints 64-bit ids. The library widens the 32-bit id to the 64-bit id nearest to the snapshot (`widen_xid`).
- **`xmin`, `xmax`**: the two bounds `pg_current_snapshot()` prints. Every id below `xmin` is finished. Every id
  at or above `xmax` counts as running. The **in-progress list** is the third field.
- **Quiet snapshot**: a snapshot with `xmin = xmax`. No id between the bounds exists, so nothing was running.
- **`P0`**: the stream position at which a table enters `backfilling` (core design 7.1).
- **Gap**: on a primary, the time between the moment a commit record is in the WAL and the moment the
  transaction becomes visible to new snapshots.

## 2. The rule on a standby

A cut on a standby is two replay positions around the moment the snapshot is taken.

- `R0` = `pg_last_wal_replay_lsn()` read by a statement that has **finished before** the snapshot is taken.
- `R1` = the same function read **after** the snapshot exists.
- `E1` = the `E` of the first transaction in the stream with `E > R1`.

For a transaction of the stream with commit record end `E`:

| Condition | Answer |
|---|---|
| `E ≤ R0` | `inside` |
| `E > E1` | `outside` |
| otherwise (`R0 < E ≤ E1`) | `unknown` |

Two ways to take the cut (`take_standby_cut_own`, `take_standby_cut_exported`):

- **own**: `SELECT pg_last_wal_replay_lsn()` in autocommit gives `R0`. Then
  `BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY`; its first statement takes the snapshot and reads `R1`
  and `pg_current_snapshot()`.
- **exported**: one read-committed transaction with three statements: `R0`; `pg_export_snapshot()`; `R1`.
  This way has a Postgres defect on every released minor version, see section 7. It is safe only for quiet
  snapshots, or on a server that carries the fix.

Three additions make `unknown` smaller. They are optional and each is proven below.

1. **By transaction id.** With the snapshot's `xmin` and `xmax`: id at or above `xmax` ⇒ `outside`; id below
   `xmin` ⇒ `inside`. On a standby the in-progress list is always empty, so ids between the bounds stay
   `unknown`.
2. **Quiet snapshot.** If `xmin = xmax`, addition 1 answers every transaction. The cut has no `unknown`.
3. **By the start of the commit record** (the sharper outside rule). A transaction whose commit record starts
   after the first record that follows `R1` is `outside`. In positions: `S > R1`, except when `R1` lies on a WAL
   page boundary and `S − R1 ≤ 40`, because then the record at `S` is the first record after `R1` (a page
   header of 24 or 40 bytes lies between). The plain form `S > R1 ⇒ outside` is wrong at page boundaries
   (model check, section 4).

Three rules of use:

- A snapshot is used only if `R0 ≥ P0`. Then everything the stream applied before the switch is `inside`.
- The stream **has passed** a snapshot when its applied position is greater than `R1`. The transaction at `E1`
  is then applied, and so is every `unknown` transaction.
- A cut is taken only on a server that is in recovery (`pg_is_in_recovery()`), and only when the standby is
  **settled** (section 8). The library checks both.

## 3. Why the rule holds

The argument uses four facts about Postgres. Each was read in `REL_16_STABLE`, `REL_17_STABLE` and
`REL_18_STABLE` (branch heads of 2026-11-09). The code is the same in the three branches apart from renames
(`ShmemVariableCache` became `TransamVariables` in 17). The table gives file, function and the line where the
function starts in 16 / 17 / 18.

| Fact | File and function | Line 16 / 17 / 18 |
|---|---|---|
| Replay is serial; the position moves after the record is applied | `xlogrecovery.c` `ApplyWalRecord` | 1874 / 1924 / 1943 |
| The SQL function returns that position | `xlogrecovery.c` `GetXLogReplayRecPtr`; `xlogfuncs.c` `pg_last_wal_replay_lsn` | 4544 / 4599 / 4630; 362 / 356 / 357 |
| A commit becomes visible inside the replay of its commit record | `xact.c` `xact_redo_commit` | 5968 / 6090 / 6151 |
| The flip is atomic for the whole transaction tree | `procarray.c` `ExpireTreeKnownAssignedTransactionIds`, `MaintainLatestCompletedXidRecovery` | 4456 / 4476 / 4477; 991 / 989 / 989 |
| A standby snapshot is built from the same shared state | `procarray.c` `GetSnapshotData`, `KnownAssignedXidsGetAndSetXmin` | 2201 / 2177 / 2175; 5126 / 5131 / 5132 |
| Visibility test for one id | `snapmgr.c` `XidInMVCCSnapshot` | 2322 / 1891 / 1905 |

**Fact 1: replay is serial and the position moves last.** `ApplyWalRecord` does three things for each record,
in this order: it stores the record's end in `replayEndRecPtr`; it calls the record's redo function; it stores
the record's end in `lastReplayedEndRecPtr`. `pg_last_wal_replay_lsn()` returns `lastReplayedEndRecPtr`. One
process does this, one record at a time. So when the function returns a value `R`, every record that ends at or
before `R` is completely applied, and at most one later record is being applied: the first record after `R`.

**Fact 2: the commit becomes visible during the redo of the commit record.** `xact_redo_commit` first marks the
transaction committed in the commit log, then calls `ExpireTreeKnownAssignedTransactionIds`. That function
takes `ProcArrayLock` exclusively, removes the top-level id and all subtransaction ids from the list of ids the
standby treats as running, and moves `latestCompletedXid` forward. This is the moment of the flip.

**Fact 3: a snapshot reads that state under the same lock.** On a standby, `GetSnapshotData` takes
`ProcArrayLock` in shared mode, sets `xmax = latestCompletedXid + 1` and copies the running ids below `xmax`.
A snapshot is therefore taken either entirely before a flip or entirely after it. `XidInMVCCSnapshot` then
treats an id as running if it is at or above `xmax` or in the copied list; any other id is judged by the commit
log, which was set before the flip.

**Fact 4: the flip covers the whole tree.** The commit record lists all subtransaction ids. They leave the
running list in the same critical section. A snapshot sees all of a transaction or nothing of it.

**Inside.** Let `E ≤ R0`. By fact 1 the commit record was completely applied when `R0` was read. By fact 2 the
flip had happened. The snapshot is taken later, so by fact 3 it is taken after the flip. It sees the
transaction. This needs `R0` to be read by a statement that finished before the snapshot was taken. A position
read by the statement that takes the snapshot does not qualify: the statement's snapshot is taken first and the
function runs later, so the position can be newer than the snapshot. Section 5.4 shows violations when such a
value is used as `R0`.

**Outside.** `R1` is read after the snapshot exists. By fact 1, when `R1` was read, no record after the first
record that follows `R1` had begun to be applied. A commit record that starts after that first record had not
begun; its flip had not happened; the snapshot, which is older than the reading of `R1`, does not see the
transaction. That is the sharper rule (addition 3). The rule in `E` follows: the first record after `R1` ends
after `R1`. If it is the commit record of a stream transaction, that transaction has the smallest `E` above
`R1`, which is `E1`. Every transaction with `E > E1` has a commit record that starts at or after the end of
that first record, so it is `outside`.

**Unknown.** A transaction with `R0 < E ≤ R1` was flipped between the two readings; the snapshot may be older
or newer than the flip. The transaction at `E1` may have been inside its redo when the snapshot was taken:
after the flip, before the position moved. `xact_redo_commit` does more work after the flip (invalidation
messages, lock release, and for a transaction that dropped tables a WAL flush and the file deletions), so this
window is real.

**By transaction id.** On a standby, `GetSnapshotData` starts with `xmin = xmax` and lowers `xmin` to the oldest
id in the running list (`KnownAssignedXidsGetAndSetXmin`). An id at or above `xmax` is treated as running
(fact 3), so its transaction is `outside`. An id below `xmin` is not in the list and is judged by the commit
log; a transaction of the stream is committed, so it is `inside`. If `xmin = xmax` the list is empty and every
id falls on one side. `pg_current_snapshot()` prints the list `xip`; on a standby the running ids are kept in
the other list (`subxip`), so the printed in-progress list is always empty (`xid8funcs.c`
`pg_current_snapshot`, line 409 / 370 / 370).

**`R0 ≥ P0`.** `P0` is the `E` of a transaction the stream delivered. Every transaction the stream applied
before the switch has `E ≤ P0 ≤ R0` and is `inside`. The condition is checked for every snapshot. It is not
implied by anything else:

- With the slot on the same standby it holds while the standby keeps running, because the slot decodes only WAL
  the standby has replayed (`walsender.c` `XLogSendLogical`, `logicalfuncs.c`
  `pg_logical_slot_get_changes_guts`: the end of WAL in recovery is the replay position). It can fail after the
  standby restarts. A standby replays again from its restart point and accepts queries as soon as it reaches
  its consistency point (`xlogrecovery.c` `CheckRecoveryConsistency`, 2142 / 2192 / 2212), which can lie
  before what the slot delivered earlier. Section 5.6, variant 5, has such cuts.
- With the slot on the primary and the snapshots on a standby it fails whenever the standby lags (variant 6).

In both cases the backfill waits.

**Has passed.** The stream delivers transactions in the order of `E` (section 6). When the applied position is
greater than `R1`, the stream has applied a transaction with `E > R1`, hence the one at `E1` and every
transaction before it.

What the argument does not cover is the state of a standby right after it starts. Section 8 shows that the
`inside` rule can fail there, and gives the condition under which it holds.

## 4. Model check

The model check tests the argument of section 3 by enumeration. It does not test Postgres. It answers one
question: given the order of operations read from the source, is the rule correct for **every** interleaving of
replay and reader, or only for the ones a live test happens to hit?

The model (`src/cdc_v2/testing/cutmodel.py`, class `Standby`) is a standby that replays a short WAL. Each record
is replayed in the atomic steps of section 3: ids become known, the commit flips visibility, the position moves.
A reader does three things at any three points between those steps: read `R0`, take the snapshot, read `R1`. A
**world** is one WAL: a choice of transactions (with or without a subtransaction, in the stream or filtered out
of it), an order of write, commit, abort and other records, an order in which ids were assigned, a start position
(including one that puts a record end on a page boundary and on a segment start). The check runs every reader
against every world and compares every answer of every rule with what the snapshot sees.

| Size | Worlds | Reader interleavings | Result |
|---|---|---|---|
| 1 transaction, up to 3 records | 804 | 47,060 | no sound rule violated |
| 2 transactions, up to 3 records | 11,280 | 1,178,688 | no sound rule violated |
| 2 transactions, up to 4 records | 65,280 | 9,638,328 | no sound rule violated |

Evidence: `evidence/p3/cut/model.txt` (command `tools/cutstress model --sizes 1:3,2:3,2:4`). The default
test run (`tests/cut/test_cut_model.py`) explores two smaller sizes in about a second: 1 transaction with up to
4 records, 2 transactions with up to 2 records.

Rules that hold in every world: `inside`, `outside`, by id below `xmin`, by id at or above `xmax`, the sharper
outside rule with the page-boundary exception, the library's combined answer, "has passed", and `R0 ≥ P0`.

Rules that the model refutes, each with a printed counterexample:

| Wrong rule | Why it fails |
|---|---|
| `E ≤ R1 ⇒ inside` | the commit was replayed between the snapshot and the reading of `R1` |
| `E > R1 ⇒ outside` (no allowance for `E1`) | the snapshot was taken after the flip of the record being replayed, before the position moved |
| `S > R1 ⇒ outside` (no page-boundary exception) | `R1` is on a page boundary, the commit record starts after the page header and is being replayed |
| id below `xmax` ⇒ `inside` | the transaction was still running; on a standby it is not printed in the in-progress list |
| id at or above `xmin` ⇒ `outside` | the transaction committed before the snapshot while an older one was still running |

The model also runs a deliberately wrong server that moves the position before the flip. There `inside`,
`R0 ≥ P0` and the library's answer are violated. So the check does notice when the server's order is not the one
the rule assumes. That order is the one fact the rule rests on, and section 3 shows where it is in the source.

The primary has its own model (class `Primary`): a transaction gets an id, writes its commit record (the slot can
deliver it from then on), and later leaves the set of running transactions (snapshots see it from then on).
Histories of up to 3 transactions (7, 145 and 6,265 histories, and 535 with two markers) confirm the rules of
section 10, including the library's gap tracker, and refute three wrong ones.
