# The cut: where a snapshot lies in the stream

Status: result of the P3 cut work, revised after the coordinator's review. This document states the rule,
argues it from the Postgres source, reports what was measured, and proposes text for `docs/design/core.md` 7.2
and its open points 1 and 12.

Code: `src/cdc_v2/pg/cut.py` (the only code that takes or judges a cut), `src/cdc_v2/testing/pgoutput.py`
(decoder for tests), `src/cdc_v2/testing/cutnarrow.py` (a narrowing that only the tests can use, section 2),
`src/cdc_v2/testing/cutstress.py`, `cutprimary.py`, `cutmodel.py` and `tools/cutstress` (the tests).
Evidence: `evidence/p3/cut/` (index in its `README.md`). Every number below names its evidence file.

## Result in brief

- **The standby rule of core 7.2 holds**, with two conditions and one correction that 7.2 does not have yet.
  Tested by a model check over every interleaving of small histories and by stress runs against Postgres 18.6.
  Seven runs had the complete checker: 1,192,278 cuts over 5,025,620 transactions. Seven earlier runs, the
  main campaign among them (2,982,523 cuts over 9,353,889 transactions), ran before the checker's last step
  existed; section 5.1 says exactly which. No violation of a rule in any of them. Six wrong rules were caught,
  each many times (sections 3 to 5).
- **Condition 1: the standby is settled.** A standby that has just started can hide a committed transaction
  whose commit record it has replayed. The `inside` rule then fails. A cheap check repairs it (section 8).
- **Condition 2: an exported snapshot is quiet, or the server carries a fix that is not released yet.** On
  every released version a session that imports a snapshot on a standby can see transactions the exporter does
  not see, and can spoil rows for every other reader of that standby. The rule for tables without a key is
  decided: a quiet exported snapshot within a bounded, declared wait, otherwise one reader with its own
  snapshot (section 7).
- **Correction: `R0 ≥ P0` is checked for every snapshot**, also on the standby that holds the slot. After a
  restart its replay position can lie before what the slot delivered (sections 3 and 5.6).
- **The unknown set is small.** With the snapshot's bounds, about two of three cuts under load (66.93% in the
  main campaign) and every cut on an idle source have no `unknown` transaction at all. The start of the commit
  record would narrow it further, but the pipeline's stream does not deliver it (sections 2 and 5.2).
- **A cut belongs to the timeline it was taken on.** After a promotion or a slot finding, open cuts are
  abandoned and their backfills start again (section 9.2).
- **On a primary** the gap between "commit record written" and "visible to snapshots" is real and can last as
  long as a synchronous standby is away. The answer by 64-bit transaction id is exact (197,212 cuts, 110 million
  answers, none wrong). `R0 ≥ P0` is replaced by a rule that needs no wait in the normal case. The
  `pg_xact_status` check proposed in review does not work (sections 10 and 11).
- **Proposed text** for core 7.2, its open points 1 and 12, and small changes to 7.1, 7.3 and 7.4: section 13.
  What is refuted and what stays open: section 12.

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
- **Timeline**: the number of one WAL history. A promotion starts a new timeline. A position is meaningful
  only together with its timeline.
- **Settle floor**: the transaction id a standby snapshot's `xmax` must have reached before a cut is used
  (section 8).

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

Two additions make `unknown` smaller. They are optional and each is proven below. The library has both
(`StandbyCut.classify(e, xid)`).

1. **By transaction id.** With the snapshot's `xmin` and `xmax`: id at or above `xmax` ⇒ `outside`; id below
   `xmin` ⇒ `inside`. On a standby the in-progress list is always empty, so ids between the bounds stay
   `unknown`.
2. **Quiet snapshot.** If `xmin = xmax`, addition 1 answers every transaction. The cut has no `unknown`.

A third addition is sound and **is not available to the pipeline**:

3. **By the start of the commit record** (the sharper outside rule). A transaction whose commit record starts
   after the first record that follows `R1` is `outside`. In positions: `S > R1`, except when `R1` lies on a WAL
   page boundary and `S − R1 ≤ 40`, because then the record at `S` is the first record after `R1` (a page
   header of 24 or 40 bytes lies between). The plain form `S > R1 ⇒ outside` is wrong at page boundaries
   (model check, section 4).

   Raw pgoutput gives `S` (section 6). Stock Debezium does not: no field of its records equals `S`
   (`docs/spike/local_streaming.md` section 4: 0 of 1,222 transactions; `docs/design/engine.md` section 5: the
   position of a BEGIN record lies below `S`; `docs/design/core.md` section 2: "the commit record's start LSN
   is not available from Debezium"). The engine therefore never has `S`. The library
   has no argument for it. The addition lives in `src/cdc_v2/testing/cutnarrow.py` (`starts_after_r1`,
   `classify_with_start`), a module of the test tools, which the test decoder feeds. Sections 4 and 5 report it
   as a result and as a measurement. Nothing in the proposed design text (section 13) uses it.

Four rules of use:

- A snapshot is used only if `R0 ≥ P0`. Then everything the stream applied before the switch is `inside`.
- The stream **has passed** a snapshot when its applied position is greater than `R1`. The transaction at `E1`
  is then applied, and so is every `unknown` transaction.
- A cut is taken only on a server that is in recovery (`pg_is_in_recovery()`), and only when the standby is
  **settled** (section 8). The library checks both. The settle floor is not optional: every taker needs it.
- A cut is valid only on the **timeline** it was taken on (section 9.2). After a promotion or a slot finding
  (core design, section 8), open cuts are abandoned and their backfills start again
  (`StandbyCut.valid_on(timeline)`).

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
transaction. That is the sharper rule (addition 3, test only). The rule in `E` follows: the first record after
`R1` ends after `R1`. If it is the commit record of a stream transaction, that transaction has the smallest
`E` above `R1`, which is `E1`. Every transaction with `E > E1` has a commit record that starts at or after the
end of that first record, so it is `outside`.

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

Evidence: `evidence/p3/cut/model.txt` (command `tools/cutstress model --sizes 1:3,2:3,2:4`).

The check runs at three levels, so that the default test run stays fast:

| Where | Sizes (transactions : records) | Worlds | Reader interleavings | Time |
|---|---|---|---|---|
| default test run (`tests/cut/test_cut_model.py`) | 1:2 and 2:2; 60 random larger worlds | 1,416 | 91,860 | under 1 s |
| the same file with the marker `localpg` | 1:4 and 2:3; 600 random larger worlds | 13,704 | 1,390,340 | about 10 s |
| `tools/cutstress model` (default sizes 1:4 and 2:3; any size with `--sizes`) | as asked | | | seconds; about half a minute for the sizes of the evidence file |

The marker `localpg` only keeps the larger sizes out of the default run; they need no Postgres. The small sizes
still refute every wrong rule of the table below (`test_every_wrong_rule_is_refuted`).

Rules that hold in every world: `inside`, `outside`, by id below `xmin`, by id at or above `xmax`, the library's
combined answer, "has passed", and `R0 ≥ P0`; and, for the tests only, the sharper outside rule with the
page-boundary exception and the library's answer narrowed by it.

Rules that the model refutes, each with a printed counterexample:

| Wrong rule | Why it fails |
|---|---|
| `E ≤ R1 ⇒ inside` | the commit was replayed between the snapshot and the reading of `R1` |
| `E > R1 ⇒ outside` (no allowance for `E1`) | the snapshot was taken after the flip of the record being replayed, before the position moved |
| `S > R1 ⇒ outside` (no page-boundary exception) | `R1` is on a page boundary, the commit record starts after the page header and is being replayed |
| id below `xmax` ⇒ `inside` | the transaction was still running; on a standby it is not printed in the in-progress list |
| id at or above `xmin` ⇒ `outside` | the transaction committed before the snapshot while an older one was still running |

The model also runs a deliberately wrong server that moves the position before the flip. There `inside`,
`R0 ≥ P0`, the library's answer and the narrowed answer of the tests are violated. So the check does notice when
the server's order is not the one the rule assumes. That order is the one fact the rule rests on, and section 3
shows where it is in the source.

The primary has its own model (class `Primary`): a transaction gets an id, writes its commit record (the slot can
deliver it from then on), and later leaves the set of running transactions (snapshots see it from then on).
Histories of up to 3 transactions (7, 145 and 6,265 histories, and 535 with two markers) confirm the rules of
section 10, including the library's gap tracker, and refute three wrong ones. The default test run explores the
histories of 1 and 2 transactions (687); the 6,265 histories of 3 transactions run with the marker `localpg`
and in `tools/cutstress model`.

## 5. Measurement on a standby

### 5.1 The stress test and the main campaign

The stress test (`tools/cutstress run`, code in `src/cdc_v2/testing/cutstress.py`) runs against the private
cluster of `tools/localpg`: one primary, one streaming standby, Postgres 18.6. It has four parts.

- **Counters.** One table with one row per writer: `(k int PRIMARY KEY, n bigint)`. Every transaction of a
  writer does `UPDATE … SET n = n + 1` on its own row, among whatever else its kind does. So transaction number
  `n` of key `k` is visible to a snapshot exactly when the snapshot reads `n` or more in row `k`. This is the
  evidence every rule is judged by. It does not depend on positions or ids.
- **Writers** (at most 4 sessions, on the primary). `plain`: the increment alone. `async`: the same with
  `synchronous_commit = off`. `abort`: rolls back every third transaction. `subxact`: 100 subtransactions (3,000
  in two runs) on a side table, then the increment; more than 64 overflows the subtransaction cache.
  `big`: 20,000 inserted rows. `drop`: creates and drops 20 tables, which makes the redo of the commit record
  long. `long`: holds the transaction open for 0.3 s. `twophase`: `PREPARE TRANSACTION`, then another session
  commits or rolls back the prepared transactions in a different order.
- **Stream reader.** One session reads the logical slot (`pgoutput`, protocol version 1) on the standby through
  `pg_logical_slot_get_binary_changes` and records for every transaction `S`, `E`, the transaction id, the key
  and `n`. It also checks that `E` never goes down.
- **Cut takers** (1 to 4 sessions, on the standby). Each takes cuts with the library, one after another, and
  reads all counter rows in the snapshot of the cut.

The **checker** judges a cut once the stream has passed it. It does three things.

1. It walks every transaction with `R0 < E ≤ E1` and compares each answer of the library (with the id, and with
   the id and the record start) with the evidence.
2. For each key it takes the last transaction the snapshot sees and the first it does not see. Per key, `E`, `S`
   and the id grow with `n`, so these two transactions decide every rule for all transactions of the key. The
   sound rules must hold. The wrong rules of section 5.4 are evaluated on the same two transactions and counted.
3. It requires a row for every key. A snapshot that returns no row has lost one. This step was added late, see
   the end of this section.

A **violation** is a case where a sound rule gives an answer and the evidence says otherwise. A run with one
violation is reported as failed. Examples are kept in the JSON file of the run.

**The main campaign** (`main.txt`, `main.json`):
`tools/cutstress run --label main --seconds 1800 --writers plain,async,subxact,abort --takers-own 3`.

| | |
|---|---|
| Cuts checked | 1,854,463 (none left unchecked) |
| Transactions delivered by the stream | 7,305,063 |
| Violations of the sound rules | 0 |
| Stream out of `E` order | 0 |
| Cuts with `R0 = R1` | 1,289,783 |
| Quiet snapshots | 1,006,841 (54.3%) |
| Load average (1 min) before / during / after | 5.25 / 6.35 / 6.24 |

The machine was shared with a throughput measurement and other workers during every run. The evidence index
gives the load averages of each run.

A first attempt of the main campaign was interrupted from outside after about 1,320 s. Its partial output is
kept as `main_interrupted.txt` and is not used. The campaign was run again in full.

**Which runs had the complete checker.** Step 3 of the checker was added after the main campaign and the first
runs of the variants had finished. Before it, a cut whose snapshot returned no row for a key was not judged for
that key by step 2; step 1 still judged it. A lost row is one of the ways the defect of section 7 shows. The
claims of this document therefore rest on runs with all three steps:

| Run | What it is | Cuts | Transactions | Violations | Missing rows |
|---|---|---|---|---|---|
| `main_recheck` | the main campaign again, 600 s | 628,741 | 2,451,105 | 0 | 0 |
| `v5b_restart_crash` | variant 5: 7 restarts and 7 kills of the standby | 89,718 | 722,901 | 0 | 0 |
| `v7b_exported_no_subxact` | variant 7: exported way, one-statement transactions | 120,737 | 485,205 | 0 | 0 |
| `v9_exported_open_no_subxact` | exported way, transactions held open, no subtransactions | 93,600 | 464,260 | 0 | 0 |
| `qb_quiet_exported` | exported way, quiet snapshots only, subtransactions in the load | 82,928 | 268,363 | 0 | 0 |
| `v1b_big_drop` | variant 1 | 95,014 | 155,847 | 0 | 0 |
| `v3b_ddl_walswitch_pause` | variant 3 | 81,540 | 477,939 | 0 | 0 |
| sum | | 1,192,278 | 5,025,620 | 0 | 0 |

The three runs that are expected to fail (`x_exported_subxact`, `xb_exported_subxact`, `xc_exported_subxact`,
section 5.5) had the complete checker too.

How to tell from the files. The result files written after the review (`v7b`, `v9`, `qb`, `v1b`, `v3b`, `xb`,
`xc`) name the steps of their checker (`checker_steps`). The files of `main_recheck`, `v5b_restart_crash` and
`x_exported_subxact` are older than that field. That they ran with step 3 follows from two facts:
`x_exported_subxact`, the first of the three, reports a missing row, which only step 3 counts; and step 3 and
the three outputs were committed together (`58c2c64`).

**Runs with steps 1 and 2 only:** `main`, `v2_twophase_subxact`, `v4_apply_delay`, `v5_restart_crash`,
`v6_slot_on_primary`, `v8_idle` and `nc_mid_redo`: 2,982,523 cuts over 9,353,889 transactions, no violation.
The main campaign was repeated in a shorter form and variant 5 in full; the other five were not repeated. In
these seven runs a lost row would have gone unseen unless step 1 caught it. All their cuts were taken the own
way. They are used below for what steps 1 and 2 decide: the answers for single transactions, the wrong rules,
and the measurements of sections 5.2 to 5.4.

**Outputs that are not used:** `v1_big_drop`, `v3_ddl_walswitch_pause`, `v7_exported_no_subxact` and
`q_quiet_exported` are the first runs of four variants that were repeated after the review. Their files are
kept. No number in this document comes from them.

### 5.2 The size of the unknown set

The unknown set of a cut is the set of stream transactions for which the cut answers `unknown`. Its size was
counted for every cut, for the plain rule in positions (`base`), with the id (addition 1 of section 2, **what
the pipeline has**), with the record start (addition 3, which only the test decoder has), and with both.

Main campaign, 1,854,463 cuts (`main.txt`):

| Rule | Who can use it | Cuts with no `unknown` | Mean size | Largest |
|---|---|---|---|---|
| positions only | the pipeline | 0.00% | 1.878 | 265 |
| + transaction id | the pipeline | **66.93%** | 0.709 | 263 |
| + record start | tests only | 68.99% | 0.891 | 264 |
| + both | tests only | 74.44% | 0.631 | 263 |

Distribution of the size:

| Size | positions only | with the id (the pipeline) | with the id and the record start (tests only) |
|---|---|---|---|
| 0 | 0 | 1,241,252 | 1,380,492 |
| 1 | 1,295,248 | 389,339 | 252,874 |
| 2 | 290,826 | 117,728 | 116,277 |
| 3–5 | 220,693 | 78,424 | 77,175 |
| 6–10 | 31,483 | 17,995 | 17,949 |
| 11–100 | 14,581 | 9,227 | 9,198 |
| 101 and more | 1,632 | 498 | 498 |

What the numbers say:

- With positions only the set is never empty. The transaction at `E1` is always `unknown`. In 1,289,783 cuts
  (69.6%) the two readings were equal, and the set is that one transaction.
- **The id empties the set in two of three cuts** (66.93%) and brings the mean from 1.878 to 0.709. This is the
  number the design can count on.
- The sharper outside rule answers the transaction at `E1` in most cuts: alone it empties the set in 68.99% of
  the cuts, together with the id in 74.44%. That is how much the start of the commit record would add: 7.5
  points. The pipeline cannot have it, because stock Debezium does not deliver `S` (section 2). The two rows are
  kept as a measurement of what is given up.
- The tail is long. The size is the number of commits replayed between the two readings, plus one. The campaign
  replayed about 4,060 stream transactions per second, so the largest set (265) corresponds to about 65 ms
  between the two readings of one cut.
- Of the transactions in the plain unknown set, the snapshot really sees 0.865 per cut on average. The rest
  (1.013) it does not see. So neither side dominates and no simple default answer exists.

On an idle source (variant 8 of section 5.6: one transaction every half second, 7,734 cuts): the plain set has
1 transaction in 7,716 cuts and 2 in 18. With the id it is empty in every cut, because every snapshot is quiet.

Across the variants of section 5.6 the share of cuts with no `unknown` at all, with the id, lies between 56.9%
(prepared transactions held open) and 100% (idle). The table in 5.6 has every value.

**What this means for keyless tables** (core design 7.4). That section reads the whole table under one snapshot
whose cut has no `unknown` transaction that touches the table, and takes another snapshot otherwise. The share
measured here counts cuts with no `unknown` transaction at all, so it is a lower bound for the share 7.4 can
use. With the id, about two of three cuts qualify under load, and every cut on an idle source. The expected
number of snapshots per table is below 2 in every load tested (1 / 0.569 = 1.8 in the worst variant). This
holds for the own way. The exported way is limited by section 7.

### 5.3 How often a snapshot is quiet

A quiet snapshot (`xmin = xmax`) is exact and is the only safe exported snapshot on a released server (section
7). Share of quiet snapshots among the cuts taken the own way:

| Run | Load | Quiet |
|---|---|---|
| `v8_idle` | idle | 100% |
| `v7b_exported_no_subxact` | plain, async, abort: one statement per transaction | 99.9% |
| `main` | plain, async, abort and a writer with 100 subtransactions per transaction | 54.3% |
| `v4_apply_delay` | as main without aborts, apply delay 300 ms | 54.0% |
| `v6_slot_on_primary` | as main, slot on the primary | 53.6% |
| `v3b_ddl_walswitch_pause` | as main, with DDL, WAL switches, replay pauses | 53.0% |
| `v5b_restart_crash` (`v5_restart_crash`) | as main, standby restarts and crashes | 51.6% (48.7%) |
| `qb_quiet_exported` | plain, abort and a writer with 100 subtransactions per transaction | 51.0% |
| `v9_exported_open_no_subxact` | plain, async, abort and one writer that holds each transaction 0.3 s | 17.8% |
| `v1b_big_drop` | transactions held 0.3 s, big transactions, drops | 9.4% |
| `nc_mid_redo` | drops, 3,000 subtransactions, 4 takers | 6.6% |
| `v2_twophase_subxact` | prepared transactions held open, 3,000 subtransactions | 4.4% |

A snapshot is quiet when no transaction is in flight on the standby at that moment. With one-statement
transactions nearly every snapshot is quiet. With one writer whose transactions span 100 subtransactions about
half are. A transaction that stays open keeps every snapshot from being quiet for as long as it stays open: the
share falls with the time transactions are held (`v9`, `v1b`, `v2`). A transaction open for an hour means no
quiet snapshot for an hour. This is why the rule for keyless tables has a bounded wait and a second way
(section 7.1).

Run `qb_quiet_exported` takes exported cuts and keeps only quiet ones, retrying at once otherwise. Two such
takers kept 36,413 snapshots and dropped 106,106 in 120 s (25.5% kept). The share per attempt is lower than the
share per cut of the own-way taker in the same run (51.0%). The likely reason is that a dropped snapshot is
retried at once, inside the same busy moment; this was not examined further. The rate is what matters: each
taker got a quiet snapshot about 150 times per second under 3 writers.

### 5.4 Negative control: wrong rules are caught

A test that never fails proves nothing. The checker therefore evaluates six wrong rules on every cut and
counts the cuts in which the evidence contradicts them. Each wrong rule is one tempting simplification.

| Wrong rule | Meaning | Caught, main | Caught, variants (range) |
|---|---|---|---|
| `stmt_as_r0` | `R0` read by the statement that takes the snapshot | 15,291 | 366 to 23,160; 0 when idle |
| `r1_inside` | `E ≤ R1 ⇒ inside` | 15,291 | 366 to 25,591; 0 when idle |
| `r1_outside` | `E > R1 ⇒ outside`, no allowance for `E1` | 31 | 0 to 34,497 |
| `naive_sharp` | `S > R1 ⇒ outside`, no page-boundary exception | 0 | 0 to 34 |
| `xmax_inside` | id below `xmax` ⇒ `inside` | 716,780 | 83 to 97,986; 0 when idle |
| `xmin_outside` | id at or above `xmin` ⇒ `outside` | 847,284 | 100 to 656,624; 0 when idle |

These are the wrong rules the model check refutes in section 4. All of them were caught live. The range is
over the runs of the table in section 5.6. `naive_sharp` is a wrong form of the narrowing by `S`, which only
the tests use (section 2); it stays in the list because the test narrowing is itself checked on every cut.

**The in-statement position** (claim 5 of the task). On the own way the statement that takes the snapshot also
reads the position, and the library calls that value `R1`. Used as `R0` it gives wrong `inside` answers: 15,291
cuts in the main campaign. One of them (`main.json`): `R0` = 0/5675F8E8, in-statement position = 0/56760990. The
transaction with `E` = 0/56760990 has `n` = 44 for its key and the snapshot reads 43. The snapshot was taken
first; the function ran after the commit was replayed. So the in-statement position is an upper bound only.

**Mid-redo** (`r1_outside`). The snapshot sees a transaction with `E > R1`. Example from the main campaign:
`R1` = `S` = 0/57803F38 and `E` = `E1` = 0/57803F68. The commit record is the first record after `R1`. Its flip
had happened and the position had not yet moved when the snapshot was taken. This is the transaction at `E1`,
and it is why the rule says `E > E1` and not `E > R1`. The case is rare with short redo (31 of 1.85 million
cuts) and common when the redo of a commit record is long: 7,638 cuts in `v1b_big_drop` and 34,497 in the run
made for it (`nc_mid_redo`: writers that drop 20 tables per transaction, 4 takers without a pause, 715,694
cuts).

**Page boundary** (`naive_sharp`). 2 cuts in `v1b_big_drop` and 34 in `nc_mid_redo`. Example
(`nc_mid_redo.json`): `R1` = 1/9DC3C000, a page boundary. `S` = 1/9DC3C018, which is `R1` + 24, right after the
page header. The snapshot sees the transaction (`n` = 803, snapshot reads 803). `S > R1` holds and the
transaction is inside. The page-boundary exception of the test narrowing (section 2, `cutnarrow.py`) covers
exactly this, and the narrowing was not wrong in any of these cuts.

**By id.** `xmax_inside` and `xmin_outside` fail in a large share of cuts under load, as expected: ids between
the bounds are undecided on a standby, because the in-progress list is not printed there.

### 5.5 The exported way in bulk

Six runs compare the two ways under the same load, all with the complete checker. Section 7 explains the
defect behind the result.

| Run | Writers | Exported cuts | Of them not quiet | With a violation | Own-way cuts in the same run | With a violation | Missing rows |
|---|---|---|---|---|---|---|---|
| `v7b_exported_no_subxact` | plain, async, abort | 72,533 | 58 | 0 | 48,204 | 0 | 0 |
| `v9_exported_open_no_subxact` | long, plain, async, abort | 55,598 | 45,600 | 0 | 38,002 | 0 | 0 |
| `qb_quiet_exported` (quiet snapshots only) | plain, subxact, abort | 36,413 | 0 | 0 | 46,515 | 0 | 0 |
| `x_exported_subxact` (every snapshot) | plain, subxact, abort | 53,951 | 25,393 | 771 | 37,110 | 0 | 1 |
| `xb_exported_subxact` (the same again) | plain, subxact, abort | 57,066 | 25,513 | 724 | 38,107 | **37** | 1 |
| `xc_exported_subxact` (the same again) | plain, subxact, abort | 61,599 | 26,797 | 684 | 40,251 | 0 | 2 |

- **A quiet exported snapshot is safe** (`qb`). 36,413 quiet exported cuts, no violation and no missing row,
  under the load that breaks the exported way in the three runs below it. The rule for keyless tables rests on
  this (section 7.1). `xc` agrees: none of its 684 exported cuts with a violation was quiet, and its 34,802
  quiet exported cuts were all right. (`x` and `xb` do not record this.)
- **Without subtransactions the exported way is right, also when the snapshot is not quiet** (`v9`). 45,600
  exported cuts that were not quiet, none wrong. `v7b` alone hardly shows this: its transactions are one
  statement long, and all but 58 of its exported snapshots were quiet. The result agrees with the cause (section
  7): the defect needs a subtransaction. **The design does not use this result.** The pipeline cannot know that
  a source never opens a subtransaction, so on a server with the defect the library refuses every exported
  snapshot that is not quiet.
- **With subtransactions and every exported snapshot used, the defect shows in bulk.** 771, 724 and 684
  exported cuts (1.4%, 1.3%, 1.1%) break a sound rule: the importing session sees transactions that the
  exporting snapshot does not see. These three runs are expected to fail. They are kept as evidence of the
  defect, not as a result of the rule.
- **Rows are lost.** In each of the three runs one or two exported cuts returned no row at all for a key that
  exists. That is the worst form of the defect, and it is what step 3 of the checker was added for.
- **Readers that import nothing are hit too, sometimes.** In `xb`, 37 of 38,107 cuts taken the own way broke a
  rule. In `x` and `xc`, none did (37,110 and 40,251 cuts). This is the damage section 7 describes: an importer
  with a broken snapshot marks a row version as aborted, and from then on every session of that standby reads
  the old row, until the key is written again. Which rule the own-way cuts of `xb` broke is an inference, not
  a record: the per-way record was added to the checker after `xb`, for `xc`, and `xc` had no such cut. The
  counts fit the damage: rule `inside` (a transaction with `E ≤ R0` that the snapshot does not see) is violated
  90 times in `xb` and in no other run of this document, and 37 `inside` plus 37 `library` violations make the
  74 of the own way. How many spoiled rows are behind them is not known. The single case of
  `export_defect.json` shows the same damage directly (section 7, last column of the table).
- In `qb` the takers import quiet snapshots only, so no importer has a broken snapshot. The 46,515 own-way
  cuts of that run were all right.

### 5.6 Variants

Each variant changes one thing. All ran on Postgres 18.6 with the stream out of `E` order 0 times, no cut left
unchecked and the slot dropped at the end. "No `unknown`" is the share of cuts with an empty unknown set with
the snapshot's bounds (additions 1 and 2 of section 2), which is what the pipeline has. "Checker" says whether
the run had all three steps (section 5.1).

| # | Label | What it adds | Seconds | Cuts | Transactions | Violations | No `unknown` | Checker |
|---|---|---|---|---|---|---|---|---|
| 1 | `v1b_big_drop` | big transactions, table drops, transactions held 0.3 s | 120 | 95,014 | 155,847 | 0 | 69.16% | complete |
| 2 | `v2_twophase_subxact` | two-phase commit finished out of order by another session; 3,000 subtransactions | 120 | 141,029 | 315,222 | 0 | 56.88% | steps 1, 2 |
| 3 | `v3b_ddl_walswitch_pause` | DDL on the counter table, WAL segment switches, replay pause and resume (13, 12, 12 times) | 120 | 81,540 | 477,939 | 0 | 64.22% | complete |
| 4 | `v4_apply_delay` | `recovery_min_apply_delay` = 300 ms | 120 | 100,763 | 363,741 | 0 | 76.02% | steps 1, 2 |
| 5 | `v5_restart_crash` | standby restarted 7 times and killed 7 times | 180 | 76,431 | 703,213 | 0 | 63.86% | steps 1, 2 |
| 5 | `v5b_restart_crash` | the same again | 180 | 89,718 | 722,901 | 0 | 67.95% | complete |
| 6 | `v6_slot_on_primary` | slot on the primary, snapshots on the standby | 120 | 86,409 | 477,973 | 0 | 65.37% | steps 1, 2 |
| 7 | `v7b_exported_no_subxact` | exported way, no subtransactions, one-statement transactions | 120 | 120,737 | 485,205 | 0 | 99.92% | complete |
| 8 | `v8_idle` | idle source | 120 | 7,734 | 236 | 0 | 100% | steps 1, 2 |
| 9 | `v9_exported_open_no_subxact` | exported way, no subtransactions, one writer holds each transaction 0.3 s | 120 | 93,600 | 464,260 | 0 | 67.81% | complete |
| | `nc_mid_redo` | long redo, 4 takers without a pause | 150 | 715,694 | 188,441 | 0 | 73.89% | steps 1, 2 |
| | `qb_quiet_exported` | exported way, quiet snapshots only | 120 | 82,928 | 268,363 | 0 | 76.82% | complete |

Sums, with the main campaign and its repeat (section 5.1):

- complete checker, seven runs: 1,192,278 cuts and 5,025,620 transactions, no violation of a sound rule, no
  missing row;
- steps 1 and 2 only, seven runs: 2,982,523 cuts and 9,353,889 transactions, no violation of a sound rule.

Variants 1, 3 and 7 and the quiet-only run were run twice. The table has the second run; the first is not
used (section 5.1). Variant 5 was run twice too, and both runs are in the table.
Variant 9 was added after the review, because variant 7 has almost no exported snapshot that is not quiet.

What each variant covers, from the list of the task:

- `synchronous_commit = off`: writer `async` in the main campaign and in variants 3 to 7 and 9.
- Big transactions: variant 1 (20,000 rows per transaction).
- More than 64 subtransactions: writer `subxact` in the main campaign (100) and in variant 2 (3,000).
- Two-phase commit, also finished by another session in another order: variant 2 (157,681 prepared
  transactions committed, 38,851 rolled back).
- Aborts: writer `abort` in the main campaign and in variants 3, 5, 6, 7 and 9.
- DDL, WAL segment switches, replay pause and resume: variant 3. The DDL adds and drops a column, changes the
  replica identity and rewrites the table with `VACUUM FULL`.
- `recovery_min_apply_delay`: variant 4.
- Standby restart between cuts: variant 5. The takers reconnected 255 times in the first run and 194 times
  in the second.

**Variants 5 and 6 and the rule `R0 ≥ P0`.** The checker also counts cuts whose `R0` lies below the last `E`
the stream had delivered when the cut was taken. Such a cut would fail `R0 ≥ P0` if `P0` were that `E`.

| Run | Cuts with `R0` below the stream | Of them, cuts that miss a delivered transaction |
|---|---|---|
| `v5_restart_crash` (slot on the standby, restarts) | 6 | 6 (20 keys) |
| `v6_slot_on_primary` | 4 | 4 (6 keys) |
| every other run | 0 | 0 |

The repeat of variant 5 (`v5b_restart_crash`) had no such cut. So the case is rare: 6 cuts in one of two runs
with 14 standby restarts each. This count does not depend on step 3 of the checker.

No sound rule is violated in these cuts: the rule `R0 ≥ P0` refuses them, and the answers for single
transactions stay right. But they show that the condition is not implied by the place of the slot. In variant 5
the slot is on the same standby as the snapshots. After a restart the standby replays again from its restart
point and takes queries before it is back where the slot had been. A backfill that skipped the check because
"the slot is on the same node" would have read a table state older than changes the stream had already applied,
and lost them. Section 3 gives the source. The check `R0 ≥ P0` must be made for every snapshot (section 13).

## 6. The stream: which field is `E`, and in what order transactions arrive

**Fields.** In pgoutput protocol version 1 a transaction is a BEGIN message, its changes, and a COMMIT message.

| Value | pgoutput field | Source |
|---|---|---|
| `S`, start of the commit record | BEGIN `final_lsn`; also COMMIT `commit_lsn` | `proto.c` `logicalrep_write_begin` (60 / 60 / 49) writes `txn->final_lsn` |
| `E`, end of the commit record | COMMIT `end_lsn` | `proto.c` `logicalrep_write_commit` (89 / 89 / 78) writes `txn->end_lsn` |
| transaction id | BEGIN `xid`, 32 bits, the top-level id | same function |

`decode.c` `DecodeCommit` (658 / 672 / 667) passes the start and the end of the commit record to
`reorderbuffer.c` `ReorderBufferCommit` (2747 / 2780 / 2887), which stores them as `final_lsn` and `end_lsn`.
When the slot is read with `pg_logical_slot_get_binary_changes`, the `lsn` column of the COMMIT row is `E` too.
Ids of subtransactions do not appear in the stream.

**What the pipeline has.** The pipeline does not read pgoutput itself; stock Debezium does. Of the three values
Debezium passes on two (`docs/spike/local_streaming.md` section 4; `docs/design/engine.md` section 5): `E` is
the `lsn_commit` of the END record, and the 32-bit id is the `txId` of a change record and the first part of
the `id` of BEGIN and END. It does not pass on `S`: no Debezium field equals it (0 of 1,222 transactions; the
position of its BEGIN record lies below `S`; `docs/design/core.md` section 2). So the
library takes `E` and the id and nothing else. The tests read pgoutput with their own decoder, which is how
they have `S` and can measure the narrowing by it (section 2, addition 3).

**The id has 32 bits and wraps; the bounds of a snapshot have 64.** `widen_xid(xid, near)` gives the 64-bit id
that lies closest to a 64-bit reference. It is exact when the transaction and the reference are less than 2^31
ids apart. On a standby the reference is the `xmax` of the cut's own snapshot (`StandbyCut.classify`). On a
primary it is the `xmax` of the latest probe (`GapTracker.widen`, section 10.4). It is never an id kept from an
earlier run or read from the destination.

**Order.** Decoding reads the WAL in order. A transaction is delivered when its commit record is decoded, as a
whole, before the next record is read. So transactions arrive in the order of their commit records, and `E`
increases strictly. Two things could change that and are not used: the `streaming` option (protocol 2 and
later) delivers parts of open transactions, and a slot with `two_phase` on delivers a PREPARE on its own. With
`two_phase` off a prepared transaction is delivered at its `COMMIT PREPARED`; its `S` and `E` are the start and
end of that record.

**Measured** (`evidence/p3/cut/positions.json`, command `tools/cutstress positions`, Postgres 18.6; the earlier
run without prepared transactions is `positions_no2pc.json`). Ten known transactions were committed one at a
time and the insert position was read before and after each commit:

- a small one; a small one inside the lifetime of a big one; the big one (20,000 rows), started first and
  committed after; a write inside a subtransaction; a commit with `synchronous_commit = off`;
- two prepared transactions, committed in the opposite order by another session, with small transactions
  before and between them.

In all ten: `E` equals `pg_current_wal_insert_lsn()` read right after the commit returned; `S` is at or after
the insert position read before the commit; the stream order equals the commit order (the transaction prepared
second and committed first arrives first); a slot on the standby delivers the same `(xid, S, E)` as a slot on
the primary. The commit record is 48 bytes, 56 with one subtransaction, 72 for `COMMIT PREPARED`.

In the campaigns the checker counts every transaction whose `E` is not greater than the one before it: 0 in
7,305,063 transactions of the main campaign and 0 in every variant of section 5.6, which include big
transactions next to small ones, commits with 3,000 subtransactions, and prepared transactions committed by
another session.

## 7. The exported snapshot on a standby is unsafe on every released version

This is a defect in Postgres, not in the rule. It contradicts core design 7.1 and 7.4, which read a table
through one exporting connection and several importing connections on a standby.

**What happens.** A session that imports a snapshot exported on a standby can see a transaction that was still
running when the snapshot was exported. It sees the transaction as soon as it commits. Before the commit it
judges the transaction's rows as aborted and writes that judgement into the rows (wrong hint bits). From then
on every session of that standby reads those rows wrongly, also sessions that never imported anything.

**Why.** A standby keeps all running ids of a snapshot in the list `subxip`; the list `xip` is empty
(`procarray.c` `GetSnapshotData`, section 3). `ExportSnapshot` (`snapmgr.c`, line 1125 / 1095 / 1113) writes the
flag `sof:1` and leaves out `subxip` when the snapshot is marked overflowed. On a primary that is correct:
an overflowed snapshot is completed from another structure. On a standby the list it leaves out is the only
list. The importing session gets a snapshot with no running ids. `XidInMVCCSnapshot` then answers "not running"
for every id between `xmin` and `xmax`, and the commit log decides: committed if the transaction has committed
by now, otherwise treated as aborted.

**When a standby snapshot is marked overflowed.** When its `xmin` is at or below `lastOverflowedXid`
(`procarray.c` `GetSnapshotData`, REL_18 line 2400). The standby sets `lastOverflowedXid` when it replays an
assignment record (`ProcArrayApplyXidAssignment`, REL_18 lines 1367–1370). The primary writes an assignment
record in two cases (`xact.c` `AssignTransactionId`, declared at 630 / 637 / 640, record written at 749 / 758 /
761):

1. after 64 subtransaction ids of one transaction;
2. under `wal_level = logical`, when a subtransaction gets its id before the top-level transaction has written
   anything. That is every transaction whose first write happens after a savepoint or inside a PL/pgSQL block
   with an exception handler. Many client libraries wrap each statement in a savepoint.

This pipeline needs `wal_level = logical`. So one running transaction of case 2 on the primary is enough.

**Measured** (`evidence/p3/cut/export_defect.json`, server 18.6, command `tools/cutstress export-defect`). One
transaction on the primary increments a counter from 0 to 1 and stays open. A snapshot is exported on the
standby while it runs. The right answers: an importer reads 0 before and after the commit; an ordinary session
after the commit reads 1.

| Shape of the open transaction | Importer that first reads after the commit | Ordinary session after the commit, when an importer read before the commit |
|---|---|---|
| no subtransaction | 0 (right) | 1 (right) |
| top level writes first, then one subtransaction | 0 (right) | 1 (right) |
| first write inside a subtransaction | **1 (wrong)** | **0 (wrong)** |
| 65 subtransactions after a top-level write | **1 (wrong)** | **0 (wrong)** |

Three stress runs with exporting takers and subtransaction writers show the same thing in bulk, both columns
of it (section 5.5): 684 to 771 wrong exported cuts per run, lost rows in every run, and in one of the three
runs 37 wrong cuts of a reader that imported nothing.

**Versions.** Present in 16.15, 17.11 and 18.6 and everything before them. Fixed on the stable branches by
commit `74c9832ca610`, "Export subxip[] for snapshots taken during recovery." (REL_18_STABLE, 2026-08-26,
back-patched through 14). No release carries it yet. The library's table (`export_defect`) assumes that 16.16,
17.12 and 18.7 will; that has to be checked when they appear. I could not test a fixed server: none is released
and I may not build one.

**A quiet snapshot is safe.** If `xmin = xmax` there is no id between the bounds. The list that is lost is empty.
This holds on every version.

**What the library does.**

- `export_defect(server_version_num)` is the one place that knows which versions have the defect. Nothing else
  in the library, the tools or the tests names a version.
- `take_standby_cut_exported` refuses on a server with the defect. With `quiet_only=True` it keeps a snapshot
  only if it is quiet and raises `SnapshotNotQuiet` otherwise. `accept_export_defect=True` exists for tests
  only.
- `take_standby_cut_after(conn, p0, deadline, settle_floor, exported=True, quiet_only=True)` takes snapshots
  until one is quiet or the `Deadline` has passed. It ends with a cut, or with the result `NoQuietSnapshot`
  (the number of attempts and the oldest id still running at the last one). That result is not an error and is
  not raised: it is false in a condition, so the caller can tell it from a cut and from a failure. A deadline
  that passes while replay has not reached `P0`, or while the standby has not settled, is still an error
  (`DeadlineExceeded`).
- `take_keyless_standby_cut(conn, p0, deadline, settle_floor, quiet_wait_s=60)` is the rule below in one call.

### 7.1 Tables without a key: the rule (decided)

Core 7.1 reads a keyless table "under one snapshot" through an exporter and importers, and 7.4 depends on it.
On a released server that is unsafe. The coordinator has decided the rule; it goes to the owner as a decision
made, not as a question.

On a standby, for a table without a key:

1. **The server has the defect** (`export_defect(version)` is true): wait for a **quiet exported snapshot**,
   within a bounded, declared wait (core 4.3; default 60 s, `QUIET_WAIT_DEFAULT_S`). A quiet cut also has no
   `unknown` transaction, which is exactly what 7.4 wants.
2. **If none comes within the wait: one reader with its own snapshot.** No snapshot is exported or imported.
3. **The server carries the fix:** a plain exported snapshot, quiet or not.

Why not "quiet only". A snapshot is quiet only when no write transaction is open on the primary at that moment
(section 5.3). One transaction that stays open, a session idle in a transaction or a forgotten prepared
transaction, keeps every snapshot from being quiet for as long as it lasts. A backfill that waits for a quiet
snapshot without a bound would never complete, for a reason the source is free to have.

Why not "one reader" always. One reader holds its snapshot, and its lock on the table, for the whole read of the
table. On a standby a replayed `ACCESS EXCLUSIVE` lock on that table (DDL, `VACUUM FULL`, a truncating vacuum)
conflicts with it. Replay then waits for up to `max_standby_streaming_delay` and after that cancels the
reader. While replay waits, the standby replays nothing, so the slot on it delivers nothing either: the whole
stream stalls, not only this table. Importers read in chunks and hold nothing between them. So the one reader
is the fallback, not the default.

The **declared wait** is part of the rule: the backfill reports that it waits for a quiet snapshot, and for
which transaction (the oldest running id of `NoQuietSnapshot`), and after the wait it reports that it reads the
table with one reader.

Measured (`evidence/p3/cut/keyless.json`, command `tools/cutstress keyless --quiet-wait 1`, Postgres 18.6, which
has the defect). A transaction that wrote inside a subtransaction is held open on the primary:

| Step | Result |
|---|---|
| quiet-only wait of 1 s while the transaction is open | `NoQuietSnapshot` after 1.01 s and 18 attempts; false in a condition; its oldest running id is the open transaction |
| `take_keyless_standby_cut` while it is open | a cut of the own way, `R0 ≥ P0`, snapshot `44654627:44654629:`; the one reader does not see the open transaction and sees the committed one; the `NoQuietSnapshot` is kept in the cut for the log |
| `take_keyless_standby_cut` after the commit | a cut of the exported way, quiet (`44654629:44654629:`); a reader that imports it sees both transactions |

Tests: `tests/cut/test_cut_logic.py` (hermetic: the result object, the deadline, the order of the three
cases), `tests/cut/test_cut_live.py` `test_keyless_table_gets_a_quiet_exported_snapshot_or_one_reader`. The
stress run `qb_quiet_exported` (section 5.5) is the bulk evidence that a quiet exported snapshot is safe.

### 7.2 A runbook rule

**No other program imports snapshots on the standby we read.** Any other program that imports a snapshot on
the same standby (parallel `pg_dump` does) can leave wrong hint bits in the tables we read. A reader with its
own snapshot then reads those rows wrongly too, and so does a reader that imports a quiet snapshot. The
pipeline cannot detect this. It is a requirement on the standby for as long as its server has the defect.

Evidence. The table of measurements above shows the effect in a single case (last column). In bulk it showed
in one of three runs: in `xb_exported_subxact`, 37 of 38,107 cuts of a reader with its own snapshot were wrong
while two other sessions imported defective snapshots on the same standby; in `x_exported_subxact` and
`xc_exported_subxact`, none of 77,361 (section 5.5). When the other sessions imported quiet snapshots only
(`qb_quiet_exported`), none of 46,515 was wrong. So the damage is intermittent, and a short test that does not
show it proves nothing.

The pipeline itself keeps the rule: on a server with the defect its own importers import quiet snapshots only.

## 8. A standby that has just started: the `inside` rule can fail

This section reports a violation of `E ≤ R0 ⇒ inside`. It contradicts core design 7.2 as written, which states
the rule without a condition. The condition that repairs it is cheap, and the library enforces it.

### 8.1 What happens

A transaction `T` has written and flushed its commit record on the primary and is not yet finished there: it
waits for a synchronous standby (section 10.1 measures that state). While it waits, the primary takes a
checkpoint whose redo pointer is at or after the end of `T`'s commit record. The standby the engine reads
replays that checkpoint, makes it its restart point, and is restarted (a crash does the same).

Before the restart the standby sees `T`. After the restart it does not, although its replay position is beyond
`T`'s commit record. It hides `T` until it replays a commit or abort record of a transaction with a higher id.
If nothing else commits, it hides `T` even after `T` has returned on the primary and is visible there.

### 8.2 Why (source)

- An online checkpoint logs a list of running transactions after its redo pointer is fixed (`xlog.c`
  `CreateCheckPoint`: `CheckPointGuts` at REL_18 line 7213, `LogStandbySnapshot()` at 7239). The list comes
  from the proc array. `T` is still in it, and the record's `latestCompletedXid` (`standby.c`, REL_18 line 1363)
  is below `T`'s id when no transaction with a higher id has ended.
- A standby that starts replays from the redo pointer. `T`'s commit record lies before it and is not replayed
  again. When the standby reaches the list, `procarray.c` `ProcArrayApplyRecoveryInfo` (1056 / 1054 / 1054)
  drops `T` from the running ids because the standby's commit log already says `committed` (line 1172 / 1181 /
  1181), and sets `latestCompletedXid` to the value in the record (REL_18 line 1293).
- A standby snapshot has `xmax = latestCompletedXid + 1` (`GetSnapshotData`). `T`'s id is at or above `xmax`,
  and a snapshot treats such an id as running.
- Replay of a commit or abort record raises `latestCompletedXid` to that record's highest id
  (`ExpireTreeKnownAssignedTransactionIds`). A later list of running transactions does not: once the standby
  accepts queries, `ProcArrayApplyRecoveryInfo` returns before that step (line 1080 / 1089 / 1089).

The three branches have the same code in these places. A transaction whose commit record lies after the redo
pointer is replayed again after the start and is not affected. A new standby made from a base backup whose
start checkpoint was taken during such a wait starts the same way; that case is argued, not measured.

I did not look for an upstream report. A standby query that misses a commit the primary has acknowledged, with
replay beyond it, looks like a Postgres defect worth reporting.

### 8.3 Measured

`evidence/p3/cut/standby_start.json` (command `tools/cutstress standby-start`, Postgres 18.6). The primary's
`synchronous_standby_names` names a standby that does not exist, so one commit waits. The scenario was run
several times with the same outcome, and the live test repeats it. The file holds the last run, made with the
library as it is now:

| Moment | Snapshot on the standby | Sees `T` (id 44654445) | `E ≤ R0` | Settle floor of the connection | Library |
|---|---|---|---|---|---|
| before the restart | `44654446:44654446:` | yes | yes | 44654429 | accepts |
| after the restart, `T` still waiting | `44654445:44654445:` | **no** | yes | 44654446 | refuses |
| after `T` returned on the primary | `44654445:44654445:` | **no** | yes | 44654446 | refuses |
| after one later commit | `44654447:44654447:` | yes | yes | 44654446 | accepts |

The redo pointer of the checkpoint (`2/21048E50`) equals the end of `T`'s commit record. The restart ends the
connection, so the rows after it use a new connection and the floor read on that one.

### 8.4 The condition: a settled standby

A cut is used only if the snapshot's `xmax` is at or above the **settle floor**: the next transaction id of the
checkpoint named in the standby's control file, `next_xid` of `pg_control_checkpoint()` (text `epoch:xid`).

Why this is enough. Every hidden transaction has its commit record before the redo pointer of the checkpoint
the standby started from, so its id is below that checkpoint's next id. The control file names that checkpoint
or a later one, so the floor is at or above that next id. A hidden transaction is not in the standby's list of
running ids. So a snapshot with `xmax` at or above the floor sees it.

How it is used. Read the floor once per connection, before the first cut. A restart ends the connection; the
floor of a running standby only grows; `xmax` never moves back. So after one settled cut every later cut on
the connection is settled. A standby that is not settled becomes settled with the next commit or abort it
replays. The heartbeat write is such a commit, so the wait is bounded by the heartbeat interval plus replay
lag; it has the deadline of the `R0 ≥ P0` wait and reports the two ids. On a standby that started normally the
first snapshot is settled unless a transaction with one of the newest ids was open at the checkpoint.

The runtime role of the test cluster may call `pg_control_checkpoint()` on the standby (checked by calling
it). Whether a managed service restricts the function is not checked; the pre-flight check should call it.

Library. The floor is not optional, and it is bound to its connection.

- `settle_floor(conn)` reads the floor and returns a `SettleFloor`: the value together with the connection it
  was read on. On a node that is not in recovery it refuses.
- Every function that takes a standby cut has the floor as a required argument: `take_standby_cut_own`,
  `take_standby_cut_exported`, `take_standby_cut_after`, `take_keyless_standby_cut`. None has a default and
  none accepts `None` or a bare number.
- The taker asks the object for the value for its own connection (`SettleFloor.for_connection(conn)`). For any
  other connection that raises `CutError`. So a floor read on one standby is never used on another, and a floor
  read before a restart is never used after it, because the restart ended that connection.
- A snapshot below the floor is dropped and `StandbyNotSettled` is raised. `take_standby_cut_after` waits and
  takes another, within its `Deadline`.
- Tests that need to look at an unsettled cut pass a separately named switch, `accept_unsettled=True`, as
  `accept_export_defect` does for the other defect. Production code has no reason to name either.

Tests: `tests/cut/test_cut_logic.py` (hermetic; among them
`test_no_function_takes_a_standby_cut_without_a_settle_floor` and
`test_a_settle_floor_read_on_one_standby_is_refused_on_another`), `tests/cut/test_cut_live.py`
`test_standby_that_has_just_started_is_refused_until_it_settles`.

The campaign with standby restarts and crashes (section 5.6, variant 5) has no violation without the floor,
because no commit waits across a checkpoint there. The failure needs the wait.

## 9. An open cut across a standby restart and across a promotion

An **open cut** is a cut whose snapshot is still held: the reader has not finished, or the stream has not yet
passed `R1`.

### 9.1 Standby restart

- **The snapshot is lost.** A restart ends every session, and a snapshot lives in its session. An exported
  snapshot is lost too: its file is deleted at startup (`xlog.c` `StartupXLOG` calls
  `DeleteAllExportedSnapshotFiles`, line 5374 / 5724 / 5808). What was being read under the cut has to be read
  again under a new cut: one chunk of a keyed table, the whole table for a table without a key (core 7.4).
- **A cut whose read is complete stays valid.** Its positions and bounds are numbers the engine holds. They
  describe a snapshot that existed and WAL that does not change. The rows read under it can be used, and
  "has passed" is judged as before when the stream goes on. Variant 5 and its repeat (section 5.6) check
  this: cuts taken before a restart are judged after it, when the stream has passed them. 166,149 cuts, 28
  restarts or kills of the standby, no violation.
- **A new cut needs three things again.** A new connection. The settle floor, read on that connection, because
  a standby that has just started can hide a commit (section 8). The check `R0 ≥ P0`, because the standby
  replays again from its restart point and its replay position can be lower than before the restart (section
  5.6: 6 cuts in variant 5).
- **The stream may repeat itself.** If the slot is on the restarted standby, it goes on from its last confirmed
  position that reached disk. Transactions after it are delivered again with the same `E`. The stress test
  recognises them by `E`, as the engine does (core design, section 2).

### 9.2 Promotion

Scenario `tools/cutstress promotion`, evidence `promotion.json` (Postgres 18.6): a second standby is added, a
cut is taken on it and held open, the standby is promoted, and a row is written on it.

| Observation | Value |
|---|---|
| The open cut, `R0` = `R1` | 2/21000578; taken on timeline 1; its snapshot reads 1 |
| The open cut's `valid_on` for the timeline of the source (1), and for no timeline | true; false |
| `pg_is_in_recovery()` after promotion | false |
| Timeline of the promoted node (from its WAL insert position) | 2 |
| Timeline in its control file right after the promotion | still 1 |
| The open cut's `valid_on(2)` | false |
| `pg_last_wal_replay_lsn()` after promotion, before and after a local commit | 2/21000578 both times |
| The local commit record | ends after that position |
| A new snapshot after the local commit | reads 2 |
| The open cut's snapshot after the local commit | still reads 1 |
| The library on a new cut, own way and exported way | refuses: "not a standby: pg_is_in_recovery() is false; use take_primary_cut" |
| The library on a new settle floor | refuses: "not a standby: pg_is_in_recovery() is false" |
| A primary cut on the promoted node | timeline 2; `valid_on(2)` true; `valid_on(1)` false |

**The rule: a cut belongs to the timeline it was taken on.** After a promotion or a slot finding (core design,
section 8), open cuts are abandoned and their backfills start again.

- `StandbyCut.valid_on(timeline)` and `PrimaryCut.valid_on(timeline)` are true only for the timeline the cut
  recorded. The caller passes the timeline the stream is on, read exactly and **after the cut was taken**. The
  slot check has it: `cdc_v2.pg.slot.timeline_history` reads it with `IDENTIFY_SYSTEM` over a replication
  connection, and `_cdc.stream_state` stores it next to the applied position (core design, section 8). A cut
  without a timeline, and a call without one, give false.
- Abandoned means: the answers the cut gave, and everything read under its snapshot, are dropped; the table is
  requested again with a new `P0`, as section 8 of the core design does for every table after a finding.
- Why not keep the cut. A position means something only together with its timeline (section 1). WAL that the
  old primary wrote after the point of promotion is not part of the new history; an `E` of the old timeline
  and an `R0` of the new one can be the same number and name different records. And after a finding the applied
  position is set anew, so `R0 ≥ P0` no longer says what it said when the cut was taken.

What a standby can tell about its timeline. No SQL function gives the timeline that replay is on. The exact
value needs `IDENTIFY_SYSTEM` over a replication connection; the core design (section 8) decides by that and
not by the control file, for this reason. The cut records the timeline of the checkpoint named in the control
file (`pg_control_checkpoint()`), read by the statement that reads `R0`. That is the replay timeline or, between
a timeline switch and the standby's next restart point, the one before it: the table shows the lag (the control
file still says 1 on a node that is on timeline 2). The recorded value never runs ahead, and the timeline of a
node never goes back. So

    recorded timeline  ≤  timeline the cut was taken on  ≤  exact timeline of the same node, read later

and if the first and the last are equal, the cut was taken on that timeline. This is why the value passed to
`valid_on` must have been read after the cut:

- Read after the cut, the lag has one effect only. A cut taken on a standby that has followed a timeline switch
  and has not yet made a restart point records the old timeline and is refused, although it is good. The
  backfill tries again and succeeds after the next restart point.
- Read before the cut, an equal value proves nothing. The standby may have followed a switch in between, and
  the control file would not show it yet.
- When backfill reads use another replica than the slot (core design 7.2), the two nodes can be on different
  histories, in which equal positions name different records. The caller then reads the timeline of the
  reading replica in the same way, after the cut, and uses the cut only if `valid_on` is true for that value
  and the stream's timeline is the same number.

The backfill can make the comparison once per table, before the swap, with the timeline of a slot check that
ran after the table's last cut was taken: the check runs at every slot acquisition and every
`slot.verify_interval`, and nothing of a backfill is visible before its swap. On a primary the recorded value is
exact (`timeline(conn)`; the first eight digits of the WAL file name of the insert position).

What else the measurement shows:

- **The session and its snapshot survive a promotion.** The open cut keeps reading the same rows. Its answers
  stay right for transactions of the WAL the node has replayed. Local transactions after the promotion commit
  after the snapshot and have `E` beyond the frozen replay position. The rule abandons the cut all the same:
  it is simple, and a promotion is rare.
- **The standby rule must not be used on a promoted node.** The replay position no longer moves, while commits
  go on becoming visible. A cut made from it would call every later commit `outside` once one of them is `E1`,
  and later snapshots see them all. The library therefore reads `pg_is_in_recovery()` in the statement that
  reads `R1`, after `R1`, and refuses when it is false. A node takes local writes only once recovery has ended
  (`xlog.c` `XLogInsertAllowed`, 6003 / 6361 / 6423). So a cut that passes this check was taken entirely on a
  standby.
- **After a promotion the node needs the rules of a primary** (section 10), including the rule that replaces
  `R0 ≥ P0`. The engine's stream lane starts blind there, as after an engine start.

Tests: `tests/cut/test_cut_logic.py` `test_a_cut_is_valid_only_on_the_timeline_it_was_taken_on` and
`test_timeline_of_a_primary_is_exact_and_of_a_standby_is_the_control_file` (hermetic);
`tests/cut/test_cut_live.py` `test_a_cut_is_valid_only_on_the_timeline_it_was_taken_on` (the promotion
scenario). Not run: a standby that follows a new primary onto a new timeline (a cascade), and reads from
another replica than the slot. For those cases the lag of the control file is measured on the promoted node
only, and the rule about which value to pass is argued, not tested live.

## 10. The cut on a primary

This is the non-default mode: the slot and the snapshots are both on the primary. It is open point 12 of the
core design. The result: the answer for one transaction is exact and needs no positions. What needs a new rule
is the condition `R0 ≥ P0`, because on a primary the stream can deliver a transaction before any snapshot sees
it.

### 10.1 The gap

A commit on a primary has two moments.

1. The commit record is written to the WAL and flushed. From here on the slot can decode the transaction:
   decoding reads up to the flush position (`logicalfuncs.c` `pg_logical_slot_get_changes_guts`, REL_18 line
   196; `walsender.c` `WalSndWaitForWal`, REL_18 line 1910).
2. The transaction leaves the set of running transactions. From here on new snapshots see it.

Between the two the backend may wait for a synchronous standby. Source, `xact.c` `RecordTransactionCommit`
(1283 / 1305 / 1316), in REL_18: commit record at line 1446, flush at 1506, commit log at 1518,
`SyncRepWaitForLSN` at 1567. `CommitTransaction` (2162 / 2195 / 2244) calls `ProcArrayEndTransaction` only
after that (REL_18 line 2405). A prepared transaction does the same at `COMMIT PREPARED`: `twophase.c`
`RecordTransactionCommitPrepared` (2317 / 2326 / 2325) writes the record, flushes, sets the commit log and
waits (REL_18 lines 2359, 2395, 2398, 2411); `FinishPreparedTransaction` removes the id from the running set
afterwards (`ProcArrayRemove`, REL_18 line 1577).

The **gap** is the time between moment 1 and moment 2. Without a synchronous standby it is a few instructions
long. With one it is as long as the standby takes to confirm, and unbounded when the standby is down.

**Measured** (`evidence/p3/cut/primary_gap.json`, command `tools/cutstress primary-gap`, Postgres 18.6). One
transaction commits while the synchronous standby is held back. Six cases: a plain transaction, one that wrote
in a subtransaction, and a prepared transaction at its `COMMIT PREPARED`; each once with the standby stopped
(`synchronous_commit = on`) and once with the standby's replay paused (`synchronous_commit = remote_apply`).
In all six:

- the slot delivered the transaction 0.062 to 0.063 s after the commit was issued, while the commit had not
  returned;
- for the 3 s the standby was held back, a new snapshot listed the transaction in its in-progress list (for
  example `27968900:27968903:27968900`), the row was not visible, and `pg_xact_status` said `in progress`;
- after the standby was released the commit returned, the snapshot was quiet, the row was visible, and
  `pg_xact_status` said `committed`.

With `synchronous_commit = off` and nothing held back, the first snapshot after delivery saw the transaction.

A healthy synchronous standby on the same machine did not open the gap wide enough to be hit: 0 of 37,499
snapshots missed a delivered transaction (`primary_sync_on.json`). With the standby's replay paused every two
seconds, 2,317 of 54,489 did (`primary_remote_apply.json`). Without a synchronous standby, 0 of 51,540
(`primary_async.json`). The gap is the standby round trip.

### 10.2 `synchronized_standby_slots` does not close the gap

Postgres 17 and 18 can make a logical slot wait for physical standbys: a slot created with `failover` does not
deliver WAL that the standbys named in `synchronized_standby_slots` have not confirmed (`slot.c`
`StandbySlotsHaveCaughtup`, 17 line 2630, 18 line 2911). The test compares the physical slot's `restart_lsn`
with the position (17 line 2725, 18 line 3006). That is what the standby has received and flushed, not what it
has applied, and it says nothing about the committing backend having finished.

Measured (same file, key `failover_slot`):

| Standby | `synchronous_commit` | Failover slot delivered the transaction | Commit had returned |
|---|---|---|---|
| stopped | `on` | no, the read blocked until its timeout | no |
| replay paused | `remote_apply` | **yes** | **no** |

In the second case the snapshot taken at delivery listed the transaction as in progress. So the setting
narrows the gap in one configuration and leaves it open in another. It is also absent in Postgres 16 and is a
setting of the owner's server. The engine does not rely on it.

### 10.3 The answer for one transaction is exact

On a primary, `pg_current_snapshot()` prints `xmin`, `xmax` and the in-progress list. For a transaction of the
stream with 64-bit top-level id `x`:

| Condition | Answer |
|---|---|
| `x < xmax` and `x` is not in the in-progress list | `inside` |
| otherwise | `outside` |

There is no `unknown`, and no position is needed (`PrimaryCut.classify`).

Why. `GetSnapshotData` copies, under `ProcArrayLock`, `xmax = latestCompletedXid + 1` and the ids of all
running top-level transactions below `xmax`. `ProcArrayEndTransaction` removes a transaction's id and all its
subtransaction ids and moves `latestCompletedXid` under the same lock, exclusively. So a snapshot is taken
either before or after the whole transaction left the running set, and the printed fields say which.

- **Subtransactions.** The printed in-progress list holds top-level ids only, whether or not a transaction has
  more than 64 subtransactions. The stream carries the top-level id. Nothing else is needed.
- **Prepared transactions.** The id stays in the running set, and in every snapshot's in-progress list, from
  `PREPARE` until `COMMIT PREPARED` has finished. That can be hours. The answer is still exact.
- **`synchronous_commit = off`.** The transaction leaves the running set without waiting for the flush. The
  answer is the same.
- **xid epoch.** The stream's 32-bit id is widened to the 64-bit id nearest to a 64-bit reference
  (`widen_xid`). For one answer the reference is the `xmax` of the snapshot that is asked
  (`PrimaryCut.classify`). For the tracker of section 10.4 it is the `xmax` of the latest probe. Both are
  correct while the transaction and the reference are less than 2^31 ids apart. A wrap of the 32-bit id cannot
  be produced on a test cluster in reasonable time; the widening is covered by unit tests
  (`tests/cut/test_cut_logic.py`) only.

**Measured.** Four campaigns (`tools/cutstress primary-stress`) compare the answer with what the snapshot
reads, for up to 64 transactions on each side of the boundary of every counter row in every cut:

| Campaign | Cuts | Transactions | Answers checked | Wrong answers |
|---|---|---|---|---|
| `primary_async`: no synchronous standby; writers plain, async, subtransactions (100 each), long | 51,540 | 184,261 | 25,092,453 | 0 |
| `primary_remote_apply`: `remote_apply`, standby replay paused every 2 s; same writers | 54,489 | 167,712 | 26,441,568 | 0 |
| `primary_sync_on`: healthy synchronous standby; same writers | 37,499 | 117,841 | 17,780,355 | 0 |
| `primary_twophase`: `remote_apply`, replay paused every 2 s; writers plain, subtransactions, prepared transactions committed or rolled back by another session | 53,684 | 131,143 | 40,979,142 | 0 |

Together 197,212 cuts and 110,293,518 answers. Each campaign has its `.json` and, for the last one, `.txt` file
in `evidence/p3/cut/`.

### 10.4 What replaces `R0 ≥ P0`

**The requirement.** A table switches to `backfilling` at stream position `P0`. Changes the stream delivered
at or before `P0` are not kept for the table. So the snapshot must see every transaction the stream delivered
at or before `P0`. On a standby `R0 ≥ P0` guarantees that. On a primary "the snapshot is taken after `P0` was
applied" does not: 2,317 violations in `primary_remote_apply` (wrong rule `later_snapshot`).

**A fact used by all three rules.** Once a snapshot sees a committed transaction, every later snapshot sees it:
an id that has left the running set does not come back.

**Rule A: `xmin ≥ X`.** After the switch is committed, the engine reads `X = pg_current_xact_id()`. Every
transaction delivered before has a smaller id. A snapshot with `xmin ≥ X` was taken when all ids below `X` had
finished, so it sees all of them that committed. Sound, and it needs no state. It waits for **every**
transaction that was open when `X` was read, whether it touches a published table or not: a long transaction,
a session idle in a transaction, a prepared transaction that nobody commits. The wait can be indefinite.

**Rule B: `xmin > M`**, where `M` is the largest id the stream has delivered. Same argument, no extra query. It
waits for every open transaction older than the newest delivered one.

**Rule C: the gap tracker (exact).** The engine keeps the set `G` of ids of delivered transactions that no
snapshot of its own has seen yet (`GapTracker` in the library).

- For each delivered transaction: add its 64-bit id to `G` (`GapTracker.delivered`).
- Once per applied group: take a snapshot and remove from `G` every id it sees (a **probe**). It is one
  statement, `SELECT pg_current_snapshot()`, in autocommit on a primary connection of the stream lane
  (`probe_snapshot(conn)`). It opens no transaction and holds nothing.
- At the switch of a table: copy `G` (a **mark**). It is almost always empty.
- A snapshot taken after the mark is usable for the table exactly if it sees every id of the mark
  (`GapMark.admits`).

**The ids the tracker gets.** The stream has the 32-bit id of a transaction's `BEGIN`; `G` holds 64-bit ids.
The stream widens the id with the `xmax` of the latest probe (`GapTracker.widen`; `delivered_xid32` widens and
adds in one call). A probe is at most one group old, so the transaction and the reference are far less than
2^31 ids apart and the result is exact. The stream never widens with a stale reference: an id kept from an
earlier run, or read from the destination, can be 2^31 ids old on a busy source and gives an id of the wrong
epoch. The tracker would then wait for a transaction that does not exist, or miss the one that does. Before the
first probe the reference is the id the tracker was started with (below). `delivered` refuses an id that lies
2^31 or more from the reference, which is what a 32-bit id passed by mistake looks like once the source has
used 2^32 ids.

Why it is exact. A transaction delivered at or before `P0` is either in the mark, or a probe saw it before the
mark was made, and then the later snapshot sees it too. So the snapshot sees every delivered transaction if
and only if it sees every id of the mark. The rule waits for the transactions in the gap and for nothing else.

After an engine start the tracker does not know what was delivered before. It starts **blind** with
`unknown_below = X`, where `X = pg_current_xact_id()` is read after the start (`next_xid_marker(conn)`): marks
also demand `xmin ≥ X` (rule A) until one probe has `xmin ≥ X`. From then on the tracker is exact again.

**Measured** in the same four campaigns. A marker is a simulated switch (about 40 per second); the checker
knows for every snapshot which delivered transactions it reads.

| | `primary_async` | `primary_remote_apply` | `primary_sync_on` | `primary_twophase` |
|---|---|---|---|---|
| Markers | 2,382 | 2,418 | 1,609 | 2,455 |
| Snapshots that miss a delivered transaction | 0 | 2,317 | 0 | 3,103 |
| Violations of rule A, B, C, C blind | 0 | 0 | 0 | 0 |
| Snapshots judged with a mark; mark and exact answer differ | 95,226; 0 | 96,705; 0 | 64,312; 0 | 98,176; 0 |
| Marks that are empty / not empty (largest) | 2,382 / 0 | 2,312 / 106 (3) | all empty | 2,323 / 132 (3) |
| Wait after a marker, rule A: median / 99% / max | 0.133 / 0.300 / 0.304 s | 0.143 / 0.455 / 0.562 s | 0.135 / 0.298 / 0.304 s | 0.012 / 0.217 / 0.293 s |
| Rule B | 0.035 / 0.241 / 0.277 s | 0.059 / 0.389 / 0.509 s | 0.052 / 0.253 / 0.279 s | 0.0003 / 0.154 / 0.239 s |
| Exact rule (first snapshot that sees everything) | 0.0003 / 0.003 / 0.033 s | 0.0002 / 0.136 / 0.245 s | 0.0002 / 0.002 / 0.005 s | 0.0003 / 0.138 / 0.239 s |

The waits of rules A and B in the first three campaigns are the lifetime of the `long` writer's transactions
(0.3 s; `primary_twophase` has no such writer) and would be hours with a forgotten prepared transaction. The
exact rule waits only when a commit is held back (the 99% values of the two campaigns with a paused standby).
The wait of rule C itself is in the evidence files (`wait_after_a_marker`, `gap_tracker`); it is measured over
the first 40 snapshots after each marker only, so its tail is cut off and it is not in the table.

**Recommendation.** Rule C, with rule A while the tracker is blind after a restart. The wait has the deadline
of the `R0 ≥ P0` wait; when it expires the engine reports the ids it waits for (`GapMark.blocking`,
`PrimaryCut.blocking_xids`). The same rule replaces `R0 ≥ caught_up_lsn` for a repair snapshot (core design
7.3) on a primary: the mark is made at `caught_up_lsn`. The first snapshot's `xmin ≥ X` in 7.5 is rule A and
stays correct.

Three rules that look right and are wrong (counts: async / remote_apply / sync on / twophase):

| Wrong rule | Caught | Why it fails |
|---|---|---|
| any snapshot taken after `P0` was applied | 0 / 2,317 / 0 / 3,103 | the gap |
| the same, with the `pg_xact_status` check of section 11 | 0 / 2,313 / 0 / 3,092 | the check does not see the gap |
| `xmax ≥ X` in place of `xmin ≥ X` | 0 / 2,346 / 1 / 3,277 | a delivered transaction is still in the in-progress list |

### 10.5 "The stream has passed the snapshot" on a primary

The stream has passed a snapshot when its applied position is at or above `I`, where
`I = pg_current_wal_insert_lsn()` read **after** the snapshot was taken.

Why. A transaction the snapshot sees had left the running set before the snapshot was taken. It wrote its
commit record before that. So its `E` is at or below the insert position at the time of the snapshot, which is
at or below `I`. The stream delivers in the order of `E` (section 6). "Greater than `I`" is sound too; "at or
above" does not wait for one more transaction on an idle server, where `E` of the last commit equals `I`
(section 6).

The flush position (`pg_current_wal_flush_lsn()`) is wrong here: with `synchronous_commit = off` a snapshot
sees a transaction whose commit record is not flushed yet.

Measured: the `E` of the last transaction each snapshot reads was compared with `I` and with the flush
position, per table and cut. Above `I`: 0 in the four campaigns. Above the flush position: 10,130 / 11,938 /
6,917 cuts in the three campaigns with an `async` writer, and 0 in `primary_twophase`, which has none.

## 11. The reviewer's proposal for the primary: `pg_xact_status`

During the work the coordinator passed on a proposal from a design review. It was reasoned from the source and
not run. It has four parts. Each was tested.

**Part 1: the check.** "After `pg_current_snapshot()`, read `pg_xact_status(x)` for every transaction id in the
snapshot's in-progress list. If one of them is already `committed`, discard the snapshot and take a new one."

Refuted. The check assumes that `pg_xact_status` reads the commit log, which is set before the id leaves the
running set. It does not: `xid8funcs.c` `pg_xact_status` (REL_18 line 640) first asks
`TransactionIdIsInProgress`, and answers `in progress` for every id that is still in the running set. The
comment in the function says why: so that it does not report `committed` for a transaction that snapshots do
not see yet. So the function agrees with snapshots by design, and cannot see the gap.

- Gap scenario (`primary_gap.json`, section 10.1): in all six held cases `pg_xact_status` said `in progress`
  at delivery and for the 3 s the commit was held, with the commit record flushed and delivered by the slot.
  The check did not discard the snapshot.
- Campaign (`primary_remote_apply.json`): of 2,317 snapshots that miss a delivered transaction the check
  discarded 4. The other 2,313 passed it (wrong rule `status_check`). With prepared transactions
  (`primary_twophase.json`): of 3,103 it discarded 11; 3,092 passed.
- The check also discards snapshots that miss nothing: 643, 1,211, 836 and 3,833 in the four campaigns. It
  fires when a listed transaction finishes between the snapshot and the status query. That is unrelated to the
  gap.

**Part 2: the answer for one transaction.** "A stream transaction is `inside` iff the snapshot sees it, judged
by its 64-bit transaction id; with the check above there is no `unknown`."

Confirmed, and without the check: section 10.3, more than 110 million answers, none wrong.

**Part 3: has passed.** "The applied position is greater than `pg_current_wal_insert_lsn()` read after the
snapshot was taken."

Confirmed: section 10.5. "At or above" is enough.

**Part 4: what replaces `R0 ≥ P0`.** "The same check, since a transaction the stream applied before `P0` is
`committed` by then and can no longer be in the in-progress list of a snapshot that passed it."

Refuted with part 1. Such a transaction is in the in-progress list, and the check passes the snapshot. The
replacement is the gap tracker of section 10.4.

**The negative control the review asked for.** "The rule without the `pg_xact_status` check must be caught by
the stress test." It is: wrong rule `later_snapshot`, 2,317 catches in `primary_remote_apply` and 3,103 in
`primary_twophase`. The rule with the check is caught 2,313 and 3,092 times in the same runs. The scenario with
a stopped synchronous standby is `primary-gap`; the campaign uses a standby whose replay is paused, because
writers must make progress.

**The three things the proposal left open.**

- *Subtransactions.* `pg_current_snapshot()` on a primary lists top-level ids only, with or without an
  overflowed subtransaction cache. The stream carries top-level ids. No special case (section 10.3; the gap
  scenario includes a subtransaction, the campaigns a writer with 100 subtransactions per transaction).
- *Prepared transactions.* The id is in the in-progress list from `PREPARE` to the end of `COMMIT PREPARED`.
  The gap opens at `COMMIT PREPARED` as for a plain commit (gap scenario, cases `prepared`). The answer by id
  stays exact. Rule A of section 10.4 would wait for every older prepared transaction; that is a reason for
  rule C.
- *Can `pg_xact_status` answer `in progress` for a transaction whose commit record is flushed?* Yes, see part
  1.

A check that would see the gap needs something written at moment 1 of section 10.1 and readable by SQL. The
commit timestamp is such a thing (`pg_xact_commit_timestamp`, set right after the commit record is written,
REL_18 `xact.c` line 1472), but it needs `track_commit_timestamp = on`, which is off by default and needs a
restart. It was not tested.
The gap tracker needs no server setting, because the engine already knows what the stream delivered.

**The three statements of the design on `main`** (the last chunk of a table is open-ended; a repair snapshot
needs `R0 ≥ caught_up_lsn`; the wait for `R0 ≥ P0` has a deadline). None is contradicted. Two additions follow
from this work: on a primary the repair snapshot needs the rule of section 10.4 in place of
`R0 ≥ caught_up_lsn`, and the wait for a settled standby (section 8) falls under the same deadline.

## 12. What is proven, what is refuted, what is open

### 12.1 The claims of the task

| # | Claim | Result | Where |
|---|---|---|---|
| 1 | `E ≤ R0` ⇒ the snapshot sees the transaction | Holds on a settled standby. Fails on a standby that has just started | 3, 5.1, 8 |
| 2 | `E > E1` ⇒ the snapshot does not see it; the sharper form by record start | Holds. The sharper form holds too, with the page-boundary exception, but only the tests can use it: stock Debezium does not deliver the start of the commit record | 2, 3, 5.2, 5.4 |
| 3 | Size of the unknown set, idle and under load | Measured. With the transaction id about two of three cuts under load (66.93% in the main campaign) and every idle cut have none | 5.2, 5.6 |
| 4 | `pg_current_snapshot()` on a standby | In-progress list always empty. Below `xmin` ⇒ inside, at or above `xmax` ⇒ outside; both hold. A quiet snapshot is exact | 3, 5.3, 5.4 |
| 5 | Both ways of taking the cut are valid; the in-statement position is an upper bound only | Own way: valid. Exported way: valid only for quiet snapshots or on a fixed server; what a table without a key does is decided (section 7.1). In-statement position as `R0`: violated 15,291 times in the main campaign | 5.4, 5.5, 7 |
| 6 | "Has passed" when the applied position is above `R1`; `R0 ≥ P0` | Both hold. `R0 ≥ P0` is not implied by where the slot is and must always be checked | 3, 5.6, 6 |
| 7 | The rules survive the listed conditions | Yes, every one of them, no violation. Seven runs had the complete checker, seven others its first two steps only | 5.1, 5.6, 9 |
| | Timelines | A cut belongs to the timeline it was taken on. Shown for a promotion | 9.2 |
| | The primary (open point 12) | Gap shown. Answer by 64-bit id is exact. A rule without the `xmin ≥ X` wait is proposed and tested | 10, 11 |

### 12.2 Refuted

- Core 7.2 as written: the `inside` rule without a condition. It needs a settled standby (section 8).
- Core 7.1 and 7.4 as written: readers import an exported snapshot on a standby. Unsafe on every released
  version unless the snapshot is quiet (section 7).
- Core 7.2 says `R0 ≥ P0` "matters when backfill reads use a different replica than the slot". It also matters
  on the slot's own replica, after a restart (sections 3 and 5.6).
- The plain form `S > R1 ⇒ outside` (sections 4 and 5.4).
- The position read by the statement that takes the snapshot as `R0` (section 5.4).
- On a primary: the `pg_xact_status` check, `synchronized_standby_slots` as a way to close the gap, and "has
  passed" judged by the flush position (sections 10 and 11).

### 12.3 Open, or covered less than I would like

- **Versions.** Everything was run on Postgres 18.6. Versions 16 and 17 are covered by reading the source
  only; the code in question is the same in the three branches (section 3). The live tests and `tools/cutstress`
  take the binaries from `CDC_V2_PG_BIN`, so that they can be repeated on 16 with another cluster home.
- **The fix for exported snapshots.** No release carries it, so it was not tested. The library's version table
  (16.16, 17.12, 18.7) is an expectation and has to be checked when those versions appear.
- **The standby that hides a commit** (section 8). The case of a new standby made from a base backup is argued,
  not measured. I did not look for an upstream report.
- **`pg_control_checkpoint()` on managed services.** With the default privileges the runtime role may call it,
  on a primary and on a standby, and the pre-flight check of the source package tests that
  (`docs/design/source.md` section 6a). Whether a managed service allows it is not checked.
- **Transaction id wraparound.** The widening to 64 bits is tested without a server
  (`tests/cut/test_cut_logic.py`). No live run crossed an epoch. The widening is right while a transaction and
  its reference are less than 2^31 transactions apart. The reference is the cut's own `xmax` on a standby and
  the `xmax` of the latest probe on a primary; the tracker refuses an id that is further away.
- **Wrong hint bits left by other importers** (sections 5.5 and 7.2). Seen in bulk in one of three runs: 37 of
  38,107 cuts of a reader that imported nothing were wrong in `xb_exported_subxact`, and none of 77,361 in the
  two other runs. That run did not record the rule per way, so the kind of these violations is inferred from
  the counts. The single case of section 7 shows the mechanism.
- **Runs before the missing-row check** (section 5.1). Repeated with the complete checker: the main campaign in
  a form of 10 minutes, variants 1, 3, 5 and 7 and the quiet-only run. Not repeated: the main campaign at its
  length of 30 minutes, variants 2, 4, 6 and 8 and the run `nc_mid_redo`. All of these take cuts the own way
  only. In them a row that a snapshot lost would not have been counted.
- **Stream options.** The rule is stated for a stream of whole transactions with `two_phase` off (section 6).
  Streaming of open transactions and `two_phase` on were not examined.
- **Timelines** (section 9.2). The rule is tested for a promotion. A standby that follows a new primary onto a
  new timeline was not run, and neither were a standby of a standby and reads from another replica than the
  slot. For these the rule about which timeline to compare with is argued, not measured.
- **The commit timestamp as a gap detector on a primary** (section 11). Not tested; it needs a server setting.
- **Load.** The distributions of section 5 come from one shared machine. They show the order of magnitude. They
  are not a guarantee for another machine or load. The correctness results do not depend on load. During the
  repeated runs other workers loaded the machine heavily (1-minute load average up to 85).

## 13. Proposed text for the core design

The text below replaces section 7.2 of `docs/design/core.md` as it stands on `main` (commit `29d345f`; its
section 7.2 and the rows 1 and 12 of its open points are those of `95ab8ee`), and those two rows. Changes
against the present text are marked **[new]** or **[changed]**; the marks are not part of the text. After it
come the changes outside 7.2.

Nothing in this text uses the start of the commit record: the pipeline does not have it (section 2).

### 13.1 Section 7.2

> ### 7.2 Where a snapshot lies in the stream
>
> Each snapshot has a **cut**: a function that answers, for a source transaction, `inside`, `outside` or
> `unknown`. The stream identifies a transaction by the end of its commit record (`E`, section 2), so the cut is
> stated in `E`. **[new]** The argument and the measurements are in `docs/design/cut.md`. `src/cdc_v2/pg/cut.py`
> is the only code that takes or judges a cut.
>
> **On a standby** (replica mode) the cut is a pair of replay positions around the moment the snapshot is
> taken: `R0` = `pg_last_wal_replay_lsn()` read by a statement that has finished before the snapshot is taken,
> and `R1` = the same function read after the snapshot exists. Two ways to get them:
>
> - a reader's own snapshot: `SELECT pg_last_wal_replay_lsn()` in autocommit (`R0`), then
>   `BEGIN ISOLATION LEVEL REPEATABLE READ`, whose first statement takes the snapshot and reads `R1`;
> - an exported snapshot: one read-committed transaction with three statements: `R0`, `pg_export_snapshot()`,
>   `R1`. **[new]** On a standby whose server has the defect of cut.md section 7, an exported snapshot is used
>   only if it is **quiet** (`xmin = xmax` in `pg_current_snapshot()`). A session that imports a snapshot that
>   is not quiet can see transactions the exporter does not see, and can lose rows. The fix is expected in
>   16.16, 17.12 and 18.7; `export_defect(server_version_num)` in the library is the one place that knows the
>   versions. What a table without a key does when no quiet snapshot comes is in 7.1.
>
> A value read by the statement that takes the snapshot is an upper bound only, never `R0`.
>
> Replay is serial, and a commit becomes visible to snapshots during the replay of its commit record, before
> the replay position moves past that record. Therefore, with `E1` = the `E` of the first transaction in the
> stream with `E > R1`:
>
> - `E ≤ R0` ⇒ `inside`;
> - `E > E1` ⇒ `outside`;
> - otherwise `unknown`: the commits in `(R0, R1]`, and the one at `E1`, whose record may have been in replay
>   while the snapshot was taken.
>
> **[new]** The first line holds only on a **settled** standby. A standby that has just started can hide a
> transaction whose commit record lies before its restart point until it replays the next commit or abort
> (cut.md section 8). A cut is therefore used only if the snapshot's `xmax` is at or above the next transaction
> id named in the standby's control file (`next_xid` of `pg_control_checkpoint()`). This value, the settle
> floor, is read once per connection before its first cut and is used with that connection only: the floor of
> one standby says nothing about another. No cut is taken without it. The heartbeat write settles a standby
> within one heartbeat interval. The pre-flight check calls `pg_control_checkpoint()` with the runtime role.
>
> The stream **has passed** a snapshot when its applied position is greater than `R1`; the transaction at `E1`
> is then applied. The heartbeat write guarantees that such a transaction arrives.
>
> A snapshot is used only if `R0 ≥ P0`: everything the stream applied before the table entered `backfilling` is
> then inside. **[changed]** The check is made for every snapshot. It matters when backfill reads use a
> different replica than the slot, and also on the slot's own replica: after a restart a standby answers
> queries from a replay position that can lie before what the slot has already delivered. The wait for it, and
> for a settled standby, has a deadline (the replica may be stalled); past it the backfill reports and keeps
> waiting as a declared wait (4.3), it does not spin and does not read.
>
> `pg_current_snapshot()` cannot give the answer on a standby: its in-progress list is empty there. Its `xmin`
> and `xmax` can still turn `unknown` into an answer (transaction id below `xmin` ⇒ `inside`, at or above `xmax`
> ⇒ `outside`; **[new]** the 32-bit id of the stream is widened with the snapshot's own `xmax`). **[new]** A
> quiet snapshot has no `unknown` transaction at all. Both help 7.4. (The start of the commit record would
> narrow `unknown` further, but stock Debezium does not deliver it.)
>
> **On a primary** (primary mode, or reads from the primary) there is no such pair of positions: a commit record
> is written, and can be decoded by the slot, before the transaction becomes visible to snapshots; with
> synchronous replication that gap can be long. The cut is `pg_current_snapshot()` together with the
> transaction id of each stream transaction, **[new]** widened to 64 bits: `inside` if the id is below `xmax`
> and not in the in-progress list, otherwise `outside`. It never answers `unknown`. **[new]** The stream has
> passed the snapshot when its applied position is at or above `pg_current_wal_insert_lsn()` read after the
> snapshot was taken (not the flush position).
>
> **[new, replaces the reference to open point 12]** In place of `R0 ≥ P0`, the stream lane keeps the ids of
> the transactions it has delivered and that no snapshot of its own has seen yet. It takes a snapshot for this
> once per group (one statement, `pg_current_snapshot()`, on a primary connection of the stream lane; delivered
> ids are widened with that snapshot's `xmax`) and drops the ids that snapshot sees; the set is nearly always
> empty. The set as it is at `P0` belongs to the table's backfill. A snapshot taken later is used only if it
> sees every id of that set. After an engine start the set is unknown: the lane reads
> `X = pg_current_xact_id()`, and until one of its own snapshots has `xmin ≥ X`, a snapshot is used only if it
> also has `xmin ≥ X`. The wait has the deadline of the `R0 ≥ P0` wait and reports the transaction ids it waits
> for. The same rule stands in for every other condition of the form `R0 ≥ position` (7.3). `pg_xact_status`
> cannot be used for this: it answers `in progress` for as long as snapshots do not see the transaction.
> `synchronized_standby_slots` does not close the gap either.
>
> **[new]** A cut belongs to the timeline it was taken on. After a promotion or a slot finding, open cuts are
> abandoned and their backfills start again. The timeline it is compared with is the stream's, read by a slot
> check that ran after the cut was taken (section 8: `IDENTIFY_SYSTEM`, never the control file). When backfill
> reads use a different replica than the slot, that replica's timeline is read in the same way and must be the
> same.
>
> **[changed]** **[test]** P3 has tested 7.2 (`docs/design/cut.md`, `evidence/p3/cut/`). On a standby: a model
> check over every interleaving of small histories, and stress runs that compare the rows visible in each
> snapshot with the commits of the stream; every deliberately wrong rule was caught. Seven runs had the complete
> checker (the window of `unknown`, the boundary of every key, rows missing from the snapshot): `main_recheck`,
> `v5b_restart_crash`, `v7b_exported_no_subxact`, `v9_exported_open_no_subxact`, `qb_quiet_exported`,
> `v1b_big_drop` and `v3b_ddl_walswitch_pause`, together 1,192,278 cuts over 5,025,620 transactions
> (subtransactions, aborts, asynchronous commits, big transactions, dropped tables, DDL, WAL switches, replay
> pauses, standby restarts and kills, exported and quiet exported snapshots), with no violation and no missing
> row. Seven earlier runs had the first two steps only, all with a reader's own snapshot: `main`,
> `v2_twophase_subxact`, `v4_apply_delay`, `v5_restart_crash`, `v6_slot_on_primary`, `v8_idle` and
> `nc_mid_redo`, together 2,982,523 cuts over 9,353,889 transactions (also two-phase commit, apply delay, the
> slot on the primary, an idle source), with no violation; a row that a snapshot lost would not have been
> counted in them. On a primary: 197,212 cuts and 110 million answers, none wrong. The tests stay in the
> repository: `tests/cut/` (hermetic, in the default run) and `tests/cut/test_cut_live.py` (marker `localpg`),
> and `tools/cutstress` repeats every campaign.

### 13.2 Open points

> | 1 | Cut on a standby (7.2) | Closed by P3 (`docs/design/cut.md`). Exact as described, with two conditions now in 7.2: the standby is settled; an exported snapshot is quiet or the server carries the fix |
> | 12 | Cut on a primary (7.2) | Closed by P3 (`docs/design/cut.md` section 10). Exact by transaction id; `R0 ≥ P0` is replaced by the rule in 7.2 |

Two new rows are proposed. The first is a decision made by the coordinator, which goes to the owner as such.
The second needs a decision of the owner.

> | new | A table without a key on a standby whose server has the exported-snapshot defect (7.1, 7.4) | Decided, reported to the owner: a quiet exported snapshot, waited for within a bounded, declared wait (default 60 s); if none comes, one reader with its own snapshot; on a server with the fix a plain exported snapshot (cut.md section 7.1) |
> | new | Two Postgres defects found by P3: exported snapshots on a standby (fixed upstream, not released), a standby that hides a commit after a restart (cut.md section 8, not reported) | Owner decides whether to report the second upstream; re-check the first when 16.16, 17.12, 18.7 appear |

### 13.3 Changes outside 7.2

- **7.1, third rule of "Read"** ("a table without a key needs one snapshot for the whole table (7.4). It is
  held by an exporter connection … readers import it per chunk with `SET TRANSACTION SNAPSHOT`"). Add after
  that sentence:

  > On a standby whose server has the exported-snapshot defect (7.2) the exported snapshot must be quiet. The
  > exporter takes snapshots until one is quiet, within a bounded, declared wait (4.3; default 60 s). If none
  > comes, the table is read by one reader with its own snapshot, in one transaction: waiting for a quiet
  > snapshot without a bound would never complete while a write transaction stays open on the primary. The one
  > reader is the fallback and not the rule, because it holds its lock on the table for the whole read: a
  > replayed `ACCESS EXCLUSIVE` lock cancels it after `max_standby_streaming_delay` and stalls replay, and with
  > it the slot, until then. On a server with the fix a plain exported snapshot is used.
  > `take_keyless_standby_cut` in `src/cdc_v2/pg/cut.py` does all three. No other program may import snapshots
  > on the standby we read: on a server with the defect an importer leaves wrong hint bits, which every later
  > reader of those rows inherits (runbook, P5).

- **7.3, repair read** ("The repair snapshot must have `R0 ≥ caught_up_lsn`"). Add: "On a primary: the rule of
  7.2 that stands in for `R0 ≥ P0`, with the set taken at `caught_up_lsn`."
- **7.4** ("one snapshot whose cut has no `unknown` transaction that touches the table"). Add: "A quiet
  snapshot has none. With the narrowing of 7.2 by transaction id, about two of three cuts under load (66.93% in
  the main campaign) and every cut on an idle source have no `unknown` transaction at all (cut.md section
  5.2)."
- **7.5** needs no change. Its `xmin ≥ X` is sound on a standby and on a primary (cut.md sections 3 and 10.4).
  On a standby the snapshot must also be settled, which 7.2 now demands of every cut.
