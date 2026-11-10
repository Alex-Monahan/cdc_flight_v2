# Apply (package A)

Status: P3 package A, built against contracts C0, C3a and C4 of `docs/design/p3_plan.md` and design section 6 of
`docs/design/core.md`. It turns one change set into the statements that change one destination table, inside a
transaction that someone else opened and will commit.

| Part | Where |
|---|---|
| Code | `src/cdc_v2/apply/` (`types.py`, `meta.py`, `changeset.py`, `plan.py`, `backend.py`, `statements.py`, `core.py`) |
| Tests, tier T0 (default run) | `tests/apply/test_apply_types.py`, `test_apply_cases.py`, `test_apply_property.py` |
| Tests on MotherDuck (marker `md`) | `tests/apply/test_apply_md.py` |
| Test model | `tests/apply/apply_sim.py` (simulated source table, oracle canonical forms), `apply_cases.py` (named cases), `apply_harness.py`, `apply_standin.py` (stand-in for D's lane transaction) |
| Campaign, speed | `tests/apply/apply_campaign.py`, `tests/apply/apply_perf.py` |
| Evidence | `evidence/p3/apply/` (section 9) |

```python
from cdc_v2.apply import Column, TableMeta, Target, LocalChangeSet, RelationChangeSet, apply_group, truncate
meta = TableMeta((Column(0, "id", "BIGINT", key=True), Column(1, "body", "VARCHAR", unchanged_possible=True)))
counts = apply_group(tx, Target("public", "t"), LocalChangeSet(rows, transactions, meta), mode="stream")
truncate(tx, Target("public", "t"))
reduce_duplicates(tx, target, change_set)        # catch-up, design 7.3 step 1
```

`tx` is a lane transaction (C4). Apply calls `tx.execute`, `tx.fetch_arrow` and `tx.register` only, and never
begins, commits, rolls back or fences. Until package D is merged the tests use `apply_standin.StandInTx`, which has
D's call shapes over a plain DuckDB connection (local or MotherDuck).

## 1. What a change set means

Terms. A *record* is one row of a change set (C3a): `op` `c`, `u` or `d`, the new image `a<i>`, for a table
without a key the old image `b<i>`, the flags `u<i>` ("value absent because unchanged", SPEC 2.6, H-003), and for a
table with a key the old key `k<i>` on the create that follows a key change. Records are ordered by (transaction,
`seq`); `seq` alone orders nothing across transactions. A change set is a contiguous run of records in stream order
and is not reduced. It may start and end inside a source transaction (design 6.5 cuts a group at a TRUNCATE or a
schema change); the one rule for a cut is that the `d` and the `c` of one key change are never separated. Nothing
in the plan depends on transaction ends.

The result of `apply_group` must be the table the source had after the change set, given the table the source had
before it. For a table without a key the result is the multiset of row images. Two values are *equal* as the
oracle says (`cdc_v2.oracle.canonical`, `docs/oracle.md` 1.2): NULL equals NULL, every NaN equals every NaN, `-0`
and `0` differ, `1 mon`, `30 days` and `720 hours` differ, a UNION value compares by member and value, lists and
structs element by element. Apply's own form of this is `types.exact(expr, type)` (section 5.1).

## 2. Tables with a key

### 2.1 The model: one key, at most one row

Rule K (core design section 6): a table has a key only if its primary key is immediate; a table with a deferrable
primary key is a table without a key and takes the path of section 4 with its full old images. So every table
that reaches this path is keyed by an immediate replica-identity key, and (H-015, measured on Postgres 16.6 and
18.6):

> For a table keyed by an immediate replica-identity key, no two rows hold one key at any record of the stream
> (WAL order inside a transaction, commit order across). So a key's first record in the change set fixes its start
> state (`c`: absent; `u`, `d`: present), and every later record must keep at most one row. A record that does not
> is a divergence (`ApplyInvariantError`). A key change is the `d` of the old key directly followed by the `c` of
> the new key with `k<i>`.

The plan (`plan.py`, steps `w` and `kp`) computes per exact key `p` (1 if the first record is `u` or `d`) and the
running sum `S` (+1 per `c`, −1 per `d`); a key is *bad* if after any record `p + S ∉ {0, 1}`, or a `u` finds
`p + S ≠ 1`. A bad key raises `ApplyInvariantError` in both modes, naming the table and the number of such keys:
a `c` at a key that holds a row, a `u` or a `d` at a key that holds none, is not a source history. Each key holds
at most one *arrival* (row version) at a time: arrival 0 is the start row if `p = 1`, each `c` adds the next one,
each `d` removes the current one, a `u` changes it. The final row is arrival `n_d` if `p + n_c − n_d = 1`.

Keys are compared exactly (`types.exact`), which is finer than the source: for Postgres `-0 = 0` and
`1 mon = 30 days = 720 hours`. Two rows whose keys differ only in form cannot exist at once at the source; one row
can change the form of its key, which arrives as a key change `d(-0) c(0, k=-0)` and is a move like any other
(3.4). A change set that holds two forms of one key at once (the second review's case B) is not a source history;
it ends in `ApplyInvariantError` (named case `review_B_two_forms_of_one_key_held_at_once`).

### 2.2 Where an unchanged value comes from

For every column that can be unchanged, the final row's value is the newest present value along its history: its
own records first (the `c` that created it, the `u`s that changed it), then, following the link of its `c`, the
records of the arrival it was moved from, and so on (step `res`, a recursive query over the links). The search
ends:

* at a present value: the column is concrete (its record number is kept, the value is read at the end);
* at arrival 0 of some key X (the row the source had at the start): the value is the destination row X's value,
  the *root* (C3a rule 2); the row is moved, not copied (section 3);
* elsewhere (a `c` without `k<i>` that carries "unchanged", or a broken link): no source.

Mode `stream`: a final row without a source raises `ApplyInvariantError`, and so does a root that the destination
lacks (the row mover's count). Only final rows count: a row that is deleted later in the change set needs no
values.

### 2.3 Mode catch_up

In catch-up the destination was loaded under several snapshots (design 7.3), so a destination row is some version
of the row with that key, older or newer than the change set's start. With `p` fixed by the first record, the
rules (C3a rules 2 and 3) are:

1. the root X (arrival 0 of the key the row had at the start, so the first record at X is a `u` or a `d`) is used
   if the destination holds exactly one row for X and **no `c` of the change set lands on X**. Why the second
   condition: a destination row at X is the row at X at its snapshot's time; if the change set creates a row at X
   (the key is reused) a snapshot after that holds the new row, and nothing in the destination says which. With no
   `c` at X, every destination row at X is the start row or nothing. A chain `1→5→6` resolves by this rule; a swap
   through a free key does not (it creates rows at both roots), and named case
   `catch_up_swap_through_a_free_key_after_the_swap` shows the wrong value the rule prevents.
   One exception (second fix round, item 5): the root X is the final key itself and the only `c` at X is the one
   that creates the final row (`d(K) c(K, k=K)`, or `K → L → K`). A destination row at X is then the start row or
   the final row (with u's applied that the change set holds), and the final row's unchanged column is, by the
   link chain the plan followed, the start row's value, with no record in between carrying another: both hold
   the same value of that column, so either may supply it. With two creates at X an arrival between could hold
   another value, and the key stays unresolved (named cases `round2_catch_up_*`);
2. else, if the final row's own `c` is the first record of its key in the change set, the destination row with the
   final key, if exactly one (C3a rule 3: the snapshot already saw the row under its new name). The first record
   being a `c` means the key was free at the start (`p = 0`), so a destination row there can only be this row's;
3. else the key is **unresolved**: nothing is inserted or moved for it, its destination row is deleted (2.6), and
   its key columns are returned in `ApplyCounts.unresolved` for the repair read.

`c(10) d(10)` leaves nothing unresolved (the first record fixes `p = 0`). Catch-up asks the destination once per
change set for the candidate rows of rules 1 and 2 (a lookup by exact key, returning counts). On the shape of the
second review (rounds with repair reads, up to 6 snapshots, unchanged-value rate 0.8; `rounds_scenario`, the same
200 seeds for both codes) the share of log records that end as an unresolved key went from 13.9 % (the code before
this round) to 3.2 %.

Idempotence (C3a): **only the immediate repeat of a call is safe**: applying the same change set twice in a row
gives the same table except for keys returned as unresolved by either call, and the caller repairs the unresolved
keys of every call, a repeated one included. (Example: after `d(2) d(1) c(2, k=1)` with an unchanged column, the
first call resolves key 2 by rule 1; the second finds root 1 gone and rule 2 does not apply, so it reports key 2
and deletes its correct row.) A call repeated after a later call is wrong without a sign: start `(1, 'a', 'B1')`,
CS1 `d(1) c(2, 'a', big unchanged, k=1)`, CS2 `c(1, 'new', 'Z')`; CS1, CS2, CS1 gives `(2, 'a', 'Z')` and
reports no key (the repeat takes `big` from the row CS2 wrote at the reused key 1;
`test_apply_limits.py::test_only_the_immediate_repeat_of_a_call_is_safe`). Tested: named cases (`catch_up_*`,
`twice=True`), every catch-up property example, and `test_catch_up_rounds_with_repair_reads` with repeated calls
whose keys are all repaired.

A repair **replaces** the key's row: in the repair's own transaction, delete every destination row of the key,
then insert the source's row (2.6). A plain INSERT duplicates the row when a later call has already resolved a key
that an earlier call reported (`test_apply_limits.py::test_a_repair_replaces_the_row_of_its_key`; the rounds
property loads its repair reads this way).

The bound (asserted in every rounds example): a key is unresolved only if its row at the end of the call has a
column whose value is in no record of the change set. A planner that reports everything unresolved fails it
(negative control, section 8).

### 2.4 Several destination rows with one key

Not apply's to repair (C3a), but what happens:

* stream mode: K1, K3 and C0 count the rows per key inside the statement and act only where a key holds what the
  source held; the change set raises `ApplyInvariantError` (2.5);
* catch-up: K1 removes all of them (it joins by key); catch-up never uses a root with more than one row (2.3);
* `reduce_duplicates(tx, target, change_set)` is offered to the backfill (design 7.3 step 1): for the keys of a
  change set it keeps one row per key (`DELETE ... WHERE rowid IN (... row_number() OVER (PARTITION BY key) > 1)`).

### 2.5 Stream mode checks the start state it relies on

In stream mode the destination equals the source at the start of the change set (exactly-once by position, C4).
So a key whose first record is `c` (`p = 0`) has no destination row, and a key whose first record is `u` or `d`
(`p = 1`) has exactly one. Both are checked **per key, inside the statements that act on the keys** (no extra
round trip), and a difference raises `ApplyInvariantError`:

* K1 (keys with `p = 1` whose row goes, and keys with `p = 0` that the change set creates and removes again)
  deletes only if every key of the statement holds `p` rows; otherwise it deletes nothing, or, when no key of the
  statement has `p = 1`, every row it finds. Its count therefore equals the number of `p = 1` keys only if every
  key is right, whatever the sums (checked per slice of `statement_rows`);
* K3 moves a root only if it holds exactly one row; C0 (3.4) finds exactly one row per copy root. Their counts
  must equal the plan's. (Before the second fix round these were sums: a destination holding key 1 twice and
  lacking key 3 passed `d(1) c(11, k=1)`, `d(3) c(13, k=3)` and kept two rows at 11; named cases `round2_4b_*`.)
* a key with `p = 0` that ends with a row must be free: K6 inserts only at free keys
  (`... FROM ins n ANTI JOIN t ON key(t) = n.x`), K3 and K7 move or copy onto a target with `p = 0` only if it is
  free; their counts must equal the plan's. This is `ApplySettings.check_new_keys` (default on). An insert-only
  change set is one statement (K6); before this round the check was an extra K1. MotherDuck from this laptop,
  1,000,000 rows, change sets of 10,000 records, medians of 5 (`evidence/p3/apply/check_new_keys_cost.json`):

  | change set | before (68df804) | after |
  |---|---|---|
  | inserts, BIGINT keys, check off | 361 ms, 1 round trip | 368 ms, 1 |
  | inserts, BIGINT keys, check on | 567 ms, 2 (K1, K6) | 359 ms, 1 (K6) |
  | inserts, UUID keys, check on | 579 ms, 2 | 378 ms, 1 |
  | updates with an unchanged column (K3; the per-key count) | 369 ms, 1 | 386 ms, 1 |

  With the switch off, two things are unchecked: an insert at a key that holds a row, and a held key moved onto a
  key that the source did not hold but the destination does (both silent then; named cases
  `round2_*_unchecked_with_the_switch_off`). A key created and removed inside the change set is checked in K1
  only with the switch on;
* two keys that the change set ends with and that the source's equality merges (`-0` and `0`, `1 mon` and
  `30 days`) are not a source history (H-015): `ApplyInvariantError` in both modes, from the plan
  (`review_B_two_forms_of_one_key_held_at_once`).

This makes a `c` at a key that a row held before the change set loud: the second review's A1, A4 and SEG, a table
wrongly treated as keyed, and a delete or full update of a missing row (named cases `review_*`,
`stream_*_raises`). Known limit: the checks compare exact keys, so a create at another form of a key that a row
the change set does not touch holds (`c(-0)` while the destination holds `0`) is not seen; it is not a source
history either.

### 2.6 Preconditions of catch_up (proposed for C3a)

1. **A catch-up call starts at or before the lower bound of every snapshot not yet passed, and its change set
   reaches at least `R1` of every snapshot whose rows the table holds, repair reads included; the backfill waits
   for the pending log to reach that point.** Otherwise the call can take an unchanged value from a destination
   row that a later transaction (outside the call) put under a reused key, and nothing in the call shows the
   reuse. `test_catch_up_split_before_the_newest_snapshot` (strict `xfail`) shows the wrong value when the rule is
   broken. **A call is repeated only directly after itself**: once a later change set has been applied to the
   table, an earlier one is never applied again (2.3; the repeat can take a value from a row the later call wrote,
   without a sign).
2. **The row of an unresolved key is deleted.** It is stale by definition, and a later call (the next round) would
   otherwise take values from it. The repair read loads it again; it must cover every reported key, including keys
   the source no longer holds. **A repair replaces the key's row**: in the repair's own transaction it deletes
   every destination row of the key and then inserts the source's row (or nothing, if the source has none). A
   plain INSERT duplicates the row when a later call resolved the key before the repair ran (2.3).

The backfill loads a repair read with **`apply.repair_keys(tx, target, meta, rows, params=(), keys=None,
key_params=()) -> ApplyCounts`**, in the caller's transaction. `rows` is the repair read with the destination's
column names: an Arrow table (registered on `tx`) or a SELECT the destination runs (with `params`); `keys`, in the
same two forms, adds reported keys that the source no longer holds. It sends two statements: RK1 deletes every
destination row whose key the source calls equal to a key of `rows` or `keys` (DuckDB `=` through
`types.source_wrap`: `-0` = `0`, NaN = NaN, `1 mon` = `30 days`), so that a stale row under another form of the
key does not survive the repair of key `0`; RK2 inserts `rows`. It counts both (`deleted`, `inserted`). Tested
in both forms, local and on MotherDuck: the late repair of 2.3 and a stale `-0` / `1 mon` row
(`test_apply_limits.py`, `test_apply_md.py::test_repair_keys_on_motherduck`); the rounds property loads every
repair read through it.

### 2.7 Other readings of C3a (contract notes)

* A change set is `LocalChangeSet(rows, transactions, meta)` or `RelationChangeSet(sql, params, meta,
  scratch_prefix="_cdc_apply", u_present=None)`. `transactions` is not needed for the current-state table and may
  be `None`; the local order column is `txn`, the relation order column `commit_lsn`.
* A NULL `u<i>` means "not unchanged". A `u<i>` column is honoured whenever it is present, also if `meta` does not
  flag the column (otherwise its NULL `a<i>` would be written as a value). `u<i>` on a key column is ignored: a key
  value is always present; a record without its key value raises `ApplyInvariantError` (an unchanged TOASTed key
  column, which pgoutput can send, is a hazard for the catalogue).
* A key change is recognised only when the `c` with `k<i>` directly follows the `d` of that old key in the same
  transaction (the engine's order). Otherwise the link is broken: values that need it are unresolved.
* `op = 't'` (or anything but `c`, `u`, `d`) raises `ChangeSetError`, as do two records with one
  (transaction, `seq`) and a record without a transaction order or `seq`.
* Mode `catch_up` on a table without a key is accepted and behaves as `stream` (design 7.4 replays such a table
  from one exact cut, so it needs nothing else).
* `ApplyCounts`: `records`, `by_op`, `deleted`, `inserted`, `updated` (rows moved or updated by K3), `statements`
  (statements sent through `tx`, one round trip each), `shapes` (the shape of each, section 3.2), `unresolved`
  (catch-up: an Arrow table with the key columns `a<i>` in destination types, also when empty; `None` in stream
  mode).
* `ApplySettings`: `statement_rows`; for the planning database of the local form `local_threads`,
  `local_memory_limit` (1 GB), `local_temp_directory` (required: the pipeline's spill area; a missing directory is
  `ApplySettingsError`) and `local_max_temp_directory_size` (4 GB); all are set before that database's first
  statement. A plan that exceeds both limits raises `ApplySpillFull` (no value in the message; the database is
  closed, so its spill files go, and the next call starts afresh); the stream can then stage the change set and
  use the relation form. `check_new_keys` (2.5).
* The relation form creates its working tables in a schema `<scratch_prefix>_<random>` inside the caller's
  transaction and drops every table and the schema before returning (one `DROP TABLE` per table, section 5).
* `TableMeta` refuses two names that differ only by case. Any other string is a destination name, including
  `rowid` (section 4.3), quotes, dots, spaces, keywords, non-ASCII and our own column names (`a1`, `x0`, `n`).

## 3. Statements for a table with a key

### 3.1 The plan

The plan (`plan.py`) materialises, step by step, the relations the statements read: `del` (keys to delete),
`ins` (complete final rows), `upd` (final rows with values from a root: root key, final key, the values the
change set supplies, and per unchanged-possible column a flag `h<i>` "value supplied"), `copy` (see 3.4) and, in
catch-up, `unres`. Local form: in a private in-memory DuckDB (one per thread, a cursor and schema per call; about
15 ms for a small change set on this laptop). Relation form: in the caller's transaction, as tables of a schema of
its own; no row comes to the client.

One rule for every step: **a value of a row never passes through an aggregate, a window or QUALIFY** (section
5.3). Values live only in `r0` (the change set as given: locally a view over the Arrow data, remotely one copy);
every other relation holds record numbers, positions and exact keys. The outputs are then built from positions:
in the local form by Arrow `take` from the change set's own columns (no SQL touches a value on the way, so no
normalisation can happen), in the relation form by joins on the row id of `r0`. Locally, intermediate tables are
dropped after their last use, and the planning database has a memory limit (`ApplySettings.local_memory_limit`,
default 1 GB) beyond which DuckDB spills. Measured for one million lineitem-width records (180 MB of Arrow): the
plan holds 90-460 MB of tables depending on the limit, and the process's peak resident set grows by about
1.1-1.3 GB during planning (freed memory is not returned to the system, so the peak is a high-water mark);
assembling the outputs adds about 60 MB per 333K rows. Client memory is proportional to the change set.

### 3.2 The shapes

`t` is the destination table, `n` the plan's relation, `x<i>` exact keys, `rx<i>` root keys. Order as listed;
each runs only when the plan says it has work.

| Shape | Statement | Rows |
|---|---|---|
| lookup | `SELECT c.ci, count(t.k) FROM cand c LEFT JOIN t ON key GROUP BY ALL` | catch-up only: candidate roots |
| K1 | `DELETE FROM t USING del n WHERE key(t) = n.x` (stream: `AND (every key of del holds p rows OR no key of del has p = 1)`) | stream: keys with `p = 1`, and keys with `p = 0` that end without a row; catch-up: every key of the change set; never a root that will be moved or copied |
| C0 | `SELECT n.ci, t.rowid FROM copy_roots n JOIN t ON key(t) = n.rx` | 3.4 only: the row id of each copy root |
| K2 | `UPDATE t SET d = CASE WHEN need THEN sentinel ELSE t.d END, ... FROM upd n WHERE key(t) = n.rx AND (need ...)` | root rows whose always-present FLOAT/DOUBLE/INTERVAL/STRUCT column changes (5.2) |
| K3 | `UPDATE t SET key = n.key, c = n.c ... FROM upd n WHERE key(t) = n.rx` — keys and always-present columns (stream: `upd` joined to the count of rows per root, `= 1`, and to the targets with `p = 0` that are free) | every `upd` row; the count must equal the plan's |
| K7, K8 | `INSERT INTO t SELECT n.key, CASE WHEN n.h THEN n.c ELSE t.c END ... FROM copy n JOIN t ON t.rowid = n.rid`; `DELETE FROM t WHERE rowid IN (SELECT rid FROM copy)` | 3.4 only |
| K4.i | `UPDATE t SET c = sentinel(n.c) FROM upd n WHERE key(t) = n.x AND n.h AND need` | column i, if its type is affected (5.2) |
| K5.i | `UPDATE t SET c = n.c FROM upd n WHERE key(t) = n.x AND n.h AND ex(t.c) IS DISTINCT FROM ex(n.c)` | one per unchanged-possible column that some `upd` row supplies, and per always-present LIST/MAP/UNION column |
| K6 | `INSERT INTO t (cols) SELECT n.a<i> FROM ins n` (stream with `check_new_keys`: `ANTI JOIN t ON key(t) = n.x`) | complete final rows |

The full SQL of every shape for a sample table is in `evidence/p3/apply/shapes.json` (generated from 120 random
change sets; one text per shape). The number of shapes is at most `7 + (unchanged-possible columns) + (LIST/MAP/
UNION columns that cannot be unchanged) + (affected unchanged-possible columns)`: it depends on the columns only
(`Shapes.shape_bound`, asserted by `test_statement_shapes_depend_on_columns_only`). DELETE and INSERT from local
data run in slices of `ApplySettings.statement_rows` (default 1,000,000) rows: bounded hand-over (design 6.1);
the statement count grows, the shapes do not.

Why it is right, in order (the order is K1, C0, K2, K3, K7, K8, K4/K5, K6):

1. K1 removes every destination row whose key the change set touches, except roots of moved or copied rows.
   After it, the keys of the change set hold nothing but roots, each its own row.
2. C0 reads the row id of every copy root (3.4) while no statement of the call has moved or inserted a row: each
   root key holds exactly its own row (checked).
3. K3 moves each root row to its final key and writes the key and every always-present column, in **one**
   statement, so a permutation of keys (a swap or rotation through a free key, moves that land on keys other
   moves leave) is a parallel assignment: the join reads the rows before any is changed. It joins on the roots of
   `upd` only; those hold their own rows (step 1), and no copy has been inserted yet, so it never meets a copy.
   Nothing unchanged is written, nothing is read into the client.
4. K7 and K8 find each copy's old row by its row id (step 2), never by its key. A K3 move may have put another row
   at the copy root's exact key (second fix round X1: `-0,1 → 0,2` copied while `5,5 → -0,1` moves), a copy may
   land on the key that another copy leaves (`0,2 → -0,3`, then `-0,1 → 0,2`), or the keys may form a ring with a
   move (`-0,1 → 0,2`, `0,2 → 5,5`, `5,5 → -0,1`): the row ids are the same rows in every case. Row ids are
   stable in the transaction: K1 deletes other rows, K2/K3 update other rows in place (no LIST/MAP/UNION column,
   5.4). A copy lands on its final key after every move, so no later statement joins on it but K4/K5/K6 at other
   final keys. Every mixture of moves and form-only key changes therefore meets only the rows each step means
   (named cases `round2_*`, the campaign's `keyed_form` kind).
5. K5 writes, per column, the values the change set supplied, only for rows that carry one, and only where the
   value differs exactly. An unchanged value is never written (it is not in the SET list for that row) and never
   read into the client (SPEC 2.6).
6. K6 inserts complete final rows; their keys are free after K1, K3 and K8.

Round trips (local form): a change set of complete images on a table with a key is K1 + K6 (insert-only: K6);
on a table without a key L1 + L2; an insert-only change set is K6 (the check of 2.5 is inside it); partial images add K3 and
one K5 per supplied unchanged-possible column (the cost of "never written"; a single combined `SET c = CASE WHEN
n.h THEN n.c ELSE t.c END` would be one statement but assigns unchanged columns their own value, see the report).
The relation form adds the plan's tables (about 20 statements). Measured counts: `md_cases.json`,
`perf_md.json`.

### 3.3 Equal values that DuckDB does not store (finding 1)

An UPDATE stores nothing when the new value compares equal (C++ `==`) to the value in the column's data slot:
`-0 → 0`, `1 mon → 30 days`, also inside a STRUCT, and also through a NULL (the slot under a NULL keeps its old
value, so `NULL → 0` over a slot holding `-0` reads back `-0`). There is no single-statement UPDATE that always
writes. The cure is two writes of the changed columns only: a *sentinel* (a value unequal to the new one: `1` or
`2`, `1 µs` or `2 µs`, a STRUCT with sentinel members) and then the value (K2 before K3, K4 before K5), only for
rows where the stored and the new value differ exactly and the new value is not NULL. Complete images use DELETE +
INSERT, which always writes. Affected types: FLOAT, DOUBLE, INTERVAL and STRUCTs that hold them. LIST, MAP, UNION
and ARRAY are not affected: their update rewrites the row (5.4). Tested for every affected type, complete and
partial, in both modes, local and on MotherDuck (`special_cases` in `apply_cases.py`, `test_apply_types.py`).

### 3.4 Keys that change only in representation

A key of type DOUBLE or INTERVAL can change from `-0` to `0` or from `1 mon` to `30 days` (Debezium sends a key
change: Java compares the doubles by bits). Exact keys keep the two apart, so this is a move like any other — but
`UPDATE SET key = 0` over `-0` stores nothing (3.3), and a sentinel cannot be used on the key (the next statement
finds rows by key). Such a row with values from its root is copied instead (K7: the root's unchanged values are
copied on the server; K8 deletes the root). This is the only place where an unchanged value is rewritten, and only
for this case. It also applies to a composite key when one DOUBLE or INTERVAL part changes only in form while
another part changes (found by the review: K3 would set the other part and leave the stored `-0`). The root is
found by its row id (C0, read before any row moves), so that a move onto the root's old form in the same call
does not make it two rows (second fix round, X1; order in 3.2). A table with a column named `rowid` has it renamed
around the statements when the change set holds a copy (4.3).

## 4. Statements for a table without a key

### 4.1 delta

Per record: −1 for the old image (`d`, `u`), +1 for the new image (`c`, `u`; an unchanged column takes the old
image's value, which REPLICA IDENTITY FULL always carries). Images are grouped by their exact form
(`exact(column)` per column; GROUP BY treats NULL as equal), summed, and groups with a sum of 0 dropped. A
representative record number per group supplies the values.

### 4.2 Statements

| Shape | Statement |
|---|---|
| L1 | `DELETE FROM t WHERE rowid IN (SELECT rid FROM (SELECT t.rowid AS rid, n.n AS need, row_number() OVER (PARTITION BY n.gid ORDER BY t.rowid) AS rk FROM t JOIN neg n ON ex(t.c) IS NOT DISTINCT FROM n.e ...) WHERE rk <= need)` |
| L2 | `INSERT INTO t SELECT v<i> FROM (SELECT *, unnest(range(n)) FROM pos)` |

L1 deletes exactly `need` rows per image, chosen by row id (open point 2: `rowid` works on MotherDuck, also for
rows the same transaction inserted; `duckdb_semantics.json`). It must remove `sum(need)` rows; fewer is
`ApplyInvariantError` in both modes (C3a). The image `-0` never deletes a row `0`, and a change `-0 → 0` does not
net to nothing (named cases `keyless_*`).

### 4.3 A column named `rowid`

A column named `rowid` (any case) hides DuckDB's row id. Apply renames it inside the caller's transaction
(`ALTER TABLE ... RENAME COLUMN`, shapes R1 and R2), runs L1, the copy of 3.4 or `reduce_duplicates` with the temporary name, and
renames it back; a roll-back undoes the rename. Tested locally and on MotherDuck.

## 5. DuckDB and MotherDuck behaviours this package relies on or avoids

All with DuckDB 1.5.6; "both" means local DuckDB and MotherDuck alike (`evidence/p3/apply/duckdb_semantics.json`
unless another file is named).

1. **Equality merges different values** (both): `=`, GROUP BY, PARTITION BY and DISTINCT treat `-0` = `0`,
   NaN = −NaN, and intervals of equal length (`1 mon` = `30 days` = `720 hours`, `1 day` = `24 hours`,
   `1 mon -30 days` = `0`) as one, also inside LIST, STRUCT, MAP and UNION values. TIME WITH TIME ZONE, VARCHAR,
   BLOB, JSON, DECIMAL (one scale per column), dates and timestamps with ±infinity compare exactly. `exact()` maps
   FLOAT and DOUBLE to their bits with one NaN, INTERVAL to its text (months, days and microseconds kept apart),
   and nested values member by member. No destination type of C3 is left that cannot be compared exactly; NaN
   payloads are one value by the oracle's rule.
2. **An UPDATE to an equal value stores nothing**, also through a NULL (both; 3.3).
3. **Aggregates over nested values normalise -0 and NaN** (both): `any_value`, `first`, `arg_max`, `max_by` of a
   LIST, STRUCT or UNION return `0` for a stored `-0`; scalar arguments are kept. So does `last_value(...
   IGNORE NULLS)` over a STRUCT that holds a LIST, and a `CREATE TABLE AS` whose query has QUALIFY over rows with a
   STRUCT holding a DOUBLE (the same SELECT without CREATE TABLE keeps `-0`). Joins, projections, UNION ALL and
   plain windows keep it. Hence the plan's rule (3.1).
4. **An UPDATE that sets a LIST, ARRAY, MAP or UNION column is a delete and an insert of the row** (both; its row
   id changes): every column of the row is copied on the server. K5 sets such a column only where its value
   changes; when it does change, the row is copied — the server's doing, not a statement of ours.
5. **A hash join on plain DATE or TIMESTAMP values misses ±infinity** in rows the same transaction wrote, when the
   other side is Arrow data (both; DuckDB's join filter pushdown, gone with
   `SET disabled_optimizers = 'join_filter_pushdown'`). Every key join compares both sides through
   `types.join_wrap` (`date + 0`, `timestamp + INTERVAL '0 microseconds'`); both sides must be expressions. Found
   by the property test (seed 2273038); named cases `infinite_*_keys_in_one_transaction`. The same pushdown, into
   the Arrow scan, fails outright for UUID ("Unimplemented type UUID for Arrow Filter Pushdown") and JSON (no
   `is_in` kernel) keys once the transaction has written rows; `join_wrap` compares their text (found by the
   campaign, seeds 10002906, 10005444, 10005702; named cases `uuid_keys_in_one_transaction` and others). On
   MotherDuck a DELETE that joined a stored `-0` with an Arrow-side `0` by the plain DOUBLE column found no row
   (where `=` says equal); `repair_keys`, which compares by the source's equality, therefore compares FLOAT,
   DOUBLE and INTERVAL through an expression (`types.source_wrap`). (A first fix cast DATE to
   TIMESTAMP; the review found that this overflows for dates past 294247 AD, which Postgres and DuckDB allow, and
   puts the value into the error text: named cases `*date*beyond_the_timestamp_range`.) The cost: DuckDB cannot
   prune the destination scan by the join's key range for DATE, TIMESTAMP, FLOAT, DOUBLE and INTERVAL keys; every
   statement scans the destination's key column.
6. **`DROP SCHEMA ... CASCADE` of tables created in the same transaction fails at COMMIT** ("another transaction
   has dropped this table"; local DuckDB, see `duckdb_semantics.json` for MotherDuck). The relation form drops
   its tables one by one.
7. **`.arrow()` returns a RecordBatchReader** in DuckDB 1.5.6 (lazy). Registering it on the connection that
   produced it and querying deadlocks; apply uses `.to_arrow_table()`.
8. **Arrow export changes UUID, TIME WITH TIME ZONE and JSON** unless `arrow_lossless_conversion` is on (UUID and
   JSON become VARCHAR; TIMETZ becomes TIME and loses the offset). The planning connection converts losslessly;
   key values that come back through the lane connection travel as text and are cast back (`RemoteBackend.
   fetch_arrow`), and lookups return candidate numbers, not keys. Found by the property test (seeds 0 and 260).
9. **A UNION whose member holds NULL arrives through Arrow as NULL** (local): the oracle's rule is met by the
   transport; a change set staged by SQL could still hold one, and apply would write it as given.
10. **A column named `rowid` hides the row id** (both; 4.3).

## 6. Open points of design section 11

* **2 (rowid for k of n identical rows):** works, on MotherDuck too, including rows the same transaction inserted
  and with a column named `rowid` (renamed for the statement).
* **3 (canonical form for UNION and NaN):** apply's equality is the oracle's (`exact()` agrees with
  `oracle.canonical` on every case of `test_apply_types.py` and in every property example, which compare the
  destination with the model through the oracle's forms). A UNION compares by member and value; this is stricter
  than the oracle, which compares a member by its source family's rule (`1.5` as DECIMAL and as VARCHAR are equal
  for the oracle, different for apply). The pipeline writes one member per source value, so the two agree on
  everything the pipeline writes; after a type change (P4) old values keep their member and stay distinct.
* **9 (key change with unchanged values):** the old key on the create, chains resolved by links back to the root
  (2.2), the root moved by K3 (3.2) or copied (3.4); chains, swaps and rotations through a free key, form-only key changes and
  rotations, a key change back to the same key and onto a deleted key, within one change set and across two, in
  both modes: named cases and the property tests.

## 7. Speed indications (not results)

`evidence/p3/apply/perf_md.json`: this laptop to MotherDuck over the internet, lineitem width, short runs. The
spike's numbers come from an F16 Flight (`docs/spike/md_semantics.md` Q3: 0.066 s for 1-100 rows, 212K rows/s at
100K, 342-355K rows/s at 1M) and are bound by round trips (8 ms on a Flight, about 80 ms here) and upload. See
the report for the comparison.

## 8. How to run

```sh
uv run -q pytest -q tests/apply      # default run: every named case (local form; relation form for catch-up and
                                     # error cases), the regression cases, 3 examples per property, one small case
                                     # of each negative control; about 7 s on this laptop
CDC_V2_APPLY_FULL=1 CDC_V2_APPLY_EXAMPLES=300 CDC_V2_APPLY_SHAPE_SEEDS=25 uv run -q pytest -q tests/apply   # full
CDC_V2_MD_PREFIX=cdc_v2_t_a_ tools/with-env uv run pytest -m md tests/apply          # MotherDuck
CDC_V2_MD_PREFIX=cdc_v2_t_a_ CDC_V2_APPLY_PERF=1 tools/with-env uv run pytest -m md tests/apply -k speed
uv run python tests/apply/apply_campaign.py --seconds 600 --controls 200            # campaign and controls, local
uv run python tests/apply/apply_checkcost.py                                         # cost of check_new_keys
```

Negative controls (`tests/apply/apply_controls.py`): the properties run against planners that are wrong on
purpose and must catch each: (a) "the last record per key decides" for keyed tables, (b) plain GROUP BY on the
values for keyless tables, (c) a catch-up that reports every key unresolved (bound of 2.3). The campaign records
how many examples it took (`campaign_local.json`, `negative_controls_examples_to_catch`).

## 9. Evidence

| File | What |
|---|---|
| `duckdb_semantics.json` | section 5, local and MotherDuck side by side |
| `md_cases.json` | every named case on MotherDuck, both forms, with statements and seconds |
| `md_property.json` | property examples on MotherDuck |
| `campaign_local.json` | the campaign: summary, seed range, every failing seed, the negative controls, the code version (second fix round: 10,249 seeds, 192,121 records, six kinds including `keyed_form`, 0 failures; the second reviewer's keyed stream program, unchanged, on seeds 1000-2835 and 200000-201686 plus its 15 refused seeds: 3,525 seeds, 0 bad) |
| `check_new_keys_cost.json` | the cost of the stream start-state check (2.5) |
| `shapes.json` | the SQL of every statement shape for a sample table |
| `perf_md.json` | speed indications |

## 10. Not done, limits

* History tables and soft deletes (SPEC 8.1, 8.2) are P4. The plan computes, per record, its arrival and the
  final row of each key; a history table needs exactly that, from the same pass over the change set.
* A table with a deferrable key reaches apply as a table without a key (rule K, not this package).
* A key column sent as "unchanged" (a TOASTed key) cannot be applied: `ApplyInvariantError`.
* Speed is measured from this laptop only; Flight numbers are P9.
* The relation form sends about 40 statements per change set (one CREATE TABLE and one DROP TABLE per plan step;
  `md_cases.json`: median 41 against 2 for the local form). It serves oversized transactions and pending logs,
  where the rows dominate; merging steps would lower it.
* `join_wrap` covers DATE, TIMESTAMP and TIMESTAMP WITH TIME ZONE; the other timestamp precisions are not produced
  by C3 today.
* Towards package D's lane (D is reviewed, not merged; the stand-in `tests/apply/apply_standin.py` stays until
  then, and replacing it by D's lane is a separate small step after D's merge):
  * a statement that the destination refuses (a value that does not fit a column, a constraint) reaches the
    caller as D's `md.StatementError`, with the class of the error and the kind of statement, never a value. Apply
    catches nothing of D's: `DestinationError`, `DeadlineExceeded` and `LaneMisuse` pass through unchanged (with
    the stand-in, DuckDB's own exception passes through, and its text can name a value);
  * apply relies on the count of `execute` only for INSERT, UPDATE and DELETE without `RETURNING` (it sends no
    `RETURNING`);
  * the names apply passes to `tx.register` match `^[a-z_][a-z0-9_]{0,62}$` (`cdc_v2_apply_<relation>`; the
    stand-in asserts it);
  * a deadline is a `Deadline` object or None; apply passes none of its own;
  * the cursors of the shared planning database set their options with `SET SESSION`, never a plain `SET`
    (a plain `SET arrow_lossless_conversion` is global and would reach the other calls of the thread;
    `test_planning_settings_stay_with_the_cursor`).
* `reduce_duplicates` plans a local change set in the same planning database as `apply_group`, under the same
  `ApplySettings` (spill directory required; `ApplySpillFull` beyond the limits).
* C0 to K7/K8 relies on `rowid` staying the same within one transaction, also for a row that an earlier change
  set of the transaction inserted and that is not committed yet (named case
  `round3_copy_root_inserted_earlier_in_the_transaction`, both forms, local and on MotherDuck).
* Catch-up rule 1 with two creates at the root key (`d(K) c(K, k=K)` twice, each taking a value from the start
  row) stays unresolved, although it could be resolved (2.3).
