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
`seq`); `seq` alone orders nothing across transactions. A change set holds whole source transactions and is not
reduced.

The result of `apply_group` must be the table the source had after the change set, given the table the source had
before it. For a table without a key the result is the multiset of row images. Two values are *equal* as the
oracle says (`cdc_v2.oracle.canonical`, `docs/oracle.md` 1.2): NULL equals NULL, every NaN equals every NaN, `-0`
and `0` differ, `1 mon`, `30 days` and `720 hours` differ, a UNION value compares by member and value, lists and
structs element by element. Apply's own form of this is `types.exact(expr, type)` (section 5.1).

## 2. Tables with a key

### 2.1 The model: one queue per key

Design 6.2 says "the last record per key decides". That is wrong when a source transaction holds two rows with one
key at once, which a deferred unique constraint allows: `UPDATE t SET id = 3 - id WHERE id IN (1, 2)` sends
`d(1) c(2, k=1) d(2) c(1, k=2)`. After `c(2, k=1)` the source has two rows with key 2; `d(2)` removes the row that
had key 2 before, not the one that just arrived. The last record for key 2 is a `d`, yet key 2 exists at the end.
The same holds for `UPDATE t SET id = id + 1` over a whole table with a deferrable key (a realistic oversized
transaction).

Apply therefore models the source one key at a time, as a queue of row versions (*arrivals*):

* arrival 0 is the row the source had at the start of the change set, if it had one (`p` = 1);
* `c` adds an arrival at the back; `d` removes the front one (the oldest); `u` changes the front one;
* a key change is a `d` of the old key directly followed by the `c` of the new key with `k<i>` = the old key
  (`docs/design/engine.md` 2: "a key change arrives as a delete with the old key followed by that create"); the
  new arrival is *linked* to the arrival that `d` removed;
* at the end of the change set each key holds at most one arrival; that arrival is the key's *final row*.

`p` is not known to the client, but the records almost always fix it. With `S(t)` the running sum of +1 per `c`
and −1 per `d` at the key, consistency requires `p + S ≥ 0` after every `d`, `p + S ≥ 1` at every `u`, and
`p + S ∈ {0, 1}` at the end of every transaction. That gives bounds `lo ≤ p ≤ hi` (`plan.py`, step `kb`).
`lo > hi` is a divergence (stream: `ApplyInvariantError`; catch-up: the key is unresolved). `lo < hi` happens only
when every record at the key comes after a `c` of the same transaction and nothing forces the start state, for
example an insert and a delete in one transaction. Then:

* stream mode asks the destination (one *lookup* statement for all such keys of the change set, only when there
  are any): the destination equals the source before the change set;
* catch-up mode cannot ask (its table was loaded under other snapshots): the key is unresolved.

With `p` known, arrival numbers are counts: a `c` creates arrival `p + (c before it)`, a `d` removes arrival
`(d before it)`, a `u` changes arrival `(d before it)`, and the final arrival is number `n_d` if
`p + n_c − n_d = 1`. All of this is window sums over the records (steps `w`, `kp`, `ev`), with no loop.

What the model cannot know: with a deferred constraint and default replica identity, `INSERT 2; DELETE the old 2`
and `INSERT 2; DELETE the new 2` send the same records (a `d` carries only the key). Apply removes the oldest
arrival, which is right for every single-statement permutation (deferred swap, rotation, shift). This is a limit
of the stream, not of apply; it belongs in the hazard catalogue next to H-015.

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
lacks (the row mover's count, section 3.3). Only final rows count: a row that is deleted later in the change set
needs no values (SPEC is met; the destination ends as the source).

### 2.3 Mode catch_up

In catch-up the destination was loaded under several snapshots (design 7.3), so a destination row is some version
of the row with that key, older or newer than the change set's start. Rules, in order (C3a rules 2 and 3):

1. the root X (arrival 0 of the key the row had before the change set) is used if the destination holds exactly
   one row for X and **no `c` of the change set lands on X** (otherwise the destination row at X can be a later
   row that reused the key; see 2.6);
2. else, if the final row's own `c` is the first record of its key in the change set **and the source did not
   hold that key when the change set starts** (`p = 0`), the destination row with the final key, if exactly one
   (C3a rule 3: the snapshot already saw the row under its new name). The second condition was found by the
   campaign (seeds 10000325, 10004510, 10008026): in a deferred permutation the final row's `c` can arrive while
   the key's old row is still there, and the destination row under that key is then the old row;
3. else the key is **unresolved**: nothing is inserted or moved for it, its destination row is deleted (2.6), and
   its key columns are returned in `ApplyCounts.unresolved` for the repair read.

Catch-up asks the destination once per change set for the candidate rows of rules 1 and 2 (a lookup by exact key,
returning counts).

Idempotence (C3a): applying the same change set twice gives the same table except for keys returned as
unresolved. Complete final rows are rewritten identically; a moved row whose root is gone the second time is found
again under its final key by rule 2 when its `c` is the first record of that key. When it is not (the log deleted
the final key before moving a row onto it: `d(2) d(1) c(2, k=1)` with an unchanged column), the second call
reports key 2 unresolved and deletes its (correct) row: allowed by C3a, but a run-again then costs a repair read
(found by the review). Tested in the named cases (`catch_up_*`, `twice=True`) and in every catch-up property
example.

### 2.4 Several destination rows with one key

Not apply's to repair (C3a), but what happens:

* the delete removes all of them (it joins by key);
* the row mover (K3) moves all of them; its count then exceeds the expected count and stream mode raises
  `ApplyInvariantError`; catch-up never uses a root with more than one row (2.3);
* `reduce_duplicates(tx, target, change_set)` is offered to the backfill (design 7.3 step 1): for the keys of a
  change set it keeps one row per key (`DELETE ... WHERE rowid IN (... row_number() OVER (PARTITION BY key) > 1)`).
  Which one stays does not matter (design 7.3).

### 2.5 Stream mode trusts the destination

Stream mode deletes only keys with `p = 1`: a key the source did not have at the start has no destination row,
because the destination equals the source before the change set (exactly-once by position, C4). An insert-only
change set is one statement. A diverged destination is not detected by stream apply except where values are
needed from it (roots, counted). That is C3a's own statement: "mode stream makes no such promise and needs none".

### 2.6 Preconditions of catch_up (proposed for C3a)

Both were found by the property tests; each has a regression test.

1. **A catch-up call must reach past every snapshot whose rows the table holds** (chunk snapshots and repair
   reads): its change set must contain every transaction up to the upper bound `R1` of each of them. Otherwise
   the call can take an unchanged value from a destination row that a *later* transaction (outside the call) put
   under a reused key, and nothing in the call shows the reuse. `test_catch_up_split_before_the_newest_snapshot`
   (strict `xfail`) shows a wrong value when the rule is broken; with it, no later transaction can have reused
   the key, because any reuse before a snapshot is in the call (2.3 rule 1 then sees the `c`). The backfill
   meets it by starting catch-up only after the stream has passed every snapshot (the same condition as the swap,
   design 7.3) and, after a repair read, by the next round reaching past the repair snapshot.
2. **The row of an unresolved key is deleted.** It is stale by definition, and a later call (the next round)
   would otherwise take values from it (seed 135239). The repair read loads it again.

### 2.7 Other readings of C3a (contract notes)

* A change set is `LocalChangeSet(rows, transactions, meta)` or `RelationChangeSet(sql, params, meta,
  scratch_prefix="_cdc_apply", u_present=None)`. `transactions` is not needed for the current-state table and may
  be `None`; the local order column is `txn`, the relation order column `commit_lsn`.
* A NULL `u<i>` means "not unchanged". A `u<i>` column is honoured whenever it is present, also if `meta` does not
  flag the column (otherwise its NULL `a<i>` would be written as a value). `u<i>` on a key column is ignored: a key value is always present; a record
  without its key value raises `ApplyInvariantError` (an unchanged TOASTed key column, which pgoutput can send, is
  a hazard for the catalogue).
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
| lookup | `SELECT c.ci, count(t.k) FROM cand c LEFT JOIN t ON key GROUP BY ALL` | stream: keys with undetermined `p`; catch-up: candidate roots |
| K1 | `DELETE FROM t USING del n WHERE key(t) = n.x` | stream: keys with `p = 1`; catch-up: every key of the change set, but not a root that will be moved |
| K2 | `UPDATE t SET d = CASE WHEN need THEN sentinel ELSE t.d END, ... FROM upd n WHERE key(t) = n.rx AND (need ...)` | root rows whose always-present FLOAT/DOUBLE/INTERVAL/STRUCT column changes (5.2) |
| K3 | `UPDATE t SET key = n.key, c = n.c ... FROM upd n WHERE key(t) = n.rx` — keys and always-present columns | every `upd` row; the count must equal the plan's |
| K7, K8 | `INSERT INTO t SELECT n.key, CASE WHEN n.h THEN n.c ELSE t.c END ... FROM copy n JOIN t ON key(t) = n.rx`; `DELETE FROM t USING copy n WHERE key(t) = n.rx` | 3.4 only |
| K4.i | `UPDATE t SET c = sentinel(n.c) FROM upd n WHERE key(t) = n.x AND n.h AND need` | column i, if its type is affected (5.2) |
| K5.i | `UPDATE t SET c = n.c FROM upd n WHERE key(t) = n.x AND n.h AND ex(t.c) IS DISTINCT FROM ex(n.c)` | one per unchanged-possible column that some `upd` row supplies, and per always-present LIST/MAP/UNION column |
| K6 | `INSERT INTO t (cols) SELECT a<i> FROM ins` | complete final rows |

The full SQL of every shape for a sample table is in `evidence/p3/apply/shapes.json` (generated from 120 random
change sets; one text per shape). The number of shapes is at most `6 + (unchanged-possible columns) + (LIST/MAP/
UNION columns that cannot be unchanged) + (affected unchanged-possible columns)`: it depends on the columns only
(`Shapes.shape_bound`, asserted by `test_statement_shapes_depend_on_columns_only`). DELETE and INSERT from local
data run in slices of `ApplySettings.statement_rows` (default 1,000,000) rows: bounded hand-over (design 6.1);
the statement count grows, the shapes do not.

Why it is right, in order:

1. K1 removes every destination row whose key the change set touches, except roots of moved rows. After it, the
   keys of the change set hold nothing but roots.
2. K3 moves each root row to its final key and writes the key and every always-present column, in **one**
   statement, so a permutation of keys (deferred swap, rotation) is a parallel assignment: the join reads the
   rows before any is changed. Nothing unchanged is written, nothing is read into the client.
3. K5 writes, per column, the values the change set supplied, only for rows that carry one, and only where the
   value differs exactly. An unchanged value is never written (it is not in the SET list for that row) and never
   read into the client (SPEC 2.6).
4. K6 inserts complete final rows; their keys are free after K1.

Round trips (local form): a change set of complete images on a table with a key is K1 + K6 (insert-only: K6);
on a table without a key L1 + L2; a lookup is added only for keys of undetermined `p`; partial images add K3 and
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
another part changes (found by the review: K3 would set the other part and leave the stored `-0`).

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
(`ALTER TABLE ... RENAME COLUMN`, shapes R1 and R2), runs L1 or `reduce_duplicates` with the temporary name, and
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
   campaign, seeds 10002906, 10005444, 10005702; named cases `uuid_keys_in_one_transaction` and others). (A first fix cast DATE to
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
  (2.2), the root moved by K3 (3.2) or copied (3.4); chains, swaps through a third key, deferred swaps and
  rotations, a key change back to the same key and onto a deleted key, within one change set and across two, in
  both modes: named cases and the property tests.

## 7. Speed indications (not results)

`evidence/p3/apply/perf_md.json`: this laptop to MotherDuck over the internet, lineitem width, short runs. The
spike's numbers come from an F16 Flight (`docs/spike/md_semantics.md` Q3: 0.066 s for 1-100 rows, 212K rows/s at
100K, 342-355K rows/s at 1M) and are bound by round trips (8 ms on a Flight, about 80 ms here) and upload. See
the report for the comparison.

## 8. How to run

```sh
uv run -q pytest -q tests/apply                                                       # T0, a few seconds
CDC_V2_APPLY_EXAMPLES=800 uv run -q pytest -q tests/apply/test_apply_property.py      # more examples, local
CDC_V2_APPLY_FULL=1 uv run -q pytest -q tests/apply/test_apply_cases.py               # every case in both forms, local
CDC_V2_MD_PREFIX=cdc_v2_t_a_ tools/with-env uv run pytest -m md tests/apply          # MotherDuck
CDC_V2_MD_PREFIX=cdc_v2_t_a_ CDC_V2_APPLY_PERF=1 tools/with-env uv run pytest -m md tests/apply -k speed
uv run python tests/apply/apply_campaign.py --seconds 600                             # ledger, local
```

## 9. Evidence

| File | What |
|---|---|
| `duckdb_semantics.json` | section 5, local and MotherDuck side by side |
| `md_cases.json` | every named case on MotherDuck, both forms, with statements and seconds |
| `md_property.json` | property examples on MotherDuck |
| `campaign_local.json` | the opt-in campaign's ledger |
| `shapes.json` | the SQL of every statement shape for a sample table |
| `perf_md.json` | speed indications |

## 10. Not done, limits

* History tables and soft deletes (SPEC 8.1, 8.2) are P4. The plan computes, per record, its arrival and the
  final row of each key; a history table needs exactly that, from the same pass over the change set.
* The deferred-constraint ambiguity of 2.1 is a property of the stream.
* A key column sent as "unchanged" (a TOASTed key) cannot be applied: `ApplyInvariantError`.
* Speed is measured from this laptop only; Flight numbers are P9.
* The relation form sends about 40 statements per change set (one CREATE TABLE and one DROP TABLE per plan step;
  `md_cases.json`: median 41 against 2 for the local form). It serves oversized transactions and pending logs,
  where the rows dominate; merging steps would lower it.
* `join_wrap` covers DATE, TIMESTAMP and TIMESTAMP WITH TIME ZONE; the other timestamp precisions are not produced
  by C3 today.
* DuckDB errors (a value that does not fit a column, a constraint) pass through unchanged and can name a value.
