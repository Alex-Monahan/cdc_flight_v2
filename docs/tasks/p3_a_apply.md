# Task: package A — apply

Read `docs/tasks/README.md` first; its rules bind you.

You build the code that turns a change set into the statements that change one destination table, inside a
transaction that someone else opened and will commit. Contract: `docs/design/p3_plan.md` C0, C3a (and C4 for the
`tx` you receive). Design: `docs/design/core.md` section 6. SPEC items that this package decides: 1.3 and 1.4
(what a group does to a table), 2.6 (TOAST handled efficiently), the keyless half of 1.1, and the cost side of 5.1
and 5.2.

## Read as well

- `docs/design/core.md` sections 2, 3, 6, 7.3 and 11 (open points 2, 3, 9, 10 are on your path).
- `docs/spike/md_semantics.md`: Q2, Q3, Q4, Q7, the F16 follow-up and findings 2, 5, 6, 7, 10 (what the apply
  shapes cost on MotherDuck and which conflicts are not detected).
- The oracle (package O): `docs/oracle.md` and `src/cdc_v2/oracle/` (`canonical.py` holds the rules). The
  canonical form in which the oracle compares values is the form your keyless comparison must agree with (open
  point 3, settled by the oracle: a UNION value compares by member and by the rule of the member's source
  family; NaN equals NaN; `-0` and `0` are different; a UNION whose member holds NULL is **not** NULL in DuckDB,
  so the pipeline writes a real NULL for a source NULL). If the two cannot agree, stop and report.
- The oracle's findings about DuckDB that decide your statements — read `docs/oracle.md` (findings) and
  `evidence/p3/oracle/duckdb_equal_update.json` before you write any SQL:
  1. **An `UPDATE` whose new value compares equal to the stored one is not written.** After `SET x = 0` a stored
     `-0` is still `-0`; after `SET i = INTERVAL '30 days'` a stored `1 mon` is still `1 mon`; local DuckDB and
     MotherDuck alike. `DELETE` followed by `INSERT` always writes. The source tells these values apart, the
     oracle does too, so an apply that leaves the old one in place is a wrong result.
  2. **`GROUP BY`, `PARTITION BY`, `DISTINCT` and `=` merge values that are different in the source**: `-0` and
     `0`, NaN and −NaN, `1 mon` / `30 days` / `720 hours`. There may be more types like this (time with zone,
     strings under a collation, structs and lists that contain such values): find the full list for the
     destination types of C3 and write it down.
- `docs/wal_hazards.md` H-003 and the hazards on key changes and keyless tables.

## Resources

MotherDuck through `tools/with-env` only; databases `cdc_v2_t_a_*` only. No Postgres, no JVM: your inputs are
change sets that your tests build. The account is shared: keep performance runs short and say what ran beside
them.

## Interfaces

```python
meta = TableMeta(columns=[Column(position, name, type, key, unchanged_possible), ...])   # frozen; has_key derived
target = Target(schema, table)                    # destination identifiers; you quote them, always
apply_group(tx, target, change_set, mode="stream") -> ApplyCounts     # mode: "stream" | "catch_up"
truncate(tx, target)
```

- `change_set` is as C3a, in one of two forms. Local form: `rows` (Arrow, with `txn`), the `transactions` table
  (`xid` as an integer, `commit_lsn`, `commit_ts`) and `meta`; order (`txn`, `seq`). Relation form: a `SELECT`
  (text and parameters) over rows that are already in MotherDuck, with `commit_lsn`, `xid`, `commit_ts` in place
  of `txn`, and `meta`; order (`commit_lsn`, `seq`). `seq` alone orders nothing across transactions: it starts at
  0 again with every engine instance, and a pending log can hold rows of several. A row with `op = 't'` never
  reaches you; the caller cuts its log into segments there.
- Rows are whole source transactions and are **not reduced**: you reduce. Columns are named by position (`a<i>`,
  `b<i>`, `u<i>`, `k<i>`), never by source name; the destination column names come from `meta` and appear only
  as quoted identifiers of the target.
- `tx` is an open lane transaction (C4). You execute statements on it and never begin, commit, roll back or
  fence. Until package D is merged, build against a minimal stand-in with the same `execute` shape over a plain
  DuckDB connection; keep it in your tests, not in `src/`.
- `ApplyCounts`: rows deleted, inserted and updated in the destination, records read, records by `op`, the
  number of statements executed, and `unresolved` (below).
- Destination column names are compared without regard to case (DuckDB does). `TableMeta` refuses two columns
  whose names differ only by case, and a column whose name collides with nothing of ours can be any string at all:
  quotes, spaces, dots, keywords, non-ASCII, empty-looking names. Test that.

## What it must do

**Tables with a key** (design 6.2). The result must equal applying every record of the change set one by one, in
the order of the change set, to the destination table as it was before:

- `c`: the row with that key becomes the new image (replacing a row that is there).
- `u`: the row with that key takes the new values; a column flagged in `u<i>` keeps its value — from the nearest
  earlier record of the same row in the change set that had one, else from the destination row.
- `d`: the row with that key is gone; no row there is not an error.
- Key change: a `d` of the old key and a `c` of the new key with `k<i>` set to the old key. Columns flagged
  unchanged on that `c` take their values from the row that had the old key — again first from earlier records
  in the change set, else from the destination. Chains (A→B, B→C), swaps (A↔B through a third value or deferred),
  a key change back to the same key, and a key change onto a key that another record of the set deleted, all
  inside one change set and across two change sets.
- Where an unchanged value comes from, in this order (C3a): (1) the nearest earlier record of the same row in
  the change set, following key changes; (2) the destination row that has the key the row had before the change
  set; (3) in mode `catch_up` only: the destination row with the row's new key, if no earlier record of the
  change set addressed that key. If none gives the value, the key is **unresolved**. Never invent a NULL.
  - Mode `stream`: an unresolved key raises `ApplyInvariantError`.
  - Mode `catch_up` (a log replayed onto a table that the backfill loaded under several snapshots, design 7.3):
    the records of unresolved keys are not applied, the rest is, and the keys come back in
    `ApplyCounts.unresolved` (key columns, on the client: the backfill reads these rows again from the source).
    In this mode the table may hold no row for a key of the log although the log only updates it, and a row for
    a key under its new name already; neither is an error.
- Mode `catch_up`: applying the same change set twice gives the same table as applying it once, except for keys
  returned as unresolved. Test it with key-change chains, with a key that another record deleted first, and with
  every split of a log into two calls. Mode `stream` makes no such promise.
- Several rows with one key in the destination (possible after loading under several snapshots) are not yours to
  repair, but say in your document what your statements do when they meet them, and offer the backfill a
  statement that reduces them to one row per key for the keys of a change set (design 7.3, step 1).
- SPEC 2.6: an unchanged value is never written to the destination and never read into the client; no statement
  per row; the number of statement shapes depends on the table's columns only, not on the data or on which
  columns are unchanged in which row. Write the shapes down and count them in a test.
- Every value that a record changes is really written (finding 1 above). Design 6.2 applies partial images with
  `UPDATE … FROM net`: that statement, as it stands, loses a change from `-0` to `0`, or between two equal
  intervals. Find the shape that always writes and still does not rewrite the unchanged columns — or show that
  there is none on MotherDuck and say what the cheapest correct shape costs (for example: rewriting the row on
  the server from the old row and the record, only for rows where a changed column has one of the affected
  types). Test it for every affected type, as a complete image and as a partial image, in both modes, on
  MotherDuck.

**Tables without a key** (design 6.3). Every record carries the old image (`b<i>`) and the new one (`a<i>`);
where `u<i>` is set, the new value equals the old one. The table is a multiset: the result holds each distinct
row image as many times as the old count plus new images minus old images.

- Deleting k of n identical rows deletes exactly k (open point 2: decide how — `rowid`, or rewriting the group of
  identical rows — and prove it on MotherDuck, also when the same transaction inserted or deleted rows of that
  image just before).
- Fewer rows than must be removed: `ApplyInvariantError`, nothing ignored; the caller rolls back.
- Equality of images is exact and total, and it is the oracle's: NULL equals NULL, NaN equals NaN, −0 and +0
  are different, `1 mon` and `30 days` are different, values of UNION columns compare by member and value, nested
  lists and structs compare element by element. Plain `GROUP BY` / `PARTITION BY` over the columns does **not**
  give this (finding 2 above): `delta` and the choice of rows to delete need a grouping key per column that keeps
  these values apart and still treats NULL as equal to NULL. An image `-0` must never delete a row `0`, and a
  record that changes `-0` to `0` must not net to nothing. No value is compared through a lossy cast. State which
  destination types cannot be compared exactly, if any, and what you do for them.

**Both.**

- `net` and `delta` are computed on the client, once per table and change set; from them you know locally which
  statements have work to do, and you send only those. For a small change set the number of round trips is what
  costs (spike: a few round trips per transaction); count and minimise them.
- A change set in relation form (rows already in MotherDuck: a transaction larger than a group, 6.4, or a
  pending log) runs the same logic without bringing the rows to the client; only unresolved keys and counts come
  back.
- Local Arrow data is handed over in bounded batches (design 6.1); client memory for one call is bounded by the
  change set plus a constant.
- `truncate(tx, target)` empties the table inside the transaction.
- The records of one change set carry their transaction. For the current-state table you need it for the order
  only.
  History tables and soft deletes (SPEC 8.1, 8.2) are built in a later wave from the same change set: do not build
  them, and do not shape `apply_group` so that they would need a second pass over the source data.

## Tests

- A reference model in plain Python (a dict for keyed tables, a multiset for keyless ones) that applies records one
  by one. Property-based tests (`hypothesis`) generate tables, change sets with every operation, key changes,
  unchanged flags, duplicates, NULLs and special values, split them into one or several change sets, and compare
  the model with the destination after `apply_group`. They run on local DuckDB in the default run (bounded
  examples, a few seconds) and on MotherDuck under the marker `md` with more examples; a long opt-in campaign
  writes a ledger (seed, counts, outcome) to your evidence.
- Named regression cases for every item in the lists above, and one for each failure the property tests find.
- Type coverage: at least integers, `DECIMAL`, `FLOAT` and `DOUBLE` with NaN, −NaN, ±infinity and −0,
  `INTERVAL` with equal values of different form, `VARCHAR` (empty, zero byte if the destination holds it,
  long), `BLOB`, `DATE`/`TIMESTAMP`/`TIMESTAMPTZ` with infinity, `TIME` and time with zone, `UUID`, `JSON`,
  lists, structs, and a `UNION` column (also with a NULL member value and as a real NULL) — as key columns where
  the destination allows it, and as values. The model of the property tests compares with the oracle's rules
  (`cdc_v2.oracle.canonical`), not with Python's `==`.
- Statement shapes and round trips counted and asserted.
- Indications of speed on MotherDuck (short): a small change set (1–100 rows; the spike's number is 0.066 s for the
  whole transaction), 100K and 1M rows, keyed and keyless, with and without unchanged columns; compare with the
  spike's numbers and explain a difference.

## Deliver

- `src/cdc_v2/apply/`, the tests, `docs/design/apply.md` (the statements for each case, the shapes, why they are
  right, the answers to open points 2, 3 and 9 as far as they are yours, contract notes), evidence under
  `evidence/p3/apply/`.
- In your report: anything in C3a or design section 6 that should change, and every MotherDuck behaviour that
  differed from local DuckDB.
