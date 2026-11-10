# Task: package K — the key rule

Read `docs/tasks/README.md` first; its rules bind you.

You make rule K true in the code. Rule K (`docs/design/core.md`, start of section 6) says which tables have a
key: a table has a key when its primary key is not deferrable (`pg_index.indimmediate`) and its replica identity
is `DEFAULT`, `FULL`, or `USING INDEX` on the primary key's own index. Every other table that can be published is
a table without a key and needs `REPLICA IDENTITY FULL`. The catalog (`cdc_v2.pg.catalog`) is the one authority;
the engine is told which tables are without a key.

Why the rule exists: `docs/wal_hazards.md` H-015 and `tests/pg_source/test_pg_keys.py` (measured on Postgres
16.6 and 18.6). Only a table with a deferrable primary key and `REPLICA IDENTITY FULL` can hold two rows with one
key inside a transaction, and its decoded stream shows that state. Today the catalog calls such a table keyed,
and so does the engine (`tests/engine/test_engine_stream.py`, table `t2`). Both are wrong under rule K.

This is a small package. Keep it small: no new module, no new abstraction.

## Read as well

- `docs/design/source.md` sections 2 and 3 (catalog, publication, replica identity) and `docs/design/engine.md`
  (settings, the `SCHEMA` frame, the columns `a<i>`, `b<i>`, `u<i>`, `k<i>` of a `ROWS` frame, key changes).
- `src/cdc_v2/pg/catalog.py` (`_KEYS_SQL`, `SourceTable`, `identity_problem`, `shape_hash`, `TableFacts`,
  `_OBSERVE_SQL`), `src/cdc_v2/pg/publication.py` (`check`), `src/cdc_v2/pg/replica_identity.py`.
- `src/cdc_v2/stream/engine.py` (`EngineSettings`, `PROTECTED_KEYS`, `build_properties`) and
  `jvm/src/cdc_v2/engine/CdcEngine.java` (`layout()`, `tableFor`, `fill`, the `__debezium.oldkey` header).
- Plan section 6.2, "Rule K": what packages S, Y, B and T will do with what you build.

## Resources

- Source tests: the homes of package P, `tools/localpg --home ~/.cache/cdc_flight_v2/pg-p --port-base 55640`
  (Postgres 18) and `--home ~/.cache/cdc_flight_v2/pg-p2 --port-base 55648` (Postgres 16, through
  `CDC_V2_P_PG_MAJOR=16`; never both versions at once). See `docs/design/source.md` section 10.
- Engine tests: the home of package E, `--home ~/.cache/cdc_flight_v2/pg-e --port-base 55620`. JDK and jars only
  as locked (`jvm/jdk.lock.json`, `jvm/debezium.lock.json`). Debezium is used unmodified (SPEC R2).
- No MotherDuck. Other workers run at the same time; keep the JVM heap at 1 GB or less.

## What to build

### 1. The catalog follows rule K

- `_KEYS_SQL` and `_OBSERVE_SQL` read `pg_index.indimmediate` of the primary key.
- `SourceTable` says whether the primary key is deferrable and has one property that answers "does this table
  have a key" by rule K. For a table without a key, `key` and `key_attnums` are empty and `key_source` is
  `None`. A table that `check` refuses anyway may keep what it reports today.
- `identity_problem`: a table with a deferrable primary key that is not `REPLICA IDENTITY FULL` is refused, with
  a message of its own ("primary key is deferrable: needs REPLICA IDENTITY FULL (replica identity is …)"). The
  other refusals stay as they are, among them a primary key with an identity index that is another index.
- `shape_hash` changes when a table gains or loses its key by rule K (also when the key columns stay the same).
  `TableFacts` carries the same fact, and its `key_attnums` follows the rule.
- `publication.check`, `publication.ensure` and `replica_identity.set_full` work for a table with a deferrable
  primary key: `set_full` then `ensure` publishes it, and the source's own `UPDATE` and `DELETE` keep working
  (H-014).
- Measure, on both versions, how a primary key can become deferrable or immediate on an existing table (`ALTER
  TABLE … ALTER CONSTRAINT`, or only drop and add), and what the catalog reports before and after. Package T
  needs the answer.

### 2. The engine is told which tables are without a key

- `EngineSettings` gets a field that lists tables without a key, as exact (schema, table) names: no patterns.
  A name with a dot, a comma, a quote or upper-case letters must work. It travels as one `cdc_v2.*` property.
- In `CdcEngine`, a listed table is a table without a key whatever Debezium says: `has_key` is 0 in its `SCHEMA`
  frame, it has no key positions, its rows carry the old image in `b<i>` and no `k<i>`, and the old-key header
  is ignored. A table that is not listed is treated as today (Debezium's key decides).
- An update of a listed table that changes a primary key column: measure what Debezium emits (one update, or a
  delete and a create), and what the engine sends. Both shapes are correct for a table without a key as long as
  the old image and the new image are complete; write down which it is, because the changelog of P4 shows it.
- A `u` or `d` of a table without a key that arrives without a complete old image stays an engine error, as
  today.
- The list is read at start. A table that appears later (SPEC R20) is treated by Debezium's key; the consumer
  (package S) compares each `SCHEMA` frame with the catalog and restarts the engine with a new list when they
  differ. You do not build that comparison; you make sure the frame carries what it needs.

### 3. Measure what Debezium calls a key

Through the real engine, with `REPLICA IDENTITY FULL` in each case, and without the list:

1. a table with a deferrable primary key (today's `t2`);
2. a table with no primary key and a unique index on `NOT NULL` columns;
3. the same with a nullable column in the unique index;
4. a table with no primary key and a deferrable unique constraint;
5. a table with no primary key and no unique index (the control).

For each: the `has_key` flag and key positions of the `SCHEMA` frame, and the rows of one insert, one update of a
non-key column, one update of the "key" column and one delete. Then the same five with the list: all five are
tables without a key, and for case 1 the direct swap and the "insert a duplicate, then delete the old row"
transaction of `tests/pg_source/test_pg_keys.py` arrive as records whose old and new images are complete and in
WAL order.

Any case where Debezium gives a key and the catalog says "no key" must be named in `docs/design/engine.md`,
because the list is what makes the two agree.

### 4. Tests

- Default run (hermetic): `identity_problem` and the rule-K property over every combination of (primary key:
  none, immediate, deferrable) × (replica identity: default, index on the key, other index, full, nothing);
  `shape_hash` differs between an immediate and a deferrable key on the same columns; `build_properties` writes
  the list and refuses a list that an `extra` setting tries to override.
- `localpg`, in `tests/pg_source` (both versions): the catalog, `check`, `set_full` and `ensure` on real tables
  of the shapes above; the observation (`TableFacts`) agrees with `SourceTable`.
- `jvm and localpg`, in `tests/engine`: section 3. Change the `t2` case of
  `tests/engine/test_engine_stream.py`: with the list it is a stream without a key; keep one assertion that
  documents what the engine sends without the list.
- Negative control: with the list left out, the test that expects a stream without a key for case 1 fails.
  Show it once and say so in the evidence.

### 5. Documents and evidence

- `docs/design/source.md` section 3: the rule, the new refusal, and remove the sentence "Open until package K
  is merged". `docs/design/engine.md`: the setting, what a listed table's frames look like, the table of
  section 3. In `docs/wal_hazards.md` you may add test names to the "Tests" line of H-015; change nothing else
  there.
- Evidence under `evidence/p3/key_rule/`, redacted. The secret scan rejects some harmless names: a long token
  with upper case, lower case and digits can look random to it. Choose plain lower-case file names with few
  digits, and never bypass the scan.

## Not yours

The applier (package A), the stream consumer and its restart (S), the plan check of the transform (Y), history
(P4). `docs/design/core.md`, `docs/design/p3_plan.md` and `docs/tasks/` are the orchestrator's. If rule K as
written cannot work (for example Debezium cannot be made to send a complete old image for a listed table), stop
that part and report the case with its evidence.

## Deliver

Your branch, pushed, and a report of at most 500 words: what each of the five cases of section 3 gave with and
without the list; how a key can become deferrable on an existing table; what changed in `SourceTable` and
`TableFacts` (names of fields and properties, for the packages that read them); the counts of each suite on each
Postgres version; the state you left the machine in.
