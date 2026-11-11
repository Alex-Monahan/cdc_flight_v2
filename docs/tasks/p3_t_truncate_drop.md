# Task: package T — tables that change: the catalog gate, truncate, drop and recreate, the machines document

Read `docs/tasks/README.md` first; its rules bind you.

Debezium names a table by schema and name only, and Postgres does not decode DDL. A table can be dropped,
created again under the same name, renamed, or lose and regain a column of the same name and type, and the
stream shows nothing, or shows rows that look like rows of the old table. You build the part of the pipeline
that knows **which table a row belongs to before the row is applied**, that makes the destination follow a
`TRUNCATE`, a `DROP TABLE` and a drop-and-recreate exactly, and you close SPEC 1.9: one document and one test
over every state machine of the pipeline.

Contracts: `docs/design/p3_plan.md` C0, C1, C3a, C4, C5, C7 (`catalog.observe`, `catalog.diff`,
`publication.ensure`), C9, C10 (the catalog gate; "the destination follows the source's names"), C11, and
section 6.2 "T". Design: `docs/design/core.md` sections 6.5, 7.2, 7.5 and 9. SPEC items you own the gate tests
of: **1.5**, and **1.9** as far as it spans all machines (every owner has already run the enumeration for its
own). Nothing you build may stand in the way of SPEC 2.1–2.4 (the schema wave of P4 changes tables in place)
or of SPEC 3.4.

## Read as well

- `docs/design/stream.md` (package S): groups and segments, the routing map, the table row and the stored
  identity, the seams `tables.confirm` and `tables.on_group` with their stand-ins, S's own schema rule (a
  `SCHEMA` frame that disagrees with the plan), the idle position step, the test helper that starts the local
  path, the settings loader.
- `docs/design/backfill.md` (package B): requests and how they are joined, the entry, the identity a plan
  records, what a backfill does when its table is gone, the swap, the table-status log, the request with `X`.
- `docs/design/source.md` (package P): what `catalog.observe` reads and how its bracket is proven on a standby
  and on a primary, `catalog.diff`, `publication.ensure` and `publication.state`, recovery conflicts.
- `docs/design/engine.md` (package E): the `TRUNCATE` frame, what the engine does on a Relation message with a
  new relation id, the start-up check of the effective Debezium configuration (`skipped.operations`).
- `docs/design/apply.md` (package A): `truncate(tx, target)` and segments. `docs/design/cut.md` (package X): what
  "the stream has passed a position" means. `docs/design/destination.md` (package D): lanes, DDL inside a
  transaction on MotherDuck, `state.declare`, the enumeration helper. `docs/oracle.md`.
- `docs/wal_hazards.md`: H-004 to H-008, H-014, H-015, H-016, H-022, H-023, H-030, H-034. Where the prevention of
  a hazard is a check of the catalog and P3 has no other owner for it, the check belongs in the observation you
  build. Say in your report which you took and which stay for P4 or P5.

## From the engine as built — build on it

Package E is built and reviewed. `docs/design/engine.md` is the full account; where it differs from what this task
says elsewhere, these points and that document hold.

- A TRUNCATE arrives as one `TRUNCATE` frame per table, in message order; children that a `CASCADE` reached arrive
  too when they are published. The `CASCADE` and `RESTART IDENTITY` flags are not carried.
- `skipped.operations=none` is enforced by the engine: it refuses to start with another value, so a truncate is
  never dropped silently. Each `TRUNCATE` frame counts one in `END.event_count`.
- A TRUNCATE of a table outside the include list inside a transaction that also touches included tables: the
  engine's document says what arrives; build on what it shows.

## From the source package as built — build on it

Package P is built and reviewed. `docs/design/source.md` is the full account; where it differs from what this task
or contract C7 says elsewhere, these points and that document hold.

- `catalog.observe` gives one observation of every configured table under one snapshot, with the position it
  covers; `catalog.diff` compares two. Build on both; add no catalog reader of your own.
- `TableFacts.identity` is the identity the gate compares (OID, columns by `attnum` with type and modifier,
  key). `relfilenode` is not part of it: a rewrite keeps the table. `published` includes membership through a
  partition ancestor.
- **Primary mode.** On a primary, an observation is usable for `group_end` only if `visible(xid)` holds for
  every transaction of the group (64-bit, widened by the observation's `xmax`) and S's `GapMark` at `group_end`
  admits its snapshot (contract C9). A transaction that holds only DDL carries no row, so the stream never sees
  it; one that is still waiting on a synchronous standby can be seen one group late. This is a stated limit of
  primary mode: write it into your document and into the list of residual cases, with the test that shows it.
- Publication membership is decided when a transaction is decoded: a transaction that wrote a table before it
  joined the publication and committed after is delivered with all its rows (`source.md` section 7).

## From the cut as built (package X)

`catalog.observe` is a cut. On a standby its bracket comes from the takers of `cdc_v2.pg.cut` with the connection's
settle floor, and `b ≥ group_end` is the `R0 ≥ P0` check. On a primary an observation is usable for `group_end` only
if the `GapMark` that package S hands you at `group_end` admits it (a DDL can be decoded before snapshots see it).
An observation made on a node other than the stream's is used only if `valid_on` holds. (Contract C9,
`docs/design/cut.md` 13.4.)

## What may be missing when you start

Packages S and B are merged, and with them X, E, D, A, P, Y, O and H. Packages V, L and F may or may not be.
You need none of the three; if F is merged, its DDL mix is one more workload for your gate test.

## Resources

- Postgres: `tools/localpg --home ~/.cache/cdc_flight_v2/pg-t --port-base 55816` (one home: create it again
  with `--pg-bin /opt/homebrew/opt/postgresql@16/bin` for the runs on Postgres 16). Tests that restart or
  reconfigure a node take the fixture `private_cluster`.
  For the shared fixtures: `CDC_V2_PG_HOME`, `CDC_V2_PG_PORT_BASE`, `CDC_V2_MD_PREFIX=cdc_v2_t_t_`.
- MotherDuck through `tools/with-env` only; databases `cdc_v2_t_t_*` only. The engine as package E starts it.

## What to build (`src/cdc_v2/tables/`)

### 1. Observations and identity

- An **observation** is one call of `catalog.observe(node)` (C7): the catalog facts of every replicated name,
  with the bracket (b, a). It **covers** a position G when `b ≥ G`: every DDL that committed up to G is in it.
  The stream has **passed** it when the applied position has passed `a`, by the comparison that C9 defines for
  the upper bound of a snapshot. Use that function; do not write a second comparison.
- Observe on the node the stream decodes on (the standby by default). On a primary the bracket is only as good
  as what packages P and X proved there; use their rule and say so.
- The **identity** of a table is what its destination table was built for: the OID, and per column `attnum`, name and
  type with its modifier, and the key columns. Package B records it with the plan and stores it at the swap; if
  something is missing there, add it through migration steps of your own. `relfilenode` is **not** part of it. As
  built by package K, `TableFacts.identity` also holds the replica identity, the identity index's attnums and
  `pk_deferrable` (`pg_index.indimmediate`): `confirm` answers `changed` when one of them changed, or when the
  catalog's `replica_identity_problem` is not `None`. A rename into a name with a `name_problem` (a `"` in the schema
  or table name, H-051) removes the table from the publication, as a drop.
- Write the list of commands that keep the identity (the gate must answer `same`: `TRUNCATE`, `VACUUM FULL`,
  `CLUSTER`, `REINDEX`, `SET TABLESPACE`, constraints, indexes that are not the key, owner, comment, statistics)
  and of those that change it (drop and create, rename of table, schema or column, `SET SCHEMA`, add or drop a
  column, a column dropped and added again with the same name and type, a type change, a change of the key).
  One test per command, on Postgres 18 and 16.
- The same observation shows what silently stops a table's changes: the table left the publication, became
  unlogged, lost the replica identity the pipeline needs, the publication got a column list or a row filter.
  Each is at least a finding with an alert (section 3); none may pass as `same`.
- Cost. Measure one observation with 1,000 replicated tables of 30 columns. If `catalog.observe` as delivered
  is too heavy to run once per group, add a lighter identity-only read **in package P's module** (one row per
  table), not a second catalog reader of your own. A recovery conflict reads again. A read that fails is never
  an answer.

### 2. The gate: `tables.confirm(group_end, tables, deadline) -> {table: same | changed}`

- Called by S for every group, beside the group's client-side work. For every table of the group whose routing
  is not `skip`: `same` only if an observation that covers `group_end` shows the stored identity; for a table
  that is backfilling, the identity of the live backfill's plan.
- The latest observation is used again when it covers the group; otherwise one is read: one round trip per
  group and no more. A node that has not yet replayed up to `group_end` is waited for, as a declared wait.
- **Never `same` by default.** When no covering observation can be had before the deadline, the call raises,
  the group is not applied, and S's rules for a failed call to the source decide what follows. Show with a TCP
  proxy that drops the catalog session that nothing is applied unconfirmed and that the stream goes on when the
  catalog can be read again.
- Measure what the gate adds to a group (p50, p99), locally and with 20 ms of delay on the catalog session.
- S's own schema rule stays as a second line behind the gate. A table the gate calls `changed` is handled by
  you alone in that group; make sure in S's code that its rule does not also enter a backfill at an earlier
  position. Say which changes only the gate can see and which only S's rule can see.

### 3. Findings: `tables.on_group(tx, group_end) -> routing changes`

Called by S in every group and every idle position step, immediately before `backfill.on_group`. A finding is
what an observation says about one replicated name: `dropped` (no table of that name), `recreated` (another
OID), `columns` (the same OID, another identity), `unpublished` (section 1, last bullet but one), `appeared` (a
configured name that exists at the source and has no destination table: in P3 only a name you dropped before).

**Step 1, in the first group after the observation, in `tx`.** The finding is recorded in a table of your own
in `_cdc` (kind, `b`, `a`, the identity seen, time). The table leaves the visible path: a state of the table
machine that says its identity is in doubt, routing `skip`, an alert. A live backfill of that table is abandoned
through package B's function. From this group on its rows are neither applied nor logged; count them.

**Step 2, in the first group at which the stream has passed the finding's `a`, in `tx`**, by the latest
observation (a newer observation replaces the finding and its `a`):

| Finding | Enacted |
|---|---|
| `dropped` | the destination table is dropped, or renamed under `on_source_drop: retain`; routing and stored identity removed; table machine → `dropped` or `retained` |
| `recreated` | as `dropped` for the old table, then as `appeared`, in the same transaction |
| `appeared` | a backfill request (after a gap) with the identity seen, and with `X` (design 7.5) when the table had to be added to the publication |
| `columns` | a backfill request (after a gap); the visible table stays until the swap (C10, the schema rule of P3) |
| `unpublished` | the table is added to the publication again; a backfill request (after a gap) with `X` |

- Why step 2 waits: a backfill entered at a `P0` before the DDL would log rows of the old table and replay them
  onto the new one. Past `a`, every later row belongs to the table the observation saw, or to a still newer one,
  which the next observation finds. Keep a negative control that enacts at step 1.
- Prepared outside, enacted inside, like package B: `tables.on_group` makes no call to the source.
  `publication.ensure` (it returns `X`) runs before, on the primary, from your component. If the process dies
  after it and before the request is written, the next attempt finds the table already published and takes a new
  `X`; a later `X` is always safe. Prove it at a crash point.
- `tables.on_group` keeps nothing between two calls that is not in MotherDuck or cannot be observed again: S
  runs a group again after `Conflict` or an unknown commit, and a new run starts with an empty memory. The
  first observation of a run finds what happened while the pipeline was stopped.
- **Idle tables.** A dropped table sends no more rows, so the gate alone never sees it. A poller observes every
  `tables.observe_interval` (default 5 s) as a component under the supervisor (C11). State and measure the
  bound: `DROP TABLE` at the source → destination table gone, under load and with an idle source.
- **`on_source_drop: retain`.** Per table, default `drop`; add it to S's settings loader,
  `docs/configuration.md` and `config/example.toml`. The retained table gets a name that cannot collide with a
  source table or an earlier retained copy (C0's rules on names and their length), receives nothing further, and
  its row says which stream position its content reflects: the last group for which the table was `same`. If you
  can prove for a case that the rows in doubt belong to the dropped table, you may apply them first; say which
  cases. Say, with a test, what a consumer's view over the table does across the rename.
- A rename is, in P3, a drop of the old name and, if the new name is configured, a fresh table (C10). Test
  `RENAME TO`, `SET SCHEMA`, and two names swapped in one transaction. The rename in place is P4; write down
  what P4 needs from your observation (the mapping by OID and `attnum`).
- Crash points after every durable step (`tables.finding_recorded`, `tables.finding_enacted`, and the
  publication step above), by the rule of C1.
- Declare the life of a finding as a machine with `state.declare`: this is the schema-change machine of design
  section 9 as far as P3 has it. Extend the table machine (package S's declaration, extended by B) with the
  states and transitions you enact.

### 4. Truncate, end to end

The pieces exist: the `TRUNCATE` frame (E), segments in a change set (C3a), `truncate(tx, target)` (A), the
`'t'` row of a pending log (B), S's order of segments. You prove the whole and close what is missing.

- The engine must refuse to start when the effective Debezium configuration skips truncates (H-016). If
  package E's start-up check does not cover it, add it there.
- On the local path, under a seeded workload, each followed by `compare(..., position=Q)`: `TRUNCATE t`;
  `TRUNCATE a, b`; `CASCADE` to a replicated table and to a table outside the publication (nothing arrives for
  it; say so in the document); `RESTART IDENTITY` followed by inserts that reuse the old key values; a truncate
  inside a transaction with inserts of the same keys before and after it; a transaction that truncates one
  table and changes others; a truncate in a transaction larger than a group (design 6.4); a table without a
  key; a truncate while the table is being read by a backfill, while it catches up, and in the group of its
  swap; a truncate of a table whose identity is in doubt (skipped with its rows).
- "In commit order within the same destination transaction as neighbouring changes" (SPEC 1.5): show it by
  comparing after every single group, with groups of one source transaction and with groups of many.
- Partitioned tables are refused in P3 (package B); `TRUNCATE` of a partition belongs to P4.

### 5. The machines (SPEC 1.9)

- Check the list of SPEC 1.9 and design section 9 against the declarations in the code: leader and run, slot and
  stream, table replication, backfill progress, schema change, batch commit and acknowledge, quarantine. Each
  must be declared once, changed only through `state.transition`, and logged. What is missing you declare, as
  far as P3 enacts it: the quarantine of a table (S's attempt counter and the state `quarantined`) must be an
  enumerated machine, or a part of the table machine that the document names as such. For the batch commit and
  acknowledge machine, which is one row per slot (plan 6.3): document it and make sure a test enumerates its
  transitions; if that needs a change in S's commit path, report it instead of making it.
- `docs/state_machines.md` (package D started it, every owner added its machine; you complete it): for every
  machine its purpose, its key, where the state is stored and with which data it commits, the states, the
  transition table (from, to, who, cause), and the transitions P4 will add. A default-run test compares the
  document with the declarations, so that the two cannot drift.
- One default-run test walks the registry and runs package D's enumeration helper for every declared machine:
  every pair of states, exactly the declared ones accepted. It fails when a machine exists that the document
  lacks. The leader machine's copy in `main.py` is compared as C5 says.
- No code writes a state column except through the transition function: a default-run test over the SQL in the
  source tree.
- `state.audit(conn)`: for every machine and key, the rows of `_cdc.transitions` form one chain, every step is
  legal, and the chain ends in the stored state. Call it at the end of every T1 test through package S's helper
  (so that the tests of B, V and F run it too) and after every kill at a crash point: a state and its log must
  agree after any crash. A failure it finds in another package's machine is a result for your report, with the
  seed.
- Report how fast `_cdc.transitions` grows on the local path; pruning is P4.

## Tests

**Default run (hermetic).** Identity and diff over the table of commands with scripted observations; the gate
against a scripted source (covers or not, a deadline that passes, a newer observation that replaces a finding);
`on_group` against a local DuckDB with crash points and repeated runs of one group; the machines tests of
section 5.

**Marker `localpg`.** Section 1 on real nodes, Postgres 18 and 16, on a standby and on a primary: each command
of the list, the bracket around a DDL (an observation that does not cover a group never answers for it), the
cost with 1,000 tables, a recovery conflict.

**Marker `t1` — the gate test of SPEC 1.5**, on the whole local path of package S's helper, with a seeded
workload that keeps writing to the other tables, the slot on the standby and once on the primary:

1. Section 4, every case.
2. `DROP TABLE`: under load and with an idle source; with rows of the table still in flight; inside a
   transaction with changes to other tables; of a table that is backfilling; `DROP SCHEMA … CASCADE`; with
   `on_source_drop: retain`.
3. Drop and recreate: in one transaction; in two transactions inside one observation interval (hold the poller
   at a fault site) and across intervals; with and without rows in between; with other columns and with
   identical columns; twice in a row; while the pipeline is stopped between two runs. The cases of H-007.
4. H-008: a column renamed, and a column dropped and added with the same name and type. Both end equal.
5. The pipeline killed at each crash point, at least 3 seeds each; the catalog session black-holed; the
   observed node restarted.

After each: `compare(..., position=Q)` is equal (a dropped table is absent on both sides; a retained table
equals the source table as it was at the position its row names); `state.audit` passes; the table-status log
shows when the table left the guarantee and when it came back; the stream did not stop and no operator acted.

**Negative controls**, kept as tests, each caught by a test above: a gate that always answers `same` (drop and
recreate with identical columns); an observation that does not cover the group used for `same`; an identity
without `attnum`; enactment at step 1; a truncate applied after the other changes of its group instead of in
order; `relfilenode` taken into the identity (a `TRUNCATE` must not cause a backfill).

## Deliver

- `src/cdc_v2/tables/`, your migration steps, the settings, the tests, `state.audit` wired into the helper.
- `docs/design/tables.md`: identity and the two lists of commands; observation, covers and passed; the gate and
  its cost; each finding from detection to enactment with its bounds; retain; truncate; each crash point and
  what the next run does after it; contract notes; what the schema wave of P4 needs from this code.
- `docs/state_machines.md`.
- Evidence under `evidence/p3/tables/`.
- In your report: every way you found to change a table that the observation does not see; what `state.audit`
  found in other packages; what packages F and V must know.
