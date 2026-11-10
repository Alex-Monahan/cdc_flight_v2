# Task: package B — backfill

Read `docs/tasks/README.md` first; its rules bind you.

A table gets into the destination, and gets replaced there, by **one** mechanism: the first snapshot, a table
added later, an operator's re-backfill, the automatic re-backfill after a slot finding or a quarantine, the
backfill that P3 uses for a schema change, and the continuation of any of these after a crash. You build it. The
stream never stops for it, a consumer never sees a missing, empty or partial table, and when a backfill is done
the table equals the source at the stream's position — with writers running the whole time.

Contracts: `docs/design/p3_plan.md` C0, C1, C3, C3a, C4, C5, C7, C8, C9 (the cut), C10, C11, and section 6.2
"B (backfill)". Design: `docs/design/core.md` section 7 (all of it), 6.1 to 6.3, section 9 (the table
replication machine and the backfill progress machine), open points 5, 10, 11 and 12. SPEC item you own the gate
test of: **1.6**. Items your code decides or must not block: 1.3 (the table-status log), 1.5 and 1.8 (their
answers end in your backfills), 1.7, 3.1, 3.2, 3.3, 3.4, 3.5, 3.6, 3.7, 4.0, 5.2. A second reviewer attacks your
protocol with cases of its own.

## Read as well

- `docs/design/cut.md` (package X) and plan C9: the cut as it was proven — its interface, the rule on a standby
  and on a primary, what the stress test measured. Build on it; do not derive it again. Where this task and C9
  as merged disagree, C9 wins; say so in your report.
- `docs/design/stream.md` (package S): the routing map, the request table, `backfill.on_group` and its stand-in,
  `stream.reposition`, `stream.applied_position`, the table machine's declaration, the schema-change rule of C10,
  the idle position step, the helper that starts the local path.
- `docs/design/apply.md` (package A): `apply_group` in mode `catch_up`, the relation form, `ApplyCounts.unresolved`,
  the statement that reduces several versions of a key, `truncate`, the round trips.
- `docs/design/destination.md` (package D): lanes `backfill.<i>`, the run-again rule, `execute_one`,
  `CommitUnknown`, `Conflict` after a DDL, migrations, `state.transition`, alerts.
- `docs/design/source.md` (package P): `pg.connect` and the fixed session settings, the error taxonomy (recovery
  conflict), `catalog.tables` (identity, `relfilenode`, key, row-level security), `publication.ensure` and the
  transaction id it returns.
- `docs/design/transform.md` (package Y): `transform.plan`, `transform.backfill`, the text form it expects,
  thread safety.
- `docs/spike/local_throughput.md` (the reader's shape, the rules of its section 8, the measured rates),
  `docs/spike/md_semantics.md` (DDL inside a transaction, rename, conflicts between connections, what another
  connection sees), `docs/oracle.md`, `docs/testing/harness.md`.
- In `docs/wal_hazards.md` every hazard about backfill reads and snapshots: H-002, H-013, H-021, H-030, H-031,
  H-039, H-044, H-046, H-048. Where a hazard says **[test]** and the fact decides a backfill read, the test is
  yours unless a merged package's document already answers it; then cite it.

## From the engine as built — build on it

Package E is built and reviewed. `docs/design/engine.md` is the full account; where it differs from what this task
says elsewhere, these points and that document hold.

- Rows of a table that is being backfilled arrive in ordinary `ROWS` frames. Package Y's cross-path test is the
  guarantee that the text of a chunk and the transport forms of the stream give equal destination values; a type
  that fails it is a finding for Y, not something to convert a second way here.
- A table that the stream quarantined because the engine cannot read one of its values (package S) is backfilled
  "after a gap": its `P0` and its hand-off follow the same rules as any other gap.

## From the source package as built — build on it

Package P is built and reviewed. `docs/design/source.md` is the full account; where it differs from what this task
or contract C7 says elsewhere, these points and that document hold.

- Every read goes through `pg.connect` with the read settings (`source.md` section 1): they pin the text form
  that package Y's `transform.backfill` expects (`IntervalStyle=iso_8601`, `DateStyle=ISO, YMD`,
  `extra_float_digits=3`, `bytea_output=hex`, UTC, `lc_monetary=C`). Never set a session setting of your own on a
  read connection, and never read through a connection that was not opened this way.
- A raw `COPY` runs inside `conn.guard`, so that the deadline closes the socket and server errors arrive as the
  package's error classes. `RecoveryConflict` (a standby cancelled the read) means: read the chunk again. It is
  not a failure of the table and does not count towards quarantine.
- `TableFacts.key_attnums` never include the `INCLUDE` columns of an index. Chunk ranges and repair reads use
  exactly these columns.
- `relfilenode` is not part of a table's identity (a rewrite changes it and keeps the table); check it per chunk
  as section 4 says, with the catalog reader of package P.
- On a source of Postgres 17 or later the backfill reads `infinity` and `-infinity` in interval columns as they
  are. The stream cannot (plan C3, "Ambiguous zero interval"); nothing follows for you in P3.

## What may be missing when you start

Packages X, E, D, A, P, Y, S and O are merged. Package V (slot continuity) may or may not be: its finding reaches
you only through `stream.reposition`. Package T (truncate and drop) comes after you: a dropped and recreated
table reaches you as an ordinary request. Package L may or may not be merged; for a hand-over use what package
S's helper offers.

## Resources

- Postgres: `tools/localpg --home ~/.cache/cdc_flight_v2/pg-b --port-base 55800`, and a second pair
  `--home ~/.cache/cdc_flight_v2/pg-b2 --port-base 55808` (Postgres 16 through `--pg-bin`). Tests that
  reconfigure a node, pause replay, fill WAL or need a second standby take the fixture `private_cluster`.
  For the shared fixtures: `CDC_V2_PG_HOME`, `CDC_V2_PG_PORT_BASE`, `CDC_V2_MD_PREFIX=cdc_v2_t_b_`.
- MotherDuck through `tools/with-env` only; databases `cdc_v2_t_b_*` only.

## What to build (`src/cdc_v2/backfill/`)

### 1. Requests and the entry

- A request is a row in the request table (package S's; C10): the table, the cause, whether it is *after a gap*
  or *without a gap* (design 7.1), and for a table that has just joined the publication the transaction id `X`
  of design 7.5. Whoever asks writes it, in any lane. You own what the rows mean: several waiting requests for
  one table are one backfill; a backfill serves every request written before it was entered; a request written
  later for a table that is backfilling **abandons** that backfill and enters a new one.
- **Prepared outside, enacted inside.** The enactment runs inside a stream-lane group transaction and makes no
  call to the source. What it needs from the catalog (the table's identity, the transform plan) is prepared
  before, by your component, and checked inside: the plan must be the one the stream uses for the table at that
  moment, else the request is prepared again.
- The enactment, in the group's `tx` (inside `backfill.on_group`): the backfill's row with `P0` = the group's
  end; the new table, built from the plan; the pending log; the routing (C10: after a gap the visible table is
  left as it is, or does not exist yet; without a gap it keeps receiving the changes); the table machine's
  transition to `backfilling`. `stream.reposition` enacts through the same function.
- From that group on, `on_group` appends the table's change sets to the pending log in the same `tx` that moves
  the position: both forms of C3a (a change set in relation form — an oversized transaction — with one
  `INSERT … SELECT`, no row comes to the client), a truncate as a row with `op = 't'`.
- With no backfill active and no request waiting, `on_group` adds **no round trip** to a group. A hint in memory
  may say "look"; the truth is always what `tx` reads. A request written by another session is found within a
  few seconds, also while the source is idle (S's idle step); measure request → entered.
- `on_group` keeps nothing between two calls that is not in MotherDuck: S runs a group again after `Conflict`
  or an unknown commit, and the second run must do the same thing.

### 2. Where the new table lives

The new table must become the destination table `s.t` by a rename inside the swap transaction; no row is copied
there. Find out on MotherDuck what that allows. If a table can only be renamed within its schema, the new table
lives in the destination schema under a reserved name that starts with `_cdc`, and C0 gets one more sentence
(such names in a replicated schema are ours; a source table with such a name is not replicated in P3). If it can
be moved between schemas inside a transaction, it lives in a `_cdc` schema. Decide with a test, use the form of
the swap that `docs/spike/md_semantics.md` proved, and tell the orchestrator which it is: the oracle and package
F's consumer loop need to know what a consumer may see.

### 3. Chunk plan and progress (SPEC 3.7)

- Everything a resume needs is in MotherDuck, in `_cdc` (your migration list): one row per backfill (this row is
  the backfill progress machine), one row per chunk (range, state, rows, bytes, the snapshot's cut, the
  `relfilenode` it saw, lane, times). Local disk holds nothing a resume needs.
- Chunks are `ctid` ranges by pages, sized from the table's size so that a chunk is about `backfill.chunk_bytes`
  (take the default from the spike). The last chunk has no upper bound. An empty table is one open-ended chunk.
  The plan is written in one transaction, so a table is either planned or not.
- Memory is bounded whatever a chunk holds: pages say nothing about TOASTed values. Show it with rows that carry
  100 MB values.
- The plan records the identity the new table is built for: the table's OID, per column `attnum`, name and type
  with its modifier, and the key columns (C10, the catalog gate). Every chunk's snapshot must show that identity,
  and the swap stores it as the table's identity, in the swap transaction. Package T compares it with the
  catalog for every group.
- A partitioned table is read leaf by leaf in the Postgres-features wave of P4. In P3 the planner refuses it
  with a report, before anything is created.
- A view for an operator: per table the state, chunks done and total, rows, rate, the positions.

### 4. Read and load

- A reader has its own connection (`pg.connect`) to the configured backfill source: by default the replica that
  holds the slot; another replica or the primary are settings. Use the primary only if C9 as merged proves the
  cut there; otherwise refuse the setting with a report.
- `COPY (SELECT <columns of the plan> FROM ONLY t WHERE ctid >= … AND ctid < …) TO STDOUT`, in the form the
  spike found fastest, into Arrow without Python code per row, then `transform.backfill`, then the destination.
  Prove the framing: NULL against the empty string; newlines, delimiters, quotes, backslashes and `\.` inside
  values; every byte sequence the encoding allows; a 100 MB value. Prove with `EXPLAIN` that a chunk is a TID
  range scan and never a scan of the whole table.
- Every chunk is read in its own short transaction (design 7.1): repeatable read, read only, the cut taken as C9
  says, the plan's identity and `pg_relation_filenode` read under the same snapshot (a chunk never trusts the
  name alone: the table may have been dropped and created again). A snapshot is used only if `R0 ≥ P0`; the wait for
  it has a deadline and is a declared wait (C11), it does not spin and does not read. A recovery conflict
  (SQLSTATE 40001 and what package P maps to it) reads the chunk again.
- **A table with a key**: every chunk has its own snapshot and its own cut; nothing is held between chunks.
- **A table without a key** (rule K: also a table whose primary key is deferrable, and a table with only a unique
  index): one snapshot for the whole table, exported by a connection that never touches the table (it may still be
  split into chunks by `pk_attnums` when it has a primary key, all read under that one snapshot); before anything is
  read, the stream must have passed the snapshot and no transaction that touches the table may be `unknown` (design
  7.4, C9); else another snapshot. `on_group` sees every transaction of a group; decide with `docs/design/cut.md` how
  the stream side of the cut is recorded. A table under a steady flow of small transactions must still get its
  snapshot: measure how many attempts it takes at the write rate of the gate test, and say what happens when none
  succeeds (an alert, a declared wait; never a read under a doubtful snapshot). The exporter's age is reported and has
  an alert threshold. A lost snapshot discards what was loaded and starts the table again: the known limit for SPEC
  3.7, to be shown as a measurement.
- A role that is subject to row-level security on the table is refused before the first read (H-039).
- **Load.** A load transaction covers one or more chunks, in the loader's lane `backfill.<i>`: fence, rows into
  the new table, the chunks moved to "loaded" with `execute_one` under the condition that the backfill is still
  the live one, commit. So a chunk is loaded at most once, and the loader of an abandoned backfill or of a
  deposed run writes nothing. Crash points before the commit (run with `kill_later`) and after it
  (`backfill.chunk_committed`). `CommitUnknown`: read the chunk's state in a new transaction and go on from it.
- Readers run in parallel, within a table and across tables: one pool (`backfill.readers`) over the chunks of
  all backfilling tables, so that a small table does not wait behind a large one. Back-pressure when the
  destination is slower than the source. Prove on MotherDuck that parallel loaders (appends to one table,
  updates of different chunk rows) do not conflict, or measure how often and show that the run-again rule
  absorbs it.
- A failure that belongs to one table (a value that cannot be converted, a permission, a table that keeps being
  rewritten) is retried `backfill.max_table_attempts` times; then the table is quarantined with an alert (SPEC
  4.0) and the other tables go on.

### 5. Catch up

- For a table with a key, catch-up starts when all chunks are loaded, and replays the log from the smallest `R0`
  of the table's snapshots; `unknown` counts as `outside`. For a table without a key the log is replayed from
  exactly its cut.
- Rounds, in a `backfill.<i>` lane: one transaction reads the applied position and takes the log rows up to it
  (never past it: the stream lane is appending), reduces several versions of a key (design 7.3, package A's
  statement), calls `apply_group(tx, new_table, change_set, mode="catch_up")` with the log in relation form, and
  moves `caught_up_lsn` with `execute_one`. Rows with `op = 't'` cut the log into segments, with `truncate`
  between them. A crash point per round. **Open point 5 first**: before anything depends on it, prove on
  MotherDuck that appends by the stream lane and reads by a backfill lane on one log neither conflict nor show a
  round a torn group.
- **Repair read.** Keys that `apply_group` returns as unresolved are read from the source by key, under a new
  snapshot with `R0 ≥ caught_up_lsn` (a declared wait), one statement per round, loaded like a chunk, counted
  towards the bound of SPEC 3.7. The key values go back to the source as parameters, never as SQL text. This
  needs the exact way back from a destination key value to the source value; prove it through a local Postgres
  for every type that can be a key. A table whose key has no exact way back is read under **one** snapshot like
  a table without a key: then no repair read can be needed. Say which types these are.
- When what is left of the log behind the applied position is small (`backfill.swap_ready_rows`), the backfill
  lane moves `catching_up → swap_ready` and writes nothing more for this table (C10).
- Say what bounds the size of a pending log, and whether applied rows are deleted on the way or dropped with the
  log.

### 6. Swap (SPEC 3.2)

Inside `on_group`, in the group's `tx`, for a backfill in `swap_ready`:

1. The group's end must be past the upper bound of every snapshot the table used, repair reads included (design
   7.3); if not, a later group swaps. The source heartbeat guarantees that one comes.
2. The rest of the log, this group's changes included, is applied to the new table in mode `catch_up`. An
   unresolved key: nothing is swapped, the backfill goes back to `catching_up`, the group commits as usual.
3. The old table is dropped if there is one, the new one renamed, the routing set to `streaming`, the table
   machine and the backfill machine moved, the log dropped. All of it in the transaction that carries the
   group's position and fence.

- The rest must stay small, so that a swap does not hold the other tables of the group (SPEC 5.2): when it is
  larger than `backfill.max_swap_rows`, go back to `catching_up` instead. Measure the duration of swap groups.
- From the next group on, the table's changes are applied directly. This is the moment the table reaches
  streaming for SPEC 1.3.
- Say, with a test, what happens to a consumer's view over the table across a swap.

### 7. Resume, abandon, start again

- The backfill is a component under the supervisor (C11), started by the pipeline. At every start it reads its
  tables and goes on: loaded chunks are not read again; a table without a key whose snapshot is gone starts
  again; a backfill in `swap_ready` waits for the stream.
- A changed `relfilenode` (`VACUUM FULL`, `CLUSTER`, a rewriting `ALTER TABLE`, `TRUNCATE`) discards the loaded
  chunks and starts the table's reading again, in one transaction, with an alert that says why. A name that
  has no table any more, or a table with another identity than the plan's: the backfill is abandoned and the
  table reported (package T decides what follows; give it one function that abandons a table's live backfill
  inside a stream-lane transaction).
- A new request, a new plan for the table in the stream (a schema change during its own backfill: open point 11;
  in P3 the answer is "again, with the new plan"), a second slot finding: the running backfill is abandoned and
  a new one entered at the new `P0`, in one stream-lane transaction.
- Nothing is left behind: the new table and the log of a backfill that is `done` or `abandoned` are dropped in
  the transaction that ends it, or by a sweep at start that only touches objects whose backfill row says so.
  Never drop a table by the look of its name alone.

### 8. The machines and the table-status log

- The backfill progress machine, declared here with `state.declare` (C5): `planned → reading → catching_up ⇄
  swap_ready → done`, and `abandoned` from every state before `done`. The table replication machine is package
  S's declaration, extended by you with the transitions you enact. Every transition goes through
  `state.transition` in the transaction that does the work. Run package D's enumeration helper for both.
- The table-status log of SPEC 1.3: a view over the table machine's transitions — table, from, to, stream
  position, time, cause — that shows when a table left the guarantee and when it joined again. Package F's
  consumer loop reads it.

### 9. A table that joins the publication

Design 7.5, in the small form the gate test needs (discovery itself is P4): when a request carries `X`, the
table's first snapshot must have `xmin ≥ X`; the wait is a declared wait with a deadline and is reported while a
transaction stays open across the change. Extend package S's test helper so that a table that already has rows
is registered through a request.

### 10. Settings

A frozen dataclass of your own (C0), added to package S's loader, to `docs/configuration.md` and to
`config/example.toml`: readers, chunk size, backfill source, the waits, the two swap bounds, attempts, the
exporter's age threshold.

## Tests

**Default run (hermetic).** The planner; request joining; `on_group` against a local DuckDB with scripted change
sets — enter, append in both forms, swap, a swap that goes back, the same call twice; catch-up with several
versions, missing rows, key changes and repair reads against a scripted source; resume from every combination of
chunk states; both machines' enumeration; the crash points of the loader and of a round.

**Marker `localpg`.** On Postgres 18 and 16, on a standby and (as far as C9 allows) on a primary: the chunks of
one snapshot cover a table exactly once — every row in exactly one chunk, with an open-ended last chunk, an
empty table, a table that grew after planning, a table with empty pages; the scan plan; the framing cases; the
`relfilenode` check for each rewriting command; a recovery conflict provoked and read again; an exported
snapshot imported by parallel readers, and lost; row-level security; the way back for key values.

**Marker `md`.** Open point 5. Parallel loaders. The load transaction killed in flight, and `CommitUnknown` with
both outcomes. A loader of an abandoned backfill and a loader with an old epoch write nothing. The swap watched
by consumer loops on other connections over at least 200 swaps (count and checksum; never missing, never empty,
never partial). A `Conflict` on the swap and the group run again.

**Marker `t1` — the gate tests**, on the whole local path of package S's helper, with the oracle's seeded
workload writing through everything: inserts, updates, deletes, key changes, unchanged TOASTed values,
transactions over several tables, updates that move rows between pages; tables with and without a key.

1. **SPEC 1.6.** (a) The first snapshot of tables that have rows. (b) A re-backfill of one table by request,
   while it streams (without a gap) and after a gap. (c) A resumed backfill: the pipeline killed at each of your
   crash points and at the stream's crash points while a backfill is in each of its states, at least 3 seeds
   each. After each: `Workload.quiesce()`, `cdc_v2.oracle.compare(..., position=Q)` is equal; nothing lost,
   nothing applied twice (the table without a key shows a duplicate).
2. **SPEC 3.2.** A consumer loop on its own connection during all of 1: what it sees of a table is a complete
   version, the old one until the swap and the new one from it.
3. **SPEC 3.7.** A table with a key of at least a million rows, the pipeline killed at 50 %: after the resume
   the rows read are at most 1.1 × the table, counted by something the pipeline cannot flatter (the source
   node's own table statistics) next to its own counter; progress per table and per chunk is readable in
   MotherDuck during the run.
4. **SPEC 3.3 and 5.2.** While a large table backfills, the delay of the other tables' changes (commit at the
   source → visible in the destination), p50 and p99, with and without the backfill; no pause in the applied
   position.
5. **Disturbances during a backfill**, each ending in oracle equality: `VACUUM FULL`; `TRUNCATE`; growth past
   the planned end; deletes and a vacuum that truncates the tail; a recovery conflict; the replica restarted; a
   planned stop and a new run; the source black-holed in the middle of a chunk and the destination in the
   middle of a load (TCP proxy, fault sites); a second request; `stream.reposition`; an oversized transaction
   that touches the backfilling table; a transaction that stays open across the publication change of section 9.
6. **Speed.** Rows and megabytes per second from the request to the swap, narrow and wide rows, one reader and
   several, one large table and many small ones, and where the time goes. SPEC 3.1's number is measured on the
   Flight in stage 2; deliver the local number and what it says about F16.

**Negative controls**, kept as tests, each caught by a test above: `unknown` counted as `inside`; a table
without a key read although a transaction that touches it was `unknown`; replay from the largest `R0`; no wait
for `R0 ≥ P0` (a second standby whose replay is paused); a swap before the stream has passed a snapshot; no
`relfilenode` check; a load that commits rows without the chunk's state; a swap outside the group's
transaction.

## Deliver

- `src/cdc_v2/backfill/`, your migration list and settings, the tests, the extension of package S's helper.
- `docs/design/backfill.md`: the tables; the life of a backfill transaction by transaction, with the lane that
  writes each; every crash point and what a restart finds after it; the rules for tables with and without a
  key; the repair read; when a backfill is abandoned or starts again; the bounds (memory, size of a log, size
  and duration of a swap group, every wait); the answers on open points 5, 10, 11 and 12; the known limits;
  contract notes; what the P4 waves need from this code (the modes of SPEC 3.5, the operator's control table of
  3.4, the lag rule of 3.6, discovery, a schema change in place).
- Evidence under `evidence/p3/backfill/`.
- In your report: whatever in C3a, C9 or C10 did not fit, and what packages V, T and F must know.
