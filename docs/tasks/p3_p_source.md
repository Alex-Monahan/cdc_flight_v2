# Task: package P — source

Read `docs/tasks/README.md` first; its rules bind you.

You build everything the pipeline does on Postgres apart from the engine's replication stream and the backfill's
chunk reads: connections with deadlines, the catalog and its observation over time, the publication, the slot (on
a standby and on a primary), the facts that slot continuity compares, the timeline history and the source-side
heartbeat. Contract: `docs/design/p3_plan.md` C0, C1, C7. Packages Y (transform), V (slot continuity), B
(backfill), S (stream) and T (truncate and drop) build on it.

## Read as well

- `docs/design/core.md` sections 7.2, 7.5 and 8; `docs/design/p3_plan.md` section 6.2 (what V, S and T will ask
  of you).
- `docs/research/replica_decoding.md` completely, and `docs/research/debezium.md` sections on the slot, the
  publication and the heartbeat.
- `docs/wal_hazards.md`: H-007, H-008 (the catalog fence), H-014, H-022, H-023, H-029, H-030, H-031, H-034 to
  H-048. Where a hazard says **[test]** and the fact is one your functions return or rely on, the test is yours.
- `docs/localpg.md` (the local primary and standby, roles, known behaviour) and `docs/testing/harness.md` (fault
  sites, the TCP fault proxy; note its finding that TCP keepalives do not detect a black hole).

## Resources

- Postgres: `tools/localpg --home ~/.cache/cdc_flight_v2/pg-p --port-base 55640`; a second pair
  `--home ~/.cache/cdc_flight_v2/pg-p2 --port-base 55648` where a test needs a different major version or a
  cluster it can break. A home uses 8 ports from its base; TCP fault proxies of your own take the upper four of
  a range whose home has at most two standbys.
- No MotherDuck, no JVM. A logical slot can be read in tests with `pg_logical_slot_get_binary_changes` or a
  `psycopg` replication connection where you need a consumer.
- Everything runs as the roles of `docs/localpg.md`. What the runtime does must work as `cdc_v2_runtime`; what
  needs more (creating the publication, setting replica identity) is the setup role's, and the split is part of
  what you deliver.

## Found since this task was written — build on it

- **`tools/localpg` cannot start a pair with the Postgres 16 binaries.** It writes `sync_replication_slots`
  (17+) for every node, and its status query reads slot columns that exist from 17 on. Repair the tool first:
  settings, queries and the failover-slot commands depend on the server's version, and a command that needs 17
  says so instead of failing in SQL. Run the tool's self-tests (`tests/localpg`, their own home and ports) with
  the 16 binaries too (`CDC_V2_PG_BIN`), skipping only what 16 cannot do, and keep them green on 18. Update
  `docs/localpg.md`. Commit this alone, early, and push it: packages E, V and T wait for it.
- **Slot creation on a standby** (`docs/spike/local_streaming.md` section 1): unaided it took 0.95–8.1 s here,
  and 0.7–0.9 s with `pg_log_standby_snapshot()` on the primary. The admin role of `docs/localpg.md` (the role
  shaped like a managed provider's) was **refused** that function; only the bootstrap superuser could call it.
  So `slot.create` must not depend on it: use it where the role may, and otherwise rely on what the server
  logs by itself (say exactly what: a checkpoint, the background writer after other WAL and at which interval)
  together with a write of your own. Bound and test both ways, and report which grant would be needed.
- **The holder of the slot.** Debezium sets the application name of its replication connection to a fixed text,
  so an engine instance cannot be told by a name. The engine will be recognised by the walsender's process id
  together with its backend start time. `slot.facts` therefore returns the holder's `backend_start` with full
  precision; show by test that the runtime role sees that row of `pg_stat_activity` for a walsender of the same
  role, on a primary and on a standby, without `pg_read_all_stats`.
- **Heartbeat.** Debezium's own heartbeat records do not advance when the WAL holds only changes of tables
  outside the publication (measured by package E). The source-side heartbeat must therefore be something that
  the publication carries or that the engine turns into a frame with a position (the engine frames logical
  messages as `MESSAGE`; inside a transaction they count in the END record's `event_count`). Decide with that.
  A black-holed replication connection is noticed neither by the engine nor by the driver; the pipeline will
  notice it because a heartbeat it wrote does not come back. So `heartbeat.beat` returns what the stream needs
  to recognise that write when it arrives (its position on the primary and a value unique to the write).
- Package E's document (`docs/design/engine.md`) reaches `main` during your work. Merge `origin/main` when your
  report is near, read its sections on HELLO, HEARTBEAT and MESSAGE frames, and name any disagreement.

- **From the review of package E.** A table without a primary key must have `REPLICA IDENTITY FULL` before it is
  published, also when it has `REPLICA IDENTITY USING INDEX`: Debezium gives such a table no key, and the engine
  would carry an incomplete old image. `publication.ensure` and the catalog observation report it.
- The heartbeat table is in the publication (it is what makes an idle slot advance and what the stream's marker rule
  of SPEC 4.6 watches).
- `slot.facts` returns the server's `wal_sender_timeout`. It bounds how long a black-holed replication connection
  keeps the slot; 0 is refused by the pipeline (package V), so report it as a fact, with its unit.

## What to build (`src/cdc_v2/pg/`)

### 1. Connections

`pg.connect(target, deadline, *, replication=False)` — `target` is `'primary'`, `'replica'` or a configured name;
settings in a frozen dataclass; the password from the environment or a callable, never an attribute that can be
printed, logged or put into an error message.

- Fixed session settings on every connection, so that every value Postgres prints is the same text everywhere:
  time zone UTC, ISO dates, `IntervalStyle`, `extra_float_digits`, `bytea_output = hex`, client encoding UTF8,
  an empty-safe `search_path`, plus `statement_timeout`, `lock_timeout` and
  `idle_in_transaction_session_timeout` from the settings. The transform (package Y) and the backfill read text
  under exactly these settings (C3); write them down as a table in your document and test that a role-level or
  database-level `ALTER … SET` to the contrary does not get through.
- TLS: `verify-full` whenever the settings name a certificate authority or a remote host; a local test cluster
  without TLS must be an explicit setting, never a silent fall-back.
- Every call that waits takes a `cdc_v2.deadline.Deadline`: connect, each statement, close. A statement that
  passes its deadline is cancelled; a connection that does not answer the cancel is closed from another thread;
  nothing can block the caller beyond deadline plus a short grace. Show it with the fault proxy: a black hole
  during connect, during a statement and while idle in a transaction; a reset; a slow link. State which
  keepalive and `tcp_user_timeout` settings you chose and what they do and do not detect.
- Errors as a small taxonomy the callers can act on: connection lost (run again), deadline, recovery conflict on
  a standby (SQLSTATE 40001 and the conflict codes: read again), lock timeout, permission, everything else.
- Fault sites `pg.connect` and `pg.execute` (C1), declared here.

### 2. Catalog

`catalog.tables(conn, include=…) -> [SourceTable]`, one immutable value per table with a stable hash:

- identity: OID, schema, name, `relfilenode`, `relkind` (ordinary, partitioned, partition and its root),
  persistence (permanent, unlogged, temporary), row-level security flags and whether the reading role is subject
  to them (H-039);
- columns in `attnum` order: name, type OID and the full type name with modifiers, the base type of a domain,
  enum labels, array element type and dimensions as declared, composite and range structure as far as the
  catalog gives it, `NOT NULL`, default present, generated (stored, virtual) and identity kind, dropped columns
  left out but their `attnum` gaps visible, storage and whether the type can be TOASTed ("unchanged possible");
- key: the primary key, or the replica identity index, in key order; the replica identity setting (`default`,
  `index`, `full`, `nothing`);
- publication: whether our publication contains the table, with column list and row filter if any (H-023; the
  pipeline refuses them later, you report them).

Names that differ only by case, names with quotes, dots, spaces, non-ASCII, a 63-byte name, a table in a schema
named like one of ours: all read back exactly.

`catalog.observe(node) -> CatalogObservation` — the catalog fence of H-008. On a standby: read
`b = pg_last_wal_replay_lsn()`, then in one `REPEATABLE READ` transaction the facts
`(oid, relname, nspname, relfilenode, relreplident, relkind, relpersistence)` and per column
`(attnum, attname, atttypid, atttypmod, attisdropped, attgenerated, default present)` for the published tables,
and `a = pg_last_wal_replay_lsn()` inside it. The observation carries the bracket `(b, a)`: the catalog state is
the one at some replay position in `[b, a]`. On a primary the bracket is built from
`pg_current_wal_insert_lsn()` in the same way; say what it means there (a DDL transaction's catalog change
becomes visible at its commit). Deliver also `catalog.diff(old, new)`: tables that appeared, disappeared, kept
their name with a new OID (drop and recreate, H-007), were renamed (same OID), had columns added, dropped,
renamed (same `attnum`), retyped, or a new `relfilenode` (rewrite, truncate). The observation is cheap (one
round trip plus the bracket) and safe to run every 2–5 s; measure it with 1,000 tables.

**[test]** the bracket: with a writer that runs DDL in a loop on the primary and records the commit position of
each DDL, every observation on the standby must show exactly the DDL transactions that committed at or before
`b`, none that committed after `a`, and either answer in between. A deliberately wrong bracket (`a` read before
the transaction) must be caught.

### 3. Publication and replica identity

- `publication.ensure(primary, tables)`: creates the publication if it is missing and adds tables. It **only
  ever adds**: nothing on the source is fenced by the leader epoch, so a deposed run that repeats this call must
  be harmless. Removing a table is a separate, explicit function that nothing calls automatically. The
  publication publishes insert, update, delete and truncate, has no row filters and no column lists, and
  `publish_via_partition_root` as the design decides (say what you chose and why, H-021).
- `publication.state(conn)`: what exists, to compare with the configuration (H-034: a publication dropped and
  recreated is a break; say which facts show it).
- `replica_identity.set_full(primary, table, deadline)` with a `lock_timeout`; a table without a key and without
  `FULL` in the publication breaks the source's own updates and deletes (H-014): provide the check that finds
  such tables **before** they are added, and a test that shows the source failure when the order is wrong.
- After adding a table: return the primary's next transaction id (`pg_current_xact_id()` or the snapshot's
  `xmax`, state which) for the rule of design 7.5.

### 4. Slot

- `slot.create(node, name, deadline)` — logical, `pgoutput`.
  - On a standby: creation waits for a running-transactions record from the primary. Request it
    (`pg_log_standby_snapshot()` on the primary, repeated while waiting) and bound the wait by the deadline;
    an idle primary, a long transaction and a prepared transaction on the primary each get a test (H-040). A
    creation that is abandoned leaves no half-created slot behind (H-042: a slot left by a cancelled creation).
  - On a primary: `failover = true` where the server supports it (17+), two-phase off.
  - Returns the slot's consistent point.
- `slot.drop(node, name, deadline)` — only an explicit operator action or the enactment of a slot finding calls
  it. It refuses a slot that is active unless told to terminate the holder first.
- `slot.facts(node, name) -> SlotFacts | None`, read in **one statement** so that the facts belong together, on
  Postgres 16, 17 and 18 (columns differ; absent ones are `None`): plugin, database, `slot_type`, `temporary`,
  `active`, `active_pid`, `restart_lsn`, `confirmed_flush_lsn`, `wal_status`, `safe_wal_size`, `conflicting` /
  `invalidation_reason`, `inactive_since`, `failover`, `synced`, `two_phase`, `catalog_xmin`; of the holder:
  backend start time and `application_name` (from `pg_stat_activity` by `active_pid`); of the server:
  `system_identifier`, timeline, `pg_is_in_recovery()`, the current replay or insert position, and the server
  version.
- **[test]** slot identity (design open point 6): is there any fact that tells a slot that was dropped and
  recreated under the same name from the original, on each version? Candidates: the statistics entry
  (`pg_stat_replication_slots`, `stats_reset`), `inactive_since`, `catalog_xmin`, `restart_lsn`, the slot's
  state file. Report what you find; if there is none, say so plainly.
- `slot.terminate_holder(node, name, deadline)`: `pg_terminate_backend` of the `active_pid` of **our slot name
  only**, never of a pid taken from anywhere else; returns when the slot is inactive or the deadline passes.
  Test the race: the holder exits by itself; another session takes the slot between the read and the
  terminate.
- `slot.timeline_history(node, timeline)` over a `replication=database` connection (`IDENTIFY_SYSTEM`,
  `TIMELINE_HISTORY`); verify that `psycopg` 3 can do it and that `cdc_v2_runtime` may; and
  `timeline.contains(history, position, timeline)` — whether a position lies on the history of the server's
  current timeline. Test with a real promotion on the second cluster pair: a position before the switch point,
  after it on the old timeline, and after it on the new one (H-029, H-045).

### 5. Heartbeat

`heartbeat.beat(primary, deadline)` — the source-side write of SPEC 4.4 that makes the stream move when the
published tables are idle. Decide between a logical message (`pg_logical_emit_message`, transactional or not)
and a row in a heartbeat table, by what the engine can turn into a position that may be acknowledged
(`docs/research/debezium.md`; package E's `HEARTBEAT` frame) and by what a role without table privileges can
do; write the decision down. The write is idempotent and harmless when two runs do it. Also provide the read
that tells how far a standby is behind the primary in bytes and, where the server gives it, in time (H-044).

## Tests

- Default run (hermetic, a few seconds): the SQL builders, quoting of identifiers, `catalog.diff`,
  `timeline.contains`, error mapping from SQLSTATE, settings validation, deadline handling with fault sites.
- Marker `localpg`: everything above on the private pair, as the runtime role where the runtime calls it;
  the fault-proxy cases; slot creation on the standby under the three waiting conditions; slot facts while a
  consumer is attached, detached, terminated; invalidation by `max_slot_wal_keep_size` (facts before and after,
  H-036) and, on 18, by the idle timeout if the server has it (H-037); `pg_replication_slot_advance` seen in
  the facts; the bracket test; publication add under concurrent writers; a promotion.
- Versions. Postgres 18 is the version of the local tiers and everything must pass on it (the default binaries
  of `tools/localpg`). Postgres 16 is installed as well (`--pg-bin /opt/homebrew/opt/postgresql@16/bin`, on the
  second home): run `slot.facts`, `slot.create`, the catalog queries and the identity question there too, since
  the columns differ. Postgres 17 is not installed on this machine: do not install anything with Homebrew; write
  the statements for 17 from the documentation, mark them as not run, and say so in your report.
- Nothing is left behind: no slot, no publication, no cluster running after the tests, also when they fail.

## Deliver

- `src/cdc_v2/pg/`, the tests, `docs/design/source.md` (the API as built, the session settings table, the role
  split with the exact grants, the answers to the **[test]** points, contract notes), evidence under
  `evidence/p3/source/`.
- In your report: every Postgres behaviour that differs from the research documents or the hazard list, the
  answer on slot identity per version, and what packages V, B, S, T and Y must know that C7 does not say yet.
