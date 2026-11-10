# Source side (package P): `cdc_v2.pg`

Status: built in P3 (2026-11-10). Contract: `docs/design/p3_plan.md` C0, C1, C7. Evidence: `evidence/p3/source/`
(`localpg_pg18.json`, `localpg_pg16.json`, written by the `localpg` suite with `CDC_V2_P_EVIDENCE=1`; five files
of single tests are named where they are used). Every number below comes from those files; they were measured on a
shared laptop and are indications only.

Modules under `src/cdc_v2/pg/`:

| Module | What |
|---|---|
| `settings.py` | `NodeSettings` (one server), `SourceSettings` (all nodes by name, our object names), `Password`, the fixed session settings |
| `conn.py` | `connect`, `PgConnection`: deadlines, cancel, fault sites `pg.connect` and `pg.execute` |
| `errors.py` | the error taxonomy and `classify` |
| `catalog.py` | `tables` → `SourceTable`, `observe` → `CatalogObservation` (H-008), `diff` → `CatalogDiff` |
| `publication.py` | `ensure` (only adds), `state`, `check`, `remove` (explicit only) |
| `replica_identity.py` | `set_full`, `problem` |
| `slot.py` | `create`, `drop`, `facts` → `SlotFacts`, `server_facts`, `terminate_holder`, `timeline_history`, `nudge_method` |
| `timeline.py` | `parse_history`, `contains` |
| `heartbeat.py` | `beat` → `HeartbeatWrite`, `ensure`, `check`, `setup_sql`, `standby_lag` |
| `preflight.py` | `check` → `Preflight`: what a node must offer the runtime role (§6a) |
| `sql.py` | identifier quoting |

## 1. Connections

```python
from cdc_v2 import pg
from cdc_v2.deadline import Deadline
pg.configure(pg.SourceSettings(nodes={"primary": NodeSettings(...), "replica": NodeSettings(...)}))
conn = pg.connect("replica", Deadline.after(10))            # or a NodeSettings; replication=True for replication=database
rows = conn.execute("SELECT …", params, deadline=Deadline.after(5))
with conn.transaction(deadline, isolation="repeatable read", read_only=True): ...
conn.raw                                                      # the psycopg connection (autocommit), e.g. for COPY
with conn.guard(deadline, "chunk read"): conn.raw.copy(...)   # the same deadline rules around raw psycopg calls
conn.close(deadline)
```

**Settings.** `NodeSettings(host, port, dbname, user, password=Password(env=…|fn=…), tls='verify-full',
sslrootcert, application_name, connect_timeout_s=10, statement_timeout_ms=30000, lock_timeout_ms=5000,
idle_in_transaction_session_timeout_ms=60000, keepalives_idle_s=30, keepalives_interval_s=10, keepalives_count=6,
tcp_user_timeout_ms=120000, cancel_grace_s=2)`. The port is always explicit; there is no default port anywhere. The
password is never an attribute: `Password` reads it from an environment variable or a callable at connect time,
shows only the variable name in `repr`, refuses pickling, and the value is removed from the connect keywords after
the call. TLS is `verify-full` (against `sslrootcert`, or the system store); `tls='disable'` is refused unless set
explicitly and the host is on this machine (a socket path, `localhost` or a loopback address).

**Fixed session settings.** Sent in the startup packet (`options=-c …`, source "client", which outranks
`ALTER ROLE … SET` and `ALTER DATABASE … SET`), then read back from `pg_settings`; a connection on which one
differs is refused with `SessionSettingsMismatch`. The text form of the backfill (C3, package Y) is defined by this
table and by nothing else.

| Setting | Value | Why |
|---|---|---|
| `TimeZone` | `UTC` | `timestamptz` prints as `2026-01-02 03:04:05.678901+00` |
| `DateStyle` | `ISO, YMD` | ISO output; unambiguous input order |
| `IntervalStyle` | `postgres` | `10 mons 3 days 04:05:06.5`: months, days and time kept apart, each signed; the same style as the engine's walsender session, so a text value that holds intervals (an array, a composite) prints alike on both paths (decided in the fix round of package Y) |
| `extra_float_digits` | `3` | shortest exact round-trip text for `float4`/`float8` |
| `bytea_output` | `hex` | `\x00ff` |
| `client_encoding` | `UTF8` | |
| `lc_monetary` | `C` | `money` prints the same everywhere |
| `standard_conforming_strings` | `on` | |
| `search_path` | `pg_catalog, pg_temp` | no user schema is searched; temporary objects cannot shadow catalog names (every query of ours qualifies names) |
| `row_security` | `off` | a read that row-level security would filter fails instead (H-039) |
| `default_transaction_isolation` | `read committed` | a role cannot make our statements serializable |
| `statement_timeout`, `lock_timeout`, `idle_in_transaction_session_timeout` | from `NodeSettings` | server-side bounds behind the client deadlines |

Test `test_session_settings_are_pinned_against_role_and_database_settings`: all of `DateStyle`, `IntervalStyle`,
`extra_float_digits`, `bytea_output`, `TimeZone`, `lc_monetary`, `search_path`, `row_security`, the timeouts and the
default isolation set to other values at role and at database level; every value held on primary and standby, on SQL
and on replication connections, and the printed texts are the ones above.

**Deadlines.** Every call takes a `cdc_v2.deadline.Deadline`.

- Connect: libpq `connect_timeout` = the remaining time (at least 2 s, libpq's floor), and the connection is opened
  on a helper thread; the caller is released at deadline + `cancel_grace_s` whatever happens (a connection that is
  opened late is closed by the helper).
- Statement: a watchdog fires at the deadline, sends a cancel request (`cancel_safe`, bounded by `cancel_grace_s`),
  and if the call has not returned `cancel_grace_s` later shuts the socket down, which fails the blocked call at once.
  A call reached by the watchdog raises `DeadlineExceeded` and the connection is closed (a late cancel could hit the
  next statement); the close runs in the background after the socket is shut, so it adds no wait. Bound:
  deadline + 2 × `cancel_grace_s`. The shutdown uses a duplicate of the socket that the connection keeps for its
  lifetime, so it can never reach a reused descriptor of another connection.
- `transaction()`: after an SQL error inside the block, the ROLLBACK has its own `cancel_grace_s` deadline.
- Close: on a helper thread; past the deadline the socket is shut down and the close finishes in the background.
- Limit: a fault-site `block` rule at `pg.execute` sleeps in the calling thread and is not interrupted by the
  watchdog (test runs with a control file only).

Measured with the TCP fault proxy (`evidence … proxy`, grace 0.5 s): black hole during connect, during a statement
and while idle in a transaction all end at the deadline plus at most two graces; while idle in a transaction the
server ends the session itself through `idle_in_transaction_session_timeout`, so no lock of ours outlives a network
wait; a reset and a refused connection are noticed in milliseconds (`ConnectionLost`); a 0.2 s delay each way makes a
statement take ≈ 0.4 s and a 0.2 s deadline fails it. A pure `pg_sleep(30)` with a 0.5 s deadline returns after
0.5 s (the cancel is answered).

**Keepalives** (client side): idle 30 s, interval 10 s, 6 probes → a dead peer of an idle connection is found after
about 90 s; `tcp_user_timeout` 120 s bounds unacknowledged sent data on Linux (macOS has no such option; libpq
ignores it there). The socket options are verified by test on macOS. Neither detects a server that is alive but does
not answer, nor a black hole inside a proxy whose kernel acknowledges probes (`docs/testing/harness.md` §7): the
statement deadlines and the end-to-end heartbeat (§5) do.

**Errors** (`cdc_v2.pg.errors`):

| Class | SQLSTATE / cause | Caller |
|---|---|---|
| `ConnectionLost` | class 08, 57P01 (terminated), 57P02, 57P03, 57P05, 25P03, driver errors without SQLSTATE | connect again, run again |
| `DeadlineExceeded` (`cdc_v2.deadline`) | our deadline, or a server `statement_timeout` (57014 with that text) | the caller's rule |
| `RecoveryConflict` (`ReadAgain`) | 40001/40P01 on a standby or with "conflict with recovery"; 57P04 | read again |
| `SerializationFailure` (`ReadAgain`) | 40001/40P01 on a primary | run again |
| `LockTimeout` | 55P03 | later, with backoff |
| `PermissionDenied` | 42501, 28000, 28P01; libpq login refusals without SQLSTATE | alert, retry with backoff |
| `SourceError` | everything else, including TLS certificate faults and a cancel by someone else | not retried blindly |

Messages keep only what the server said: libpq's "connection to server at …, port … failed:" prefix is removed, the
host, the user name and the password are replaced by `<redacted>`, and the driver exception is not chained.
A recovery conflict was produced for real: a standby transaction holding `ACCESS SHARE` while the primary took
`ACCESS EXCLUSIVE`; after `max_standby_streaming_delay` (30 s) the next statement raised `RecoveryConflict` (40001).

## 2. Catalog

`catalog.tables(conn, include=None | [(schema, name)] | callable, *, publication, deadline) -> [SourceTable]`, read in
one REPEATABLE READ transaction. Default: every ordinary or partitioned table outside `pg_catalog`,
`information_schema`, `pg_toast*` and `pg_temp_*` (temporary tables of other sessions are not visible anyway).
`SourceTable` (frozen; `hash()` over everything, `shape_hash()` over OID, names, columns, the key facts (`has_key`,
`key_attnums`, `primary_key_deferrable`) and replica identity — not `relfilenode`, row security or publication
membership; a key that becomes deferrable or immediate changes it):

- identity: `oid`, `schema`, `name`, `relfilenode`, `relkind` (`ordinary` | `partitioned`), `is_partition`,
  `partition_root`, `persistence` (`permanent` | `unlogged` | `temporary`), `rls_enabled`, `rls_forced`,
  `rls_applies_to_reader` (`row_security_active()` for the reading role, H-039);
- `columns` in `attnum` order, dropped ones left out (`max_attnum` shows the gaps): `attnum`, `name`, `type_oid`,
  `typmod`, `type_name` (`format_type`, with modifiers), `type` (`TypeInfo`: kind base/array/domain/enum/composite/
  range/multirange/pseudo; a domain's ultimate base, enum labels in sort order, array element, composite fields,
  range subtype), `ndims` (as declared), `not_null`, `has_default`, `generated` (`stored`, `virtual` on 18),
  `identity`, `storage`, `toastable` ("unchanged possible": variable length and storage other than plain);
- key (rule K, §3): `has_key` (property: the table has a key by rule K), `key` / `key_attnums` (the primary key's
  columns in key order when `has_key`, else empty; the key columns of the index only, never its `INCLUDE`
  columns), `key_source` (`'primary key'` when `has_key`, else `None`), `has_primary_key`,
  `primary_key_deferrable` (`NOT pg_index.indimmediate` of the primary key), `replica_identity` (`default` |
  `index` | `full` | `nothing`), `replica_identity_index` (named also when it is no key),
  `replica_identity_index_is_key`, and `replica_identity_problem` (§3);
- publication: `published` (directly or through a partition ancestor), `publication_columns`,
  `publication_row_filter` (H-023: reported; the pipeline refuses them).

Names that differ only by case, with quotes, dots, spaces, non-ASCII, 63 bytes, and tables in schemas named like
ours (`_cdc…`, `cdc_v2…`) read back exactly (`test_names_read_back_exactly`).

`catalog.observe(node, names=(), *, publication, deadline) -> CatalogObservation` — the catalog fence of H-008, for
every table in `names` and every table in our publication. One pipeline, **one network round trip**, two statements:

1. `b` = `pg_last_wal_replay_lsn()` on a standby, `pg_current_wal_insert_lsn()` on a primary, in its own statement;
2. one statement that reads per table `(oid, relname, nspname, relfilenode, relreplident, relkind, relpersistence,
   relispartition)`, per column `(attnum, attname, atttypid, atttypmod, attisdropped, attgenerated, atthasdef)`
   (dropped ones included), the primary-key and replica-identity index columns, publication membership with
   "has column list" and "has row filter", the publication row (`to_jsonb(pg_publication)`), `pg_current_snapshot()`
   and `a` (the same function as `b`).

One statement has one snapshot, taken before execution, so the facts are those of one snapshot and `a` is read after
it: the equivalent of the REPEATABLE READ transaction of the task, with one round trip instead of five. Only raw
catalog columns are read (no `format_type`, no `regclass` casts): those use the latest catalog, not the snapshot.

- **On a standby** the catalog state is that of one replay position in `[b, a]`: every DDL transaction whose commit
  ended at or before `b` is visible, none that committed after `a` is.
- **On a primary** only the upper bound is a guarantee: no commit inserted after `a` is visible. A commit record is
  written before its transaction becomes visible (and with synchronous replication it can stay invisible for long), so
  a DDL whose commit ended before `b` may still be invisible (shown with a commit held in that state: §7). Whether a
  given transaction is in the
  observation is `CatalogObservation.visible(xid)`: below the snapshot's `xmax` and not in its in-progress list
  (a transaction at or above `xmax` is invisible without being listed). A DDL transaction's catalog change becomes
  visible at its commit, never before.

`TableFacts.identity` = (OID, per live column `(attnum, name, type oid, typmod)`, key attnums) — what package T
compares. `relfilenode` is not part of it. `TableFacts` carries the raw `pk_attnums`, `pk_deferrable` (read in the
same statement), `has_key` (rule K) and `key_attnums` (`pk_attnums` when `has_key`, else empty), so an
observation and `catalog.tables` give the same answer (`test_catalog_follows_rule_k`, 14 table shapes).

**[test] the bracket** (`test_bracket_on_standby`, `test_bracket_on_primary`): a writer creates tables on the
primary, each in its own transaction, recording an interval that holds its commit (`L0` = insert position inside the
transaction, `L1` = after COMMIT), while an observer observes all names in a loop. Rule checked for every
observation and DDL: `L1 ≤ b` ⇒ visible; `L0 ≥ a` ⇒ not visible. Negative control in the same run: an upper bound
read **before** the observing statement (with 20 ms between) must be caught. Results: see §7.

**[test] the lower bound on a primary** (`test_lower_bound_on_primary_while_a_commit_waits`, on the breakable pair):
a transaction creates a table; `synchronous_standby_names` is then set to a standby that does not exist, so its
COMMIT writes and flushes the commit record and waits (`wait_event = SyncRep`) before the transaction becomes
visible. The commit record's end is read from the WAL (`pg_walinspect`). An observation taken now must have
`b ≥` that end, must not contain the table, and `visible(xid)` must be false. The setting is removed, the COMMIT
returns, and the next observation contains the table with `visible(xid)` true.

`catalog.diff(old, new) -> CatalogDiff` over two observations, two lists of `SourceTable`/`TableFacts`, or maps
OID → value: `appeared`, `disappeared`, `recreated` (same name, new OID, old OID gone: H-007), `renamed` (same OID;
a name freed by a rename and taken by a new table is `renamed` + `appeared`, not `recreated`; a swap a↔b is two
renames), and per OID a `TableChange`: columns added, dropped, renamed (same `attnum`), retyped (type or typmod),
`rewritten` (`relfilenode`: TRUNCATE, VACUUM FULL, CLUSTER, rewriting ALTER), `key_changed` (key attnums by rule K,
or the deferrability of the primary key),
`replica_identity_changed`, `persistence_changed`, `publication_changed`. A drop and add of a column with the same
name and type is a drop of one `attnum` and an add of another, never a rename.

## 3. Publication and replica identity

- `publication.ensure(primary, tables, *, publication='cdc_v2_pub', deadline) -> EnsureResult(created, added,
  present, next_xid, state)`. Creates the publication if missing: `WITH (publish = 'insert, update, delete,
  truncate', publish_via_partition_root = false)`, no row filters, no column lists. Then adds only the missing
  tables, in one `ALTER PUBLICATION … ADD TABLE`. It never removes or alters anything, so a deposed run that repeats
  it is harmless; concurrent callers are harmless too (duplicate-object and unique violations from a racing caller
  are absorbed: test with 4 concurrent callers). `publish_via_partition_root = false` (H-021): leaves are published
  under their own names and a partition's TRUNCATE is delivered; the combined-table mode of SPEC 7.3 is the
  applier's job. `publish_generated_columns` is left at the server default (P4 type wave decides, H-022).
- **Rule K** (core design section 6). A table *has a key* when it has a primary key that is not deferrable
  (`pg_index.indimmediate`) and its replica identity is `DEFAULT`, `FULL`, or `USING INDEX` on the primary key's
  own index; its key is the primary key. Every other table that can be published is a *table without a key* and
  needs `REPLICA IDENTITY FULL`: a table with no primary key (also with unique indexes, also with `USING INDEX`),
  and a table whose primary key is deferrable. `catalog.has_key(...)` is the rule, `SourceTable.has_key` and
  `TableFacts.has_key` apply it, and for a table without a key `key` and `key_attnums` are empty and `key_source`
  is `None`. The engine is told which tables are without a key (`docs/design/engine.md` 4a), because Debezium
  calls more tables keyed than rule K does.
- Before anything is added, every named table is read again and `check` refuses (`NotPublishable` with a reason per
  table): unlogged or temporary tables (H-030); a table **without a primary key that is not `REPLICA IDENTITY FULL`,
  also with `REPLICA IDENTITY USING INDEX`** (it has no key by rule K and the engine would carry an incomplete old
  image); a table **with a deferrable primary key that is not `REPLICA IDENTITY FULL`** ("primary key is
  deferrable: needs REPLICA IDENTITY FULL (replica identity is …)"); `REPLICA IDENTITY NOTHING`; a table with an
  immediate primary key whose identity index is another index. `set_full` then `ensure` publishes a table with a
  deferrable primary key, and the source's own `UPDATE` and `DELETE` keep working
  (`test_deferrable_key_set_full_then_publish`). For a partitioned table its leaf partitions are checked too (with `publish_via_partition_root = false`
  they are what is published). `replica_identity.set_full(primary, table, deadline, lock_timeout_ms=None)` fixes
  all of them (`SET LOCAL lock_timeout`; `LockTimeout` when the ACCESS EXCLUSIVE lock is not granted in time;
  returns False when nothing had to change); for a partitioned table it sets every existing leaf as well (an
  `ALTER TABLE` on the parent does not reach the partitions; a partition attached later needs its own call). The catalog observation reports the same rule as `TableFacts.replica_identity_problem`.
  The wrong order is shown by test: a keyless table added without FULL makes the source's own `UPDATE` and `DELETE`
  fail (SQLSTATE 55000, "does not have a replica identity and publishes updates").
- What Postgres lets a key be is measured in `tests/pg_source/test_pg_keys.py` (Postgres 16.6 and 18.6, hazard
  H-015, evidence `replica_identity_keys_pg16.json` and `replica_identity_keys_pg18.json`): an immediate key is
  checked row by row, so the decoded stream never shows two rows with one key; a deferrable primary key is not a
  replica identity (a published table with one and `REPLICA IDENTITY DEFAULT` refuses its own `UPDATE` and
  `DELETE`, SQLSTATE 55000), and with `REPLICA IDENTITY FULL` it is the only table shape whose stream can show two
  rows with one key.
- **How a key becomes deferrable or immediate on an existing table** (`test_how_a_primary_key_becomes_deferrable`,
  Postgres 16.6 and 18.6, evidence `evidence/p3/key_rule/localpg_pg16.json` and `localpg_pg18.json`, section
  `keys`).
  `ALTER TABLE … ALTER CONSTRAINT <pkey> [NOT] DEFERRABLE` is refused on both versions (42809, "constraint … is not
  a foreign key constraint"). The only way is to drop the primary key and add a new one, which can be one
  statement: `ALTER TABLE t DROP CONSTRAINT t_pkey, ADD PRIMARY KEY (id) DEFERRABLE`, or `… ADD CONSTRAINT t_pkey
  PRIMARY KEY USING INDEX i [DEFERRABLE]` with a unique index built beforehand (Postgres then marks that index
  not immediate). The table keeps its OID and `relfilenode`; the primary key's index is a new one. The catalog
  then reports `has_key` false, `key_attnums` empty, `primary_key_deferrable` true; `shape_hash` changes;
  `diff` reports `key_changed` (not `rewritten`, not a column change). Going back to an immediate key gives the
  first `shape_hash` again. Under `REPLICA IDENTITY DEFAULT` a published table whose key became deferrable
  refuses its own `UPDATE` at once (55000, H-014): making a key deferrable on a published table needs `REPLICA
  IDENTITY FULL` first, which only the table's owner can do.
- `next_xid` (X of design 7.5) = `pg_snapshot_xmax(pg_current_snapshot())` read after the change committed: the first
  unassigned transaction id. Every transaction with a smaller id started before the change.
- `publication.state(conn)` → `PublicationState(exists, oid, owner, all_tables, publish, via_root,
  generated_columns, tables[(oid, schema, name, columns, row_filter)])`, `.problems` lists differences from what we
  create. H-034: a publication that was dropped and recreated has another `oid` (test); compare it with the oid
  recorded when the slot was created. Changes between the drop and the recreate are not in the stream.
- `publication.has_table(conn, schema, name, *, publication, deadline) -> bool`: the table is in the publication
  (directly, through a partition ancestor, or FOR ALL TABLES); a catalog read the runtime role may do. The attach
  step uses it, through `heartbeat.check`, for the heartbeat table.
- `publication.leaves(conn, schema, name)`: the leaf partitions of a partitioned table.
- `publication.remove(...)` exists for an operator; nothing calls it automatically.

## 4. Slot

`slot.create(node, name, deadline, *, primary=None, nudge='auto', nudge_interval_s=0.5, failover=True) ->
CreatedSlot(name, consistent_point, on_standby, failover, seconds, nudges, nudge_method)`; logical, `pgoutput`, not
temporary, two-phase off; refuses an existing name (`SlotExists`).

- The creating statement runs with `statement_timeout = 0` (the caller's deadline bounds it, not the session's
  30 s) and `client_connection_check_interval = 1000`; both are reset afterwards.
- **On a standby** the creation waits for a running-transactions record from the primary. With `primary` given, a
  helper thread asks for one every 0.5 s, on a connection of its own opened from `primary`'s settings (the caller's
  connection is only used to choose the method and for the report), stopping 1 s before the deadline: `pg_log_standby_snapshot()` when the role may call it
  (`nudge_method` = `standby_snapshot`), otherwise a write of its own (`write`: a non-transactional logical message
  `pg_logical_emit_message(false, 'cdc_v2.nudge', '')`, which needs no table privilege). What the server does by
  itself: the primary's background writer logs a running-transactions record at most every 15 s and only when
  "important" WAL was written since the previous one (`LOG_SNAPSHOT_INTERVAL_MS`, `bgwriter.c`); a checkpoint also
  logs one. So with only our writes, the record arrives within about 25 s of the first write (15 s plus up to 10 s
  of background-writer hibernation, §7 finding 7); on a truly idle primary
  without writes it never arrives. `nudge` may also be any callable (for example the heartbeat write). A long
  transaction and a prepared transaction on the primary block creation for as long as they stay open (the snapshot
  builder waits for every transaction running at the first record).
- **Grant needed for the fast path**: `GRANT EXECUTE ON FUNCTION pg_catalog.pg_log_standby_snapshot() TO <runtime
  role>`, given by a superuser (the admin role of a managed provider cannot, it does not own the function).
- **When the creation fails** — at the deadline (`SlotCreateTimeout`, a `DeadlineExceeded`), on a lost connection or
  any other error, on a standby and on a primary — the creating backend, if still there, is terminated by process
  id **and** backend start time from a second connection; then the call checks whether a slot exists
  (`report["leftover"]`: True, False, or None with `cleanup_error` when the check itself failed). The report is the
  exception's `report` attribute. The report names what the roles may see
  of the cause: the primary's oldest running transaction id (`pg_snapshot_xmin`), the number and age of prepared
  transactions, the nudge method and count.
- A client that dies during the creation: with `client_connection_check_interval = 1000` (set by `create` for the
  creating statement) the backend notices the closed socket and abandons the creation; without it the slot appears
  later, when the running-transactions record arrives (negative control in `test_creation_whose_client_is_killed_
  leaves_no_slot`). A creation abandoned on a dead network path (no FIN reaches the server) can still finish later:
  a fresh slot of our name then exists that nobody recorded; its `confirmed_flush_lsn` is its consistent point (§8).
- **On a primary**: `failover = true` on 17+ (`facts.failover` is then true); on 16 the server has no failover slots
  and `failover` is None.

`slot.drop(node, name, deadline, *, terminate=False)` refuses an active slot (`SlotActive`) unless told to terminate
its holder first; returns False when there was no slot. Only an operator action or the enactment of a slot finding
calls it.

`slot.facts(node, name, *, deadline) -> SlotFacts | None` — one statement (a CTE of server facts, left-joined to
`pg_replication_slots`, `pg_stat_activity` on `active_pid` and `pg_stat_replication_slots`), the column list chosen by
the server version (columns a version lacks are None). `None` when the slot does not exist;
`slot.server_facts(node)` gives the server part alone.

| Field | 16 | 17 | 18 | Note |
|---|---|---|---|---|
| `plugin`, `database`, `slot_type`, `temporary`, `active`, `active_pid`, `restart_lsn`, `confirmed_flush_lsn`, `wal_status`, `safe_wal_size`, `two_phase`, `catalog_xmin`, `xmin` | yes | yes | yes | positions as int |
| `conflicting` | yes | yes | yes | |
| `invalidation_reason`, `inactive_since`, `failover`, `synced` | None | yes | yes | |
| `two_phase_at` | None | None | yes | |
| holder: `holder_backend_start` (µs precision), `holder_backend_start_us` (exact int), `holder_application_name`, `holder_backend_type`; `holder` = (pid, start µs) | yes | yes | yes | `pg_stat_activity` row of `active_pid` |
| `stats_reset`, `total_txns`, `total_bytes`, `spill_bytes` | yes | yes | yes | `pg_stat_replication_slots` |
| server: `system_identifier`, `timeline`, `timeline_source`, `in_recovery`, `position`, `receive_position`, `server_version_num`, `wal_level`, `wal_sender_timeout_ms`, `max_slot_wal_keep_size_mb`, `idle_replication_slot_timeout_s` (18+, else None), `hot_standby_feedback`, `primary_slot_name`, `read_at` | yes | yes | yes | |

- **The holder** (C2): the runtime role sees the `pg_stat_activity` row of a walsender of its own role, with
  `backend_start` to the microsecond, on a primary and on a standby, without `pg_read_all_stats` (test with
  `pg_recvlogical` as the consumer; `pg_has_role(…, 'pg_read_all_stats')` is false).
- **`wal_sender_timeout_ms`**: `pg_settings.setting × unit` in milliseconds (the server's unit is ms; 0 = no timeout,
  which package V refuses). Local servers: 60000.
- **Timeline**: on a primary from the current WAL file name (`timeline_source = 'wal'`). On a standby
  `pg_stat_wal_receiver.received_tli` is visible only with `pg_read_all_stats`; for the runtime role it is NULL, so
  the value is `pg_control_checkpoint().timeline_id` (`'checkpoint'`), which can lag behind a timeline switch until
  the next restartpoint. The exact current timeline of a standby comes from `IDENTIFY_SYSTEM`
  (`slot.timeline_history`).
- **`system_identifier`** is the same on a primary and all its physical standbys (test): it tells servers of
  different clusters apart, not one replica from another (H-047).

`slot.terminate_holder(node, name, deadline, *, expect=None) -> TerminateResult(outcome, terminated, facts)`:
reads the facts, then runs `pg_terminate_backend(active_pid, wait)` **in a statement that checks again that the slot's
`active_pid` and that backend's start time are the ones read**; repeats until the slot is inactive (`'inactive'`),
missing (`'missing'`), or — with `expect = (pid, start µs)` — held by another backend (`'other holder'`, nothing
terminated). `'holder unknown'`: the slot is active but its holder's start time is not visible to this role (a
backend of another role, without `pg_read_all_stats`); nothing is terminated and `drop(terminate=True)` raises
`SlotActive`. The terminate does not wait for the exit (`pg_terminate_backend(pid)`); the loop reads the facts again.
Never terminates a pid from anywhere else. Races tested: the holder exits by itself between the read and
the terminate (nothing is terminated); another session takes the slot in between (the stale read terminates
nothing; without `expect` the new holder of our slot is terminated in the next round, with `expect` the call returns
`'other holder'`).

`slot.timeline_history(node, timeline=None, *, deadline) -> TimelineHistory(timeline, entries, system_identifier,
position)` over a `replication=database` connection (opened from the node's settings when `node` is not one):
`IDENTIFY_SYSTEM`, then `TIMELINE_HISTORY <tli>` (timeline 1 has no history). psycopg 3 can do it with the simple
query protocol (`PgConnection.simple`, libpq `PQexec`); `cdc_v2_runtime` (REPLICATION) may.
`timeline.contains(history, position, timeline)`: true when `timeline` is the current one, or an ancestor and
`position ≤` its switch point; false for a position past the switch on the old timeline and for an unknown timeline.
The comparison is right for end-of-record positions (a commit end, `confirmed_flush_lsn`, the applied position), not
for a record's start such as `restart_lsn`.
Tested with a real promotion on the breakable pair: the old primary kept writing on timeline 1 after the standby was
promoted; a position before the switch → true, after it on timeline 1 → false, on timeline 2 → true.

**[test] slot identity** (design open point 6): see §7.

## 5. Heartbeat

**Built form: a row in a published heartbeat table of ours** (the recommended form of the task; not a logical
message), written as a transaction of its own on the primary:
`INSERT INTO cdc_v2.heartbeat (slot, marker, n, beat_at) … ON CONFLICT (slot) DO UPDATE SET marker = …, n = n + 1,
beat_at = now()` — one row per slot name. It arrives on the stream as `BEGIN`, `ROWS`, `END`; only an `END` moves the
applied position (C2), so an idle slot advances and so does a slot behind busy unpublished tables (test). A
non-transactional logical message would not do (it has no `END`); a transactional one would need the message prefix
in `message.prefix.include.list` and `pg_logical_emit_message` on the primary.

What the runtime role needs for it: `USAGE` on schema `cdc_v2` and `SELECT, INSERT, UPDATE` on `cdc_v2.heartbeat`
(nothing else; no `pg_logical_emit_message`).

What creates it: `heartbeat.ensure(setup, runtime_role, *, publication, deadline)`, run by the setup (bootstrap)
role, idempotent: `CREATE SCHEMA IF NOT EXISTS`, `CREATE TABLE IF NOT EXISTS` (a primary key, so no replica identity
change), the grants, and `publication.ensure` for the table. `heartbeat.setup_sql(runtime_role)` lists the
statements. The check for the attach step, with the runtime role: `heartbeat.check(conn, *, publication, deadline)
-> [problems]` (table missing; not in the publication — `publication.has_table`; this role may not write it). Empty
means ready. Without the table in the publication an idle source never moves and the marker never comes back.

`heartbeat.beat(primary, deadline, *, slot='cdc_v2_slot') -> HeartbeatWrite(slot, marker, n, xid, lower, upper,
position)`:

- `position` (= `upper`) is `pg_current_wal_insert_lsn()` read on the same connection **after COMMIT returned**, so
  the beat's commit end is ≤ `position`. The stream recognises the beat by position: an `END` whose `ack_position`
  (a commit end) is ≥ `position` belongs to a transaction that committed at or after the beat, and the slot decodes in
  commit order, so the beat has been decoded when that `END` arrives — also when the heartbeat table is outside
  `table.include.list`. When no other WAL is written between the commit and the read, the beat's own `END` has
  `ack_position == position` (measured: §7); otherwise the next transaction's `END` (at the latest the next beat)
  proves it.
- `marker` is a new UUID per write and `xid` the 64-bit transaction id (pgoutput and Debezium carry its low 32 bits);
  `lower ≤` start of the commit record `<` commit end `≤ upper` (tested against the decoded `C` message).
- Idempotent; two runs may beat at once: each sets its slot's row to its own marker. Cost: §7.

`heartbeat.standby_lag(primary, standby, *, deadline) -> StandbyLag(bytes_behind, replay_position,
receive_position, primary_position, seconds_since_replayed_commit, replay_lag_s, replay_paused,
recovery_min_apply_delay_ms)`: bytes = primary insert position (read first) − standby replay position; time = age of
the last replayed commit (meaningful while the heartbeat writes); `replay_lag_s` from `pg_stat_replication` needs
`pg_read_all_stats` or the walreceiver's role and is None for the runtime role (H-044).

## 6. Role split

| Role | Attributes | Grants | Calls |
|---|---|---|---|
| setup role (local: `cdc_v2_admin`; stage 2: the provider's admin role) | owns the replicated tables, or is a member of their owner | `CREATE` on the database (schema of the heartbeat) | `publication.ensure`, `publication.remove`, `replica_identity.set_full`, `heartbeat.setup_sql` |
| runtime role (`cdc_v2_runtime`) | `LOGIN REPLICATION`, not a superuser, no `pg_read_all_stats` | `USAGE` on each replicated schema and `SELECT` on each table (backfill); `SELECT, INSERT, UPDATE` on `cdc_v2.heartbeat`; optional: `EXECUTE` on `pg_log_standby_snapshot()` from a superuser | `connect`, `catalog.*`, `publication.state`, `slot.*` (create, drop, facts, terminate its own walsender, advance), `timeline_history`, `heartbeat.beat`, `standby_lag` |

Shown by test: the runtime role cannot run `publication.ensure` or `set_full` (`PermissionDenied`), can read
`publication.state`, can terminate the walsender of its own role holding our slot, can call `TIMELINE_HISTORY`, and
sees `pg_control_system()`/`pg_control_checkpoint()`.

## 6a. Pre-flight

`preflight.check(conn, *, deadline) -> Preflight(in_recovery, control_checkpoint_callable, checkpoint_next_xid,
control_system_callable, replication_role, wal_level, hot_standby_feedback, primary_slot_name,
recovery_min_apply_delay_ms, replay_paused, problems)`, run as the role that will use the node:

- `pg_control_checkpoint()` must be callable: the backfill reads its `next_xid` once per connection on the server it
  reads from (a standby that has just started can hide a committed transaction until it replays its next commit; the
  cut is used only when the snapshot's `xmax` is at or above that value). `checkpoint_next_xid` is the 64-bit value
  (`epoch << 32 | xid` from the `epoch:xid` text). Shown by test: the runtime role can call it on the primary and on
  the standby, on 18 and 16, with the default privileges (EXECUTE for PUBLIC). Where a provider has revoked it, the
  grant needed is `GRANT EXECUTE ON FUNCTION pg_catalog.pg_control_checkpoint() TO <runtime role>` on the primary
  (it replicates to the standbys); the test revokes it from PUBLIC, sees the problem named, grants it to the runtime
  role and sees it pass. The same for `pg_control_system()`.
- the REPLICATION attribute; `wal_level = logical`; on a standby `hot_standby_feedback = on`, a
  `primary_slot_name`, no `recovery_min_apply_delay`, replay not paused (H-042, H-044; test with feedback off).

## 7. Answers to the [test] points and measurements

Postgres 18.6 and 16.6 (`localpg_pg18.json`, `localpg_pg16.json`). Postgres 17 is not installed on this machine:
the 17 column list of `slot.facts` (`invalidation_reason`, `inactive_since`, `failover`, `synced` present,
`two_phase_at` absent) is written from the documentation and **not run**.

| Point | Answer | Evidence key |
|---|---|---|
| Bracket on a standby (H-008) | 0 violations in either direction over 125–160 observations of 400 DDLs per run, on 18 and 16; the negative control (upper bound read before the observing statement) was caught 230–290 times per run | `bracket.standby` |
| Bracket on a primary, concurrent writer | upper bound: 0 violations in every run. Lower bound: 0 violations in the committed runs and in 40 repeats on 18 (12,000 DDLs, 5,081 observations, `bracket_primary_repeats_pg18.json`). An earlier run of the same test on 18 was reported with 12 cases; no file of that run exists, and the repeats did not reproduce it, so that number is not evidence | `bracket.primary`, `bracket.primary_repeats` |
| Lower bound on a primary, commit held between its WAL record and its visibility | on 18 and 16: the observation's `b` and `a` both equal the end of the commit record, the table is not in the observation and `visible(xid)` is false; after the release the table is there and `visible(xid)` is true. The lower bound is not a guarantee on a primary; use `visible(xid)` | `bracket.primary_commit_waiting` in `primary_commit_waiting_pg18.json`, `primary_commit_waiting_pg16.json` |
| Observation cost, 1,000 tables × 30 columns | p50 ≈ 0.07 s, max ≈ 0.1 s on standby and primary (one round trip; local); a single observation of a few hundred names ≈ 0.006–0.01 s | `observe_cost`, `bracket.*.observe_p50_s` |
| Session settings vs `ALTER ROLE/DATABASE … SET` | held, on SQL and replication connections, primary and standby | `session_settings` |
| Deadlines with the fault proxy (grace 0.5 s) | black hole: connect (deadline 1.5 s) 2.0 s, statement (1.0 s) 1.5 s, idle in transaction (1.0 s) 1.5 s and the server session gone after ≈ 2.1 s; reset and refused: < 1 ms; 0.2 s delay each way: statement 0.40 s; `pg_sleep(30)` with 0.5 s: 0.51 s | `proxy`, `deadlines` |
| Recovery conflict | `RecoveryConflict` (40001) after `max_standby_streaming_delay` = 30 s | `errors.recovery_conflict` |
| Slot creation on a standby, idle primary | `write` nudge (runtime role): 2.3–21.7 s across runs (one or two background-writer cycles of 15 s; 6.9 s in the final 18 run); `standby_snapshot` granted: 0.02–0.2 s; no nudge on an idle primary (autovacuum off): times out at the deadline, nothing left (an attempt during which the primary wrote WAL anyway is retried and recorded) | `slot_create.*` |
| … long transaction on the primary | times out at the deadline (6 s), report names `oldest_running_xid` = the open transaction; succeeds 11–15 s after its commit | `slot_create.long_transaction` |
| … prepared transaction | times out, `prepared_transactions = 1`; succeeds ≈ 7.6–8 s after `COMMIT PREPARED` | `slot_create.prepared_transaction` |
| … client killed while waiting (creation verified waiting at the kill) | with `client_connection_check_interval` no slot appears; negative control without it: the slot appears later | `slot_create.killed_client` |
| `pg_control_checkpoint()` as the runtime role | callable on primary and standby, 18 and 16 (default EXECUTE for PUBLIC); the grant if revoked: see §6a | `preflight` |
| Holder visible to the runtime role | yes, primary and standby, `backend_start` to the µs, without `pg_read_all_stats` | `slot_facts.holder_visible_to_runtime_*` |
| `pg_replication_slot_advance` in the facts | `confirmed_flush_lsn` moved to the target exactly | `slot_facts.advance_seen` |
| Invalidation by `max_slot_wal_keep_size` (standby slot, restartpoint) | 18: `wal_status = lost`, `invalidation_reason = wal_removed`, `conflicting = false`, `restart_lsn` NULL; **16: `wal_status = lost`, `conflicting = true`**, no reason column | `invalidation.max_slot_wal_keep_size` |
| Idle timeout (18, `idle_replication_slot_timeout = 1s`) | primary slot and standby slot both invalidated (`idle_timeout`); the standby slot at a restartpoint | `invalidation.idle_timeout_pg18` |
| Timeline history after a real promotion | switch point found by the runtime role over `replication=database`; `contains`: before → true, after on the old timeline → false, new timeline → true | `timeline.promotion` |
| Publication add under writers, `next_xid` | every transaction with id ≥ `next_xid` decoded (≈ 446 of ≈ 893); see the finding on the open transaction below | `publication.add_under_writers` |
| Heartbeat | decoded, recognised by marker and xid, commit inside `[lower, upper]`; own `END` == `position` in 4 of 4 beats; ≈ 230 WAL bytes per write; table stays at one row per slot (3 pages after 200 writes) | `heartbeat.*` |

**Slot identity** (design open point 6). No fact identifies one incarnation of a slot, on 16 or 18. After a drop and a
recreate under the same name: `restart_lsn`, `confirmed_flush_lsn` and `catalog_xmin` are those of the new creation
point (later than the original's, but the same slot also moves forward, so a later value proves nothing);
`inactive_since` (17+) is just the time of the last release; `stats_reset` is NULL on both; the statistics
(`total_txns`, `total_bytes`) start again from 0 — but they are also reset by `pg_stat_reset_replication_slot()` and
lost by a crash restart, so a drop to 0 is a hint, not a proof. The slot's state file is not readable through SQL
without superuser rights. So continuity rests on what design section 8 and plan 6.2 already say: same system
identifier, applied position on the timeline history, a valid slot of our plugin and database, and
`confirmed_flush_lsn ≤` applied position; a recreated slot that is not ahead has lost nothing.

**Findings that differ from the research documents, the hazard list or the design**

1. Design 7.5 says that changes written before a table joins the publication are not published even if their
   transaction commits later. Measured on 18 and 16: a transaction that wrote a row **before** the
   `ALTER PUBLICATION … ADD TABLE` and another after it, and committed after it, had **both** rows decoded. The rule
   "first snapshot with `xmin ≥ next_xid`" stays correct (that transaction's id is below `next_xid`, so the snapshot
   waits for it and contains its rows; the cut then treats its stream rows as inside).
2. On 16, a standby slot invalidated by `max_slot_wal_keep_size` shows `conflicting = true` (the research note
   says `conflicting` is true only for recovery conflicts). On 18 it is false with `invalidation_reason =
   wal_removed`. `SlotFacts.invalid` treats any of `wal_status = lost`, a reason, or `conflicting` as invalid.
3. On a primary the observation's lower bound is not a guarantee: a commit that waits for a synchronous standby
   has its record in the WAL below `b` and is not visible (§2, §7). With a concurrent writer and no synchronous
   standby the window is the time between the flush of the commit record and the end of the transaction in the
   process array; 40 repeats of the bracket test did not hit it.
4. `pg_stat_wal_receiver` columns other than `pid` are hidden from the runtime role, so a standby's timeline in
   `slot.facts` comes from `pg_control_checkpoint()` and can lag behind a timeline switch until the next
   restartpoint; `IDENTIFY_SYSTEM` gives the exact one.
5. `system_identifier` is equal on a primary and its standbys: it does not tell one replica from another (H-047).
6. Slot creation that is abandoned by a dead client finishes later unless `client_connection_check_interval` is set
   (fixed in `create`). A creation abandoned on a dead network path (no FIN reaches the server) can still finish
   later; the result is a fresh, unused slot of our name whose `confirmed_flush_lsn` is its consistent point.
7. The background writer logs a running-transactions record at most every 15 s after important WAL, and while it
   hibernates it wakes only every 10 s, so after a write the record can take ≈ 25 s (measured on 16: a
   `Standby/RUNNING_XACTS` record of 50 bytes ≈ 20 s after a CHECKPOINT, identified with `pg_walinspect`; the
   `write` nudge took 21.7 s once). With `pg_log_standby_snapshot` granted it is below 1 s. Package V's
   slot-creation deadline must be well above 30 s (the default of 120 s is). "An idle primary" in a test therefore
   means no new WAL for more than 25 s.
8. `idle_replication_slot_timeout` (18) invalidates ordinary slots on a standby too (at a restartpoint), as H-037
   says; reading the setting is in `ServerFacts.idle_replication_slot_timeout_s`.
9. Heartbeat statistics: `pg_stat_user_tables` update counters were not yet flushed when read (0); the table size
   (3 pages after 200 writes) is the evidence that it stays small.

## 8. Contract notes

Readings of C7 where it left something open (simplest reading taken):

1. Every function takes an already open `PgConnection` (`node`, `conn`, `primary`), not a target name; `connect` is
   the only place where names are resolved. `connect(target, deadline, *, replication=False, settings=None,
   application_name=None)`: `target` may also be a `NodeSettings`; names come from `pg.configure(SourceSettings)`.
2. Every call takes a `deadline` (keyword where C7 shows none), as C0 requires.
3. `slot.facts` returns `None` for a missing slot (the task's signature); `slot.server_facts` gives the server part.
4. `slot.create` takes `primary=` (a connection to the primary) for the running-transactions request, and returns
   `CreatedSlot` (the consistent point and how it was reached).
5. `slot.terminate_holder(node, name, deadline, *, expect=None)` returns `TerminateResult`; `expect` lets the
   attach step terminate only the previous engine.
6. `slot.timeline_history(node, timeline=None, *, deadline)` returns `TimelineHistory` (with the system identifier
   and position of `IDENTIFY_SYSTEM`).
7. `catalog.tables(conn, include, *, publication, deadline)` and `catalog.observe(node, names, *, publication,
   deadline)`: `names` are the replicated names (absent ones are reported in `absent`); tables in the publication are
   always included.
8. The observation reads its facts in one statement instead of a REPEATABLE READ transaction: the same snapshot
   semantics with one round trip (§2).
9. `heartbeat.beat(primary, deadline, *, slot='cdc_v2_slot')`: one row per slot name, so that two pipelines on one
   source do not share a marker row.
10. `publication.ensure` and `replica_identity.set_full` need the setup role (ownership); the runtime role gets
    `PermissionDenied`.
11. Tests use the TCP fault proxy on this package's own ports (55644–55647 / 55652–55655) by setting
    `cdc_v2.testing.netproxy.PORT_FIRST/PORT_LAST` in the test: `NetProxy` refuses ports outside 55440–55459.
    A port-range parameter in `NetProxy` would remove that.
12. Rule K (package K): the new fields have defaults (`SourceTable.primary_key_deferrable = False`,
    `TableFacts.pk_deferrable = False`), so values built by hand in other packages' tests keep working; such a
    value must then carry `key_attnums` that follow the rule itself. `identity_problem` takes `pk_deferrable` as
    a keyword (default `False`).

**What packages V, B, S, T and Y must know that C7 does not say**

- V: use `SlotFacts.holder` (pid, start µs) for the attach rule; `terminate_holder(…, expect=…)`; `'holder unknown'`
  means another role holds the slot; `SlotCreateTimeout.report`; a leftover slot of our name after an abandoned
  creation is a fresh slot (`confirmed_flush_lsn` = consistent point); `wal_sender_timeout_ms` is in the facts; no
  slot identity exists (above); `timeline.contains` wants end-of-record positions.
- B: read connections pin the session settings of §1 (the backfill's text form); `row_security = off` makes an RLS
  filtered read fail (H-039); `RecoveryConflict` is "read again"; `conn.guard(deadline, …)` wraps raw psycopg COPY.
  `SourceTable.relfilenode` and `key_attnums` (without INCLUDE columns).
- S: `heartbeat.beat` returns `position`; recognise a beat by an `END` with `ack_position ≥ position`;
  `heartbeat.check` before the stream starts; xids are compared on their low 32 bits.
- T: `catalog.observe` on the standby gives the bracket; on a primary use `visible(xid)`; `TableFacts.identity`
  excludes `relfilenode`; `published` includes membership through a partition ancestor; `publication.has_table`.
  A primary key becomes deferrable or immediate only by drop and add (§3): same OID and `relfilenode`,
  `TableFacts.has_key`, `key_attnums` and `pk_deferrable` change, `diff` gives `key_changed`.
- Y: the session settings table of §1 (IntervalStyle `postgres`, DateStyle `ISO, YMD`, `extra_float_digits = 3`,
  `bytea_output = hex`, TimeZone UTC, `lc_monetary = C`).

## 9. Crash points and fault sites

Declared here (C1): fault sites `pg.connect` (in the connecting thread, so a `block` rule is bounded by the connect
deadline) and `pg.execute` (inside the statement guard, so an injected `OSError` maps to `ConnectionLost`); crash
points `pg.slot_created`, `pg.slot_dropped`, `pg.holder_terminated`, `pg.publication_changed`,
`pg.replica_identity_set`, each after the step that changed state on the source. They are reached only by the
callers' crash suites (V, T, S); a declared point that their runner never reaches is a failure of that suite.

## 10. Running the tests

```sh
uv run -q pytest -q tests/pg_source/test_pg_hermetic.py                        # default run (T0): ≈ 1 s
uv run pytest -m localpg tests/pg_source                                        # Postgres 18: pg-p (55640) + pg-p2
CDC_V2_P_PG_MAJOR=16 uv run pytest -m localpg tests/pg_source                   # Postgres 16: pg-p2 (55648)
CDC_V2_P_EVIDENCE=1 …                                                           # also writes evidence/p3/source/
CDC_V2_P_EVIDENCE=1 CDC_V2_P_EVIDENCE_NAME=x.json … -k <test>                   # a partial run names its own file
CDC_V2_P_BRACKET_RUNS=40 … -k test_bracket_on_primary                           # repeats the primary bracket run
CDC_V2_P_EVIDENCE=1 CDC_V2_P_EVIDENCE_NAME=replica_identity_keys_pg18.json … tests/pg_source/test_pg_keys.py  # H-015
```

Package K (rule K) writes its evidence to `evidence/p3/key_rule/`: the variable `CDC_V2_P_EVIDENCE_DIR` names a
directory under `evidence/p3` (here `key_rule`); the files are `localpg_pg18.json` and `localpg_pg16.json` (the
whole suite, run with `CDC_V2_P_EVIDENCE_NAME` set to that name).

The `localpg` suite builds or starts its own pairs through `tools/localpg`, runs ≈ 9–11 minutes per version (the
recovery-conflict test waits 30 s; slot-creation tests wait for an idle primary, 26 s each), removes every schema,
publication and slot it created, and stops its clusters at the end. Tests that reconfigure or promote use the
breakable pair (`pg-p2`), which is rebuilt after each of them. Do not run the two versions at the same time (they
share `pg-p2`).
