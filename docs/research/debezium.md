# Stock Debezium for cdc_flight_v2: version pin, standby decoding, delivery contract

Research note for PLAN phase P5 (started early). Status: **draft, 2026-11-09**. Author: hazard researcher.

Every non-obvious claim carries one of three marks:

- **[doc]** documented, with a URL;
- **[src]** read in source, with file and version (Debezium sources were read in a shallow clone of tag
  `v3.7.1.Final`, commit `4b4b894c`; PostgreSQL sources were read on branch `REL_18_STABLE` as of 2026-11-09 and,
  where stated, `REL_17_STABLE`);
- **[test]** to be verified by test (the experiment is named).

Paths written `dbz:…` are relative to the Debezium repository root
(<https://github.com/debezium/debezium/tree/v3.7.1.Final>); paths written `pg:…` are relative to the PostgreSQL
repository root (<https://github.com/postgres/postgres/tree/REL_18_STABLE>). Debezium's issue tracker moved from Jira
to GitHub in 2025; current issues are `debezium/dbz#N` = <https://github.com/debezium/dbz/issues/N>, old ones keep
their `DBZ-N` Jira key.

---

## 1. Release line and pin

### 1.1 What exists on 2026-11-09

| Line | Latest | Released | Status |
|---|---|---|---|
| 3.8 | 3.8.0.Alpha1 | 2026-11 | development, not for use |
| **3.7** | **3.7.1.Final** | **2026-11-06** | **latest stable** |
| 3.6 | 3.6.3.Final | 2026 | previous stable |

Sources: tags `v3.7.0.Final`, `v3.7.1.Final`, `v3.8.0.Alpha1` in <https://github.com/debezium/debezium> (listed with
`git ls-remote --tags`); "3.7 latest stable, 3.6 stable, 3.8 development" and the release dates on
<https://debezium.io/releases/3.7/> and <https://debezium.io/releases/3.7/release-notes> **[doc]**.

### 1.2 Pin: `3.7.1.Final`

Reasons, in order of weight:

1. **Data-loss fix that only 3.7.1 has.** debezium/dbz#2889 ("changes lost on restart when `lsn_events_processed` is 1,
   or when the offset is stored at a transactional logical message") was fixed in 3.7.1.Final; the reporter states
   that 3.6.x "loses (1)'s rows for any count" **[doc]** (<https://github.com/debezium/dbz/issues/2889>, 3.7 release
   notes). Our design avoids the code path entirely (§3.4), but the pin should not carry a known silent-loss bug.
2. Standby fixes that are in 3.7.0+ (and partly back-ported to 3.6.2): read-only incremental snapshot on a hot
   standby (debezium/dbz#2431, debezium/dbz#1080) **[doc]** (3.6 and 3.7 release notes).
3. PostgreSQL 18 is in the tested matrix of 3.7 **[doc]** (<https://debezium.io/releases/3.7/>: "PostgreSQL 14, 15, 16,
   17, 18 … decoderbufs and pgoutput … Driver 42.7.13").
4. 3.7.0 → 3.7.1 fixed regressions we would otherwise hit: `JdbcOffsetBackingStore` + transaction metadata
   `ClassCastException` (debezium/dbz#2786), infinity-date inconsistency (debezium/dbz#2721) **[doc]** (3.7 release
   notes).

Pin policy proposal: pin the exact Maven version `3.7.1.Final` and the artifact SHA-256 sums in our build file; a
change of pin is a change to correctness code and restarts the fuzzer counters (SPEC §4).

### 1.3 Java

- Connectors and core libraries (this includes `debezium-embedded` and `debezium-connector-postgres`) are compiled for
  **Java 17** (`<debezium.java.connector.target>17</debezium.java.connector.target>` in `dbz:pom.xml`) **[src]**.
- Debezium Server, Operator, Outbox and the Quarkus extension need **Java 21**
  (`<debezium.java.specific.target>21</…>`, same file) **[src]**; the release page states "17+ for connectors; 21+ for
  Debezium Server, Operator, Outbox and Quarkus extension" **[doc]** (<https://debezium.io/releases/3.7/>).
- The project builds with JDK 21 (`<debezium.java.source>21</…>`) **[src]**. CI fixes for Java 25 appear in the 3.7
  notes (debezium/dbz#2488), i.e. 25 is being exercised but is not the baseline **[doc]**.

**Recommendation:** run a Temurin/OpenJDK **21 LTS** JRE on the Flight (one JRE serves both candidate architectures A
and B). The P2 spike measures the download and start-up time of that JRE (PLAN §2 A 1).

### 1.4 Artifacts

**(b) Embedded engine (architecture B, expected choice)** — Maven coordinates:

```
io.debezium:debezium-embedded:3.7.1.Final
io.debezium:debezium-connector-postgres:3.7.1.Final
```

Resolving these two with Maven (`dependency:copy-dependencies`, runtime scope) yields **72 jars, 38 MB** on disk;
the largest are `kafka-clients-4.3.1` (10 MB; the engine uses Kafka Connect APIs and `connect-runtime` but needs no
Kafka broker), `zstd-jni`, `snappy-java`, `protobuf-java`, `jackson-databind`, `postgresql-42.7.13` (1.2 MB),
`connect-runtime-4.3.1` (measured 2026-11-09 against Maven Central). `debezium-connector-postgres` itself is 437 KB,
`debezium-embedded` 110 KB.

**(a) Debezium Server (architecture A)** — `io.debezium:debezium-server-dist:3.7.1.Final:tar.gz`, **747 MB**
(HTTP `Content-Length` on Maven Central, 2026-11-09). It bundles every connector and sink. It needs Java 21. Two
Server regressions were fixed only in 3.7.1 ("keeps running after engine has failed", debezium/dbz#3010; "silently
ignores a connector.class not in distribution", debezium/dbz#2801) **[doc]**. The size alone argues for B: the Flight
has no JVM and no durable disk, so every run (every minute when not leader, PLAN §2 A 2) would have to fetch or cache
the artifacts; 38 MB of jars from a pinned list is a much smaller start-up budget than 747 MB.

### 1.5 Newest PostgreSQL with `pgoutput`

PostgreSQL **18** (tested matrix, <https://debezium.io/releases/3.7/>) **[doc]**. Note that Debezium speaks
**pgoutput protocol version 1 only**: `PgOutputMessageDecoder.defaultOptions` hard-codes
`withSlotOption("proto_version", 1)` and adds `messages=true` on servers ≥ 14
(`dbz:debezium-connector-postgres/src/main/java/io/debezium/connector/postgresql/connection/pgoutput/PgOutputMessageDecoder.java`)
**[src]**. Consequences are in §5 (no streaming of in-progress transactions, no two-phase, no binary mode); the request
to lift this is open (debezium/dbz#1359, debezium/dbz#2898) **[doc]**.

Since a 2026 minor release, PostgreSQL refuses output plugins that are not listed in the new GUC
`output_plugin_libraries`; the default `'pgoutput, test_decoding'` covers us **[doc]**
(<https://www.postgresql.org/docs/18/runtime-config-replication.html>), **[src]** (`pg:src/backend/replication/logical/logical.c`,
`check_output_plugin_libraries`). Stage 2 must check the value on PlanetScale **[test]**.

---

## 2. Decoding from a standby with stock Debezium

### 2.1 Verdict

**Supported, with conditions that our code must meet.** Stock Debezium 3.7.1 streams from a PostgreSQL ≥ 16 hot
standby. It has done so since **2.5.0.Beta1 (2023-12-04)**, DBZ-7181 "Support logical decoding from Postgres 16
stand-bys" (<https://issues.redhat.com/browse/DBZ-7181>; commit `0ff6c730d6` "Avoiding queries not available in
recovery mode") **[doc]**; the Debezium documentation says "When you deploy Debezium with a PostgreSQL 16 or later
cluster, you can set up logical replication slots on replica servers" **[doc]**
(<https://debezium.io/documentation/reference/stable/connectors/postgresql.html#postgresql-supported-topologies-pg16>).
3.2.0.Alpha1 added DBZ-8743 "Prevent write operations in PostgreSQL in read-only mode" (skip publication DDL when
`pg_is_in_recovery()`) **[doc]** (3.2 release notes, commit `369b386f5c`).

However: **upstream has no integration test for streaming from a standby or across failover** — debezium/dbz#1162
"Add integration tests for streaming off replica and failover [DBZ-6410]" is open since 2023 **[doc]**
(<https://github.com/debezium/dbz/issues/1162>). Our own test suite is therefore the only evidence that it works; every
statement below marked [src] needs a matching T1 test.

What we must provide around stock Debezium (each point is explained in 2.2–2.9):

1. The publication is created and maintained **on the primary** by our code, before the slot exists;
   Debezium runs with `publication.autocreate.mode=disabled` (2.3).
2. The slot is created **by our code** on the standby (not by Debezium), with a deadline, while our primary heartbeat
   guarantees `xl_running_xacts` records (2.4). Debezium only ever finds an existing slot.
3. `heartbeat.action.query` is **not** used (it would run on the standby and fail); the primary heartbeat is our own
   writer (R8) (2.6).
4. `slot.failover` is irrelevant on a standby (silently ignored) (2.7).
5. Slot health (`invalidation_reason`, `conflicting`, `wal_status`) is checked **by our code** at every acquisition and
   periodically: Debezium does not look at it and retries forever on an invalidated slot (2.8).
6. The walreceiver must be streaming when Debezium starts, or Debezium's start-up can fail on a NULL LSN (2.5) **[test]**.

### 2.2 What stock Debezium does when it is connected to a standby (3.7.1, read in source)

| Step | Code (3.7.1) | On a primary | On a standby |
|---|---|---|---|
| Detect recovery | `PostgresReplicationConnection.isReadOnlyDb()` runs `select pg_is_in_recovery()` | false | true |
| Current WAL position | `PostgresConnection.currentXLogLocation()`: `select (case pg_is_in_recovery() when 't' then pg_last_wal_receive_lsn() else pg_current_wal_lsn() end)` | `pg_current_wal_lsn()` | `pg_last_wal_receive_lsn()` — **NULL** if streaming replication "is disabled, or if it has not yet started" ([doc](https://www.postgresql.org/docs/18/functions-admin.html)); the result is passed to `LogSequenceNumber.valueOf(String)` → see 2.5 |
| Current xid | `PostgresConnection.currentTransactionId()`: `case pg_is_in_recovery() when 't' then '0'::xid8 else pg_current_xact_id() end` → returns `null` on a standby | real xid | `null` (only used for the offset's `txId`) |
| Publication | `initPublication()` (see 2.3) | may create/alter | never writes (except `no_tables`, see 2.3) |
| Replica identity | `initReplicaIdentity()` runs `ALTER TABLE … REPLICA IDENTITY` only if `replica.identity.autoset.values` is set | may alter | **would fail**: do not set this option |
| Slot lookup | `select * from pg_replication_slots where slot_name = ? and database = ? and plugin = ?` | | works; no check of `invalidation_reason`/`conflicting` |
| Slot creation | `CREATE_REPLICATION_SLOT "<slot>" LOGICAL pgoutput [( FAILOVER )]` over the replication protocol, `statement_timeout` = `create.slot.command.timeout` (internal option, default **90 s**), retried on 57014/55P03 up to `slot.max.retries` (default 6) every `slot.retry.delay.ms` (10 s) | immediate unless long transactions | waits for an `xl_running_xacts` record from the primary (2.4) |
| Failover flag | `useFailOverSlot()`: only if `slot.failover=true`, server ≥ 17 **and** not in recovery; otherwise logs at DEBUG "Can't create a failover on a replica server" | `( FAILOVER )` | flag dropped silently |
| Start position | `IDENTIFY_SYSTEM` `xlogpos`, then `min(slot.confirmed_flush_lsn, xlogpos)` if no offset is stored | | `xlogpos` on a standby is its flush position |
| Streaming | `START_REPLICATION SLOT … LOGICAL <lsn> ("proto_version" '1', "publication_names" '<pub>', "messages" 'true')` | | same; decoding is limited to the standby's **replay** position (see 2.9) |
| LSN feedback | `setFlushedLSN`/`setAppliedLSN` + `forceUpdateStatus()` on `commitOffset` (§3) | | same; advances the standby slot only |
| Heartbeat query | `heartbeat.action.query` on a separate JDBC connection to **the same host** (`connectorConfig.getJdbcConfig()`) | works | fails with 25006 (read-only); logged as ERROR and ignored, not fatal (`DatabaseHeartbeatImpl`) |
| Incremental snapshots | `read.only=true` switches to `PostgresReadOnlyIncrementalSnapshotChangeEventSource` (uses `pg_current_snapshot()` watermarks; `pg_current_xact_id()` guarded since debezium/dbz#2431) | | works since 3.6.2/3.7.0 — **we do not use it** (R3) |
| Initial snapshot | exported snapshot from slot creation, `SET TRANSACTION SNAPSHOT` | | not used by us (R3; `snapshot.mode=no_data`) |

Files: `connection/PostgresReplicationConnection.java`, `connection/PostgresConnection.java`,
`PostgresConnectorTask.java`, `PostgresReadOnlyIncrementalSnapshotChangeEventSource.java`,
`dbz:debezium-connector-common/src/main/java/io/debezium/heartbeat/DatabaseHeartbeatImpl.java`, all under
`dbz:debezium-connector-postgres/src/main/java/io/debezium/connector/postgresql/` unless stated, tag `v3.7.1.Final`
**[src]**.

### 2.3 Publication handling on a read-only server

Publications are catalog objects; they can be created only on the primary and reach the standby through physical
replication (Morling, <https://www.morling.dev/blog/logical-replication-from-postgres-stand-by-servers-debezium-and-failover-slots/>:
"publications (unlike replication slots) can only be created on a primary server") **[doc]**.

`PostgresReplicationConnection.initPublication()` in 3.7.1, per `publication.autocreate.mode`, **when the connected
server is in recovery** **[src]**:

| Mode | Publication missing | Publication present |
|---|---|---|
| `disabled` | throws `ConnectException("Publication autocreation is disabled, please create one and restart the connector.")` | used as is |
| `all_tables` (default) | logs "The Postgres server in stand by mode, skip create statement execution" and **continues without a publication** | used as is |
| `filtered` | calls `validatePublications()` | `puballtables = true` ⇒ throws; otherwise calls `validatePublications()` |
| `no_tables` | **executes `CREATE PUBLICATION` anyway** ⇒ fails on a standby | used as is |

Two defects make every mode except `disabled` unsafe on a standby:

1. **`all_tables` with a missing publication fails silently on PostgreSQL 18.** Debezium continues; `pgoutput` on
   PostgreSQL 18 no longer errors on an unknown publication but emits `WARNING: skipped loading publication "…"` /
   "The publication does not exist at this point in the WAL." and **publishes nothing**
   (`pg:src/backend/replication/pgoutput/pgoutput.c`, `LoadPublications`, `GetPublicationByName(pubname, true)`)
   **[src]**. On PostgreSQL 17 the same function uses `missing_ok = false` and decoding fails with "publication … does
   not exist" (`REL_17_STABLE`) **[src]**. On 18 the result is a connector that runs, acknowledges WAL, and delivers
   no changes. → hazard H-034.
2. **`filtered` on a standby has inverted logic.** `validatePublications()` collects the publication's tables and the
   captured tables and then does `if (dbTableNamesHashSet.equals(tableNames)) throw new SQLException("Database
   replication is not up to date");` — it fails exactly when the publication is correct and passes when it is not
   **[src]** (`PostgresReplicationConnection.java`, v3.7.1.Final). To be confirmed by test and reported upstream
   **[test]**.

**Rule:** `publication.autocreate.mode=disabled`, publication created and maintained by our code on the primary
(SPEC 2.3 already assigns publication maintenance to the Flight). `disabled` is the only mode that fails loudly when
the publication is missing.

Additional publication facts that constrain the design:

- `FOR ALL TABLES` and `FOR TABLES IN SCHEMA` require a superuser; adding a table requires owning it **[doc]**
  (<https://www.postgresql.org/docs/18/sql-createpublication.html>). PlanetScale grants no superuser **[doc]**
  (<https://planetscale.com/docs/postgres/postgres-compatibility.md>). So on PlanetScale the publication is a table
  list maintained with `ALTER PUBLICATION … ADD TABLE` by a role that owns (or is a member of the owner of) every
  table **[test]** in stage 2. New tables therefore join the publication only after discovery; rows written before
  the `ADD TABLE` reaches the WAL are not streamed and are covered by the per-table snapshot that SPEC 2.3 already
  requires.
- pgoutput evaluates publication membership with the historic catalog snapshot of the WAL position being decoded,
  so `ADD TABLE` takes effect at its own LSN, not at the time Debezium sees it **[doc]**
  (<https://www.postgresql.org/docs/18/logicaldecoding-explanation.html>), **[test]**.

### 2.4 Slot creation on a standby

- Creating a logical slot needs a `xl_running_xacts` record; on a standby it "has to be obtained from primary. Thus,
  slot creation may need to wait for some activity to happen on the primary. If the primary is idle, creating a
  logical slot on standby may take noticeable time. This can be sped up by calling the `pg_log_standby_snapshot`
  function on the primary." **[doc]** (<https://www.postgresql.org/docs/18/logicaldecoding-explanation.html>).
- The primary's bgwriter logs that record at most every 15 s (`LOG_SNAPSHOT_INTERVAL_MS 15000`) and **only if
  "important" WAL was written since the last one** (`pg:src/backend/postmaster/bgwriter.c`) **[src]**; on a standby
  the slot builder cannot request one (`SnapBuildWaitSnapshot`: "During recovery we can't enforce that, so we'll have
  to wait", `pg:src/backend/replication/logical/snapbuild.c`) **[src]**.
- Long-running transactions on the primary extend the wait: the snapshot builder must see every transaction that was
  running at the first `xl_running_xacts` finish **[src]** (snapbuild.c state machine).
- Debezium bounds `CREATE_REPLICATION_SLOT` with `create.slot.command.timeout` (90 s, internal option) and then
  retries; meanwhile the walsender disables its own timeout during creation
  (`pg:src/backend/replication/walsender.c`, `CreateReplicationSlot`: `last_reply_timestamp = 0`) **[src]**.
- `pg_log_standby_snapshot()` is restricted to superusers by default (it is in the admin function family whose
  members are "restricted to superusers by default, but other users can be granted EXECUTE") — on PlanetScale it is
  probably not callable **[test]** (stage 2: `SELECT pg_log_standby_snapshot()` as the runtime and default roles).

**Rule:** our code creates the slot on the standby itself — `SELECT * FROM pg_create_logical_replication_slot(
'cdc_v2_…', 'pgoutput')` or `CREATE_REPLICATION_SLOT … LOGICAL pgoutput (SNAPSHOT 'nothing')` (or `'export'` if the
backfill hand-off imports the snapshot) — with a deadline, after verifying that the primary heartbeat (default 10 s,
SPEC 4.4) is running, so an `xl_running_xacts` record is guaranteed within about 15 s. A creation that exceeds its
deadline is a reportable condition (long transaction on the primary), never a hang. → H-040.

### 2.5 Calls that do not work in recovery, and what replaced them

| Call | In recovery | Debezium 3.7.1 |
|---|---|---|
| `pg_current_wal_lsn()` | ERROR "recovery is in progress" | replaced by `pg_last_wal_receive_lsn()` (DBZ-7181) |
| `pg_current_xact_id()` / `txid_current()` | ERROR (cannot assign xid) | replaced by `'0'::xid8` / `0` → `null` txId |
| `pg_logical_emit_message()` | ERROR (read-only) | only used by `heartbeat.action.query` if the user configures it; not used internally |
| `CREATE/ALTER PUBLICATION`, `ALTER TABLE … REPLICA IDENTITY` | ERROR (read-only) | skipped when `isReadOnlyDb()` except `no_tables` and `replica.identity.autoset.values` |
| signal table `INSERT` (incremental snapshot watermarks) | ERROR | avoided by `read.only=true` |
| `pg_replication_slot_advance()` | allowed for logical slots on a standby **[test]** | used only with `offset.mismatch.strategy=trust_offset` (we leave it at `no_validation`) |

**Open defect, to be verified:** `pg_last_wal_receive_lsn()` returns NULL "if streaming replication is disabled, or
if it has not yet started" **[doc]** (<https://www.postgresql.org/docs/18/functions-admin.html>). Debezium passes the
string to `LogSequenceNumber.valueOf(rs.getString(1))` without a null check (`PostgresConnection.currentXLogLocation`)
**[src]**; with pgjdbc 42.7.13 this is expected to throw a `NullPointerException` during
`PostgresOffsetContext.initialContext` (called at the start of every streaming phase without a stored offset) **[test]**:
restart a standby with `primary_conninfo` pointing at a closed port, connect Debezium, observe. If confirmed, the guard
"walreceiver is `streaming` (`pg_stat_wal_receiver.status`) before starting the engine" prevents it → H-041.

### 2.6 Heartbeats in replica mode

- `heartbeat.interval.ms` (no database write; emits a heartbeat *record* carrying the current offset) works
  unchanged on a standby **[src]**. We use it to give the consumer a chance to acknowledge during idle periods (§3).
- `heartbeat.action.query` runs on a second JDBC connection built from the same `database.*` settings, i.e. **on the
  standby**, and fails with SQLSTATE 25006; `DatabaseHeartbeatImpl` logs "Could not execute heartbeat action" and
  continues **[src]**. Do not configure it in replica mode.
- The primary heartbeat that keeps the slot advancing (SPEC 4.4) and proves end-to-end liveness (SPEC 4.6) is our own
  writer on a separate primary connection (R8, R16): an `UPDATE` (or `INSERT … ON CONFLICT`) of a one-row table that is
  **in the publication**, every 10 s. Being in the publication matters: pgoutput skips transactions that touch no
  published table and Debezium only flushes LSNs of messages it receives (`lsn.flush.mode=connector`), so without a
  published heartbeat a busy-but-unreplicated workload makes retained WAL grow without bound (Debezium logs a warning
  after 10,000 filtered events: `PostgresStreamingChangeEventSource.maybeWarnAboutGrowingWalBacklog`) **[src]**.
- An alternative that needs no table: a *transactional* `pg_logical_emit_message(true, 'cdc_v2_hb', …)` on the
  primary, which Debezium delivers as a message event (with `message.prefix.include.list`). Two restart bugs involve
  offsets stored at transactional messages (debezium/dbz#2889 fixed in 3.7.1; debezium/dbz#2901 open) **[doc]** — they
  do not affect us if we run without stored offsets (§3.4), but the heartbeat table is the simpler, bug-free choice.

### 2.7 `slot.failover` and failover slots

- `slot.failover=true` makes Debezium add `( FAILOVER )` only on a PostgreSQL ≥ 17 **primary**
  (`useFailOverSlot()`) **[src]**; Debezium documents it as Technology Preview **[doc]** (3.0.5, DBZ-8412; docs
  "Enable failover slots").
- PostgreSQL does not synchronise slots to cascading standbys: "we do not support syncing slots to cascading
  standbys" (`pg:src/backend/replication/walsender.c`, `PhysicalWakeupLogicalWalSnd`) **[src]**. A slot created on a
  standby is therefore **never** a failover slot and exists only on that one server: losing that server loses the
  slot. SPEC 4.1 already plans for this (attach to another replica, new slot, automatic backfill).
- In primary mode (non-default) our code should create the slot itself with `failover = true` (5th argument of
  `pg_create_logical_replication_slot`, or `CREATE_REPLICATION_SLOT … (FAILOVER)`) and verify `failover`/`synced` at
  every acquisition (SPEC 4.1); `slot.failover=true` stays set on the Debezium side as a second line.

### 2.8 Invalidated slots: Debezium does not notice

Debezium reads `pg_replication_slots` only for `confirmed_flush_lsn`/`restart_lsn`/`xmin`; it never reads
`invalidation_reason`, `conflicting` or `wal_status` **[src]** (grep of the 3.7.1 Postgres connector). When the slot
is invalidated, `START_REPLICATION` fails with "can no longer get changes from replication slot … This slot has been
invalidated because it was conflicting with recovery"; Debezium retries 6 × 10 s, then **starts a new cycle, forever**,
and reports RUNNING — debezium/dbz#2430 (open; reported on a PostgreSQL 16 standby, where it "ran undetected in
production for over a week across 19 separate connectors") **[doc]** (<https://github.com/debezium/dbz/issues/2430>).

**Rule:** slot verification is ours (SPEC 1.8): before starting the engine and periodically while streaming, query
`pg_replication_slots` on the server holding the slot for `invalidation_reason IS NULL`, `wal_status <> 'lost'`,
`conflicting = false`, `active_pid`, `confirmed_flush_lsn ≤ applied LSN` (behind is normal, see §3.5) and the slot's
identity; any failure ⇒ stop the engine, alert, take the backfill path. → H-042.

### 2.9 Behaviour that standby decoding adds

- **Decoding never runs ahead of replay.** A logical walsender on a standby waits for `GetXLogReplayRecPtr()`
  (`WalSndWaitForWal`, `pg:src/backend/replication/walsender.c`) **[src]**, so replay lag, replay conflicts that delay
  replay (`max_standby_streaming_delay`) and paused replay (`pg_wal_replay_pause()`) all become decoding latency
  (counted in SPEC 5.2). Details in `docs/research/replica_decoding.md`.
- **Promotion.** After a standby is promoted, its walsenders exit ("terminating walsender process after promotion",
  `StartLogicalReplication` in walsender.c) **[src]**; the slot survives because it is a local object. Debezium sees a
  connection loss and reconnects (retry settings §8). Continuity must be proven by our slot check (SPEC 4.1)
  **[test]**.
- **Slot position persists only at checkpoints / restartpoints.** "The current position of each slot is persisted only
  at checkpoint, so in the case of a crash the slot might return to an earlier LSN" **[doc]**
  (<https://www.postgresql.org/docs/18/logicaldecoding-explanation.html>). On a standby a checkpoint is a restartpoint;
  after a standby crash the slot can move backwards and the server re-sends transactions we already applied. The
  applier discards them by commit LSN (§3.5).
- Debezium's documentation recommends, for PostgreSQL 16 standbys, manual periodic synchronisation of standby slots
  with primary slots; that advice applies to keeping a *second* slot warm for failover and is not needed in our design
  (one slot, on the replica, backfill on loss) **[doc]**.

### 2.10 Known open issues relevant to standby decoding (2026-11-09)

| Issue | State | Effect on us | Mitigation |
|---|---|---|---|
| debezium/dbz#1162 no upstream IT for standby/failover | open | no upstream evidence | our T1 suite |
| debezium/dbz#2430 invalidated slot ⇒ infinite retry, RUNNING | open | silent stall | our slot check (H-042) |
| `filtered` publication validation inverted (this note, 2.3) | not reported | start-up fails or passes wrongly | `disabled` mode |
| debezium/dbz#3035 NPE in read-only incremental snapshot notifications | open | none (we do not use incremental snapshots) | — |
| debezium/dbz#1438 read-only incremental snapshot fails with heartbeat on | open | none | — |
| debezium/dbz#2460 slot acknowledged ahead of persisted offset | open | affects every mode; exact acknowledgement depends on our consumer (§3) | consumer marks processed only after the destination commit |

---

## 3. Delivery contract of the embedded engine

*Section in progress — to follow in the next commit (batch API, flush timing, exact acknowledgement, restart window,
offset storage, starting from a supplied position).*

## 4. Transactions

*To follow.*

## 5. pgoutput message types

*To follow.*

## 6. Schema handling

*To follow.*

## 7. Values

*To follow.*

## 8. Replica identity, liveness, throughput, recommended configuration

*To follow.*
