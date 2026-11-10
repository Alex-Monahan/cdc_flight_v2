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

### 3.1 The batch API

`DebeziumEngine.ChangeConsumer<R>.handleBatch(List<R> records, RecordCommitter<R> committer)` is called on the
engine's polling thread with each batch returned by the connector task (`AsyncEmbeddedEngine.PollRecords.doCall`;
SMTs and conversion may run in parallel before it, `record.processing.threads`) **[src]**
(`dbz:debezium-embedded/src/main/java/io/debezium/embedded/async/AsyncEmbeddedEngine.java`). `RecordCommitter`
(`dbz:debezium-api/src/main/java/io/debezium/engine/DebeziumEngine.java`) has `markProcessed(record)`,
`markProcessed(record, Offsets)` and `markBatchFinished()`; its Javadoc says markProcessed "must be called for each
record" **[src]**. What the implementation does (`AsyncEmbeddedEngine.SourceRecordCommitter`) **[src]**:

- `markProcessed(r)` → `task.commitRecord(r, null)` (stores `r.sourceOffset()` as the task's `lastOffsets`,
  `BaseSourceTask.commitRecord`) and `offsetWriter.offset(partition, offset)`. Nothing is sent to Postgres.
- `markBatchFinished()` → if `offset.commit.policy` says so (default `PeriodicCommitOffsetPolicy` with
  `offset.flush.interval.ms`, default **60 000**), `commitOffsets()`: flush the offset writer into the
  `OffsetBackingStore`, wait up to `offset.flush.timeout.ms` (5 s), then `task.commit()`.
- `task.commit()` only sets a flag; on the **next** `poll()` the task runs `performCommit()`, which calls
  `coordinator.commitOffset(partition, lastOffsets)` → `PostgresStreamingChangeEventSource.commitOffset` → on its
  `lsn-flush` thread `stream.setFlushedLSN(lsn)`, `setAppliedLSN(lsn)`, `forceUpdateStatus()`, bounded by
  `lsn.flush.timeout.ms` (30 s, then `lsn.flush.timeout.action`, default `fail`) (`dbz:debezium-connector-common/…/BaseSourceTask.java`,
  `PostgresStreamingChangeEventSource.java`) **[src]**.
- The LSN sent is `lsn_commit` of the offset if present, else `lsn_proc` (`commitOffset`) **[src]**. `lsn_commit` is
  updated only at a COMMIT message (and at a non-transactional logical message), to the LSN the walsender attached to
  that message, which is the **end** of the commit record (`ctx->write_location = txn->end_lsn`,
  `pg:src/backend/replication/logical/logical.c`, `commit_cb_wrapper`) **[src]**. The offset of a record in the middle
  of transaction T therefore carries the commit LSN of the transaction *before* T.
- On restart the server skips every transaction whose commit record starts before `confirmed_flush_lsn`
  (`DecodeTXNNeedSkip` → `SnapBuildXactNeedsSkip(builder, buf->origptr)` with `start_decoding_at` = confirmed flush,
  `pg:src/backend/replication/logical/decode.c`, `snapbuild.c`) **[src]**. Confirming T's commit end LSN skips exactly
  T and everything before it.
- `lsnFlushingAllowed` is false until the first message after (re)connect has been received; earlier commit requests
  are ignored (`commitOffset`) **[src]**.

### 3.2 When does the slot advance relative to our acknowledgement?

Only through `performCommit()` with `lastOffsets` = the offset of the last record passed to `markProcessed`, and only
after (a) `markBatchFinished` decided to flush, (b) the offset store accepted the flush, (c) the next `poll()` ran. With
`lsn.flush.mode=connector` nothing else moves the slot **[src]**. Two ways the slot could still move ahead of the
destination, both excluded by configuration and by our consumer:

- `lsn.flush.mode=connector_and_driver` (Technology Preview) lets the pgjdbc keepalive thread flush the server's
  keepalive LSN when Debezium has nothing pending **[doc]** (connector docs, `lsn.flush.mode`); it has had monotonicity
  bugs (debezium/dbz#1489, debezium/dbz#1605) **[doc]**. Not used.
- debezium/dbz#2460 (open): `performCommit()` acknowledges the newest in-memory `lastOffsets`, which can be ahead of
  what the offset store persisted **[doc]** (<https://github.com/debezium/dbz/issues/2460>). Harmless for us *because we
  call `markProcessed` only after the destination commit*: `lastOffsets` is then never ahead of the destination, whatever
  the offset store holds.

### 3.3 Making "acknowledge only after the destination commit" exact

Consumer protocol (our Java code, architecture B):

1. `handleBatch` streams every record to the applier (§8.3) and keeps, per source transaction, only a reference to
   its **transaction END record** (`provide.transaction.metadata=true`; the END record's offset has `lsn_commit` = that
   transaction's commit end LSN). It does **not** block on the destination (blocking would stall the stream for the
   duration of every MotherDuck commit).
2. The applier commits each source transaction, or a group of whole transactions — never a part (SPEC 1.3) — in one
   MotherDuck transaction together with the applied position, then reports "committed up to commit LSN X" on the control
   channel.
3. At the start and end of every `handleBatch` call the consumer drains those reports, calls
   `committer.markProcessed(endRecord)` for the newest END record with commit LSN ≤ X, then `markBatchFinished()`.
4. `offset.flush.interval.ms=0`, so every `markBatchFinished` flushes; `heartbeat.interval.ms` (e.g. 5 000) guarantees a
   `handleBatch` call during idle periods so acknowledgements are never delayed indefinitely.

Property: the slot's `confirmed_flush_lsn` ≤ the destination's applied commit LSN at all times (order: destination
commit → report → markProcessed → flush → next poll → walsender). The acknowledgement lags by at most one batch or
heartbeat interval, which only costs retained WAL **[src]**, **[test]** (crash points between every step, SPEC 1.7;
continuous invariant probe "slot never ahead of destination", the same probe debezium/dbz#2460 used).

Calling `markProcessed` only for END records departs from the Javadoc's "must be called for each record" but not from
the implementation, which keeps only the last offset **[src]**. Honouring the letter would mean keeping every record
reference of a 10M-row transaction in the JVM until its commit; we should not.

### 3.4 Offset storage and the starting position: run without stored offsets

Offset stores in 3.7: `FileOffsetBackingStore` (default), `MemoryOffsetBackingStore`, Kafka, JDBC
(`JdbcOffsetBackingStore`; 3.7.1 fixed debezium/dbz#2786), Redis and others in `debezium-storage`, or any class
implementing Kafka Connect's `OffsetBackingStore`, selected with `offset.storage` **[doc]**
(<https://debezium.io/documentation/reference/stable/development/engine.html>).

**Recommendation: `offset.storage=org.apache.kafka.connect.storage.MemoryOffsetBackingStore`; the replication slot is the
restart point; the destination holds the applied position (SPEC 1.1).** Reasons:

- With no stored offset and `snapshot.mode=no_data`, no snapshot runs (`shouldSnapshotData(false, false)` is false) and
  streaming receives a null offset (`ChangeEventSourceCoordinator.executeChangeEventSources`,
  `PostgresSnapshotChangeEventSource.getSnapshottingTask`) **[src]**; it logs "No previous LSN found … streaming from the
  latest xlogpos or flushed LSN" and starts at `min(slot.confirmed_flush_lsn, IDENTIFY_SYSTEM xlogpos)` with an empty
  `WalPositionLocator` — **no position search and no event skipping** (`PostgresStreamingChangeEventSource.execute`,
  `PostgresReplicationConnection.initReplicationSlot`) **[src]**. The connector documentation says the same for
  ephemeral stores: "the connector always defers to the replication slot position on startup" **[doc]** (connector docs,
  `lsn.flush.mode`).
- All recent **data-loss and OOM bugs on restart are in the stored-offset path** (`WalPositionLocator`,
  `lsn_events_processed`): debezium/dbz#2889 (fixed 3.7.1), debezium/dbz#2901 (open: DML lost after restarting from a
  transactional message following COPY), debezium/dbz#2905 (open: restarting from a mid-transaction offset needs memory
  proportional to the transaction; OOM on an 8M-row transaction) **[doc]**. Without stored offsets none of them is
  reachable **[src]**, **[test]** (SIGKILL at random points of a 10M-row COPY with a transactional message inside; expect no
  loss and bounded JVM memory).
- SPEC 1.1 puts the applied position in the destination; a second local copy is a second source of truth.

"Starting from a position we supply" then means: before starting the engine, our slot check (SPEC 1.8) compares
`confirmed_flush_lsn` with the destination's applied commit LSN. `confirmed_flush_lsn ≤ applied` is normal (the server
re-sends transactions we already have; the applier discards every transaction whose commit LSN ≤ applied).
`confirmed_flush_lsn > applied` means changes were consumed without being applied ⇒ alert + backfill. To move the slot
forward ourselves (e.g. after a lag-triggered backfill, SPEC 3.6), use `pg_replication_slot_advance(slot, lsn)` before
the engine starts (allowed for logical slots on a standby **[test]**) or drop and re-create the slot. A custom
`OffsetBackingStore` returning `{lsn, lsn_proc, lsn_commit}` = applied commit LSN is possible but re-enters the
`WalPositionLocator` path and is **not recommended**.

### 3.5 Redelivery window after restart

- Clean stop: `engine.close()` commits the offset of the last marked record and stops the task (`stopSourceTasks`)
  **[src]**; redelivery = transactions after our last acknowledgement.
- Crash of our process: redelivery = transactions after the last acknowledgement that reached the walsender (≤ one
  batch or heartbeat interval plus anything in flight); the engine docs describe the same "up to *n* duplicates" **[doc]**
  (engine docs, failure handling).
- Crash of the server holding the slot: the slot may return to the position persisted at the last checkpoint or
  restartpoint **[doc]** (<https://www.postgresql.org/docs/18/logicaldecoding-explanation.html>); redelivery can span
  minutes.
- The server re-sends **whole transactions** in commit order (protocol 1 never sends partial transactions), so
  discard-by-commit-LSN is exact. The 32-bit xid of the BEGIN message (`Integer.toUnsignedLong` in `handleBeginMessage`)
  wraps around and must not be used as a key **[src]**.

## 4. Transactions

- **Boundary events** (`provide.transaction.metadata=true`): one BEGIN and one END record per transaction (`id` =
  `txid:lsn`; END carries `event_count` and per-table `data_collections[].event_count`); every data record gets a
  `transaction` block with `id`, `total_order`, `data_collection_order` **[doc]**
  (<https://debezium.io/documentation/reference/stable/connectors/postgresql.html#postgresql-transaction-metadata>). The
  END counts let the applier verify that it received every event before committing — a cheap completeness check for the
  stream fuzzer's truncation cases (SPEC §4).
- **Ordering**: one slot, one walsender, one Debezium streaming thread; protocol 1 sends each transaction contiguously at
  commit time and transactions in commit order, so ordering is total across tables **[doc]**
  (<https://www.postgresql.org/docs/18/protocol-logical-replication.html>), **[src]**. `record.processing.order=ORDERED`
  (default) keeps that order through parallel conversion **[doc]** (engine docs).
- **A 10M-row transaction**: the walsender decodes into its reorder buffer and spills to `pg_replslot/<slot>/` on the
  **decoding server's disk** beyond `logical_decoding_work_mem` (default 64 MB) **[doc]**
  (<https://www.postgresql.org/docs/18/runtime-config-resource.html>); nothing is sent before COMMIT. After COMMIT the
  whole transaction is read back from spill and streamed. In the JVM, memory is bounded by the `ChangeEventQueue`
  (`max.queue.size` records, default 8 192; `max.queue.size.in.bytes`, default 0 = unbounded; `max.batch.size` 2 048;
  `poll.interval.ms` 500); the producer blocks when the queue is full **[src]** (`CommonConnectorConfig`,
  `ChangeEventQueue`). The applier stages the transaction (spill, SPEC 5.4) and commits it once.
- **Streaming of in-progress transactions**: not used. pgoutput streams only if the client asks for `streaming` with
  `proto_version ≥ 2` (`pgoutput_startup`: "requested proto_version=%d does not support streaming",
  `pg:src/backend/replication/pgoutput/pgoutput.c`) **[src]**; Debezium sends protocol 1 and no `streaming` option
  **[src]**. Consequences: large transactions cost spill disk on the replica (H-049) and add head-of-line latency for
  everything committed after them (H-001).
- **Two-phase**: not requested; a prepared transaction is decoded at `COMMIT PREPARED` as an ordinary transaction and
  `ROLLBACK PREPARED` produces nothing **[doc]** (<https://www.postgresql.org/docs/18/logicaldecoding-two-phase-commits.html>),
  **[test]**.

## 5. pgoutput messages and what Debezium does with each

From `PgOutputMessageDecoder` and `PostgresStreamingChangeEventSource` (3.7.1) **[src]**; message formats from
<https://www.postgresql.org/docs/18/protocol-logicalrep-message-formats.html> **[doc]**.

| Message | Debezium 3.7.1 | Record emitted | Lost / not representable |
|---|---|---|---|
| Begin `B` | stores xid and commit time; clears origin | transaction BEGIN (metadata on) | BEGIN's `final_lsn` not exposed |
| Commit `C` | updates `lsn_commit`, resets `lsn_events_processed`, may emit a heartbeat | transaction END | commit flags; `end_lsn` only via the offset |
| Relation `R` | builds a `Table` from the message **plus out-of-band JDBC queries of the current catalog** (defaults, optionality, type names, primary key); **refuses** tables whose column names differ only in case (`DebeziumException`) | none (schema embedded in later records) | relation OID, replica-identity setting and per-column key flags not exposed; historic metadata replaced by current metadata (H-006, H-032) |
| Type `Y` | always skipped | none | type names come from Debezium's own JDBC `TypeRegistry` (H-033) |
| Insert `I` | decoded against the cached relation | `c` | unknown types dropped unless `include.unknown.datatypes=true` (§7) |
| Update `U` | old tuple `K`/`O` if present, new tuple `N` | `u`; key change ⇒ `d` + tombstone + `c` | unchanged TOAST `u` ⇒ placeholder (§7) |
| Delete `D` | old key `K` or old row `O` | `d` (+ tombstone if `tombstones.on.delete`) | non-key columns absent under DEFAULT identity |
| Truncate `T` | **skipped unless `skipped.operations` excludes `t`; the default is `"t"`** (`CommonConnectorConfig.SKIPPED_OPERATIONS`) | one `t` per resolved table, the last one flagged | `CASCADE`/`RESTART IDENTITY` flags parsed and dropped |
| Origin `O` | kept in the offset context | `origin` fields in source info | — |
| Message `M` | transactional: inside its transaction; non-transactional: its own commit point; filtered by `message.prefix.include/exclude.list` | `m` with prefix and content bytes | — |
| Stream Start/Stop/Commit/Abort, Stream Prepare | never requested (protocol 1) | — | an unexpected type would throw `IllegalArgumentException("Unsupported message type")` (`MessageType.forType`) |
| Begin Prepare, Prepare, Commit Prepared, Rollback Prepared | never requested | — | — |
| tuple value kind `b` (binary) | not requested; an unknown kind is logged at TRACE and the column is **silently omitted** (`resolveColumnsFromStreamTupleData`) | — | binary mode request open (debezium/dbz#2898) |

Column values are always decoded as UTF-8 (`readColumnValueAsString` → `Charset.forName("UTF-8")`) whatever the server
encoding **[src]** → H-013.

## 6. Schema handling

- The Postgres connector emits **no DDL events**. It learns of schema changes only from Relation messages, which
  pgoutput sends before the first change to a relation after its definition changed — also in the middle of a
  transaction **[doc]** (protocol docs), **[src]** (`handleRelationMessage`). `DROP TABLE`, `ALTER TABLE … RENAME`,
  `CREATE TABLE` without rows, `ALTER TYPE` produce **no message at all**.
- `schema.refresh.mode` (`columns_diff` default, or `columns_diff_exclude_unchanged_toast`) decides when Debezium
  re-reads a table schema over JDBC on a mismatch with an incoming message **[src]**.
- **Column add/drop/type change**: next Relation message → new record schema. **Rename**: Relation message with a new
  name in the same position; indistinguishable from drop+add (no attnum in the message) → H-008.
- **DDL and DML in one transaction**: the Relation message precedes the affected DML in the stream, but Debezium's
  out-of-band queries return the catalog **as it is now** on the connected server, which can be later than the decoded
  position (Debezium documents this "from the future" race in a comment in `handleRelationMessage`) **[src]** → H-006.
- **Drop and recreate with a new OID**: no DROP message; the new table's first change brings a Relation message with a
  new relation id and the same name; records do not carry the relation OID, so the records alone cannot reveal the
  recreate → H-007.
- **New tables**: rows are published from the WAL position of `ALTER PUBLICATION … ADD TABLE`; Debezium needs no
  restart if its own filters are open; we keep Debezium's filters broad and filter in the applier, and maintain the
  publication ourselves (§2.3). Signals and incremental snapshots are not used (R3).
- **Partitioned tables**: with `publish_via_partition_root = false` (our default) every leaf is published under its own
  name; with `true`, changes appear as the root, `TRUNCATE` on a partition is **not replicated**, and `ATTACH` does not
  replicate existing rows **[doc]** (<https://www.postgresql.org/docs/18/sql-createpublication.html>) → H-021.
- **Generated columns**: virtual ones (the default in 18) are never published; stored ones only with
  `publish_generated_columns = stored` or a column list (18) **[doc]** → H-022. **Identity columns** are ordinary values.
- **Column lists and row filters**: debezium/dbz#2375 (open) — after any schema refresh the decoder binds tuple values
  to the wrong columns when the publication has a column list (silent corruption) **[doc]**; row filters turn updates
  into inserts/deletes **[doc]** → H-023. We use neither.
- **Enums and types**: debezium/dbz#2521 (open) NPE in `TypeRegistry` kills the task permanently when an enum type is
  recreated; debezium/dbz#304 (open) schema not updated after `ALTER TYPE … ADD VALUE` **[doc]** → H-033.

## 7. Values

Rules for lossless delivery with stock Debezium 3.7.1 — every item is configuration or our own SPI class:

1. `decimal.handling.mode=string`. `precise` throws `ConnectException("Got a special value (NaN/Infinity) for Decimal
   type …")` on NaN/±Infinity (`SpecialValueDecimal.fromLogical`) and, for unconstrained `numeric`, strips trailing zeros
   (`stripTrailingZeros()` in `PostgresValueConverter.convertDecimal`), so `1.50` arrives as `1.5` **[src]**. `string`
   delivers `BigDecimal.toPlainString()` or `NaN`/`Infinity`/`-Infinity` **[src]**.
2. `time.precision.mode=structured` (documented in 3.7): dates, times, timestamps and intervals as structs of calendar
   and clock components with a `specialValue` field for ±infinity; "To preserve finite PostgreSQL TIMESTAMP values that
   exceed the range of an epoch-microsecond integer, use the pgoutput logical decoding plug-in" **[doc]** (connector
   docs, `time.precision.mode=structured`), **[src]** (`dbz:debezium-connector-common/…/time/StructuredTimestamp.java`).
   The epoch-based modes map ±infinity to finite sentinels, and `nanoseconds` fails outside 1677–2262 **[doc]**.
3. `interval.handling.mode=string` (ISO-8601) — `numeric` converts with an average month length (lossy) **[doc]**,
   **[test]**; `structured` time mode also yields a structured duration **[doc]**.
4. `binary.handling.mode=bytes` (no base64/hex in the hand-off) **[doc]**.
5. `include.unknown.datatypes=true`. With the default `false`, values of types Debezium does not know (composites,
   custom base types, `pg_lsn`, `tsquery`, `txid_snapshot`, multiranges, …) are **dropped** (`asDefault` returns null)
   **[src]**; with `true` they arrive as the PostgreSQL text representation.
6. `hstore.handling.mode=json` **[doc]**.
7. `unavailable.value.placeholder` set to a value that cannot occur in text-like columns, plus our converter for TOAST
   markers where a placeholder could collide (H-003).
8. **Our own `CustomConverter` classes** (documented SPI, <https://debezium.io/documentation/reference/stable/development/converters.html>,
   enabled with `converters=…`) for the types below where Debezium's default mapping is lossy. A custom converter replaces
   the default converter for that column and receives the *raw* value
   (`customConverterRegistry.getValueConverter(…).orElse(valueConverterProvider.converter(…))` in
   `dbz:debezium-connector-common/src/main/java/io/debezium/relational/TableSchemaBuilder.java`) **[src]**: for arrays a
   pgjdbc `PgArray` built from the text form, for bit strings the text, for an unchanged TOAST column the marker object
   (`UnchangedToastedReplicationMessageColumn.UNCHANGED_TOAST_VALUE` and its array variants, compared by identity).

| PostgreSQL type | Debezium 3.7.1 default | Lossless with stock Debezium? | Configuration / workaround |
|---|---|---|---|
| `numeric` (constrained) | `precise`: Decimal | not for NaN/±Inf (connector fails) | `decimal.handling.mode=string` |
| `numeric` (unconstrained, precision > 38, NaN, ±Inf) | VariableScaleDecimal, trailing zeros stripped | no | `string` |
| `money` | text parsed by `PostgresMoney.parse`: strips one leading currency symbol and `,` separators → BigDecimal | only if `lc_monetary` gives `.` decimals and a one-character prefix; otherwise `ConnectException` (refusal) | `lc_monetary=C` in the walsender session **[test]**, or a custom converter emitting text |
| `float4`, `float8` incl. NaN, ±Inf, -0 | Java float/double from text | yes with `extra_float_digits ≥ 1` (shortest-exact output since 12) **[doc]**; pgjdbc sets 3 at connect **[test]** | — |
| `date`, `timestamp` incl. ±infinity, BC, year > 9999 | epoch-based; infinity → sentinels; BC parse fixed in 3.7.0 (debezium/dbz#1340, #2664) | `structured`: yes **[doc]**; BC encoding to verify **[test]** | `time.precision.mode=structured` |
| `timestamptz` extremes | OffsetDateTime at UTC | `structured` keeps components and offset **[doc]** | same |
| `time` (incl. `24:00:00`), `timetz` | micro/nano time | `24:00:00` unverified **[test]** | `structured` |
| `interval` | `numeric`: micros with average month | no | `string` |
| `bytea` | bytes | yes | `binary.handling.mode=bytes` |
| `bit(n)`, `varbit` | `bit(1)`/`varbit(1)` → boolean; others → little-endian bytes padded to the declared length (`convertBits`) | **no** for `varbit`: per-value bit length lost (`B'101'` = `B'0101'`); `varbit(1)` value `B''` → `Integer.valueOf("", 2)` throws **[src]**, **[test]** | custom converter → text |
| arrays | Connect ARRAY from `PgArray` | **no** for non-default lower bounds (`'[0:2]={…}'`) and multi-dimensional arrays **[test]**; LTREE arrays with NULLs fixed in 3.7.1 (debezium/dbz#2770) | custom converter → `PgArray.toString()` (the text form) **[test]** |
| `hstore` | `json` or `map` | `json`: yes | `hstore.handling.mode=json` |
| enums | string | yes (DDL hazards H-033) | — |
| domains | resolved to the base type | yes | — |
| composite / row types | unknown → dropped | with `include.unknown.datatypes=true`: text **[test]** | `include.unknown.datatypes=true` |
| ranges / multiranges | ranges string; multiranges unknown | ranges yes; multiranges as text **[test]** | `include.unknown.datatypes=true` |
| geometric (`point`, `box`, …) | STRING since 3.7.0 (debezium/dbz#2135) | yes as text **[test]** | — |
| network (`inet`, `cidr`, `macaddr*`) | string | yes | — |
| `json`, `jsonb` (huge numbers) | string (`io.debezium.data.Json`) | yes (text passed through) | the applier must not parse numbers into doubles |
| `xml`, `uuid`, `ltree`, `citext`, `tsvector`, `vector` | string | yes | — |
| `"char"` | string | yes (non-ASCII bytes are octal-escaped by `charout` since 15) **[test]** | — |
| `oid` / large objects | INT64; large-object content never decoded | the oid yes, the content no | document as out of scope |
| PostGIS `geometry`/`geography` | string (hex EWKB) | yes | — |
| unchanged TOAST | in-band placeholder (string, bytes or one-element array) | ambiguous in principle (H-003) | NUL-containing placeholder + custom converter |

## 8. Replica identity, liveness, throughput, configuration

### 8.1 Replica identity

| Identity | UPDATE | DELETE | Source |
|---|---|---|---|
| DEFAULT with PK | new row only; if a key column changed, old key `K` ⇒ Debezium emits `d` (old key) + tombstone + `c` (new row) | old key only | protocol docs; `PostgresChangeRecordEmitter` **[src]** |
| USING INDEX | as DEFAULT, keyed by the index columns | index columns only | **[doc]** (ALTER TABLE) |
| FULL | old row `O` (all columns, TOASTed values included) + new row; Debezium fills `u` columns of the new row from the old row (`cachedOldToastedValues`, DBZ-1258) | old row | **[src]** |
| NOTHING / DEFAULT without PK / index dropped | **the source's own UPDATE/DELETE fails** when the table is in a publication that publishes them: "Attempting such operations will result in an error on the publisher" | same | **[doc]** (<https://www.postgresql.org/docs/18/logical-replication-publication.html>) → H-014 |

Tombstones (`tombstones.on.delete`, default true) are Kafka compaction artefacts; set it to `false`.

### 8.2 Liveness

- **Keepalives**: Debezium runs a keepalive thread per replication stream that calls `stream.forceUpdateStatus()` every
  `status.update.interval.ms` (default 10 s) independently of the consumer (`startKeepAlive`) **[src]**; pgjdbc also
  answers server keepalive requests. The walsender terminates a client that has not replied for `wal_sender_timeout`
  (default 60 s) **[doc]**. A long destination apply blocks the streaming thread on the full queue; TCP back-pressure
  stops the walsender, but status updates keep flowing, so there is no timeout **[src]**, **[test]** (10-minute apply
  stall with `wal_sender_timeout=60s`). A JVM frozen longer than `wal_sender_timeout` is disconnected — correct.
- **Heartbeats**: `heartbeat.interval.ms` emits heartbeat records (no write); `heartbeat.action.query` writes on the
  connected server (unusable on a standby, §2.6).
- **Connection loss and retry**: `slot.max.retries` (6) × `slot.retry.delay.ms` (10 s) for stream start; engine-level
  `errors.max.retries`, `errors.retry.delay.initial.ms`/`max.ms`; a `RetriableException` restarts the task after
  `retriable.restart.connector.wait.ms` **[src]**. Recommendation: small retry counts in the JVM; our supervisor (SPEC 4.5)
  restarts the child process after its own slot checks.
- **Dead connections**: pgjdbc enables `SO_KEEPALIVE` (`tcpKeepAlive=true`, Debezium default) but sets no idle/interval,
  so detection follows OS defaults (Linux: 2 h) **[doc]** (<https://jdbc.postgresql.org/documentation/use/>), **[test]**.
  SPEC 4.6's end-to-end marker (heartbeat row seen in the stream within 60 s, else kill and reconnect) is the real
  detector.
- **Known hang scenarios**: infinite retry on an invalidated slot (debezium/dbz#2430, open); shutdown stalls while the
  producer is blocked on a full queue (debezium/dbz#2758, open); Server not exiting after engine failure
  (debezium/dbz#3010, fixed 3.7.1); slot creation waiting on running transactions (H-040) **[doc]**.
- **Clean shutdown**: `engine.close()` → commit the last marked offset, stop tasks (bounded by
  `task.management.timeout.ms`), close the replication connection; the walsender exits and the slot becomes inactive.
  Expect seconds; bounded by `debezium.embedded.shutdown.pause.before.interrupt.ms` (default 5 min) and the
  debezium/dbz#2758 stall **[src]**, **[test]**.
- **Killing safely**: with §3.3 the slot is never ahead of the destination, so `SIGKILL` is always safe; its only cost is
  redelivery. If the host or network dies instead, the old walsender holds the slot (`active_pid`) until
  `wal_sender_timeout`; the next leader must `pg_terminate_backend(active_pid)` on the server holding the slot (SPEC 4.2;
  needs `pg_signal_backend` or the same role), otherwise `START_REPLICATION` fails with "replication slot … is active for
  PID …" **[src]**, **[test]**.

### 8.3 Throughput

- No official Debezium benchmark exists for the Postgres connector at our row shape. Community figures are in the
  thousands of events per second per connector behind Kafka (e.g. "7,000 events/s per task" in an Instaclustr 2021
  talk, <https://www.slideshare.net/slideshow/change-data-capture-cdc-with-kafka-connect-and-the-debezium-postgresql-source-connector/251940030>,
  weak source) **[doc]**. A 2025 Debezium profile found per-record type-modifier regex parsing as a hotspot (fixed by
  caching) **[doc]** (<https://debezium.io/blog/2025/07/07/quick-perf-check/>); 3.7.0 reduced per-record allocation in
  column extraction (debezium/dbz#2202) **[doc]**.
- Structural bottlenecks: one walsender per slot decodes and text-encodes every column on the **replica** (one core);
  one Debezium streaming thread parses text into Java objects; Connect `Struct` building; GC pressure (debezium/dbz#531)
  **[doc]**. On an F16 (2 vCPU) the JVM, the Python applier and DuckDB share two cores: CPU, not I/O, is the expected
  limit **[test]** (P2 spike, PLAN §2 A 5).
- **Cheapest hand-off out of the JVM**: `DebeziumEngine.create(Connect.class)` (no JSON/Avro; we get `SourceRecord`s with
  `Struct` values) writing **Arrow IPC record batches** (columns typed from the Connect schema; one or more batches per
  source transaction) over a **Unix domain socket or the child's stdout pipe**, plus a small framed control channel
  (BEGIN/END, commit LSN, END counts, CRC32C per frame) so truncation and corruption are detected (SPEC §4 stream
  fuzzer). `pyarrow` reads the stream and DuckDB scans Arrow without copying. Arrow Java adds roughly 10 MB of jars and
  needs `--add-opens=java.base/java.nio=ALL-UNNAMED` **[test]**.
- Settings that matter for 100K rows/s at 16 columns (tuned in P9): `max.batch.size` 8 192–32 768, `max.queue.size`
  4–8 × batch, `max.queue.size.in.bytes` (e.g. 256 MB, bounded heap), `poll.interval.ms` 10–50,
  `record.processing.threads` 1–2, no SMTs, capped `-Xmx` (SPEC 5.4), `logical_decoding_work_mem` on the replica sized so
  typical transactions do not spill **[test]**.

### 8.4 Recommended configuration (Debezium 3.7.1, embedded engine)

Replica mode (default). Connection values come from the environment and are never committed.

```properties
# engine
name=cdc_v2_engine                                       # engine name in logs and metrics
connector.class=io.debezium.connector.postgresql.PostgresConnector
offset.storage=org.apache.kafka.connect.storage.MemoryOffsetBackingStore  # slot is the restart point (3.4)
offset.flush.interval.ms=0                               # flush on every markBatchFinished (3.3)
offset.flush.timeout.ms=5000                             # memory store, never slow
record.processing.order=ORDERED                          # keep commit order
record.processing.threads=1                              # 2 vCPU, no SMTs (tune in P9)
errors.max.retries=1                                     # fail fast; the supervisor restarts (SPEC 4.5)
# connection
database.hostname=${PG_REPLICA_HOST}                     # the replica
database.port=${PG_REPLICA_PORT}
database.dbname=${PG_DATABASE}
database.user=${PG_USER}
database.password=${PG_PASSWORD}
database.sslmode=verify-full                             # PLAN 8
database.tcpKeepAlive=true                               # default; OS-level only (8.2)
topic.prefix=cdc_v2                                      # required; names record topics
# decoding
plugin.name=pgoutput                                     # SPEC 7.1
slot.name=cdc_v2_slot                                    # created by our code beforehand (2.4)
publication.name=cdc_v2_pub                              # created by our code on the primary (2.3)
publication.autocreate.mode=disabled                     # the only safe mode on a standby (2.3)
slot.drop.on.stop=false                                  # the slot survives restarts
slot.failover=false                                      # ignored on a standby anyway (2.7)
slot.max.retries=2                                       # fail fast to the supervisor
slot.retry.delay.ms=5000
lsn.flush.mode=connector                                 # slot moves only on our acknowledgement (3.2)
offset.mismatch.strategy=no_validation                   # we verify the slot ourselves (SPEC 1.8)
status.update.interval.ms=10000                          # keepalive well under wal_sender_timeout
snapshot.mode=no_data                                    # backfill is ours (R3)
provide.transaction.metadata=true                        # BEGIN/END with counts (4)
skipped.operations=none                                  # the default "t" drops TRUNCATE (5)
tombstones.on.delete=false                               # no Kafka compaction here
heartbeat.interval.ms=5000                               # handleBatch calls when idle (3.3)
message.prefix.include.list=cdc_v2_.*                    # plus the configured SPEC 7.4 prefixes
schema.refresh.mode=columns_diff                         # safest
# values (7)
decimal.handling.mode=string
time.precision.mode=structured
interval.handling.mode=string
binary.handling.mode=bytes
hstore.handling.mode=json
include.unknown.datatypes=true
unavailable.value.placeholder=hex:006364635f76325f746f61737400   # NUL-framed, impossible in text (H-003)
converters=cdcv2                                         # our CustomConverter: varbit, arrays, TOAST markers, money
cdcv2.type=<our converter class>
# queue (tuned in P9)
max.batch.size=8192
max.queue.size=32768
max.queue.size.in.bytes=268435456
poll.interval.ms=50
```

Primary mode (non-default) differs only in:

```properties
database.hostname=${PG_HOST}                             # the primary
database.port=${PG_PORT}
slot.failover=true                                       # second line; our code creates the slot WITH failover (2.7)
```

and our code creates the slot with `failover = true`, checks `failover` and `synced` on the standbys at every
acquisition (SPEC 4.1), and writes the heartbeat row on the primary on a separate connection.

Deliberately **not** used: `heartbeat.action.query`, `replica.identity.autoset.values` (would `ALTER TABLE` on the
connected server), `publication.autocreate.mode` other than `disabled`, `lsn.flush.mode=connector_and_driver`,
`read.only` and `signal.*` (no incremental snapshots), any `snapshot.mode` other than `no_data`,
`offset.mismatch.strategy=trust_*`.
