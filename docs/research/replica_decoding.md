# Logical decoding on a standby, PostgreSQL 16–18

Research note for PLAN phase P5 and the PlanetScale spike (PLAN §2 B 6). Status: **draft, 2026-11-09**.

Marks: **[doc]** documented (URL given), **[src]** read in source (file and branch given), **[test]** to be verified by
test. `pg:` paths are relative to <https://github.com/postgres/postgres/tree/REL_18_STABLE> unless another branch is
named. Hazard numbers `H-0xx` refer to `docs/wal_hazards.md`. Debezium-specific behaviour on a standby is in
`docs/research/debezium.md` §2.

---

## 1. Requirements

| Requirement | Where | Why | Source |
|---|---|---|---|
| `wal_level = logical` on the **primary** | primary | standby decoding refuses with "logical decoding on standby requires wal_level >= logical on the primary" | `pg:src/backend/replication/logical/logical.c` `CheckLogicalDecodingRequirements` **[src]** |
| `wal_level = logical` on the **standby** as well | standby | the same function first checks the local `wal_level` ("logical decoding requires wal_level >= logical"); a standby with a logical slot and `wal_level < logical` refuses to start (FATAL "logical replication slot … exists, but wal_level < logical") | logical.c; `pg:src/backend/replication/slot.c` `RestoreSlotFromDisk` **[src]** |
| `hot_standby = on` | standby | FATAL "logical replication slot … exists on the standby, but hot_standby = off" | slot.c `RestoreSlotFromDisk` **[src]** |
| `hot_standby_feedback = on` | standby | keeps the slot's `catalog_xmin` on the primary, otherwise required catalog rows are vacuumed and the slot is invalidated | <https://www.postgresql.org/docs/18/logicaldecoding-explanation.html> **[doc]** |
| physical replication slot between primary and standby (`primary_slot_name`) | both | "Otherwise, hot_standby_feedback will work but only while the connection is alive (for example a node restart would break it)" | same page **[doc]** |
| `max_replication_slots`, `max_wal_senders` with headroom | standby | the logical slot and its walsender live on the standby | <https://www.postgresql.org/docs/18/runtime-config-replication.html> **[doc]** |
| role with `REPLICATION` (or superuser), `LOGIN` | standby (catalog replicated from primary) | replication protocol, slot functions ("Use of functions for replication slots is restricted to superusers and users having REPLICATION privilege") | <https://www.postgresql.org/docs/18/functions-admin.html> **[doc]** |
| publication exists on the primary before the slot's start point | primary | pgoutput reads it with the historic snapshot of the decoded LSN; 17 errors if missing, 18 warns and **publishes nothing** (H-034) | `pg:src/backend/replication/pgoutput/pgoutput.c` `LoadPublications` (18 vs `REL_17_STABLE`) **[src]** |
| `pgoutput` in `output_plugin_libraries` (new GUC, default `'pgoutput, test_decoding'`) | standby | requests for other plugins are refused | <https://www.postgresql.org/docs/18/runtime-config-replication.html> **[doc]** |

PostgreSQL 16 introduced the feature: "Allow logical decoding on standbys … Snapshot WAL records are required for
logical slot creation but cannot be created on standbys. To avoid delays, the new function pg_log_standby_snapshot()
allows creation of such records." **[doc]** (<https://www.postgresql.org/docs/release/16.0/>).

## 2. Slot creation waits for a running-transactions record

- "Creation of a logical slot requires information about all the currently running transactions. On the primary,
  this information is available directly, but on a standby, this information has to be obtained from primary. Thus,
  slot creation may need to wait for some activity to happen on the primary. If the primary is idle, creating a
  logical slot on standby may take noticeable time. This can be sped up by calling the pg_log_standby_snapshot
  function on the primary." **[doc]** (<https://www.postgresql.org/docs/18/logicaldecoding-explanation.html>).
- The record (`xl_running_xacts`) is written by the primary's bgwriter at most every 15 s and only if important WAL was
  inserted since the previous one (`LOG_SNAPSHOT_INTERVAL_MS 15000`, `pg:src/backend/postmaster/bgwriter.c`) **[src]**.
  An idle primary therefore produces none at all.
- The snapshot builder needs up to three such records when transactions are running: it must wait until every
  transaction seen running at the first one has finished (`pg:src/backend/replication/logical/snapbuild.c`,
  states `SNAPBUILD_BUILDING_SNAPSHOT` → `FULL_SNAPSHOT` → `CONSISTENT`; on a standby "we can't enforce that, so we'll
  have to wait", `SnapBuildWaitSnapshot`) **[src]**. A long-running or **prepared** (2PC) transaction on the primary
  therefore blocks slot creation on the standby for as long as it stays open **[src]**, **[test]**.
- `pg_log_standby_snapshot()` is superuser-only by default (admin function family; "restricted to superusers by
  default, but other users can be granted EXECUTE") **[doc]** — expected to be unavailable on PlanetScale **[test]**.

**Prevention (H-040):** the primary heartbeat (SPEC 4.4, every 10 s) is running before any slot is created, which
guarantees an `xl_running_xacts` record within about 15 s; slot creation has a deadline (default 120 s) and on expiry
reports the oldest transaction on the primary (`pg_stat_activity` with `backend_xid`/`backend_xmin`, and
`pg_prepared_xacts`) instead of waiting forever. Test: create the slot with the primary idle and the heartbeat off
(expect the deadline), then with the heartbeat on (expect success within 20 s), then with a prepared transaction open
(expect the deadline and the report naming the prepared transaction).

## 3. Every cause of slot invalidation on a standby, and its prevention

PostgreSQL 18 knows four invalidation causes (`ReplicationSlotInvalidationCause` in `pg:src/include/replication/slot.h`;
names in `pg:src/backend/replication/slot.c`): `wal_removed`, `rows_removed`, `wal_level_insufficient`,
`idle_timeout` **[src]**, shown in `pg_replication_slots.invalidation_reason` (17+) **[doc]**
(<https://www.postgresql.org/docs/18/view-pg-replication-slots.html>). `conflicting` (16+) is true for slots invalidated
by a conflict with recovery (`rows_removed`, `wal_level_insufficient`) **[doc]**. Measured in P3 (package P): on
Postgres 16 a slot invalidated by the WAL cap shows `conflicting = true` (16 has no `invalidation_reason`); from 17 on
it shows `conflicting = false` and `invalidation_reason = 'wal_removed'`. A slot is invalid when `wal_status = 'lost'`,
or `invalidation_reason` is set, or `conflicting` is true (`SlotFacts.invalid`). An invalidated slot can never be used
again: "lost means that this slot is no longer usable" **[doc]**. Debezium does not check any of these columns and
retries forever on an invalidated slot (debezium/dbz#2430; debezium.md §2.8).

| Cause | Mechanism on a standby | Prevention | Hazard |
|---|---|---|---|
| `rows_removed` | During replay of a cleanup record on a catalog relation whose conflict horizon ≥ the slot's `catalog_xmin` (or `xmin`), the standby invalidates the slot **immediately** — there is no `max_standby_streaming_delay` grace (`ResolveRecoveryConflictWithSnapshot` → `InvalidateObsoleteReplicationSlots(RS_INVAL_HORIZON, …)`, `pg:src/backend/storage/ipc/standby.c`; `DetermineSlotInvalidationCause`, slot.c) **[src]**. It happens when the primary vacuumed catalog rows the slot still needs: `hot_standby_feedback` off; feedback not yet sent after a standby (re)connect; no physical slot, so the primary forgot the standby's `catalog_xmin` while it was disconnected (docs, §1); the physical slot itself invalidated or dropped; the standby following a new primary that never received its feedback (§5). | `hot_standby_feedback = on` and a physical slot (`primary_slot_name`) are **preconditions checked by our code** before slot creation and at every acquisition (`SHOW hot_standby_feedback`, `SHOW primary_slot_name` on the standby; on the primary `pg_replication_slots` row of that physical slot with non-null `catalog_xmin` equal to or older than our slot's). Replica mode refuses to start (and alerts) when a precondition fails, instead of creating a slot that will be invalidated later. | H-042 |
| `wal_level_insufficient` | The primary's `wal_level` drops below `logical`; the standby invalidates its logical slots "as soon as the standby detects such a change in the WAL stream" **[doc]** (logicaldecoding-explanation). | Check `wal_level` on the primary at every acquisition and in the periodic slot check; on PlanetScale changing `wal_level` requires a restart with switchover (parameters page), so it is also visible as a configuration change. Cannot be prevented from our side; detected and answered by backfill (SPEC 4.1). | H-042 |
| `wal_removed` | The slot's `restart_lsn` falls behind what the standby keeps: on the standby, WAL retention for its own logical slot is governed by the **standby's** `max_slot_wal_keep_size` (checked at restartpoints) **[src]** (`InvalidateObsoleteReplicationSlots(RS_INVAL_WAL_REMOVED …)` is called from both `CreateCheckPoint` and `CreateRestartPoint`, `pg:src/backend/access/transam/xlog.c`). | Size lag trigger (SPEC 3.6: 50 % of the cap on the server holding the slot) fires long before the cap; our periodic check reads `wal_status`/`safe_wal_size` on the standby and alerts at `unreserved`. | H-036 |
| `idle_timeout` (18) | A slot inactive for longer than `idle_replication_slot_timeout` is invalidated at the next checkpoint/restartpoint; synced slots on a standby are exempt, ordinary standby slots are not (`CanInvalidateIdleSlot`, slot.c) **[src]**; default 0 = off **[doc]**. | Read the setting on the standby at every acquisition; refuse to rely on a slot whose idle timeout is shorter than 10 × the worst-case hand-over gap (R18: a run is replaced within 90 s plus start-up); alert. | H-037 |

Two further ways to lose the slot without invalidation:

- **The standby itself is lost** (replaced, rebuilt, rewound, re-created from backup). A logical slot on a standby is a
  local object that is never synchronised elsewhere ("we do not support syncing slots to cascading standbys",
  `pg:src/backend/replication/walsender.c`) **[src]**. On PlanetScale, "Replica failures are automatically detected and
  automatically replaced" **[doc]** (<https://planetscale.com/docs/postgres/operations-philosophy.md>). Prevention is
  impossible; SPEC 4.1 answers it with attach-elsewhere + automatic backfill. → H-043, H-047.
- **The standby loses its WAL source.** If the primary removes WAL the standby has not received (no physical slot,
  or the physical slot invalidated by the primary's `max_slot_wal_keep_size`), the standby can no longer stream and
  must be rebuilt, taking the logical slot with it **[doc]** (`wal_keep_size` paragraph, runtime-config-replication).
  → H-043.

## 4. Standby restart

- Logical slots on a standby are persistent and are restored from `pg_replslot/` at start-up (`RestoreSlotFromDisk`,
  with the `wal_level`/`hot_standby` checks above) **[src]**.
- Positions are persisted "only at checkpoint, so in the case of a crash the slot might return to an earlier LSN,
  which will then cause recent changes to be sent again" **[doc]** (logicaldecoding-explanation). On a standby the
  relevant point is the restartpoint. Consequence: after a standby crash, transactions we already acknowledged are
  sent again → applier discards by commit LSN (H-025).
- While the standby is down, the primary keeps the standby's `catalog_xmin` only through the physical slot (if any);
  without one, catalog cleanup on the primary invalidates our slot on replay (H-042).
- After restart, decoding is possible only after the standby reached a consistent state and, if Debezium starts
  before the walreceiver streams, `pg_last_wal_receive_lsn()` is NULL — see debezium.md §2.5 (H-041) **[test]**.
- Test (T1): `tools/localpg kill` the replica during streaming, restart it, verify (a) slot still valid, (b) duplicate
  transactions are discarded, (c) oracle equality; repeat with the physical slot removed and a `VACUUM` of a catalog
  table on the primary during the outage (expect `rows_removed`, the alert, and the automatic backfill).

## 5. Promotion and timeline switches

**Our replica is promoted** (failover or planned switchover onto it):

- Walsenders on the promoted server terminate ("terminating walsender process after promotion",
  `StartLogicalReplication`, walsender.c) **[src]**; clients must reconnect. The logical slot remains (local object)
  and can be used on the new primary; the walsender determines the timeline after waiting for WAL, explicitly to cover
  "the promotion or timeline change cases" (`logical_read_xlog_page`, walsender.c) **[src]**.
- After promotion the server is a primary: Debezium switches from `pg_last_wal_receive_lsn()` to
  `pg_current_wal_lsn()` automatically (it tests `pg_is_in_recovery()` per call) **[src]**. Our pipeline is now in the
  situation "decoding on the primary"; R17 wants replica mode, so the orchestrator either accepts this as primary mode
  until another replica is healthy (SPEC 4.1 rules) or moves to another replica with backfill. Decision for the
  architecture: **staying on the promoted node needs no backfill**; moving does.
- Test (T1): `tools/localpg promote` the decoding replica mid-transaction-stream; verify reconnect, continuity
  (`confirmed_flush_lsn` ≤ applied, no gap in commit LSNs), oracle.

**The primary fails over to another standby** (ours stays a standby):

- Our replica must be re-pointed at the new primary and follow its timeline (`recovery_target_timeline = latest`,
  the default since 12) **[doc]**. Logical decoding reads across the timeline switch (`XLogReadDetermineTimeline`)
  **[src]**.
- If our replica had received WAL beyond the new primary's switch point, it has diverged and must be rewound or
  rebuilt — the slot is lost (H-045). With semi-synchronous commit ("at least one replica should have received all
  the writes that the primary had seen", PlanetScale **[doc]**) our replica can be the one that is *ahead*.
- The new primary has its own physical slots; until our replica's feedback reaches it, catalog cleanup there can
  invalidate our slot on replay (H-042).
- On a managed provider we do not control the re-pointing; PlanetScale automates it **[doc]** (ops philosophy) and
  stage 2 must test it **[test]**.

## 6. Failover slots and slot synchronisation (17+), and what a provider must expose

These matter for **primary mode** (SPEC 4.1, non-default). They do not help replica mode (a standby's slot is never
synchronised, §3).

- A failover slot is created with `failover = true` (`pg_create_logical_replication_slot(..., failover => true)`, or
  `CREATE_REPLICATION_SLOT … (FAILOVER)`, or `ALTER_REPLICATION_SLOT … (FAILOVER true)`) **[doc]**
  (<https://www.postgresql.org/docs/18/logicaldecoding-explanation.html#LOGICALDECODING-REPLICATION-SLOTS-SYNCHRONIZATION>).
- Each standby needs `sync_replication_slots = on`, `hot_standby_feedback = on`, a physical slot
  (`primary_slot_name`) and a `dbname` in `primary_conninfo` **[doc]** (same section).
- `synchronized_standby_slots` on the primary is "highly recommended … to prevent the subscriber from consuming
  changes faster than the hot standby"; logical walsenders then wait for those physical slots, and "logical replication
  will not proceed if the slots specified in the synchronized_standby_slots do not exist or are invalidated" **[doc]**
  (<https://www.postgresql.org/docs/18/runtime-config-replication.html>). Without it, our consumer can apply
  transactions that the promoted standby never received (H-029).
- Readiness: only slots with `synced = true`, `temporary = false` and `invalidation_reason IS NULL` on the standby
  before failover are usable; a standby persists a synced slot only after the consumer advanced the slot while that
  standby was following ("If the logical replication slot is actively used by a consumer, no manual intervention is
  needed") **[doc]** (logicaldecoding-explanation; PlanetScale blog
  <https://planetscale.com/blog/postgres-ha-with-cdc>). Our consumer advances the slot continuously (heartbeat every
  10 s), so freshly replaced standbys become eligible within one sync cycle **[test]**.
- After promotion, `synced` may remain true on the new primary ("may (if leftover from a promoted standby) also be
  true") **[doc]** (view docs).

**A provider must expose, for primary mode:** `wal_level = logical`; `failover = true` on slot creation (via SQL or the
replication protocol); `sync_replication_slots = on` and `hot_standby_feedback = on` on the HA standbys; physical slots
for those standbys; ideally `synchronized_standby_slots`; read access to `pg_replication_slots` on the primary (and,
for verification after failover, on the promoted node); `max_slot_wal_keep_size`. **For replica mode:** a stable,
deterministic connection to one specific standby that accepts **replication-protocol** connections; `hot_standby_feedback
= on` and a physical slot for that standby; `max_replication_slots`/`max_wal_senders` on the standby; visibility of
`pg_stat_wal_receiver`, `pg_last_wal_replay_lsn()`, `pg_last_xact_replay_timestamp()` and the standby's settings.

## 7. Replay lag and decoding latency

- A logical walsender on a standby sends only what has been **replayed**: `WalSndWaitForWal` sets
  `RecentFlushPtr = GetXLogReplayRecPtr(NULL)` when `RecoveryInProgress()` (walsender.c) **[src]**. (The page-read
  path may read received-but-unreplayed WAL, `GetStandbyFlushRecPtr`, but decoding output is gated by replay.) Decoding
  latency = primary commit → standby receive → standby replay → decode → client.
- Replay can be delayed by: recovery conflicts waiting up to `max_standby_streaming_delay` (default 30 s, "the maximum
  total time allowed to apply WAL data once it has been received") **[doc]**; `recovery_min_apply_delay` (delays every
  commit record) **[doc]**; `pg_wal_replay_pause()` (pauses indefinitely) **[doc]**
  (<https://www.postgresql.org/docs/18/functions-admin.html>); I/O saturation of the replica.
- Our own sessions on the replica cause replay conflicts: backfill `SELECT`s and catalog polls hold `ACCESS SHARE`
  locks that conflict with `ACCESS EXCLUSIVE` locks replayed from the primary (DDL, `TRUNCATE`, `VACUUM` truncation),
  and their snapshots conflict with cleanup records (mitigated by `hot_standby_feedback`) **[doc]**
  (<https://www.postgresql.org/docs/18/hot-standby.html#HOT-STANDBY-CONFLICT>). A 30 s replay stall is a 30 s latency
  spike for *every* table; then our query is cancelled (H-048).
- SPEC 5.2 counts replica lag in latency (p99 ≤ 30 s, max ≤ 60 s). Measurement: canary row on the primary (SPEC 5.2)
  plus per-interval metrics on the replica: `pg_last_wal_receive_lsn()`, `pg_last_wal_replay_lsn()`,
  `now() - pg_last_xact_replay_timestamp()`, `pg_is_wal_replay_paused()`/`pg_get_wal_replay_pause_state()`,
  `SHOW recovery_min_apply_delay`, `pg_stat_database_conflicts`.
- **Prevention (H-044, H-048):** refuse replica mode when `recovery_min_apply_delay > 0` or replay is paused (alert,
  `replica.fallback_after` rules); set `lock_timeout` and short `statement_timeout`s on our catalog polls; backfill reads
  in chunks that each finish well under `max_standby_streaming_delay` so a conflicting replay waits at most one chunk;
  log `pg_stat_database_conflicts` deltas.

## 8. Catalog horizon effects on the primary

- `hot_standby_feedback` sends both the standby's oldest query snapshot (`xmin`) and its slots' oldest `catalog_xmin`
  upstream; the primary holds them in the standby's physical slot and VACUUM "cannot remove catalog tuples deleted by
  any later transaction" **[doc]** (view docs: `xmin`, `catalog_xmin`; hot-standby: feedback "can cause database bloat
  on the primary").
- Effects of our pipeline on the primary, through the replica: (a) **catalog bloat** while our slot lags (its
  `catalog_xmin` stays at the decoded position); (b) **table bloat** while any long snapshot is open on the replica
  — our backfill chunks and any `REPEATABLE READ` snapshot used for the snapshot hand-off; (c) in the extreme, anti-
  wraparound pressure: "In extreme cases this could cause the database to shut down to prevent transaction ID
  wraparound" **[doc]** (logicaldecoding-explanation, caution box).
- **Prevention (H-046):** bounded by the R21 lag trigger (the backlog is abandoned after 2 h of non-shrinking lag, which
  must free the old slot or advance it); our periodic check reads `age(catalog_xmin)` and `age(xmin)` of the physical
  slot on the primary and of our slot on the replica and alerts above a threshold (default 50 M xids); backfill chunks
  use short snapshots (no multi-hour `REPEATABLE READ` on the replica). When the Flight is stopped for long, the slot
  keeps holding the horizon: the hand-over/fallback logic must drop an abandoned slot whose position is older than the
  lag-trigger bound.

## 9. What changed in 16, 17 and 18

| Version | Change | Source |
|---|---|---|
| 16 | Logical decoding on standbys; `pg_log_standby_snapshot()`; `pg_replication_slots.conflicting` | <https://www.postgresql.org/docs/release/16.0/> **[doc]** |
| 15 (context) | "Prevent logical replication of empty transactions" — pgoutput sends BEGIN lazily, so transactions that touch no published table produce no messages at all (H-026) | <https://www.postgresql.org/docs/release/15.0/> **[doc]** |
| 17 | Failover slots (`failover` option), `sync_replication_slots`, slotsync worker, `pg_sync_replication_slots()`, `synchronized_standby_slots` | <https://www.postgresql.org/docs/release/17.0/> **[doc]** |
| 17 | `pg_replication_slots.invalidation_reason`, `inactive_since` | same **[doc]** |
| 17 | `pg_upgrade` preserves logical slots (17 → later) | same **[doc]** |
| 17 | Faster decoding of transactions with many subtransactions | same **[doc]** |
| 18 | `idle_replication_slot_timeout` (new invalidation cause `idle_timeout`) | <https://www.postgresql.org/docs/release/18.0/> **[doc]** |
| 18 | Stored generated columns can be published (`publish_generated_columns`, column lists); generated columns are **virtual by default** and virtual ones are never published | same **[doc]** |
| 18 | pgoutput skips a missing publication with a WARNING instead of failing (silent loss risk, H-034) | `pg:…/pgoutput.c` `LoadPublications` 18 vs 17 **[src]** |
| 18 | `max_active_replication_origins` split from `max_replication_slots` | release notes **[doc]** |
| 2026 minor releases | `output_plugin_libraries` GUC (trusted output plugins; default includes pgoutput) | runtime-config-replication **[doc]**, logical.c **[src]** |

## 10. PlanetScale Postgres (public documentation, read 2026-11-09)

| Topic | What the documentation says | URL |
|---|---|---|
| Versions | PostgreSQL 17 and 18; currently 17.11 and 18.6; no in-place major upgrades | <https://planetscale.com/docs/postgres/cluster-configuration/versions.md> |
| Topology | Production clusters: 1 primary + 2 replicas in 3 AZs; semi-synchronous ("at least one replica should have received all the writes"); replicas also serve reads | <https://planetscale.com/docs/postgres/operations-philosophy.md>, <https://planetscale.com/docs/postgres/scaling/replicas.md> |
| Replica addressing | Append `\|replica` to the user name, direct port 5432; PgBouncer (6432) "does not support replica routing"; for dedicated read replicas "The generated username ends in `\|replica`, which distributes connections across the read-only instances" | replicas.md; <https://planetscale.com/docs/postgres/scaling/dedicated-read-replicas.md>; <https://planetscale.com/docs/postgres/connecting.md> |
| Replica failure | "Replica failures are automatically detected and automatically replaced"; connections to a failed replica are terminated and re-routed "to the other available replica"; a replacement catches up in "minutes to hours" | operations-philosophy.md |
| Failover | Automatic "reparenting" in seconds; planned switchovers via dashboard/CLI/API (`POST …/branches/$BRANCH/switchovers`, optional `candidate`); a failed switchover has an "unconfirmed outcome" | replicas.md; <https://planetscale.com/docs/postgres/troubleshooting/switchovers.md> |
| Restarts | Parameter changes that need a restart are rolling: replicas restarted first, then a switchover, then the old primary | <https://planetscale.com/docs/postgres/cluster-configuration/parameters.md> |
| Logical replication parameters | `wal_level = logical`; `max_replication_slots` and `max_wal_senders` "2 x replicas"; `max_slot_wal_keep_size` "> 4GB"; for failover: `sync_replication_slots = on`, `hot_standby_feedback = on`; PlanetScale-specific `logical_slot_sync_timeout` (default 300 s, "Time to wait for logical slot sync on standbys before failover proceeds") | <https://planetscale.com/docs/postgres/integrations/logical-cdc.md> |
| Slot policy | Slots should be created with `failover = true`; "If PlanetScale detects a logical replication slot with failover = false, you will be notified and given 3 days to update the configuration. If the slot is not reconfigured … PlanetScale will resume performing planned failovers as normal" (implies planned failovers are held back while such a slot exists) | logical-cdc.md |
| Roles | No `SUPERUSER`; default role has `REPLICATION`, `BYPASSRLS`, `CREATEROLE`, `pg_monitor`, `pg_signal_backend`, `pg_read_all_settings`, `pg_create_subscription`, …; user-defined roles can be created `WITH REPLICATION` via API (`with_replication: true`, inheriting `postgres`) or SQL; SQL-created roles log in as `name.<branch id>` | <https://planetscale.com/docs/postgres/postgres-compatibility.md>; logical-cdc.md; <https://planetscale.com/docs/postgres/connecting/roles.md> |
| Publications | "You will need to do this as the owner of the tables or the default role"; tables added individually (no `FOR ALL TABLES` without superuser) | logical-cdc.md |
| Replication protocol | Documented for the primary (`replication=database` connection to run `ALTER_REPLICATION_SLOT`) | logical-cdc.md |
| Storage | "Storage autoscaling is enabled by default for network-attached storage clusters" | postgres-compatibility.md |
| Replicas and logical decoding | **Not mentioned.** No page documents creating a logical slot on a replica, replication-protocol connections to a replica, or the effect of a replica slot on failover policy | — |
| HA-with-CDC caveats | Synced slots become failover-eligible only after the consumer advances them while the standby follows; quiet consumers block planned switchovers | <https://planetscale.com/blog/postgres-ha-with-cdc> |

### What stage 2 must test on PlanetScale (P7, first)

1. **Addressing one replica deterministically.** Does `|replica` pin a connection to one instance, or distribute
   across both HA replicas (as documented for dedicated read replicas)? Identify the instance behind each connection
   (e.g. `pg_control_system()` is identical across physical replicas, so use `inet_server_addr()`,
   `pg_postmaster_start_time()`, the replica's own `pg_stat_wal_receiver.sender_host`, or the slot list) **[test]**.
   If routing is not deterministic, replica mode needs an instance-specific endpoint from PlanetScale (H-047).
2. Does the replica endpoint accept **replication-protocol** connections (`replication=database`) through the proxy?
   With TLS `verify-full` and pgjdbc (does the proxy need `sslnegotiation=direct`, as the docs' psql examples use, and
   does pgjdbc 42.7.13 support it)? **[test]**
3. `wal_level`, `hot_standby_feedback`, `primary_slot_name`, `max_replication_slots`, `max_wal_senders`,
   `max_slot_wal_keep_size`, `idle_replication_slot_timeout`, `recovery_min_apply_delay`,
   `max_standby_streaming_delay`, `output_plugin_libraries`, `logical_decoding_work_mem` on the primary and on each
   replica; physical slots on the primary (one per replica?) **[test]**.
4. Create a logical slot on a replica as the runtime role (`pg_create_logical_replication_slot` and
   `CREATE_REPLICATION_SLOT`); time it with an idle primary and with the heartbeat; `pg_log_standby_snapshot()`
   privilege **[test]**.
5. Decode under load (lag, replay lag, `pg_stat_database_conflicts`), long idle period (≥ 1 h) **[test]**.
6. Slot fate across: replica restart (parameter change with restart), primary restart, planned switchover with our
   replica as candidate and as non-candidate, unplanned failover, replica replacement **[test]**. Does PlanetScale's
   "failover = false" detector flag our replica slot, and does it hold back planned failovers (cost-neutral, but it
   changes the provider's behaviour)? **[test]**
7. Privileges for publication maintenance (ownership of tables), `pg_logical_emit_message`, terminating our own
   walsender (`pg_terminate_backend` via `pg_signal_backend`), reading `pg_replication_slots` on replicas **[test]**.
8. **Cost:** does retained WAL (slot lag) trigger storage autoscaling? Record the baseline first (PLAN §5); keep
   `max_slot_wal_keep_size` and the size-lag trigger below the autoscaling threshold (H-038) **[test]**.
9. Primary mode: create a `failover = true` slot; verify `synced` on both replicas; run a switchover; resume without
   backfill; measure `logical_slot_sync_timeout` effects **[test]**.
