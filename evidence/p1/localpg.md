# P1 evidence: `tools/localpg`

Date: 2026-11-09. Machine: macOS (Apple silicon, 16 GB) with PostgreSQL 18.6 (Homebrew `postgresql@18`), run from
`$CDC_V2_PG_BIN`. The usage reference is `docs/localpg.md`. This file contains no password. Paths under the user's
home are shown as `~`.

## Test run

The command is `tools/with-env uv run pytest -m localpg tests/localpg`. All 22 tests carry the marker `localpg`;
the default `pytest` run deselects that marker (it stays hermetic), so the suite is selected explicitly. The self-tests use their own home (`~/.cache/cdc_flight_v2/pg-selftest`) and
ports 55460–55467.

- Final recorded run, after the last code change: **22 passed in 44.60 s**. Nothing failed and nothing was skipped.
- Three back-to-back runs before the last two small changes also passed 22/22 each (53.6 s, 54.1 s, 52.4 s).
- The rest of the repository's tests: `pytest -m "not localpg"` → 8 passed, 22 deselected.

| Test | What it verifies |
|---|---|
| `test_refuses_port_base_that_includes_5432_or_is_privileged` [5432, 5425, 5430, 80] | `up`, `status` and `reset` exit 2 with "refused" for a port range that contains 5432 or a privileged port. This holds whether the base comes from the command line or from the environment. Nothing is created and nothing is started. |
| `test_refuses_foreign_or_unmarked_home` | A home under `/opt/homebrew/var` is refused without being read. `reset` refuses a non-empty directory that has no `.localpg-home` marker, and the file in that directory is left in place. |
| `test_reset_rebuilds_from_half_initialised_state` | (1) The state says "not initialised" while both nodes run: `up` stops them and rebuilds. (2) The state file is deleted and a stray node directory exists while the nodes run: `reset` stops the old processes through their pid files and rebuilds a clean primary + replica. No old pid survives. |
| `test_up_is_idempotent` | A second `up` reports "already running" for both nodes, keeps the same pids and takes less than 5 s. |
| `test_down_leaves_no_process_and_up_restarts` | After `down`, every postmaster and child pid recorded before it is gone. No port accepts TCP, no `postmaster.pid` remains and `status` exits 3. A second `down` is a no-op. `up` restarts the nodes with their data intact, and replication resumes. |
| `test_frozen_postmaster_does_not_hang_status_or_down` | With the replica's postmaster frozen by SIGSTOP, `status` returns in under 20 s and flags the node. `down` finishes in under 30 s and leaves no process. |
| `test_status_reports_nodes_and_slots` | `status --json` reports role, port, version 18.x, timeline, LSNs, replay lag, the replication state `streaming` and the physical slot (`active`, `wal_status = reserved`). The human-readable table shows the same. |
| `test_row_written_on_primary_is_visible_on_replica` | A row inserted on the primary can be read on the replica. A write on the replica fails as read-only. |
| `test_settings_on_replica` | The replica runs with `hot_standby_feedback = on`, `wal_level = logical`, `sync_replication_slots = on`, `primary_slot_name` set, `shared_buffers = 128MB`, `listen_addresses = localhost`, `ssl = on` and SCRAM password encryption. |
| `test_logical_slot_with_pgoutput_on_replica_delivers_changes` | The **runtime** role creates a `pgoutput` slot **on the replica**; `pg_log_standby_snapshot()` on the primary releases the creation wait. An insert on the primary is decoded on the replica as B/R/I/C messages with the value present. A second read returns nothing, so each change is consumed once. |
| `test_failover_slot_is_synchronised_to_standby` | The runtime role creates a `failover = true` slot on the primary. It appears on the replica as `synced`, not `temporary`, with `failover` set. Dropping it on the primary removes the synced copy. |
| `test_tls_verify_full_and_no_plaintext_tcp` | `sslmode=verify-full` with the private CA works for both `localhost` and `127.0.0.1` (`pg_stat_ssl.ssl = true`, TLSv1.x). `sslmode=disable` is refused with "no encryption". The system CA store does not verify the server. The bootstrap superuser is rejected over TCP. A wrong password fails SCRAM. The libpq password file works on its own. |
| `test_roles_have_managed_provider_privileges` | The three `cdc_v2*` roles have the attributes in the Roles table below, and their passwords are stored as SCRAM verifiers. The admin role can create a schema, a table, a publication and a `REPLICATION` role, but it is refused `ALTER SYSTEM` and `CREATE ROLE … SUPERUSER`. The runtime role is not a superuser and cannot create roles. |
| `test_password_files_and_conninfo` | `passwords.json` and `pgpass` have mode 600, and their directory has mode 700. `conninfo --json` lists hosts, ports, database, roles, the CA and both file paths. No password appears in the output of `conninfo`, `conninfo --json`, `conninfo --env`, `status`, `status --json` or `up`, nor in any server log. Only `--show-passwords` prints them. |
| `test_kill_primary_then_up_recovers_without_data_loss` | The primary holds 5,000 committed rows and an open client backend. `kill primary` leaves no postmaster and no orphaned child, and the port is closed. `up` recovers all 5,000 rows, and a new row reaches the replica. A second `kill` is a no-op. |
| `test_kill_replica_then_up_catches_up` | `kill 55461` addresses the replica by port. Rows written while the replica is down arrive after `up`. |
| `test_set_reloads_or_restarts_a_node` | `set primary max_slot_wal_keep_size=64MB` reloads the primary, and the replica stays at `-1`. `shared_buffers=64MB` restarts the node (new pid). Unknown settings and settings managed by localpg are refused with exit 2. A standby-matched setting on one node prints a warning. `unset` restores the defaults, and the replica's replay is not paused. |
| `test_add_standby_failover_promote_and_follow` | `add-standby` creates `standby2` on base+2; it streams and its slot is active on the primary. Failover drill: `kill primary`; `promote replica` (timeline **1 → 2**, accepts writes; a second promote fails); `follow standby2 replica` (streams on timeline 2 and receives the new row); `remove primary`; `add-standby --name standby0` reuses the freed port. A final `down` leaves no process. |
| `test_promote_failover_slot_survives_on_promoted_standby` | Primary mode (SPEC 4.1): a failover slot synced to the replica can be consumed after `kill primary` + `promote replica`. It delivers the change made on the new primary and not the change consumed before the failover. |

The slowest tests in the final run: the rebuild from a half-initialised state took 8.1 s, the failover, promote and
follow drill 7.2 s, the frozen-postmaster test 6.1 s, the failover-slot promotion 4.2 s and the set/reload/restart
test 3.4 s. Every other test took less than 2 s.

## Lifecycle timings (development home `~/.cache/cdc_flight_v2/pg`, ports 55432–55434)

Produced by `tools/with-env python3 tools/checks/localpg_timings.py`. Each time is the wall-clock time of the whole
command.

| Step | Command | Seconds | Output |
|---|---|---|---|
| reset (wipe + build from nothing) | `localpg reset` | 2.20 | reset: wiped ~/.cache/cdc_flight_v2/pg and started primary (55432) and replica (55433) in 2.1 s |
| up, already running (no-op) | `localpg up` | 0.12 | up: primary (55432) already running; replica (55433) already running (0.1 s) |
| down (2 nodes) | `localpg down` | 0.42 | down: stopped replica, primary (0.4 s) |
| up from stopped (2 nodes) | `localpg up` | 0.73 | up: primary (55432) started; replica (55433) started (0.6 s) |
| add-standby | `localpg add-standby` | 0.88 | add-standby: standby2 on port 55434 streaming from primary (0.8 s) |
| kill primary (SIGKILL) | `localpg kill primary` | 0.18 | kill: SIGKILL primary: postmaster 59733 and 12 children |
| up after kill (crash recovery) | `localpg up` | 1.00 | up: primary (55432) started; replica (55433) already running; standby2 (55434) already running (0.9 s) |
| promote standby2 | `localpg promote standby2` | 0.28 | promote: standby2 (55434) is primary, timeline 1 -> 2 (0.2 s) |
| down (3 nodes) | `localpg down` | 0.59 | down: stopped replica, primary, standby2 (0.5 s) |
| reset from a 3-node, 2-primary state | `localpg reset` | 2.09 | reset: wiped ~/.cache/cdc_flight_v2/pg and started primary (55432) and replica (55433) in 2.0 s |
| down (final) | `localpg down` | 0.41 | down: stopped replica, primary (0.4 s) |

The 12 children of the killed primary are its backends and auxiliary processes, among them the checkpointer, the WAL
writer, the WAL senders and the I/O workers. All of them were killed together with the postmaster. The "up after
kill" step includes crash recovery and the wait until both standbys stream again. It relies on
`wal_retrieve_retry_interval = 1s`; with the default of 5 s the same step took 5.0 s.

`localpg status` after the final reset:

```
home ~/.cache/cdc_flight_v2/pg, port base 55432
NODE     PORT   STATE    ROLE     VERSION          TLI  LSN (current/replay)  UPSTREAM  REPLICATION  LAG
primary  55432  running  primary  18.6 (Homebrew)  1    0/3000000             -         1 sender(s)  -
replica  55433  running  standby  18.6 (Homebrew)  1    0/3000000             primary   streaming    0 B / 0.00 s
slots:
  NODE     SLOT                 TYPE      ACTIVE  WAL_STATUS  FAILOVER  SYNCED  RESTART_LSN
  primary  cdc_v2_phys_replica  physical  yes     reserved    no        no      0/3000000
```

## Settings in force

| Setting | primary | replica |
|---|---|---|
| `server_version` | 18.6 (Homebrew) | 18.6 (Homebrew) |
| `wal_level` | logical | logical |
| `hot_standby` | on | on |
| `hot_standby_feedback` | on | on |
| `sync_replication_slots` | on | on |
| `primary_slot_name` | '' | cdc_v2_phys_replica |
| `synchronized_standby_slots` | '' | '' |
| `wal_retrieve_retry_interval` | 1s | 1s |
| `max_slot_wal_keep_size` | -1 | -1 |
| `wal_keep_size` | 0 | 0 |
| `shared_buffers` | 128MB | 128MB |
| `max_connections` | 50 | 50 |
| `max_wal_senders` | 20 | 20 |
| `max_replication_slots` | 20 | 20 |
| `max_worker_processes` | 16 | 16 |
| `listen_addresses` | localhost | localhost |
| `port` | 55432 | 55433 |
| `ssl` | on | on |
| `ssl_min_protocol_version` | TLSv1.2 | TLSv1.2 |
| `password_encryption` | scram-sha-256 | scram-sha-256 |
| `data_checksums` | on | on |

`pg_hba.conf` accepts TCP only through `hostssl … scram-sha-256`. It rejects `hostnossl` explicitly and rejects
the bootstrap superuser over TCP. The Unix socket directory has mode 700 and also requires SCRAM.

## Roles

| Role | superuser | replication | createrole | createdb |
|---|---|---|---|---|
| `cdc_v2_admin` | False | True | True | True |
| `cdc_v2_boot` | True | True | True | True |
| `cdc_v2_runtime` | False | True | False | False |

`cdc_v2_admin` is also a member of `pg_monitor` and `pg_signal_backend` and owns the database `cdc_v2_test`. It
created `cdc_v2_runtime` and the database.

## Final state

`localpg down` stopped every cluster of both homes, and `status` exits 3 for both. Nothing listens on ports
55432–55479. localpg never connected to, signalled or read anything belonging to the Postgres on port 5432. Every
client call names the private socket directory and a port from the home's range, and the code checks that port.
A port range that contains 5432 is refused, as tested above.
