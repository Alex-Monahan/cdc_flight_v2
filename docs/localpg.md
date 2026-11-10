# `tools/localpg` — local Postgres for tier T1

`tools/localpg` is the only way this build manages its local Postgres clusters (PLAN §5). It is one Python 3 file
with no third-party imports, and it runs the Homebrew formula's own binaries (`initdb`, `pg_ctl`, `pg_basebackup`,
`psql`). It never uses `brew services`, and nothing it starts runs at login.

```sh
tools/with-env tools/localpg up          # build (first time) or start primary + replica; a no-op if both run
tools/with-env tools/localpg status      # table; --json for programs
tools/with-env tools/localpg down        # stop everything; no process is left behind
```

## Layout and defaults

| What | Default | Override |
|---|---|---|
| Binaries | `/opt/homebrew/opt/postgresql@18/bin` | `$CDC_V2_PG_BIN`, `--pg-bin` |
| Home (data, TLS, passwords, logs, socket) | `~/.cache/cdc_flight_v2/pg` | `$CDC_V2_PG_HOME`, `--home` |
| Ports | primary 55432, replica 55433, more standbys 55434–55439 | `$CDC_V2_PG_PORT_BASE`, `--port-base` |

A home uses 8 ports, from the base to base + 7. A base whose range would include 5432, or a privileged port, is
refused (exit 2). 55440 and above belong to the TCP proxies. The self-tests use the home
`~/.cache/cdc_flight_v2/pg-selftest` and ports 55460–55467, so they never touch a development cluster. Once a home is
built, its port base is stored in `state.json`. A different explicit base for that home is refused.

```
<home>/state.json            nodes, ports, upstreams, setting overrides
<home>/nodes/<node>/data     data directories (postgresql.conf includes localpg.conf, then localpg_overrides.conf)
<home>/tls/                  private CA (ca.crt, ca.key) and the server certificate for localhost (server.crt/.key)
<home>/secrets/              passwords.json and pgpass, mode 600 (directory mode 700)
<home>/logs/<node>.log       server logs
<home>/run/                  Unix socket directory, mode 700 (localpg's own connections)
```

## Commands

Exit codes: 0 ok · 1 error (one line on stderr) · 2 usage error or refused · 3 (`status`) not every node is running.
Nodes are named by name or by port (`localpg kill 55433`).

| Command | What it does |
|---|---|
| `up` | Builds the cluster if the home is empty or half-initialised, otherwise starts every stopped node: primaries first, then standbys. It waits until each standby is streaming. With everything already running it is a no-op. After `kill` it recovers the node (crash recovery). |
| `down` | Stops every node of the home, standbys first, with a fast shutdown. If that fails it escalates to an immediate shutdown and then to SIGKILL. A frozen (SIGSTOPped) postmaster gets SIGCONT first. `down` returns only when the postmaster and all its children are gone. |
| `reset` | Stops whatever runs under the home (found through the pid files, even without a state file), wipes the home, rebuilds it with new passwords and a new CA, and starts it. It works from any state. It refuses a non-empty directory that lacks the `.localpg-home` marker. |
| `status [--json]` | Shows each node: running or stopped, pid, role, port, server version, timeline, current or replay LSN, WAL receiver state and replay-pause state. It also shows replication state and lag (bytes, and seconds from `pg_stat_replication`), setting overrides, and the replication slots (name, type/plugin, active, `wal_status`, failover, synced, restart LSN). |
| `add-standby [--name N] [--from P]` | Creates a physical slot `cdc_v2_phys_<N>` on the running primary, takes a `pg_basebackup` (`-X stream -S slot`), configures the new node as a standby and starts it. The name defaults to `standby<port offset>`, on the next free port. |
| `promote <node> [--json]` | Runs `pg_ctl promote` and waits until the node accepts writes. It reports the old and new timelines. It also drops the node's physical slot on its former upstream when that upstream is running. Other standbys are not re-pointed; use `follow` for that. |
| `follow <standby> [<upstream>]` | Points a standby at another primary, for example after a promotion. It creates a slot on the new upstream, restarts the standby and waits until it streams on the new timeline. |
| `kill <node>` | Crash test. It sends SIGSTOP to the postmaster so that it cannot fork, collects the postmaster's children, sends SIGKILL to the children and then to the postmaster, and waits until all of them are gone. No orphaned backend survives. |
| `remove <node>` | Stops a node, drops its slot on its upstream and deletes its data directory. Use it, for example, for an old primary after a failover drill. |
| `set <node\|all> NAME=VALUE…` / `unset <node\|all> NAME…` | Writes per-node overrides to `localpg_overrides.conf`, reloads the node, and restarts it when a setting needs a restart (`pending_restart`). Example: `localpg set primary max_slot_wal_keep_size=64MB`. Unknown settings and settings that localpg manages (port, sockets, TLS, `primary_conninfo`) are refused. |
| `conninfo [--json] [--env admin\|runtime [--node N]] [--show-passwords]` | Shows hosts, ports, database, role names, the CA path and the paths of the two password files. It prints passwords only with `--show-passwords`. `--env` prints libpq `export` lines. |
| `sql <node> [-c SQL] [--role bootstrap\|admin\|runtime] [--db D]` | Runs SQL. The bootstrap role goes through the Unix socket. The admin and runtime roles go over TCP with `verify-full`. |
| `logs <node> [-n N]` | Prints the end of a node's server log. |

Every subprocess has a timeout (10 s for quick tools, 60 s for SQL, 90 s for `pg_ctl`, 600 s for `initdb` and
`pg_basebackup`). Mutating commands take an exclusive lock on the home, so a second concurrent invocation waits for
up to 300 s.

## Postgres versions

localpg runs the binaries of `--pg-bin` / `$CDC_V2_PG_BIN`. The local tiers use Postgres 18 (the default); Postgres
16 is installed as well (`--pg-bin /opt/homebrew/opt/postgresql@16/bin`), on a home of its own. What depends on the
version:

- The major version of a home is the one of its data directories (`PG_VERSION`); `state.json` records it as
  `pg_major` when the home is built. `up` (and every start) refuses binaries of another major version with exit 2
  and a message; use the matching binaries, another home, or `reset`.
- Settings that a version does not have are not written: `sync_replication_slots` only from 17 on. `set` refuses
  `sync_replication_slots` and `synchronized_standby_slots` (17+) and `idle_replication_slot_timeout` (18+) on an
  older node with exit 2 and "needs Postgres N or later", instead of an SQL error.
- `status` reads every column of `pg_replication_slots` that the server has; `failover`, `synced`,
  `invalidation_reason` (17+) are `null` on 16 (shown as `-`). `status --json` also reports `pg_major` (per node
  and for the home), `bin_major` (the binaries) and `server_version_num`.
- Failover slots (`failover = true`) and their synchronisation exist from 17 on. On 16 the self-tests that need
  them are skipped; logical decoding on a standby and `pg_log_standby_snapshot()` exist from 16 on.

## Topology and settings

- **Primary**: `wal_level = logical`. **Standbys**: they are created with `pg_basebackup` and stream through a
  physical slot (`primary_slot_name = cdc_v2_phys_<node>`) with `hot_standby_feedback = on`, so logical decoding on
  the replica works. On 17 and later they also have `sync_replication_slots = on`, and their `primary_conninfo` names a
  database, so failover slots (`failover = true`) that are created on the primary are synchronised to them.
- Each instance gets `shared_buffers = 128MB`, `max_connections = 50`, `max_wal_senders = 20`,
  `max_replication_slots = 20`, `max_worker_processes = 16` and `wal_retrieve_retry_interval = 1s` (the default is
  5 s; with 1 s, standbys reconnect quickly after a primary restart or crash). It listens on `localhost` (127.0.0.1 and ::1) only.
  Every other setting is the Postgres default (`max_slot_wal_keep_size = -1`, `wal_keep_size = 0`,
  `synchronized_standby_slots` empty).
- **TLS**: on, with a private CA (EC P-256). The server certificate covers `DNS:localhost`, `IP:127.0.0.1` and
  `IP:::1`. `pg_hba.conf` accepts TCP only as `hostssl … scram-sha-256` and rejects `hostnossl` explicitly. The Unix
  socket directory has mode 700 and also requires SCRAM.

## Roles and passwords

| Role | Attributes | Used by |
|---|---|---|
| `cdc_v2_boot` | superuser (bootstrap, `initdb -U`) | localpg only. It is rejected over TCP and reachable only through the private socket. Standbys use it for physical replication. |
| `cdc_v2_admin` | `LOGIN CREATEDB CREATEROLE REPLICATION`, member of `pg_monitor` and `pg_signal_backend`, not a superuser | Owns `cdc_v2_test`. It can create roles, schemas, tables and publications, as a managed provider's admin role can. It cannot use `ALTER SYSTEM`, create superusers or create `FOR ALL TABLES` publications. |
| `cdc_v2_runtime` | `LOGIN REPLICATION`, not a superuser | It is created by the admin role. It has no table privileges: grant what a test needs from the admin role. |

Passwords are random (32 URL-safe characters). They are generated at build or reset and stored in
`<home>/secrets/passwords.json` (`{"bootstrap"|"admin"|"runtime": {"user", "password"}}`) and in
`<home>/secrets/pgpass` (libpq format, for `PGPASSFILE` / `passfile=`). Both files have mode 600. Roles are created
with a SCRAM verifier that localpg computes itself, so the plain text never reaches the server or its log. Clients
connect like this:

```
host=localhost port=55433 dbname=cdc_v2_test user=cdc_v2_runtime sslmode=verify-full
sslrootcert=<home>/tls/ca.crt passfile=<home>/secrets/pgpass
```

## Safety rules

- localpg never connects to port 5432 and never reads `/opt/homebrew/var`. Every `psql` and `pg_*` call names its host
  (the private socket directory) and its port explicitly. That port is checked against the home's range before the
  call. All `PG*` variables of the caller are removed from the subprocess environment.
- A server process is signalled only after localpg has identified it through a `postmaster.pid` in one of the home's
  data directories. The pid file must name that directory, and the process start time must match the file within
  5 s, which guards against a reused pid. The children of such a verified postmaster may also be signalled.
  localpg never matches process names (no `pkill`, no `killall`). On a timeout, localpg also kills its own direct
  subprocess.

## Known behaviour (handle it in tests)

- **Creating a logical slot on a standby waits** for a running-transactions record from the primary. An idle primary
  writes one only about every 15 s. Call `select pg_log_standby_snapshot()` on the primary (superuser, or granted
  `EXECUTE`) repeatedly while the creation runs. `tests/localpg/test_localpg.py::create_slot_on_standby` shows how.
- **Slot synchronisation creates the standby's copy only once the primary's failover slot has moved past what the
  standby can guarantee.** Until then the standby logs `could not synchronize replication slot … Synchronization could
  lead to data loss, because the remote slot needs … catalog xmin N, but the standby has … catalog xmin M`. A consumer
  that keeps advancing the slot resolves this by itself. A test advances the slot
  (`pg_replication_slot_advance(slot, pg_current_wal_lsn())`) after a fresh `pg_log_standby_snapshot()`, repeated
  until `synced and not temporary` (`wait_slot_synced`).
- **A standby needs `max_connections`, `max_worker_processes`, `max_wal_senders`, `max_prepared_transactions` and
  `max_locks_per_transaction` at least as high as its primary.** If you raise one of them on the primary only, the
  standby pauses replay (`recovery has paused`, shown by `status`) and needs a restart after the fix. Use
  `localpg set all …` for these settings; `set` warns when they are applied to a single node.
- **Promotion does not re-point the other standbys and does not fence the old primary.** In a failover drill, `kill`
  the old primary, `promote` the standby, `follow` the remaining standbys and `remove` the old primary. `reset`
  restores the standard primary + replica pair.
- `synchronized_standby_slots` is empty by default, so logical walsenders on the primary do not wait for the
  standbys. A primary-mode test that needs that guarantee sets it explicitly:
  `localpg set primary synchronized_standby_slots=cdc_v2_phys_replica`.

## Self-tests

```sh
tools/with-env uv run pytest -m localpg tests/localpg   # opt-in: the default run deselects the marker localpg
CDC_V2_PG_BIN=/opt/homebrew/opt/postgresql@16/bin tools/with-env uv run pytest -m localpg tests/localpg
```

Run them one at a time: they share one home (`pg-selftest`, ports 55460–55467). A run with binaries of another
major version than the home's rebuilds the home (`reset`). On 16 the two failover-slot tests are skipped (measured
2026-11-10: 18.6: 24 passed; 16.6: 22 passed, 2 skipped).
