# Flight platform facts (P2 spike, PLAN §2 A 4 and platform basics)

Measured on 2026-11-10 (UTC) with a real **F16** Flight `cdc_v2_spike_hello` (source `flights/spike_hello/main.py`,
driver `tools/spike/flight_platform_probe.py`, tool `tools/flightctl`). Each fact is marked **documented** (MotherDuck
public documentation, link given) or **measured** (evidence file under `evidence/p2/flight_platform/`). Where the two
differ, the measurement wins and the difference is listed in §9.

Documentation read: [Flights SQL functions](https://motherduck.com/docs/sql-reference/motherduck-sql-reference/flights/),
[Flights concept and plan limits](https://motherduck.com/docs/concepts/compute/flights),
[Flight sizes and pricing](https://motherduck.com/docs/about-motherduck/billing/flight-sizes),
[Authentication, config and secrets](https://motherduck.com/docs/key-tasks/flights/flights-authentication-config-and-secrets),
[Packages and runtime](https://motherduck.com/docs/key-tasks/flights/packages-and-runtime),
[Scheduling and runs](https://motherduck.com/docs/key-tasks/flights/scheduling-and-runs),
[Monitoring and debugging](https://motherduck.com/docs/key-tasks/flights/observability/monitoring-and-debugging),
[CREATE SECRET … TYPE FLIGHTS](https://motherduck.com/docs/sql-reference/motherduck-sql-reference/create-secret),
[MD_USER_INFO](https://motherduck.com/docs/sql-reference/motherduck-sql-reference/md-user-info).

## 1. Organisation and plan

| Fact | Value | Kind | Evidence |
|---|---|---|---|
| Plan (`md_user_info().org_type`) | `business` | measured (local and inside the Flight) | `run_facts.json` → `facts.motherduck` |
| Region | `us-east-1` (pricing tier 1) | measured | same |
| Default and maximum run time | Business: 8 h default (`max_runtime_sec` = 28800 on a new Flight); Lite: 1 h | documented; default measured | `deploy.json` → `versions[].max_runtime_sec` |
| Concurrent runs | unlimited on Business and Lite (unlimited); 1 on Lite (with limits). "Multiple concurrent runs of the same Flight are allowed" | documented (not measured here; run semantics are the next spike) | — |
| Sizes | F4 0.5 vCPU / 4 GB, F16 2 vCPU / 16 GB, F32 4 vCPU / 32 GB; Business offers all three | documented | — |
| Server DuckDB | `v1.5.6`; supported client range in us-east-1 1.4.0 – 1.5.6 (`duckdb-versions.json`) | measured / documented | `run_facts.json` |

## 2. Runtime (F16)

| Fact | Value | Kind | Evidence |
|---|---|---|---|
| OS | Debian GNU/Linux 12 (bookworm), glibc 2.36, kernel 6.12 (amzn2023), in a containerd container (overlay root) | measured; docs say "base Debian image" | `run_facts.json` → `os_release`, `platform`, `mounts` |
| CPU architecture | `x86_64` (AMD EPYC 7571) | measured | `facts.cpu` |
| CPU | **cgroup quota 2 CPUs** (`cpu.max` = `200000 100000`), but `os.cpu_count()` and the affinity mask report **8** host CPUs | measured | `facts.cpu` |
| Memory | **cgroup limit 16 GiB** (`memory.max` = 17179869184); `/proc/meminfo` shows the host's 64 GB; no swap | measured | `facts.memory` |
| Local disk | `/`, `/tmp`, `/app` (cwd, xfs) and `/root` all writable, ~537 GB total / ~525 GB free (shared host volume, no quota visible); `/dev/shm` 64 MB; 256 MB written + fsync to `/tmp` in 1.1 s; not durable across runs (each run is a fresh container) | measured; non-durability documented | `facts.disk`, `facts.tmp_write_256mb_s` |
| User | **root** (uid 0, gid 0), home `/root`, cwd `/app`, script `/app/main.py` run as `/app/./main.py` | measured | `facts.user` |
| Python | CPython **3.12.15** in a fresh uv virtual env `/app/.venv` | measured | `facts.python` |
| Preinstalled binaries | `git` 2.39.5, `curl`, `tar`, `gzip`, `xz`, `gcc`/`cc`, `make`, `bash`, `timeout`, `apt-get`, `uv` (`/root/.local/bin/uv`). **Absent**: `java`/`javac`, `wget`, `unzip`, `zstd`, `sudo`, `pip`, `ps`, `kill` (binary), `psql`, `ssh` | measured; docs say only that `git` is preinstalled and that `apt-get` works | `facts.binaries` |
| Resource limits | `nofile` 1048576, processes unlimited, stack 10 MB | measured | `facts.limits` |
| Time zone | UTC (`TZ` set but empty) | measured | `facts.time` |

## 3. Requirements and DuckDB

| Fact | Value | Kind | Evidence |
|---|---|---|---|
| How requirements install | `uv` creates `.venv` and installs `requirements_txt` **on every run**, before our code; the uv output is the first lines of the log. `duckdb==1.5.6` (20.6 MiB) resolved, downloaded and installed in ≈ 2.5 s | measured; docs: "installed once, before main() starts" (per run) | `run_facts.json` → `logs[0..8]` |
| Preinstalled Python packages | **none**: with empty requirements the venv has 0 packages and `import duckdb` fails | measured | `run_no_requirements.json` |
| `duckdb==1.5.6` (our pin) in a Flight | works: imports, connects to `md:` in ≈ 1.6 s, server `v1.5.6` | measured | `run_facts.json` → `facts.motherduck` |
| Empty `requirements_txt` | accepted by `MD_UPDATE_FLIGHT` | measured | `no_requirements_update.json` |
| Size limits | source ≤ 204,800 bytes, requirements ≤ 20,480 bytes (docs: 200 KB / 20 KB) | measured | `definition_limits.json` |

## 4. Network

| Fact | Value | Kind | Evidence |
|---|---|---|---|
| Anonymous `git clone --depth 1` of `https://github.com/Alex-Monahan/cdc_flight_v2` | works, 0.27–0.33 s | measured | `run_net1.json`, `run_net2.json` → `git_clone_public_repo` |
| Maven Central (`repo1.maven.org`) | reachable; Debezium Postgres connector jar (0.4 MB) in 0.05–0.13 s | measured | `facts.maven_central*` |
| JDK download hosts | `api.adoptium.net` (Temurin 21) and `corretto.aws` (Corretto 21): first 100 MB in 0.38–0.42 s, **≈ 235–265 MB/s** | measured | `facts.jdk_download_*` |
| PyPI | reachable, ≈ 5 MB/s for a 3 MB index page | measured | `facts.pypi` |
| Egress address | `32.195.40.107` in two consecutive runs; documented us-east-1 egress set is `54.145.91.19`, `32.195.40.107` (fixed, outbound only) | measured / documented | `facts.egress_ip` |
| Inbound connections | none ("A Flight does not accept inbound connections"); a `PORT` variable is nevertheless set | documented / measured | `facts.env_names` |

## 5. Environment, token and secrets

| Fact | Value | Kind | Evidence |
|---|---|---|---|
| Variable names inside a Flight | `HOME`, `HOSTNAME`, `LANG`, `LC_CTYPE`, `MOTHERDUCK_FLIGHTS_RUN`, `MOTHERDUCK_FLIGHT_ID`, `MOTHERDUCK_FLIGHT_RUN_ID`, `MOTHERDUCK_FLIGHT_RUN_NUMBER`, `MOTHERDUCK_HOST`, `MOTHERDUCK_TOKEN`, `PATH`, `PORT`, `PYTHONPATH`, `PYTHONUNBUFFERED`, `REQUESTS_CA_BUNDLE`, `SLING_BINARY`, `SSL_CERT_FILE`, `TEMP`, `TMP`, `TMPDIR`, `TZ`, `UV`, `UV_LINK_MODE`, `UV_RUN_RECURSION_DEPTH`, `VIRTUAL_ENV`, plus every `config` key and the secret variables | measured (names only) | `run_facts.json` → `facts.env_names` |
| Self-identification | **`MOTHERDUCK_FLIGHT_ID` and `MOTHERDUCK_FLIGHT_RUN_NUMBER` tell a run its own Flight and run number** (not documented); the leader guard can use them to cancel a *previous* run and never itself | measured | same |
| `config` | each key → env var of the same name; per-run override via `MD_RUN_FLIGHT(config := …)` of existing keys works | documented, measured | `run_net1.json` → `md_run_flight_returned.config` |
| Secrets | `CREATE SECRET <name> IN MOTHERDUCK (TYPE FLIGHTS, PARAMS MAP {...})`, attached with `flight_secret_names`. Each key arrives **twice**: `<secret_name>_<KEY>` (case preserved) and bare `<KEY>`; both carried the exact value (HMAC compared) | documented, measured | `identity_and_secret_check.json` |
| Token the Flight receives | `MOTHERDUCK_TOKEN` (upper case only; `motherduck_token` absent), the owner's default `MotherDuck Flights` access token; it is **the same user as the creator** (HMAC of the user name compared, name not recorded) | documented, measured | `identity_and_secret_check.json`, `deploy.json` → `access_token_name` |
| What that token sees | 74 databases: **2 named `cdc_v2_*`, 72 others** — the same counts as the build session. PLAN §2 A 4 "the run-time token sees only this project's databases" **does not hold** (expected, PLAN §9) | measured (counts only) | `identity_and_secret_check.json` |
| Flight SQL functions from inside a Flight | work with the injected token (`MD_LIST_FLIGHTS`, `MD_LIST_FLIGHT_RUNS` on its own Flight; it saw itself as `RUNNING`) | measured | `run_facts.json` → `flight_functions_inside`, `own_runs_seen_inside` |

## 6. Timing and run states

Medians over the runs of this spike (`timing` in each `run_*.json`):

| Interval | With `duckdb==1.5.6` | Without requirements | Kind |
|---|---|---|---|
| `MD_RUN_FLIGHT` → `started_at` | 0.26 – 1.4 s | 1.3 s | measured |
| `started_at` → first line of our code | **3.0 – 3.3 s** (≈ 0.6 s container + ≈ 2.5 s uv install) | 0.94 s | measured |
| `MD_RUN_FLIGHT` → first line of our code | **3.3 – 4.5 s** | 2.2 s | measured |
| Last line → `ended_at` | ≈ 0.45 s | — | measured |
| Shortest whole run (`started_at` → `ended_at`) of a script that exits at once | **3.7 s** | 2.7 s | measured (`run_exit3.json`, `run_no_requirements.json`) |
| Log line printed → readable through `MD_GET_FLIGHT_LOGS` | **< 1 s** (median 0.23 s, max 0.89 s, polling every ≈ 1 s; `reported_at` within 2 ms of the print) | | measured (`run_ticks_log_visibility.json`) |

- **States** (measured, `observed_states` in every run file): `PENDING` → `RUNNING` → `SUCCEEDED` | `FAILED` |
  `CANCELLED`. The DuckDB 1.5.6 client returns them **without** the `RUN_STATUS_` prefix (documented: "newer
  clients"); Flight status is `ACTIVE` (docs show `JOB_STATUS_ACTIVE`). `cdc_v2.flightctl.normalize_status` accepts both.
- **Exit code** (measured): `sys.exit(3)` → `FAILED`, `exit_code` 3 (`run_exit3.json`). An uncaught exception →
  `FAILED`, `exit_code` 1, traceback in the log (`run_exception.json`). Success → `SUCCEEDED`, `exit_code` 0.
- **Cancel from this SQL connection** (measured, `run_cancel.json`): `MD_CANCEL_FLIGHT_RUN` returned
  `canceled_count = 1` in ≈ 1 s; the process stopped within 0.4 s of the call (no further log lines); the run shows
  `CANCELLED` with `cancelled_at` set and **`ended_at` and `exit_code` NULL**. Cancelling a terminal run fails with
  `Constraint Error: Run N is already in terminal state "cancelled" and cannot be cancelled`.
- `scheduled_at` is filled for on-demand runs too (≈ the request time).

## 7. Cost

Documented ([pricing](https://motherduck.com/docs/about-motherduck/billing/flight-sizes)): "billed for the time each run
takes, at the hourly rate for the Flight's size"; F16 in tier 1 (us-east-1) = **$0.60 per hour**. Billing granularity
and whether install time or a minimum charge applies are **not documented**; the figures below assume per-second
billing of `started_at` → `ended_at`.

| Case | Cost |
|---|---|
| 10-second F16 run | 10 / 3600 × $0.60 = **$0.00167** |
| 1,440 such runs a day (every minute) | 4 h/day = **$2.40 per day** (≈ $72 per 30 days) |
| 1,440 runs of the measured minimum (3.7 s, our pin installed, exit at once) | ≈ $0.89 per day |
| 1,440 runs of a probe that also connects to MotherDuck (≈ 6.7 s measured) | ≈ $1.61 per day |

## 8. Schedule and definition limits

| Fact | Value | Kind | Evidence |
|---|---|---|---|
| Source limit | **204,800 bytes** (200 KiB): 204,800 accepted, 204,801 rejected with `source_code exceeds the 204800-byte limit` | measured; docs "200 KB" | `definition_limits.json` |
| Requirements limit | **20,480 bytes** (20 KiB): 20,480 accepted, 20,481 rejected | measured; docs "20 KB" | `definition_limits.json` |
| Cron syntax | standard 5 fields, UTC, numbers / ranges / steps / lists only. Rejected by the server: 6 fields (no seconds), `@hourly`/`@yearly`, names (`MON`, `JAN`), out-of-range values, expressions that never fire (`0 0 31 2 *`) | documented (5 fields, UTC); rejections measured | `cron_server.json` |
| Granularity | one minute (`* * * * *` accepted and fired) | measured | `schedule.json` |
| A scheduled run | `* * * * *` set at 02:12:45; a run record (`is_scheduled` true) **appeared 2.7 s later, before its time**, with `scheduled_at` 02:13:00; it started at 02:13:01.3 (1.3 s after the minute) and our first line came 4.8 s after `started_at` (≈ 6.1 s after the minute boundary); `started_by_*` NULL | measured (one observation) | `schedule.json` → `scheduled_run` |
| Clearing a schedule (`schedule_cron := ''`) | **keeps the cron text and sets `schedule_status` = `DISABLED`**; no scheduled run followed in the next 70 s; the run already in progress was not cancelled. Clearing again succeeds; clearing a Flight that never had a schedule fails with `Cannot clear schedule: Flight has no schedule` | measured; docs say the Flight "reverts to on-demand only" | `schedule.json`, `cron_server.json` |

`schedule.json` also lists `*/30 * * * * *` and `@hourly` as refused; those two refusals came from flightctl's own
client-side check (it refuses anything that is not 5 fields), so the server's verdict is the one in `cron_server.json`.

## 9. Where documentation and measurement differ, and surprises for SPEC/PLAN

1. **Clearing a schedule does not null it** (§8). `schedule_cron` keeps the expression and `schedule_status` becomes
   `DISABLED`. Code that decides "is this Flight scheduled?" must read `schedule_status`
   (`cdc_v2.flightctl.schedule_active`), not `schedule_cron`.
2. **Status names without prefixes.** Docs show `RUN_STATUS_*` and `JOB_STATUS_ACTIVE`; the DuckDB 1.5.6 client gets
   `PENDING`, `RUNNING`, `SUCCEEDED`, `FAILED`, `CANCELLED`, `ACTIVE`, `DISABLED`. Docs mention this for newer clients
   on `MD_RUN_FLIGHT` only.
3. **A cancelled run has `ended_at` NULL and `exit_code` NULL** (only `cancelled_at`). Not documented.
4. **Size limits are binary KiB** (204,800 and 20,480 bytes), slightly above the documented "200 KB / 20 KB".
5. **More is preinstalled than SPEC §2 says.** SPEC: "Only `git` is preinstalled: no JVM." Measured: no JVM indeed,
   but also `curl`, `tar`, `gzip`, `xz`, `gcc`, `make`, `apt-get`, `uv`, and the run is **root**. A JDK download is
   ≈ 0.4 s per 100 MB, so installing a JDK per run is cheap (PLAN §2 A 1 measures it fully).
6. **No Python packages are preinstalled, not even `duckdb`.** The pipeline and the leader guard must list
   `duckdb==1.5.6` in `requirements_txt`; each run re-downloads it (≈ 2.5 s, uv, no cache across runs). This is the
   largest part of the ≈ 3.3 s start-up before our first line.
7. **CPU count lies to Python.** The F16 cgroup quota is 2 CPUs but `os.cpu_count()` returns 8 and `/proc/meminfo`
   shows 64 GB. Thread and process pools, DuckDB's local `threads` and the JVM must be sized from the cgroup
   (`/sys/fs/cgroup/cpu.max`, `memory.max`), not from `os.cpu_count()` or total memory.
8. **The local disk is large (~525 GB free) and writable**, shared with the host and with no visible quota; it is not
   durable across runs. SPEC 5.4's spill quota must be self-imposed.
9. **A run knows its own identity**: `MOTHERDUCK_FLIGHT_ID`, `MOTHERDUCK_FLIGHT_RUN_ID`,
   `MOTHERDUCK_FLIGHT_RUN_NUMBER` (undocumented). Useful for SPEC 4.2: the guard can find and cancel the other active
   runs of its own Flight with `MD_LIST_FLIGHT_RUNS` / `MD_CANCEL_FLIGHT_RUN`, which are callable from inside a Flight
   (listing measured here; cancelling from inside is the run-semantics spike).
10. **A scheduled run is listed as soon as it is planned**, before its minute arrives (one observation, §8). A guard
    that cancels "every other active run" could cancel the *next* scheduled run while it is still `PENDING`; it must
    look at `scheduled_at` / `started_at`. The run-semantics spike should confirm this.
11. **The run-time token is the owner's personal default token** (`MotherDuck Flights`), the same user as the build
    session, seeing 72 databases outside `cdc_v2_*` (PLAN §9). SPEC §2 "a dedicated service account that can see only
    this project's databases" does not hold in this session.
12. **The egress address was stable** (`32.195.40.107` in every network run) and is one of the two documented
    us-east-1 addresses; an allowlist needs both.
13. `schedule_cron` validation is strict (no names, no `@` macros). It concerns only the Flight's own schedule; the
    SPEC 3.5 per-table crons run inside the long-lived Flight with our own parser.

## 10. PLAN §2 A 4 checklist

| Operation from a SQL connection, secrets attached | Result | Evidence |
|---|---|---|
| create (`MD_CREATE_FLIGHT`, F16, config, secret) | works | `deploy.json` |
| update (source, requirements, config, size, secrets) | works; every content change bumps the version | `deploy.json`, `no_requirements_update.json` |
| schedule / unschedule | works; a `* * * * *` schedule fired once, then was disabled (§8) | `schedule.json` |
| run now (`MD_RUN_FLIGHT`, with per-run config) | works | `run_*.json` |
| list runs / get run | works | every `run_*.json` (`final`) |
| cancel (`MD_CANCEL_FLIGHT_RUN`) | works | `run_cancel.json` |
| read logs (`MD_GET_FLIGHT_LOGS`) | works, also while `RUNNING` | `run_ticks_log_visibility.json` |
| delete (`MD_DELETE_FLIGHT`) | works | `cleanup.json`, `definition_limits.json` |
| "run-time token sees only this project's databases" | **does not hold**: 72 other databases visible (PLAN §9) | `identity_and_secret_check.json` |

## 11. Tool

`tools/flightctl` (logic in `src/cdc_v2/flightctl.py`). All results come back as JSON rendered by the server
(`to_json`), so no `pytz`/`pyarrow` is needed; owner and starter user names are redacted; only Flights and secrets
named `cdc_v2*` are listed or touched.

```
tools/with-env uv run python tools/flightctl deploy cdc_v2_x --source flights/x/main.py --requirements flights/x/requirements.txt --size F16 [--secret S] [--config K=V]
tools/with-env uv run python tools/flightctl run cdc_v2_x --wait --logs          # exit 3 if the run did not succeed
tools/with-env uv run python tools/flightctl --json runs cdc_v2_x | cancel cdc_v2_x [N] | logs cdc_v2_x [N] | unschedule | delete
```

Secrets: `flightctl secret-set cdc_v2_s --from-env KEY[=LOCAL_VAR]` (values read from the local environment,
never printed). From inside a Flight (leader guard): `FlightCtl().cancel_by_id(os.environ["MOTHERDUCK_FLIGHT_ID"], n)`
and `FlightCtl().active_runs(flight_id)`; the module needs only `duckdb`.
