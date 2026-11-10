# Build status

Last updated: 2026-11-10, by the orchestrator. Resume from `SPEC.md`, `PLAN.md` (read §9), this file and `git log`.

## Where the build stands

- **Stage 1 (local). P1 Environment: gate passed on 2026-11-09** (one item cannot be shown from this session, see the
  table). **P2 Platform spike: in progress**, with the hazard research of P5 running alongside.
- Stage 2 (PlanetScale) cannot start: of its names only `PLANETSCALE_ORG` and `PLANETSCALE_DATABASE` are in the env
  file (checked again at the P1 gate). The service token and the connection values are owed by the owner.

## P1 gate — passed 2026-11-09

| Item | State | Evidence |
|---|---|---|
| Session start checks | 1 and 4 pass; 2 resolved by the owner; **3 not enforced in this session** (owner informed) | `evidence/p1/session_start.md` |
| Newest stable Postgres installed with Homebrew | done — `postgresql@18` 18.6_1; the service on port 5432 untouched | `evidence/p1/postgres_install.md` |
| `tools/localpg` brings a primary and a replica up and down | done — 22 self-tests pass (`pytest -m localpg tests/localpg`, 49 s); reset 2.2 s, up 0.7 s, down 0.4 s; no process left after `down`; port 5432 still served by the owner's unchanged process | `evidence/p1/localpg.md`, `docs/localpg.md` |
| Connectivity to MotherDuck | pass, with the session's `motherduck_token` (PLAN §9), DuckDB client 1.5.6 | `evidence/p1/motherduck_connectivity.txt` |
| A commit that carries a planted fake credential is rejected | pass — content and message | `evidence/p1/secret_gate.txt` |
| Canary denials counted in the audit receipt | **cannot be shown from this session** (guard not active here). Receipt at the gate: status `ok`, findings 0, last run 20 minutes before the gate | `evidence/p1/session_start.md` |

## P2 gate — open

| Spike (PLAN §2 A) | State | Report |
|---|---|---|
| 4 Flight SQL functions, platform facts | done | `docs/spike/flight_platform.md` |
| 3 MotherDuck semantics | laptop part done and merged; the network-bound measurements are being repeated inside an F16 Flight | `docs/spike/md_semantics.md` |
| 1 JVM and stock Debezium on a Flight | running | `docs/spike/jvm_on_flight.md` |
| 2 Run semantics | done | `docs/spike/run_semantics.md` |
| 5 Local throughput (first indication) | part 1 (read rate from the replica) running; part 2 (stock Debezium streaming rate) waits for the JVM program of spike 1 | `docs/spike/local_throughput.md` |
| `docs/spike_report.md` and the architecture decision record | after the spikes | — |

## Decisions

| Date | Decision | By |
|---|---|---|
| 2026-11-09 | The build runs in the session that failed start checks 2 and 3; it uses the session's lowercase `motherduck_token` and a salt given in chat. Consequences are in PLAN §9 | owner |
| 2026-11-09 | Values given in chat are kept in `~/.config/cdc_flight_v2/env.local`; local commands run through `tools/with-env` | orchestrator |
| 2026-11-09 | `duckdb==1.5.6` pinned: MotherDuck refuses a 2.0.0 client | orchestrator |
| 2026-11-09 | Python project managed with `uv` (`pyproject.toml`, `uv.lock`), package `cdc_v2` under `src/` | orchestrator |
| 2026-11-09 | Fetch and push of this clone use the repository-only credential through the clone's git configuration | orchestrator |
| 2026-11-09 | The default `pytest` run is hermetic (tier T0). Suites that start local Postgres clusters or real Flights are opt-in markers (`localpg`, `flight_integration`), so parallel workers do not collide on ports | orchestrator |
| 2026-11-09 | SPEC R23 "database user name" means names that come from the env file (provider or stage 2 roles; the scanner's exact-value layer enforces it). The role names that `tools/localpg` creates for local fixtures (`cdc_v2_boot`, `cdc_v2_admin`, `cdc_v2_runtime`) are code and appear in the repository | orchestrator |
| 2026-11-09 | The JVM spike uses a throw-away Postgres installed inside the Flight container, as a spike fixture only, to prove that stock Debezium streams on F16. The product never installs Postgres; numbers from it are indications, not performance results | orchestrator |

## Questions for the owner / owed by the owner

- **Boundary guard**: not enforcing in this session, and the hourly audit does not see it. The build follows PLAN §4
  by instruction. If the owner wants the guarded session instead, start it through the launcher; it resumes from
  this file.
- **MotherDuck account**: Flights and `cdc_v2_*` databases are created under the owner's own account (PLAN §9), not
  under the service account. Say so if the service account should be used instead.
- `SLACK_WEBHOOK_URL` and the alert channel: not provided; alerts are tested against a local stub until then.
- MotherDuck plan and region: measured in P2 — Business plan, us-east-1, maximum run time 8 h
  (`docs/spike/flight_platform.md`). No longer open.
- Stage 2 values of PLAN §8 (PlanetScale service token, branch, hosts, admin role, TLS mode).
- **Billing of short runs.** MotherDuck documents no billing granularity or minimum charge per run. The cost of
  the every-minute schedule above assumes per-second billing; with a minimum of one minute per run it would be
  $14.40 a day. The owner accepted the cost of the short runs in advance (R18); the number needs an invoice or a
  word from MotherDuck to be confirmed.
- **Replacement time of a dead leader (SPEC 4.2: "within 90 s plus start-up").** Measured worst case: 30 s until
  the heartbeat is stale, up to 60 s to the next scheduled minute, then 4–8 s until the newcomer's guard has decided
  — about 98 s. The criterion holds if "start-up" includes the newcomer's own guard decision; that reading is used
  unless the owner says otherwise.
- **Latency and very large transactions (research finding F-3).** Stock Debezium speaks only version 1 of the
  `pgoutput` protocol, which sends a transaction after its commit, in one piece. Everything committed after a
  10M-row transaction therefore waits until that transaction is applied (at the target rate: 100 s or more). SPEC 5.2
  measures latency when idle and under 2,000 small transactions per second, so the criterion as written is not
  affected; but a very large transaction inside a latency window will exceed the 60 s maximum, and R19 does not
  exclude it. Working rule until the owner rules: such windows are logged and reported separately, and they are
  **not** removed from the 5.2 statistic.
- **Tables without a key (F-7).** A table without a usable replica identity cannot be added to a publication that
  publishes updates and deletes without breaking the application's own `UPDATE` and `DELETE`. Working rule: before
  such a table joins the publication the pipeline sets `REPLICA IDENTITY FULL` on it (a configuration switch turns
  this off; the table then replicates by scheduled full refresh and stays out of the publication). This is one more
  action on the primary than SPEC R17 lists, and it increases the WAL volume of those tables. Say so if the default
  should be the other way round.
- **Generated columns, unlogged tables, column names that differ only in case (F-6, F-8, F-9).** They cannot be
  streamed with stock Debezium. Proposed handling (decided in the P4 schema wave, reported at its gate): stored
  generated columns are published (`publish_generated_columns`), tables that cannot be streamed replicate by
  scheduled full refresh, with a warning in `_cdc`; nothing is refused silently.

## To verify in stage 2

(Collected as stage 1 proceeds.)

- Everything marked *(Flight)* in SPEC §3 with a real source: 3.1, 5.1, 5.2, 5.3, the 24 h soak.
- Replica decoding on PlanetScale (PLAN §2 B 6), failover behaviour of both modes (4.1), privileges of the runtime
  role on a managed provider, TLS `verify-full`, client address restrictions.
- The nine points of `docs/research/replica_decoding.md` §10 ("What stage 2 must test on PlanetScale"), first of
  all: whether one replica can be addressed deterministically and accepts replication-protocol connections, and
  whether WAL retained by a slot can trigger storage autoscaling (**a cost change — baseline first, PLAN §5**).
- PlanetScale's documented slot policy (slots without `failover = true` are flagged and planned failovers held back
  for 3 days): does a slot on a replica trigger it?

## Background jobs in flight

Each in its own worktree and branch (Opus, high effort); the orchestrator merges.

| Job | Phase | Started | Delivers |
|---|---|---|---|
| Spike worker: MotherDuck semantics, second part — the same measurements inside an F16 Flight (`cdc_v2_spike_md*`) | P2 (§2 A 3) | 2026-11-09 | `flights/spike_md/`, additions to `docs/spike/md_semantics.md`, `evidence/p2/md_semantics/flight_*` |
| Spike worker: JVM and stock Debezium on a Flight | P2 (§2 A 1) | 2026-11-09 | `flights/spike_jvm/`, `docs/spike/jvm_on_flight.md`, `evidence/p2/jvm_on_flight/` |
| Spike worker: local read throughput (private cluster, home `pg-spike-tp`, ports 55500–55509; MotherDuck database `cdc_v2_spike_tp`) | P2 (§2 A 5, part 1) | 2026-11-09 | `spikes/local_throughput/`, `docs/spike/local_throughput.md`, `evidence/p2/local_throughput/` |

Not started yet: part 2 of the local throughput spike (stock Debezium streaming rate for small transactions and for
one 10M-row transaction). It reuses the JVM program of the JVM spike and the private cluster of part 1.

Finished and merged:

- 2026-11-10 — Run semantics (§2 A 2): `flights/spike_runs/`, `tools/spike/run_semantics_*.py`,
  `docs/spike/run_semantics.md`, `evidence/p2/run_semantics/`. All 12 spike Flights and both spike databases deleted.
  What it settles: every scheduled minute starts a run whatever is active (no skip, no queue); a run can cancel
  another run from inside; nothing is retried; every platform stop kills without a signal; a cancelled run can still
  commit for about 2.5 s (fencing is mandatory); an out-of-memory kill of any child ends the whole run; the bootstrap
  hard-exit ends a frozen run 65 s after the freeze; a dead leader is replaced at most about 98 s after its death;
  the every-minute schedule costs about $1.46 a day with a guard that installs `duckdb`, $0.73 with a guard that
  reads the leader record through MotherDuck's Postgres endpoint (per-second billing assumed; not documented).
- 2026-11-09 — MotherDuck semantics, laptop part (§2 A 3): `spikes/md_semantics/`, `docs/spike/md_semantics.md`,
  `evidence/p2/md_semantics/`. Design consequences (binding for P3 unless the Flight run changes them): heartbeat
  time comes from `uuid_extract_timestamp(uuidv7())` because `now()` is computed by the client; destination tables
  carry no keys or indexes and apply is `DELETE … USING` + `INSERT`; the leader record stores the leader's
  connection id and a takeover interrupts that connection (a dead client's open transaction otherwise holds its rows
  for about 30 s); the fence is the last statement before `COMMIT`; the leader row is only ever updated; parallel
  appliers have separate state rows; many source transactions share one destination commit; `TimeZone` is `UTC`.
- 2026-11-09 — Hazard research, first pass (P5, started early): `docs/research/debezium.md` (pin, delivery contract,
  recommended configuration), `docs/research/replica_decoding.md` (standby decoding in Postgres 16–18, PlanetScale's
  public documentation, what stage 2 must test first), `docs/wal_hazards.md` (H-001 to H-050, every prevention still
  "proposed", findings F-1 to F-11). Items marked `[test]` in these files are claims that still need an experiment.
- 2026-11-09 — `tools/localpg` (P1): the tool, `tests/localpg/`, `docs/localpg.md`, `evidence/p1/localpg.md`.
- 2026-11-09 — Flight tooling and platform facts (§2 A 4): `tools/flightctl`, `src/cdc_v2/flightctl.py`,
  `docs/spike/flight_platform.md`, `evidence/p2/flight_platform/`. All spike Flights and the spike secret deleted.

## Changes to the secret scanner, its tests or `.secretscan-allow` (for the owner's review)

- 2026-11-09, `.secretscan-allow` created with one rule: `^cdc_v2_spike_secret(_CDC_V2_PROBE_SECRET)?$`. It is the
  *name* of the spike's Flight secret and of the environment variable it produces (no value); the randomness rule
  flagged the name. The scanner and its tests are unchanged.

Also for the owner: commit `aa6a435` contains a pytest evidence file whose header shows a local path including the
operating-system user name of this machine. It is not a credential, hostname, database user name or connection
string; it was redacted in the next commit and stays in history.

Note for the owner: the exact-value layer of the scanner reads the env file only. The token this session uses is not
in that file, so it is covered by the pattern layer (JWT format), by `gitleaks` and by the harness redaction, not by
the exact-value layer.

## Next steps

1. P2: merge the running spikes as they report; run the local throughput spike; write `docs/spike_report.md` and the
   architecture decision record; send the report to the owner and continue.
2. P3 core: oracle, crash-point harness and fuzzers first. The harness must give parallel workers isolated sources:
   a private database on the shared development cluster for ordinary tests, a private cluster (own home and port
   block, through `tools/localpg`) for destructive drills.
3. Carry into P5 (from the `tools/localpg` work): slot creation on a standby waits for a running-transactions record
   and `pg_log_standby_snapshot()` needs a privileged role; a failover slot is synchronised to a standby only after it
   has advanced past the standby's catalog horizon; raising a standby-matched setting on the primary pauses replay.
