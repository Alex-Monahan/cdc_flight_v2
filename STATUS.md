# Build status

Last updated: 2026-11-09, by the orchestrator. Resume from `SPEC.md`, `PLAN.md` (read §9), this file and `git log`.

## Where the build stands

- **Stage 1 (local), phase P1 Environment — in progress.**
- Stage 2 (PlanetScale) cannot start: of its names only `PLANETSCALE_ORG` and `PLANETSCALE_DATABASE` are in the env
  file. The service token and the connection values are owed by the owner.

## P1 gate

| Item | State | Evidence |
|---|---|---|
| Session start checks | 1 and 4 pass; 2 resolved by the owner; **3 not enforced in this session** (owner informed) | `evidence/p1/session_start.md` |
| Newest stable Postgres installed with Homebrew | done — `postgresql@18` 18.6_1; the service on port 5432 untouched | `evidence/p1/postgres_install.md` |
| `tools/localpg` brings a primary and a replica up and down | **in progress** (implementation worker) | — |
| Connectivity to MotherDuck | pass, with the session's `motherduck_token` (PLAN §9), DuckDB client 1.5.6 | `evidence/p1/motherduck_connectivity.txt` |
| A commit that carries a planted fake credential is rejected | pass — content and message | `evidence/p1/secret_gate.txt` |
| Canary denials counted in the audit receipt | **cannot be shown from this session** (guard not active here) | `evidence/p1/session_start.md` |

## Decisions

| Date | Decision | By |
|---|---|---|
| 2026-11-09 | The build runs in the session that failed start checks 2 and 3; it uses the session's lowercase `motherduck_token` and a salt given in chat. Consequences are in PLAN §9 | owner |
| 2026-11-09 | Values given in chat are kept in `~/.config/cdc_flight_v2/env.local`; local commands run through `tools/with-env` | orchestrator |
| 2026-11-09 | `duckdb==1.5.6` pinned: MotherDuck refuses a 2.0.0 client | orchestrator |
| 2026-11-09 | Python project managed with `uv` (`pyproject.toml`, `uv.lock`), package `cdc_v2` under `src/` | orchestrator |
| 2026-11-09 | Fetch and push of this clone use the repository-only credential through the clone's git configuration | orchestrator |

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

## To verify in stage 2

(Collected as stage 1 proceeds.)

- Everything marked *(Flight)* in SPEC §3 with a real source: 3.1, 5.1, 5.2, 5.3, the 24 h soak.
- Replica decoding on PlanetScale (PLAN §2 B 6), failover behaviour of both modes (4.1), privileges of the runtime
  role on a managed provider, TLS `verify-full`, client address restrictions.

## Background jobs in flight

Each in its own worktree and branch (Opus, high effort); the orchestrator merges.

| Job | Phase | Started | Delivers |
|---|---|---|---|
| Implementation worker: `tools/localpg` | P1 | 2026-11-09 | the tool, `tests/localpg/`, `docs/localpg.md`, `evidence/p1/localpg.md` |
| Spike worker: MotherDuck semantics | P2 (§2 A 3) | 2026-11-09 | `spikes/md_semantics/`, `docs/spike/md_semantics.md`, `evidence/p2/md_semantics/` |
| Hazard researcher: Debezium facts, replica decoding, hazard catalog | P5 research, started early | 2026-11-09 | `docs/research/debezium.md`, `docs/research/replica_decoding.md`, `docs/wal_hazards.md` |
| Spike worker: JVM and stock Debezium on a Flight | P2 (§2 A 1) | 2026-11-10 | `flights/spike_jvm/`, `docs/spike/jvm_on_flight.md`, `evidence/p2/jvm_on_flight/` |
| Spike worker: run semantics | P2 (§2 A 2) | 2026-11-10 | `flights/spike_runs/`, `docs/spike/run_semantics.md`, `evidence/p2/run_semantics/` |

Waiting for `tools/localpg`: the local throughput spike (§2 A 5).

Finished and merged:

- 2026-11-10 — Flight tooling and platform facts (§2 A 4): `tools/flightctl`, `src/cdc_v2/flightctl.py`,
  `docs/spike/flight_platform.md`, `evidence/p2/flight_platform/`. All spike Flights and the spike secret deleted.

## Changes to the secret scanner, its tests or `.secretscan-allow` (for the owner's review)

- 2026-11-10, `.secretscan-allow` created with one rule: `^cdc_v2_spike_secret(_CDC_V2_PROBE_SECRET)?$`. It is the
  *name* of the spike's Flight secret and of the environment variable it produces (no value); the randomness rule
  flagged the name. The scanner and its tests are unchanged.

Also for the owner: commit `aa6a435` contains a pytest evidence file whose header shows a local path including the
operating-system user name of this machine. It is not a credential, hostname, database user name or connection
string; it was redacted in the next commit and stays in history.

Note for the owner: the exact-value layer of the scanner reads the env file only. The token this session uses is not
in that file, so it is covered by the pattern layer (JWT format), by `gitleaks` and by the harness redaction, not by
the exact-value layer.

## Next steps

1. Finish P1: merge `tools/localpg` with its tests and evidence; close the gate.
2. P2 platform spike on a real F16 Flight (PLAN §2 A), with the hazard research (P5) starting alongside.
3. P3 core: oracle, crash-point harness and fuzzers first.
