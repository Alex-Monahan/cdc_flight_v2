# Build plan (DRAFT for refinement)

Companion to `SPEC.md`. Nothing here starts until the owner approves both documents.

## 1. Phases and gates

| Phase | Outcome | Gate to pass |
|---|---|---|
| P0 Refinement | Open decisions D1–D13 settled | Owner sign-off on SPEC and PLAN |
| P1 Access | Secrets in place, source parameters verified, project boundary enforced | Connectivity checks green; boundary canaries all denied |
| P2 Flight spike | `docs/spike_report.md` with measured numbers and an architecture decision record | Owner checkpoint on D1, D3, D9 |
| P3 Core | Oracle + crash-point harness first, then commit/ack protocol, state machines, snapshot↔stream handoff, slot verification, lease/fencing | 1.1–1.9, 4.2 tests green locally and against PlanetScale |
| P4 Feature waves | Schema and types · backfill modes · failure handling · observability · Postgres features · CDC features | Each wave: tests + evidence, independent review |
| P5 Hazards | `docs/wal_hazards.md`, fuzzer, prevention tests (research starts in P2, runs alongside P3–P4) | Every hazard class has a prevention and a test |
| P6 Performance | 3.1, 5.1, 5.2 (24 h), 5.3, 5.4 on real Flights | Numbers meet SPEC or are escalated with data |
| P7 Grading | Fresh-context graders score all 43 items; reviewer scores 2.6 and 3.3 | 214/215, or each shortfall escalated |
| P8 Handover | Runbook, config reference, deployed Flight | Owner acceptance |

## 2. What the spike must answer (P2)

All on a real Flight against the PlanetScale instance, on F4 and F16.

1. **JVM on a Flight**: install a JDK at start-up from the bootstrap, clone this repo with a read-only credential,
   time to first event, memory left for the pipeline.
2. **Run semantics**: can two runs of one Flight overlap; are failed runs retried; what happens at the maximum run
   time; how fast can a scheduled run take over (drives 4.2, 5.2 and the 8-hour handover).
3. **Per-stage throughput at `lineitem` width**: Postgres read rate (parallel `COPY`, DuckDB's Postgres reader),
   MotherDuck bulk-load rate, stock Debezium streaming rate for small transactions and for one 10M-row transaction.
4. **MotherDuck semantics**: drop + rename inside a transaction under a concurrent reader; `ALTER … TYPE UNION(…)`;
   size limits of one transaction; behaviour of concurrent writers.
5. **PlanetScale specifics**: failover-enabled slot behaviour across a real failover, whether logical decoding from a
   replica is permitted, privileges for publication maintenance and `pg_logical_emit_message`, sender timeouts.

### Architecture candidates (chosen by spike data, recorded as an ADR)

- **A** — stock Debezium Server as a supervised child process, delivering to a local sink consumed by the applier.
- **B** — stock Debezium embedded engine inside a small JVM program of ours, run as a supervised child process, handing
  batches to the applier with explicit acknowledgement after the destination commit.
- **C** — embedded engine loaded into the Python process through a JVM bridge.

Expected choice: **B** (a child process can be health-checked, memory-capped, killed and restarted, which 4.5 and 4.7
need; the engine's batch API gives an explicit acknowledge step for exactly-once). Backfill is independent of the
choice: direct parallel reads from Postgres into MotherDuck staging tables, then swap (SPEC R3, 3.1, 3.2).

## 3. Agents

No Sonnet anywhere. Every agent call names its model explicitly.

| Role | Model / effort | Notes |
|---|---|---|
| Orchestrator | Fable, xhigh in P2–P3, high afterwards | Owns SPEC/PLAN, gates, owner communication, Flight deployments |
| Coordinator (subagent) | Opus 5.5, xhigh | One per phase or wave. Splits work, launches workers, merges, runs suites, reports to the orchestrator |
| Core-correctness worker | Fable, xhigh | Commit/ack protocol, handoff, slot verification, fencing |
| Implementation workers | Opus 5.5, high | One worktree each, at most 4 writing at once |
| Hazard researcher | Opus 5.5, high | Protocol documentation, Debezium documentation and issue tracker |
| Graders | Opus 5.5, xhigh, fresh context | Never grade code they wrote |
| Subjective reviewer (2.6, 3.3) | Opus 5.5, xhigh, fresh context | Scores against the SPEC checklists |
| Log triage, test watching, docs | Opus 5.5, medium | |

Working rules: tests before features; one worktree per writer; small commits pushed often; long suites run as
background jobs; a claim without an evidence file is not accepted; performance claims only from Flight runs.

## 4. Project boundary

Agents work **only** with this repository, the test environments in §5 and public documentation of Postgres,
Debezium, DuckDB, MotherDuck and PlanetScale. Everything else on this machine, every other repository of the owner,
and every MotherDuck database not created by this project is out of bounds. A tool-call guard enforces this, denies
by default when unsure, and logs every denial; the log is audited at each gate. A denied call is not to be retried by
another route — report it to the coordinator instead.

MotherDuck databases created by this project are named `cdc_v2_*`; tests create and drop only such databases.

## 5. Test tiers

| Tier | Source | Destination | Used for |
|---|---|---|---|
| T0 | none | in-memory | unit tests, state-machine transition tests |
| T1 | local Postgres 18 from the installed binaries, private data directories, ports ≥ 55432 (never 5432, never Docker); primary + standby pair | MotherDuck `cdc_v2_*` | crash points, network faults through a TCP proxy, failover drills, fuzzer |
| T2 | PlanetScale, process on this machine | MotherDuck | compatibility, replica and failover-slot behaviour |
| T3 | PlanetScale, **real Flight** | MotherDuck | all performance items, 24 h latency, run handover, real failover |

## 6. Needed from the owner

Values are never pasted into chat or committed. Local runs read a mode-600 file outside the repo
(`~/.config/cdc_flight_v2/env`); Flights read MotherDuck secrets of type `flights` with the same names.

| Name | What |
|---|---|
| `PG_HOST`, `PG_PORT`, `PG_DATABASE` | PlanetScale primary, direct connection (not a pooler) |
| `PG_ADMIN_USER`, `PG_ADMIN_PASSWORD` | Role that can create tables, roles and publications (test fixtures) |
| `PG_USER`, `PG_PASSWORD` | Runtime role with `REPLICATION` (can be created from the admin role) |
| `PG_REPLICA_HOST`, `PG_REPLICA_PORT` | A replica endpoint, if the cluster has one |
| `PG_SSLMODE` | `verify-full` expected |
| `PLANETSCALE_SERVICE_TOKEN_ID`, `PLANETSCALE_SERVICE_TOKEN`, `PLANETSCALE_ORG`, `PLANETSCALE_DATABASE`, `PLANETSCALE_BRANCH` | Only if failovers and parameter changes should be scripted |
| `MOTHERDUCK_TOKEN` | Ideally a token that can see only this project's databases |
| `SLACK_WEBHOOK_URL` | Incoming webhook for the alert channel |
| `CDC_PII_SALT` | Random 32+ bytes |
| `CDC_REPO_DEPLOY_KEY` | Read-only credential for cloning this repo inside a Flight (can be generated here) |

Facts needed (no secrets): Postgres major version; cluster size and free storage (the benchmark table is 60M rows);
number of replicas; whether cluster parameters may be changed (`wal_level`, `max_replication_slots`,
`max_wal_senders`, `max_slot_wal_keep_size`, `sync_replication_slots`, `hot_standby_feedback`); whether the instance
is disposable (heavy writes, slot drops, backend kills, forced failovers, WAL growth) and any spend limit; whether IP
restrictions are on (the Flight egress addresses and this machine must be allowed); the MotherDuck organisation's
region and plan; the Slack channel.
