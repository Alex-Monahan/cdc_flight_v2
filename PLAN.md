# Build plan (DRAFT for refinement, revision 2 — 2026-11-09)

Companion to `SPEC.md`. Nothing here starts until the owner approves both documents.

## 1. Phases and gates

| Phase | Outcome | Gate to pass |
|---|---|---|
| P0 Refinement | Remaining open decisions settled or accepted at their defaults (SPEC §5: D2, D3, D14–D17) | Owner sign-off on SPEC and PLAN |
| P1 Access and boundary | Secrets in place; build sessions run on the project's MotherDuck service account; source parameters verified; project boundary enforced; hourly boundary audit installed (§4) | Connectivity checks green; boundary canaries all denied; the audit raises an alert on a planted test entry |
| P2 Flight spike | `docs/spike_report.md` with measured numbers and an architecture decision record | Owner checkpoint on D3, D14, D15, D17 |
| P3 Core | Oracle, crash-point harness and the fuzzers first; then commit/ack protocol, state machines, snapshot↔stream handoff, slot verification, leader guard and fencing | 1.1–1.9, 4.2 tests green locally and against PlanetScale |
| P4 Feature waves | Schema and types · backfill modes · failure handling · observability · Postgres features · CDC features | Each wave: tests + evidence, independent review |
| P5 Hazards | `docs/wal_hazards.md` and prevention tests (research starts in P2, runs alongside P3–P4) | Every hazard class has a prevention and a test |
| P6 Performance | 3.1, 5.1, 5.2 (24 h), 5.3, 5.4 on real F16 Flights | Numbers meet SPEC or are escalated with data |
| P7 Robustness campaign | Robustness bar of SPEC §4 met, including the 72 h soak (§6) | Ledger shows every minimum reached with zero open divergences |
| P8 Grading | Fresh-context graders score all 43 items; reviewer scores 2.6 and 3.3 | 214/215, or each shortfall escalated |
| P9 Handover | Runbook, config reference, deployed Flight | Owner acceptance |

## 2. What the spike must answer (P2)

All on a real Flight against the PlanetScale instance, on F16 (F4 only for the watchdog and tick-cost measurements).

1. **JVM on a Flight**: install a JDK at start-up from the bootstrap, clone this repo with a read-only credential,
   time from run start to first applied event (the start-up budget of a hand-over), memory left for the pipeline.
2. **Run semantics** (drives 4.2, 5.2, D14): what the platform does with a scheduled run while another run of the
   same Flight is active (start, skip or queue); what a run that exits within 10 s costs, times 1,440 a day; whether a
   run can cancel another run from inside a Flight, and how quickly; whether failed runs are retried; what happens at
   the maximum run time.
3. **Per-stage throughput at `lineitem` width**: Postgres read rate from a replica (parallel `COPY`, DuckDB's
   Postgres reader), MotherDuck bulk-load rate, stock Debezium streaming rate for small transactions and for one
   10M-row transaction.
4. **MotherDuck semantics**: drop + rename inside a transaction under a concurrent reader; `ALTER … TYPE UNION(…)`;
   size limits of one transaction; concurrent writers; the leader record as a compare-and-swap with one winner; a
   single clock for heartbeat age.
5. **PlanetScale specifics** (drives D15, D17): Postgres version; whether logical decoding on a replica is permitted;
   how a slot on a replica behaves across replica restart, replica replacement and promotion; replica replay lag under
   load; failover-slot behaviour in primary mode across a real failover; privileges for publication maintenance,
   `pg_logical_emit_message` and terminating our own replication session; sender timeouts.
6. **Service-account ownership**: Flights created and run by the project's service account, with secrets attached;
   confirm the run-time token sees only this project's databases.

### Architecture candidates (chosen by spike data, recorded as an ADR)

- **A** — stock Debezium Server as a supervised child process, delivering to a local sink consumed by the applier.
- **B** — stock Debezium embedded engine inside a small JVM program of ours, run as a supervised child process, handing
  batches to the applier with explicit acknowledgement after the destination commit.
- **C** — embedded engine loaded into the Python process through a JVM bridge.

Expected choice: **B** (a child process can be health-checked, memory-capped, killed and restarted, which 4.5 and 4.7
need; the engine's batch API gives an explicit acknowledge step for exactly-once). Backfill is independent of the
choice: direct parallel reads from a replica into MotherDuck staging tables, then swap (SPEC R3, R17, 3.1, 3.2).

## 3. Agents

No Sonnet anywhere. Every agent call names its model explicitly. Haiku is used for one thing only: the hourly
boundary audit (owner's instruction, to keep its cost down). It never writes code and never works in this repository.

| Role | Model / effort | Notes |
|---|---|---|
| Orchestrator | Fable, xhigh in P2–P3, high afterwards | Owns SPEC/PLAN, gates, owner communication, Flight deployments |
| Coordinator (subagent) | Opus 5.5, xhigh | One per phase or wave. Splits work, launches workers, merges, runs suites, reports to the orchestrator |
| Core-correctness worker | Fable, xhigh | Commit/ack protocol, handoff, slot verification, leader guard and fencing |
| Implementation workers | Opus 5.5, high | One worktree each, at most 4 writing at once |
| Hazard researcher | Opus 5.5, high | Protocol documentation, Debezium documentation and issue tracker |
| Fuzzer and campaign owner | Opus 5.5, high | Builds the fuzzers, keeps the campaign running, turns each failure into a seeded regression test |
| Graders | Opus 5.5, xhigh, fresh context | Never grade code they wrote; must attack with cases outside the hazard catalog |
| Subjective reviewer (2.6, 3.3) | Opus 5.5, xhigh, fresh context | Scores against the SPEC checklists |
| Log triage, test watching, docs | Opus 5.5, medium | |
| Hourly boundary audit | Haiku | Outside the build sessions; only judges and summarises hits of a deterministic scan (§4) |

Working rules: tests before features; one worktree per writer; small commits pushed often; long suites run as
background jobs; a claim without an evidence file is not accepted; performance claims only from Flight runs.

## 4. Project boundary

Agents work **only** with this repository, the test environments in §5 and public documentation of Postgres,
Debezium, DuckDB, MotherDuck and PlanetScale. Everything else on this machine, every other repository of the owner,
and every MotherDuck database not created by this project is out of bounds. A tool-call guard enforces this, denies
by default when unsure, and logs every denial; the log is audited at each gate. A denied call is not to be retried by
another route — report it to the coordinator instead.

**MotherDuck service account.** All build sessions, tests and Flights use the dedicated service account
`cdc_flight_v2_build`, which cannot see any database outside this project. Build sessions are started through a
launcher that puts only that account's token in the environment; Flights are created while signed in as that account,
so they run with its permissions. MotherDuck databases created by this project are named `cdc_v2_*`; tests create and
drop only such databases.

**Hourly boundary audit (owner's instruction).** A script outside this repository runs every hour. It scans the logs
of every Claude session and subagent used for this build, and the guard's denial log, for any use of an out-of-bounds
location. If it finds something, the owner is alerted (Slack and a local notification) with what was touched, by which
session or agent, and when. The scan itself is deterministic; hits are judged and summarised by Haiku, and an hour
without hits makes no model call. The audit is installed and tested in P1, before the first build agent starts, and
its rule list is kept outside the repository.

## 5. Test tiers

| Tier | Source | Destination | Used for |
|---|---|---|---|
| T0 | none | in-memory | unit tests, state-machine transition tests |
| T1 | local Postgres 18 from the installed binaries, private data directories, ports ≥ 55432 (never 5432, never Docker); primary + two standbys, decoding on a standby by default | MotherDuck `cdc_v2_*` | crash points, network faults through a TCP proxy, replica loss and promotion drills, fuzzers |
| T2 | PlanetScale, process on this machine | MotherDuck | compatibility, replica-mode and primary-mode behaviour |
| T3 | PlanetScale, **real F16 Flight** | MotherDuck | all performance items, 24 h latency, run hand-over, real failover, 72 h soak |

## 6. Robustness campaign (SPEC R22)

- The five fuzzers of SPEC §4 (workload, fault, value, stream, leadership) are built at the start of P3, before the
  features they attack, and run as background jobs from then on, on T1 continuously and on T2 nightly.
- A ledger under `evidence/robustness/` records, per fuzzer: seeds run, cumulative hours, faults injected, failures
  found, and the commit at which its counters last restarted. A fix to correctness code restarts the counters of the
  affected fuzzer.
- Every failure is reduced to a seeded regression test before it is fixed.
- The 72 h soak runs on T3 after P6, with scheduled hand-overs, at least two forced failovers and one lag-triggered
  backfill.

## 7. Run configuration decided so far

- Flight size F16, scheduled every minute; every run starts with the leader guard (SPEC 4.2).
- `run.max_runtime` 7 h 30 min (50 min on a Lite plan); the leader releases and exits just before a minute boundary.
- Replica mode by default; primary mode by configuration (SPEC 4.1, 7.2).
- Operations schema `_cdc`; alerts to Slack and to `_cdc`.

## 8. Needed from the owner

Values are never pasted into chat or committed. Local runs read a mode-600 file outside the repo
(`~/.config/cdc_flight_v2/env`); Flights read MotherDuck secrets of type `flights` with the same names.

| Name | What | Status |
|---|---|---|
| `MOTHERDUCK_TOKEN` | Token of the service account `cdc_flight_v2_build` (90 days, expires about 2027-02-07) | in place |
| `PG_HOST`, `PG_PORT`, `PG_DATABASE` | PlanetScale primary, direct connection (not a pooler) | needed |
| `PG_REPLICA_HOST`, `PG_REPLICA_PORT` | A replica endpoint, direct connection. Required for the default mode; a second replica lets replica replacement be tested | needed |
| `PG_ADMIN_USER`, `PG_ADMIN_PASSWORD` | Role that can create tables, roles and publications (test fixtures) | needed |
| `PG_USER`, `PG_PASSWORD` | Runtime role with `REPLICATION` (can be created from the admin role) | needed |
| `PG_SSLMODE` | `verify-full` expected | needed |
| `PLANETSCALE_SERVICE_TOKEN_ID`, `PLANETSCALE_SERVICE_TOKEN`, `PLANETSCALE_ORG`, `PLANETSCALE_DATABASE`, `PLANETSCALE_BRANCH` | Only if failovers and parameter changes should be scripted | optional |
| `SLACK_WEBHOOK_URL` | Incoming webhook for the alert channel (also used by the hourly boundary audit) | needed |
| `CDC_PII_SALT` | Random 32+ bytes | can be generated here |
| `CDC_REPO_DEPLOY_KEY` | Read-only credential for cloning this repo inside a Flight | can be generated here |

Facts needed (no secrets): Postgres major version (decoding on a replica needs 16 or later); number of replicas and
whether replication connections to a replica are allowed; cluster size and free storage (the benchmark table is 60M
rows); whether cluster parameters may be changed (`wal_level`, `max_replication_slots`, `max_wal_senders`,
`max_slot_wal_keep_size`, `sync_replication_slots`, `hot_standby_feedback`); whether the instance is disposable (heavy
writes, slot drops, backend kills, forced failovers, WAL growth) and any spend limit; whether IP restrictions are on
(the Flight egress addresses and this machine must be allowed); the MotherDuck organisation's region and plan; the
Slack channel.
