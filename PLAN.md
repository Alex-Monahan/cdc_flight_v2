# Build plan (revision 3 — 2026-11-09)

Companion to `SPEC.md`. Every owner decision is made (SPEC §5). The build starts when the owner hands both documents
to the build orchestrator. It starts as a **fully local project** (stage 1) and moves to PlanetScale later (stage 2).

## 0. Orchestrator: read this first

You are the build orchestrator: a Fable session that the owner started in the root of this repository. You own SPEC
and PLAN, the gates, communication with the owner and the Flight deployments. You start with no knowledge beyond this
repository; that is intended.

1. Read `SPEC.md` (what to build and how it is graded) and this file (how to build it) completely. Then read
   `STATUS.md` if it exists: it records where the build stands. If it does not exist, create it and start at P1.
2. **State lives in the repository.** Agent memory and notes outside the repository are not available to build
   sessions. `STATUS.md` holds the current phase, gate results with links to evidence, decisions taken, questions for
   the owner, background jobs in flight and the next steps. Update and commit it at every gate, before any long wait
   and whenever the plan changes. A new session must be able to resume from `SPEC.md`, `PLAN.md`, `STATUS.md` and
   `git log` alone.
3. Run the session start checks below, then work through the stages and phases of §1 in order, through subagents
   (§3). Stage 1 needs nothing from PlanetScale. Stage 2 starts when the PlanetScale values of §8 are in the env file.
4. Do not stop between phases. Ask the owner only when a gate says so, when SPEC would have to change, when an action
   could change what the PlanetScale instance costs (§5), when a boundary check fails, or when a secret may have
   leaked. Otherwise decide, record the decision in `STATUS.md` and continue.
5. Commit small and often and push after every commit series (§4 has the rules for a public repository).

**Session start checks** (every session; stop and tell the owner if one fails):

1. `git status` is clean and `main` is up to date with the remote.
2. The environment holds the names that §8 lists for the current stage. Print names only, never values. If the
   PlanetScale names have appeared, note in `STATUS.md` that stage 2 can start.
3. The boundary guard is active. Two canaries must be **denied**: listing `~/Public` from the shell, and starting a
   subagent without a `model`. If either is allowed, the session was not started through the project launcher.
4. The audit receipt `~/.config/cdc_flight_v2/audit_status.json` shows a run within the last 70 minutes (§4). If it
   does not, tell the owner, record it in `STATUS.md` and continue.

## 1. Stages, phases and gates

The build runs in two stages.

- **Stage 1 — fully local. Starts now.** The source is the local Postgres primary and replica of §5; the destination
  is MotherDuck under the project's service account. Nothing in this stage waits for PlanetScale.
- **Stage 2 — PlanetScale.** Starts when the owner has put the PlanetScale values of §8 into the env file. The owner
  does not have them yet. Until then, stage 2 work is not attempted and nothing is reported as finished.

Stage 1 builds for PlanetScale from the first day: the runtime role is not a superuser and has only the privileges
a managed provider grants; the pipeline reaches Postgres through SQL connections only (no file-system access, no
`ALTER SYSTEM`); TLS is on; provider-specific behaviour sits behind configuration. Everything that can only be
verified on PlanetScale or on a Flight with a real source is listed in `STATUS.md` under "to verify in stage 2".

| Phase | Outcome | Gate to pass |
|---|---|---|
| **Stage 1 — local** | | |
| P1 Environment | Session start checks green. The newest stable Postgres is installed with Homebrew, and `tools/localpg` brings a local primary and replica up and down (§5). Connectivity to MotherDuck as the service account. Secret-scan gate verified (§4) | Each check has an evidence file. A commit that carries a planted fake credential is rejected. The canary denials are counted in the audit receipt after its next run |
| P2 Platform spike | The parts of §2 that need no Postgres source, on a real F16 Flight (§2 A), and per-stage throughput measured locally as a first indication. `docs/spike_report.md` and an architecture decision record | Report sent to the owner. The build continues without waiting, unless a finding contradicts SPEC |
| P3 Core | Oracle, crash-point harness and the fuzzers first; then commit/ack protocol, state machines, snapshot↔stream handoff, slot verification, leader guard, fencing and the heartbeat hard-exit | 1.1–1.9 and 4.2 tests green on T1 |
| P4 Feature waves | Schema and types · backfill modes · failure handling · observability · Postgres features · CDC features | Each wave: tests, evidence and an independent review, on T1 |
| P5 Hazards | `docs/wal_hazards.md` and prevention tests (research starts in P2 and runs alongside P3–P4) | Every hazard class has a prevention and a test |
| P6 Local robustness and pre-grading | The fuzz campaign of §6 runs on T1 and reaches every minimum of SPEC §4 that does not need PlanetScale or a Flight. Fresh-context graders pre-grade every item that is not marked *(Flight)* | Ledger and pre-grading report sent to the owner, with the list "to verify in stage 2" |
| **Stage 2 — PlanetScale** | | |
| P7 PlanetScale spike | §2 B on PlanetScale and on a real F16 Flight: replica decoding first. Cost baseline recorded (§5). Spike report extended | Replica decoding works on PlanetScale; if it does not yet, the owner decides |
| P8 Port and re-verify | Every suite runs on T2; whatever differs from local behaviour is fixed and gets a test | 1.1–1.9, 4.x and the feature suites green against PlanetScale |
| P9 Performance | 3.1, 5.1, 5.2 (24 h), 5.3, 5.4 on real F16 Flights; tuning of the streaming rate on F16 (SPEC 5.1) | Numbers meet SPEC, or the shortfall is reported with the bottleneck analysis and tuning log |
| P10 Robustness campaign | Robustness bar of SPEC §4 met in full, including the 24 h soak (§6) | The ledger shows every minimum reached with zero open divergences |
| P11 Grading | Fresh-context graders score all 43 items; the reviewer scores 2.6 and 3.3 | 214/215, or each shortfall reported to the owner |
| P12 Handover | Runbook, configuration reference, deployed Flight | Owner acceptance |

If stage 1 is complete before the PlanetScale values arrive: tell the owner, keep the fuzz campaign running, and use
the time for hardening and review. Do not start stage 2 against anything other than the owner's PlanetScale instance.

## 2. What the spikes must answer

A Flight cannot reach this machine, so a Flight has no Postgres source until stage 2.

### A. Platform spike (P2, stage 1) — real F16 Flight, no Postgres source

1. **JVM on a Flight**: install a JDK at start-up from the bootstrap, clone this public repository without a
   credential, start stock Debezium far enough to prove that it runs, time from run start to that point (the
   start-up budget of a hand-over), memory left for the pipeline.
2. **Run semantics** (drives SPEC 4.2 and 5.2): what the platform does with a scheduled start while another run of
   the same Flight is active (start, skip or queue); how long a run that finds a healthy leader takes and what 1,440
   of them a day cost (the cost is accepted in advance; report the number); whether a run can cancel another run from
   inside a Flight and how quickly; whether failed runs are retried; what happens at the maximum run time; how long a
   frozen run takes to exit through the heartbeat hard-exit.
3. **MotherDuck semantics**: drop + rename inside a transaction under a concurrent reader; `ALTER … TYPE UNION(…)`;
   size limits of one transaction; concurrent writers; the leader record as a compare-and-swap with one winner; a
   single clock for heartbeat age; bulk-load rate at `lineitem` width from generated data.
4. **Service-account ownership**: Flights are created, scheduled, run, cancelled and read with MotherDuck's Flight SQL
   functions (`MD_CREATE_FLIGHT`, `MD_UPDATE_FLIGHT`, `MD_RUN_FLIGHT`, `MD_CANCEL_FLIGHT_RUN`, `MD_GET_FLIGHT_LOGS`;
   see MotherDuck's public documentation) from a connection authenticated as the project's service account, with
   secrets attached. Confirm that the run-time token sees only this project's databases.
5. **Local throughput, as a first indication only**: Postgres read rate from the local replica (parallel `COPY`,
   DuckDB's Postgres reader) and stock Debezium streaming rate for small transactions and for one 10M-row
   transaction, on this machine. These numbers choose the architecture; they are never reported as performance
   results (SPEC: performance claims come from Flight runs).

### B. PlanetScale spike (P7, stage 2) — PlanetScale, and a real F16 Flight with PlanetScale as its source

6. **Replica decoding first** (SPEC R17, 4.1): Postgres version; how a replica is addressed; creating a logical slot
   on a replica and decoding from it under load; behaviour of that slot across replica restart, replica replacement,
   primary restart and failover or promotion; slot invalidation causes and how each is prevented; replica replay lag
   under load; a long idle period. This is tested extensively and it must be made to work — primary mode is not an
   acceptable outcome without the owner's review.
7. **Primary mode**: failover-slot behaviour across a real failover; privileges for publication maintenance,
   `pg_logical_emit_message` and terminating our own replication session; sender timeouts.
8. **Per-stage throughput at `lineitem` width on a Flight**: read rate from a PlanetScale replica, MotherDuck
   bulk-load rate, stock Debezium streaming rate for small transactions and for one 10M-row transaction. The
   streaming number is the starting point of the tuning work in P9; the Flight size stays F16.

### Architecture candidates (chosen by spike data, recorded as an ADR)

- **A** — stock Debezium Server as a supervised child process, delivering to a local sink consumed by the applier.
- **B** — stock Debezium embedded engine inside a small JVM program of ours, run as a supervised child process, handing
  batches to the applier with explicit acknowledgement after the destination commit.
- **C** — embedded engine loaded into the Python process through a JVM bridge.

Expected choice: **B** (a child process can be health-checked, memory-capped, killed and restarted, which 4.5 and 4.7
need; the engine's batch API gives an explicit acknowledge step for exactly-once). Backfill is independent of the
choice: direct parallel reads from a replica into MotherDuck staging tables, then swap (SPEC R3, R17, 3.1, 3.2).

## 3. Agents

No Sonnet anywhere. Every agent call sets `model` to `opus` or `fable`; a call without it is denied. Haiku is used
for one thing only, outside the build: the hourly boundary audit (owner's instruction, to keep its cost down).

| Role | Model / effort | Notes |
|---|---|---|
| Orchestrator | Fable (the session the owner starts), xhigh in P2–P3, high afterwards | Owns SPEC/PLAN/STATUS, gates, owner communication, Flight deployments |
| Coordinator (subagent) | Opus 5.5, xhigh | One per phase or wave. Splits the work, launches workers, merges, runs the suites, reports to the orchestrator |
| Core-correctness worker | Fable, xhigh | Commit/ack protocol, handoff, slot verification, leader guard and fencing |
| Implementation workers | Opus 5.5, high | One worktree each, at most 4 writing at once |
| Hazard researcher | Opus 5.5, high | Protocol documentation, Debezium documentation and issue tracker |
| Fuzzer and campaign owner | Opus 5.5, high | Builds the fuzzers, keeps the campaign running, turns each failure into a seeded regression test |
| Graders | Opus 5.5, xhigh, fresh context | Never grade code they wrote; must attack with cases outside the hazard catalog |
| Subjective reviewer (2.6, 3.3) | Opus 5.5, xhigh, fresh context | Scores against the SPEC checklists |
| Log triage, test watching, docs | Opus 5.5, medium | |

If a subagent cannot launch subagents in this environment, the coordinator plans, reviews and merges, and the
orchestrator launches the workers on the coordinator's instructions.

Every subagent is given `SPEC.md`, `PLAN.md` and its task, and nothing else. Working rules: tests before features;
one worktree per writer; small commits pushed often; long suites run as background jobs; a claim without an evidence
file is not accepted; performance claims only from Flight runs.

## 4. Project boundary and repository rules

Agents work **only** with this repository, the test environments in §5 and public documentation of Postgres,
Debezium, DuckDB, MotherDuck and PlanetScale. Everything else on this machine, every other repository of the owner,
and every MotherDuck database not created by this project is out of bounds. A tool-call guard enforces this, denies
by default when unsure, and logs every denial. Other folders on this machine are locked at the file-system level;
that is deliberate. A denied call is not retried by another route — report it to the coordinator instead. Build
sessions have no browser and no MCP tools.

**MotherDuck service account.** All build sessions, tests and Flights use the dedicated service account
`cdc_flight_v2_build`, which cannot see any database outside this project. Build sessions are started through a
launcher that puts only the project's credentials in the environment; Flights are created from a connection
authenticated as that account, so they run with its permissions. MotherDuck databases created by this project are
named `cdc_v2_*`; tests create and drop only such databases.

**GitHub.** Build sessions hold a GitHub credential that is valid for this repository only: `git` fetch and push
work for `Alex-Monahan/cdc_flight_v2` and for nothing else, and public repositories can be cloned anonymously. The
`gh` command has no credential in build sessions — everything the build needs goes through `git`. Do not look for,
or switch to, any other GitHub credential.

**Names.** Everything this build creates carries a name that starts with `cdc_v2`: the Python package (`cdc_v2`),
MotherDuck databases, Postgres roles, publications and slots, Flights, local folders. The repository name
`cdc_flight_v2` is always written in full. Other spellings of the project name are refused by the boundary guard.

**Public repository (SPEC R23).** Everything committed is published. Therefore:

- Secrets, hostnames, database user names and connection strings exist only in `~/.config/cdc_flight_v2/env`
  (mode 600, outside the repository) and as MotherDuck secrets attached to the Flight. The PlanetScale organisation
  and database are never named in the repository. Code reads all of them from the environment.
- `tools/check_secrets.py` runs before every commit (staged content and the commit message) and before every push
  (every commit being pushed). It has three layers: the exact values of the env file (also base64- and URL-encoded),
  forbidden file names, and pattern and randomness rules; `gitleaks` runs as a second opinion. The hooks are active in
  every clone and worktree of a session started through the launcher. They are never skipped and git's hook
  configuration is never changed.
- A false positive of a pattern rule is suppressed with `secret-scan: allow` on that line or a regular expression in
  `.secretscan-allow`. The exact-value and file-name layers cannot be suppressed: remove the value. Every change to
  the scanner, its tests or `.secretscan-allow` is listed in `STATUS.md` for the owner's review at the next gate.
- The test harness redacts hostnames, user names, tokens and connection strings when it writes evidence or logs.
- GitHub secret scanning and push protection are enabled on the repository as a further net.
- If a secret reaches the remote anyway: stop, tell the owner at once and say which credential must be rotated.
  Rewriting history is not a fix.

**Hourly boundary audit (owner's instruction).** A script outside this repository runs every hour; it is already
installed. It scans the logs of every Claude session and subagent of this build, and the guard's denial log, for any
use of an out-of-bounds location. If it finds something, the owner is alerted (Slack and a local notification) with
what was touched, by which session or agent, and when. The scan is deterministic; hits are judged and summarised by
Haiku, and an hour without hits makes no model call. Its rule list is kept outside the repository. After each run it
writes a receipt to `~/.config/cdc_flight_v2/audit_status.json` (times and counts only: `last_run`, `status`,
`findings_total`, `denials_seen_total`, `alerts_total`, …), which build sessions read in the session start checks and
at each gate. A `status` other than `ok`, or a `findings_total` above zero, is a boundary failure: tell the owner.

## 5. Test environments

| Tier | Source | Destination | Used for |
|---|---|---|---|
| T0 | none | in-memory | unit tests, state-machine transition tests |
| T1 | **local Postgres primary and replica** (below), decoding on the replica by default | MotherDuck `cdc_v2_*` | all development, crash points, network faults through a TCP proxy, replica loss and promotion drills, fuzzers |
| T2 (stage 2) | PlanetScale, process on this machine | MotherDuck | compatibility, replica-mode and primary-mode behaviour |
| T3 (stage 2) | PlanetScale, **real F16 Flight** | MotherDuck | all performance items, 24 h latency, run hand-over, real failover, 24 h soak |

### Local Postgres (T1)

The pipeline is built and debugged against a local primary and replica first; PlanetScale and Flight runs come after
a change is green locally.

- **Version**: the newest stable major version that Homebrew offers when P1 starts. Run `brew update` first; on
  2026-11-09 that is the formula `postgresql@18`, and an older copy of it is installed and must be upgraded
  (`brew upgrade`). If Homebrew offers a newer stable major version by then, use that one.
- **Installed with Homebrew and run as native processes. Never Docker, never any other container or virtual
  machine** — this machine does not have the memory for it.
- Run the formula's own binaries (`initdb`, `pg_ctl`, `pg_basebackup`) directly, not as a Homebrew service: nothing
  starts at login and nothing of this build listens on port 5432.
- **A Postgres that belongs to other work of the owner runs as a Homebrew service on port 5432. It is not part of
  this build: never connect to it, stop it, restart it or touch its data directory.** Upgrade the formula with
  `HOMEBREW_NO_INSTALL_CLEANUP=1 brew upgrade postgresql@18`, so that the binaries it is running stay on disk; it
  picks up the new version whenever the owner next restarts it. Build sessions have `PGPORT=55432` in their
  environment, so a connection that names no port reaches this build's primary and not that service.
- Data directories live outside the repository, under `~/.cache/cdc_flight_v2/pg/`. Ports: primary 55432, replica
  55433, further standbys from 55434, TCP proxies from 55440. Listen on `localhost` only.
- Topology: a primary with `wal_level = logical`, and a streaming replica created with `pg_basebackup` that uses a
  physical replication slot and `hot_standby_feedback`. Logical decoding runs on the replica by default, as in
  production (SPEC R17). A second standby is started only for the replica-replacement and promotion drills.
- The machine has 16 GB of memory: keep `shared_buffers` small (≤ 256 MB per instance), run only the primary and one
  replica continuously, and stop the clusters when nothing uses them.
- `tools/localpg` (built in P1) is the only way clusters are managed: `up`, `down`, `status`, `reset`,
  `add-standby`, `promote`, `kill` (for crash tests). It is idempotent and leaves no stray processes behind.

### PlanetScale (T2, T3) — stage 2

Nothing here is used before the owner has provided the PlanetScale values of §8. The owner's approval (2026-11-09): the PlanetScale database is a **development instance**. Its configuration and
contents may be changed however the build needs — cluster parameters, roles, publications, replication slots,
restarts, failovers, schemas and data. One hard limit: **nothing may change what the instance costs.**

- Before the first change, find out what the bill depends on (cluster size, number of replicas, storage, branches,
  add-ons) and record the current settings as the cost baseline, with names redacted. Never resize the cluster, add
  or remove replicas, create a database or a branch, enable a paid add-on, or let billed storage grow. After every
  configuration change, verify that the baseline is unchanged.
- Size test data to the storage that is already provisioned. If the 60M-row benchmark table (SPEC 5.1) does not fit
  without growing billed storage, stop and ask the owner before loading it.
- If it is unclear whether an action changes the cost, ask the owner first.
- Create everything under names starting with `cdc_v2_` (schemas, roles, publications, slots) and clean up after
  tests. Do not drop or alter objects the build did not create; if the build cannot proceed without that, ask the
  owner.
- Configuration is changed through PlanetScale's API with the service token of §8 — build sessions have no browser.
  If the token is missing or lacks a permission, ask the owner for the token or for the change.

## 6. Robustness campaign (SPEC R22)

- The five fuzzers of SPEC §4 (workload, fault, value, stream, leadership) are built at the start of P3, before the
  features they attack, and run as background jobs from then on: on T1 continuously, and on T2 nightly once stage 2
  has started.
- A ledger under `evidence/robustness/` records, per fuzzer: seeds run, cumulative hours, faults injected, failures
  found, and the commit at which its counters last restarted. A fix to correctness code restarts the counters of the
  affected fuzzer.
- Every failure is reduced to a seeded regression test before it is fixed.
- The **24 h soak** runs on T3 after P9, in replica mode, with `run.max_runtime` set to 1 hour so that at least 20
  hand-overs occur, at least two forced failovers and one lag-triggered backfill.

## 7. Run configuration decided so far

- Flight size F16, scheduled every minute; every run starts with the leader guard (SPEC 4.2). There is no separate
  watchdog Flight.
- A run whose heartbeat has not been refreshed for 60 s hard-exits (SPEC 4.2).
- `run.max_runtime` 7 h 30 min (50 min on a Lite plan; 1 h during the soak); the leader releases and exits just
  before a minute boundary.
- Replica mode by default; primary mode by configuration. `replica.fallback_after` 15 min: with every replica
  unavailable for that long, primary mode until a replica is healthy again (SPEC 4.1, 7.2).
- Operations schema `_cdc`; alerts to Slack and to `_cdc`.

## 8. Values and facts from the owner

Values are never pasted into chat or committed. Local runs read `~/.config/cdc_flight_v2/env`; the launcher exports
its content into every build session. Flights receive the same names as environment variables from MotherDuck
secrets attached to the Flight.

| Name | What | Stage | Status on 2026-11-09 |
|---|---|---|---|
| `MOTHERDUCK_TOKEN` | Token of the service account `cdc_flight_v2_build` (expires about 2027-02-07) | 1 | in place |
| `CDC_PII_SALT` | Random salt for pseudonymisation (SPEC 8.3) | 1 | in place |
| `SLACK_WEBHOOK_URL` | Incoming webhook for the alert channel (also used by the hourly boundary audit). Until it exists, alerts are tested against a local stub | 1 | not yet provided |
| `PLANETSCALE_ORG`, `PLANETSCALE_DATABASE` | The development instance | 2 | in place |
| `PLANETSCALE_SERVICE_TOKEN_ID`, `PLANETSCALE_SERVICE_TOKEN`, `PLANETSCALE_BRANCH` | For configuration changes, restarts and failovers through PlanetScale's API | 2 | not yet provided |
| `PG_HOST`, `PG_PORT`, `PG_DATABASE` | PlanetScale primary, direct connection (not a pooler) | 2 | not yet provided |
| `PG_REPLICA_HOST`, `PG_REPLICA_PORT` | A replica, direct connection. If PlanetScale addresses replicas in another way (for example through the user name), the build records that way in the env file | 2 | not yet provided |
| `PG_ADMIN_USER`, `PG_ADMIN_PASSWORD` | Role that can create roles, schemas, tables and publications | 2 | not yet provided |
| `PG_USER`, `PG_PASSWORD` | Runtime role with `REPLICATION`; created by the build from the admin role if absent | 2 | created in P7 |
| `PG_SSLMODE` | `verify-full` expected | 2 | not yet provided |

The local clusters of stage 1 need no entries here: `tools/localpg` creates their roles and passwords and keeps them
under `~/.cache/cdc_flight_v2/pg/`.

If a stage 1 value is missing when a phase needs it, ask the owner for it and continue with the work that does not
depend on it.

Facts still to be confirmed with the owner: the MotherDuck organisation's region and plan (the plan decides the
maximum run time; measure it in P2 if still unknown); the Slack channel for alerts; and, for stage 2, whether the
PlanetScale instance restricts client addresses (the Flight egress addresses and this machine must be allowed).
