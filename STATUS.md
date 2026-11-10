# Build status

Last updated: 2026-11-10, by the orchestrator. Resume from `SPEC.md`, `PLAN.md` (read §9), this file and `git log`.

## Where the build stands

- **Stage 1 (local). P1 Environment: gate passed on 2026-11-09** (one item cannot be shown from this session, see
  the table). **P2 Platform spike: gate passed on 2026-11-10** — every spike is done and merged, the report
  (`docs/spike_report.md`) is complete and was sent to the owner; nothing found contradicts SPEC, so the build
  continues without waiting. **P3 Core: in progress** (table below): the fault harness, the oracle, the source
  package and the engine are merged; the apply package is delivered and reviewed (a short fix round is owed); the
  cut is in its round of fixes; the destination basics, the transform and the fuzzers are being built; the task
  file of every package is written.
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

## P2 gate — passed 2026-11-10

| Spike (PLAN §2 A) | State | Report |
|---|---|---|
| 4 Flight SQL functions, platform facts | done | `docs/spike/flight_platform.md` |
| 3 MotherDuck semantics | done (laptop part and the repeat inside an F16 Flight) | `docs/spike/md_semantics.md` |
| 1 JVM and stock Debezium on a Flight | done | `docs/spike/jvm_on_flight.md` |
| 2 Run semantics | done | `docs/spike/run_semantics.md` |
| 5 Local throughput (first indication) | done — part 1: read rate from the replica, end to end into MotherDuck, hazards of long reads on a standby; part 2: stock Debezium streaming rate on the replica's slot, position fields, restart inside a large transaction | `docs/spike/local_throughput.md`, `docs/spike/local_streaming.md` |
| `docs/spike_report.md` and the architecture decision record | complete, checked against the spike reports, **sent to the owner on 2026-11-10**. Nothing found contradicts SPEC | `docs/spike_report.md`, `docs/adr/0001-architecture.md` |

At the gate (2026-11-10 07:05 UTC): no `cdc_v2*` Flight, no `cdc_v2*` Flight secret and no `cdc_v2*` database exists
on MotherDuck; the audit receipt reads status `ok`, findings 0 in total, last run 2026-11-09 23:08 MST (the receipt
is written by a job outside this session; it had not run again since).

## P3 Core — in progress

Plan and contracts: `docs/design/p3_plan.md`. Design: `docs/design/core.md`. Rules for every worker:
`docs/tasks/README.md`. One task file per package under `docs/tasks/`. Gate: the tests of SPEC 1.1–1.9 and 4.2
green on T1 (local primary and standby, real MotherDuck, local JVM).

| Package | What | Worker | State on 2026-11-10 |
|---|---|---|---|
| H | Crash points, fault sites, process control, crash runner, TCP fault proxy, byte-stream relay | Opus | **merged** |
| O | Oracle, T1 harness, evidence writer, workload generator and observer | Opus | **merged** (fix round included) |
| X | The cut: where a snapshot lies in the stream | Fable | review: merge after fixes (below); **fix round running** |
| E | Engine: the JVM program around stock Debezium, frames, the Python side | Opus | **merged** (two reviews, two fix rounds) |
| D | Destination basics: lanes, fence, deadlines, errors, `_cdc` schema, transition function, alert records | Fable | running |
| A | Apply: the statements over a change set, keyed and keyless | Opus | **delivered**; coordinator: merge after a short fix round (below); a second review (Fable) of its row-version model is running; the fix round starts at the next free writer slot |
| P | Source: connections, catalog and its observation, publication, slot, slot facts, timeline, heartbeat | Opus | **merged** (with the repair of `tools/localpg` for Postgres 16; one fix after the review) |
| L | Leadership: guard, take-over, heartbeat, hand-over, bootstrap, platform interface | Fable | task written; after D |
| Y | Transform: source types to destination types | Opus | **running** |
| S | Stream: transactions, groups, commit and acknowledge, supervisor, pipeline process | Fable | task written; after E, D, A, P |
| V | Slot continuity | Fable | task written; after S |
| F | Fuzzers (stream, workload, fault, leadership), the consumer loop of 1.3 and 3.2 | Opus | **running** |
| B | Backfill and hand-off | Fable | task written; after X, S, A, P, Y |
| T | The catalog gate, truncate, drop and recreate, the document and the test over all state machines | Opus | task written; after S, B |

At most four workers write at once (PLAN §3). The coordinator (Opus, xhigh) writes no code: it reviewed the plan
and reviews every package branch before its merge; D, L, S, V, B and the acknowledgement path of E get a second
reviewer.

Parts of the P3 gate items that need a later feature, and where they go (the list the plan promises at the gate):

| Part | Carried to |
|---|---|
| 1.4 "with history mode on" | P4, CDC features |
| 1.7 crash points at every step of a schema change | P4, schema and types |
| 1.9 schema change applied in place (P3 has the life of a catalog finding, which ends in a drop or a backfill); the full quarantine machine (P3 has the exit to `quarantined` and the operator's re-backfill) | P4 |
| 1.7 soak of at least 1 h with at least 200 faults | a short soak at the P3 gate; the full one in the P6 campaign |
| 1.1/1.2 for one value: `interval 'infinity'` / `'-infinity'` in a scalar `interval` column of a source on Postgres 17 or later reaches the stream as a zero interval (the Postgres driver, before any code of ours). P3 marks and counts such rows and alerts; the repair read that makes the value exact is built on the stream's group loop (plan C3, "Ambiguous zero interval"). No P3 gate workload holds an interval column | P4, schema and types |

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
| 2026-11-10 | P3 is split into 14 packages with written contracts (`docs/design/p3_plan.md`). The orchestrator starts the workers and merges; the coordinator reviews and writes no code | orchestrator |
| 2026-11-10 | Shared modules for every package: `cdc_v2.lsn` (positions as integers) and `cdc_v2.deadline` (every wait has a deadline) | orchestrator |
| 2026-11-10 | The coordinator's review of the plan (11 findings that had to be fixed) is folded into the design and the contracts, commit `913a375`; the record is `docs/design/p3_plan.md` §6. The main changes: the pipeline registers its MotherDuck connections per leader epoch, and a take-over interrupts exactly those; the leader tables are created once, with every row, by the bootstrap, and their shape is frozen; the slot is checked before the engine starts **and** again after the engine holds it, and nothing is applied before the continuity decision; every backfill request is enacted inside a stream transaction; staging rows are written by fenced transactions only; a table whose apply fails repeatedly leaves the stream with an alert instead of stopping every table | orchestrator, after the coordinator's review |
| 2026-11-10 | SPEC 1.3 during a re-backfill: a table that is re-backfilled **without a gap in the stream** (operator request, mode change) keeps receiving every transaction in the visible table until its swap, so it never leaves the cross-table guarantee. After a gap (slot finding, lag-triggered re-backfill, quarantine, drop and recreate) the visible table is frozen at its last complete version (SPEC 3.6) and each table swaps on its own; until its swap it is outside the guarantee, like a table in its first backfill. See the questions for the owner | orchestrator |
| 2026-11-10 | The shared T1 fixtures take their Postgres cluster and their MotherDuck prefix from the environment (`CDC_V2_PG_HOME`, `CDC_V2_PG_PORT_BASE`, `CDC_V2_MD_PREFIX`). Every worker has a home, a port range and a prefix of its own (plan §4); the default home is for the orchestrator's gate runs; a test that may break a cluster leases a private one from ports 55840–56095 | orchestrator |
| 2026-11-10 | Catalog gate (plan C10). Debezium names a table by schema and name only. Before a group is applied, the stream confirms from a catalog observation that covers the group that every table in it is still the table its destination was built for (OID, columns by `attnum`, key). Cost: one catalog round trip per group. Rows of a table in doubt are not applied; what follows is enacted once the stream has passed the observation | orchestrator |
| 2026-11-10 | P3 rule "the destination follows the source's names" (plan C10). A name that is gone at the source: the destination table is dropped, or renamed under `on_source_drop: retain`. A name with a new OID: the old destination table is dropped (or renamed) and the new table appears when its first snapshot is complete; the name is absent in between. A rename is a drop and, if the new name is configured, a fresh table. A change of columns that the stream cannot place sends the table through a backfill. Rename and column changes in place (SPEC 2.1–2.4) come with the schema wave of P4 | orchestrator |
| 2026-11-10 | All work was stopped for one hour (2026-11-09 22:33–23:33 MST) at the owner's request and resumed by a timer. The four running workers were stopped and resumed; they repeat any measurement that was interrupted | owner |
| 2026-11-10 | P2 gate passed. The report is sent; the build continues without waiting (PLAN §2: nothing found contradicts SPEC) | orchestrator |
| 2026-11-10 | `offset.flush.interval.ms=0` together with `heartbeat.interval.ms` is required configuration, not tuning: without it Debezium never confirms the last transaction before an idle period (`docs/spike/local_streaming.md` §6; `docs/design/core.md` §3) | orchestrator |
| 2026-11-10 | The records of one transaction are found by their order in the stream (BEGIN … END), not by Debezium's transaction `id`, which differs from record to record; only the END record carries the transaction's commit end position (`docs/design/core.md` §2) | orchestrator |
| 2026-11-10 | `tools/localpg` cannot start a pair on the Postgres 16 binaries (it writes a setting and reads slot columns that exist from 17 on). Package P repairs it first, as a commit of its own; the tests that packages E, V and T owe on 16 run after that | orchestrator |
| 2026-11-10 | Local test versions: Postgres 18 is required (SPEC R15); facts that differ by version are also run on the installed Postgres 16; Postgres 17 is not installed and nothing more is installed. The version of the stage 2 instance decides what else is needed | orchestrator |
| 2026-11-10 | Contract C2, attach rule: Debezium gives every replication connection the same application name, so the pipeline recognises its own engine by the slot holder's process id together with that backend's start time, which the engine reads and names in `HELLO`. Never by the process id alone. The engine never attaches a second time by itself | orchestrator, with the coordinator's text |
| 2026-11-10 | **Only END records are acknowledged.** A Debezium heartbeat never moves the applied position and is never marked by the engine. Reason (second review of the engine): Debezium dispatches a heartbeat that already carries a transaction's commit position before that transaction's END record exists; a pipeline written to the earlier contract lost 80 of 20,000 transactions in the reviewer's run. On an idle source the position moves with the pipeline's own heartbeat write, a row in a published table on the primary every 10 s (which SPEC 4.6 needs anyway). Contract C2, design sections 2 and 3, and the tasks of S, F, P and E say so | orchestrator |
| 2026-11-10 | A black-holed replication connection (neither Debezium nor the driver notices one) is found by two rules: the slot-continuity check sees that the holder is gone once the server has ended the silent walsender (`wal_sender_timeout` + the check interval; a `wal_sender_timeout` of 0 is refused), and the stream's marker rule of SPEC 4.6 (a marker written on the primary must come back within 60 s; counted only while no transaction is open). Engine frames prove that the engine is alive, not that its connection is | orchestrator, with the coordinator |
| 2026-11-10 | The engine refuses configuration that would weaken a guarantee (acknowledgement, offsets, retries, skipped operations, snapshot and publication modes, converter, heartbeat interval outside 0–10 s), on the Python side and again in the JVM against the effective configuration | orchestrator, from the coordinator's review |
| 2026-11-10 | The apply design takes two findings of the oracle package: DuckDB does not write an `UPDATE` whose new value compares equal to the stored one (`-0`/`0`, `1 mon`/`30 days`), so changed columns of such types need a shape that always writes; and the keyless grouping key must be exact for such types, in the oracle's canonical form (design 6.2, 6.3; open point 3 settled) | orchestrator |
| 2026-11-10 | Tables without a key on a standby whose Postgres has the exported-snapshot defect found by the cut package (every released version today; a session that imports a snapshot can see transactions the exporter does not see while a subtransaction is open): the backfill waits up to 60 s, as a declared wait, for a **quiet** exported snapshot (no transaction open), which is safe; if none comes it reads the table with one reader under its own snapshot; on a server that carries the fix it uses a plain exported snapshot. Reason: "quiet only" never completes while a write transaction stays open on the primary, and one reader for the whole table holds a lock that can stall replay (and the slot) when the primary takes an exclusive lock on that table | orchestrator, on the coordinator's recommendation |
| 2026-11-10 | **Infinite intervals through the stream (source on Postgres 17 or later).** The Postgres driver inside stock Debezium turns `interval 'infinity'` and `'-infinity'` of a scalar `interval` column into a zero interval before any code of ours runs; no configuration changes that. Rule (plan C3, "Ambiguous zero interval"): on such a source a zero in such a column is ambiguous. Before a group that holds one is applied, the stream closes the group on a cut (a snapshot whose place in the stream is known), reads the affected rows in that snapshot and takes the value found; the group is still one destination transaction, and the value is the source's value at the group's end, so no state is shown that the source never had (SPEC 1.3) and no value is refused (SPEC 2.4). The column takes its wide form `UNION(INTERVAL, VARCHAR)` when an infinite value appears. A table without a key that has such a column on such a source is served by full refresh, with an alert. Not taken: applying whatever the row holds at the time of the read (it can mix two source transactions in one visible row). **Built in the type wave of P4**; in P3 the transform names the columns, the stream counts the marked rows, alerts once per table and applies the delivered zero, and leaves the place for the repair in its group loop. Until then such a value arrives as zero, with an alert | orchestrator (the coordinator's variant "take the newer value and count it" was not taken) |
| 2026-11-10 | An interval whose hour part exceeds 2^31 stops the engine at the same transaction on every start (the driver cannot parse it). The stream finds the table by restarts that leave candidate tables out of the engine's table list (halving; such restarts apply nothing and confirm nothing), quarantines it (SPEC 4.0) and continues with the others. `FATAL` names the position of the failing event, never a value | orchestrator |
| 2026-11-10 | A silent engine is found by three rules, not two: no frame at all for `stream.frame_silence` (default 30 s) while the stream reads and no commit or declared wait is in progress ends the engine; the slot-continuity check; and the marker rule of SPEC 4.6, counted only while no transaction is open. Measured: on a black-holed connection the engine's own frames stop after about 8 s | orchestrator, with the coordinator |
| 2026-11-10 | From the review of the source package. (1) No fact that SQL can read identifies one incarnation of a replication slot; continuity rests on position alone, and a recreated slot that is not ahead gets an alert and no backfill (design section 8, open point 6 closed for identity). (2) A standby's timeline is read with `IDENTIFY_SYSTEM` over a replication connection; `pg_control_checkpoint()` can lag behind a switch. (3) A slot is invalid when `wal_status = 'lost'`, or `invalidation_reason` is set, or `conflicting` is true (Postgres 16 shows a slot invalidated by the WAL cap only as `conflicting`). (4) Slot creation on a standby without the snapshot grant completes within about 25 s; its deadline is 120 s. (5) Primary mode: a catalog observation is usable for a group only if it sees every transaction of the group; a transaction that holds only DDL carries no row, and one still waiting on a synchronous standby can be seen one group late — a stated limit of primary mode. (6) Publication membership is decided when a transaction is decoded: a transaction that wrote a table before it joined the publication and committed after is delivered with all its rows | orchestrator, from the coordinator's review |
| 2026-11-10 | **A lower flush position and the slot (design open point 14), measured.** A status update with a flush position below the slot's `confirmed_flush_lsn` moves the slot back to that position on Postgres 16.6 and does nothing on 18.6. Postgres fixed it in 13.22, 14.19, 15.14, 16.10, 17.6 and 18.0 (16.10 read in its release notes; the others taken from the back-patch). The attach rule does not depend on the fix: the slot is read before the engine starts; our engine sends no flush position before its first `DURABLE`; the pipeline sends no `DURABLE` before the attach decision and none below the applied position of that decision. Stated limit, on a server without the fix only: a foreign replication client that streams from our slot, confirms a higher position and then a lower one is not found by the slot check (design section 8, hazard H-035). Carried: a preflight warning for such a server (P4), the runbook rule "nothing else streams from our slot" (P5), the provider's minor release (stage 2) | orchestrator |
| 2026-11-10 | The engine's 14 version-dependent tests were repeated on Postgres 16.6: 13 give the same result as on 18.6; the fourteenth is the slot test of the row above, which now asserts the behaviour of the server it runs on (`docs/design/engine.md` section 13) | orchestrator |
| 2026-11-10 | The database name of a source is treated like a credential in code: it is kept out of `repr` and out of every error text of the source package (found in the review of P; the stage 2 name must never reach a log or the repository) | orchestrator |

## Questions for the owner / owed by the owner

- **Boundary guard**: not enforcing in this session, and the hourly audit does not see it. The build follows PLAN §4
  by instruction. If the owner wants the guarded session instead, start it through the launcher; it resumes from
  this file.
- **MotherDuck account**: Flights and `cdc_v2_*` databases are created under the owner's own account (PLAN §9), not
  under the service account. Say so if the service account should be used instead.
- **One MotherDuck account, one server instance.** Measured on a Flight: a heavy query from any other session of
  the same MotherDuck user made the Flight's server-side work 4–5 times slower. Two consequences. (1) Because the
  build runs under the owner's own account, its test and fuzz campaigns share the server instance with everything
  else the owner runs under that account, in both directions. (2) Performance numbers (SPEC 3.1, 5.1–5.3) are only
  valid when the account is otherwise idle; the build makes its own workers idle for such runs but cannot see the
  owner's other sessions. A dedicated service account for the pipeline removes both; SPEC already expects one in
  production.
- **SPEC 1.3 and a re-backfill after a gap (design open point 15).** SPEC 1.3 names one exception to "one source
  transaction is one MotherDuck transaction": a table in its *initial* backfill. SPEC 3.6 says that after a
  lag-triggered re-backfill consumers keep the last complete version of each table until its swap. Both cannot
  hold for several tables that are re-backfilled after a gap unless all of them swap in one transaction, which
  would make every table wait for the largest. Working rule (see Decisions): each table swaps on its own and is
  treated like a table in its initial backfill until then; the table-status log shows the window. Say so if the
  tables of one re-backfill must instead swap together.
- **SPEC 4.2 "a newcomer exits within 10 s" (design open point 17).** The pipeline can only bound the time from
  the first line of its own `main.py` to the exit. The platform's own start of a run took up to 21 s in the spike
  (p95 7 s) and is outside our control. The criterion is measured from the first line of `main.py` unless the
  owner says otherwise.
- **Identity of a replication slot (SPEC 1.8).** The design compares stored facts of the slot with the live slot
  to find a slot that was dropped and recreated under the same name. Postgres may not expose anything that tells
  the two apart when the new slot is created at a position the pipeline has not passed yet; package P tests this
  on each version. If no such fact exists, a recreated slot that is not ahead of the applied position is treated
  as continuous (nothing can have been lost: every change after the applied position is still delivered) and gets
  an alert; a slot that is ahead triggers the re-backfill as SPEC 1.8 says.
- `SLACK_WEBHOOK_URL` and the alert channel: not provided; alerts are tested against a local stub until then.
- MotherDuck plan and region: measured in P2 — Business plan, us-east-1, maximum run time 8 h
  (`docs/spike/flight_platform.md`). No longer open.
- Stage 2 values of PLAN §8 (PlanetScale service token, branch, hosts, admin role, TLS mode).
- **Billing of short runs.** MotherDuck documents no billing granularity or minimum charge per run. The cost of
  the every-minute schedule above assumes per-second billing; with a minimum of one minute per run it would be
  $14.40 a day. The owner accepted the cost of the short runs in advance (R18); the number needs an invoice or a
  word from MotherDuck to be confirmed.
- **Streaming rate of stock Debezium against the 100K rows/s target (R5, SPEC 5.1, decision D3).** On an F16 Flight
  with the fixture Postgres on the same 2 vCPUs: 24–26K rows/s for a 1M-row transaction, 30K for a 10M-row
  transaction, 10K one-row transactions per second from a backlog. On the laptop against the local replica (first
  indication, not a performance result): 77–84K rows/s inside large transactions, 48–58K one-row transactions per
  second from a backlog, 57K rows/s for updates of a `REPLICA IDENTITY FULL` table. The limit in both places is
  Debezium's single streaming thread (11 µs of CPU per row on the laptop, 30–40 µs on the Flight); batch sizes,
  queue sizes, heap and the acknowledgement round trip did not move it. Not a contradiction of SPEC (D3 expects the
  case), so the build continues; what an F16 reaches with the source outside the Flight is the first measurement of
  stage 2, and the tuning work and its log belong to P9. The owner should expect 5.1 to be reported as measured,
  below the target, unless tuning finds much more than the 10 % found so far.
- **Recovery after a restart behind a large transaction (SPEC 5.2: under 30 s again within 120 s).** Nothing inside
  a transaction can be confirmed to the slot. An engine that starts while a large transaction is received but not
  yet applied makes the server decode it again before anything arrives: 36 s for 10M rows locally (2.6 GB spilled on
  the node that holds the slot), before the apply. A hand-over or failover that falls behind such a transaction
  therefore cannot meet the 120 s; working rule: such a recovery is logged with the size of the transaction and
  reported separately, and it is **not** removed from the 5.2 statistic.
- **A replication connection that goes silent (SPEC 5.2 "max ≤ 60 s", SPEC 4.6).** When the network path of the
  replication connection black-holes, nothing fails: the server ends the silent walsender only after
  `wal_sender_timeout` (60 s by default), and the marker rule of SPEC 4.6 allows 60 s before the connection is torn
  down. Noticing therefore takes 60–90 s and recovery about 70–100 s in all, so the latency of that one event is
  above 60 s by construction, and "back under 30 s within 120 s" is met only narrowly. Working rule: such an event
  is reported with its cause and counted in the 5.2 statistic as it is; if the owner wants it noticed faster, the
  marker interval of 4.6 must be shortened (cost: more heartbeat writes on the source).
- **"≥ 10,000 injected faults" (SPEC §4, fault fuzzer).** Working rule: a fault counts only when its effect was
  observed (the process died at the point, the proxy held or cut an open connection, a backend was terminated); an
  injection that hit nothing is logged and not counted. The bar is claimed only when every kind of fault the system
  supports has at least 100 counted injections, so that 10,000 cheap faults of one kind cannot pass for the
  campaign.
- **A table that is dropped and created again, and a rename (SPEC 1.9), in P3.** The destination follows the
  source's names. After a drop and recreate the old destination table is dropped (or renamed under `on_source_drop:
  retain`) and the name is absent at the destination until the new table's first snapshot is complete. A rename is
  treated as a drop and, if the new name is configured, a fresh table with a new backfill. Renaming in place belongs
  to the schema wave of P4.
- **Postgres 17 locally.** Homebrew has 16 and 18 installed here; the facts that differ by version are run on both.
  The coordinator asks for 17 as well (infinite intervals exist from 17; some slot columns start there). Nothing
  more is installed without the owner's word; say so if `postgresql@17` should be installed. Otherwise 17 is covered
  only if the stage 2 instance runs it.
- **Slot creation on a standby and the admin role.** A slot on a standby needs a running-transactions record from
  the primary. Unaided it took 0.95–8.1 s locally; with `pg_log_standby_snapshot()` on the primary 0.7–0.9 s, but
  the local role that is shaped like a provider's admin role was refused that function (only a superuser may call it
  unless it is granted). The pipeline does not depend on it; stage 2 checks whether the provider's admin role may
  call it.
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
- **Other sessions on the replica can stall the stream (replica mode, SPEC 5.2).** Measured locally: when any
  session on the replica holds a lock on a table (a long report query is enough) and the primary takes an
  `ACCESS EXCLUSIVE` lock on that table, replay on the replica stops for up to `max_standby_streaming_delay`
  (default 30 s), and the slot on the replica delivers nothing during that time. The pipeline's own reads avoid
  it (short chunk transactions); it cannot prevent it for other users of the same replica. Working rule: the
  pipeline detects and reports such stalls (replay lag against the heartbeat), the runbook recommends a replica
  that long queries do not use, and a latency window that contains such a stall is reported separately but **not**
  removed from the 5.2 statistic. Stage 2 must check what PlanetScale sets for `max_standby_streaming_delay`.
- **Resume bound for tables without a key (SPEC 3.7).** A table without a key must be read under one snapshot
  (a change cannot be attributed to a chunk, and replaying a change twice changes a row count). If that snapshot
  is lost (crash, hand-over), the table is read again from the start. So "source rows read ≤ 1.1 × table size" is
  met for tables with a key only; a table without a key is correct after a resume but can be read twice. If 3.7 is
  graded on a keyless table this costs points; the alternative found so far is no better (a snapshot cannot outlive
  the session that holds it).
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
- **Two Postgres defects found by the cut package.** (1) On a standby, a session that imports an exported snapshot
  can see transactions that the exporting session does not see while a subtransaction is open on the primary
  (reproduced: 771 wrong cuts of 53,951; fixed upstream in commit `74c9832ca610`, not released; expected in 16.16,
  17.12, 18.7 — to be re-checked when they appear). (2) A standby that has just started can hide a committed
  transaction, whose commit record it has already replayed, from snapshots until it replays its next commit or abort
  (reproduced three times; argued from the source; not reported upstream). The pipeline guards against both
  (Decisions, and the settle rule of the cut). **For the owner:** should the second be reported to the Postgres
  project, and by whom? Nothing is sent anywhere without the owner's word.
- **Infinite intervals (source on Postgres 17 or later).** Stock Debezium cannot deliver `interval 'infinity'`
  through the stream: the value arrives as a zero interval (Decisions). The rule decided makes the value exact
  by a read at a known place in the stream, built in P4; a table without a key that has an `interval` column on
  such a source is served by scheduled full refresh instead of the stream. Until P4 the stream alerts and applies
  the zero. Say so if such tables should rather be refused, or if the read is not wanted on the source (it is
  one short statement per group that holds a zero interval, on the node the pipeline already reads).

## To verify in stage 2

(Collected as stage 1 proceeds.)

- Everything marked *(Flight)* in SPEC §3 with a real source: 3.1, 5.1, 5.2, 5.3, the 24 h soak.
- Replica decoding on PlanetScale (PLAN §2 B 6), failover behaviour of both modes (4.1), privileges of the runtime
  role on a managed provider, TLS `verify-full`, client address restrictions.
- The nine points of `docs/research/replica_decoding.md` §10 ("What stage 2 must test on PlanetScale"), first of
  all: whether one replica can be addressed deterministically and accepts replication-protocol connections, and
  whether WAL retained by a slot can trigger storage autoscaling (**a cost change — baseline first, PLAN §5**).
- CPU per row of Debezium's streaming thread on an F16 Flight with the source outside the Flight (the local 11 µs
  against the 30–40 µs measured with the fixture Postgres on the same 2 vCPUs); a run on the locked JDK 21 (the
  local runs used the machine's OpenJDK 23).
- Whether the provider's admin role may call `pg_log_standby_snapshot()`, and how long a slot creation on the
  replica takes without it.
- The engine reads the slot's holder over an SQL connection of its own. Behind a provider's proxy or pooler that
  connection must reach the same server as the replication connection (hazard H-047); show it on PlanetScale, in
  both modes.
- The engine tests that depend on the server version (a lower flush position and the slot; the holder pair;
  infinite intervals) on the version the provider runs. First of these: whether the provider's minor release has
  the guard that keeps `confirmed_flush_lsn` from moving backwards (13.22, 14.19, 15.14, 16.10, 17.6, 18.0).
- PlanetScale's documented slot policy (slots without `failover = true` are flagged and planned failovers held back
  for 3 days): does a slot on a replica trigger it?
- Whether the runtime role may call `pg_control_checkpoint()` on the provider's servers (the cut uses its `next_xid`
  to tell a standby that has just started; the pre-flight check tests it).
- The engine's JVM tests on the locked Temurin 21 (the lock has an entry for Linux only, so the local runs used
  the machine's OpenJDK 23).
- The columns of `pg_replication_slots` that `slot.facts` reads on Postgres 17 (read from the documentation; 16 and
  18 are tested), if the provider runs 17.
- Whether the runtime role may read the timeline history over a replication connection on the provider
  (`slot.timeline_history`; it may on local 16 and 18).

## Background jobs in flight

Each in its own worktree and branch; the orchestrator merges. The P3 packages are in the table above.

| Job | Phase | Started | Delivers |
|---|---|---|---|
| Package D, destination basics (Fable, xhigh). MotherDuck databases `cdc_v2_t_d_*`. No Postgres | P3 | 2026-11-10 | `src/cdc_v2/md/`, `src/cdc_v2/state/`, `src/cdc_v2/alerts.py`, `docs/design/destination.md`, `docs/state_machines.md`, `evidence/p3/destination/` |
| Package A, apply (Opus, high): **delivered**, branch `worktree-agent-adbee9c1aec8e2051`, not merged. MotherDuck databases `cdc_v2_t_a_*` | P3 | 2026-11-10 | `apply_group`, `truncate`, `reduce_duplicates`, the statement shapes for tables with and without a key, `docs/design/apply.md`, tests against the oracle's canonical form, cost numbers on MotherDuck. Owed: the fix round below |
| Second review of package A (Fable, reads only): looks for a counterexample to the row-version model and the catch-up rules | P3 | 2026-11-10 | findings for A's fix round |
| Package F, fuzzers (Opus, high). Homes `pg-f<n>`, ports 55700 + 8·n; MotherDuck databases `cdc_v2_t_f_*` | P3 | 2026-11-10 | the four fuzzers with seeds and replay, the consumer loop of SPEC 1.3 and 3.2, their self-tests with planted defects |
| Package X, fix round (Fable; the worker that built the cut). Home `pg-cut`, ports 55600–55607. No MotherDuck | P3 | 2026-11-10 | the items listed below; then the coordinator checks it again and it is merged |
| Package Y, transform (Opus, high). Home `pg-y`, ports 55656–55663. No MotherDuck | P3 | 2026-11-10 | `src/cdc_v2/transform/`, `docs/design/transform.md`, the cross-path test (stream form against backfill form through a local Postgres), the list of ambiguous interval columns and their mask |

**Package A, apply — delivered 2026-11-10, coordinator's verdict: merge after a short fix round.** What the
worker owes before the merge:

- The default test run of `tests/apply` is too slow (14 s): a small set of named cases and few property examples
  by default, the rest behind a switch.
- `evidence/p3/apply/campaign_local.json` is 3 MB: keep the summary, the failing and regression seeds and the
  command; drop the per-seed ledger.
- The private planning database spills to DuckDB's default directory without a limit when no directory is given:
  the settings take the pipeline's spill area and a size limit, and refuse to run without them outside tests
  (SPEC 5.4).
- A negative control for the property tests and the campaign: the same properties against a deliberately wrong
  planner ("the last record per key decides"; plain `GROUP BY` for keyless tables) must fail.
- The test simulator must not parse types with the package's own parser.
- After D merges: the MotherDuck tests once through D's real lane transaction.

What the review changes in the design and the contracts (folded in at the merge): a key is a queue of row
versions, because a deferred unique constraint lets one transaction hold two rows of one key for a while ("the
last record per key decides" is wrong there); the rules for unchanged values and for unresolved keys in mode
`catch_up`; exact grouping for keyless tables; the planning schema that lives inside one lane transaction; the
calls apply needs from D's transaction. Limits accepted for P3 and carried: an unchanged TOASTed key column and
the remaining ambiguity under a deferrable key both need `REPLICA IDENTITY FULL` on such tables (owner's question
with F-7, P4); no pruning by key for some key types (measured in P9).

**Package E, the engine — merged 2026-11-10** (`11599e6`; JVM program `jvm/src/cdc_v2/engine/`, the Python side
`src/cdc_v2/stream/{frames,engine,jvm}.py`, `tests/engine/`, `docs/design/engine.md`, `evidence/p3/engine/`).
Both reviews said "merge after fixes"; two fix rounds followed, and the orchestrator read the last one line by
line before the merge. What had to change:

- From the second review (acknowledgement path). The engine never confirmed more than it was told was durable, in
  every run. But a heartbeat could carry the commit position of a transaction not delivered yet (loss under the
  earlier contract; now only END records are acknowledged, see Decisions), and the bounded list of unconfirmed
  records could stop the slot from following while the pipeline was more than about 24,000 transactions behind
  (no loss, but WAL is kept and a restart decodes the backlog again): the list gets a stated bound. The restart
  test did not catch a planted over-acknowledgement; it gets a lagging pipeline and a kept negative control.
- From the coordinator. Extra properties could override settings that carry a guarantee (now refused, on both
  sides). `HELLO` was sent although no backend held the slot (now `FATAL`). The holder probe fell back to
  `localhost` and port 5432 (now refused). `FATAL` gets the position and the table and never a source value.
  Values the driver cannot parse (infinite intervals, very large intervals) get a test of what the engine writes
  before it stops and whether leaving the table out lets the stream pass.
- Smaller: the hermetic tests take 16 s (to be cut); three missing cases of the event count; proxy ports through a
  parameter of the harness; the tests owed on Postgres 16 read their binaries from the environment so that they can
  be repeated once `tools/localpg` runs on 16.

The fix round of package E (finished 2026-11-10; every item done except one run on the Java version the Flight
will use — the lock of the Java runtime has an entry for Linux only, so that run is left for the Flight):

- Settings that carry a guarantee are refused as extra properties in Python and checked again by the Java side at
  start. No `HELLO` without a holder of the slot. No default host or port anywhere. `FATAL` carries positions and
  never a source value (messages only from an allow-list; SQL errors reduced to their state code).
- Only `END` records are held and confirmed; a `HEARTBEAT` frame has no position to acknowledge. Test: 40,000
  single-row transactions and 2,000 message-only transactions at a 1 ms heartbeat with three kills — the slot was
  never above the last durable position, nothing missing, nothing doubled. A pipeline that confirms one `END` too
  early is kept in the suite as a negative control and is caught.
- Stated bounds: on an idle source the slot reaches the last durable position within the heartbeat interval plus
  1 s (measured 0.3 s); while the pipeline is behind, a durable position confirms an `END` at most k − 1 records
  below it, with k ≤ max(1, 2 × lead / 4096) (60,000 transactions behind: within 15 records, in 0.7 s).
- An interval above 2^31 hours ends the engine 1.4–1.5 s after every start, at the same event; the engine cannot
  name the table (the failure is in Debezium's decoding thread), but it passes the transaction once the table is
  left out. **New finding: `interval 'infinity'` and `'-infinity'` (Postgres 17 and later) do not fail — the
  Postgres driver turns both into a zero interval, silently.** The rule is in Decisions and in plan C3; until P4
  builds the repair, the stream alerts and delivers zero for these two values.
- On a black-holed connection the engine's `HEARTBEAT` frames stop after about 8 s; the server ended its sender
  after about 60 s. The stream's task is corrected for this and for the two points above.

The second fix round (three items from the coordinator's re-check, all done):

- The bounded list of unconfirmed `END` records now keeps its oldest entry when it thins, so the stated bound
  holds for every list size from 2 up (a property test over random sizes; the earlier thinning broke it 580 times
  in the same test).
- Three more settings are required and checked in the JVM (`record.processing.order`, `include.unknown.datatypes`,
  `schema.refresh.mode`), and every setting that filters, renames or rewrites records is refused as an extra
  property (column filters, transforms, predicates, key overrides, table and schema lists other than our own).
- `FATAL` names the position (`lsn` and transaction id) of the event that Debezium failed on, when Debezium names
  one. The stream compares it across restarts to tell "the same event again" from a new failure.

Its findings that reach other packages (now rules in the contracts and the tasks):

- Debezium sets the application name of its replication connection to a fixed text; the engine is recognised by
  the holder's process id and backend start time (contract C2).
- Neither Debezium nor the Postgres driver notices a black-holed replication connection (not within 150 s); see
  Decisions for who notices it.
- Debezium's own heartbeat does not advance while the WAL holds only changes of tables outside the publication; an
  idle slot needs a heartbeat that the publication carries (package P).
- The END record's `event_count` counts rows, one TRUNCATE record per table and transactional logical messages; a
  key change counts two.
- Local rates (indications): 59.7K rows/s at 1,000 rows per transaction, 64–71K rows/s in a 1M-row transaction,
  15.9K one-row transactions per second; our own handler costs 1.2–1.3 µs of the 28 µs per row.
- Not run on Postgres 16 yet: the 14 version-dependent tests take their binaries from the environment and are
  repeated by the orchestrator now that `tools/localpg` runs on 16 (next steps).

Finished, not merged yet: **package X, the cut** (branch pushed; `src/cdc_v2/pg/cut.py`, a `pgoutput` decoder and
stress tools for tests, `tools/cutstress`, `tests/cut/`, `docs/design/cut.md`, `evidence/p3/cut/`). Reviewed by the
coordinator: merge after fixes. What it shows (Postgres 18.6; 16 and 17 by reading the source):

- On a standby the rule of design 7.2 holds with the reader's own snapshot: a model check of every interleaving of
  small histories; 1,854,463 cuts over 7.3 million transactions in the main campaign and 2,244,033 cuts in twelve
  further runs (subtransactions, two-phase commit, aborts, DDL, WAL switches, replay pauses, apply delay, 28 standby
  restarts or kills), no violation; six wrong rules planted as a negative control were all caught. The reviewer
  confirmed that the checker decides from independent ground truth.
- Three corrections to 7.2: a standby that has just started can hide a committed transaction (a cut is used only on
  a settled standby: the snapshot's `xmax` is at or above `next_xid` of `pg_control_checkpoint()`); `R0 ≥ P0` must
  be checked for every snapshot, also on the slot's own standby (after a restart it answered from a replay position
  below what the slot had delivered); an exported snapshot on a standby is unsafe on every released version while a
  subtransaction is open (see Decisions and Questions).
- On a primary the answer by 64-bit transaction id is exact (197,212 cuts, 110 million answers, none wrong). The
  coordinator's earlier proposal (`pg_xact_status`) is refuted, and the coordinator agrees; in place of `R0 ≥ P0`
  the stream keeps the ids it delivered that none of its own snapshots has seen yet, and a snapshot is used only if
  it sees all of them.
- What must change before the merge: the two runs with exported snapshots were made before the checker looked for a
  row missing from a snapshot, so they are repeated (the rule for tables without a key rests on them); the proposed
  design text used the start of the commit record, which stock Debezium does not deliver (removed; by transaction id
  alone about two of three cuts under load have no undecided transaction); the settle check must not be optional;
  positions and deadlines go through the shared modules; a cut records its timeline.
- Not examined, by its own account: streaming of open transactions, `two_phase`, cascading standbys, timeline
  switches; a new standby made from a base backup is argued, not measured.

Written by the orchestrator for P3: `docs/design/p3_plan.md` — the packages of the core (engine, destination
basics and leadership, apply, source, stream, fuzzers, backfill and hand-off, truncate/drop and the machines), the
contracts between them (engine frames in a column-oriented layout, change batch, destination lanes, transition
function, platform interface with a local simulator, source interface), private ports and databases per package,
and the review rule. The next workers start as writer slots become free.

Written by the orchestrator while the spikes ran: `docs/design/core.md`, the draft contracts of the core
(position, commit and acknowledge, leader/fence/heartbeat rows, bootstrap, apply, snapshot and stream, slot
continuity, state machines, modules, open points). P3 workers build against it. Changed on 2026-11-10 after the
throughput spike (section 7): chunks of a keyed table are read under their own snapshots in short transactions;
a row that no chunk read and whose change carries unchanged large values is fetched by key (repair read); a key
change that carries unchanged values takes them from the row with the old key; the swap waits until the stream
has passed every snapshot used; the cut is stated in commit end positions; the cut on a primary is an open point
for the cut worker.

Finished and merged:

- 2026-11-10 — P3 package P, the source (reviewed by the coordinator: merge after one fix, done):
  `src/cdc_v2/pg/` (`settings`, `conn`, `errors`, `catalog`, `publication`, `replica_identity`, `slot`,
  `timeline`, `heartbeat`, `preflight`, `sql`), `docs/design/source.md`, `tests/pg_source/` (18 live tests, about
  10 minutes, on Postgres 18 and 16), `evidence/p3/source/`; `tools/localpg` now builds a pair on the Postgres 16
  binaries. Every call has a deadline that closes the socket; read connections put their session settings into
  the startup packet and read them back; a recovery conflict on the standby is its own error ("read again").
  The slot is created, read and — only with the expected holder pair — taken from another holder; nothing drops
  a slot implicitly. The heartbeat is a row in a published table written on the primary, and the `END` that
  carries it proves that it was decoded. Role split: a setup role (publication, heartbeat table, replica
  identity) and a runtime role that cannot change the source's schema. Findings that reach other packages are in
  Decisions (slot identity, timeline, slot invalidation on 16, slot creation time, the primary-mode catalog rule,
  publication membership). The review asked for an evidence run of the primary-mode finding (the package reported
  12 cases of a lower-bound violation without a file). Done 2026-11-10: 40 repeats found none, so the 12 cases
  are not evidence; a new test holds a commit between its WAL record and its visibility and shows the rule on 18
  and 16 (`docs/design/source.md` section 7). Still open from the review: two test-support clean-ups.
- 2026-11-10 — P3 package O, the oracle and the T1 harness (reviewed by the coordinator, fix round included):
  `src/cdc_v2/oracle/`, `tools/oracle`, `docs/oracle.md`; T1 harness
  `src/cdc_v2/testing/{pg,md,loader,datasets}.py` and the shared fixtures in `tests/conftest.py`; redaction
  library `src/cdc_v2/evidence.py` with one writer of
  evidence files (`cdc_v2.testing.harness_evidence.write_evidence`); seeded workload generator and transfers
  observer; `evidence/p3/oracle/`. A comparison is taken at a stated stream position (`compare(..., position=Q,
  slot_name=…)`: the source snapshot is at or past Q and the destination's applied position is read in the same
  read transaction); a comparison without a position must say so and is not a gate result; a comparison that
  compared nothing raises. Fixtures take their cluster and MotherDuck prefix from the environment; destructive
  commands on the default home are refused; every MotherDuck statement has a deadline. Numbers: default run 309
  passed, 1 skipped; 88 T1 tests; 60 planted differences all detected; an equal 5M-row table compared in 23.6 s
  (211,500 rows/s), one changed value in it found in 77 s, 172 MB peak. Findings that changed the design (now in
  `docs/design/core.md` 6.2, 6.3 and open point 3, and in the task of package A): DuckDB does not write an `UPDATE`
  whose new value compares equal to the stored one (`-0` stays after `SET 0`, `1 mon` after `SET 30 days`);
  `GROUP BY` and `PARTITION BY` merge such values, so the keyless apply needs an exact grouping key; a source NULL
  in a UNION column must be written as a real NULL; DuckDB reads a Postgres BC date text as an AD date and float
  texts differ between the two servers (both go to the transform package). After the merge the harness test of
  the TCP proxy against Postgres was changed to take its cluster from the same fixture. The default test run now
  takes about 50 s (to be cut). The coordinator checked the four must-fixes again on `main`: all closed; one hole
  found and closed (a named home that was not built yet, given without a port base, would have been built on the
  default home's ports — the fixtures now refuse it). The same re-check found the rule "only `END` records are
  acknowledged" sound; the sentences that still relied on a heartbeat position are corrected (plan 6.2, tasks of E,
  P, S), and C2, C8 and C10 gained one rule each (an `END` without `lsn_commit` is `FATAL`; the table list and
  standby reads of a gate comparison; every reposition is followed by a new engine).
- 2026-11-10 — Local streaming rate of stock Debezium on the replica's slot (§2 A 5, part 2):
  `spikes/local_throughput/stream_*.py`, `spikes/local_throughput/java/FactsMain.java`,
  `docs/spike/local_streaming.md`, `evidence/p2/local_streaming/`. **Indications only, laptop numbers.** One
  transaction of 1M / 10M rows: 78K / 84K rows/s; keyed updates 83K, deletes 106K; `REPLICA IDENTITY FULL` updates
  57K; a backlog of one-row transactions 48–58K per second; live load of 500–8,000 transactions per second kept up
  with a delay of p99 ≤ 0.95 s (the poll interval dominates). Streaming thread 11 µs per row, walsender 7–8 µs;
  replica and primary do not differ. A 10M-row transaction spills 2.6 GB on the node that holds the slot; a restart
  inside it delivers the first row after 36 s and then exactly 10,000,000 rows. Position fields mapped to the
  server's own stream (END carries the commit end position and an exact count; change records carry the previous
  transaction's). **The last transaction before an idle period is never confirmed** unless
  `offset.flush.interval.ms=0` and a heartbeat interval are set. Slot creation on a standby: see the questions for
  the owner. The private cluster is stopped; its data (6.5 GB under the cache directory) is kept until the P3 gate
  for repeats.
- 2026-11-10 — P3 package H, the crash-point and fault harness: `src/cdc_v2/crashpoints.py`,
  `src/cdc_v2/faultsites.py`, `src/cdc_v2/testing/{procs,crash,netproxy,pipefaults,faults,harness_evidence}.py`,
  `tools/netproxy`, `tests/harness/`, `docs/testing/harness.md`, `evidence/p3/harness/`. Named crash points with
  seeded actions (kill, kill later, freeze, hang), run-time fault sites, process control for competing runs, a
  crash runner with a ledger, a TCP fault proxy (ports 55440–55459) and a byte-stream relay for the engine's pipe.
  Proven against a toy system that is correct and one whose commit protocol is deliberately wrong (the harness
  finds the wrong one). Finding used by later packages: a black hole on the path to Postgres is noticed only by a
  client-side deadline (2.0 s in the test, which is the deadline); TCP keepalives and the server's
  `statement_timeout` do not help, so every source call carries a deadline that closes the socket.
- 2026-11-10 — Local read throughput, part 1 (§2 A 5): `spikes/local_throughput/`, `docs/spike/local_throughput.md`,
  `evidence/p2/local_throughput/`; dependency group `spike` (`psutil`, `pyarrow`) in `pyproject.toml`. TPC-H SF2
  `lineitem` (12.0M rows, 2 GB) on a private cluster; **indications only, laptop numbers**. Read rate from the
  replica: one connection is bound by its Postgres backend at about 1.0–1.1M rows/s on a `ctid` scan whatever the
  format; our `COPY` over `ctid` ranges parsed into Arrow gives 0.67M / 1.31M / 2.47M rows/s with 1 / 2 / 4
  connections; ranges over the primary key cost twice the backend CPU; DuckDB's Postgres reader is 20–40 % faster
  on the client but holds every connection in one transaction for the whole scan and reports no progress per chunk.
  End to end into MotherDuck from the laptop: 474K–586K rows/s, limited by the upload. Estimate for an F16 Flight
  (not measured): 0.65–0.95M rows/s for our reader. The 100K rows/s of SPEC 3.1 has a wide margin; the real number
  is a stage 2 measurement. **Decision for the backfill**: our own parallel `COPY` over `ctid` ranges from the
  replica, two connections on F16, every chunk in its own short transaction; DuckDB's reader stays as the reference.
  **Hazards found** (`docs/spike/local_throughput.md` §8; written into `docs/design/core.md` 7.1 and into the P5
  list): any `ACCESS EXCLUSIVE` lock on the primary (`ALTER TABLE`, `TRUNCATE`, `VACUUM FULL`, … and an ordinary
  vacuum that truncates empty pages at the end of a table) cancels a read on the replica that has held a lock on
  that table for `max_standby_streaming_delay` (30 s), and **stops all replay on that replica for those 30 s**. In
  replica mode the slot decodes replayed WAL, so the stream stalls too. With chunk transactions of well under a
  second under a snapshot holder that holds no lock: no cancellation, replay lag ≤ 0.22 s over 625 chunks. A
  snapshot held on the replica holds back cleanup on the primary (`hot_standby_feedback`). A table rewrite during
  a chunked read is detected by comparing `pg_relation_filenode()` per chunk.
- 2026-11-10 — MotherDuck semantics repeated inside an F16 Flight (§2 A 3, second part): `flights/spike_md/`,
  `spikes/md_semantics/{apply_txn,flight_extract}.py`, the "On an F16 Flight" part of `docs/spike/md_semantics.md`,
  `evidence/p2/md_semantics/flight_r4_*` (run 4 is the uncontended run; runs 2–3 are kept and labelled as
  contended). The spike Flight is deleted; no Flight, secret or spike database is left. What it settles: a round
  trip from a Flight is 8 ms (80 ms from the laptop) and the Flight's clock agrees with the server within 4 ms (the
  single-clock rule stays); a complete apply transaction of 1–100 rows takes 0.066 s; apply reaches 212K rows/s at
  100K-row groups and 342–355K rows/s at 1M-row groups on one connection; bulk load of real `lineitem` rows by
  `COPY` from Parquet reaches 1.58M rows/s; more than two loader processes never help (2 CPUs). The applier is
  limited by round trips for small groups and by client upload and CPU for large ones, not by MotherDuck. Design
  consequences, written into `docs/design/core.md` 6.1: groups are applied directly from local Arrow data, staging
  tables in MotherDuck are used only for a transaction larger than a group; Arrow is handed over in bounded
  batches. **All sessions of one MotherDuck user share one server instance** (see the questions for the owner).
- 2026-11-10 — JVM and stock Debezium on a Flight (§2 A 1): `jvm/` (lock files, `EngineMain.java`),
  `src/cdc_v2/artifacts.py`, `flights/spike_jvm/`, `spikes/jvm_on_flight/`, `docs/spike/jvm_on_flight.md`,
  `evidence/p2/jvm_on_flight/` (8 Flight runs). The spike Flight is deleted. What it settles: candidate B works on
  an F16 Flight (Debezium 3.7.1.Final embedded in our own small JVM program, Temurin 21 fetched and verified at
  start, 73 jars / 40 MB verified by SHA-256); a hand-over needs 14.6 s from the platform's start to the first
  change event; the JVM peaks at 0.3–0.8 GB; change frames travel on their own pipe; the JVM must watch its
  parent, or it is orphaned and keeps the slot; a frozen JVM holds the slot for 60 s (`wal_sender_timeout`).
  **Streaming rate: 24–30K rows/s at `lineitem` width, with the fixture Postgres on the same 2 vCPUs** (see the
  questions for the owner).
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
- 2026-11-10, one rule added by the JVM spike worker: `^(2B\d/)?OpenJDK\d\dU-jdk_x64_linux_hotspot_\d\d$`. It
  matches the file names of the Eclipse Temurin JDK tarballs in `jvm/jdk.lock.json` (public vendor artifacts); the
  randomness rule flagged them. Anchored at both ends; reviewed by the orchestrator at the merge. The scanner and
  its tests are unchanged.

Also for the owner: commit `aa6a435` contains a pytest evidence file whose header shows a local path including the
operating-system user name of this machine. It is not a credential, hostname, database user name or connection
string; it was redacted in the next commit and stays in history.

Note for the owner: the exact-value layer of the scanner reads the env file only. The token this session uses is not
in that file, so it is covered by the pattern layer (JWT format), by `gitleaks` and by the harness redaction, not by
the exact-value layer.

## Next steps

1. P3, as writer slots become free (at most four; now D, the fix round of X, Y and F): A's fix round at the next
   free slot, with the findings of both reviews in one list; then the coordinator checks it again, A is merged
   and the review's sentences go into design 6.1–6.3, plan C0, C3a, C4 and C10, the tasks of D, Y, S and B, the
   hazard notes (H-015 and a new entry for a TOASTed key column sent as unchanged) and
   `docs/spike/md_semantics.md`. Review and merge D, X, Y and F as they report: the coordinator reviews each, D
   also gets a second reviewer. Then L after D; S after D and A; then V, B, T in the order of the table above.
   When X merges: design 7.1–7.4 and open points 1 and 12 take its text, plan C9 is completed, and the tasks of
   B, S, V and T take the rules of the cut (settle floor, `R0 ≥ P0` for every snapshot, the tracker on a primary,
   timeline).
2. Add to the owner's questions when A merges: `REPLICA IDENTITY FULL` for a table whose key column can be
   TOASTed or whose key is deferrable (more WAL; with F-7); whether the DuckDB join-filter defect that apply
   works around should be reported upstream.
3. Carry into P4: the preflight warning for a server without the confirmed-flush guard. Carry into P5 (from the
   `tools/localpg` work): slot creation on a standby waits for a running-transactions record
   and `pg_log_standby_snapshot()` needs a privileged role; a failover slot is synchronised to a standby only after it
   has advanced past the standby's catalog horizon; raising a standby-matched setting on the primary pauses replay;
   the runbook rules "no other program imports snapshots on our standby" and "nothing else streams from our slot".
