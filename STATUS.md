# Build status

Last updated: 2026-11-10, by the orchestrator. Resume from `SPEC.md`, `PLAN.md` (read §9), this file and `git log`.

## Where the build stands

- **Stage 1 (local). P1 Environment: gate passed on 2026-11-09** (one item cannot be shown from this session, see
  the table). **P2 Platform spike: gate passed on 2026-11-10** — every spike is done and merged, the report
  (`docs/spike_report.md`) is complete and was sent to the owner; nothing found contradicts SPEC, so the build
  continues without waiting. **P3 Core: in progress** (table below): the fault harness and the oracle are
  merged; the engine is finished and reviewed twice, and its round of fixes waits for a free writer slot; the cut,
  the destination basics, the source package and the apply package are being built; the task file of every
  package is written.
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
| X | The cut: where a snapshot lies in the stream | Fable | running |
| E | Engine: the JVM program around stock Debezium, frames, the Python side | Opus | **finished**; both reviews say: merge after fixes (below); the fix round goes to its worker when a writer slot is free |
| D | Destination basics: lanes, fence, deadlines, errors, `_cdc` schema, transition function, alert records | Fable | running |
| A | Apply: the statements over a change set, keyed and keyless | Opus | running |
| P | Source: connections, catalog and its observation, publication, slot, slot facts, timeline, heartbeat | Opus | running; first repairs `tools/localpg` for Postgres 16 |
| L | Leadership: guard, take-over, heartbeat, hand-over, bootstrap, platform interface | Fable | task written; after D |
| Y | Transform: source types to destination types | Opus | task written; after E and P |
| S | Stream: transactions, groups, commit and acknowledge, supervisor, pipeline process | Fable | task written; after E, D, A, P |
| V | Slot continuity | Fable | task written; after S |
| F | Fuzzers (stream, workload, fault, leadership), the consumer loop of 1.3 and 3.2 | Opus | task written; starts when a writer slot is free (H and O are merged) |
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
- The engine tests that depend on the server version (a lower flush position does not move the slot back; the holder
  pair; infinite intervals) on the version the provider runs.
- PlanetScale's documented slot policy (slots without `failover = true` are flagged and planned failovers held back
  for 3 days): does a slot on a replica trigger it?

## Background jobs in flight

Each in its own worktree and branch; the orchestrator merges. The P3 packages are in the table above.

| Job | Phase | Started | Delivers |
|---|---|---|---|
| Package X, the cut (Fable, xhigh). Library, a small `pgoutput` decoder for tests, a stress tool with a negative control, the argument from the Postgres source. Home `pg-cut`, ports 55600–55607. No MotherDuck use | P3 | 2026-11-10 | `src/cdc_v2/pg/cut.py`, `src/cdc_v2/testing/pgoutput.py`, `tools/cutstress`, `tests/cut/`, `docs/design/cut.md`, `evidence/p3/cut/` |
| Package D, destination basics (Fable, xhigh). MotherDuck databases `cdc_v2_t_d_*`. No Postgres | P3 | 2026-11-10 | `src/cdc_v2/md/`, `src/cdc_v2/state/`, `src/cdc_v2/alerts.py`, `docs/design/destination.md`, `docs/state_machines.md`, `evidence/p3/destination/` |
| Package P, the source (Opus, high). Homes `pg-p` and `pg-p2`, ports 55640–55655; the `tools/localpg` self-tests on their own home. No MotherDuck, no JVM | P3 | 2026-11-10 | first `tools/localpg` on Postgres 16; then `src/cdc_v2/pg/`, `tests/source/`, `docs/design/source.md`, `evidence/p3/source/` |
| Package A, apply (Opus, high). MotherDuck databases `cdc_v2_t_a_*`. No Postgres, no JVM | P3 | 2026-11-10 | `apply_group`, `truncate`, the statement shapes for tables with and without a key, tests against the oracle's canonical form, cost numbers on MotherDuck |

Finished, not merged yet: **package E** (branch pushed; JVM program `jvm/src/cdc_v2/engine/`, the Python side
`src/cdc_v2/stream/{frames,engine,jvm}.py`, `tests/engine/`, `docs/design/engine.md`, `evidence/p3/engine/`).
Both reviews say "merge after fixes"; the fix round is written and goes to the worker that built the engine as
soon as one of the four writer slots is free. What must change:

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
- Not tested on Postgres 16 (waits for the `tools/localpg` repair).

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
  takes about 50 s (to be cut).
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

1. P3, as writer slots become free (at most four): the fix round of package E (message written), then package F.
   Review and merge X, D, P, A and E as they report: the coordinator reviews each, D also gets a second reviewer.
   Then L after D, Y after E and P, S after E, D, A and P; then V, B, T in the order of the table above. Plan C9 is
   completed from package X's report when it merges.
2. After P's repair of `tools/localpg` is on `main`: repeat the engine's version-dependent tests on Postgres 16.
3. Carry into P5 (from the `tools/localpg` work): slot creation on a standby waits for a running-transactions record
   and `pg_log_standby_snapshot()` needs a privileged role; a failover slot is synchronised to a standby only after it
   has advanced past the standby's catalog horizon; raising a standby-matched setting on the primary pauses replay.
