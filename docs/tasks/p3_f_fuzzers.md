# Task: package F — fuzzers and the campaign ledger (round 1)

Read `docs/tasks/README.md` first; its rules bind you.

You build the machinery that attacks the pipeline: the workload, fault, stream and leadership fuzzers of the
robustness bar (SPEC §4, R22), the campaign runner that runs them for hours in the background, the ledger that
proves the quantities of the bar, the consumer loops of SPEC 1.3 and 3.2, and the suite of SPEC 1.7. PLAN §6 says
why now: the fuzzers exist before the features they attack.

The pipeline itself (packages S, L, B, V) does not exist yet. So this round builds every fuzzer against a small
interface, and proves each one on a **toy replicator** that has deliberately wrong variants: a fuzzer that cannot
find a planted bug is not finished. A second round, after S and L are merged, connects the real pipeline and runs
the gate tests (1.3 with the 10M-row transaction, 1.7, 4.2 with 100 hand-overs under load and the hung
component). The value fuzzer belongs to the P4 type wave; the ledger has its place for it from the start.

## Read as well

- SPEC §4 "Robustness bar", and items 1.1, 1.2, 1.3, 1.7, 3.2, 4.2, 4.5, 5.4. PLAN §6.
- `docs/testing/harness.md` completely: crash points and fault sites and their environment contract, process
  control, the crash runner with its adapter and ledger, the TCP fault proxy, the byte-stream relay. You use
  these; you do not build a second one of any of them.
- `docs/oracle.md` completely and contract C8: the oracle, the T1 fixtures, the seeded workload generator, the
  transfers observer.
- `docs/design/core.md` sections 1 to 4 and 9, and `docs/design/p3_plan.md` C1, C2, C4, C6, C10, C11 and 6.1:
  what you will attack in round 2, and which names (crash points, fault sites, frames, the platform simulator)
  it will have.
- `docs/wal_hazards.md`, for what is known already. The graders attack with cases that are not in that catalog
  (SPEC §4, last line): your generators must be able to produce shapes nobody listed.
- `docs/localpg.md`: what a local cluster can be told to do (kill, restart, promote, add a standby, settings).

## From the engine as built — build on it

Package E is built and reviewed. `docs/design/engine.md` is the full account; where it differs from what this task
says elsewhere, these points and that document hold.

- The stream fuzzer builds frames with `cdc_v2.testing.frames` (byte-identical to the Java encoder, with a shared
  generator) and injects byte faults with `pipefaults`; it needs no JVM. The decoder stays broken after its first
  error, by design: a test that expects recovery restarts the reader.
- `END.event_count` = rows + `TRUNCATE` frames + transactional `MESSAGE` frames; a key change counts two; an `END`
  with count 0 is legal. `HEARTBEAT` frames may arrive anywhere, also inside a transaction, and never move the
  applied position (plan C2): a generated stream whose `HEARTBEAT` carries a position above the last `END` must
  change nothing. `seq` starts at 0 for every engine life. A generator that breaks one of these on purpose is a
  negative control.

## Resources

- Postgres: homes `~/.cache/cdc_flight_v2/pg-f<n>` with port base `55700 + 8·n`, n = 0 … 11. For the shared T1
  fixtures export `CDC_V2_PG_HOME`, `CDC_V2_PG_PORT_BASE` (one of your homes) and `CDC_V2_MD_PREFIX=cdc_v2_t_f_`.
  The fixture `private_cluster` (leased ports) is open to you as to everyone.
- MotherDuck through `tools/with-env` only, databases `cdc_v2_t_f_*` only. The toy writes to a local DuckDB file
  by default; a run against MotherDuck is an option that your tests use sparingly (the account is shared).
- JVM: only where the stream fuzzer records frames from the real engine, if package E is merged when you get
  there (`src/cdc_v2/stream/engine*`; look, do not assume).
- The machine has 10 cores and 16 GB and other workers build on it at the same time. While you build: at most 2
  scenarios in parallel, no single command above 20 minutes, and not more than about one hour of campaign time
  on the toy in total. The long campaigns are started by the orchestrator later, with your runner.

## What to build

Code under `src/cdc_v2/testing/fuzz/` (the pipeline never imports `cdc_v2.testing`), a command `tools/fuzz` (a
thin wrapper around `uv run python -m cdc_v2.testing.fuzz`), the toy under `tests/fuzz/toy/`.

### 1. The system under test

One interface, `fuzz.System`, that extends the adapter of the crash runner (`docs/testing/harness.md` section 4)
by what the fuzzers need and no more. At least:

- `setup(workdir, seed)`, `teardown()`: an isolated source database and destination per scenario.
- the source: the cluster (primary, standby, how to run `localpg` on it when the scenario owns it) and the
  database; whether the slot is on the standby or on the primary (`source_node`: both modes are in the bar);
- the destination as the oracle takes it, and how an observer opens a connection of its own to it;
- `start(tag, env) -> Proc` for one instance, and, for the leadership fuzzer, the platform that starts and
  cancels runs (contract C6: `LocalPlatform`, when package L delivers it; the toy brings a minimal one);
- the applied-position lookup for `wait_for_applied` and `compare(position=Q)`;
- `crash_points()`, `fault_sites()`, and `supports(kind)` for every fault kind of section 3 and every workload
  feature, so that a system can say what it cannot do yet — and the ledger shows what was therefore not attacked;
- the network links that can be put behind the TCP proxy (pipeline → source at least);
- how to make the spill area full;
- the list of compared tables and the oracle configuration.

Two systems in this round:

**The toy replicator** (`tests/fuzz/toy/`). A real, small, crash-only replicator that runs as its own process: it
reads a logical slot of a local cluster and applies whole source transactions to a DuckDB destination, with the
applied position stored in the same destination transaction, and it confirms to the slot only what is durable.
It has a leader epoch and a fence of its own (so that several instances can compete), crash points after every
step that changes durable state and before every commit, a few fault sites, and it reaches Postgres through an
address that can be a proxy. It may support only a part of the workload generator's features (simple types, the
transfers tables, a keyed and a keyless table); it says so through `supports`. Reading the slot through the SQL
functions (`pg_logical_slot_peek_binary_changes`, then `pg_replication_slot_advance` after the destination
commit) is the smallest way; choose what you like. It is test code: simple and obviously right beats fast.

**Wrong variants of the toy**, one switch each, each a real class of bug:

| Variant | The bug | Must be found by |
|---|---|---|
| `ack_early` | confirms to the slot before the destination commit | fault fuzzer (kill between the two): a lost change |
| `position_apart` | stores the position in a second transaction after the data | fault fuzzer: a change applied twice (seen on the keyless table) |
| `split` | commits one source transaction in two destination transactions | the consumer loop — the end-state oracle cannot see it |
| `reorder` | applies two transactions of one group in the wrong order | workload fuzzer |
| `no_fence` | does not check the epoch when it commits | leadership fuzzer: two writers |
| `skip_on_reconnect` | after a lost source connection continues from the slot's newest position | fault fuzzer (network) |
| `swallow` | ignores an apply error of one table and moves the position on | workload fuzzer |
| `trust_frames` | applies what it received before the end of a transaction arrived, or without checking it | stream fuzzer |

Add variants when you see a class that none of these covers. For every variant, the evidence says which fuzzer
found it, after how many seeds and how many seconds; and the correct toy passes the same runs. A planted bug that
a fuzzer does not find within a bound you state is a defect of the fuzzer: fix the fuzzer, not the bound.

### 2. Workload fuzzer

Drives `cdc_v2.testing.workload` with a seed against a system and checks with the oracle.

- Shapes (SPEC): transaction sizes from one row to large, interleaved transactions of several sessions,
  savepoints and rollbacks (also a rolled-back large transaction), multi-table transactions, key changes with
  chains and swaps, unchanged TOAST values, keyless tables with identical rows, TRUNCATE, and DDL where the
  system supports it. Where the generator of package O lacks a shape of this list, add it there behind a
  `Features` flag and keep its properties: every session's plan is a function of the seed; what is not (the
  interleaving of sessions) is recorded in the replay log, so that a replay repeats it.
- One run = one seed: set up, start the system, load for a budget (rounds or seconds), then `quiesce() → Q`, wait
  until the applied position is at or past Q (deadline), `compare(position=Q)`. During the load: checkpoints
  (`pause`, wait, compare, `resume`) and the consumer loops of section 5 all the time.
- Outcomes: `pass`; `diverged` (oracle); `violation` (a consumer loop); `stuck` (no catch-up before the deadline —
  a failure, never a retry); `error`; `infrastructure` (the scenario could not be set up; it must be rare, carries
  its proof, and counts neither as a pass nor as a failure).
- A failure leaves a bundle (seed, configuration, replay log, the oracle's result, the system's logs, scrubbed)
  and goes to the **reducer**: it replays the log and removes rounds, sessions and features while the failure
  stays, and writes the smallest case as a seeded regression test (a `pytest` file that replays the reduced log).
  SPEC: every failure becomes a seeded regression test before it is fixed.

### 3. Fault fuzzer

The workload fuzzer's run with a seeded schedule of faults at random times. Each fault is an object with a kind,
its parameters, `inject()` and `heal()`, logged with times. Kinds, as SPEC lists them:

- **Process**: SIGKILL at a declared crash point with a seeded hit (the environment contract of the harness;
  `kill_later` on the points before a commit), SIGKILL of the process group at a random time, freeze and
  continue (SIGSTOP, SIGCONT), hang at a point.
- **Network** on a proxied link: drop, black hole, reset, latency, for a random time.
- **Source errors**: termination or cancellation of the pipeline's own backends (the walsender through the
  slot's `active_pid`, the SQL sessions through the pipeline's `application_name` — never a wider filter; the
  replication connection itself carries Debezium's fixed name), a conflicting
  `ACCESS EXCLUSIVE` lock on a published table for a bounded time, a long transaction on the primary, a WAL
  switch and a checkpoint, a restart of the primary.
- **Destination errors**: the fault sites of C4 (`md.connect`, `md.execute`, `md.fence`, `md.commit`) and the
  system's own, armed per instance through the environment contract with seeded hits: error, delay, hang.
- **Slot loss**: the slot dropped (after its holder was terminated, or while no instance runs); the slot
  invalidated by `max_slot_wal_keep_size`.
- **Replica**: restart; loss and promotion, then a new standby. Only on a cluster the scenario owns.
- **Full spill area**, through the system's hook.

Rules:

- After every fault, or burst of faults, the system must recover **without help** and reach a new Q with oracle
  equality. Where the documented answer to a fault is an alert and a new backfill (slot loss), the end state is
  still equality.
- What counts as one injected fault: an injection that is observed to have had its effect (the process died at
  the point; the proxy cut or held at least one open connection; a backend was terminated). An injection that
  hit nothing is logged as `no_effect` and is not counted. Say how each kind proves its effect.
- The schedule draws kinds from weights that the ledger reports. The count of the bar (≥ 10,000) is only claimed
  when every kind the system supports has at least 100 counted injections; a campaign of 10,000 cheap faults of
  one kind is not what SPEC asks for.
- Several faults may overlap (a kill during a black hole); the schedule generates that on purpose.
- **The suite of SPEC 1.7** is a fixed part of this fuzzer, runnable by one command (`tools/fuzz suite-1.7
  --system …`): every declared crash point with ≥ 3 seeds (the crash runner; a point never reached is a
  failure), each network fault, destination errors, source backend termination, slot loss, the full spill area,
  and a randomized soak of ≥ 1 hour with ≥ 200 faults; pass = oracle equality after every run. In this round it
  runs on the toy, with a soak of 15 minutes; its output format is the evidence of 1.7 later.

### 4. Stream fuzzer

Between the engine process and the applier (contract C2: frames with magic, type, sequence number, length,
payload and CRC-32). With the byte-stream relay of the harness, at random offsets:

- disconnect (end of stream inside a frame, between frames, inside a transaction);
- stall (no bytes for longer than the reader's deadline; a slow trickle);
- duplicate delivery (a frame repeated, a run of frames repeated, a whole transaction delivered again as after
  a restart of the engine);
- truncation and corruption (bit flips anywhere, also in lengths, sequence numbers, positions and counts; frames
  swapped; a frame of another transaction inserted; a valid frame with a count in `END` that does not match).

The property: what the reader hands on is always a prefix of the true sequence of whole transactions, followed
by either the true continuation or a detected error. Nothing partial, corrupt or repeated is ever applied, and
no position is confirmed beyond what was delivered whole and applied. Two targets in this round:

- The reader of package E, if it is merged: record frame streams from the real engine once (a workload with all
  frame types), keep a small corpus in the repository (generated data only), and fuzz the reader offline — this
  is fast, so run it by the hundred thousand cases, and put a bounded part into the default run.
- The toy (variant `trust_frames` must be caught).

State exactly which corruptions the frame format cannot detect, if you find any; that is a finding for the
contract, not something to work around.

### 5. Consumer loops (SPEC 1.3, 3.2)

Observers that read the destination on connections of their own, all the time, as fast as a bound allows, each
observation in **one** snapshot (one statement, or one read transaction):

- the transfers observer of package O (a constant global sum);
- parent and children: a parent row carries the number of its children, and children are inserted and deleted
  only together with the update of that number, across two tables, in one source transaction;
- two tables whose totals are equal (debits and credits);
- a large transaction (the generator's `large_txn_rows`, to ≥ 10M in round 2) that is visible either not at all
  or completely, together with its row in a second table;
- for SPEC 3.2 (package B writes the test, you deliver the observer): count and checksum of one table, polled
  throughout; only versions from a set the test names are ever seen — never a missing, empty or partial table.

Each observer reports how many observations it made and the longest gap between two (an observer that looked
ten times proves nothing), and on a violation what it saw, as counts and keys. Variant `split` of the toy must be
caught by the first three within seconds.

### 6. Leadership fuzzer

Rounds of seeded actions over at least 3 competing runs, under workload: start (also overlapping, also many at
once), pause for a random time (shorter and longer than the stale age), kill, cancel as the platform does it,
planned hand-over at the maximum run time, and the fault kinds of section 3 on top. Invariants:

1. **Never two writers.** The check must not rest on the fence it tests. For the toy: every data transaction
   also appends (epoch, run, position, transaction id) to a log table; over the whole log, positions rise
   strictly, epochs never fall, and every source transaction appears exactly once. For the pipeline, round 2
   takes the same facts from what S and L record; say which facts you will need from them.
2. **Never a lost or duplicated change**: oracle equality at a position at the end of every round (the keyless
   table is what shows a duplicate).
3. **Progress**: after the last disturbance of a round some run leads and catches up within a deadline. A system
   that is safe because it stopped is a failure.
4. A newcomer that finds a healthy leader exits in time (SPEC 4.2: 10 s; measured in round 2 on the real
   bootstrap).

The bar is ≥ 1,000 rounds. Variant `no_fence` must be caught.

### 7. Campaign runner and ledger

`tools/fuzz run --fuzzer workload|fault|stream|leadership --system toy|… --source-node replica|primary
[--seeds A..B | --hours H] --workers N`:

- takes the next seeds that the ledger does not hold yet; can be stopped at any moment and started again;
- one home of your pool per scenario, held under a file lock, so that two campaigns can never use one cluster;
  a cluster is reused across seeds (a fresh database per seed) until a fault kind breaks it, then rebuilt;
- runs at low priority, leaves nothing behind (processes, clusters, slots, databases), also when it is killed —
  and a `tools/fuzz clean` that removes what a killed campaign left, by the names in its own lock files only.

Ledger under `evidence/robustness/` (PLAN §6):

- `<fuzzer>/runs.jsonl`: one line per run — fuzzer, system, tier (`local`; later the stage 2 tiers), source mode,
  seed, commit and whether the tree was clean, start time (UTC), duration, counts (rounds, transactions, rows),
  faults counted per kind and `no_effect` per kind, outcome, failure id.
- `<fuzzer>/failures/<id>/`: the reduced case, the path of its regression test, later the fixing commit.
- `ledger.json`, written only by `tools/fuzz status --write`: per fuzzer, system, tier and mode the seeds,
  cumulative hours (the sum of run durations), faults in total and per kind, rounds, failures found (open,
  fixed), the commit at which the counters last restarted and why, and each number next to its minimum of SPEC §4
  with "met" or "not met". Nothing in it is typed by hand.
- **Counter restart** (SPEC: after a fix to correctness code the counters of the affected fuzzer restart from
  zero). Each fuzzer names, in one table in the code, the paths whose change affects it (the pipeline's source,
  the engine, the bootstrap; not documents, not tests). The restart commit of a fuzzer is the newest commit that
  touches one of these paths. Only runs made on that commit or a descendant, on a clean tree, are counted. A
  commit that touches such a path without changing behaviour can be exempted in `evidence/robustness/exempt.json`
  (commit, reason); every exemption is listed by `status` for review. Runs on the toy are shown as the self-test
  of the fuzzers and never count toward the bar.
- Evidence is written through `harness_evidence.write_evidence` only. Bundles hold generated data only; they
  still pass the same redaction.

## Tests

- Default run (hermetic, a few seconds): schedules and plans are functions of the seed; ledger arithmetic and
  the restart rule on a scratch git repository; the reducer on a synthetic failing predicate; the stream
  fuzzer's properties on the recorded corpus (bounded); the observers' logic on a local DuckDB file with a
  writer that splits on purpose.
- Markers `localpg` / `t1`: the toy end to end under each fuzzer for a short time; every wrong variant found by
  the fuzzer named in the table, the correct toy passing the same seeds; `suite-1.7` on the toy; a campaign
  killed in the middle and started again; `clean` after a kill; nothing left behind.

## Deliver

- The code, the toy and its variants, the tests; `docs/testing/fuzzers.md` (how to run a campaign and read the
  ledger, the `System` interface, how each fault kind proves its effect, what each fuzzer can and cannot find,
  contract notes); evidence under `evidence/robustness/` (the toy's self-test runs and the generated
  `ledger.json`) and `evidence/p3/fuzzers/` (the table of wrong variants: found by, after how many seeds and
  seconds).
- In your report: what packages S, L, B and V must expose so that round 2 can attack them (facts for the
  two-writers check, the spill hook, links that can be proxied, crash points you expect), every weakness of the
  harness or of the generator that you met, and how long a round of each fuzzer takes on this machine, so that
  the long campaigns can be planned.
