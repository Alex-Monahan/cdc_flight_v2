# Fuzzers and the campaign ledger (package F, round 1)

The machinery that attacks the pipeline: the workload, fault, stream and leadership fuzzers of the robustness bar
(SPEC §4, R22), the consumer loops of SPEC 1.3 and 3.2, the suite of SPEC 1.7, the campaign runner and the ledger
that proves the quantities of the bar (PLAN §6). The pipeline (packages S, L, B, V) does not exist yet, so every
fuzzer works against a small interface (`System`) and is proven on a **toy replicator** with deliberately wrong
variants: a fuzzer that cannot find a planted bug is not finished. Round 2 connects the real pipeline.

| File | What |
|---|---|
| `src/cdc_v2/testing/fuzz/system.py` | `System` (the interface), `PgSystem` (local Postgres pair from the pool), `FAULT_KINDS`, registry |
| `src/cdc_v2/testing/fuzz/pool.py` | homes `pg-f0` … `pg-f11` under file locks, the record of what each scenario created, `clean()` |
| `src/cdc_v2/testing/fuzz/run.py` | one run = one seed: supervisor, paced load, episodes, checkpoints, outcomes, bundle |
| `src/cdc_v2/testing/fuzz/faultkinds.py` | the fault kinds (inject, heal, proof of effect) and the seeded schedule |
| `src/cdc_v2/testing/fuzz/observers.py` | the consumer loops (transfers, parent/child, debit/credit, large transaction, versions) |
| `src/cdc_v2/testing/fuzz/stream_fuzzer.py` | the stream fuzzer: byte and meaning faults on frames, reference assembler, live pipe |
| `src/cdc_v2/testing/fuzz/leadership.py` | the leadership fuzzer and the writer-log check |
| `src/cdc_v2/testing/fuzz/reduce.py` | the reducer and the seeded regression test writer |
| `src/cdc_v2/testing/fuzz/ledger.py` | `runs.jsonl`, failures, `ledger.json`, the counter-restart rule |
| `src/cdc_v2/testing/fuzz/campaign.py`, `suite17.py`, `variants.py`, `__main__.py`, `tools/fuzz` | runner, suite 1.7, variant table, command line |
| `src/cdc_v2/testing/workload.py` | four new `Features` for the consumer loops (section 5) |
| `tests/fuzz/toy/toy.py`, `tests/fuzz/toy/system.py` | the toy replicator, its variants, its `System` and its minimal platform |
| `tests/fuzz/corpus/*.frames`, `tests/fuzz/record_corpus.py` | frame streams recorded from the real engine (generated data) |
| `tests/fuzz/test_fuzz_unit.py`, `tests/fuzz/test_fuzz_localpg.py` | hermetic tests (default run) and the opt-in suite |
| `evidence/robustness/` | the ledger: `<fuzzer>/runs.jsonl`, `<fuzzer>/failures/<id>/`, `ledger.json` |
| `evidence/p3/fuzzers/` | the variant table, the suite of SPEC 1.7 on the toy, the stream fuzzer's offline result |

## 1. Running a campaign and reading the ledger

```sh
tools/fuzz run --fuzzer workload|fault|stream|leadership --system toy[:variant] --source-node replica|primary \
               [--seeds A..B | --hours H] --workers N [--budget S] [--kinds a,b] [--reduce] [--option k=v]
tools/fuzz status [--write]          # the ledger; --write writes evidence/robustness/ledger.json
tools/fuzz suite-1.7 --system toy [--soak-minutes 15]
tools/fuzz variants                  # the wrong variants of the toy (section 2)
tools/fuzz stream-offline --cases 100000 [--negative-controls]
tools/fuzz reduce <failure id>       # a kept failure -> tests/fuzz/regressions/test_<id>.py
tools/fuzz clean                     # what killed campaigns left, by the names in their lock records only
```

`tools/fuzz` is `tools/with-env uv run python -m cdc_v2.testing.fuzz`. The runner:

- takes the next seeds that the ledger does not hold for this fuzzer, system, tier and mode (an explicit range minus
  the held seeds, or upward from the highest held seed for `--hours`); an `infrastructure` outcome holds no seed;
- holds one home of the pool per scenario (`flock` on `~/.cache/cdc_flight_v2/fuzz/locks/pg-f<n>.lock`); a cluster
  is reused across seeds with a fresh database per seed until a fault kind changes its shape (promotion), then the
  next holder rebuilds it with `localpg reset`; nodes use base … base+4 of a home, the TCP proxies base+5 … base+7;
- runs at nice 10, at most 2 scenarios in parallel on this machine; SIGINT or SIGTERM stop it at the next episode
  (the run in progress is dropped, its seed stays free); after SIGKILL, `tools/fuzz clean` kills the processes the
  lock records name (pid and start time checked), drops the databases they name and stops the clusters. It never
  matches processes by name and never lists databases.

**Ledger** (`evidence/robustness/`, every file through `harness_evidence.write_evidence`):

| File | Content |
|---|---|
| `<fuzzer>/runs.jsonl` | one line per run: fuzzer, system, `self_test`, tier, source mode, seed, commit, `clean`, start (UTC), `duration_s`, counts (rounds, transactions, rolled back, final rows, checkpoints, observations), faults counted per kind and `no_effect` per kind (with up to 20 `faults_without_effect` and their proof), outcome, reason, `failure_id`, the observers' reports, the checkpoints |
| `<fuzzer>/failures/<id>/` | `failure.json` (seed, outcome, reason, `regression_test`, later `fixed_commit`), `bundle/` (record, oracle result, fault log, events and log tails; generated data only, redacted; a replay log above 256 KiB stays in the local bundle), after reduction `case.json` |
| `ledger.json` | only from `tools/fuzz status --write`: per fuzzer, system, tier and mode the seeds, hours (sum of run durations), faults in total and per kind, `no_effect` per kind, rounds, outcomes, failures open and fixed, the restart commit and why, each number next to its minimum of SPEC §4 with met / not met, the weights of the schedule and the kinds not attacked, the exempt commits |
| `exempt.json` | `{"exempt": [{"commit": …, "reason": …}]}`: commits that touch an affecting path without changing behaviour; `status` lists each for review |

**Counter restart** (SPEC §4). `ledger.AFFECTS` names per fuzzer the paths whose change affects it: the pipeline's
source (`src/cdc_v2/{stream,apply,md,pg,transform,backfill,state,leader}`, `supervisor.py`, `pipeline.py`,
`config.py`, …), the engine (`jvm/src`) and the bootstrap (`flights/cdc_v2`); never documents or tests. The restart
commit of a fuzzer is the newest commit that touches one of them and is not exempt. Only runs made on that commit or
a descendant (`git merge-base --is-ancestor`), on a clean tree, are counted. *Clean* means no change to a tracked file
outside `evidence/` (the ledger is written while campaigns run). Runs of a self-test system (the toy) are shown
apart and never count toward the bar.

**Minimums** (`ledger.MINIMUMS`): workload ≥ 1,000 seeds and ≥ 100 h; fault ≥ 500 seeds, ≥ 100 h, ≥ 10,000 faults,
≥ 100 counted injections for every kind the system supports, zero divergences; leadership ≥ 1,000 rounds; value ≥
1,000,000 values per family (P4). For workload and fault, SPEC asks for local Postgres and PlanetScale, replica and
primary mode: the totals are summed over those four combinations and `combos.missing` must be empty (contract note
3).

## 2. The toy replicator and its wrong variants

`tests/fuzz/toy/toy.py` is a real, small, crash-only replicator, run as its own process:

- it reads a logical slot (plugin `test_decoding`) on the replica or the primary through the SQL functions
  (`pg_logical_slot_peek_changes`, `pg_replication_slot_advance` after the destination commit), over the proxied link;
- it encodes each batch of whole transactions as engine frames (the C2 layout, `cdc_v2.testing.frames`), passes them
  through a spill file under a byte quota and, with the stream fuzzer, through seeded byte faults, and reads them
  back with the engine's decoder and a strict assembler;
- it applies every source transaction whole to a DuckDB file, with `_cdc.stream_state.applied_lsn` (the commit end
  position of the last applied transaction) updated in the same transaction, conditioned on the value it read
  (run-again rule of C4), and confirms the slot only afterwards;
- it has a leader epoch, a fence row and a heartbeat (`_cdc.toy_leader`, `_cdc.toy_fence`); a newcomer waits (or, under
  the leadership fuzzer's platform, exits) while a healthy heartbeat exists, and takes over otherwise; every data
  transaction checks the fence and appends (epoch, run, position, transaction id) to `_cdc.toy_log`;
- when its slot is missing or invalid, or a resynchronisation is marked pending, it marks the destination, drops and
  creates the slot with a snapshot (`CREATE_REPLICATION_SLOT … (SNAPSHOT 'use')` in a `REPEATABLE READ READ ONLY`
  transaction of a replication connection) and loads every table from that snapshot in one destination transaction,
  at the consistent point. A fresh destination starts this way;
- crash points `toyrep.*` after every step that changes durable state and before every commit; fault sites `md.connect`,
  `md.execute`, `md.fence`, `md.commit` (the names of C4, so that the schedule needs no change in round 2) and
  `toyrep.slot_peek`, `toyrep.slot_advance`, `toyrep.source_connect`;
- every source statement has a client-side deadline that shuts the socket down (a black hole through the proxy is
  found by it); the destination simulates MotherDuck's write-write conflicts at a seeded rate (`conflict_rate`
  0.1 per group attempt, retried as C4 says) — its only ambient error, which makes error-handling bugs visible to a
  workload-only run (contract note 5).

It supports every workload feature but DDL. The fuzzers found two defects in it on the way, both fixed: a snapshot
slot created on a primary needs a `READ ONLY` transaction (the standby had hidden it), and after a promotion the old
primary must be removed or `localpg up` starts it again as a second primary.

**Wrong variants** — one switch each (`toy:<variant>`), each a real class of bug. Results:
`evidence/p3/fuzzers/variants.json` (found by, after how many seeds and seconds; the correct toy on the same seeds).

| Variant | The bug | Must be found by | Found as |
|---|---|---|---|
| `ack_early` | confirms the slot before the destination commit | fault fuzzer (process kills) | `diverged`: the toy itself reports a row lost (unchanged value with no earlier row) |
| `position_apart` | position in a second transaction after the data | fault fuzzer (process kills) | `diverged`: reported by the toy (`ApplyInvariantError`) |
| `split` | one source transaction in two destination transactions | consumer loops | `violation` (debit/credit loop) after 1 s |
| `reorder` | two transactions of one group in the wrong order | workload fuzzer | `violation` (its consumer loops) after 2 s; earlier runs also `stuck` |
| `no_fence` | does not check its epoch | leadership fuzzer | `violation`: two writers in the writer log |
| `skip_on_reconnect` | after a lost source connection continues from the slot's newest position | fault fuzzer (network, backends) | `diverged` (oracle); earlier runs `violation` (transfer sum) |
| `swallow` | ignores an apply error of one table and moves the position on | workload fuzzer | `violation` (debit/credit loop); earlier runs `diverged` |
| `trust_frames` | applies frames without checking checksum, sequence or count, and a transaction cut off before its END | stream fuzzer | `diverged` (oracle: a changed value) |

Result (`evidence/p3/fuzzers/variants.json`, commit ffdd6f4, `no_fence` row at 4dffb64): every variant was found on
the first seed it was run with (bound: 6 seeds for the fault fuzzer, 4 for workload and stream, 3 for leadership),
and the correct toy passed the same seed every time. Seconds of run until detection: `split` 1.0, `reorder` 1.6
(consumer loop), `swallow` 9.5, `skip_on_reconnect` 12.1, `trust_frames` 14.4, `ack_early` 18.9, `no_fence` 22.6
(two writers in round 1), `position_apart` 44.5. In the first table run (before the review fixes) `no_fence` needed
3 seeds; the leadership fuzzer now pauses longer than the stale age more often and logs the take-over, and found it
at once in both later runs.

The offline stream fuzzer (`evidence/p3/fuzzers/stream_offline.json`): 100,000 cases (seeds 1–100,000, 19 fault
kinds, about 5,200 each, 305 s), no failure; undetected by the format and accepted as such: 5,957 clean ends at a
frame boundary after a shortening fault and 3,788 content replacements with a valid checksum; the negative controls
(lenient decoder and trusting assembler) failed 112 of 2,000 cases, so the property can fail.

## 3. The `System` interface

`System` extends the crash runner's `Adapter` (docs/testing/harness.md section 4): `name`, `crash_points()`,
`setup(workdir, seed)`, `start(tag, env) -> Proc`, `workload(step, seed)` (one round), `wait_caught_up(timeout)`,
`check()`, `teardown()`. It adds:

| Member | Contract |
|---|---|
| `tier`, `source_node`, `is_self_test` | `local`; `replica` or `primary` (where the slot is; both modes are in the bar); True for the toy |
| `supports(kind)`, `capabilities()` | every name of `FAULT_KINDS` and of `WORKLOAD_FEATURES`; what is not supported is not scheduled, and the ledger lists it as not attacked |
| `fault_sites()` | the system's fault sites (C1, C4) |
| `cluster`, `db`, `primary_node`, `slot_node`, `standby_node()`, `after_promotion(old, new)` | the source the scenario owns (a home of the pool) and which node holds what; `localpg` through `lease.localpg(...)` |
| `wl` (a `Workload`), `quiesce() -> Q`, `heartbeat()` | the seeded workload of the scenario; the source-side heartbeat write that gives a position after Q on an idle source |
| `source()`, `destination()`, `close_destination()`, `compared_tables()`, `oracle_config()` | what the oracle takes (C8); the heartbeat table is never compared |
| `applied_position()`, `wait_for_applied(Q, deadline)`, `compare(Q)` | the applied position (stand-in reader of `_cdc.stream_state` until S's), waiting with heartbeats, the positioned comparison |
| `observer_connect()` | a connection of the observer's own (for a DuckDB file: one open per statement, so one statement is one snapshot) |
| `links()` | `{"source": NetProxy}`: the link pipeline → source behind the TCP proxy (ports base+5 … base+7 of the home) |
| `set_spill_full(bool)`, `spill_full_seen(since)` | the spill hook and its proof |
| `writer_log()` | the facts of "never two writers": (order, epoch, run, position, xid, kind) in commit order |
| `platform()` | starts and cancels runs (C6; the toy brings `ToyPlatform`: cancel marks first, kills later; maximum run time enforced late) |
| `app_prefix` | `application_name` prefix of the system's own SQL sessions (the only filter for backend termination) |
| `run_counts()`, `divergence_reported(since)`, `events()` | the system's own counts (stream faults, detections) and its own report that the destination diverged (`ApplyInvariantError`) |

`PgSystem` implements everything except how to start an instance, where the destination is, the points and sites,
the spill hook and the writer log.

## 4. The fault kinds and how each proves its effect

One fault counts only when its effect was observed; otherwise it is logged as `no_effect` with its proof and not
counted. The count of the bar (≥ 10,000) is claimed only when every supported kind has ≥ 100 counted injections.

| Kind | What it does | Proof of effect |
|---|---|---|
| `kill_point` | SIGKILL at a declared crash point with a seeded hit (the next instance is armed, the current one killed) | the instance died by SIGKILL and its own hit log has the fire line of that point |
| `kill_point_later` | the same with `kill_later` at a point immediately before a commit (COMMIT in flight) | as above |
| `kill_group` | SIGKILL of the process group at a random time | the instance was alive and died by SIGKILL |
| `freeze` | SIGSTOP of the group, 0.2–1.5 s or 2.5–5 s (beyond the stale age), then SIGCONT | `ps` shows state T |
| `hang_point` | the instance hangs at a point with a live process, then is killed | the fire line, the process still alive |
| `net_drop`, `net_reset` | FIN / RST on every open connection of the link and on new ones, 0.3–4 s | the proxy's `closed_by_fault`/`accept_closed` (`reset_by_fault`/`accept_reset`) rose |
| `net_blackhole` | nothing forwarded, 0.3–4 s | a connection was open at injection or opened during the hole |
| `net_latency` | every chunk delayed 50–400 ms, 0.3–4 s | bytes moved while the delay was set |
| `src_terminate`, `src_cancel` | `pg_terminate_backend` / `pg_cancel_backend` of the system's own sessions (`application_name` prefix, current database) on the slot node and the primary | at least one backend signalled |
| `src_lock` | `ACCESS EXCLUSIVE` on a compared table for 0.5–3 s | the lock was granted (waiters counted) |
| `src_long_txn` | a transaction with an assigned id open on the primary for 1–6 s | the id was assigned and held |
| `src_wal_switch` | `pg_switch_wal()` and `CHECKPOINT` | the WAL file name changed |
| `src_restart_primary` | `localpg kill` and `up` of the primary (the load pauses) | the postmaster pid changed |
| `dest_error`, `dest_delay`, `dest_hang` | a rule at a destination site (`md.*`) with seeded `skip` and 1–3 `hits`: `duckdb.IOException`, a delay, a bounded block | the fault log has a fired line of the rule |
| `slot_drop` | the slot dropped after its holder was terminated, or while no instance runs | the drop succeeded |
| `slot_invalidate` | instance frozen, `max_slot_wal_keep_size=2MB` on the slot node, WAL generated and checkpoints, setting removed | `wal_status = 'lost'` or an invalidation reason |
| `replica_restart` | `localpg kill` and `up` of the standby | the postmaster pid changed |
| `replica_promote` | primary killed, standby promoted, old primary removed, a new standby added (the load pauses; the cluster is rebuilt for the next seed) | the promotion's report (new timeline) |
| `spill_full` | the spill area filled through the system's hook for 0.8–4 s | the system reported the full area (toy: event `spill_full`) |

The schedule (`Schedule.plan`) draws 1–3 faults per episode of 10 s (the suite's soak: 2–5) at random offsets with
the weights of `faultkinds.DEFAULT_WEIGHTS` (process 29, network 18, source 15, destination 14, slot 6, replica 5,
spill 5); each fault runs in a thread of its own, so faults overlap on purpose. Kinds that kill or lose the primary
pause the workload at a round boundary first, so that the workload's own connections never fail in the middle of a
transaction. After every episode: heal, checkpoint, equality.

## 5. The fuzzers: what they can and cannot find

**Workload fuzzer** (`run.py`, `--fuzzer workload`). One run = one seed: setup, the first instance reaches a position,
then episodes of `episode_s` (10 s) of paced rounds (`pace_s` 0.25 s between rounds), each closed by a checkpoint:
the load stops at a round boundary, `quiesce() → Q`, wait until applied ≥ Q (90 s), `compare(position=Q)`, resume.
The consumer loops run all the time; a violation ends the run at once. Outcomes: `pass`, `diverged`, `violation`,
`stuck` (a failure, never a retry), `error`, `infrastructure` (could not be set up, or the source failed under the
workload; it carries the reason and counts neither way). If the system itself reports three times that its
destination diverged (`ApplyInvariantError`), the run ends as `diverged` without waiting for the deadline.

Shapes: every feature of the generator — single-row to large transactions, interleaved sessions, savepoints and
rollbacks, multi-table transactions, key chains and swaps, unchanged TOAST values, keyless tables with identical rows,
TRUNCATE, DDL where supported — plus four new `Features` for the consumer loops:

| Feature | Tables | Invariant in every committed source state |
|---|---|---|
| `parent_child` | `w_parent(id, w, n_children)`, `w_child(id, parent_id, w, v)` | `n_children` = number of children; children change only together with the parent row, both orders of the two tables |
| `ledger` | `w_debit`, `w_credit` | the two totals are equal (book, rebook, cancel; the side written first varies) |
| `large_marked` | `w_bulk(id, batch, v)`, `w_bulk_mark(batch, n)` | a batch of `large_txn_rows` rows is visible with its marker row, or not at all (deleted again later, also in one transaction) |
| `large_rollback` | the same | a rolled-back large insert is never visible |

Each keeps the generator's properties: every session's plan is a function of the seed; the interleaving is in the
replay log. *Can find*: lost, duplicated, reordered or partially applied transactions visible in the end state or
in a cross-table invariant; error handling that moves on. *Cannot find*: a bug that needs a fault to show (that is
the fault fuzzer), a split that never coincides with an observation (the observers poll every few milliseconds),
anything in a feature the system does not support (DDL on the toy).

**Fault fuzzer** (`--fuzzer fault`): the workload run plus the schedule of section 4 in every episode. *Can find*:
every protocol bug whose window a kill, a freeze, a lost connection, a source or destination error, a lost slot or
a full spill area can hit; the suite of SPEC 1.7 is a fixed part (section 6). *Cannot find*: a window narrower than
the chosen crash points (a point that does not exist cannot be armed), TCP keepalive behaviour (the proxy's kernel
answers keepalives, docs/testing/harness.md section 7), faults on links that are not proxied (MotherDuck; only the
fault sites reach it).

**Stream fuzzer** (`stream_fuzzer.py`). Property: the reader hands on a prefix of the true transactions, then the
true continuation or a detected error; nothing partial, corrupt or repeated is applied; no position beyond the last
whole transaction. A clean end short of the true transactions is accepted only after a fault that really shortens
the stream (a cut or truncation, a dropped last frame or END, a corrupted last length); after any other fault it is a
failure, so a reader that silently drops the tail is caught. Two targets:

- *offline, against package E's decoder*: the corpus (`tests/fuzz/corpus/`, recorded once from the real engine with
  every frame type, re-encoded byte-identically) and synthetic consistent streams; byte faults (`pipefaults`
  flips, duplicated ranges, cuts, truncation; targeted flips in header length, sequence and type and in payloads;
  raw frame swaps, repeated frame runs, a dropped frame, a cut at a frame boundary) under random chunking; meaning
  faults re-encoded with valid sequence numbers and checksums (END count off by one, a ROWS frame of another
  transaction inserted, END or BEGIN dropped, records swapped, a falling END position, a whole transaction again
  in the same life, a HEARTBEAT with a position ahead of the last END, content replaced with the same shape, a new
  life replaying transactions). The reference assembler (`Assembler`) stands in for S's: HELLO first; record `seq`
  strictly rising; BEGIN/END paired; END count = rows + TRUNCATE + transactional MESSAGE; END position present and
  rising; HEARTBEAT never moves the position. Its negative controls (`TrustingAssembler`, a lenient decoder) must be
  caught; a stall longer than the reader's deadline on a real pipe must surface as a deadline, never as a partial
  frame (`run_live`);
- *the toy*, with byte faults on its engine-to-applier path in about a third of its batches; `trust_frames` must be
  caught.

**What the frame format cannot detect** (findings for contract C2):

1. A clean end of the stream exactly at a frame boundary looks like a clean end (`FrameReader.read` returns None);
   only the engine's exit status tells it from a STOP (`EngineExited`). The assembler must therefore never apply a
   transaction without its END — the reference does not.
2. A whole new life (sequence 0, HELLO) that repeats transactions — by design; discarded by position.
3. Content that is wrong before it is encoded (for example the rows of a ROWS frame replaced by as many other rows
   with the same record numbers) has a valid checksum: no transport check can see it.
4. The header has no checksum of its own: a corrupted length is detected only after that many further bytes arrived
   (the next frames are swallowed as payload until the CRC fails; on an idle stream only the next HEARTBEAT brings
   the bytes), and the decoder first allocates up to `max_frame_bytes` (default 1 GiB + 16 MiB) for it. Bounded,
   detected, but slow; a header CRC would detect it at once.

Every other case of the catalogue above was detected (frame layer or assembler) in every run; the counts are in
`evidence/p3/fuzzers/stream_offline.json` and the ledger.

**Leadership fuzzer** (`leadership.py`). One run = one seed = `rounds` rounds over the system's platform, which starts
a run every `tick_s` (1 s; the every-minute schedule) and lets newcomers that find a healthy leader exit. Each round
draws 3–8 actions over about 6 s: start (also 2 or 3 at once), pause (0.3–1.5 s or 2.5–5 s; the toy's stale age is
2 s), kill, cancel as the platform does it, a planned hand-over at a short maximum run time, and faults that need no
supervisor (network, backend termination, destination, spill). At the end of every round: progress (some run leads
and catches up within 60 s), oracle equality, the writer log (`check_writer_log`: epochs never fall, positions rise
strictly — a resynchronisation or take-over row may repeat the last position —, every source transaction once, one
epoch per run and one run per epoch, a take-over row always raises the epoch), every run that never took over and
that the fuzzer did not pause, kill or cancel ended within 10 s, no consumer-loop violation. The writer log is
written by the data transactions themselves and the toy writes its take-over into it in the take-over transaction,
so the check does not rest on the fence it tests, and an old leader that writes after a take-over is caught even
before the new leader's first write.

**Consumer loops** (`observers.py`). One statement per observation (one snapshot), as fast as `min_interval`
allows (5 ms in the runs); each loop reports observations, the longest gap between two successful ones, unavailable
reads and the first violations as counts and keys. `versions(table, allowed)` is the observer of SPEC 3.2 (package B
writes the test): count and checksum (`sum(hash(row))`) of one table, only versions the test names, a missing table
is a violation.

**Reducer** (`reduce.py`). `replay_case` runs a case: fresh scenario, first instance frozen once it holds a position,
the reduced replay log applied on one connection, the instance continued (it meets the backlog in large groups, which
makes grouping bugs repeatable), then the recorded faults one by one, quiesce, catch-up, oracle, consumer loops.
`reduce` removes rounds, then writers, then entries, then faults (delta debugging; a candidate fails if one of two
replays fails), writes `case.json` and `tests/fuzz/regressions/test_<id>.py` (marker `localpg`). For a toy variant
the test asserts that the correct toy passes and that the planted bug is still found (`xfail(strict=True)`); for a
real system it asserts that the case passes, so it fails until the fix lands.

## 6. The suite of SPEC 1.7

`tools/fuzz suite-1.7 --system toy` (output `evidence/p3/fuzzers/suite-1.7-<system>-<mode>/`): every declared crash
point with seeds 1, 2, 3 through the crash runner (a point never reached is a failure) and the points immediately
before a commit again with `kill_later`; one fault-fuzzer run each for the network faults, the destination errors,
source backend termination, slot loss and the full spill area (every kind of the group must have had its effect);
a randomized soak with every supported kind (this round: 15 minutes, at least 200 counted faults; SPEC: ≥ 1 h and
≥ 200). Pass = oracle equality after every run. `summary.json`, `crash_summary.json`, `crash_ledger.jsonl` and
`runs.jsonl` are the format of the evidence of SPEC 1.7.

**Result on the toy (commit d4deb59, before the review fixes of section 11):** 42 of 42 crash-point scenarios
passed and all 13 declared points were reached (3 seeds each, plus `toyrep.commit_sent` with `kill_later`); every
group run and the 15-minute soak ended with oracle equality (soak: 171 counted faults of all 23 kinds, 920 s; suite
1,250 s). Two shortfalls of the suite itself, not of the system: the destination group drew no `dest_hang` (its kinds
were drawn at random), and the soak counted 171 faults, below its minimum of 200. Both are fixed (every kind of a
group in every episode; 3–6 faults per soak episode), but the suite was not run again within this round's campaign
budget; `evidence/p3/fuzzers/suite-1.7-toy-replica/` holds the run as it was (`all_passed: false` for these two
reasons).

## 7. Tests

```sh
uv run -q pytest -q tests/fuzz/test_fuzz_unit.py                                   # default run, about 3 s
tools/with-env uv run pytest -q -m localpg tests/fuzz/test_fuzz_localpg.py         # about 25 min (variants included)
tools/with-env uv run pytest -q -m localpg tests/fuzz/test_fuzz_localpg.py -k "not variant"   # about 6 min
tools/with-env uv run pytest -q -m "jvm and localpg" tests/fuzz/record_corpus.py   # records the corpus again
```

Hermetic: schedules and plans are functions of the seed; every fault kind has a weight, a maker and a proof; the new
workload shapes are reproducible and keep their invariants after every planned transaction (replayed into DuckDB);
ledger arithmetic, the restart rule, exemptions and self-test exclusion on a scratch git repository; `ddmin` on
synthetic predicates; the stream property on the recorded corpus (bounded: 430 cases), its negative controls and
the live pipe; the consumer loops against a writer that splits on purpose and one that does not; the writer-log
check; the toy's `test_decoding` parser and frame path. Opt-in: the toy end to end under each fuzzer, every variant
found and the correct toy passing the same seeds, a small suite 1.7, a campaign killed in the middle, cleaned and
started again, nothing left behind.

## 8. Contract notes

1. `supports(kind)` takes the names of `FAULT_KINDS` and `WORKLOAD_FEATURES`; a system says per name what it can take.
2. *Clean tree* for the counter rule means no change to a tracked file outside `evidence/`: the ledger is itself
   written during campaigns, and counting it as a change would make every second run dirty.
3. SPEC §4's "≥ 1,000 seeds and ≥ 100 cumulative hours, on local Postgres and on PlanetScale, in replica mode and in
   primary mode" is read as: the totals over the four combinations, and each combination with at least one counted
   run. If the owner means the quantities per combination, `against()` is applied per group already; only the
   `bar_met` rule changes.
4. SPEC gives the stream fuzzer no quantity; the ledger reports its cases, detections and undetected classes only.
5. The toy's only ambient error is a simulated destination write-write conflict (`conflict_rate`), modelled on
   MotherDuck's (C4: run again after `Conflict`). Without some ambient error, no workload-only run can show an
   error-handling bug such as `swallow`.
6. A run ends as `diverged` when the system reports its own `ApplyInvariantError` three times during a catch-up, so
   that a lost change costs seconds, not the 90 s deadline; `stuck` remains the outcome when nothing is reported.
7. `infrastructure` also covers a failure of the workload on the source (which would leave the generator's model
   ahead of the source); with the load paused for primary kills it has not happened.
8. The toy reads `test_decoding` through the SQL functions (no Debezium, no publication); it is a model of the
   protocol, not of the engine. Its frames use the C2 layout with every value as text.
9. "A newcomer exits within 10 s" is measured for the toy as process exit time; round 2 measures the real bootstrap
   (SPEC 4.2).
10. Fault sites `md.connect`, `md.execute`, `md.fence`, `md.commit` are declared by the toy itself (it is a separate
    process), with the names of C4, so the schedule is unchanged when package D's lanes declare them.
11. `tools/fuzz run` refuses more than 2 workers (the machine is shared); the long campaigns raise it when the
    machine is theirs.

## 9. What round 2 needs from S, L, B and V

- **Facts for "never two writers"** (S, L): one row per committed stream-lane group in `_cdc` (for example
  `_cdc.batches`): the epoch the fence checked, the run id, the previous and new applied position, and the commit
  positions (or first and last, and the count) of the source transactions in it; likewise for backfill lanes (epoch,
  run, table, chunk). The order of commit must be recoverable (a sequence value taken in the transaction).
- **The applied-position reader** `stream.applied_position(conn, slot)` (C8), and the slot name.
- **The spill hook** (S): the fault site in front of the spill write (for example `stream.spill`) on which the
  fuzzer arms `raise OSError(ENOSPC)`, and an alert row (`_cdc.alerts`, kind `spill_full`) as the proof of effect;
  or the `max_temp_directory_size` of the transform connection as a setting the fuzzer can make tiny.
- **Links that can be proxied**: every source address of the configuration per node (replica and primary: the
  engine's replication connection, catalog and backfill reads, the primary heartbeat and publication maintenance)
  must be settable to a proxy port; the MotherDuck link is reached only through the `md.*` fault sites.
- **Crash points** expected: the steps of design §3 (`stream.staged`, `stream.begun`, `stream.applied`,
  `stream.position_written`, `stream.fenced`, `stream.commit_sent`, `stream.committed`, `stream.ack_sent`,
  `engine.acked` in Java), the lane points of C4 (`<lane>.fenced`, `<lane>.commit_sent`, `<lane>.committed`), take-over,
  release and heartbeat (`leader.*`), slot creation, backfill chunk commit, pending log append, swap; all declared
  through `cdc_v2.crashpoints` and listable with `python -m cdc_v2.crashpoints --import …`. A point reached only on
  some paths should say so, so the suite arms it where it is reachable.
- **Own sessions**: a common `application_name` prefix (`cdc_v2_`) for every SQL session of the pipeline; the
  replication session is found through the slot's `active_pid`.
- **Platform** (L, C6): `LocalPlatform.start_run(env)`, `cancel(run)`, `run_state(run)`, the runs with start and end
  times and exit codes, so that the leadership fuzzer can drive it and measure "newcomer exits within 10 s".
- **Divergence report** (S): the quarantine exit and `ApplyInvariantError` as alert rows, for
  `divergence_reported`.
- **Backfill requests** (B): the request table, so that re-backfills of a subset can be part of the workload, and
  the `versions` observer for the swap test of SPEC 3.2.
- **Slot findings** (V): the enactment after `slot_drop`, `slot_invalidate` and `replica_promote` (end state:
  equality after a backfill).

## 10. Weaknesses met in the harness and the generator

- `NetProxy.stop()` with open connections prints asyncio "Task was destroyed but it is pending" and
  `CancelledError` tracebacks on standard error (noise only; the proxy stops).
- The generator's model assumes every planned transaction committed: a transaction lost to a source fault leaves it
  ahead of the source (with `parent_child` that can create orphans at the source). The fault fuzzer pauses the load
  for the kinds that kill the primary.
- The replay log is in completion order of the writers, not in commit order; replay is exact only because writers
  own disjoint rows and transfers commute.
- `PrivateDatabase` has no constructor for an existing database by name (`clean` needs one; it uses `__new__`).
- The crash runner's adapter factory is called per scenario from worker threads; a system that needs a home per
  worker needs a thread-local lease (`suite17._Leases`).
- The hit log of the crash points records every call of every point; long runs make large logs (one per instance
  here).
- The oracle works against a local DuckDB file through `Destination(conn=…)`, but a reader holds the file's lock:
  the toy and every observer open the file per statement.
- `localpg up` restarts every stopped node, also a killed former primary after a promotion (`remove` it first).
- **System V shared memory.** This machine allows 32 segments (`kern.sysv.shmmni`), and every running Postgres
  instance holds one. A node that was killed (`localpg kill`) and whose data directory is then deleted (`remove`,
  `reset`) leaves its segment behind for ever, because only a new start in the same directory removes it. On
  2026-11-10 twenty such orphans of other work filled the table, and new clusters failed with "could not create shared
  memory segment: No space left on device". The first suite run therefore failed in `infrastructure`. This package
  left one orphan (its first primary of `pg-f0`, killed by a promotion drill, identified by creator pid and time) and
  removed it with `ipcrm`; the others are not this package's to remove. localpg should remove a killed node's
  segment before it deletes the directory, and the orchestrator should look at the orphans.
- `procs.Proc.signal_group` catches only `ProcessLookupError`; on macOS, `killpg` of a group whose leader has just
  exited answers `PermissionError` (EPERM). The leadership fuzzer treats that as "the run is gone".
- Writing a large failure bundle through `write_evidence` takes minutes: the redaction rules run over every text.
  It does not slow the run itself, but the campaign waits.

## 11. Review

A read-only reviewer with fresh context found weaknesses. All are fixed, with tests where they can be tested
(commit ffdd6f4):

- a failed injection could be counted through its heal;
- overlapping network faults on one link undid each other: now one at a time per link, and the second is logged as
  `no_effect`;
- `src_cancel` counted idle backends;
- the stream property accepted a clean short end after any fault;
- the writer log did not check that each epoch belongs to one run, and did not record the take-over itself;
- a newcomer that stayed alive without deciding was not seen;
- consumer loops with no observation passed;
- the supervisor's pause was not nested, and the load's pause had a race;
- the restart rule followed commit order instead of history. It is merge-safe now: a run counts only if no
  non-exempt affecting commit of HEAD's history is missing from it. An untracked new module also counted as a clean
  tree;
- the per-kind total ignored supported kinds that had no injection;
- `clean` erased a dirty mark;
- the home of the T1 fixtures could be leased;
- an evidence refusal could lose a run's line;
- the variant table counted an `error` as found, and the reducer accepted a different failure.

Still open:

- a keyless update with an unchanged TOAST value would insert NULL in the toy; no workload table reaches that case;
- `register_root` grows by two roots per run, which slows only very long campaigns.

The variant table and the offline stream result were produced again after these fixes; the suite was not (section 6).

## 12. How long a round takes on this machine

Measured on the toy (shared 10-core laptop; other workers' load present; see the evidence files for the exact runs):
a workload run of 40 s of load takes about 45–55 s; a fault run of 60 s about 70–90 s (catch-up after faults and
resynchronisations); a leadership round about 11 s (3 rounds: 34 s); the offline stream fuzzer about 1,000 cases in
8–15 s on the recorded corpus. For the long campaigns: ≥ 1,000 leadership rounds ≈ 3–4 h per scenario slot; the
workload bar (100 h) and the fault bar (100 h, ≥ 10,000 faults: at about one counted fault per 6–8 s, ≈ 20 h of fault
runs, so the hour bar decides) need weeks of one slot or days of several.
