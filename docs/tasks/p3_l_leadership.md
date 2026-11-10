# Task: package L — leadership

Read `docs/tasks/README.md` first; its rules bind you.

You build what makes "one leader" true (SPEC 4.2, R18): the bootstrap `flights/cdc_v2/main.py` with the guard
decision and the take-over, the heartbeat and the hard exit, the planned hand-over and the release, the leader
machine, and the platform interface with its local simulator. Design: `docs/design/core.md` sections 1, 4 and 5.
Contracts: `docs/design/p3_plan.md` C0, C1, C4, C5, C6, C11, and 6.2 "L". This is a core-correctness package:
every other package relies on the promise that a run which is not the leader cannot commit, and that a leader
which stopped making progress is replaced.

You stand on package D (`cdc_v2.md`: lanes, the fence, the connection registry, the leader tables, the clock,
errors, deadlines; `cdc_v2.state`; `cdc_v2.alerts`). Read `docs/design/destination.md` completely before you
write anything: its contract notes say what D found on MotherDuck.

## Read as well

- `docs/spike/run_semantics.md` completely (what the platform really does: overlap, schedule timing, cancel,
  maximum run time, the process model, the hard exit, take-over timing), `docs/spike/md_semantics.md` (clock,
  conflicts, late commits, interrupting server connections), `docs/spike/flight_platform.md` (how a Flight is
  packaged and what its environment is). The spike Flights under `flights/spike_*` and `src/cdc_v2/flightctl.py`
  show working calls.
- `docs/adr/0001-architecture.md`.
- `docs/testing/harness.md`: crash points, fault sites, process control (`procs`, `Fleet`), the crash runner.
- Design open points 16 and 17.

## Resources

MotherDuck through `tools/with-env` only; databases `cdc_v2_t_l_*` only. No Postgres, no JVM. No Flight is
created by this package: `MotherDuckPlatform` is written from the spike's findings and tested against the real
Flight functions only in read-only ways that need no Flight of ours (say what you could not run; stage 1's T2
wave runs it for real). The account is shared with other workers.

## What to build

### 1. The bootstrap, `flights/cdc_v2/main.py`

One file that runs before anything of the repository can be imported (design section 5), with no dependency
beyond the `duckdb` client. In order:

1. **Guard decision** (design 4.4), before the clone, the JVM, the slot or any DDL other than the leader
   tables: read the leader record; healthy ⇒ exit (0 for a run of the same Flight; alert and non-zero for a
   different Flight); `released` or not healthy ⇒ take over. "Healthy" is decided on MotherDuck's clock only
   (design 4.1), inside the statement that reads the record.
2. **Leader tables at the first run.** The bootstrap carries a copy of the statements of `cdc_v2.md.schema`
   (D delivers the T0 helper that compares the two character for character; call it in a test). Several first
   runs at once end with one row set (open point 16: D proved the statements; you prove the bootstrap's use of
   them with ≥ 3 simultaneous real starts, many repetitions).
3. **Take-over** (design 4.5): the three updates in one transaction, exactly one changed leader row.
   - Exactly one of several simultaneous starters wins; a loser sees zero rows or a conflict, reads the record
     again and exits by the guard rule.
   - A conflict with an open transaction of the previous leader: interrupt the server connections of earlier
     epochs listed in `_cdc.connections` (ids from that table only — D provides the function; the bootstrap
     carries what it needs of it) and try again, until it wins, or a healthy heartbeat appears, or
     `leader.takeover_timeout` (default 45 s) has passed; then exit non-zero.
   - A take-over whose commit outcome is unknown is never repeated blindly: read the record again after more
     than 2 s and decide by `run_id`.
   - After winning: cancel the stored previous run through the platform if it is still running — that run
     number only, never "all other runs" (finding 8 of the run spike).
   - The alert `takeover` (previous run, age of its heartbeat, reason), unless the previous leader had released.
     `_cdc.alerts` belongs to D's migrations and may not exist yet at a first run, and a contending Flight that
     must raise `contending_flight` is not the leader: decide how both alerts are recorded without breaking the
     rule that only a leader migrates, write it down, and test it.
4. **Start-up heartbeat**: a thread that writes `phase = 'starting'` every interval until the pipeline's first
   tick, bounded by `run.startup_timeout` (default 180 s); past it the bootstrap exits non-zero.
5. **Clone and artifacts**: the repository at the pinned commit, the pinned JRE and jars
   (`src/cdc_v2/artifacts.py` exists). For local tiers a local source path replaces the clone (C6) and the
   artifact step can be switched off. No secret appears in an argument, a log line or an error message.
6. **Start the pipeline** with `start_new_session=True`, passing the epoch, the run identity and the tick pipe.
7. **Watch**: read ticks; use no network and no MotherDuck connection from here on. No tick for the hard-exit
   age (default 60 s) ⇒ `SIGKILL` to the pipeline's process group, then `os._exit(1)`. Pipeline exit ⇒ exit with
   its status. The watch must not be able to hang itself: no lock shared with a thread that can block, no
   blocking read without a timeout, and it works when the pipeline is frozen (SIGSTOP), when the pipe is full
   and when the pipeline has closed the pipe without exiting.

Everything with a deadline (C0); every timing a setting, with the order checked when the settings are built
(interval < stale age / 3, stale age < hard-exit age), so that tests run the protocol in seconds. The measured
point of "a newcomer exits within 10 s" is the first line of `main.py` (open point 17): log a monotonic time
there and at exit, and report the distribution.

The bootstrap carries its own copy of the leader machine (states `leading`, `released`; C5 names this
exception); a T0 test compares it with the declaration in the package.

### 2. In the pipeline: `src/cdc_v2/leader/`

- **Heartbeat** (design 4.3): a dedicated connection and thread; the statement with `WHERE epoch = :my_epoch`;
  its only input is `supervisor.progress() -> (ok, phase, progress_at)` (C11; package S delivers the supervisor,
  you build against the interface with a stand-in of your own). No progress ⇒ no heartbeat. Zero rows changed ⇒
  another run leads: stop every lane and exit non-zero. After each successful heartbeat a tick to the bootstrap.
  A heartbeat statement that hangs must not keep the process alive past the hard-exit age: it has a deadline,
  and a missed tick is what ends the run.
- **Reaction to `FenceLost`** anywhere: one function that every component calls; it stops all lanes and ends
  the process, non-zero, without writing anything more.
- **Planned hand-over** (design 4.6): at `run.max_runtime` ask the stream to finish the group in flight (an
  interface S will implement: `finish_current_group(deadline)`; an oversized transaction that is still arriving
  is abandoned, not waited for), then set `state = 'released'` in a fenced `control`-lane transaction through
  `state.transition`, close, exit 0, timed to end about 5 s before a minute boundary of MotherDuck's clock.
  A release whose commit outcome is unknown: say what the next run sees in both cases and that both are safe.
- **Leader machine**: declared with `state.declare` (two states), added to `docs/state_machines.md` through the
  declaration; the enumeration helper of D runs over it (SPEC 1.9).
- **Clean-up of `_cdc.connections`** for epochs before the previous one (D provides the statement; you call it
  at the right time and say when).
- **`leader_record` view**, so that SPEC 4.2 and 6.1 can name one record.

### 3. Platform (`src/cdc_v2/platform/`, contract C6)

- `MotherDuckPlatform`: `identity()`, `run_state(flight_id, run_number)`, `cancel(flight_id, run_number)` over
  the Flight SQL functions, as the run spike used them. The bootstrap needs the same three before the clone and
  therefore carries its own small copy; keep both obviously the same and compare them in a test.
- `LocalPlatform`, the simulator for T1, driven by tests and later by the leadership fuzzer (package F). Runs are
  local process groups started from `flights/cdc_v2/main.py` with a local source path. It imitates what the
  spike measured, and every imitated behaviour cites its finding:
  - starts on a schedule (every "minute", with a configurable length of a minute and the measured jitter) and on
    demand; starts overlap freely;
  - `cancel`: the record says cancelled at once, the processes are killed about 2.5 s later, without a signal
    they could catch;
  - the maximum run time is enforced late (a configurable delay);
  - a run ends when its main process exits, and then **every process the run started is killed, in any session
    or process group** (the real platform destroys the container); a test proves it for a pipeline that started
    its own session and for a grandchild that called `setsid`;
  - run records (`PENDING`, `RUNNING`, terminal states) that `run_state` reads, also from another process.
  - It signals only processes it started (README), finds them without `pkill`, and leaves nothing behind when
    the test that owns it fails.
- A stand-in pipeline for your tests (`tests/leader/toy_pipeline.py`): it ticks, commits a counter and a
  position through a `stream` lane, can be told to stop making progress, to hang in a commit, to ignore
  `FenceLost` (to show that the fence, not good behaviour, is what protects), and has D's lane crash points.

## Tests

T0 (default run; local DuckDB, timings in tenths of seconds):

- Guard decisions for every state of the record (absent tables, `released`, stale, healthy same Flight, healthy
  other Flight, a heartbeat exactly at the limit).
- Take-over from threads at once: exactly one winner, every repetition.
- The watch: frozen pipeline, closed pipe, full pipe, pipeline exit codes passed on.
- Settings order; the copies (tables, machine, platform calls) equal to the package's.
- Heartbeat stops without progress; zero rows ⇒ exit path; hand-over timing arithmetic.

Marker `md` (real MotherDuck, `LocalPlatform`, real processes) — the gate tests of SPEC 4.2 that are yours:

- **3 simultaneous starts** (and 5, and 9: the spike saw up to 9 runs at once): one leader, the others exit in
  time with the right code; many repetitions; also on a database with no `_cdc` at all.
- **SIGSTOP / SIGCONT of the leader across a take-over**: the frozen leader's every later write is rejected, it
  sees the newer epoch and ends itself; the counter-and-position table shows exactly-once. Freeze at each lane
  crash point (`<lane>.fenced`, `<lane>.commit_sent` with `kill_later` and with freeze, `<lane>.committed`), at
  random times, and with the freeze longer and shorter than the stale age.
- **Kill of the leader** (SIGKILL of the pipeline, of the bootstrap, of both): replaced within 90 s plus
  start-up at the default timings — measure with scaled timings, and once with the real ones.
- **A hung component with a live process**: the pipeline stops reporting progress but stays alive; the heartbeat
  stops, the next run takes over, the old run ends by its own hard exit, and it cannot commit in between.
- **A leader that hangs inside a commit** and holds a fence row: the take-over interrupts its connections and
  wins within the take-over deadline; without the interrupt it does not (show both).
- **Take-over commit outcome unknown** (kill at the commit of the take-over with `kill_later`; a fault site):
  the rule "read again after more than 2 s, decide by `run_id`" never yields two leaders or none.
- **Cancel is late**: the cancelled old run tries to commit during its last 2.5 s and fails on the fence.
- **Planned hand-over**: released, exit 0, the next run takes over without an alert and without interrupting
  anything; 20 consecutive hand-overs with the toy pipeline committing all the time, exactly-once at the end.
  (The 100 hand-overs under load with the oracle are run by package F on the real pipeline.)
- **A different Flight** contends: alert `contending_flight`, non-zero, nothing changed.
- **A run that never becomes healthy** (start-up exceeds its deadline): it exits non-zero, the next run takes
  over; no sequence of runs that each kill the previous one before it finished starting (design 4.3: the
  declared waits) — show the case with a slow start-up that stays below the deadline and is left alone.
- The crash runner of the harness over the toy pipeline with the real bootstrap: every crash point of the
  bootstrap and the leader code reached, 3 seeds, random kills, freeze with a second instance.
- Timings as measurements: guard decision to exit for a newcomer (distribution; the 10 s of SPEC), take-over
  duration, time from a leader's death to the next leader's first commit.

## Deliver

- `flights/cdc_v2/main.py`, `src/cdc_v2/leader/`, `src/cdc_v2/platform/`, the tests;
  `docs/design/leadership.md` (the protocol as built, every timing and its setting, what each test proves, the
  answers to open points 16 and 17, contract notes); evidence under `evidence/p3/leadership/`.
- In your report: every behaviour of MotherDuck that differed from the spike or from D's document; what
  package S must implement for you (`supervisor.progress`, `finish_current_group`, the tick, the one
  `FenceLost` reaction) and what package F's leadership fuzzer can read to check "never two writers" without
  relying on the fence itself.
