# Run semantics of MotherDuck Flights (P2 spike, PLAN §2 A item 2)

Measured on 2026-11-10 (UTC) on real **F16** Flights in us-east-1 (Business plan), DuckDB client 1.5.6. Every finding
is marked **measured** (evidence file under `evidence/p2/run_semantics/`) or **documented** (MotherDuck public
documentation, URL given). Where the two differ, the measurement wins.

- Flight sources: `flights/spike_runs/main.py` (one instrument, many modes), `guard_duckdb.py`, `guard_pgwire.py`.
- Drivers: `tools/spike/run_semantics_*.py` (one per question; `run_semantics_common.py` holds the helpers). Run
  them through `tools/with-env .venv/bin/python tools/spike/run_semantics_<q>.py`.
- Every Flight, the database `cdc_v2_spike_runs_db` and its tables were created by this spike and deleted at the end
  (`cleanup.json`; `cdc_v2_spike_runs_def` was deleted during Q10, the probe database `cdc_v2_spike_runs_clock` by its
  driver). No Flight secret was created.

How the instrument measures: every process of a run writes a row (`ticks`) every 200 ms to 1 s and `events` rows
for decisions; handlers are installed for **every catchable signal** (1–64 except SIGKILL and SIGSTOP), log the
signal on stdout and then try to COMMIT a transaction. Rows written through MotherDuck's Postgres endpoint carry
MotherDuck's own clock (`now()` evaluated by MotherDuck); rows written through the duckdb client carry the Flight
container's clock — see finding 0.

Documentation read: [Flights concept](https://motherduck.com/docs/concepts/compute/flights),
[Scheduling and runs](https://motherduck.com/docs/key-tasks/flights/scheduling-and-runs),
[MD_CREATE_FLIGHT](https://motherduck.com/docs/sql-reference/motherduck-sql-reference/flights/md-create-flight/),
[MD_CANCEL_FLIGHT_RUN](https://motherduck.com/docs/sql-reference/motherduck-sql-reference/flights/md-cancel-flight-run/),
[Packages and runtime](https://motherduck.com/docs/key-tasks/flights/packages-and-runtime),
[Monitoring and debugging](https://motherduck.com/docs/key-tasks/flights/observability/monitoring-and-debugging),
[Flight sizes and pricing](https://motherduck.com/docs/about-motherduck/billing/flight-sizes),
[Postgres endpoint](https://motherduck.com/docs/sql-reference/postgres-endpoint/),
[MD_INFORMATION_SCHEMA](https://motherduck.com/docs/sql-reference/motherduck-sql-reference/md_information_schema/introduction/).
None of them says anything about overlapping scheduled runs, retries, cancel signals or grace periods, or what
`max_runtime_sec` does when exceeded.

## Findings that contradict SPEC 4.2 / 5.2 or force a design choice

1. **A cancelled run keeps running for about 2.3 s and can still commit.** `MD_CANCEL_FLIGHT_RUN` marks the run
   `CANCELLED` 0.5–0.6 s after the call, but its processes (main, child, detached grandchild) keep running and
   committing until **2.28–2.51 s** after the call (n = 4; 12–14 committed rows per process after the call). No
   catchable signal arrives first: they are killed outright. A `CANCELLED` status therefore does **not** mean the
   old leader has stopped writing. The fencing epoch inside every destination transaction (SPEC 4.2) is mandatory,
   not an extra; the new leader must not assume the old one is gone when the record says so (Q4).
2. **No graceful shutdown on any platform stop.** Cancel, `max_runtime_sec`, deleting the Flight and the teardown
   after `main.py` exits all kill without SIGTERM. The pipeline gets no chance to release the leader record or to
   flush; every stop must be treated as a crash (Q4, Q5, Q7).
3. **`max_runtime_sec` is enforced late and coarsely.** With 60 s, runs were killed 65–118 s after the limit
   (n = 8, clustered at the same wall-clock instants, so a periodic sweep); with 180 s, 3.3 s after the limit. The
   final state is `CANCELLED` with `ended_at` and `exit_code` NULL, the same as a manual cancel; no retry, no alert.
   Any value from 0 to 604800 is accepted (0 = no timeout), including more than 8 h. The planned hand-over before
   `run.max_runtime` must stay a leader decision; the platform limit is only a coarse last resort (Q5).
4. **Deleting a Flight does not stop its active runs**, and afterwards they cannot be read or cancelled ("Flight not
   found"); they were killed about 123 s later by the same kind of sweep. Clearing the schedule does not stop them
   either. Operations must cancel runs **before** deleting a Flight (Q10).
5. **An out-of-memory kill in any child kills the whole run** (`memory.oom.group` = 1): the run is `FAILED` with
   exit code 137 and nothing in the log, exactly like an OOM in `main.py`. A memory-hungry pipeline child cannot be
   "restarted by the supervisor" after an OOM (SPEC 4.5); the next scheduled run takes over instead (Q6).
6. **A frozen bootstrap is bounded, but only by the next run's guard.** If everything in the run is frozen, nothing
   inside it can act; the next scheduled run sees a stale heartbeat, takes over and cancels the frozen run (measured
   31.5 s from freeze to the frozen run's end in one take-over; worst case ≈ 98 s plus 2.5 s kill, see the closing
   section). Without a guard, `max_runtime_sec` bounds it, late (finding 3). This works **because overlapping
   scheduled starts are not skipped** (Q1).
7. **The duckdb client evaluates `now()` locally.** `INSERT … now()`, `DEFAULT now()` and
   `get_current_timestamp()` through the duckdb client all carry the client's clock (within 1 ms of the client clock,
   against a 100 ms round trip). A heartbeat age computed with `now()` therefore compares the clocks of two
   containers. Inside us-east-1 the Flight clock was within 2–38 ms of MotherDuck's clock (median 3 ms, n = 1,934),
   so the 30 s health threshold is not at risk, but "a single clock for heartbeat age" needs the Postgres endpoint or
   a server-side mechanism (Q4 evidence; local probe in finding 0).
8. **Do not use `MD_LIST_FLIGHT_RUNS` to find "the run to cancel".** Overlapping runs are normal (up to 9 at once
   were measured), a newcomer of the next minute is `RUNNING` while the guard runs, and a pending scheduled run is not
   listed at all. The guard must cancel exactly the run number stored in the leader record (Q2, Q9).

None of these breaks R18 or the 90 s replacement target; findings 1, 2 and 5 change how the guard, the supervisor
and fencing must be built.

## Finding 0: clocks

*Measured* (`clock_probe_local.json`, driver `run_semantics_clock.py`, 10 statements per method from this machine).
Through the duckdb client, `INSERT … now()`, `DEFAULT now()` and `get_current_timestamp()` stored a value within
1 ms of this machine's clock taken just before the statement, although each statement took ≈ 100 ms: the duckdb client
evaluates `now()` locally. Through MotherDuck's Postgres endpoint the stored `now()` was 66 ms after the client
clock (median; round trip 136 ms), i.e. evaluated at MotherDuck. Inside a Flight, rows written through the endpoint
had `server_ts − container clock` of 2–38 ms (median 3 ms; n = 1,934; `cancel_same_flight.json` →
`victim.server_minus_client_clock_s`): Flight containers and MotherDuck agree within a few milliseconds.

## Q1 Overlap: start, skip or queue?

*Measured* (`overlap.json`, driver `run_semantics_overlap.py`). `cdc_v2_spike_runs_overlap` scheduled `* * * * *` for
13 minutes, each run lasting 335 s. **Every minute started a new run while earlier runs were active**: 13 scheduled
runs in 13 consecutive minutes, none skipped, none queued (each started 0.3–2.8 s after its minute). Up to **6
scheduled runs** ran at once, and **9 in total** when three on-demand `MD_RUN_FLIGHT` calls were made while 6 were
active; the on-demand runs started 1.7–2.5 s after the call, exactly as with no other run active. Clearing the
schedule left the 5 active runs running (cancelled by the driver afterwards). *Documented*: "Concurrent runs:
Unlimited" on Business ([Flights](https://motherduck.com/docs/concepts/compute/flights)); nothing on overlap.

For SPEC 4.2: the "start" case. Every scheduled minute produces a run, whatever is running, so a hung or frozen
leader is always examined by a newcomer within a minute; the guard must make every non-leader exit at once, and
nothing in the platform limits the number of overlapping runs.

## Q2 Schedule timing

*Measured* (`schedule_timing.json`, driver `run_semantics_schedule.py scheduled`; `pending_record_probe.json`,
driver `run_semantics_pending.py`; also `overlap.json`). Two guard Flights scheduled `* * * * *` side by side
for 34 minutes (02:36–03:10 UTC): **68 scheduled runs, no missed and no duplicated minute** on either Flight (34 each),
every one `SUCCEEDED`.

| Delay from the minute boundary (68 runs) | min | median | p95 | max |
|---|---|---|---|---|
| to `started_at` | 0.26 s | 1.05 s | 3.18 s | 12.50 s |
| to the first line of our code | 1.43 s | 3.80 s | 14.92 s | 20.94 s |
| … duckdb guard only (34) | 2.26 s | 4.32 s | 5.85 s | 20.94 s |
| … Postgres-endpoint guard only (34) | 1.43 s | 3.04 s | 14.92 s | 18.52 s |

`started_at` → first line is the container plus `uv` (and, for the duckdb guard, the 20.6 MiB `duckdb` install): median
3.19 s (duckdb) and 1.96 s (no requirements) at the minute, against 3.18 s and 0.93 s on demand at other times. Three of
68 scheduled runs needed 14–19 s before our first line (`started_to_first_line_s` in `schedule_timing.json`): start-up
on the minute is slower and has a tail, presumably because many schedules fire at :00. A newcomer's guard decision
therefore comes 2–21 s after the minute (median ≈ 4 s).

**When the record appears.** A scheduled run's `created_at` is about 20–22 s before its minute (all 13 runs in
`overlap.json`: −20.5 to −21.9 s), but the record is **not visible** then: `MD_GET_FLIGHT_RUN` on the next run
number returns "Run N does not exist" until about a second before the minute, then `PENDING` for an instant, and
`MD_LIST_FLIGHT_RUNS` (limit 1000) does not list the run until it is `RUNNING` (`pending_record_probe.json`, two
minutes; no poll of `overlap.json` ever saw a scheduled run as `PENDING`). In the 34-minute run, polling every 2 s,
the list showed 5 of 68 runs as `PENDING`, each **after** its minute (0.6–1.4 s, waiting for a container), and no run
before its minute (`first_seen_minus_scheduled_s` ≥ 0.6 s in `schedule_timing.json`). The first observation in
`docs/spike/flight_platform.md` (a record listed as `PENDING` 12 s before its minute, right after the schedule was
set) was the first run after setting the schedule; in steady state that does not happen.

How the guard tells "the next scheduled run that has not started" from "a run that must be cancelled": it never
needs to. It cancels only the run number recorded in the leader record (the previous leader), after checking with
`MD_GET_FLIGHT_RUN` that this run is `RUNNING`. A `PENDING` or not-yet-visible run is never the leader. Never cancel
"every other active run": the newcomer of the next minute may already be `RUNNING`.

## Q3 The short run, and the cost of the every-minute schedule

*Measured* (`guard_variants.json`, driver `run_semantics_schedule.py variants`; `schedule_timing.json`). The guard
reads one row of `guard_leader` in `cdc_v2_spike_runs_db`, sees a heartbeat younger than 30 s (kept fresh by the
driver) and exits 0.

| Variant (on demand, n = 5 each) | whole run `started_at` → `ended_at`, median (min–max) | `started_at` → first line | in our code | connect |
|---|---|---|---|---|
| duckdb 1.5.6, `md:<db>?attach_mode=single` | 5.34 s (5.15–6.91) | 3.18 s | 2.08 s | 1.51 s |
| same, `os._exit(0)` at the end | 5.52 s (5.32–6.55) | 3.28 s | 2.11 s | 1.54 s |
| duckdb 1.5.6, plain `md:` | 5.31 s (3.84–5.61) | 3.10 s | 2.13 s | 1.56 s |
| **no requirements, stdlib Postgres-wire client** | **1.55 s (1.45–2.46)** | 0.93 s | 0.56 s | 0.35 s |
| same, `os._exit(0)` at the end | **1.45 s (1.39–1.51)** | 0.93 s | 0.49 s | 0.30 s |

What helped: **dropping the `duckdb` requirement** (uv installs it on every run: ≈ 2.2 s before our first line) and
reading through MotherDuck's documented [Postgres endpoint](https://motherduck.com/docs/sql-reference/postgres-endpoint/)
(TLS, user `postgres`, the token as password, `options=--attach_mode=single`) with a 150-line standard-library client
(`flights/spike_runs/guard_pgwire.py`): 3.6× shorter. `attach_mode=single` and skipping interpreter teardown made
no measurable difference.

**Scheduled, 34 runs each** (`schedule_timing.json` → `summary`):

| Guard | whole run median | mean | p95 | max | 1,440 runs/day at $0.60/h (mean) | per 30 days |
|---|---|---|---|---|---|---|
| duckdb 1.5.6 | 5.53 s | 6.06 s | 6.97 s | 21.35 s | **$1.46/day** (2.43 h) | $43.7 |
| Postgres endpoint, no requirements | 2.46 s | 3.04 s | 6.59 s | 14.83 s | **$0.73/day** (1.22 h) | $21.9 |

The scheduled runs are longer than the on-demand ones (2.46 s against 1.55 s median for the Postgres guard) because
start-up at :00 is slower (Q2); the in-code part is the same (0.47 s and 2.21 s median).

**Billing granularity and minimum charge** are not visible anywhere. *Documented*: "Flights are billed for the
time each run takes, at the hourly rate for the Flight's size", F16 tier 1 $0.60/h
([pricing](https://motherduck.com/docs/about-motherduck/billing/flight-sizes)); no granularity, minimum or
definition of "the time each run takes". *Measured*: no MotherDuck SQL function mentions billing, usage, cost,
credit or compute (`billing_visibility.json`, 76 `MD_*` functions); the documented `MD_INFORMATION_SCHEMA` views
contain none either (not queried, PLAN §9). The cost figures assume per-second billing of `started_at` →
`ended_at`; if MotherDuck bills a per-run minimum, the every-minute schedule costs 1,440 × that minimum per day.

## Q4 Cancel from inside

*Measured* (`cancel_same_flight.json`, `cancel_same_flight_2.json`, `cancel_same_flight_3.json`,
`cancel_cross_flight.json`, `cancel_self.json`; driver `run_semantics_cancel.py`).

- **It works.** Run B, using only `MOTHERDUCK_TOKEN`, `MOTHERDUCK_FLIGHT_ID` and `MOTHERDUCK_FLIGHT_RUN_NUMBER`,
  listed its Flight's runs and cancelled run A: `MD_CANCEL_FLIGHT_RUN` returned `canceled_count = 1` in 0.67–0.76 s.
- **Timing** (n = 4): A's record shows `cancelled_at` 0.53–0.61 s after the call; A's processes write their last row
  (MotherDuck clock) **2.28–2.51 s** after the call, all of them within 0.2 s of each other.
- **Signal**: none. Handlers were installed for every catchable signal in main, child and grandchild; the only signal
  ever logged was the SIGCHLD of the grandchild's double fork, long before the cancel. No SIGTERM, SIGINT, SIGHUP or
  SIGPWR arrived; the processes were killed (SIGKILL or container destruction). **No grace period** that a program
  could use.
- **Transactions after the cancel**: yes. Every process committed 12–14 rows after the call (each an
  autocommitted INSERT), the last ≈ 1.8 s after the record said `CANCELLED`.
- **Children**: the child (same process group) and the detached grandchild (double fork + `setsid`, re-parented to
  PID 1) died at the same moment as main. The container is destroyed as a whole.
- **Self-cancel**: works (`cancel_self.json`): the call returned after 0.57 s, the run saw itself `CANCELLED` and
  kept running another 0.2 s until its own normal exit; the record stayed `CANCELLED` (not overwritten by exit 0).
- **Across Flights**: a run of `cdc_v2_spike_runs_cancel` cancelled a run of `cdc_v2_spike_runs_cancel2` (same
  account): works, same timing (last row 2.51 s after the call).

For SPEC 4.2: "kills the previous run — cancels it through the MotherDuck API" is available from inside a Flight,
but the previous run can still write for ≈ 2.5 s after the call and for ≈ 2 s after its record says `CANCELLED`; the
epoch check in every destination transaction is what keeps it out. A different Flight of the same account can
cancel our runs too (relevant to the "contending Flight" alert).

## Q5 Maximum run time

*Measured* (`max_runtime_limits.json`, `max_runtime_exceed.json`, `max_runtime_stagger_60s.json`,
`watchdog_frozen_bootstrap.json`; driver `run_semantics_maxruntime.py`).

- **Accepted values**: every value from 0 to 604,800 s (7 days), including 1 s and values above the 8 h default; −1
  is rejected (UINT32). The value is stored per Flight version. *Documented*: "`0` means no timeout"; "up to 8
  hours by default on Business"
  ([packages and runtime](https://motherduck.com/docs/key-tasks/flights/packages-and-runtime)). Whether a run really
  survives past 8 h with a larger value was **not measured** (it needs an 8-hour run).
- **What happens when a run exceeds it**: the run becomes `CANCELLED`, `ended_at` and `exit_code` NULL — the same
  record as a manual cancel. No signal reaches any process (handlers in main, child, grandchild logged nothing);
  processes write until 0–1.8 s after `cancelled_at`, then all die together, detached grandchild included.
- **How exact**: not at all at small values. 60 s limit: killed **65.6–117.5 s late** (n = 8: 82.8 and 79.7 s in
  `max_runtime_exceed.json`; 73.3, 117.5, 100.5, 82.5, 65.6, 90.5 s in the stagger run). Runs started 17 s apart were
  killed at the **same instant** (four at 02:44:59.2–.8), so the limit is enforced by a periodic sweep, not a per-run
  timer. 180 s limit with everything frozen: killed 3.3 s late.
- **Alert or retry**: neither. No new run appeared in the 3 minutes after (`extra_runs_not_started_by_driver: []`);
  no alert mechanism is documented
  ([monitoring](https://motherduck.com/docs/key-tasks/flights/observability/monitoring-and-debugging) says a
  cancelled run means "someone clicked Cancel in the UI, or the run hit a timeout").

For SPEC 4.2 / PLAN §7: keep `run.max_runtime` (7 h 30 min) as the leader's own planned hand-over; set the Flight's
`max_runtime_sec` well above it (it can be raised; the default 28,800 s leaves 30 min) as a coarse backstop only.

## Q6 Retries

*Measured* (`retries_ondemand.json`, `retries_scheduled.json`; driver `run_semantics_retries.py`).

| Failure | Run record | Log |
|---|---|---|
| `sys.exit(3)` | `FAILED`, exit 3 | normal |
| uncaught exception | `FAILED`, exit 1 | traceback |
| OOM in a child process (allocating past 16 GiB) | `FAILED`, **exit 137** | nothing after the last allocation; main.py's own "child exited" line never appears |
| OOM in main.py | `FAILED`, exit 137 | nothing after the last allocation |
| cancelled / `max_runtime_sec` | `CANCELLED`, exit NULL | nothing (Q4, Q5) |

- **No retry**: in 4 minutes after four failed on-demand runs no run appeared that the driver had not started; with
  `* * * * *` and every run failing, exactly one run per minute (4 minutes, 4 runs). Cancelled and timed-out runs
  (Q4, Q5) were not retried either. **No retry setting exists**: the documented `MD_CREATE_FLIGHT` parameters are
  name, source_code, schedule_cron, requirements_txt, config, access_token_name, flight_secret_names, instance_type,
  max_runtime_sec, metadata ([MD_CREATE_FLIGHT](https://motherduck.com/docs/sql-reference/motherduck-sql-reference/flights/md-create-flight/)).
- **OOM kills the whole cgroup**: `memory.oom.group` is 1 and `memory.max` 17,179,869,184 (16 GiB); `memory.current`
  reached 16.29 GB before the kill. The run ended 5.3–6.0 s after the last log line (the kill comes during the next 1 GiB allocation). *Documented*: OOM-killed runs fail "with
  little or no log" ([monitoring](https://motherduck.com/docs/key-tasks/flights/observability/monitoring-and-debugging)).

For SPEC: the every-minute schedule is the retry mechanism; there is no other. SPEC 5.4's memory caps must keep every
child (JVM included) well below 16 GiB in total, because one child's OOM ends the run.

## Q7 Process model

*Measured* (`process_orphan_pipe.json`, `process_orphan_devnull.json`, `process_silent.json`, `watchdog_variants.json`;
driver `run_semantics_process.py`).

- The process tree of a run: PID 1 is the platform's runtime (`tower-runtime-e…`), its child `uv`, whose child is
  `python main.py`; `uv` and `main.py` share one process group (`process_table_at_main_exit`). A `killpg` of
  `main.py`'s own group would hit `uv` too: start the pipeline in its own session.
- **The run ends when `main.py` exits**, whatever else is alive: the record turns terminal with `main.py`'s exit
  code (0 → `SUCCEEDED`, non-zero → `FAILED`) and the container is destroyed with every process in it — the child and
  the detached grandchild stopped writing at that moment, without any signal.
- **The 5-second wait**: when a child or grandchild still holds the run's stdout, the run ends **5.1 s** after
  `main.py` exits (orphan with inherited stdout 5.13 s; all four watchdog variants 5.08 s); when they write to
  `/dev/null`, **0.19 s**. The platform waits up to ≈ 5 s for the output pipe to close, then kills everything.
- **Silence**: a run that printed nothing for 364 s (database rows only) ran to its end and `SUCCEEDED`; the platform
  does not react to a run without output.

For SPEC 4.2: `main.py` exiting is a reliable way to end the whole run — no process survives it. The hard exit
needs no cleanup logic for leftovers; killing the pipeline first only saves the 5 s pipe wait.

## Q8 Heartbeat hard-exit

*Measured* (`watchdog_variants.json`, `watchdog_frozen_bootstrap.json`; driver `run_semantics_watchdog.py`). The
bootstrap (`main.py`, no database connection) starts the stand-in pipeline in its own session; the pipeline writes a
heartbeat row every 10 s, then touches a local file, and starts a grandchild in its own group that writes every
second. A helper in a third session writes every second and sends SIGCONT to the pipeline group 100 s after the
freeze. At 30 s the pipeline SIGSTOPs its whole process group; the bootstrap checks the file's age every second and
hard-exits above 60 s.

| Way to end the run | Run record | freeze → hard exit | hard exit → run terminal | freeze → run terminal | processes writing after the end |
|---|---|---|---|---|---|
| `os._exit(1)` of the bootstrap alone | `FAILED`, exit 1 | 60.5 s | 5.08 s | 65.6 s | none |
| `os._exit(0)` alone | `SUCCEEDED`, exit 0 | 60.3 s | 5.08 s | 65.4 s | none |
| SIGKILL to the pipeline group, then `os._exit(1)` | `FAILED`, exit 1 | 60.3 s | 5.08 s | 65.4 s | none |
| same, `os._exit(0)` | `SUCCEEDED`, exit 0 | 60.4 s | 5.08 s | 65.4 s | none |

All four are reliable: the frozen processes never ran again (the helper's SIGCONT never came — the container was
gone), and the unfrozen helper stopped at the run's end. Recommended: SIGKILL the pipeline's process group (and any
other session the bootstrap started), then `os._exit(1)`. The non-zero code makes the run `FAILED` and visible
(`os._exit(0)` would record a hard exit as `SUCCEEDED`). Killing first lets the run end without the 5 s pipe wait
when no other process holds stdout. **65.4–65.6 s ≤ 90 s: SPEC 4.2 "exited within 90 s" holds** with the 60 s
threshold and a 1 s check. SIGSTOP cannot be caught or delayed, and the pipeline froze immediately after a heartbeat (60 s counts from the
last heartbeat), so this is the worst case for a frozen pipeline; a freeze later in the 10 s cycle ends the run up to
10 s sooner.

**The bootstrap itself frozen** (`watchdog_frozen_bootstrap.json`): the pipeline SIGSTOPs `main.py` and then its
own group, so nothing in the run can act. On a Flight with `max_runtime_sec = 180`, the run was cancelled 183.3 s
after it started (147 s after the freeze) and the unfrozen helper died 1.8 s later. Nothing inside a run bounds that
case. What bounds it is (a) the **next scheduled run's guard**, which sees the stale heartbeat, takes over and
cancels the frozen run — measured in Q9 (`freeze_all`): 30.7 s from freeze to take-over, 31.5 s to the frozen run's
end — and (b) `max_runtime_sec` as a coarse backstop (up to ≈ 2 min late, Q5). Because overlapping scheduled
starts are not skipped (Q1), (a) always applies.

## Q9 Take-over timing, end to end

*Measured* (`takeover.json`, driver `run_semantics_takeover.py`). Flight `cdc_v2_spike_runs_leader` scheduled every
minute; a stand-in leader record (run number, epoch, heartbeat written with `now()`, state); guard threshold 30 s;
take-over = compare-and-swap of the epoch (`UPDATE … WHERE epoch = old RETURNING`), then cancel the previous run
number if it is `RUNNING`; guard runs install `duckdb` (≈ 5–7 s each).

| Scenario | Event → new leader holds the record | Detail |
|---|---|---|
| Planned: leader releases 5 s before a minute boundary | **10.4, 12.4, 15.3 s** | released at :55.02 each time; the next minute's run took over 4.5–8.3 s after its own start |
| Leader cancelled from outside (`MD_CANCEL_FLIGHT_RUN`) at :23.4 | **43.3 s** | next minute's guard saw age 50.0 s → took over; the cancelled run had stopped (`previous_not_running`) |
| Pipeline child SIGKILLed at :49.1, bootstrap exits at once | **76.8 s** | run ended 0.19 s after the kill (`FAILED`, exit 1); the next minute's guard saw age 15.7 s (healthy) and exited, the one after took over at age 76.8 s |
| Whole run frozen (bootstrap included) at :36.8 | **30.7 s**; frozen run ended 31.5 s after the freeze | next minute's guard saw age 30.7 s → took over and cancelled the frozen run (`cancelled_previous`) |

The 7 guard runs that met a healthy leader exited 0 after 4.2–7.2 s. Every take-over raised the epoch by one (10 take-overs, epochs 1–10 in order); no run took over while a
healthy leader existed.

Worst case of an unplanned replacement: the heartbeat becomes stale 30 s after the last refresh (the last refresh
is up to 10 s before the death); the first minute boundary at which a newcomer's guard read happens after that
point takes over. Replacement after death ≤ 30 s + 60 s + guard decision time (4.2–8.3 s after the run's start with duckdb, measured here; ≈ 1.5 s with the
Postgres endpoint), i.e. **≤ ≈ 98 s with duckdb**, plus the start-up of the new leader. The die_child case (76.8 s)
was close to that bound. SPEC 4.2's "a dead or hung leader is replaced within 90 s plus start-up" holds when
"start-up" includes the guard's own decision (4–8 s with duckdb).

## Q10 Definition changes under a running Flight

*Measured* (`defchange.json`, `defchange_after_delete.json`; driver `run_semantics_defchange.py`).

- **`MD_UPDATE_FLIGHT` (new source v2 and new config) while run A is active**: A kept running on version 1 and its
  old config (71 rows tagged `main:v1` after the update). The next scheduled run used **version 2 and the new
  config** (`start` event `version: v2`, rows `main:v2`, `flight_version` 2). *Documented*: "the in-progress run
  finishes against the version it started with; only the next run picks up the updated source"
  ([scheduling and runs](https://motherduck.com/docs/key-tasks/flights/scheduling-and-runs)).
- **Clearing the schedule** while A and the scheduled run S were active: both continued (31 and 32 rows after the
  clear; also seen in Q1).
- **Deleting the Flight** while A and S were active: `MD_DELETE_FLIGHT` succeeded, `MD_LIST_FLIGHT_RUNS` then failed
  with `Flight "…" not found`, but **both runs kept writing for 122–123 s** after the delete and then stopped at the
  same instant, without their normal end (their 240 s were not over). They could not be cancelled in between: the
  Flight no longer existed for the API.

For SPEC / operations: a configuration change (a new pinned commit for the bootstrap, a new config) takes effect at
the next run without disturbing the leader; the hand-over then happens through the guard as usual. To stop the
pipeline: clear the schedule, cancel the active runs, and only then delete.

## Consequences for SPEC 4.2 and 5.2

**Can the guard as specified work? Yes.** The platform starts every scheduled minute regardless of active runs
(Q1), a run can cancel another run of its Flight with nothing but its own environment (Q4), there are no platform
retries that could start a second leader (Q6), and ending `main.py` reliably destroys everything in the run (Q7).
What the measurements force on the design:

1. **Fencing is mandatory.** A cancelled leader can commit for ≈ 2.5 s after the cancel call (Q4). The epoch check
   in every destination transaction (already in 4.2) is the only thing that keeps it out; the guard must not wait for
   `CANCELLED` and assume silence.
2. **No graceful stop exists**: cancel, timeout, delete and teardown all kill without a signal (Q4, Q5, Q7, Q10).
   Releasing the record happens only on the leader's own planned exit; every other end is a crash, and the next
   leader treats it as one.
3. **The guard cancels exactly the previous leader's run number** from the leader record, after `MD_GET_FLIGHT_RUN`
   shows it `RUNNING` — never "all other active runs" (overlaps are normal, pending runs are invisible) (Q1, Q2).
4. **The bootstrap hard-exit works**: the frozen pipeline's run ends 65.4–65.6 s after the freeze, within the 90 s
   of 4.2 (Q8). Use SIGKILL of the pipeline group, then `os._exit(1)`.
5. **A pipeline child's OOM ends the whole run** (Q6): the supervisor (4.5) cannot restart it; the next scheduled
   run takes over. Memory caps must hold for the sum of all processes.
6. **`max_runtime_sec` is a coarse backstop**, up to ≈ 2 min late (Q5). Keep the leader's own `run.max_runtime`
   (7 h 30 min) for the planned hand-over and set `max_runtime_sec` above it (accepted up to 7 days; enforcement
   above 8 h not measured).
7. **Heartbeat age uses two containers' clocks** through the duckdb client (`now()` is client-side); measured offset
   ≤ 38 ms, harmless at a 30 s threshold. If the leader protocol needs one clock, read `now()` through the Postgres
   endpoint.

**Measured worst-case replacement time of a frozen run.** A frozen *pipeline* ends its own run ≤ 60 s + 1 s (check)
+ 5.1 s (pipe wait) after the freeze: **measured 65.4–65.6 s** for a freeze immediately after a heartbeat, the worst
phase. A frozen *whole
run* (bootstrap included) is replaced by the next scheduled run: ≤ 30 s (stale) + ≤ 60 s (next minute) + guard read
(≤ 8.3 s duckdb, ≈ 1.5 s Postgres endpoint) = **≤ ≈ 98 s** with duckdb, and the frozen run is gone ≈ 2.5 s after the
cancel (measured 30.7 s / 31.5 s for one favourable phase). Either way a newcomer holds the record at most ≈ 98 s
after the death; SPEC 4.2's "replaced within 90 s plus start-up" holds if "start-up" counts the guard's read. Only if
there were no guard at all would `max_runtime_sec` bound it, late by up to ≈ 2 min.

**What the every-minute schedule costs.** Measured over 34 scheduled runs per variant, priced at the documented F16
rate of $0.60/h and assuming per-second billing of `started_at` → `ended_at`: **$1.46 per day (≈ $44 per 30 days)**
for a guard that installs `duckdb`, **$0.73 per day (≈ $22 per 30 days)** for the guard that reads the leader row
through the Postgres endpoint with no requirements. The leader's own run (24 h/day of F16 = $14.40/day) is not part of
this figure. Billing granularity and any per-run minimum are not documented or visible (Q3); a per-run minimum
of m seconds would cost 1,440 × m / 3600 × $0.60 per day instead (e.g. $14.40/day for a 60 s minimum).

For SPEC 5.2: a hand-over (planned) leaves the destination without a leader for 10–15 s plus the new leader's
start-up (JVM, clone, slot — measured by the JVM spike); unplanned replacements take up to ≈ 98 s plus start-up.
Both are excluded from the latency statistic by R19 but must be reported; "back under 30 s within 120 s" after a
hand-over is a start-up budget question for the JVM spike.

## Open points

- `max_runtime_sec` above 28,800 s is accepted; that a run really survives past 8 h was not measured.
- Billing granularity and any minimum charge per run are not documented and not visible through SQL; the cost
  figures assume per-second billing of `started_at` → `ended_at`. Ask MotherDuck or read an invoice.
- A mutual watchdog (the pipeline checks that `main.py` is not stopped, via `/proc/<ppid>/stat`) would let a frozen
  bootstrap end its own run; not built here, because the next run's guard already bounds that case.
- The guard measured here reads through the Postgres endpoint without the `duckdb` package; the real guard also
  needs to take over (CAS, cancel, clone) — that path may install `duckdb` after the decision, so only the healthy
  path (1,440 times a day) needs to be short.
