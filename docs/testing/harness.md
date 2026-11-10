# Crash-point and fault-injection harness

The machinery that proves SPEC 1.7, 4.2, 4.5, 4.6 and the fault, stream and leadership fuzzers of SPEC §4 (R22). It
was built before the pipeline (PLAN §3, "tests before features") and is proven against a toy system that is correct
and one that is deliberately wrong (`tests/harness/`).

| File | What |
|---|---|
| `src/cdc_v2/crashpoints.py` | named crash points; product code calls `crash_point("stream.committed")` |
| `src/cdc_v2/faultsites.py` | run-time fault sites; product code calls `fault_site("md.commit")` |
| `src/cdc_v2/testing/procs.py` | start a system under test in its own session and group; signals; waits with deadlines; survivors |
| `src/cdc_v2/testing/crash.py` | crash runner: every crash point, random kills, freeze and continue, hang; ledger and summary |
| `src/cdc_v2/testing/faults.py` | test-side writer of the fault-site control file |
| `src/cdc_v2/testing/netproxy.py`, `tools/netproxy` | TCP fault proxy, ports 55440–55459 |
| `src/cdc_v2/testing/pipefaults.py` | byte-stream fault relay for the engine's frame pipe |
| `src/cdc_v2/testing/harness_evidence.py` | the one function that writes evidence files (scrubs, then fails closed) |
| `tests/harness/` | toy system under test, its adapter, and the self-tests |

## 1. Crash points

### Declaring and calling a point

```python
from cdc_v2.crashpoints import crash_point, declare

declare("stream.committed", "COMMIT of the stream group returned")
declare("leader.taken_over", "take-over transaction committed", once=True)   # reached at most once per process

def commit_group(...):
    ...
    conn.execute("COMMIT")
    crash_point("stream.committed")
```

- Declare each point once, at module level, with a one-line description. Names are lower case and dotted, with at
  least two parts (`stream.position_written`, `backfill.chunk_committed`). The step tables of
  `docs/design/core.md` §3 name the points of the commit protocol.
- `once=True` marks a start-up step that a process reaches at most once; the runner then arms it at hit 1 only.
- `crashpoints.registry()` lists every declared point. `python -m cdc_v2.crashpoints --import <module> …` prints
  them as JSON (import the modules that declare them first).
- `tests/harness/test_harness_crashpoints.py::test_every_crash_point_called_in_product_code_is_declared` checks
  statically that every `crash_point("x")` under `src/cdc_v2/` has a `declare("x", …)` and the reverse.
- When nothing is switched on, `crash_point` tests one module global and returns (measured below 1 µs per call; the
  test asserts it).

### Environment contract

The JVM engine will implement the same contract in Java. Variables are read once, at import.

| Variable | Meaning |
|---|---|
| `CDC_V2_CRASH_POINT` | name of the one armed point; unset or empty = none |
| `CDC_V2_CRASH_HIT` | which hit fires, counted per process from 1: `N` (default 1), or `seed:S:M` = 1 + splitmix64(S) mod M (S unsigned 64-bit, M ≥ 1) |
| `CDC_V2_CRASH_ACTION` | `kill` (default; SIGKILL to itself), `kill_later` (a separate thread sends SIGKILL after `CDC_V2_CRASH_DELAY_MS` while the caller carries on into the next call), `stop` (SIGSTOP to itself; continues after SIGCONT), `hang` (the calling thread blocks for ever), `raise` (`CrashPointHit`, a `BaseException`) |
| `CDC_V2_CRASH_DELAY_MS` | delay of `kill_later` in milliseconds (integer ≥ 0, default 0) |
| `CDC_V2_CRASH_LOG` | optional hit log: one line per call of every declared point, armed or not |
| `CDC_V2_CRASH_TAG` | optional instance tag written into each log line (`[A-Za-z0-9_.-]`, ≤ 64 characters) |

A point fires at most once per process.

`kill_later` exists for the state that a plain `kill` cannot produce: a blocking call (COMMIT) in flight when the
process dies, so that its outcome is unknown (`docs/design/core.md` §3, step 6). Put a point immediately before the
call (`stream.commit_sent`) and run it with `kill_later`; with `kill` the same point is only "nothing sent yet".

An invalid value fails at start-up (`ValueError`), so a typo in a harness
never runs silently without a crash; calling an undeclared point fails while the module is active.

splitmix64 (the standard finaliser) on unsigned 64-bit values:

```
x = x + 0x9E3779B97F4A7C15
z = (x ^ (x >>> 30)) * 0xBF58476D1CE4E5B9
z = (z ^ (z >>> 27)) * 0x94D049BB133111EB
return z ^ (z >>> 31)
```

Reference values: splitmix64(0) = `0xE220A8397B1DCDAF`, splitmix64(1) = `0x910A2DEC89025CC1`. In Java use `long`
arithmetic, `>>>` and `Long.remainderUnsigned(z, M)`.

Hit log line: one JSON object and `\n`, written with a single `write` on a file opened with `O_APPEND`, **before** the
action runs (so it survives SIGKILL and concurrent writers do not interleave):

```
{"point":"stream.committed","hit":3,"pid":4711,"tag":"a","t":1762731234.123456,"fire":true,"action":"kill"}
```

`hit` counts calls of that point in that process; `fire` is true only on the firing hit; `action` is empty on the
other lines. Notes for the Java side: `Runtime.halt` is an exit, not SIGKILL; the runner requires death by SIGKILL,
so the JVM must signal itself (for example run `kill -KILL <own pid>` and block until it arrives); `stop` likewise
with `-STOP`; `kill_later` from a separate thread after the delay, without blocking the caller. The pipeline passes
the `CDC_V2_CRASH_*` variables to the JVM child unchanged; both processes may append to the same hit log (their lines
carry different pids), and each arms only a point that it declares and calls.

## 2. Fault sites

For errors the TCP proxy cannot produce, because the destination is a cloud service reached through its own client.

```python
from cdc_v2.faultsites import declare_site, fault_site

declare_site("md.commit", "COMMIT of a MotherDuck data transaction")
...
fault_site("md.commit")
conn.execute("COMMIT")
```

| Variable | Meaning |
|---|---|
| `CDC_V2_FAULT_FILE` | control file (JSON); unset = off. It may appear later. |
| `CDC_V2_FAULT_LOG` | optional log, one JSON line per fired fault (and an `unblock` line when a block ends) |
| `CDC_V2_FAULT_TAG` | optional instance tag; a rule with `tag` applies only to that instance |

The file is re-read only when one `stat` shows a new inode, modification time or size. A rule:

```json
{"id": "r1", "site": "md.commit", "action": "raise", "exception": "duckdb.IOException", "message": "injected",
 "skip": 0, "hits": 2, "start": 1762731234.0, "end": 1762731294.0}
```

`site` may be an `fnmatch` pattern. `action` is `raise`, `delay` (`seconds`) or `block` (while the rule is in the file
and its window is open; at most `seconds` if given). `exception` is a built-in name or `module.Class` of a module that
the process has already imported (the reader never imports); the default is `FaultInjected`, and the raised exception
carries `cdc_v2_fault = <rule id>`; a `raise` rule whose exception cannot be built logs an `error` line and does
not fire. `skip` hits pass first, then `hits` hits fire; hits outside the window neither fire nor count. Counters
belong to the rule (id and content), so a changed rule starts from zero. The first rule that fires decides.

Test side:

```python
from cdc_v2.testing.faults import FaultControl

fc = FaultControl(tmp_path / "faults")
proc = adapter.start("a", fc.env(tag="a"))
fc.raise_at("md.commit", "ConnectionError", hits=2)          # also delay_at, block_at, remove, clear
...
assert len(fc.fired(site="md.commit")) == 2                   # the log proves the fault was in effect
```

## 3. Process control (`procs`)

`procs.start(argv, env=…, log_path=…, name=…)` starts a process with `start_new_session=True` (its own session and
process group), stdout and stderr appended to `log_path`. The child inherits the caller's environment **without** the
`CDC_V2_CRASH_*` and `CDC_V2_FAULT_*` variables, plus `env`.

- `stop()`, `cont()`, `kill()` signal the process; `stop_group()`, `cont_group()`, `kill_group()` the group;
  `kill_tree()` the group plus descendants that left it (`setsid`), recognised by pid and start time.
- Every wait has a deadline: `wait(timeout)` returns an `ExitStatus` (`code` or `signal`, `.killed`) or raises
  `DeadlineExceeded`; `wait_state("T", timeout)` waits until `ps` shows the process stopped.
- `assert_no_survivors(procs, timeout)` raises `SurvivorError` naming any live member or descendant.
- `Fleet` holds several competing instances; leaving `with Fleet()` kills them all and checks survivors.
- Signals go only to processes this module started. A group seen empty is never signalled again (no reused id is
  hit). Limit: a descendant that calls `setsid` and whose parent dies before the harness has seen it is not tracked.

## 4. Writing an adapter

The runner drives an adapter; it knows nothing about the pipeline. Subclass `cdc_v2.testing.crash.Adapter`:

| Method | Contract |
|---|---|
| `name` | short label written to the ledger |
| `crash_points()` | names of the points the system under test declares (the runner arms each one) |
| `max_hit(point)` | optional; highest hit to arm (default 1 for `once` points in this process's registry, else `Config.max_hit` = 3) |
| `setup(workdir, seed)` | fresh, isolated source and destination for one scenario, under `workdir` |
| `start(tag, env)` | start one instance with `env` added (crash and fault variables); return a `procs.Proc` |
| `workload(step, seed)` | apply one seeded workload step to the source; the runner calls it repeatedly while it waits |
| `wait_caught_up(timeout)` | return when the destination holds everything the source has; raise when the deadline passes |
| `check()` | return `{"ok": bool, "reason": str, …}` (JSON-able details, no paths, no secrets); later the oracle |
| `teardown()` | optional clean-up (drop the scenario's databases) |

The runner creates one adapter per scenario (it calls the factory), so scenarios can run in parallel
(`Config(workers=4)`). It stops instances with SIGKILL: the system is crash-only (`docs/design/core.md` §1), so
`check()` must hold after a kill at any moment. `tests/harness/toy_adapter.py` is a complete example.

## 5. Running each mode

```python
from cdc_v2.testing.crash import Config, CrashRunner

r = CrashRunner(MyAdapter, workdir, ledger_path="evidence/.../ledger.jsonl", config=Config(workers=4))
r.run_crash_points(seeds=(1, 2, 3))                       # (a) every declared point, 3 seeds, SIGKILL
r.run_crash_points(points=["stream.commit_sent"], action="kill_later", delay_ms=0)   # COMMIT in flight
r.run_random_kills(seeds=(1, 2, 3), kills=4)              # (b) kills at random times from the seed
r.run_freeze(seeds=(1, 2, 3))                             # (c) SIGSTOP at a random time, second instance, SIGCONT
r.run_freeze(seeds=(1,), at_points=["stream.committed"])  # (c) stop exactly at a point
r.run_freeze(seeds=(1,), at_points=["stream.applied"], action="hang")   # hung component, live process
r.write_summary("evidence/.../summary.json")
```

or `python -m cdc_v2.testing.crash --adapter module:Factory --mode points --mode random --mode freeze --seeds 1,2,3
--workdir W --ledger L --summary S --workers 4` (exit 0 only when every run passed and every point was reached).

- **(a) crash points.** For each point and seed the hit is `seed:<hit_seed(point, seed)>:<max hit>`, so seeds and
  points choose different hits. The runner feeds workload until the instance dies. It must die by SIGKILL and its last
  `fire` line must name the point; otherwise the outcome is `not_reached` or `wrong_exit` — a point that is never
  reached is a failure, not a pass. Then a second instance starts without the switch, the runner waits until caught
  up, kills everything, asserts no survivor and calls `check()`.
- **(b) random kills.** Instances are killed (process group and descendants) at random times in
  `Config.kill_after`; the ledger lists the last point each one reached. Then a final instance catches up.
- **(c) freeze.** The instance's group is stopped at a random time (or the instance stops itself at a point), a
  second instance starts while it is frozen, the first is continued after `Config.freeze_for`, then catch-up and
  check. With `action="hang"` the first instance hangs at the point and is never continued; the second must make
  progress anyway (SPEC 4.2, 4.5).

Every wait in the runner has a deadline (`crash_timeout`, `catch_up_timeout`, `stop_timeout`).

## 6. Reading the ledger

One JSON line per scenario:

| Field | Meaning |
|---|---|
| `adapter`, `mode`, `point`, `seed` | what ran (`mode`: `crash_point`, `random_kill`, `freeze`, `hang`) |
| `hit`, `hit_spec`, `action` | the armed hit and how it was chosen |
| `outcome` | `pass`, or a failure: `not_reached`, `wrong_exit`, `no_catch_up`, `diverged` (check failed), `survivor`, `error` |
| `reason` | why it failed (scrubbed) |
| `timings` | `to_crash_s`, `catch_up_s`, `total_s` |
| `exit` | how the crashed instance died (`signal_name` `SIGKILL`) |
| `hits`, `hits_total` | the hit log: `tag`, `point`, `hit`, `fire`, `action`, in order (at most 200 kept) |
| `kills` | random-kill mode: per kill `after_s`, `exit`, `last_point`, `points_hit` |
| `frozen_after_point`, `frozen_s`, `second_instance` | freeze and hang modes |
| `check` | the adapter's result (for the toy: missing, duplicated, extra and wrong ids, position) |
| `workload_steps`, `ts` | workload steps fed; UTC time of the record |

The summary (`write_summary`) gives `declared_points`, `reached_points`, `missing_points`, `coverage`
(points that fired at least once in mode (a) / declared; both are `null` when mode (a) did not run), outcomes per
mode and per point, the failures and `all_passed`. A crash suite is complete when `missing_points` is empty and `all_passed` is true.

Evidence files contain no absolute path and no user name: `harness_evidence.write_evidence` replaces, longest path
first and only at a path boundary, the run directory with `<run>`, the temporary directory with `<tmp>`, the Python
installation with `<python>`, the home with `~` and the user name with `<user>`, and then refuses any text that still
holds an absolute path under `/Users`, `/home`, `/root`, `/private`, `/var`, `/tmp`, `/Volumes` or `/opt`, or the
user name. A scenario directory is new for every scenario, and only hit-log lines of the scenario's own process
(its pid) count, so a stale log can never produce a pass. At the merge with the oracle worker's
evidence writer, only the body of that function changes.

## 7. TCP fault proxy

```python
from cdc_v2.testing.netproxy import NetProxy

with NetProxy("localhost", 55432) as px:        # a free port in 55440-55459
    dsn = "host=localhost port=%d ..." % px.port
    px.set_fault("blackhole", direction="both")  # up = client to server, down = server to client
```

| Mode | Effect |
|---|---|
| `pass` | forward |
| `refuse` | stop listening (ECONNREFUSED); open connections continue |
| `close` / `reset` | FIN / RST on every open connection, and on each new one while set |
| `blackhole` | nothing forwarded in the direction(s); up to 1 MiB per direction is read and held (`held_up`, `held_down`), then the proxy stops reading; held bytes are delivered when the hole is lifted |
| `delay` | each chunk forwarded `seconds` after it was read, in order |
| `throttle` | `rate` bytes per second |

`set_fault(..., conn=<id>)` applies to one connection (`connections()` lists them); `set_policy()` combines delay,
throttle and black hole per direction. `stats()` counts accepted connections, bytes per direction, held bytes, closes
and resets by fault, accept-time closes and resets, and upstream errors, so a test can prove the fault was in effect.
TLS passes through untouched (the bytes are not changed; `verify-full` against `localhost` works).

Ports and control: the proxy leases its port with an exclusive `flock` on
`~/.cache/cdc_flight_v2/netproxy/port-<n>.lock` (released when the process dies), so concurrent test processes never
share a port. Another process controls it through the Unix socket `port-<n>.sock` next to it (`ProxyControl(port)`),
or with the command line:

```sh
tools/netproxy serve --target-port 55432           # prints {"port": N}; runs until stop, SIGINT or SIGTERM
tools/netproxy set N blackhole --direction up
tools/netproxy stats N [--connections]
tools/netproxy list
tools/netproxy stop N
```

A local target on port 5432 is refused. Output never contains the target host.

**Limit: a proxy black hole does not exercise TCP keepalives.** The proxy's own kernel still acknowledges keepalive
probes, so an idle connection in a black hole stays healthy to TCP (measured: 5 s with keepalive idle 1 s, interval
1 s, count 2, no socket error, and the connection worked again afterwards). A black hole through the proxy is found by
a client-side statement deadline or an end-to-end heartbeat (SPEC 4.6's second mechanism). Keepalive detection of a
dead path needs real packet loss: stage 2 against a remote source, or a packet filter, which this machine's rules
exclude.

Measured through the proxy against the local primary (psycopg 3.3.8, `tests/harness/test_harness_pg_proxy.py`,
`evidence/p3/harness/pg_proxy_detection.json`):

| Fault | Client noticed after |
|---|---|
| black hole, both directions, statement with a 2 s client deadline | 2.0 s (the deadline) |
| black hole of the reply only (`SELECT pg_sleep(0.1)`) | 2.0 s (the deadline) |
| reset, idle connection, next statement | a few milliseconds |
| reset while a statement runs | a few milliseconds after the reset |
| close (FIN), next statement | a few milliseconds |
| refused, new connection | a few milliseconds |
| delay 0.15 s each way | the statement succeeds in ≈ 0.3 s |

The client deadline in the test shuts the client socket down when it expires (libpq has no client-side statement
timeout, and a cancel request would travel through the same broken path); the server-side `statement_timeout` does
not help when the request or the reply is lost.

## 8. Byte-stream fault relay

For the one-directional frame pipe from the engine to the applier. Faults chosen from a seed at random input offsets:
`stall`, `cut` (end of stream; the input is closed too, so the writer sees EPIPE), `dup` (a byte range relayed twice),
`truncate` (the last input bytes dropped at the end, before any other fault sees them; a fault inside that tail is
logged as not fired), `flip` (one bit). At most one terminal fault (cut or truncate) per
plan.

```python
from cdc_v2.testing import pipefaults as pf

plan = pf.make_plan(seed, size_hint)
out, log = pf.apply_bytes(data, plan)        # pure; stalls recorded, not slept
relay = pf.PipeRelay(plan).start()           # producer writes relay.write_fd, consumer reads relay.read_fd
log = relay.join(timeout)
```

or `python -m cdc_v2.testing.pipefaults --seed S --size N [--kinds …] --log L < in > out`. The log lists each fault
with its input and output offset (and, for a flip, the byte before and after); faults whose offset lies beyond the
stream are listed with `fired: false`. The output depends only on the plan and the input bytes, not on how reads are
chunked. Pass the relay's ends to child processes with `pass_fds` and close the parent's copies.

## 9. Self-tests and evidence

```sh
uv run -q pytest -q tests/harness                                     # hermetic, about 30 s
tools/with-env uv run -q pytest -q -m t1 tests/harness/test_harness_pg_proxy.py    # shared local cluster, about 10 s
CDC_V2_HARNESS_LONG=1 uv run -q pytest -q tests/harness/test_harness_crash_runner.py -k long   # opt-in campaign
CDC_V2_HARNESS_EVIDENCE=evidence/p3/harness uv run -q pytest -q tests/harness    # also writes the evidence files
```

The `t1` test runs `tools/localpg up` (a no-op when the cluster runs) and nothing else on the cluster. The evidence
of this harness is under `evidence/p3/harness/` (`README.md` there lists the files).
