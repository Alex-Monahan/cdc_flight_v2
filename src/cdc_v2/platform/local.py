"""`LocalPlatform`: the Flight platform simulated with local processes (contract C6), for tier T1 and the
leadership fuzzer (package F). A run is the process tree of `flights/cdc_v2/main.py`, started with a local
source path. Every imitated behaviour cites what the run spike measured (`docs/spike/run_semantics.md`):

| Behaviour | Imitation | Finding |
|---|---|---|
| a scheduled run every minute, whatever else runs; on demand too; starts overlap freely | `schedule(minute_s)` starts a run every `minute_s` after a start delay drawn from `start_delay_s`; `start_run` at any time | Q1 (every minute started a run, up to 9 at once), Q2 (0.26 to 2.8 s after the minute) |
| run records `PENDING`, `RUNNING`, `SUCCEEDED`, `FAILED`, `CANCELLED`, readable by any process | one JSON file per run under `<dir>/runs/<flight>/<n>.json`, replaced atomically | flight_platform 6 |
| cancel: the record says `CANCELLED` at once, the processes die about 2.5 s later, without a signal they could catch, and can commit until then | `cancel` marks at once; SIGKILL to every process of the run `cancel_lag_s` later | Q4 (2.28 to 2.51 s; no signal) |
| a cancelled run keeps `CANCELLED` (exit code NULL), also when it exits by itself meanwhile | the record is not overwritten | Q4 (self-cancel) |
| the maximum run time is enforced late and coarsely; the run becomes `CANCELLED` | killed `max_runtime_lag_s` after the limit | Q5 (65 to 118 s late for 60 s; 3.3 s for 180 s) |
| the run ends when its main process exits; every process it started dies with it, in any session or process group | at the exit of the bootstrap every tracked process of the run gets SIGKILL | Q7 (the container is destroyed) |
| exit status: 0 `SUCCEEDED`, else `FAILED` with the code; an out-of-memory kill is 137 | a death by signal n is recorded as 128 + n | Q6 |
| a run knows its Flight, run number and run id | `MOTHERDUCK_FLIGHT_ID`, `MOTHERDUCK_FLIGHT_RUN_NUMBER`, `MOTHERDUCK_FLIGHT_RUN_ID` | flight_platform 5 |

**Which processes belong to a run.** The platform's monitor thread looks at the process table every `track_s`
and remembers every descendant of the run (pid and start time), so that a process that left the run's session or
process group, or whose parent died, is still known. As a second net, every process of a run inherits
`CDC_V2_LOCAL_RUN=<token>`, and the environment of every process is searched for it (`ps -E`) when the run ends:
that finds a descendant that called `setsid` and whose parent died before the monitor saw it. Limit: a process
that does both and also clears its environment, or whose environment `ps` may not read (a system binary on
macOS), is not found. Signals go only to processes found these ways, verified by pid and start time; never by
name.

Cross-process use: `LocalPlatformClient(dir)` (and the bootstrap's own copy) reads the records and asks for a
cancel by writing a request file into `<dir>/cancel/`, which the monitor answers with a `.done` file.
"""
from __future__ import annotations

import json
import os
import random
import re
import signal
import subprocess
import sys
import threading
import time
import uuid
from pathlib import Path

from cdc_v2.testing import procs

from . import TERMINAL, identity as _identity, normalize_state

REPO = Path(__file__).resolve().parents[3]
BOOTSTRAP = REPO / "flights" / "cdc_v2" / "main.py"
MARK = "CDC_V2_LOCAL_RUN"


def _safe_part(text):
    return re.sub(r"[^A-Za-z0-9_-]", "_", str(text))[:64]


def record_path(base, flight_id, run_number):
    """Where the record of a run lies (the bootstrap holds the same rule)."""
    return os.path.join(str(base), "runs", _safe_part(flight_id), "%d.json" % int(run_number))


def _write_json(path, doc):
    tmp = "%s.tmp-%s" % (path, uuid.uuid4().hex[:6])
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(doc, f, sort_keys=True)
    os.replace(tmp, path)


def marked_processes(token):
    """``{pid: start}`` of every process whose environment holds ``CDC_V2_LOCAL_RUN=<token>``."""
    needle = "%s=%s" % (MARK, token)
    found = {}
    if sys.platform.startswith("linux"):
        for name in os.listdir("/proc"):
            if not name.isdigit():
                continue
            try:
                with open("/proc/%s/environ" % name, "rb") as f:
                    if needle.encode() in f.read().split(b"\0"):
                        found[int(name)] = None
            except OSError:
                continue
        if found:
            table = procs.process_table()
            found = {pid: table[pid][3] for pid in found if pid in table}
        return found
    try:
        out = subprocess.run(["ps", "-A", "-wwE", "-o", "pid=,lstart=,command="], capture_output=True, text=True,
                             timeout=10, check=False).stdout
    except (OSError, subprocess.SubprocessError):
        return found
    for line in out.splitlines():
        parts = line.split(None, 6)
        if len(parts) < 7 or not parts[0].isdigit():
            continue
        if needle + " " in parts[6] + " ":
            found[int(parts[0])] = " ".join(parts[1:6])
    return found


class LocalRun:
    """One run: its identity, its record and its processes. ``proc`` is a `RunProc` (None while `PENDING`)."""

    def __init__(self, platform, flight_id, run_number, tag, max_runtime_s=None):
        self.platform = platform
        self.flight_id = flight_id
        self.run_number = run_number
        self.run_id = str(uuid.uuid4())
        self.tag = tag
        self.token = uuid.uuid4().hex
        self.state = "PENDING"
        self.proc = None
        self.started = None                    # monotonic
        self.ended_at = None                   # monotonic: the bootstrap exited, or the run was killed
        self.exit_code = None
        self.signal = None
        self.cancelled_at = None
        self.timed_out = False
        self.kill_at = None
        self.killed = False
        self.max_runtime_s = max_runtime_s
        self.max_runtime_at = None
        self.tracked = {}                      # pid -> start time of every process seen in the run
        self.log_path = os.path.join(platform.dir, "logs", "%s.log" % tag)
        self.wall = {"created": time.time()}

    @property
    def pid(self):
        return self.proc.pid if self.proc is not None else None

    def record(self):
        return {"tag": self.tag, "flight_id": self.flight_id, "run_number": self.run_number, "run_id": self.run_id,
                "state": self.state, "exit_code": self.exit_code, "signal": self.signal, "timed_out": self.timed_out,
                "max_runtime_s": self.max_runtime_s, "pid": self.pid, "pids": sorted(self.tracked),
                "started_s": None if self.started is None else round(self.started - self.platform.t0, 3),
                "ended_s": None if self.ended_at is None else round(self.ended_at - self.platform.t0, 3),
                "cancelled_s": None if self.cancelled_at is None else round(self.cancelled_at - self.platform.t0, 3),
                "wall": self.wall}

    def boot_lines(self):
        """The bootstrap's ``BOOT {json}`` lines of this run's log."""
        out = []
        try:
            with open(self.log_path, encoding="utf-8", errors="replace") as f:
                for line in f:
                    if line.startswith("BOOT "):
                        try:
                            out.append(json.loads(line[5:]))
                        except ValueError:
                            pass
        except FileNotFoundError:
            pass
        return out

    def __repr__(self):
        return "<LocalRun %s %s #%d %s>" % (self.tag, self.flight_id[:8], self.run_number, self.state)


class RunProc(procs.Proc):
    """The process of a run as the harness sees it (`cdc_v2.testing.procs.Proc`), widened to the whole run: its
    group operations reach every process of the run (the bootstrap's group, the pipeline's own session, their
    children), as stopping or killing a container would. ``instance_pids()`` names every process of the run, so
    that a crash point that fires in the pipeline counts for the run (`cdc_v2.testing.crash`)."""

    def __init__(self, popen, name, log_path, run):
        super().__init__(popen, name, log_path)
        self.run = run

    def instance_pids(self):
        return {self.pid} | set(self.run.tracked)

    def run_members(self, table):
        """Live processes of the run: the bootstrap's group while the bootstrap lives (a group id is not reused
        while its leader lives), and every process the platform tracked for the run (pid and start time)."""
        alive = self.popen.poll() is None
        out = set()
        for pid, (_ppid, pgid, stat, started) in table.items():
            if stat.startswith("Z"):
                continue
            if (alive and pgid == self.pgid) or self.run.tracked.get(pid) == started:
                out.add(pid)
        if alive:
            out.add(self.pid)
        return sorted(out)

    def signal_group(self, sig, table=None):
        table = table if table is not None else procs.process_table()
        sent = False
        for pid in self.run_members(table):
            try:
                os.kill(pid, sig)
                sent = True
            except (ProcessLookupError, PermissionError):
                pass
        return sent

    def survivors(self, table=None):
        table = table if table is not None else procs.process_table()
        self._remember(table)
        return sorted(set(self.run_members(table)) | set(self._known_alive(table)))

    def wait_state(self, letter, timeout, interval=0.02):
        """Wait until a process of the run is in state ``letter`` (``T`` after a stop at a crash point of the
        pipeline, or after `stop_group`)."""
        deadline = time.monotonic() + timeout
        while True:
            table = procs.process_table()
            states = [table[p][2] for p in self.run_members(table) if p in table]
            if any(s.startswith(letter) for s in states):
                return letter
            if time.monotonic() >= deadline:
                raise procs.DeadlineExceeded("%s: no process in state %s within %.1f s (states %r)"
                                             % (self.name, letter, timeout, states))
            time.sleep(interval)


class LocalPlatform:
    """The simulator. ``env`` is the base environment of every run (destination, source path, timings, the
    pipeline to start); ``start_run(env)`` adds to it."""

    def __init__(self, workdir, env=None, flight_id=None, python=None, bootstrap=None, cancel_lag_s=2.5,
                 max_runtime_lag_s=(0.5, 1.5), start_delay_s=(0.0, 0.0), minute_s=60.0, track_s=0.1, seed=None):
        self.dir = os.path.join(str(workdir), "platform")
        for sub in ("runs", "cancel", "logs"):
            os.makedirs(os.path.join(self.dir, sub), exist_ok=True)
        self.flight_id = flight_id or str(uuid.uuid4())
        self.env = {k: str(v) for k, v in (env or {}).items()}
        self.python = python or sys.executable
        self.bootstrap = str(bootstrap or BOOTSTRAP)
        self.cancel_lag_s = cancel_lag_s
        self.max_runtime_lag_s = tuple(max_runtime_lag_s)
        self.start_delay_s = tuple(start_delay_s)
        self.minute_s = minute_s
        self.track_s = track_s
        self.rng = random.Random(seed)
        self.t0 = time.monotonic()
        self.runs = []
        self._numbers = {}
        self._lock = threading.RLock()
        self._step_lock = threading.Lock()
        self._stop = threading.Event()
        self._schedule_stop = threading.Event()
        self._schedule_thread = None
        self._events = os.path.join(self.dir, "runs.jsonl")
        self._monitor = threading.Thread(target=self._monitor_loop, name="cdc_v2-local-platform", daemon=True)
        self._monitor.start()

    # ------------------------------------------------------------------ runs
    def _new_run(self, flight_id, max_runtime_s, tag):
        with self._lock:
            n = self._numbers.get(flight_id, 0) + 1
            self._numbers[flight_id] = n
            run = LocalRun(self, flight_id, n, tag or "r%d%s" % (n, "" if flight_id == self.flight_id
                                                                   else "-" + flight_id[:6]), max_runtime_s)
            self.runs.append(run)
        self._save(run, "created")
        return run

    def _launch(self, run, env):
        full = dict(self.env)
        full.update({k: str(v) for k, v in (env or {}).items()})
        full.update({"MOTHERDUCK_FLIGHT_ID": run.flight_id, "MOTHERDUCK_FLIGHT_RUN_NUMBER": str(run.run_number),
                     "MOTHERDUCK_FLIGHT_RUN_ID": run.run_id, "CDC_V2_LOCAL_PLATFORM_DIR": self.dir,
                     MARK: run.token, "PYTHONUNBUFFERED": "1"})
        full.setdefault("CDC_V2_WORK_DIR", os.path.join(self.dir, "work", run.tag))
        p = procs.start([self.python, self.bootstrap], env=full, log_path=run.log_path, name=run.tag)
        proc = RunProc(p.popen, run.tag, run.log_path, run)
        with self._lock:
            run.proc = proc
            run.started = time.monotonic()
            run.wall["started"] = time.time()
            run.state = "RUNNING" if run.state == "PENDING" else run.state
            if run.max_runtime_s:
                run.max_runtime_at = run.started + run.max_runtime_s + self.rng.uniform(*self.max_runtime_lag_s)
        self._save(run, "started")
        return run

    def start_run(self, env=None, max_runtime_s=None, flight_id=None, tag=None, delay_s=0.0):
        """Start a run now (on demand), or after ``delay_s`` (the record is `PENDING` meanwhile). Another
        ``flight_id`` makes a run of a different Flight (a contender). Returns the `LocalRun`."""
        run = self._new_run(flight_id or self.flight_id, max_runtime_s, tag)
        if delay_s > 0:
            threading.Thread(target=self._delayed, args=(run, env, delay_s), daemon=True).start()
            return run
        return self._launch(run, env)

    def _delayed(self, run, env, delay_s):
        if not self._stop.wait(delay_s) and run.state == "PENDING":
            self._launch(run, env)

    def schedule(self, minute_s=None, env=None, max_runtime_s=None):
        """Start a run every ``minute_s`` (default: the platform's minute), each after a start delay drawn from
        ``start_delay_s``, until `stop_schedule`."""
        minute = minute_s or self.minute_s
        self._schedule_stop.clear()

        def loop():
            t0, k = time.monotonic(), 1
            while not self._schedule_stop.wait(max(0.0, t0 + k * minute - time.monotonic())):
                self.start_run(env, max_runtime_s=max_runtime_s, delay_s=self.rng.uniform(*self.start_delay_s))
                k += 1
        self._schedule_thread = threading.Thread(target=loop, name="cdc_v2-local-schedule", daemon=True)
        self._schedule_thread.start()

    def stop_schedule(self):
        self._schedule_stop.set()
        if self._schedule_thread is not None:
            self._schedule_thread.join(5)
            self._schedule_thread = None

    def find(self, flight_id, run_number):
        with self._lock:
            for r in self.runs:
                if r.flight_id == str(flight_id) and r.run_number == int(run_number):
                    return r
        return None

    def run_state(self, run_or_flight, run_number=None):
        run = run_or_flight if isinstance(run_or_flight, LocalRun) else self.find(run_or_flight, run_number)
        self.poll()
        return None if run is None else run.state

    def cancel(self, run_or_flight, run_number=None):
        """Cancel a run: its record says `CANCELLED` at once; its processes get SIGKILL ``cancel_lag_s`` later.
        False for a terminal or unknown run (the real call fails then)."""
        run = run_or_flight if isinstance(run_or_flight, LocalRun) else self.find(run_or_flight, run_number)
        if run is None:
            return False
        with self._lock:
            if run.state in TERMINAL:
                return False
            run.state = "CANCELLED"
            run.cancelled_at = time.monotonic()
            run.wall["cancelled"] = time.time()
            run.kill_at = run.cancelled_at + self.cancel_lag_s
        self._save(run, "cancelled")
        return True

    def poll(self):
        """Bring the records up to date now (the monitor does it every `track_s` too)."""
        self._step()

    def alive(self):
        """Runs whose bootstrap has not ended (a `PENDING` run counts)."""
        self.poll()
        with self._lock:
            return [r for r in self.runs if r.ended_at is None]

    def records(self):
        with self._lock:
            return [r.record() for r in self.runs]

    @staticmethod
    def read_records(base_dir):
        """Every run record under a platform directory, from any process."""
        out = []
        root = os.path.join(str(base_dir), "runs")
        for folder, _dirs, files in os.walk(root):
            for name in files:
                if name.endswith(".json"):
                    try:
                        with open(os.path.join(folder, name), encoding="utf-8") as f:
                            out.append(json.load(f))
                    except (OSError, ValueError):
                        pass
        return sorted(out, key=lambda r: (r.get("flight_id"), r.get("run_number")))

    # ------------------------------------------------------------------ the monitor
    def _save(self, run, event):
        path = record_path(self.dir, run.flight_id, run.run_number)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        rec = run.record()
        _write_json(path, rec)
        line = json.dumps({"event": event, "tag": run.tag, "state": run.state, "t_s": round(time.monotonic()
                                                                                          - self.t0, 3)})
        with open(self._events, "a", encoding="utf-8") as f:
            f.write(line + "\n")

    def _monitor_loop(self):
        while not self._stop.is_set():
            try:
                self._step()
            except Exception:                        # noqa: BLE001 - the monitor never dies
                pass
            self._stop.wait(self.track_s)

    def _track(self, table):
        with self._lock:
            runs = [r for r in self.runs if r.proc is not None and (r.ended_at is None or not r.killed)]
        for run in runs:
            roots = [run.proc.pid] + list(run.tracked)
            for root in roots:
                if root in table and (root == run.proc.pid or run.tracked.get(root) == table[root][3]):
                    for pid in procs._descendants(table, root):
                        if pid in table and pid not in run.tracked:
                            run.tracked[pid] = table[pid][3]

    def _step(self):
        with self._step_lock:
            self._step_locked()

    def _step_locked(self):
        with self._lock:
            busy = any(r.proc is not None and (r.ended_at is None or not r.killed) for r in self.runs)
        if busy:
            self._track(procs.process_table())
        self._process_requests()
        now = time.monotonic()
        with self._lock:
            runs = list(self.runs)
        for run in runs:
            if run.proc is None:
                continue
            rc = run.proc.popen.poll()
            if rc is not None and run.ended_at is None:
                with self._lock:
                    run.ended_at = now
                    run.wall["ended"] = time.time()
                    run.exit_code = rc if rc >= 0 else 128 - rc
                    run.signal = -rc if rc < 0 else None
                    if run.state not in ("CANCELLED",):
                        run.state = "SUCCEEDED" if rc == 0 else "FAILED"
                self._kill_run(run)                  # the container is destroyed with everything in it (Q7)
                self._save(run, "ended")
                continue
            if run.ended_at is None and run.kill_at is not None and now >= run.kill_at:
                self._kill_run(run)
                continue
            if run.ended_at is None and run.max_runtime_at is not None and now >= run.max_runtime_at \
                    and run.state == "RUNNING":
                with self._lock:
                    run.state = "CANCELLED"
                    run.timed_out = True
                    run.cancelled_at = now
                    run.wall["cancelled"] = time.time()
                self._kill_run(run)
                self._save(run, "timed_out")

    def _kill_run(self, run):
        """SIGKILL to every known process of the run, verified by pid and start time; no catchable signal."""
        table = procs.process_table()
        self._track(table)
        targets = {}
        for pid, started in list(run.tracked.items()):
            if pid in table and table[pid][3] == started and not table[pid][2].startswith("Z"):
                targets[pid] = started
        for pid, started in marked_processes(run.token).items():
            if pid in table and (started is None or table[pid][3] == started):
                run.tracked.setdefault(pid, table[pid][3])
                targets[pid] = table[pid][3]
        if run.proc is not None and run.proc.popen.poll() is None:
            targets[run.proc.pid] = None
        for pid in targets:
            try:
                os.kill(pid, signal.SIGKILL)
            except (ProcessLookupError, PermissionError):
                pass
        if run.proc is not None:
            try:
                run.proc.popen.wait(timeout=5)
            except subprocess.TimeoutExpired:
                pass
        with self._lock:
            run.killed = True

    def _process_requests(self):
        req_dir = os.path.join(self.dir, "cancel")
        try:
            names = sorted(n for n in os.listdir(req_dir) if n.endswith(".req"))
        except FileNotFoundError:
            return
        for name in names:
            path = os.path.join(req_dir, name)
            try:
                with open(path, encoding="utf-8") as f:
                    req = json.load(f)
            except (OSError, ValueError):
                continue
            run = self.find(req.get("flight_id"), req.get("run_number", -1))
            if run is None:
                result = {"canceled_count": 0, "error": "Run does not exist"}
            elif run.state in TERMINAL:
                result = {"canceled_count": 0, "error": "Run is already in terminal state"}
            else:
                self.cancel(run)
                result = {"canceled_count": 1}
            _write_json(path[:-4] + ".done", result)
            try:
                os.remove(path)
            except OSError:
                pass

    # ------------------------------------------------------------------ the end
    def stop_all(self, timeout=10.0):
        """Kill every run and check that no process of any run is left (`procs.SurvivorError` otherwise)."""
        self.stop_schedule()
        with self._lock:
            runs = list(self.runs)
            for run in runs:
                if run.state == "PENDING":
                    run.state = "CANCELLED"
        for run in runs:
            if run.proc is not None:
                self._kill_run(run)
        self.poll()
        live = [r.proc for r in runs if r.proc is not None]
        procs.assert_no_survivors(live, timeout)
        left = {r.tag: sorted(marked_processes(r.token)) for r in runs if r.proc is not None}
        left = {k: v for k, v in left.items() if v}
        if left:
            raise procs.SurvivorError("processes of runs survived: %r" % (left,))

    def close(self, timeout=10.0):
        try:
            self.stop_all(timeout)
        finally:
            self._stop.set()
            self._monitor.join(5)


class LocalPlatformClient:
    """Contract C6 for a process of a local run: the same calls as `MotherDuckPlatform`, through the records and
    request files of a `LocalPlatform` directory (`CDC_V2_LOCAL_PLATFORM_DIR`)."""

    def __init__(self, base_dir=None, environ=None):
        env = os.environ if environ is None else environ
        self.dir = str(base_dir or env.get("CDC_V2_LOCAL_PLATFORM_DIR") or "")
        if not self.dir:
            raise ValueError("no local platform directory")
        self._environ = environ

    def identity(self):
        return _identity(self._environ)

    def run_state(self, flight_id, run_number, deadline=None):
        try:
            with open(record_path(self.dir, flight_id, run_number), encoding="utf-8") as f:
                return normalize_state(json.load(f).get("state"))
        except (OSError, ValueError):
            return None

    def cancel(self, flight_id, run_number, deadline=None):
        req_dir = os.path.join(self.dir, "cancel")
        os.makedirs(req_dir, exist_ok=True)
        name = "%s-%d-%s" % (_safe_part(flight_id), int(run_number), uuid.uuid4().hex[:8])
        tmp = os.path.join(req_dir, "." + name)
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump({"flight_id": str(flight_id), "run_number": int(run_number)}, f)
        os.replace(tmp, os.path.join(req_dir, name + ".req"))
        done = os.path.join(req_dir, name + ".done")
        end = time.monotonic() + (deadline.remaining() if deadline is not None else 10.0)
        while time.monotonic() < end:
            try:
                with open(done, encoding="utf-8") as f:
                    return bool(json.load(f).get("canceled_count"))
            except (OSError, ValueError):
                time.sleep(0.02)
        return False


__all__ = ["LocalPlatform", "LocalRun", "RunProc", "LocalPlatformClient", "record_path", "marked_processes",
           "BOOTSTRAP"]
