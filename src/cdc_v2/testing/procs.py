"""Process control for systems under test: own session and process group, signals, waits with deadlines.

    with Fleet() as fleet:
        a = fleet.start([sys.executable, "sut.py"], env={...}, name="a", log_path=run / "a.log")
        a.stop_group(); a.cont_group()
        a.kill_group(); status = a.wait(10)      # ExitStatus(code=None, signal=9)
    # leaving the block kills whatever is left and asserts that no process of any group survives

Rules (SPEC 4.5 applies to test code too): every wait takes a deadline and raises `DeadlineExceeded` when it passes.
Signals go only to processes this module started (and their process groups and descendants), never by name.
A process group is signalled only while it can still contain our processes: once it is seen empty it is never
signalled again, so a reused process-group id cannot be hit.

Environment: a child inherits the caller's environment without the crash-point and fault-site variables
(`CDC_V2_CRASH_*`, `CDC_V2_FAULT_*`), plus ``env``; pass them in ``env`` to switch the harness on for that child.
"""
import os
import signal
import subprocess
import time

HARNESS_ENV_PREFIXES = ("CDC_V2_CRASH_", "CDC_V2_FAULT_")


class DeadlineExceeded(TimeoutError):
    pass


class SurvivorError(AssertionError):
    pass


class ExitStatus:
    """``code`` for a normal exit, ``signal`` for a death by signal (exactly one of them is set)."""

    __slots__ = ("code", "signal")

    def __init__(self, returncode):
        if returncode < 0:
            self.code, self.signal = None, -returncode
        else:
            self.code, self.signal = returncode, None

    @property
    def killed(self):
        return self.signal == signal.SIGKILL

    def as_dict(self):
        return {"code": self.code, "signal": self.signal,
                "signal_name": signal.Signals(self.signal).name if self.signal else None}

    def __repr__(self):
        if self.signal:
            return "ExitStatus(signal=%s)" % signal.Signals(self.signal).name
        return "ExitStatus(code=%d)" % self.code


def process_table():
    """{pid: (ppid, pgid, stat, started)} for every process; one ``ps`` call with a deadline. ``started`` is the
    start time as ``ps`` prints it, used with the pid to recognise a process again (pids are reused)."""
    out = subprocess.run(["ps", "-A", "-o", "pid=,ppid=,pgid=,stat=,lstart="], capture_output=True, text=True,
                         timeout=10, check=True).stdout
    table = {}
    for line in out.splitlines():
        parts = line.split()
        if len(parts) >= 5 and parts[0].isdigit():
            table[int(parts[0])] = (int(parts[1]), int(parts[2]), parts[3], " ".join(parts[4:]))
    return table


def _descendants(table, root):
    children = {}
    for pid, (ppid, _pgid, _st, _start) in table.items():
        children.setdefault(ppid, []).append(pid)
    out, todo = [], [root]
    while todo:
        p = todo.pop()
        for c in children.get(p, []):
            out.append(c)
            todo.append(c)
    return out


class Proc:
    def __init__(self, popen, name, log_path=None):
        self.popen = popen
        self.name = name
        self.log_path = log_path
        self.pid = popen.pid
        self.pgid = popen.pid                 # start_new_session=True: the child leads its own session and group
        self.started = time.monotonic()
        self.group_gone = False
        self.known = {}                       # descendants seen so far: pid -> start time (survivor check)

    # -- state -------------------------------------------------------------------------------------------------
    def poll(self):
        rc = self.popen.poll()
        return None if rc is None else ExitStatus(rc)

    def alive(self):
        return self.popen.poll() is None

    @property
    def exit_status(self):
        return self.poll()

    def ps_state(self):
        """``ps`` state letters of the process (``T`` = stopped, ``Z`` = zombie), or None when it is gone."""
        if self.popen.returncode is not None:
            return None
        row = process_table().get(self.pid)
        return row[2] if row else None

    def group_members(self, table=None):
        """Live (non-zombie) pids whose process group is ours."""
        if self.group_gone:
            return []
        table = table if table is not None else process_table()
        members = [pid for pid, (_pp, pg, st, _s) in table.items() if pg == self.pgid and not st.startswith("Z")]
        if not members and self.popen.poll() is not None:
            self.group_gone = True
        return members

    def _remember(self, table):
        for pid in _descendants(table, self.pid) if self.popen.poll() is None else []:
            self.known.setdefault(pid, table[pid][3])

    def _known_alive(self, table):
        """Remembered descendants that are still the same process (pid and start time) and not zombies."""
        return [pid for pid, started in self.known.items()
                if pid in table and table[pid][3] == started and not table[pid][2].startswith("Z")]

    def survivors(self, table=None):
        """Our processes that still live: group members plus descendants seen earlier that left the group."""
        table = table if table is not None else process_table()
        self._remember(table)
        live = set(self.group_members(table)) | set(self._known_alive(table))
        if self.popen.poll() is None:
            live.add(self.pid)
        return sorted(live)

    # -- signals -----------------------------------------------------------------------------------------------
    def signal(self, sig):
        """Signal the process itself; False if it has already been reaped."""
        if self.popen.poll() is not None:
            return False
        try:
            os.kill(self.pid, sig)
            return True
        except ProcessLookupError:
            return False

    def signal_group(self, sig, table=None):
        """Signal the whole process group; False if the group is gone. After the leader has been reaped the group is
        signalled only if ``ps`` still shows members (a group id is not reused while the group has members)."""
        if self.group_gone:
            return False
        if self.popen.poll() is not None and not self.group_members(table):
            return False
        try:
            os.killpg(self.pgid, sig)
            return True
        except ProcessLookupError:
            if self.popen.poll() is not None:
                self.group_gone = True
            return False

    def stop(self):
        return self.signal(signal.SIGSTOP)

    def cont(self):
        return self.signal(signal.SIGCONT)

    def kill(self):
        return self.signal(signal.SIGKILL)

    def stop_group(self):
        return self.signal_group(signal.SIGSTOP)

    def cont_group(self):
        return self.signal_group(signal.SIGCONT)

    def kill_group(self):
        return self.signal_group(signal.SIGKILL)

    def kill_tree(self):
        """SIGKILL to the group and to every descendant that left it (found through parent pids)."""
        table = process_table()
        self._remember(table)
        self.signal_group(signal.SIGKILL, table)
        for pid in self._known_alive(table):
            try:
                os.kill(pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
        self.kill()

    # -- waits -------------------------------------------------------------------------------------------------
    def wait(self, timeout):
        """Wait for the process to exit; returns ExitStatus or raises DeadlineExceeded."""
        try:
            return ExitStatus(self.popen.wait(timeout=timeout))
        except subprocess.TimeoutExpired:
            raise DeadlineExceeded("%s (pid %d) still running after %.1f s" % (self.name, self.pid, timeout)) from None

    def wait_state(self, letter, timeout, interval=0.02):
        """Wait until ``ps`` shows the state letter (``T`` after SIGSTOP); raises DeadlineExceeded. A ``ps`` that
        misses the row of a process that has not exited (``popen.poll()`` is None) counts as "not yet"."""
        deadline = time.monotonic() + timeout
        while True:
            st = self.ps_state()
            if st is not None and st.startswith(letter):
                return st
            gone = st is None and self.popen.poll() is not None
            if gone or time.monotonic() >= deadline:
                raise DeadlineExceeded("%s (pid %d) not in state %s within %.1f s (state %r)"
                                       % (self.name, self.pid, letter, timeout, st))
            time.sleep(interval)

    def kill_and_wait(self, timeout=10):
        self.kill_tree()
        return self.wait(timeout)

    def log_text(self):
        if not self.log_path:
            return ""
        try:
            with open(self.log_path, encoding="utf-8", errors="replace") as f:
                return f.read()
        except FileNotFoundError:
            return ""

    def __repr__(self):
        return "Proc(%s, pid=%d)" % (self.name, self.pid)


def child_env(env=None, inherit=True):
    base = {k: v for k, v in os.environ.items() if not k.startswith(HARNESS_ENV_PREFIXES)} if inherit else {}
    base.update({k: str(v) for k, v in (env or {}).items()})
    return base


def start(argv, env=None, cwd=None, log_path=None, name=None, inherit_env=True, stdin=subprocess.DEVNULL,
          pass_fds=()):
    """Start ``argv`` in its own session and process group. Output goes to ``log_path`` (appended) or /dev/null."""
    out = open(log_path, "ab") if log_path else subprocess.DEVNULL
    try:
        p = subprocess.Popen([str(a) for a in argv], env=child_env(env, inherit_env), cwd=cwd, stdin=stdin,
                             stdout=out, stderr=subprocess.STDOUT, start_new_session=True, close_fds=True,
                             pass_fds=pass_fds)
    finally:
        if log_path:
            out.close()
    return Proc(p, name or os.path.basename(str(argv[0])), log_path)


def assert_no_survivors(procs, timeout=10, interval=0.05):
    """Wait until no process of any group or known descendant lives; raise SurvivorError naming them."""
    procs = list(procs)
    deadline = time.monotonic() + timeout
    while True:
        for p in procs:
            p.popen.poll()                   # reap the direct children
        table = process_table()
        left = {p.name: p.survivors(table) for p in procs}
        left = {k: v for k, v in left.items() if v}
        if not left:
            return
        if time.monotonic() >= deadline:
            raise SurvivorError("processes survived after %.1f s: %r" % (timeout, left))
        time.sleep(interval)


class Fleet:
    """Several competing instances. Leaving the ``with`` block kills them all and checks for survivors."""

    def __init__(self):
        self.procs = []

    def start(self, argv, **kw):
        p = start(argv, **kw)
        self.procs.append(p)
        return p

    def add(self, proc):
        self.procs.append(proc)
        return proc

    def alive(self):
        return [p for p in self.procs if p.alive()]

    def kill_all(self, timeout=10):
        """SIGKILL every instance's tree, reap, and assert that nothing survives. Returns {name: ExitStatus}."""
        for p in self.procs:
            if p.alive() or not p.group_gone:
                p.kill_tree()
        statuses = {}
        for p in self.procs:
            statuses[p.name] = p.wait(timeout)
        assert_no_survivors(self.procs, timeout)
        return statuses

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.kill_all()
        return False
