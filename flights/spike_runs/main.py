"""P2 run-semantics spike instrument (docs/spike/run_semantics.md). One source, many modes.

This file is public. It reads everything from its environment, prints no value of any variable, no host name and
no database name other than its own cdc_v2_* database. Every process it starts writes rows with a server-side
timestamp (`now()` inside the INSERT) into the database CDC_V2_RUNS_DB:

    ticks(flight, run_number, role, pid, seq, client_unix, server_ts, tag)
    events(flight, run_number, role, pid, kind, client_unix, server_ts, detail)

and prints `EV {json}` lines (signals, decisions) to stdout.

Config (environment; every key exists in the Flight config so that MD_RUN_FLIGHT can override it per run):
  CDC_V2_RUNS_MODE      ticker | canceller | fail | orphan | silent | watchdog | leader
  CDC_V2_RUNS_FLIGHT    a label written into every row (the Flight name)
  CDC_V2_RUNS_DB        database (must start with cdc_v2_)
  CDC_V2_RUNS_TICK_S    ticker period in seconds (default 0.2)
  CDC_V2_RUNS_TICK_VIA  duckdb | pgwire (ticks through MotherDuck's Postgres endpoint: now() is MotherDuck's clock)
  CDC_V2_RUNS_DURATION_S how long the main role runs (default 600)
  CDC_V2_RUNS_CHILDREN  1: start a child process that starts a detached grandchild (both tick too)
  CDC_V2_RUNS_ON_TERM   ignore | exit  (what the main role does on SIGTERM/SIGINT/SIGHUP)
  CDC_V2_RUNS_TARGET    canceller: others | self | <flight_id>:<run_number> | others_of:<flight_id>
  CDC_V2_RUNS_FAIL      fail: exit3 | exception | oom_child | oom_main
  CDC_V2_RUNS_STDOUT    orphan: pipe | devnull (stdout of the child and the grandchild)
  CDC_V2_RUNS_MAIN_S    orphan: seconds before main.py exits
  CDC_V2_RUNS_WD_EXIT   watchdog/leader: osexit1 | osexit0 | killpg1 | killpg0
  CDC_V2_RUNS_FREEZE    watchdog: pipeline | all | none ; CDC_V2_RUNS_FREEZE_AT_S (default 30)
  CDC_V2_RUNS_STALE_S   watchdog/leader: heartbeat age that triggers the hard exit (default 60)
  CDC_V2_RUNS_LEADER_END leader: release | die_child | run | freeze_all ; CDC_V2_RUNS_LEAD_S (default 120)
"""
import time

T0 = time.time()
print("FIRST %.3f" % T0, flush=True)

import json  # noqa: E402
import os  # noqa: E402
import signal  # noqa: E402
import subprocess  # noqa: E402
import sys  # noqa: E402
import threading  # noqa: E402

SOURCE_VERSION = "v1"
E = os.environ.get
MODE = E("CDC_V2_RUNS_MODE", "ticker")
ROLE = E("CDC_V2_RUNS_ROLE", "main")
FLIGHT = E("CDC_V2_RUNS_FLIGHT", "?")
DB = E("CDC_V2_RUNS_DB", "cdc_v2_spike_runs_db")
RUN = int(E("MOTHERDUCK_FLIGHT_RUN_NUMBER") or 0)
FLIGHT_ID = E("MOTHERDUCK_FLIGHT_ID", "")
HB_FILE = "/tmp/cdc_v2_spike_runs_heartbeat"
STOP = threading.Event()
SIGNALS_SEEN = []
if not DB.startswith("cdc_v2_"):
    raise SystemExit("CDC_V2_RUNS_DB must start with cdc_v2_")


def uuid_lit(value):
    """Table-function arguments must be constants: render a validated UUID literal."""
    import uuid
    return "'%s'::UUID" % uuid.UUID(str(value))


def num(name, default):
    v = E(name)
    return float(v) if v not in (None, "") else float(default)


def log(kind, **kw):
    rec = {"kind": kind, "role": ROLE, "pid": os.getpid(), "run": RUN, "t": round(time.time(), 3),
           "v": SOURCE_VERSION}
    rec.update(kw)
    try:
        os.write(1, ("EV " + json.dumps(rec, default=str) + "\n").encode())
    except OSError:
        pass


# ------------------------------------------------------------------------------------------------ database
# Minimal PostgreSQL wire client (same as guard_pgwire.py): through MotherDuck's Postgres endpoint, now() is
# evaluated by MotherDuck, not by this container (the duckdb client evaluates now() locally; measured).
import base64  # noqa: E402
import hashlib  # noqa: E402
import hmac  # noqa: E402
import socket  # noqa: E402
import ssl  # noqa: E402
import struct  # noqa: E402


class PgError(Exception):
    pass


class PgWire:
    """Just enough of the PostgreSQL v3 protocol for one simple query (text results)."""

    def __init__(self, host, port, user, password, database, options=None, timeout=15.0):
        raw = socket.create_connection((host, port), timeout=timeout)
        raw.sendall(struct.pack("!ii", 8, 80877103))  # SSLRequest
        if raw.recv(1) != b"S":
            raise PgError("server refused TLS")
        ctx = ssl.create_default_context()
        self.sock = ctx.wrap_socket(raw, server_hostname=host)
        params = {"user": user, "database": database, "application_name": "cdc_v2_spike_runs_guard"}
        if options:
            params["options"] = options
        body = struct.pack("!i", 196608) + b"".join(k.encode() + b"\0" + v.encode() + b"\0"
                                                    for k, v in params.items()) + b"\0"
        self.sock.sendall(struct.pack("!i", len(body) + 4) + body)
        self._buf = b""
        self._auth(password)

    def _recv_exact(self, n):
        while len(self._buf) < n:
            chunk = self.sock.recv(65536)
            if not chunk:
                raise PgError("connection closed")
            self._buf += chunk
        out, self._buf = self._buf[:n], self._buf[n:]
        return out

    def _msg(self):
        kind, length = struct.unpack("!ci", self._recv_exact(5))
        return kind, self._recv_exact(length - 4)

    def _send(self, kind, payload):
        self.sock.sendall(kind + struct.pack("!i", len(payload) + 4) + payload)

    @staticmethod
    def _error(payload):
        fields = {}
        for part in payload.split(b"\0"):
            if part:
                fields[chr(part[0])] = part[1:].decode(errors="replace")
        return PgError("%s %s" % (fields.get("C", "?"), fields.get("M", "?")))

    def _auth(self, password):
        scram = None
        while True:
            kind, payload = self._msg()
            if kind == b"E":
                raise self._error(payload)
            if kind == b"R":
                code = struct.unpack("!i", payload[:4])[0]
                if code == 0:
                    continue
                if code == 3:  # cleartext (inside TLS)
                    self._send(b"p", password.encode() + b"\0")
                elif code == 5:  # md5
                    md5_seed = payload[4:8]
                    inner = hashlib.md5(password.encode() + b"postgres").hexdigest().encode()
                    self._send(b"p", b"md5" + hashlib.md5(inner + salt).hexdigest().encode() + b"\0")
                elif code == 10:  # SASL: SCRAM-SHA-256
                    nonce = base64.b64encode(os.urandom(18)).decode()
                    first_bare = "n=,r=" + nonce
                    data = ("n,," + first_bare).encode()
                    self._send(b"p", b"SCRAM-SHA-256\0" + struct.pack("!i", len(data)) + data)
                    scram = {"nonce": nonce, "first_bare": first_bare}
                elif code == 11:
                    server_first = payload[4:].decode()
                    attrs = dict(kv.split("=", 1) for kv in server_first.split(","))
                    salted = hashlib.pbkdf2_hmac("sha256", password.encode(), base64.b64decode(attrs["s"]),
                                                 int(attrs["i"]))
                    client_key = hmac.new(salted, b"Client Key", hashlib.sha256).digest()
                    stored = hashlib.sha256(client_key).digest()
                    final_wo_proof = "c=biws,r=" + attrs["r"]
                    auth_msg = ",".join((scram["first_bare"], server_first, final_wo_proof)).encode()
                    sig = hmac.new(stored, auth_msg, hashlib.sha256).digest()
                    proof = base64.b64encode(bytes(a ^ b for a, b in zip(client_key, sig))).decode()
                    self._send(b"p", (final_wo_proof + ",p=" + proof).encode())
                elif code == 12:
                    continue
                else:
                    raise PgError("unsupported authentication code %d" % code)
            elif kind == b"Z":
                return

    def query(self, sql):
        self._send(b"Q", sql.encode() + b"\0")
        rows, err = [], None
        while True:
            kind, payload = self._msg()
            if kind == b"D":
                n = struct.unpack("!h", payload[:2])[0]
                pos, row = 2, []
                for _ in range(n):
                    ln = struct.unpack("!i", payload[pos:pos + 4])[0]
                    pos += 4
                    if ln < 0:
                        row.append(None)
                    else:
                        row.append(payload[pos:pos + ln].decode())
                        pos += ln
                rows.append(row)
            elif kind == b"E":
                err = self._error(payload)
            elif kind == b"Z":
                if err:
                    raise err
                return rows

    def close(self):
        try:
            self._send(b"X", b"")
            self.sock.close()
        except OSError:
            pass


def sql_str(v):
    return "NULL" if v is None else "'" + str(v).replace("'", "''") + "'"


def connect():
    import duckdb
    con = duckdb.connect("md:%s?attach_mode=single" % DB)
    con.execute("SET TimeZone = 'UTC'")
    return con


class Db:
    """One connection per thread that uses it; a lock keeps a connection single-threaded.

    CDC_V2_RUNS_TICK_VIA=pgwire writes ticks through the Postgres endpoint (server-side now())."""

    def __init__(self):
        self.con = connect()
        self.lock = threading.Lock()
        self.pg = None
        if E("CDC_V2_RUNS_TICK_VIA", "duckdb") == "pgwire":
            self.pg = PgWire("pg.%s-aws.motherduck.com" % E("CDC_V2_RUNS_REGION", "us-east-1"), 5432, "postgres",
                             E("MOTHERDUCK_TOKEN", ""), DB, options="--attach_mode=single")

    def tick(self, seq, tag):
        with self.lock:
            if self.pg is not None:
                self.pg.query("INSERT INTO ticks VALUES (%s, %d, %s, %d, %d, %r, now(), %s)"
                              % (sql_str(FLIGHT), RUN, sql_str(ROLE), os.getpid(), seq, time.time(), sql_str(tag)))
                return
            self.con.execute("INSERT INTO ticks VALUES (?, ?, ?, ?, ?, ?, now(), ?)",
                             [FLIGHT, RUN, ROLE, os.getpid(), seq, time.time(), tag])

    def event(self, kind, detail=None):
        with self.lock:
            self.con.execute("INSERT INTO events VALUES (?, ?, ?, ?, ?, ?, now(), ?)",
                             [FLIGHT, RUN, ROLE, os.getpid(), kind, time.time(),
                              json.dumps(detail, default=str) if detail is not None else None])

    def query(self, sql, params=None):
        with self.lock:
            return self.con.execute(sql, params or []).fetchall()


def ticker_loop(period, tag, db=None, stdout_every=0.0):
    db = db or Db()
    seq, nxt, last_print = 0, time.time(), 0.0
    while not STOP.is_set():
        try:
            db.tick(seq, tag)
        except Exception as exc:  # noqa: BLE001
            log("tick_error", seq=seq, err=str(exc)[:200])
        if stdout_every and time.time() - last_print >= stdout_every:
            log("tick", seq=seq, tag=tag)
            last_print = time.time()
        seq += 1
        nxt += period
        STOP.wait(max(0.0, nxt - time.time()))


# ------------------------------------------------------------------------------------------------ signals
EVDB = None  # a separate connection for writes made after a signal arrived


def post_signal_write(name):
    t = time.time()
    try:
        with EVDB.lock:
            EVDB.con.execute("BEGIN")
            EVDB.con.execute("INSERT INTO events VALUES (?, ?, ?, ?, 'after_signal', ?, now(), ?)",
                             [FLIGHT, RUN, ROLE, os.getpid(), time.time(), json.dumps({"signal": name})])
            EVDB.con.execute("COMMIT")
        log("after_signal_commit_ok", signal=name, took_s=round(time.time() - t, 3))
    except Exception as exc:  # noqa: BLE001
        log("after_signal_commit_failed", signal=name, took_s=round(time.time() - t, 3), err=str(exc)[:200])


def install_signal_handlers(on_term="ignore"):
    def handler(signum, frame):
        name = signal.Signals(signum).name
        SIGNALS_SEEN.append(name)
        log("signal", signal=name)
        if signum == signal.SIGCHLD:
            return
        if EVDB is not None:
            threading.Thread(target=post_signal_write, args=(name,), daemon=True).start()
        if on_term == "exit" and signum in (signal.SIGTERM, signal.SIGINT, signal.SIGHUP):
            STOP.set()

    caught = []
    for s in signal.valid_signals():
        if s in (signal.SIGKILL, signal.SIGSTOP):
            continue
        try:
            signal.signal(s, handler)
            caught.append(int(s))
        except (OSError, ValueError, RuntimeError):
            pass
    return caught


# ------------------------------------------------------------------------------------------------ helpers
def spawn(role, extra_env=None, new_session=False, stdout=None):
    env = dict(os.environ)
    env["CDC_V2_RUNS_ROLE"] = role
    env.update(extra_env or {})
    p = subprocess.Popen([sys.executable, os.path.abspath(__file__)], env=env, start_new_session=new_session,
                         stdout=stdout, stderr=subprocess.STDOUT if stdout is not None else None)
    log("spawned", child_role=role, child_pid=p.pid, new_session=new_session)
    return p


def spawn_detached(role, stdout_devnull):
    """Double fork + setsid: the grandchild is re-parented away from its starter."""
    env = dict(os.environ)
    env["CDC_V2_RUNS_ROLE"] = role
    pid = os.fork()
    if pid == 0:
        os.setsid()
        if os.fork() != 0:
            os._exit(0)
        if stdout_devnull:
            fd = os.open(os.devnull, os.O_WRONLY)
            os.dup2(fd, 1)
            os.dup2(fd, 2)
        os.execve(sys.executable, [sys.executable, os.path.abspath(__file__)], env)
    os.waitpid(pid, 0)
    log("spawned_detached", child_role=role)


def proc_tree():
    """pid, ppid, state, comm of every process visible in the container (there is no `ps`)."""
    out = []
    for d in os.listdir("/proc"):
        if d.isdigit():
            try:
                with open("/proc/%s/stat" % d) as f:
                    s = f.read()
                comm = s[s.index("(") + 1:s.rindex(")")]
                rest = s[s.rindex(")") + 2:].split()
                out.append({"pid": int(d), "ppid": int(rest[1]), "pgid": int(rest[2]), "state": rest[0],
                            "comm": comm})
            except (OSError, ValueError):
                pass
    return sorted(out, key=lambda r: r["pid"])


def read_file(path):
    try:
        with open(path) as f:
            return f.read().strip()
    except OSError as exc:
        return "unreadable: %s" % type(exc).__name__


# ------------------------------------------------------------------------------------------------ roles
def role_ticker_child():
    """child / grandchild of ticker and orphan modes: ticks until killed (or CDC_V2_RUNS_CHILD_S)."""
    global EVDB
    install_signal_handlers("ignore")
    EVDB = Db()
    EVDB.event("start", {"ppid": os.getppid(), "pgid": os.getpgid(0), "sid": os.getsid(0)})
    if ROLE == "child" and E("CDC_V2_RUNS_GRANDCHILD", "1") == "1":
        spawn_detached("grandchild", E("CDC_V2_RUNS_STDOUT", "pipe") == "devnull")
    stop_at = time.time() + num("CDC_V2_RUNS_CHILD_S", 900)
    threading.Thread(target=lambda: (STOP.wait(max(0, stop_at - time.time())), STOP.set()), daemon=True).start()
    ticker_loop(num("CDC_V2_RUNS_TICK_S", 0.2), ROLE, stdout_every=10)


def mode_ticker():
    global EVDB
    caught = install_signal_handlers(E("CDC_V2_RUNS_ON_TERM", "ignore"))
    EVDB = Db()
    EVDB.event("start", {"mode": MODE, "version": SOURCE_VERSION, "caught_signals": caught, "pgid": os.getpgid(0),
                         "first_line_unix": T0})
    log("start", mode=MODE, caught=len(caught), config_version=E("CDC_V2_RUNS_CONFIG_TAG"))
    if E("CDC_V2_RUNS_CHILDREN") == "1":
        spawn("child")
    t = threading.Thread(target=ticker_loop, args=(num("CDC_V2_RUNS_TICK_S", 0.2), "main:" + SOURCE_VERSION),
                         kwargs={"stdout_every": 10}, daemon=True)
    t.start()
    STOP.wait(num("CDC_V2_RUNS_DURATION_S", 600))
    STOP.set()
    t.join(5)
    code = int(num("CDC_V2_RUNS_EXIT_CODE", 0))
    EVDB.event("end", {"exit_code": code, "signals": SIGNALS_SEEN})
    log("end", exit_code=code)
    sys.stdout.flush()
    os._exit(code)


def mode_canceller():
    """Run B: cancel other run(s) with MD_CANCEL_FLIGHT_RUN using only the Flight's own environment."""
    db = Db()
    target = E("CDC_V2_RUNS_TARGET", "others")
    log("start", mode=MODE, target=target, own_flight_id=FLIGHT_ID)
    targets = []
    if target == "self":
        targets = [(FLIGHT_ID, RUN)]
    elif target.startswith("others"):
        fid = target.split(":", 1)[1] if target.startswith("others_of:") else FLIGHT_ID
        rows = db.query("SELECT run_number, status, is_scheduled, scheduled_at, started_at FROM "
                        "MD_LIST_FLIGHT_RUNS(flight_id := %s, \"limit\" := 50)" % uuid_lit(fid))
        listed = [{"run_number": r[0], "status": r[1], "is_scheduled": r[2], "scheduled_at": r[3],
                   "started_at": r[4]} for r in rows]
        log("listed_runs", runs=listed)
        targets = [(fid, r["run_number"]) for r in listed
                   if str(r["status"]).upper().endswith("RUNNING") and not (fid == FLIGHT_ID and r["run_number"] == RUN)]
    else:
        fid, n = target.split(":")
        targets = [(fid, int(n))]
    results = []
    for fid, n in targets:
        before = db.query("SELECT epoch_ms(now())")[0][0] / 1000.0
        t = time.time()
        try:
            # MD_CANCEL_FLIGHT_RUN takes constants only, so the values are rendered (a UUID and an integer).
            res = db.query("SELECT * FROM MD_CANCEL_FLIGHT_RUN(flight_id := %s, run_number := %d)"
                           % (uuid_lit(fid), int(n)))
            out = {"ok": True, "result": [list(map(str, r)) for r in res]}
        except Exception as exc:  # noqa: BLE001
            out = {"ok": False, "error": str(exc)[:300]}
        out.update(flight_id=fid, run_number=n, call_client_unix=t, call_server_unix=before,
                   returned_client_unix=time.time(), took_s=round(time.time() - t, 3))
        results.append(out)
        log("cancel_result", **out)
        db.event("cancel_call", out)
    # watch the target(s) until terminal
    for fid, n in targets:
        deadline = time.time() + 60
        states = []
        while time.time() < deadline:
            r = db.query("SELECT status, cancelled_at, ended_at, exit_code FROM "
                         "MD_GET_FLIGHT_RUN(flight_id := %s, run_number := %d)" % (uuid_lit(fid), int(n)))
            st = str(r[0][0]) if r else None
            if not states or states[-1][0] != st:
                states.append((st, time.time()))
                log("target_state", run_number=n, status=st, cancelled_at=r[0][1] if r else None,
                    ended_at=r[0][2] if r else None, exit_code=r[0][3] if r else None)
            if st and any(st.upper().endswith(x) for x in ("SUCCEEDED", "FAILED", "CANCELLED")):
                break
            time.sleep(0.25)
    db.event("end", {"targets": len(targets)})
    log("end")


def mode_fail():
    kind = E("CDC_V2_RUNS_FAIL", "exit3")
    log("start", mode=MODE, fail=kind, oom_group=read_file("/sys/fs/cgroup/memory.oom.group"),
        memory_max=read_file("/sys/fs/cgroup/memory.max"), memory_events=read_file("/sys/fs/cgroup/memory.events"))
    if kind == "exit3":
        sys.exit(3)
    if kind == "exception":
        raise RuntimeError("deliberate failure (spike)")
    if kind == "oom_main":
        allocate_until_killed()
    if kind == "oom_child":
        p = spawn("oom_child")
        rc = p.wait()
        log("oom_child_exited", returncode=rc, memory_events=read_file("/sys/fs/cgroup/memory.events"),
            procs=proc_tree())
        for i in range(5):
            log("main_still_alive", i=i)
            time.sleep(1)
        log("end", exit_code=0)


def allocate_until_killed():
    chunks = []
    gib = 1 << 30
    for i in range(64):
        chunks.append(b"\x01" * gib)  # really touched: bytes * n writes every byte
        log("allocated_gib", total=i + 1, memory_current=read_file("/sys/fs/cgroup/memory.current"))
    log("allocation_finished_without_kill")


def mode_orphan():
    """main.py starts a child (same session) that starts a detached grandchild, then exits after MAIN_S."""
    global EVDB
    install_signal_handlers("ignore")
    EVDB = Db()
    devnull = E("CDC_V2_RUNS_STDOUT", "pipe") == "devnull"
    EVDB.event("start", {"mode": MODE, "stdout": "devnull" if devnull else "pipe"})
    spawn("child", stdout=subprocess.DEVNULL if devnull else None)
    t = threading.Thread(target=ticker_loop, args=(1.0, "main", EVDB), daemon=True)
    t.start()
    time.sleep(num("CDC_V2_RUNS_MAIN_S", 20))
    log("main_exiting", procs=proc_tree())
    STOP.set()
    t.join(3)
    EVDB.event("main_exit", {"procs": proc_tree()})
    sys.stdout.flush()
    os._exit(0)


def mode_silent():
    db = Db()
    log("start", mode=MODE, silent_s=num("CDC_V2_RUNS_DURATION_S", 360))
    end = time.time() + num("CDC_V2_RUNS_DURATION_S", 360)
    seq = 0
    while time.time() < end:
        db.tick(seq, "silent")  # database rows only, nothing on stdout or stderr
        seq += 1
        time.sleep(5)
    log("end", silent_for_s=round(time.time() - T0, 1))


# ------------------------------------------------------------------------------------------------ watchdog
def hard_exit(variant, pgid):
    log("hard_exit", variant=variant, pipeline_pgid=pgid)
    if variant.startswith("killpg") and pgid:
        try:
            os.killpg(pgid, signal.SIGKILL)
        except OSError as exc:
            log("killpg_failed", err=str(exc))
    os._exit(1 if variant.endswith("1") else 0)


def bootstrap_watch(pipeline, extra_children=()):
    """Minimal bootstrap watchdog: no database connection; the pipeline touches HB_FILE after each heartbeat."""
    stale = num("CDC_V2_RUNS_STALE_S", 60)
    variant = E("CDC_V2_RUNS_WD_EXIT", "killpg1")
    start = time.time()
    while True:
        rc = pipeline.poll()
        if rc is not None:
            log("pipeline_exited", returncode=rc)
            for c in extra_children:
                try:
                    os.killpg(c.pid, signal.SIGKILL)
                except OSError:
                    pass
            os._exit(0 if rc == 0 else 1)
        try:
            age = time.time() - os.stat(HB_FILE).st_mtime
        except OSError:
            age = time.time() - start
        if age > stale:
            log("heartbeat_stale", age_s=round(age, 1))
            hard_exit(variant, pipeline.pid)
        time.sleep(1)


def mode_watchdog():
    """Bootstrap: pipeline child (own process group) + unfrozen helper (own session); watch the heartbeat."""
    if ROLE == "main":
        signal.signal(signal.SIGTERM, lambda s, f: log("signal", signal="SIGTERM"))
        log("start", mode=MODE, freeze=E("CDC_V2_RUNS_FREEZE", "pipeline"), variant=E("CDC_V2_RUNS_WD_EXIT"))
        pipeline = spawn("pipeline", new_session=True)
        helper = spawn("helper", {"CDC_V2_RUNS_PIPELINE_PGID": str(pipeline.pid)}, new_session=True)
        bootstrap_watch(pipeline, [helper])
    elif ROLE == "pipeline":
        db = Db()
        install_signal_handlers("ignore")
        spawn("pgrand")  # same process group as the pipeline
        freeze = E("CDC_V2_RUNS_FREEZE", "pipeline")
        freeze_at = time.time() + num("CDC_V2_RUNS_FREEZE_AT_S", 30)
        seq = 0
        while True:
            db.tick(seq, "hb")
            with open(HB_FILE, "w") as f:
                f.write(str(time.time()))
            seq += 1
            if freeze != "none" and time.time() >= freeze_at:
                db.event("freezing", {"freeze": freeze, "pgid": os.getpgid(0)})
                log("freezing", freeze=freeze)
                if freeze == "all":
                    os.kill(os.getppid(), signal.SIGSTOP)
                os.killpg(os.getpgid(0), signal.SIGSTOP)
                log("resumed_after_freeze")
                db.event("resumed_after_freeze")
                freeze = "none"
            time.sleep(10 if freeze == "none" else max(0.1, min(10, freeze_at - time.time())))
    elif ROLE == "pgrand":
        install_signal_handlers("ignore")
        ticker_loop(1.0, "pgrand", stdout_every=10)
    elif ROLE == "helper":
        db = Db()
        pgid = int(E("CDC_V2_RUNS_PIPELINE_PGID"))
        cont_at = time.time() + num("CDC_V2_RUNS_FREEZE_AT_S", 30) + num("CDC_V2_RUNS_CONT_AFTER_S", 100)
        seq, sent = 0, False
        while True:
            db.tick(seq, "helper")
            seq += 1
            if not sent and time.time() >= cont_at:
                try:
                    os.killpg(pgid, signal.SIGCONT)
                    db.event("sent_sigcont", {"pgid": pgid})
                    log("sent_sigcont")
                except OSError as exc:
                    db.event("sigcont_failed", {"err": str(exc)})
                sent = True
            time.sleep(1)


# ------------------------------------------------------------------------------------------------ leader
def mode_leader():
    """Measuring instrument for take-over timing (not the real protocol): guard, CAS on epoch, cancel previous."""
    if ROLE == "pipeline":
        return leader_pipeline()
    db = Db()
    row = db.query("SELECT run_number, epoch, state, date_diff('millisecond', heartbeat_at, now()) FROM leader "
                   "WHERE id = 1")
    prev = {"run_number": row[0][0], "epoch": row[0][1], "state": row[0][2], "age_ms": row[0][3]} if row else None
    if prev and prev["state"] == "leading" and prev["age_ms"] < 30000:
        db.event("guard_exit_healthy", prev)
        log("guard_exit_healthy", **prev)
        os._exit(0)
    try:
        won = db.query("UPDATE leader SET run_number = ?, epoch = epoch + 1, heartbeat_at = now(), state = 'leading' "
                       "WHERE id = 1 AND epoch = ? RETURNING epoch", [RUN, prev["epoch"]])
    except Exception as exc:  # noqa: BLE001  a concurrent writer conflict means we lost
        won, err = [], str(exc)[:200]
        log("cas_error", err=err)
    if not won:
        db.event("guard_lost_race", prev)
        log("guard_lost_race")
        os._exit(0)
    epoch = won[0][0]
    db.event("takeover", {"prev": prev, "epoch": epoch})
    log("takeover", epoch=epoch, prev=prev)
    if prev and prev["state"] == "leading" and prev["run_number"]:
        try:
            st = db.query("SELECT status FROM MD_GET_FLIGHT_RUN(flight_id := %s, run_number := %d)"
                          % (uuid_lit(FLIGHT_ID), int(prev["run_number"])))
            status = str(st[0][0]) if st else None
            if status and status.upper().endswith("RUNNING"):
                db.query("SELECT * FROM MD_CANCEL_FLIGHT_RUN(flight_id := %s, run_number := %d)"
                         % (uuid_lit(FLIGHT_ID), int(prev["run_number"])))
                db.event("cancelled_previous", {"run_number": prev["run_number"]})
                log("cancelled_previous", run_number=prev["run_number"])
            else:
                db.event("previous_not_running", {"run_number": prev["run_number"], "status": status})
        except Exception as exc:  # noqa: BLE001
            db.event("cancel_previous_failed", {"err": str(exc)[:200]})
    db.con.close()
    pipeline = spawn("pipeline", {"CDC_V2_RUNS_EPOCH": str(epoch)}, new_session=True)
    bootstrap_watch(pipeline)


def leader_pipeline():
    db = Db()
    install_signal_handlers("ignore")
    epoch = int(E("CDC_V2_RUNS_EPOCH"))
    end = E("CDC_V2_RUNS_LEADER_END", "release")
    lead_until = time.time() + num("CDC_V2_RUNS_LEAD_S", 120)
    before_boundary = num("CDC_V2_RUNS_RELEASE_BEFORE_S", 5)
    db.event("leading", {"epoch": epoch, "end": end})
    while True:
        n = db.query("UPDATE leader SET heartbeat_at = now() WHERE id = 1 AND epoch = ? RETURNING epoch", [epoch])
        if not n:
            db.event("fenced", {"epoch": epoch})
            log("fenced", epoch=epoch)
            os._exit(4)
        with open(HB_FILE, "w") as f:
            f.write(str(time.time()))
        now = time.time()
        if now >= lead_until:
            if end == "release":
                # release a few seconds before the next minute boundary
                boundary = (int(now // 60) + 1) * 60
                if boundary - now < before_boundary + 1:
                    boundary += 60
                time.sleep(max(0, boundary - before_boundary - time.time()))
                db.query("UPDATE leader SET state = 'released', heartbeat_at = now() WHERE id = 1 AND epoch = ?",
                         [epoch])
                db.event("released", {"epoch": epoch})
                log("released", epoch=epoch)
                os._exit(0)
            if end == "die_child":
                db.event("pipeline_sigkill_self", {"epoch": epoch})
                log("pipeline_sigkill_self")
                os.kill(os.getpid(), signal.SIGKILL)
            if end == "freeze_all":
                db.event("freezing", {"freeze": "all", "epoch": epoch})
                log("freezing_all")
                os.kill(os.getppid(), signal.SIGSTOP)
                os.kill(os.getpid(), signal.SIGSTOP)
            # end == "run": keep leading until cancelled from outside
        time.sleep(min(10, max(0.5, lead_until - time.time())) if time.time() < lead_until else 10)


MODES = {"ticker": mode_ticker, "canceller": mode_canceller, "fail": mode_fail, "orphan": mode_orphan,
         "silent": mode_silent, "watchdog": mode_watchdog, "leader": mode_leader}


def main():
    if ROLE in ("child", "grandchild"):
        return role_ticker_child()
    if ROLE == "oom_child":
        log("start", memory_max=read_file("/sys/fs/cgroup/memory.max"))
        return allocate_until_killed()
    MODES[MODE]()


if __name__ == "__main__":
    main()
