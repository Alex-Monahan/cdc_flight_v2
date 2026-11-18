"""Bootstrap of the cdc_flight_v2 Flight (SPEC R4, R18, 4.2; docs/design/core.md sections 4 and 5;
docs/design/leadership.md). It is the only code stored in the Flight, and it runs before anything of the
repository can be imported, so it needs nothing but the Python standard library and the `duckdb` client.

In order (design 5):

1. **Guard** (design 4.4): read the leader record. A healthy leader (state `leading` and a heartbeat younger than
   the stale age by MotherDuck's clock) ends this run: exit 0 when it is a run of this Flight, a notice for the
   alert `contending_flight` and exit 3 when it is another Flight. Absent leader tables are created first (the only
   DDL before the decision).
2. **Take-over** (design 4.5): one transaction raises the epoch of the leader row, of every fence row (with their
   counter `n`) and of the heartbeat row, and appends the leader machine's row to `_cdc.transitions` when that
   table exists. Losers read the record again and exit by rule 1. A conflict with an open transaction of an earlier
   run is answered by interrupting the connections that `_cdc.connections` lists for earlier epochs. An unknown
   commit outcome is decided by `run_id` in a new read. After winning: interrupt once more, cancel the previous run
   if it still runs.
3. **Start-up heartbeat**: a thread writes `phase = 'starting'` every interval until the pipeline's first tick,
   bounded by the start-up timeout.
4. **Source and artifacts**: the repository at the pinned commit (or a local source path), the pinned JRE and jars.
5. **Pipeline**: `python -m cdc_v2.pipeline <config>` in its own session, with the epoch, the run identity and the
   tick descriptor in its environment (contract C12).
6. **Watch**: read ticks, no network. No tick for the hard-exit age: SIGKILL to the pipeline's process group, then
   `os._exit(1)`. The pipeline exits: exit with its status.

Every MotherDuck statement has a deadline; every timing is a setting, checked when the settings are built. The
SQL texts below that write are the take-over and checked copies of the package's statements
(`tests/leader/test_bootstrap_copies.py`). Nothing printed here holds a secret, a host or a connection string.
"""
import time

T0_MONO = time.monotonic()                   # open point 17: "a newcomer exits within 10 s" is measured from here
T0_WALL = time.time()
if __name__ == "__main__":
    print('BOOT {"event": "first_line"}', flush=True)

import json  # noqa: E402
import os  # noqa: E402
import random  # noqa: E402
import re  # noqa: E402
import select  # noqa: E402
import shutil  # noqa: E402
import signal  # noqa: E402
import subprocess  # noqa: E402
import sys  # noqa: E402
import threading  # noqa: E402
import uuid  # noqa: E402

try:
    import duckdb
except ImportError:                          # the Flight's requirements hold duckdb; tests of the watch need none
    duckdb = None

# ------------------------------------------------------------------------------------------------- exit codes
EXIT_OK = 0                 # a healthy leader of this Flight exists; or the pipeline released (its 0)
EXIT_HARD = 1               # hard exit: no tick, or no start-up heartbeat, for the hard-exit age
EXIT_SETTINGS = 2           # the environment does not make valid settings
EXIT_CONTENDING = 3         # a healthy leader of another Flight exists (alert contending_flight)
EXIT_NO_TAKEOVER = 4        # the take-over did not win within leader.takeover_timeout
EXIT_STARTUP = 5            # the pipeline sent no tick within run.startup_timeout
EXIT_SOURCE = 6             # clone, artifacts or the start of the pipeline failed
EXIT_DAMAGED = 7            # the leader tables hold what no run writes
EXIT_NO_GUARD = 8           # the guard could not read the record within its time
EXIT_SUPERSEDED = 10        # another run took over (also the pipeline's own code for it, C12)

# ------------------------------------------------------------------------------------------------- the leader machine
#: The bootstrap's copy of the leader machine (contract C5 names this exception); compared with the declaration in
#: `cdc_v2.leader.machine` by a T0 test. The take-over makes the first two transitions, the release the third.
LEADER_STATES = ("leading", "released")
LEADER_TRANSITIONS = (("released", "leading"), ("leading", "leading"), ("leading", "released"))

# ------------------------------------------------------------------------------------------------- settings
#: (attribute, environment variable, default seconds, shared with the pipeline's `cdc_v2.leader.LeaderSettings`)
TIMINGS = (
    ("heartbeat_interval_s", "CDC_V2_HEARTBEAT_INTERVAL_S", 10.0, True),
    ("stale_age_s", "CDC_V2_STALE_AGE_S", 30.0, True),
    ("hard_exit_age_s", "CDC_V2_HARD_EXIT_AGE_S", 60.0, True),
    ("startup_timeout_s", "CDC_V2_STARTUP_TIMEOUT_S", 180.0, True),
    ("takeover_timeout_s", "CDC_V2_TAKEOVER_TIMEOUT_S", 45.0, True),
    ("late_commit_wait_s", "CDC_V2_LATE_COMMIT_WAIT_S", 2.5, True),
    ("max_runtime_s", "CDC_V2_RUN_MAX_RUNTIME_S", 27000.0, True),
    ("release_before_boundary_s", "CDC_V2_RELEASE_BEFORE_BOUNDARY_S", 5.0, True),
    ("minute_s", "CDC_V2_MINUTE_S", 60.0, True),
    ("takeover_pause_s", "CDC_V2_TAKEOVER_PAUSE_S", 0.25, False),
    ("guard_timeout_s", "CDC_V2_GUARD_TIMEOUT_S", 8.0, False),
    ("statement_timeout_s", "CDC_V2_STATEMENT_TIMEOUT_S", 8.0, False),
    ("grace_s", "CDC_V2_GRACE_S", 1.0, False),
    ("cancel_timeout_s", "CDC_V2_CANCEL_TIMEOUT_S", 10.0, False),
    ("step_timeout_s", "CDC_V2_STEP_TIMEOUT_S", 150.0, False),
)
_HEX40 = re.compile(r"^[0-9a-f]{40}$")
_MD_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,62}$")
_MEM_NAME = re.compile(r"^:memory:[A-Za-z0-9_]*$")
_UUIDISH = re.compile(r"^[0-9A-Za-z-]{1,64}$")


def check_order(interval, stale, hard_exit):
    """The order of the leader timings (design 4.3): interval at most a third of the stale age (the design writes
    "<", and its own defaults 10 s and 30 s meet it only with equality), stale age < hard-exit age. Returns the
    list of what is wrong (empty: fine). The package checks the same (`cdc_v2.leader.settings`)."""
    out = []
    for name, v in (("heartbeat interval", interval), ("stale age", stale), ("hard-exit age", hard_exit)):
        if not v > 0:
            out.append("%s must be > 0" % name)
    if not interval <= stale / 3.0:
        out.append("heartbeat interval %.3f s must be at most a third of the stale age %.3f s" % (interval, stale))
    if not stale < hard_exit:
        out.append("stale age %.3f s must be below the hard-exit age %.3f s" % (stale, hard_exit))
    return out


class Settings:
    """Everything the bootstrap needs, from the environment (the Flight's config and secrets, or LocalPlatform)."""

    def __init__(self, env):
        g = env.get
        problems = []
        for attr, var, default, _shared in TIMINGS:
            text = (g(var) or "").strip()
            try:
                value = float(text) if text else float(default)
            except ValueError:
                problems.append("%s is not a number of seconds" % var)
                value = float(default)
            if not (value > 0 and value < float("inf")):
                problems.append("%s must be a number of seconds > 0" % var)
            setattr(self, attr, value)
        problems += check_order(self.heartbeat_interval_s, self.stale_age_s, self.hard_exit_age_s)
        self.database = (g("CDC_V2_DATABASE") or "").strip()
        if not self.database:
            problems.append("CDC_V2_DATABASE is not set")
        elif self.database.lower().startswith(("md:", "motherduck:")) or "?" in self.database:
            problems.append("CDC_V2_DATABASE is a database name, not a connection string")
        elif self.database.startswith(":memory:") and not _MEM_NAME.match(self.database):
            problems.append("CDC_V2_DATABASE: an in-memory database is ':memory:<letters, digits, _>'")
        self.marker = (g("CDC_V2_MARKER") or "cdc_v2").strip()
        self.flight_id = (g("MOTHERDUCK_FLIGHT_ID") or "").strip()
        number = (g("MOTHERDUCK_FLIGHT_RUN_NUMBER") or "").strip()
        self.run_number = int(number) if number.isdigit() else None
        self.run_id = (g("MOTHERDUCK_FLIGHT_RUN_ID") or "").strip() or uuid.uuid4().hex
        if not self.flight_id or not _UUIDISH.match(self.flight_id):
            problems.append("MOTHERDUCK_FLIGHT_ID is missing or not an id")
        if self.run_number is None:
            problems.append("MOTHERDUCK_FLIGHT_RUN_NUMBER is missing or not a number")
        if not _UUIDISH.match(self.run_id):
            problems.append("MOTHERDUCK_FLIGHT_RUN_ID is not an id")
        self.source_dir = (g("CDC_V2_SOURCE_DIR") or "").strip()
        self.repo = (g("CDC_V2_REPO") or "https://github.com/Alex-Monahan/cdc_flight_v2").strip()
        self.commit = (g("CDC_V2_COMMIT") or "").strip()
        if not self.source_dir and not _HEX40.match(self.commit):
            problems.append("CDC_V2_COMMIT must be a full commit hash (or CDC_V2_SOURCE_DIR a local source path)")
        if self.source_dir and not os.path.isdir(self.source_dir):
            problems.append("CDC_V2_SOURCE_DIR is not a directory")
        self.work_dir = (g("CDC_V2_WORK_DIR") or "/tmp/cdc_v2_run").strip()
        self.artifacts = (g("CDC_V2_ARTIFACTS") or "1").strip() not in ("0", "false", "no", "off")
        self.requirements = (g("CDC_V2_PIP_REQUIREMENTS") or "").strip()
        self.pipeline = (g("CDC_V2_PIPELINE") or "module:cdc_v2.pipeline").strip()
        if not re.match(r"^(module:[A-Za-z_][A-Za-z0-9_.]*|script:[A-Za-z0-9_./-]+\.py)$", self.pipeline) \
                or ".." in self.pipeline:
            problems.append("CDC_V2_PIPELINE is module:<name> or script:<path in the source tree>")
        self.config = (g("CDC_V2_CONFIG") or "flights/cdc_v2/pipeline.toml").strip()
        self.local_platform_dir = (g("CDC_V2_LOCAL_PLATFORM_DIR") or "").strip()
        self.interrupt = (g("CDC_V2_TAKEOVER_INTERRUPT") or "1").strip() not in ("0", "false", "no", "off")
        if problems:
            raise ValueError("; ".join(problems))

    @property
    def is_motherduck(self):
        return bool(_MD_NAME.match(self.database))

    @property
    def target(self):
        """The text passed to `duckdb.connect` (never holds the token: the client reads it from the environment)."""
        if self.is_motherduck:
            return "md:%s?attach_mode=single" % self.database
        return self.database


# ------------------------------------------------------------------------------------------------- output
_out_lock = threading.Lock()


def say(event, **fields):
    """One line on standard output: ``BOOT {json}``. Identifiers, counts and durations only (no value of a row, no
    secret, no host)."""
    fields = dict(fields)
    fields["event"] = event
    fields["t_s"] = round(time.monotonic() - T0_MONO, 3)
    line = "BOOT " + json.dumps(fields, sort_keys=True, default=str) + "\n"
    with _out_lock:
        try:
            os.write(1, line.encode())
        except OSError:
            pass


def finish(code, **fields):
    """End the bootstrap now. `os._exit`: no interpreter teardown that could wait for a blocked thread."""
    say("exit", code=code, after_first_line_s=round(time.monotonic() - T0_MONO, 3), **fields)
    os._exit(code)


# ------------------------------------------------------------------------------------------------- crash points
#: The bootstrap's crash points. They cannot be declared through `cdc_v2.crashpoints` (nothing of the repository is
#: importable here), so this list is mirrored by `cdc_v2.leader.points` and a T0 test compares the two. The
#: environment contract is the one of `docs/testing/harness.md`, implemented again below.
BOOT_POINTS = (
    ("leader.tables_created", "bootstrap: the leader tables were absent and are now created (a first run only)",
     True),
    ("leader.takeover_sent", "bootstrap: immediately before COMMIT of the take-over (run with kill_later)", True),
    ("leader.taken_over", "bootstrap: COMMIT of the take-over returned; this run leads", True),
    ("leader.interrupted", "bootstrap: the connections of earlier epochs were interrupted after the take-over",
     True),
    ("leader.previous_cancelled", "bootstrap: the previous run was cancelled (only when it was still running)",
     True),
    ("leader.startup_beat", "bootstrap: a start-up heartbeat committed", False),
    ("leader.pipeline_started", "bootstrap: the pipeline process was started", True),
    ("leader.first_tick", "bootstrap: the pipeline's first tick arrived; the start-up heartbeat stops", True),
)
_POINT_NAMES = frozenset(p[0] for p in BOOT_POINTS)
_M64 = (1 << 64) - 1


def splitmix64(x):
    x = (x + 0x9E3779B97F4A7C15) & _M64
    z = x
    z = ((z ^ (z >> 30)) * 0xBF58476D1CE4E5B9) & _M64
    z = ((z ^ (z >> 27)) * 0x94D049BB133111EB) & _M64
    return z ^ (z >> 31)


def parse_hit(text):
    text = (text or "").strip()
    if not text:
        return 1
    m = re.match(r"^seed:(\d+):(\d+)$", text)
    if m:
        seed, mx = int(m.group(1)), int(m.group(2))
        if seed > _M64 or mx < 1:
            raise ValueError("CDC_V2_CRASH_HIT: seed must fit 64 bits and M must be >= 1")
        return 1 + splitmix64(seed & _M64) % mx
    if not text.isdigit() or int(text) < 1:
        raise ValueError("CDC_V2_CRASH_HIT: expected N >= 1 or seed:S:M")
    return int(text)


class _Crash:
    def __init__(self, env):
        self.point = (env.get("CDC_V2_CRASH_POINT") or "").strip()
        self.action = (env.get("CDC_V2_CRASH_ACTION") or "kill").strip() or "kill"
        if self.action not in ("kill", "kill_later", "stop", "hang", "raise"):
            raise ValueError("CDC_V2_CRASH_ACTION: unknown action")
        self.target = parse_hit(env.get("CDC_V2_CRASH_HIT"))
        delay = (env.get("CDC_V2_CRASH_DELAY_MS") or "0").strip() or "0"
        if not delay.isdigit():
            raise ValueError("CDC_V2_CRASH_DELAY_MS: expected milliseconds >= 0")
        self.delay_ms = int(delay)
        self.tag = (env.get("CDC_V2_CRASH_TAG") or "").strip()
        if not re.match(r"^[A-Za-z0-9_.-]{0,64}$", self.tag):
            raise ValueError("CDC_V2_CRASH_TAG: [A-Za-z0-9_.-], at most 64 characters")
        log = (env.get("CDC_V2_CRASH_LOG") or "").strip()
        self.log_fd = os.open(log, os.O_WRONLY | os.O_APPEND | os.O_CREAT, 0o644) if log else None
        self.counts = {}
        self.fired = False
        self.lock = threading.Lock()

    def hit(self, name):
        with self.lock:
            n = self.counts.get(name, 0) + 1
            self.counts[name] = n
            fire = (not self.fired) and name == self.point and n == self.target
            if fire:
                self.fired = True
            if self.log_fd is not None:
                line = json.dumps({"point": name, "hit": n, "pid": os.getpid(), "tag": self.tag,
                                   "t": round(time.time(), 6), "fire": fire,
                                   "action": self.action if fire else ""}, separators=(",", ":")) + "\n"
                os.write(self.log_fd, line.encode())
        if not fire:
            return
        if self.action == "kill_later":
            def later():
                time.sleep(self.delay_ms / 1000.0)
                os.kill(os.getpid(), signal.SIGKILL)
            threading.Thread(target=later, daemon=True).start()
        elif self.action == "kill":
            os.kill(os.getpid(), signal.SIGKILL)
            while True:
                time.sleep(1)
        elif self.action == "stop":
            os.kill(os.getpid(), signal.SIGSTOP)
        elif self.action == "hang":
            while True:
                time.sleep(3600)
        else:
            raise SystemExit("crash point %s fired at hit %d" % (name, n))


_crash = None


def crash_point(name):
    if name not in _POINT_NAMES:
        raise RuntimeError("undeclared bootstrap crash point %r" % name)
    if _crash is not None:
        _crash.hit(name)


def crash_init(env):
    global _crash
    point = (env.get("CDC_V2_CRASH_POINT") or "").strip()
    _crash = _Crash(env) if (point or (env.get("CDC_V2_CRASH_LOG") or "").strip()) else None


# ------------------------------------------------------------------------------------------------- SQL
# Copies of `cdc_v2.md.schema.NAMED_STATEMENTS` (compared character for character by a T0 test).
CREATE_SCHEMA_SQL = "CREATE SCHEMA IF NOT EXISTS _cdc"
CREATE_LEADER_SQL = (
    "CREATE TABLE IF NOT EXISTS _cdc.leader AS SELECT "
    "CAST(0 AS BIGINT) AS epoch, "
    "CAST(NULL AS VARCHAR) AS flight_id, "
    "CAST(NULL AS BIGINT) AS run_number, "
    "CAST(NULL AS VARCHAR) AS run_id, "
    "CAST('released' AS VARCHAR) AS state, "
    "CAST(NULL AS TIMESTAMPTZ) AS taken_at"
)
CREATE_FENCE_SQL = (
    "CREATE TABLE IF NOT EXISTS _cdc.fence AS SELECT "
    "CAST(lane AS VARCHAR) AS lane, CAST(0 AS BIGINT) AS epoch, CAST(0 AS BIGINT) AS n "
    "FROM (VALUES ('stream'), ('control'), "
    "('backfill.0'), ('backfill.1'), ('backfill.2'), ('backfill.3'), "
    "('backfill.4'), ('backfill.5'), ('backfill.6'), ('backfill.7'), "
    "('backfill.8'), ('backfill.9'), ('backfill.10'), ('backfill.11'), "
    "('backfill.12'), ('backfill.13'), ('backfill.14'), ('backfill.15')) AS lanes(lane)"
)
CREATE_HEARTBEAT_SQL = (
    "CREATE TABLE IF NOT EXISTS _cdc.heartbeat AS SELECT "
    "CAST(0 AS BIGINT) AS epoch, "
    "CAST(NULL AS TIMESTAMPTZ) AS beat_at, "
    "CAST(NULL AS TIMESTAMPTZ) AS progress_at, "
    "CAST('none' AS VARCHAR) AS phase"
)
CREATE_CONNECTIONS_SQL = (
    "CREATE TABLE IF NOT EXISTS _cdc.connections "
    "(epoch BIGINT, lane VARCHAR, connection_id UUID, registered_at TIMESTAMPTZ)"
)
CHECK_LEADER_TABLES_SQL = (
    "SELECT (SELECT count(*) FROM _cdc.leader), (SELECT count(*) FROM _cdc.fence), "
    "(SELECT count(DISTINCT lane) FROM _cdc.fence), (SELECT count(*) FROM _cdc.heartbeat)"
)
HEARTBEAT_SQL = (
    "UPDATE _cdc.heartbeat SET beat_at = uuid_extract_timestamp(uuidv7()), "
    "progress_at = uuid_extract_timestamp(uuidv7()) - to_microseconds(CAST(? AS BIGINT)), phase = ? WHERE epoch = ?"
)
REGISTER_SQL = ("INSERT INTO _cdc.connections (epoch, lane, connection_id, registered_at) "
                "VALUES (?, ?, ?::UUID, uuid_extract_timestamp(uuidv7()))")
REGISTER_FENCE_SQL = "UPDATE _cdc.fence SET n = n + 1 WHERE lane = ? AND epoch = ?"
LIST_EARLIER_SQL = "SELECT DISTINCT connection_id::VARCHAR FROM _cdc.connections WHERE epoch < ?"
INTERRUPT_EARLIER_SQL = ("SELECT count(*), count(*) FILTER (WHERE md_interrupt_server_connection(connection_id)) "
                         "FROM (SELECT DISTINCT connection_id FROM _cdc.connections "
                         "WHERE epoch < ? AND connection_id <> ?::UUID)")
NO_CONNECTION = "00000000-0000-0000-0000-000000000000"
COPIES = {
    "create_schema": CREATE_SCHEMA_SQL,
    "create_leader": CREATE_LEADER_SQL,
    "create_fence": CREATE_FENCE_SQL,
    "create_heartbeat": CREATE_HEARTBEAT_SQL,
    "create_connections": CREATE_CONNECTIONS_SQL,
    "check_leader_tables": CHECK_LEADER_TABLES_SQL,
    "heartbeat": HEARTBEAT_SQL,
    "register_connection": REGISTER_SQL,
    "register_fence": REGISTER_FENCE_SQL,
    "list_earlier_connections": LIST_EARLIER_SQL,
    "interrupt_earlier_connections": INTERRUPT_EARLIER_SQL,
}
LEADER_TABLE_STATEMENTS = (CREATE_SCHEMA_SQL, CREATE_LEADER_SQL, CREATE_FENCE_SQL, CREATE_HEARTBEAT_SQL,
                           CREATE_CONNECTIONS_SQL)
LANE_COUNT = 18

# The take-over (design 4.5, as built by package D; compared with `cdc_v2.testing.destination`). Each exclusion
# rests on a column that both sides change: two take-overs meet on the leader row's `epoch`; a take-over and an old
# run's lane transaction, or a registration, on a fence row's `n` (raised, never put back); a take-over and a
# heartbeat on `beat_at`; a release (a fenced control-lane transaction) on the control fence row's `n`.
TAKEOVER_LEADER_SQL = (
    "UPDATE _cdc.leader SET epoch = epoch + 1, flight_id = ?, run_number = ?, run_id = ?, state = 'leading', "
    "taken_at = uuid_extract_timestamp(uuidv7()) WHERE epoch = ?"
)
TAKEOVER_FENCE_SQL = "UPDATE _cdc.fence SET epoch = ? + 1, n = n + 1"
TAKEOVER_HEARTBEAT_SQL = (
    "UPDATE _cdc.heartbeat SET epoch = ? + 1, beat_at = uuid_extract_timestamp(uuidv7()), phase = 'starting'"
)
#: The leader machine's row in `_cdc.transitions`, written in the take-over transaction (a copy of the statement
#: of `cdc_v2.state`). `seq` is the control fence row's `n` after the commit, as for a control-lane transaction.
TRANSITION_SQL = ("INSERT INTO _cdc.transitions (machine, key, from_state, to_state, cause, epoch, lane, seq, detail, "
                  "changed_at) VALUES (?, ?, ?, ?, ?, ?, ?, (SELECT CAST(n + 1 AS UBIGINT) FROM _cdc.fence WHERE "
                  "lane = ?), CAST(? AS JSON), uuid_extract_timestamp(uuidv7()))")
#: A contending Flight's notice (`cdc_v2.leader.schema.NOTICE_SQL`); the leader turns it into the alert.
NOTICE_SQL = ("INSERT INTO _cdc.contenders (noticed_at, flight_id, run_number, run_id, leader_epoch, "
              "leader_flight_id, leader_run_number) VALUES (uuid_extract_timestamp(uuidv7()), ?, ?, ?, ?, ?, ?)")

#: The leader record with the heartbeat's age by MotherDuck's clock, computed inside the statement (design 4.1).
READ_RECORD_SQL = (
    "SELECT l.epoch, l.state, l.flight_id, l.run_number, l.run_id, h.epoch, h.phase, "
    "epoch_us(uuid_extract_timestamp(uuidv7())) - epoch_us(h.beat_at) "
    "FROM _cdc.leader AS l, _cdc.heartbeat AS h"
)
HAS_TABLE_SQL = ("SELECT count(*) FROM information_schema.tables WHERE table_catalog = current_database() "
                 "AND table_schema = '_cdc' AND table_name = ?")
OWN_ID_SQL = "SELECT md_current_client_connection_id()"
INTERRUPT_SQL = "SELECT md_interrupt_server_connection(?::UUID)"

# ------------------------------------------------------------------------------------------------- errors
# The classification of `cdc_v2.md.errors` (rules 2 to 5), in its smallest form; the patterns are compared.
TRANSPORT_RE = re.compile(
    r"(?i:\bConnection Error\b|\binterrupted by user\b|\blease expired\b|\btransaction already rolled back\b|"
    r"\bRequest failed\b|\bCould not connect\b|\bConnection already closed\b|\bNo open result set\b|"
    r"\bconnection (?:has been |was |is )?closed\b|\bService Unavailable\b|\bBad Gateway\b|\bGateway Time-?out\b)|"
    r"\bUNAVAILABLE\b|\bDEADLINE_EXCEEDED\b")
CONFLICT_RE = re.compile(r"(?i:\bconflict on \w+|\bwrite-write conflict\b|\bTransaction conflict\b)")
_QUOTE = re.compile(r"""(?<![A-Za-z])['"]""")


class BootError(Exception):
    """Base of the bootstrap's own errors. The message holds a class name and a kind, never the client's text."""


class Transport(BootError):
    pass


class Conflict(BootError):
    pass


class Refused(BootError):
    pass


class Timeout(BootError):
    pass


class Unknown(BootError):
    """The outcome of a COMMIT is not known."""


def head(exc):
    text = str(exc).strip()
    line = text.splitlines()[0] if text else ""
    m = _QUOTE.search(line)
    return line if m is None else line[:m.start()].rstrip()


def kind_of(exc):
    """``transport``, ``conflict`` or ``other`` (the rules of `cdc_v2.md.errors.kind_of`)."""
    if duckdb is not None:
        if isinstance(exc, (duckdb.DataError, duckdb.IntegrityError, duckdb.ProgrammingError)):
            return "other"
        if isinstance(exc, (duckdb.ConnectionException, duckdb.InterruptException)):
            return "transport"
    if isinstance(exc, (ConnectionError, TimeoutError, OSError)):
        return "transport"
    if duckdb is None or not isinstance(exc, duckdb.Error):
        return "other"
    text = head(exc)
    if TRANSPORT_RE.search(text):
        return "transport"
    if isinstance(exc, duckdb.IOException):
        return "other"
    if CONFLICT_RE.search(text):
        return "conflict"
    return "other"


def label(exc):
    m = TRANSPORT_RE.search(head(exc)) if duckdb is not None and isinstance(exc, duckdb.Error) else None
    return "%s (%s)" % (type(exc).__name__, m.group(0)) if m else type(exc).__name__


# ------------------------------------------------------------------------------------------------- connections
class _Job:
    __slots__ = ("fn", "done", "result", "error")

    def __init__(self, fn):
        self.fn, self.done, self.result, self.error = fn, threading.Event(), None, None


class _Worker:
    """One thread that runs the calls of one client connection; a caller waits for a call until its deadline."""

    def __init__(self, name):
        self.jobs = []
        self.cv = threading.Condition()
        self.current = None
        self.thread = threading.Thread(target=self._loop, name=name, daemon=True)
        self.thread.start()

    def _loop(self):
        while True:
            with self.cv:
                while not self.jobs:
                    self.cv.wait()
                job = self.jobs.pop(0)
            if job is None:
                return
            self.current = job
            try:
                job.result = job.fn()
            except BaseException as e:          # noqa: BLE001 - handed to the caller
                job.error = e
            finally:
                job.done.set()

    def submit(self, fn):
        job = _Job(fn)
        with self.cv:
            self.jobs.append(job)
            self.cv.notify()
        return job

    def stop(self):
        with self.cv:
            self.jobs.append(None)
            self.cv.notify()

    def waiting(self):
        job = self.current
        return job is not None and not job.done.is_set()


class Db:
    """One client connection to the destination, with a deadline on every call (contract C0). The client cannot
    end a statement that runs on MotherDuck (`docs/design/destination.md` section 4), so a call past its deadline
    is interrupted on the server through a side connection, waited for a grace period and then given up: the
    connection is never used again and the next call opens a new one. Calls given up are counted; past
    `MAX_ABANDONED` no new connection is opened (each holds a thread until the network answers)."""

    MAX_ABANDONED = 4

    def __init__(self, s, role, interrupt_ids=None):
        self.s, self.role = s, role
        self.raw = None
        self.sid = None
        self.worker = None
        self.abandoned = []
        self.interrupt_ids = interrupt_ids          # local destinations only (tests): interrupts ids of this process

    # -- plumbing
    def _wait(self, job, deadline):
        left = max(0.0, deadline - time.monotonic())
        if job.done.wait(left):
            if job.error is not None:
                raise job.error
            return job.result
        self._interrupt_self()
        job.done.wait(self.s.grace_s)
        self._abandon()
        raise Timeout("%s: no answer within the deadline" % self.role)

    def _interrupt_self(self):
        raw, sid = self.raw, self.sid
        if raw is not None:
            try:
                raw.interrupt()
            except Exception:                        # noqa: BLE001
                pass
        if self.s.is_motherduck and sid:
            target, marker = self.s.target, self.s.marker

            def side():
                try:
                    c = duckdb.connect(target, config={"custom_user_agent": marker})
                    try:
                        c.execute(INTERRUPT_SQL, [sid]).fetchall()
                    finally:
                        c.close()
                except Exception:                    # noqa: BLE001 - best effort; the reaper is MotherDuck's lease
                    pass
            threading.Thread(target=side, daemon=True).start()

    def _abandon(self):
        if self.worker is not None:
            self.abandoned.append(self.worker)
            self.worker.stop()
        self.worker, self.raw, self.sid = None, None, None

    def drop(self):
        """Give the connection up (after a lost connection); the next call opens a new one."""
        raw = self.raw
        self._abandon()
        if raw is not None:
            threading.Thread(target=lambda: _quiet(raw.close), daemon=True).start()

    def ensure(self, deadline):
        if self.raw is not None:
            return
        self.abandoned = [w for w in self.abandoned if w.waiting()]
        if len(self.abandoned) >= self.MAX_ABANDONED:
            raise Transport("%s: %d calls that were given up still wait for the destination" %
                            (self.role, len(self.abandoned)))
        s = self.s

        def open_():
            raw = duckdb.connect(s.target, config={"custom_user_agent": s.marker})
            raw.execute("SET SESSION TimeZone = 'UTC'")
            sid = str(raw.execute(OWN_ID_SQL).fetchone()[0]) if s.is_motherduck else str(uuid.uuid4())
            return raw, sid

        self.worker = _Worker("cdc_v2-boot-%s" % self.role)
        try:
            self.raw, self.sid = self._wait(self.worker.submit(open_), deadline)
        except Timeout:
            raise
        except Exception as e:                       # noqa: BLE001 - a connect that fails is a lost connection
            self._abandon()
            raise Transport("%s: cannot connect (%s)" % (self.role, type(e).__name__)) from None

    def run(self, sql, params=None, fetch=None, deadline=None):
        """One statement. ``fetch``: None, ``one``, ``all`` or ``count`` (rows an UPDATE/INSERT changed)."""
        deadline = deadline if deadline is not None else time.monotonic() + self.s.statement_timeout_s
        self.ensure(deadline)
        raw = self.raw

        def call():
            cur = raw.execute(sql, params) if params is not None else raw.execute(sql)
            if fetch == "one":
                return cur.fetchone()
            if fetch == "all":
                return cur.fetchall()
            if fetch == "count":
                row = cur.fetchone()
                return int(row[0]) if row is not None and row[0] is not None else 0
            return None

        try:
            return self._wait(self.worker.submit(call), deadline)
        except BootError:
            raise
        except Exception as e:                       # noqa: BLE001 - classified, the client's text is not kept
            kind = kind_of(e)
            if kind == "transport":
                self.drop()
                raise Transport("%s: connection lost (%s)" % (self.role, label(e))) from None
            if kind == "conflict":
                raise Conflict("%s: write-write conflict" % self.role) from None
            raise Refused("%s: the destination refused a statement (%s)" % (self.role, type(e).__name__)) from None

    def commit(self, deadline):
        """COMMIT. A conflict: nothing applied. Anything else that goes wrong: the outcome is unknown."""
        try:
            self.run("COMMIT", None, None, deadline)
        except Conflict:
            self.rollback()
            raise
        except BootError as e:
            self.drop()
            raise Unknown("%s: the outcome of COMMIT is unknown (%s)" % (self.role, type(e).__name__)) from None

    def rollback(self):
        if self.raw is None:
            return
        try:
            self.run("ROLLBACK", None, None, time.monotonic() + min(self.s.statement_timeout_s, 5.0))
        except BootError:
            self.drop()

    def close(self):
        raw, w = self.raw, self.worker
        self.raw = self.worker = self.sid = None
        if w is not None:
            w.stop()
        if raw is not None:
            threading.Thread(target=lambda: _quiet(raw.close), daemon=True).start()


def _quiet(fn):
    try:
        fn()
    except Exception:                                # noqa: BLE001
        pass


# ------------------------------------------------------------------------------------------------- the record
class Record:
    """The leader record as the guard reads it. ``age_us`` is the heartbeat's age by MotherDuck's clock (None when
    no heartbeat was ever written)."""

    __slots__ = ("epoch", "state", "flight_id", "run_number", "run_id", "hb_epoch", "phase", "age_us")

    def __init__(self, row):
        self.epoch, self.state, self.flight_id = int(row[0]), row[1], row[2]
        self.run_number = None if row[3] is None else int(row[3])
        self.run_id, self.hb_epoch, self.phase = row[4], int(row[5]), row[6]
        self.age_us = None if row[7] is None else int(row[7])

    def as_dict(self):
        return {k: getattr(self, k) for k in self.__slots__}


def healthy(rec, stale_age_s):
    """Design 4.3: healthy = state `leading` and a heartbeat of the leader's epoch newer than the stale age. A
    heartbeat exactly at the limit is not healthy."""
    return (rec.state == "leading" and rec.hb_epoch == rec.epoch and rec.age_us is not None
            and rec.age_us < int(round(stale_age_s * 1e6)))


def decide(rec, own_flight_id, stale_age_s):
    """The guard decision (design 4.4): ``(decision, reason)``. Decisions: ``exit_same`` (healthy leader of this
    Flight), ``exit_other`` (healthy leader of another Flight), ``damaged`` (a state no run writes) and
    ``take_over``; the reason of a take-over is ``initial`` (nobody led yet), ``released`` or ``stale``."""
    if rec.state not in LEADER_STATES:
        return "damaged", "state"
    if healthy(rec, stale_age_s):
        return ("exit_same" if rec.flight_id == own_flight_id else "exit_other"), "healthy"
    if rec.state == "released":
        return "take_over", ("initial" if rec.epoch == 0 else "released")
    return "take_over", "stale"


# ------------------------------------------------------------------------------------------------- platform copy
# What the bootstrap needs of `cdc_v2.platform` (contract C6) before the clone: the state of a run and its cancel.
# The SQL builders are copies of `cdc_v2.platform.motherduck` (compared by a T0 test); the local copy follows the
# file protocol of `cdc_v2.platform.local`.
def _uuid_literal(value):
    return "'%s'::UUID" % uuid.UUID(str(value))


def run_state_sql(flight_id, run_number):
    return "SELECT status FROM MD_GET_FLIGHT_RUN(flight_id := %s, run_number := %d)" % (
        _uuid_literal(flight_id), int(run_number))


def cancel_sql(flight_id, run_number):
    return "SELECT * FROM MD_CANCEL_FLIGHT_RUN(flight_id := %s, run_number := %d)" % (
        _uuid_literal(flight_id), int(run_number))


def normalize_state(status):
    if status is None:
        return None
    s = str(status).upper()
    for prefix in ("RUN_STATUS_",):
        if s.startswith(prefix):
            s = s[len(prefix):]
    return s


class MotherDuckRuns:
    def __init__(self, db):
        self.db = db

    def run_state(self, flight_id, run_number, deadline):
        try:
            row = self.db.run(run_state_sql(flight_id, run_number), None, "one", deadline)
        except Refused:
            return None                              # "Run N does not exist", a Flight that is gone
        return normalize_state(row[0]) if row else None

    def cancel(self, flight_id, run_number, deadline):
        try:
            self.db.run(cancel_sql(flight_id, run_number), None, "all", deadline)
            return True
        except Refused:
            return False                             # already terminal: "cannot be cancelled"


def _safe_part(text):
    return re.sub(r"[^A-Za-z0-9_-]", "_", str(text))[:64]


def local_record_path(base, flight_id, run_number):
    return os.path.join(base, "runs", _safe_part(flight_id), "%d.json" % int(run_number))


class LocalRuns:
    """The local copy: `cdc_v2.platform.local.LocalPlatform` keeps one JSON record per run and processes cancel
    requests that any process writes into its directory."""

    def __init__(self, base):
        self.base = base

    def run_state(self, flight_id, run_number, deadline):
        try:
            with open(local_record_path(self.base, flight_id, run_number), encoding="utf-8") as f:
                return normalize_state(json.load(f).get("state"))
        except (OSError, ValueError):
            return None

    def cancel(self, flight_id, run_number, deadline):
        req_dir = os.path.join(self.base, "cancel")
        os.makedirs(req_dir, exist_ok=True)
        name = "%s-%d-%s" % (_safe_part(flight_id), int(run_number), uuid.uuid4().hex[:8])
        tmp = os.path.join(req_dir, "." + name)
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump({"flight_id": str(flight_id), "run_number": int(run_number)}, f)
        os.replace(tmp, os.path.join(req_dir, name + ".req"))
        done = os.path.join(req_dir, name + ".done")
        while time.monotonic() < deadline:
            try:
                with open(done, encoding="utf-8") as f:
                    return bool(json.load(f).get("canceled_count"))
            except (OSError, ValueError):
                time.sleep(0.02)
        return False


# ------------------------------------------------------------------------------------------------- the bootstrap
class AnotherRunLeads(BootError):
    pass


class Damaged(BootError):
    pass


class Boot:
    """One run of the bootstrap. Tests drive the parts (guard, take-over) on a local DuckDB database in this
    process; `interrupt_ids` stands in for the server's interrupt there."""

    def __init__(self, s, env=None, interrupt_ids=None):
        self.s = s
        self.env = dict(os.environ if env is None else env)
        self.interrupt_ids = interrupt_ids
        self.db = Db(s, "guard", interrupt_ids)
        self.report = {"attempts": 0, "conflicts": 0, "listed": 0, "interrupted": 0, "unknown": 0}
        self.epoch = None
        self.facts = None
        self.pipeline = None
        self.step_proc = None
        self.hb = None
        self.first_tick = False
        self.won_at = None
        # as a script the guard's time counts from the first line of this file (open point 17)
        self.guard_deadline = (T0_MONO if __name__ == "__main__" else time.monotonic()) + s.guard_timeout_s

    # ----------------------------------------------------------------------------------------------- guard
    def read_record(self, deadline):
        row = self.db.run(READ_RECORD_SQL, None, "one", deadline)
        if row is None:
            raise Damaged("the leader record has no row")
        return Record(row)

    def ensure_tables(self, deadline):
        """Create the leader tables with every row if they are not there (copies of `cdc_v2.md.schema`); several
        first runs at once end with one row set (design open point 16)."""
        for sql in LEADER_TABLE_STATEMENTS:
            while True:
                try:
                    self.db.run(sql, None, None, deadline)
                    break
                except (Conflict, Transport):
                    if time.monotonic() >= deadline:
                        raise Timeout("creating the leader tables")
                    time.sleep(0.02 + 0.08 * random.random())
        row = self.db.run(CHECK_LEADER_TABLES_SQL, None, "one", deadline)
        if row is None or tuple(int(x) for x in row) != (1, LANE_COUNT, LANE_COUNT, 1):
            raise Damaged("the leader tables do not hold their rows")

    def guard(self):
        """Read the record (creating the leader tables at a first run) within the guard's time."""
        deadline = self.guard_deadline
        created = False
        while True:
            try:
                rec = self.read_record(deadline)
                break
            except Refused:
                if created:
                    raise Damaged("the leader record cannot be read after the leader tables were created")
                self.ensure_tables(deadline)            # the only DDL before the decision
                created = True
                crash_point("leader.tables_created")
            except (Transport, Conflict):
                if time.monotonic() >= deadline:
                    raise Timeout("reading the leader record")
                time.sleep(0.1)
        return rec

    def notice_contention(self, rec, deadline):
        """A healthy leader of another Flight: leave a notice that the leader turns into the alert
        `contending_flight` (`cdc_v2.leader.contenders`); this run changes nothing else and exits non-zero."""
        try:
            self.db.run(NOTICE_SQL, [self.s.flight_id, self.s.run_number, self.s.run_id, rec.epoch,
                                     rec.flight_id, rec.run_number], None, deadline)
            return True
        except BootError as e:                          # no table yet (nobody migrated), or no connection
            say("contention_notice_not_written", why=type(e).__name__)
            return False

    def exit_by_guard(self, rec, reason):
        """Rule 2 of design 4.4, also for the loser of a take-over (it read the record again)."""
        decision, why = decide(rec, self.s.flight_id, self.s.stale_age_s)
        fields = dict(reason=reason, leader_epoch=rec.epoch, leader_flight_id=rec.flight_id,
                      leader_run_number=rec.run_number, heartbeat_age_ms=_ms(rec.age_us))
        if decision == "exit_other":
            left = self.guard_deadline - time.monotonic()
            noticed = self.notice_contention(rec, time.monotonic() + max(1.0, left))
            finish(EXIT_CONTENDING, decision="contending_flight", noticed=noticed, **fields)
        if decision == "damaged":
            finish(EXIT_DAMAGED, decision="damaged", **fields)
        finish(EXIT_OK, decision="leader_healthy" if decision == "exit_same" else "another_run_leads", **fields)

    # ----------------------------------------------------------------------------------------------- take-over
    def take_over(self, rec):
        """Design 4.5. Returns the new epoch, or ends the process by the guard rule (another run leads, or the old
        leader is healthy again) or with `EXIT_NO_TAKEOVER`. The epoch it takes over from is decided once."""
        s, rep = self.s, self.report
        seen = rec.epoch
        t0 = time.monotonic()
        deadline = t0 + s.takeover_timeout_s
        with_transition = True
        while True:
            rep["attempts"] += 1
            try:
                won = self._attempt(seen, with_transition, deadline)
                rep["seconds"] = round(time.monotonic() - t0, 3)
                return won
            except AnotherRunLeads:
                rep["seconds"] = round(time.monotonic() - t0, 3)
                raise
            except Damaged:
                raise
            except Conflict:
                rep["conflicts"] += 1
                now = self._reread(seen, deadline)
                if now is not None and now.epoch == seen and healthy(now, s.stale_age_s):
                    raise AnotherRunLeads("the leader of epoch %d is healthy again" % seen)
                if s.interrupt:
                    listed, done = self.interrupt_earlier(seen + 1, deadline)
                    rep["listed"] += listed
                    rep["interrupted"] += done
            except Unknown:
                rep["unknown"] += 1
                time.sleep(min(s.late_commit_wait_s, max(0.0, deadline - time.monotonic())))
                self.db.drop()
                now = self._reread(seen, deadline + s.statement_timeout_s)
                if now is not None and now.run_id == s.run_id and now.epoch == seen + 1:
                    rep["seconds"] = round(time.monotonic() - t0, 3)
                    return self._won(seen, None)
            except Refused:
                if not with_transition:
                    raise Damaged("a statement of the take-over was refused")
                with_transition = False                    # the pipeline writes the row after its migrations
                say("takeover_without_transition_row")
            except (Transport, Timeout):
                pass                                       # nothing was applied; a new connection next time
            if time.monotonic() >= deadline:
                rep["seconds"] = round(time.monotonic() - t0, 3)
                finish(EXIT_NO_TAKEOVER, decision="no_takeover", **rep)
            time.sleep(min(s.takeover_pause_s * (0.5 + random.random()), max(0.0, deadline - time.monotonic())))

    def _reread(self, seen, deadline):
        """The record in a new read: another run's id at a higher epoch ends this take-over (`AnotherRunLeads`)."""
        try:
            now = self.read_record(max(deadline, time.monotonic() + 1.0))
        except BootError:
            return None
        if now.epoch != seen and not (now.run_id == self.s.run_id and now.epoch == seen + 1):
            raise AnotherRunLeads("the leader row moved from epoch %d to %d" % (seen, now.epoch))
        return now

    def _attempt(self, seen, with_transition, deadline):
        db, s = self.db, self.s
        sd = min(deadline, time.monotonic() + s.statement_timeout_s)
        db.run("BEGIN", None, None, sd)
        try:
            rec = self.read_record(sd)
            if rec.epoch != seen:
                db.rollback()
                if rec.run_id == s.run_id and rec.epoch == seen + 1:
                    return self._won(seen, None)
                raise AnotherRunLeads("the leader row moved from epoch %d to %d" % (seen, rec.epoch))
            if rec.state not in LEADER_STATES:
                raise Damaged("the leader row holds a state no run writes")
            reason = "released" if rec.state == "released" else "stale"
            if rec.epoch == 0:
                reason = "initial"
            facts = {"previous_epoch": rec.epoch, "previous_state": rec.state, "previous_flight_id": rec.flight_id,
                     "previous_run_number": rec.run_number, "previous_run_id": rec.run_id,
                     "heartbeat_age_ms": _ms(rec.age_us), "reason": reason}
            has_log = with_transition and int(db.run(HAS_TABLE_SQL, ["transitions"], "one", sd)[0]) > 0
            n = db.run(TAKEOVER_LEADER_SQL, [s.flight_id, s.run_number, s.run_id, seen], "count", sd)
            if n != 1:
                raise Conflict("the leader row changed")
            if has_log:
                detail = {"run_id": s.run_id, "flight_id": s.flight_id, "run_number": s.run_number,
                          "previous_epoch": rec.epoch, "previous_run_id": rec.run_id,
                          "previous_run_number": rec.run_number, "heartbeat_age_ms": _ms(rec.age_us)}
                detail = {k: v for k, v in detail.items() if v is not None}
                db.run(TRANSITION_SQL, ["leader", "[]", rec.state, "leading", "take-over (%s)" % reason, seen + 1,
                                        "control", "control", json.dumps(detail, sort_keys=True,
                                                                         separators=(",", ":"))], "count", sd)
            if db.run(TAKEOVER_FENCE_SQL, [seen], "count", sd) != LANE_COUNT:
                raise Damaged("the fence rows are not the %d lanes" % LANE_COUNT)
            if db.run(TAKEOVER_HEARTBEAT_SQL, [seen], "count", sd) != 1:
                raise Damaged("the heartbeat row is missing")
            facts["transition_row"] = bool(has_log)
        except (Conflict, Refused, Damaged):
            db.rollback()
            raise
        except AnotherRunLeads:
            raise
        crash_point("leader.takeover_sent")
        db.commit(min(deadline, time.monotonic() + s.statement_timeout_s))
        crash_point("leader.taken_over")
        return self._won(seen, facts)

    def _won(self, seen, facts):
        self.epoch = seen + 1
        self.won_at = time.monotonic()
        if facts is None:                          # decided by run_id after an unknown outcome
            facts = {"previous_epoch": seen, "previous_state": None, "reason": "unknown_outcome",
                     "transition_row": None}
        self.facts = facts
        return self.epoch

    def interrupt_earlier(self, epoch, deadline):
        """Interrupt the connections that `_cdc.connections` lists for epochs before ``epoch`` (ids from that table
        only; `cdc_v2.md.interrupt_earlier`). Returns ``(listed, interrupted)``."""
        d = min(deadline, time.monotonic() + self.s.statement_timeout_s) if deadline else \
            time.monotonic() + self.s.statement_timeout_s
        try:
            if self.s.is_motherduck:
                row = self.db.run(INTERRUPT_EARLIER_SQL, [epoch, NO_CONNECTION], "one", d)
                return (int(row[0]), int(row[1] or 0)) if row else (0, 0)
            rows = self.db.run(LIST_EARLIER_SQL, [epoch], "all", d)
            ids = [r[0] for r in rows]
            done = self.interrupt_ids(ids) if self.interrupt_ids is not None and ids else 0
            return len(ids), int(done)
        except BootError as e:
            say("interrupt_failed", why=type(e).__name__)
            return 0, 0

    def after_win(self):
        """Interrupt once more (a take-over that met no conflict interrupted nothing, destination finding 2), then
        cancel the previous run if it still runs: that run number only, never "all other runs"."""
        s, facts = self.s, self.facts
        listed, done = self.interrupt_earlier(self.epoch, None)
        facts.update(listed_after=listed, interrupted_after=done)
        crash_point("leader.interrupted")
        facts["cancelled"] = False
        prev_flight, prev_run = facts.get("previous_flight_id"), facts.get("previous_run_number")
        if facts.get("previous_state") == "leading" and prev_flight and prev_run is not None \
                and not (prev_flight == s.flight_id and prev_run == s.run_number):
            runs = LocalRuns(s.local_platform_dir) if s.local_platform_dir else MotherDuckRuns(self.db)
            d = time.monotonic() + s.cancel_timeout_s
            try:
                state = runs.run_state(prev_flight, prev_run, d)
                facts["previous_run_state"] = state
                if state == "RUNNING":
                    facts["cancelled"] = bool(runs.cancel(prev_flight, prev_run, d))
                    crash_point("leader.previous_cancelled")
            except (BootError, OSError, ValueError) as e:
                say("cancel_failed", why=type(e).__name__)
        facts.update({k: self.report[k] for k in ("attempts", "conflicts", "interrupted", "unknown")})
        facts["seconds"] = self.report.get("seconds")
        say("taken_over", epoch=self.epoch, **facts)

    # ----------------------------------------------------------------------------------------------- start-up
    def run(self):
        s = self.s
        try:
            rec = self.guard()
        except Damaged as e:
            finish(EXIT_DAMAGED, decision="damaged", why=str(e))
        except BootError as e:
            finish(EXIT_NO_GUARD, decision="no_guard", why=type(e).__name__)
        decision, reason = decide(rec, s.flight_id, s.stale_age_s)
        say("guard", decision=decision, reason=reason, leader_epoch=rec.epoch, leader_state=rec.state,
            heartbeat_age_ms=_ms(rec.age_us))
        if decision != "take_over":
            self.exit_by_guard(rec, reason)
        try:
            self.take_over(rec)
        except AnotherRunLeads as e:
            try:
                now = self.read_record(time.monotonic() + s.guard_timeout_s)
            except BootError:
                finish(EXIT_OK, decision="another_run_leads", why=str(e))
            self.exit_by_guard(now, "another_run_leads")
        except Damaged as e:
            finish(EXIT_DAMAGED, decision="damaged", why=str(e))
        self.after_win()
        self.start_up()

    def start_up(self):
        s = self.s
        self.hb = StartupHeartbeat(s, self.epoch, self.interrupt_ids)
        self.hb.start()
        self.startup_deadline = self.won_at + s.startup_timeout_s
        threading.Thread(target=self._watchdog, name="cdc_v2-boot-watchdog", daemon=True).start()
        try:
            src = self.source()
            self.artifacts(src)
            self.start_pipeline(src)
        except BootError as e:
            say("startup_failed", why=str(e))
            self.hard_exit(EXIT_SOURCE, "startup_failed")
        self.watch()

    def _watchdog(self):
        """The rule of the watch while the main thread is busy (clone, artifacts): no start-up heartbeat for the
        hard-exit age, the start-up timeout, or a start-up heartbeat that changed no row ends the run."""
        s = self.s
        while not self.first_tick:
            time.sleep(min(1.0, s.heartbeat_interval_s / 2.0))
            if self.first_tick:
                return
            now = time.monotonic()
            if self.hb.superseded:
                self.hard_exit(EXIT_SUPERSEDED, "superseded")
            if now > self.startup_deadline:
                self.hard_exit(EXIT_STARTUP, "startup_timeout")
            if now - max(self.hb.last_ok, self.won_at) > s.hard_exit_age_s:
                self.hard_exit(EXIT_HARD, "no_startup_heartbeat")

    def hard_exit(self, code, why):
        """SIGKILL to every process group this bootstrap started, then `os._exit`. Never waits for anything."""
        for proc in (self.pipeline, self.step_proc):
            if proc is not None:
                try:
                    os.killpg(proc.pid, signal.SIGKILL)
                except (ProcessLookupError, PermissionError):
                    pass
        finish(code, decision="hard_exit", why=why)

    def _step(self, argv, cwd, env, timeout, what):
        """A start-up step as a child in its own process group, bounded by its own timeout and the start-up
        deadline. Output is discarded but for the last lines of an error; no argument holds a secret."""
        left = min(timeout, self.startup_deadline - time.monotonic())
        if left <= 0:
            raise Timeout("%s: no time left before the start-up deadline" % what)
        proc = subprocess.Popen(argv, cwd=cwd, env=env, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
                                stderr=subprocess.STDOUT, start_new_session=True)
        self.step_proc = proc
        try:
            out, _ = proc.communicate(timeout=left)
        except subprocess.TimeoutExpired:
            try:
                os.killpg(proc.pid, signal.SIGKILL)
            except (ProcessLookupError, PermissionError):
                pass
            proc.wait()
            raise Timeout("%s took longer than %.0f s" % (what, left)) from None
        finally:
            self.step_proc = None
        if proc.returncode != 0:
            raise Transport("%s failed with exit %s" % (what, proc.returncode))
        return out.decode("utf-8", "replace")

    def source(self):
        """The source tree: a local path (local tiers, contract C6), or this repository at the pinned commit,
        fetched anonymously (the repository is public, R4)."""
        s = self.s
        if s.source_dir:
            say("source", kind="local")
            return os.path.abspath(s.source_dir)
        t = time.monotonic()
        clone = os.path.join(s.work_dir, "src")
        if os.path.isdir(clone):
            shutil.rmtree(clone, ignore_errors=True)
        os.makedirs(clone)
        env = {k: v for k, v in os.environ.items() if not k.upper().startswith(("MOTHERDUCK", "CDC_V2_"))}
        env["GIT_TERMINAL_PROMPT"] = "0"
        for args in (["init", "-q"], ["remote", "add", "origin", s.repo],
                     ["fetch", "-q", "--depth", "1", "origin", s.commit], ["checkout", "-q", "FETCH_HEAD"]):
            self._step(["git", "-c", "advice.detachedHead=false"] + args, clone, env, s.step_timeout_s,
                       "git %s" % args[0])
        head = self._step(["git", "rev-parse", "HEAD"], clone, env, 30.0, "git rev-parse").strip()
        if head != s.commit:
            raise Transport("the clone is at another commit")
        say("source", kind="clone", commit=s.commit, seconds=round(time.monotonic() - t, 3))
        return clone

    def artifacts(self, src):
        """The pipeline's Python dependencies (when a requirements file is named) and the pinned JRE, jars and
        compiled engine classes (`cdc_v2.stream.jvm.prepare`, standard library only)."""
        s = self.s
        env = dict(os.environ)
        env["PYTHONPATH"] = os.path.join(src, "src") + os.pathsep + env.get("PYTHONPATH", "")
        if s.requirements:
            t = time.monotonic()
            uv = env.get("UV") or shutil.which("uv")
            if not uv:
                raise Transport("no uv to install the pipeline's requirements")
            self._step([uv, "pip", "install", "--quiet", "--python", sys.executable, "-r",
                        os.path.join(src, s.requirements)], src, env, s.step_timeout_s, "requirements")
            say("requirements", seconds=round(time.monotonic() - t, 3))
        if not s.artifacts:
            say("artifacts", skipped=True)
            return
        t = time.monotonic()
        snippet = ("import json, sys\nfrom cdc_v2.stream import jvm\na = jvm.prepare(sys.argv[1])\n"
                   "print(json.dumps({'jdk': a.jdk_source}))\n")
        self._step([sys.executable, "-c", snippet, os.path.join(s.work_dir, "cache")], src, env, s.step_timeout_s,
                   "artifacts")
        say("artifacts", seconds=round(time.monotonic() - t, 3))

    def start_pipeline(self, src):
        """Contract C12: own session; epoch, run identity and the tick descriptor in the environment; no secret is
        an argument (the secrets are in the environment the pipeline inherits)."""
        s = self.s
        r, w = os.pipe()
        os.set_blocking(r, False)
        env = dict(os.environ)
        env.update({
            "CDC_V2_EPOCH": str(self.epoch), "CDC_V2_RUN_ID": s.run_id, "CDC_V2_FLIGHT_ID": s.flight_id,
            "CDC_V2_RUN_NUMBER": str(s.run_number), "CDC_V2_TICK_FD": str(w),
            "CDC_V2_TAKEOVER": json.dumps(self.facts, sort_keys=True, default=str),
            "CDC_V2_RUN_ELAPSED_S": "%.3f" % (time.monotonic() - T0_MONO),
            "PYTHONPATH": os.path.join(src, "src") + os.pathsep + os.environ.get("PYTHONPATH", ""),
            "PYTHONUNBUFFERED": "1",
        })
        for attr, var, _default, shared in TIMINGS:
            if shared:
                env[var] = repr(getattr(s, attr))
        kind, _, what = s.pipeline.partition(":")
        argv = [sys.executable, "-m", what] if kind == "module" else [sys.executable, os.path.join(src, what)]
        argv.append(os.path.join(src, s.config))
        try:
            self.pipeline = subprocess.Popen(argv, cwd=src, env=env, pass_fds=(w,), stdin=subprocess.DEVNULL,
                                             start_new_session=True)
        except OSError as e:
            os.close(r)
            os.close(w)
            raise Transport("the pipeline did not start (%s)" % type(e).__name__) from None
        os.close(w)
        self.tick_r = r
        say("pipeline_started", pid=self.pipeline.pid, epoch=self.epoch)
        crash_point("leader.pipeline_started")

    # ----------------------------------------------------------------------------------------------- watch
    def watch(self):
        """Design 5, step 5. Reads ticks and the pipeline's exit; no network, no lock shared with a thread that can
        block, no read without a timeout. Works with a frozen pipeline (no ticks: the hard exit), a full pipe (the
        pipeline drops ticks; everything available is read each time) and a closed pipe (no more ticks: the hard
        exit, unless the pipeline exits first)."""
        exit_code = watch_loop(self.pipeline, self.tick_r, self.s.hard_exit_age_s, self._before_first_tick,
                               self._on_first_tick, min(1.0, self.s.heartbeat_interval_s / 2.0))
        if exit_code is None:
            self.hard_exit(EXIT_HARD, "no_tick")
        pass_on(self.pipeline, exit_code)

    def _before_first_tick(self, now):
        """Before the first tick the start-up heartbeat and the start-up deadline decide (returns a reason to end
        the run, or None)."""
        if self.hb.superseded:
            self.hard_exit(EXIT_SUPERSEDED, "superseded")
        if now > self.startup_deadline:
            self.hard_exit(EXIT_STARTUP, "startup_timeout")
        if now - max(self.hb.last_ok, self.won_at) > self.s.hard_exit_age_s:
            self.hard_exit(EXIT_HARD, "no_startup_heartbeat")

    def _on_first_tick(self):
        self.first_tick = True
        self.hb.stop()
        say("first_tick", after_takeover_s=round(time.monotonic() - self.won_at, 3))
        crash_point("leader.first_tick")


def watch_loop(proc, tick_r, hard_exit_age_s, before_first_tick=None, on_first_tick=None, check_s=1.0):
    """The watch, as a function a test can drive with any process and any pipe. Returns the pipeline's return code
    when it exits, or None when no tick came for ``hard_exit_age_s`` (the caller then kills and exits). Before the
    first tick ``before_first_tick(now)`` decides about the start-up (it may end the process)."""
    last_tick = None
    eof = False
    while True:
        rc = proc.poll()
        if rc is not None:
            return rc
        got = False
        if not eof:
            try:
                ready, _, _ = select.select([tick_r], [], [], check_s)
            except InterruptedError:
                ready = []
            if ready:
                while True:
                    try:
                        data = os.read(tick_r, 65536)
                    except BlockingIOError:
                        break
                    except OSError:
                        data = b""
                    if not data:
                        eof = True             # the pipeline closed its end: no more ticks will come
                        break
                    got = True
        else:
            time.sleep(check_s)
        now = time.monotonic()
        if got:
            if last_tick is None and on_first_tick is not None:
                on_first_tick()
            last_tick = now
        if last_tick is None:
            if before_first_tick is not None:
                before_first_tick(now)
        elif now - last_tick > hard_exit_age_s:
            rc = proc.poll()
            return rc                          # None: no tick for the hard-exit age


def pass_on(proc, rc):
    """The pipeline exited: kill what is left of its group (the engine), then exit with its status. A pipeline
    killed by SIGKILL is passed on as a death by SIGKILL (as the platform's own kill looks); another signal as
    128 + its number."""
    if proc is not None:
        try:
            os.killpg(proc.pid, signal.SIGKILL)
        except (ProcessLookupError, PermissionError):
            pass
    if rc is None:
        finish(EXIT_HARD, decision="hard_exit", why="no_tick")
    if rc < 0:
        say("exit", code=None, signal=-rc, decision="pipeline_killed",
            after_first_line_s=round(time.monotonic() - T0_MONO, 3))
        if -rc == signal.SIGKILL:
            os.kill(os.getpid(), signal.SIGKILL)
            time.sleep(5)
        os._exit(128 - rc)
    finish(rc, decision="pipeline_exit")


class StartupHeartbeat:
    """Writes `phase = 'starting'` every interval from the take-over until the pipeline's first tick (design 5,
    step 2). Its connection registers under `control` first, like the pipeline's heartbeat connection (contract
    note 15 of the destination), so that a take-over can interrupt it. A heartbeat that changes no row means that
    another run took over: `superseded`, and the watch ends the run."""

    def __init__(self, s, epoch, interrupt_ids=None):
        self.s, self.epoch = s, epoch
        self.db = Db(s, "heartbeat", interrupt_ids)
        self.stopped = threading.Event()
        self.superseded = False
        self.last_ok = 0.0
        self.beats = 0
        self.registered_sid = None

    def start(self):
        threading.Thread(target=self._loop, name="cdc_v2-boot-heartbeat", daemon=True).start()

    def stop(self):
        self.stopped.set()

    def _register(self, deadline):
        db = self.db
        db.ensure(deadline)
        if self.registered_sid == db.sid:
            return
        db.run("BEGIN", None, None, deadline)
        try:
            db.run(REGISTER_SQL, [self.epoch, "control", db.sid], None, deadline)
            n = db.run(REGISTER_FENCE_SQL, ["control", self.epoch], "count", deadline)
        except BootError:
            db.rollback()
            raise
        if n != 1:
            db.rollback()
            self.superseded = True
            return
        db.commit(deadline)
        self.registered_sid = db.sid

    def beat_once(self):
        s = self.s
        deadline = time.monotonic() + min(s.statement_timeout_s, s.hard_exit_age_s / 3.0)
        self._register(deadline)
        if self.superseded:
            return False
        n = self.db.run(HEARTBEAT_SQL, [0, "starting", self.epoch], "count", deadline)
        if n != 1:
            self.superseded = True
            return False
        self.last_ok = time.monotonic()
        self.beats += 1
        crash_point("leader.startup_beat")
        return True

    def _loop(self):
        s = self.s
        while not self.stopped.is_set() and not self.superseded:
            pause = s.heartbeat_interval_s
            try:
                self.beat_once()
            except Unknown:
                self.registered_sid = None
                pause = min(s.heartbeat_interval_s, 1.0)
            except BootError:
                pause = min(s.heartbeat_interval_s, 1.0)      # tried again soon; the watch bounds the silence
            self.stopped.wait(pause)
        self.db.close()


def _ms(us):
    return None if us is None else int(us // 1000)


def main(env=None):
    env = os.environ if env is None else env
    try:
        crash_init(env)
        s = Settings(env)
    except ValueError as e:
        finish(EXIT_SETTINGS, decision="settings", why=str(e))
    if duckdb is None:
        finish(EXIT_SETTINGS, decision="settings", why="the duckdb client is not installed")
    say("start", flight_id=s.flight_id, run_number=s.run_number, run_id=s.run_id,
        database_kind="motherduck" if s.is_motherduck else "local")
    Boot(s, env).run()


if __name__ == "__main__":
    main()
