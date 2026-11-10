"""Connections to the destination: one worker thread per connection, a deadline on every call.

Why a worker thread. The client library offers no time limit for a statement, and on MotherDuck its local
``interrupt()`` does not end a statement that is in flight on the server (measured: the statement returns at its
natural end; `evidence/p3/destination/error_mapping.json`). So the caller never runs a statement itself: it hands it
to the connection's worker thread and waits for the answer until the deadline. When the deadline passes

1. a **watchdog thread** interrupts the statement: the client's ``interrupt()`` and, on MotherDuck, the server-side
   ``md_interrupt_server_connection(<own id>)`` through a second connection (this one ends a statement in flight
   within a fraction of a second and rolls the open transaction back);
2. the caller waits for the answer during a short **grace period**;
3. answer or not, the connection is **retired**: it is never used again, a reaper thread interrupts its server
   connection once more, closes it in the background (``close()`` blocks until the statement in flight returns) and
   ends the worker thread. The `Connection` object takes a new client connection at its next call and first
   interrupts the old server connection by id, so that no abandoned transaction keeps holding a row.

So a caller is never blocked longer than deadline plus grace, whatever the statement does.

A destination that is a local DuckDB database (tier T0) runs the same code. The server's interrupt function does
not exist there; `interrupt_server_connection` then acts on the connections of this process: it interrupts the
statement in flight, rolls the open transaction back and makes the next statement fail with the text MotherDuck
uses, which is what the server does (same evidence file).
"""
from __future__ import annotations

import functools
import logging
import queue
import re
import threading
import uuid

import duckdb

from cdc_v2.deadline import Deadline, DeadlineExceeded
from cdc_v2.faultsites import declare_site, fault_site

from . import errors
from .errors import CommitUnknown, Conflict, ConnectionLost, LaneMisuse
from .settings import Settings

log = logging.getLogger("cdc_v2.md")

SITE_CONNECT = declare_site("md.connect", "opening a destination connection")
SITE_EXECUTE = declare_site("md.execute", "a destination statement (BEGIN, the caller's statements, ROLLBACK)")
SITE_FENCE = declare_site("md.fence", "the fence statement of a lane transaction")
SITE_COMMIT = declare_site("md.commit", "COMMIT of a lane transaction, immediately before it is sent")
SITE_COMMIT_REPLY = declare_site("md.commit_reply", "the answer to COMMIT, after the destination executed it")

#: The expression for MotherDuck's clock (design 4.1). It is the server's clock only inside a statement that runs on
#: the server: every statement that writes a MotherDuck table, and every read FROM a MotherDuck table. A SELECT
#: without a table is evaluated by the client (evidence/p3/destination/clock.json), so never read the clock that way;
#: use `Connection.clock_us` or `CLOCK_US_SQL`.
clock_sql = "uuid_extract_timestamp(uuidv7())"
#: Reads MotherDuck's clock as microseconds since 1970 (an integer: the client cannot fetch TIMESTAMPTZ without the
#: `pytz` module, which is not a dependency). `_cdc.leader` always has exactly one row.
CLOCK_US_SQL = "SELECT epoch_us(uuid_extract_timestamp(uuidv7())) FROM _cdc.leader"

INTERRUPT_SQL = "SELECT md_interrupt_server_connection(?::UUID)"
INTERRUPTED_TEXT = "Connection Error: interrupted by user"      # MotherDuck's text after a server-side interrupt
_ROLE = re.compile(r"^[a-z][a-z0-9_.-]{0,40}$")
_NO_TX = re.compile(r"no transaction is active", re.IGNORECASE)
# Statements that would remove or re-create a row of the leader tables, or change their shape (design 4.1: the
# rows are only ever updated; "a delete next to an update is not detected as a conflict").
_LEADER_ROWS = re.compile(
    r"""\b(?:insert\s+(?:or\s+\w+\s+)?into|delete\s+from|truncate(?:\s+table)?|merge\s+into|"""
    r"""drop\s+table(?:\s+if\s+exists)?|alter\s+table(?:\s+if\s+exists)?|"""
    r"""create\s+or\s+replace\s+(?:temp(?:orary)?\s+)?table)\s+"""
    r"""(?:"?\w+"?\s*\.\s*)?"?_cdc"?\s*\.\s*"?(?:leader|fence|heartbeat)"?(?![\w"])""", re.IGNORECASE)
_DROP_CDC = re.compile(r"""\bdrop\s+schema\s+(?:if\s+exists\s+)?(?:"?\w+"?\s*\.\s*)?"?_cdc"?(?![\w"])""",
                       re.IGNORECASE)

_default_settings = None
_registry_lock = threading.Lock()
_open_sessions = set()                 # sessions whose client connection is open (leak checks)
_local_sessions = {}                   # server id -> session, local destinations only
_background = set()                    # watchdog and reaper threads that have not ended


def configure(settings):
    """Set the settings that `connect` uses when it is given none. Returns the settings."""
    global _default_settings
    if settings is not None and not isinstance(settings, Settings):
        raise TypeError("configure() takes a cdc_v2.md.Settings")
    _default_settings = settings
    return settings


def default_settings():
    if _default_settings is None:
        raise LaneMisuse("no destination settings: pass settings to md.connect() or call md.configure() first")
    return _default_settings


def as_deadline(deadline, default_s):
    """A `Deadline` from the caller's argument; None means the default of the settings, never 'no deadline'."""
    if deadline is None:
        return Deadline.after(default_s)
    if not isinstance(deadline, Deadline):
        raise TypeError("a deadline is a cdc_v2.deadline.Deadline, got %s" % type(deadline).__name__)
    return deadline


@functools.lru_cache(maxsize=1024)
def check_sql(sql):
    """Check one statement of a caller. Returns True when it only reads (SELECT, EXPLAIN).

    A caller passes exactly one statement, and never a transaction statement: BEGIN, COMMIT and ROLLBACK belong to
    the lane (contract C4). A statement that deletes, inserts or re-creates rows of the three leader tables is
    refused as well; this makes the delete-and-insert mistake hard, it is not a security boundary."""
    if not isinstance(sql, str):
        raise LaneMisuse("a statement is SQL text")
    stmts = duckdb.extract_statements(sql)
    if len(stmts) != 1:
        raise LaneMisuse("exactly one statement per call, got %d" % len(stmts))
    kind = stmts[0].type
    if kind == duckdb.StatementType.TRANSACTION:
        raise LaneMisuse("BEGIN, COMMIT and ROLLBACK are issued by the lane only")
    if _LEADER_ROWS.search(sql) or _DROP_CDC.search(sql):
        raise LaneMisuse("the rows of _cdc.leader, _cdc.fence and _cdc.heartbeat are only ever updated, and the "
                         "tables keep their shape (design 4.1)")
    return kind in (duckdb.StatementType.SELECT, duckdb.StatementType.EXPLAIN)


def _read(cur, fetch):
    if fetch == "count":
        d = cur.description
        if d is not None and len(d) == 1 and d[0][0] == "Count":
            row = cur.fetchone()
            return int(row[0]) if row is not None and row[0] is not None else None
        return None
    if fetch == "all":
        return cur.fetchall()
    if fetch == "one":
        return cur.fetchone()
    if fetch == "arrow":
        return cur.to_arrow_table()
    return None


class _Job:
    __slots__ = ("fn", "done", "result", "error")

    def __init__(self, fn):
        self.fn = fn
        self.done = threading.Event()
        self.result = None
        self.error = None


class Outcome:
    """What became of one call: ``sent`` (False: the deadline had passed before it started), ``late`` (the deadline
    passed while it ran; the connection is retired), ``returned`` (an answer exists), ``result`` / ``error``."""
    __slots__ = ("sent", "late", "returned", "result", "error")

    def __init__(self, sent, late, returned, result=None, error=None):
        self.sent, self.late, self.returned, self.result, self.error = sent, late, returned, result, error

    @property
    def ok(self):
        return self.sent and self.returned and self.error is None


def _spawn(name, target, *args):
    """Start a background thread of this package (watchdog, reaper, close). It is counted until it ends."""
    def run():
        try:
            target(*args)
        except Exception as e:                       # noqa: BLE001 - a background helper never raises
            log.warning("md background step failed", extra={"cdc": {"thread": name, "error": errors.safe_text(e)}})
        finally:
            with _registry_lock:
                _background.discard(t)

    t = threading.Thread(target=run, name=name, daemon=True)
    with _registry_lock:
        _background.add(t)
    t.start()
    return t


class Session:
    """One client connection and the thread that runs its statements. Internal; callers use `Connection`."""

    def __init__(self, settings, role):
        self.settings = settings
        self.role = role
        self.raw = None
        self.server_id = None
        self.state = "new"                 # new -> open -> retired -> closed
        self.in_tx = False                 # a transaction is open, as far as this side knows
        self.kill_text = None              # local stand-in for a server interrupt: the next statement raises this
        self.lock = threading.Lock()       # held while a statement runs on the client connection
        self._current = None               # the job handed to the worker last
        self._q = queue.SimpleQueue()
        self._thread = threading.Thread(target=self._loop, name="cdc_v2-md-%s" % role, daemon=True)
        self._thread.start()

    # ------------------------------------------------------------------ the worker thread
    def _loop(self):
        while True:
            job = self._q.get()
            if job is None:
                return
            try:
                job.result = job.fn()
            except BaseException as e:               # noqa: BLE001 - handed to the caller's thread
                job.error = e
            finally:
                job.done.set()

    def submit(self, fn):
        """Hand ``fn`` to the worker thread. Returns the job, or None when the connection is not usable."""
        if self.state not in ("open", "new"):
            return None
        job = _Job(fn)
        self._current = job
        self._q.put(job)
        return job

    def wait(self, job, deadline, interrupt=True):
        """Wait for a job until ``deadline``; past it interrupt, wait the grace period, retire the connection."""
        if job is None:
            return Outcome(True, False, True, error=duckdb.ConnectionException("Connection Error: connection closed"))
        if job.done.wait(max(0.0, deadline.remaining())):
            return Outcome(True, False, True, job.result, job.error)
        if interrupt:
            _spawn("cdc_v2-md-watchdog", self._interrupt_from_watchdog)
        returned = job.done.wait(self.settings.grace_s)
        self.retire()
        return Outcome(True, True, returned, job.result, job.error)

    def call(self, fn, deadline, interrupt=True):
        """Run ``fn()`` on the worker thread and wait for it until ``deadline`` (plus grace after the interrupt).
        A call whose deadline has already passed is not started."""
        if deadline.expired:
            return Outcome(False, False, False)
        return self.wait(self.submit(fn), deadline, interrupt)

    # ------------------------------------------------------------------ statements (run on the worker thread)
    def _open(self):
        fault_site(SITE_CONNECT)
        s = self.settings
        raw = duckdb.connect(s.target, config={"custom_user_agent": s.marker})
        with self.lock:
            if self.state != "new":                  # abandoned while connecting: leave nothing open
                raw.close()
                raise duckdb.ConnectionException("Connection Error: connection closed")
            self.raw = raw                           # from here on the reaper closes it
            raw.execute("SET TimeZone = 'UTC'")
            if s.is_motherduck:
                sid = str(raw.execute("SELECT md_current_client_connection_id()").fetchone()[0])
                # connect() does no network round trip when this process already has the database open, so one
                # statement goes to the server here: a server that cannot be reached is found now, inside the
                # deadline of the connect, and not by the first statement of the caller. (The answer is not used:
                # the server lists a connection only while it has a statement in flight or a transaction open.)
                raw.execute("SELECT count(*) FROM md_active_server_connections() WHERE client_connection_id = ?::UUID",
                            [sid]).fetchall()
            else:
                sid = str(uuid.uuid4())
            self.server_id = sid
        with _registry_lock:
            _open_sessions.add(self)
            if self.state != "new":                  # abandoned meanwhile; the reaper closes the connection
                raise duckdb.ConnectionException("Connection Error: connection closed")
            self.state = "open"
            if not s.is_motherduck:
                _local_sessions[sid] = self
        return sid

    def statement(self, site, sql, params, fetch, tx=None):
        """The function a job runs for one statement: fault site, then the statement, then its result.
        ``tx`` is True for BEGIN and False for ROLLBACK: what the statement does to the open transaction."""
        def run():
            fault_site(site)
            with self.lock:
                self._usable_locked()
                cur = self.raw.execute(sql, params) if params else self.raw.execute(sql)
                if tx is not None:
                    self.in_tx = tx
                return _read(cur, fetch)
        return run

    def commit(self):
        """The function a job runs for COMMIT. A fault at the first site stands for a COMMIT that never arrived,
        a fault at the second one for a COMMIT that was executed and whose answer was lost."""
        def run():
            fault_site(SITE_COMMIT)
            with self.lock:
                self._usable_locked()
                self.raw.execute("COMMIT")
                self.in_tx = False
            fault_site(SITE_COMMIT_REPLY)
        return run

    def _usable_locked(self):
        if self.state != "open":
            raise duckdb.ConnectionException("Connection Error: connection closed")
        if self.kill_text is not None:
            text, self.kill_text = self.kill_text, None
            raise duckdb.ConnectionException(text)

    def view(self, name, data):
        def run():
            with self.lock:
                if self.state != "open":
                    raise duckdb.ConnectionException("Connection Error: connection closed")
                if data is None:
                    self.raw.unregister(name)
                else:
                    self.raw.register(name, data)
        return run

    # ------------------------------------------------------------------ interrupt, retire, close
    def _interrupt_from_watchdog(self):
        raw = self.raw
        if raw is not None:
            try:
                raw.interrupt()
            except Exception:                        # noqa: BLE001
                pass
        if self.settings.is_motherduck and self.server_id is not None:
            _interrupt_through_side_connection(self.settings, self.server_id)

    def local_interrupt(self):
        """Local stand-in for the server's interrupt: end the statement in flight, roll the open transaction back,
        make the next statement fail the way MotherDuck's does. True when the connection is known and not closed.

        Like the server's interrupt it also reaches a connection that its owner has given up and that is not
        closed yet (a retired connection whose statement has not returned)."""
        raw = self.raw
        if self.state not in ("open", "retired") or raw is None:
            return False
        try:
            raw.interrupt()
        except Exception:                            # noqa: BLE001
            return False
        if not self.lock.acquire(timeout=self.settings.interrupt_deadline_s):
            return True                              # still inside a statement: the interrupt is on its way
        try:
            if self.raw is not None and self.in_tx:
                try:
                    self.raw.execute("ROLLBACK")
                except duckdb.Error:
                    pass
                self.in_tx = False
                self.kill_text = INTERRUPTED_TEXT
        finally:
            self.lock.release()
        return True

    def abort(self):
        """From any thread: fail the call in flight at once and retire the connection."""
        job = self._current
        if job is not None and not job.done.is_set():
            if job.error is None:
                job.error = duckdb.ConnectionException("Connection Error: connection closed (aborted)")
            job.done.set()
        self.retire()

    def retire(self):
        """Never use this connection again; close it in the background. Safe to call twice."""
        with _registry_lock:
            if self.state in ("retired", "closed"):
                return
            self.state = "retired"
        _spawn("cdc_v2-md-reaper", self._reap)

    def _reap(self):
        raw, s = self.raw, self.settings
        if raw is not None:
            try:
                raw.interrupt()
            except Exception:                        # noqa: BLE001
                pass
        if s.is_motherduck and self.server_id is not None:
            try:
                _interrupt_through_side_connection(s, self.server_id)
            except Exception:                        # noqa: BLE001
                pass
        elif not s.is_motherduck:
            self.local_interrupt()                   # as the server does: the open transaction is rolled back now
        self._close_raw()

    def _close_raw(self):
        self._q.put(None)
        self._thread.join()                          # ends when the statement in flight has returned
        with self.lock:
            raw, self.raw = self.raw, None
        if raw is not None:
            try:
                raw.close()
            except Exception:                        # noqa: BLE001
                pass
        with _registry_lock:
            self.state = "closed"
            _open_sessions.discard(self)
            if self.server_id is not None:
                _local_sessions.pop(self.server_id, None)

    def close(self, deadline):
        """Orderly close of a connection that is not inside a statement. Past the deadline it is retired."""
        with _registry_lock:
            if self.state in ("retired", "closed"):
                return
            self.state = "retired"
        t = _spawn("cdc_v2-md-close", self._close_raw)
        t.join(deadline.remaining())


def _interrupt_through_side_connection(settings, server_id):
    """Call the server's interrupt function for one id through a short-lived connection of its own. Returns the
    function's answer. Used by the watchdog and the reaper, which cannot use the connection they interrupt."""
    side = duckdb.connect(settings.target, config={"custom_user_agent": settings.marker})
    try:
        row = side.execute(INTERRUPT_SQL, [server_id]).fetchone()
        return bool(row[0]) if row is not None else False
    finally:
        side.close()


def _interrupt_local(server_id):
    with _registry_lock:
        sess = _local_sessions.get(str(server_id))
    return sess.local_interrupt() if sess is not None else False


def _open_session(settings, role, deadline):
    sess = Session(settings, role)
    out = sess.call(sess._open, deadline, interrupt=False)
    if out.ok:
        return sess
    if not out.late:
        sess.retire()
    if not out.sent or out.late:
        raise DeadlineExceeded("connecting to the destination (%s)" % role)
    if isinstance(out.error, DeadlineExceeded):
        raise out.error
    # the client's text for a failed connect holds a local path; only the replaced text travels on
    raise ConnectionLost("cannot connect to the destination (%s): %s" % (role, errors.safe_text(out.error))) from None


def connect(role, settings=None, deadline=None):
    """A connection to the destination (contract C4): session time zone UTC, the pipeline's marker as
    ``custom_user_agent``, the server-side connection id read and kept (``conn.server_id``).

    ``role`` names what the connection is for (``stream``, ``heartbeat``, ...); it appears in thread names and logs.
    Raises `ConnectionLost` when the destination cannot be reached, `DeadlineExceeded` when it does not answer
    within the deadline (default: the attempts of the settings, each with ``connect_deadline_s``)."""
    if not isinstance(role, str) or not _ROLE.match(role):
        raise ValueError("a connection role is a short lower-case name, got %r" % (role,))
    settings = default_settings() if settings is None else settings
    if not isinstance(settings, Settings):
        raise TypeError("settings must be a cdc_v2.md.Settings")
    conn = Connection(role, settings)
    conn.ensure(as_deadline(deadline, settings.connect_attempts * (settings.connect_deadline_s
                                                                   + settings.connect_retry_pause_s)))
    return conn


class Connection:
    """A destination connection with a deadline on every call.

    The object outlives its client connection: when a call retires the client connection (deadline, lost
    connection), the next call opens a new one and first interrupts the old server connection by id.
    ``server_id`` therefore changes over time; `generation` counts the client connections so far.

    Outside a lane transaction every statement is its own transaction (autocommit). The errors:
    `Conflict` (nothing applied), `ConnectionLost` / `DeadlineExceeded` for a statement that only reads,
    `CommitUnknown` for a statement that writes and whose outcome is not known (lost connection or deadline).
    Any other error is passed on unchanged.
    """

    def __init__(self, role, settings):
        self.role = role
        self.settings = settings
        self.generation = 0
        self._session = None
        self._stale_ids = []               # server ids of retired connections, not yet interrupted from a new one
        self._lane = None                  # the lane that owns this connection, if any
        self._in_block = False             # a lane transaction is open on it
        self._closed = False

    def __repr__(self):
        return "<md.Connection %s %s gen=%d>" % (self.role, "open" if self.usable else "not connected",
                                                 self.generation)

    # ------------------------------------------------------------------ state
    @property
    def is_motherduck(self):
        return self.settings.is_motherduck

    @property
    def usable(self):
        return self._session is not None and self._session.state == "open"

    @property
    def server_id(self):
        """The server-side id of the current client connection (text), or None while there is none."""
        return self._session.server_id if self.usable else None

    def _drop_session(self, only=None):
        """Retire the current client connection; its server id is interrupted before the next one is used."""
        if only is not None and self._session is not only:
            only.retire()
            return
        sess, self._session = self._session, None
        if sess is not None:
            if sess.server_id is not None and sess.server_id not in self._stale_ids:
                self._stale_ids.append(sess.server_id)
            sess.retire()

    def ensure(self, deadline):
        """Make sure a client connection is open: connect if needed (with the attempts of the settings), then
        interrupt the server connections this object retired earlier. Returns the session."""
        if self._closed:
            raise LaneMisuse("this connection was closed")
        if self._session is not None and self._session.state != "open":
            self._drop_session()
        if self._session is None:
            s = self.settings
            attempt = 0
            while True:
                attempt += 1
                try:
                    self._session = _open_session(s, self.role, deadline.within(s.connect_deadline_s))
                    break
                except ConnectionLost:
                    if attempt >= s.connect_attempts or deadline.remaining() <= s.connect_retry_pause_s:
                        raise
                    deadline.sleep(s.connect_retry_pause_s, "connecting to the destination (%s)" % self.role)
            self.generation += 1
        while self._stale_ids:
            # on the connection made sure of above, not through `_interrupt`: that one asks `ensure` for a connection
            self._interrupt_on(self._session, self._stale_ids[0],
                               deadline.within(self.settings.interrupt_deadline_s))
            self._stale_ids.pop(0)
        return self._session

    # ------------------------------------------------------------------ calls
    def _run(self, site, sql, params, fetch, deadline, writes):
        """One autocommit statement with the error rules of the class docstring."""
        return self._run_on(self.ensure(deadline), site, sql, params, fetch, deadline, writes)

    def _run_on(self, sess, site, sql, params, fetch, deadline, writes):
        """`_run` on a session that is already made sure of."""
        out = sess.call(sess.statement(site, sql, params, fetch), deadline)
        if out.ok and not out.late:
            return out.result
        if not out.sent:
            raise DeadlineExceeded("destination statement not started before its deadline (%s)" % self.role)
        if out.late:
            self._drop_session(sess)
            if writes:
                if out.ok:
                    return out.result                # it did complete; only the connection is replaced
                raise CommitUnknown("deadline passed during a writing statement outside a transaction (%s)"
                                    % self.role)
            raise DeadlineExceeded("destination statement (%s)" % self.role)
        err = out.error
        kind = errors.kind_of(err)
        if kind == "transport":
            self._drop_session(sess)
            text = errors.safe_text(err)
            if writes:
                raise CommitUnknown("connection lost during a writing statement outside a transaction (%s): %s"
                                    % (self.role, text)) from err
            raise ConnectionLost("connection lost (%s): %s" % (self.role, text)) from err
        if kind == "conflict":
            raise Conflict("write-write conflict in a statement outside a transaction (%s)" % self.role) from None
        raise err

    def _autocommit(self, sql, params, fetch, deadline, site=SITE_EXECUTE):
        if self._in_block:
            raise LaneMisuse("this connection is inside a lane transaction; use the transaction object")
        reads = check_sql(sql)
        return self._run(site, sql, params, fetch, as_deadline(deadline, self.settings.statement_deadline_s),
                         writes=not reads)

    def execute(self, sql, params=None, deadline=None):
        """Run one statement in autocommit. Returns the number of changed rows for INSERT, UPDATE, DELETE and
        CREATE TABLE AS, else None."""
        return self._autocommit(sql, params, "count", deadline)

    def fetchall(self, sql, params=None, deadline=None):
        return self._autocommit(sql, params, "all", deadline)

    def fetchone(self, sql, params=None, deadline=None):
        return self._autocommit(sql, params, "one", deadline)

    def clock_us(self, deadline=None):
        """MotherDuck's clock, microseconds since 1970 (design 4.1). Needs the leader tables."""
        return int(self.fetchone(CLOCK_US_SQL, None, deadline)[0])

    def _interrupt_on(self, sess, server_id, deadline):
        """Interrupt one server connection by id, with a statement on the session ``sess`` (MotherDuck)."""
        if not self.is_motherduck:
            return _interrupt_local(server_id)
        row = self._run_on(sess, SITE_EXECUTE, INTERRUPT_SQL, [str(server_id)], "one", deadline, writes=False)
        return bool(row[0]) if row is not None else False

    def _interrupt(self, server_id, deadline):
        if not self.is_motherduck:
            return _interrupt_local(server_id)
        return self._interrupt_on(self.ensure(deadline), server_id, deadline)

    def interrupt_server_connection(self, server_id, deadline=None):
        """Interrupt another connection of this pipeline by its server-side id: a statement in flight ends, an open
        transaction is rolled back. Returns False when there was nothing to end: the connection is idle outside a
        transaction, is closed, or the server does not know the id."""
        if self._in_block:
            raise LaneMisuse("this connection is inside a lane transaction")
        if self.usable and str(server_id) == self.server_id:
            raise LaneMisuse("a connection does not interrupt itself")
        return self._interrupt(server_id, as_deadline(deadline, self.settings.interrupt_deadline_s))

    def abort(self):
        """From any thread (a supervisor that restarts a component): end the call in flight at once, interrupt the
        server connection and close this connection for good. The thread that uses the connection gets
        `ConnectionLost` from its statement (`CommitUnknown` when it was inside COMMIT) and `LaneMisuse` from
        every later call."""
        self._closed = True
        sess = self._session
        if sess is not None:
            sess.abort()

    def close(self, deadline=None):
        """Close the connection. An open transaction is rolled back by the close."""
        self._closed = True
        sess, self._session = self._session, None
        self._stale_ids = []
        if sess is not None:
            sess.close(as_deadline(deadline, self.settings.rollback_deadline_s))

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()


# ---------------------------------------------------------------------- leak checks for tests and shutdown
def open_connections():
    """How many client connections of this package are open in this process."""
    with _registry_lock:
        return len(_open_sessions)


def background_threads():
    """Watchdog, reaper and close threads that have not ended."""
    with _registry_lock:
        return [t for t in _background if t.is_alive()]


def wait_idle(deadline):
    """Wait until every background thread of this package has ended. True when none is left."""
    while True:
        alive = background_threads()
        if not alive:
            return True
        if deadline.expired:
            return False
        alive[0].join(min(0.2, deadline.remaining()))
