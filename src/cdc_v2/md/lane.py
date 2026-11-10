"""Lanes: the one place that issues BEGIN, the fence statement and COMMIT (contract C4, design 4.2).

A lane is one connection that commits data or state for one epoch. ``lane.transaction(deadline)`` gives a block:

    with lane.transaction(deadline) as tx:
        tx.execute(...)
    # leaving the block: fence statement (exactly one row), COMMIT

What the block guarantees, whatever happens inside it:

* it commits only if the fence statement changed exactly one row, so only for the epoch that still leads;
* after any failed statement nothing is committed. The transaction is rolled back at once and the object is dead:
  a COMMIT that follows a failed statement is answered "OK" by MotherDuck and commits nothing
  (`evidence/p3/destination/error_mapping.json`), so the lane never sends one;
* every step has a deadline; a step that does not come back retires the connection (see `connection`). A
  statement, the registration and the fence statement each have at most ``statement_deadline_s`` (and never more
  than what is left of the block); only `COMMIT` has what is left of the block, because a commit has no progress
  signal and its deadline scales with the size of the group (design 3). The clean-up after a step that failed
  (roll-back, reading the fence row, removing the registered data) has one ``rollback_deadline_s`` per call for
  all of it;
* an exception that breaks a step off in the caller's thread (``KeyboardInterrupt``, ...) gives the connection
  up and fails the transaction: nothing is committed after it, also when the caller swallows it;
* the errors are the five of contract C4 and `StatementError` for a statement that the destination refused.
  Nothing of the client's error text travels on: it can name a value of a row (`errors`).
"""
from __future__ import annotations

import logging
import re
import threading

from cdc_v2 import crashpoints
from cdc_v2.deadline import Deadline, DeadlineExceeded

from . import errors, registry
from .connection import (CLOCK_US_SQL, SITE_EXECUTE, SITE_FENCE, Connection, _NO_TX, as_deadline, check_sql,
                         count_fetch, counts_rows, returns_rows, statement_kind)
from .errors import (CommitUnknown, Conflict, ConnectionLost, DestinationError, FenceLost, LaneMisuse,
                     StatementError)
from .settings import LANES

log = logging.getLogger("cdc_v2.md")

#: The fence statement (design 4.2): the last statement before every COMMIT of a lane. Parameters: epoch, lane,
#: epoch.
#:
#: It writes the column `epoch` as well as `n`, with the value the row already has. DuckDB and MotherDuck detect a
#: write-write conflict per column, not per row: the statement of design 4.2 (`SET n = n + 1` alone) does not
#: conflict with a take-over that writes `epoch` alone (design 4.5), and a transaction of the old run that began
#: before the take-over committed then passes its fence and commits after it
#: (`evidence/p3/destination/fence_columns.json`: 5 of 5 on local DuckDB and on MotherDuck, in both orders). Writing
#: `epoch` here makes the take-over and the fence statement meet on one column, whatever else either writes.
FENCE_SQL = "UPDATE _cdc.fence SET n = n + 1, epoch = ? WHERE lane = ? AND epoch = ?"
FENCE_EPOCH_SQL = "SELECT epoch FROM _cdc.fence WHERE lane = ?"

_VIEW_NAME = re.compile(r"^[a-z_][a-z0-9_]{0,62}$")


def _arrow_table(data):
    """What `register` hands to the client: one ``pyarrow.Table``. Anything that is not a table, a record batch
    or a list of record batches is refused; no message repeats a piece of the data."""
    import pyarrow as pa
    if isinstance(data, pa.Table):
        return data
    if isinstance(data, pa.RecordBatch):
        data = [data]
    if isinstance(data, pa.RecordBatchReader):
        raise LaneMisuse("a record batch reader is not registered: read it into a pyarrow.Table first")
    if not isinstance(data, (list, tuple)) or not all(isinstance(b, pa.RecordBatch) for b in data):
        raise LaneMisuse("register takes a pyarrow.Table, a record batch or a list of record batches")
    if not data:
        raise LaneMisuse("nothing to register: the list of record batches is empty")
    schema = data[0].schema
    if any(not b.schema.equals(schema) for b in data[1:]):
        raise LaneMisuse("the record batches of one registered name have one schema")
    return pa.Table.from_batches(list(data))


def _declare_points():
    points = {}
    for lane in LANES:
        points[lane] = (
            crashpoints.declare(lane + ".fenced", "lane %s: fence statement executed, COMMIT not sent" % lane),
            crashpoints.declare(lane + ".commit_sent", "lane %s: immediately before COMMIT (run with kill_later)" % lane),
            crashpoints.declare(lane + ".committed", "lane %s: COMMIT returned" % lane),
        )
    return points


_POINTS = _declare_points()


def crash_points(lane):
    """The three crash point names of a lane: fenced, commit_sent, committed."""
    return _POINTS[lane]


class Lane:
    """One connection that commits for one epoch under one lane name (contract C4).

    A connection belongs to at most one lane. Before its first transaction, and again after every replacement of
    the client connection, the lane writes the connection's server-side id into ``_cdc.connections``; when the
    leader row no longer holds the lane's epoch that raises `FenceLost`.
    """

    def __init__(self, conn, name, epoch):
        if not isinstance(conn, Connection):
            raise TypeError("a lane needs a connection from md.connect()")
        if name not in LANES:
            raise ValueError("unknown lane %r: the lanes are stream, control and backfill.0 to backfill.15" % (name,))
        if isinstance(epoch, bool) or not isinstance(epoch, int) or epoch < 1:
            raise ValueError("a lane's epoch is an integer >= 1 (the epoch its run took over with)")
        if conn._lane is not None:
            raise LaneMisuse("this connection already belongs to lane %s" % conn._lane.name)
        self.conn = conn
        self.name = name
        self.epoch = epoch
        self._registered_id = None
        self._guard = threading.Lock()         # held while a transaction block of this lane is open
        conn._lane = self

    def __repr__(self):
        return "<md.Lane %s epoch=%d>" % (self.name, self.epoch)

    def transaction(self, deadline=None):
        """A transaction block. ``deadline`` bounds the whole block including the fence statement and COMMIT
        (None: ``transaction_deadline_s`` of the settings)."""
        return Transaction(self, as_deadline(deadline, self.conn.settings.transaction_deadline_s))


class Transaction:
    """The object a transaction block works with. It lives exactly as long as the block, in the thread that
    opened it; any other use raises `LaneMisuse`."""

    def __init__(self, lane, deadline):
        self._lane = lane
        self._conn = lane.conn
        self._deadline = deadline
        self._state = "new"                    # new -> open -> failed -> ended
        self._error = None
        self._thread = None
        self._sess = None
        self._views = []
        self._cleanup = None                   # the deadline for the clean-up of the call that is running

    # ------------------------------------------------------------------ what a caller may read
    @property
    def lane(self):
        return self._lane.name

    @property
    def epoch(self):
        return self._lane.epoch

    @property
    def deadline(self):
        return self._deadline

    @property
    def is_motherduck(self):
        return self._conn.is_motherduck

    # ------------------------------------------------------------------ the block
    def __enter__(self):
        lane, conn, d = self._lane, self._conn, self._deadline
        if self._state != "new":
            raise LaneMisuse("a transaction object is good for one block")
        if not lane._guard.acquire(blocking=False):
            raise LaneMisuse("lane %s already has an open transaction (nested, or from another thread)" % lane.name)
        try:
            if conn._in_block:
                raise LaneMisuse("the connection of lane %s is inside a transaction" % lane.name)
            sess = conn.ensure(d)
            if lane._registered_id != sess.server_id:
                # a transaction of its own that also writes the lane's fence row: it cannot commit after a
                # take-over (registry.register); the statement deadline bounds it, not the block's
                registry.register(conn, lane.name, lane.epoch, self._stmt_deadline(None))
                if conn._session is not sess or sess.state != "open":
                    raise ConnectionLost("lane %s: the connection was replaced while it registered" % lane.name)
                lane._registered_id = sess.server_id
            self._sess = sess
            out = self._on(sess.statement(SITE_EXECUTE, "BEGIN", None, None, tx=True), self._stmt_deadline(None))
            if not (out.ok and not out.late):
                raise self._failure(out, "BEGIN", "BEGIN", rollback=out.ok)
        except BaseException:
            self._state = "ended"
            lane._guard.release()
            raise
        conn._in_block = True
        self._sess = sess
        self._thread = threading.get_ident()
        self._state = "open"
        return self

    def __exit__(self, exc_type, exc, tb):
        lane, conn = self._lane, self._conn
        state, self._state = self._state, "ended"
        self._cleanup = None
        try:
            if state == "failed":
                if exc is None:
                    raise self._error              # a swallowed error: the transaction was rolled back, say so
                return False
            if exc is not None:
                self._rollback()
                return False
            self._finish()
            return False
        finally:
            try:
                self._drop_views()
            finally:
                conn._in_block = False
                lane._guard.release()

    # ------------------------------------------------------------------ statements
    def execute(self, sql, params=None, deadline=None):
        """Run one statement. Returns the number of rows an INSERT, UPDATE or DELETE changed, else None (also
        for CREATE TABLE AS and MERGE, and for a statement with RETURNING: its rows are read with
        `fetchall`)."""
        n = self._call(sql, params, count_fetch(sql), deadline)
        return n if counts_rows(sql) else None

    def execute_one(self, sql, params=None, deadline=None):
        """Run a statement that must change exactly one row (the run-again rule of C4). Any other count rolls the
        transaction back and raises `Conflict`. A statement that reports no count (a SELECT, or a statement with
        RETURNING: its result is its rows) is a mistake of the caller: rolled back, `LaneMisuse`."""
        n = self._call(sql, params, count_fetch(sql), deadline)
        if returns_rows(sql):
            n = None                                 # a returned value is never taken for a count
        if n == 1:
            return 1
        self._rollback()
        if n is None:
            raise self._fail(LaneMisuse("execute_one needs a statement that reports its changed rows"))
        raise self._fail(Conflict("lane %s: a statement that must change exactly one row changed %d"
                                  % (self.lane, n)))

    def fetchall(self, sql, params=None, deadline=None):
        return self._call(sql, params, "all", deadline)

    def fetchone(self, sql, params=None, deadline=None):
        return self._call(sql, params, "one", deadline)

    def fetch_arrow(self, sql, params=None, deadline=None):
        """The result of a query as a ``pyarrow.Table``, read completely before the call returns. (The
        client's lazy reader is never handed out: registering a reader on the connection that produced it and
        querying it does not come back.)"""
        return self._call(sql, params, "arrow", deadline)

    def clock_us(self, deadline=None):
        """MotherDuck's clock inside this transaction, microseconds since 1970 (design 4.1)."""
        return int(self._call(CLOCK_US_SQL, None, "one", deadline)[0])

    def register(self, name, data, deadline=None):
        """Make local Arrow data readable by this connection's statements under ``name`` until the block ends:
        a ``pyarrow.Table``, a ``pyarrow.RecordBatch`` or a list of record batches. A name that is registered
        already gets the new data. When the block ends the name is removed, also when it ends by an error or
        a deadline.

        ``name`` matches ``^[a-z_][a-z0-9_]{0,62}$``. A record batch reader is refused: it can be read once,
        and a reader that this connection produced cannot be read by a statement of this connection (the
        client waits for itself)."""
        self._check()
        if not isinstance(name, str) or not _VIEW_NAME.fullmatch(name):     # `$` alone lets a last newline pass
            raise LaneMisuse("a registered name is a lower-case identifier of at most 63 characters")
        data = _arrow_table(data)
        out = self._on(self._sess.view(name, data), self._stmt_deadline(deadline))
        if not (out.ok and not out.late):
            raise self._fail(self._failure(out, "registering local data", "REGISTER"))
        if name not in self._views:
            self._views.append(name)

    def fail(self, error):
        """Roll the transaction back now, because the caller found something that must not be committed. Every
        later use raises `LaneMisuse`; leaving the block without an exception raises ``error``. Returns ``error``,
        so that a caller can write ``raise tx.fail(MyError(...))``."""
        self._check()
        if not isinstance(error, BaseException):
            raise TypeError("fail() takes the exception that says why")
        self._rollback()
        return self._fail(error)

    # ------------------------------------------------------------------ internals
    def _check(self):
        if self._state == "failed":
            raise LaneMisuse("this transaction failed (%s) and was rolled back" % type(self._error).__name__)
        if self._state != "open":
            raise LaneMisuse("a transaction object is used inside its block only")
        if threading.get_ident() != self._thread:
            raise LaneMisuse("a transaction is used by the thread that opened it")
        self._cleanup = None                   # a new call: its clean-up has its own time

    def _stmt_deadline(self, deadline):
        d = as_deadline(deadline, self._conn.settings.statement_deadline_s)
        return d if d < self._deadline else self._deadline

    def _call(self, sql, params, fetch, deadline, site=SITE_EXECUTE):
        self._check()
        try:
            check_sql(sql, "lane %s" % self._lane.name)
        except StatementError as e:                 # the client cannot parse it; nothing was sent. A failed
            self._rollback()                        # statement like any other: nothing is committed after it
            raise self._fail(e)
        out = self._on(self._sess.statement(site, sql, params, fetch), self._stmt_deadline(deadline))
        if out.ok and not out.late:
            return out.result
        raise self._fail(self._failure(out, "a statement", statement_kind(sql)))

    def _on(self, fn, deadline):
        """Run one step of the block on its connection and wait for it (`Session.call`).

        The call itself raises only when something breaks the wait off in this thread (``KeyboardInterrupt``,
        ...). What the step did is not known then. The connection is given up, so that nothing can be committed
        on it, the transaction is failed, and the exception is passed on."""
        sess = self._sess
        try:
            return sess.call(fn, deadline)
        except BaseException as e:
            self._conn._drop_session(sess)
            if self._state == "open":
                self._fail(e)
            raise

    def _budget(self):
        """The deadline for the clean-up of the call that is running. The roll-back, the read of the fence row
        and the removal of the registered data share one ``rollback_deadline_s``, counted from the first of
        them: a call returns at the latest ``rollback_deadline_s`` plus the grace period after the step that
        failed."""
        if self._cleanup is None:
            self._cleanup = Deadline.after(self._conn.settings.rollback_deadline_s)
        return self._cleanup

    def _fail(self, error):
        self._state = "failed"
        self._error = error
        return error

    def _failure(self, out, what, statement, rollback=True):
        """The error for a step before COMMIT that did not succeed. Afterwards the transaction is gone: rolled
        back on a connection that still works, or left to the reaper of a retired one. ``statement`` is the
        kind of the step, for the `StatementError` of a step that the destination refused."""
        name, conn, sess = self._lane.name, self._conn, self._sess
        if not out.sent:
            if rollback:
                self._rollback()
            return DeadlineExceeded("lane %s: the deadline passed before %s" % (name, what))
        if out.late:
            conn._drop_session(sess)
            return DeadlineExceeded("lane %s: %s" % (name, what))
        err = out.error
        if err is None:                             # completed, but the caller treats it as failed (BEGIN late)
            conn._drop_session(sess)
            return DeadlineExceeded("lane %s: %s" % (name, what))
        kind = errors.kind_of(err)
        if kind == "transport":
            conn._drop_session(sess)
            # the client's error is not attached, and of its text only a phrase from the list travels on
            return ConnectionLost("lane %s: connection lost during %s: %s" % (name, what, errors.label(err)))
        if rollback:
            self._rollback()
        if kind == "conflict":
            return Conflict("lane %s: write-write conflict in %s" % (name, what))
        # the client's text can name a value of a row (SPEC 8.3): its class and the kind of statement travel on
        return errors.refused(err, "lane %s" % name, statement)

    def _rollback(self):
        """ROLLBACK within the clean-up time of the call (`_budget`). A connection that cannot roll back in time
        is retired; its reaper interrupts the server connection, which rolls the transaction back there."""
        conn, sess = self._conn, self._sess
        if sess is None:
            return
        if sess.state != "open":
            conn._drop_session(sess)
            return
        out = self._on(sess.statement(SITE_EXECUTE, "ROLLBACK", None, None, tx=False), self._budget())
        if out.ok and not out.late:
            return
        if out.sent and out.returned and not out.late and out.error is not None and _NO_TX.search(str(out.error)):
            sess.in_tx = False
            return
        log.warning("roll-back failed; connection retired",
                    extra={"cdc": {"lane": self._lane.name, "late": bool(out.late),
                                   "error": errors.label(out.error) if out.error is not None else None}})
        conn._drop_session(sess)

    def _drop_views(self):
        sess = self._sess
        views, self._views = self._views, []
        if sess is None or sess.state != "open":
            return
        d = self._budget()
        for name in views:
            out = self._on(sess.view(name, None), d)
            if not (out.ok and not out.late):
                self._conn._drop_session(sess)
                return

    def _fence_conflict(self):
        """The fence row is being written, or was written, by someone else. After the roll-back, read the row to
        tell a finished take-over (`FenceLost`) from one in progress or a lane used twice (`Conflict`)."""
        lane, conn, d = self._lane, self._conn, self._budget()
        try:
            # no time left: not read. `Conflict` then; the next try of the caller meets the fence statement
            row = () if d.expired else conn._run(SITE_EXECUTE, FENCE_EPOCH_SQL, [lane.name], "one", d,
                                                 writes=False)
        except (DestinationError, DeadlineExceeded):
            row = ()
        if row is None or (row and int(row[0]) != lane.epoch):
            return FenceLost("lane %s: epoch %d no longer leads (the fence row has another epoch)"
                             % (lane.name, lane.epoch))
        return Conflict("lane %s: the fence row is being written by another transaction" % lane.name)

    def _finish(self):
        """Leaving the block without an exception: fence statement, exactly one row, COMMIT."""
        lane, conn, sess, d = self._lane, self._conn, self._sess, self._deadline
        name, epoch = lane.name, lane.epoch
        fenced, commit_sent, committed = _POINTS[name]
        # the statement deadline, not the block's: a fence statement that waits for an answer the network does
        # not bring holds the caller at most statement_deadline_s plus the grace period (SPEC 4.5)
        out = self._on(sess.statement(SITE_FENCE, FENCE_SQL, [epoch, name, epoch], "count"), self._stmt_deadline(None))
        if not (out.ok and not out.late):
            err = self._failure(out, "the fence statement", "FENCE")
            if isinstance(err, Conflict):
                err = self._fence_conflict()
            raise err
        if out.result != 1:
            self._rollback()
            raise FenceLost("lane %s: epoch %d no longer leads (the fence statement changed %s rows)"
                            % (name, epoch, out.result))
        try:
            crashpoints.crash_point(fenced)
            if d.expired:
                raise DeadlineExceeded("lane %s: the deadline passed before COMMIT" % name)
            crashpoints.crash_point(commit_sent)
        except BaseException:
            self._rollback()
            raise
        job = sess.submit(sess.commit())
        if job is None:                             # retired from outside (abort) before anything was sent
            conn._drop_session(sess)
            raise ConnectionLost("lane %s: connection closed before COMMIT" % name)
        try:
            out = sess.wait(job, d)
        except BaseException:                       # the wait was broken off: the outcome of COMMIT is unknown
            conn._drop_session(sess)
            raise
        if out.ok:
            if out.late:                            # it did commit; only the connection is replaced
                conn._drop_session(sess)
        else:
            err = out.error
            if out.returned and not out.late and err is not None and errors.kind_of(err) == "conflict":
                self._rollback()
                raise Conflict("lane %s: write-write conflict at COMMIT" % name)
            conn._drop_session(sess)
            # a constraint that COMMIT reports names a key: only the class of the client's error travels on, and
            # the error is not attached (SPEC 8.3)
            why = errors.label(err) if err is not None else "no answer within the deadline and the grace period"
            raise CommitUnknown("lane %s: the outcome of COMMIT is unknown (%s)" % (name, why))
        crashpoints.crash_point(committed)
