"""Test support for the destination layer (package D): a destination for one test, and a small take-over.

- `local_destination()` gives a private in-memory DuckDB destination with the leader tables: tier T0, no network.
- `motherduck_destination(prefix)` creates a MotherDuck database with the given prefix and drops it at the end.
- `take_over(settings)` performs the take-over of design 4.5 (epoch + 1 in the leader row, every fence row and the
  heartbeat row, in one transaction) and returns the new epoch. It is a **stand-in for the bootstrap**
  (`flights/cdc_v2/main.py`, package L), which is the only production code that takes over: like the bootstrap it
  works on a plain client connection with its own BEGIN and COMMIT, not through a lane (a lane needs an epoch, and
  the take-over is what makes one).
- `Destination.lead()` is the short form for tests of other packages: take over, migrate, return the epoch.
"""
from __future__ import annotations

import contextlib
import time
import uuid

import duckdb

from cdc_v2 import md
from cdc_v2.deadline import Deadline, DeadlineExceeded
from cdc_v2.md import errors as md_errors
from cdc_v2.md import registry
from cdc_v2.md.connection import _interrupt_local

READ_LEADER_SQL = "SELECT epoch, state, run_id, flight_id, run_number FROM _cdc.leader"
TAKEOVER_LEADER_SQL = (
    "UPDATE _cdc.leader SET epoch = epoch + 1, flight_id = ?, run_number = ?, run_id = ?, state = 'leading', "
    "taken_at = uuid_extract_timestamp(uuidv7()) WHERE epoch = ?"
)
TAKEOVER_FENCE_SQL = "UPDATE _cdc.fence SET epoch = ? + 1"
TAKEOVER_HEARTBEAT_SQL = (
    "UPDATE _cdc.heartbeat SET epoch = ? + 1, beat_at = uuid_extract_timestamp(uuidv7()), phase = 'starting'"
)
LATE_COMMIT_WAIT_S = 2.5        # design 4.5: read again "after more than 2 s (a commit can land that late)"


class TakeoverFailed(Exception):
    """The take-over did not win within its time."""


def raw_connect(settings):
    """A plain client connection to the destination of ``settings`` (tests only)."""
    raw = duckdb.connect(settings.target, config={"custom_user_agent": settings.marker})
    raw.execute("SET TimeZone = 'UTC'")
    return raw


def _interrupt_earlier_raw(raw, settings, epoch):
    if settings.is_motherduck:
        row = raw.execute(registry.INTERRUPT_EARLIER_SQL, [epoch, registry.NO_CONNECTION]).fetchone()
        return int(row[0]), int(row[1] or 0)
    rows = raw.execute(registry.LIST_EARLIER_SQL, [epoch]).fetchall()
    return len(rows), sum(1 for (sid,) in rows if _interrupt_local(sid))


def take_over(settings, run_id=None, flight_id="test-flight", run_number=1, timeout_s=45.0, interrupt=True,
              report=None, pause_s=0.2):
    """Take over as design 4.5 says and return the new epoch.

    On a conflict with an open transaction of an earlier run: interrupt the connections that `_cdc.connections`
    lists for earlier epochs (``interrupt=False`` leaves that out, to show what happens without it) and try again
    until ``timeout_s`` has passed, then raise `TakeoverFailed`. An unknown commit outcome is not repeated blindly:
    the record is read again after more than 2 s and ``run_id`` decides.

    ``report``, a dict, receives what happened: ``attempts``, ``conflicts``, ``interrupted``, ``seconds``, and the
    text class of the last error."""
    run_id = run_id or uuid.uuid4().hex
    rep = report if report is not None else {}
    rep.update(attempts=0, conflicts=0, listed=0, interrupted=0, unknown=0, last_error=None)
    t0 = time.monotonic()
    deadline = Deadline.after(timeout_s)
    raw = raw_connect(settings)
    try:
        while True:
            rep["attempts"] += 1
            sent_commit = False
            try:
                seen = int(raw.execute(READ_LEADER_SQL).fetchone()[0])
                raw.execute("BEGIN")
                n = raw.execute(TAKEOVER_LEADER_SQL, [flight_id, run_number, run_id, seen]).fetchone()[0]
                if n != 1:
                    raise duckdb.TransactionException("Conflict on update! (the leader row has another epoch)")
                raw.execute(TAKEOVER_FENCE_SQL, [seen])
                raw.execute(TAKEOVER_HEARTBEAT_SQL, [seen])
                sent_commit = True
                raw.execute("COMMIT")
                rep["seconds"] = round(time.monotonic() - t0, 3)
                return seen + 1
            except duckdb.Error as e:
                kind = md_errors.kind_of(e)
                rep["last_error"] = md_errors.safe_text(e)
                with contextlib.suppress(duckdb.Error):
                    raw.execute("ROLLBACK")
                if kind == "transport" or (sent_commit and kind != "conflict"):
                    rep["unknown"] += 1
                    time.sleep(LATE_COMMIT_WAIT_S)
                    with contextlib.suppress(duckdb.Error):
                        raw.close()
                    raw = raw_connect(settings)
                    row = raw.execute(READ_LEADER_SQL).fetchone()
                    if row[2] == run_id:
                        rep["seconds"] = round(time.monotonic() - t0, 3)
                        return int(row[0])
                elif kind == "conflict":
                    rep["conflicts"] += 1
                    if interrupt:
                        listed, done = _interrupt_earlier_raw(raw, settings, seen + 1)
                        rep["listed"] += listed
                        rep["interrupted"] += done
                else:
                    raise
            if deadline.expired:
                rep["seconds"] = round(time.monotonic() - t0, 3)
                raise TakeoverFailed("no take-over within %.1f s (%d attempts, last error %s)"
                                     % (timeout_s, rep["attempts"], rep["last_error"]))
            time.sleep(min(pause_s, max(0.0, deadline.remaining())))
    finally:
        with contextlib.suppress(duckdb.Error):
            raw.close()


def read_leader(settings):
    """The leader row as a dict (tests only)."""
    raw = raw_connect(settings)
    try:
        row = raw.execute(READ_LEADER_SQL).fetchone()
        return dict(zip(("epoch", "state", "run_id", "flight_id", "run_number"), row))
    finally:
        raw.close()


class Destination:
    """A destination for a test: its settings, the connections the test opened through it, and the clean-up."""

    def __init__(self, settings, drop=None):
        self.settings = settings
        self._drop = drop
        self._conns = []
        self._keeper = None

    @property
    def database(self):
        return self.settings.database

    def connect(self, role="test"):
        conn = md.connect(role, self.settings)
        self._conns.append(conn)
        return conn

    def lane(self, name, epoch, role=None):
        """A lane on a new connection."""
        return md.Lane(self.connect(role or name), name, epoch)

    def take_over(self, **kw):
        return take_over(self.settings, **kw)

    def lead(self, migrate=True, **kw):
        """Take over, run the migrations as the new leader, return the epoch."""
        epoch = self.take_over(**kw)
        if migrate:
            conn = md.connect("control", self.settings)
            try:
                md.migrate(md.Lane(conn, "control", epoch))
            finally:
                conn.close()
        return epoch

    def raw(self):
        return raw_connect(self.settings)

    def close(self):
        """Close what the test opened; wait for the background threads of the layer; drop the database."""
        for conn in self._conns:
            with contextlib.suppress(Exception):
                conn.close()
        self._conns = []
        md.wait_idle(Deadline.after(max(10.0, 4 * self.settings.grace_s)))
        if self._keeper is not None:
            with contextlib.suppress(duckdb.Error):
                self._keeper.close()
            self._keeper = None
        if self._drop is not None:
            drop, self._drop = self._drop, None
            drop()


# Short times for tests: a blocked statement is given up after fractions of a second, not after minutes.
FAST = dict(connect_deadline_s=5.0, connect_attempts=2, connect_retry_pause_s=0.05, statement_deadline_s=10.0,
            transaction_deadline_s=20.0, rollback_deadline_s=1.0, grace_s=0.3, interrupt_deadline_s=1.0)


def local_settings(name=None, **overrides):
    """Settings for a private in-memory destination of this process."""
    opts = dict(FAST)
    opts.update(overrides)
    return md.Settings(database=":memory:%s" % (name or "cdc_v2_t_%s" % uuid.uuid4().hex[:12]), **opts)


@contextlib.contextmanager
def local_destination(leader_tables=True, **overrides):
    """A private in-memory DuckDB destination (tier T0). With ``leader_tables`` the leader tables exist."""
    settings = local_settings(**overrides)
    dest = Destination(settings)
    dest._keeper = raw_connect(settings)        # an in-memory database lives as long as one connection is open
    try:
        if leader_tables:
            conn = dest.connect("init")
            md.ensure_leader_tables(conn)
            conn.close()
        yield dest
    finally:
        dest.close()


def _admin(sql):
    con = duckdb.connect("md:")
    try:
        con.execute(sql)
    finally:
        con.close()


@contextlib.contextmanager
def motherduck_destination(prefix, leader_tables=True, **overrides):
    """A MotherDuck database ``<prefix><random>`` for one test; dropped at the end, also when the test fails.
    ``prefix`` is the package's own (plan section 4), for example ``cdc_v2_t_d_``. The token comes from the
    environment (`tools/with-env`); nothing here reads it."""
    if not prefix.startswith("cdc_v2_t_") or not prefix.endswith("_"):
        raise ValueError("a test database prefix is cdc_v2_t_<package>_")
    name = "%s%s" % (prefix, uuid.uuid4().hex[:12])
    opts = dict(statement_deadline_s=30.0, transaction_deadline_s=60.0, rollback_deadline_s=5.0, grace_s=2.0)
    opts.update(overrides)
    settings = md.Settings(database=name, **opts)
    _admin("CREATE DATABASE " + name)
    dest = Destination(settings, drop=lambda: _admin("DROP DATABASE IF EXISTS " + name))
    try:
        if leader_tables:
            conn = dest.connect("init")
            md.ensure_leader_tables(conn)
            conn.close()
        yield dest
    finally:
        dest.close()


def wait_no_leftovers(seconds=10.0):
    """True when the layer has no open client connection and no background thread left in this process."""
    d = Deadline.after(seconds)
    while True:
        if md.wait_idle(d) and md.open_connections() == 0:
            return True
        if d.expired:
            return False
        try:
            d.sleep(0.02, "waiting for destination connections to close")
        except DeadlineExceeded:
            return False
