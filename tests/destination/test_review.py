"""Tier T0 (local DuckDB): seeded regression tests for what the review of this package found (SPEC section 4).

Each test builds the case of one finding and fails on the code as it was before the fix. The findings and what
was done about each are in `docs/design/destination.md`, section 14.2.

Two more seeded tests of the review stand where their subject is tested: the sorting of client errors by the head
of their text in `test_errors.py`, two creators of one key in `test_state.py`.
"""
import logging
import time
import traceback

import duckdb
import pytest

from cdc_v2 import alerts, md
from cdc_v2.deadline import Deadline
from cdc_v2.md import connection as mdc
from cdc_v2.md import registry
from cdc_v2.testing import destination as td

GRACE = td.FAST["grace_s"]
BUMP = "UPDATE t SET v = v + 1 WHERE k = 1"
WATCHDOG = "cdc_v2-md-watchdog"


def counter(dest):
    """A table with one counter row; returns a connection to look at it."""
    dest.run("CREATE TABLE t (k INTEGER, v BIGINT)")
    dest.run("INSERT INTO t VALUES (1, 0)")
    return dest.connect("look")


def value(c):
    return c.fetchone("SELECT v FROM t WHERE k = 1")[0]


def everything_of(exc):
    """Every text a log or a report could take from an exception: message, repr, arguments, attributes and the
    formatted traceback with its causes."""
    return "\n".join([str(exc), repr(exc), repr(exc.args), repr(vars(exc)),
                      "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))])


# ----------------------------------------------------------------- finding 1: a wait that raises
def test_a_deadline_ends_the_transaction_also_when_no_watchdog_thread_can_start(led, faults, monkeypatch, caplog):
    """The wait for a statement started a watchdog thread at the deadline. A process that cannot start another
    thread got `RuntimeError` out of the wait; a caller that swallowed it left the block normally, and the block
    committed a transaction whose statement had no answer. Now the missing thread is logged, the wait goes on to
    the end of the grace period, the connection is given up and the block fails."""
    c = counter(led)
    lane = led.lane("stream", led.epoch)
    with lane.transaction() as tx:                              # registered; the fault counts from here
        tx.execute("SELECT 1")
    real = mdc._spawn

    def spawn(name, target, *args):
        if name == WATCHDOG:
            raise RuntimeError("can't start new thread")
        return real(name, target, *args)
    monkeypatch.setattr(mdc, "_spawn", spawn)
    caplog.set_level(logging.WARNING, logger="cdc_v2")
    faults.block_at("md.execute", skip=1, hits=1)               # BEGIN passes, the statement has no answer
    t0 = time.monotonic()
    with pytest.raises(md.DeadlineExceeded):
        with lane.transaction() as tx:
            try:
                tx.execute(BUMP, deadline=Deadline.after(0.15))
            except Exception:                                   # noqa: BLE001 - a caller that swallows it
                pass
    took = time.monotonic() - t0
    assert 0.15 <= took < 0.15 + GRACE + 1.0, took
    assert not lane.conn.usable
    lines = [r for r in caplog.records if r.getMessage() == "md background thread not started"]
    assert [(r.cdc["thread"], r.cdc["error"]) for r in lines] == [(WATCHDOG, "RuntimeError")]
    faults.clear()
    assert md.wait_idle(Deadline.after(5.0))
    assert value(c) == 0                                        # nothing was committed
    with lane.transaction() as tx:
        tx.execute(BUMP)
    assert value(c) == 1 and lane.conn.generation == 2


class _Breaks:
    """Stands for the event a thread waits on when something breaks the wait off (Ctrl-C, a signal handler that
    raises). The worker thread still sets the real event."""

    def __init__(self, event):
        self._event = event

    def wait(self, timeout=None):
        raise KeyboardInterrupt()

    def set(self):
        self._event.set()

    def is_set(self):
        return self._event.is_set()


@pytest.mark.parametrize("step, nth", [("statement", 2), ("fence", 3), ("commit", 4)])
def test_a_wait_that_is_broken_off_never_leaves_a_transaction_that_commits(led, monkeypatch, step, nth):
    """An exception in the waiting thread left the connection as it was: the statement could still be running,
    and a caller that went on (or an outer block that caught the exception) committed on it. Now the connection
    is given up before the exception is passed on. Jobs of a block on a registered connection: BEGIN, the
    caller's statement, the fence statement, COMMIT."""
    c = counter(led)
    lane = led.lane("stream", led.epoch)
    with lane.transaction() as tx:
        tx.execute("SELECT 1")
    sess, seen, real = lane.conn._session, [], mdc.Session.submit

    def submit(self, fn):
        job = real(self, fn)
        if self is sess and job is not None:
            seen.append(job)
            if len(seen) == nth:
                job.done = _Breaks(job.done)
        return job
    monkeypatch.setattr(mdc.Session, "submit", submit)
    with pytest.raises(KeyboardInterrupt):
        with lane.transaction() as tx:
            try:
                tx.execute(BUMP)
            except KeyboardInterrupt:                           # a caller that goes on after it
                pass
    monkeypatch.undo()
    assert len(seen) == nth                                     # nothing was sent on that connection afterwards
    assert not lane.conn.usable and sess.state != "open"
    assert md.wait_idle(Deadline.after(5.0))
    after = value(c)
    assert after in ((0, 1) if step == "commit" else (0,)), after   # unknown only once COMMIT was sent
    with lane.transaction() as tx:
        tx.execute(BUMP)
    assert value(c) == after + 1 and lane.conn.generation == 2


# ----------------------------------------------------------------- finding 3: deadline plus grace
def test_the_clean_up_after_a_failed_step_has_one_time_budget(dest, faults):
    """After a conflict in the fence statement the block rolls back and then reads the fence row. Each of the two
    had `rollback_deadline_s` plus the grace period of its own, so a destination that stopped answering held the
    caller twice as long as the documented bound. Now they share one budget."""
    budget = 0.6
    led = dest
    led.settings = td.local_settings(name=led.settings.database.split(":", 2)[2], rollback_deadline_s=budget)
    epoch = led.lead()
    c = counter(led)
    lane = led.lane("stream", epoch)
    with lane.transaction() as tx:
        tx.execute("SELECT 1")
    faults.raise_at("md.fence", "duckdb.TransactionException",
                    message="TransactionContext Error: Conflict on update!", hits=1)
    faults.block_at("md.execute", skip=2)                       # BEGIN and the statement pass; nothing after them
    t0 = time.monotonic()
    with pytest.raises(md.Conflict):
        with lane.transaction() as tx:
            tx.execute(BUMP)
    took = time.monotonic() - t0
    assert budget <= took < 2 * budget + GRACE, took            # before: twice the budget plus twice the grace
    assert not lane.conn.usable
    faults.clear()
    assert md.wait_idle(Deadline.after(5.0))
    assert value(c) == 0
    with lane.transaction() as tx:
        tx.execute(BUMP)
    assert value(c) == 1 and lane.conn.generation == 2


# ----------------------------------------------------------------- finding 4: values in client texts
def test_a_key_value_that_commit_reports_does_not_travel_on(led, faults):
    """A constraint that `COMMIT` reports names the key (`duplicate key "..."`), and a transport error can repeat
    what was sent. The errors of the layer carried the first line of the client's text and the client's exception
    as their cause, so a log with a traceback printed the value. Now they carry the class of the client's error
    and, for a lost connection, the phrase from the fixed list; the client's exception is not attached."""
    key = "-".join(["4111"] * 4)                                # built here: it stands for a value of a row
    number = "4111" * 4
    c = counter(led)
    lane = led.lane("stream", led.epoch)
    with lane.transaction() as tx:
        tx.execute("SELECT 1")

    def clean(exc, message):
        assert str(exc) == message
        assert exc.__cause__ is None and exc.__context__ is None
        text = everything_of(exc)
        assert key not in text and number not in text, text

    faults.raise_at("md.commit", "duckdb.TransactionException", hits=1,
                    message="TransactionContext Error: Failed to commit: PRIMARY KEY or UNIQUE constraint "
                            "violation: duplicate key \"%s\"" % key)
    with pytest.raises(md.CommitUnknown) as info:
        with lane.transaction() as tx:
            tx.execute(BUMP)
    clean(info.value, "lane stream: the outcome of COMMIT is unknown (TransactionException)")
    with lane.transaction() as tx:                              # the new connection registers and works
        tx.execute("SELECT 1")
    # a lost connection in a statement: a value in quotation marks and one without
    lost = "IO Error: Request failed for row %s with key \"%s\"" % (number, key)
    faults.raise_at("md.execute", "duckdb.IOException", message=lost, skip=1, hits=1)
    with pytest.raises(md.ConnectionLost) as info:
        with lane.transaction() as tx:
            tx.execute(BUMP)
    clean(info.value, "lane stream: connection lost during a statement: IOException (Request failed)")
    # the same for the layer's own write in autocommit (the heartbeat of package L): the outcome is unknown
    faults.raise_at("md.execute", "duckdb.IOException", message=lost, hits=1)
    with pytest.raises(md.CommitUnknown) as info:
        md.schema.heartbeat(c, led.epoch, "test")
    clean(info.value, "connection lost during a writing statement outside a transaction (look): "
                      "IOException (Request failed)")
    faults.raise_at("md.execute", "duckdb.IOException", message=lost, hits=1)
    with pytest.raises(md.ConnectionLost) as info:
        c.fetchall("SELECT v FROM t")
    clean(info.value, "connection lost (look): IOException (Request failed)")
    assert value(c) == 0


# ----------------------------------------------------------------- finding 5: the id that registration writes
def test_a_connect_that_answers_after_its_deadline_is_not_handed_out(dest, faults):
    """A connect that answered inside the grace period, after its deadline, was handed to the caller although the
    wait had given the connection up already: the caller held a connection that was being closed. Now a late
    answer is `DeadlineExceeded` and nothing stays open."""
    before = mdc.open_connections()
    faults.delay_at("md.connect", 0.3, hits=1)                  # deadline 0.15 s < answer 0.3 s < 0.15 s + grace
    t0 = time.monotonic()
    with pytest.raises(md.DeadlineExceeded):
        md.connect("late", dest.settings, deadline=Deadline.after(0.15))
    assert 0.15 <= time.monotonic() - t0 < 0.15 + GRACE + 1.0
    assert len(faults.fired(site="md.connect")) == 1            # one attempt: a deadline is not tried again
    assert md.wait_idle(Deadline.after(5.0))
    assert mdc.open_connections() == before
    conn = md.connect("intime", dest.settings)
    assert conn.usable and conn.generation == 1
    conn.close()


def test_registration_writes_the_id_of_the_connection_that_runs_it(led, monkeypatch):
    """Registration read the id of the connection and then ran its statement through the ordinary path, which
    replaces a connection that was given up meanwhile: the row then held the id of the old connection, and a
    take-over would have interrupted the wrong one. Now the statement runs on the connection whose id it writes,
    and a connection that is replaced while it registers is `ConnectionLost`."""
    look = led.connect("look")
    listed = "SELECT connection_id::VARCHAR FROM _cdc.connections WHERE connection_id::VARCHAR IN (?, ?)"
    beat = led.connect("heartbeat")
    first = beat.server_id
    seen, real_ensure = [], mdc.Connection.ensure

    def ensure(self, deadline):
        sess = real_ensure(self, deadline)
        if self is beat:
            seen.append(sess.server_id)
            if len(seen) == 1:
                sess.retire()                                   # given up from outside right after its id was read
        return sess
    monkeypatch.setattr(mdc.Connection, "ensure", ensure)
    with pytest.raises(md.ConnectionLost, match="registration"):
        registry.register(beat, "control", led.epoch)
    assert look.fetchall(listed, [first, first]) == []          # no row under the id of the connection that is gone
    sid = registry.register(beat, "control", led.epoch)
    monkeypatch.undo()
    assert sid == beat.server_id and sid != first and beat.generation == 2
    assert look.fetchall(listed, [first, sid]) == [(sid,)]
    real = mdc.Session.commit

    def commit(self):
        run = real(self)

        def then_given_up():
            run()
            if beat._session is self:
                beat._drop_session()                            # given up right after its COMMIT returned
        return then_given_up
    monkeypatch.setattr(mdc.Session, "commit", commit)
    with pytest.raises(md.ConnectionLost, match="replaced while it registered"):
        registry.register(beat, "control", led.epoch)
    monkeypatch.undo()
    third = registry.register(beat, "control", led.epoch)
    assert third == beat.server_id and third != sid and beat.generation == 3
    lane = led.lane("stream", led.epoch)
    with lane.transaction() as tx:
        with pytest.raises(md.LaneMisuse):                      # not on a connection that is inside a block
            registry.register(lane.conn, "stream", led.epoch)
        tx.execute("SELECT 1")
    rows = look.fetchall("SELECT connection_id::VARCHAR FROM _cdc.connections WHERE lane = 'stream'")
    assert rows == [(lane.conn.server_id,)]
    beat.close()


# ----------------------------------------------------------------- suspicion A: an id that cannot be interrupted
def test_an_old_connection_id_that_the_destination_refuses_does_not_block_the_lane_for_ever(led, faults, monkeypatch):
    """The id of a connection that was given up is interrupted before the next connection is used. An interrupt
    that the destination refused left the id first in the list, so every later call failed in the same way. Now
    the call that met the refusal fails once, with the class of the client's error and without its text, and the
    id is dropped."""
    marker = "zq-marker-5150"
    c = counter(led)
    lane = led.lane("stream", led.epoch)
    with lane.transaction() as tx:
        tx.execute(BUMP)
    first = lane.conn.server_id
    faults.raise_at("md.fence", "ConnectionError", message="injected: connection reset", hits=1)
    with pytest.raises(md.ConnectionLost):
        with lane.transaction() as tx:
            tx.execute(BUMP)
    assert not lane.conn.usable
    calls = []

    def refuse(self, sess, server_id, deadline):
        calls.append(str(server_id))
        raise duckdb.BinderException("Binder Error: no function matches the argument %s" % marker)
    monkeypatch.setattr(mdc.Connection, "_interrupt_on", refuse)
    with pytest.raises(md.StatementError) as info:
        with lane.transaction() as tx:
            tx.execute(BUMP)
    e = info.value
    assert str(e) == "connection stream: the destination refused a statement (BinderException in INTERRUPT)"
    assert (e.error_class, e.statement) == ("BinderException", "INTERRUPT")
    assert e.__cause__ is None and e.__context__ is None and marker not in everything_of(e)
    with lane.transaction() as tx:                              # the next call does not meet the id again
        tx.execute(BUMP)
    assert calls == [first]
    assert value(c) == 2 and lane.conn.generation == 2


# ----------------------------------------------------------------- finding 9: log lines before the commit
def test_a_log_line_written_inside_a_transaction_says_that_nothing_is_committed_yet(led, caplog):
    """The lines "alert" and "state transition" are written when the row is written, inside the caller's open
    transaction. They read like facts; the transaction below rolls back. Each line now carries
    ``committed: False``: the tables `_cdc.alerts` and `_cdc.transitions` are the record of what holds."""
    caplog.set_level(logging.INFO, logger="cdc_v2")
    look = led.connect("look")
    lane = led.lane("stream", led.epoch)
    with pytest.raises(RuntimeError, match="boom"):
        with lane.transaction() as tx:
            alerts.emit(tx, "apply_failed", "warning", {"table_id": 3})
            raise RuntimeError("boom")
    lines = {r.getMessage(): r.cdc for r in caplog.records if r.name in ("cdc_v2.alerts", "cdc_v2.state")}
    assert sorted(lines) == ["alert", "state transition"]
    assert lines["alert"]["committed"] is False and lines["state transition"]["committed"] is False
    assert look.fetchone("SELECT count(*) FROM _cdc.alerts")[0] == 0
    assert look.fetchone("SELECT count(*) FROM _cdc.transitions")[0] == 0
