"""Tier T0 (local DuckDB): lanes, the fence, the connection registry, deadlines, connection replacement and the
lane crash points. `dest` also checks, after every test, that no connection and no thread of the layer is left."""
import threading
import time

import pytest

from cdc_v2 import crashpoints, md
from cdc_v2.crashpoints import CrashPointHit
from cdc_v2.deadline import Deadline
from cdc_v2.md import connection as mdc
from cdc_v2.md import registry
from cdc_v2.testing import destination as td

GRACE = td.FAST["grace_s"]


def counter(dest):
    """A table with one counter row; returns a connection to look at it."""
    c = dest.connect("look")
    c.execute("CREATE TABLE t (k INTEGER, v BIGINT)")
    c.execute("INSERT INTO t VALUES (1, 0)")
    return c


def value(c):
    return c.fetchone("SELECT v FROM t WHERE k = 1")[0]


def fence(c, lane="stream"):
    return tuple(c.fetchone("SELECT epoch, n FROM _cdc.fence WHERE lane = ?", [lane]))


BUMP = "UPDATE t SET v = v + 1 WHERE k = 1"


# --------------------------------------------------------------------------------------------- commit and fence
def test_commit_runs_the_fence_statement_and_registers_the_connection(led):
    c = counter(led)
    lane = led.lane("stream", led.epoch)
    with lane.transaction() as tx:
        assert tx.execute(BUMP) == 1
        assert tx.execute_one("UPDATE t SET v = v + ? WHERE k = 1", [10]) == 1
        assert tx.fetchall("SELECT v FROM t") == [(11,)]
        assert tx.fetchone("SELECT count(*) FROM t") == (1,)
        assert tx.lane == "stream" and tx.epoch == led.epoch
        assert value(c) == 0                                    # not visible before the commit
    assert value(c) == 11
    assert fence(c) == (1, 1)
    assert fence(c, "control")[1] >= 1 and fence(c, "backfill.3") == (1, 0)     # control ran the migrations
    rows = c.fetchall("SELECT epoch, lane, connection_id::VARCHAR FROM _cdc.connections WHERE lane = 'stream'")
    assert rows == [(1, "stream", lane.conn.server_id)]
    with lane.transaction() as tx:
        tx.execute(BUMP)
    assert fence(c) == (1, 2) and value(c) == 12
    assert c.fetchone("SELECT count(*) FROM _cdc.connections WHERE lane = 'stream'") == (1,)   # registered once


def test_empty_transaction_still_runs_the_fence(led):
    c = led.connect("look")
    lane = led.lane("backfill.15", led.epoch)
    with lane.transaction():
        pass
    assert fence(c, "backfill.15") == (1, 1)


def test_stale_epoch_gets_fence_lost_and_changes_nothing(led):
    c = counter(led)
    old = led.lane("stream", led.epoch)
    with old.transaction() as tx:
        tx.execute(BUMP)
    assert led.take_over() == 2
    with pytest.raises(md.FenceLost):
        with old.transaction() as tx:
            tx.execute(BUMP)                                    # the statement works; the fence at the end does not
    assert value(c) == 1 and fence(c) == (2, 1)                 # a take-over raises the epoch and keeps the count
    with pytest.raises(md.FenceLost):                           # and again: the lane stays fenced out
        with old.transaction() as tx:
            tx.execute(BUMP)
    new = led.lane("stream", 2)
    with new.transaction() as tx:
        tx.execute(BUMP)
    assert value(c) == 2 and fence(c) == (2, 2)


def test_new_connection_of_a_stale_epoch_is_refused_at_registration(led):
    c = counter(led)
    assert led.take_over() == 2
    late = led.lane("stream", 1)                                # a connection opened by the old run after the take-over
    with pytest.raises(md.FenceLost):
        with late.transaction() as tx:
            tx.execute(BUMP)
    assert value(c) == 0
    assert c.fetchone("SELECT count(*) FROM _cdc.connections WHERE lane = 'stream'") == (0,)


def test_take_over_between_begin_and_the_fence_statement(led):
    c = counter(led)
    lane = led.lane("stream", led.epoch)
    with pytest.raises(md.FenceLost):
        with lane.transaction() as tx:
            tx.execute(BUMP)
            assert led.take_over(interrupt=False) == 2          # commits while the transaction is open
    assert value(c) == 0 and fence(c) == (2, 0)


def test_conflicts_are_per_column_which_is_why_the_fence_statement_writes_the_epoch(led):
    """Seeded regression (evidence/p3/destination/fence_columns.json). DuckDB detects a write-write conflict per
    column. With the statement texts of design 4.2 and 4.5 (fence writes `n`, take-over writes `epoch`) a
    transaction that began before the take-over committed passes its fence and commits after it. The statement the
    lane really sends also writes `epoch` and is stopped."""
    from cdc_v2.md import lane as lane_module
    design_fence = "UPDATE _cdc.fence SET n = n + 1 WHERE lane = ? AND epoch = ?"
    assert "epoch = ?" in lane_module.FENCE_SQL.split("WHERE")[0]
    c = counter(led)
    old = led.raw()
    try:
        old.execute("BEGIN")
        old.execute(BUMP)
        assert led.take_over(interrupt=False) == 2
        assert old.execute(design_fence, ["stream", 1]).fetchone()[0] == 1      # the old snapshot still sees epoch 1
        old.execute("COMMIT")
        assert value(c) == 1 and fence(c) == (2, 1)             # committed after the take-over: the hazard
        old.execute("BEGIN")
        old.execute(BUMP)
        assert led.take_over(interrupt=False) == 3
        with pytest.raises(Exception) as info:
            old.execute(lane_module.FENCE_SQL, [2, "stream", 2])
        assert "Conflict on update" in str(info.value)
        old.execute("ROLLBACK")
        assert value(c) == 1 and fence(c) == (3, 1)
    finally:
        old.close()


def test_a_commit_that_hangs_holds_the_fence_row_until_the_take_over_interrupts_it(led, faults):
    """Design 4.5: "a hung commit holds a fence row". The old run has sent its fence statement and its COMMIT does
    not come back. The take-over conflicts, interrupts the connections of earlier epochs and wins; the old
    transaction never commits and its caller gets `CommitUnknown`."""
    c = counter(led)
    lane = led.lane("stream", led.epoch)
    with lane.transaction() as tx:
        tx.execute("SELECT 1")
    faults.block_at("md.commit", hits=1)
    seen = []

    def old_run():
        try:
            with lane.transaction(Deadline.after(3.0)) as tx:
                tx.execute(BUMP)
        except BaseException as e:      # noqa: BLE001
            seen.append(e)
    t = threading.Thread(target=old_run)
    t.start()
    try:
        d = Deadline.after(5.0)
        while not faults.fired(site="md.commit"):
            d.sleep(0.01, "waiting for the old run to reach COMMIT")
        rep = {}
        with pytest.raises(td.TakeoverFailed):
            led.take_over(interrupt=False, timeout_s=0.3, pause_s=0.02, report=rep)
        assert rep["conflicts"] >= 2
        rep = {}
        assert led.take_over(timeout_s=5, pause_s=0.02, report=rep) == 2
        assert rep["conflicts"] == 1 and rep["interrupted"] >= 1
    finally:
        faults.clear()
        t.join(10)
    assert not t.is_alive()
    assert len(seen) == 1 and isinstance(seen[0], md.CommitUnknown)
    assert value(c) == 0 and fence(c) == (2, 1)                 # only the first, empty transaction counted


def test_same_lane_same_epoch_twice_never_commits_both(led):
    """Two connections that use one lane name with one epoch (a programming error) cannot both commit
    transactions that overlap: the fence row is the same row."""
    c = counter(led)
    a, b = led.lane("stream", led.epoch), led.lane("stream", led.epoch)
    with pytest.raises(md.Conflict):
        with a.transaction() as tx:
            tx.execute("INSERT INTO t VALUES (2, 0)")
            with b.transaction() as tx2:
                tx2.execute("INSERT INTO t VALUES (3, 0)")
    assert c.fetchall("SELECT k FROM t ORDER BY k") == [(1,), (3,)]
    assert fence(c) == (1, 1)


def test_execute_one_needs_exactly_one_row(led):
    c = counter(led)
    c.execute("INSERT INTO t VALUES (2, 0), (2, 0)")
    lane = led.lane("stream", led.epoch)
    for sql in ("UPDATE t SET v = 5 WHERE k = 99", "UPDATE t SET v = 5 WHERE k = 2"):
        with pytest.raises(md.Conflict):
            with lane.transaction() as tx:
                tx.execute(BUMP)
                tx.execute_one(sql)
        assert c.fetchall("SELECT k, v FROM t ORDER BY k") == [(1, 0), (2, 0), (2, 0)]     # all rolled back
    assert fence(c) == (1, 0)
    with pytest.raises(md.LaneMisuse):
        with lane.transaction() as tx:
            tx.execute_one("SELECT 1")
    with lane.transaction() as tx:                              # the lane works after each of these
        tx.execute_one(BUMP)
    assert value(c) == 1


def test_an_exception_rolls_back_and_is_passed_on(led):
    c = counter(led)
    lane = led.lane("stream", led.epoch)
    with pytest.raises(KeyError):
        with lane.transaction() as tx:
            tx.execute(BUMP)
            raise KeyError("boom")
    assert value(c) == 0 and fence(c) == (1, 0)
    with pytest.raises(Exception) as info:                      # an error of the statement itself: unchanged type
        with lane.transaction() as tx:
            tx.execute(BUMP)
            tx.execute("SELECT * FROM no_such_table")
    assert type(info.value).__name__ == "CatalogException"
    assert value(c) == 0
    with lane.transaction() as tx:
        tx.execute(BUMP)
    assert value(c) == 1


def test_a_failed_transaction_cannot_end_quietly(led):
    """A caller that swallows the error of a statement does not get a commit of the rest."""
    c = counter(led)
    lane = led.lane("stream", led.epoch)
    with pytest.raises(md.Conflict):
        with lane.transaction() as tx:
            tx.execute(BUMP)
            try:
                tx.execute_one("UPDATE t SET v = 5 WHERE k = 99")
            except md.Conflict:
                pass
    assert value(c) == 0
    with pytest.raises(md.LaneMisuse):
        with lane.transaction() as tx:
            try:
                tx.execute_one("UPDATE t SET v = 5 WHERE k = 99")
            except md.Conflict:
                pass
            tx.execute(BUMP)                                    # the object is dead after its roll-back
    assert value(c) == 0


def test_arrow_data_and_row_counts(led):
    pa = pytest.importorskip("pyarrow")
    c = counter(led)
    lane = led.lane("backfill.0", led.epoch)
    batch = pa.table({"k": [10, 11, 12], "v": [1, 2, 3]})
    with lane.transaction() as tx:
        tx.register("incoming", batch)
        assert tx.execute("INSERT INTO t SELECT k, v FROM incoming") == 3
        assert tx.fetch_arrow("SELECT k FROM t WHERE k >= 10 ORDER BY k").column("k").to_pylist() == [10, 11, 12]
        assert tx.clock_us() > 1_600_000_000_000_000
    assert c.fetchone("SELECT count(*), sum(v) FROM t") == (4, 6)
    with pytest.raises(Exception) as info:                      # the view is gone after the block
        with lane.transaction() as tx:
            tx.fetchall("SELECT * FROM incoming")
    assert type(info.value).__name__ == "CatalogException"


# --------------------------------------------------------------------------------------------- misuse
def test_misuse_is_refused(led):
    c = counter(led)
    conn = led.connect("stream")
    for bad in ("backfill.16", "stream ", "Stream", "heartbeat", ""):
        with pytest.raises(ValueError):
            md.Lane(conn, bad, 1)
    for bad in (0, -1, True, "1", None):
        with pytest.raises(ValueError):
            md.Lane(conn, "stream", bad)
    with pytest.raises(TypeError):
        md.Lane(led.raw(), "stream", 1)
    lane = md.Lane(conn, "stream", led.epoch)
    with pytest.raises(md.LaneMisuse):
        md.Lane(conn, "control", led.epoch)                     # one connection, one lane

    with lane.transaction() as tx:
        with pytest.raises(md.LaneMisuse):                      # nested
            with lane.transaction():
                pass
        with pytest.raises(md.LaneMisuse):                      # the connection itself is busy with the block
            conn.execute(BUMP)
        for sql in ("BEGIN", "COMMIT", "ROLLBACK", "begin transaction", BUMP + "; " + BUMP, "", "ABORT",
                    "END", "START TRANSACTION"):
            with pytest.raises(md.LaneMisuse):
                tx.execute(sql)
        seen = []

        def other():
            try:
                tx.execute(BUMP)
            except BaseException as e:      # noqa: BLE001
                seen.append(e)
        t = threading.Thread(target=other)
        t.start()
        t.join()
        assert len(seen) == 1 and isinstance(seen[0], md.LaneMisuse)    # another thread
        tx.execute(BUMP)
    assert value(c) == 1                                        # the refused calls did not harm the transaction
    with pytest.raises(md.LaneMisuse):                          # after its block
        tx.execute(BUMP)
    with pytest.raises(md.LaneMisuse):
        tx.fetchall("SELECT 1")
    with pytest.raises(md.LaneMisuse):
        with tx:
            pass
    assert value(c) == 1 and fence(c) == (1, 1)


def test_leader_rows_cannot_be_deleted_or_inserted(led):
    conn = led.connect("x")
    lane = led.lane("control", led.epoch)
    bad = ["DELETE FROM _cdc.fence WHERE lane = 'stream'", "INSERT INTO _cdc.leader SELECT * FROM _cdc.leader",
           "insert into _cdc.heartbeat (epoch) values (1)", "TRUNCATE _cdc.fence", "DROP TABLE _cdc.leader",
           "DROP TABLE IF EXISTS _cdc.heartbeat", "ALTER TABLE _cdc.fence ADD COLUMN x INT", "DROP SCHEMA _cdc CASCADE",
           'DELETE FROM "_cdc"."leader"', "CREATE OR REPLACE TABLE _cdc.fence AS SELECT 1",
           "INSERT OR REPLACE INTO _cdc.fence VALUES ('stream', 1, 1)"]
    for sql in bad:
        with pytest.raises(md.LaneMisuse):
            conn.execute(sql)
    with lane.transaction() as tx:
        for sql in bad:
            with pytest.raises(md.LaneMisuse):
                tx.execute(sql)
        tx.execute("UPDATE _cdc.heartbeat SET phase = 'ok' WHERE epoch = ?", [led.epoch])   # updates are the way
    assert conn.fetchone("SELECT phase FROM _cdc.heartbeat") == ("ok",)
    md.schema.check_leader_tables(conn)


# --------------------------------------------------------------------------------------------- registry
def test_open_transaction_of_an_old_run_blocks_a_take_over_until_it_is_interrupted(led):
    """Design 4.5: an open transaction of the old run that changed a row the take-over needs makes the take-over
    fail with a conflict; interrupting the connections that `_cdc.connections` lists for earlier epochs ends it."""
    c = counter(led)
    old = led.lane("control", led.epoch)
    with pytest.raises(md.ConnectionLost):
        with old.transaction() as tx:
            tx.execute(BUMP)
            tx.execute("UPDATE _cdc.heartbeat SET phase = 'busy' WHERE epoch = ?", [led.epoch])
            rep = {}
            with pytest.raises(td.TakeoverFailed):
                led.take_over(interrupt=False, timeout_s=0.3, pause_s=0.02, report=rep)
            assert rep["conflicts"] >= 2 and rep["interrupted"] == 0
            assert td.read_leader(led.settings)["epoch"] == 1
            rep = {}
            assert led.take_over(timeout_s=5, pause_s=0.02, report=rep) == 2
            assert rep["conflicts"] == 1 and rep["interrupted"] >= 1 and rep["attempts"] == 2
            tx.execute(BUMP)                                    # the old run learns it at its next statement
    assert value(c) == 0
    assert c.fetchone("SELECT phase FROM _cdc.heartbeat") == ("starting",)
    with pytest.raises(md.FenceLost):                           # its lane reconnects and is refused
        with old.transaction() as tx:
            tx.execute(BUMP)


def test_interrupt_earlier_and_cleanup(led):
    c = counter(led)
    lanes = {1: led.lane("stream", 1)}
    with lanes[1].transaction() as tx:
        tx.execute(BUMP)
    for epoch in (2, 3):
        assert led.take_over() == epoch
        lanes[epoch] = led.lane("stream", epoch)
        with lanes[epoch].transaction() as tx:
            tx.execute(BUMP)
    stale = lanes[2].transaction()
    stale.__enter__()
    stale.execute(BUMP)                                         # epoch 2 holds the counter row
    admin = led.connect("admin")
    assert registry.interrupt_earlier(admin, 1) == (0, 0)       # exclusive: nothing is earlier than epoch 1
    listed, done = registry.interrupt_earlier(admin, 3)
    assert listed == 3 and done >= 1                            # init (migrations), stream of 1, stream of 2
    with pytest.raises(md.ConnectionLost):
        stale.execute(BUMP)
    with pytest.raises(md.ConnectionLost):
        stale.__exit__(None, None, None)
    with lanes[3].transaction() as tx:                          # the leader's own connection was not touched
        tx.execute(BUMP)
    assert value(c) == 4
    assert registry.cleanup(admin, 3) >= 1                      # rows older than the previous epoch
    assert c.fetchall("SELECT DISTINCT epoch FROM _cdc.connections ORDER BY 1") == [(2,), (3,)]
    with lanes[3].transaction() as tx:
        with pytest.raises(md.LaneMisuse):
            registry.interrupt_earlier(lanes[3].conn, 3)


def test_a_replaced_connection_is_interrupted_by_id_before_the_new_one_is_used(led, faults, monkeypatch):
    c = counter(led)
    calls = []
    real = mdc._interrupt_local
    monkeypatch.setattr(mdc, "_interrupt_local", lambda sid: calls.append(str(sid)) or real(sid))
    lane = led.lane("stream", led.epoch)
    with lane.transaction() as tx:
        tx.execute(BUMP)
    first = lane.conn.server_id
    faults.block_at("md.execute", skip=1, hits=1)               # BEGIN passes, the statement blocks
    t0 = time.monotonic()
    with pytest.raises(md.DeadlineExceeded):
        with lane.transaction() as tx:
            tx.execute(BUMP, deadline=Deadline.after(0.15))
    assert time.monotonic() - t0 < 0.15 + GRACE + 1.0
    assert not lane.conn.usable and calls == []
    faults.clear()
    with lane.transaction() as tx:
        tx.execute(BUMP)
    assert calls == [first]                                     # the old id, once, before the new connection worked
    assert lane.conn.server_id != first and lane.conn.generation == 2
    assert value(c) == 2
    rows = c.fetchall("SELECT connection_id::VARCHAR FROM _cdc.connections WHERE lane = 'stream' ORDER BY registered_at")
    assert [r[0] for r in rows] == [first, lane.conn.server_id]     # the new connection registered itself


# --------------------------------------------------------------------------------------------- deadlines
@pytest.mark.parametrize("site, skip, error, lands", [
    ("md.execute", 1, md.DeadlineExceeded, False),      # a statement in the block
    ("md.execute", 0, md.DeadlineExceeded, False),      # BEGIN
    ("md.fence", 0, md.DeadlineExceeded, False),        # the fence statement: COMMIT was not sent
    ("md.commit", 0, md.CommitUnknown, False),          # COMMIT sent, no answer: unknown (here: it never arrived)
    ("md.commit_reply", 0, md.CommitUnknown, True),     # COMMIT executed, the answer never came: unknown (it landed)
])
def test_a_blocked_step_never_holds_the_caller_beyond_deadline_plus_grace(led, faults, site, skip, error, lands):
    c = counter(led)
    lane = led.lane("stream", led.epoch)
    with lane.transaction() as tx:                              # registered; skip counts from here
        tx.execute("SELECT 1")
    open_before = md.open_connections()
    faults.block_at(site, skip=skip, hits=1)
    t0 = time.monotonic()
    with pytest.raises(error):
        with lane.transaction(Deadline.after(0.2)) as tx:
            tx.execute(BUMP)
    took = time.monotonic() - t0
    assert 0.2 <= took < 0.2 + GRACE + 1.0, took
    assert faults.fired(site=site)
    assert not lane.conn.usable                                 # the connection was given up
    faults.clear()                                              # the blocked step may run on now
    assert md.wait_idle(Deadline.after(5.0))                    # and its connection is closed in the background
    assert md.open_connections() == open_before - 1
    assert value(c) == (1 if lands else 0)
    with lane.transaction() as tx:                              # the lane goes on with a new connection
        tx.execute(BUMP)
    assert value(c) == (2 if lands else 1)
    assert lane.conn.generation == 2


def test_an_expired_deadline_stops_the_block_before_commit(led):
    c = counter(led)
    lane = led.lane("stream", led.epoch)
    with pytest.raises(md.DeadlineExceeded):
        with lane.transaction(Deadline.after(0.05)) as tx:
            tx.execute(BUMP)
            time.sleep(0.08)
    assert value(c) == 0
    with pytest.raises(md.DeadlineExceeded):
        with lane.transaction(Deadline.after(0.05)) as tx:
            time.sleep(0.08)
            tx.execute(BUMP)                                    # not started: its deadline has passed
    assert value(c) == 0 and lane.conn.generation == 1          # nothing was abandoned for that


def test_a_long_query_is_interrupted_at_its_deadline(led):
    lane = led.lane("stream", led.epoch)
    t0 = time.monotonic()
    with pytest.raises(md.DeadlineExceeded):
        with lane.transaction() as tx:
            tx.fetchall("SELECT count(*) FROM range(100000000000) r(i) WHERE i % 7 = 3",
                        deadline=Deadline.after(0.1))
    assert time.monotonic() - t0 < 0.1 + GRACE + 1.0
    conn = led.connect("auto")
    t0 = time.monotonic()
    with pytest.raises(md.DeadlineExceeded):
        conn.fetchall("SELECT count(*) FROM range(100000000000) r(i) WHERE i % 7 = 3", None, Deadline.after(0.1))
    assert time.monotonic() - t0 < 0.1 + GRACE + 1.0


def test_a_roll_back_that_hangs_gives_the_connection_up(dest, faults):
    led = dest
    led.settings = td.local_settings(name=led.settings.database.split(":", 2)[2], rollback_deadline_s=0.15)
    epoch = led.lead()
    c = counter(led)
    lane = led.lane("stream", epoch)
    with lane.transaction() as tx:
        tx.execute(BUMP)
    faults.block_at("md.execute", skip=2, hits=1)               # BEGIN, statement, then ROLLBACK blocks
    t0 = time.monotonic()
    with pytest.raises(KeyError):
        with lane.transaction() as tx:
            tx.execute(BUMP)
            raise KeyError("boom")
    assert time.monotonic() - t0 < 0.15 + GRACE + 1.0
    assert not lane.conn.usable
    faults.clear()
    with lane.transaction() as tx:
        tx.execute(BUMP)
    assert value(c) == 2 and lane.conn.generation == 2


def test_connect_has_a_deadline(dest, faults):
    faults.block_at("md.connect")
    t0 = time.monotonic()
    with pytest.raises(md.DeadlineExceeded):
        md.connect("slow", dest.settings, Deadline.after(0.15))
    assert time.monotonic() - t0 < 0.15 + GRACE + 1.0
    faults.clear()
    faults.raise_at("md.connect", "ConnectionError", hits=1)    # one failed attempt is tried again
    conn = md.connect("retry", dest.settings)
    assert conn.usable and conn.generation == 1
    conn.close()
    faults.raise_at("md.connect", "ConnectionError")
    with pytest.raises(md.ConnectionLost):
        md.connect("down", dest.settings)
    assert len(faults.fired(site="md.connect", action="raise")) == 1 + dest.settings.connect_attempts


# --------------------------------------------------------------------------------------------- lost connections
@pytest.mark.parametrize("site, skip, error, lands", [
    ("md.execute", 1, md.ConnectionLost, False),        # during a statement
    ("md.fence", 0, md.ConnectionLost, False),          # before COMMIT
    ("md.commit", 0, md.CommitUnknown, False),          # during COMMIT, it never arrived
    ("md.commit_reply", 0, md.CommitUnknown, True),     # during COMMIT, it was executed
])
def test_a_lost_connection_is_reported_by_where_it_happened(led, faults, site, skip, error, lands):
    c = counter(led)
    lane = led.lane("stream", led.epoch)
    faults.raise_at(site, "ConnectionError", message="injected: connection reset", skip=skip, hits=1)
    with pytest.raises(error):
        with lane.transaction() as tx:
            tx.execute(BUMP)
    assert value(c) == (1 if lands else 0)
    assert not lane.conn.usable
    with lane.transaction() as tx:
        tx.execute(BUMP)
    assert value(c) == (2 if lands else 1) and lane.conn.generation == 2


def test_an_unclassified_error_after_commit_was_sent_is_commit_unknown(led, faults):
    c = counter(led)
    lane = led.lane("stream", led.epoch)
    faults.raise_at("md.commit", hits=1)                        # FaultInjected: nothing this layer knows
    with pytest.raises(md.CommitUnknown):
        with lane.transaction() as tx:
            tx.execute(BUMP)
    faults.raise_at("md.execute", skip=1, hits=1)               # the same error in a statement is passed on unchanged
    with pytest.raises(Exception) as info:
        with lane.transaction() as tx:
            tx.execute(BUMP)
    assert type(info.value).__name__ == "FaultInjected"
    assert value(c) == 0


def test_abort_from_another_thread_ends_the_call_at_once(led, faults):
    """A supervisor that restarts a component closes its connection from outside (`Connection.abort`)."""
    c = counter(led)
    lane = led.lane("stream", led.epoch)
    faults.block_at("md.execute", skip=1, hits=1)
    timer = threading.Timer(0.1, lane.conn.abort)
    timer.start()
    t0 = time.monotonic()
    with pytest.raises(md.ConnectionLost):
        with lane.transaction() as tx:
            tx.execute(BUMP)                                    # would wait for its 10 s deadline
    assert time.monotonic() - t0 < 2.0
    timer.join()
    faults.clear()
    with pytest.raises(md.LaneMisuse):                          # an aborted connection is closed for good
        with lane.transaction() as tx:
            tx.execute(BUMP)
    assert value(c) == 0


# --------------------------------------------------------------------------------------------- crash points
def test_every_lane_has_its_three_crash_points(led, crash_at):
    """All 54 points are declared, each is reached by a transaction of its lane, and what is committed at each is
    what contract C4 says: nothing at `.fenced` and `.commit_sent`, everything at `.committed`."""
    c = counter(led)
    declared = {p for lane in md.LANES for p in md.crash_points(lane)}
    assert len(declared) == 54
    assert all(crashpoints.declared(p) for p in declared)
    expected = 0
    for name in md.LANES:
        lane = led.lane(name, led.epoch)
        fenced, commit_sent, committed = md.crash_points(name)
        assert (fenced, commit_sent, committed) == (name + ".fenced", name + ".commit_sent", name + ".committed")
        for point in (fenced, commit_sent, committed):
            crash_at(point)
            with pytest.raises(CrashPointHit) as info:
                with lane.transaction() as tx:
                    tx.execute(BUMP)
            assert info.value.point == point
            expected += 1 if point == committed else 0
            assert value(c) == expected, point
        crashpoints.reconfigure({})
        with lane.transaction() as tx:                          # the lane works after each
            tx.execute(BUMP)
        expected += 1
        lane.conn.close()
    assert value(c) == expected == 36


def test_no_connection_and_no_thread_is_left(no_leftovers):
    before = threading.active_count()
    with td.local_destination() as d:
        epoch = d.lead()
        lanes = [d.lane(n, epoch) for n in ("stream", "control", "backfill.0")]
        for lane in lanes:
            with lane.transaction() as tx:
                tx.execute("CREATE TABLE IF NOT EXISTS x (i INT)")
        assert md.open_connections() == 3
    assert td.wait_no_leftovers(5.0)
    assert md.open_connections() == 0 and not md.background_threads()
    assert threading.active_count() == before
