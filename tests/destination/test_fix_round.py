"""Tier T0 (local DuckDB): seeded regression tests of the fix round of package D (SPEC section 4).

Items 1 to 7 come from the coordinator's review, 8 to 16 from the second reviewer's programs (their cases are
copied into `review_cases.py`). Each test names its item and fails on the code of the delivery that was
reviewed (`9e389cc`). What was done for each item is in `docs/design/destination.md`, section 14.3. The
MotherDuck forms of these tests are in `test_md_again.py` (the same functions) and in `test_md_fix_round.py`.
"""
import threading
import time

import pytest

import fence_campaign
import review_cases
from cdc_v2 import alerts, md, state
from cdc_v2.deadline import Deadline
from cdc_v2.md import connection as mdc
from cdc_v2.md import lane as mdl
from cdc_v2.md import registry, schema
from cdc_v2.testing import destination as td
from cdc_v2.testing import machines as tm

BUMP = "UPDATE t SET v = v + 1 WHERE k = 1"
LOG = ("SELECT machine, key, from_state, to_state, cause, epoch, lane, seq, detail FROM _cdc.transitions "
       "WHERE machine = ? ORDER BY seq, from_state NULLS FIRST")


def counter(dest):
    c = dest.connect("look")
    c.execute("CREATE TABLE t (k INTEGER, v BIGINT)")
    c.execute("INSERT INTO t VALUES (1, 0)")
    return c


def value(c):
    return c.fetchone("SELECT v FROM t WHERE k = 1")[0]


def fence_n(c, lane):
    return int(c.fetchone("SELECT n FROM _cdc.fence WHERE lane = ?", [lane])[0])


@pytest.fixture
def ex(led):
    """A leading destination with the two example machines declared and their tables created."""
    lane = led.lane("stream", led.epoch)
    with tm.example_machines(lane) as (door, job):
        led.door, led.job, led.stream, led.look = door, job, lane, led.connect("look")
        yield led


# ------------------------------------------------------------------------------ item 1: the facts of a group
def test_a_transition_row_carries_its_lane_epoch_seq_and_detail(ex):
    """Item 1. Every row of `_cdc.transitions` carries the lane, the epoch, `seq` (the lane's fence counter
    after the transaction committed) and the detail."""
    with ex.stream.transaction() as tx:
        state.create(tx, ex.door, "d1", "closed", "installed", values={"opened": 0}, detail={"by": "test", "n": 1})
    n1 = fence_n(ex.look, "stream")
    with ex.stream.transaction() as tx:
        state.transition(tx, ex.door, "d1", "closed", "open", "someone came", values={"opened": 1},
                         detail={"lsn": 2 ** 64 - 1, "seconds": 0.25})
    n2 = fence_n(ex.look, "stream")
    rows = [tuple(r) for r in ex.look.fetchall(LOG, ["example_door"])]
    assert rows == [
        ("example_door", '["d1"]', None, "closed", "installed", ex.epoch, "stream", n1, '{"by":"test","n":1}'),
        ("example_door", '["d1"]', "closed", "open", "someone came", ex.epoch, "stream", n2,
         '{"lsn":18446744073709551615,"seconds":0.25}')]
    assert n2 == n1 + 1
    assert ex.look.fetchone("SELECT opened FROM _cdc_example.door WHERE door_id = 'd1'")[0] == 1
    for bad in ({"text": "x" * 129}, {"Bad": 1}, {"v": True}, {"v": float("nan")}, {"v": b"bytes"}, [1]):
        with pytest.raises(state.FieldError):                       # checked before the database is touched
            state.transition(object(), ex.door, "d1", "open", "closed", "cause", detail=bad)
        with pytest.raises(state.FieldError):
            state.create(object(), ex.door, "d9", "closed", "cause", detail=bad)


def test_seq_has_no_gaps_and_equals_the_fence_counter_on_two_lanes_and_two_epochs(ex):
    """Item 1. Two lanes, two epochs, one transition per transaction: per lane the values of `seq` are the
    numbers 1 to n without a gap, and the last is the lane's fence counter n (the take-over and a registration
    do not move n)."""
    with ex.stream.transaction() as tx:
        state.create(tx, ex.door, "d1", "closed", "installed")
    control = ex.lane("control", ex.epoch)
    with control.transaction() as tx:
        state.create(tx, ex.job, (1, 1), "queued", "new")
    epochs = [ex.epoch]
    lanes = {"stream": ex.stream, "control": control}
    for round_ in range(2):
        for _ in range(3):
            with lanes["stream"].transaction() as tx:
                stored = state.read(tx, ex.door, "d1")
                state.transition(tx, ex.door, "d1", stored, "open" if stored == "closed" else "closed", "flip")
            with lanes["control"].transaction() as tx:
                state.transition(tx, ex.job, (1, 1), state.read(tx, ex.job, (1, 1)), "running", "work")
                state.transition(tx, ex.job, (1, 1), "running", "failed", "work failed")
                state.transition(tx, ex.job, (1, 1), "failed", "queued", "again")
        if round_ == 0:
            epochs.append(ex.take_over())
            lanes = {name: ex.lane(name, epochs[-1]) for name in ("stream", "control")}
    rows = ex.look.fetchall("SELECT lane, epoch, seq FROM _cdc.transitions ORDER BY lane, seq")
    for lane in ("stream", "control"):
        seqs = sorted({int(s) for (ln, e, s) in rows if ln == lane})
        n = fence_n(ex.look, lane)
        assert seqs == list(range(seqs[0], n + 1)) and seqs[-1] == n, (lane, seqs, n)
        assert {int(e) for (ln, e, s) in rows if ln == lane} >= set(epochs)
    # one transaction of the control lane wrote three rows: they share one seq
    per_seq = {}
    for ln, e, s in rows:
        per_seq.setdefault((ln, s), set()).add(e)
    assert all(len(es) == 1 for es in per_seq.values())               # one epoch per (lane, seq): one writer


def test_a_rolled_back_transaction_leaves_no_transition_row(ex):
    """Item 1. A transaction that rolls back, by the caller's error, by a refused statement, or at the fence,
    leaves no row in `_cdc.transitions` and does not move the fence counter."""
    with ex.stream.transaction() as tx:
        state.create(tx, ex.door, "d1", "closed", "installed")
    before = ex.look.fetchone("SELECT count(*) FROM _cdc.transitions")[0]
    n = fence_n(ex.look, "stream")
    with pytest.raises(RuntimeError):
        with ex.stream.transaction() as tx:
            state.transition(tx, ex.door, "d1", "closed", "open", "then the caller fails")
            raise RuntimeError("boom")
    with pytest.raises(md.StatementError):
        with ex.stream.transaction() as tx:
            state.transition(tx, ex.door, "d1", "closed", "open", "then a statement fails")
            tx.execute("INSERT INTO _cdc_example.job VALUES ('not a number', 1, 'queued')")
    twin = ex.lane("stream", ex.epoch)
    with pytest.raises(md.Conflict):
        with ex.stream.transaction() as tx1:
            state.transition(tx1, ex.door, "d1", "closed", "locked", "first")
            with twin.transaction() as tx2:
                state.create(tx2, ex.door, "d2", "closed", "second, refused at the fence row")
    assert ex.look.fetchone("SELECT count(*) FROM _cdc.transitions")[0] == before
    assert fence_n(ex.look, "stream") == n
    assert state.read(ex.look, ex.door, "d1") == "closed"


def test_a_run_that_bypasses_the_fence_shows_a_duplicate_seq(ex, monkeypatch):
    """Item 1. Two transactions of one lane read the same fence counter. With the fence the second cannot
    commit. A lane whose fence statement is left out on purpose commits both, and the duplicate (lane, seq)
    shows the second writer."""
    with ex.stream.transaction() as tx:
        state.create(tx, ex.door, "a", "closed", "installed")
        state.create(tx, ex.door, "b", "closed", "installed")
    twin = ex.lane("stream", ex.epoch)
    with pytest.raises(md.Conflict):                                    # with the fence
        with ex.stream.transaction() as tx1:
            state.transition(tx1, ex.door, "a", "closed", "open", "first")
            with twin.transaction() as tx2:
                state.transition(tx2, ex.door, "b", "closed", "open", "second")
    dup = "SELECT lane, seq, count(*) FROM _cdc.transitions GROUP BY lane, seq HAVING count(*) > 1"
    assert ex.look.fetchall(dup) == [("stream", fence_n(ex.look, "stream"), 2)]   # the two creates: one transaction

    def no_fence(self):                                                # the fence statement left out on purpose
        out = self._sess.wait(self._sess.submit(self._sess.commit()), self._deadline)
        assert out.ok
    monkeypatch.setattr(mdl.Transaction, "_finish", no_fence)
    n = fence_n(ex.look, "stream")
    with ex.stream.transaction() as tx1:
        state.transition(tx1, ex.door, "a", "closed", "open", "first, no fence")
        with twin.transaction() as tx2:
            state.transition(tx2, ex.door, "b", "closed", "open", "second, no fence")
    monkeypatch.undo()
    rows = ex.look.fetchall("SELECT cause, lane, seq FROM _cdc.transitions WHERE cause LIKE '%no fence' ORDER BY 1")
    assert rows == [("first, no fence", "stream", n + 1), ("second, no fence", "stream", n + 1)]
    assert fence_n(ex.look, "stream") == n                              # and the counter did not move


def test_values_and_expect_in_the_same_conditional_update(ex):
    """Item 1. `values` sets more columns in the same UPDATE; `expect` adds a condition and, when the row no
    longer holds what the transaction read, rolls the transaction back with `Conflict` (the run-again rule). The
    UPDATE also writes the expected columns, so a concurrent writer of them conflicts (conflicts are per
    column)."""
    with ex.stream.transaction() as tx:
        state.create(tx, ex.door, "d1", "closed", "installed", values={"opened": 0})
    with ex.stream.transaction() as tx:
        state.transition(tx, ex.door, "d1", "closed", "open", "first", values={"opened": 1}, expect={"opened": 0})
    assert ex.look.fetchall("SELECT state, opened FROM _cdc_example.door") == [("open", 1)]
    with pytest.raises(md.Conflict, match="opened"):
        with ex.stream.transaction() as tx:
            tx.execute("INSERT INTO _cdc_example.job VALUES (5, 5, 'queued')")
            state.transition(tx, ex.door, "d1", "open", "closed", "stale read", values={"opened": 2},
                             expect={"opened": 0})
    assert ex.look.fetchall("SELECT state, opened FROM _cdc_example.door") == [("open", 1)]
    assert ex.look.fetchone("SELECT count(*) FROM _cdc_example.job")[0] == 0          # the transaction failed
    with ex.stream.transaction() as tx:                       # NULL is expected with IS NOT DISTINCT FROM
        tx.execute("UPDATE _cdc_example.door SET opened = NULL WHERE door_id = 'd1'")
    with ex.stream.transaction() as tx:
        state.transition(tx, ex.door, "d1", "open", "closed", "null expected", expect={"opened": None})
    # a writer of an expected column conflicts with the transition, also when the transition does not change it
    control = ex.lane("control", ex.epoch)
    with pytest.raises(md.Conflict):
        with ex.stream.transaction() as tx1:
            state.transition(tx1, ex.door, "d1", "closed", "open", "keeps opened", expect={"opened": None})
            with control.transaction() as tx2:
                tx2.execute("UPDATE _cdc_example.door SET opened = 7 WHERE door_id = 'd1'")
    assert ex.look.fetchall("SELECT state, opened FROM _cdc_example.door") == [("closed", None)]
    for bad in ({"door_id": 1}, {"state": "x"}, {"bad col": 1}):
        with pytest.raises(state.StateError):
            state.transition(object(), ex.door, "d1", "closed", "open", "cause", expect=bad)
        with pytest.raises(state.StateError):
            state.transition(object(), ex.door, "d1", "closed", "open", "cause", values=bad)


# ------------------------------------------------------------------------------ item 2: registration
def test_a_registration_while_a_take_over_is_open_does_not_commit(led):
    """Item 2. The registration read `_cdc.leader` and wrote only `_cdc.connections`, so it committed next to an
    open take-over and, on MotherDuck, could commit after it. Now it writes the epoch of its lane's fence row,
    which the take-over writes too: while a take-over is open the registration gets `Conflict`, and after it
    `FenceLost`."""
    c = counter(led)
    lane = led.lane("stream", led.epoch)
    raw = led.raw()
    try:
        raw.execute("BEGIN")
        raw.execute(td.TAKEOVER_LEADER_SQL, ["f", 1, "newcomer", led.epoch])
        raw.execute(td.TAKEOVER_FENCE_SQL, [led.epoch])
        raw.execute(td.TAKEOVER_HEARTBEAT_SQL, [led.epoch])
        with pytest.raises(md.Conflict, match="registration"):
            with lane.transaction() as tx:
                tx.execute(BUMP)
        raw.execute("COMMIT")
    finally:
        raw.close()
    with pytest.raises(md.FenceLost):
        with lane.transaction() as tx:
            tx.execute(BUMP)
    assert value(c) == 0
    assert c.fetchone("SELECT count(*) FROM _cdc.connections WHERE lane = 'stream'")[0] == 0


def test_a_registration_frozen_between_its_check_and_its_commit_and_a_take_over_in_between(led, faults):
    """Item 2. A registration that has run its statements and waits before its COMMIT holds the fence row of its
    lane: a take-over cannot commit meanwhile (it meets a conflict and finds nothing to interrupt, because the
    row is not committed). When the registration commits, the take-over wins at its next attempt and its
    interrupt of the earlier epochs finds the connection. A registration that is frozen before it writes the
    fence row, with a take-over that commits meanwhile, gets `Conflict`, and the next attempt `FenceLost`."""
    c = counter(led)
    lane = led.lane("stream", led.epoch)
    faults.block_at("md.commit", hits=1)                       # the registration's COMMIT waits
    box = {}

    def first_transaction():
        try:
            with lane.transaction() as tx:
                tx.execute(BUMP)
        except Exception as e:                                  # noqa: BLE001
            box["error"] = e

    t = threading.Thread(target=first_transaction)
    t.start()
    end = time.monotonic() + 5
    while not faults.fired(site="md.commit") and time.monotonic() < end:
        time.sleep(0.01)
    rep = {}
    with pytest.raises(td.TakeoverFailed):
        led.take_over(timeout_s=0.4, pause_s=0.02, report=rep)
    assert rep["conflicts"] >= 1 and rep["interrupted"] == 0
    faults.clear()
    t.join(10)
    assert "error" not in box                                   # registered, and its transaction committed
    assert value(c) == 1
    new = led.take_over(timeout_s=5)
    listed, _ = md.interrupt_earlier(led.connect("leader"), new)
    assert listed >= 1
    assert c.fetchone("SELECT count(*) FROM _cdc.connections WHERE lane = 'stream' AND epoch = ?",
                      [led.epoch])[0] == 1
    # frozen before its fence statement (BEGIN and INSERT ran), and the take-over commits meanwhile
    old = led.lane("backfill.0", new)
    faults.block_at("md.execute", skip=2, hits=1)
    box.clear()

    def other():
        try:
            with old.transaction() as tx:
                tx.execute(BUMP)
        except Exception as e:                                  # noqa: BLE001
            box["error"] = e

    t = threading.Thread(target=other)
    t.start()
    end = time.monotonic() + 5
    while not faults.fired(site="md.execute") and time.monotonic() < end:
        time.sleep(0.01)
    assert led.take_over(timeout_s=5) == new + 1
    faults.clear()
    t.join(10)
    assert isinstance(box.get("error"), md.Conflict), box
    with pytest.raises(md.FenceLost):
        with old.transaction() as tx:
            tx.execute(BUMP)
    assert value(c) == 1
    assert c.fetchone("SELECT count(*) FROM _cdc.connections WHERE lane = 'backfill.0'")[0] == 0


def test_a_connection_that_is_not_a_lane_registers_under_control_and_retries_a_conflict(led):
    """Item 2. The heartbeat connection registers under `control`. Its registration writes the fence row of
    `control`, which a transaction of the control lane holds between its fence statement and its COMMIT; that
    rare `Conflict` it tries again (`retry_conflict=True`)."""
    control = led.lane("control", led.epoch)
    beat = led.connect("heartbeat")
    with fence_campaign.Hooks() as hooks:
        box = {}

        def register_now():
            with pytest.raises(md.Conflict):
                registry.register(beat, "control", led.epoch)
            box["retried"] = threading.Thread(target=lambda: box.setdefault(
                "sid", registry.register(beat, "control", led.epoch, Deadline.after(5), retry_conflict=True)))
            box["retried"].start()
            time.sleep(0.3)
            box["waiting_while_held"] = box["retried"].is_alive()

        hooks.at("control.fenced", register_now)
        with control.transaction() as tx:
            tx.execute("SELECT 1")
    box["retried"].join(5)
    assert box["waiting_while_held"] and box["sid"] == beat.server_id
    assert led.connect("look").fetchone("SELECT count(*) FROM _cdc.connections WHERE connection_id = ?::UUID",
                                        [beat.server_id])[0] == 1


# ------------------------------------------------------------------------------ item 6 and F7: waiting calls
def test_the_calls_that_were_given_up_are_counted_and_capped_per_connection(led, faults):
    """Items 6 and 15 (F7). On a network that answers nothing, every attempt of a lane left a worker thread, a
    reaper thread and a client connection behind, without a bound, and the counters did not count the workers.
    Now `md.waiting_calls()` counts the calls that were given up and still wait, `background_threads()` counts
    the worker of every retired connection, and a connection with `max_waiting_calls` (2) such calls fails the
    next call at once with `ConnectionLost` and starts nothing."""
    c = counter(led)
    lane = led.lane("stream", led.epoch)
    with lane.transaction() as tx:
        tx.execute(BUMP)
    base_threads = threading.active_count()
    faults.block_at("md.execute")                               # every statement waits, as on a black hole
    try:
        for i in range(2):
            t0 = time.monotonic()
            with pytest.raises(md.DeadlineExceeded):
                with lane.transaction(Deadline.after(0.2)) as tx:
                    tx.execute(BUMP)
            assert time.monotonic() - t0 < 0.2 + td.FAST["grace_s"] + 1.0
            assert md.waiting_calls() == i + 1 and lane.conn.waiting_calls() == i + 1
            assert len([t for t in md.background_threads() if t.name.startswith("cdc_v2-md-stream")]) == i + 1
        threads = threading.active_count()
        assert threads <= base_threads + 2 * 3                  # per waiting call: worker, watchdog, reaper
        for _ in range(5):
            t0 = time.monotonic()
            with pytest.raises(md.ConnectionLost, match="still wait"):
                with lane.transaction(Deadline.after(0.2)) as tx:
                    tx.execute(BUMP)
            assert time.monotonic() - t0 < 0.05                 # at once
        assert threading.active_count() <= threads and md.waiting_calls() == 2   # nothing new was started
    finally:
        faults.clear()
    assert md.wait_idle(Deadline.after(5.0)) and md.waiting_calls() == 0
    with lane.transaction() as tx:                              # the lane goes on when the calls have returned
        tx.execute(BUMP)
    assert value(c) == 2


def test_the_fence_statement_and_the_registration_wait_the_statement_deadline_not_the_blocks(faults):
    """F7. The fence statement and the registration waited for what was left of the block's deadline (up to
    300 s by default, above the 120 s of SPEC 4.5). Now each waits at most the statement deadline."""
    with td.local_destination(statement_deadline_s=0.3, transaction_deadline_s=20.0) as dest:
        epoch = dest.lead()
        c = counter(dest)
        lane = dest.lane("stream", epoch)
        with lane.transaction() as tx:
            tx.execute(BUMP)
        faults.block_at("md.fence", hits=1)
        t0 = time.monotonic()
        with pytest.raises(md.DeadlineExceeded):
            with lane.transaction() as tx:                      # the block's deadline is 20 s
                tx.execute(BUMP)
        assert time.monotonic() - t0 < 0.3 + td.FAST["grace_s"] + 1.0
        faults.clear()
        faults.block_at("md.execute", hits=1)                   # the next transaction: a new connection registers
        t0 = time.monotonic()
        with pytest.raises(md.DeadlineExceeded):
            with lane.transaction() as tx:
                tx.execute(BUMP)
        assert time.monotonic() - t0 < 0.3 + td.FAST["grace_s"] + 1.0
        faults.clear()
        assert md.wait_idle(Deadline.after(5.0))
        with lane.transaction() as tx:
            tx.execute(BUMP)
        assert value(c) == 2
        c.close()


# ------------------------------------------------------------------------------ item 8 (F0): take-over at every point
def test_a_take_over_at_every_point_of_a_lane_transaction_keeps_the_fence(dest):
    """Item 8 (F0), the permanent form at T0 (`test_md_fix_round.py` runs it on MotherDuck, and
    `fence_campaign.py` runs it many times). After every round: every fence row and the heartbeat hold the
    leader's epoch, the old block committed only when the take-over came after its COMMIT returned, it reported
    success exactly when it committed, and the old lane's next transaction gets `FenceLost`."""
    fence_campaign.setup(dest)
    probe = dest.connect("probe")
    with fence_campaign.Hooks() as hooks, tm.example_machines() as (door, _job):
        with md.Lane(dest.connect("door"), "stream", td.read_leader(dest.settings)["epoch"]).transaction() as tx:
            for sql in tm.EXAMPLE_TABLES:
                tx.execute(sql)
            state.create(tx, door, fence_campaign.DOOR, "closed", "setup")
        cases = [("nothing", p) for p in fence_campaign.POINTS] + [(s, "fenced") for s in fence_campaign.SHAPES[1:]]
        for i, (shape, point) in enumerate(cases):
            rec = fence_campaign.one_round(dest, probe, hooks, shape, point, i, door)
            assert rec["verdict"] == "pass", rec


# ------------------------------------------------------------------------------ items 9 to 11: the error mapping
def test_ddl_on_one_lane_while_another_lane_writes_is_a_conflict_and_runs_again(led):
    """Item 9 (F1). `Transaction conflict: attempting to insert into/update table ... altered by a different
    transaction` was a `StatementError`, which counts as a failure of the table. It is a `Conflict`: run again,
    it succeeds (after a DROP the table is gone, and that is a `StatementError`)."""
    c = led.connect("setup")
    c.execute("CREATE SCHEMA rv")
    a, b = led.lane("stream", led.epoch), led.lane("backfill.0", led.epoch)
    for name, op_a, op_b, again in review_cases.CONFLICT_SHAPES:
        t = "t_" + name
        for sql in review_cases.fmt(review_cases.SETUP, t):
            c.execute(sql)
        with pytest.raises(md.Conflict):
            with b.transaction() as tb:
                tb.fetchall("SELECT count(*) FROM rv.%s" % t)       # B's snapshot starts here
                with a.transaction() as ta:
                    for sql in review_cases.fmt(op_a, t):
                        ta.execute(sql)
                for sql in review_cases.fmt(op_b, t):
                    tb.execute(sql)
        if again:
            with b.transaction() as tb:
                for sql in review_cases.fmt(op_b, t):
                    tb.execute(sql)
        else:
            with pytest.raises(md.StatementError) as info:
                with b.transaction() as tb:
                    for sql in review_cases.fmt(op_b, t):
                        tb.execute(sql)
            assert info.value.error_class == "CatalogException"
    assert b.conn.generation == 1                               # no connection was given up for any of it


def test_a_missing_file_is_refused_and_not_a_lost_connection(led):
    """Item 11 (F3). An `IOException` counted as a lost connection by its class: a missing file gave
    `ConnectionLost` five times out of five, with a new connection each time, and an autocommit INSERT from it
    `CommitUnknown` with nothing applied. It is a refused statement."""
    c = led.connect("setup")
    c.execute("CREATE TABLE rv_io (a INTEGER)")
    lane = led.lane("backfill.0", led.epoch)
    sql = review_cases.MISSING_FILE.replace("rv.io", "rv_io")
    for _ in range(5):
        with pytest.raises(md.StatementError) as info:
            with lane.transaction() as tx:
                tx.execute(sql)
        assert (info.value.error_class, info.value.statement) == ("IOException", "INSERT")
    assert lane.conn.generation == 1
    with pytest.raises(md.StatementError):
        c.execute(sql)
    with pytest.raises(md.StatementError):
        c.fetchall(review_cases.MISSING_FILE_READ)
    assert c.fetchone("SELECT count(*) FROM rv_io")[0] == 0 and c.generation == 1


def test_the_misleading_statements_through_the_layer_are_refused(led):
    """Item 10 (F2), through a lane and in autocommit: each of the 168 statements of `e4_misleading.py` that
    fails is a `StatementError`; none gives up a connection."""
    c = led.connect("look")
    lane = led.lane("stream", led.epoch)
    refused = 0
    for label, planted, setup, stmt in review_cases.misleading_statements():
        for sql in setup:
            c.execute(sql)
        try:
            with lane.transaction() as tx:
                tx.fetchall(stmt) if not stmt.startswith("INSERT") else tx.execute(stmt)
        except md.StatementError:
            refused += 1
            with pytest.raises(md.StatementError):
                c.fetchall(stmt) if not stmt.startswith("INSERT") else c.execute(stmt)
    assert refused >= 150
    assert lane.conn.generation == 1 and c.generation == 1


# ------------------------------------------------------------------------------ item 12 (F4): a plain connection
def test_the_state_module_and_alerts_write_only_inside_a_lane_transaction(led):
    """Item 12 (F4). After a take-over the old run changed a state and created a row through a plain connection,
    without the fence; the row of `_cdc.transitions` had no epoch and was committed on its own. Now
    `state.transition`, `state.create` and `alerts.emit` take an `md.Transaction` only; anything else is
    `LaneMisuse`, and nothing is written. `state.read` still takes a connection."""
    look = led.connect("look")
    with tm.example_machines(led.lane("stream", led.epoch)) as (door, _job):
        with led.lane("stream", led.epoch).transaction() as tx:
            state.create(tx, door, "d1", "closed", "setup")
        old_conn = led.connect("old")                       # a plain connection of the old run
        led.take_over()
        before = look.fetchone("SELECT count(*) FROM _cdc.transitions")[0]
        with pytest.raises(md.LaneMisuse, match="md.Transaction"):
            state.transition(old_conn, door, "d1", "closed", "open", "old run, plain connection")
        with pytest.raises(md.LaneMisuse, match="md.Transaction"):
            state.create(old_conn, door, "d3", "closed", "old run, plain connection")
        with pytest.raises(md.LaneMisuse, match="md.Transaction"):
            alerts.emit(old_conn, "test_alert", "info", {"n": 1})
        assert state.read(old_conn, door, "d1") == "closed"     # reading is allowed
        assert state.read(look, door, "d3") is None
        assert look.fetchone("SELECT count(*) FROM _cdc.transitions")[0] == before
        assert look.fetchone("SELECT count(*) FROM _cdc.transitions WHERE epoch IS NULL OR lane IS NULL "
                             "OR seq IS NULL")[0] == 0


# ------------------------------------------------------------------------------ item 13 (F5): migrations
def test_a_changed_or_renamed_applied_step_refuses_to_start(led, monkeypatch):
    """Item 13 (F5). Only the last applied step's name was compared: a first step changed under the same name,
    and a renamed first step, were both accepted. Now the name and a hash of every applied step are stored and
    compared; a difference refuses with `SchemaError` and nothing runs."""
    monkeypatch.setattr(schema, "_lists", dict(schema._lists))
    one = schema.Step("one", ["CREATE TABLE _cdc.rv_one (a INTEGER)"])
    two = schema.Step("two", ["CREATE TABLE _cdc.rv_two (a INTEGER)"])
    three = schema.Step("three", ["CREATE TABLE _cdc.rv_three (a INTEGER)"])
    schema.register_migrations("rvpkg", [one, two])
    ctl = led.lane("control", led.epoch)
    assert md.migrate(ctl) == [("rvpkg", 1, "one"), ("rvpkg", 2, "two")]
    look = led.connect("look")
    assert schema.applied_steps(look, "rvpkg") == [("one", schema.step_hash(one)), ("two", schema.step_hash(two))]
    cases = {
        "first step changed, same name": ([schema.Step("one", ["CREATE TABLE _cdc.rv_one (a INTEGER, b VARCHAR)"]),
                                           two, three], "is not the step the database applied"),
        "first step renamed": ([schema.Step("uno", ["CREATE TABLE _cdc.rv_one (a INTEGER)"]), two, three],
                               "renamed, removed or inserted"),
        "a step inserted before the last applied one": (
            [one, schema.Step("half", ["CREATE TABLE _cdc.rv_half (a INTEGER)"]), two], "this code calls it"),
        "the last applied step changed": ([one, schema.Step("two", ["CREATE TABLE _cdc.rv_two (b INTEGER)"]), three],
                                          "is not the step the database applied"),
    }
    for name, (steps, words) in cases.items():
        schema._lists["rvpkg"] = tuple(steps)               # stands for a new version of the code
        with pytest.raises(schema.SchemaError, match=words):
            md.migrate(ctl)
        assert schema.versions(look)["rvpkg"] == (2, "two"), name
        assert look.fetchone("SELECT count(*) FROM information_schema.tables WHERE table_schema = '_cdc' "
                             "AND table_name IN ('rv_three', 'rv_half')")[0] == 0, name
    schema._lists["rvpkg"] = (one, two, three)                  # the same steps and one more at the end
    assert md.migrate(ctl) == [("rvpkg", 3, "three")]
    assert [x[0] for x in schema.applied_steps(look, "rvpkg")] == ["one", "two", "three"]


# ------------------------------------------------------------------------------ item 14 (F6): starters at once
def test_three_simultaneous_starters_have_exactly_one_leader(dest):
    """Item 14 (F6). With three simultaneous starters the stand-in take-over returned all three as leader (5
    rounds of 5): a loser took over again from the winner. Design 4.5: the losers read again and exit. Now a
    loser raises `AnotherRunLeads` when the leader row moved past the epoch it started from under another run's
    id."""
    for rnd in range(5):
        out, barrier = {}, threading.Barrier(3)
        before = td.read_leader(dest.settings)["epoch"]

        def starter(i):
            barrier.wait()
            try:
                out[i] = ("leader", td.take_over(dest.settings, run_id="starter%d-%d" % (rnd, i), timeout_s=10,
                                                 pause_s=0.02))
            except td.AnotherRunLeads:
                out[i] = ("another run leads", None)

        ts = [threading.Thread(target=starter, args=(i,)) for i in range(3)]
        for t in ts:
            t.start()
        for t in ts:
            t.join(30)
        leaders = [i for i, v in out.items() if v[0] == "leader"]
        assert len(out) == 3 and len(leaders) == 1, out
        row = td.read_leader(dest.settings)
        assert row["epoch"] == before + 1 == out[leaders[0]][1]
        assert row["run_id"] == "starter%d-%d" % (rnd, leaders[0])


# ------------------------------------------------------------------------------ item 16 (F8): RETURNING "Count"
def test_a_returning_column_named_count_that_holds_text_is_no_count(led):
    """Item 16 (F8). `INSERT ... RETURNING b AS "Count"` with a text value became `StatementError(ValueError)`
    and was rolled back: the layer read the returned value as a count. A statement with RETURNING is never read
    as a count: `execute` returns None and the row is committed."""
    c = led.connect("look")
    c.execute("CREATE TABLE rv_t (a INTEGER, b VARCHAR)")
    lane = led.lane("stream", led.epoch)
    with lane.transaction() as tx:
        assert tx.execute('INSERT INTO rv_t VALUES (1, ?) RETURNING b AS "Count"', ["some text"]) is None
        assert tx.execute('UPDATE rv_t SET b = b || ? RETURNING b AS "Count"', ["!"]) is None
    assert c.execute('INSERT INTO rv_t VALUES (2, ?) RETURNING b AS "Count"', ["more text"]) is None
    assert c.fetchall("SELECT a, b FROM rv_t ORDER BY a") == [(1, "some text!"), (2, "more text")]
    with pytest.raises(md.LaneMisuse):                         # and execute_one still needs a count
        with lane.transaction() as tx:
            tx.execute_one('UPDATE rv_t SET b = ? WHERE a = 1 RETURNING b AS "Count"', ["x"])
