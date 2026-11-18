"""Tier T0: the leader package inside the pipeline, on a local DuckDB destination in this process: settings and
run context, the heartbeat and its tick, the one reaction to `FenceLost`, the hand-over clock and the release,
the leader's first transaction, the contenders' notices."""
import json
import os
import threading
import time
import uuid

import pytest

from cdc_v2 import alerts, crashpoints, faultsites, leader, md
from cdc_v2.deadline import Deadline
from cdc_v2.leader import handover, stop
from cdc_v2.testing import destination as td
from cdc_v2.testing.faults import FaultControl

from leader_standin import StandInSupervisor

pytestmark = pytest.mark.timeout(60)

FAST = leader.LeaderSettings(heartbeat_interval_s=0.2, stale_age_s=0.9, hard_exit_age_s=2.0, late_commit_wait_s=0.2,
                             max_runtime_s=30.0, minute_s=2.0, release_before_boundary_s=0.5, finish_budget_s=0.5,
                             contender_check_s=0.2)


def ctx_for(epoch, tick_fd=None, takeover=None, **kw):
    return leader.RunContext(epoch=epoch, run_id=kw.pop("run_id", str(uuid.uuid4())), flight_id="f-local",
                             run_number=kw.pop("run_number", 1), tick_fd=tick_fd, takeover=takeover or {},
                             settings=kw.pop("settings", FAST), **kw)


@pytest.fixture
def led():
    with td.local_destination() as dest:
        dest.epoch = dest.lead()
        yield dest


# ---------------------------------------------------------------------------------------------- settings
def test_the_run_context_comes_from_the_bootstraps_environment():
    env = {"CDC_V2_EPOCH": "7", "CDC_V2_RUN_ID": "abc", "CDC_V2_FLIGHT_ID": "f", "CDC_V2_RUN_NUMBER": "12",
           "CDC_V2_TICK_FD": "5", "CDC_V2_TAKEOVER": json.dumps({"reason": "stale", "previous_epoch": 6}),
           "CDC_V2_RUN_ELAPSED_S": "4.5", "CDC_V2_HEARTBEAT_INTERVAL_S": "1", "CDC_V2_STALE_AGE_S": "3",
           "CDC_V2_HARD_EXIT_AGE_S": "6"}
    ctx = leader.RunContext.from_env(env)
    assert (ctx.epoch, ctx.run_id, ctx.flight_id, ctx.run_number, ctx.tick_fd) == (7, "abc", "f", 12, 5)
    assert ctx.takeover["reason"] == "stale" and ctx.run_elapsed_s == 4.5
    assert (ctx.settings.heartbeat_interval_s, ctx.settings.stale_age_s) == (1.0, 3.0)
    assert ctx.run_age_s() >= 4.5
    with pytest.raises(ValueError):
        leader.RunContext.from_env({"CDC_V2_RUN_ID": "abc"})
    with pytest.raises(ValueError):
        leader.RunContext.from_env(dict(env, CDC_V2_EPOCH="0"))


def test_settings_refuse_what_is_not_a_number_of_seconds():
    for bad in ({"heartbeat_interval_s": 0}, {"stale_age_s": float("nan")}, {"waiting_calls_bound": 0},
                {"minute_s": 1.0, "release_before_boundary_s": 2.0}, {"max_runtime_s": True}):
        with pytest.raises(ValueError):
            leader.LeaderSettings(**bad)
    assert leader.LeaderSettings().beat_timeout_s == 15.0
    s = leader.LeaderSettings.from_env(FAST.to_env())
    assert s == FAST


# ---------------------------------------------------------------------------------------------- heartbeat
def test_a_beat_registers_writes_the_row_and_ticks(led):
    r, w = os.pipe()
    ctx = ctx_for(led.epoch, tick_fd=w)
    sup = StandInSupervisor(FAST, deadline_s=5.0)
    sup.report("stream", "streaming")
    hb = leader.Heartbeat(ctx, sup, md_settings=led.settings)
    try:
        assert hb.beat_once() == "beat"
        assert os.read(r, 10) == b"t"
        row = led.run("SELECT epoch, phase, beat_at IS NOT NULL, progress_at <= beat_at FROM _cdc.heartbeat")[0]
        assert row == (led.epoch, "streaming", True, True)
        regs = led.run("SELECT epoch, lane FROM _cdc.connections WHERE connection_id = ?::UUID", [hb.conn.server_id])
        assert regs == [(led.epoch, "control")]
        assert hb.beat_once() == "beat"                              # registered once per client connection
        assert len(led.run("SELECT * FROM _cdc.connections WHERE lane = 'control'")) == 1
        hb.conn._drop_session()                                      # the client connection is replaced
        assert hb.beat_once() == "beat"
        assert len(led.run("SELECT * FROM _cdc.connections WHERE lane = 'control'")) == 2
    finally:
        hb.stop()
        os.close(r)
        os.close(w)


def test_no_progress_no_beat(led):
    ctx = ctx_for(led.epoch)
    sup = StandInSupervisor(FAST, deadline_s=0.1)
    sup.report("stream")
    hb = leader.Heartbeat(ctx, sup, md_settings=led.settings)
    try:
        time.sleep(0.2)
        assert hb.beat_once() == "no_progress"
        assert hb.conn is None and led.run("SELECT beat_at FROM _cdc.heartbeat")[0] == (
            led.run("SELECT beat_at FROM _cdc.heartbeat")[0])
        with sup.waiting("stream", "a commit in flight", Deadline.after(5.0)):
            assert hb.beat_once() == "beat"                          # a declared wait counts as progress
        sup.halted = True
        assert hb.beat_once() == "no_progress"
    finally:
        hb.stop()


def test_a_beat_after_a_take_over_is_the_end_of_the_run(led):
    lost = []
    ctx = ctx_for(led.epoch)
    sup = StandInSupervisor(FAST)
    sup.report("stream")
    hb = leader.Heartbeat(ctx, sup, md_settings=led.settings, on_lost=lost.append)
    try:
        assert hb.beat_once() == "beat"
        led.take_over(run_id="next")
        assert hb.beat_once() == "lost" and lost and hb.lost
    finally:
        hb.stop()


def test_a_beat_that_meets_a_take_over_in_progress_is_tried_again_and_not_ticked(led):
    r, w = os.pipe()
    ctx = ctx_for(led.epoch, tick_fd=w)
    sup = StandInSupervisor(FAST)
    sup.report("stream")
    hb = leader.Heartbeat(ctx, sup, md_settings=led.settings)
    raw = led.raw()
    try:
        assert hb.beat_once() == "beat"
        os.read(r, 10)
        raw.execute("BEGIN")
        raw.execute("UPDATE _cdc.heartbeat SET epoch = epoch + 1, beat_at = uuid_extract_timestamp(uuidv7())")
        assert hb.beat_once() == "retry"
        with pytest.raises(BlockingIOError):
            os.set_blocking(r, False)
            os.read(r, 10)
        raw.execute("ROLLBACK")
        assert hb.beat_once() == "beat"
    finally:
        raw.close()
        hb.stop()
        os.close(r)
        os.close(w)


def test_the_heartbeat_thread_beats_every_interval_and_stops(led):
    r, w = os.pipe()
    os.set_blocking(r, False)
    sup = StandInSupervisor(FAST, deadline_s=5.0)
    sup.report("stream")
    hb = leader.Heartbeat(ctx_for(led.epoch, tick_fd=w), sup, md_settings=led.settings).start()
    time.sleep(1.0)
    hb.stop()
    ticks = len(os.read(r, 1000))
    assert 3 <= ticks <= 7 and hb.beats == ticks
    os.close(r)
    os.close(w)


def test_the_ticker_drops_on_a_full_pipe_and_ends_the_run_without_a_reader():
    r, w = os.pipe()
    gone = []
    t = leader.Ticker(w, on_gone=lambda: gone.append(1))
    n = 0
    while t.dropped == 0 and n < 1_000_000:
        t.tick()
        n += 1
    assert t.dropped == 1 and t.sent > 0 and not gone
    os.close(r)
    t.tick()
    assert gone == [1]
    os.close(w)
    assert leader.Ticker(None).tick() is False


# ---------------------------------------------------------------------------------------------- stop
def test_the_reaction_to_fence_lost_aborts_every_connection_and_exits_10(led, monkeypatch):
    codes = []
    monkeypatch.setattr(stop, "_exit", codes.append)
    stop._reset_for_tests()
    conn = led.connect("x")
    lane = md.Lane(conn, "stream", led.epoch)
    with lane.transaction() as tx:
        tx.execute("CREATE TABLE IF NOT EXISTS t (v BIGINT)")
    assert md.open_connections() >= 1
    crashpoints.reconfigure(crashpoints.env_for("leader.fence_lost", action="raise"))
    try:
        with pytest.raises(crashpoints.CrashPointHit):
            leader.on_fence_lost("test")
    finally:
        crashpoints.reconfigure({})
    stop._reset_for_tests()
    leader.on_fence_lost("test")
    assert codes == [10] and stop.ending()[0] == 10
    assert not conn.usable                                         # aborted: the next call takes a new one
    leader.on_fence_lost("a second component")                     # waits for the end; here the exit returned
    assert codes == [10]
    stop._reset_for_tests()


def test_md_abort_all_retires_every_open_client_connection(led):
    a, b = led.connect("a"), led.connect("b")
    assert a.usable and b.usable
    assert md.abort_all() >= 2
    assert not a.usable and not b.usable
    assert a.fetchone("SELECT 1")[0] == 1                          # a new client connection at the next call


# ---------------------------------------------------------------------------------------------- hand-over
@pytest.mark.parametrize("now_s,minute,before,budget,start_in,end_s", [
    (0.0, 60, 5, 10, 45.0, 55.0),
    (44.0, 60, 5, 10, 1.0, 55.0),
    (45.5, 60, 5, 10, 59.5, 115.0),             # too late for this minute: the next one
    (60.0, 60, 5, 10, 45.0, 115.0),             # exactly on a boundary
    (1.0, 2.0, 0.5, 0.5, 0.0, 1.5),
])
def test_the_hand_over_ends_before_a_minute_boundary(now_s, minute, before, budget, start_in, end_s):
    s, end, boundary = handover.schedule(int(now_s * 1e6), minute, before, budget)
    assert abs(s - start_in) < 1e-6 and end == int(end_s * 1e6) and boundary - end == int(before * 1e6)
    assert boundary % int(minute * 1e6) == 0 and end - int(now_s * 1e6) >= int(budget * 1e6)


def test_the_hand_over_clock_counts_from_the_bootstraps_first_line():
    ctx = ctx_for(1, run_elapsed_s=29.5)
    clock = handover.HandoverClock(ctx)
    assert 0 < clock.remaining_s() <= 0.5 and not clock.due()
    time.sleep(0.55)
    assert clock.due()
    assert handover.HandoverClock(ctx_for(1, run_elapsed_s=99.0)).due()


def test_the_release_moves_the_record_and_writes_the_machines_row(led):
    ctx = ctx_for(led.epoch)
    lane = led.lane("control", led.epoch)
    assert handover.release(lane, ctx) == "released"
    assert led.run("SELECT epoch, state FROM _cdc.leader")[0] == (led.epoch, "released")
    n = led.run("SELECT n FROM _cdc.fence WHERE lane = 'control'")[0][0]
    rows = led.run("SELECT from_state, to_state, epoch, lane, seq, cause FROM _cdc.transitions WHERE machine = "
                   "'leader'")
    assert rows == [("leading", "released", led.epoch, "control", n, "planned hand-over")]
    with pytest.raises(md.FenceLost):                              # released -> released is not a transition
        handover.release(lane, ctx)


def test_a_release_after_a_take_over_is_fence_lost(led):
    lane = led.lane("control", led.epoch)
    with lane.transaction():
        pass
    led.take_over(run_id="next")
    with pytest.raises(md.FenceLost):
        handover.release(lane, ctx_for(led.epoch))
    assert led.run("SELECT state FROM _cdc.leader")[0] == ("leading",)


def test_a_release_whose_answer_was_lost_is_found_landed(led, tmp_path):
    fc = FaultControl(tmp_path / "faults")
    faultsites.reconfigure(fc.env())
    try:
        lane = led.lane("control", led.epoch)
        with lane.transaction():
            pass
        fc.raise_at("md.commit_reply", "ConnectionError", hits=1)
        assert handover.release(lane, ctx_for(led.epoch)) == "released"
        assert len(fc.fired(site="md.commit_reply")) == 1
        assert led.run("SELECT state FROM _cdc.leader")[0] == ("released",)
        assert led.run("SELECT count(*) FROM _cdc.transitions WHERE machine = 'leader'")[0] == (1,)
    finally:
        fc.clear()
        faultsites.reconfigure({})


def test_hand_over_finishes_the_group_releases_and_stops_the_heartbeat(led):
    calls = []

    class Stream:
        def finish_current_group(self, deadline):
            calls.append(("finish", round(deadline.remaining(), 1)))

    class Beat:
        def stop(self):
            calls.append(("heartbeat stopped",))

    slept = []
    code = handover.hand_over(ctx_for(led.epoch), led.lane("control", led.epoch), Stream(), Beat(), sleep=slept.append)
    assert code == 0 and [c[0] for c in calls] == ["finish", "heartbeat stopped"]
    assert 0 <= slept[0] <= FAST.minute_s and 0 < calls[0][1] <= FAST.finish_budget_s
    assert led.run("SELECT state FROM _cdc.leader")[0] == ("released",)


# ---------------------------------------------------------------------------------------------- first transaction
def test_the_leaders_first_transaction_records_the_take_over_alerts_and_cleans_up(led):
    for e in (led.epoch - 1, led.epoch - 2, led.epoch - 3):
        led.run("INSERT INTO _cdc.connections VALUES (?, 'stream', ?::UUID, uuid_extract_timestamp(uuidv7()))",
                [e, str(uuid.uuid4())])
    facts = {"previous_epoch": 0, "previous_state": "leading", "previous_run_id": "old-run", "previous_run_number": 41,
             "heartbeat_age_ms": 45210, "reason": "stale", "attempts": 2, "conflicts": 1, "interrupted": 1,
             "cancelled": True, "seconds": 1.25}
    ctx = ctx_for(led.epoch, takeover=facts)
    lane = led.lane("control", led.epoch)
    done = leader.on_leading(lane, ctx)
    assert done == {"row": True, "alert": True, "cleaned": 2}
    row = led.run("SELECT from_state, to_state, epoch, lane FROM _cdc.transitions WHERE machine = 'leader'")
    assert row == [("leading", "leading", led.epoch, "control")]
    alert = led.run("SELECT kind, severity, fields FROM _cdc.alerts")
    assert len(alert) == 1 and alert[0][:2] == ("takeover", "warning")
    fields = json.loads(alert[0][2])
    assert fields["previous_run_number"] == 41 and fields["heartbeat_age_ms"] == 45210 and fields["cancelled"] == 1
    assert leader.on_leading(lane, ctx) == {"row": False, "alert": False, "cleaned": 0}    # once only


@pytest.mark.parametrize("facts", [{"previous_state": "released", "reason": "released", "previous_epoch": 3},
                                   {"previous_state": "released", "reason": "initial", "previous_epoch": 0}])
def test_no_takeover_alert_after_a_release_or_at_the_first_run(led, facts):
    done = leader.on_leading(led.lane("control", led.epoch), ctx_for(led.epoch, takeover=facts))
    assert done["row"] and not done["alert"]
    assert led.run("SELECT count(*) FROM _cdc.alerts")[0] == (0,)


def test_contenders_notices_become_one_alert_per_flight(led):
    for flight, n in (("f-other", 3), ("f-other", 4), ("f-third", 9)):
        led.run("INSERT INTO _cdc.contenders VALUES (uuid_extract_timestamp(uuidv7()), ?, ?, 'r', ?, 'f', 1)",
                [flight, n, led.epoch])
    led.run("INSERT INTO _cdc.contenders VALUES (uuid_extract_timestamp(uuidv7()) - INTERVAL 2 DAY, 'f-old', 1, "
            "'r', 1, 'f', 1)")
    watch = leader.ContenderWatch(ctx_for(led.epoch))
    lane = led.lane("control", led.epoch)
    assert watch.due() and watch.run(lane) == 2 and not watch.due()
    rows = led.run("SELECT kind, fields FROM _cdc.alerts ORDER BY fields")
    assert [json.loads(f)["contending_flight_id"] for _k, f in rows] == ["f-other", "f-third"]
    assert json.loads(rows[0][1])["notices"] == 2 and json.loads(rows[0][1])["last_run_number"] == 4
    assert watch.run(lane) == 0                                     # an open alert per Flight
    assert led.run("SELECT count(*) FROM _cdc.contenders WHERE flight_id = 'f-old'")[0] == (0,)


def test_the_takeover_alert_fields_are_what_an_alert_may_hold():
    ctx = ctx_for(3, takeover={"previous_state": "leading", "previous_run_id": "x" * 200, "reason": "stale",
                               "heartbeat_age_ms": 1, "cancelled": False})
    fields = leader.takeover_alert_fields(ctx)
    assert "previous_run_id" not in fields and fields["cancelled"] == 0
    alerts.check_fields(fields)


def test_the_supervisor_stand_in_ends_the_process_past_the_bound_of_waiting_calls():
    ended = []
    sup = StandInSupervisor(FAST, stop=lambda code, why: ended.append(code), exceeded=lambda s: True)
    sup.progress()
    assert ended == [1]
    assert leader.waiting_calls_exceeded(FAST) == (md.waiting_calls() > FAST.waiting_calls_bound)
