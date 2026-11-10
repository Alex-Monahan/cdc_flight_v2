"""procs: own session and group, signals to a process or its group, deadlines, exit status, survivors."""
import os
import signal
import sys
import time

import pytest

from cdc_v2.testing import procs

# A parent with a child in its group and a grandchild that escapes into its own session.
TREE = r'''
import os, sys, time, subprocess
child = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(600)"])
esc = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(600)"], start_new_session=True)
print(child.pid, esc.pid, flush=True)
time.sleep(600)
'''
SLEEPER = "import time; time.sleep(600)"


def test_own_session_group_and_signals(tmp_path):
    with procs.Fleet() as fleet:
        p = fleet.start([sys.executable, "-c", SLEEPER], name="s")
        assert os.getpgid(p.pid) == p.pid and os.getsid(p.pid) == p.pid
        assert os.getpgid(p.pid) != os.getpgid(0)
        p.stop()
        p.wait_state("T", 5)
        p.cont()
        deadline = time.monotonic() + 5
        while p.ps_state().startswith("T"):
            assert time.monotonic() < deadline
            time.sleep(0.02)
        with pytest.raises(procs.DeadlineExceeded):
            p.wait(0.2)
        p.kill()
        st = p.wait(5)
        assert st.killed and st.code is None and st.as_dict()["signal_name"] == "SIGKILL"
        assert not p.signal(signal.SIGTERM)                  # reaped: never signalled again


def test_a_reaped_and_empty_group_is_never_signalled():
    p = procs.start([sys.executable, "-c", SLEEPER])
    p.kill()
    assert p.wait(5).killed
    assert not p.group_gone
    assert p.signal_group(signal.SIGKILL) is False and p.group_gone      # checked with ps, then marked gone
    assert p.signal_group(signal.SIGKILL) is False


def test_exit_status_of_normal_exit(tmp_path):
    p = procs.start([sys.executable, "-c", "import sys; print('hi'); sys.exit(7)"], log_path=str(tmp_path / "o.log"))
    st = p.wait(10)
    assert st.code == 7 and st.signal is None and "hi" in p.log_text()
    procs.assert_no_survivors([p], 5)


def test_group_stop_and_kill_reach_children_and_escapees_are_tracked(tmp_path):
    log = tmp_path / "tree.log"
    fleet = procs.Fleet()
    p = fleet.start([sys.executable, "-c", TREE], name="tree", log_path=str(log))
    deadline = time.monotonic() + 10
    while not log.read_text().strip():
        assert time.monotonic() < deadline
        time.sleep(0.02)
    child, esc = map(int, log.read_text().split())
    members = p.group_members()
    assert p.pid in members and child in members and esc not in members
    assert set(p.survivors()) == {p.pid, child, esc}          # the escapee is found through its parent
    p.stop_group()
    p.wait_state("T", 5)
    assert procs.process_table()[child][2].startswith("T")
    p.cont_group()
    statuses = fleet.kill_all(10)                            # kills group and escapee, then checks survivors
    assert statuses["tree"].killed
    table = procs.process_table()
    assert child not in table and esc not in table


def test_survivor_check_fails_while_something_lives():
    p = procs.start([sys.executable, "-c", SLEEPER])
    try:
        with pytest.raises(procs.SurvivorError):
            procs.assert_no_survivors([p], 0.3)
    finally:
        p.kill_and_wait(5)
    procs.assert_no_survivors([p], 5)


def test_three_competing_instances():
    with procs.Fleet() as fleet:
        ps = [fleet.start([sys.executable, "-c", SLEEPER], name="i%d" % i) for i in range(3)]
        assert len({p.pgid for p in ps}) == 3
        ps[1].stop_group()
        ps[1].wait_state("T", 5)
        ps[0].kill_group()
        assert ps[0].wait(5).killed
        assert [p.alive() for p in ps] == [False, True, True]
    assert all(not p.alive() for p in ps)


def test_harness_variables_are_not_inherited(monkeypatch, tmp_path):
    monkeypatch.setenv("CDC_V2_CRASH_POINT", "x.y")
    monkeypatch.setenv("CDC_V2_FAULT_FILE", "/nonexistent")
    env = procs.child_env({"CDC_V2_CRASH_TAG": "a"})
    assert "CDC_V2_CRASH_POINT" not in env and "CDC_V2_FAULT_FILE" not in env and env["CDC_V2_CRASH_TAG"] == "a"
