"""crashpoints: declaration, registry, the environment contract, every action, the hit log, cost when off."""
import glob
import json
import os
import re
import signal
import subprocess
import sys
import time

import pytest

from cdc_v2 import crashpoints as cp
from cdc_v2.testing import procs

REPO = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
SRC = os.path.join(REPO, "src")

cp.declare("harness_test.alpha", "test point alpha")
cp.declare("harness_test.beta", "test point beta")

CHILD = r'''
import sys, time
from cdc_v2 import crashpoints as cp
cp.declare("child.one", "first")
cp.declare("child.two", "second")
for i in range(5):
    cp.crash_point("child.one")
    cp.crash_point("child.two")
print("survived", flush=True)
'''


def run_child(env, timeout=20):
    e = procs.child_env(dict(env, PYTHONPATH=SRC))
    return subprocess.run([sys.executable, "-c", CHILD], env=e, capture_output=True, text=True, timeout=timeout)


def test_splitmix64_reference_vector_and_seeded_hit():
    assert cp.splitmix64(0) == 0xE220A8397B1DCDAF        # the standard first output for state 0
    assert cp.splitmix64(1) == 0x910A2DEC89025CC1
    hits = [cp.hit_from_seed(s, 3) for s in range(200)]
    assert set(hits) == {1, 2, 3}
    assert cp.parse_hit("seed:12345:3") == cp.hit_from_seed(12345, 3)
    assert cp.parse_hit("") == 1 and cp.parse_hit("4") == 4


@pytest.mark.parametrize("bad", ["0", "-1", "x", "seed:1", "seed:1:0", "seed:%d:2" % (1 << 64)])
def test_bad_hit_is_refused(bad):
    with pytest.raises(ValueError):
        cp.parse_hit(bad)


def test_declare_validates_and_registry_lists():
    with pytest.raises(ValueError):
        cp.declare("NoDots", "x")
    with pytest.raises(ValueError):
        cp.declare("a.b", "")
    with pytest.raises(ValueError):
        cp.declare("harness_test.alpha", "a different description")
    assert cp.declare("harness_test.alpha", "test point alpha") == "harness_test.alpha"   # same again: no-op
    names = [p.name for p in cp.registry("harness_test.")]
    assert names == ["harness_test.alpha", "harness_test.beta"]
    assert cp.get("harness_test.alpha").module == __name__


def test_invalid_environment_raises_at_configure():
    for env in ({cp.ENV_POINT: "Bad"}, {cp.ENV_POINT: "a.b", cp.ENV_ACTION: "explode"},
                {cp.ENV_POINT: "a.b", cp.ENV_HIT: "zero"}, {cp.ENV_LOG: "/dev/null", cp.ENV_TAG: "bad tag!"}):
        with pytest.raises(ValueError):
            cp.reconfigure(env)
    cp.reconfigure({})
    assert not cp.active()


def test_raise_action_fires_once_at_the_chosen_hit(tmp_path):
    log = tmp_path / "hits.jsonl"
    try:
        assert cp.reconfigure(cp.env_for("harness_test.beta", hit=2, action="raise", log=str(log), tag="t1"))
        cp.crash_point("harness_test.alpha")
        cp.crash_point("harness_test.beta")
        with pytest.raises(cp.CrashPointHit) as ei:
            cp.crash_point("harness_test.beta")
        assert (ei.value.point, ei.value.hit) == ("harness_test.beta", 2)
        assert not isinstance(ei.value, Exception)            # `except Exception` must not swallow a crash
        cp.crash_point("harness_test.beta")                    # fires only once
        with pytest.raises(cp.UndeclaredCrashPoint):
            cp.crash_point("harness_test.nope")
    finally:
        cp.reconfigure({})
    lines = cp.read_log(str(log))
    assert [(h["point"], h["hit"], h["fire"]) for h in lines] == [
        ("harness_test.alpha", 1, False), ("harness_test.beta", 1, False), ("harness_test.beta", 2, True),
        ("harness_test.beta", 3, False)]
    assert lines[2]["action"] == "raise" and all(h["tag"] == "t1" and h["pid"] == os.getpid() for h in lines)
    assert list(lines[0].keys()) == ["point", "hit", "pid", "tag", "t", "fire", "action"]


def test_kill_action_kills_the_process_at_the_point(tmp_path):
    log = str(tmp_path / "hits.jsonl")
    p = run_child(cp.env_for("child.two", hit=3, log=log, tag="k"))
    assert p.returncode == -signal.SIGKILL and "survived" not in p.stdout
    lines = cp.read_log(log)
    assert lines[-1] == {**lines[-1], "point": "child.two", "hit": 3, "fire": True, "action": "kill"}
    assert [h["point"] for h in lines] == ["child.one", "child.two"] * 3      # order of reaching the points


KILL_LATER_CHILD = r'''
import time
from cdc_v2 import crashpoints as cp
cp.declare("child.commit_sent", "before a blocking call")
cp.crash_point("child.commit_sent")
print("after the point", flush=True)       # the caller carries on into the blocking call
time.sleep(5)
print("blocking call returned", flush=True)
'''


def test_kill_later_kills_while_the_next_call_is_in_flight(tmp_path):
    log = str(tmp_path / "hits.jsonl")
    env = procs.child_env(dict(cp.env_for("child.commit_sent", action="kill_later", log=log, delay_ms=300),
                               PYTHONPATH=SRC))
    t = time.monotonic()
    p = subprocess.run([sys.executable, "-c", KILL_LATER_CHILD], env=env, capture_output=True, text=True, timeout=20)
    elapsed = time.monotonic() - t
    assert p.returncode == -signal.SIGKILL
    assert "after the point" in p.stdout and "blocking call returned" not in p.stdout
    assert 0.3 <= elapsed < 4
    assert cp.read_log(log)[-1]["action"] == "kill_later"
    with pytest.raises(ValueError):
        cp.reconfigure({cp.ENV_POINT: "a.b", cp.ENV_DELAY: "-5"})
    cp.reconfigure({})


def test_seeded_hit_matches_the_contract(tmp_path):
    log = str(tmp_path / "hits.jsonl")
    p = run_child(cp.env_for("child.one", hit="seed:99:5", log=log))
    assert p.returncode == -signal.SIGKILL
    assert cp.read_log(log)[-1]["hit"] == cp.hit_from_seed(99, 5)


def test_no_switch_no_crash_and_log_only_mode(tmp_path):
    p = run_child({})
    assert p.returncode == 0 and "survived" in p.stdout
    log = str(tmp_path / "hits.jsonl")
    p = run_child(cp.env_for(None, log=log))
    assert p.returncode == 0
    assert len(cp.read_log(log)) == 10 and not any(h["fire"] for h in cp.read_log(log))


def test_stop_action_freezes_and_continues(tmp_path):
    log = str(tmp_path / "hits.jsonl")
    with procs.Fleet() as fleet:
        p = fleet.start([sys.executable, "-c", CHILD], env=dict(cp.env_for("child.one", hit=2, action="stop", log=log),
                                                                    PYTHONPATH=SRC), log_path=str(tmp_path / "o.log"))
        p.wait_state("T", 10)
        assert cp.read_log(log)[-1]["fire"] is True
        p.cont()
        assert p.wait(10).code == 0
        assert "survived" in p.log_text()


def test_hang_action_blocks_until_killed(tmp_path):
    log = str(tmp_path / "hits.jsonl")
    with procs.Fleet() as fleet:
        p = fleet.start([sys.executable, "-c", CHILD], env=dict(cp.env_for("child.two", action="hang", log=log),
                                                                    PYTHONPATH=SRC))
        deadline = time.monotonic() + 10
        while not any(h["fire"] for h in cp.read_log(log)):
            assert time.monotonic() < deadline
            time.sleep(0.02)
        with pytest.raises(procs.DeadlineExceeded):
            p.wait(0.5)
        p.kill_group()
        assert p.wait(5).killed


def test_cost_when_off():
    cp.reconfigure({})
    n = 1_000_000
    t = time.perf_counter()
    for _ in range(n):
        cp.crash_point("stream.committed")
    per_call = (time.perf_counter() - t) / n
    assert per_call < 1e-6, per_call


def test_cli_lists_points():
    out = subprocess.run([sys.executable, "-m", "cdc_v2.crashpoints", "--import", "toy_sut", "--prefix", "toy."],
                         env=procs.child_env({"PYTHONPATH": SRC + os.pathsep + os.path.dirname(__file__)}),
                         capture_output=True, text=True, timeout=30, check=True).stdout
    pts = {p["name"]: p for p in json.loads(out)}
    assert pts["toy.schema_ready"]["once"] is True and "toy.committed" in pts


def test_every_crash_point_called_in_product_code_is_declared():
    """Static check over src/: crash_point("x") needs declare("x", ...) somewhere in src/ (and the reverse)."""
    called, declared = set(), set()
    for path in glob.glob(os.path.join(SRC, "cdc_v2", "**", "*.py"), recursive=True):
        if path.endswith(("crashpoints.py",)) or os.sep + "testing" + os.sep in path:
            continue
        text = open(path, encoding="utf-8").read()
        called |= set(re.findall(r"\bcrash_point\(\s*[\"']([^\"']+)[\"']", text))
        declared |= set(re.findall(r"\bdeclare\(\s*[\"']([^\"']+)[\"']", text))
    assert called <= declared, called - declared
    assert declared <= called, declared - called
