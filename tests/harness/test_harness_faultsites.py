"""faultsites + testing.faults: raise, delay, block; hits, skip, window; re-read on change; a running process obeys."""
import os
import sys
import threading
import time

import pytest

from cdc_v2 import faultsites as fs
from cdc_v2.testing import procs
from cdc_v2.testing.faults import FaultControl

SRC = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), "src")

fs.declare_site("harness_test.commit", "test site: commit")
fs.declare_site("harness_test.read", "test site: read")


@pytest.fixture
def fc(tmp_path):
    c = FaultControl(tmp_path / "f")
    fs.reconfigure(c.env(tag="me"))
    yield c
    fs.reconfigure({})


def calls(site, n):
    out = []
    for _ in range(n):
        try:
            fs.fault_site(site)
            out.append("ok")
        except Exception as exc:                 # noqa: BLE001
            out.append(type(exc).__name__)
    return out


def test_off_is_a_noop_and_cheap():
    fs.reconfigure({})
    fs.fault_site("not.declared_but_off")        # off: no check at all
    n = 1_000_000
    t = time.perf_counter()
    for _ in range(n):
        fs.fault_site("harness_test.commit")
    assert (time.perf_counter() - t) / n < 1e-6


def test_raise_for_a_number_of_hits_after_skip(fc):
    rid = fc.raise_at("harness_test.commit", "ConnectionError", hits=2, skip=1)
    assert calls("harness_test.commit", 5) == ["ok", "ConnectionError", "ConnectionError", "ok", "ok"]
    assert calls("harness_test.read", 2) == ["ok", "ok"]
    fired = fc.fired(site="harness_test.commit")
    assert [e["n"] for e in fired] == [1, 2] and all(e["rule"] == rid and e["tag"] == "me" for e in fired)


def test_exception_classes_and_marker(fc):
    import duckdb  # noqa: F401 - makes duckdb.IOException resolvable

    fc.raise_at("harness_test.commit", "duckdb.IOException", message="injected io", hits=1, rid="io")
    with pytest.raises(duckdb.IOException, match="injected io") as ei:
        fs.fault_site("harness_test.commit")
    assert ei.value.cdc_v2_fault == "io"
    fc.raise_at("harness_test.commit", hits=1, rid="default")
    with pytest.raises(fs.FaultInjected):
        fs.fault_site("harness_test.commit")
    with pytest.raises(fs.FaultFileError):
        fc.raise_at("harness_test.commit", "NotAnException")
    with pytest.raises(fs.FaultFileError):
        fs.resolve_exception("os.system")                      # not an Exception class
    with pytest.raises(fs.FaultFileError):
        fs.resolve_exception("never_imported_module.Error")    # never imports


def test_an_exception_that_cannot_be_built_does_not_fire(fc):
    fc.raise_at("harness_test.commit", "never_imported_module.Error", rid="bad")
    assert calls("harness_test.commit", 2) == ["ok", "ok"]
    assert fc.fired(rule="bad") == []
    assert any(e.get("rule") == "bad" and "error" in e for e in fc.log())


def test_a_new_rule_that_reuses_an_id_starts_from_zero(fc):
    fc.raise_at("harness_test.commit", "OSError", hits=1, rid="same")
    assert calls("harness_test.commit", 2) == ["OSError", "ok"]
    fc.clear()
    fc.raise_at("harness_test.commit", "TimeoutError", hits=1, rid="same")   # same id, different rule
    assert calls("harness_test.commit", 2) == ["TimeoutError", "ok"]
    other = FaultControl(os.path.dirname(fc.path))                          # a second writer on the directory
    assert other._id(None) != fc._id(None)


def test_window_and_glob(fc):
    fc.raise_at("harness_test.*", "TimeoutError", window=(0.3, 0.6))
    assert calls("harness_test.read", 1) == ["ok"]           # before the window
    time.sleep(0.35)
    assert calls("harness_test.read", 1) == ["TimeoutError"]
    time.sleep(0.35)
    assert calls("harness_test.commit", 1) == ["ok"]         # after the window


def test_delay(fc):
    fc.delay_at("harness_test.commit", 0.2, hits=1)
    t = time.monotonic()
    fs.fault_site("harness_test.commit")
    assert time.monotonic() - t >= 0.19
    t = time.monotonic()
    fs.fault_site("harness_test.commit")
    assert time.monotonic() - t < 0.1


def test_block_until_rule_removed(fc):
    rid = fc.block_at("harness_test.commit")
    done = []
    th = threading.Thread(target=lambda: (fs.fault_site("harness_test.commit"), done.append(time.monotonic())))
    t = time.monotonic()
    th.start()
    time.sleep(0.4)
    assert not done                                          # still blocked
    fc.remove(rid)
    th.join(5)
    assert done and 0.35 < done[0] - t < 2
    log = fc.log()
    assert [e["action"] for e in log if e.get("rule") == rid] == ["block", "unblock"]


def test_block_with_time_limit(fc):
    fc.block_at("harness_test.commit", seconds=0.2)
    t = time.monotonic()
    fs.fault_site("harness_test.commit")
    assert 0.18 < time.monotonic() - t < 1.5


def test_tag_filter_and_undeclared(fc):
    fc.raise_at("harness_test.commit", "OSError", tag="someone_else")
    assert calls("harness_test.commit", 2) == ["ok", "ok"]
    with pytest.raises(fs.UndeclaredFaultSite):
        fs.fault_site("harness_test.undeclared")


def test_validation_errors(tmp_path):
    for doc in ({"rules": [{"site": "a.b", "action": "raise"}]},
                {"rules": [{"id": "x", "site": "a.b", "action": "explode"}]},
                {"rules": [{"id": "x", "site": "a.b", "action": "delay"}]},
                {"rules": [{"id": "x", "site": "a.b", "action": "raise", "hits": -1}]},
                {"rules": [{"id": "x", "site": "a.b", "action": "raise"}, {"id": "x", "site": "a.b",
                                                                          "action": "raise"}]}):
        with pytest.raises(fs.FaultFileError):
            fs.validate_rules(doc)


CHILD = r'''
import sys, time
from cdc_v2 import faultsites as fs
fs.declare_site("child.commit", "commit")
ok = err = 0
end = time.monotonic() + float(sys.argv[1])
while time.monotonic() < end:
    try:
        fs.fault_site("child.commit"); ok += 1
    except ConnectionError:
        err += 1
    time.sleep(0.005)
print(ok, err, flush=True)
'''


def test_a_running_process_obeys_changes_of_the_control_file(tmp_path):
    fc = FaultControl(tmp_path / "f")
    out = tmp_path / "child.out"
    with procs.Fleet() as fleet:
        p = fleet.start([sys.executable, "-c", CHILD, "1.5"], env=dict(fc.env(tag="c"), PYTHONPATH=SRC),
                        log_path=str(out))
        time.sleep(0.3)
        fc.raise_at("child.commit", "ConnectionError", hits=10)
        time.sleep(0.4)
        fc.clear()
        assert p.wait(10).code == 0
    ok, err = map(int, out.read_text().split())
    assert err == 10 and ok > 50
    assert len(fc.fired(site="child.commit")) == 10
