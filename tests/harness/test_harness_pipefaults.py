"""pipefaults: every fault kind, reproducible from the seed, independent of chunking, detected by a receiver, and
relayed between two real processes."""
import json
import os
import sys
import time

import frames
import pytest

from cdc_v2.testing import pipefaults as pf
from cdc_v2.testing import procs
from cdc_v2.testing.harness_evidence import write_evidence

HERE = os.path.dirname(os.path.abspath(__file__))
SRC = os.path.join(os.path.dirname(os.path.dirname(HERE)), "src")
EVIDENCE = os.environ.get("CDC_V2_HARNESS_EVIDENCE")
DATA = bytes(range(256)) * 40                         # 10,240 bytes


def ev(kind, **kw):
    return dict(kind=kind, id=0, **kw)


def test_each_kind_exactly():
    out, log = pf.apply_bytes(DATA, [ev("cut", offset=1000)])
    assert out == DATA[:1000] and log[0]["in_offset"] == 1000 and log[0]["fired"]
    out, log = pf.apply_bytes(DATA, [ev("dup", offset=500, length=100)])
    assert out == DATA[:600] + DATA[500:600] + DATA[600:]
    assert log[0]["out_offset"] == 600
    out, log = pf.apply_bytes(DATA, [ev("truncate", length=37)])
    assert out == DATA[:-37] and log[0]["dropped"] == 37 and log[0]["in_offset"] == len(DATA) - 37
    out, log = pf.apply_bytes(DATA, [ev("flip", offset=4321, mask=0x10)])
    diff = [i for i in range(len(DATA)) if out[i] != DATA[i]]
    assert len(out) == len(DATA) and diff == [4321] and out[4321] == DATA[4321] ^ 0x10
    assert (log[0]["before"], log[0]["after"]) == (DATA[4321], DATA[4321] ^ 0x10)
    out, log = pf.apply_bytes(DATA, [ev("stall", offset=10, seconds=0.2)])
    assert out == DATA and log[0]["kind"] == "stall"
    out, log = pf.apply_bytes(DATA, [ev("flip", offset=len(DATA) + 5, mask=1)])
    assert out == DATA and log[0]["fired"] is False


def test_truncate_acts_on_the_input_before_other_faults():
    """A dup or flip inside the truncated tail never fires, and the log says so (review finding)."""
    data = bytes(range(100))
    plan = [dict(kind="dup", id=0, offset=90, length=10), dict(kind="truncate", id=1, length=10)]
    out, log = pf.apply_bytes(data, plan, chunk=7)
    assert out == data[:90]
    by = {e["kind"]: e for e in log}
    assert by["dup"]["fired"] is False and by["dup"]["why"] == "in the truncated tail"
    assert by["truncate"]["fired"] and by["truncate"]["in_offset"] == 90 and by["truncate"]["dropped"] == 10
    out, log = pf.apply_bytes(data, [dict(kind="flip", id=0, offset=95, mask=1), dict(kind="truncate", id=1,
                                                                                     length=10)])
    assert out == data[:90] and [e["fired"] for e in log if e["kind"] == "flip"] == [False]
    out, log = pf.apply_bytes(data, [dict(kind="flip", id=0, offset=80, mask=1), dict(kind="truncate", id=1,
                                                                                     length=10)])
    assert out[80] == data[80] ^ 1 and len(out) == 90 and all(e["fired"] for e in log)


def test_reproducible_and_independent_of_chunking():
    for seed in range(50):
        plan = pf.make_plan(seed, len(DATA))
        assert plan == pf.make_plan(seed, len(DATA))
        ref = pf.apply_bytes(DATA, plan)
        for chunk in (1, 7, 4096):
            assert pf.apply_bytes(DATA, plan, chunk=chunk) == ref, (seed, chunk)
    kinds = {e["kind"] for s in range(50) for e in pf.make_plan(s, len(DATA))}
    assert kinds == set(pf.KINDS)
    assert all(sum(e["kind"] in pf.TERMINAL for e in pf.make_plan(s, 1000, events=5)) <= 1 for s in range(200))


def test_every_corrupting_fault_is_detected_and_contained():
    """1,000 seeds over a framed stream: whenever a cut, dup, truncate or flip fired, the receiver reports an error
    and has applied only a prefix of the true frames; with only stalls the stream arrives whole."""
    stats = {"seeds": 0, "faults_fired": 0, "detected": 0, "clean": 0, "by_kind": {}}
    for seed in range(1000):
        data, payloads = frames.make_stream(40, seed)
        plan = pf.make_plan(seed, len(data))
        out, log = pf.apply_bytes(data, plan)
        fired = [e for e in log if e.get("fired")]
        applied, err = frames.receive(out)
        assert applied == payloads[:len(applied)], seed                  # never a corrupted frame applied
        corrupting = [e for e in fired if e["kind"] != "stall"]
        stats["seeds"] += 1
        stats["faults_fired"] += len(fired)
        for e in fired:
            stats["by_kind"][e["kind"]] = stats["by_kind"].get(e["kind"], 0) + 1
        if corrupting:
            assert err is not None, (seed, corrupting)
            stats["detected"] += 1
        else:
            assert err is None and applied == payloads, seed
            stats["clean"] += 1
    if EVIDENCE:
        write_evidence(os.path.join(EVIDENCE, "pipefaults_detection.json"), stats)


PRODUCER = r'''
import os, sys, frames
data, _ = frames.make_stream(int(sys.argv[2]), int(sys.argv[3]))
fd = int(sys.argv[1])
view = memoryview(data)
try:
    while view:
        n = os.write(fd, view[:997])
        view = view[n:]
except BrokenPipeError:
    sys.exit(3)
'''
CONSUMER = r'''
import json, os, sys, frames
fd = int(sys.argv[1])
buf = b""
while True:
    chunk = os.read(fd, 65536)
    if not chunk:
        break
    buf += chunk
applied, err = frames.receive(buf)
print(json.dumps({"applied": len(applied), "error": err, "bytes": len(buf)}), flush=True)
'''


def run_pair(tmp_path, name, plan, n=300, seed=5):
    relay = pf.PipeRelay(plan).start()
    env = {"PYTHONPATH": os.pathsep.join([SRC, HERE])}
    out = tmp_path / ("consumer_%s.out" % name)
    t = time.monotonic()
    with procs.Fleet() as fleet:
        prod = fleet.start([sys.executable, "-c", PRODUCER, relay.write_fd, n, seed], env=env, name="producer",
                           pass_fds=(relay.write_fd,))
        cons = fleet.start([sys.executable, "-c", CONSUMER, relay.read_fd, n, seed], env=env, name="consumer",
                           pass_fds=(relay.read_fd,), log_path=str(out))
        os.close(relay.write_fd)              # the children hold their own copies
        os.close(relay.read_fd)
        prod_st, cons_st = prod.wait(20), cons.wait(20)
        log = relay.join(20)
    return json.loads(out.read_text()), log, prod_st, cons_st, time.monotonic() - t


def test_relay_between_two_processes(tmp_path):
    data, payloads = frames.make_stream(300, 5)
    res, log, prod_st, cons_st, elapsed = run_pair(tmp_path, "stall", [ev("stall", offset=2000, seconds=0.4)])
    assert res == {"applied": 300, "error": None, "bytes": len(data)} and prod_st.code == 0 and cons_st.code == 0
    assert elapsed >= 0.4 and log[-1] == {"kind": "end", "how": "eof", "in_bytes": len(data), "out_bytes": len(data)}

    res, log, prod_st, _, _ = run_pair(tmp_path, "cut", [ev("cut", offset=5000)], n=5000)   # > pipe buffer
    assert res["error"] and res["bytes"] == 5000 and res["applied"] < 5000
    assert log[-1]["how"] == "cut" and prod_st.code == 3           # the writer saw EPIPE

    res, log, _, _, _ = run_pair(tmp_path, "dup", [ev("dup", offset=3000, length=50)])
    assert res["error"] and res["bytes"] == len(data) + 50

    res, log, _, _, _ = run_pair(tmp_path, "flip", [ev("flip", offset=7777, mask=4)])
    assert res["error"] and res["bytes"] == len(data) and res["applied"] < 300


def test_module_runs_as_a_process(tmp_path):
    data, _ = frames.make_stream(100, 9)
    src = tmp_path / "in.bin"
    src.write_bytes(data)
    logf = tmp_path / "log.json"
    with open(src, "rb") as fin, open(tmp_path / "out.bin", "wb") as fout:
        p = procs.start([sys.executable, "-m", "cdc_v2.testing.pipefaults", "--seed", "11", "--size", len(data),
                         "--kinds", "dup,flip,truncate", "--log", logf], env={"PYTHONPATH": SRC}, stdin=fin)
        # stdout of procs.start goes to a log file or /dev/null; use the pure function as the reference instead
        assert p.wait(20).code == 0
    plan = pf.make_plan(11, len(data), kinds=["dup", "flip", "truncate"])
    doc = json.loads(logf.read_text())
    assert doc["plan"] == plan
    ref_out, ref_log = pf.apply_bytes(data, plan)
    assert [e for e in doc["log"] if e["kind"] != "end"] == ref_log


@pytest.mark.parametrize("bad", [{"kinds": ["boom"]}, {"size": 0}])
def test_bad_plan_arguments(bad):
    with pytest.raises(ValueError):
        pf.make_plan(1, bad.get("size", 10), kinds=bad.get("kinds", pf.KINDS))
