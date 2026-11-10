"""The Java side without Postgres (marker jvm): encoder byte for byte against Python, decoder on Java output, and
the crash-point environment contract in Java."""
import json
import os
import signal
import subprocess
import time

import pytest

from cdc_v2 import crashpoints
from cdc_v2.stream import frames as F
from cdc_v2.stream.engine import JAVA_CRASH_POINTS, java_crash_points
from cdc_v2.testing import frames as T
from stream_helpers import LONG

pytestmark = [pytest.mark.jvm, pytest.mark.timeout(600)]
MAIN = "cdc_v2.engine.CdcEngine"


def java(artifacts, *args, env=None, timeout=120, **kw):
    e = {"PATH": os.environ.get("PATH", "")}
    e.update(env or {})
    return subprocess.run([artifacts.java, "-cp", artifacts.classpath, MAIN, *args], capture_output=True, text=True,
                          timeout=timeout, env=e, **kw)


def test_java_encoder_equals_python_encoder_and_python_decodes_java(artifacts, tmp_path):
    seeds = 400 if LONG else 60
    out = tmp_path / "gen.bin"
    p = java(artifacts, "--framegen", "1000", str(out), str(seeds))
    assert p.returncode == 0, p.stderr
    jb = out.read_bytes()
    frames = T.generate(1000, seeds)
    assert T.Encoder().encode_all(frames) == jb
    decoded = F.decode_all(jb, chunk=4093)
    assert [T.encode_payload(f) for f in decoded] == [T.encode_payload(f) for f in frames]


def test_splitmix_reference_values(artifacts):
    p = java(artifacts, "--splitmix", "0", "1", "18446744073709551615")
    vals = [int(x) for x in p.stdout.split()]
    assert vals[0] == 0xE220A8397B1DCDAF and vals[1] == 0x910A2DEC89025CC1
    assert vals[2] == crashpoints.splitmix64((1 << 64) - 1)


def test_java_crash_point_list(artifacts):
    pts = java_crash_points(artifacts)
    assert {p["name"]: p["description"] for p in pts} == JAVA_CRASH_POINTS


def selftest(artifacts, tmp_path, point="engine.acked", n=5, linger_ms=0, **crash):
    log = tmp_path / "hits.log"
    env = crashpoints.env_for(point=point if crash else None, log=str(log), **crash)
    proc = subprocess.Popen([artifacts.java, "-Dcdc_v2.selftest.linger.ms=%d" % linger_ms, "-cp", artifacts.classpath,
                             MAIN, "--crash-selftest", point, str(n)],
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
                            env=dict(PATH=os.environ.get("PATH", ""), **env))
    return proc, log


def test_kill_at_hit_from_seed_writes_the_line_first(artifacts, tmp_path):
    hit = crashpoints.hit_from_seed(77, 4)
    proc, log = selftest(artifacts, tmp_path, point="engine.acked", hit="seed:77:4", action="kill", tag="j1")
    out, _ = proc.communicate(timeout=60)
    assert proc.returncode == -signal.SIGKILL
    assert out.split() == [x for i in range(1, hit) for x in ("after", str(i))]
    lines = crashpoints.read_log(log)
    assert [l["hit"] for l in lines] == list(range(1, hit + 1))
    assert lines[-1]["fire"] is True and lines[-1]["action"] == "kill" and lines[-1]["tag"] == "j1"
    assert all(l["point"] == "engine.acked" and l["pid"] == proc.pid for l in lines)
    assert set(lines[0]) == {"point", "hit", "pid", "tag", "t", "fire", "action"}
    assert abs(lines[0]["t"] - time.time()) < 120


def test_kill_later_lets_the_caller_continue(artifacts, tmp_path):
    proc, log = selftest(artifacts, tmp_path, n=3, hit=2, action="kill_later", delay_ms=1500, linger_ms=60000)
    out, _ = proc.communicate(timeout=60)
    assert proc.returncode == -signal.SIGKILL
    assert "after 2" in out and "after 3" in out        # carried on into the next calls before dying


def test_stop_then_continue(artifacts, tmp_path):
    proc, log = selftest(artifacts, tmp_path, n=3, hit=1, action="stop")
    deadline = time.time() + 30
    while time.time() < deadline:
        st = subprocess.run(["ps", "-o", "state=", "-p", str(proc.pid)], capture_output=True, text=True).stdout
        if st.strip().startswith("T"):
            break
        time.sleep(0.1)
    else:
        proc.kill()
        pytest.fail("the JVM did not stop itself")
    os.kill(proc.pid, signal.SIGCONT)
    out, _ = proc.communicate(timeout=60)
    assert proc.returncode == 0 and "after 3" in out


def test_hang_blocks_until_killed(artifacts, tmp_path):
    proc, log = selftest(artifacts, tmp_path, n=3, hit=2, action="hang")
    time.sleep(3)
    assert proc.poll() is None
    proc.kill()
    out, _ = proc.communicate(timeout=30)
    assert "after 1" in out and "after 2" not in out


def test_raise(artifacts, tmp_path):
    proc, log = selftest(artifacts, tmp_path, n=3, hit=3, action="raise")
    out, _ = proc.communicate(timeout=60)
    assert "raised 3" in out and proc.returncode == 0


def test_log_only_and_invalid_environment(artifacts, tmp_path):
    proc, log = selftest(artifacts, tmp_path, n=2)
    proc.communicate(timeout=60)
    assert [l["fire"] for l in crashpoints.read_log(log)] == [False, False]
    for bad in ({"CDC_V2_CRASH_ACTION": "explode", "CDC_V2_CRASH_POINT": "engine.acked"},
                {"CDC_V2_CRASH_HIT": "0", "CDC_V2_CRASH_POINT": "engine.acked"},
                {"CDC_V2_CRASH_POINT": "Not A Name"}):
        p = java(artifacts, "--crash-selftest", "engine.acked", "1", env=bad)
        assert p.returncode != 0 and "IllegalArgumentException" in p.stderr
    p = java(artifacts, "--crash-selftest", "engine.undeclared", "1", env={"CDC_V2_CRASH_LOG": str(tmp_path / "x")})
    assert p.returncode != 0 and "not declared" in p.stderr


def run_with_config(artifacts, props, timeout=60):
    """Start the engine with exactly these properties (bypassing the Python checks); return (frames, exit code)."""
    from cdc_v2.stream.engine import jvm_command, EngineSettings
    r, w = os.pipe()
    s = EngineSettings(host="localhost", port=55621, dbname="d", user="u", password="p")
    script = 'exec 3>&%d %d>&- && exec "$@"' % (w, w)
    p = subprocess.Popen(["/bin/sh", "-c", script, "x"] + jvm_command(artifacts, s), stdin=subprocess.PIPE,
                         stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, pass_fds=(w,),
                         env={"PATH": os.environ.get("PATH", "")})
    os.close(w)
    p.stdin.write(("CONFIG " + json.dumps(props) + "\n").encode())
    p.stdin.flush()
    with os.fdopen(r, "rb") as f:
        data = f.read()
    err = p.stderr.read().decode()
    p.stdin.close()
    return F.decode_all(data), p.wait(timeout), err


def base_props():
    from cdc_v2.stream.engine import EngineSettings, build_properties
    return build_properties(EngineSettings(host="localhost", port=55621, dbname="d", user="u",
                                           password="secret-pw-xyz"), None, "e1")


@pytest.mark.parametrize("key,value", [
    ("skipped.operations", "t"), ("offset.storage", "org.apache.kafka.connect.storage.FileOffsetBackingStore"),
    ("offset.flush.interval.ms", "60000"), ("lsn.flush.mode", "connector_and_driver"), ("errors.max.retries", "3"),
    ("slot.max.retries", "6"), ("provide.transaction.metadata", "false"), ("snapshot.mode", "initial"),
    ("publication.autocreate.mode", "all_tables"), ("converters", "other"), ("cdcv2.type", "x.Y"),
    ("tombstones.on.delete", "true"), ("offset.mismatch.strategy", "trust_offset"), ("heartbeat.interval.ms", "0"),
    ("heartbeat.interval.ms", "60000"), ("heartbeat.action.query", "select 1"),
    ("replica.identity.autoset.values", "*.*:FULL"), ("read.only", "true"), ("signal.enabled.channels", "source"),
    ("offset.commit.policy", "x"), ("offset.storage.file.filename", "/x"), ("cdcv2.extra", "1"),
    ("database.hostname", ""), ("database.port", None), ("skipped.operations", None),
    ("record.processing.order", "UNORDERED"), ("include.unknown.datatypes", "false"),
    ("schema.refresh.mode", "columns_diff_exclude_unchanged_toast"), ("column.mask.with.5.chars", "s.t.c"),
    ("column.truncate.to.10.chars", "s.t.c"), ("column.mask.hash.SHA-256.with.salt.x", "s.t.c"),
    ("column.exclude.list", "s.t.c"), ("column.include.list", "s.t.c"), ("transforms", "x"),
    ("transforms.x.type", "y"), ("predicates", "p"), ("predicates.p.type", "q"), ("message.key.columns", "s.t:c"),
    ("table.exclude.list", "s.t"), ("schema.include.list", "s"), ("schema.exclude.list", "s")])
def test_java_refuses_a_weakened_configuration(artifacts, key, value):
    props = base_props()
    if value is None:
        props.pop(key)
    else:
        props[key] = value
    frames, code, err = run_with_config(artifacts, props)
    assert code == 1 and len(frames) == 1 and isinstance(frames[0], F.Fatal), (code, frames, err[-500:])
    msg = frames[0].message
    assert "Refused" in frames[0].error_class and (key in msg or "database" in msg), msg
    assert "secret-pw-xyz" not in msg and "secret-pw-xyz" not in err


def test_java_held_list_bound_and_end_without_lsn_commit(artifacts):
    p = java(artifacts, "--held-selftest")
    r = json.loads(p.stdout.strip().splitlines()[-1])
    k = r["spacing_after_60000"]
    assert k <= 2 * 60000 // 4096 + 1 and r["held"] <= 4097
    for t in r["takes"]:
        if t["confirmed"] is not None:
            assert t["confirmed"] <= t["durable"]                         # never above
            assert t["durable"] - t["confirmed"] <= (k - 1) * 1000 + 999  # within k - 1 END records
    assert r["takes"][0]["confirmed"] is None and r["after_empty_spacing"] == 1
    assert r["refill_confirmed"] == 1000 * (60000 + 15)
    assert r["end_without_lsn_commit"] == "END record without lsn_commit in its offset"


@pytest.mark.parametrize("seed", [1, 2, 3])
def test_java_held_list_property_random_limits(artifacts, seed):
    """Random limits (odd ones included) and random add/take sequences: every take(p) returns an END with ack <= p,
    and afterwards the newest END <= p lies at most k - 1 records above the newest END confirmed so far. (The first
    version's thinning fails this with odd limits: 580 violations for seed 1, 100 rounds.)"""
    p = java(artifacts, "--held-property", str(seed), "60", timeout=300)
    r = json.loads(p.stdout.strip().splitlines()[-1])
    assert r["odd_limits"] > 0 and r["takes"] > 10000
    assert r["returned_above_p"] == 0 and r["beyond_k_minus_1"] == 0, r
    from stream_helpers import evidence
    evidence("held_list_property_seed%d.json" % seed, dict(r, test="tests/engine/test_jvm.py::"
                                                              "test_java_held_list_property_random_limits"))


@pytest.mark.parametrize("message,want", [
    ("Error while processing event at offset {transaction_id=7, lsn_proc=90, lsn_events_processed=1, "
     "messageType=INSERT, lsn_commit=90, lsn=88, txId=7, ts_usec=1}", "88 7"),
    ("Error while processing event at offset {lsn=5}", "5 -1"),
    ("Error while processing event at offset {no positions here}", "null"),
    ("An exception occurred in the change event producer.", "null")])
def test_java_failing_position_parsing(artifacts, message, want):
    p = java(artifacts, "--failing-position", message)
    assert p.stdout.strip() == want
