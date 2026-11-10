"""cdc_v2.stream.engine without a JVM (hermetic): a fake engine speaks the same pipes."""
import glob
import hashlib
import json
import os
import re
import sys
import time

import pytest

from cdc_v2.deadline import Deadline
from cdc_v2.stream import frames as F
from cdc_v2.stream.engine import (JAVA_CRASH_POINTS, Engine, EngineExited, EngineFatal, EngineSettings,
                                  build_properties)
from cdc_v2.stream.jvm import JvmArtifacts, SRC_DIR

HERE = os.path.dirname(os.path.abspath(__file__))
PASSWORD = "pw-" + "x7Qz" * 6


@pytest.fixture
def fake(tmp_path):
    java = tmp_path / "java"
    java.write_text("#!%s\nimport runpy, sys\nsys.argv = [%r] + sys.argv[1:]\nrunpy.run_path(%r, run_name='__main__')\n"
                    % (sys.executable, os.path.join(HERE, "fake_engine.py"), os.path.join(HERE, "fake_engine.py")))
    java.chmod(0o755)
    arts = JvmArtifacts(java=str(java), classpath="cp", classes_dir="", jars_dir="", java_version="fake",
                        jdk_source="fake")
    out = tmp_path / "out.jsonl"
    started = []

    def start(mode, **kw):
        env = {"FAKE_MODE": mode, "FAKE_OUT": str(out), "FAKE_EXPECT_PW_SHA": hashlib.sha256(PASSWORD.encode()).hexdigest()}
        s = EngineSettings(host="localhost", port=55621, dbname="d", user="u", password=PASSWORD, slot="sl",
                           publication="pub", **kw)
        e = Engine(s, 42, arts, env=env)
        started.append(e)
        return e

    def notes():
        return [json.loads(l) for l in out.read_text().splitlines()] if out.exists() else []

    yield start, notes
    for e in started:
        e.kill()


def test_properties_carry_the_password_only_in_config():
    s = EngineSettings(host="h", port=55621, dbname="d", user="u", password=PASSWORD)
    p = build_properties(s, 7, "eid")
    assert p["database.password"] == PASSWORD and p["cdc_v2.start.position"] == "7"
    assert p["errors.max.retries"] == "0" and p["slot.max.retries"] == "0"
    assert p["skipped.operations"] == "none" and p["offset.flush.interval.ms"] == "0"
    assert PASSWORD not in repr(s)


def test_settings_refuse_the_owners_port_and_bad_limits():
    with pytest.raises(ValueError):
        EngineSettings(host="localhost", port=5432, dbname="d", user="u", password="p")
    with pytest.raises(ValueError):
        EngineSettings(host="h", port=55621, dbname="d", user="u", password="p", max_value_bytes=1 << 31,
                       frame_max_bytes=1 << 31)


def test_frames_hello_first_and_secret_never_in_argv_or_env(fake):
    start, notes = fake
    e = start("normal")
    got = []
    end = Deadline.after(10)
    while sum(isinstance(f, F.Heartbeat) for f in got) < 5:
        got.append(e.next_frame(end))
    assert isinstance(got[0], F.Hello) and got[0].engine_id == e.engine_id
    assert sum(isinstance(f, F.Heartbeat) for f in got) == 5
    first = notes()[0]
    assert PASSWORD not in json.dumps(first["argv"]) and PASSWORD not in json.dumps(first["env"])
    assert notes()[1]["password_ok"]
    # stdout is a log, never the frame stream; a line that is not ours or Debezium's is counted, not copied
    assert not any("noise on stdout" in l for l in e.log_lines)
    assert e.stop(Deadline.after(10)) == 0
    assert any("not copied" in l for l in e.log_lines)     # counted when the stream ends or a kept line follows
    assert not e.alive


def test_durable_never_blocks_while_fd3_is_full(fake):
    start, notes = fake
    e = start("flood")
    e.next_frame(Deadline.after(5))                 # HELLO; then nobody reads fd 3 for a while
    time.sleep(0.3)
    t0 = time.monotonic()
    for p in range(1, 20001):
        e.durable(p)
    assert time.monotonic() - t0 < 2.0              # 20,000 calls; none waits for the pipe
    time.sleep(0.3)
    seen = [n["durable"] for n in notes() if "durable" in n]
    assert seen and seen[-1] == 20000               # the engine read commands while its fd 3 write was blocked
    assert not any("flood_done" in n for n in notes())
    end = Deadline.after(10)
    rows = 0
    while rows < 12:
        rows += isinstance(e.next_frame(end), F.Rows)
    for _ in range(100):
        if any("flood_done" in n for n in notes()):
            break
        time.sleep(0.02)
    assert any("flood_done" in n for n in notes())


def test_fatal_frame_raises_engine_fatal(fake):
    start, _ = fake
    e = start("fatal")
    with pytest.raises(EngineFatal) as ei:
        list(e.frames(Deadline.after(5)))
    assert ei.value.frame.error_class == "x.Boom" and "deeper" in str(ei.value)
    assert not e.alive


def test_unexpected_exit_raises_engine_exited(fake):
    start, _ = fake
    e = start("exit")
    with pytest.raises(EngineExited) as ei:
        list(e.frames(Deadline.after(5)))
    assert ei.value.status == 3


def test_garbage_on_fd3_raises_engine_broken(fake):
    start, _ = fake
    e = start("garbage")
    with pytest.raises(F.EngineBroken):
        list(e.frames(Deadline.after(5)))
    assert not e.alive


def test_stop_kills_after_the_deadline(fake):
    start, notes = fake
    e = start("ignore_stop")
    e.next_frame(Deadline.after(5))
    t0 = time.monotonic()
    status = e.stop(Deadline.after(0.5))
    assert status == -9 and time.monotonic() - t0 < 3
    assert any(n.get("stop") for n in notes())


def test_kill_leaves_no_process_and_no_fd(fake):
    start, _ = fake
    before = len(os.listdir("/dev/fd"))
    e = start("normal")
    e.next_frame(Deadline.after(5))
    e.kill()
    assert not e.alive
    assert len(os.listdir("/dev/fd")) <= before


def test_java_crash_points_declared_and_called():
    """Static check over the Java sources, like the Python one: every Crash.point(X) has a Crash.declare(name)
    constant, and the Python list JAVA_CRASH_POINTS equals the Java declarations."""
    text = "".join(open(p, encoding="utf-8").read() for p in glob.glob(os.path.join(SRC_DIR, "*.java")))
    declared = dict(re.findall(r'(\w+) = Crash\.declare\("([^"]+)",', text))
    called = set(re.findall(r"Crash\.point\((\w+)\)", text))
    assert called == set(declared)
    names = set(declared.values())
    assert names == set(JAVA_CRASH_POINTS)
    descs = re.findall(r'Crash\.declare\("([^"]+)",\s*"([^"]+)"\)', text.replace('"\n            + "', ""))
    for name, desc in descs:
        assert JAVA_CRASH_POINTS[name].replace(" ", "") == desc.replace(" ", "")


def test_stop_and_next_frame_after_fatal_raise_cleanly(fake):
    start, _ = fake
    e = start("fatal")
    with pytest.raises(EngineFatal):
        list(e.frames(Deadline.after(5)))
    assert e.stop(Deadline.after(5)) == 1                # no OSError on the closed frame descriptor
    with pytest.raises(F.EngineBroken):
        e.next_frame(Deadline.after(1))


def test_settings_refuse_a_frame_target_above_the_maximum():
    with pytest.raises(ValueError):
        EngineSettings(host="h", port=55621, dbname="d", user="u", password="p", frame_target_bytes=1 << 31,
                       frame_max_bytes=1 << 30, max_value_bytes=1 << 20)


PROTECTED_EXAMPLES = ["skipped.operations", "offset.storage", "offset.storage.file.filename", "offset.flush.interval.ms",
                      "offset.commit.policy", "lsn.flush.mode", "errors.max.retries", "slot.max.retries",
                      "provide.transaction.metadata", "snapshot.mode", "publication.autocreate.mode", "converters",
                      "cdcv2.type", "cdcv2.anything", "tombstones.on.delete", "heartbeat.interval.ms",
                      "heartbeat.action.query", "replica.identity.autoset.values", "read.only",
                      "signal.data.collection", "signal.enabled.channels", "offset.mismatch.strategy",
                      "cdc_v2.max.value.bytes", "database.password", "slot.name", "record.processing.order",
                      "include.unknown.datatypes", "schema.refresh.mode", "column.mask.with.5.chars",
                      "column.truncate.to.10.chars", "column.mask.hash.SHA-256.with.salt.x", "column.exclude.list",
                      "column.include.list", "transforms", "transforms.x.type", "predicates", "predicates.p.type",
                      "message.key.columns", "table.include.list", "table.exclude.list", "schema.include.list",
                      "schema.exclude.list"]


@pytest.mark.parametrize("key", PROTECTED_EXAMPLES)
def test_extra_may_not_set_a_protected_key(key):
    with pytest.raises(ValueError, match="protected"):
        EngineSettings(host="h", port=55621, dbname="d", user="u", password="p", extra=((key, "x"),))


def test_extra_may_set_an_unprotected_key():
    s = EngineSettings(host="h", port=55621, dbname="d", user="u", password="p", extra=(("poll.interval.ms", "10"),))
    assert build_properties(s)["poll.interval.ms"] == "10"


ODD_NAMES = (("a.b", "t,1"), ('q"s', "MixedCase"), ("s", "x y"), ("schéma", "t'q"), ("s", "t"))


def test_build_properties_writes_the_list_of_tables_without_a_key():
    from cdc_v2.stream.engine import decode_table_names
    s = EngineSettings(host="h", port=55621, dbname="d", user="u", password="p",
                       keyless_tables=list(ODD_NAMES) + [("s", "t")])
    p = build_properties(s)
    assert decode_table_names(p["cdc_v2.keyless.tables"]) == tuple(sorted(set(ODD_NAMES)))
    assert set(decode_table_names(p["cdc_v2.keyless.tables"])) == set(ODD_NAMES)
    assert p["cdc_v2.keyless.tables"].count("\x00") == 2 * len(ODD_NAMES)
    # through the CONFIG line (JSON) and back: exact
    assert decode_table_names(json.loads(json.dumps(p))["cdc_v2.keyless.tables"]) == s.keyless_tables
    assert build_properties(EngineSettings(host="h", port=55621, dbname="d", user="u",
                                           password="p"))["cdc_v2.keyless.tables"] == ""


@pytest.mark.parametrize("key", ["cdc_v2.keyless.tables", "cdc_v2.keyless"])
def test_extra_may_not_override_the_list_of_tables_without_a_key(key):
    with pytest.raises(ValueError, match="protected"):
        EngineSettings(host="h", port=55621, dbname="d", user="u", password="p", keyless_tables=(("s", "t"),),
                       extra=((key, ""),))


@pytest.mark.parametrize("bad", [("s",), ("s", "t", "u"), ("", "t"), ("s", ""), ("s\x00", "t"), "st", ("s", 1)])
def test_list_of_tables_without_a_key_refuses_what_is_not_a_name_pair(bad):
    with pytest.raises(ValueError, match="keyless_tables"):
        EngineSettings(host="h", port=55621, dbname="d", user="u", password="p", keyless_tables=(bad,))


@pytest.mark.parametrize("ms", [0, -1, 10001])
def test_heartbeat_interval_bounds(ms):
    with pytest.raises(ValueError, match="heartbeat"):
        EngineSettings(host="h", port=55621, dbname="d", user="u", password="p", heartbeat_interval_ms=ms)


def test_log_filter_never_passes_a_stack_trace_or_quoted_text():
    from cdc_v2.stream.engine import log_filter
    assert log_filter("CDCV2 STAT 1 rows=3") == "CDCV2 STAT 1 rows=3"
    assert log_filter("12 [main] INFO io.debezium.X - Starting 'a.b'") == "12 [main] INFO io.debezium.X - Starting 'a.b'"
    assert log_filter("12 [t] ERROR io.debezium.X - bad value 'SECRET' and \"S2\"") == \
        "12 [t] ERROR io.debezium.X - bad value '…' and '…'"
    for line in ("java.lang.NumberFormatException: For input string: \"2562047788\"", "\tat a.b.C.d(C.java:1)",
                 "Caused by: x.Y: z", "\t... 12 more", "random text"):
        assert log_filter(line) is None
