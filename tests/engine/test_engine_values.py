"""Values the driver cannot parse, the FATAL frame without values, the probe without a holder (jvm, localpg)."""
import json
import logging
import os
import subprocess
import time

import pytest

from cdc_v2.deadline import Deadline
from cdc_v2.stream import frames as F
from cdc_v2.stream.engine import Engine, EngineExited, EngineFatal, build_properties, jvm_command
from stream_helpers import Collector, evidence

pytestmark = [pytest.mark.jvm, pytest.mark.localpg, pytest.mark.timeout(900)]
MARK = "PLANTED_7f3a"


def server_major(pair):
    return int(pair.p("show server_version_num")[0][0]) // 10000


def run_until_end(eng, timeout=60):
    """(frames before the end, exception, seconds)."""
    got, t0 = [], time.monotonic()
    try:
        for f in eng.frames(Deadline.after(timeout)):
            got.append(f)
    except (EngineFatal, EngineExited, F.EngineBroken) as e:
        return got, e, time.monotonic() - t0
    return got, None, time.monotonic() - t0


def kinds(frames):
    return [(type(f).__name__, getattr(f, "table", "")) for f in frames if not isinstance(f, (F.Heartbeat, F.BatchEnd))]


def test_unparsable_interval_ends_in_fatal_without_the_value(pair, scenario, artifacts, caplog):
    caplog.set_level(logging.INFO, logger="cdc_v2.engine")
    sc = scenario("create table {s}.ok (id int primary key, v text)",
                  "create table {s}.bad (id int primary key, iv interval, marker text)")
    s = sc.schema
    pair.p("insert into %s.ok values (1, 'before')" % s)
    pair.p("insert into %s.bad values (1, '2562047788 hours', '%s')" % (s, MARK))
    pair.p("insert into %s.ok values (3, 'after')" % s)
    runs = []
    for attempt in range(3):
        eng = Engine(sc.settings(), None, artifacts)
        try:
            got, err, secs = run_until_end(eng)
        finally:
            eng.kill()
        assert isinstance(err, EngineFatal), err
        text = repr(err.frame) + "\n".join(eng.log_lines) + caplog.text
        assert "2562047788" not in text and MARK not in text, "a source value escaped"
        assert err.frame.positions.get("lsn"), err.frame
        runs.append({"seconds_to_fatal": round(secs, 2), "frames_before_fatal": kinds(got),
                     "fatal_class": err.frame.error_class, "fatal_message": err.frame.message,
                     "fatal_causes": [list(c) for c in err.frame.causes],
                     "fatal_positions": err.frame.positions, "fatal_table": err.frame.schema + "." + err.frame.table})
    # every attempt ends at the same transaction: nothing of it beyond its BEGIN is framed (how much of the
    # transactions before it is framed depends on how far the engine's queue was drained), and the cause names
    # the offset of the failing event, the same in every attempt
    for r in runs:
        assert ("Rows", "bad") not in r["frames_before_fatal"]
        assert r["frames_before_fatal"].count(("End", "")) <= 1      # at most the transaction before it
    failing = [next(m for _c, m in r["fatal_causes"] if m.startswith("Error while processing event at offset"))
               for r in runs]
    assert len({f.split("ts_usec")[0] for f in failing}) == 1
    # with the table left out of table.include.list the transaction passes (END with count 0) and the stream goes on
    with Engine(sc.settings(table_include=s + ".ok"), None, artifacts) as eng:
        col = Collector(eng).run(lambda c: len(c.txns) >= 3, timeout=60)
        eng.stop(Deadline.after(20))
    assert [t.end.event_count for t in col.txns[:3]] == [1, 0, 1]
    assert [r["a1"] for r in col.rows_of("ok")] == ["before", "after"]
    evidence("unparsable_interval.json", {
        "test": "tests/engine/test_engine_values.py::test_unparsable_interval_ends_in_fatal_without_the_value",
        "value": "interval '2562047788 hours' (hour part above 2^31)", "runs": runs,
        "value_or_marker_in_fatal_or_logs": False,
        "with_table_excluded": {"end_counts": [t.end.event_count for t in col.txns[:3]],
                                "rows_after": [r["a1"] for r in col.rows_of("ok")]}})


def test_infinite_interval_is_delivered_as_zero(pair, scenario, artifacts):
    """A finding, not a guarantee: pgjdbc's PGInterval parses 'infinity' and '-infinity' into a zero interval, and
    Debezium hands only that object to our converter. Recorded so that a change in the driver is noticed."""
    if server_major(pair) < 17:
        pytest.skip("infinite intervals exist from Postgres 17 on")
    sc = scenario("create table {s}.inf (id int primary key, iv interval)")
    pair.p("insert into %s.inf values (1, 'infinity'), (2, '-infinity'), (3, '0')" % sc.schema)
    with Engine(sc.settings(), None, artifacts) as eng:
        col = Collector(eng).run(lambda c: len(c.rows_of("inf")) >= 3, timeout=60)
        eng.stop(Deadline.after(20))
    got = {r["a0"]: r["a1"] for r in col.rows_of("inf")}
    evidence("infinite_interval.json", {
        "test": "tests/engine/test_engine_values.py::test_infinite_interval_is_delivered_as_zero",
        "server_version": pair.p("show server_version")[0][0], "delivered": got,
        "finding": "infinity and -infinity arrive as the zero interval, indistinguishable from a real zero"})
    assert got == {1: "0 months 0 days 0 microseconds", 2: "0 months 0 days 0 microseconds",
                   3: "0 months 0 days 0 microseconds"}


def test_slot_not_held_at_probe_is_fatal(pair, scenario, artifacts):
    """The probe that fills HELLO finds no holder: FATAL, never a HELLO with empty fields. The test points the probe
    at a second, inactive slot (test hook cdc_v2.test.probe.slot, which Python's `extra` cannot set)."""
    sc = scenario("create table {s}.a (id int primary key)")
    other = sc.slot + "_idle"
    pair.create_slot(other)
    try:
        props = build_properties(sc.settings(), None, "e-probe")
        props["cdc_v2.test.probe.slot"] = other
        r, w = os.pipe()
        script = 'exec 3>&%d %d>&- && exec "$@"' % (w, w)
        p = subprocess.Popen(["/bin/sh", "-c", script, "x"] + jvm_command(artifacts, sc.settings()),
                             stdin=subprocess.PIPE, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                             pass_fds=(w,), env={"PATH": os.environ.get("PATH", "")})
        os.close(w)
        p.stdin.write(("CONFIG " + json.dumps(props) + "\n").encode())
        p.stdin.flush()
        with os.fdopen(r, "rb") as f:
            frames = F.decode_all(f.read())
        code = p.wait(60)
        p.stdin.close()
    finally:
        pair.drop_slot(other)
    assert code == 1 and [type(x).__name__ for x in frames] == ["Fatal"], frames
    assert frames[0].message == "slot not held at probe"
