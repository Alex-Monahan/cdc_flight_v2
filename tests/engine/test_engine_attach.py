"""Attach rule, position mapping, loss of the replication connection, parent death, slot regression (jvm, localpg).

Plan C2 "Attach rule": HELLO only after START_REPLICATION; which backend holds the slot; no second attach; any loss
of the replication connection ends in FATAL and a non-zero exit.
"""
import json
import os
import signal
import struct
import subprocess
import sys
import time

import pytest

from cdc_v2 import lsn as L
from cdc_v2.deadline import Deadline
from cdc_v2.stream import frames as F
from cdc_v2.stream.engine import Engine, EngineExited, EngineFatal
from cdc_v2.testing import netproxy
from stream_helpers import Collector, evidence, wait_for

pytestmark = [pytest.mark.jvm, pytest.mark.localpg, pytest.mark.timeout(900)]
HERE = os.path.dirname(os.path.abspath(__file__))


def hello_of(eng, timeout=60):
    col = Collector(eng)
    col.run(lambda c: eng.hello is not None, timeout=timeout)
    return col


def ended(eng, timeout):
    """Read until the engine ends; returns (exception, seconds)."""
    t0 = time.monotonic()
    try:
        for _ in eng.frames(Deadline.after(timeout)):
            pass
    except (EngineFatal, EngineExited, F.EngineBroken) as e:
        return e, time.monotonic() - t0
    return None, time.monotonic() - t0


def test_hello_names_the_backend_that_holds_the_slot(pair, scenario, artifacts):
    sc = scenario("create table {s}.a (id int primary key)")
    with Engine(sc.settings(heartbeat_interval_ms=500), None, artifacts) as eng:
        hello_of(eng)
        h = eng.hello
        slot = pair.slot(sc.slot)
        assert slot[1] is True and h.active_pid == slot[2]
        app = pair.r("select application_name, (extract(epoch from backend_start) * 1000000)::bigint "
                     "from pg_stat_activity where pid = %s", (slot[2],), role="bootstrap")[0]
        repl = pair.r("select application_name from pg_stat_replication where pid = %s", (slot[2],),
                      role="bootstrap")[0][0]
        assert app[0] == h.application_name == repl == "Debezium Streaming"   # Debezium's fixed name, not ours
        assert app[1] == h.backend_start_us
        assert h.engine_id == eng.engine_id and h.jvm_pid == eng.pid
        eng.stop(Deadline.after(20))
    evidence("attach_hello.json", {"test": "tests/engine/test_engine_attach.py::test_hello_names_the_backend_that_holds_the_slot",
                                   "application_name_on_server": h.application_name,
                                   "hello_active_pid_equals_slot_active_pid": True,
                                   "hello_backend_start_equals_pg_stat_activity": True})


def test_terminated_walsender_ends_the_engine_and_the_slot_is_not_taken_again(pair, scenario, artifacts):
    sc = scenario("create table {s}.a (id int primary key)")
    eng = Engine(sc.settings(), None, artifacts)
    try:
        hello_of(eng)
        pid = pair.slot(sc.slot)[2]
        t0 = time.monotonic()
        pair.r("select pg_terminate_backend(%s)", (pid,), role="bootstrap")
        err, secs = ended(eng, 120)
        assert err is not None, "the engine did not end after its walsender was terminated"
        assert not eng.alive and eng.proc.returncode != 0
        gone = time.monotonic() - t0
        time.sleep(5)          # anything that would reconnect would have done so by now
        slot = pair.slot(sc.slot)
        assert slot[1] is False and slot[2] is None
    finally:
        eng.kill()
    evidence("attach_walsender_terminated.json", {
        "test": "tests/engine/test_engine_attach.py::test_terminated_walsender_ends_the_engine_and_the_slot_is_not_taken_again",
        "error": type(err).__name__, "message": str(err)[:300], "seconds_to_end": round(gone, 2),
        "exit_status": eng.proc.returncode, "slot_active_5s_later": slot[1]})


def test_slot_held_by_another_engine(pair, scenario, artifacts):
    sc = scenario("create table {s}.a (id int primary key)")
    first = Engine(sc.settings(), None, artifacts)
    try:
        hello_of(first)
        holder = pair.slot(sc.slot)[2]
        second = Engine(sc.settings(), None, artifacts)
        try:
            err, secs = ended(second, 180)
            assert err is not None and second.hello is None
            assert pair.slot(sc.slot)[2] == holder          # the first one still holds it
        finally:
            second.kill()
    finally:
        first.kill()
    evidence("attach_slot_active.json", {
        "test": "tests/engine/test_engine_attach.py::test_slot_held_by_another_engine",
        "error": type(err).__name__, "message": str(err)[:400], "seconds_to_fail": round(secs, 2)})


@pytest.mark.parametrize("fault", ["reset", "close", "blackhole"])
def test_broken_source_connection_through_the_proxy(pair, scenario, artifacts, fault):
    sc = scenario("create table {s}.a (id int primary key)")
    bound = 150
    # proxies of package E: the upper half of its own port range
    with netproxy.NetProxy("localhost", pair.replica_port, port_range=(55624, 55627)) as px:
        s = sc.settings(heartbeat_interval_ms=1000)
        s = type(s)(**{**s.__dict__, "port": px.port})
        eng = Engine(s, None, artifacts)
        try:
            hello_of(eng)
            pid = pair.slot(sc.slot)[2]
            t0 = time.monotonic()
            px.set_fault(fault)
            hb_times, ws_end = [], None
            err = None
            end = Deadline.after(bound)
            while not end.expired:
                try:
                    for f in eng.frames(end.within(1.0)):
                        if isinstance(f, F.Heartbeat):
                            hb_times.append(round(time.monotonic() - t0, 1))
                except (EngineFatal, EngineExited, F.EngineBroken) as e:
                    err = e
                    break
                if ws_end is None and not pair.r("select 1 from pg_stat_activity where pid = %s", (pid,),
                                                 role="bootstrap"):
                    ws_end = round(time.monotonic() - t0, 1)
            secs = time.monotonic() - t0
            stats = px.stats()
            outcome = type(err).__name__ if err is not None else "no end within %d s" % bound
            px.set_fault("pass")
            alive_after = eng.alive
        finally:
            eng.kill()
    time.sleep(3)
    slot = pair.slot(sc.slot)
    evidence("proxy_%s.json" % fault, {
        "test": "tests/engine/test_engine_attach.py::test_broken_source_connection_through_the_proxy[%s]" % fault,
        "fault": fault, "outcome": outcome, "message": str(err)[:300] if err else "",
        "seconds": round(secs, 2), "engine_alive_at_bound": alive_after,
        "heartbeat_interval_ms": 1000, "heartbeat_frames_during_fault": len(hb_times),
        "last_heartbeat_frame_at_s": hb_times[-1] if hb_times else None,
        "server_walsender_ended_at_s": ws_end, "wal_sender_timeout": "60s (default)",
        "slot_active_after_kill": slot[1], "proxy_stats": {k: stats[k] for k in ("accepted", "closed_by_fault",
                                                                                   "reset_by_fault")}})
    if fault in ("reset", "close"):
        assert err is not None and secs < 60
    assert slot[1] is False          # nobody re-took the slot


def test_parent_death_ends_the_jvm_and_frees_the_slot(pair, scenario, artifacts, tmp_path):
    sc = scenario("create table {s}.a (id int primary key)")
    s = sc.settings()
    spec = tmp_path / "spec.json"
    spec.write_text(json.dumps({"settings": {k: v for k, v in s.__dict__.items() if k != "extra"},
                                "artifacts": artifacts.__dict__}))
    child = subprocess.Popen([sys.executable, os.path.join(HERE, "engine_parent.py"), str(spec)],
                             stdout=subprocess.PIPE, text=True, env={**os.environ, "PYTHONPATH": os.pathsep.join(sys.path)})
    try:
        line = child.stdout.readline()
        jvm_pid = int(json.loads(line)["jvm_pid"])
        wait_for(lambda: pair.slot(sc.slot)[1], timeout=60, what="slot active")
        t0 = time.monotonic()
        os.kill(child.pid, signal.SIGKILL)
        child.wait(10)

        def jvm_gone():
            try:
                os.kill(jvm_pid, 0)
                return False
            except ProcessLookupError:
                return True
        wait_for(jvm_gone, timeout=30, interval=0.05, what="JVM gone")
        t_jvm = time.monotonic() - t0
        wait_for(lambda: not pair.slot(sc.slot)[1], timeout=30, interval=0.05, what="slot inactive")
        t_slot = time.monotonic() - t0
    finally:
        if child.poll() is None:
            child.kill()
    assert t_jvm < 10 and t_slot < 15
    evidence("parent_death.json", {"test": "tests/engine/test_engine_attach.py::test_parent_death_ends_the_jvm_and_frees_the_slot",
                                   "jvm_gone_after_s": round(t_jvm, 3), "slot_inactive_after_s": round(t_slot, 3)})


def parse_pgoutput(rows):
    """[(kind, fields)] of Begin and Commit messages of pgoutput protocol 1."""
    out = []
    for _lsn, data in rows:
        data = bytes(data)
        if data[:1] == b"B":
            final_lsn, ts, xid = struct.unpack(">qqI", data[1:21])
            out.append(("B", {"final_lsn": final_lsn, "xid": xid}))
        elif data[:1] == b"C":
            _flags, commit_lsn, end_lsn, ts = struct.unpack(">bqqq", data[1:26])
            out.append(("C", {"commit_lsn": commit_lsn, "end_lsn": end_lsn}))
    return out


def test_positions_map_to_pgoutput_begin_and_commit(pair, scenario, artifacts):
    sc = scenario("create table {s}.m (id int primary key, v text)")
    peek_slot = sc.slot + "_peek"
    pair.create_slot(peek_slot)
    try:
        for i in range(5):
            pair.p("insert into %s.m select g, md5(g::text) from generate_series(%d, %d) g"
                   % (sc.schema, i * 10, i * 10 + i))
        pair.wait_replayed()
        with Engine(sc.settings(), None, artifacts) as eng:
            col = Collector(eng).run(lambda c: len(c.txns) >= 5, timeout=60)
            eng.stop(Deadline.after(20))
        msgs = parse_pgoutput(pair.r("select lsn, data from pg_logical_slot_peek_binary_changes(%s, null, null, "
                                     "'proto_version', '1', 'publication_names', %s, 'messages', 'true')",
                                     (peek_slot, sc.pub), role="runtime"))
    finally:
        pair.drop_slot(peek_slot)
    begins = [m for k, m in msgs if k == "B"]
    commits = [m for k, m in msgs if k == "C"]
    rows = []
    for t, b, c in zip(col.txns, begins, commits):
        assert t.end.ack_position == c["end_lsn"] == t.end.positions["lsn_commit"] == t.end.positions["lsn"]
        assert b["final_lsn"] == c["commit_lsn"]
        assert t.begin.positions["lsn"] < b["final_lsn"]       # the start of the transaction's first record
        assert t.xid == b["xid"]
        rows.append({"begin.lsn": L.format(t.begin.positions["lsn"]), "pgoutput.B.final_lsn": L.format(b["final_lsn"]),
                     "pgoutput.C.commit_lsn": L.format(c["commit_lsn"]),
                     "end.ack_position": L.format(t.end.ack_position), "pgoutput.C.end_lsn": L.format(c["end_lsn"])})
    evidence("position_mapping.json", {"test": "tests/engine/test_engine_attach.py::test_positions_map_to_pgoutput_begin_and_commit",
                                       "rule": "BEGIN positions.lsn < B.final_lsn = C.commit_lsn (start of the "
                                               "commit record); END ack_position = positions.lsn_commit = "
                                               "positions.lsn = C.end_lsn (end of the commit record)",
                                       "transactions": rows})


def repl_probe(pair, artifacts, slot, pub, start, flush):
    rt = pair.passwords["runtime"]
    cfg = {"database.hostname": "localhost", "database.port": str(pair.replica_port), "database.dbname":
           pair.info["database"], "database.user": rt["user"], "database.password": rt["password"],
           "database.sslmode": "verify-full", "database.sslrootcert": pair.info["ca_cert"], "slot.name": slot,
           "publication.name": pub, "probe.start": start, "probe.flush": flush, "probe.hold.ms": "3000"}
    p = subprocess.run([artifacts.java, "-cp", artifacts.classpath, "cdc_v2.engine.CdcEngine", "--repl-probe"],
                       input="CONFIG " + json.dumps(cfg) + "\n", capture_output=True, text=True, timeout=60,
                       env={"PATH": os.environ.get("PATH", "")})
    assert p.returncode == 0, p.stderr[-2000:].replace(rt["password"], "<password>")
    return p.stdout


# First minor release of each branch with "Never move a replication slot's confirmed-flush position backwards"
# (Postgres commit of 2025-05-19, back-patched through 13); 18.0 has it from the start.
CONFIRMED_FLUSH_GUARD = {13: 130022, 14: 140019, 15: 150014, 16: 160010, 17: 170006}


def test_lower_flush_position_and_the_slot(pair, scenario, artifacts):
    """Open point 14 of the core design: a status update with a flush position below confirmed_flush_lsn.

    A server with the guard keeps the slot where it is (measured on 18.6); a server without it moves the slot back to
    the position sent (measured on 16.6). Our engine sends no flush position before its first DURABLE, so by itself
    it moves the slot in neither case.
    """
    sc = scenario("create table {s}.w (id int primary key)")
    for i in range(3):
        pair.p("insert into %s.w values (%d)" % (sc.schema, i))
    pair.wait_replayed()
    old = pair.slot(sc.slot)[0]
    target = pair.r("select pg_last_wal_replay_lsn()::text")[0][0]
    pair.r("select pg_replication_slot_advance(%s, %s::pg_lsn)", (sc.slot, target), role="runtime")
    advanced = pair.slot(sc.slot)[0]
    assert L.parse(advanced) > L.parse(old)
    out = repl_probe(pair, artifacts, sc.slot, sc.pub, old, old)
    after_probe = pair.slot(sc.slot)[0]
    # the engine variant: start the engine (memory offsets: it starts from the slot) and let it send updates
    with Engine(sc.settings(status_update_interval_ms=500, heartbeat_interval_ms=500), None, artifacts) as eng:
        hello_of(eng)
        time.sleep(3)
        during_engine = pair.slot(sc.slot)[0]
        eng.stop(Deadline.after(20))
    version = pair.r("show server_version")[0][0]
    num = int(pair.r("show server_version_num")[0][0])
    guarded = num >= CONFIRMED_FLUSH_GUARD.get(num // 10000, 0)
    evidence("slot_flush_backwards_pg%s.json" % version.split(".")[0], {
        "test": "tests/engine/test_engine_attach.py::test_lower_flush_position_and_the_slot",
        "server_version": version, "server_has_the_guard": guarded,
        "confirmed_flush_before_advance": old, "after_advance": advanced,
        "after_status_updates_with_flush_at_the_old_position": after_probe, "while_engine_streams": during_engine,
        "probe": out.strip()})
    assert after_probe == (advanced if guarded else old)
    assert during_engine == after_probe
