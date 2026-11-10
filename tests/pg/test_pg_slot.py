"""Package P: the slot on a standby and on a primary, its facts, invalidation, identity, terminate_holder and the
timeline history after a real promotion (marker localpg)."""
from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import threading
import time

import pytest

from cdc_v2 import lsn as lsnmod
from cdc_v2.deadline import Deadline, DeadlineExceeded
from cdc_v2.pg import slot, timeline

from conftest import MAJOR, ROOT, pgoutput_changes, record, wait_for

pytestmark = [pytest.mark.localpg, pytest.mark.timeout(600)]


def q(name):
    return '"' + name.replace('"', '""') + '"'


def D(s=10):
    return Deadline.after(s)


def published_table(scratch, name="t"):
    scratch.admin(f"create table {q(scratch.schema)}.{name} (id int primary key, v text)")
    scratch.admin(f"create publication {q(scratch.pub)} for table {q(scratch.schema)}.{name}")
    scratch.c.replay_to_primary()


def idle_primary(cluster, seconds=16):
    """Let the primary go idle: after this, its background writer logs no running-transactions record until new
    WAL is written (it logs one at most every 15 s and only after other WAL)."""
    time.sleep(seconds)


# ----------------------------------------------------------------------------------------------- create

def test_create_on_standby_nudged_by_a_write_of_our_own(cluster, scratch):
    published_table(scratch)
    prim, stby = scratch.connect("primary"), scratch.connect("replica")
    assert slot.nudge_method(prim, D()) == "write"           # the runtime role may not call pg_log_standby_snapshot
    idle_primary(cluster)
    c = slot.create(stby, scratch.slot, D(60), primary=prim)
    assert c.on_standby and c.nudge_method == "write" and c.consistent_point > 0
    f = slot.facts(stby, scratch.slot, deadline=D())
    assert f.plugin == "pgoutput" and f.slot_type == "logical" and not f.temporary and not f.two_phase
    assert f.confirmed_flush_lsn == c.consistent_point and f.server.in_recovery
    record("slot_create", "standby_write_nudge_after_idle_s", round(c.seconds, 2))
    assert c.seconds < 20


def test_create_on_standby_with_granted_standby_snapshot(cluster, scratch):
    published_table(scratch)
    rt = scratch.runtime
    cluster.boot("primary", "grant execute on function pg_catalog.pg_log_standby_snapshot() to %s" % rt,
                 db=cluster.info["database"])
    try:
        prim, stby = scratch.connect("primary"), scratch.connect("replica")
        assert slot.nudge_method(prim, D()) == "standby_snapshot"
        idle_primary(cluster)
        c = slot.create(stby, scratch.slot, D(60), primary=prim)
        assert c.nudge_method == "standby_snapshot" and c.seconds < 5
        record("slot_create", "standby_snapshot_granted_after_idle_s", round(c.seconds, 2))
    finally:
        cluster.boot("primary", "revoke execute on function pg_catalog.pg_log_standby_snapshot() from %s" % rt,
                     db=cluster.info["database"])


def test_create_on_idle_primary_without_nudge_times_out_and_leaves_nothing(cluster, scratch):
    published_table(scratch)
    stby = scratch.connect("replica", cancel_grace_s=0.5)
    idle_primary(cluster)
    t = time.monotonic()
    with pytest.raises(slot.SlotCreateTimeout) as ei:
        slot.create(stby, scratch.slot, D(4), nudge=None)
    took = time.monotonic() - t
    rep = ei.value.report
    assert rep["leftover"] is False and took < 4 + 6
    with scratch.connect("replica") as c:
        assert slot.facts(c, scratch.slot, deadline=D()) is None
    # a running-transactions record now arrives; the abandoned creation must not finish the slot later
    for _ in range(10):
        cluster.boot("primary", "select pg_log_standby_snapshot()")
        time.sleep(0.3)
    with scratch.connect("replica") as c:
        assert slot.facts(c, scratch.slot, deadline=D()) is None
    record("slot_create", "idle_primary_no_nudge", {"deadline_s": 4, "returned_after_s": round(took, 2),
                                                    "report": rep})


def test_create_blocked_by_a_long_transaction(cluster, scratch):
    published_table(scratch)
    with cluster.raw("primary", "admin") as long_tx:
        long_tx.execute("begin")
        xid = long_tx.execute(f"insert into {q(scratch.schema)}.t values (1, 'open') "
                              "returning pg_current_xact_id()::text::bigint").fetchone()[0]
        prim, stby = scratch.connect("primary"), scratch.connect("replica", cancel_grace_s=0.5)
        with pytest.raises(slot.SlotCreateTimeout) as ei:
            slot.create(stby, scratch.slot, D(6), primary=prim)
        rep = ei.value.report
        assert rep["leftover"] is False and rep["oldest_running_xid"] == xid and rep["nudges"] > 0
        long_tx.execute("commit")
    stby = scratch.connect("replica")
    c = slot.create(stby, scratch.slot, D(60), primary=scratch.connect("primary"))
    record("slot_create", "long_transaction", {"timed_out_report": rep, "after_commit_s": round(c.seconds, 2)})


def test_creation_whose_client_is_killed_leaves_no_slot(cluster, scratch):
    """H-042: a creation abandoned by a killed client. With client_connection_check_interval the backend aborts;
    the negative control (a plain creation without it) shows that the slot would otherwise appear later."""
    published_table(scratch)
    creds = cluster.passwords["runtime"]
    args = {"port": cluster.ports["replica"], "db": cluster.info["database"], "user": creds["user"],
            "password": creds["password"], "ca": cluster.info["ca_cert"]}
    script_ours = (
        "import json,sys\nfrom cdc_v2 import pg\nfrom cdc_v2.pg import slot\nfrom cdc_v2.deadline import Deadline\n"
        "a=json.load(sys.stdin)\n"
        "n=pg.NodeSettings(host='localhost',port=a['port'],dbname=a['db'],user=a['user'],"
        "password=pg.Password(fn=lambda:a['password']),sslrootcert=a['ca'])\n"
        "c=pg.connect(n,Deadline.after(10))\nprint('ready',flush=True)\n"
        "slot.create(c,a['slot'],Deadline.after(120),nudge=None)\n")
    script_plain = (
        "import json,sys,psycopg\na=json.load(sys.stdin)\n"
        "c=psycopg.connect(host='localhost',port=a['port'],dbname=a['db'],user=a['user'],password=a['password'],"
        "sslmode='verify-full',sslrootcert=a['ca'],autocommit=True)\nprint('ready',flush=True)\n"
        "c.execute(\"select pg_create_logical_replication_slot(%s,'pgoutput')\",(a['slot'],))\n")
    out = {}
    for label, script in (("ours", script_ours), ("plain_negative_control", script_plain)):
        name = scratch.slot + ("_a" if label == "ours" else "_b")
        idle_primary(cluster)
        env = dict(os.environ, PYTHONPATH=os.path.join(ROOT, "src"))
        p = subprocess.Popen([sys.executable, "-c", script], stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                             stderr=subprocess.DEVNULL, text=True, env=env, start_new_session=True)
        p.stdin.write(json.dumps(dict(args, slot=name)))
        p.stdin.close()
        assert p.stdout.readline().strip() == "ready"
        time.sleep(2.0)                                    # the creation is waiting now
        p.send_signal(signal.SIGKILL)
        p.wait(10)
        time.sleep(2.0)                                    # > client_connection_check_interval
        for _ in range(10):
            cluster.boot("primary", "select pg_log_standby_snapshot()")
            time.sleep(0.3)
        exists = bool(cluster.boot("replica", "select 1 from pg_replication_slots where slot_name = %s", (name,)))
        out[label] = {"slot_exists_afterwards": exists}
        p.stdout.close()
    record("slot_create", "killed_client", out)
    assert out["ours"]["slot_exists_afterwards"] is False
    assert out["plain_negative_control"]["slot_exists_afterwards"] is True


def test_create_on_primary_failover_flag(cluster, scratch):
    published_table(scratch)
    prim = scratch.connect("primary")
    c = slot.create(prim, scratch.slot, D(30))
    f = slot.facts(prim, scratch.slot, deadline=D())
    assert not c.on_standby and not f.server.in_recovery
    if MAJOR >= 17:
        assert c.failover is True and f.failover is True
    else:
        assert c.failover is None and f.failover is None
    with pytest.raises(slot.SlotExists):
        slot.create(prim, scratch.slot, D(30))


# ----------------------------------------------------------------------------------------------- facts

@pytest.mark.parametrize("node", ["replica", "primary"])
def test_facts_with_consumer_attached_detached_terminated(cluster, scratch, consumers, node):
    published_table(scratch)
    conn = scratch.connect(node)
    slot.create(conn, scratch.slot, D(60), primary=scratch.connect("primary"))
    assert cluster.q(node, "select pg_has_role(current_user, 'pg_read_all_stats', 'USAGE')", role="runtime") == \
        [(False,)]
    r = consumers(cluster, node, scratch.slot, scratch.pub)
    f = wait_for(lambda: (lambda x: x if x and x.active else None)(slot.facts(conn, scratch.slot, deadline=D())),
                 timeout=20, what="consumer attached")
    truth = cluster.boot(node, "select pid, (extract(epoch from backend_start) * 1000000)::bigint, application_name, "
                               "backend_type from pg_stat_activity where pid = %s", (f.active_pid,))[0]
    assert f.holder == (truth[0], truth[1]) and f.holder_application_name == "pg_recvlogical" == truth[2]
    assert f.holder_backend_type == "walsender" and f.holder_backend_start.microsecond == truth[1] % 1_000_000
    assert f.server.wal_sender_timeout_ms == 60000
    attached = f
    r.stop()
    f = wait_for(lambda: (lambda x: x if x and not x.active else None)(slot.facts(conn, scratch.slot, deadline=D())),
                 timeout=70, what="consumer detached")
    assert f.holder is None and f.active_pid is None
    if MAJOR >= 17:
        assert f.inactive_since is not None
    r2 = consumers(cluster, node, scratch.slot, scratch.pub)
    f2 = wait_for(lambda: (lambda x: x if x and x.active else None)(slot.facts(conn, scratch.slot, deadline=D())),
                  timeout=20, what="consumer attached again")
    assert f2.holder != attached.holder
    res = slot.terminate_holder(conn, scratch.slot, D(15))
    assert res.outcome == "inactive" and res.terminated == (f2.holder,)
    wait_for(lambda: not r2.alive(), timeout=10, what="consumer exited")
    record("slot_facts", "holder_visible_to_runtime_" + node, {
        "pg_read_all_stats": False, "backend_start_precision": "microseconds",
        "application_name": attached.holder_application_name, "backend_type": attached.holder_backend_type,
        "inactive_since_after_detach": f.inactive_since is not None})


def test_facts_server_part_and_advance(cluster, scratch):
    published_table(scratch)
    prim, stby = scratch.connect("primary"), scratch.connect("replica")
    slot.create(stby, scratch.slot, D(60), primary=prim)
    f1 = slot.facts(stby, scratch.slot, deadline=D())
    sp = slot.server_facts(prim, deadline=D())
    ss = f1.server
    assert ss.system_identifier == sp.system_identifier            # physical replicas share it (H-047)
    assert ss.in_recovery and not sp.in_recovery and ss.timeline == sp.timeline == 1
    assert sp.timeline_source == "wal" and ss.timeline_source in ("receiver", "checkpoint")
    assert ss.server_version_num // 10000 == MAJOR and ss.wal_level == "logical" and ss.hot_standby_feedback
    assert ss.primary_slot_name == "cdc_v2_phys_replica" and ss.receive_position is not None
    assert (ss.idle_replication_slot_timeout_s is not None) == (MAJOR >= 18)
    scratch.admin(f"insert into {q(scratch.schema)}.t select g, 'x' from generate_series(1, 100) g")
    cluster.replay_to_primary()
    target = cluster.q("replica", "select pg_last_wal_replay_lsn()::text")[0][0]
    cluster.q("replica", "select pg_replication_slot_advance(%s, %s)", (scratch.slot, target), role="runtime")
    f2 = slot.facts(stby, scratch.slot, deadline=D())
    assert f2.confirmed_flush_lsn == lsnmod.parse(target) > f1.confirmed_flush_lsn
    record("slot_facts", "advance_seen", {"confirmed_flush_moved_bytes": f2.confirmed_flush_lsn -
                                          f1.confirmed_flush_lsn, "timeline_source_standby": ss.timeline_source})


def test_slot_identity_after_drop_and_recreate(cluster, scratch):
    """Design open point 6: is there a fact that tells a recreated slot of the same name from the original?"""
    published_table(scratch)
    prim, stby = scratch.connect("primary"), scratch.connect("replica")
    slot.create(stby, scratch.slot, D(60), primary=prim)
    scratch.admin(f"insert into {q(scratch.schema)}.t select g, 'x' from generate_series(1, 50) g")
    cluster.replay_to_primary()
    pgoutput_changes(cluster, "replica", scratch.slot, scratch.pub)           # consume: stats and positions move
    time.sleep(1.0)
    before = slot.facts(stby, scratch.slot, deadline=D())
    assert slot.drop(stby, scratch.slot, D())
    slot.create(stby, scratch.slot, D(60), primary=prim)
    after = slot.facts(stby, scratch.slot, deadline=D())
    fields = ["restart_lsn", "confirmed_flush_lsn", "catalog_xmin", "inactive_since", "stats_reset", "total_txns",
              "total_bytes", "two_phase_at", "wal_status", "safe_wal_size"]

    def show(v):
        return v.isoformat() if hasattr(v, "isoformat") else v
    cmp = {k: {"original": show(getattr(before, k)), "recreated": show(getattr(after, k)),
               "differs": getattr(before, k) != getattr(after, k)} for k in fields}
    record("slot_identity", "drop_and_recreate", cmp)
    # what holds for any slot: a recreated slot starts at or after the original's positions; its statistics start
    # from zero. Neither is an identity: positions also move forward for the same slot, and statistics are reset by
    # pg_stat_reset_replication_slot() and lost after a crash restart.
    assert after.restart_lsn >= before.restart_lsn and after.total_txns == 0 and before.total_txns > 0


def test_drop_refuses_an_active_slot(cluster, scratch, consumers):
    published_table(scratch)
    stby = scratch.connect("replica")
    slot.create(stby, scratch.slot, D(60), primary=scratch.connect("primary"))
    consumers(cluster, "replica", scratch.slot, scratch.pub)
    wait_for(lambda: slot.facts(stby, scratch.slot, deadline=D()).active, timeout=20, what="active")
    with pytest.raises(slot.SlotActive):
        slot.drop(stby, scratch.slot, D())
    assert slot.drop(stby, scratch.slot, D(20), terminate=True)
    assert slot.facts(stby, scratch.slot, deadline=D()) is None
    assert slot.drop(stby, scratch.slot, D()) is False


def test_terminate_holder_races(cluster, scratch, consumers):
    published_table(scratch)
    stby = scratch.connect("replica")
    slot.create(stby, scratch.slot, D(60), primary=scratch.connect("primary"))

    def attached():
        return wait_for(lambda: (lambda x: x if x and x.active else None)(slot.facts(stby, scratch.slot,
                                                                                   deadline=D())),
                        timeout=20, what="attached")
    # 1. the holder exits by itself between the read and the terminate: nothing else is terminated
    a = consumers(cluster, "replica", scratch.slot, scratch.pub)
    fa = attached()

    def exit_by_itself(f):
        a.stop()
        wait_for(lambda: not slot.facts(stby, scratch.slot, deadline=D()).active, timeout=70, what="released")
    res = slot.terminate_holder(stby, scratch.slot, D(90), _after_read=exit_by_itself)
    assert res.outcome == "inactive" and res.terminated == ()
    # 2. another session takes the slot between the read and the terminate
    a = consumers(cluster, "replica", scratch.slot, scratch.pub)
    fa = attached()
    taken = {}

    def take_over(f):
        if taken:
            return
        a.stop()
        wait_for(lambda: not slot.facts(stby, scratch.slot, deadline=D()).active, timeout=70, what="released")
        taken["b"] = consumers(cluster, "replica", scratch.slot, scratch.pub)
        taken["fb"] = attached()
    res = slot.terminate_holder(stby, scratch.slot, D(90), expect=fa.holder, _after_read=take_over)
    assert res.outcome == "other holder" and res.terminated == () and taken["b"].alive()
    assert res.facts.holder == taken["fb"].holder != fa.holder
    # without `expect` the new holder of our slot is terminated in the next round
    res = slot.terminate_holder(stby, scratch.slot, D(30))
    assert res.outcome == "inactive" and res.terminated == (taken["fb"].holder,)
    record("terminate_holder", "races", {"holder_exits_itself": "nothing terminated",
                                         "other_session_takes_slot": "not terminated by the stale read; "
                                         "terminated in the next round only without expect"})


# ----------------------------------------------------------------------------------------------- breakable pair

def test_invalidation_by_max_slot_wal_keep_size(breakable):
    c = breakable
    c.q("primary", "create table if not exists cdc_v2_inv (id int primary key, v text)")
    c.q("primary", "create publication cdc_v2_pub_inv for table cdc_v2_inv")
    c.replay_to_primary()
    c.lp("set", "replica", "max_slot_wal_keep_size=1MB")
    stby = c.connect("replica")
    prim = c.connect("primary")
    slot.create(stby, "cdc_v2_slot_inv", D(60), primary=prim)
    before = slot.facts(stby, "cdc_v2_slot_inv", deadline=D())
    for i in range(5):
        c.boot("primary", "insert into cdc_v2_inv select g + %s, repeat('x', 100) from generate_series(1, 1000) g",
               (i * 1000,))
        c.boot("primary", "select pg_switch_wal()")
    c.boot("primary", "checkpoint")
    c.replay_to_primary()
    c.boot("replica", "checkpoint")                     # a restartpoint removes WAL and invalidates the slot
    after = wait_for(lambda: (lambda f: f if f.wal_status == "lost" else None)(
        slot.facts(stby, "cdc_v2_slot_inv", deadline=D())), timeout=30, what="slot lost")
    assert before.wal_status in ("reserved", "extended") and after.invalid
    if MAJOR >= 17:
        assert after.invalidation_reason == "wal_removed"
    record("invalidation", "max_slot_wal_keep_size", {
        "before": {"wal_status": before.wal_status, "safe_wal_size": before.safe_wal_size,
                   "invalidation_reason": before.invalidation_reason, "conflicting": before.conflicting},
        "after": {"wal_status": after.wal_status, "safe_wal_size": after.safe_wal_size,
                  "invalidation_reason": after.invalidation_reason, "conflicting": after.conflicting,
                  "restart_lsn": after.restart_lsn}})
    stby.close()
    prim.close()


@pytest.mark.skipif(MAJOR < 18, reason="idle_replication_slot_timeout exists from Postgres 18 on")
def test_invalidation_by_idle_timeout(breakable):
    c = breakable
    c.q("primary", "create table if not exists cdc_v2_idle (id int primary key)")
    c.q("primary", "create publication cdc_v2_pub_idle for table cdc_v2_idle")
    c.replay_to_primary()
    c.lp("set", "all", "idle_replication_slot_timeout=1s")
    stby, prim = c.connect("replica"), c.connect("primary")
    slot.create(stby, "cdc_v2_slot_idle", D(60), primary=prim)
    slot.create(prim, "cdc_v2_slot_idle_p", D(60))
    time.sleep(3)
    c.boot("primary", "insert into cdc_v2_idle values (1)")
    c.boot("primary", "checkpoint")
    c.replay_to_primary()
    c.boot("replica", "checkpoint")
    fp = wait_for(lambda: (lambda f: f if f.invalid else None)(slot.facts(prim, "cdc_v2_slot_idle_p", deadline=D())),
                  timeout=30, what="primary slot invalidated")
    fs = slot.facts(stby, "cdc_v2_slot_idle", deadline=D())
    record("invalidation", "idle_timeout_pg18", {
        "primary_slot": {"invalidation_reason": fp.invalidation_reason, "wal_status": fp.wal_status},
        "standby_slot": {"invalidation_reason": fs.invalidation_reason, "wal_status": fs.wal_status,
                         "note": "a standby slot is invalidated at a restartpoint"}})
    assert fp.invalidation_reason == "idle_timeout"
    stby.close()
    prim.close()


def test_prepared_transaction_blocks_creation_on_standby(breakable):
    c = breakable
    c.lp("set", "all", "max_prepared_transactions=5")
    c.q("primary", "create table if not exists cdc_v2_prep (id int primary key)")
    c.q("primary", "create publication cdc_v2_pub_prep for table cdc_v2_prep")
    c.replay_to_primary()
    with c.raw("primary", "admin") as tx:
        tx.execute("begin")
        tx.execute("insert into cdc_v2_prep values (1)")
        tx.execute("prepare transaction 'cdc_v2_gid'")
    try:
        stby, prim = c.connect("replica", cancel_grace_s=0.5), c.connect("primary")
        with pytest.raises(slot.SlotCreateTimeout) as ei:
            slot.create(stby, "cdc_v2_slot_prep", D(6), primary=prim)
        rep = ei.value.report
        assert rep["prepared_transactions"] == 1 and rep["leftover"] is False
    finally:
        c.q("primary", "commit prepared 'cdc_v2_gid'")
    stby = c.connect("replica")
    created = slot.create(stby, "cdc_v2_slot_prep", D(60), primary=c.connect("primary"))
    record("slot_create", "prepared_transaction", {"timed_out_report": rep,
                                                   "after_commit_prepared_s": round(created.seconds, 2)})


def test_timeline_history_after_a_real_promotion(breakable):
    """H-029, H-045: positions before the switch, after it on the old timeline, and on the new timeline."""
    c = breakable
    c.q("primary", "create table if not exists cdc_v2_tl (id int primary key)")
    c.q("primary", "insert into cdc_v2_tl values (1)")
    p_before = lsnmod.parse(c.boot("primary", "select pg_current_wal_insert_lsn()::text")[0][0])
    c.replay_to_primary()
    h1 = slot.timeline_history(c.connect("replica"), deadline=D())
    assert h1.timeline == 1 and h1.entries == ()
    out = json.loads(c.lp("promote", "replica", "--json").stdout)
    assert out["new_timeline"] == 2
    # the old primary is still running (localpg does not fence it): it writes on timeline 1 past the switch point
    for i in range(2, 50):
        c.q("primary", "insert into cdc_v2_tl values (%s)", (i,))
    p_old_after = lsnmod.parse(c.boot("primary", "select pg_current_wal_insert_lsn()::text")[0][0])
    c.q("replica", "insert into cdc_v2_tl values (1000)")
    p_new = lsnmod.parse(c.boot("replica", "select pg_current_wal_insert_lsn()::text")[0][0])
    new_primary = c.connect("replica")                     # runtime role, replication=database connection inside
    h = slot.timeline_history(new_primary, deadline=D())
    assert h.timeline == 2 and len(h.entries) == 1 and h.entries[0].timeline == 1
    switch = h.entries[0].switch
    assert p_before <= switch < p_old_after
    assert timeline.contains(h, p_before, 1)
    assert not timeline.contains(h, p_old_after, 1)
    assert timeline.contains(h, p_new, 2)
    sf = slot.server_facts(new_primary, deadline=D())
    assert sf.timeline == 2 and not sf.in_recovery
    record("timeline", "promotion", {"switch": lsnmod.format(switch), "before": lsnmod.format(p_before),
                                     "old_timeline_after_switch": lsnmod.format(p_old_after),
                                     "new_timeline": lsnmod.format(p_new),
                                     "contains": {"before": True, "old_after": False, "new": True},
                                     "runtime_role_over_replication_connection": True})
    new_primary.close()
