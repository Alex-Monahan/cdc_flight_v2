"""Package P: publication, replica identity and heartbeat on the private pair (marker localpg)."""
from __future__ import annotations

import threading
import time

import psycopg
import pytest

from cdc_v2 import lsn as lsnmod
from cdc_v2.deadline import Deadline
from cdc_v2.pg import catalog, errors, heartbeat, publication, replica_identity, slot

from .conftest import pgoutput_changes, record, wait_for

pytestmark = [pytest.mark.localpg, pytest.mark.timeout(300)]


def q(name):
    return '"' + name.replace('"', '""') + '"'


def D(s=10):
    return Deadline.after(s)


def test_ensure_creates_adds_only_and_is_idempotent(cluster, scratch):
    s = q(scratch.schema)
    scratch.admin(f"create table {s}.a (id int primary key); create table {s}.b (id int primary key)")
    setup = scratch.connect("primary", role="admin")
    r1 = publication.ensure(setup, [(scratch.schema, "a")], publication=scratch.pub, deadline=D())
    assert r1.created and r1.added == ((scratch.schema, "a"),) and r1.next_xid > 0
    st = r1.state
    assert st.exists and st.problems == [] and st.publish == publication.PUBLISH and not st.via_root
    r2 = publication.ensure(setup, [(scratch.schema, "a"), (scratch.schema, "b")], publication=scratch.pub,
                            deadline=D())
    assert not r2.created and r2.added == ((scratch.schema, "b"),) and r2.present == ((scratch.schema, "a"),)
    assert r2.next_xid >= r1.next_xid
    r3 = publication.ensure(setup, [(scratch.schema, "b")], publication=scratch.pub, deadline=D())
    assert r3.added == () and r3.present == ((scratch.schema, "b"),)
    # ensure never removes: a table not named stays in
    assert {(t.schema, t.name) for t in publication.state(setup, publication=scratch.pub, deadline=D()).tables} == {
        (scratch.schema, "a"), (scratch.schema, "b")}
    assert publication.remove(setup, [(scratch.schema, "a")], publication=scratch.pub, deadline=D()) == 1
    assert not publication.state(setup, publication=scratch.pub, deadline=D()).has(scratch.schema, "a")


def test_role_split_runtime_cannot_maintain_the_publication(cluster, scratch):
    scratch.admin(f"create table {q(scratch.schema)}.a (id int primary key)")
    rt = scratch.connect("primary")
    with pytest.raises(errors.PermissionDenied):
        publication.ensure(rt, [(scratch.schema, "a")], publication=scratch.pub, deadline=D())
    with pytest.raises(errors.PermissionDenied):
        replica_identity.set_full(rt, (scratch.schema, "a"), D())
    # but it can read the state (catalog views are public)
    assert not publication.state(rt, publication=scratch.pub, deadline=D()).exists


def test_concurrent_ensure_calls_are_harmless(cluster, scratch):
    s = q(scratch.schema)
    for i in range(6):
        scratch.admin(f"create table {s}.t{i} (id int primary key)")
    tables = [(scratch.schema, "t%d" % i) for i in range(6)]
    results, errs = [], []

    def run():
        try:
            with cluster.connect("primary", role="admin") as c:
                results.append(publication.ensure(c, tables, publication=scratch.pub, deadline=D(30)))
        except Exception as e:      # noqa: BLE001
            errs.append(e)
    ths = [threading.Thread(target=run) for _ in range(4)]
    for t in ths:
        t.start()
    for t in ths:
        t.join(60)
    assert not errs, errs
    st = publication.state(scratch.connect("primary"), publication=scratch.pub, deadline=D())
    assert sorted((t.schema, t.name) for t in st.tables) == tables
    assert sum(1 for r in results if r.created) <= 1


def test_replica_identity_guard_and_the_wrong_order(cluster, scratch):
    s = q(scratch.schema)
    scratch.admin(f"""create table {s}.nokey (x int); insert into {s}.nokey values (1);
                     create table {s}.idx (x int not null); create unique index on {s}.idx (x);
                     insert into {s}.idx values (1);
                     create unlogged table {s}.unl (x int primary key)""")
    scratch.admin(f"alter table {s}.idx replica identity using index idx_x_idx")
    setup = scratch.connect("primary", role="admin")
    with pytest.raises(publication.NotPublishable) as ei:
        publication.ensure(setup, [(scratch.schema, "nokey"), (scratch.schema, "idx"), (scratch.schema, "unl")],
                           publication=scratch.pub, deadline=D())
    assert set(ei.value.problems) == {(scratch.schema, "nokey"), (scratch.schema, "idx"), (scratch.schema, "unl")}
    assert "FULL" in ei.value.problems[(scratch.schema, "idx")]
    st = publication.state(setup, publication=scratch.pub, deadline=D())
    assert st.tables == ()                                     # nothing was added
    # the wrong order (H-014): published without FULL, the source's own UPDATE and DELETE fail
    scratch.admin(f"alter publication {q(scratch.pub)} add table {s}.nokey")
    with pytest.raises(psycopg.errors.ObjectNotInPrerequisiteState, match="replica identity"):
        scratch.admin(f"update {s}.nokey set x = 2")
    with pytest.raises(psycopg.errors.ObjectNotInPrerequisiteState):
        scratch.admin(f"delete from {s}.nokey")
    scratch.admin(f"alter publication {q(scratch.pub)} drop table {s}.nokey")
    # the right order
    assert replica_identity.set_full(setup, (scratch.schema, "nokey"), D())
    assert not replica_identity.set_full(setup, (scratch.schema, "nokey"), D())       # already FULL
    assert replica_identity.set_full(setup, (scratch.schema, "idx"), D())
    r = publication.ensure(setup, [(scratch.schema, "nokey"), (scratch.schema, "idx")], publication=scratch.pub,
                           deadline=D())
    assert len(r.added) == 2
    scratch.admin(f"update {s}.nokey set x = 3; delete from {s}.idx")      # the source's writes work
    obs = catalog.observe(setup, publication=scratch.pub, deadline=D())
    assert all(t.replica_identity_problem is None for t in obs.tables.values())


def test_set_full_lock_timeout(cluster, scratch):
    s = q(scratch.schema)
    scratch.admin(f"create table {s}.busy (x int)")
    with cluster.raw("primary", "admin") as holder:
        holder.execute("begin")
        holder.execute(f"select * from {s}.busy")                 # ACCESS SHARE blocks ACCESS EXCLUSIVE
        setup = scratch.connect("primary", role="admin")
        t = time.monotonic()
        with pytest.raises(errors.LockTimeout):
            replica_identity.set_full(setup, (scratch.schema, "busy"), D(), lock_timeout_ms=300)
        assert time.monotonic() - t < 3
        holder.execute("rollback")
    assert replica_identity.set_full(setup, (scratch.schema, "busy"), D())


def test_publication_recreated_is_seen_by_oid(cluster, scratch):
    scratch.admin(f"create table {q(scratch.schema)}.a (id int primary key)")
    setup = scratch.connect("primary", role="admin")
    o1 = publication.ensure(setup, [(scratch.schema, "a")], publication=scratch.pub, deadline=D()).state.oid
    scratch.admin(f"drop publication {q(scratch.pub)}")
    o2 = publication.ensure(setup, [(scratch.schema, "a")], publication=scratch.pub, deadline=D()).state.oid
    assert o1 != o2


def test_publication_add_under_concurrent_writers_and_next_xid(cluster, scratch):
    """Design 7.5: every transaction with an id >= next_xid that writes the table is published; a transaction that
    wrote before the change and commits after it is not, and its id is < next_xid."""
    s = q(scratch.schema)
    scratch.admin(f"create table {s}.w (id bigserial primary key, writer int, xid bigint)")
    scratch.admin(f"create table {s}.other (id int primary key)")
    setup = scratch.connect("primary", role="admin")
    publication.ensure(setup, [(scratch.schema, "other")], publication=scratch.pub, deadline=D())
    cluster.q("primary", "select pg_create_logical_replication_slot(%s, 'pgoutput')", (scratch.slot,), role="runtime")
    stop = threading.Event()
    written = []          # (xid, writer) of committed transactions
    lock = threading.Lock()

    def writer(k):
        with cluster.raw("primary", "admin") as w:
            while not stop.is_set():
                w.execute("begin")
                x = w.execute(f"insert into {s}.w (writer, xid) values (%s, pg_current_xact_id()::text::bigint) "
                              "returning xid", (k,)).fetchone()[0]
                w.execute("commit")
                with lock:
                    written.append((x, k))
                time.sleep(0.002)

    long_tx = cluster.raw("primary", "admin")
    long_tx.execute("begin")
    long_xid = long_tx.execute(f"insert into {s}.w (writer, xid) values (-1, pg_current_xact_id()::text::bigint) "
                               "returning xid").fetchone()[0]
    ths = [threading.Thread(target=writer, args=(k,)) for k in range(3)]
    for t in ths:
        t.start()
    time.sleep(0.5)
    r = publication.ensure(setup, [(scratch.schema, "w")], publication=scratch.pub, deadline=D())
    time.sleep(0.5)
    long_tx.execute(f"insert into {s}.w (writer, xid) values (-2, %s)", (long_xid,))
    long_tx.execute("commit")
    long_tx.close()
    stop.set()
    for t in ths:
        t.join(10)
    msgs = pgoutput_changes(cluster, "primary", scratch.slot, scratch.pub)
    decoded_xids = set()
    long_rows = []
    cur = None
    for m in msgs:
        if m["kind"] == "B":
            cur = m["xid"]
        elif m["kind"] == "I":
            decoded_xids.add(cur)
            if cur == (long_xid & 0xFFFFFFFF):
                long_rows.append(m["tuple"][-1][1][1])
    X = r.next_xid
    after = [x for x, _ in written if x >= X]
    before = [x for x, _ in written if x < X]
    assert after and all((x & 0xFFFFFFFF) in decoded_xids for x in after)
    # the transaction open across the change has an id below next_xid, so a snapshot with xmin >= next_xid waits for
    # it. Measured: pgoutput publishes ALL its rows, also the one written before the change (design 7.5 says the
    # opposite); the snapshot rule stays correct, the cut then sees those rows as inside the snapshot.
    assert long_xid < X
    record("publication", "add_under_writers", {
        "committed_writer_transactions": len(written), "with_xid_ge_next_xid": len(after),
        "all_of_them_decoded": True, "before_next_xid": len(before),
        "before_next_xid_decoded": sum(1 for x in before if (x & 0xFFFFFFFF) in decoded_xids),
        "open_across_change_rows_decoded": long_rows,
        "open_across_change_note": "-1 was written before the ALTER PUBLICATION, -2 after; both were decoded"})


# ----------------------------------------------------------------------------------------------- heartbeat

def test_heartbeat_write_is_published_and_recognisable(cluster, scratch):
    hs = "cdc_v2_hb_" + scratch.tag
    try:
        for stmt in heartbeat.setup_sql(scratch.runtime, schema=hs):
            scratch.admin(stmt)
        setup = scratch.connect("primary", role="admin")
        publication.ensure(setup, [(hs, heartbeat.TABLE)], publication=scratch.pub, deadline=D())
        cluster.replay_to_primary()
        rt_primary = scratch.connect("primary")
        rt_standby = scratch.connect("replica")
        slot.create(rt_standby, scratch.slot, D(60), primary=rt_primary)
        beats = [heartbeat.beat(rt_primary, scratch.slot, schema=hs, deadline=D()) for _ in range(3)]
        other = heartbeat.beat(cluster.connect("primary"), scratch.slot, schema=hs, deadline=D())  # a second run
        assert len({b.marker for b in beats + [other]}) == 4 and [b.n for b in beats] == [1, 2, 3]
        for b in beats:
            assert b.lower < b.upper
        cluster.replay_to_primary()
        msgs = pgoutput_changes(cluster, "replica", scratch.slot, scratch.pub)
        seen = {}
        cur, markers = None, []
        for m in msgs:
            if m["kind"] == "B":
                cur, markers = m, []
            elif m["kind"] in "IU":
                markers.append(m["tuple"][-1][1][1])
            elif m["kind"] == "C":
                for mk in markers:
                    seen[mk] = (cur["xid"], m["commit_lsn"], m["end_lsn"])
        commits = {v[2] for v in seen.values()}
        for b in beats + [other]:
            xid, start, end = seen[b.marker]
            assert xid == (b.xid & 0xFFFFFFFF)
            assert b.lower <= start < end <= b.upper     # the commit record lies in the returned bracket
        lag = heartbeat.standby_lag(rt_primary, rt_standby, deadline=D())
        assert lag.bytes_behind is not None and lag.bytes_behind >= 0 and lag.replay_paused == "not paused"
        assert lag.recovery_min_apply_delay_ms == 0
        record("heartbeat", "decoded", {"beats": 4, "recognised_by_marker_and_xid": True,
                                        "commit_in_bracket": True, "standby_lag": {
                                            "bytes_behind": lag.bytes_behind,
                                            "seconds_since_replayed_commit": lag.seconds_since_replayed_commit,
                                            "replay_lag_s_visible_to_runtime": lag.replay_lag_s is not None}})
        assert len(commits) >= 4
    finally:
        cluster.cleanup_slots(scratch.slot)
        scratch.admin(f"drop schema if exists {q(hs)} cascade")


def test_heartbeat_advances_an_idle_slot_behind_unpublished_traffic(cluster, scratch):
    """Only unpublished tables are busy; the heartbeat transaction is what a consumer can confirm past."""
    hs = "cdc_v2_hb_" + scratch.tag
    try:
        for stmt in heartbeat.setup_sql(scratch.runtime, schema=hs):
            scratch.admin(stmt)
        scratch.admin(f"create table {q(scratch.schema)}.busy (id serial primary key, v text)")
        setup = scratch.connect("primary", role="admin")
        publication.ensure(setup, [(hs, heartbeat.TABLE)], publication=scratch.pub, deadline=D())
        cluster.replay_to_primary()
        rt_primary = scratch.connect("primary")
        rt_standby = scratch.connect("replica")
        slot.create(rt_standby, scratch.slot, D(60), primary=rt_primary)
        scratch.admin(f"insert into {q(scratch.schema)}.busy (v) select repeat('x', 100) from generate_series(1, 20000)")
        b = heartbeat.beat(rt_primary, scratch.slot, schema=hs, deadline=D())
        cluster.replay_to_primary()
        msgs = pgoutput_changes(cluster, "replica", scratch.slot, scratch.pub)
        assert [m for m in msgs if m["kind"] == "I" or m["kind"] == "U"]           # only the heartbeat row
        f = slot.facts(rt_standby, scratch.slot, deadline=D())
        assert f.confirmed_flush_lsn >= b.lower
    finally:
        cluster.cleanup_slots(scratch.slot)
        scratch.admin(f"drop schema if exists {q(hs)} cascade")


def test_heartbeat_cost(cluster, scratch):
    """WAL bytes per heartbeat write and the table size after many writes (one row per slot)."""
    hs = "cdc_v2_hb_" + scratch.tag
    try:
        for stmt in heartbeat.setup_sql(scratch.runtime, schema=hs):
            scratch.admin(stmt)
        setup = scratch.connect("primary", role="admin")
        publication.ensure(setup, [(hs, heartbeat.TABLE)], publication=scratch.pub, deadline=D())
        prim = scratch.connect("primary")
        heartbeat.beat(prim, scratch.slot, schema=hs, deadline=D())
        n = 200
        start = heartbeat.beat(prim, scratch.slot, schema=hs, deadline=D()).upper
        for _ in range(n):
            last = heartbeat.beat(prim, scratch.slot, schema=hs, deadline=D())
        per_write = (last.upper - start) / n
        heartbeat.beat(prim, scratch.slot + "_other", schema=hs, deadline=D())      # a second slot: a second row
        rows = scratch.admin(f"select count(*), pg_relation_size('{q(hs)}.heartbeat') from {q(hs)}.heartbeat")[0]
        time.sleep(0.7)                                                       # statistics are flushed lazily
        hot = scratch.admin("select n_tup_upd, n_tup_hot_upd from pg_stat_user_tables "
                            "where schemaname = %s and relname = 'heartbeat'", (hs,))
        record("heartbeat", "cost", {"writes": n, "wal_bytes_per_write_upper_bound": round(per_write, 1),
                                     "rows_after_writes_two_slots": rows[0], "table_bytes": rows[1],
                                     "updates_and_hot_updates": list(hot[0]) if hot else None,
                                     "note": "insert-position difference on an otherwise idle primary, divided "
                                             "by the writes; includes each transaction's commit record"})
        assert rows[0] == 2 and per_write < 1000
    finally:
        scratch.admin(f"drop schema if exists {q(hs)} cascade")
