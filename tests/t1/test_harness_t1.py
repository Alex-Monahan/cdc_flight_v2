"""The T1 harness against real servers: isolation of private databases, private clusters for drills, the workload
generator's replay log and quiesce points, and waiting for an applied position on MotherDuck."""
import threading
import time

import psycopg
import pytest

from cdc_v2 import lsn
from cdc_v2.deadline import Deadline, DeadlineExceeded
from cdc_v2.oracle import OracleConfig, PositionNotReached, compare, wait_for_applied
from cdc_v2.testing import loader
from cdc_v2.testing import md as tmd
from cdc_v2.testing import pg as tpg
from cdc_v2.testing.md import STAND_IN_SLOT, write_stand_in_position
from cdc_v2.testing.workload import Features, Workload, WorkloadConfig, per_writer_statements, read_log, replay

pytestmark = [pytest.mark.t1, pytest.mark.timeout(900)]
FACTS = {}


@pytest.fixture(scope="module", autouse=True)
def write_facts(evidence):
    yield
    evidence("p3/oracle").write_json("harness", FACTS)


def test_private_databases_are_isolated(make_source_db, shared_pg):
    a, b = make_source_db(), make_source_db()
    assert a.name != b.name and a.name.startswith("cdc_v2_t_") and b.name.startswith("cdc_v2_t_")
    a.sql("CREATE TABLE only_in_a (x int)")
    assert b.sql("SELECT count(*) FROM pg_class WHERE relname = 'only_in_a'")[0][0] == 0
    # the runtime role can use a private database
    a.sql("GRANT SELECT ON only_in_a TO cdc_v2_runtime")
    assert a.sql("SELECT count(*) FROM only_in_a", role="runtime")[0][0] == 0
    FACTS["private_databases"] = {"isolated": True, "cluster_ports": [shared_pg.primary_port, shared_pg.replica_port]}


def test_private_database_drop_removes_it_and_its_slots(shared_pg):
    db = tpg.PrivateDatabase(shared_pg)
    slot = db.name + "_slot"
    db.sql("SELECT pg_create_logical_replication_slot(%s, 'pgoutput')", (slot,))
    db.drop()
    assert shared_pg.sql("SELECT count(*) FROM pg_database WHERE datname = %s", (db.name,))[0][0] == 0
    assert shared_pg.sql("SELECT count(*) FROM pg_replication_slots WHERE slot_name = %s", (slot,))[0][0] == 0
    FACTS["private_database_drop"] = {"database_gone": True, "logical_slot_gone": True}


def test_drop_terminates_the_holder_of_a_slot_in_use(shared_pg):
    """A slot in use cannot be dropped, and then its database cannot be dropped: drop() terminates the holder."""
    import os
    import subprocess
    db = tpg.PrivateDatabase(shared_pg)
    slot = db.name + "_held"
    db.sql("SELECT pg_create_logical_replication_slot(%s, 'pgoutput')", (slot,))
    db.sql("CREATE PUBLICATION cdc_v2_o_pub")                     # empty: nothing is ever decoded
    p = db.params()
    env = {k: v for k, v in os.environ.items() if not k.startswith("PG")}
    env.update(PGPASSWORD=p["password"], PGSSLMODE="verify-full", PGSSLROOTCERT=p["sslrootcert"])
    holder = subprocess.Popen([os.path.join(os.environ.get("CDC_V2_PG_BIN", "/opt/homebrew/opt/postgresql@18/bin"),
                                            "pg_recvlogical"), "-h", "localhost", "-p", str(p["port"]), "-U", p["user"],
                               "-d", db.name, "-S", slot, "--start", "--no-loop", "-f", "/dev/null",
                               "-o", "proto_version=1", "-o", "publication_names=cdc_v2_o_pub"],
                              env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    try:
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline and not shared_pg.sql(
                "SELECT active FROM pg_replication_slots WHERE slot_name = %s", (slot,))[0][0]:
            time.sleep(0.1)
        assert shared_pg.sql("SELECT active FROM pg_replication_slots WHERE slot_name = %s", (slot,))[0][0]
        db.drop()
        assert shared_pg.sql("SELECT count(*) FROM pg_database WHERE datname = %s", (db.name,))[0][0] == 0
        assert shared_pg.sql("SELECT count(*) FROM pg_replication_slots WHERE slot_name = %s", (slot,))[0][0] == 0
        assert holder.wait(timeout=30) is not None
    finally:
        if holder.poll() is None:
            holder.kill()
            holder.wait(timeout=10)
    FACTS["drop_with_slot_in_use"] = {"slot_was_active": True, "database_gone": True, "slot_gone": True}


def test_private_cluster_for_destructive_drills(private_cluster):
    c = private_cluster
    base = c.port_base
    assert base >= tpg.PRIVATE_BASE and (base - tpg.PRIVATE_BASE) % tpg.PRIVATE_STEP == 0
    assert {c.primary_port, c.replica_port} <= set(range(base, base + 8))
    assert c.home != tpg.SHARED_HOME
    c.sql("CREATE TABLE drill (x int)")
    c.sql("INSERT INTO drill VALUES (1)")
    c.wait_replica_caught_up()
    assert c.sql("SELECT count(*) FROM drill", node="replica")[0][0] == 1
    # destructive commands are allowed on a private cluster: crash the replica and bring it back
    c.localpg("kill", "replica")
    st = {n["name"]: n for n in c.status()["nodes"]}
    assert not st["replica"]["pid"]
    c.localpg("up")
    c.wait_replica_caught_up()
    assert c.sql("SELECT count(*) FROM drill", node="replica")[0][0] == 1
    FACTS["private_cluster"] = {"port_base": base, "kill_and_recover_replica": True}


def test_private_cluster_is_removed_and_its_ports_released():
    c = tpg.PrivateCluster()
    home, base = c.home, c.port_base
    assert (home / ".localpg-home").exists() and tpg._port_in_use(c.primary_port)
    c.stop()
    assert not home.exists()
    assert not any(tpg._port_in_use(p) for p in range(base, base + 8))
    again = tpg.PortLease.acquire(start=base, blocks=1)
    try:
        assert again.base == base                     # the block can be leased again
    finally:
        again.release()
    FACTS["private_cluster_cleanup"] = {"home_removed": True, "ports_free": True, "lease_reusable": True}


def test_two_private_clusters_get_different_blocks():
    a = tpg.PrivateCluster()
    try:
        b = tpg.PrivateCluster()
        try:
            assert a.port_base != b.port_base and abs(a.port_base - b.port_base) >= 8
        finally:
            b.stop()
    finally:
        a.stop()


# ------------------------------------------------------------------------------------------------ workload

ALL = Features(large_txn=True, truncate=True, ddl=True, transfers=True)


def test_workload_replay_reproduces_the_source(make_source_db, dest_db, tmp_path):
    a, b = make_source_db(), make_source_db()
    log = tmp_path / "replay.jsonl"
    cfg = WorkloadConfig(seed=101, writers=3, txns_per_round=25, features=ALL, large_txn_rows=3000,
                         large_txn_prob=0.03, truncate_prob=0.1, ddl_prob=0.6)
    wl = Workload(cfg, connect=a.connect, log_path=log)
    wl.setup()
    wl.run(8)
    wl.quiesce()
    wl.close()
    seen = replay(log, b.connect)
    # the same seed, planned without a database, gives the same statements per writer
    same_plan = per_writer_statements(Workload(cfg).plan(8)) == per_writer_statements(read_log(log))
    q = lsn.parse(b.sql("SELECT pg_current_wal_insert_lsn()::text")[0][0])
    loader.load_database(a.params(), dest_db.conn)
    write_stand_in_position(dest_db.conn, q)
    r = compare(b.source(), dest_db.destination(), OracleConfig(chunk_rows=200), position=q, slot_name=STAND_IN_SLOT)
    rows = {t.source: t.source_rows for t in r.tables}
    FACTS["workload_replay"] = {"config": cfg.to_dict(), "stats": dict(wl.stats), "log_entries": dict(seen),
                                "plan_equals_log_per_writer": same_plan, "replayed_equals_original": r.equal,
                                "rows": rows}
    assert same_plan
    assert r.equal, r.summary()
    assert sum(rows.values()) > 0
    for k in ("pk_chain", "pk_swap", "txn_savepoint", "txn_rollback", "transfer", "large_txn"):
        assert wl.stats[k] > 0, k


def test_quiesce_points_in_continuous_mode(source_db, dest_db, tmp_path):
    """Writers pause at a round boundary; the source is then still; the oracle compares there."""
    cfg = WorkloadConfig(seed=202, writers=2, txns_per_round=10, features=Features(transfers=True))
    wl = Workload(cfg, connect=source_db.connect, log_path=tmp_path / "log.jsonl")
    wl.setup()
    wl.start()
    time.sleep(1.0)
    lsn1 = wl.pause()
    q = "SELECT count(*), sum(hashtext(t::text)) FROM (SELECT * FROM w_k0 UNION ALL SELECT * FROM w_k1) t"
    before = source_db.sql(q)
    time.sleep(0.5)
    assert source_db.sql(q) == before                     # nothing is written while paused
    # a stand-in for the pipeline: copy the quiesced source, record its position, wait for it, compare
    loader.load_database(source_db.params(), dest_db.conn)
    write_stand_in_position(dest_db.conn, lsn1)
    assert wait_for_applied(dest_db.conn, lsn1, Deadline.after(30), STAND_IN_SLOT) == lsn1
    r = compare(source_db.source(), dest_db.destination(), OracleConfig(), position=lsn1, slot_name=STAND_IN_SLOT)
    wl.resume()
    time.sleep(0.5)
    lsn2 = wl.pause()
    wl.stop()
    wl.close()
    FACTS["quiesce"] = {"first": lsn.format(lsn1), "second": lsn.format(lsn2), "equal_at_first": r.equal,
                        "rounds": wl.round}
    assert r.equal, r.summary()
    assert lsn2 > lsn1


def test_wait_for_applied_on_motherduck(dest_db):
    write_stand_in_position(dest_db.conn, 0x100, slot_name="s")
    write_stand_in_position(dest_db.conn, 1 << 40, slot_name="another_slot")     # must not count for "s"
    target = (1 << 32) + 0x2000

    def advance():
        time.sleep(1.5)
        w = dest_db.connect()
        w.execute("UPDATE _cdc.stream_state SET applied_lsn = ? WHERE slot_name = 's'", [target])
        w.close()

    t = threading.Thread(target=advance)
    t0 = time.monotonic()
    t.start()
    got = wait_for_applied(dest_db.connect(), target, Deadline.after(60), "s", poll=0.2)
    waited = time.monotonic() - t0
    t.join()
    with pytest.raises(DeadlineExceeded):
        wait_for_applied(dest_db.connect(), 2 << 32, Deadline.after(1.0), "s", poll=0.2)
    FACTS["wait_for_applied"] = {"returned": lsn.format(got), "waited_seconds": round(waited, 2)}
    assert got == target and waited >= 1.4


def test_no_test_database_is_left_on_motherduck_by_earlier_runs():
    """Ledger entries of databases that were created and never dropped (checked by attaching each by name)."""
    left = tmd.leftovers()
    FACTS["motherduck_leftovers_at_check"] = left
    assert all(n.startswith(tmd.prefix()) for n in left)


def test_lost_last_transaction_on_a_lagging_standby_is_not_equal(private_cluster, dest_db, evidence):
    """Contract C8: the comparison is tied to the position Q. The pipeline loses the last transaction T but reports
    Q as applied; the standby has not replayed T yet. Without the position both sides lack T and the oracle would say
    EQUAL; with it the comparison is refused until the standby has replayed Q, and then reports the loss."""
    c = private_cluster
    db = tpg.PrivateDatabase(c)
    try:
        db.sql("CREATE TABLE t (id int PRIMARY KEY, v text)")
        db.sql("INSERT INTO t SELECT g, 'v' || g FROM generate_series(1, 10) g")
        c.wait_replica_caught_up()
        loader.load_database(db.params(), dest_db.conn)                 # the pipeline applied the first ten rows
        c.sql("SELECT pg_wal_replay_pause()", role="bootstrap", node="replica")
        db.sql("INSERT INTO t VALUES (11, 'lost')")                       # T: committed on the primary ...
        q = lsn.parse(db.sql("SELECT pg_current_wal_insert_lsn()::text")[0][0])
        write_stand_in_position(dest_db.conn, q)                          # ... and lost by the pipeline
        replica = db.source(node="replica")
        unpositioned = compare(replica, dest_db.destination(), OracleConfig(), unpositioned=True)
        t0 = time.monotonic()
        with pytest.raises(PositionNotReached, match="standby replayed only") as refused:
            compare(replica, dest_db.destination(), OracleConfig(position_wait_s=3), position=q,
                    slot_name=STAND_IN_SLOT)
        waited = time.monotonic() - t0
        c.sql("SELECT pg_wal_replay_resume()", role="bootstrap", node="replica")
        after = compare(replica, dest_db.destination(), OracleConfig(position_wait_s=30), position=q,
                        slot_name=STAND_IN_SLOT)
        evidence("p3/oracle").write_json("lost_transaction", {
            "what": "the last source transaction is lost by a stand-in pipeline that still reports its position; "
                    "the oracle reads a standby whose replay is paused (private cluster)",
            "position": lsn.format(q),
            "unpositioned_on_lagging_standby": {"equal": unpositioned.equal,
                                                "note": "the hazard: both sides lack the lost row"},
            "positioned_on_lagging_standby": {"refused": str(refused.value), "seconds": round(waited, 1)},
            "positioned_after_replay": {"equal": after.equal, "source_position": after.source_position,
                                        "destination_position": after.destination_position,
                                        "missing_rows": after.tables[0].missing_rows,
                                        "sample": after.tables[0].sample["missing"]}})
        assert unpositioned.equal                                        # what the oracle said before the fix
        assert not after.equal and after.tables[0].missing_rows == 1
        assert after.tables[0].sample["missing"][0]["id"] == "11"
    finally:
        c.sql("SELECT pg_wal_replay_resume()", role="bootstrap", node="replica")
        db.drop()
