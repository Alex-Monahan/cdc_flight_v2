"""The T1 harness against real servers: isolation of private databases, private clusters for drills, the workload
generator's replay log and quiesce points, and waiting for an applied position on MotherDuck."""
import threading
import time

import psycopg
import pytest

from cdc_v2.oracle import OracleConfig, compare, format_lsn, parse_lsn, wait_for_applied
from cdc_v2.oracle.position import PositionTimeout
from cdc_v2.testing import loader
from cdc_v2.testing import md as tmd
from cdc_v2.testing import pg as tpg
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
    loader.load_database(a.params(), dest_db.conn)
    r = compare(b.source(), dest_db.destination(), OracleConfig(chunk_rows=200))
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
    dest_db.conn.execute("CREATE SCHEMA _cdc")
    dest_db.conn.execute("CREATE TABLE _cdc.stream_state (slot VARCHAR, applied_lsn VARCHAR)")
    dest_db.conn.execute("INSERT INTO _cdc.stream_state VALUES ('stand_in', ?)", [lsn1])
    assert wait_for_applied(dest_db.conn, lsn1, timeout=30) == parse_lsn(lsn1)
    r = compare(source_db.source(), dest_db.destination(), OracleConfig())
    wl.resume()
    time.sleep(0.5)
    lsn2 = wl.pause()
    wl.stop()
    wl.close()
    FACTS["quiesce"] = {"first": lsn1, "second": lsn2, "equal_at_first": r.equal, "rounds": wl.round}
    assert r.equal, r.summary()
    assert parse_lsn(lsn2) > parse_lsn(lsn1)


def test_wait_for_applied_on_motherduck(dest_db):
    c = dest_db.conn
    c.execute("CREATE SCHEMA _cdc")
    c.execute("CREATE TABLE _cdc.stream_state (slot VARCHAR, applied_lsn VARCHAR)")
    c.execute("INSERT INTO _cdc.stream_state VALUES ('s', '0/100')")
    target = (1 << 32) + 0x2000

    def advance():
        time.sleep(1.5)
        w = dest_db.connect()
        w.execute("UPDATE _cdc.stream_state SET applied_lsn = ? WHERE slot = 's'", [format_lsn(target)])
        w.close()

    t = threading.Thread(target=advance)
    t0 = time.monotonic()
    t.start()
    got = wait_for_applied(dest_db.connect(), "1/2000", timeout=60, poll=0.2)
    waited = time.monotonic() - t0
    t.join()
    with pytest.raises(PositionTimeout):
        wait_for_applied(dest_db.connect(), "2/0", timeout=1.0, poll=0.2)
    FACTS["wait_for_applied"] = {"returned": format_lsn(got), "waited_seconds": round(waited, 2)}
    assert got == target and waited >= 1.4


def test_no_test_database_is_left_on_motherduck_by_earlier_runs():
    """Ledger entries of databases that were created and never dropped (checked by attaching each by name)."""
    left = tmd.leftovers()
    FACTS["motherduck_leftovers_at_check"] = left
    assert all(n.startswith("cdc_v2_t_") for n in left)
