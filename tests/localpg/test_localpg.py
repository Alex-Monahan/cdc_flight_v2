"""Self-tests of tools/localpg (PLAN §5, phase P1). Marker: localpg."""
import json
import os
import shutil
import signal
import stat
import threading
import time
import uuid

import psycopg
import pytest

from conftest import (LOCALPG, PG_BIN, PG_MAJOR, TEST_BASE, TEST_HOME, Cluster, alive, child_pids, lp, lp_env,
                      needs_17, port_open, wait_for)

pytestmark = [pytest.mark.localpg, pytest.mark.timeout(300)]


def uniq(prefix):
    return "%s_%s" % (prefix, uuid.uuid4().hex[:8])


def admin_table(c, name):
    c.q("primary", "create table %s (id bigint primary key, v text)" % name, role="admin")


def replica_has(c, table, id_, node="replica"):
    return wait_for(lambda: c.q(node, "select v from %s where id = %%s" % table, (id_,), role="admin"),
                    what="row %s visible on %s" % (id_, node))


def create_slot_on_standby(c, standby, slot, primary="primary"):
    """Logical slot creation on a standby waits for a running-transactions record from the primary; an idle primary
    writes one only every 15 s (bgwriter), so we request one with pg_log_standby_snapshot() until creation returns."""
    result, errors = {}, []

    def create():
        try:
            result["lsn"] = c.q(standby, "select lsn from pg_create_logical_replication_slot(%s, 'pgoutput')",
                                (slot,), role="runtime")[0][0]
        except Exception as e:      # noqa: BLE001 - reported below
            errors.append(e)

    t = threading.Thread(target=create)
    t.start()
    deadline = time.time() + 60
    while t.is_alive() and time.time() < deadline:
        c.q(primary, "select pg_log_standby_snapshot()")
        t.join(0.3)
    assert not t.is_alive(), "slot creation on the standby did not finish"
    assert not errors, errors
    return result["lsn"]


def pgoutput_changes(c, node, slot, publication):
    return [bytes(r[0]) for r in c.q(
        node, "select data from pg_logical_slot_get_binary_changes(%s, null, null, 'proto_version', '4', "
              "'publication_names', %s)", (slot, publication), role="runtime")]


def wait_slot_synced(c, slot, standby="replica", primary="primary"):
    """Slot synchronisation (PG17+) creates the synced slot only once the primary's slot has moved past the catalog
    xmin and WAL the standby can guarantee. A real consumer keeps advancing its slot; here we advance it ourselves,
    with a fresh running-transactions record each round, until the standby holds a persistent synced copy."""
    def synced():
        c.q(primary, "select pg_log_standby_snapshot()")
        c.q(primary, "select pg_replication_slot_advance(%s, pg_current_wal_lsn())", (slot,), role="runtime")
        r = c.q(standby, "select synced, temporary, failover, confirmed_flush_lsn::text "
                         "from pg_replication_slots where slot_name = %s", (slot,))
        return r and r[0][0] and not r[0][1] and r[0][2] and r[0]
    return wait_for(synced, timeout=90, interval=0.5, what="failover slot %s synced to %s" % (slot, standby))


# ------------------------------------------------------------------------------------------------ safety and refusal

@pytest.mark.parametrize("base", ["5432", "5425", "5430", "80"])
def test_refuses_port_base_that_includes_5432_or_is_privileged(base):
    home = os.path.join(os.path.dirname(TEST_HOME), "pg-selftest-refused")
    for cmd in (["up"], ["status"], ["reset"]):
        p = lp("--home", home, "--port-base", base, *cmd, check=False)
        assert p.returncode == 2, p.stderr
        assert "refused" in p.stderr
    assert not os.path.exists(home)
    p = lp("up", check=False, env=lp_env(CDC_V2_PG_HOME=home, CDC_V2_PG_PORT_BASE=base))
    assert p.returncode == 2 and not os.path.exists(home)


def test_refuses_foreign_or_unmarked_home():
    p = lp("--home", "/opt/homebrew/var/postgresql@18", "status", check=False)
    assert p.returncode == 2 and "refused" in p.stderr
    home = os.path.join(os.path.dirname(TEST_HOME), "pg-selftest-unmarked")
    os.makedirs(home, exist_ok=True)
    keep = os.path.join(home, "somebody-elses-file")
    open(keep, "w").close()
    try:
        p = lp("--home", home, "reset", check=False)
        assert p.returncode == 2 and "not a localpg home" in p.stderr
        assert os.path.exists(keep)
    finally:
        shutil.rmtree(home)


# ------------------------------------------------------------------------------------------------ lifecycle

def test_reset_rebuilds_from_half_initialised_state():
    lp("reset")
    c = Cluster()
    old = c.pids()
    assert old
    # 1. state says "not initialised" while both nodes still run: `up` must stop them and rebuild
    with open(os.path.join(TEST_HOME, "state.json")) as f:
        st = json.load(f)
    st["initialized"] = False
    with open(os.path.join(TEST_HOME, "state.json"), "w") as f:
        json.dump(st, f)
    p = lp("up")
    assert "built" in p.stdout
    assert not [x for x in old if alive(x)]
    # 2. the state file is gone and a stray initdb directory exists, with the nodes running: `reset` still cleans up
    old = Cluster().pids()
    os.unlink(os.path.join(TEST_HOME, "state.json"))
    os.makedirs(os.path.join(TEST_HOME, "nodes", "standby5", "data"))
    lp("reset")
    assert not [x for x in old if alive(x)]
    assert not os.path.exists(os.path.join(TEST_HOME, "nodes", "standby5"))
    s = Cluster().status()
    assert s["all_running"] and [n["name"] for n in s["nodes"]] == ["primary", "replica"]


def test_up_is_idempotent(cluster):
    before = {n["name"]: n["pid"] for n in cluster.status()["nodes"]}
    t0 = time.time()
    p = lp("up")
    assert time.time() - t0 < 5
    assert p.stdout.count("already running") == 2
    assert {n["name"]: n["pid"] for n in cluster.status()["nodes"]} == before


def test_down_leaves_no_process_and_up_restarts(cluster):
    t = uniq("t_down")
    admin_table(cluster, t)
    cluster.q("primary", "insert into %s values (1, 'kept')" % t, role="admin")
    pids = cluster.pids()
    assert len(pids) > 4
    lp("down")
    assert wait_for(lambda: not [x for x in pids if alive(x)], timeout=5, what="processes gone")
    for n in cluster.info["nodes"]:
        assert not port_open(n["port"])
        assert not os.path.exists(os.path.join(TEST_HOME, "nodes", n["name"], "data", "postmaster.pid"))
    assert lp("status", check=False).returncode == 3
    assert "nothing was running" in lp("down").stdout
    lp("up")
    assert cluster.q("primary", "select v from %s" % t, role="admin") == [("kept",)]
    assert replica_has(cluster, t, 1) == [("kept",)]


def test_frozen_postmaster_does_not_hang_status_or_down(cluster):
    pid = cluster.node("replica")["pid"]
    os.kill(pid, signal.SIGSTOP)         # pid identified through the replica's postmaster.pid (localpg status)
    try:
        t0 = time.time()
        s = json.loads(lp("status", "--json", check=False, timeout=60).stdout)
        assert time.time() - t0 < 20
        rep = next(n for n in s["nodes"] if n["name"] == "replica")
        assert rep["running"] and "error" in rep
        t0 = time.time()
        lp("down", timeout=120)
        assert time.time() - t0 < 30
        assert not alive(pid)
    finally:
        if alive(pid):
            os.kill(pid, signal.SIGCONT)
    lp("up")


def test_status_reports_nodes_and_slots(cluster):
    s = cluster.status()
    assert s["all_running"] and s["port_base"] == TEST_BASE
    prim, rep = s["nodes"]
    assert (prim["name"], prim["port"], prim["role"]) == ("primary", TEST_BASE, "primary")
    assert (rep["name"], rep["port"], rep["role"]) == ("replica", TEST_BASE + 1, "standby")
    for n in (prim, rep):
        assert n["server_version"].startswith(str(PG_MAJOR)) and n["timeline"] == 1
        assert n["pg_major"] == PG_MAJOR and n["server_version_num"] // 10000 == PG_MAJOR
    assert s["pg_major"] == PG_MAJOR and s["bin_major"] == PG_MAJOR
    assert prim["current_lsn"] and rep["replay_lsn"] and "replay_lag_bytes" in rep
    assert rep["replication_state"] == "streaming"
    slot = next(x for x in prim["slots"] if x["slot_name"] == "cdc_v2_phys_replica")
    assert slot["slot_type"] == "physical" and slot["active"] and slot["wal_status"] == "reserved"
    human = lp("status").stdout
    assert "primary" in human and "replica" in human and "cdc_v2_phys_replica" in human


# ------------------------------------------------------------------------------------------------ replication

def test_row_written_on_primary_is_visible_on_replica(cluster):
    t = uniq("t_vis")
    admin_table(cluster, t)
    cluster.q("primary", "insert into %s values (42, 'hello')" % t, role="admin")
    assert replica_has(cluster, t, 42) == [("hello",)]
    with pytest.raises(psycopg.errors.ReadOnlySqlTransaction):
        cluster.q("replica", "insert into %s values (43, 'no')" % t, role="admin")


def test_settings_on_replica(cluster):
    rows = dict(cluster.q("replica", "select name, setting from pg_settings where name = any(%s)",
                          (["hot_standby_feedback", "wal_level", "sync_replication_slots", "primary_slot_name",
                            "shared_buffers", "listen_addresses", "ssl", "password_encryption"],)))
    assert rows["hot_standby_feedback"] == "on" and rows["wal_level"] == "logical"
    if PG_MAJOR >= 17:
        assert rows["sync_replication_slots"] == "on"
    else:       # the setting does not exist before 17, and localpg does not write it
        assert "sync_replication_slots" not in rows
    assert rows["primary_slot_name"] == "cdc_v2_phys_replica"
    assert rows["shared_buffers"] == "16384"        # 8 kB pages = 128 MB
    assert rows["listen_addresses"] == "localhost" and rows["ssl"] == "on"
    assert rows["password_encryption"] == "scram-sha-256"


def test_logical_slot_with_pgoutput_on_replica_delivers_changes(cluster):
    t, pub, slot = uniq("t_logical"), uniq("cdc_v2_pub"), uniq("cdc_v2_slot")
    admin_table(cluster, t)
    cluster.q("primary", "create publication %s for table %s" % (pub, t), role="admin")
    try:
        create_slot_on_standby(cluster, "replica", slot)
        cluster.q("primary", "insert into %s values (7, 'from-the-primary')" % t, role="admin")
        lsn = cluster.q("primary", "select pg_current_wal_lsn()::text")[0][0]
        wait_for(lambda: cluster.q("replica", "select pg_last_wal_replay_lsn() >= %s::pg_lsn", (lsn,))[0][0],
                 what="replay")
        msgs = pgoutput_changes(cluster, "replica", slot, pub)
        kinds = [m[:1] for m in msgs]
        assert b"B" in kinds and b"R" in kinds and b"C" in kinds
        inserts = [m for m in msgs if m[:1] == b"I"]
        assert len(inserts) == 1 and b"from-the-primary" in inserts[0]
        assert pgoutput_changes(cluster, "replica", slot, pub) == []        # consumed exactly once
    finally:
        cluster.q("replica", "select pg_drop_replication_slot(slot_name) from pg_replication_slots "
                             "where slot_name = %s", (slot,), role="runtime")
        cluster.q("primary", "drop publication if exists %s" % pub, role="admin")


@needs_17
def test_failover_slot_is_synchronised_to_standby(cluster):
    t, pub, slot = uniq("t_failover"), uniq("cdc_v2_pub"), uniq("cdc_v2_fslot")
    admin_table(cluster, t)
    cluster.q("primary", "create publication %s for table %s" % (pub, t), role="admin")
    try:
        cluster.q("primary", "select pg_create_logical_replication_slot(%s, 'pgoutput', false, false, true)",
                  (slot,), role="runtime")
        cluster.q("primary", "insert into %s values (1, 'a')" % t, role="admin")
        pgoutput_changes(cluster, "primary", slot, pub)            # consume, so the slot advances
        row = wait_slot_synced(cluster, slot)
        assert row[3] is not None
        s = next(x for x in cluster.node("replica")["slots"] if x["slot_name"] == slot)
        assert s["synced"] and s["failover"]
    finally:
        cluster.q("primary", "select pg_drop_replication_slot(slot_name) from pg_replication_slots "
                             "where slot_name = %s", (slot,))
        cluster.q("primary", "drop publication if exists %s" % pub, role="admin")
    wait_for(lambda: not cluster.q("replica", "select 1 from pg_replication_slots where slot_name = %s", (slot,)),
             timeout=60, interval=0.5, what="synced slot dropped on the replica")


# ------------------------------------------------------------------------------------------------ TLS and roles

def test_tls_verify_full_and_no_plaintext_tcp(cluster):
    with cluster.connect("primary", role="runtime") as conn:
        ssl, version = conn.execute("select ssl, version from pg_stat_ssl where pid = pg_backend_pid()").fetchone()
        assert ssl is True and version.startswith("TLSv1.")
    with cluster.connect("replica", role="admin") as conn:
        assert conn.execute("select ssl from pg_stat_ssl where pid = pg_backend_pid()").fetchone()[0] is True
    for role in ("runtime", "admin"):
        with pytest.raises(psycopg.OperationalError, match="no encryption"):
            cluster.connect("primary", role=role, sslmode="disable")
    # verify-full checks the host name: 127.0.0.1 is in the certificate, a wrong CA is not accepted
    with cluster.connect("primary", role="runtime", host="127.0.0.1"):
        pass
    with pytest.raises(psycopg.OperationalError):
        cluster.connect("primary", role="runtime", sslrootcert="system")
    # the bootstrap superuser is refused over TCP even with TLS
    boot = cluster.passwords["bootstrap"]
    with pytest.raises(psycopg.OperationalError, match="rejects connection"):
        psycopg.connect(host="localhost", port=cluster.port("primary"), dbname="postgres", user=boot["user"],
                        password=boot["password"], sslmode="verify-full", sslrootcert=cluster.info["ca_cert"],
                        connect_timeout=5)
    # a wrong password is refused (SCRAM)
    with pytest.raises(psycopg.OperationalError, match="password authentication failed"):
        cluster.connect("primary", role="runtime", password="wrong-" + uuid.uuid4().hex)
    # the libpq password file works on its own
    with psycopg.connect(host="localhost", port=cluster.port("replica"), dbname=cluster.info["database"],
                         user=cluster.info["roles"]["runtime"], sslmode="verify-full",
                         sslrootcert=cluster.info["ca_cert"], passfile=cluster.info["pgpass"], connect_timeout=5):
        pass


def test_roles_have_managed_provider_privileges(cluster):
    roles = {r[0]: r[1:] for r in cluster.q(
        "primary", "select rolname, rolsuper, rolreplication, rolcreaterole, rolcreatedb, rolcanlogin "
                   "from pg_roles where rolname like 'cdc_v2%'")}
    names = cluster.info["roles"]
    assert set(roles) == set(names.values())
    assert roles[names["bootstrap"]][0] is True
    assert roles[names["admin"]] == (False, True, True, True, True)
    assert roles[names["runtime"]] == (False, True, False, False, True)
    # stored as SCRAM verifiers
    assert all(r[0].startswith("SCRAM-SHA-256$") for r in cluster.q(
        "primary", "select rolpassword from pg_authid where rolname like 'cdc_v2%'"))
    # the admin role can create schemas, tables, publications and roles, but is no superuser
    s = uniq("cdc_v2_schema")
    cluster.q("primary", "create schema %s; create table %s.x (id int primary key); "
                         "create publication %s for table %s.x" % (s, s, s, s), role="admin")
    role = uniq("cdc_v2_tmp_role")
    cluster.q("primary", "create role %s login replication" % role, role="admin")
    cluster.q("primary", "drop role %s; drop publication %s; drop schema %s cascade" % (role, s, s), role="admin")
    with pytest.raises(psycopg.errors.InsufficientPrivilege):
        cluster.q("primary", "alter system set work_mem = '8MB'", role="admin")
    with pytest.raises(psycopg.errors.InsufficientPrivilege):
        cluster.q("primary", "create role %s superuser" % uniq("cdc_v2_x"), role="admin")
    # the runtime role cannot create roles and is not a superuser
    assert cluster.q("primary", "select current_setting('is_superuser')", role="runtime") == [("off",)]
    with pytest.raises(psycopg.errors.InsufficientPrivilege):
        cluster.q("primary", "create role %s" % uniq("cdc_v2_x"), role="runtime")


def test_password_files_and_conninfo(cluster):
    info = cluster.info
    for path in (info["passwords_json"], info["pgpass"]):
        assert stat.S_IMODE(os.stat(path).st_mode) == 0o600
    assert stat.S_IMODE(os.stat(os.path.dirname(info["passwords_json"])).st_mode) == 0o700
    assert set(info) >= {"hosts", "nodes", "database", "roles", "ca_cert", "passwords_json", "pgpass"}
    assert [n["port"] for n in info["nodes"]] == [TEST_BASE, TEST_BASE + 1]
    assert info["database"] == "cdc_v2_test" and all(r.startswith("cdc_v2") for r in info["roles"].values())
    outputs = [lp("conninfo", "--json").stdout, lp("conninfo").stdout, lp("status", "--json").stdout,
               lp("status").stdout, lp("up").stdout, lp("conninfo", "--env", "runtime").stdout]
    for pw in (v["password"] for v in cluster.passwords.values()):
        assert len(pw) >= 24
        assert not any(pw in o for o in outputs)
        for log in os.listdir(os.path.join(TEST_HOME, "logs")):
            with open(os.path.join(TEST_HOME, "logs", log), errors="replace") as f:
                assert pw not in f.read()
    shown = lp("conninfo", "--json", "--show-passwords").stdout
    assert cluster.passwords["runtime"]["password"] in shown


# ------------------------------------------------------------------------------------------------ crash and drills

def test_kill_primary_then_up_recovers_without_data_loss(cluster):
    t = uniq("t_kill")
    admin_table(cluster, t)
    cluster.q("primary", "insert into %s select g, 'row ' || g from generate_series(1, 5000) g" % t, role="admin")
    held = cluster.connect("primary", role="admin")            # an open backend that must not survive the kill
    held.execute("select 1")
    pm = cluster.node("primary")["pid"]
    kids = child_pids(pm)
    assert kids
    p = lp("kill", "primary")
    assert "SIGKILL" in p.stdout
    assert not alive(pm) and not [k for k in kids if alive(k)], "orphaned backends after kill"
    assert not port_open(TEST_BASE)
    assert cluster.node("primary")["running"] is False
    assert "not running" in lp("kill", "primary").stdout
    lp("up")
    assert cluster.q("primary", "select count(*) from %s" % t, role="admin") == [(5000,)]
    cluster.q("primary", "insert into %s values (5001, 'after crash')" % t, role="admin")
    assert replica_has(cluster, t, 5001) == [("after crash",)]
    held.close()


def test_kill_replica_then_up_catches_up(cluster):
    t = uniq("t_kill_rep")
    admin_table(cluster, t)
    pm = cluster.node("replica")["pid"]
    lp("kill", str(TEST_BASE + 1))                              # nodes can be named by port
    assert not alive(pm)
    cluster.q("primary", "insert into %s values (1, 'while replica down')" % t, role="admin")
    lp("up")
    assert replica_has(cluster, t, 1) == [("while replica down",)]


def test_version_dependent_settings_are_refused_with_a_message(cluster):
    """A setting that the node's major version does not have is refused by localpg itself, not by the server."""
    for node, name, value, since in (("replica", "sync_replication_slots", "on", 17),
                                     ("primary", "synchronized_standby_slots", "cdc_v2_phys_replica", 17),
                                     ("primary", "idle_replication_slot_timeout", "1min", 18)):
        p = lp("set", node, "%s=%s" % (name, value), check=False)
        if PG_MAJOR < since:
            assert p.returncode == 2 and "needs Postgres %d" % since in p.stderr, p.stderr
        else:
            assert p.returncode == 0, p.stderr
            lp("unset", node, name)
    assert cluster.node("replica")["overrides"] == {} and cluster.node("primary")["overrides"] == {}


def test_restart_settings_unset_and_inherited_by_a_new_standby(cluster):
    """`unset` of a restart-only setting restarts the nodes (it failed with a configuration error before), in an
    order a standby accepts; `add-standby` inherits the upstream's standby-matched settings."""
    lp("set", "all", "max_prepared_transactions=5")
    p = lp("add-standby")
    assert "standby2" in p.stdout
    c = cluster.refresh()
    assert c.q("standby2", "show max_prepared_transactions") == [("5",)]
    assert c.node("standby2")["overrides"] == {"max_prepared_transactions": "5"}
    p = lp("unset", "all", "max_prepared_transactions")
    assert p.stdout.index("unset: primary") < p.stdout.index("unset: replica")      # lowering: primary first
    for n in ("primary", "replica", "standby2"):
        assert c.q(n, "show max_prepared_transactions") == [("0",)]
    s = c.status()
    assert s["all_running"] and all(n.get("replay_pause_state") in (None, "not paused") for n in s["nodes"])
    p = lp("set", "all", "max_prepared_transactions=3")
    assert p.stdout.index("set: replica") < p.stdout.index("set: primary")            # raising: standbys first
    lp("reset")


OTHER_BIN = {16: "/opt/homebrew/opt/postgresql@18/bin", 18: "/opt/homebrew/opt/postgresql@16/bin"}.get(PG_MAJOR)


@pytest.mark.skipif(not OTHER_BIN or not os.access(os.path.join(OTHER_BIN or "", "postgres"), os.X_OK),
                    reason="needs the binaries of a second major version")
def test_binaries_of_another_major_version_are_refused(cluster):
    lp("down")
    p = lp("up", check=False, env=lp_env(CDC_V2_PG_BIN=OTHER_BIN))
    assert p.returncode == 2 and "data directory" in p.stderr and "Postgres %d" % PG_MAJOR in p.stderr, p.stderr
    assert not cluster.status()["all_running"]
    lp("up")
    assert cluster.status()["all_running"]


def test_set_reloads_or_restarts_a_node(cluster):
    p = lp("set", "primary", "max_slot_wal_keep_size=64MB")
    assert "reloaded" in p.stdout
    assert cluster.q("primary", "show max_slot_wal_keep_size") == [("64MB",)]
    assert cluster.q("replica", "show max_slot_wal_keep_size") == [("-1",)]
    pid = cluster.node("primary")["pid"]
    p = lp("set", "primary", "shared_buffers=64MB")
    assert "restarted" in p.stdout and cluster.node("primary")["pid"] != pid
    assert cluster.q("primary", "show shared_buffers") == [("64MB",)]
    assert cluster.node("primary")["overrides"] == {"max_slot_wal_keep_size": "64MB", "shared_buffers": "64MB"}
    assert lp("set", "primary", "no_such_setting=1", check=False).returncode == 2
    assert lp("set", "primary", "port=5432", check=False).returncode == 2
    assert "warning" in lp("set", "primary", "max_connections=40").stderr     # lower than the standby: safe
    lp("unset", "primary", "max_slot_wal_keep_size", "shared_buffers", "max_connections")
    assert cluster.q("primary", "show max_slot_wal_keep_size") == [("-1",)]
    assert cluster.q("primary", "show shared_buffers") == [("128MB",)]
    assert cluster.q("primary", "show max_connections") == [("50",)]
    assert cluster.node("replica")["replay_pause_state"] == "not paused"
    assert cluster.node("primary")["overrides"] == {}


def test_add_standby_failover_promote_and_follow(cluster):
    t = uniq("t_drill")
    admin_table(cluster, t)
    cluster.q("primary", "insert into %s values (1, 'before')" % t, role="admin")
    p = lp("add-standby")
    assert "standby2" in p.stdout and str(TEST_BASE + 2) in p.stdout
    c = cluster.refresh()
    assert replica_has(c, t, 1, node="standby2") == [("before",)]
    s2 = c.node("standby2")
    assert s2["role"] == "standby" and s2["wal_receiver_status"] == "streaming" and s2["upstream"] == "primary"
    assert any(x["slot_name"] == "cdc_v2_phys_standby2" and x["active"] for x in c.node("primary")["slots"])
    c.q("primary", "insert into %s values (2, 'last on timeline 1')" % t, role="admin")
    replica_has(c, t, 2)
    replica_has(c, t, 2, node="standby2")
    # failover drill: the primary dies, the replica is promoted, standby2 follows the new primary
    lp("kill", "primary")
    out = json.loads(lp("promote", "replica", "--json").stdout)
    assert out["old_timeline"] == 1 and out["new_timeline"] == 2
    assert c.node("replica")["role"] == "primary" and c.node("replica")["timeline"] == 2
    c.q("replica", "insert into %s values (3, 'timeline 2')" % t, role="admin")
    assert c.q("replica", "select count(*) from %s" % t, role="admin") == [(3,)]
    assert lp("promote", "replica", check=False).returncode == 1          # already a primary
    lp("follow", "standby2", "replica")
    assert replica_has(c, t, 3, node="standby2") == [("timeline 2",)]
    s2 = c.node("standby2")
    assert s2["upstream"] == "replica" and s2["timeline"] == 2 and s2["replication_state"] == "streaming"
    # the old primary is removed and its port reused by a fresh standby of the new primary
    lp("remove", "primary")
    p = lp("add-standby", "--name", "standby0")
    assert str(TEST_BASE) in p.stdout
    c.refresh()
    assert replica_has(c, t, 3, node="standby0") == [("timeline 2",)]
    s = c.status()
    assert s["all_running"] and {n["name"]: n["role"] for n in s["nodes"]} == {
        "standby0": "standby", "replica": "primary", "standby2": "standby"}
    pids = c.pids()
    lp("down")
    assert wait_for(lambda: not [x for x in pids if alive(x)], timeout=5, what="processes gone")
    lp("reset")                                                  # back to the canonical topology


@needs_17
def test_promote_failover_slot_survives_on_promoted_standby(cluster):
    """Primary mode (SPEC 4.1): a failover slot synced to the standby is usable after the standby is promoted."""
    t, pub, slot = uniq("t_fpromote"), uniq("cdc_v2_pub"), uniq("cdc_v2_fslot")
    admin_table(cluster, t)
    cluster.q("primary", "create publication %s for table %s" % (pub, t), role="admin")
    cluster.q("primary", "select pg_create_logical_replication_slot(%s, 'pgoutput', false, false, true)",
              (slot,), role="runtime")
    cluster.q("primary", "insert into %s values (1, 'consumed before failover')" % t, role="admin")
    pgoutput_changes(cluster, "primary", slot, pub)
    wait_slot_synced(cluster, slot)
    cluster.q("primary", "insert into %s values (2, 'after sync, before failover')" % t, role="admin")
    replica_has(cluster, t, 2)
    lp("kill", "primary")
    lp("promote", "replica")
    cluster.q("replica", "insert into %s values (3, 'on the new primary')" % t, role="admin")
    msgs = pgoutput_changes(cluster, "replica", slot, pub)
    inserted = b"".join(m for m in msgs if m[:1] == b"I")
    assert b"on the new primary" in inserted
    assert b"consumed before failover" not in inserted
    lp("reset")
