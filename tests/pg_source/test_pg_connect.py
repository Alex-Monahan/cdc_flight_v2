"""Package P: connections on the private pair (marker localpg). Session settings that role and database settings
cannot override, TLS, and deadlines against the TCP fault proxy."""
from __future__ import annotations

import os
import socket
import time

import psycopg
import pytest

from cdc_v2 import pg
from cdc_v2.deadline import Deadline, DeadlineExceeded
from cdc_v2.pg import errors
from cdc_v2.testing import netproxy as npx

from .conftest import PROXY_PORTS, record, wait_for

pytestmark = [pytest.mark.localpg, pytest.mark.timeout(300)]

GRACE = 0.5


def test_session_settings_are_pinned_against_role_and_database_settings(cluster, scratch):
    rt = scratch.runtime
    db = cluster.info["database"]
    hostile = {"DateStyle": "SQL, DMY", "IntervalStyle": "postgres_verbose", "extra_float_digits": "0",
               "bytea_output": "escape", "TimeZone": "America/New_York", "lc_monetary": "C",
               "search_path": "evil, public", "row_security": "on", "statement_timeout": "1",
               "lock_timeout": "1", "default_transaction_isolation": "serializable"}
    try:
        for k, v in hostile.items():
            scratch.admin("alter role %s set %s = %s" % (rt, k, "'%s'" % v))
            scratch.admin('alter database "%s" set %s = %s' % (db, k, "'%s'" % v))
        with cluster.raw("replica", "runtime") as plain:        # the override is real for a plain connection
            assert plain.execute("show DateStyle").fetchone()[0] == "SQL, DMY"
        for node in ("primary", "replica"):
            for replication in (False, True):
                conn = pg.connect(cluster.settings(node), Deadline.after(10), replication=replication)
                try:
                    if replication:
                        rows = conn.simple("SELECT '2026-01-02 03:04:05.678901+00'::timestamptz::text, "
                                           "'1 year -2 mons 3 days 04:05:06.5'::interval::text, 0.1::float8::text, "
                                           "'\\x00ff'::bytea::text, '2026-01-02'::date::text, "
                                           "current_setting('search_path')", deadline=Deadline.after(5))
                        rows = [tuple(x.decode() for x in rows[0])]
                    else:
                        rows = conn.execute("SELECT '2026-01-02 03:04:05.678901+00'::timestamptz::text, "
                                            "'1 year -2 mons 3 days 04:05:06.5'::interval::text, 0.1::float8::text, "
                                            "'\\x00ff'::bytea::text, '2026-01-02'::date::text, "
                                            "current_setting('search_path')", deadline=Deadline.after(5))
                    assert rows == [("2026-01-02 03:04:05.678901+00", "P10M3DT4H5M6.5S", "0.1", "\\x00ff",
                                     "2026-01-02", "pg_catalog, pg_temp")], (node, replication, rows)
                finally:
                    conn.close()
        record("session_settings", "pinned_against_role_and_database", {
            "overridden_at_role_and_database": sorted(hostile), "result": "every fixed setting held, on the "
            "primary and the standby, on SQL and replication connections"})
    finally:
        scratch.admin("alter role %s reset all" % rt)
        scratch.admin('alter database "%s" reset all' % db)


def test_tls_verify_full_and_no_silent_fallback(cluster):
    with cluster.connect("replica") as c:
        assert c.scalar("SELECT ssl FROM pg_stat_ssl WHERE pid = pg_backend_pid()", deadline=Deadline.after(5))
    with pytest.raises(errors.PermissionDenied):            # pg_hba rejects plain TCP; tls=disable is explicit
        cluster.connect("primary", tls="disable")
    with pytest.raises(errors.SourceError) as ei:
        pg.connect(cluster.settings("primary", sslrootcert="system"), Deadline.after(10))
    assert type(ei.value) is errors.SourceError                 # a certificate fault is not "run again"
    bad = cluster.settings("primary").with_(password=pg.Password(fn=lambda: "wrong-password-123"))
    with pytest.raises(errors.PermissionDenied) as ei:
        pg.connect(bad, Deadline.after(10))
    assert "wrong-password-123" not in str(ei.value) and "localhost" not in str(ei.value)
    gone = cluster.settings("primary").with_(dbname="cdc_v2_no_such_db_x")      # the server's message names it
    with pytest.raises(errors.SourceError) as ei:
        pg.connect(gone, Deadline.after(10))
    assert "cdc_v2_no_such_db_x" not in str(ei.value) and "cdc_v2_no_such_db_x" not in repr(gone)


def test_statement_deadline_cancels_and_closes(cluster):
    c = cluster.connect("replica", cancel_grace_s=GRACE)
    pid = c.backend_pid
    t = time.monotonic()
    with pytest.raises(DeadlineExceeded):
        c.execute("SELECT pg_sleep(30)", deadline=Deadline.after(0.5))
    took = time.monotonic() - t
    assert took < 0.5 + GRACE and c.closed
    wait_for(lambda: not cluster.boot("replica", "select 1 from pg_stat_activity where pid = %s", (pid,)),
             timeout=5, what="backend gone")
    record("deadlines", "statement_cancel_s", round(took, 3))


@pytest.fixture
def proxy(cluster, monkeypatch):
    """A TCP fault proxy on the upper ports of this package's range (netproxy's own range is shared)."""
    monkeypatch.setattr(npx, "PORT_FIRST", PROXY_PORTS[0])
    monkeypatch.setattr(npx, "PORT_LAST", PROXY_PORTS[-1])
    made = []

    def make(node="primary"):
        p = npx.NetProxy("localhost", cluster.ports[node], port=None).start()
        assert p.port in PROXY_PORTS
        made.append(p)
        return p
    yield make
    for p in made:
        p.stop()


def via(cluster, px, **kw):
    kw.setdefault("cancel_grace_s", GRACE)
    return pg.connect(cluster.settings("primary", port=px.port, **kw), Deadline.after(10))


def test_blackhole_during_connect(cluster, proxy):
    px = proxy()
    px.set_fault("blackhole")
    t = time.monotonic()
    with pytest.raises(DeadlineExceeded):
        pg.connect(cluster.settings("primary", port=px.port, cancel_grace_s=GRACE), Deadline.after(1.5))
    took = time.monotonic() - t
    assert took < 1.5 + GRACE + 0.3
    record("proxy", "blackhole_connect_deadline_1.5_s", round(took, 3))


def test_blackhole_during_statement(cluster, proxy):
    px = proxy()
    c = via(cluster, px)
    c.execute("SELECT 1", deadline=Deadline.after(5))
    px.set_fault("blackhole")
    t = time.monotonic()
    with pytest.raises(DeadlineExceeded):
        c.execute("SELECT 1", deadline=Deadline.after(1.0))
    took = time.monotonic() - t
    assert took < 1.0 + 2 * GRACE + 0.3 and c.closed
    assert px.stats()["held_up"] > 0
    record("proxy", "blackhole_statement_deadline_1.0_s", round(took, 3))


def test_blackhole_while_idle_in_transaction(cluster, proxy, scratch):
    scratch.admin('create table "%s".t (id int primary key)' % scratch.schema)
    scratch.admin('insert into "%s".t values (1)' % scratch.schema)
    px = proxy()
    c = via(cluster, px, idle_in_transaction_session_timeout_ms=2000)
    pid = c.backend_pid
    c.execute("BEGIN", deadline=Deadline.after(5))
    c.execute('SELECT * FROM "%s".t' % scratch.schema, deadline=Deadline.after(5))   # holds ACCESS SHARE
    px.set_fault("blackhole")
    t = time.monotonic()
    with pytest.raises(DeadlineExceeded):
        c.execute("SELECT 1", deadline=Deadline.after(1.0))
    took = time.monotonic() - t
    assert took < 1.0 + 2 * GRACE + 0.3
    # the server ends the session by itself (idle_in_transaction_session_timeout): no lock survives the network wait
    t2 = time.monotonic()
    wait_for(lambda: not cluster.boot("primary", "select 1 from pg_stat_activity where pid = %s", (pid,)),
             timeout=15, what="server session ended")
    record("proxy", "blackhole_idle_in_transaction", {"client_deadline_1.0_s": round(took, 3),
                                                     "server_session_gone_after_s": round(time.monotonic() - t2 +
                                                                                          took, 3)})


def test_reset_is_connection_lost(cluster, proxy):
    px = proxy()
    c = via(cluster, px)
    c.execute("SELECT 1", deadline=Deadline.after(5))
    px.set_fault("reset")
    t = time.monotonic()
    with pytest.raises(errors.ConnectionLost):
        c.execute("SELECT 1", deadline=Deadline.after(5))
    assert c.closed
    record("proxy", "reset_noticed_s", round(time.monotonic() - t, 3))
    px.set_fault("pass")
    t = time.monotonic()
    with pytest.raises(errors.ConnectionLost):
        px.set_fault("refuse")
        pg.connect(cluster.settings("primary", port=px.port), Deadline.after(5))
    record("proxy", "refused_connect_s", round(time.monotonic() - t, 3))


def test_slow_link(cluster, proxy):
    px = proxy()
    c = via(cluster, px)
    px.set_fault("delay", seconds=0.2)
    t = time.monotonic()
    assert c.scalar("SELECT 42", deadline=Deadline.after(5)) == 42
    slow = time.monotonic() - t
    assert 0.35 < slow < 3
    with pytest.raises(DeadlineExceeded):
        c.execute("SELECT 42", deadline=Deadline.after(0.2))
    record("proxy", "delay_0.2_each_way_statement_s", round(slow, 3))


def test_keepalive_socket_options(cluster):
    c = cluster.connect("primary", keepalives_idle_s=30, keepalives_interval_s=10, keepalives_count=6)
    s = socket.socket(fileno=os.dup(c.raw.pgconn.socket))
    try:
        got = {"SO_KEEPALIVE": s.getsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE) != 0,
               "TCP_KEEPALIVE": s.getsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPALIVE),
               "TCP_KEEPINTVL": s.getsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL),
               "TCP_KEEPCNT": s.getsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT)}
    finally:
        s.close()
        c.close()
    assert got == {"SO_KEEPALIVE": True, "TCP_KEEPALIVE": 30, "TCP_KEEPINTVL": 10, "TCP_KEEPCNT": 6}
    record("keepalive", "socket_options_macos", got)


def test_lock_timeout_and_permission_errors(cluster, scratch):
    scratch.admin('create table "%s".t (id int primary key)' % scratch.schema)
    with cluster.raw("primary", "admin") as holder:
        holder.execute("begin")
        holder.execute('lock table "%s".t in access exclusive mode' % scratch.schema)
        c = scratch.connect("primary", lock_timeout_ms=300)
        with pytest.raises(errors.LockTimeout):
            c.execute('SELECT * FROM "%s".t' % scratch.schema, deadline=Deadline.after(5))
        holder.execute("rollback")
    c = scratch.connect("primary")
    with pytest.raises(errors.PermissionDenied):
        c.execute('INSERT INTO "%s".t VALUES (1)' % scratch.schema, deadline=Deadline.after(5))
    assert not c.closed                       # an SQL error leaves the connection usable
    assert c.scalar("SELECT 1", deadline=Deadline.after(5)) == 1


def test_recovery_conflict_is_read_again(cluster, scratch):
    """A transaction on the standby holds ACCESS SHARE while the primary takes ACCESS EXCLUSIVE: replay waits up to
    max_standby_streaming_delay (default 30 s), then cancels the standby session (SQLSTATE 40001)."""
    scratch.admin('create table "%s".t (id int primary key)' % scratch.schema)
    scratch.admin('grant select on "%s".t to %s' % (scratch.schema, scratch.runtime))
    cluster.replay_to_primary()
    c = scratch.connect("replica", statement_timeout_ms=120_000, idle_in_transaction_session_timeout_ms=120_000)
    delay = int(cluster.boot("replica", "select setting from pg_settings where name = 'max_standby_streaming_delay'")
                [0][0])
    c.execute("BEGIN ISOLATION LEVEL REPEATABLE READ", deadline=Deadline.after(5))
    c.execute('SELECT count(*) FROM "%s".t' % scratch.schema, deadline=Deadline.after(5))
    scratch.admin('begin; lock table "%s".t in access exclusive mode; commit' % scratch.schema)
    t = time.monotonic()
    with pytest.raises((errors.RecoveryConflict, errors.ConnectionLost)) as ei:
        while time.monotonic() - t < delay / 1000 + 30:
            c.execute('SELECT count(*) FROM "%s".t' % scratch.schema, deadline=Deadline.after(5))
            time.sleep(0.5)
    record("errors", "recovery_conflict", {"class": type(ei.value).__name__, "sqlstate": ei.value.sqlstate,
                                           "after_s": round(time.monotonic() - t, 1),
                                           "max_standby_streaming_delay_ms": delay})
    assert isinstance(ei.value, errors.RecoveryConflict), ei.value
