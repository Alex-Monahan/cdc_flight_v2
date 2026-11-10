"""Tier T1: psycopg through the TCP fault proxy to the primary of the T1 fixtures' cluster. SPEC 4.5, 4.6.

    CDC_V2_PG_HOME=... CDC_V2_PG_PORT_BASE=... uv run pytest -m t1 tests/harness/test_harness_pg_proxy.py

The cluster is the one the shared fixtures name (`tests/conftest.py`: your own home and port base; the default home
only with CDC_V2_T1_SHARED=1).

Measures how fast a client notices a black hole (TCP keepalives plus a client-side statement deadline), a reset and
a refused connection, and that a delayed statement still succeeds. Set CDC_V2_HARNESS_EVIDENCE=<dir> to keep the
measured times. The fixture may run `tools/localpg up` (a no-op when the cluster runs); nothing is stopped or changed.
"""
import os
import select
import socket
import threading
import time

import psycopg
import pytest

from cdc_v2.testing import netproxy as npx
from cdc_v2.testing.harness_evidence import write_evidence

pytestmark = pytest.mark.t1

EVIDENCE = os.environ.get("CDC_V2_HARNESS_EVIDENCE")
KEEPALIVE = dict(keepalives=1, keepalives_idle=1, keepalives_interval=1, keepalives_count=2)
DEADLINE_S = 2.0
STATEMENT_TIMEOUT_MS = 1000
RESULTS = {}


@pytest.fixture(scope="module")
def cluster(shared_pg):
    info, creds = shared_pg.info, shared_pg.passwords["admin"]
    port = shared_pg.primary_port
    assert port != 5432
    yield {"db": info["database"], "ca": info["ca_cert"], "user": creds["user"], "password": creds["password"],
           "port": port}
    if EVIDENCE and RESULTS:
        write_evidence(os.path.join(EVIDENCE, "pg_proxy_detection.json"), {
            "what": "psycopg %s through tools/netproxy to the local primary; times in seconds" % psycopg.__version__,
            "client": {"keepalives_idle_s": 1, "keepalives_interval_s": 1, "keepalives_count": 2,
                       "client_deadline_s": DEADLINE_S, "statement_timeout_ms": STATEMENT_TIMEOUT_MS},
            "results": RESULTS})


@pytest.fixture
def px(cluster):
    p = npx.NetProxy("localhost", cluster["port"]).start()
    yield p
    p.stop()


def connect(c, port, **kw):
    params = dict(host="localhost", port=port, dbname=c["db"], user=c["user"], password=c["password"],
                  sslmode="verify-full", sslrootcert=c["ca"], connect_timeout=5, gssencmode="disable",
                  options="-c statement_timeout=%d" % STATEMENT_TIMEOUT_MS, **KEEPALIVE)
    params.update(kw)
    return psycopg.connect(autocommit=True, **params)


def execute_with_deadline(conn, sql, deadline_s=DEADLINE_S):
    """Client-side statement deadline: past it, shut the socket down so the blocked call fails at once (libpq has no
    client-side query timeout; a cancel request would travel through the same broken path)."""
    sock = socket.socket(fileno=os.dup(conn.pgconn.socket))
    timer = threading.Timer(deadline_s, lambda: sock.shutdown(socket.SHUT_RDWR))
    timer.start()
    try:
        return conn.execute(sql).fetchall()
    finally:
        timer.cancel()
        sock.close()


def timed_error(fn):
    t = time.monotonic()
    with pytest.raises(psycopg.Error) as ei:
        fn()
    return round(time.monotonic() - t, 3), type(ei.value).__name__


def keepalive_options(conn):
    s = socket.socket(fileno=os.dup(conn.pgconn.socket))
    try:
        return {"SO_KEEPALIVE": s.getsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE),
                "TCP_KEEPALIVE": s.getsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPALIVE),
                "TCP_KEEPINTVL": s.getsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL),
                "TCP_KEEPCNT": s.getsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT)}
    finally:
        s.close()


def test_tls_passes_through_and_keepalives_are_set(cluster, px):
    with connect(cluster, px.port) as conn:
        ssl, version = conn.execute("SELECT ssl, version FROM pg_stat_ssl WHERE pid = pg_backend_pid()").fetchone()
        assert ssl is True and version.startswith("TLS")
        ka = keepalive_options(conn)
        assert ka["SO_KEEPALIVE"] != 0 and ka["TCP_KEEPALIVE"] == 1 and ka["TCP_KEEPINTVL"] == 1 \
            and ka["TCP_KEEPCNT"] == 2
    st = px.stats()
    assert st["accepted"] == 1 and st["bytes_up"] > 0 and st["bytes_down"] > 0
    RESULTS["tls_through_proxy"] = {"ssl": ssl, "version": version, "keepalive_socket_options": ka}


def test_blackhole_is_noticed_by_the_statement_deadline(cluster, px):
    out = {}
    with connect(cluster, px.port) as conn:
        conn.execute("SELECT 1")
        px.set_fault("blackhole")
        out["both_directions_s"], out["both_error"] = timed_error(lambda: execute_with_deadline(conn, "SELECT 1"))
        st = px.stats()
        assert st["held_up"] > 0 and st["active"] == 1          # the fault was in effect, the connection open
    px.set_fault("pass")
    with connect(cluster, px.port) as conn:
        conn.execute("SELECT 1")
        px.set_fault("blackhole", direction="down")              # the query runs, the reply never arrives
        out["reply_lost_s"], out["reply_lost_error"] = timed_error(
            lambda: execute_with_deadline(conn, "SELECT pg_sleep(0.1)"))
        assert px.stats()["held_down"] > 0
    for k in ("both_directions_s", "reply_lost_s"):
        assert DEADLINE_S - 0.05 <= out[k] <= DEADLINE_S + 1.0, out
    RESULTS["blackhole"] = out


def test_keepalives_alone_do_not_see_a_proxy_blackhole(cluster, px):
    """Documents the proxy's limit: its own kernel answers keepalive probes, so an idle connection in a black hole
    still looks healthy to TCP after idle + interval x count = 3 s. Detection needs a deadline or a heartbeat."""
    with connect(cluster, px.port) as conn:
        conn.execute("SELECT 1")
        px.set_fault("blackhole")
        time.sleep(5)
        fd = conn.pgconn.socket
        readable, _, errored = select.select([fd], [], [fd], 0)
        s = socket.socket(fileno=os.dup(fd))
        so_error = s.getsockopt(socket.SOL_SOCKET, socket.SO_ERROR)
        s.close()
        px.set_fault("pass")
        assert conn.execute("SELECT 1").fetchone() == (1,)       # the connection survived the 5 s black hole
    assert not readable and not errored and so_error == 0
    RESULTS["blackhole_keepalive_only"] = {"waited_s": 5, "socket_error": so_error, "detected": False}


def test_reset_is_an_error_at_once(cluster, px):
    out = {}
    with connect(cluster, px.port) as conn:
        conn.execute("SELECT 1")
        px.set_fault("reset")
        out["idle_then_statement_s"], out["idle_error"] = timed_error(lambda: conn.execute("SELECT 1"))
    px.set_fault("pass")
    with connect(cluster, px.port) as conn:
        threading.Timer(0.3, lambda: px.set_fault("reset")).start()
        t, out["in_flight_error"] = timed_error(lambda: conn.execute("SELECT pg_sleep(0.8)"))
        out["in_flight_after_reset_s"] = round(t - 0.3, 3)
    px.set_fault("pass")
    st = px.stats()
    assert st["reset_by_fault"] == 2
    assert out["idle_then_statement_s"] < 0.2 and out["in_flight_after_reset_s"] < 0.2, out
    RESULTS["reset"] = out


def test_delay_still_succeeds(cluster, px):
    with connect(cluster, px.port) as conn:
        t = time.monotonic()
        conn.execute("SELECT 1")
        base = time.monotonic() - t
        px.set_fault("delay", seconds=0.15)
        t = time.monotonic()
        assert execute_with_deadline(conn, "SELECT 1") == [(1,)]
        delayed = time.monotonic() - t
    assert delayed >= 0.3 and delayed < DEADLINE_S
    RESULTS["delay"] = {"delay_each_way_s": 0.15, "statement_s": round(delayed, 3), "baseline_s": round(base, 4)}


def test_refused_and_closed(cluster, px):
    out = {}
    px.set_fault("refuse")
    out["refused_connect_s"], out["refused_error"] = timed_error(lambda: connect(cluster, px.port))
    px.set_fault("pass")
    with connect(cluster, px.port) as conn:
        conn.execute("SELECT 1")
        px.set_fault("close")
        out["closed_then_statement_s"], out["closed_error"] = timed_error(lambda: conn.execute("SELECT 1"))
    px.set_fault("pass")
    assert out["refused_connect_s"] < 1.0 and out["closed_then_statement_s"] < 0.2, out
    RESULTS["refuse_close"] = out
