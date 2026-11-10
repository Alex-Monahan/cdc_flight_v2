"""netproxy against a local echo server: every fault mode, counters, per-connection faults, the port allocator and
control from another process. Hermetic: loopback only."""
import json
import os
import socket
import socketserver
import sys
import threading
import time

import pytest

from cdc_v2.testing import netproxy as npx
from cdc_v2.testing import procs

REPO = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
NETPROXY = os.path.join(REPO, "tools", "netproxy")


class Echo(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True

    def __init__(self):
        self.received = {}
        super().__init__(("127.0.0.1", 0), EchoHandler)
        threading.Thread(target=self.serve_forever, daemon=True).start()

    @property
    def port(self):
        return self.server_address[1]


class EchoHandler(socketserver.BaseRequestHandler):
    def handle(self):
        key = self.client_address
        self.server.received[key] = 0
        try:
            while True:
                data = self.request.recv(65536)
                if not data:
                    return
                self.server.received[key] += len(data)
                self.request.sendall(data)
        except OSError:
            return


@pytest.fixture(scope="module")
def echo():
    srv = Echo()
    yield srv
    srv.shutdown()
    srv.server_close()


@pytest.fixture
def px(echo):
    p = npx.NetProxy("127.0.0.1", echo.port).start()
    yield p
    p.stop()


def connect(port, timeout=2.0):
    s = socket.create_connection(("127.0.0.1", port), timeout=timeout)
    s.settimeout(timeout)
    return s


def recv_exact(s, n):
    buf = b""
    while len(buf) < n:
        chunk = s.recv(n - len(buf))
        if not chunk:
            break
        buf += chunk
    return buf


def roundtrip(s, payload=b"ping"):
    t = time.monotonic()
    s.sendall(payload)
    assert recv_exact(s, len(payload)) == payload
    return time.monotonic() - t


def test_port_range_and_pass(px):
    assert npx.PORT_FIRST <= px.port <= npx.PORT_LAST
    with connect(px.port) as s:
        roundtrip(s, b"x" * 10000)
    st = px.wait_for(lambda st: st["active"] == 0, 5)
    assert st["accepted"] == 1 and st["bytes_up"] == 10000 and st["bytes_down"] == 10000 and st["listening"]


def test_delay(px):
    with connect(px.port) as s:
        base = roundtrip(s)
        px.set_fault("delay", seconds=0.15)
        assert roundtrip(s) >= 0.29                       # 0.15 s each way
        px.set_fault("delay", direction="up", seconds=0.2)
        assert 0.19 <= roundtrip(s) < 0.39
        px.set_fault("pass")
        assert roundtrip(s) < max(0.1, base * 10)


def test_throttle(px):
    with connect(px.port, timeout=10) as s:
        px.set_fault("throttle", direction="up", rate=100_000)
        t = roundtrip(s, b"y" * 50_000)
        assert t >= 0.45, t


def test_blackhole_both_holds_and_releases(px):
    with connect(px.port) as s:
        roundtrip(s)
        px.set_fault("blackhole")
        s.sendall(b"held")
        s.settimeout(0.5)
        with pytest.raises(socket.timeout):
            s.recv(10)
        st = px.wait_for(lambda st: st["held_up"] == 4, 2)
        assert st["active"] == 1                           # the connection stays open
        px.set_fault("pass")
        s.settimeout(2)
        assert recv_exact(s, 4) == b"held"                 # delivered once the hole is lifted


def test_blackhole_one_direction(px, echo):
    with connect(px.port) as s:
        roundtrip(s)
        before = sum(echo.received.values())
        px.set_fault("blackhole", direction="down")
        s.sendall(b"abc")
        time.sleep(0.3)
        assert sum(echo.received.values()) == before + 3   # up still flows
        s.settimeout(0.3)
        with pytest.raises(socket.timeout):
            s.recv(10)
        assert px.stats()["held_down"] == 3
        px.set_fault("blackhole", direction="up")          # replaces: down flows again, up is held
        s.settimeout(2)
        assert recv_exact(s, 3) == b"abc"
        s.sendall(b"zz")
        time.sleep(0.3)
        assert sum(echo.received.values()) == before + 3 and px.stats()["held_up"] == 2
        px.set_fault("pass")
        assert recv_exact(s, 2) == b"zz"


def test_reset_open_and_new_connections(px):
    s = connect(px.port)
    roundtrip(s)
    px.set_fault("reset")
    with pytest.raises(ConnectionResetError):              # RST, not FIN (a FIN would read b"")
        s.recv(10)
    s.close()
    s2 = connect(px.port)                                  # accepted, then reset at once
    with pytest.raises((ConnectionResetError, BrokenPipeError)):
        s2.recv(10)
    s2.close()
    st = px.stats()
    assert st["reset_by_fault"] == 1 and st["accept_reset"] == 1
    px.set_fault("pass")
    with connect(px.port) as s3:
        roundtrip(s3)


def test_close_open_and_new_connections(px):
    s = connect(px.port)
    roundtrip(s)
    px.set_fault("close")
    assert s.recv(10) == b""                               # FIN
    s.close()
    s2 = connect(px.port)
    assert s2.recv(10) == b""
    s2.close()
    st = px.stats()
    assert st["closed_by_fault"] == 1 and st["accept_closed"] == 1


def test_refuse_keeps_open_connections(px):
    s = connect(px.port)
    roundtrip(s)
    px.set_fault("refuse")
    assert not px.stats()["listening"]
    with pytest.raises(ConnectionRefusedError):
        connect(px.port)
    roundtrip(s)                                           # existing connection unaffected
    px.set_fault("pass")
    with connect(px.port) as s2:
        roundtrip(s2)
    s.close()


def test_per_connection_fault(px):
    a, b = connect(px.port), connect(px.port)
    roundtrip(a)
    roundtrip(b)
    conns = sorted(c["id"] for c in px.connections())
    px.set_fault("blackhole", conn=conns[0])
    a.sendall(b"q")
    a.settimeout(0.3)
    with pytest.raises(socket.timeout):
        a.recv(1)
    roundtrip(b)                                           # the other connection still works
    infos = {c["id"]: c for c in px.connections()}
    assert infos[conns[0]]["held_up"] == 1 and infos[conns[0]]["policy"]["blackhole_up"]
    px.set_fault("pass", conn=conns[0])
    a.settimeout(2)
    assert a.recv(1) == b"q"
    a.close()
    b.close()


def test_target_5432_on_loopback_is_refused():
    for host in ("localhost", "127.0.0.1", "::1"):
        with pytest.raises(npx.ProxyError):
            npx.NetProxy(host, 5432)


def test_two_proxies_get_distinct_ports_and_leases(echo):
    with npx.NetProxy("127.0.0.1", echo.port) as a, npx.NetProxy("127.0.0.1", echo.port) as b:
        assert a.port != b.port
        assert a.port in npx.running_ports() and b.port in npx.running_ports()
        with pytest.raises(npx.ProxyError):
            npx.NetProxy("127.0.0.1", echo.port, port=a.port).start()
    assert not npx.lease_is_held(a.port)


def wait_port(proc, timeout=10):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        text = proc.log_text().strip()
        if text:
            return json.loads(text.splitlines()[0])["port"]
        assert proc.alive(), proc.log_text()
        time.sleep(0.02)
    raise TimeoutError("netproxy serve printed no port")


def test_control_from_other_processes_and_concurrent_allocation(echo, tmp_path):
    with procs.Fleet() as fleet:
        servers = [fleet.start([sys.executable, NETPROXY, "serve", "--target-host", "127.0.0.1",
                                "--target-port", str(echo.port)], name="px%d" % i,
                               log_path=str(tmp_path / ("px%d.log" % i))) for i in range(3)]
        ports = [wait_port(p) for p in servers]
        assert len(set(ports)) == 3 and all(npx.PORT_FIRST <= p <= npx.PORT_LAST for p in ports)
        with connect(ports[0]) as s:
            roundtrip(s)
            out = procs.start([sys.executable, NETPROXY, "set", str(ports[0]), "blackhole"],
                              log_path=str(tmp_path / "set.log"))
            assert out.wait(10).code == 0
            s.sendall(b"zz")
            ctl = npx.ProxyControl(port=ports[0])
            st = ctl.stats()
            deadline = time.monotonic() + 2
            while st["held_up"] != 2 and time.monotonic() < deadline:
                time.sleep(0.02)
                st = ctl.stats()
            assert st["mode"] == "blackhole" and st["held_up"] == 2 and st["faults_set"] == 1
            assert "target_host" not in json.dumps(st) and "127.0.0.1" not in json.dumps(st)
            ctl.set_fault("pass")
            assert recv_exact(s, 2) == b"zz"
        for p in ports:
            stop = procs.start([sys.executable, NETPROXY, "stop", str(p)])
            assert stop.wait(10).code == 0
        for sp in servers:
            assert sp.wait(10).code == 0
    assert not any(npx.lease_is_held(p) for p in ports)
