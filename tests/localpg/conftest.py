"""Fixtures for the tools/localpg self-tests.

The self-tests run on their own home and ports (55460-55467 by default), so they never disturb a development cluster on
55432-55439. Run: tools/with-env uv run pytest -m localpg tests/localpg   (opt-in; the default run deselects it)
"""
import json
import os
import socket
import subprocess
import time

import psycopg
import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
LOCALPG = os.path.join(ROOT, "tools", "localpg")
TEST_HOME = os.path.expanduser(os.environ.get("CDC_V2_LOCALPG_TEST_HOME", "~/.cache/cdc_flight_v2/pg-selftest"))
TEST_BASE = int(os.environ.get("CDC_V2_LOCALPG_TEST_PORT_BASE", "55460"))
PG_BIN = os.environ.get("CDC_V2_PG_BIN", "/opt/homebrew/opt/postgresql@18/bin")


def _bin_major(pg_bin):
    try:
        out = subprocess.run([os.path.join(pg_bin, "postgres"), "--version"], capture_output=True, text=True,
                             timeout=10).stdout
        return int(out.split("(PostgreSQL)")[1].split()[0].split(".")[0])
    except (OSError, IndexError, ValueError, subprocess.SubprocessError):
        return None


PG_MAJOR = _bin_major(PG_BIN)       # the self-tests run on 16 and 18; what needs 17+ is skipped on 16
needs_17 = pytest.mark.skipif(PG_MAJOR is not None and PG_MAJOR < 17,
                              reason="failover slots and slot synchronisation need Postgres 17+")

assert 55460 <= TEST_BASE and TEST_BASE + 7 <= 55479, "self-tests must use ports in 55460-55479"


def pytest_collection_modifyitems(config, items):
    if not os.access(os.path.join(PG_BIN, "postgres"), os.X_OK):
        skip = pytest.mark.skip(reason="Postgres binaries not found in %s" % PG_BIN)
        for item in items:
            if "localpg" in item.keywords:
                item.add_marker(skip)


def lp_env(**extra):
    env = {k: v for k, v in os.environ.items() if not k.startswith("PG")}
    env.update(CDC_V2_PG_HOME=TEST_HOME, CDC_V2_PG_PORT_BASE=str(TEST_BASE), CDC_V2_PG_BIN=PG_BIN)
    env.update(extra)
    return env


def lp(*args, check=True, timeout=300, env=None):
    """Run tools/localpg on the self-test home. Returns CompletedProcess (text)."""
    p = subprocess.run([LOCALPG, *args], capture_output=True, text=True, timeout=timeout, env=env or lp_env())
    if check and p.returncode != 0:
        raise AssertionError("localpg %s failed (%d):\nstdout: %s\nstderr: %s"
                             % (" ".join(args), p.returncode, p.stdout, p.stderr))
    return p


def wait_for(fn, timeout=30, interval=0.1, what="condition"):
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        try:
            last = fn()
            if last:
                return last
        except psycopg.Error as e:          # e.g. a node that is still starting
            last = e
        time.sleep(interval)
    raise AssertionError("timed out after %ss waiting for %s (last: %r)" % (timeout, what, last))


def port_open(port):
    with socket.socket() as s:
        s.settimeout(1)
        return s.connect_ex(("127.0.0.1", port)) == 0


def child_pids(pid):
    out = subprocess.run(["/bin/ps", "-A", "-o", "pid=", "-o", "ppid="], capture_output=True, text=True,
                         timeout=10).stdout
    return [int(a) for a, b in (l.split() for l in out.splitlines() if len(l.split()) == 2) if b == str(pid)]


def alive(pid):
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True


class Cluster:
    def __init__(self):
        self.refresh()

    def refresh(self):
        self.info = json.loads(lp("conninfo", "--json").stdout)
        with open(self.info["passwords_json"]) as f:
            self.passwords = json.load(f)
        return self

    def status(self):
        return json.loads(lp("status", "--json", check=False).stdout)

    def node(self, name):
        return next(n for n in self.status()["nodes"] if n["name"] == name)

    def port(self, name):
        return next(n["port"] for n in self.info["nodes"] if n["name"] == name)

    def pids(self):
        """Postmaster and child pids of every running node."""
        out = []
        for n in self.status()["nodes"]:
            if n["pid"]:
                out += [n["pid"]] + child_pids(n["pid"])
        return out

    def connect(self, node, role="runtime", db=None, **kw):
        port = self.port(node)
        kind = {"cdc_v2_boot": "bootstrap"}.get(role, role)
        creds = self.passwords[kind]
        if kind == "bootstrap":         # the superuser is reachable through the private socket only
            params = dict(host=self.info["socket_dir"], port=port, dbname=db or "postgres")
        else:
            params = dict(host="localhost", port=port, dbname=db or self.info["database"], sslmode="verify-full",
                          sslrootcert=self.info["ca_cert"])
        params.update(user=creds["user"], password=creds["password"], connect_timeout=5, gssencmode="disable")
        params.update(kw)
        return psycopg.connect(autocommit=True, **params)

    def q(self, node, sql, params=None, role="bootstrap", db=None):
        with self.connect(node, role=role, db=db) as c:
            cur = c.execute(sql, params)
            return cur.fetchall() if cur.description else None


def canonical_topology(cluster_status):
    nodes = {n["name"]: n for n in cluster_status["nodes"]}
    return (set(nodes) == {"primary", "replica"} and nodes["primary"]["role"] == "primary"
            and nodes["replica"]["role"] == "standby" and nodes["replica"].get("upstream") == "primary"
            and nodes["replica"].get("replay_pause_state") in (None, "not paused"))


@pytest.fixture(scope="session", autouse=True)
def stop_clusters_at_end():
    yield
    if os.path.isdir(TEST_HOME):
        lp("down", check=False)


@pytest.fixture
def cluster():
    """A running primary + replica on the self-test home (reset if the topology was changed by an earlier test)."""
    st = json.loads(lp("status", "--json", check=False).stdout) if os.path.isdir(TEST_HOME) else {"nodes": []}
    if st.get("initialized") and canonical_topology(st) and st.get("pg_major") == PG_MAJOR:
        lp("up")
    else:
        lp("reset")
    c = Cluster()
    wait_for(lambda: c.node("replica").get("wal_receiver_status") == "streaming", what="replica streaming")
    return c
