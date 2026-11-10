"""Fixtures for the package P (source) tests that need Postgres (marker `localpg`).

Clusters (docs/design/p3_plan.md section 4, package P):
  * main pair: `~/.cache/cdc_flight_v2/pg-p`, ports 55640-55647 (Postgres 18), or with CDC_V2_P_PG_MAJOR=16
    `~/.cache/cdc_flight_v2/pg-p2`, ports 55648-55655 (Postgres 16 binaries);
  * breakable pair (promotion, settings changes): `pg-p2`, ports 55648-55655, same major version as the run.
TCP fault proxies of these tests use the upper ports of the main range (55644-55647 or 55652-55655).

    uv run pytest -m localpg tests/pg_source                          # Postgres 18
    CDC_V2_P_PG_MAJOR=16 uv run pytest -m localpg tests/pg_source     # Postgres 16

Every cluster that the session started is stopped at the end of the session.
"""
from __future__ import annotations

import json
import os
import shutil
import signal
import struct
import subprocess
import threading
import time
import uuid

import psycopg
import pytest

from cdc_v2 import pg
from cdc_v2.deadline import Deadline

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
LOCALPG = os.path.join(ROOT, "tools", "localpg")
CACHE = os.path.expanduser("~/.cache/cdc_flight_v2")
MAJOR = int(os.environ.get("CDC_V2_P_PG_MAJOR", "18"))
BINS = {18: "/opt/homebrew/opt/postgresql@18/bin", 16: "/opt/homebrew/opt/postgresql@16/bin"}
if MAJOR not in BINS:
    raise RuntimeError("CDC_V2_P_PG_MAJOR must be 16 or 18 (17 is not installed on this machine)")
PG_BIN = BINS[MAJOR]
MAIN = ("pg-p", 55640) if MAJOR == 18 else ("pg-p2", 55648)
BREAK = ("pg-p2", 55648)
PROXY_PORTS = range(MAIN[1] + 4, MAIN[1] + 8)
assert all(55640 <= p <= 55655 for p in PROXY_PORTS)


def pytest_collection_modifyitems(config, items):
    if not os.access(os.path.join(PG_BIN, "postgres"), os.X_OK):
        skip = pytest.mark.skip(reason="Postgres binaries not found in %s" % PG_BIN)
        for item in items:
            if "localpg" in item.keywords:
                item.add_marker(skip)


def wait_for(fn, timeout=30.0, interval=0.1, what="condition"):
    end = time.monotonic() + timeout
    last = None
    while time.monotonic() < end:
        try:
            last = fn()
            if last:
                return last
        except psycopg.Error as e:
            last = e
        time.sleep(interval)
    raise AssertionError("timed out after %ss waiting for %s (last: %r)" % (timeout, what, last))


class Cluster:
    """A primary + standby pair of tools/localpg on a home of package P."""

    def __init__(self, home, base):
        self.home_name, self.base = home, base
        self.home = os.path.join(CACHE, home)
        self.started = False

    def lp(self, *args, check=True, timeout=300):
        env = {k: v for k, v in os.environ.items() if not k.startswith("PG")}
        p = subprocess.run([LOCALPG, "--home", self.home, "--port-base", str(self.base), "--pg-bin", PG_BIN, *args],
                           capture_output=True, text=True, timeout=timeout, env=env)
        if check and p.returncode != 0:
            raise AssertionError("localpg %s failed (%d): %s %s" % (" ".join(args), p.returncode, p.stdout, p.stderr))
        return p

    def status(self):
        if not os.path.isdir(self.home):
            return {"nodes": []}
        return json.loads(self.lp("status", "--json", check=False).stdout or "{}")

    def ensure(self):
        """Running canonical pair of the run's major version (reset when the topology or the version differ)."""
        st = self.status()
        nodes = {n["name"]: n for n in st.get("nodes", [])}
        ok = (st.get("initialized") and set(nodes) == {"primary", "replica"} and st.get("pg_major") == MAJOR
              and nodes["replica"]["role"] == "standby" and nodes["primary"]["role"] == "primary"
              and not nodes["primary"].get("overrides") and not nodes["replica"].get("overrides"))
        self.lp("up" if ok else "reset")
        self.started = True
        self.load()
        wait_for(lambda: self.boot("replica", "select status from pg_stat_wal_receiver") == [("streaming",)],
                 what="replica streaming")
        return self

    def load(self):
        info = json.loads(self.lp("conninfo", "--json").stdout)
        self.info = info
        with open(info["passwords_json"]) as f:
            self.passwords = json.load(f)
        self.ports = {n["name"]: n["port"] for n in info["nodes"]}

    # -- settings and connections
    def settings(self, node="primary", role="runtime", **kw) -> pg.NodeSettings:
        creds = self.passwords[role]
        pw = creds["password"]
        return pg.NodeSettings(host="localhost", port=kw.pop("port", self.ports[node]), dbname=self.info["database"],
                               user=creds["user"], password=pg.Password(fn=lambda: pw),
                               sslrootcert=kw.pop("sslrootcert", self.info["ca_cert"]), **kw)

    def connect(self, node="primary", role="runtime", deadline=10.0, **kw) -> pg.PgConnection:
        return pg.connect(self.settings(node, role, **kw), Deadline.after(deadline))

    def raw(self, node="primary", role="admin", **kw):
        creds = self.passwords[role]
        params = dict(host="localhost", port=self.ports[node], dbname=self.info["database"], user=creds["user"],
                      password=creds["password"], sslmode="verify-full", sslrootcert=self.info["ca_cert"],
                      connect_timeout=5, gssencmode="disable")
        params.update(kw)
        return psycopg.connect(autocommit=True, **params)

    def boot_conn(self, node="primary", db=None):
        creds = self.passwords["bootstrap"]
        return psycopg.connect(host=self.info["socket_dir"], port=self.ports[node], dbname=db or self.info["database"],
                               user=creds["user"], password=creds["password"], autocommit=True, connect_timeout=5)

    def q(self, node, sql, params=None, role="admin"):
        with self.raw(node, role) as c:
            cur = c.execute(sql, params)
            return cur.fetchall() if cur.description else None

    def boot(self, node, sql, params=None, db=None):
        with self.boot_conn(node, db) as c:
            cur = c.execute(sql, params)
            return cur.fetchall() if cur.description else None

    def replay_to_primary(self, node="replica", timeout=30):
        lsn = self.boot("primary", "select pg_current_wal_insert_lsn()")[0][0]
        wait_for(lambda: self.boot(node, "select pg_last_wal_replay_lsn() >= %s", (lsn,))[0][0], timeout=timeout,
                 what="replay on %s" % node)

    def cleanup_slots(self, prefix):
        for node in list(self.ports):
            try:
                self.boot(node, "select pg_terminate_backend(active_pid) from pg_replication_slots "
                                "where slot_name like %s and active_pid is not null", (prefix + "%",))
                wait_for(lambda: not self.boot(node, "select 1 from pg_replication_slots where slot_name like %s "
                                                     "and active", (prefix + "%",)), timeout=10, what="inactive")
                self.boot(node, "select pg_drop_replication_slot(slot_name) from pg_replication_slots "
                                "where slot_name like %s", (prefix + "%",))
            except (psycopg.Error, AssertionError):
                pass


_clusters: dict[str, Cluster] = {}
EVIDENCE: dict = {}
EVIDENCE_DIR = os.path.join(ROOT, "evidence", "p3", "source")


def record(section: str, key: str, value) -> None:
    """Keep a measured fact for the evidence file of this run (written at the end when CDC_V2_P_EVIDENCE=1)."""
    EVIDENCE.setdefault(section, {})[key] = value


def get_cluster(spec) -> Cluster:
    key = spec[0]
    if key not in _clusters:
        _clusters[key] = Cluster(*spec)
    return _clusters[key]


@pytest.fixture(scope="session", autouse=True)
def _stop_clusters_at_end():
    yield
    versions = {}
    for c in _clusters.values():
        if c.started:
            try:
                versions[c.home_name] = c.boot("primary", "select version()")[0][0]
            except Exception:       # noqa: BLE001
                pass
            c.lp("down", check=False)
    if os.environ.get("CDC_V2_P_EVIDENCE") == "1" and EVIDENCE:
        from cdc_v2.testing.harness_evidence import write_evidence
        # a partial run names its own file and command (CDC_V2_P_EVIDENCE_NAME, CDC_V2_P_EVIDENCE_COMMAND), so that
        # it does not replace the file of the whole suite
        name = os.environ.get("CDC_V2_P_EVIDENCE_NAME") or "localpg_pg%d.json" % MAJOR
        assert os.path.basename(name) == name and name.endswith(".json"), name
        write_evidence(os.path.join(EVIDENCE_DIR, name), {
            "what": "package P (source) tests on a private tools/localpg pair; times in seconds",
            "command": os.environ.get("CDC_V2_P_EVIDENCE_COMMAND") or (
                "CDC_V2_P_EVIDENCE=1 CDC_V2_P_PG_MAJOR=%d uv run pytest -m localpg tests/pg_source" % MAJOR),
            "major": MAJOR, "server_versions": versions, "psycopg": psycopg.__version__,
            "machine_note": "shared 10-core laptop, up to three other workers running; indications only",
            "results": EVIDENCE})


@pytest.fixture(scope="session")
def cluster() -> Cluster:
    return get_cluster(MAIN).ensure()


@pytest.fixture
def breakable() -> Cluster:
    """The breakable pair; the test may promote or reconfigure it. It is rebuilt by the next user if needed."""
    c = get_cluster(BREAK)
    c.ensure()
    yield c
    c.lp("reset")
    c.load()                # new passwords (with the 16 binaries the main pair is this same home)


class Scratch:
    """Per-test objects: a schema owned by the admin role, names for publications and slots, and their cleanup."""

    def __init__(self, cluster: Cluster):
        self.c = cluster
        self.tag = uuid.uuid4().hex[:8]
        self.schema = "cdc_v2_t_" + self.tag
        self.pub = "cdc_v2_pub_" + self.tag
        self.slot = "cdc_v2_slot_" + self.tag
        self.runtime = cluster.passwords["runtime"]["user"]
        cluster.q("primary", 'create schema "%s"' % self.schema)
        cluster.q("primary", 'grant usage on schema "%s" to %s' % (self.schema, self.runtime))
        cluster.q("primary", 'alter default privileges in schema "%s" grant select on tables to %s'
                  % (self.schema, self.runtime))
        self.conns = []

    def connect(self, node="primary", role="runtime", **kw):
        conn = self.c.connect(node, role, **kw)
        self.conns.append(conn)
        return conn

    def admin(self, sql, params=None, node="primary"):
        return self.c.q(node, sql, params, role="admin")

    def close(self):
        for conn in self.conns:
            conn.close()
        self.c.cleanup_slots("cdc_v2_slot_" + self.tag)
        for (pub,) in self.c.q("primary", "select pubname from pg_publication where pubname like %s",
                               ("cdc_v2_pub_" + self.tag + "%",)) or []:
            self.c.q("primary", 'drop publication if exists "%s"' % pub)
        self.c.q("primary", 'drop schema if exists "%s" cascade' % self.schema)


@pytest.fixture
def scratch(cluster):
    s = Scratch(cluster)
    yield s
    s.close()


# ----------------------------------------------------------------------------------------------- pgoutput reader

def pgoutput_changes(cluster, node, slot, pub, upto=None):
    """Consume a slot with pg_logical_slot_get_binary_changes (protocol 1); returns parsed messages."""
    rows = cluster.q(node, "select lsn::text, xid::text, data from pg_logical_slot_get_binary_changes(%s, %s, null, "
                           "'proto_version', '1', 'publication_names', %s, 'messages', 'true')",
                     (slot, upto, pub), role="runtime")
    return [parse_pgoutput(bytes(r[2])) | {"lsn": r[0]} for r in rows]


def parse_pgoutput(m: bytes) -> dict:
    kind = chr(m[0])
    if kind == "B":
        final, ts, xid = struct.unpack(">QqI", m[1:21])
        return {"kind": "B", "final_lsn": final, "xid": xid}
    if kind == "C":
        _, commit, end, ts = struct.unpack(">bQQq", m[1:26])
        return {"kind": "C", "commit_lsn": commit, "end_lsn": end}
    if kind == "R":
        relid = struct.unpack(">I", m[1:5])[0]
        rest = m[5:]
        nsp, rest = rest.split(b"\0", 1)
        rel, rest = rest.split(b"\0", 1)
        return {"kind": "R", "relid": relid, "schema": nsp.decode(), "name": rel.decode()}
    if kind in "IUD":
        relid = struct.unpack(">I", m[1:5])[0]
        return {"kind": kind, "relid": relid, "tuple": _tuple_after(m[5:], kind)}
    if kind == "M":
        return {"kind": "M", "raw": m}
    return {"kind": kind}


def _tuple_after(b, kind):
    # I: 'N' tuple; U: optional 'K'/'O' tuple then 'N' tuple; D: 'K' or 'O' tuple
    tuples = []
    while b:
        tag = chr(b[0])
        ncols = struct.unpack(">H", b[1:3])[0]
        i = 3
        vals = []
        for _ in range(ncols):
            t = chr(b[i]); i += 1
            if t == "t":
                ln = struct.unpack(">I", b[i:i + 4])[0]; i += 4
                vals.append(b[i:i + ln].decode()); i += ln
            elif t == "n":
                vals.append(None)
            else:
                vals.append("<unchanged>")
        tuples.append((tag, vals))
        b = b[i:]
    return tuples


# ----------------------------------------------------------------------------------------------- slot consumer

class Recvlogical:
    """pg_recvlogical as a real walsender client that holds a slot (started by the test, killed by the test)."""

    def __init__(self, cluster: Cluster, node: str, slot: str, pub: str, role="runtime"):
        creds = cluster.passwords[role]
        dsn = ("host=localhost port=%d dbname=%s user=%s sslmode=verify-full sslrootcert=%s"
               % (cluster.ports[node], cluster.info["database"], creds["user"], cluster.info["ca_cert"]))
        env = {k: v for k, v in os.environ.items() if not k.startswith("PG")}
        env["PGPASSFILE"] = cluster.info["pgpass"]
        self.proc = subprocess.Popen(
            [os.path.join(PG_BIN, "pg_recvlogical"), "-d", dsn, "--slot", slot, "--start", "-f", "/dev/null",
             "-o", "proto_version=1", "-o", "publication_names=" + pub, "-s", "1", "-F", "1", "--no-loop"],
            env=env, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, start_new_session=True)

    def alive(self):
        return self.proc.poll() is None

    def stop(self):
        if self.proc.poll() is None:
            self.proc.send_signal(signal.SIGKILL)
        try:
            self.proc.wait(10)
        except subprocess.TimeoutExpired:
            pass
        if self.proc.stderr:
            self.proc.stderr.close()


@pytest.fixture
def consumers():
    started = []

    def start(*a, **kw):
        r = Recvlogical(*a, **kw)
        started.append(r)
        return r

    yield start
    for r in started:
        r.stop()


def create_slot_on_standby_with_superuser_nudge(cluster, standby, slot, primary="primary"):
    """Test helper: create a slot on the standby as the runtime role while the bootstrap superuser nudges."""
    result, errs = {}, []

    def create():
        try:
            result["lsn"] = cluster.q(standby, "select lsn::text from pg_create_logical_replication_slot(%s, "
                                               "'pgoutput')", (slot,), role="runtime")[0][0]
        except Exception as e:  # noqa: BLE001
            errs.append(e)

    t = threading.Thread(target=create)
    t.start()
    end = time.monotonic() + 60
    while t.is_alive() and time.monotonic() < end:
        cluster.boot(primary, "select pg_log_standby_snapshot()")
        t.join(0.3)
    assert not t.is_alive() and not errs, errs
    return result["lsn"]


def have_binary(name):
    return shutil.which(name, path=PG_BIN) is not None
