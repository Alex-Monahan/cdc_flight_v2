"""The private Postgres pair of package Y (home ~/.cache/cdc_flight_v2/pg-y, ports 55656-55663), for the
cross-path tests. Only through tools/localpg; every connection names its port. The engine decodes from the standby,
as in production; the backfill side reads the standby through `cdc_v2.pg.connect` (the fixed session settings of
docs/design/source.md section 1)."""
from __future__ import annotations

import json
import os
import subprocess
import threading
import time
import uuid

import psycopg

from cdc_v2 import pg
from cdc_v2.deadline import Deadline

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
LOCALPG = os.path.join(ROOT, "tools", "localpg")
HOME = os.path.expanduser("~/.cache/cdc_flight_v2/pg-y")
BASE = 55656
PG16_BIN = "/opt/homebrew/opt/postgresql@16/bin"


def lp(*args, timeout=600, check=True, pg_bin=None):
    env = {k: v for k, v in os.environ.items() if not k.startswith("PG")}
    cmd = [LOCALPG, "--home", HOME, "--port-base", str(BASE)]
    if pg_bin:
        cmd += ["--pg-bin", pg_bin]
    p = subprocess.run(cmd + list(args), capture_output=True, text=True, timeout=timeout, env=env)
    if check and p.returncode != 0:
        raise AssertionError("localpg %s failed: %s %s" % (" ".join(args), p.stdout[-1500:], p.stderr[-1500:]))
    return p


class Pair:
    def __init__(self, pg_bin=None):
        self.pg_bin = pg_bin
        self.info = json.loads(lp("conninfo", "--json", pg_bin=pg_bin).stdout)
        with open(self.info["passwords_json"]) as f:
            self.passwords = json.load(f)
        self.primary_port = self.info["primary"]["port"]
        self.replica_port = self.info["replica"]["port"]
        assert BASE <= self.primary_port < BASE + 8 and BASE <= self.replica_port < BASE + 8

    def connect(self, port, role="admin", autocommit=True):
        kind = {"cdc_v2_boot": "bootstrap"}.get(role, role)
        creds = self.passwords[kind]
        if kind == "bootstrap":
            params = dict(host=self.info["socket_dir"], port=port, dbname=self.info["database"])
        else:
            params = dict(host="localhost", port=port, dbname=self.info["database"], sslmode="verify-full",
                          sslrootcert=self.info["ca_cert"])
        params.update(user=creds["user"], password=creds["password"], connect_timeout=10, gssencmode="disable")
        return psycopg.connect(autocommit=autocommit, **params)

    def p(self, sql, params=None, role="admin"):
        with self.connect(self.primary_port, role) as c:
            cur = c.execute(sql, params)
            return cur.fetchall() if cur.description else None

    def r(self, sql, params=None, role="admin"):
        with self.connect(self.replica_port, role) as c:
            cur = c.execute(sql, params)
            return cur.fetchall() if cur.description else None

    def server_version_num(self):
        return int(self.p("show server_version_num")[0][0])

    def node(self, port, role="runtime"):
        creds = self.passwords[role]
        return pg.NodeSettings(host="localhost", port=port, dbname=self.info["database"], user=creds["user"],
                               password=pg.Password(fn=lambda: creds["password"]), sslrootcert=self.info["ca_cert"])

    def read_conn(self, role="runtime"):
        """A read connection of package P on the standby (the backfill's session settings)."""
        return pg.connect(self.node(self.replica_port, role), Deadline.after(20))

    def wait_replayed(self, timeout=60):
        lsn = self.p("select pg_current_wal_lsn()::text")[0][0]
        end = time.time() + timeout
        while time.time() < end:
            if self.r("select pg_last_wal_replay_lsn() >= %s::pg_lsn", (lsn,))[0][0]:
                return lsn
            time.sleep(0.05)
        raise AssertionError("replica did not replay")

    def create_slot(self, slot, timeout=90):
        result, errors = {}, []

        def create():
            try:
                result["lsn"] = self.r("select lsn::text from pg_create_logical_replication_slot(%s, 'pgoutput')",
                                       (slot,), role="runtime")[0][0]
            except Exception as e:  # noqa: BLE001
                errors.append(e)

        t = threading.Thread(target=create)
        t.start()
        end = time.time() + timeout
        while t.is_alive() and time.time() < end:
            self.p("select pg_log_standby_snapshot()", role="bootstrap")
            t.join(0.2)
        assert not t.is_alive() and not errors, errors
        return result["lsn"]

    def drop_slot(self, slot):
        for _ in range(200):
            row = self.r("select active_pid from pg_replication_slots where slot_name = %s", (slot,))
            if not row:
                return
            if row[0][0] is not None:
                self.r("select pg_terminate_backend(%s)", (row[0][0],), role="bootstrap")
                time.sleep(0.1)
                continue
            try:
                self.r("select pg_drop_replication_slot(%s)", (slot,), role="runtime")
                return
            except psycopg.errors.ObjectInUse:
                time.sleep(0.1)
        raise AssertionError("could not drop slot")

    def engine_settings(self, slot, publication, **kw):
        from cdc_v2.stream.engine import EngineSettings
        rt = self.passwords["runtime"]
        return EngineSettings(host="localhost", port=self.replica_port, dbname=self.info["database"],
                              user=rt["user"], password=rt["password"], slot=slot, publication=publication,
                              sslmode="verify-full", sslrootcert=self.info["ca_cert"], heap_mb=512, **kw)

    def oracle_source(self, node="primary"):
        from cdc_v2.oracle import Source
        creds = self.passwords["admin"]
        port = self.primary_port if node == "primary" else self.replica_port
        return Source(host="localhost", port=port, dbname=self.info["database"], user=creds["user"],
                      password=creds["password"], sslmode="verify-full", sslrootcert=self.info["ca_cert"])


class Scenario:
    """A schema, a publication and a slot of one test; `close()` removes them, also after a failure."""

    def __init__(self, pair: Pair, ddl=(), extensions=()):
        self.pair = pair
        self.tag = uuid.uuid4().hex[:8]
        self.schema = "cdc_v2_y_" + self.tag
        self.pub = "cdc_v2_y_pub_" + self.tag
        self.slot = "cdc_v2_y_slot_" + self.tag
        self.slot_made = False
        for ext in extensions:
            pair.p("create extension if not exists %s" % ext, role="bootstrap")
        pair.p("create schema %s" % self.schema)
        pair.p("grant usage on schema %s to cdc_v2_runtime" % self.schema)
        pair.p("alter default privileges in schema %s grant select on tables to cdc_v2_runtime" % self.schema)
        for sql in ddl:
            pair.p(sql.replace("{s}", self.schema))

    def start(self):
        tables = [r[0] for r in self.pair.p("select tablename from pg_tables where schemaname = %s", (self.schema,))]
        self.pair.p("create publication %s for table %s" % (
            self.pub, ", ".join("%s.%s" % (self.schema, t) for t in tables)))
        self.pair.wait_replayed()
        self.pair.create_slot(self.slot)
        self.slot_made = True

    def close(self):
        try:
            if self.slot_made:
                self.pair.drop_slot(self.slot)
        finally:
            self.pair.p("drop publication if exists %s" % self.pub)
            self.pair.p("drop schema if exists %s cascade" % self.schema)
