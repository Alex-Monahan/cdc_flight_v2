"""The private Postgres pair of package E (home ~/.cache/cdc_flight_v2/pg-e, ports 55620-55627) for engine tests.

Only through tools/localpg; every connection names its port. The engine decodes from the standby, as in production.
"""
import json
import os
import subprocess
import threading
import time
import uuid

import psycopg

from cdc_v2.stream.engine import EngineSettings

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
LOCALPG = os.path.join(ROOT, "tools", "localpg")
HOME = os.path.expanduser(os.environ.get("CDC_V2_E_PG_HOME", "~/.cache/cdc_flight_v2/pg-e"))
BASE = int(os.environ.get("CDC_V2_E_PG_PORT_BASE", "55620"))
assert 55620 <= BASE <= 55620, "package E owns ports 55620-55627"


def lp(*args, timeout=300, check=True):
    env = {k: v for k, v in os.environ.items() if not k.startswith("PG")}
    p = subprocess.run([LOCALPG, "--home", HOME, "--port-base", str(BASE), *args], capture_output=True, text=True,
                       timeout=timeout, env=env)
    if check and p.returncode != 0:
        raise AssertionError("localpg %s failed: %s %s" % (" ".join(args), p.stdout[-2000:], p.stderr[-2000:]))
    return p


class Pair:
    def __init__(self):
        self.refresh()

    def refresh(self):
        self.info = json.loads(lp("conninfo", "--json").stdout)
        with open(self.info["passwords_json"]) as f:
            self.passwords = json.load(f)
        self.primary_port = self.info["primary"]["port"]
        self.replica_port = self.info["replica"]["port"]
        return self

    def connect(self, port, role="admin", autocommit=True, **kw):
        kind = {"cdc_v2_boot": "bootstrap"}.get(role, role)
        creds = self.passwords[kind]
        if kind == "bootstrap":
            params = dict(host=self.info["socket_dir"], port=port, dbname=self.info["database"])
        else:
            params = dict(host="localhost", port=port, dbname=self.info["database"], sslmode="verify-full",
                          sslrootcert=self.info["ca_cert"])
        params.update(user=creds["user"], password=creds["password"], connect_timeout=10, gssencmode="disable")
        params.update(kw)
        return psycopg.connect(autocommit=autocommit, **params)

    def q(self, port, sql, params=None, role="admin"):
        with self.connect(port, role) as c:
            cur = c.execute(sql, params)
            return cur.fetchall() if cur.description else None

    def p(self, sql, params=None, role="admin"):
        return self.q(self.primary_port, sql, params, role)

    def r(self, sql, params=None, role="admin"):
        return self.q(self.replica_port, sql, params, role)

    def wait_replayed(self, timeout=30):
        lsn = self.p("select pg_current_wal_lsn()::text")[0][0]
        deadline = time.time() + timeout
        while time.time() < deadline:
            if self.r("select pg_last_wal_replay_lsn() >= %s::pg_lsn", (lsn,))[0][0]:
                return lsn
            time.sleep(0.05)
        raise AssertionError("replica did not replay %s" % lsn)

    def create_slot(self, slot, timeout=60):
        """A logical slot on the standby (our code creates it, research 2.4); pg_log_standby_snapshot() on the
        primary until the creation returns."""
        result, errors = {}, []

        def create():
            try:
                result["lsn"] = self.r("select lsn::text from pg_create_logical_replication_slot(%s, 'pgoutput')",
                                       (slot,), role="runtime")[0][0]
            except Exception as e:  # noqa: BLE001
                errors.append(e)

        t = threading.Thread(target=create)
        t.start()
        deadline = time.time() + timeout
        while t.is_alive() and time.time() < deadline:
            self.p("select pg_log_standby_snapshot()", role="bootstrap")
            t.join(0.2)
        assert not t.is_alive(), "slot creation on the standby did not finish"
        assert not errors, errors
        return result["lsn"]

    def drop_slot(self, slot):
        for _ in range(100):
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
        raise AssertionError("could not drop slot %s" % slot)

    def slot(self, slot):
        rows = self.r("select confirmed_flush_lsn::text, active, active_pid, restart_lsn::text "
                      "from pg_replication_slots where slot_name = %s", (slot,))
        return rows[0] if rows else None

    def settings(self, slot, publication, **kw):
        rt = self.passwords["runtime"]
        return EngineSettings(host="localhost", port=self.replica_port, dbname=self.info["database"],
                              user=rt["user"], password=rt["password"], slot=slot, publication=publication,
                              sslmode="verify-full", sslrootcert=self.info["ca_cert"], **kw)


class Scenario:
    """A schema, a publication and a slot of one test; everything is removed by `close()`."""

    def __init__(self, pair: Pair, tables_sql=(), extra_tables=()):
        self.pair = pair
        self.tag = uuid.uuid4().hex[:8]
        self.schema = "cdc_v2_e_" + self.tag
        self.pub = "cdc_v2_e_pub_" + self.tag
        self.slot = "cdc_v2_e_slot_" + self.tag
        pair.p("create schema %s" % self.schema)
        pair.p("grant usage on schema %s to cdc_v2_runtime" % self.schema)
        pair.p("alter default privileges in schema %s grant select on tables to cdc_v2_runtime" % self.schema)
        for sql in tables_sql:
            pair.p(sql.format(s=self.schema))
        self.pair.p("create publication %s" % self.pub)
        self.publish(*self.tables())
        pair.wait_replayed()
        self.slot_lsn = pair.create_slot(self.slot)

    def tables(self):
        return [r[0] for r in self.pair.p("select tablename from pg_tables where schemaname = %s", (self.schema,))]

    def publish(self, *tables):
        if tables:
            self.pair.p("alter publication %s add table %s" % (
                self.pub, ", ".join('%s."%s"' % (self.schema, t.replace('"', '""')) for t in tables)))

    def settings(self, **kw):
        return self.pair.settings(self.slot, self.pub, **kw)

    def close(self):
        try:
            self.pair.drop_slot(self.slot)
        finally:
            self.pair.p("drop publication if exists %s" % self.pub)
            self.pair.p("drop schema if exists %s cascade" % self.schema)
