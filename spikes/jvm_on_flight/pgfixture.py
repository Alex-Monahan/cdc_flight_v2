"""SPIKE FIXTURE ONLY: a throw-away PostgreSQL inside the Flight container.

A Flight cannot reach any Postgres of ours in stage 1, so the JVM spike installs PostgreSQL in its own container
(root, Debian 12, apt-get), runs it as the unprivileged `postgres` user with wal_level = logical, and lets Debezium
stream from it over 127.0.0.1. The product never installs Postgres. fsync is off (the data is thrown away with the
container); this lowers the source's cost on the shared 2 vCPUs but does not change what is decoded.
"""
import os
import pwd
import secrets
import shutil
import subprocess
import time

import psycopg

BASE = "/tmp/cdc_v2_pg"
DATA = BASE + "/data"
SOCK = "/tmp/cdc_v2_pgsock"
PORT = 5432
DB = "cdc_v2_spike"
ROLE = "cdc_v2_spike_repl"
SLOT = "cdc_v2_spike_jvm_slot"
PUB = "cdc_v2_spike_jvm_pub"
PGDG_KEY = "https://www.postgresql.org/media/keys/ACCC4CF8.asc"

APT_ENV = dict(os.environ, DEBIAN_FRONTEND="noninteractive")


def _run(cmd, **kw):
    kw.setdefault("check", True)
    kw.setdefault("stdout", subprocess.PIPE)
    kw.setdefault("stderr", subprocess.STDOUT)
    kw.setdefault("text", True)
    return subprocess.run(cmd, **kw)


def apt_update():
    t = time.time()
    _run(["apt-get", "update", "-q"], env=APT_ENV, timeout=300)
    return time.time() - t


def install(major="18"):
    """Install PostgreSQL `major` from the PostgreSQL apt repository (PGDG); fall back to Debian's own version.
    Returns a dict with the version, the source and the timings."""
    res = {}
    # never let package scripts start services or create a default cluster
    with open("/usr/sbin/policy-rc.d", "w") as f:
        f.write("#!/bin/sh\nexit 101\n")
    os.chmod("/usr/sbin/policy-rc.d", 0o755)
    os.makedirs("/etc/postgresql-common", exist_ok=True)
    with open("/etc/postgresql-common/createcluster.conf", "w") as f:
        f.write("create_main_cluster = false\n")
    t = time.time()
    try:
        os.makedirs("/etc/apt/keyrings", exist_ok=True)
        _run(["curl", "-fsSL", "-o", "/etc/apt/keyrings/pgdg.asc", PGDG_KEY], timeout=60)
        with open("/etc/apt/sources.list.d/pgdg.list", "w") as f:
            f.write("deb [signed-by=/etc/apt/keyrings/pgdg.asc] https://apt.postgresql.org/pub/repos/apt "
                    "bookworm-pgdg main\n")
        res["apt_update_s"] = apt_update()
        t2 = time.time()
        _run(["apt-get", "install", "-y", "-q", "--no-install-recommends", "-o", "Dpkg::Options::=--force-confold",
              "postgresql-%s" % major], env=APT_ENV, timeout=600)
        res["apt_install_s"] = time.time() - t2
        res["source"] = "apt.postgresql.org (PGDG)"
    except subprocess.CalledProcessError as exc:
        res["pgdg_error"] = (exc.stdout or "")[-500:]
        major = "15"
        for p in ("/etc/apt/sources.list.d/pgdg.list",):
            if os.path.exists(p):
                os.remove(p)
        res["apt_update_s"] = apt_update()
        t2 = time.time()
        _run(["apt-get", "install", "-y", "-q", "--no-install-recommends", "-o", "Dpkg::Options::=--force-confold",
              "postgresql-15"], env=APT_ENV, timeout=600)
        res["apt_install_s"] = time.time() - t2
        res["source"] = "Debian bookworm"
    res["install_total_s"] = time.time() - t
    res["major"] = major
    res["bindir"] = "/usr/lib/postgresql/%s/bin" % major
    res["version"] = _run([res["bindir"] + "/postgres", "--version"]).stdout.strip()
    return res


def _as_postgres(cmd, **kw):
    pw = pwd.getpwnam("postgres")
    return _run(cmd, user=pw.pw_uid, group=pw.pw_gid, extra_groups=[], cwd=BASE, **kw)


def start(bindir, settings=None):
    """initdb + start as the unprivileged `postgres` user. Returns timings."""
    t = time.time()
    pw = pwd.getpwnam("postgres")
    for d in (BASE, SOCK):
        shutil.rmtree(d, ignore_errors=True)
        os.makedirs(d)
        os.chown(d, pw.pw_uid, pw.pw_gid)
    _as_postgres([bindir + "/initdb", "-D", DATA, "-U", "postgres", "--auth-local=trust",
                  "--auth-host=scram-sha-256", "-E", "UTF8", "--locale=C.UTF-8"], timeout=120)
    conf = {
        "listen_addresses": "'127.0.0.1'", "port": str(PORT), "unix_socket_directories": "'%s'" % SOCK,
        "wal_level": "logical", "max_wal_senders": "10", "max_replication_slots": "10",
        "shared_buffers": "1GB", "fsync": "off", "synchronous_commit": "off", "full_page_writes": "on",
        "max_wal_size": "16GB", "checkpoint_timeout": "30min", "wal_sender_timeout": "60s",
        "logical_decoding_work_mem": "64MB", "log_min_messages": "warning",
    }
    conf.update(settings or {})
    with open(DATA + "/postgresql.auto.conf", "a") as f:
        for k, v in conf.items():
            f.write("%s = %s\n" % (k, v))
    t_init = time.time() - t
    t = time.time()
    _as_postgres([bindir + "/pg_ctl", "-D", DATA, "-l", BASE + "/server.log", "-w", "-t", "60", "start"],
                 timeout=90)
    return {"initdb_s": t_init, "start_s": time.time() - t, "settings": conf}


def connect(db=DB, autocommit=True):
    return psycopg.connect(host=SOCK, port=PORT, dbname=db, user="postgres", autocommit=autocommit)


LINEITEM = """
CREATE TABLE lineitem (
  l_orderkey bigint NOT NULL, l_partkey integer NOT NULL, l_suppkey integer NOT NULL,
  l_linenumber integer NOT NULL, l_quantity numeric(15,2) NOT NULL, l_extendedprice numeric(15,2) NOT NULL,
  l_discount numeric(15,2) NOT NULL, l_tax numeric(15,2) NOT NULL, l_returnflag char(1) NOT NULL,
  l_linestatus char(1) NOT NULL, l_shipdate date NOT NULL, l_commitdate date NOT NULL, l_receiptdate date NOT NULL,
  l_shipinstruct char(25) NOT NULL, l_shipmode char(10) NOT NULL, l_comment varchar(44) NOT NULL,
  PRIMARY KEY (l_orderkey, l_linenumber));
-- realistic TPC-H-like values from a row number i (unique key per i)
CREATE FUNCTION li_row(i bigint) RETURNS SETOF lineitem LANGUAGE sql IMMUTABLE AS $$
  SELECT (i / 4 + 1)::bigint, ((i * 7919) % 200000 + 1)::int, ((i * 104729) % 10000 + 1)::int, (i % 4 + 1)::int,
         ((i % 50) + 1)::numeric(15,2), round(((i * 31) % 9000000) / 100.0 + 900, 2)::numeric(15,2),
         ((i % 11) / 100.0)::numeric(15,2), ((i % 9) / 100.0)::numeric(15,2),
         (ARRAY['A','N','R'])[i % 3 + 1]::char(1), (ARRAY['O','F'])[i % 2 + 1]::char(1),
         (DATE '1992-01-01' + (i % 2500)::int)::date, (DATE '1992-01-31' + (i % 2500)::int)::date,
         (DATE '1992-01-15' + (i % 2500)::int)::date,
         (ARRAY['DELIVER IN PERSON','COLLECT COD','NONE','TAKE BACK RETURN'])[i % 4 + 1]::char(25),
         (ARRAY['REG AIR','AIR','RAIL','SHIP','TRUCK','MAIL','FOB'])[i % 7 + 1]::char(10),
         substr(md5(i::text) || md5((i + 7)::text), 1, (10 + (i % 34))::int)::varchar(44)
$$;
CREATE SEQUENCE small_tx_seq START 4000000000;
"""


def setup(bindir):
    """Database, table, publication and a REPLICATION role for Debezium. Returns the role's password (never
    printed; it is written only into the mode-600 engine properties file)."""
    t = time.time()
    with connect("postgres") as c:
        c.execute("CREATE DATABASE %s" % DB)
    password = secrets.token_urlsafe(24)
    with connect() as c:
        c.execute(LINEITEM)
        c.execute("CREATE ROLE %s LOGIN REPLICATION PASSWORD %%s" % ROLE, (password,))
        c.execute("GRANT SELECT ON lineitem TO %s" % ROLE)
        c.execute("CREATE PUBLICATION %s FOR TABLE lineitem" % PUB)
    return password, time.time() - t


def create_slot():
    with connect() as c:
        c.execute("SELECT pg_create_logical_replication_slot(%s, 'pgoutput')", (SLOT,))


def drop_slot():
    with connect() as c:
        c.execute("SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = %s",
                  (SLOT,))


def slot_state():
    with connect() as c:
        r = c.execute("SELECT active, active_pid, confirmed_flush_lsn::text, "
                      "pg_current_wal_lsn()::text, (SELECT count(*) FROM pg_stat_replication) "
                      "FROM pg_replication_slots WHERE slot_name = %s", (SLOT,)).fetchone()
    if r is None:
        return None
    return {"active": r[0], "active_pid": r[1], "confirmed_flush_lsn": r[2], "current_lsn": r[3],
            "walsenders": r[4]}


def walsender_pids():
    with connect() as c:
        return [r[0] for r in c.execute("SELECT pid FROM pg_stat_replication").fetchall()]


def insert_rows(start, count, one_tx=True):
    """Insert rows li_row(start .. start+count-1). Returns (seconds, commit wall time)."""
    t = time.time()
    with connect(autocommit=not one_tx) as c:
        c.execute("INSERT INTO lineitem SELECT r.* FROM generate_series(%s::bigint, %s::bigint) g, "
                  "LATERAL li_row(g) r", (start, start + count - 1))
        if one_tx:
            c.commit()
    return time.time() - t, time.time()


def stop(bindir, mode="fast"):
    try:
        _as_postgres([bindir + "/pg_ctl", "-D", DATA, "-m", mode, "-w", "stop"], timeout=60)
    except Exception:  # noqa: BLE001
        pass
