"""Isolated Postgres sources for tier-T1 tests, built on tools/localpg.

* `shared_cluster()` - the development cluster under ~/.cache/cdc_flight_v2/pg (primary 55432, replica 55433). It is
  shared with other workers, so only `up`, `status`, `conninfo`, `sql` and `logs` are ever run on it; `localpg()`
  refuses everything else for that home.
* `PrivateDatabase` - a database `cdc_v2_t_<random>` on a cluster, created and dropped by the test.
* `PrivateCluster` - a primary and replica of its own (own home, own port block) for destructive drills. The port base
  comes from `PortLease`, which takes an exclusive lock file under ~/.cache/cdc_flight_v2/ports/, so parallel test
  processes never share a block. Blocks start at 55520 and step by 8.
"""
from __future__ import annotations

import contextlib
import errno
import fcntl
import json
import os
import secrets
import shutil
import socket
import subprocess
import time
from pathlib import Path
from typing import Optional

REPO = Path(__file__).resolve().parents[3]
LOCALPG = REPO / "tools" / "localpg"
CACHE = Path(os.environ.get("CDC_V2_CACHE", Path.home() / ".cache" / "cdc_flight_v2"))
SHARED_HOME = Path(os.environ.get("CDC_V2_PG_HOME", CACHE / "pg")).expanduser()
SHARED_BASE = 55432
PRIVATE_BASE = 55520
PRIVATE_STEP = 8
PRIVATE_BLOCKS = 32                       # 55520 .. 55775
DB_PREFIX = "cdc_v2_t_"
# Commands that change or stop a cluster. Never run on the shared home: other workers use it at the same time.
SHARED_FORBIDDEN = {"down", "reset", "kill", "promote", "remove", "set", "unset", "follow", "add-standby"}


class HarnessError(Exception):
    pass


def localpg(*args: str, home: Optional[Path] = None, port_base: Optional[int] = None, timeout: int = 600,
            check: bool = True) -> subprocess.CompletedProcess:
    home = Path(home or SHARED_HOME).expanduser()
    cmd = next((a for a in args if not a.startswith("-")), "")
    if home.resolve() == SHARED_HOME.resolve() and cmd in SHARED_FORBIDDEN:
        raise HarnessError("refusing `localpg %s` on the shared development cluster" % cmd)
    env = {k: v for k, v in os.environ.items() if not k.startswith("PG")}
    env["CDC_V2_PG_HOME"] = str(home)
    argv = [str(LOCALPG), "--home", str(home)]
    if port_base is not None:
        argv += ["--port-base", str(port_base)]
    p = subprocess.run(argv + list(args), capture_output=True, text=True, timeout=timeout, env=env)
    if check and p.returncode != 0:
        raise HarnessError("localpg %s failed (%d): %s" % (" ".join(args), p.returncode, p.stderr.strip()[-2000:]))
    return p


class Cluster:
    """A localpg home: ports, roles and connection parameters (passwords are read from the home's secrets)."""

    def __init__(self, home: Path, port_base: Optional[int] = None):
        self.home = Path(home).expanduser()
        self.port_base = port_base
        self.refresh()

    def refresh(self) -> "Cluster":
        self.info = json.loads(localpg("conninfo", "--json", home=self.home, port_base=self.port_base).stdout)
        with open(self.info["passwords_json"]) as f:
            self.passwords = json.load(f)
        return self

    def port(self, node: str) -> int:
        for n in self.info["nodes"]:
            if n["name"] == node:
                return int(n["port"])
        raise HarnessError("no node %s" % node)

    @property
    def primary_port(self) -> int:
        return int(self.info["primary"]["port"])

    @property
    def replica_port(self) -> int:
        return int(self.info["replica"]["port"])

    def secrets(self) -> list:
        return [v["password"] for v in self.passwords.values()]

    def params(self, role: str = "admin", node: str = "primary", db: Optional[str] = None) -> dict:
        """psycopg keyword arguments. Roles: admin, runtime, bootstrap (superuser, private socket only)."""
        port = self.port(node)
        creds = self.passwords[role]
        if role == "bootstrap":
            p = dict(host=self.info["socket_dir"], port=port, dbname=db or "postgres")
        else:
            p = dict(host="localhost", port=port, dbname=db or self.info["database"], sslmode="verify-full",
                     sslrootcert=self.info["ca_cert"])
        p.update(user=creds["user"], password=creds["password"], connect_timeout=10, gssencmode="disable")
        return p

    def connect(self, role: str = "admin", node: str = "primary", db: Optional[str] = None, autocommit=True):
        import psycopg
        return psycopg.connect(autocommit=autocommit, **self.params(role, node, db))

    def sql(self, sql: str, params=None, role: str = "admin", node: str = "primary", db: Optional[str] = None):
        with self.connect(role, node, db) as c:
            cur = c.execute(sql, params)
            return cur.fetchall() if cur.description else None

    def status(self) -> dict:
        return json.loads(localpg("status", "--json", home=self.home, port_base=self.port_base, check=False).stdout)

    def wait_replica_caught_up(self, timeout: float = 30.0) -> None:
        """Wait until the replica has replayed the primary's current WAL position."""
        target = self.sql("SELECT pg_current_wal_lsn()::text")[0][0]
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            ok = self.sql("SELECT pg_last_wal_replay_lsn() >= %s::pg_lsn", (target,), node="replica")[0][0]
            if ok:
                return
            time.sleep(0.05)
        raise HarnessError("replica did not reach %s within %.0f s" % (target, timeout))


def shared_cluster(ensure_up: bool = True) -> Cluster:
    """The shared development cluster; `localpg up` is a no-op when it already runs."""
    if ensure_up:
        localpg("up")
    return Cluster(SHARED_HOME)


def new_db_name() -> str:
    return DB_PREFIX + secrets.token_hex(5)


class PrivateDatabase:
    """A database of its own on a cluster, owned by the admin role. `drop()` removes it and its logical slots."""

    def __init__(self, cluster: Cluster, name: Optional[str] = None):
        self.cluster = cluster
        self.name = name or new_db_name()
        if not self.name.startswith(DB_PREFIX) or not self.name.replace("_", "").isalnum():
            raise HarnessError("test databases are named %s<hex>" % DB_PREFIX)
        cluster.sql('CREATE DATABASE "%s"' % self.name)
        cluster.sql('GRANT ALL ON DATABASE "%s" TO cdc_v2_runtime' % self.name)
        cluster.sql("GRANT ALL ON SCHEMA public TO cdc_v2_runtime", db=self.name)

    def params(self, role: str = "admin", node: str = "primary") -> dict:
        return self.cluster.params(role, node, self.name)

    def connect(self, role: str = "admin", node: str = "primary", autocommit: bool = True):
        return self.cluster.connect(role, node, self.name, autocommit)

    def sql(self, sql: str, params=None, role: str = "admin", node: str = "primary"):
        return self.cluster.sql(sql, params, role, node, self.name)

    def source(self, role: str = "admin", node: str = "primary"):
        from cdc_v2.oracle import Source
        return Source(**self.params(role, node))

    def drop(self) -> None:
        for node in [n["name"] for n in self.cluster.info["nodes"]]:
            with contextlib.suppress(Exception):
                for (slot,) in self.cluster.sql(
                        "SELECT slot_name FROM pg_replication_slots WHERE database = %s", (self.name,), node=node):
                    self.cluster.sql("SELECT pg_drop_replication_slot(%s)", (slot,), node=node)
        self.cluster.sql('DROP DATABASE IF EXISTS "%s" WITH (FORCE)' % self.name)


# ------------------------------------------------------------------------------------------------ private clusters


def _port_in_use(port: int) -> bool:
    for family, addr in ((socket.AF_INET, "127.0.0.1"), (socket.AF_INET6, "::1")):
        with contextlib.suppress(OSError), socket.socket(family, socket.SOCK_STREAM) as s:
            s.settimeout(0.5)
            if s.connect_ex((addr, port)) == 0:
                return True
    return False


class PortLease:
    """An exclusive claim on a block of 8 ports, held through an flock'ed file for as long as the lease lives.

    The lock dies with the process, so a crashed test never blocks a port range for good. A block whose ports are in
    use by something that is not our own stale private cluster is skipped.
    """

    def __init__(self, base: int, fd: int, path: Path):
        self.base, self._fd, self.path = base, fd, path

    @classmethod
    def acquire(cls, lock_dir: Optional[Path] = None, start: int = PRIVATE_BASE, blocks: int = PRIVATE_BLOCKS,
                step: int = PRIVATE_STEP, home_for=None) -> "PortLease":
        lock_dir = Path(lock_dir or CACHE / "ports")
        lock_dir.mkdir(parents=True, exist_ok=True)
        home_for = home_for or private_home
        for i in range(blocks):
            base = start + i * step
            path = lock_dir / ("%d.lock" % base)
            fd = os.open(path, os.O_RDWR | os.O_CREAT, 0o644)
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except OSError as e:
                os.close(fd)
                if e.errno in (errno.EWOULDBLOCK, errno.EAGAIN, errno.EACCES):
                    continue
                raise
            busy = any(_port_in_use(p) for p in range(base, base + step))
            if busy and not (home_for(base) / ".localpg-home").exists():
                fcntl.flock(fd, fcntl.LOCK_UN)
                os.close(fd)
                continue
            os.ftruncate(fd, 0)
            os.write(fd, ("%d\n" % os.getpid()).encode())
            return cls(base, fd, path)
        raise HarnessError("no free private port block between %d and %d" % (start, start + blocks * step - 1))

    def release(self) -> None:
        if self._fd is not None:
            with contextlib.suppress(OSError):
                fcntl.flock(self._fd, fcntl.LOCK_UN)
            os.close(self._fd)
            self._fd = None


def private_home(base: int) -> Path:
    return CACHE / ("pg-t1-%d" % base)


class PrivateCluster(Cluster):
    """A primary and a replica of its own. Built with `localpg reset` on a leased port block; `stop()` shuts it
    down, deletes its home and releases the ports."""

    def __init__(self):
        self.lease = PortLease.acquire()
        home = private_home(self.lease.base)
        try:
            localpg("reset", home=home, port_base=self.lease.base)
            super().__init__(home, self.lease.base)
        except Exception:
            self._cleanup(home)
            raise

    def localpg(self, *args: str, **kw) -> subprocess.CompletedProcess:
        """Any localpg command on this private cluster (kill, promote, set, ... are allowed here)."""
        return localpg(*args, home=self.home, port_base=self.port_base, **kw)

    def _cleanup(self, home: Path) -> None:
        try:
            if home.exists():
                localpg("down", home=home, port_base=self.lease.base, check=False)
                if (home / ".localpg-home").exists():
                    shutil.rmtree(home)
        finally:
            self.lease.release()

    def stop(self) -> None:
        self._cleanup(self.home)
