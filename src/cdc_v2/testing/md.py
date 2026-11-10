"""Isolated MotherDuck destinations for tier-T1 tests: databases named cdc_v2_t_<random>.

Rules (PLAN §4, §9): only databases whose names start with `cdc_v2_t_` are created, read, written or dropped here.
Nothing lists the account's databases. A ledger (one file per database under ~/.cache/cdc_flight_v2/md_ledger/) is
written before a database is created and removed after it is dropped, so `leftovers()` can verify by name alone
that no test database is left behind.

CREATE and DROP DATABASE run on a workspace connection (`md:`) that executes exactly that one statement, as the
merged P2 spikes do. Everything else uses single attach mode (`md:<name>?attach_mode=single`), where the session
has only that database attached.
"""
from __future__ import annotations

import os
import re
import secrets
import subprocess
import sys
from pathlib import Path
from typing import Optional

PREFIX = "cdc_v2_t_"
NAME = re.compile(r"^cdc_v2_t_[a-z0-9_]+$")
CACHE = Path(os.environ.get("CDC_V2_CACHE", Path.home() / ".cache" / "cdc_flight_v2"))
LEDGER = CACHE / "md_ledger"


class DestinationError(Exception):
    pass


def check_name(name: str) -> str:
    if not NAME.match(name or ""):
        raise DestinationError("test databases must be named %s<lowercase letters, digits, _>" % PREFIX)
    return name


def new_name() -> str:
    return PREFIX + secrets.token_hex(5)


def have_token() -> bool:
    return bool(os.environ.get("motherduck_token") or os.environ.get("MOTHERDUCK_TOKEN"))


def _workspace_statement(sql: str) -> None:
    import duckdb
    con = duckdb.connect("md:")
    try:
        con.execute(sql)
    finally:
        con.close()


def connect(name: str):
    import duckdb
    con = duckdb.connect("md:%s?attach_mode=single" % check_name(name))
    con.execute("SET TimeZone = 'UTC'")
    return con


def create_database(name: Optional[str] = None) -> str:
    name = check_name(name or new_name())
    LEDGER.mkdir(parents=True, exist_ok=True)
    (LEDGER / name).write_text("%d\n" % os.getpid())
    _workspace_statement("CREATE DATABASE %s" % name)
    return name


def drop_database(name: str) -> None:
    check_name(name)
    _workspace_statement("DROP DATABASE IF EXISTS %s" % name)
    try:
        (LEDGER / name).unlink()
    except FileNotFoundError:
        pass


_PROBE = """import duckdb, sys
try:
    duckdb.connect("md:%s?attach_mode=single").execute("SELECT 1").fetchall()
    print("EXISTS")
except Exception:
    print("GONE")
"""


def database_exists(name: str) -> bool:
    """True if the database can be attached. Runs in a fresh process: a process that attached a database keeps a
    cached handle that still answers after the database was dropped."""
    out = subprocess.run([sys.executable, "-c", _PROBE % check_name(name)], capture_output=True, text=True,
                         timeout=120).stdout
    return "EXISTS" in out


def leftovers(check: bool = True) -> list:
    """Ledger entries of databases that were created and not dropped (verified by attaching, if check)."""
    if not LEDGER.exists():
        return []
    names = sorted(p.name for p in LEDGER.iterdir() if NAME.match(p.name))
    if not check:
        return names
    out = []
    for n in names:
        if database_exists(n):
            out.append(n)
        else:
            (LEDGER / n).unlink(missing_ok=True)
    return out


class TestDestination:
    """A MotherDuck database of its own. `conn` is a single-attach connection; `drop()` closes it and drops."""

    __test__ = False                      # not a pytest test class

    def __init__(self, name: Optional[str] = None):
        self.name = create_database(name)
        self._conn = None

    @property
    def conn(self):
        if self._conn is None:
            self._conn = connect(self.name)
        return self._conn

    def connect(self):
        """A new, separate connection (for observers and concurrent writers)."""
        return connect(self.name)

    def destination(self, conn=None, in_transaction: bool = False):
        from cdc_v2.oracle import Destination
        return Destination(self.name, conn=conn, in_transaction=in_transaction)

    def drop(self) -> None:
        if self._conn is not None:
            try:
                self._conn.close()
            finally:
                self._conn = None
        drop_database(self.name)
