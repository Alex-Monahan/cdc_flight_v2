"""Isolated MotherDuck destinations for tier-T1 tests: databases named <prefix><random>.

The prefix comes from `CDC_V2_MD_PREFIX` (default `cdc_v2_t_`; it must start with `cdc_v2_t_`), so that each
package's tests create only databases with that package's prefix (docs/design/p3_plan.md section 4).

Rules (PLAN §4, §9): only databases with that prefix are created, read, written or dropped here. Nothing lists the
account's databases. A ledger (one file per database under ~/.cache/cdc_flight_v2/md_ledger/) is written before a
database is created and removed after it is dropped, so `leftovers()` can verify by name alone that no test
database is left behind.

CREATE and DROP DATABASE run on a workspace connection (`md:`) that executes exactly that one statement, as the
merged P2 spikes do. Everything else uses single attach mode (`md:<name>?attach_mode=single`), where the session
has only that database attached. Every call has a deadline (C0).
"""
from __future__ import annotations

import os
import re
import secrets
import subprocess
import sys
from pathlib import Path
from typing import Optional

from cdc_v2.deadline import Deadline
from cdc_v2.oracle import mdexec

BASE_PREFIX = "cdc_v2_t_"
_PREFIX_RX = re.compile(r"^cdc_v2_t_([a-z0-9]+_)*$")
CACHE = Path(os.environ.get("CDC_V2_CACHE", Path.home() / ".cache" / "cdc_flight_v2"))
LEDGER = CACHE / "md_ledger"
CONNECT_SECONDS = 60.0
DDL_SECONDS = 120.0


class DestinationError(Exception):
    pass


def prefix() -> str:
    """The database prefix of this process: $CDC_V2_MD_PREFIX, default cdc_v2_t_."""
    p = os.environ.get("CDC_V2_MD_PREFIX", BASE_PREFIX)
    if not _PREFIX_RX.match(p):
        raise DestinationError("CDC_V2_MD_PREFIX must start with %s and end with _ (got %r)" % (BASE_PREFIX, p))
    return p


def check_name(name: str) -> str:
    p = prefix()
    if not re.fullmatch(re.escape(p) + r"[a-z0-9_]+", name or ""):
        raise DestinationError("test databases must be named %s<lowercase letters, digits, _>" % p)
    return name


def new_name() -> str:
    return prefix() + secrets.token_hex(5)


def have_token() -> bool:
    return bool(os.environ.get("motherduck_token") or os.environ.get("MOTHERDUCK_TOKEN"))


def _workspace_statement(sql: str) -> None:
    d = Deadline.after(DDL_SECONDS)
    con = mdexec.connect("md:", deadline=d)
    try:
        mdexec.execute(con, sql, deadline=d, fetch=None, what=sql)
    finally:
        con.close()


def connect(name: str, deadline: Optional[Deadline] = None):
    d = deadline or Deadline.after(CONNECT_SECONDS)
    con = mdexec.connect("md:%s?attach_mode=single" % check_name(name), deadline=d)
    mdexec.execute(con, "SET TimeZone = 'UTC'", deadline=d, fetch=None)
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
    """True if the database can be attached. Runs in a fresh process (with a timeout): a process that attached a
    database keeps a cached handle that still answers after the database was dropped."""
    out = subprocess.run([sys.executable, "-c", _PROBE % check_name(name)], capture_output=True, text=True,
                         timeout=120).stdout
    return "EXISTS" in out


def leftovers(check: bool = True) -> list:
    """Ledger entries with this process's prefix of databases that were created and not dropped (verified by
    attaching each by name, if check). Entries of other prefixes belong to other workers and are not touched."""
    if not LEDGER.exists():
        return []
    p = prefix()
    names = sorted(x.name for x in LEDGER.iterdir() if x.name.startswith(p))
    if not check:
        return names
    out = []
    for n in names:
        if database_exists(n):
            out.append(n)
        else:
            (LEDGER / n).unlink(missing_ok=True)
    return out


STAND_IN_SLOT = "cdc_v2_o_stand_in"


def write_stand_in_position(conn, position: int, slot_name: str = STAND_IN_SLOT,
                            deadline: Optional[Deadline] = None) -> None:
    """Tests without a pipeline: record `position` as the applied position of `slot_name` in the row layout the
    oracle's stand-in reader expects (`_cdc.stream_state(slot_name, applied_lsn UBIGINT)`). Package S's real
    reader replaces the stand-in; tests that use S's pipeline never call this."""
    d = deadline or Deadline.after(DDL_SECONDS)
    for sql, params in [("CREATE SCHEMA IF NOT EXISTS _cdc", None),
                        ("CREATE TABLE IF NOT EXISTS _cdc.stream_state (slot_name VARCHAR, applied_lsn UBIGINT)", None),
                        ("DELETE FROM _cdc.stream_state WHERE slot_name = ?", [slot_name]),
                        ("INSERT INTO _cdc.stream_state VALUES (?, ?)", [slot_name, int(position)])]:
        mdexec.execute(conn, sql, params, deadline=d, fetch=None, what="stand-in position")


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
