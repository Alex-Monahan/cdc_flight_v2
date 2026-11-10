"""The destination's own tables: the leader tables (frozen shape) and the migrations of everything else in `_cdc`.

**Leader tables** (design 4.1): `_cdc.leader`, `_cdc.fence`, `_cdc.heartbeat`, `_cdc.connections`. The statements
below are constant text. The bootstrap (`flights/cdc_v2/main.py`, package L) cannot import this package before the
clone, so it carries a copy; `compare_statements` compares a copy with this text character for character.

Each of the three tables with rows is created **with its rows in one statement** (`CREATE TABLE IF NOT EXISTS ... AS
SELECT`), so no reader ever sees the table without its rows and several simultaneous first runs leave exactly one
row set: the creation is one catalog change, one starter wins it, the others get a conflict or find the table and do
nothing (`evidence/p3/destination/leader_tables_concurrent.json`). The rows are only ever updated afterwards;
`connection.check_sql` refuses a statement that would delete or re-insert them.

**Everything else** in `_cdc` is created by migration steps: one ordered list per package, registered by that
package with `register_migrations`. `migrate(control_lane)` runs the missing steps, each in its own fenced
`control`-lane transaction that also moves the package's row in `_cdc.schema_version` from the version it read to
the next one (the run-again rule of contract C4), so a step runs once however often and from however many
processes `migrate` is called, and a failing step leaves nothing behind.
"""
from __future__ import annotations

import logging
import random
import re
from dataclasses import dataclass

from . import registry
from .connection import clock_sql, as_deadline
from .errors import CommitUnknown, Conflict, ConnectionLost, DestinationError, LaneMisuse
from .lane import FENCE_SQL
from .settings import LANES

log = logging.getLogger("cdc_v2.md")


class SchemaError(DestinationError):
    """The `_cdc` schema is not what this code expects: a leader table without its rows, a migration list that
    does not match what the database recorded, or a database that is newer than the code."""


# ---------------------------------------------------------------------------------------------------------------
# Leader tables. Constant text, nothing computed. Initial values: epoch 0, state 'released', nobody has led yet.
# ---------------------------------------------------------------------------------------------------------------
CREATE_SCHEMA_SQL = "CREATE SCHEMA IF NOT EXISTS _cdc"

CREATE_LEADER_SQL = (
    "CREATE TABLE IF NOT EXISTS _cdc.leader AS SELECT "
    "CAST(0 AS BIGINT) AS epoch, "
    "CAST(NULL AS VARCHAR) AS flight_id, "
    "CAST(NULL AS BIGINT) AS run_number, "
    "CAST(NULL AS VARCHAR) AS run_id, "
    "CAST('released' AS VARCHAR) AS state, "
    "CAST(NULL AS TIMESTAMPTZ) AS taken_at"
)

CREATE_FENCE_SQL = (
    "CREATE TABLE IF NOT EXISTS _cdc.fence AS SELECT "
    "CAST(lane AS VARCHAR) AS lane, CAST(0 AS BIGINT) AS epoch, CAST(0 AS BIGINT) AS n "
    "FROM (VALUES ('stream'), ('control'), "
    "('backfill.0'), ('backfill.1'), ('backfill.2'), ('backfill.3'), "
    "('backfill.4'), ('backfill.5'), ('backfill.6'), ('backfill.7'), "
    "('backfill.8'), ('backfill.9'), ('backfill.10'), ('backfill.11'), "
    "('backfill.12'), ('backfill.13'), ('backfill.14'), ('backfill.15')) AS lanes(lane)"
)

CREATE_HEARTBEAT_SQL = (
    "CREATE TABLE IF NOT EXISTS _cdc.heartbeat AS SELECT "
    "CAST(0 AS BIGINT) AS epoch, "
    "CAST(NULL AS TIMESTAMPTZ) AS beat_at, "
    "CAST(NULL AS TIMESTAMPTZ) AS progress_at, "
    "CAST('none' AS VARCHAR) AS phase"
)

CREATE_CONNECTIONS_SQL = (
    "CREATE TABLE IF NOT EXISTS _cdc.connections "
    "(epoch BIGINT, lane VARCHAR, connection_id UUID, registered_at TIMESTAMPTZ)"
)

#: The statements that create the leader tables, in the order they run.
LEADER_TABLE_STATEMENTS = (
    CREATE_SCHEMA_SQL,
    CREATE_LEADER_SQL,
    CREATE_FENCE_SQL,
    CREATE_HEARTBEAT_SQL,
    CREATE_CONNECTIONS_SQL,
)

#: Says whether the leader tables hold exactly their rows: one row (1, 18, 18, 1) when they do.
CHECK_LEADER_TABLES_SQL = (
    "SELECT (SELECT count(*) FROM _cdc.leader), (SELECT count(*) FROM _cdc.fence), "
    "(SELECT count(DISTINCT lane) FROM _cdc.fence), (SELECT count(*) FROM _cdc.heartbeat)"
)

#: Every statement of this package that a bootstrap may need a copy of, by name.
NAMED_STATEMENTS = {
    "create_schema": CREATE_SCHEMA_SQL,
    "create_leader": CREATE_LEADER_SQL,
    "create_fence": CREATE_FENCE_SQL,
    "create_heartbeat": CREATE_HEARTBEAT_SQL,
    "create_connections": CREATE_CONNECTIONS_SQL,
    "check_leader_tables": CHECK_LEADER_TABLES_SQL,
    "fence": FENCE_SQL,
    "clock": clock_sql,
    "register_connection": registry.REGISTER_SQL,
    "list_earlier_connections": registry.LIST_EARLIER_SQL,
    "interrupt_earlier_connections": registry.INTERRUPT_EARLIER_SQL,
    "cleanup_connections": registry.CLEANUP_SQL,
}

_CREATE_NAMES = ("create_schema", "create_leader", "create_fence", "create_heartbeat", "create_connections")


def _first_difference(a, b):
    n = min(len(a), len(b))
    for i in range(n):
        if a[i] != b[i]:
            return i
    return n


def compare_statements(copy, required=_CREATE_NAMES):
    """Compare a second copy of this module's statements with the originals, character for character.

    ``copy`` is a sequence (compared in order with `LEADER_TABLE_STATEMENTS`) or a mapping from a name of
    `NAMED_STATEMENTS` to its text (every name in ``required`` must be there; any other name must be known).
    Returns the list of differences as text; an empty list means the copy is exact."""
    out = []
    if isinstance(copy, dict):
        for name in required:
            if name not in copy:
                out.append("missing statement %r" % name)
        for name, text in copy.items():
            ref = NAMED_STATEMENTS.get(name)
            if ref is None:
                out.append("unknown statement name %r" % name)
            elif text != ref:
                i = _first_difference(text, ref)
                out.append("%s differs at character %d: copy %r, original %r" % (name, i, text[i:i + 24], ref[i:i + 24]))
        return out
    copy = list(copy)
    if len(copy) != len(LEADER_TABLE_STATEMENTS):
        out.append("%d statements, expected %d" % (len(copy), len(LEADER_TABLE_STATEMENTS)))
    for n, (text, ref) in enumerate(zip(copy, LEADER_TABLE_STATEMENTS)):
        if text != ref:
            i = _first_difference(text, ref)
            out.append("statement %d differs at character %d: copy %r, original %r" % (n, i, text[i:i + 24], ref[i:i + 24]))
    return out


def check_leader_tables(conn, deadline=None):
    """Raise `SchemaError` unless the leader tables hold exactly their rows: one leader row, one fence row for each
    of the 18 lanes, one heartbeat row."""
    row = conn.fetchone(CHECK_LEADER_TABLES_SQL, None, deadline)
    if row is None or tuple(int(x) for x in row) != (1, len(LANES), len(LANES), 1):
        raise SchemaError("the leader tables do not hold their rows (leader, fence, lanes, heartbeat): %r" % (row,))
    lanes = sorted(r[0] for r in conn.fetchall("SELECT lane FROM _cdc.fence", None, deadline))
    if lanes != sorted(LANES):
        raise SchemaError("the fence rows are not the 18 lanes")


def ensure_leader_tables(conn, deadline=None):
    """Create the leader tables with all their rows if they are not there, then check them. Safe to call from any
    number of processes at once and any number of times: a starter that loses the creation to another one gets a
    catalog conflict, tries again and finds the table. Returns the number of attempts that hit a conflict."""
    d = as_deadline(deadline, 4 * conn.settings.statement_deadline_s)
    retries = 0
    for sql in LEADER_TABLE_STATEMENTS:
        while True:
            try:
                conn.execute(sql, None, d.within(conn.settings.statement_deadline_s))
                break
            except (Conflict, CommitUnknown, ConnectionLost):
                # all three are safe to repeat: the statement creates everything or nothing, and only if absent
                retries += 1
                d.sleep(0.02 + 0.08 * random.random(), "creating the leader tables")
    check_leader_tables(conn, d.within(conn.settings.statement_deadline_s))
    return retries


# ---------------------------------------------------------------------------------------------------------------
# Migrations
# ---------------------------------------------------------------------------------------------------------------
CREATE_SCHEMA_VERSION_SQL = (
    "CREATE TABLE IF NOT EXISTS _cdc.schema_version "
    "(package VARCHAR, version BIGINT, step VARCHAR, epoch BIGINT, updated_at TIMESTAMPTZ)"
)
_HAS_VERSION_TABLE_SQL = (
    "SELECT count(*) FROM information_schema.tables "
    "WHERE table_catalog = current_database() AND table_schema = '_cdc' AND table_name = 'schema_version'"
)
_READ_VERSIONS_SQL = "SELECT package, version, step FROM _cdc.schema_version"
_READ_VERSION_SQL = "SELECT version, step FROM _cdc.schema_version WHERE package = ?"
_ADD_PACKAGE_SQL = (
    "INSERT INTO _cdc.schema_version (package, version, step, epoch, updated_at) "
    "SELECT ?, 0, '', ?, " + clock_sql + " FROM _cdc.leader "
    "WHERE NOT EXISTS (SELECT 1 FROM _cdc.schema_version WHERE package = ?)"
)
_MOVE_VERSION_SQL = (
    "UPDATE _cdc.schema_version SET version = ?, step = ?, epoch = ?, updated_at = " + clock_sql + " "
    "WHERE package = ? AND version = ?"
)

#: After a COMMIT without an answer: how long until the outcome is read (a sent COMMIT can become durable up to
#: about 2 s after its client is gone, contract C4).
LATE_COMMIT_WAIT_S = 2.5

_NAME = re.compile(r"^[a-z][a-z0-9_]{0,62}$")
_OWN_FIRST = ("state", "alerts")           # this package's own lists run first: later steps may use their tables
_lists = {}


@dataclass(frozen=True)
class Step:
    """One migration step: a name and the statements that one transaction runs. A step is never changed after it
    was released; a change is a new step at the end of the list."""
    name: str
    statements: tuple

    def __post_init__(self):
        if not isinstance(self.name, str) or not _NAME.match(self.name):
            raise ValueError("a step name is a lower-case identifier, got %r" % (self.name,))
        stmts = (self.statements,) if isinstance(self.statements, str) else tuple(self.statements)
        if not stmts or not all(isinstance(s, str) and s.strip() for s in stmts):
            raise ValueError("step %s needs at least one statement, each as SQL text" % self.name)
        object.__setattr__(self, "statements", stmts)


def register_migrations(package, steps):
    """Register the ordered migration list of one package. Called once by the package, at import. Registering the
    same list again does nothing; a different list for the same package is an error."""
    if not isinstance(package, str) or not _NAME.match(package):
        raise ValueError("a package name is a lower-case identifier, got %r" % (package,))
    steps = tuple(steps)
    if not steps or not all(isinstance(s, Step) for s in steps):
        raise ValueError("a migration list is a non-empty sequence of Step")
    names = [s.name for s in steps]
    if len(set(names)) != len(names):
        raise ValueError("package %s: two steps with one name" % package)
    old = _lists.get(package)
    if old is not None and old != steps:
        raise ValueError("package %s registered two different migration lists" % package)
    _lists[package] = steps
    return steps


def registered_migrations():
    """The registered lists, in the order `migrate` runs them: this package's own first, the others by name."""
    order = [p for p in _OWN_FIRST if p in _lists] + sorted(p for p in _lists if p not in _OWN_FIRST)
    return [(p, _lists[p]) for p in order]


def _load_own():
    import cdc_v2.alerts    # noqa: F401 - registers its list
    import cdc_v2.state     # noqa: F401 - registers its list


def _pending(conn, lists, deadline):
    """Read-only look: the packages that have steps to run. Raises `SchemaError` on a mismatch."""
    if int(conn.fetchone(_HAS_VERSION_TABLE_SQL, None, deadline)[0]) == 0:
        return None
    stored = {r[0]: (int(r[1]), r[2]) for r in conn.fetchall(_READ_VERSIONS_SQL, None, deadline)}
    todo = []
    for package, steps in lists:
        version, name = stored.get(package, (0, ""))
        _check_version(package, steps, version, name)
        if version < len(steps):
            todo.append(package)
    return todo


def _check_version(package, steps, version, name):
    if version > len(steps):
        raise SchemaError("package %s: the database is at step %d, this code knows %d steps (the code is older "
                          "than the database)" % (package, version, len(steps)))
    if version > 0 and steps[version - 1].name != name:
        raise SchemaError("package %s: the database recorded step %d as %r, this code calls it %r"
                          % (package, version, name, steps[version - 1].name))


def migrate(lane, deadline=None):
    """Run every registered migration step that the database has not recorded yet. ``lane`` is the `control` lane
    of a run that leads (after its take-over). Each step runs in its own fenced transaction together with the
    change of its package's version row. Returns the steps applied by this call as ``(package, version, name)``.

    Raises `FenceLost` when another run took over, `SchemaError` when the recorded versions do not fit this code,
    and passes on the error of a failing step; in each case every earlier step stays applied and the failing one
    left nothing behind."""
    if lane.name != "control":
        raise LaneMisuse("migrations run in the control lane")
    _load_own()
    conn = lane.conn
    d = as_deadline(deadline, conn.settings.transaction_deadline_s)
    lists = registered_migrations()
    todo = _pending(conn, lists, d.within(conn.settings.statement_deadline_s))
    if todo is not None and not todo:
        return []
    applied, unknown = [], []
    if todo is None:
        _retrying(d, lambda: _create_version_table(lane, d))
    for package, steps in lists:
        if todo is not None and package not in todo:
            continue
        while _retrying(d, lambda: _one_step(lane, package, steps, d, applied, unknown)):
            pass
    return applied


def _retrying(deadline, fn):
    """Run one migration transaction; repeat it after a conflict or a lost connection, and after an unknown commit
    outcome once a late commit must have landed (more than 2 s, contract C4). Each repetition reads the version
    again inside its transaction, so a step never runs twice."""
    while True:
        try:
            return fn()
        except (Conflict, ConnectionLost):
            deadline.sleep(0.05 + 0.2 * random.random(), "a migration step")
        except CommitUnknown:
            deadline.sleep(LATE_COMMIT_WAIT_S, "a migration step")


def _create_version_table(lane, deadline):
    with lane.transaction(deadline) as tx:
        tx.execute(CREATE_SCHEMA_VERSION_SQL)


def _one_step(lane, package, steps, deadline, applied, unknown):
    """One transaction: the next missing step of ``package``. Returns True when there may be another one.

    ``unknown`` holds the step whose COMMIT had no answer (`CommitUnknown`, and only then). The next call reads the
    version: when the step is recorded, it is listed as applied. (Another leader of the same epoch may have been
    the one that applied it at that moment; the list cannot tell the two apart, the database is right either way.)"""
    entry = None
    try:
        with lane.transaction(deadline) as tx:
            row = tx.fetchone(_READ_VERSION_SQL, [package])
            if row is None:
                tx.execute_one(_ADD_PACKAGE_SQL, [package, lane.epoch, package])
                version, name = 0, ""
            else:
                version, name = int(row[0]), row[1]
            while unknown:
                old = unknown.pop()
                if old[0] == package and old[1] <= version and steps[old[1] - 1].name == name:
                    applied.append(old)
            try:
                _check_version(package, steps, version, name)
            except SchemaError as e:
                raise tx.fail(e)
            if version == len(steps):
                return False
            step = steps[version]
            for sql in step.statements:
                tx.execute(sql)
            tx.execute_one(_MOVE_VERSION_SQL, [version + 1, step.name, lane.epoch, package, version])
            entry = (package, version + 1, step.name)
    except CommitUnknown:
        if entry is not None:
            unknown.append(entry)
        raise
    applied.append(entry)
    log.info("migration step applied", extra={"cdc": {"package": package, "version": version + 1, "step": step.name,
                                                      "epoch": lane.epoch}})
    return version + 1 < len(steps)


def versions(conn_or_tx, deadline=None):
    """The recorded versions: ``{package: (version, step name)}``."""
    return {r[0]: (int(r[1]), r[2]) for r in conn_or_tx.fetchall(_READ_VERSIONS_SQL, None, deadline)}


__all__ = ["SchemaError", "Step", "register_migrations", "registered_migrations", "migrate", "versions",
           "ensure_leader_tables", "check_leader_tables", "compare_statements", "LEADER_TABLE_STATEMENTS",
           "NAMED_STATEMENTS"]
