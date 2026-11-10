"""State machines (SPEC 1.9, contract C5): declared once, changed only here, logged on every change.

A machine is a set of states, a set of allowed transitions, and the place where its state is stored: one column of
one table, one row per key. A package declares its machine at import:

    TABLE_LIFECYCLE = state.declare(state.Machine(
        name="table", states=("new", "loading", "streaming"),
        transitions=(("new", "loading"), ("loading", "streaming")),
        table="_cdc.tables", key_columns=("table_id",), state_column="state"))

and changes the state only through `transition` (or `create` for a new row), inside the lane transaction that also
changes the data the state governs:

    state.transition(tx, TABLE_LIFECYCLE, table_id, "loading", "streaming", cause="backfill complete")

`transition` refuses a pair that is not declared before it touches the database, changes the row only if it still
holds ``from_state`` (one conditional UPDATE, exactly one row), and appends a row to `_cdc.transitions`. A static
test (`tests/destination/test_static.py`) searches `src/` for any other statement that writes a declared state
column. `docs/state_machines.md` is generated from the declarations (``python -m cdc_v2.state``).

One exception is declared by contract C5: the leader machine. The bootstrap writes `_cdc.leader.state` with its own
copy of the take-over statements, because it runs before this package can be imported.
"""
from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass

from cdc_v2.md import schema as _schema
from cdc_v2.md.connection import clock_sql

log = logging.getLogger("cdc_v2.state")

_NAME = re.compile(r"^[a-z][a-z0-9_]{0,62}$")
_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,62}$")
_TABLE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,62}(\.[A-Za-z_][A-Za-z0-9_]{0,62})?$")
_MAX_CAUSE = 200


class _Now:
    def __repr__(self):
        return "state.NOW"


#: As a value in ``create(values=...)``: MotherDuck's clock at the moment the statement runs.
NOW = _Now()


class StateError(Exception):
    """A state machine is used wrongly or its table is not what the declaration says (a key with two rows)."""


class IllegalTransition(StateError):
    """The transition was refused and nothing was changed.

    ``stored`` is the state the row holds (None when there is no row). ``declared`` is False when the pair
    (``from_state``, ``to_state``) is not a transition of the machine at all; the database was not touched then
    and ``stored`` is None."""

    def __init__(self, machine, key, from_state, to_state, stored=None, declared=True, what=None):
        self.machine, self.key, self.from_state, self.to_state = machine, key, from_state, to_state
        self.stored, self.declared = stored, declared
        if what is None:
            if not declared:
                what = "%r -> %r is not a transition of this machine" % (from_state, to_state)
            elif stored is None:
                what = "%r -> %r refused: there is no row" % (from_state, to_state)
            else:
                what = "%r -> %r refused: the stored state is %r" % (from_state, to_state, stored)
        super().__init__("machine %s, key %s: %s" % (machine, json.dumps(list(key), default=str), what))


@dataclass(frozen=True)
class Machine:
    """The declaration of one state machine.

    ``states``: every state, as short lower-case names. ``transitions``: the allowed pairs ``(from, to)``; a pair
    of one state with itself is allowed when it is listed. ``table``: where the state is stored (``schema.table``).
    ``key_columns``: the columns that identify one row; empty for a table with one row. ``state_column``: the
    column that holds the state. ``initial``: the states a new row may be created in (empty: any state).
    ``description``: one line for the generated document."""
    name: str
    states: tuple
    transitions: tuple
    table: str
    key_columns: tuple
    state_column: str
    initial: tuple = ()
    description: str = ""

    def __post_init__(self):
        if not isinstance(self.name, str) or not _NAME.match(self.name):
            raise ValueError("a machine name is a lower-case identifier, got %r" % (self.name,))
        states = tuple(self.states)
        if not states or len(set(states)) != len(states) or not all(isinstance(s, str) and _NAME.match(s)
                                                                    for s in states):
            raise ValueError("machine %s: states are distinct lower-case names" % self.name)
        transitions = tuple((a, b) for a, b in self.transitions)
        if len(set(transitions)) != len(transitions):
            raise ValueError("machine %s: a transition is listed twice" % self.name)
        for a, b in transitions:
            if a not in states or b not in states:
                raise ValueError("machine %s: transition %r -> %r names an undeclared state" % (self.name, a, b))
        if not isinstance(self.table, str) or not _TABLE.match(self.table):
            raise ValueError("machine %s: table is 'schema.table' or 'table'" % self.name)
        keys = (self.key_columns,) if isinstance(self.key_columns, str) else tuple(self.key_columns)
        if len(set(keys)) != len(keys) or not all(isinstance(k, str) and _IDENT.match(k) for k in keys):
            raise ValueError("machine %s: key columns are distinct column names" % self.name)
        if not isinstance(self.state_column, str) or not _IDENT.match(self.state_column) \
                or self.state_column in keys:
            raise ValueError("machine %s: the state column is a column name that is not a key column" % self.name)
        initial = tuple(self.initial)
        if not all(s in states for s in initial):
            raise ValueError("machine %s: an initial state is not declared" % self.name)
        if not isinstance(self.description, str) or "\n" in self.description:
            raise ValueError("machine %s: the description is one line" % self.name)
        for field, value in (("states", states), ("transitions", transitions), ("key_columns", keys),
                             ("initial", initial)):
            object.__setattr__(self, field, value)

    def allows(self, from_state, to_state):
        return (from_state, to_state) in self.transitions


_machines = {}


def declare(machine):
    """Register a machine. Declaring the same machine again does nothing; a different machine under the same name,
    or a second machine on the same state column, is an error. Returns the machine."""
    if not isinstance(machine, Machine):
        raise TypeError("declare() takes a state.Machine")
    old = _machines.get(machine.name)
    if old is not None:
        if old != machine:
            raise ValueError("machine %s is declared twice with different content" % machine.name)
        return old
    for other in _machines.values():
        if (other.table.lower(), other.state_column.lower()) == (machine.table.lower(), machine.state_column.lower()):
            raise ValueError("machines %s and %s share one state column" % (other.name, machine.name))
    _machines[machine.name] = machine
    return machine


def machines():
    """The declared machines, by name."""
    return [_machines[n] for n in sorted(_machines)]


def get(name):
    try:
        return _machines[name]
    except KeyError:
        raise StateError("no machine named %r is declared" % (name,)) from None


def _forget(name):
    """Tests only: remove a declaration."""
    _machines.pop(name, None)


def _resolve(machine):
    if isinstance(machine, str):
        return get(machine)
    if not isinstance(machine, Machine):
        raise TypeError("a machine is a state.Machine or the name of a declared one")
    if _machines.get(machine.name) != machine:
        raise StateError("machine %s is not declared (state.declare)" % machine.name)
    return machine


def _key(m, key):
    if isinstance(key, dict):
        if set(key) != set(m.key_columns):
            raise StateError("machine %s: the key columns are %s" % (m.name, ", ".join(m.key_columns) or "(none)"))
        key = tuple(key[c] for c in m.key_columns)
    elif isinstance(key, (tuple, list)):
        key = tuple(key)
    elif key is None:
        key = ()
    else:
        key = (key,)
    if len(key) != len(m.key_columns):
        raise StateError("machine %s: the key has %d values, the declaration %d columns"
                         % (m.name, len(key), len(m.key_columns)))
    return key


def _cause(cause):
    if not isinstance(cause, str) or not cause.strip() or len(cause) > _MAX_CAUSE or not cause.isprintable():
        raise StateError("a cause is a short line of text (at most %d characters)" % _MAX_CAUSE)
    return cause


def _where(m):
    return " AND ".join("%s IS NOT DISTINCT FROM ?" % c for c in m.key_columns) or "TRUE"


_LOG_SQL = ("INSERT INTO _cdc.transitions (machine, key, from_state, to_state, cause, epoch, changed_at) "
            "VALUES (?, ?, ?, ?, ?, ?, " + clock_sql + ")")


def _record(tx, m, key, from_state, to_state, cause):
    key_text = json.dumps(list(key), default=str)
    tx.execute(_LOG_SQL, [m.name, key_text, from_state, to_state, cause, getattr(tx, "epoch", None)])
    # The line is written inside the caller's open transaction: the change holds only if that transaction
    # commits. `_cdc.transitions` is the record; the field `committed` says what the line cannot know.
    log.info("state transition", extra={"cdc": {"machine": m.name, "key": key_text, "from": from_state,
                                                "to": to_state, "cause": cause,
                                                "epoch": getattr(tx, "epoch", None), "committed": False}})


def _stored(reader, m, key):
    rows = reader.fetchall("SELECT %s FROM %s WHERE %s" % (m.state_column, m.table, _where(m)), list(key))
    if len(rows) > 1:
        raise StateError("machine %s: %d rows for one key in %s" % (m.name, len(rows), m.table))
    return rows[0][0] if rows else None


def read(conn_or_tx, machine, key=()):
    """The stored state of one key, or None when there is no row."""
    m = _resolve(machine)
    return _stored(conn_or_tx, m, _key(m, key))


def transition(tx, machine, key, from_state, to_state, cause):
    """Move one key from ``from_state`` to ``to_state`` inside the open lane transaction ``tx``.

    Raises `IllegalTransition` and changes nothing when the pair is not declared (the database is not touched), or
    when the row does not hold ``from_state`` (``stored`` then says what it holds; the transaction stays usable).
    On success the change and its row in `_cdc.transitions` are part of ``tx``."""
    m = _resolve(machine)
    key = _key(m, key)
    cause = _cause(cause)
    if not m.allows(from_state, to_state):
        raise IllegalTransition(m.name, key, from_state, to_state, declared=False)
    n = tx.execute("UPDATE %s SET %s = ? WHERE %s AND %s = ?" % (m.table, m.state_column, _where(m), m.state_column),
                   [to_state, *key, from_state])
    if n == 1:
        _record(tx, m, key, from_state, to_state, cause)
        return
    if n == 0:
        raise IllegalTransition(m.name, key, from_state, to_state, stored=_stored(tx, m, key))
    err = StateError("machine %s: the transition changed %r rows of %s; the key is not unique" % (m.name, n, m.table))
    fail = getattr(tx, "fail", None)
    raise fail(err) if fail is not None else err


def create(tx, machine, key, state, cause, values=None):
    """Add the row of a new key in ``state``. Fails with `IllegalTransition` when the key has a row already
    (``stored`` says in which state) or when ``state`` is not one a new row may have. ``values`` gives the other
    columns of the new row (column name -> value; the value `NOW` stands for MotherDuck's clock).

    The check for an existing row sees what this transaction sees. Two transactions that create one key at the
    same time meet only through the fence row of their lane: on one lane the second one gets `Conflict`, on
    two lanes both commit and the key has two rows (`read` and `transition` then raise `StateError`). So the
    rows of one machine are created from one lane only (`docs/design/destination.md`, contract notes)."""
    m = _resolve(machine)
    key = _key(m, key)
    cause = _cause(cause)
    if state not in m.states or (m.initial and state not in m.initial):
        raise IllegalTransition(m.name, key, None, state, declared=False,
                                what="a new row cannot start in state %r" % (state,))
    values = dict(values or {})
    for col in values:
        if not isinstance(col, str) or not _IDENT.match(col) or col in m.key_columns or col == m.state_column:
            raise StateError("machine %s: %r is not a plain column name beside key and state" % (m.name, col))
    columns = list(m.key_columns) + [m.state_column] + list(values)
    exprs, params = ["?"] * (len(key) + 1), list(key) + [state]
    for value in values.values():
        if value is NOW:
            exprs.append(clock_sql)
        else:
            exprs.append("?")
            params.append(value)
    params += list(key)
    n = tx.execute("INSERT INTO %s (%s) SELECT %s WHERE NOT EXISTS (SELECT 1 FROM %s WHERE %s)"
                   % (m.table, ", ".join(columns), ", ".join(exprs), m.table, _where(m)), params)
    if n != 1:
        raise IllegalTransition(m.name, key, None, state, stored=_stored(tx, m, key),
                                what="the key has a row already")
    _record(tx, m, key, None, state, cause)


# ---------------------------------------------------------------------------------------------------------------
# The log table and the generated document
# ---------------------------------------------------------------------------------------------------------------
MIGRATIONS = _schema.register_migrations("state", (
    _schema.Step("transitions_table", (
        "CREATE TABLE _cdc.transitions (machine VARCHAR, key VARCHAR, from_state VARCHAR, to_state VARCHAR, "
        "cause VARCHAR, epoch BIGINT, changed_at TIMESTAMPTZ)",
    )),
))

DOC_HEADER = """# State machines

This file is generated from the declarations in the code. Do not edit it; run
`uv run python -m cdc_v2.state > docs/state_machines.md` after a machine was added or changed. A test
(`tests/destination/test_state.py`) fails when the file and the declarations differ.

## What a machine is

A state machine is a set of named states and a list of allowed changes from one state to another (transitions). Its
current state is stored in one column of one table in the destination, one row per key, in the same database and
the same transaction as the data the state governs (SPEC 1.9). A package declares its machine with
`cdc_v2.state.declare`.

A state is changed in one way only: `cdc_v2.state.transition(tx, machine, key, from_state, to_state, cause)`, and
`cdc_v2.state.create` for a new row. The function refuses a change that is not in the list below. It changes the
row only if the row still holds `from_state`. It writes one row to `_cdc.transitions` for every change: machine,
key, from, to, cause, epoch and MotherDuck's time. A static test fails when any other code under `src/` writes a
state column.

## How to read the blocks

Each block below is one machine.

- "Stored in" names the table and the column that hold the state.
- "Key" names the columns that identify one row.
- "States" lists every state the column can hold.
- "A new row starts in" lists the states `create` accepts.
- The table lists every allowed transition. A pair that is not in the table is refused. A state with no row in the
  "From" column is final.
"""


def load_all():
    """Import every module of `cdc_v2` (test support aside), so that every machine is declared."""
    import importlib
    import pkgutil

    import cdc_v2
    for info in pkgutil.walk_packages(cdc_v2.__path__, "cdc_v2."):
        name = info.name
        if name == "cdc_v2.testing" or name.startswith("cdc_v2.testing.") or name.endswith(".__main__"):
            continue
        importlib.import_module(name)


def render_machine(m):
    """The block of one machine in `docs/state_machines.md`."""
    lines = ["### %s" % m.name, ""]
    if m.description:
        lines += [m.description, ""]
    lines += [
        "- Stored in: `%s.%s`" % (m.table, m.state_column),
        "- Key: %s" % (", ".join("`%s`" % c for c in m.key_columns) or "none (the table has one row)"),
        "- States: %s" % ", ".join("`%s`" % s for s in m.states),
        "- A new row starts in: %s" % (", ".join("`%s`" % s for s in m.initial) or "any state"),
        "",
        "| From | To |",
        "|---|---|",
    ]
    lines += ["| `%s` | `%s` |" % (a, b) for a, b in m.transitions] or ["| (no transition) | |"]
    return "\n".join(lines) + "\n"


def render_document(which=None):
    """The whole of `docs/state_machines.md` for the given machines (default: every declared one)."""
    ms = machines() if which is None else list(which)
    return DOC_HEADER + "\n## The machines\n" + "".join("\n" + render_machine(m) for m in ms)


__all__ = ["Machine", "declare", "machines", "get", "transition", "create", "read", "IllegalTransition",
           "StateError", "NOW", "render_machine", "render_document", "load_all"]
