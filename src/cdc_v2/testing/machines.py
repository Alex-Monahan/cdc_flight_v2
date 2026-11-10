"""SPEC 1.9 for tests: "Tests enumerate every legal transition and assert illegal ones raise."

`check_machine(lane, machine, key)` does the enumeration for one declared machine on one existing row:

- the row is put into every stored state in turn: each declared state, and each name in ``unknown`` (a value the
  declaration does not know, as a damaged row would hold);
- from each stored state, `state.transition` is called for **every ordered pair** of (declared states + unknown
  names), self-pairs included;
- exactly the calls whose pair is declared and whose ``from`` is the stored state must succeed: the row then holds
  ``to`` and `_cdc.transitions` has exactly one new row that says so;
- every other call must raise `IllegalTransition` (``declared`` and ``stored`` say why) and must change nothing:
  not the row, not `_cdc.transitions`.

The helper returns the counts, or raises `AssertionError` with the first case that behaved differently. A package
calls it in its own tests for each machine it declares.

Two example machines (`example_machines`) show the use and test the helper itself; they are declared only while
that block is open and live in the schema `_cdc_example`.
"""
from __future__ import annotations

import ast
import contextlib
import json
import os
import re

from cdc_v2 import state

_COUNT_LOG_SQL = "SELECT count(*) FROM _cdc.transitions WHERE machine = ?"
_LAST_LOG_SQL = ("SELECT key, from_state, to_state, cause, epoch FROM _cdc.transitions WHERE machine = ? "
                 "ORDER BY changed_at DESC, to_state")


def _force(tx, m, key, value):
    """Tests only: put a state into the row without the transition function."""
    where = " AND ".join("%s IS NOT DISTINCT FROM ?" % c for c in m.key_columns) or "TRUE"
    n = tx.execute("UPDATE %s SET %s = ? WHERE %s" % (m.table, m.state_column, where), [value, *key])
    assert n == 1, "machine %s: the row for key %r does not exist (create it before check_machine)" % (m.name, key)


def check_machine(lane, machine, key=(), unknown=("no_such_state",), restore=True):
    """Enumerate every transition of ``machine`` on the row ``key`` (see the module text). ``lane`` is a lane of
    the leading epoch on the destination that holds the machine's table and `_cdc.transitions`. With ``restore``
    the row is put back into the state it had. Returns ``{"stored_states", "attempts", "legal", "illegal"}``."""
    m = state.get(machine) if isinstance(machine, str) else machine
    key = tuple(key) if isinstance(key, (tuple, list)) else (key,)
    names = list(m.states) + [u for u in unknown if u not in m.states]
    counts = {"stored_states": 0, "attempts": 0, "legal": 0, "illegal": 0}
    key_text = json.dumps(list(key), default=str)
    with lane.transaction() as tx:
        before = state.read(tx, m, key)
    assert before is not None, "machine %s: no row for key %r" % (m.name, key)
    for stored in names:
        counts["stored_states"] += 1
        with lane.transaction() as tx:
            for a in names:
                for b in names:
                    _force(tx, m, key, stored)
                    logged = int(tx.fetchone(_COUNT_LOG_SQL, [m.name])[0])
                    case = "machine %s, stored %r, transition %r -> %r" % (m.name, stored, a, b)
                    legal = m.allows(a, b) and a == stored
                    counts["attempts"] += 1
                    try:
                        state.transition(tx, m, key, a, b, "enumeration")
                    except state.IllegalTransition as e:
                        assert not legal, "%s: refused, but it is declared" % case
                        assert e.declared == m.allows(a, b), "%s: wrong 'declared' on the error" % case
                        assert e.stored == (stored if m.allows(a, b) else None), "%s: wrong 'stored'" % case
                        assert state.read(tx, m, key) == stored, "%s: refused, but the row changed" % case
                        now = int(tx.fetchone(_COUNT_LOG_SQL, [m.name])[0])
                        assert now == logged, "%s: refused, but _cdc.transitions grew" % case
                        counts["illegal"] += 1
                    else:
                        assert legal, "%s: accepted, but it is not a legal transition" % case
                        assert state.read(tx, m, key) == b, "%s: accepted, but the row does not hold %r" % (case, b)
                        now = int(tx.fetchone(_COUNT_LOG_SQL, [m.name])[0])
                        assert now == logged + 1, "%s: accepted, but _cdc.transitions has %d new rows" % (
                            case, now - logged)
                        last = tx.fetchall(_LAST_LOG_SQL, [m.name])
                        assert (key_text, a, b, "enumeration", lane.epoch) in [tuple(r) for r in last], \
                            "%s: the row in _cdc.transitions is not what happened" % case
                        counts["legal"] += 1
            _force(tx, m, key, before if restore else stored)
    declared_legal = len(m.transitions)
    assert counts["legal"] == declared_legal, "machine %s: %d legal transitions seen, %d declared" % (
        m.name, counts["legal"], declared_legal)
    assert counts["attempts"] == len(names) ** 3
    return counts


EXAMPLE_DOOR = state.Machine(
    name="example_door",
    states=("closed", "open", "locked"),
    transitions=(("closed", "open"), ("open", "closed"), ("closed", "locked"), ("locked", "closed")),
    table="_cdc_example.door",
    key_columns=("door_id",),
    state_column="state",
    creators=("stream",),
    initial=("closed",),
    description="Example for tests: a door. One key column, no final state.",
)

EXAMPLE_JOB = state.Machine(
    name="example_job",
    states=("queued", "running", "done", "failed"),
    transitions=(("queued", "running"), ("running", "running"), ("running", "done"), ("running", "failed"),
                 ("failed", "queued")),
    table="_cdc_example.job",
    key_columns=("batch", "part"),
    state_column="status",
    creators=("stream", "control"),
    description="Example for tests: a job. Two key columns, a transition from a state to itself, a final state; "
                "rows created from two lanes, each together with a transition of its parent door.",
)

EXAMPLE_TABLES = (
    "CREATE SCHEMA IF NOT EXISTS _cdc_example",
    "CREATE TABLE IF NOT EXISTS _cdc_example.door (door_id VARCHAR, state VARCHAR, opened BIGINT)",
    "CREATE TABLE IF NOT EXISTS _cdc_example.job (batch BIGINT, part BIGINT, status VARCHAR)",
)


@contextlib.contextmanager
def example_machines(lane=None):
    """Declare the two example machines for the time of the block; with ``lane``, also create their tables in a
    transaction of that lane. Yields ``(door, job)``."""
    door, job = state.declare(EXAMPLE_DOOR), state.declare(EXAMPLE_JOB)
    try:
        if lane is not None:
            with lane.transaction() as tx:
                for sql in EXAMPLE_TABLES:
                    tx.execute(sql)
        yield door, job
    finally:
        state._forget(door.name)
        state._forget(job.name)


# ---------------------------------------------------------------------------------------------------------------
# Static search: "No other code writes a state column" (contract C5)
# ---------------------------------------------------------------------------------------------------------------
_UNKNOWN = " \x00 "                # stands for a part of a statement that is not a literal in the source
_UPDATE = re.compile(r"\bUPDATE\s+(?:OR\s+\w+\s+)?([\w.\"]+)(?:\s+(?:AS\s+)?\w+)??\s+SET\b(.*?)(?=\bWHERE\b|$)",
                     re.IGNORECASE | re.DOTALL)
_INSERT = re.compile(r"\b(?:INSERT\s+(?:OR\s+\w+\s+)?INTO|MERGE\s+INTO)\s+([\w.\"]+)\s*(?:\(([^)]*)\))?",
                     re.IGNORECASE | re.DOTALL)


def _fold(node):
    """The text of a string expression in the source, with `_UNKNOWN` for every part that is not a literal; None
    when the expression is not built from a string literal at all."""
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    if isinstance(node, ast.JoinedStr):
        return "".join(v.value if isinstance(v, ast.Constant) and isinstance(v.value, str) else _UNKNOWN
                       for v in node.values)
    if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
        left, right = _fold(node.left), _fold(node.right)
        if left is None and right is None:
            return None
        return (_UNKNOWN if left is None else left) + (_UNKNOWN if right is None else right)
    if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Mod):
        return _fold(node.left)
    return None


def _strings(tree):
    """(line, text) of every string expression of a module, docstrings and other bare strings left out."""
    out = []

    def visit(node):
        if isinstance(node, ast.Expr) and isinstance(node.value, ast.Constant):
            return                                      # a docstring: text, never a statement
        text = _fold(node) if isinstance(node, ast.expr) else None
        if text is not None:
            out.append((node.lineno, text))
            return
        for child in ast.iter_child_nodes(node):
            visit(child)

    visit(tree)
    return out


def _plain(name):
    return name.replace('"', "").lower()


def find_state_writes(source, machines=None):
    """Search Python source text for SQL that writes the state column of a declared machine: an `UPDATE` of the
    machine's table whose `SET` list names the state column, or an `INSERT`/`MERGE` into the table that names the
    column or gives no column list. Returns ``[(line, machine name, statement text)]``.

    Limits, by construction: a statement whose table or column name is not a literal in the source (built from a
    variable) cannot be seen, and a `SET` list is read up to its first `WHERE`."""
    machines = state.machines() if machines is None else list(machines)
    by_table = {}
    for m in machines:
        by_table.setdefault(_plain(m.table), []).append(m)
    found = []
    for line, text in _strings(ast.parse(source)):
        for match in _UPDATE.finditer(text):
            for m in by_table.get(_plain(match.group(1)), ()):
                if re.search(r"(?<![\w.])(?:\w+\.)?\"?%s\"?\s*=" % re.escape(m.state_column), match.group(2),
                             re.IGNORECASE):
                    found.append((line, m.name, " ".join(text.split())))
        for match in _INSERT.finditer(text):
            for m in by_table.get(_plain(match.group(1)), ()):
                cols = match.group(2)
                if cols is None or m.state_column.lower() in [_plain(c.strip()) for c in cols.split(",")]:
                    found.append((line, m.name, " ".join(text.split())))
    return found


def find_state_writes_under(root, exclude=(), machines=None):
    """`find_state_writes` for every ``*.py`` under ``root``, except in the directories named in ``exclude``
    (relative to ``root``, with ``/``). Returns ``[(relative path, line, machine name, statement text)]``."""
    found = []
    for folder, dirs, files in os.walk(root):
        rel = os.path.relpath(folder, root).replace(os.sep, "/")
        if any(rel == e or rel.startswith(e + "/") for e in exclude):
            dirs[:] = []
            continue
        dirs[:] = sorted(d for d in dirs if d != "__pycache__")
        for name in sorted(files):
            if name.endswith(".py"):
                path = os.path.join(folder, name)
                with open(path, encoding="utf-8") as f:
                    for line, machine, text in find_state_writes(f.read(), machines):
                        found.append(("%s/%s" % (rel, name) if rel != "." else name, line, machine, text))
    return found
