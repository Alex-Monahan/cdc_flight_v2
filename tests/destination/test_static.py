"""Tier T0: rules about the source text under `src/` that keep the destination layer the only door.

- Contract C5: "No other code writes a state column." Searched with `cdc_v2.testing.machines.find_state_writes`.
- Contract C4: "No other module issues BEGIN, COMMIT or the fence statement." At run time the layer refuses a
  transaction statement from a caller (`test_lane.py::test_misuse_is_refused`). Here: only the layer opens a DuckDB
  connection to the destination (another module may open a database in the memory of its process, and then holds
  no text that could reach the destination), and only the layer holds text that writes the fence table.
- Design 4.1: the rows of the leader tables are only ever updated; times come from MotherDuck's clock.

What a search of source text cannot see is said in `find_state_writes`: SQL whose table or column name is built
at run time.
"""
import ast
import os
import re

from cdc_v2 import state
from cdc_v2.testing import machines as tm

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
SRC = os.path.join(ROOT, "src", "cdc_v2")

#: Where a DuckDB connection may be opened: the layer, its test helpers, the Flight control client (it creates
#: and runs Flights; it holds no pipeline state) and the oracle (package O). The oracle judges the pipeline, so it
#: must not read through the pipeline's own layer; it is allowed because it only reads
#: (`test_the_oracle_only_reads`).
MAY_CONNECT = ("md/", "testing/", "flightctl.py", "oracle/")
#: A DuckDB database in the memory of the process. It is a tool for computing, not the destination: the plan names
#: "the transform connection" and "apply's planning database" (`docs/design/p3_plan.md`). Any module may open one,
#: written exactly as `duckdb.connect(":memory:", ...)`.
LOCAL = ":memory:"
#: SQL text with which a local database could reach the destination after all.
_REACHES_OUT = re.compile(r"^\s*attach\b|\bmd:|^\s*(?:install|load)\s+['\"]?motherduck", re.IGNORECASE)
#: Places of `MAY_CONNECT` whose SQL text must hold no statement that writes and no COMMIT.
READ_ONLY = ("oracle/",)
#: Where text that writes `_cdc.fence` may stand: the layer (fence statement, initialisation) and the take-over of
#: the test helper. The bootstrap `flights/cdc_v2/main.py` (package L) is outside `src/`; these tests do not read it.
MAY_WRITE_FENCE = ("md/", "testing/destination.py")
#: This package's own code: no local wall clock (C4, "No code of this package compares a local wall clock with a
#: stored time").
OWN = ("md/", "state/", "alerts.py")


def sources(root=SRC):
    for folder, dirs, files in os.walk(root):
        dirs[:] = sorted(d for d in dirs if d != "__pycache__")
        for name in sorted(files):
            if name.endswith(".py"):
                path = os.path.join(folder, name)
                with open(path, encoding="utf-8") as f:
                    yield os.path.relpath(path, root).replace(os.sep, "/"), f.read()


def allowed(rel, places):
    return any(rel == p or (p.endswith("/") and rel.startswith(p)) for p in places)


# ---------------------------------------------------------------------------------------------- state columns
def test_no_code_outside_the_state_module_writes_a_state_column():
    state.load_all()
    assert [m.name for m in state.machines()], "no machine is declared: the search would be empty"
    found = tm.find_state_writes_under(SRC, exclude=("state", "testing"))
    assert found == [], "a state column is written outside cdc_v2.state: %r" % (found,)


SEEDED = '''
"""A docstring: UPDATE _cdc.alerts SET state = 'x' is text, not a statement."""
A = "UPDATE _cdc.alerts SET state = 'resolved' WHERE alert_id = ?"
B = ("INSERT INTO _cdc.alerts (alert_id, kind, "
     "state) VALUES (?, ?, ?)")
C = "UPDATE _cdc.alerts SET count = count + 1, last_at = now() WHERE state = 'open'"
D = "SELECT state FROM _cdc.alerts WHERE alert_id = ?"
E = "INSERT INTO _cdc.alerts VALUES (?, ?)"
F = "UPDATE %s SET %s = ?" % (table, column)
G = "UPDATE _cdc.alerts AS a SET a.state = ?" + " WHERE 1 = 1"
H = f'UPDATE "_cdc"."alerts" SET kind = ?, "state" = {value}'
I = "INSERT INTO _cdc.alerts (alert_id, kind) VALUES (?, ?)"
J = "UPDATE _cdc.alerts SET delivery_state = 'sent' WHERE alert_id = ?"
K = "update _cdc.alerts set kind = ?,\\n  STATE = ? where alert_id = ?"
L = "MERGE INTO _cdc.alerts USING src ON (true) WHEN MATCHED THEN UPDATE SET state = 'x'"
M = "UPDATE _cdc.other SET state = 'x'"
N = "INSERT OR REPLACE INTO _cdc.alerts BY NAME SELECT 1 AS alert_id"
def f(tx):
    """UPDATE _cdc.alerts SET state = 'in a function docstring'"""
    tx.execute("UPDATE _cdc.alerts SET severity = ?, state = ? WHERE alert_id = ?", ["info", "open", 1])
'''


def test_the_search_finds_seeded_writes_and_nothing_else():
    """The finder itself: every way of writing the state column in literal SQL is found; reads, other columns,
    other tables, docstrings and text built at run time are not."""
    from cdc_v2 import alerts
    found = tm.find_state_writes(SEEDED, [alerts.ALERT])
    names = {}
    for line in SEEDED.splitlines():
        m = re.match(r"^([A-Z]) = ", line)
        if m:
            names[SEEDED.splitlines().index(line) + 1] = m.group(1)
    hit = sorted({names.get(line, "call") for line, machine, text in found})
    assert hit == ["A", "B", "E", "G", "H", "K", "L", "N", "call"], found
    assert all(machine == "alert" for line, machine, text in found)
    # one finding per statement, with the statement's text on one line
    assert len(found) == 9 and all("\n" not in text for line, machine, text in found)
    # with no machine declared on the table there is nothing to find
    assert tm.find_state_writes(SEEDED, [tm.EXAMPLE_DOOR]) == []
    # a machine with another state column: only its column counts
    assert [t for _, _, t in tm.find_state_writes('X = "UPDATE _cdc_example.job SET status = ?, state = 1"',
                                                  [tm.EXAMPLE_JOB])] == [
        "UPDATE _cdc_example.job SET status = ?, state = 1"]
    assert tm.find_state_writes('X = "UPDATE _cdc_example.job SET state = 1 WHERE status = ?"',
                                [tm.EXAMPLE_JOB]) == []


def test_the_search_walks_a_tree_and_honours_the_exclusions(tmp_path):
    from cdc_v2 import alerts
    (tmp_path / "a").mkdir()
    (tmp_path / "a" / "b").mkdir()
    (tmp_path / "skip").mkdir()
    bad = 'S = "UPDATE _cdc.alerts SET state = ?"\n'
    (tmp_path / "top.py").write_text(bad)
    (tmp_path / "a" / "b" / "deep.py").write_text("\n\n" + bad)
    (tmp_path / "a" / "note.txt").write_text(bad)
    (tmp_path / "skip" / "inner.py").write_text(bad)
    found = tm.find_state_writes_under(str(tmp_path), exclude=("skip",), machines=[alerts.ALERT])
    assert [(path, line, machine) for path, line, machine, text in found] == [
        ("top.py", 1, "alert"), ("a/b/deep.py", 3, "alert")]
    assert len(tm.find_state_writes_under(str(tmp_path), machines=[alerts.ALERT])) == 3


# ------------------------------------------------------------------------------------------ the layer is the door
def connect_calls(source, local=None):
    """Lines that open a DuckDB connection: `duckdb.connect(...)`, or `connect` imported from `duckdb`.

    A call is local when its first argument is the literal ``":memory:"``. ``local=True`` gives only the local
    calls, ``local=False`` everything else (an import of `connect` is never local), ``None`` all of them."""
    lines = []
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and node.func.attr == "connect" \
                and isinstance(node.func.value, ast.Name) and node.func.value.id in ("duckdb", "_duckdb"):
            is_local = bool(node.args) and isinstance(node.args[0], ast.Constant) and node.args[0].value == LOCAL
            if local is None or local == is_local:
                lines.append(node.lineno)
        if isinstance(node, ast.ImportFrom) and node.module in ("duckdb", "_duckdb") \
                and any(a.name in ("connect", "*") for a in node.names) and not local:
            lines.append(node.lineno)
    return sorted(lines)


#: Every way of writing a connection that is not the one local form; each line must be found.
SEEDED_CONNECTS = "\n".join((
    "import duckdb",
    "a = duckdb.connect('md:')",
    "b = duckdb.connect(name)",
    "c = duckdb.connect()",
    "d = duckdb.connect(database=':memory:')",
    "e = duckdb.connect('local.duckdb')",
    "f = duckdb.connect(':memory:' if test else name)",
    "from duckdb import connect",
    "g = duckdb.connect(':memory:', config={'threads': 1})",
    "h = duckdb.connect(':memory:')",
)) + "\n"


def test_only_the_layer_opens_a_duckdb_connection():
    """Outside the places of `MAY_CONNECT` a module opens no DuckDB connection but a database in the memory of its
    own process, written as the literal ``":memory:"``.

    Seeded by the merge of package Y (the transform): until then this test refused every `duckdb.connect` outside
    the layer, and `transform/convert.py` opens a local database to convert values. C4 is about the destination,
    and the plan itself names the transform's connection and apply's planning database. What the rule protects is
    kept: `test_no_code_outside_the_layer_holds_text_that_reaches_the_destination`."""
    assert connect_calls("import duckdb\nc = duckdb.connect('md:')\n") == [2]
    assert connect_calls("from duckdb import connect\n") == [1]
    assert connect_calls("from cdc_v2 import md\nc = md.connect('stream')\n") == []
    assert connect_calls(SEEDED_CONNECTS) == [2, 3, 4, 5, 6, 7, 8, 9, 10]
    assert connect_calls(SEEDED_CONNECTS, local=False) == [2, 3, 4, 5, 6, 7, 8]
    assert connect_calls(SEEDED_CONNECTS, local=True) == [9, 10]
    found = [(rel, line) for rel, text in sources() for line in connect_calls(text, local=False)
             if not allowed(rel, MAY_CONNECT)]
    assert found == [], ("a DuckDB connection that is not a database in memory is opened outside cdc_v2.md (use "
                         "md.connect; contract C4): %r" % (found,))
    assert [rel for rel, text in sources() if rel.startswith("md/") and connect_calls(text)] == ["md/connection.py"]


def test_no_code_outside_the_layer_holds_text_that_reaches_the_destination():
    """A database in memory can attach another database. So outside the places of `MAY_CONNECT` no SQL text
    attaches a database, names `md:` or loads the MotherDuck extension. Documentation is not searched; text that
    is built at run time is not seen (`find_state_writes`)."""
    for s in ("ATTACH 'md:'", "attach 'md:x' AS dest", " ATTACH DATABASE 'x.duckdb'", "LOAD motherduck",
              "INSTALL 'motherduck'", "SELECT * FROM read_parquet('md:x')", "md:"):
        assert _REACHES_OUT.search(s), s
    for s in ("SELECT 1", "cannot attach the listener", "SET TimeZone = 'UTC'", "cmd: %s", "LOAD spatial",
              "the attachment", "CREATE TABLE t (cmd VARCHAR)"):
        assert not _REACHES_OUT.search(s), s
    found = [(rel, line, s[:60]) for rel, text in sources() if not allowed(rel, MAY_CONNECT)
             for line, s in statements(text) if _REACHES_OUT.search(s)]
    assert found == [], "text that can reach the destination outside cdc_v2.md (contract C4): %r" % (found,)
    # the search is not empty by accident: the layer itself holds such text
    assert any(_REACHES_OUT.search(s) for rel, text in sources() if rel.startswith("md/")
               for line, s in statements(text))


_FENCE_WRITE = re.compile(r"\b(?:UPDATE|INSERT\s+(?:OR\s+\w+\s+)?INTO|DELETE\s+FROM|MERGE\s+INTO)\s+\"?_cdc\"?\s*\.\s*"
                          r"\"?fence\"?(?![\w\"])", re.IGNORECASE)
_ROW_REMOVAL = re.compile(r"\b(?:DELETE\s+FROM|INSERT\s+(?:OR\s+\w+\s+)?INTO|MERGE\s+INTO|TRUNCATE(?:\s+TABLE)?|"
                          r"DROP\s+TABLE(?:\s+IF\s+EXISTS)?|CREATE\s+OR\s+REPLACE\s+TABLE)\s+\"?_cdc\"?\s*\.\s*"
                          r"\"?(?:leader|fence|heartbeat)\"?(?![\w\"])", re.IGNORECASE)


def statements(source):
    return tm._strings(ast.parse(source))


_WRITES = re.compile(r"\b(?:INSERT\s+(?:OR\s+\w+\s+)?INTO|UPDATE\s+[\w\".{}%]+\s+SET|DELETE\s+FROM|MERGE\s+INTO|"
                     r"TRUNCATE|CREATE\s+(?:OR\s+REPLACE\s+)?(?:TEMP\w*\s+)?(?:TABLE|SCHEMA|DATABASE|VIEW|MACRO)|"
                     r"DROP\s+(?:TABLE|SCHEMA|DATABASE|VIEW)|ALTER\s+(?:TABLE|DATABASE)|COPY\s|ATTACH\s|COMMIT)\b",
                     re.IGNORECASE)


def test_the_oracle_only_reads():
    """Seeded by the merge of package O: its `oracle/mdexec.py` opens a DuckDB connection of its own, and the
    comparison opens a read transaction on it (`BEGIN`, later `ROLLBACK`). Contract C4 says that no other module
    issues `BEGIN` or `COMMIT`; what that sentence protects is that nothing is committed past the fence. So the
    oracle may connect as long as no SQL text of it writes or commits, in either database."""
    for s in ("INSERT INTO t VALUES (1)", "update t set a = 1", "COMMIT", "CREATE TEMP TABLE x AS SELECT 1",
              "DROP TABLE t", "DELETE FROM t", "create or replace view v as select 1", "ALTER TABLE t ADD COLUMN c INT"):
        assert _WRITES.search(s), s
    for s in ("SELECT count(*) FROM t", "BEGIN TRANSACTION", "ROLLBACK", "the last update of the row",
              "BEGIN ISOLATION LEVEL REPEATABLE READ, READ ONLY"):
        assert not _WRITES.search(s), s
    assert all(allowed(p, MAY_CONNECT) for p in READ_ONLY)
    found = [(rel, line, s[:60]) for rel, text in sources() if allowed(rel, READ_ONLY)
             for line, s in statements(text) if _WRITES.search(s)]
    assert found == [], "a statement that writes or commits in code that may only read: %r" % (found,)


def test_only_the_layer_holds_the_fence_statement():
    assert _FENCE_WRITE.search("UPDATE _cdc.fence SET n = n + 1, epoch = ? WHERE lane = ? AND epoch = ?")
    assert _FENCE_WRITE.search('update "_cdc"."fence" set epoch = 2') and not _FENCE_WRITE.search(
        "SELECT n FROM _cdc.fence") and not _FENCE_WRITE.search("UPDATE _cdc.fences SET n = 1")
    found = [(rel, line) for rel, text in sources() for line, s in statements(text)
             if _FENCE_WRITE.search(s) and not allowed(rel, MAY_WRITE_FENCE)]
    assert found == [], "text that writes _cdc.fence outside the destination layer (contract C4): %r" % (found,)
    # the fence statement itself stands in exactly one place (the registration writes `n` too, without `epoch`)
    holders = [rel for rel, text in sources() for line, s in statements(text)
               if re.search(r"UPDATE\s+_cdc\.fence\s+SET\s+n\s*=\s*n\s*\+\s*1\s*,\s*epoch", s)]
    assert holders == ["md/lane.py"]


def test_no_text_deletes_or_inserts_leader_rows():
    """Design 4.1: the rows of `_cdc.leader`, `_cdc.fence` and `_cdc.heartbeat` exist after initialisation and are
    only ever updated. (At run time the layer refuses such a statement from a caller as well.)"""
    assert _ROW_REMOVAL.search("DELETE FROM _cdc.leader") and _ROW_REMOVAL.search("insert into _cdc.heartbeat")
    assert _ROW_REMOVAL.search("CREATE OR REPLACE TABLE _cdc.fence AS SELECT 1")
    assert not _ROW_REMOVAL.search("UPDATE _cdc.leader SET epoch = ?")
    assert not _ROW_REMOVAL.search("INSERT INTO _cdc.connections SELECT 1")
    assert not _ROW_REMOVAL.search("CREATE TABLE IF NOT EXISTS _cdc.fence AS SELECT 1")
    found = [(rel, line) for rel, text in sources() for line, s in statements(text) if _ROW_REMOVAL.search(s)]
    assert found == [], "leader rows are deleted, inserted or re-created: %r" % (found,)


def test_this_package_reads_no_local_wall_clock():
    """Stored times are written and compared by MotherDuck's clock in SQL (`md.clock_sql`). This package's code
    uses the local monotonic clock for durations only, never `time.time()` or `datetime`."""
    wall = re.compile(r"\btime\.time(?:_ns)?\s*\(|\bdatetime\b|\butcnow\b|\bgmtime\b|\blocaltime\b|\bstrftime\b")
    assert wall.search("x = time.time()") and wall.search("import datetime") and not wall.search("time.monotonic()")
    found = []
    for rel, text in sources():
        if allowed(rel, OWN):
            code = ast.parse(text)
            for node in ast.walk(code):                 # names and attributes only: not comments or documentation
                if isinstance(node, (ast.Import, ast.ImportFrom)):
                    mods = [a.name for a in node.names] + [getattr(node, "module", None) or ""]
                    if any(m.split(".")[0] == "datetime" for m in mods):
                        found.append((rel, node.lineno, "datetime"))
                if isinstance(node, ast.Attribute) and isinstance(node.value, ast.Name) and node.value.id == "time" \
                        and node.attr in ("time", "time_ns", "gmtime", "localtime", "strftime", "ctime"):
                    found.append((rel, node.lineno, "time." + node.attr))
    assert found == [], "a local wall clock is read in the destination layer: %r" % (found,)
    # and the clock expression is what the design names (4.1), one constant used everywhere
    from cdc_v2 import md
    assert md.clock_sql == "uuid_extract_timestamp(uuidv7())"
    users = [rel for rel, text in sources() if allowed(rel, OWN) and re.search(r"\bnow\s*\(\s*\)|current_timestamp|"
                                                                               r"get_current_time", text, re.I)]
    assert users == [], "another clock expression than md.clock_sql is used in: %r" % (users,)


# -------------------------------------------------------------------------- from the review of apply (C4)
_SCRATCH = re.compile(r"_cdc_apply|\bdrop\s+schema\b|\bcascade\b", re.I)
#: Methods of the DuckDB client (1.5.6) that return a lazy `RecordBatchReader`.
_LAZY_READERS = ("arrow", "fetch_record_batch", "fetch_arrow_reader", "to_arrow_reader")


def test_nothing_of_this_package_drops_a_schema_or_names_a_scratch_schema_of_apply():
    """Scratch schemas `_cdc_apply_<random>` belong to package A and live inside one lane transaction. Nothing of
    this package cleans them up, and nothing of it drops a schema or uses CASCADE: `DROP SCHEMA ... CASCADE` of
    tables made in the same transaction fails at COMMIT (`test_from_apply.py`)."""
    assert _SCRATCH.search("DROP  SCHEMA x") and _SCRATCH.search("drop table t cascade")
    assert _SCRATCH.search("SELECT 1 FROM _cdc_apply_ab12.t") and not _SCRATCH.search("DROP TABLE _cdc.cascaded")
    found = [(rel, line) for rel, text in sources() if allowed(rel, OWN + ("testing/destination.py",))
             for line, s in statements(text) if _SCRATCH.search(s)]
    assert found == [], "a schema is dropped, or a scratch schema of apply is named: %r" % (found,)


def test_the_layer_never_hands_out_the_lazy_reader_of_the_client():
    """In DuckDB 1.5.6 `.arrow()` returns a reader that is read later, and a reader that a connection produced
    cannot be read by a statement of the same connection. The layer reads every Arrow result completely
    (`to_arrow_table`), in one place."""
    calls = []
    for rel, text in sources():
        if allowed(rel, OWN):
            for node in ast.walk(ast.parse(text)):
                if isinstance(node, ast.Attribute) and (node.attr in _LAZY_READERS or "arrow_table" in node.attr):
                    calls.append((rel, node.attr))
    assert calls == [("md/connection.py", "to_arrow_table")], calls
