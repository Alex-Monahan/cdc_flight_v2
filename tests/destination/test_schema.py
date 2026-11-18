"""Tier T0 (local DuckDB): the leader tables (statements, initialisation by several starters at once, the comparison
helper for the bootstrap's copy) and the migration mechanism (`_cdc.schema_version`, ordered steps per package)."""
import ast
import inspect
import re
import threading

import duckdb
import pytest

from cdc_v2 import md
from cdc_v2.md import schema
from cdc_v2.testing import destination as td

LEADER_COLUMNS = {
    "leader": [("epoch", "BIGINT"), ("flight_id", "VARCHAR"), ("run_number", "BIGINT"), ("run_id", "VARCHAR"),
               ("state", "VARCHAR"), ("taken_at", "TIMESTAMP WITH TIME ZONE")],
    "fence": [("lane", "VARCHAR"), ("epoch", "BIGINT"), ("n", "BIGINT")],
    "heartbeat": [("epoch", "BIGINT"), ("beat_at", "TIMESTAMP WITH TIME ZONE"),
                  ("progress_at", "TIMESTAMP WITH TIME ZONE"), ("phase", "VARCHAR")],
    "connections": [("epoch", "BIGINT"), ("lane", "VARCHAR"), ("connection_id", "UUID"),
                    ("registered_at", "TIMESTAMP WITH TIME ZONE")],
}


def columns(conn, table):
    return [tuple(r) for r in conn.fetchall(
        "SELECT column_name, data_type FROM information_schema.columns "
        "WHERE table_catalog = current_database() AND table_schema = '_cdc' AND table_name = ? "
        "ORDER BY ordinal_position", [table])]


def tables(conn, schema_name="_cdc"):
    return sorted(r[0] for r in conn.fetchall(
        "SELECT table_name FROM information_schema.tables "
        "WHERE table_catalog = current_database() AND table_schema = ?", [schema_name]))


def everything(conn):
    """Every row of the leader tables (times as microseconds: the client needs no time-zone library then)."""
    return (conn.fetchall("SELECT * REPLACE (epoch_us(taken_at) AS taken_at) FROM _cdc.leader"),
            sorted(conn.fetchall("SELECT * FROM _cdc.fence")),
            conn.fetchall("SELECT * REPLACE (epoch_us(beat_at) AS beat_at, epoch_us(progress_at) AS progress_at) "
                          "FROM _cdc.heartbeat"),
            conn.fetchall("SELECT * REPLACE (epoch_us(registered_at) AS registered_at) FROM _cdc.connections"))


@pytest.fixture
def lists():
    """The registered migration lists, put back after the test (tests register packages of their own)."""
    schema._load_own()
    saved = dict(schema._lists)
    try:
        yield schema._lists
    finally:
        schema._lists.clear()
        schema._lists.update(saved)


def log_steps(package, n, fail_at=None):
    """A list of ``n`` steps for a test package: each adds one row to a table that the first step creates."""
    steps = []
    for i in range(1, n + 1):
        stmts = ["INSERT INTO %s_log VALUES ('s%d')" % (package, i)]
        if i == 1:
            stmts.insert(0, "CREATE TABLE %s_log (step VARCHAR)" % package)
        if i == fail_at:
            stmts += ["CREATE TABLE %s_half (x INTEGER)" % package, "SELECT * FROM %s_no_such_table" % package]
        steps.append(md.Step("s%d" % i, stmts))
    return tuple(steps)


def logged(conn, package):
    return sorted(r[0] for r in conn.fetchall("SELECT step FROM %s_log" % package))


# ------------------------------------------------------------------------------------------------ leader tables
def test_the_leader_table_statements_are_constant_text():
    """Nothing computed at run time: each statement is a string literal in the module's source."""
    tree = ast.parse(inspect.getsource(schema))
    literal = {}
    for node in tree.body:
        if isinstance(node, ast.Assign) and len(node.targets) == 1 and isinstance(node.targets[0], ast.Name):
            literal[node.targets[0].id] = isinstance(node.value, ast.Constant) and isinstance(node.value.value, str)
    names = ["CREATE_SCHEMA_SQL", "CREATE_LEADER_SQL", "CREATE_FENCE_SQL", "CREATE_HEARTBEAT_SQL",
             "CREATE_CONNECTIONS_SQL", "CHECK_LEADER_TABLES_SQL"]
    assert [n for n in names if not literal.get(n)] == []
    assert schema.LEADER_TABLE_STATEMENTS == tuple(getattr(schema, n) for n in names[:5])
    assert all(isinstance(s, str) and s == s.strip() and "\n" not in s for s in schema.NAMED_STATEMENTS.values())


def test_the_fence_statement_names_exactly_the_lanes():
    assert re.findall(r"\('([a-z0-9.]+)'\)", schema.CREATE_FENCE_SQL) == list(md.LANES)
    assert len(md.LANES) == 18 and md.LANES[:3] == ("stream", "control", "backfill.0")
    assert md.LANES[-1] == "backfill.15"


def test_initialisation_creates_every_row_with_the_frozen_shape(dest):
    c = dest.connect("look")
    for table, cols in LEADER_COLUMNS.items():
        assert columns(c, table) == cols, table
    assert c.fetchall("SELECT epoch, flight_id, run_number, run_id, state, taken_at FROM _cdc.leader") == [
        (0, None, None, None, "released", None)]
    rows = c.fetchall("SELECT lane, epoch, n FROM _cdc.fence")
    assert sorted(rows) == sorted((lane, 0, 0) for lane in md.LANES)
    assert c.fetchall("SELECT epoch, beat_at, progress_at, phase FROM _cdc.heartbeat") == [(0, None, None, "none")]
    assert c.fetchone("SELECT count(*) FROM _cdc.connections")[0] == 0
    assert tables(c) == ["connections", "fence", "heartbeat", "leader"]


@pytest.mark.parametrize("round_", range(5))
def test_starters_at_once_end_with_exactly_one_row_set(round_, no_leftovers):
    """Six starters on an empty database: every one succeeds, and there is one row set."""
    starters = 6
    with td.local_destination(leader_tables=False) as dest:
        conns = [dest.connect("starter") for _ in range(starters)]
        for c in conns:
            c.fetchone("SELECT 1")                      # connected before the race starts
        barrier = threading.Barrier(starters)
        out = [None] * starters

        def start(i):
            barrier.wait(10)
            try:
                out[i] = md.ensure_leader_tables(conns[i])
            except Exception as e:                      # noqa: BLE001 - the test reports whatever it was
                out[i] = e

        threads = [threading.Thread(target=start, args=(i,)) for i in range(starters)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(30)
        assert all(isinstance(o, int) for o in out), out
        look = dest.connect("look")
        assert tuple(look.fetchone(schema.CHECK_LEADER_TABLES_SQL)) == (1, 18, 18, 1)
        schema.check_leader_tables(look)
        assert look.fetchone("SELECT count(*) FROM _cdc.connections")[0] == 0


def test_initialising_again_changes_nothing(dest):
    c = dest.connect("look")
    assert dest.take_over() == 1
    lane = dest.lane("stream", 1)
    with lane.transaction():
        pass
    before = everything(c)
    for _ in range(3):
        assert md.ensure_leader_tables(c) == 0
    assert everything(c) == before
    # the take-over, the lane's registration and its commit raised the counter (it only grows)
    assert c.fetchone("SELECT epoch, n FROM _cdc.fence WHERE lane = 'stream'") == (1, 3)


def test_a_missing_row_is_reported_not_repaired(dest):
    """A row inserted later would miss a concurrent take-over (design 4), so a damaged row set is an error."""
    raw = dest.raw()
    raw.execute("DELETE FROM _cdc.fence WHERE lane = 'backfill.7'")
    raw.close()
    c = dest.connect("look")
    with pytest.raises(md.SchemaError):
        md.ensure_leader_tables(c)
    assert c.fetchone("SELECT count(*) FROM _cdc.fence")[0] == 17
    raw = dest.raw()
    raw.execute("INSERT INTO _cdc.fence VALUES ('backfill.7', 0, 0), ('stream', 0, 0)")
    raw.close()
    with pytest.raises(md.SchemaError):                 # 19 rows, 18 lanes
        schema.check_leader_tables(c)


def test_compare_statements_finds_every_difference():
    exact = list(schema.LEADER_TABLE_STATEMENTS)
    assert schema.compare_statements(exact) == []
    assert schema.compare_statements(tuple(exact)) == []
    assert schema.compare_statements(dict(schema.NAMED_STATEMENTS)) == []
    assert schema.compare_statements({n: schema.NAMED_STATEMENTS[n] for n in schema._CREATE_NAMES}) == []

    changed = list(exact)
    at = changed[1].index("released")
    changed[1] = changed[1].replace("released", "Released")
    assert schema.compare_statements(changed) == [
        "statement 1 differs at character %d: copy %r, original %r" % (at, changed[1][at:at + 24], exact[1][at:at + 24])]
    assert len(schema.compare_statements([s + " " for s in exact])) == 5         # a trailing blank is a difference
    assert len(schema.compare_statements([s.lower() for s in exact])) == 5
    assert schema.compare_statements(exact[:4]) == ["4 statements, expected 5"]
    assert schema.compare_statements(exact + [exact[0]]) == ["6 statements, expected 5"]
    swapped = [exact[0], exact[2], exact[1], exact[3], exact[4]]
    assert len(schema.compare_statements(swapped)) == 2

    by_name = {n: schema.NAMED_STATEMENTS[n] for n in schema._CREATE_NAMES}
    del by_name["create_fence"]
    by_name["create_other"] = "CREATE TABLE x (i INTEGER)"
    by_name["fence"] = "UPDATE _cdc.fence SET n = n + 1 WHERE lane = ? AND epoch = ?"
    diffs = schema.compare_statements(by_name)
    assert "missing statement 'create_fence'" in diffs and "unknown statement name 'create_other'" in diffs
    assert any(d.startswith("fence differs at character ") for d in diffs) and len(diffs) == 3
    assert schema.compare_statements({"fence": md.FENCE_SQL}, required=("fence", "clock")) == [
        "missing statement 'clock'"]
    assert schema.NAMED_STATEMENTS["fence"] == md.FENCE_SQL and schema.NAMED_STATEMENTS["clock"] == md.clock_sql


# --------------------------------------------------------------------------------------------------- migrations
def test_a_fresh_database_gets_the_tables_of_this_package(dest, lists):
    epoch = dest.lead(migrate=False)
    look = dest.connect("look")
    assert tables(look) == ["connections", "fence", "heartbeat", "leader"]
    lane = dest.lane("control", epoch)
    assert md.migrate(lane) == [("state", 1, "transitions_table"), ("alerts", 1, "alerts_table")]
    assert tables(look) == ["alerts", "connections", "fence", "heartbeat", "leader", "schema_version", "transitions"]
    assert schema.versions(look) == {"state": (1, "transitions_table"), "alerts": (1, "alerts_table")}
    assert look.fetchall("SELECT DISTINCT epoch FROM _cdc.schema_version") == [(epoch,)]
    assert look.fetchone("SELECT count(*) FROM _cdc.schema_version WHERE updated_at IS NULL")[0] == 0
    # the take-over, the registration, one fenced transaction for the version table and one for each step
    assert look.fetchone("SELECT epoch, n FROM _cdc.fence WHERE lane = 'control'") == (epoch, 5)


def test_running_again_does_nothing_and_writes_nothing(led, lists):
    look = led.connect("look")
    lane = led.lane("control", led.epoch)
    before = look.fetchone("SELECT n FROM _cdc.fence WHERE lane = 'control'")[0]
    for _ in range(3):
        assert md.migrate(lane) == []
    assert look.fetchone("SELECT n FROM _cdc.fence WHERE lane = 'control'")[0] == before
    assert schema.versions(look) == {"state": (1, "transitions_table"), "alerts": (1, "alerts_table")}


def test_steps_run_in_order_each_in_its_own_transaction(led, lists):
    md.register_migrations("zz_b", log_steps("zz_b", 3))
    md.register_migrations("aa_a", log_steps("aa_a", 2))
    assert [p for p, _ in schema.registered_migrations()] == ["state", "alerts", "aa_a", "zz_b"]
    look = led.connect("look")
    lane = led.lane("control", led.epoch)
    before = look.fetchone("SELECT n FROM _cdc.fence WHERE lane = 'control'")[0]
    assert md.migrate(lane) == [("aa_a", 1, "s1"), ("aa_a", 2, "s2"), ("zz_b", 1, "s1"), ("zz_b", 2, "s2"),
                                ("zz_b", 3, "s3")]
    # five steps, five fenced transactions, and the registration of the lane's new connection
    assert look.fetchone("SELECT n FROM _cdc.fence WHERE lane = 'control'")[0] == before + 6
    assert logged(look, "aa_a") == ["s1", "s2"] and logged(look, "zz_b") == ["s1", "s2", "s3"]
    assert schema.versions(look)["zz_b"] == (3, "s3") and schema.versions(look)["aa_a"] == (2, "s2")
    assert md.migrate(lane) == []

    # a release that adds a step at the end of a list: only the new step runs
    lists["zz_b"] = log_steps("zz_b", 4)
    assert md.migrate(lane) == [("zz_b", 4, "s4")]
    assert logged(look, "zz_b") == ["s1", "s2", "s3", "s4"]


def test_a_failing_step_leaves_nothing_half_done(led, lists):
    md.register_migrations("zz_f", log_steps("zz_f", 3, fail_at=2))
    look = led.connect("look")
    lane = led.lane("control", led.epoch)
    with pytest.raises(md.StatementError) as info:
        md.migrate(lane)
    assert (info.value.error_class, info.value.statement) == ("CatalogException", "SELECT")
    # step 1 stays; of step 2 neither its row nor its table nor the version change is there
    assert schema.versions(look)["zz_f"] == (1, "s1")
    assert logged(look, "zz_f") == ["s1"]
    assert "zz_f_half" not in tables(look, "main")
    with pytest.raises(md.StatementError) as info:      # again: the same result, nothing piles up
        md.migrate(lane)
    assert info.value.error_class == "CatalogException"
    assert schema.versions(look)["zz_f"] == (1, "s1") and logged(look, "zz_f") == ["s1"]

    lists["zz_f"] = log_steps("zz_f", 3)                # the corrected release
    assert md.migrate(lane) == [("zz_f", 2, "s2"), ("zz_f", 3, "s3")]
    assert logged(look, "zz_f") == ["s1", "s2", "s3"]


def test_leaders_at_once_apply_every_step_exactly_once(dest, lists, no_leftovers):
    """Several control lanes of one epoch migrate at once (as two processes of one run would): every step is
    applied once. The fence row makes any two transactions of one lane conflict; the loser reads the version again."""
    md.register_migrations("zz_c", log_steps("zz_c", 4))
    epoch = dest.lead(migrate=False)
    runners = 4
    lanes = [dest.lane("control", epoch) for _ in range(runners)]
    barrier = threading.Barrier(runners)
    out = [None] * runners

    def run(i):
        barrier.wait(10)
        try:
            out[i] = md.migrate(lanes[i])
        except Exception as e:                          # noqa: BLE001
            out[i] = e

    threads = [threading.Thread(target=run, args=(i,)) for i in range(runners)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(60)
    assert all(isinstance(o, list) for o in out), out
    applied = sorted(step for o in out for step in o)
    assert applied == sorted([("state", 1, "transitions_table"), ("alerts", 1, "alerts_table")]
                             + [("zz_c", i, "s%d" % i) for i in range(1, 5)])
    look = dest.connect("look")
    assert logged(look, "zz_c") == ["s1", "s2", "s3", "s4"]
    assert look.fetchone("SELECT count(*), count(DISTINCT package) FROM _cdc.schema_version") == (3, 3)
    assert schema.versions(look)["zz_c"] == (4, "s4")


def test_a_run_that_no_longer_leads_migrates_nothing(dest, lists):
    md.register_migrations("zz_s", log_steps("zz_s", 2))
    look = dest.connect("look")
    old = dest.lane("control", dest.lead(migrate=False))
    with old.transaction():                             # the old run's connection is registered and in use
        pass
    new_epoch = dest.take_over()
    with pytest.raises(md.FenceLost):
        md.migrate(old)
    assert "schema_version" not in tables(look) and "zz_s_log" not in tables(look, "main")
    new = dest.lane("control", new_epoch)
    assert len(md.migrate(new)) == 4
    n = look.fetchone("SELECT n FROM _cdc.fence WHERE lane = 'control'")[0]
    assert md.migrate(old) == []                        # nothing is pending: the old run reads, and writes nothing
    assert look.fetchone("SELECT n FROM _cdc.fence WHERE lane = 'control'")[0] == n
    assert logged(look, "zz_s") == ["s1", "s2"]


def test_only_the_control_lane_migrates(led, lists):
    with pytest.raises(md.LaneMisuse):
        md.migrate(led.lane("stream", led.epoch))


def test_a_database_that_does_not_fit_the_code_is_refused(led, lists):
    md.register_migrations("zz_v", log_steps("zz_v", 3))
    look = led.connect("look")
    lane = led.lane("control", led.epoch)
    assert len(md.migrate(lane)) == 3
    before = look.fetchone("SELECT n FROM _cdc.fence WHERE lane = 'control'")[0]

    lists["zz_v"] = log_steps("zz_v", 2)                # this code is older than the database
    with pytest.raises(md.SchemaError, match="older than the database"):
        md.migrate(lane)
    renamed = list(log_steps("zz_v", 4))
    renamed[2] = md.Step("s3_renamed", renamed[2].statements)
    lists["zz_v"] = tuple(renamed)                      # a released step was changed
    with pytest.raises(md.SchemaError, match="recorded step 3"):
        md.migrate(lane)
    assert schema.versions(look)["zz_v"] == (3, "s3") and logged(look, "zz_v") == ["s1", "s2", "s3"]
    assert look.fetchone("SELECT n FROM _cdc.fence WHERE lane = 'control'")[0] == before


@pytest.mark.parametrize("site, landed", [("md.commit", False), ("md.commit_reply", True)])
def test_a_step_whose_commit_has_no_answer_is_applied_once(led, lists, faults, monkeypatch, site, landed):
    """`CommitUnknown` in a step: the next look at the version says whether it landed; the step never runs twice."""
    monkeypatch.setattr(schema, "LATE_COMMIT_WAIT_S", 0.05)
    md.register_migrations("zz_u", log_steps("zz_u", 3))
    look = led.connect("look")
    lane = led.lane("control", led.epoch)
    faults.raise_at(site, "ConnectionError", message="injected: connection reset", skip=1, hits=1)   # step 2
    assert md.migrate(lane) == [("zz_u", 1, "s1"), ("zz_u", 2, "s2"), ("zz_u", 3, "s3")]
    assert len(faults.fired(site=site, action="raise")) == 1
    assert logged(look, "zz_u") == ["s1", "s2", "s3"]
    assert lane.conn.generation == 2                    # the lane took a new connection


def test_registering_lists(lists):
    step = md.Step("one", "CREATE TABLE x (i INTEGER)")
    assert step.statements == ("CREATE TABLE x (i INTEGER)",)
    assert md.register_migrations("zz_r", [step]) == (step,)
    assert md.register_migrations("zz_r", (md.Step("one", ["CREATE TABLE x (i INTEGER)"]),)) == (step,)   # the same
    with pytest.raises(ValueError, match="two different"):
        md.register_migrations("zz_r", [step, md.Step("two", "SELECT 1")])
    with pytest.raises(ValueError, match="one name"):
        md.register_migrations("zz_q", [step, step])
    for bad in ("", "Upper", "has space", "1st", None, "x" * 80):
        with pytest.raises(ValueError):
            md.register_migrations(bad, [step])
        with pytest.raises(ValueError):
            md.Step(bad, "SELECT 1")
    for bad in ((), [None], ["text instead of a Step"]):
        with pytest.raises(ValueError):
            md.register_migrations("zz_q", bad)
    for bad in ((), "", ["SELECT 1", ""], [None]):
        with pytest.raises(ValueError):
            md.Step("one", bad)
    assert "zz_q" not in lists
    with pytest.raises(Exception):                      # frozen: a step cannot be changed after it was made
        step.name = "other"
