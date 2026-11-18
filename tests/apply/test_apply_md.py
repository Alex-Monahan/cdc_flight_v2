"""Package A on MotherDuck (marker md), through package D's real lane (`apply_lane.md_db`: a database
`cdc_v2_t_a_<random>` with the leader tables, created and dropped through the test ledger): the DuckDB behaviours
the statements rely on, every named case in both change-set forms, the property tests with more examples, and
(CDC_V2_APPLY_PERF=1) short speed indications. Every evidence file names the commit and the command.

    CDC_V2_MD_PREFIX=cdc_v2_t_a_ tools/with-env uv run pytest -m md tests/apply/test_apply_md.py
"""
from __future__ import annotations

import os
import platform
import time

import duckdb
import pyarrow as pa
import pytest

from apply_cases import all_cases
from apply_harness import catch_up_scenario, md_db, run_case, scratch, stream_scenario
from apply_lane import run_command, run_commit
from cdc_v2.apply.types import exact, parse, sentinel, source_wrap

pytestmark = pytest.mark.md

NZ = "CAST('-0' AS DOUBLE)"
BITS = "CAST(CAST({} AS BIT) AS BIGINT)"
EVIDENCE = "p3/apply"
PREFIX = "cdc_v2_t_a_"


@pytest.fixture(scope="module")
def mdb():
    if not (os.environ.get("motherduck_token") or os.environ.get("MOTHERDUCK_TOKEN")):
        pytest.skip("no MotherDuck token (run through tools/with-env)")
    if os.environ.get("CDC_V2_MD_PREFIX") != PREFIX:
        pytest.skip("set CDC_V2_MD_PREFIX=%s (package A's test databases)" % PREFIX)
    db = md_db(PREFIX, shared=True)
    yield db
    db.close()


_n = [0]


def fresh_table():
    _n[0] += 1
    return "t%d" % _n[0]


def _header():
    return {"commit": run_commit(), "command": run_command(), "duckdb": duckdb.__version__,
            "lane": "package D's md.Lane 'stream' (cdc_v2.testing.destination), one lane transaction per change set "
                    "or per case's one_tx"}


# ------------------------------------------------------------------------------------------------ semantics


def _semantics(c):
    """The facts of docs/design/apply.md 5, measured on one plain client connection; returns a dict for the
    evidence."""
    out = {}
    q = lambda s: c.execute(s).fetchall()
    for typ, old, new in [("DOUBLE", NZ, "0.0::DOUBLE"), ("INTERVAL", "INTERVAL '1 month'", "INTERVAL '30 days'"),
                          ("STRUCT(a DOUBLE)", "{'a': %s}" % NZ, "{'a': 0.0::DOUBLE}")]:
        t = parse(typ)
        name = "%s %s -> %s" % (typ, old, new)
        tab = fresh_table()
        q("CREATE TABLE %s (k INT, x %s)" % (tab, typ))
        q("INSERT INTO %s VALUES (1, %s), (2, %s)" % (tab, old, old))
        want = q("SELECT %s" % exact("CAST(%s AS %s)" % (new, typ), t))[0][0]
        probe = lambda k: q("SELECT %s FROM %s WHERE k = %d" % (exact("x", t), tab, k))[0][0]
        q("UPDATE %s SET x = %s WHERE k = 1" % (tab, new))
        plain = probe(1) == want
        q("UPDATE %s SET x = NULL WHERE k = 2" % tab)
        q("UPDATE %s SET x = %s WHERE k = 2" % (tab, new))
        through_null = probe(2) == want
        q("UPDATE %s SET x = %s WHERE k IN (1, 2)" % (tab, sentinel("CAST(%s AS %s)" % (new, typ), t)))
        q("UPDATE %s SET x = %s WHERE k IN (1, 2)" % (tab, new))
        cured = probe(1) == want and probe(2) == want
        out[name] = {"update_stores_new_value": plain, "update_through_null_stores_new_value": through_null,
                     "sentinel_then_update_stores_new_value": cured}
    for typ, val in [("INTEGER[]", "[2]"), ("UNION(i INT, s VARCHAR)", "union_value(i := 2)"), ("MAP(INT, INT)", "MAP {1: 2}"),
                     ("STRUCT(a INT)", "{'a': 2}"), ("VARCHAR", "'z'")]:
        tab = fresh_table()
        q("CREATE TABLE %s (k INT, x %s)" % (tab, typ))
        q("INSERT INTO %s VALUES (1, NULL), (2, NULL)" % tab)
        before = q("SELECT rowid FROM %s WHERE k = 1" % tab)[0][0]
        q("UPDATE %s SET x = %s WHERE k = 1" % (tab, val))
        out["update of %s rewrites the row" % typ] = q("SELECT rowid FROM %s WHERE k = 1" % tab)[0][0] != before
    tab = fresh_table()
    q("CREATE TABLE %s AS SELECT 1 AS g, [%s] AS l, {'a': %s} AS st, %s AS d" % (tab, NZ, NZ, NZ))
    out["any_value(list) keeps -0"] = q("SELECT %s FROM (SELECT any_value(l) AS l FROM %s)" % (BITS.format("l[1]"), tab))[0][0] < 0
    out["arg_max(struct) keeps -0"] = q("SELECT %s FROM (SELECT arg_max(st, g) AS s FROM %s)" % (BITS.format("s.a"), tab))[0][0] < 0
    out["any_value(double) keeps -0"] = q("SELECT %s FROM (SELECT any_value(d) AS d FROM %s)" % (BITS.format("d"), tab))[0][0] < 0
    out["join keeps -0 in a list"] = q("SELECT %s FROM %s a JOIN %s b USING (g)" % (BITS.format("a.l[1]"), tab, tab))[0][0] < 0
    # identical rows: rowid deletes exactly k, also rows the transaction inserted itself
    tab = fresh_table()
    q("CREATE TABLE %s (a INT, b VARCHAR)" % tab)
    q("INSERT INTO %s VALUES (1,'x'),(1,'x'),(1,'x'),(2,'y')" % tab)
    q("BEGIN")
    q("INSERT INTO %s VALUES (1,'x'),(1,'x')" % tab)
    n = q("DELETE FROM %s WHERE rowid IN (SELECT rid FROM (SELECT t.rowid AS rid, row_number() OVER (PARTITION BY a, b "
          "ORDER BY t.rowid) AS rn FROM %s AS t WHERE a = 1) WHERE rn <= 4)" % (tab, tab))[0][0]
    q("COMMIT")
    out["rowid deletes exactly 4 of 5 identical rows, 2 of them inserted by the transaction"] = (
        n == 4 and q("SELECT count(*) FROM %s WHERE a = 1" % tab)[0][0] == 1)
    out.update(_rowid_stable(c))
    out.update(_negative_zero_against_arrow(c))
    # tables created and dropped in one transaction
    q("BEGIN")
    q("CREATE SCHEMA _cdc_apply_probe")
    q("CREATE TABLE _cdc_apply_probe.t1 AS SELECT 1 AS a")
    q("CREATE TABLE _cdc_apply_probe.t2 AS SELECT * FROM _cdc_apply_probe.t1")
    q("DROP SCHEMA _cdc_apply_probe CASCADE")
    try:
        q("COMMIT")
        out["DROP SCHEMA CASCADE of tables created in the same transaction commits"] = True
    except duckdb.Error as e:
        out["DROP SCHEMA CASCADE of tables created in the same transaction commits"] = False
        out["DROP SCHEMA CASCADE error"] = str(e).split("\n")[0][:160]
        try:
            q("ROLLBACK")
        except duckdb.Error:
            pass
    # a column named rowid, renamed inside a transaction and back
    tab = fresh_table()
    q('CREATE TABLE %s ("rowid" INT, v VARCHAR)' % tab)
    q("INSERT INTO %s VALUES (7, 'a'), (7, 'a')" % tab)
    q("BEGIN")
    q('ALTER TABLE %s RENAME COLUMN "rowid" TO rowid_tmp' % tab)
    n = q("DELETE FROM %s WHERE rowid IN (SELECT min(rowid) FROM %s)" % (tab, tab))[0][0]
    q('ALTER TABLE %s RENAME COLUMN rowid_tmp TO "rowid"' % tab)
    q("COMMIT")
    out["rename of a rowid column inside a transaction"] = n == 1 and q('SELECT count(*), min("rowid") FROM %s' % tab)[0] == (1, 7)
    return out


ROWID_STABLE = ("row ids stay the same within a transaction: a committed row and a row the transaction inserted, "
                "across a delete of another row and updates in place of VARCHAR, DOUBLE and the key")


def _rowid_stable(c):
    """What C0 to K7/K8 rely on (docs/design/apply.md 3.2 step 4, 11): a row id read inside a transaction still
    names the same row after the statements that run between C0 and K7/K8 (K1 deletes other rows; K2/K3 update
    rows in place, the key included; no LIST/MAP/UNION column is set: those rewrite the row, section 5.4), also
    for a row the transaction inserted itself; and a DELETE by that row id removes exactly that row."""
    q = lambda s: c.execute(s).fetchall()
    tab = fresh_table()
    q("CREATE TABLE %s (k BIGINT, v VARCHAR, d DOUBLE)" % tab)
    q("INSERT INTO %s VALUES (1, 'a', 1.0), (2, 'b', 2.0), (3, 'c', 3.0)" % tab)
    q("BEGIN")
    q("INSERT INTO %s VALUES (10, 'x', 4.0)" % tab)
    before = dict(q("SELECT k, rowid FROM %s WHERE k IN (1, 3, 10)" % tab))
    q("DELETE FROM %s WHERE k = 2" % tab)
    q("UPDATE %s SET v = 'cc', d = 30.0 WHERE k = 3" % tab)
    q("UPDATE %s SET k = 30 WHERE k = 3" % tab)
    q("UPDATE %s SET v = 'xx', d = %s WHERE k = 10" % (tab, NZ))
    after = dict(q("SELECT k, rowid FROM %s" % tab))
    same = after.get(1) == before[1] and after.get(30) == before[3] and after.get(10) == before[10]
    n = q("DELETE FROM %s WHERE rowid = %d" % (tab, before[10]))[0][0]
    m = q("DELETE FROM %s WHERE rowid = %d" % (tab, before[3]))[0][0]
    left = sorted(r[0] for r in q("SELECT k FROM %s" % tab))
    q("COMMIT")
    return {ROWID_STABLE: {"same_row_ids": same, "delete_by_row_id_of_the_inserted_row": n,
                           "delete_by_row_id_of_the_moved_row": m, "keys_left": left}}


NZ_ARROW = "a stored -0 (DOUBLE) against an Arrow-side 0: rows found"


def _negative_zero_against_arrow(c):
    """The finding behind `types.source_wrap` (docs/design/apply.md 5.5): a join of a stored `-0` with the value
    `0` of registered Arrow data, by the plain DOUBLE column and through the expression `x + 0` on both sides,
    for a committed row and for a row the same transaction wrote; `=` with a SQL literal for comparison."""
    q = lambda s: c.execute(s).fetchall()
    tab = fresh_table()
    q("CREATE TABLE %s (k DOUBLE, v VARCHAR)" % tab)
    q("INSERT INTO %s VALUES (%s, 'nz'), (7.0, 'seven')" % (tab, NZ))
    c.register("cdc_v2_probe_zero", pa.table({"k": pa.array([0.0], pa.float64())}))
    wrap = lambda e: source_wrap(e, parse("DOUBLE"))
    plain, wrapped = "t.k = r.k", "%s = %s" % (wrap("t.k"), wrap("r.k"))
    res = {"sql literal: k = 0.0": q("SELECT count(*) FROM %s WHERE k = 0.0" % tab)[0][0]}
    try:
        for label, cond in (("plain column", plain), ("x + 0 on both sides", wrapped)):
            res["select join, %s" % label] = q("SELECT count(*) FROM %s AS t JOIN cdc_v2_probe_zero AS r ON %s"
                                               % (tab, cond))[0][0]
            for where in ("committed row", "row written by the same transaction"):
                q("BEGIN")
                if where != "committed row":
                    q("DELETE FROM %s WHERE v = 'nz'" % tab)
                    q("INSERT INTO %s VALUES (%s, 'nz')" % (tab, NZ))
                res["delete join, %s, %s" % (label, where)] = q(
                    "DELETE FROM %s AS t USING cdc_v2_probe_zero AS r WHERE %s" % (tab, cond))[0][0]
                q("ROLLBACK")
    finally:
        c.unregister("cdc_v2_probe_zero")
    return {NZ_ARROW: res}


def test_duckdb_semantics_local_and_motherduck(mdb, evidence):
    local = _semantics(scratch())
    remote = _semantics(mdb.raw)
    differ = sorted(k for k in local if local[k] != remote.get(k))
    doc = _header()
    doc.update({"local": local, "motherduck": remote, "differ": differ,
                "note": "measured on a plain client connection of each (not through the lane): what apply's "
                        "statements rely on (docs/design/apply.md 5; docs/spike/md_semantics.md, 'Facts apply "
                        "relies on')"})
    evidence(EVIDENCE).write_json("duckdb_semantics", doc)
    for name in ("DOUBLE %s -> 0.0::DOUBLE" % NZ, "INTERVAL INTERVAL '1 month' -> INTERVAL '30 days'"):
        assert remote[name]["sentinel_then_update_stores_new_value"], name
        assert not remote[name]["update_through_null_stores_new_value"], name
    assert remote["rowid deletes exactly 4 of 5 identical rows, 2 of them inserted by the transaction"]
    assert remote["rename of a rowid column inside a transaction"]
    for side in (local, remote):
        r = side[ROWID_STABLE]
        assert r["same_row_ids"] and r["delete_by_row_id_of_the_inserted_row"] == 1, r
        assert r["delete_by_row_id_of_the_moved_row"] == 1 and r["keys_left"] == [1], r
        z = side[NZ_ARROW]
        assert z["sql literal: k = 0.0"] == 1
        assert all(v == 1 for k, v in z.items() if "x + 0" in k), z       # what repair_keys relies on


# ------------------------------------------------------------------------------------------------ cases


CASES = [(c, f) for c in all_cases() for f in ("local", "relation")]
_case_log = []


@pytest.mark.parametrize("case,form", CASES, ids=["%s-%s" % (c.name, f) for c, f in CASES])
def test_case_on_motherduck(case, form, mdb):
    t0 = time.monotonic()
    info = run_case(case, mdb, form, table=fresh_table())
    info["seconds"] = round(time.monotonic() - t0, 3)
    _case_log.append(info)


@pytest.mark.parametrize("form", ["local", "relation"])
def test_repair_keys_on_motherduck(form, mdb):
    """apply.repair_keys: the late repair and the delete by the source's equality (-0 / 0, 1 month / 30 days)."""
    from test_apply_limits import repair_case_late, repair_case_source_equal
    repair_case_late(mdb, form, fresh_table())
    repair_case_source_equal(mdb, form, fresh_table())


def test_lane_rules_on_motherduck(mdb):
    """The points of the fix round after D's merge that depend on the destination, on MotherDuck: a refused
    statement is D's StatementError without the value; D's errors pass through; registered names are gone when
    the block ends; HUGEINT sums come back as numbers (test_apply_lane.py runs the same on local DuckDB)."""
    import test_apply_lane as tl
    tl.refused_value_case(mdb, fresh_table())
    tl.names_gone_case(mdb, fresh_table())
    tl.keyless_relation_case(mdb, fresh_table())


def test_write_case_evidence(evidence):
    if not _case_log:
        pytest.skip("no case ran")
    doc = _header()
    doc.update({"cases": len(_case_log), "all_passed": True, "runs": _case_log,
                "note": "every named case of tests/apply/apply_cases.py on MotherDuck through D's lane, local form "
                        "and relation form; statements = apply's statements per change set (round trips; the lane "
                        "adds its fence statement and COMMIT per block); this machine to MotherDuck over the "
                        "internet"})
    evidence(EVIDENCE).write_json("md_cases", doc)


# ------------------------------------------------------------------------------------------------ properties


MD_EXAMPLES = int(os.environ.get("CDC_V2_APPLY_MD_EXAMPLES", "25"))
_prop_log = []


@pytest.mark.parametrize("i", range(MD_EXAMPLES))
def test_property_on_motherduck(i, mdb):
    seed = 1_000_003 * (i + 1)
    form = "relation" if i % 4 == 3 else "local"
    kind = i % 3
    if kind == 0:
        entry = stream_scenario(mdb, seed, keyed=True, form=form, table=fresh_table())
    elif kind == 1:
        entry = stream_scenario(mdb, seed, keyed=False, form=form, table=fresh_table())
    else:
        entry = catch_up_scenario(mdb, seed, form=form, table=fresh_table())
    _prop_log.append(entry)


def test_write_property_evidence(evidence):
    if not _prop_log:
        pytest.skip("no property example ran")
    doc = _header()
    doc.update({"examples": len(_prop_log), "failures": 0, "entries": _prop_log,
                "examples_setting": "CDC_V2_APPLY_MD_EXAMPLES=%d" % MD_EXAMPLES})
    evidence(EVIDENCE).write_json("md_property", doc)


# ------------------------------------------------------------------------------------------------ speed


@pytest.mark.skipif(os.environ.get("CDC_V2_APPLY_PERF") != "1", reason="speed indications: CDC_V2_APPLY_PERF=1")
def test_speed_indications(evidence):
    from apply_perf import run_perf
    with md_db(PREFIX, statement_deadline_s=600.0, transaction_deadline_s=1800.0) as db:
        result = run_perf(db, sizes=[int(x) for x in os.environ.get("CDC_V2_APPLY_PERF_SIZES",
                                                                   "1,100,100000,1000000").split(",")])
    result["machine"] = {"cpus": os.cpu_count(), "platform": platform.machine(), "where": "laptop over the internet"}
    result.update(_header())
    evidence(EVIDENCE).write_json("perf_md", result)
