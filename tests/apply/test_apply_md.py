"""Package A on MotherDuck (marker md): the DuckDB behaviours the statements rely on, every named case in both
change-set forms, the property tests with more examples, and (CDC_V2_APPLY_PERF=1) short speed indications.

    CDC_V2_MD_PREFIX=cdc_v2_t_a_ tools/with-env uv run pytest -m md tests/apply
"""
from __future__ import annotations

import os
import platform
import time

import duckdb
import pytest

from apply_cases import all_cases
from apply_harness import catch_up_scenario, local_conn, run_case, stream_scenario
from cdc_v2.apply.types import exact, parse, sentinel

pytestmark = pytest.mark.md

NZ = "CAST('-0' AS DOUBLE)"
BITS = "CAST(CAST({} AS BIT) AS BIGINT)"
EVIDENCE = "p3/apply"


@pytest.fixture(scope="module")
def mdb(make_dest_db):
    if not (os.environ.get("motherduck_token") or os.environ.get("MOTHERDUCK_TOKEN")):
        pytest.skip("no MotherDuck token (run through tools/with-env)")
    return make_dest_db()


_n = [0]


def fresh_table():
    _n[0] += 1
    return "t%d" % _n[0]


# ------------------------------------------------------------------------------------------------ semantics


def _semantics(c):
    """The facts of docs/design/apply.md 5, measured on one connection; returns a dict for the evidence."""
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


def test_duckdb_semantics_local_and_motherduck(mdb, evidence):
    local = _semantics(local_conn())
    remote = _semantics(mdb.conn)
    differ = sorted(k for k in local if local[k] != remote.get(k))
    evidence(EVIDENCE).write_json("duckdb_semantics", {
        "duckdb": duckdb.__version__, "local": local, "motherduck": remote, "differ": differ,
        "command": "CDC_V2_MD_PREFIX=cdc_v2_t_a_ tools/with-env uv run pytest -m md tests/apply/test_apply_md.py -k semantics"})
    for name in ("DOUBLE %s -> 0.0::DOUBLE" % NZ, "INTERVAL INTERVAL '1 month' -> INTERVAL '30 days'"):
        assert remote[name]["sentinel_then_update_stores_new_value"], name
        assert not remote[name]["update_through_null_stores_new_value"], name
    assert remote["rowid deletes exactly 4 of 5 identical rows, 2 of them inserted by the transaction"]
    assert remote["rename of a rowid column inside a transaction"]


# ------------------------------------------------------------------------------------------------ cases


CASES = [(c, f) for c in all_cases() for f in ("local", "relation")]
_case_log = []


@pytest.mark.parametrize("case,form", CASES, ids=["%s-%s" % (c.name, f) for c, f in CASES])
def test_case_on_motherduck(case, form, mdb):
    t0 = time.monotonic()
    info = run_case(case, mdb.conn, form, table=fresh_table())
    info["seconds"] = round(time.monotonic() - t0, 3)
    _case_log.append(info)


def test_write_case_evidence(evidence):
    if not _case_log:
        pytest.skip("no case ran")
    evidence(EVIDENCE).write_json("md_cases", {
        "cases": len(_case_log), "all_passed": True, "runs": _case_log,
        "note": "every named case of tests/apply/apply_cases.py on MotherDuck, local form and relation form; "
                "statements = statements per change set (round trips); this machine to MotherDuck over the internet",
        "command": "CDC_V2_MD_PREFIX=cdc_v2_t_a_ tools/with-env uv run pytest -m md tests/apply/test_apply_md.py"})


# ------------------------------------------------------------------------------------------------ properties


MD_EXAMPLES = int(os.environ.get("CDC_V2_APPLY_MD_EXAMPLES", "25"))
_prop_log = []


@pytest.mark.parametrize("i", range(MD_EXAMPLES))
def test_property_on_motherduck(i, mdb):
    seed = 1_000_003 * (i + 1)
    form = "relation" if i % 4 == 3 else "local"
    kind = i % 3
    if kind == 0:
        entry = stream_scenario(mdb.conn, seed, keyed=True, form=form, table=fresh_table())
    elif kind == 1:
        entry = stream_scenario(mdb.conn, seed, keyed=False, form=form, table=fresh_table())
    else:
        entry = catch_up_scenario(mdb.conn, seed, form=form, table=fresh_table())
    _prop_log.append(entry)


def test_write_property_evidence(evidence):
    if not _prop_log:
        pytest.skip("no property example ran")
    evidence(EVIDENCE).write_json("md_property", {
        "examples": len(_prop_log), "failures": 0, "entries": _prop_log,
        "command": "CDC_V2_MD_PREFIX=cdc_v2_t_a_ CDC_V2_APPLY_MD_EXAMPLES=%d tools/with-env uv run pytest -m md "
                   "tests/apply/test_apply_md.py -k property" % MD_EXAMPLES})


# ------------------------------------------------------------------------------------------------ speed


@pytest.mark.skipif(os.environ.get("CDC_V2_APPLY_PERF") != "1", reason="speed indications: CDC_V2_APPLY_PERF=1")
def test_speed_indications(mdb, evidence):
    from apply_perf import run_perf
    result = run_perf(mdb.conn, sizes=[int(x) for x in os.environ.get("CDC_V2_APPLY_PERF_SIZES", "1,100,100000,1000000").split(",")])
    result["machine"] = {"cpus": os.cpu_count(), "platform": platform.machine(), "where": "laptop over the internet"}
    evidence(EVIDENCE).write_json("perf_md", result)
