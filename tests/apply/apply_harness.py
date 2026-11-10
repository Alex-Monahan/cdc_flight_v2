"""Drive apply against a destination (local DuckDB or MotherDuck) and compare with the simulator."""
from __future__ import annotations

import random
from typing import List, Optional

import duckdb

from apply_sim import (KeyedSource, KeylessSource, Rec, canon_rows, change_set_table, create_table,
                       literal_rows_canon, random_meta)
from apply_standin import StandInTx
from cdc_v2.apply import (ApplySettings, LocalChangeSet, RelationChangeSet, Target, apply_group,
                          reduce_duplicates)
from cdc_v2.apply.types import quote_ident

SCHEMA = "s p.ä"


def local_conn():
    con = duckdb.connect()
    con.execute("SET TimeZone = 'UTC'")
    return con


def split_by_txn(recs: List[Rec], cuts: List[int]) -> List[List[Rec]]:
    """Split records into change sets at transaction boundaries: `cuts` are transaction numbers that start a new
    change set."""
    out, cur = [], []
    cuts = set(cuts)
    last = None
    for r in recs:
        if cur and r.txn != last and r.txn in cuts:
            out.append(cur)
            cur = []
        cur.append(r)
        last = r.txn
    if cur:
        out.append(cur)
    return out


def apply_sets(conn, target, meta, sets: List[List[Rec]], mode="stream", form="local", one_tx=False,
               settings: Optional[ApplySettings] = None, stage_conn=None):
    """Apply each change set (one transaction each, or all in one). Returns the list of ApplyCounts."""
    out = []

    def one(tx, recs, i):
        rows = change_set_table(meta, recs)
        if form == "local":
            cs = LocalChangeSet(rows, None, meta)
        else:
            name = "_cdc_stage_test_%d" % i
            # stage the rows as a pending log would: commit_lsn instead of txn
            conn.register("stage_src", rows)
            conn.execute("CREATE SCHEMA IF NOT EXISTS _cdc_stage")
            conn.execute("CREATE OR REPLACE TABLE _cdc_stage.%s AS SELECT * EXCLUDE (txn), "
                         "CAST(txn * 16 + 1000 AS UBIGINT) AS commit_lsn, CAST(txn AS UBIGINT) AS xid, "
                         "TIMESTAMPTZ '2026-01-01 00:00:00+00' AS commit_ts FROM stage_src" % name)
            conn.unregister("stage_src")
            cs = RelationChangeSet("SELECT * FROM _cdc_stage.%s WHERE commit_lsn >= ?" % name, (0,), meta)
        return apply_group(tx, target, cs, mode=mode, settings=settings)

    if one_tx:
        with StandInTx(conn) as tx:
            for i, s in enumerate(sets):
                out.append(one(tx, s, i))
    else:
        for i, s in enumerate(sets):
            with StandInTx(conn) as tx:
                out.append(one(tx, s, i))
    return out


def dest_canon(conn, target, meta):
    return canon_rows(conn, target.sql(), list(meta.columns))


# ------------------------------------------------------------------------------------------------ named cases


def key_canon(meta, table):
    if table is None or table.num_rows == 0:
        return set()
    from apply_sim import canon
    con = local_conn()
    con.register("u", table)
    out = {tuple(r) for r in con.execute("SELECT %s FROM u" % ", ".join(canon(c.a, c.type)
                                                                         for c in meta.key_columns)).fetchall()}
    con.close()
    return out


def run_case(case, conn, form="local", table="t"):
    """Run a named case on `conn` (local DuckDB or MotherDuck). Returns a dict for the evidence; asserts."""
    import pytest
    from apply_sim import lit_canon
    from cdc_v2.apply import ApplyInvariantError, ChangeSetError
    target = Target(SCHEMA, table)
    create_table(conn, SCHEMA, table, case.meta, case.start)
    before = dest_canon(conn, target, case.meta)
    info = {"case": case.name, "form": form, "mode": case.mode}
    if case.error:
        exc = {"invariant": ApplyInvariantError, "changeset": ChangeSetError}[case.error]
        with pytest.raises(exc):
            apply_sets(conn, target, case.meta, case.sets, mode=case.mode, form=form, one_tx=case.one_tx)
        assert dest_canon(conn, target, case.meta) == before, "the caller's roll-back leaves the table as it was"
        info["error"] = case.error
        return info
    counts = apply_sets(conn, target, case.meta, case.sets, mode=case.mode, form=form, one_tx=case.one_tx)
    got = dest_canon(conn, target, case.meta)
    want = literal_rows_canon(list(case.meta.columns), case.expect)
    assert got == want, "%s/%s: destination differs:\n extra %s\n missing %s" % (case.name, form, got - want,
                                                                                 want - got)
    info["statements"] = [c.statements for c in counts]
    info["shapes"] = [c.shapes for c in counts]
    if case.max_statements is not None and form == "local":
        assert all(c.statements <= case.max_statements for c in counts), info
    if case.unresolved is not None:
        got_u = set().union(*[key_canon(case.meta, c.unresolved) for c in counts])
        want_u = {tuple(lit_canon(l, c.type) for l, c in zip(k, case.meta.key_columns)) for k in case.unresolved}
        assert got_u == want_u, (got_u, want_u)
        info["unresolved"] = len(got_u)
    if case.twice:
        apply_sets(conn, target, case.meta, case.sets, mode=case.mode, form=form, one_tx=case.one_tx)
        again = dest_canon(conn, target, case.meta)
        assert again == got, "%s/%s: a second catch-up changed the table" % (case.name, form)
        info["idempotent"] = True
    return info
