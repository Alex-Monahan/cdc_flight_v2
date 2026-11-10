"""Negative controls (fix round, item 12): planners that are wrong on purpose. The property scenarios must catch
each of them; the number of examples it took is recorded by the campaign.

(a) keyed tables: "the last record per key decides" (design 6.2 as first written), with absent values left NULL;
(b) keyless tables: plain GROUP BY / join on the values (no exact form), so -0 and 0, NaN forms and equal-length
    intervals are merged;
(c) catch-up: a planner that reports every key of the change set unresolved (and deletes its rows), which the
    bound of item 7c must catch.
"""
from __future__ import annotations

import contextlib

import apply_harness
from apply_sim import change_set_table
from apply_standin import StandInTx
from cdc_v2.apply import ApplyCounts
from cdc_v2.oracle.canonical import quote_ident


def last_record_decides(conn, target, meta, sets, mode="stream", form="local", one_tx=False, settings=None):
    """(a): delete every key of the change set, insert the last record of each key unless it is a delete."""
    out = []
    keys = meta.key_columns
    cols = list(meta.columns)
    for recs in sets:
        rows = change_set_table(meta, recs)
        with StandInTx(conn) as tx:
            tx.register("ctl_rows", rows)
            kcond = " AND ".join("t.%s IS NOT DISTINCT FROM r.%s" % (c.qname, c.a) for c in keys)
            tx.execute("DELETE FROM %s AS t USING (SELECT DISTINCT %s FROM ctl_rows UNION SELECT DISTINCT %s FROM "
                       "ctl_rows WHERE %s IS NOT NULL) AS r WHERE %s"
                       % (target.sql(), ", ".join(c.a for c in keys),
                          ", ".join("%s AS %s" % (c.k, c.a) for c in keys), keys[0].k, kcond))
            tx.execute("INSERT INTO %s (%s) SELECT %s FROM (SELECT *, row_number() OVER (PARTITION BY %s ORDER BY "
                       "txn DESC, seq DESC) AS rk FROM ctl_rows) WHERE rk = 1 AND op <> 'd'"
                       % (target.sql(), ", ".join(c.qname for c in cols), ", ".join(c.a for c in cols),
                          ", ".join(c.a for c in keys)))
        out.append(ApplyCounts())
    return out


@contextlib.contextmanager
def plain_values():
    """(b): the planner and the statements compare raw values instead of their exact forms."""
    import cdc_v2.apply.plan as plan
    import cdc_v2.apply.statements as statements
    saved = (plan.exact, statements.exact)
    ident = lambda expr, t, depth=0: expr
    plan.exact = statements.exact = ident
    try:
        yield
    finally:
        plan.exact, statements.exact = saved


def everything_unresolved(conn, target, meta, sets, mode="catch_up", form="local", one_tx=False, settings=None):
    """(c): apply, then report every key of the change set unresolved and delete those rows."""
    out = apply_harness.apply_sets(conn, target, meta, sets, mode=mode, form=form, one_tx=one_tx, settings=settings)
    for recs, counts in zip(sets, out):
        rows = change_set_table(meta, recs)
        keys = meta.key_columns
        con = apply_harness.local_conn()
        con.register("r", rows)
        counts.unresolved = con.execute("SELECT DISTINCT %s FROM r" % ", ".join(c.a for c in keys)).to_arrow_table()
        conn.register("ctl_unres", counts.unresolved)
        conn.execute("DELETE FROM %s AS t USING ctl_unres AS u WHERE %s" % (
            target.sql(), " AND ".join("t.%s = u.%s" % (c.qname, c.a) for c in keys)))
        conn.unregister("ctl_unres")
    return out


def caught_after(run, start_seed: int, limit: int):
    """How many examples until `run(seed)` raises AssertionError (None if not within `limit`)."""
    for i in range(limit):
        try:
            run(start_seed + i)
        except AssertionError:
            return i + 1
    return None
