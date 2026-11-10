"""Speed indications for package A (not results: SPEC says performance comes from Flight runs).

A lineitem-width table (16 columns, TPC-H names) with a key; change sets of n records: one third inserts of new
keys, one third complete updates, one third deletes ("full"), or the updates leave the comment column unchanged
("unchanged"); and the same table without a key (deletes and updates carry the old image). Each change set is
applied in one stand-in transaction; the time is from apply_group's call to COMMIT returning.
"""
from __future__ import annotations

import resource
import time

import duckdb
import pyarrow as pa

from apply_standin import StandInTx
from cdc_v2.apply import ApplySettings, Column, LocalChangeSet, TableMeta, Target, apply_group

COLS = [("l_orderkey", "BIGINT"), ("l_partkey", "BIGINT"), ("l_suppkey", "BIGINT"), ("l_linenumber", "INTEGER"),
        ("l_quantity", "DECIMAL(15,2)"), ("l_extendedprice", "DECIMAL(15,2)"), ("l_discount", "DECIMAL(15,2)"),
        ("l_tax", "DECIMAL(15,2)"), ("l_returnflag", "VARCHAR"), ("l_linestatus", "VARCHAR"), ("l_shipdate", "DATE"),
        ("l_commitdate", "DATE"), ("l_receiptdate", "DATE"), ("l_shipinstruct", "VARCHAR"), ("l_shipmode", "VARCHAR"),
        ("l_comment", "VARCHAR")]
TEXT = {"VARCHAR", "DECIMAL(15,2)"}         # transported as text: "unchanged possible" (engine.md section 3)


def meta(keyed: bool) -> TableMeta:
    return TableMeta(tuple(Column(i, n, t, key=(keyed and i == 0), unchanged_possible=(t in TEXT and not (keyed and i == 0)))
                           for i, (n, t) in enumerate(COLS)))


def _row_sql(i: str, v: str = "0") -> list:
    """Column expressions of row `i` (version `v`), deterministic, as DuckDB SQL."""
    return ["CAST(%s AS BIGINT)" % i, "CAST(%s * 7 + %s AS BIGINT)" % (i, v), "CAST(%s %% 10000 AS BIGINT)" % i,
            "CAST(%s %% 7 + 1 AS INTEGER)" % i, "CAST((%s %% 50) + 1 AS DECIMAL(15,2))" % i,
            "CAST((%s %% 100000) / 100.0 + %s AS DECIMAL(15,2))" % (i, v), "CAST((%s %% 11) / 100.0 AS DECIMAL(15,2))" % i,
            "CAST((%s %% 9) / 100.0 AS DECIMAL(15,2))" % i, "CASE %s %% 3 WHEN 0 THEN 'A' WHEN 1 THEN 'N' ELSE 'R' END" % i,
            "CASE %s %% 2 WHEN 0 THEN 'O' ELSE 'F' END" % i, "DATE '1992-01-01' + CAST(%s %% 2500 AS INTEGER)" % i,
            "DATE '1992-02-01' + CAST(%s %% 2500 AS INTEGER)" % i, "DATE '1992-03-01' + CAST(%s %% 2500 AS INTEGER)" % i,
            "'DELIVER IN PERSON'", "'TRUCK'", "'comment number ' || CAST(%s AS VARCHAR) || ' version ' || %s" % (i, v)]


def _local():
    c = duckdb.connect()
    c.execute("SET TimeZone = 'UTC'")
    c.execute("SET arrow_lossless_conversion = true")
    return c


def change_set(m: TableMeta, n: int, base: int, shape: str) -> pa.Table:
    """n records over a table that holds keys 0 .. base-1."""
    keyed = m.has_key
    third = max(1, n // 3)
    ins, upd, dele = n - 2 * third if n >= 3 else n, third if n >= 3 else 0, third if n >= 3 else 0
    c = _local()
    parts = []
    names = [col.a for col in m.columns]
    old = _row_sql("i")
    new = _row_sql("i", "1")
    nulls = ["CAST(NULL AS %s)" % col.type for col in m.columns]
    u_cols = [col for col in m.unchanged_columns]
    def u_flags(on):
        return ", ".join("%s AS %s" % ("true" if (on and col.name == "l_comment") else "false", col.u) for col in u_cols)
    k_or_b = (lambda vals: ", ".join("%s AS k0" % v for v in vals[:1])) if keyed else (
        lambda vals: ", ".join("%s AS %s" % (v, col.b) for v, col in zip(vals, m.columns)))
    seq = "row_number() OVER () - 1"
    # inserts of new keys
    parts.append("SELECT 0 AS txn, CAST(i AS BIGINT) AS seq, 'c' AS op, %s, %s, %s FROM range(%d, %d) t(i)" % (
        ", ".join("%s AS %s" % (v, a) for v, a in zip(_row_sql("i"), names)), u_flags(False),
        k_or_b(nulls), base, base + ins))
    if upd:
        a = list(new)
        if shape == "unchanged":
            a[-1] = "CAST(NULL AS VARCHAR)"
        parts.append("SELECT 0, CAST(%d + i AS BIGINT), 'u', %s, %s, %s FROM range(0, %d) t(i)" % (
            base + ins, ", ".join(a), u_flags(shape == "unchanged"), k_or_b(nulls if keyed else old), upd))
    if dele:
        a = (["CAST(i AS BIGINT)"] + nulls[1:]) if keyed else nulls
        parts.append("SELECT 0, CAST(%d + i AS BIGINT), 'd', %s, %s, %s FROM range(%d, %d) t(i)" % (
            base + ins + upd, ", ".join(a), u_flags(False), k_or_b(nulls if keyed else old), upd, upd + dele))
    t = c.execute(" UNION ALL ".join(parts)).to_arrow_table()
    c.close()
    return t


def prepare(conn, schema: str, table: str, m: TableMeta, rows: int):
    conn.execute('CREATE SCHEMA IF NOT EXISTS "%s"' % schema)
    conn.execute('CREATE OR REPLACE TABLE "%s"."%s" (%s)' % (schema, table, ", ".join('"%s" %s' % (n, t) for n, t in COLS)))
    conn.execute('INSERT INTO "%s"."%s" SELECT %s FROM range(%d) t(i)' % (schema, table, ", ".join(_row_sql("i")), rows))


def run_perf(conn, sizes=(1, 100, 100_000, 1_000_000), reps_small: int = 5):
    out = {"runs": [], "note": __doc__.strip()}
    for keyed in (True, False):
        m = meta(keyed)
        for shape in (("full", "unchanged") if keyed else ("full",)):
            for n in sizes:
                base = max(3 * n, 1000)
                table = "perf_%s_%s_%d" % ("k" if keyed else "kl", shape, n)
                t0 = time.monotonic()
                prepare(conn, "perf", table, m, base)
                setup = time.monotonic() - t0
                rows = change_set(m, n, base, shape)
                reps = reps_small if n <= 100 else 1
                times, cpus, stmts = [], [], None
                for r in range(reps):
                    if r:
                        prepare(conn, "perf", table, m, base)
                    rss0 = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
                    c0, w0 = time.process_time(), time.monotonic()
                    with StandInTx(conn) as tx:
                        counts = apply_group(tx, Target("perf", table), LocalChangeSet(rows, None, m),
                                             settings=ApplySettings())
                    times.append(time.monotonic() - w0)
                    cpus.append(time.process_time() - c0)
                    stmts = counts.shapes
                got = conn.execute('SELECT count(*) FROM "perf"."%s"' % table).fetchone()[0]
                expect = base + (n - 2 * max(1, n // 3) if n >= 3 else n) - (max(1, n // 3) if n >= 3 else 0)
                times.sort()
                med = times[len(times) // 2]
                out["runs"].append({"keyed": keyed, "shape": shape, "records": n, "table_rows": base,
                                    "seconds_median": round(med, 4), "seconds_all": [round(x, 4) for x in times],
                                    "rows_per_second": round(n / med) if med else None,
                                    "client_cpu_seconds": round(sorted(cpus)[len(cpus) // 2], 4),
                                    "statements": stmts, "row_count_ok": got == expect,
                                    "setup_seconds": round(setup, 2),
                                    "peak_rss_mb_process": round(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 2 ** 20, 1)})
                conn.execute('DROP TABLE "perf"."%s"' % table)
    return out
