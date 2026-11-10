"""Q1 (SPEC 3.2, R7): is a staged load + swap inside one transaction atomic for concurrent readers on MotherDuck?

A writer process swaps a new version of table t in, over and over, with one of several swap variants. Reader
processes (separate processes, separate connections) poll count(*) and a checksum of t as fast as they can and
classify every observation: ok (one complete version), missing table, empty, partial/mixed, other error. Readers also
check that versions never go backwards. Further tests: a reader inside its own transaction across swaps, a long scan
across swaps, and rollback of DDL.

Version v of t holds n(v) rows (id 0..n-1, ver = v); a complete version has count = n(min ver), min ver = max ver and
sum(id) = n(n-1)/2.

Run:  tools/with-env uv run python spikes/md_semantics/swap.py [--swaps 1000] [--variant-swaps 300]
"""
from __future__ import annotations

import argparse
import multiprocessing as mp
import time

from common import Evidence, connect, create_db, drop_db, err_class, dist

DB = "cdc_v2_spike_md_swap"


def n_rows(v: int) -> int:
    return 1000 + (v * 37) % 1000


def classify(row) -> tuple[str, int | None]:
    cnt, s, vmin, vmax = row
    if cnt == 0:
        return "empty", None
    if vmin != vmax:
        return "mixed_versions", None
    n = n_rows(vmin)
    if cnt != n or s != n * (n - 1) // 2:
        return "partial", vmin
    return "ok", vmin


def stage_sql(v: int, name: str = "t_staging", extra_col: bool = False) -> str:
    extra = ", 'x' || range AS extra" if extra_col else ""
    return f"CREATE TABLE {name} AS SELECT range AS id, {v} AS ver{extra} FROM range({n_rows(v)})"


def swap_sql(variant: str) -> list[str]:
    if variant in ("drop_rename", "drop_rename_new_schema"):
        return ["BEGIN", "DROP TABLE t", "ALTER TABLE t_staging RENAME TO t", "COMMIT"]
    if variant == "create_or_replace":
        return ["BEGIN", "CREATE OR REPLACE TABLE t AS SELECT * FROM t_staging", "DROP TABLE t_staging", "COMMIT"]
    if variant == "rename_away":
        return ["BEGIN", "ALTER TABLE t RENAME TO t_old", "ALTER TABLE t_staging RENAME TO t", "DROP TABLE t_old",
                "COMMIT"]
    if variant == "drop_rename_autocommit":  # control: no transaction around the two statements
        return ["DROP TABLE t", "ALTER TABLE t_staging RENAME TO t"]
    raise ValueError(variant)


def writer(variant: str, swaps: int, start_v: int, q: mp.Queue, stop: mp.Event) -> None:
    con = connect(DB)
    errors: dict[str, int] = {}
    durations = []
    done = 0
    for i in range(swaps):
        v = start_v + i
        try:
            con.execute(stage_sql(v, extra_col=(variant == "drop_rename_new_schema" and v % 2 == 1)))
            t0 = time.time()
            for stmt in swap_sql(variant):
                con.execute(stmt)
            durations.append(time.time() - t0)
            done += 1
        except Exception as e:  # record and continue: a failed swap must leave t intact
            errors[err_class(e)] = errors.get(err_class(e), 0) + 1
            try:
                con.execute("ROLLBACK")
            except Exception:
                pass
            try:
                con.execute("DROP TABLE IF EXISTS t_staging")
            except Exception:
                pass
    stop.set()
    q.put({"role": "writer", "swaps_done": done, "errors": errors, "swap_txn_seconds": dist(durations),
           "last_v": start_v + swaps - 1})
    con.close()


def reader(rid: int, q: mp.Queue, stop: mp.Event) -> None:
    con = connect(DB)
    counts: dict[str, int] = {}
    errors: dict[str, int] = {}
    backwards = 0
    last_v = -1
    versions = set()
    lat = []
    while not stop.is_set():
        t0 = time.time()
        try:
            row = con.execute("SELECT count(*), coalesce(sum(id), 0), min(ver), max(ver) FROM t").fetchone()
            lat.append(time.time() - t0)
            cls, v = classify(row)
            counts[cls] = counts.get(cls, 0) + 1
            if v is not None:
                versions.add(v)
                if v < last_v:
                    backwards += 1
                last_v = max(last_v, v)
        except Exception as e:
            msg = err_class(e)
            cls = "missing_table" if "does not exist" in msg else "error"
            counts[cls] = counts.get(cls, 0) + 1
            errors[msg] = errors.get(msg, 0) + 1
    q.put({"role": f"reader{rid}", "observations": counts, "errors": errors, "version_went_backwards": backwards,
           "distinct_versions_seen": len(versions), "poll_seconds": dist(lat)})
    con.close()


def run_variant(ev: Evidence, variant: str, swaps: int, start_v: int, readers: int = 2) -> int:
    con = connect(DB)
    con.execute("DROP TABLE IF EXISTS t")
    con.execute("DROP TABLE IF EXISTS t_staging")
    con.execute("DROP TABLE IF EXISTS t_old")
    con.execute(stage_sql(start_v - 1, name="t"))
    con.close()
    ctx = mp.get_context("spawn")
    q, stop = ctx.Queue(), ctx.Event()
    procs = [ctx.Process(target=reader, args=(r, q, stop)) for r in range(readers)]
    for p in procs:
        p.start()
    time.sleep(3)  # readers connected and polling
    t0 = time.time()
    w = ctx.Process(target=writer, args=(variant, swaps, start_v, q, stop))
    w.start()
    results = [q.get() for _ in range(readers + 1)]
    for p in procs + [w]:
        p.join()
    elapsed = time.time() - t0
    total = {}
    for r in results:
        for k, n in r.get("observations", {}).items():
            total[k] = total.get(k, 0) + n
    anomalies = sum(n for k, n in total.items() if k != "ok")
    backwards = sum(r.get("version_went_backwards", 0) for r in results)
    ev.log(f"\n== variant {variant}: {swaps} swaps in {elapsed:.0f} s")
    for r in results:
        ev.log("  ", r)
    ev.log(f"  TOTAL observations {total}; anomalies {anomalies}; backwards {backwards}")
    ev.put(f"variant_{variant}", {"swaps": swaps, "elapsed_s": elapsed, "results": results, "total": total,
                                  "anomalies": anomalies, "backwards": backwards})
    return start_v + swaps


# ------------------------------------------------------------ reader inside its own transaction / long scans

def txn_reader_test(ev: Evidence, rounds: int, start_v: int) -> int:
    """A reader opens a transaction, reads t, waits while swaps commit, reads t again, commits."""
    ctx = mp.get_context("spawn")
    con = connect(DB)
    con.execute("DROP TABLE IF EXISTS t_staging")
    con.execute("CREATE OR REPLACE TABLE t AS SELECT range AS id, %d AS ver FROM range(%d)" % (start_v - 1,
                                                                                            n_rows(start_v - 1)))
    q, stop = ctx.Queue(), ctx.Event()
    w = ctx.Process(target=writer, args=("drop_rename", rounds * 6, start_v, q, stop))
    w.start()
    outcomes: dict[str, int] = {}
    examples = []
    while not stop.is_set():
        try:
            con.execute("BEGIN")
            a = con.execute("SELECT count(*), coalesce(sum(id),0), min(ver), max(ver) FROM t").fetchone()
            time.sleep(1.5)
            try:
                b = con.execute("SELECT count(*), coalesce(sum(id),0), min(ver), max(ver) FROM t").fetchone()
                key = "same_version_again" if a == b else f"different: {classify(a)[0]}->{classify(b)[0]}"
            except Exception as e:
                key = "second_read_error: " + err_class(e)
            try:
                con.execute("COMMIT")
            except Exception as e:
                key += " | commit_error: " + err_class(e)
            outcomes[key] = outcomes.get(key, 0) + 1
            if len(examples) < 5:
                examples.append((a[2], key))
        except Exception as e:
            k = "first_read_error: " + err_class(e)
            outcomes[k] = outcomes.get(k, 0) + 1
            try:
                con.execute("ROLLBACK")
            except Exception:
                pass
    wres = q.get()
    w.join()
    ev.log("\n== reader inside its own transaction across swaps:", outcomes)
    ev.log("   writer:", wres)
    ev.put("txn_reader", {"outcomes": outcomes, "writer": wres, "examples": examples})
    con.close()
    return start_v + rounds * 6


def long_scan_test(ev: Evidence, swaps: int, rows: int, start_v: int, mult: int = 8) -> None:
    """Big version tables; a reader runs a multi-second scan while the writer swaps."""
    con = connect(DB)
    con.execute("DROP TABLE IF EXISTS t_staging")
    big = lambda v, name: (f"CREATE OR REPLACE TABLE {name} AS SELECT range AS id, {v} AS ver, "
                           f"md5(range::VARCHAR) AS pad FROM range({rows})")
    con.execute(big(start_v - 1, "t"))
    ctx = mp.get_context("spawn")
    q, stop = ctx.Queue(), ctx.Event()
    w = ctx.Process(target=_big_writer, args=(swaps, rows, start_v, q, stop))
    w.start()
    scans = []
    outcomes: dict[str, int] = {}
    while not stop.is_set():
        t0 = time.time()
        try:
            r = con.execute(f"SELECT count(*), sum(id), min(ver), max(ver), "
                            f"count(DISTINCT md5(pad || id::VARCHAR || m::VARCHAR)) FROM t, range({mult}) mm(m)").fetchone()
            ok = (r[0] == mult * rows and r[1] == mult * (rows * (rows - 1) // 2) and r[2] == r[3]
                  and r[4] == mult * rows)
            k = "consistent" if ok else f"INCONSISTENT {r}"
        except Exception as e:
            k = "error: " + err_class(e)
        scans.append(time.time() - t0)
        outcomes[k] = outcomes.get(k, 0) + 1
    wres = q.get()
    w.join()
    ev.log(f"\n== long scans ({rows} rows) across swaps: outcomes {outcomes}; scan seconds {dist(scans)}")
    ev.log("   writer:", wres)
    ev.put("long_scan", {"rows": rows, "outcomes": outcomes, "scan_s": dist(scans), "writer": wres})
    con.execute("DROP TABLE IF EXISTS t")
    con.close()


def _big_writer(swaps, rows, start_v, q, stop):
    con = connect(DB)
    errs = {}
    dur = []
    time.sleep(2)
    for i in range(swaps):
        try:
            con.execute(f"CREATE TABLE t_staging AS SELECT range AS id, {start_v + i} AS ver, "
                        f"md5(range::VARCHAR) AS pad FROM range({rows})")
            t0 = time.time()
            for s in swap_sql("drop_rename"):
                con.execute(s)
            dur.append(time.time() - t0)
        except Exception as e:
            errs[err_class(e)] = errs.get(err_class(e), 0) + 1
            try:
                con.execute("ROLLBACK")
            except Exception:
                pass
    stop.set()
    q.put({"swaps": swaps, "errors": errs, "swap_txn_s": dist(dur)})
    con.close()


# ------------------------------------------------------------ rollback of DDL

def rollback_test(ev: Evidence) -> None:
    con = connect(DB)
    other = connect(DB)  # a second connection, to check what is visible outside
    res = {}

    def state():
        t = other.execute("SELECT count(*), min(ver) FROM t").fetchone()
        tabs = other.execute("SELECT table_name, column_count FROM duckdb_tables() "
                             "WHERE database_name = current_database() ORDER BY 1").fetchall()
        cols = other.execute("SELECT column_name, data_type FROM duckdb_columns() WHERE database_name = "
                             "current_database() AND table_name = 't' ORDER BY column_index").fetchall()
        return {"t": t, "tables": tabs, "t_columns": cols}

    for name in ("t", "t_staging", "t_old", "t_new"):
        con.execute(f"DROP TABLE IF EXISTS {name}")
    con.execute("CREATE TABLE t AS SELECT range AS id, 1 AS ver FROM range(%d)" % n_rows(1))
    con.execute(stage_sql(2))
    before = state()
    cases = {
        "drop_rename": ["DROP TABLE t", "ALTER TABLE t_staging RENAME TO t"],
        "create_or_replace": ["CREATE OR REPLACE TABLE t AS SELECT * FROM t_staging"],
        "alter_add_col_and_dml": ["ALTER TABLE t ADD COLUMN extra VARCHAR DEFAULT 'x'", "DELETE FROM t WHERE id < 10",
                                  "UPDATE t SET ver = 99"],
        "alter_type_and_dml": ["ALTER TABLE t ALTER COLUMN ver TYPE BIGINT", "INSERT INTO t VALUES (100000, 5)"],
        "rename_column": ["ALTER TABLE t RENAME COLUMN ver TO version"],
        "create_new_table": ["CREATE TABLE t_new (a INT)", "INSERT INTO t_new VALUES (1)"],
    }
    for name, stmts in cases.items():
        try:
            con.execute("BEGIN")
            for s in stmts:
                con.execute(s)
            mid = state()  # what the other connection sees while the transaction is open
            con.execute("ROLLBACK")
            after = state()
            res[name] = {"restored": after == before, "other_conn_saw_before_state_mid_txn": mid == before}
        except Exception as e:
            res[name] = {"error": err_class(e)}
            try:
                con.execute("ROLLBACK")
            except Exception:
                pass
    ev.log("\n== DDL rollback (state seen from a second connection):")
    for k, v in res.items():
        ev.log("  ", k, v)
    ev.put("rollback", {"before": before, "cases": res})
    con.close()
    other.close()


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--swaps", type=int, default=1000)
    ap.add_argument("--variant-swaps", type=int, default=300)
    ap.add_argument("--long-rows", type=int, default=5_000_000)
    ap.add_argument("--long-swaps", type=int, default=15)
    ap.add_argument("--scan-mult", type=int, default=8, help="each long scan processes rows x this")
    ap.add_argument("--only", default="")
    a = ap.parse_args()
    ev = Evidence("swap" + ("_" + a.only.replace(",", "_") if a.only else "") + (f"_x{a.scan_mult}" if a.scan_mult != 8 else ""))
    create_db(DB)
    try:
        v = 10
        only = set(a.only.split(",")) if a.only else None
        want = lambda k: only is None or k in only
        if want("rollback"):
            rollback_test(ev)
        if want("drop_rename"):
            v = run_variant(ev, "drop_rename", a.swaps, v)
        for var in ("create_or_replace", "rename_away", "drop_rename_new_schema", "drop_rename_autocommit"):
            if want(var):
                v = run_variant(ev, var, a.variant_swaps, v)
        if want("txn_reader"):
            v = txn_reader_test(ev, 10, v)
        if want("long_scan"):
            long_scan_test(ev, a.long_swaps, a.long_rows, v + 1, a.scan_mult)
    finally:
        drop_db(DB)
        ev.log("database dropped")
        ev.write()


if __name__ == "__main__":
    main()
