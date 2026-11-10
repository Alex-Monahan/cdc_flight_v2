"""Q3 (SPEC 1.3, 5.1, 5.2, 5.3, 5.4): transaction size limits, set-based apply shapes, fixed cost of a commit.

Part 1  one transaction that inserts, then updates, then deletes N rows (default 10M) at lineitem width; per-statement
        and commit duration, client peak memory (RSS), any limit or timeout.
Part 2  statement shapes for set-based apply, from a staged table that is already on the server, at several batch
        sizes: INSERT ... SELECT, DELETE ... USING, UPDATE ... FROM, MERGE (half updates, half inserts),
        DELETE+INSERT upsert, INSERT OR REPLACE (needs a primary key). Rows per second, one transaction each.
Part 3  fixed cost of a small transaction: commits per second, 1-row and 100-row batches sent from the client
        (one INSERT ... VALUES plus a state-row UPDATE per transaction), 1 and 4 connections in parallel.

All numbers are measured from a laptop: server-side work is real, round trips are the laptop's. Indications only.

Run:  tools/with-env uv run python spikes/md_semantics/txn_size.py [--big 10000000] [--sizes 10000,1000000,10000000]
"""
from __future__ import annotations

import argparse
import multiprocessing as mp
import resource
import sys
import time

from common import LINEITEM_DDL, Evidence, connect, create_db, drop_db, err_class, lineitem_select, dist

DB = "cdc_v2_spike_md_txn"


def rss_mb() -> float:
    r = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return r / (1024 * 1024) if sys.platform == "darwin" else r / 1024


def timed(con, sql):
    t0 = time.time()
    r = con.execute(sql).fetchall()
    return time.time() - t0, (r[0][0] if r and len(r[0]) == 1 else None)


def part1(ev: Evidence, con, n: int) -> None:
    ev.log(f"== Part 1: one transaction, INSERT then UPDATE then DELETE of {n:,} rows (lineitem width, 16 columns)")
    con.execute(f"CREATE OR REPLACE TABLE li {LINEITEM_DDL}")
    con.execute("INSERT INTO li " + lineitem_select(10**12, 1000))  # a few rows that stay
    res = {"rows": n}
    t_all = time.time()
    try:
        con.execute("BEGIN")
        res["insert_s"], res["inserted"] = timed(con, "INSERT INTO li " + lineitem_select(0, n))
        res["update_s"], res["updated"] = timed(
            con, f"UPDATE li SET l_quantity = l_quantity + 1, l_comment = 'updated ' || l_comment "
                 f"WHERE l_orderkey < {n}")
        res["delete_s"], res["deleted"] = timed(con, f"DELETE FROM li WHERE l_orderkey < {n}")
        t0 = time.time()
        con.execute("COMMIT")
        res["commit_s"] = time.time() - t0
        res["status"] = "committed"
    except Exception as e:
        res["status"] = "ERROR " + err_class(e)
        try:
            con.execute("ROLLBACK")
        except Exception:
            pass
    res["total_s"] = time.time() - t_all
    res["rows_after"] = con.execute("SELECT count(*) FROM li").fetchone()[0]
    res["client_peak_rss_mb"] = round(rss_mb())
    for k, v in res.items():
        ev.log(f"   {k}: {round(v, 2) if isinstance(v, float) else v}")
    if res.get("status") == "committed":
        ev.log(f"   => {3 * n / res['total_s']:,.0f} row changes per second over the whole transaction")
    ev.put(f"part1_{n}", res)
    # one where the transaction ends with a large net insert (10M rows survive)
    t0 = time.time()
    try:
        con.execute("BEGIN")
        a, _ = timed(con, "INSERT INTO li " + lineitem_select(0, n))
        b, _ = timed(con, f"UPDATE li SET l_tax = l_tax + 0.01 WHERE l_orderkey < {n}")
        t1 = time.time()
        con.execute("COMMIT")
        r2 = {"insert_s": a, "update_s": b, "commit_s": time.time() - t1, "total_s": time.time() - t0}
    except Exception as e:
        r2 = {"status": "ERROR " + err_class(e)}
        try:
            con.execute("ROLLBACK")
        except Exception:
            pass
    ev.log(f"   variant: INSERT {n:,} then UPDATE them, commit (rows survive): "
           f"{ {k: round(v, 2) for k, v in r2.items() if isinstance(v, float)} }")
    ev.put(f"part1_{n}_surviving", r2)
    con.execute("DROP TABLE li")


SHAPES = ["insert_select", "delete_using", "update_from", "merge_half_update_half_insert", "delete_insert_upsert",
          "insert_or_replace_pk"]


def part2(ev: Evidence, con, sizes: list[int], base: int) -> None:
    ev.log(f"== Part 2: apply shapes from a staged table on the server; target holds {base:,} rows; one transaction "
           "per measurement")
    con.execute(f"CREATE OR REPLACE TABLE base AS {lineitem_select(0, base)}")
    res = {}
    for size in sizes:
        # staged batch: keys [base - size/2, base + size/2): half existing, half new
        lo = base - size // 2
        con.execute(f"CREATE OR REPLACE TABLE stg AS {lineitem_select(lo, size)}")
        con.execute("UPDATE stg SET l_comment = 'changed', l_quantity = l_quantity + 1")
        con.execute(f"CREATE OR REPLACE TABLE stg_new AS {lineitem_select(base + 10**9, size)}")  # all new keys
        for shape in SHAPES:
            if shape == "insert_or_replace_pk":
                con.execute(f"CREATE OR REPLACE TABLE tgt (l_orderkey BIGINT PRIMARY KEY, "
                            f"{LINEITEM_DDL.strip()[1:].split(',', 1)[1]}")
                con.execute("INSERT INTO tgt SELECT * FROM base")
            else:
                con.execute("CREATE OR REPLACE TABLE tgt AS SELECT * FROM base")
            sql = {
                "insert_select": ["INSERT INTO tgt SELECT * FROM stg_new"],
                "delete_using": ["DELETE FROM tgt USING stg WHERE tgt.l_orderkey = stg.l_orderkey"],
                "update_from": ["UPDATE tgt SET l_quantity = stg.l_quantity, l_comment = stg.l_comment, "
                                "l_extendedprice = stg.l_extendedprice, l_shipdate = stg.l_shipdate "
                                "FROM stg WHERE tgt.l_orderkey = stg.l_orderkey"],
                "merge_half_update_half_insert": [
                    "MERGE INTO tgt USING stg ON tgt.l_orderkey = stg.l_orderkey "
                    "WHEN MATCHED THEN UPDATE SET l_quantity = stg.l_quantity, l_comment = stg.l_comment "
                    "WHEN NOT MATCHED THEN INSERT *"],
                "delete_insert_upsert": ["DELETE FROM tgt USING stg WHERE tgt.l_orderkey = stg.l_orderkey",
                                         "INSERT INTO tgt SELECT * FROM stg"],
                "insert_or_replace_pk": ["INSERT OR REPLACE INTO tgt SELECT * FROM stg"],
            }[shape]
            t0 = time.time()
            try:
                con.execute("BEGIN")
                for s in sql:
                    con.execute(s)
                con.execute("COMMIT")
                dt = time.time() - t0
                res[f"{shape}@{size}"] = {"seconds": round(dt, 3), "rows_per_s": round(size / dt)}
                ev.log(f"   {shape:32s} batch {size:>10,}: {dt:8.2f} s  {size / dt:>12,.0f} rows/s")
            except Exception as e:
                try:
                    con.execute("ROLLBACK")
                except Exception:
                    pass
                res[f"{shape}@{size}"] = {"error": err_class(e)}
                ev.log(f"   {shape:32s} batch {size:>10,}: ERROR {err_class(e)}")
            con.execute("DROP TABLE tgt")
    con.execute("DROP TABLE IF EXISTS stg")
    con.execute("DROP TABLE IF EXISTS stg_new")
    con.execute("DROP TABLE base")
    ev.put("part2", res)


def tiny_worker(wid: int, batch: int, seconds: float, q) -> None:
    con = connect(DB)
    con.execute(f"CREATE OR REPLACE TABLE tiny_{wid} {LINEITEM_DDL}")
    con.execute(f"CREATE OR REPLACE TABLE tiny_state_{wid} (pos BIGINT)")
    con.execute(f"INSERT INTO tiny_state_{wid} VALUES (0)")
    row = ("({k}, 1, 1, 1, 1.00, 2.00, 0.05, 0.01, 'A', 'F', DATE '1995-01-01', DATE '1995-01-02', "
           "DATE '1995-01-03', 'NONE', 'MAIL', 'a comment of moderate length here')")
    lat = []
    k = 0
    pos = 0
    end = time.time() + seconds
    errs = 0
    while time.time() < end:
        vals = ",".join(row.format(k=k + i) for i in range(batch))
        t0 = time.time()
        try:
            con.execute("BEGIN")
            con.execute(f"INSERT INTO tiny_{wid} VALUES {vals}")
            con.execute(f"UPDATE tiny_state_{wid} SET pos = {pos + 1}")
            con.execute("COMMIT")
            lat.append(time.time() - t0)
            k += batch
            pos += 1
        except Exception:
            errs += 1
            try:
                con.execute("ROLLBACK")
            except Exception:
                pass
    con.execute(f"DROP TABLE tiny_{wid}")
    con.execute(f"DROP TABLE tiny_state_{wid}")
    q.put((wid, pos, lat, errs))
    con.close()


def part3(ev: Evidence, seconds: float) -> None:
    ev.log(f"== Part 3: small transactions sent from the client (INSERT VALUES + state UPDATE + COMMIT), "
           f"{seconds:.0f} s each")
    ctx = mp.get_context("spawn")
    res = {}
    for batch in (1, 100):
        for par in (1, 4):
            q = ctx.Queue()
            ps = [ctx.Process(target=tiny_worker, args=(w, batch, seconds, q)) for w in range(par)]
            for p in ps:
                p.start()
            out = [q.get() for _ in ps]
            for p in ps:
                p.join()
            commits = sum(o[1] for o in out)
            lat = [x for o in out for x in o[2]]
            cps = commits / seconds
            res[f"batch{batch}_conn{par}"] = {"commits_per_s": round(cps, 1), "rows_per_s": round(cps * batch),
                                              "txn_latency_s": dist(lat), "errors": sum(o[3] for o in out)}
            ev.log(f"   batch {batch:>3} rows, {par} connection(s): {cps:6.1f} commits/s, {cps * batch:8.0f} rows/s, "
                   f"txn latency p50 {dist(lat)['p50']:.3f} s p99 {dist(lat)['p99']:.3f} s")
    ev.put("part3", res)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--big", default="10000000", help="comma list of sizes for part 1")
    ap.add_argument("--sizes", default="10000,1000000,10000000")
    ap.add_argument("--base", type=int, default=10_000_000)
    ap.add_argument("--tiny-seconds", type=float, default=60)
    ap.add_argument("--only", default="")
    a = ap.parse_args()
    ev = Evidence("txn_size" + ("_" + a.only if a.only else ""))
    create_db(DB)
    con = connect(DB)
    try:
        if a.only in ("", "1"):
            for n in [int(x) for x in a.big.split(",")]:
                part1(ev, con, n)
        if a.only in ("", "2"):
            part2(ev, con, [int(x) for x in a.sizes.split(",")], a.base)
        if a.only in ("", "3"):
            part3(ev, a.tiny_seconds)
    finally:
        con.close()
        drop_db(DB)
        ev.log("database dropped")
        ev.write()


if __name__ == "__main__":
    main()
