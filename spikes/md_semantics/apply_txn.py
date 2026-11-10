"""Q3b (SPEC 1.1, 1.3, 5.1-5.3): a realistic apply transaction, measured end to end from the client.

One destination transaction per batch:
    BEGIN
    stage the batch (lineitem width + an _op column; 1/3 inserts, 1/3 updates, 1/3 deletes) from the client
    DELETE FROM tgt USING <batch> WHERE key matches and _op IN ('U','D')
    INSERT INTO tgt SELECT ... FROM <batch> WHERE _op IN ('I','U')
    UPDATE state SET pos = pos + 1
    UPDATE leader SET hb = uuid_extract_timestamp(uuidv7()) WHERE epoch = 1     -- the fence, last statement
    COMMIT
into a 10M-row target, for batch sizes 1 .. 1M and three ways of staging the batch:
    temp     CREATE TEMP TABLE chg AS SELECT * FROM <arrow>      (a temporary table)
    md       CREATE TABLE chg AS SELECT * FROM <arrow>           (a table in the MotherDuck database, dropped in the txn)
    direct   the DELETE and the INSERT scan the registered Arrow table themselves (no staging table)
Reported per transaction: wall seconds split into stage / delete / insert / state+fence / commit, rows per second,
and the client's CPU seconds (user + system of this process, DuckDB threads included) per million rows.
The batch is built locally before the clock starts. The target's row count is verified after every transaction.

Run:  tools/with-env uv run --with pyarrow python spikes/md_semantics/apply_txn.py
Env:  CDC_V2_APPLY_BASE (default 10000000), CDC_V2_APPLY_PLAN (default "1:10,100:10,10000:5,100000:3,1000000:2"),
      CDC_V2_APPLY_MODES (default temp,md,direct)
"""
from __future__ import annotations

import os
import time

import duckdb

from common import Evidence, connect, cpu_seconds, create_db, drop_db, err_class, lineitem_select, dist, \
    cgroup_cpus, peak_rss_mb

DB = "cdc_v2_spike_md_apply"
COLS = ("l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, "
        "l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode, l_comment")


class Keys:
    def __init__(self, base: int):
        self.upd, self.dele, self.ins = 0, base // 2, 10**12

    def batch(self, local, size: int):
        sql = (f"SELECT CASE l_orderkey % 3 WHEN 0 THEN 'I' WHEN 1 THEN 'U' ELSE 'D' END AS _op, "
               f"* REPLACE (CASE l_orderkey % 3 WHEN 0 THEN {self.ins} + l_orderkey WHEN 1 THEN {self.upd} + l_orderkey // 3 "
               f"ELSE {self.dele} + l_orderkey // 3 END AS l_orderkey) FROM ({lineitem_select(0, size)})")
        tbl = local.execute(sql).to_arrow_table()
        n_i = (size + 2) // 3
        n_d = size // 3
        self.upd += size // 3 + 1  # updates use [0, base/2), deletes [base/2, base); never the same key twice
        self.dele += size // 3 + 1
        self.ins += size
        return tbl, n_i - n_d


def one_txn(con, mode: str, tbl) -> dict:
    con.register("chg_arrow", tbl)
    src = "chg_arrow" if mode == "direct" else "chg"
    t = {}
    c0 = cpu_seconds()
    t0 = time.time()
    con.execute("BEGIN")
    if mode == "temp":
        con.execute("CREATE TEMP TABLE chg AS SELECT * FROM chg_arrow")
    elif mode == "md":
        con.execute("CREATE TABLE chg AS SELECT * FROM chg_arrow")
    t1 = time.time()
    deleted = con.execute(f"DELETE FROM tgt USING {src} c WHERE tgt.l_orderkey = c.l_orderkey "
                          f"AND c._op IN ('U', 'D')").fetchone()[0]
    t2 = time.time()
    inserted = con.execute(f"INSERT INTO tgt SELECT {COLS} FROM {src} WHERE _op IN ('I', 'U')").fetchone()[0]
    t3 = time.time()
    con.execute("UPDATE state SET pos = pos + 1")
    if mode in ("temp", "md"):
        con.execute("DROP TABLE chg")
    n = con.execute("UPDATE leader SET hb = uuid_extract_timestamp(uuidv7()) WHERE epoch = 1").fetchone()[0]
    assert n == 1
    t4 = time.time()
    con.execute("COMMIT")
    t5 = time.time()
    cpu = cpu_seconds() - c0
    con.unregister("chg_arrow")
    t.update(total=t5 - t0, stage=t1 - t0, delete=t2 - t1, insert=t3 - t2, state_fence=t4 - t3, commit=t5 - t4,
             cpu=cpu, deleted=deleted, inserted=inserted)
    return t


def main() -> None:
    base = int(os.environ.get("CDC_V2_APPLY_BASE", "10000000"))
    plan = [tuple(int(x) for x in p.split(":")) for p in
            os.environ.get("CDC_V2_APPLY_PLAN", "1:10,100:10,10000:5,100000:3,1000000:2").split(",")]
    modes = os.environ.get("CDC_V2_APPLY_MODES", "temp,md,direct").split(",")
    ev = Evidence(os.environ.get("CDC_V2_APPLY_EVIDENCE_NAME", "apply_txn"))
    ev.put("client_cpus_cgroup", cgroup_cpus())
    create_db(DB)
    con = connect(DB)
    local = duckdb.connect()
    res = {}
    try:
        t0 = time.time()
        con.execute(f"CREATE TABLE tgt AS {lineitem_select(0, base)}")
        con.execute("CREATE TABLE state (pos BIGINT)")
        con.execute("INSERT INTO state VALUES (0)")
        con.execute("CREATE TABLE leader (epoch BIGINT, owner VARCHAR, hb TIMESTAMPTZ)")
        con.execute("INSERT INTO leader VALUES (1, 'apply', NULL)")
        ev.log(f"target of {base:,} lineitem rows created in {time.time() - t0:.1f} s; client CPUs (cgroup) "
               f"{cgroup_cpus()}")
        rtt = []
        for _ in range(30):
            a = time.time()
            con.execute("SELECT count(*) FROM state").fetchall()
            rtt.append(time.time() - a)
        ev.log("round trip of a trivial remote query (s):", dist(rtt))
        ev.put("rtt_s", dist(rtt))
        keys = Keys(base)
        expect = base
        for size, reps in plan:
            for mode in modes:
                runs = []
                err = None
                for _ in range(reps):
                    tbl, delta = keys.batch(local, size)
                    try:
                        runs.append(one_txn(con, mode, tbl))
                        expect += delta
                    except Exception as e:
                        err = err_class(e)
                        try:
                            con.execute("ROLLBACK")
                        except Exception:
                            pass
                        try:
                            con.unregister("chg_arrow")
                        except Exception:
                            pass
                        break
                got = con.execute("SELECT count(*) FROM tgt").fetchone()[0]
                if err:
                    ev.log(f"   {mode:6s} batch {size:>9,}: ERROR {err}")
                    res[f"{mode}@{size}"] = {"error": err}
                    expect = got
                    continue
                med = lambda k: sorted(r[k] for r in runs)[len(runs) // 2]  # noqa: E731
                row = {k: round(med(k), 4) for k in ("total", "stage", "delete", "insert", "state_fence", "commit",
                                                     "cpu")}
                row["rows_per_s"] = round(size / row["total"])
                row["cpu_s_per_million_rows"] = round(row["cpu"] / size * 1e6, 2)
                row["reps"] = len(runs)
                row["count_ok"] = got == expect
                res[f"{mode}@{size}"] = row
                ev.log(f"   {mode:6s} batch {size:>9,}: {row['total']:7.3f} s/txn {row['rows_per_s']:>10,} rows/s | "
                       f"stage {row['stage']:.3f} delete {row['delete']:.3f} insert {row['insert']:.3f} "
                       f"state+fence {row['state_fence']:.3f} commit {row['commit']:.3f} | client CPU "
                       f"{row['cpu']:.3f} s = {row['cpu_s_per_million_rows']} s/M rows | count ok {row['count_ok']}")
        ev.put("client_peak_rss_mb", peak_rss_mb())
        ev.log("client peak RSS MB:", peak_rss_mb())
    finally:
        ev.put("results", res)
        con.close()
        drop_db(DB)
        ev.log("database dropped")
        ev.write()


if __name__ == "__main__":
    main()
