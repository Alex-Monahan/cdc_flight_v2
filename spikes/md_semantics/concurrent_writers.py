"""Q4 (SPEC 4.2, 3.3): two processes writing in overlapping transactions on MotherDuck.

Two worker processes (A and B), each with its own connection, execute statements in an order the controller
dictates, so the interleaving is exact. For each case we record, per step, OK (with the rows changed) or the error,
i.e. whether a conflict surfaces at statement time or at COMMIT; then the losing side retries its whole transaction.

Two interleavings per case:
  overlap  A: BEGIN, A: stmt, B: BEGIN, B: stmt, A: COMMIT, B: COMMIT
  stale    A: BEGIN, B: BEGIN, B: snapshot read, A: stmt, A: COMMIT, B: stmt, B: COMMIT
           (B's transaction started before A committed; B writes after A's commit)

Run:  tools/with-env uv run python spikes/md_semantics/concurrent_writers.py
"""
from __future__ import annotations

import multiprocessing as mp

from common import Evidence, connect, create_db, drop_db, err_class

DB = "cdc_v2_spike_md_conc"

SETUP = [
    "CREATE OR REPLACE TABLE t1 (id BIGINT, v BIGINT)",
    "CREATE OR REPLACE TABLE t2 (id BIGINT, v BIGINT)",
    "INSERT INTO t1 SELECT range, 0 FROM range(1000)",
    "INSERT INTO t2 SELECT range, 0 FROM range(1000)",
    "CREATE OR REPLACE TABLE state (stream VARCHAR, pos BIGINT)",
    "INSERT INTO state VALUES ('s1', 0), ('s2', 0)",
    "DROP TABLE IF EXISTS t1_staging", "DROP TABLE IF EXISTS t3", "DROP TABLE IF EXISTS t4",
]

# name -> (A's statements, B's statements)
CASES = {
    "different tables, insert/insert": (["INSERT INTO t1 VALUES (5000, 1)"], ["INSERT INTO t2 VALUES (5000, 2)"]),
    "same table, append/append": (["INSERT INTO t1 VALUES (5000, 1)"], ["INSERT INTO t1 VALUES (5001, 2)"]),
    "same table, update/update different rows": (["UPDATE t1 SET v = 1 WHERE id = 10"],
                                                  ["UPDATE t1 SET v = 2 WHERE id = 11"]),
    "same table, update/update same row": (["UPDATE t1 SET v = 1 WHERE id = 10"],
                                           ["UPDATE t1 SET v = 2 WHERE id = 10"]),
    "same table, delete/update same row": (["DELETE FROM t1 WHERE id = 10"], ["UPDATE t1 SET v = 2 WHERE id = 10"]),
    "same table, delete/delete same row": (["DELETE FROM t1 WHERE id = 10"], ["DELETE FROM t1 WHERE id = 10"]),
    "same table, update/append": (["UPDATE t1 SET v = 1 WHERE id = 10"], ["INSERT INTO t1 VALUES (5001, 2)"]),
    "same table, set-based update/update disjoint halves": (["UPDATE t1 SET v = 1 WHERE id < 500"],
                                                            ["UPDATE t1 SET v = 2 WHERE id >= 500"]),
    "different tables, each also updates its own state row": (
        ["INSERT INTO t1 VALUES (5000, 1)", "UPDATE state SET pos = pos + 1 WHERE stream = 's1'"],
        ["INSERT INTO t2 VALUES (5000, 2)", "UPDATE state SET pos = pos + 1 WHERE stream = 's2'"]),
    "different tables, both update ONE shared state row": (
        ["INSERT INTO t1 VALUES (5000, 1)", "UPDATE state SET pos = pos + 1 WHERE stream = 's1'"],
        ["INSERT INTO t2 VALUES (5000, 2)", "UPDATE state SET pos = pos + 1 WHERE stream = 's1'"]),
    "DDL vs DML: ALTER ADD COLUMN t1 / INSERT t1": (["ALTER TABLE t1 ADD COLUMN extra VARCHAR"],
                                                    ["INSERT INTO t1 VALUES (5001, 2)"]),
    "DDL vs DML: ALTER ADD COLUMN t1 / UPDATE t1": (["ALTER TABLE t1 ADD COLUMN extra VARCHAR"],
                                                    ["UPDATE t1 SET v = 2 WHERE id = 11"]),
    "DML vs DDL: INSERT t1 / ALTER ADD COLUMN t1": (["INSERT INTO t1 VALUES (5001, 2)"],
                                                    ["ALTER TABLE t1 ADD COLUMN extra VARCHAR"]),
    "DDL vs DML on another table: ALTER t1 / INSERT t2": (["ALTER TABLE t1 ADD COLUMN extra VARCHAR"],
                                                          ["INSERT INTO t2 VALUES (5001, 2)"]),
    "DDL vs DDL, different tables": (["ALTER TABLE t1 ADD COLUMN extra VARCHAR"],
                                     ["ALTER TABLE t2 ADD COLUMN extra VARCHAR"]),
    "DDL vs DDL, same table": (["ALTER TABLE t1 ADD COLUMN extra VARCHAR"],
                               ["ALTER TABLE t1 ADD COLUMN extra2 VARCHAR"]),
    "CREATE TABLE vs CREATE TABLE, different names": (["CREATE TABLE t3 (a INT)"], ["CREATE TABLE t4 (a INT)"]),
    "ALTER TYPE vs INSERT same table": (["ALTER TABLE t1 ALTER COLUMN v TYPE HUGEINT"],
                                        ["INSERT INTO t1 VALUES (5001, 2)"]),
    "swap (DROP t1 + RENAME staging) vs INSERT t1": (
        ["DROP TABLE t1", "ALTER TABLE t1_staging RENAME TO t1"], ["INSERT INTO t1 VALUES (5001, 2)"]),
    "INSERT t1 vs swap (DROP t1 + RENAME staging)": (
        ["INSERT INTO t1 VALUES (5001, 2)"], ["DROP TABLE t1", "ALTER TABLE t1_staging RENAME TO t1"]),
    "swap of t1 vs INSERT into a different table t2": (
        ["DROP TABLE t1", "ALTER TABLE t1_staging RENAME TO t1"], ["INSERT INTO t2 VALUES (5001, 2)"]),
}


def worker(conn) -> None:
    con = connect(DB)
    while True:
        sql = conn.recv()
        if sql is None:
            break
        try:
            cur = con.execute(sql)
            try:
                r = cur.fetchall()
            except Exception:
                r = None
            conn.send(("OK", r[0][0] if r and len(r[0]) == 1 else r))
        except Exception as e:
            conn.send(("ERROR", err_class(e)))
    con.close()


def main() -> None:
    ev = Evidence("concurrent_writers")
    create_db(DB)
    ctx = mp.get_context("spawn")
    pipes = {}
    procs = []
    for name in ("A", "B"):
        parent, child = ctx.Pipe()
        p = ctx.Process(target=worker, args=(child,))
        p.start()
        pipes[name] = parent
        procs.append(p)

    def do(who, sql):
        pipes[who].send(sql)
        return pipes[who].recv()

    results = {}
    try:
        for case, (sa, sb) in CASES.items():
            for mode in ("overlap", "stale"):
                for s in SETUP:
                    do("A", s)
                if "staging" in " ".join(sa + sb):
                    do("A", "CREATE TABLE t1_staging AS SELECT range AS id, 7 AS v FROM range(2000)")
                steps = []
                if mode == "overlap":
                    plan = [("A", "BEGIN")] + [("A", s) for s in sa] + [("B", "BEGIN")] + [("B", s) for s in sb] + \
                           [("A", "COMMIT"), ("B", "COMMIT")]
                else:
                    plan = [("A", "BEGIN"), ("B", "BEGIN"), ("B", "SELECT count(*) FROM t2")] + \
                           [("A", s) for s in sa] + [("A", "COMMIT")] + [("B", s) for s in sb] + [("B", "COMMIT")]
                failed = {"A": False, "B": False}
                for who, sql in plan:
                    if failed[who]:  # after an error the transaction is aborted; just roll back at its COMMIT
                        if sql == "COMMIT":
                            st = do(who, "ROLLBACK")
                            steps.append((who, "ROLLBACK (after error)", st[0]))
                        continue
                    st = do(who, sql)
                    steps.append((who, sql, ("OK" if st[1] is None or sql in ("BEGIN", "COMMIT") else f"OK rows={st[1]}")
                                  if st[0] == "OK" else st[1]))
                    if st[0] == "ERROR":
                        failed[who] = True
                        if sql == "COMMIT":
                            do(who, "ROLLBACK")
                retry = None
                loser = "B" if failed["B"] else ("A" if failed["A"] else None)
                if loser:
                    stmts = sa if loser == "A" else sb
                    rs = [do(loser, "BEGIN")] + [do(loser, s) for s in stmts] + [do(loser, "COMMIT")]
                    errs = [r[1] for r in rs if r[0] == "ERROR"]
                    if errs:
                        do(loser, "ROLLBACK")
                    retry = "retry OK" if not errs else f"retry failed: {errs[0]}"
                final = {}
                for tname in ("t1", "t2"):
                    st = do("A", f"SELECT count(*), sum(v), max(CASE WHEN id = 10 THEN v END), "
                                 f"count(CASE WHEN id = 10 THEN 1 END) FROM {tname}")
                    final[tname] = st[1]
                final["state"] = do("A", "SELECT list(pos ORDER BY stream) FROM state")[1]
                summary = {"steps": steps, "conflict": loser is not None, "loser": loser, "retry": retry,
                           "final_after_retry": final}
                results[f"{case} [{mode}]"] = summary
                ev.log(f"== {case} [{mode}]: {'CONFLICT (' + loser + ' fails)' if loser else 'no conflict'}"
                       f"{'; ' + retry if retry else ''}")
                for s in steps:
                    if s[2] != "OK":
                        ev.log(f"     {s[0]}: {s[1]} -> {s[2]}")
                ev.log(f"     final (count, sum(v), v of id 10, rows with id 10): {final}")
    finally:
        for p in pipes.values():
            p.send(None)
        for p in procs:
            p.join()
        ev.put("cases", results)
        drop_db(DB)
        ev.log("database dropped")
        ev.write()


if __name__ == "__main__":
    main()
