"""Q5 (SPEC 4.2): the leader record as a compare-and-swap, and fencing of a paused-then-resumed old leader.

Part A  N processes (separate connections) read the epoch, meet at a barrier, and all try to raise it at the same
        instant. Variants: autocommit UPDATE ... WHERE epoch = ?; the same inside BEGIN/COMMIT; read-then-update in one
        transaction; INSERT of epoch+1 into a table whose primary key is the epoch. Every round must have exactly one
        winner; we record what the losers observe.
Part B  fencing. An old leader L1 (its own process) runs data transactions that check the epoch (fence UPDATE of the
        leader row, either first or last statement of the transaction). It is SIGSTOPped inside a transaction (or
        between transactions); the newcomer L2 (this process) takes over, killing L1's open server transaction with
        md_interrupt_server_connection() when the takeover hits a conflict, writes under the new epoch, then L1 gets
        SIGCONT. Every write of L1 from the paused transaction on must be rejected, and L1 must step down.
        One trial waits WITHOUT the interrupt, to see whether MotherDuck ever ends the paused transaction itself.

Run:  tools/with-env uv run python spikes/md_semantics/leader.py [--rounds 1000] [--procs 4]
"""
from __future__ import annotations

import argparse
import multiprocessing as mp
import os
import signal
import time
import uuid

from common import Evidence, connect, create_db, drop_db, err_class, dist

DB = "cdc_v2_spike_md_leader"
SERVER_NOW = "uuid_extract_timestamp(uuidv7())"  # evaluated by the server (see clock.py)


def own_connection_id(con) -> str:
    """This connection's MotherDuck client_connection_id: the only active query that contains a fresh marker."""
    m = "cdc_v2_marker_" + uuid.uuid4().hex
    return str(con.execute("SELECT client_connection_id FROM md_active_server_connections() "
                           f"WHERE client_query LIKE '%{m}%'").fetchone()[0])


# ------------------------------------------------------------------------------------------- Part A: CAS

def cas_worker(wid: int, variants: list, b1, b2, q) -> None:
    con = connect(DB)
    me = f"w{wid}"
    for variant, rounds in variants:
        for _ in range(rounds):
            b1.wait(180)
            if variant == "insert_pk":
                e = con.execute("SELECT max(epoch) FROM epochs").fetchone()[0]
            else:
                e = con.execute("SELECT epoch FROM leader").fetchone()[0]
            b2.wait(180)
            t0 = time.time()
            res = None
            try:
                if variant == "autocommit_update":
                    res = con.execute("UPDATE leader SET epoch = epoch + 1, owner = ? WHERE epoch = ?",
                                      [me, e]).fetchone()[0]
                elif variant == "txn_update":
                    con.execute("BEGIN")
                    res = con.execute("UPDATE leader SET epoch = epoch + 1, owner = ? WHERE epoch = ?",
                                      [me, e]).fetchone()[0]
                    con.execute("COMMIT")
                elif variant == "txn_read_then_update":
                    con.execute("BEGIN")
                    e2 = con.execute("SELECT epoch FROM leader").fetchone()[0]
                    res = con.execute("UPDATE leader SET epoch = ?, owner = ? WHERE epoch = ?",
                                      [e2 + 1, me, e2]).fetchone()[0]
                    con.execute("COMMIT")
                elif variant == "insert_pk":
                    res = con.execute("INSERT INTO epochs VALUES (?, ?)", [e + 1, me]).fetchone()[0]
                outcome = "won" if res == 1 else "zero_rows"
            except Exception as ex:
                outcome = "error: " + err_class(ex)
                try:
                    con.execute("ROLLBACK")
                except Exception:
                    pass
            q.put((wid, variant, e, outcome, time.time() - t0))
    con.close()


def part_a(ev: Evidence, procs: int, rounds: int, other_rounds: int) -> None:
    con = connect(DB)
    con.execute("CREATE OR REPLACE TABLE leader (epoch BIGINT, owner VARCHAR)")
    con.execute("INSERT INTO leader VALUES (0, 'init')")
    con.execute("CREATE OR REPLACE TABLE epochs (epoch BIGINT PRIMARY KEY, owner VARCHAR)")
    con.execute("INSERT INTO epochs VALUES (0, 'init')")
    variants = [("autocommit_update", rounds), ("txn_update", other_rounds), ("txn_read_then_update", other_rounds),
                ("insert_pk", other_rounds)]
    ctx = mp.get_context("spawn")
    b1, b2, q = ctx.Barrier(procs + 1), ctx.Barrier(procs + 1), ctx.Queue()
    ps = [ctx.Process(target=cas_worker, args=(w, variants, b1, b2, q)) for w in range(procs)]
    for p in ps:
        p.start()
    summary = {}
    for variant, n in variants:
        stats = {"rounds": n, "exactly_one_winner": 0, "bad_rounds": [], "loser_outcomes": {}, "latency": []}
        for r in range(n):
            b1.wait(180)
            b2.wait(180)
            res = [q.get() for _ in range(procs)]
            winners = [x for x in res if x[3] == "won"]
            if variant == "insert_pk":
                db_e, db_owner = con.execute("SELECT epoch, owner FROM epochs ORDER BY epoch DESC LIMIT 1").fetchone()
                cnt = con.execute("SELECT count(*) FROM epochs WHERE epoch = ?", [res[0][2] + 1]).fetchone()[0]
            else:
                db_e, db_owner = con.execute("SELECT epoch, owner FROM leader").fetchone()
                cnt = 1
            same_start = len({x[2] for x in res}) == 1
            good = (len(winners) == 1 and same_start and db_e == res[0][2] + 1 and db_owner == f"w{winners[0][0]}"
                    and cnt == 1)
            if good:
                stats["exactly_one_winner"] += 1
            elif len(stats["bad_rounds"]) < 20:
                stats["bad_rounds"].append({"round": r, "results": res, "db": (db_e, db_owner, cnt)})
            for x in res:
                stats["latency"].append(x[4])
                if x[3] != "won":
                    stats["loser_outcomes"][x[3]] = stats["loser_outcomes"].get(x[3], 0) + 1
        stats["latency"] = dist(stats["latency"])
        summary[variant] = stats
        ev.log(f"== Part A {variant}: {procs} processes x {n} rounds: exactly one winner in "
               f"{stats['exactly_one_winner']}/{n}; loser outcomes {stats['loser_outcomes']}; "
               f"bad rounds {len(stats['bad_rounds'])}")
        for b in stats["bad_rounds"][:5]:
            ev.log("    bad:", b)
    for p in ps:
        p.join()
    ev.put("part_a", summary)
    con.close()


# ------------------------------------------------------------------------------------------- Part B: fencing

def old_leader(fence: str, touch_shared: bool, conn) -> None:
    con = connect(DB)
    cid = own_connection_id(con)
    e = con.execute("SELECT epoch FROM leader").fetchone()[0]
    n = con.execute("UPDATE leader SET epoch = epoch + 1, owner = 'L1', conn_id = ?, hb = " + SERVER_NOW +
                    " WHERE epoch = ?", [cid, e]).fetchone()[0]
    my = e + 1
    conn.send(("leader", my, n))
    seq = 0
    while True:
        try:
            con.execute("BEGIN")
            if fence == "first":
                if con.execute("UPDATE leader SET hb = " + SERVER_NOW + " WHERE epoch = ? AND owner = 'L1'",
                               [my]).fetchone()[0] != 1:
                    con.execute("ROLLBACK")
                    conn.send(("stepped_down_zero_rows", seq, None))
                    return
            con.execute("INSERT INTO data VALUES (?, 'L1', ?)", [my, seq])
            if touch_shared:
                con.execute("UPDATE shared SET v = v + 1, last = 'L1'")
            conn.send(("in_txn", seq, None))
            time.sleep(0.3)
            if fence == "last":
                if con.execute("UPDATE leader SET hb = " + SERVER_NOW + " WHERE epoch = ? AND owner = 'L1'",
                               [my]).fetchone()[0] != 1:
                    con.execute("ROLLBACK")
                    conn.send(("stepped_down_zero_rows", seq, None))
                    return
            con.execute("COMMIT")
            conn.send(("committed", seq, None))
        except Exception as ex:
            conn.send(("error", seq, err_class(ex)))
            try:
                con.execute("ROLLBACK")
            except Exception:
                pass
            try:
                cur = con.execute("SELECT epoch FROM leader").fetchone()[0]
            except Exception as ex2:
                conn.send(("error_reading_epoch", seq, err_class(ex2)))
                continue
            if cur != my:
                conn.send(("stepped_down_newer_epoch", seq, cur))
                return
        seq += 1


def fencing_trial(con, ctx, fence: str, where: str, takeover: str, touch_shared: bool, max_wait: float) -> dict:
    con.execute("CREATE OR REPLACE TABLE leader (epoch BIGINT, owner VARCHAR, conn_id VARCHAR, hb TIMESTAMPTZ)")
    con.execute("INSERT INTO leader VALUES (0, 'none', NULL, NULL)")
    con.execute("CREATE OR REPLACE TABLE data (epoch BIGINT, writer VARCHAR, seq BIGINT)")
    con.execute("CREATE OR REPLACE TABLE shared (v BIGINT, last VARCHAR)")
    con.execute("INSERT INTO shared VALUES (0, 'none')")
    parent, child = ctx.Pipe()
    p = ctx.Process(target=old_leader, args=(fence, touch_shared, child))
    p.start()
    msgs = []

    def recv(timeout=60):
        if parent.poll(timeout):
            m = parent.recv()
            msgs.append(m)
            return m
        return None

    recv()  # leader
    target = "in_txn" if where == "in_txn" else "committed"
    seen_commits = 0
    stopped_seq = None
    while True:
        m = recv()
        if m is None:
            break
        if m[0] == "committed":
            seen_commits += 1
        if seen_commits >= 3 and m[0] == target:
            os.kill(p.pid, signal.SIGSTOP)
            stopped_seq = m[1]
            break
    t_stop = time.time()
    # newcomer: takeover
    e, old_cid = con.execute("SELECT epoch, conn_id FROM leader").fetchone()
    attempts, errors, interrupted = 0, {}, False
    took = None
    while time.time() - t_stop < max_wait:
        attempts += 1
        try:
            n = con.execute("UPDATE leader SET epoch = epoch + 1, owner = 'L2', conn_id = NULL, hb = " + SERVER_NOW +
                            " WHERE epoch = ?", [e]).fetchone()[0]
            if n == 1:
                took = time.time() - t_stop
                break
            errors["zero_rows"] = errors.get("zero_rows", 0) + 1
            break
        except Exception as ex:
            errors[err_class(ex)] = errors.get(err_class(ex), 0) + 1
            if takeover == "interrupt" and not interrupted:
                con.execute("SELECT md_interrupt_server_connection(?::UUID)", [old_cid]).fetchall()
                interrupted = True
            else:
                time.sleep(0.5 if takeover == "interrupt" else 2.0)
    # new leader writes under its epoch, fenced the same way
    l2_ok, l2_err = 0, {}
    for s in (range(3) if took is not None else []):
        for _ in range(20):
            try:
                con.execute("BEGIN")
                assert con.execute("UPDATE leader SET hb = " + SERVER_NOW + " WHERE epoch = ? AND owner = 'L2'",
                                   [e + 1]).fetchone()[0] == 1
                con.execute("INSERT INTO data VALUES (?, 'L2', ?)", [e + 1, s])
                if touch_shared:
                    con.execute("UPDATE shared SET v = v + 1, last = 'L2'")
                con.execute("COMMIT")
                l2_ok += 1
                break
            except Exception as ex:
                l2_err[err_class(ex)] = l2_err.get(err_class(ex), 0) + 1
                try:
                    con.execute("ROLLBACK")
                except Exception:
                    pass
                if takeover == "interrupt" and old_cid:
                    con.execute("SELECT md_interrupt_server_connection(?::UUID)", [old_cid]).fetchall()
                time.sleep(0.5)
    os.kill(p.pid, signal.SIGCONT)
    t_cont = time.time()
    while time.time() - t_cont < 60:
        m = recv(5)
        if m and m[0].startswith("stepped_down"):
            break
    p.join(5)
    if p.is_alive():
        p.kill()
        p.join()
    l1_after = con.execute("SELECT count(*) FROM data WHERE writer = 'L1' AND seq >= ?", [stopped_seq]).fetchone()[0]
    l1_before = con.execute("SELECT count(*) FROM data WHERE writer = 'L1' AND seq < ?", [stopped_seq]).fetchone()[0]
    l2_rows = con.execute("SELECT count(*) FROM data WHERE writer = 'L2'").fetchone()[0]
    final = con.execute("SELECT epoch, owner FROM leader").fetchone()
    shared = con.execute("SELECT v, last FROM shared").fetchone()
    ok = (l1_after == (1 if where == "between_txns" else 0) and took is not None and final == (e + 1, "L2")
          and l2_rows == 3 and any(m[0].startswith("stepped_down") for m in msgs))
    # between_txns: the transaction with seq == stopped_seq committed before the stop; nothing after it may
    if where == "between_txns":
        l1_after = con.execute("SELECT count(*) FROM data WHERE writer = 'L1' AND seq > ?", [stopped_seq]).fetchone()[0]
        ok = (l1_after == 0 and took is not None and final == (e + 1, "L2") and l2_rows == 3
              and any(m[0].startswith("stepped_down") for m in msgs))
    if takeover == "wait":
        ok = None  # informational: does MotherDuck end a paused client's open transaction by itself?
    after_cont = [m for m in msgs if m[0] not in ("committed", "in_txn", "leader") or (m[1] or 0) >= stopped_seq]
    return {"fence": fence, "stopped": where, "takeover": takeover, "touch_shared": touch_shared,
            "pass": ok, "takeover_after_s": took, "takeover_attempts": attempts, "takeover_errors": errors,
            "interrupt_used": interrupted, "l2_commits": l2_ok, "l2_errors": l2_err,
            "l1_rows_committed_from_paused_txn_on": l1_after, "l1_rows_before_pause": l1_before,
            "final_leader": final, "shared_counter": shared, "l1_messages_from_pause_on": after_cont[-6:]}


def part_b(ev: Evidence, trials: int, wait_trial_s: float) -> None:
    con = connect(DB)
    ctx = mp.get_context("spawn")
    plan = []
    for i in range(trials):
        plan.append(("first", "in_txn", "interrupt", i % 2 == 1))
        plan.append(("last", "in_txn", "interrupt", i % 2 == 1))
        plan.append(("first", "between_txns", "interrupt", False))
    if wait_trial_s > 0:
        plan.append(("first", "in_txn", "wait", False))
    results = []
    for fence, where, takeover, touch in plan:
        r = fencing_trial(con, ctx, fence, where, takeover, touch,
                          max_wait=(wait_trial_s if takeover == "wait" else 60))
        results.append(r)
        verdict = {True: "PASS", False: "FAIL", None: "INFO"}[r["pass"]]
        ev.log(f"== Part B fence={fence} paused={where} takeover={takeover} shared_row={touch}: {verdict}")
        ev.log("    ", {k: v for k, v in r.items() if k not in ("fence", "stopped", "takeover", "touch_shared")})
    ev.put("part_b", results)
    con.close()


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--procs", type=int, default=4)
    ap.add_argument("--rounds", type=int, default=1000)
    ap.add_argument("--other-rounds", type=int, default=300)
    ap.add_argument("--fence-trials", type=int, default=4)
    ap.add_argument("--wait-trial-s", type=float, default=600)
    ap.add_argument("--only", default="")
    a = ap.parse_args()
    ev = Evidence("leader")
    create_db(DB)
    try:
        if a.only in ("", "a"):
            part_a(ev, a.procs, a.rounds, a.other_rounds)
        if a.only in ("", "b"):
            part_b(ev, a.fence_trials, a.wait_trial_s)
    finally:
        drop_db(DB)
        ev.log("database dropped")
        ev.write()


if __name__ == "__main__":
    main()
