"""Q2 (SPEC 1.1, 1.3): data in several tables plus a position row, committed in one MotherDuck transaction.

Part A  atomicity under a concurrent reader: G writer processes, each owning a group of tables a_g, b_g, state_g,
        commit transactions that insert paired rows into a_g and b_g (amounts +x / -x), update a few existing pairs,
        and advance state_g (pos, rows). Reader processes check, in one statement (one snapshot), the cross-table
        invariant: count(a) = count(b) = state.rows, sum(a.amount) + sum(b.amount) = 0, max(a.id) + 1 = state.rows.
        A second reader checks the same invariant with separate statements inside one transaction.
Part B  visibility: a reader polls state_0.pos as fast as it can; per commit we know when COMMIT was sent and when it
        returned, so we get the delay until another connection sees it, and count polls that started after COMMIT
        returned but still saw an older position (stale reads).
Part C  client crash: a child process opens a transaction, writes a large batch to both tables and the state row,
        and is SIGKILLed either mid-transaction or right after it sent COMMIT (random delay). The parent checks
        the invariant and whether the batch was applied, immediately and again after a pause.

Run:  tools/with-env uv run python spikes/md_semantics/atomic_multi.py [--groups 4] [--commits 1500]
"""
from __future__ import annotations

import argparse
import multiprocessing as mp
import os
import random
import signal
import time

from common import Evidence, connect, create_db, drop_db, err_class, dist

DB = "cdc_v2_spike_md_atomic"

CHECK_ONE = """SELECT (SELECT count(*) FROM a_{g}), (SELECT coalesce(sum(amount),0) FROM a_{g}),
       (SELECT count(*) FROM b_{g}), (SELECT coalesce(sum(amount),0) FROM b_{g}),
       (SELECT coalesce(max(id),-1) FROM a_{g}), s.pos, s.rows FROM state_{g} s"""


def ok(r) -> bool:
    ca, sa, cb, sb, mx, pos, rows = r
    return ca == cb == rows and sa + sb == 0 and mx + 1 == rows


def setup(con, g: int) -> None:
    con.execute(f"CREATE OR REPLACE TABLE a_{g} (id BIGINT, amount BIGINT)")
    con.execute(f"CREATE OR REPLACE TABLE b_{g} (id BIGINT, amount BIGINT)")
    con.execute(f"CREATE OR REPLACE TABLE state_{g} (pos BIGINT, rows BIGINT)")
    con.execute(f"INSERT INTO state_{g} VALUES (0, 0)")


def writer(g: int, commits: int, q: mp.Queue, stop: mp.Event) -> None:
    con = connect(DB)
    rng = random.Random(g)
    rows = 0
    log = []  # (pos, t_commit_sent, t_commit_returned)
    errors: dict[str, int] = {}
    dur = []
    for pos in range(1, commits + 1):
        k = rng.randint(1, 50)
        x = rng.randint(1, 1000)
        t0 = time.time()
        try:
            con.execute("BEGIN")
            con.execute(f"INSERT INTO a_{g} SELECT range, {x} FROM range({rows}, {rows + k})")
            con.execute(f"INSERT INTO b_{g} SELECT range, -{x} FROM range({rows}, {rows + k})")
            if rows > 0:
                m = rng.randint(2, 50)
                con.execute(f"UPDATE a_{g} SET amount = amount + 3 WHERE id % {m} = 0")
                con.execute(f"UPDATE b_{g} SET amount = amount - 3 WHERE id % {m} = 0")
            con.execute(f"UPDATE state_{g} SET pos = {pos}, rows = {rows + k}")
            t1 = time.time()
            con.execute("COMMIT")
            t2 = time.time()
            rows += k
            log.append((pos, t1, t2))
            dur.append(t2 - t0)
        except Exception as e:
            errors[err_class(e)] = errors.get(err_class(e), 0) + 1
            try:
                con.execute("ROLLBACK")
            except Exception:
                pass
    q.put({"role": f"writer{g}", "commits": len(log), "errors": errors, "txn_seconds": dist(dur),
           "commit_log": log if g == 0 else None})
    con.close()


def reader_one_stmt(groups: int, q: mp.Queue, stop: mp.Event) -> None:
    con = connect(DB)
    checks = violations = 0
    errs: dict[str, int] = {}
    examples = []
    while not stop.is_set():
        for g in range(groups):
            try:
                r = con.execute(CHECK_ONE.format(g=g)).fetchone()
                checks += 1
                if not ok(r):
                    violations += 1
                    if len(examples) < 10:
                        examples.append((g, r))
            except Exception as e:
                errs[err_class(e)] = errs.get(err_class(e), 0) + 1
    q.put({"role": "reader_single_statement", "checks": checks, "violations": violations, "examples": examples,
           "errors": errs})
    con.close()


def reader_txn(groups: int, q: mp.Queue, stop: mp.Event) -> None:
    """Same invariant, but read with seven separate statements inside one transaction (snapshot across statements)."""
    con = connect(DB)
    checks = violations = 0
    errs: dict[str, int] = {}
    examples = []
    while not stop.is_set():
        g = checks % groups
        try:
            con.execute("BEGIN")
            ca = con.execute(f"SELECT count(*) FROM a_{g}").fetchone()[0]
            sa = con.execute(f"SELECT coalesce(sum(amount),0) FROM a_{g}").fetchone()[0]
            cb = con.execute(f"SELECT count(*) FROM b_{g}").fetchone()[0]
            sb = con.execute(f"SELECT coalesce(sum(amount),0) FROM b_{g}").fetchone()[0]
            mx = con.execute(f"SELECT coalesce(max(id),-1) FROM a_{g}").fetchone()[0]
            pos, rows = con.execute(f"SELECT pos, rows FROM state_{g}").fetchone()
            con.execute("COMMIT")
            checks += 1
            r = (ca, sa, cb, sb, mx, pos, rows)
            if not ok(r):
                violations += 1
                if len(examples) < 10:
                    examples.append((g, r))
        except Exception as e:
            errs[err_class(e)] = errs.get(err_class(e), 0) + 1
            try:
                con.execute("ROLLBACK")
            except Exception:
                pass
    q.put({"role": "reader_multi_statement_txn", "checks": checks, "violations": violations, "examples": examples,
           "errors": errs})
    con.close()


def reader_visibility(q: mp.Queue, stop: mp.Event) -> None:
    con = connect(DB)
    polls = []  # (t_start, t_end, pos)
    while not stop.is_set():
        t0 = time.time()
        try:
            p = con.execute("SELECT pos FROM state_0").fetchone()[0]
            polls.append((t0, time.time(), p))
        except Exception:
            pass
    q.put({"role": "reader_visibility", "polls": polls})
    con.close()


def part_ab(ev: Evidence, groups: int, commits: int) -> None:
    con = connect(DB)
    for g in range(groups):
        setup(con, g)
    con.close()
    ctx = mp.get_context("spawn")
    q, stop = ctx.Queue(), ctx.Event()
    readers = [ctx.Process(target=reader_one_stmt, args=(groups, q, stop)),
               ctx.Process(target=reader_one_stmt, args=(groups, q, stop)),
               ctx.Process(target=reader_txn, args=(groups, q, stop)),
               ctx.Process(target=reader_visibility, args=(q, stop))]
    for p in readers:
        p.start()
    time.sleep(3)
    t0 = time.time()
    writers = [ctx.Process(target=writer, args=(g, commits, q, stop)) for g in range(groups)]
    for p in writers:
        p.start()
    results = []
    while len([r for r in results if r["role"].startswith("writer")]) < groups:
        results.append(q.get())
    stop.set()
    while len(results) < groups + len(readers):
        results.append(q.get())
    for p in readers + writers:
        p.join()
    elapsed = time.time() - t0
    commit_log = next(r for r in results if r["role"] == "writer0")["commit_log"]
    polls = next(r for r in results if r["role"] == "reader_visibility")["polls"]
    # first poll (by end time) that saw each pos, and stale reads
    seen_end: dict[int, float] = {}
    for ts, te, p in polls:
        for pp in range(p, 0, -1):
            if pp in seen_end:
                break
            seen_end[pp] = te
    returned = {p: t2 for p, t1, t2 in commit_log}
    sent = {p: t1 for p, t1, t2 in commit_log}
    vis_after_return = [seen_end[p] - returned[p] for p in returned if p in seen_end]
    vis_after_sent = [seen_end[p] - sent[p] for p in sent if p in seen_end]
    # stale: poll started after COMMIT of pos p returned, but saw pos < p
    ret_sorted = sorted(returned.items())
    stale = 0
    stale_by = []
    j = 0
    max_returned_before = 0
    events = sorted(polls)
    for ts, te, p in events:
        while j < len(ret_sorted) and ret_sorted[j][1] < ts:
            max_returned_before = max(max_returned_before, ret_sorted[j][0])
            j += 1
        if p < max_returned_before:
            stale += 1
            nxt = min((q for q in returned if q > p), default=None)
            if nxt is not None:
                stale_by.append(ts - returned[nxt])
    total_commits = sum(r["commits"] for r in results if r["role"].startswith("writer"))
    total_checks = sum(r.get("checks", 0) for r in results)
    total_viol = sum(r.get("violations", 0) for r in results)
    ev.log(f"== Part A: {groups} writer groups, {total_commits} commits in {elapsed:.0f} s, "
           f"{total_checks} invariant checks, {total_viol} violations")
    for r in results:
        if r["role"] in ("reader_visibility",):
            continue
        rr = {k: v for k, v in r.items() if k != "commit_log"}
        ev.log("  ", rr)
    ev.log(f"== Part B: visibility on another connection (group 0, {len(commit_log)} commits, {len(polls)} polls; "
           f"poll interval p50 {dist([te - ts for ts, te, _ in polls]).get('p50')})")
    ev.log("   first-seen minus COMMIT returned (s):", dist(vis_after_return))
    ev.log("   first-seen minus COMMIT sent (s):", dist(vis_after_sent))
    ev.log(f"   stale reads (poll started after COMMIT returned, saw older pos): {stale}; "
           f"how long after return: {dist(stale_by)}")
    ev.put("part_a", {"groups": groups, "commits": total_commits, "checks": total_checks, "violations": total_viol,
                      "results": [{k: v for k, v in r.items() if k not in ("commit_log", "polls")} for r in results]})
    ev.put("part_b", {"seen_minus_returned": dist(vis_after_return), "seen_minus_sent": dist(vis_after_sent),
                      "polls": len(polls), "stale_reads": stale, "stale_after_return": dist(stale_by)})


# ------------------------------------------------------------------------------------------ Part C: crashes

BIG = 300_000


def crash_child(trial: int, conn) -> None:
    con = connect(DB)
    s = con.execute("SELECT rows FROM state_c").fetchone()[0]
    con.execute("BEGIN")
    con.execute(f"INSERT INTO a_c SELECT range, {trial} FROM range({s}, {s + BIG})")
    conn.send("in_txn")
    con.execute(f"INSERT INTO b_c SELECT range, -{trial} FROM range({s}, {s + BIG})")
    con.execute(f"UPDATE state_c SET pos = {trial}, rows = {s + BIG}")
    conn.send("pre_commit")
    con.execute("COMMIT")
    conn.send("committed")
    time.sleep(60)


def part_c(ev: Evidence, trials: int) -> None:
    con = connect(DB)
    con.execute("CREATE OR REPLACE TABLE a_c (id BIGINT, amount BIGINT)")
    con.execute("CREATE OR REPLACE TABLE b_c (id BIGINT, amount BIGINT)")
    con.execute("CREATE OR REPLACE TABLE state_c (pos BIGINT, rows BIGINT)")
    con.execute("INSERT INTO state_c VALUES (0, 0)")
    ctx = mp.get_context("spawn")
    rng = random.Random(7)
    outcomes: dict[str, int] = {}
    rows_out = []
    check = CHECK_ONE.format(g="c")
    for trial in range(1, trials + 1):
        mode = "mid_txn" if trial % 4 == 0 else "during_commit"
        delay = rng.uniform(0, 1.5) if mode == "during_commit" else rng.uniform(0, 0.1)
        parent, child = ctx.Pipe()
        p = ctx.Process(target=crash_child, args=(trial, child))
        p.start()
        want = "in_txn" if mode == "mid_txn" else "pre_commit"
        got = None
        committed_msg = False
        while got != want:
            if not parent.poll(120):
                break
            got = parent.recv()
        time.sleep(delay)
        if parent.poll(0):
            committed_msg = parent.recv() == "committed"
        os.kill(p.pid, signal.SIGKILL)
        p.join()
        t_kill = time.time()
        r1 = con.execute(check).fetchone()
        inv_ok = ok(r1)
        landed_after = 0.0 if r1[5] == trial else None
        r2 = r1
        while time.time() - t_kill < 20:  # poll: does an in-flight COMMIT land after the client died?
            r2 = con.execute(check).fetchone()
            inv_ok = inv_ok and ok(r2)
            if landed_after is None and r2[5] == trial:
                landed_after = time.time() - t_kill
            time.sleep(0.2)
        applied1 = r1[5] == trial
        applied2 = r2[5] == trial
        key = (f"{mode}: invariant_ok={inv_ok} applied={applied2} visible_at_first_check={applied1} "
               f"client_saw_commit_ack={committed_msg}")
        outcomes[key] = outcomes.get(key, 0) + 1
        rows_out.append({"trial": trial, "mode": mode, "delay_s": round(delay, 3), "applied_at_first_check": applied1,
                         "applied_after_20s": applied2, "landed_after_kill_s": landed_after,
                         "invariant_ok": inv_ok, "ack_seen": committed_msg})
    ev.log(f"== Part C: SIGKILL of a client inside a {2 * BIG}-row transaction, {trials} trials")
    for k, n in sorted(outcomes.items()):
        ev.log(f"   {n:3d} x {k}")
    landed = [r["landed_after_kill_s"] for r in rows_out if r["landed_after_kill_s"]]
    ev.log("   commit landed this long after the SIGKILL (s), when it landed late:", dist(landed))
    for r in rows_out:
        ev.log("   ", r)
    ev.put("part_c", {"rows_per_txn": 2 * BIG, "outcomes": outcomes, "trials": rows_out})
    con.close()


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--groups", type=int, default=4)
    ap.add_argument("--commits", type=int, default=1500)
    ap.add_argument("--crash-trials", type=int, default=40)
    ap.add_argument("--only", default="")
    a = ap.parse_args()
    ev = Evidence("atomic_multi")
    create_db(DB)
    try:
        if a.only in ("", "ab"):
            part_ab(ev, a.groups, a.commits)
        if a.only in ("", "c"):
            part_c(ev, a.crash_trials)
    finally:
        drop_db(DB)
        ev.log("database dropped")
        ev.write()


if __name__ == "__main__":
    main()
