"""Scenarios around the cut that are not a standby campaign (docs/design/cut.md): the exported snapshot on a
standby, the gap on a primary, a campaign of cuts taken on a primary, promotion, a primary that crashes after a
commit it had not flushed, and what the stream says about one known transaction.

Each scenario returns a dictionary of plain facts (no host, user or path in it) and prints it as JSON.
Test code: never imported by the engine.
"""

from __future__ import annotations

import json
import random
import threading
import time
from bisect import bisect_left, bisect_right
from collections import Counter
from dataclasses import replace
from pathlib import Path
from typing import Callable, Optional

import psycopg

from cdc_v2 import lsn as L
from cdc_v2.deadline import Deadline, DeadlineExceeded
from cdc_v2.pg import cut as C
from cdc_v2.testing import cutstress as S
from cdc_v2.testing import pgoutput as P

T = S.COUNTER
SIDE = S.SCHEMA + ".side"
INC = "UPDATE %s SET n = n + 1 WHERE k = %%s" % T
GET = "SELECT n FROM %s WHERE k = %%s" % T
FAILOVER_SLOT = "cdc_v2_cut_failover_slot"
INSERT_LSN = "SELECT pg_current_wal_insert_lsn()::text"
FLUSH_LSN = "SELECT pg_current_wal_flush_lsn()::text"
#: The check the coordinator proposed: is a transaction of the snapshot's in-progress list already committed?
STATUS_CHECK = ("SELECT coalesce(bool_or(pg_xact_status(x) = 'committed'), false) "
                "FROM pg_snapshot_xip(pg_current_snapshot()) AS x")


def _lsn(conn, sql: str) -> int:
    return L.parse(conn.execute(sql).fetchone()[0])


def _flush_and_replay(cl: S.Cluster, primary_conn) -> int:
    """Everything the primary has written so far is replayed on the standby."""
    target = _lsn(primary_conn, INSERT_LSN)
    S.wait_replay(cl, target, timeout=30)
    return target


def _version_num(conn) -> int:
    return int(conn.execute(C.SQL_VERSION_NUM).fetchone()[0])


def _key_of(txn: P.Transaction, dec: P.StreamDecoder) -> Optional[int]:
    """The counter key a decoded transaction incremented."""
    names = dec.column_names(T)
    for _lsn_, table, msg in txn.changes:
        if table == T and isinstance(msg, P.Update):
            return int(msg.new[names.index("k")])
    return None


# --------------------------------------------------------------------------------------------------------------
# The exported snapshot on a standby
# --------------------------------------------------------------------------------------------------------------

EXPORT_CASES = {
    "no subtransaction": [INC],
    "top level writes first, then one subtransaction": ["UPDATE %s SET v = v + 1 WHERE k = %%s" % SIDE,
                                                        "SAVEPOINT s", INC],
    "first write inside a subtransaction": ["SAVEPOINT s", INC],
    "65 subtransactions after a top-level write": [
        "UPDATE %s SET v = v + 1 WHERE k = %%s" % SIDE,
        "DO $$ BEGIN FOR j IN 1..65 LOOP BEGIN UPDATE %s SET v = v + 1 WHERE k = 1; "
        "EXCEPTION WHEN division_by_zero THEN NULL; END; END LOOP; END $$" % SIDE, INC],
}
EXPORT_CORRECT = {"importer_before_the_commit_sees": 0, "importer_after_the_commit_sees": 0,
                  "same_importer_after_the_commit_sees": 0, "ordinary_session_after_the_commit_sees": 1}


def export_defect(cl: S.Cluster) -> dict:
    """A transaction is open on the primary and a younger one has committed, so the open one lies between the
    snapshot's xmin and xmax.  The standby exports a snapshot.  Then the open transaction commits.  A session
    that imports the snapshot must not see it; an ordinary session afterwards must see it."""
    keys = list(range(1, 2 + 2 * len(EXPORT_CASES)))
    S.setup(cl, keys, "replica")
    out: dict = {"cases": {}}
    try:
        with cl.connect("primary") as a, cl.connect("primary") as b, cl.connect("replica") as x, \
                cl.connect("replica") as y:
            out["server_version_num"] = _version_num(x)
            out["library_expects_defect"] = C.export_defect(out["server_version_num"])
            out["wal_level"] = a.execute("SHOW wal_level").fetchone()[0]
            floor = C.settle_floor(x)
            for i, (name, statements) in enumerate(EXPORT_CASES.items()):
                res: dict = {}
                for part, key in (("late", 2 + 2 * i), ("early", 3 + 2 * i)):
                    a.execute("BEGIN")
                    for sql in statements:
                        a.execute(sql, (key,) if "%s" in sql else None)
                    b.execute(INC, (1,))         # a younger transaction commits: xmax moves past the open one
                    _flush_and_replay(cl, a)
                    cut = C.take_standby_cut_exported(x, floor, accept_export_defect=True)
                    res["snapshot_" + part] = str(cut.bounds)
                    res["snapshot_%s_is_quiet" % part] = cut.is_quiet
                    if part == "early":          # import and read while the transaction is still running
                        C.import_snapshot(y, cut.snapshot_id)
                        res["importer_before_the_commit_sees"] = y.execute(GET, (key,)).fetchone()[0]
                    a.execute("COMMIT")
                    _flush_and_replay(cl, a)
                    if part == "late":           # import and read after the commit was replayed
                        C.import_snapshot(y, cut.snapshot_id)
                        res["importer_after_the_commit_sees"] = y.execute(GET, (key,)).fetchone()[0]
                    else:
                        res["same_importer_after_the_commit_sees"] = y.execute(GET, (key,)).fetchone()[0]
                        with cl.connect("replica") as fresh:
                            res["ordinary_session_after_the_commit_sees"] = fresh.execute(GET, (key,)).fetchone()[0]
                    C.end_transaction(y)
                    C.end_transaction(x)
                res["wrong"] = sorted(k for k, v in EXPORT_CORRECT.items() if res[k] != v)
                out["cases"][name] = res
        out["correct_answers"] = EXPORT_CORRECT
        out["defect_shown"] = any(c["wrong"] for c in out["cases"].values())
    finally:
        out["slot_dropped"] = S.drop_slot(cl, "replica")
    return out


# --------------------------------------------------------------------------------------------------------------
# Tables without a key on a standby: quiet exported snapshot within a bounded wait, else one reader
# --------------------------------------------------------------------------------------------------------------


def keyless(cl: S.Cluster, quiet_wait_s: float = 1.0) -> dict:
    """The rule for tables without a key (docs/design/cut.md), on whatever server this is.

    While a write transaction is open on the primary no snapshot of the standby is quiet: the wait for a quiet
    exported snapshot ends with a result (not an error) and the library's entry point falls back to one reader
    with its own snapshot.  After the transaction has ended, the same entry point returns a quiet exported
    snapshot, which a second session imports.  On a server without the export defect the entry point returns an
    exported snapshot in both situations.  Every cut the entry point gives out is sealed: it read the standby's
    ``IDENTIFY_SYSTEM`` and start time on a replication connection right after the cut."""
    open_key, other_key = 2, 3
    S.setup(cl, [1, open_key, other_key], "replica")
    out: dict = {"quiet_wait_s": quiet_wait_s}
    try:
        with cl.connect("primary") as a, cl.connect("primary") as b, cl.connect("replica") as x, \
                cl.connect("replica") as y, cl.connect("replica", replication="database") as xr:
            out["server_version_num"] = C.server_version_num(x)
            defect = out["server_has_the_export_defect"] = C.export_defect(out["server_version_num"])
            floor = C.settle_floor(x)
            a.execute("BEGIN")
            a.execute(INC, (open_key,))               # stays open: the standby knows it as running
            open_xid = int(a.execute("SELECT pg_current_xact_id()::text").fetchone()[0])
            b.execute(INC, (other_key,))
            p0 = _flush_and_replay(cl, a)
            t = time.monotonic()
            got = C.take_standby_cut_after(x, xr, p0, Deadline.after(quiet_wait_s), floor, exported=True,
                                           quiet_only=True)
            out["while_a_transaction_is_open"] = {
                "quiet_only_wait_returned": type(got).__name__, "is_false_in_a_condition": not got,
                "seconds": round(time.monotonic() - t, 2),
                "attempts": getattr(got, "attempts", None),
                "oldest_running_is_the_open_transaction": getattr(got, "oldest_running", None) == open_xid,
                "text": str(got) if isinstance(got, C.NoQuietSnapshot) else None}
            if isinstance(got, C.StandbyCut):
                C.end_transaction(x)
            cut = C.take_keyless_standby_cut(x, xr, p0, Deadline.after(60), floor, quiet_wait_s=quiet_wait_s)
            fallback = cut.extra.get("quiet_wait")
            out["keyless_cut_while_open"] = {
                "way": cut.way, "usable_after_p0": cut.usable_after(p0), "snapshot": str(cut.bounds),
                "sealed": cut.sealed,
                "quiet_wait_result_kept": type(fallback).__name__ if fallback is not None else None,
                "the_one_reader_sees_the_open_transaction": x.execute(GET, (open_key,)).fetchone()[0] != 0,
                "the_one_reader_sees_the_committed_one": x.execute(GET, (other_key,)).fetchone()[0] == 1}
            C.end_transaction(x)
            a.execute("COMMIT")
            p1 = _flush_and_replay(cl, a)
            cut = C.take_keyless_standby_cut(x, xr, p1, Deadline.after(60), floor, quiet_wait_s=30.0)
            reader = x
            if cut.way == "exported":
                C.import_snapshot(y, cut.snapshot_id)
                reader = y
            out["keyless_cut_after_the_commit"] = {
                "way": cut.way, "is_quiet": cut.is_quiet, "usable_after_p0": cut.usable_after(p1),
                "snapshot": str(cut.bounds), "sealed": cut.sealed,
                "a_reader_that_imports_it_sees_both": [reader.execute(GET, (k,)).fetchone()[0]
                                                       for k in (open_key, other_key)] == [1, 1]}
            if cut.way == "exported":
                C.end_transaction(y)
            C.end_transaction(x)
        first, second, third = (out[k] for k in ("while_a_transaction_is_open", "keyless_cut_while_open",
                                                 "keyless_cut_after_the_commit"))
        out["ok"] = bool(
            first["quiet_only_wait_returned"] == "NoQuietSnapshot" and first["is_false_in_a_condition"]
            and first["oldest_running_is_the_open_transaction"]
            and second["way"] == ("own" if defect else "exported") and second["usable_after_p0"]
            and second["sealed"] and third["sealed"]
            and (second["quiet_wait_result_kept"] == "NoQuietSnapshot") == defect
            and not second["the_one_reader_sees_the_open_transaction"]
            and second["the_one_reader_sees_the_committed_one"]
            and third["way"] == "exported" and (third["is_quiet"] or not defect) and third["usable_after_p0"]
            and third["a_reader_that_imports_it_sees_both"])
    finally:
        out["slot_dropped"] = S.drop_slot(cl, "replica")
    return out


# --------------------------------------------------------------------------------------------------------------
# The gap on a primary
# --------------------------------------------------------------------------------------------------------------


def _read_slot(conn, slot: str, dec: P.StreamDecoder, timeout_s: float = 20) -> list:
    """Transactions the slot delivers within the timeout (at least one, or an empty list)."""
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        txns = list(dec.feed(conn.execute(P.SQL_GET_BINARY_CHANGES, (slot, 1000, S.PUB)).fetchall()))
        if txns:
            return txns
        time.sleep(0.05)
    return []


def _observe(o, txn: P.Transaction, key: int) -> dict:
    """What a session on the primary can know about a transaction the stream has delivered.  The id `marker` is
    assigned first, then one snapshot is taken and everything else is read under it or after it."""
    marker = C.next_xid_marker(o)
    cut = C.take_primary_cut(o)
    try:
        full = C.widen_xid(txn.xid, cut.bounds.xmax)
        fired = o.execute(STATUS_CHECK).fetchone()[0]
        status = o.execute("SELECT pg_xact_status(%s::xid8)", (str(full),)).fetchone()[0]
        visible = o.execute(GET, (key,)).fetchone()[0] == 1
        insert, flush = _lsn(o, INSERT_LSN), _lsn(o, FLUSH_LSN)
        waits = o.execute("SELECT count(*) FROM pg_stat_activity WHERE wait_event = 'SyncRep'").fetchone()[0]
    finally:
        C.end_transaction(o)
    return {"snapshot": str(cut.bounds), "stream_xid": full,
            "in_the_snapshot_in_progress_list": full in cut.bounds.xip,
            "library_verdict": cut.classify(txn.xid).name, "row_visible": visible,
            "pg_xact_status": status, "status_check_discards_the_snapshot": bool(fired),
            "rule_xmin_at_or_after_marker_accepts_the_snapshot": cut.usable_after_xid(marker),
            "commit_record_end_at_or_before_insert_position": txn.e <= insert,
            "commit_record_end_at_or_before_flush_position": txn.e <= flush,
            "sessions_waiting_for_the_synchronous_standby": waits}


def _gap_case(cl: S.Cluster, o, dec: P.StreamDecoder, kind: str, key: int, hold: Callable, release: Callable,
              commit_level: str, hold_s: float) -> dict:
    done = threading.Event()
    errors: list = []
    gid = "cdc_v2_cut_gap_%d" % key

    def committer() -> None:
        try:
            with cl.connect("primary") as w:
                w.execute("SET synchronous_commit = " + commit_level)
                if kind == "prepared":
                    w.execute("COMMIT PREPARED '%s'" % gid)
                else:
                    w.execute("BEGIN")
                    if kind == "subtransaction":
                        w.execute("SAVEPOINT s")
                    w.execute(INC, (key,))
                    w.execute("COMMIT")
        except Exception as exc:
            errors.append(cl.redact(str(exc))[:200])
        done.set()

    res: dict = {"kind": kind, "synchronous_commit": commit_level}
    if kind == "prepared":
        with cl.connect("primary") as w:
            w.execute("SET synchronous_commit = local")
            w.execute("BEGIN")
            w.execute(INC, (key,))
            w.execute("PREPARE TRANSACTION '%s'" % gid)
    hold()
    t = threading.Thread(target=committer, daemon=True)
    t.start()
    t0 = time.monotonic()
    txns = _read_slot(o, S.SLOT, dec)
    if len(txns) != 1 or _key_of(txns[0], dec) != key:
        release()
        t.join(120)
        return {**res, "error": "the slot delivered %d transactions; %s" % (len(txns), errors)}
    txn = txns[0]
    res["stream_delivered_after_s"] = round(time.monotonic() - t0, 3)
    res["commit_returned_when_delivered"] = done.is_set()
    res["at_delivery"] = _observe(o, txn, key)
    time.sleep(hold_s)
    res["held_s"] = hold_s
    res["commit_returned_while_held"] = done.is_set()
    res["while_held"] = _observe(o, txn, key)
    release()
    t.join(120)
    res["commit_returned_after_release"] = done.is_set() and not errors
    res["after_release"] = _observe(o, txn, key)
    if errors:
        res["error"] = errors[0]
    return res


def primary_gap(cl: S.Cluster, hold_s: float = 3.0, kinds: tuple = ("plain", "subtransaction", "prepared")) -> dict:
    """The commit record is written and the slot delivers the transaction, while the transaction is still in
    every new snapshot's in-progress list: for as long as the synchronous standby does not answer."""
    out: dict = {"cases": [], "failover_slot": {}}
    S.setup(cl, list(range(1, 20)), "primary")
    with cl.connect("primary") as o:
        prepared_ok = int(o.execute("SHOW max_prepared_transactions").fetchone()[0]) > 0
        out["server_version_num"] = _version_num(o)

    def pause() -> None:
        with cl.connect("replica") as r:
            r.execute("SELECT pg_wal_replay_pause()")

    def resume() -> None:
        with cl.connect("replica") as r:
            r.execute("SELECT pg_wal_replay_resume()")

    def stop() -> None:
        cl.localpg("kill", "replica")

    def start() -> None:
        cl.localpg("up")

    holds = (("standby stopped", stop, start, "on"), ("standby replay paused", pause, resume, "remote_apply"))
    try:
        cl.localpg("set", "primary", "synchronous_standby_names=*")
        with cl.connect("primary") as o:
            o.execute("SET synchronous_commit = local")
            dec = P.StreamDecoder()
            key = 1
            for how, hold, release, level in holds:
                for kind in kinds:
                    if kind == "prepared" and not prepared_ok:
                        out["cases"].append({"held_by": how, "kind": kind,
                                             "skipped": "max_prepared_transactions is 0"})
                        continue
                    key += 1
                    out["cases"].append({"held_by": how, **_gap_case(cl, o, dec, kind, key, hold, release, level,
                                                                     hold_s)})
            key += 1                                  # a commit that does not wait: no gap to see
            with cl.connect("primary") as w:
                w.execute("SET synchronous_commit = off")
                w.execute(INC, (key,))
            txns = _read_slot(o, S.SLOT, dec)
            out["cases"].append({"held_by": "nothing", "kind": "plain", "synchronous_commit": "off",
                                 "at_delivery": _observe(o, txns[0], key) if txns else None})
            if out["server_version_num"] >= 170000:   # synchronized_standby_slots exists from Postgres 17
                out["failover_slot"] = _failover_slot(cl, o, key + 1, holds)
    finally:
        cl.localpg("up", check=False)
        try:
            resume()
        except psycopg.Error:
            pass
        cl.localpg("unset", "primary", "synchronous_standby_names", "synchronized_standby_slots", check=False)
        out["slots_dropped"] = [S.drop_slot(cl, "primary"), S.drop_slot(cl, "primary", FAILOVER_SLOT)]
    return out


def _failover_slot(cl: S.Cluster, o, key: int, holds: tuple) -> dict:
    """Does `synchronized_standby_slots` keep the stream from running ahead of what snapshots see?  A failover
    slot delivers a transaction only after the named physical slot's standby has received it."""
    res: dict = {}
    phys = o.execute("SELECT slot_name FROM pg_replication_slots WHERE slot_type = 'physical' AND active").fetchone()
    if phys is None:
        return {"skipped": "no active physical slot"}
    o.execute("SELECT pg_create_logical_replication_slot(%s, 'pgoutput', false, false, true)", (FAILOVER_SLOT,))
    cl.localpg("set", "primary", "synchronized_standby_slots=" + phys[0])
    dec = P.StreamDecoder()
    for how, hold, release, level in holds:
        done = threading.Event()

        def committer(k=key, lv=level, ev=done) -> None:
            with cl.connect("primary") as w:
                w.execute("SET synchronous_commit = " + lv)
                w.execute(INC, (k,))
            ev.set()

        hold()
        t = threading.Thread(target=committer, daemon=True)
        t.start()
        time.sleep(1.0)
        case: dict = {"synchronous_commit": level}
        try:
            o.execute("SET statement_timeout = '4s'")
            mine = [x for x in dec.feed(o.execute(P.SQL_GET_BINARY_CHANGES, (FAILOVER_SLOT, 1000, S.PUB)).fetchall())
                    if _key_of(x, dec) == key]
            case.update(failover_slot_delivered_the_transaction=bool(mine), commit_returned=done.is_set())
            if mine:
                case["at_delivery"] = _observe(o, mine[0], key)
        except psycopg.errors.QueryCanceled:
            dec = P.StreamDecoder()
            case.update(failover_slot_delivered_the_transaction=False, slot_read_blocked_until_timeout=True,
                        commit_returned=done.is_set())
        finally:
            o.execute("SET statement_timeout = 0")
        release()
        t.join(120)
        res[how] = case
        key += 1
    return res


# --------------------------------------------------------------------------------------------------------------
# A campaign of cuts taken on a primary
# --------------------------------------------------------------------------------------------------------------

PRIMARY_SOUND = ("exact", "passed_insert", "xmin_ge_x", "xmin_gt_m", "delivered_inside", "gap_tracker",
                 "gap_tracker_blind")
JUDGED = 40     # cuts judged with each mark of the gap tracker
PRIMARY_WRONG = ("later_snapshot", "status_check", "xmax_ge_x", "passed_flush")
WINDOW = 64     # transactions per key checked on each side of what the snapshot saw


class PCut:
    """One cut taken on the primary, with the evidence read under its snapshot."""

    __slots__ = ("t0", "t", "head", "bounds", "fired", "insert", "flush", "vis", "lib_insert")

    def __init__(self, t0, t, head, bounds, fired, insert, flush, vis, lib_insert):
        self.t0, self.t, self.head, self.bounds, self.fired = t0, t, head, bounds, fired    # t0 < snapshot < t
        self.insert, self.flush, self.vis = insert, flush, vis      # positions read by a later statement
        self.lib_insert = lib_insert                                # the library's: read with the snapshot


def _ptaker(cl: S.Cluster, stream: S.Stream, out: list, stop: threading.Event, stats: Counter, sleep_s: float) -> None:
    try:
        with cl.connect("primary") as conn:
            C.set_statement_timeout(conn, 30)
            while not stop.is_set():
                head = stream.count              # transactions the stream had delivered before the snapshot
                t0 = time.monotonic()
                cut = C.take_primary_cut(conn)
                t = time.monotonic()
                try:
                    fired = conn.execute(STATUS_CHECK).fetchone()[0]
                    a, b = conn.execute("SELECT pg_current_wal_insert_lsn()::text, "
                                        "pg_current_wal_flush_lsn()::text").fetchone()
                    vis = dict(conn.execute(S.READ_COUNTERS).fetchall())
                finally:
                    C.end_transaction(conn)
                out.append(PCut(t0, t, head, cut.bounds, bool(fired), L.parse(a), L.parse(b), vis,
                                cut.insert_lsn))
                if sleep_s:
                    stop.wait(sleep_s)
    except Exception as exc:
        stats["taker_error: " + cl.redact(str(exc))[:200]] += 1


def _marker_loop(cl: S.Cluster, stream: S.Stream, markers: list, stop: threading.Event, period: float,
                 stats: Counter) -> None:
    """A marker is what the engine would have at a table's switch: the stream position reached (as a count of
    delivered transactions) and a transaction id assigned after it."""
    try:
        with cl.connect("primary") as conn:
            while not stop.is_set():
                i0 = stream.count
                markers.append((time.monotonic(), i0, C.next_xid_marker(conn)))
                stop.wait(period)
    except Exception as exc:
        stats["marker_error: " + cl.redact(str(exc))[:200]] += 1


def _pause_loop(cl: S.Cluster, stop: threading.Event, period: float, seed: int, stats: Counter) -> None:
    """Hold back replay on the synchronous standby for a moment, again and again: commits that wait for
    remote_apply then stay in the gap that long."""
    rnd = random.Random(seed)
    try:
        with cl.connect("replica") as conn:
            while not stop.wait(period):
                conn.execute("SELECT pg_wal_replay_pause()")
                stop.wait(rnd.uniform(0.05, 0.3))
                conn.execute("SELECT pg_wal_replay_resume()")
                stats["replay_pauses"] += 1
    except Exception as exc:
        stats["pause_error: " + cl.redact(str(exc))[:200]] += 1


def _percentiles(values: list) -> dict:
    if not values:
        return {"count": 0}
    v = sorted(values)
    pick = lambda q: round(v[min(len(v) - 1, int(q * len(v)))], 4)   # noqa: E731
    return {"count": len(v), "p50_s": pick(0.5), "p90_s": pick(0.9), "p99_s": pick(0.99), "max_s": round(v[-1], 4)}


def check_primary(stream: S.Stream, cuts: list, markers: list, ref_xid: int) -> dict:
    """Check every cut against every relevant transaction of the stream (all of it has been delivered)."""
    n = stream.count
    full = [C.widen_xid(x, ref_xid) for x in stream.xid[:n]]
    prefix_max = [0] * (n + 1)                      # largest id among the first i delivered transactions
    for i in range(n):
        prefix_max[i + 1] = max(prefix_max[i], full[i])
    marker_x = [m[2] for m in markers]
    where = {x: i for i, x in enumerate(full)}      # the place of each transaction id in the stream
    violations, wrong, stats = Counter(), Counter(), Counter()
    examples: dict = {}

    def note(table: Counter, rule: str, c: PCut, detail: dict) -> None:
        table[rule] += 1
        examples.setdefault(rule, {"snapshot": str(c.bounds)[:200], **detail})

    def missing(c: PCut, i0: int) -> int:
        """Transactions among the first i0 delivered that the snapshot does not see."""
        return sum(max(0, bisect_left(ix, i0) - c.vis.get(k, 0)) for k, ix in stream.idx.items())

    def all_inside(c: PCut, i0: int) -> bool:
        """The exact rule, from transaction ids alone: each of the first i0 delivered transactions is inside."""
        return prefix_max[i0] < c.bounds.xmax and all(where.get(x, n) >= i0 for x in c.bounds.xip)

    cuts.sort(key=lambda c: c.t)
    for c in cuts:
        cut = C.PrimaryCut(c.bounds, insert_lsn=c.lib_insert)
        stats["cuts"] += 1
        if c.lib_insert is None or c.lib_insert > c.insert:      # the library read its position first
            note(violations, "evidence", c, {"library_insert": str(c.lib_insert), "insert": L.format(c.insert)})
            continue
        stats["in_progress_ids_listed"] += len(c.bounds.xip)
        for k, ix in stream.idx.items():
            v = c.vis.get(k, 0)
            if v > len(ix):
                note(violations, "evidence", c, {"key": k, "seen": v, "delivered": len(ix)})
                continue
            for j in range(max(0, v - WINDOW), min(len(ix), v + WINDOW)):
                want = C.INSIDE if j < v else C.OUTSIDE
                stats["verdicts_checked"] += 1
                if cut.classify_full(full[ix[j]]) is not want:
                    note(violations, "exact", c, {"key": k, "n": j + 1, "seen": v, "xid": full[ix[j]]})
            if v:
                e = stream.e[ix[v - 1]]           # the last transaction of this key the snapshot sees
                stats["passed_rule_checked"] += 1
                if cut.has_passed(e - 1) or e > c.insert:     # "passed" before this transaction is applied
                    note(violations, "passed_insert", c, {"key": k, "e": L.format(e),
                                                          "library_insert": L.format(c.lib_insert)})
                if e > c.flush:
                    note(wrong, "passed_flush", c, {"key": k, "e": L.format(e), "flush": L.format(c.flush)})
        bad = missing(c, c.head)
        if all_inside(c, c.head):
            stats["cuts_the_exact_rule_accepts"] += 1
            if bad:
                note(violations, "delivered_inside", c, {"delivered_but_not_seen": bad})
        else:
            stats["cuts_the_exact_rule_rejects"] += 1
            if not bad:
                stats["complete_cuts_the_exact_rule_rejects"] += 1
        if bad:
            stats["cuts_that_miss_a_delivered_transaction"] += 1
            note(wrong, "later_snapshot", c, {"delivered_but_not_seen": bad})
            if c.fired:
                stats["of_those_discarded_by_the_status_check"] += 1
            else:
                note(wrong, "status_check", c, {"delivered_but_not_seen": bad})
        elif c.fired:
            stats["complete_cuts_discarded_by_the_status_check"] += 1
        m = bisect_right(marker_x, c.bounds.xmin) - 1        # the latest marker the rule xmin >= X accepts
        if m >= 0:
            stats["cuts_with_xmin_at_or_after_a_marker"] += 1
            if missing(c, markers[m][1]):
                note(violations, "xmin_ge_x", c, {"marker": markers[m][2]})
        i0 = bisect_left(prefix_max, c.bounds.xmin) - 1      # the longest stream prefix with every id below xmin
        if i0 > 0 and missing(c, i0):
            note(violations, "xmin_gt_m", c, {"prefix": i0})
        m = bisect_right(marker_x, c.bounds.xmax) - 1        # wrong: only xmax is at or after the marker
        if m >= 0 and missing(c, markers[m][1]):
            note(wrong, "xmax_ge_x", c, {"marker": markers[m][2]})
    times = [c.t for c in cuts]
    waits: dict = {"xmin_ge_x": [], "xmin_gt_m": [], "exact": [], "gap_tracker": []}
    # The library's gap tracker, fed as an engine would feed it: the stream's transactions in order, and a probe
    # with every snapshot taken before the mark.  The mark made at a marker must admit exactly those later cuts
    # that see every transaction delivered before the marker.
    tracker = C.GapTracker()
    fed = probed = 0
    gap_sizes = Counter()
    for t, i0, x in markers:
        for xid in full[fed:i0]:
            tracker.delivered(xid)
        fed = max(fed, i0)
        while probed < len(cuts) and cuts[probed].t < t:
            tracker.probe(C.PrimaryCut(cuts[probed].bounds))
            probed += 1
        mark = tracker.mark()
        gap_sizes[len(mark.gap)] += 1
        judged = 0
        admitted = False
        for c in cuts[bisect_left(times, t):]:
            if c.t0 < t:
                continue                                   # its snapshot may be older than the mark
            judged += 1
            if judged > JUDGED:
                break
            stats["cuts_judged_with_a_gap_mark"] += 1
            admits = mark.admits(C.PrimaryCut(c.bounds))
            if admits and missing(c, i0):
                note(violations, "gap_tracker", c, {"mark": sorted(mark.gap)[:8], "marker": x})
            if admits != all_inside(c, i0):
                note(violations, "gap_tracker", c, {"mark": sorted(mark.gap)[:8], "differs_from_exact_rule": True})
            if not admits:
                stats["cuts_a_gap_mark_refuses"] += 1
            elif not admitted:
                admitted = True
                waits["gap_tracker"].append(c.t - t)
    # A tracker that starts blind at a marker (an engine restart): it knows nothing delivered before, only that
    # those ids are below the marker's id.  Its mark, made five markers later, must not admit a cut that misses
    # a transaction delivered before that mark.
    for m in range(0, len(markers) - 5, 10):
        t1, i1, x1 = markers[m]
        t2, i2, _ = markers[m + 5]
        blind = C.GapTracker(unknown_below=x1)
        for xid in full[i1:i2]:
            blind.delivered(xid)
        for c in cuts[bisect_left(times, t1):bisect_left(times, t2)]:
            if c.t0 >= t1:
                blind.probe(C.PrimaryCut(c.bounds))
        mark = blind.mark()
        stats["blind_marks"] += 1
        if mark.unknown_below is not None:
            stats["blind_marks_still_blind"] += 1
        judged = 0
        for c in cuts[bisect_left(times, t2):]:
            if c.t0 < t2:
                continue
            judged += 1
            if judged > JUDGED:
                break
            if mark.admits(C.PrimaryCut(c.bounds)):
                stats["cuts_a_blind_mark_admits"] += 1
                if missing(c, i2):
                    note(violations, "gap_tracker_blind", c, {"mark": sorted(mark.gap)[:8], "marker": x1})
            else:
                stats["cuts_a_blind_mark_refuses"] += 1
    stats["gap_mark_empty"] = gap_sizes.get(0, 0)
    stats["gap_mark_not_empty"] = sum(v for k, v in gap_sizes.items() if k)
    stats["gap_mark_largest"] = max(gap_sizes) if gap_sizes else 0
    # How long does each rule make a table wait after its switch?
    unanswered = Counter()
    for t, i0, x in markers:
        start = bisect_left(times, t)
        tests = {"xmin_ge_x": lambda c: c.bounds.xmin >= x, "xmin_gt_m": lambda c: c.bounds.xmin > prefix_max[i0],
                 "exact": lambda c: all_inside(c, i0)}
        for rule, accept in tests.items():
            for c in cuts[start:start + 20000]:
                if accept(c):
                    waits[rule].append(c.t - t)
                    break
            else:
                unanswered[rule] += 1
    return {"violations": dict(violations), "wrong_rules": {r: wrong.get(r, 0) for r in PRIMARY_WRONG},
            "examples": examples, "checker": dict(stats),
            "wait_after_a_marker": {r: {**_percentiles(w), "markers_without_an_accepted_cut": unanswered[r]}
                                    for r, w in waits.items()}}


def primary_stress(cl: S.Cluster, label: str = "primary", seconds: float = 20.0,
                   writers: tuple = ("plain", "async", "subxact", "long"), takers: int = 2, sync: str = "off",
                   pause_period: float = 0.0, marker_period: float = 0.02, taker_sleep: float = 0.001,
                   seed: int = 1, log: Callable = lambda text: None) -> dict:
    specs = [S.WriterSpec.parse(w) for w in writers]
    if sum(2 if s.kind == "twophase" else 1 for s in specs) > 4 or takers > 4:
        raise S.StressError("at most 4 writer sessions (a two-phase writer uses 2) and 4 cut takers")
    if sync not in ("off", "on", "remote_apply"):
        raise S.StressError("sync is one of off, on, remote_apply")
    cl.info(refresh=True)
    keys_of = [s.keys(i) for i, s in enumerate(specs)]
    with cl.connect("primary") as conn:
        version = conn.execute("SHOW server_version").fetchone()[0]
        if any(s.kind == "twophase" for s in specs) and int(conn.execute("SHOW max_prepared_transactions")
                                                            .fetchone()[0]) < 8:
            raise S.StressError("two-phase writers need max_prepared_transactions >= 8")
    S.setup(cl, [S.CLOSER_KEY] + [k for ks in keys_of for k in ks], "primary")
    stream, stats, cuts, markers = S.Stream(), Counter(), [], []
    stop_w, stop_t, stop_s = threading.Event(), threading.Event(), threading.Event()
    threads_w = [threading.Thread(target=S.writer_loop, args=(cl, s, ks, stop_w, stats), daemon=True)
                 for s, ks in zip(specs, keys_of)]
    if pause_period:
        threads_w.append(threading.Thread(target=_pause_loop, args=(cl, stop_w, pause_period, seed, stats),
                                          daemon=True))
    threads_t = [threading.Thread(target=_ptaker, args=(cl, stream, cuts, stop_t, stats, taker_sleep), daemon=True)
                 for _ in range(takers)]
    threads_t.append(threading.Thread(target=_marker_loop, args=(cl, stream, markers, stop_t, marker_period, stats),
                                      daemon=True))
    reader = threading.Thread(target=S.stream_loop, args=(cl, "primary", stream, stop_s, 20000), daemon=True)
    result: dict = {"label": label, "server_version": version,
                    "config": {"seconds": seconds, "writers": list(writers), "takers": takers, "sync": sync,
                               "pause_period": pause_period, "marker_period": marker_period, "seed": seed},
                    "started_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}
    problems: list = []
    t0 = time.monotonic()
    try:
        if sync != "off":
            cl.localpg("set", "primary", "synchronous_standby_names=*", "synchronous_commit=" + sync)
        for t in [reader, *threads_w, *threads_t]:
            t.start()
        while time.monotonic() - t0 < seconds and not stream.errors:
            time.sleep(1.0)
            if int(time.monotonic() - t0) % 30 == 0:
                log("%5.0fs cuts=%d transactions=%d" % (time.monotonic() - t0, len(cuts), stream.count))
        stop_w.set()
        for t in threads_w:
            t.join(120)
        with cl.connect("replica") as conn:
            conn.execute("SELECT pg_wal_replay_resume()")
        time.sleep(0.3)
        stop_t.set()
        for t in threads_t:
            t.join(60)
        with cl.connect("primary") as conn:
            conn.execute(S.INC.format(key=S.CLOSER_KEY))
            final = dict(conn.execute(S.READ_COUNTERS).fetchall())
            ref_xid = C.next_xid_marker(conn)
        deadline = time.monotonic() + 120
        while time.monotonic() < deadline and not stream.errors and any(
                len(stream.idx.get(k, ())) < v for k, v in final.items()):
            time.sleep(0.05)
        stop_s.set()
        reader.join(60)
        for k, v in sorted(final.items()):
            if len(stream.idx.get(k, ())) != v:
                problems.append("key %d: %d committed, %d delivered" % (k, v, len(stream.idx.get(k, ()))))
        result["load_seconds"] = round(time.monotonic() - t0, 1)
        result.update(check_primary(stream, cuts, markers, ref_xid))
    except Exception as exc:
        problems.append("run failed: %s: %s" % (type(exc).__name__, cl.redact(str(exc))[:300]))
    finally:
        for ev in (stop_w, stop_t, stop_s):
            ev.set()
        for t in [reader, *threads_w, *threads_t]:
            if t.is_alive():
                t.join(30)
        try:
            with cl.connect("replica") as conn:
                conn.execute("SELECT pg_wal_replay_resume()")
        except psycopg.Error:
            pass
        if sync != "off":
            cl.localpg("unset", "primary", "synchronous_standby_names", "synchronous_commit", check=False)
        result["slot_dropped"] = S.drop_slot(cl, "primary")
    errors = {k: v for k, v in stats.items() if "error" in k}
    result.update(cuts_checked=len(cuts), markers=len(markers), transactions_delivered=stream.count,
                  e_order_violations=stream.stats.get("e_order_violations", 0), stream=dict(stream.stats),
                  activity={k: v for k, v in sorted(stats.items()) if k not in errors},
                  problems=problems + stream.errors + ["%s (x%d)" % kv for kv in errors.items()])
    result.setdefault("violations", {})
    result["ok"] = not result["violations"] and not result["problems"] and not result["e_order_violations"]
    return result


# --------------------------------------------------------------------------------------------------------------
# What the stream says about known transactions
# --------------------------------------------------------------------------------------------------------------


def positions(cl: S.Cluster) -> dict:
    """Transactions committed in a known order, one at a time, with the primary's insert position read after
    each commit.  The same WAL is decoded by a slot on the primary and by a slot on the standby."""
    keys = list(range(1, 12))
    S.setup(cl, keys, "primary")
    S.drop_slot(cl, "replica")
    S.create_slot(cl, "replica")
    out: dict = {}
    try:
        with cl.connect("primary") as a, cl.connect("primary") as b:
            prepared_ok = int(a.execute("SHOW max_prepared_transactions").fetchone()[0]) > 0
            order: list = []                          # (key, what, insert position before, insert position after)

            def commit(conn, key: int, what: str, sql: str = "COMMIT") -> None:
                before = _lsn(a, INSERT_LSN)
                conn.execute(sql)
                order.append((key, what, before, _lsn(a, INSERT_LSN)))

            def small(key: int, what: str = "small") -> None:
                a.execute("BEGIN")
                a.execute(INC, (key,))
                commit(a, key, what)

            small(2)
            b.execute("BEGIN")                        # a big transaction starts first and commits later
            b.execute("INSERT INTO %s.bulk SELECT 3, repeat('x', 200) FROM generate_series(1, 20000)" % S.SCHEMA)
            b.execute(INC, (3,))
            small(4, "small, inside the big one's lifetime")
            commit(b, 3, "big (20000 rows), started before the previous one")
            if prepared_ok:                           # prepared first, finished later, in the other order
                for key in (5, 6):
                    b.execute("BEGIN")
                    b.execute(INC, (key,))
                    b.execute("PREPARE TRANSACTION 'cdc_v2_cut_pos_%d'" % key)
                small(7, "small, after two transactions were prepared")
                commit(a, 6, "prepared second, committed first, by another session", "COMMIT PREPARED 'cdc_v2_cut_pos_6'")
                small(8)
                commit(a, 5, "prepared first, committed last, by another session", "COMMIT PREPARED 'cdc_v2_cut_pos_5'")
            a.execute("BEGIN")
            a.execute("SAVEPOINT s")
            a.execute(INC, (9,))
            a.execute("RELEASE SAVEPOINT s")
            commit(a, 9, "write inside a subtransaction")
            a.execute("SET synchronous_commit = off")
            small(10, "synchronous_commit = off")
            a.execute("SET synchronous_commit = on")
            small(11, "last")
            _flush_and_replay(cl, a)
        seen: dict = {}
        for node in ("primary", "replica"):
            with cl.connect(node) as conn:
                dec = P.StreamDecoder()
                got: list = []
                deadline = time.monotonic() + 30
                while len(got) < len(order) and time.monotonic() < deadline:
                    got += [(_key_of(x, dec), x) for x in _read_slot(conn, S.SLOT, dec, 5)]
                seen[node] = got
                out["e_order_violations_" + node] = dec.e_order_violations
        rows, previous_after, ok = [], 0, True
        for (key, what, before, after), (got_key, txn) in zip(order, seen["primary"]):
            facts = {"key_in_commit_order": key, "key_in_stream_order": got_key, "what": what,
                     "S_begin_final_lsn": L.format(txn.s), "E_commit_end_lsn": L.format(txn.e),
                     "commit_record_bytes": txn.e - txn.s,
                     "insert_position_before_commit <= S": before <= txn.s,
                     "E <= insert_position_after_commit": txn.e <= after,
                     "E == insert_position_after_commit": txn.e == after,
                     "S >= insert_position_after_previous_commit": txn.s >= previous_after}
            ok = ok and key == got_key and before <= txn.s and txn.e <= after and txn.s >= previous_after
            previous_after = after
            rows.append(facts)
        out["transactions"] = rows
        out["stream_order_equals_commit_order"] = [k for k, *_ in order] == [k for k, _ in seen["primary"]]
        out["standby_slot_delivers_the_same_xid_S_E"] = (
            [(k, x.xid, x.s, x.e) for k, x in seen["primary"]] == [(k, x.xid, x.s, x.e) for k, x in seen["replica"]])
        out["ok"] = bool(ok and out["stream_order_equals_commit_order"]
                         and out["standby_slot_delivers_the_same_xid_S_E"] and len(rows) == len(order)
                         and not out["e_order_violations_primary"] and not out["e_order_violations_replica"])
    finally:
        out["slots_dropped"] = [S.drop_slot(cl, "primary"), S.drop_slot(cl, "replica")]
    return out


# --------------------------------------------------------------------------------------------------------------
# Promotion of the standby the cuts are taken on
# --------------------------------------------------------------------------------------------------------------

PROMO_NODE = "cutpromo"


def identify(conn) -> C.TimelineReading:
    """``IDENTIFY_SYSTEM`` on a ``replication=database`` connection: the node's exact timeline and its position."""
    _sysid, tli, pos, _db = conn.execute("IDENTIFY_SYSTEM").fetchone()
    return C.TimelineReading(int(tli), L.parse(pos))


def stream_reading(cl: S.Cluster, stream_node: str) -> C.TimelineReading:
    """What ``valid_on`` of a cut asks for at the swap: ``IDENTIFY_SYSTEM`` on the node the stream reads from,
    read then.  Nothing about the cut's own node is read here: the taker read it right after the cut."""
    with cl.connect(stream_node, replication="database") as s:
        return identify(s)


def _reading_fact(reading) -> Optional[dict]:
    if reading is None:
        return None
    return {"timeline": reading.timeline, "position": L.format(reading.position)}


def promotion(cl: S.Cluster) -> dict:
    """A second standby is added and a cut is taken on it with the library's taker, which reads the node's
    ``IDENTIFY_SYSTEM`` and start time on a replication connection right after the cut and keeps them on the cut
    (the cut is sealed).  The cut is left open.  The stream, on the source, passes it, and with the stream's
    reading made then the cut is valid.  Then the standby is promoted.  What the open cut still reads, what the
    timeline rule says with the stream's reading made now (the stream on the source, or on the promoted node), what
    the library says to a new cut, and what the replay position does.  A cut taken by the one-attempt function,
    which reads nothing after the cut, is the negative control: it is valid nowhere."""
    out: dict = {}
    S.setup(cl, [1, 2], "primary")
    S.drop_slot(cl, "primary")
    try:
        cl.localpg("add-standby", "--name", PROMO_NODE)
        cl.info(refresh=True)
        with cl.connect("primary") as a:
            out["server_version_num"] = _version_num(a)
            a.execute(INC, (1,))
            target = _lsn(a, INSERT_LSN)
            source_timeline = C.timeline(a)           # exact: the source is a primary
        with cl.connect(PROMO_NODE) as held, cl.connect(PROMO_NODE) as other, \
                cl.connect(PROMO_NODE, replication="database") as held_node, cl.connect(PROMO_NODE) as bare_conn:
            floor_held, floor_other = C.settle_floor(held), C.settle_floor(other)
            cut = C.take_standby_cut_after(held, held_node, target, Deadline.after(60), floor_held)
            # one attempt, which reads nothing after the cut (settled or not does not matter for this control)
            bare = C.take_standby_cut_own(bare_conn, C.settle_floor(bare_conn), accept_unsettled=True)
            C.end_transaction(bare_conn)
            with cl.connect("primary") as a:
                a.execute(INC, (2,))                  # the stream goes on: a transaction that ends after R1
                applied = _lsn(a, FLUSH_LSN)
            C.wait_replay_reached(other, applied, Deadline.after(60))
            at_swap = stream_reading(cl, "primary")
            out["open_cut"] = {"r0": L.format(cut.r0), "r1": L.format(cut.r1), "timeline": cut.timeline,
                               "timeline_of_the_source": source_timeline,
                               "server_start_is_the_one_of_its_node": cut.server_start == C.server_start(other),
                               "sealed": cut.sealed,
                               "reading_on_the_cut": _reading_fact(cut.reader),
                               "reading_on_the_cut_is_of_its_server_life": cut.reader_start == cut.server_start,
                               "has_passed": cut.has_passed(applied),
                               "stream_reading_at_the_swap": _reading_fact(at_swap),
                               "valid_at_the_swap": cut.valid_on(at_swap),
                               "valid_without_the_stream_reading": cut.valid_on(None),
                               "one_attempt_cut_sealed": bare.sealed,
                               "one_attempt_cut_valid_at_the_swap": bare.valid_on(at_swap),
                               "reads_before_promotion": held.execute(GET, (1,)).fetchone()[0]}
            cl.localpg("promote", PROMO_NODE)
            deadline = time.monotonic() + 60
            while other.execute("SELECT pg_is_in_recovery()").fetchone()[0] and time.monotonic() < deadline:
                time.sleep(0.1)
            control_timeline = int(other.execute(C.SQL_TIMELINE).fetchone()[0])
            new_timeline = C.timeline(other)          # exact now: the node is a primary
            frozen = other.execute(C.SQL_REPLAY_LSN).fetchone()[0]
            other.execute(INC, (1,))                  # a commit of the promoted node: not in the source's stream
            end = _lsn(other, INSERT_LSN)
            on_source = stream_reading(cl, "primary")          # the stream stays on the source
            on_promoted = stream_reading(cl, PROMO_NODE)       # the stream follows the promotion
            out["after_promotion"] = {
                "in_recovery": other.execute("SELECT pg_is_in_recovery()").fetchone()[0],
                "timeline": new_timeline,
                "timeline_in_the_control_file_right_after": control_timeline,
                "server_start_unchanged": C.server_start(other) == cut.server_start,
                "the_promoted_node_now": _reading_fact(on_promoted),
                "stream_reading_on_the_source": _reading_fact(on_source),
                "the_open_cut_is_valid_with_the_stream_on_the_source": cut.valid_on(on_source),
                "stream_reading_on_the_promoted_node": _reading_fact(on_promoted),
                "the_open_cut_is_valid_with_the_stream_on_the_promoted_node": cut.valid_on(on_promoted),
                "replay_position_before_a_local_commit": frozen,
                "replay_position_after_a_local_commit": other.execute(C.SQL_REPLAY_LSN).fetchone()[0],
                "local_commit_record_ends_after_that_position": end > L.parse(frozen),
                "a_new_snapshot_reads": other.execute(GET, (1,)).fetchone()[0],
                "the_open_cut_still_reads": held.execute(GET, (1,)).fetchone()[0]}
            C.end_transaction(held)
            for key, take in (
                    ("library_on_a_new_cut", lambda: C.take_standby_cut_own(other, floor_other)),
                    ("library_on_a_new_exported_cut",
                     lambda: C.take_standby_cut_exported(other, floor_other, accept_export_defect=True)),
                    ("library_on_a_new_sealed_cut",
                     lambda: C.take_standby_cut_after(other, held_node, cut.r1, Deadline.after(5), floor_other)),
                    ("library_on_a_new_settle_floor", lambda: C.settle_floor(other))):
                try:
                    take()
                    out[key] = "accepted"
                except C.CutError as exc:
                    out[key] = "refused: " + str(exc)
            # the promoted node is a primary of its own timeline: the primary taker waits for the flush and seals
            primary_cut = C.take_primary_cut_after(other, C.next_xid_marker(other), Deadline.after(30))
            C.end_transaction(other)
            other.execute(INC, (1,))                  # its own stream passes the cut
            passed = _lsn(other, FLUSH_LSN)
            own = stream_reading(cl, PROMO_NODE)
            source = stream_reading(cl, "primary")
            out["primary_cut_on_the_promoted_node"] = {
                "timeline": primary_cut.timeline,
                "insert_position": L.format(primary_cut.insert_lsn),
                "sealed": primary_cut.sealed,
                "reading_on_the_cut": _reading_fact(primary_cut.reader),
                "has_passed_before_the_next_commit": primary_cut.has_passed(primary_cut.insert_lsn - 1),
                "has_passed": primary_cut.has_passed(passed),
                "stream_reading_at_the_swap": _reading_fact(own),
                "valid_at_the_swap": primary_cut.valid_on(own),
                "valid_with_the_stream_on_the_source": primary_cut.valid_on(source)}
            promoted = out["primary_cut_on_the_promoted_node"]
            first = out["open_cut"]
            after = out["after_promotion"]
            out["ok"] = (after["the_open_cut_still_reads"] == 1
                         and after["a_new_snapshot_reads"] == 2
                         and all(out[k].startswith("refused") for k in (
                             "library_on_a_new_cut", "library_on_a_new_exported_cut",
                             "library_on_a_new_sealed_cut", "library_on_a_new_settle_floor"))
                         and cut.timeline == source_timeline
                         and first["sealed"] and first["reading_on_the_cut_is_of_its_server_life"]
                         and first["has_passed"] and first["valid_at_the_swap"]
                         and not first["valid_without_the_stream_reading"]
                         and not first["one_attempt_cut_sealed"] and not first["one_attempt_cut_valid_at_the_swap"]
                         and new_timeline > source_timeline
                         and after["server_start_unchanged"]
                         and after["the_open_cut_is_valid_with_the_stream_on_the_source"]
                         and not after["the_open_cut_is_valid_with_the_stream_on_the_promoted_node"]
                         and primary_cut.timeline == new_timeline and promoted["sealed"]
                         and promoted["has_passed"] and not promoted["has_passed_before_the_next_commit"]
                         and promoted["valid_at_the_swap"]
                         and not promoted["valid_with_the_stream_on_the_source"])
    finally:
        cl.localpg("remove", PROMO_NODE, check=False)
        cl.info(refresh=True)
        with cl.connect("primary") as a:
            out["replication_slots_left_on_the_primary"] = [r[0] for r in a.execute(
                "SELECT slot_name FROM pg_replication_slots ORDER BY 1").fetchall()]
    return out


# --------------------------------------------------------------------------------------------------------------
# A primary that crashes after a commit whose record it had not flushed
# --------------------------------------------------------------------------------------------------------------

WAL_WRITER_SLOW = "wal_writer_delay=10s"
#: How long a lost-commit attempt lets the library's seal wait before the kill: short, so that the WAL writer,
#: which sleeps 10 s, does not flush the commit in the meantime.
SEAL_TRY_S = 0.2


def _close_quietly(*conns) -> None:
    for conn in conns:
        try:
            conn.close()
        except Exception:      # the server behind it was killed
            pass


def _lost_commit(cl: S.Cluster, key: int) -> dict:
    """One attempt.  The WAL writer is slow.  A session commits with ``synchronous_commit = off``, a cut is taken
    with the one-attempt function and reads the committed row; the library's seal (it waits until the WAL is
    flushed up to the cut's insert position) is given a short wait; the primary is killed before the WAL writer
    flushes the commit record.  After the restart: what the row reads, whether the cut can still be sealed, and
    what ``valid_on`` says with the stream's reading."""
    case: dict = {}
    cl.localpg("set", "primary", WAL_WRITER_SLOW)
    time.sleep(0.5)
    w, held = cl.connect("primary"), cl.connect("primary")
    sealed = None
    try:
        w.execute("SET synchronous_commit = off")
        case["row_before"] = w.execute(GET, (key,)).fetchone()[0]
        w.execute(INC, (key,))        # wakes the WAL writer if it hibernates; after it, it sleeps its full delay
        time.sleep(0.5)
        w.execute(INC, (key,))        # this commit record stays in memory until the WAL writer wakes up again
        cut = C.take_primary_cut(held)
        case["the_cut_reads"] = held.execute(GET, (key,)).fetchone()[0]
        flushed = _lsn(held, FLUSH_LSN)
        try:
            sealed = C.seal_primary_cut(held, cut, Deadline.after(SEAL_TRY_S))
            seal_before = "sealed"
        except DeadlineExceeded as exc:
            seal_before = "not within %.1f s: %s" % (SEAL_TRY_S, exc.what)
        case["cut"] = {"timeline": cut.timeline, "insert_position": L.format(cut.insert_lsn),
                       "flush_position_read_under_the_cut": L.format(flushed),
                       "wal_not_flushed_when_the_cut_was_taken": flushed < cut.insert_lsn,
                       "sealed_as_taken": cut.sealed,
                       "seal_before_the_kill": seal_before}
        cl.localpg("kill", "primary")
    finally:
        _close_quietly(w, held)
    cl.localpg("up")
    cl.info(refresh=True)
    a = _connect_when_up(cl, "primary")
    try:
        case["after_the_restart_the_row_reads"] = a.execute(GET, (key,)).fetchone()[0]
        timeline_now = C.timeline(a)
        started_now = C.server_start(a)
        committed = 0
        while True:                   # other transactions: their records take the place of the lost ones
            a.execute(INC, (key + 1,))
            committed += 1
            applied = _lsn(a, FLUSH_LSN)
            if applied >= cut.insert_lsn or committed >= 1000:
                break
        try:
            C.seal_primary_cut(a, cut, Deadline.after(5))
            seal_after = "sealed"
        except C.CutError as exc:
            seal_after = "refused: " + str(exc)
    finally:
        _close_quietly(a)
    stream = stream_reading(cl, "primary")
    case["the_commit_was_lost"] = case["after_the_restart_the_row_reads"] < case["the_cut_reads"]
    case["after_the_restart"] = {
        "timeline": timeline_now,
        "transactions_committed": committed,
        "stream_reading_at_the_swap": _reading_fact(stream),
        "same_timeline": stream.timeline == cut.timeline,
        "positions_of_the_cut_reached_again": stream.position >= cut.insert_lsn,
        "server_start_differs": started_now != cut.server_start,
        "has_passed": cut.has_passed(applied),
        "seal_after_the_restart": seal_after,
        "valid_at_the_swap": (sealed or cut).valid_on(stream),
        # the negative control: a reading of the new server life taken as the seal, the start time not compared
        "valid_if_the_server_start_were_not_compared": replace(
            cut, reader=stream, reader_start=cut.server_start).valid_on(stream)}
    with cl.connect("replica") as r:  # the standby receives flushed WAL only: it never had the lost commit
        C.wait_replay_reached(r, applied, Deadline.after(60))
        case["the_standby_reads"] = r.execute(GET, (key,)).fetchone()[0]
    return case


def _attempt_ok(c: dict) -> bool:
    after = c["after_the_restart"]
    if not (after["server_start_differs"] and c["the_standby_reads"] == c["after_the_restart_the_row_reads"]):
        return False
    if c["cut"]["seal_before_the_kill"] == "sealed":     # the WAL was flushed in time: nothing was lost
        return after["valid_at_the_swap"] and c["after_the_restart_the_row_reads"] == c["the_cut_reads"]
    return not after["valid_at_the_swap"] and after["seal_after_the_restart"].startswith("refused")


def _sealed_survives(cl: S.Cluster, key: int) -> dict:
    """The WAL writer is slow.  A session commits twice with ``synchronous_commit = off``.  The library's primary
    taker takes a cut and waits until the WAL is flushed up to the cut's insert position; another session commits
    with ``synchronous_commit = on`` after one second, which flushes it (the heartbeat write does that in the
    pipeline).  Then the primary is killed.  The commits the cut saw were on disk and survive the crash, and the
    sealed cut is valid with the stream's reading after the restart."""
    case: dict = {}
    cl.localpg("set", "primary", WAL_WRITER_SLOW)
    time.sleep(0.5)
    w, held, other = cl.connect("primary"), cl.connect("primary"), cl.connect("primary")
    try:
        x = C.next_xid_marker(other)  # before the writes: this commit flushes, the two below do not
        w.execute("SET synchronous_commit = off")
        case["row_before"] = w.execute(GET, (key,)).fetchone()[0]
        w.execute(INC, (key,))
        time.sleep(0.5)
        w.execute(INC, (key,))
        flushed_before = _lsn(other, FLUSH_LSN)
        timer = threading.Timer(1.0, lambda: other.execute(INC, (key + 1,)))
        t = time.monotonic()
        timer.start()
        try:
            cut = C.take_primary_cut_after(held, x, Deadline.after(30))
        finally:
            timer.join()
        case["taker_waited_s"] = round(time.monotonic() - t, 2)
        case["the_cut_reads"] = held.execute(GET, (key,)).fetchone()[0]
        case["cut"] = {"timeline": cut.timeline, "insert_position": L.format(cut.insert_lsn),
                       "flush_position_before_the_cut": L.format(flushed_before),
                       "wal_not_flushed_before_the_cut": flushed_before < cut.insert_lsn,
                       "sealed": cut.sealed, "reading_on_the_cut": _reading_fact(cut.reader)}
        cl.localpg("kill", "primary")
    finally:
        _close_quietly(w, held, other)
    cl.localpg("up")
    cl.info(refresh=True)
    a = _connect_when_up(cl, "primary")
    try:
        case["after_the_restart_the_row_reads"] = a.execute(GET, (key,)).fetchone()[0]
        started_now = C.server_start(a)
        a.execute(INC, (key + 1,))
        applied = _lsn(a, FLUSH_LSN)
    finally:
        _close_quietly(a)
    stream = stream_reading(cl, "primary")
    case["after_the_restart"] = {"stream_reading_at_the_swap": _reading_fact(stream),
                                 "server_start_differs": started_now != cut.server_start,
                                 "has_passed": cut.has_passed(applied),
                                 "valid_at_the_swap": cut.valid_on(stream)}
    after = case["after_the_restart"]
    # whether the taker had to wait depends on what else flushed the WAL meanwhile: recorded, not required
    case["ok"] = bool(case["cut"]["sealed"]
                      and case["after_the_restart_the_row_reads"] == case["the_cut_reads"] == case["row_before"] + 2
                      and after["server_start_differs"] and after["has_passed"] and after["valid_at_the_swap"])
    return case


def primary_crash(cl: S.Cluster, attempts: int = 3) -> dict:
    """A snapshot on a primary sees a transaction whose commit record is not flushed, and the primary crashes.
    After the restart the transaction does not exist, the timeline is the same, and other records stand at the
    positions the cut recorded.  A stream that applies one of them has "passed" the cut.  The library never gave
    the cut out for use: its seal waits for the flush and is refused in the new server life, so ``valid_on``
    says no.  A cut that the taker sealed before a crash stays valid (``sealed_before_the_crash``).

    The commit is lost only if nothing flushed the WAL in the half second before the kill, so up to ``attempts``
    attempts are made; ``shown`` says whether one lost its commit."""
    out: dict = {"attempts": []}
    S.setup(cl, list(range(1, 2 * attempts + 4)), "primary")
    S.drop_slot(cl, "primary")
    try:
        with cl.connect("primary") as a:
            out["server_version_num"] = _version_num(a)
        for i in range(attempts):
            case = _lost_commit(cl, 1 + 2 * i)
            out["attempts"].append(case)
            if case["the_commit_was_lost"]:
                break
        out["sealed_before_the_crash"] = _sealed_survives(cl, 1 + 2 * attempts)
    finally:
        cl.localpg("up", check=False)
        cl.localpg("unset", "primary", "wal_writer_delay", check=False)
    out["shown"] = bool(out["attempts"]) and out["attempts"][-1]["the_commit_was_lost"]
    out["ok"] = (bool(out["attempts"]) and all(_attempt_ok(c) for c in out["attempts"])
                 and out["sealed_before_the_crash"]["ok"])
    if out["shown"]:
        after = out["attempts"][-1]["after_the_restart"]
        out["ok"] = (out["ok"] and after["same_timeline"] and after["positions_of_the_cut_reached_again"]
                     and after["has_passed"] and after["valid_if_the_server_start_were_not_compared"])
    return out


# --------------------------------------------------------------------------------------------------------------
# A standby that starts from a checkpoint taken while a commit was still waiting
# --------------------------------------------------------------------------------------------------------------

NO_STANDBY = "cdc_v2_cut_no_such_standby"
CONTROL = "SELECT redo_lsn::text, next_xid FROM pg_control_checkpoint()"


def _control_xid(text: str) -> int:
    epoch, _, xid = text.partition(":")
    return (int(epoch) << 32) | int(xid)


def _connect_when_up(cl: S.Cluster, name: str, timeout_s: float = 60):
    deadline = time.monotonic() + timeout_s
    while True:
        try:
            return cl.connect(name)
        except psycopg.OperationalError:
            if time.monotonic() > deadline:
                raise
            time.sleep(0.2)


def _standby_view(conn, floor: C.SettleFloor, txn: P.Transaction, key: int) -> dict:
    """One cut on the standby and what its snapshot reads, next to what the rule says about the transaction.

    ``floor`` is the settle floor of ``conn``, read once when the connection was made, as the pipeline does.
    The first cut is taken with the test switch ``accept_unsettled``, to show what an unsettled cut reads; the
    second one the way the pipeline takes it."""
    cut = C.take_standby_cut_own(conn, floor, accept_unsettled=True)
    try:
        seen = conn.execute(GET, (key,)).fetchone()[0]
        redo, next_xid = conn.execute(CONTROL).fetchone()
    finally:
        C.end_transaction(conn)
    full = C.widen_xid(txn.xid, cut.bounds.xmax)
    try:
        C.take_standby_cut_own(conn, floor)
        C.end_transaction(conn)
        refused = False
    except C.StandbyNotSettled:
        refused = True
    return {"snapshot": str(cut.bounds), "stream_xid": full, "snapshot_reads": seen,
            "commit_record_end_at_or_before_r0": txn.e <= cut.r0,
            "rule_by_position": cut.classify_by_position(txn.e).name,
            "xid_at_or_above_xmax": full >= cut.bounds.xmax,
            "control_file_redo": redo,
            "restart_point_redo_at_or_after_the_end_of_the_commit_record": L.parse(redo) >= txn.e,
            "control_file_next_xid": C.parse_epoch_xid(next_xid),
            "settle_floor_of_the_connection": floor.value,
            "snapshot_xmax_at_or_above_control_file_next_xid": cut.is_settled(C.parse_epoch_xid(next_xid)),
            "snapshot_xmax_at_or_above_the_settle_floor": cut.is_settled(floor.value),
            "library_refuses_a_cut_taken_now": refused}


def standby_start(cl: S.Cluster) -> dict:
    """A transaction has written its commit record and waits for a synchronous standby that does not answer.
    A checkpoint is taken.  The standby we read from (not the synchronous one) makes that checkpoint its restart
    point and is restarted.  Its replay position is then beyond the commit record: does its snapshot see the
    transaction?"""
    out: dict = {}
    key, later_key = 2, 3
    S.setup(cl, [1, key, later_key], "primary")
    done = threading.Event()
    errors: list = []

    def committer() -> None:
        try:
            with cl.connect("primary") as w:
                w.execute(INC, (key,))
        except Exception as exc:
            errors.append(cl.redact(str(exc))[:200])
        done.set()

    t = threading.Thread(target=committer, daemon=True)
    try:
        with cl.connect("primary") as o:
            out["server_version_num"] = _version_num(o)
            o.execute("SET synchronous_commit = local")
            cl.localpg("set", "primary", "synchronous_standby_names=" + NO_STANDBY)
            t.start()
            dec = P.StreamDecoder()
            txns = _read_slot(o, S.SLOT, dec)
            if len(txns) != 1 or _key_of(txns[0], dec) != key:
                return {**out, "error": "the slot delivered %d transactions; %s" % (len(txns), errors)}
            txn = txns[0]
            out["commit_record_end"] = L.format(txn.e)
            out["commit_returned_when_delivered"] = done.is_set()
            S.wait_replay(cl, txn.e, timeout=30)
            with cl.connect("replica") as r:
                out["before_the_restart"] = _standby_view(r, C.settle_floor(r), txn, key)
            cl.boot_sql("primary", "CHECKPOINT")
            redo, next_xid = o.execute(CONTROL).fetchone()
            out["primary_checkpoint"] = {"redo": redo,
                                         "redo_at_or_after_the_end_of_the_commit_record":
                                             L.parse(redo) >= txn.e,
                                         "next_xid": _control_xid(next_xid),
                                         "commit_returned": done.is_set()}
            _flush_and_replay(cl, o)
            cl.boot_sql("replica", "CHECKPOINT")
            cl.localpg("kill", "replica")
            cl.localpg("up")
            with _connect_when_up(cl, "replica") as r:
                floor = C.settle_floor(r)             # once, for all three views on this connection
                out["after_the_restart"] = _standby_view(r, floor, txn, key)
                out["commit_returned_while_the_standby_restarted"] = done.is_set()
                cl.localpg("unset", "primary", "synchronous_standby_names")
                t.join(120)
                out["commit_returned_after_release"] = done.is_set() and not errors
                _flush_and_replay(cl, o)
                out["after_the_commit_returned"] = _standby_view(r, floor, txn, key)
                o.execute(INC, (later_key,))
                _flush_and_replay(cl, o)
                out["after_a_later_commit"] = _standby_view(r, floor, txn, key)
        views = [out[k] for k in ("before_the_restart", "after_the_restart", "after_the_commit_returned",
                                  "after_a_later_commit")]
        out["rule_violated"] = any(v["rule_by_position"] == "INSIDE" and v["snapshot_reads"] != 1 for v in views)
        out["guard_accepts_a_wrong_cut"] = any((v["snapshot_xmax_at_or_above_control_file_next_xid"]
                                                or v["snapshot_xmax_at_or_above_the_settle_floor"]
                                                or not v["library_refuses_a_cut_taken_now"])
                                               and v["rule_by_position"] == "INSIDE" and v["snapshot_reads"] != 1
                                               for v in views)
        out["guard_refuses_a_right_cut_at_the_end"] = views[-1]["library_refuses_a_cut_taken_now"]
        out["ok"] = not errors and views[-1]["snapshot_reads"] == 1
    finally:
        cl.localpg("unset", "primary", "synchronous_standby_names", check=False)
        cl.localpg("up", check=False)
        if t.is_alive():
            t.join(60)
        out["slot_dropped"] = S.drop_slot(cl, "primary")
    return out


# --------------------------------------------------------------------------------------------------------------
# Command line (subcommands of tools/cutstress)
# --------------------------------------------------------------------------------------------------------------


def add_commands(sub) -> None:
    sub.add_parser("export-defect", help="exported snapshot on a standby while a transaction is open").add_argument(
        "--out")
    g = sub.add_parser("primary-gap", help="a commit the stream delivers before snapshots see it")
    g.add_argument("--hold", type=float, default=3.0)
    g.add_argument("--out")
    s = sub.add_parser("primary-stress", help="writers, a slot and cut takers, all on the primary")
    s.add_argument("--label", default="primary")
    s.add_argument("--seconds", type=float, default=20)
    s.add_argument("--writers", type=S._csv, default=("plain", "async", "subxact", "long"))
    s.add_argument("--takers", type=int, default=2)
    s.add_argument("--sync", choices=("off", "on", "remote_apply"), default="off",
                   help="make the standby synchronous at this level")
    s.add_argument("--pause-period", type=float, default=0.0, help="pause replay on the standby this often (s)")
    s.add_argument("--seed", type=int, default=1)
    s.add_argument("--out")
    sub.add_parser("positions", help="known transactions and their positions in the stream").add_argument("--out")
    sub.add_parser("promotion", help="an open cut across a promotion (adds and removes a standby)").add_argument(
        "--out")
    c = sub.add_parser("primary-crash", help="a cut that saw a commit the primary lost in a crash (kills the primary)")
    c.add_argument("--attempts", type=int, default=3)
    c.add_argument("--out")
    sub.add_parser("standby-start", help="a standby restarted from a checkpoint taken while a commit waited"
                   ).add_argument("--out")
    k = sub.add_parser("keyless", help="tables without a key: quiet exported snapshot within a wait, else one reader")
    k.add_argument("--quiet-wait", type=float, default=1.0)
    k.add_argument("--out")


def primary_summary(r: dict) -> str:
    lines = ["%s: %s" % (r["label"], "OK" if r["ok"] else "FAILED"),
             "  Postgres %s, %s s of load, writers %s, %d takers, synchronous standby: %s" % (
                 r["server_version"], r.get("load_seconds"), ",".join(r["config"]["writers"]),
                 r["config"]["takers"], r["config"]["sync"]),
             "  cuts checked: %d   transactions delivered: %d   markers: %d" % (
                 r["cuts_checked"], r["transactions_delivered"], r["markers"]),
             "  violations of the sound rules: %s" % (r["violations"] or "none"),
             "  wrong rules caught: %s" % ", ".join("%s=%d" % kv for kv in r.get("wrong_rules", {}).items())]
    lines += ["  %s: %s" % kv for kv in sorted(r.get("checker", {}).items())]
    lines += ["  wait after a marker, %s: %s" % kv for kv in r.get("wait_after_a_marker", {}).items()]
    lines += ["  PROBLEM: " + p for p in r["problems"]]
    return "\n".join(lines)


def dispatch(cl: S.Cluster, args) -> int:
    if args.cmd == "export-defect":
        result = export_defect(cl)
        ok = result["defect_shown"] == result["library_expects_defect"]
    elif args.cmd == "primary-gap":
        result = primary_gap(cl, hold_s=args.hold)
        ok = not any("error" in c for c in result["cases"])
    elif args.cmd == "primary-stress":
        result = primary_stress(cl, label=args.label, seconds=args.seconds, writers=tuple(args.writers),
                                takers=args.takers, sync=args.sync, pause_period=args.pause_period, seed=args.seed,
                                log=lambda text: print(text, flush=True))
        ok = result["ok"]
    elif args.cmd == "positions":
        result = positions(cl)
        ok = result.get("ok", False)
    elif args.cmd == "promotion":
        result = promotion(cl)
        ok = result.get("ok", False)
    elif args.cmd == "primary-crash":
        result = primary_crash(cl, attempts=args.attempts)
        ok = result.get("ok", False) and result.get("shown", False)
    elif args.cmd == "standby-start":
        result = standby_start(cl)
        ok = result.get("ok", False)
    elif args.cmd == "keyless":
        result = keyless(cl, quiet_wait_s=args.quiet_wait)
        ok = result.get("ok", False)
    else:
        raise SystemExit("unknown command %s" % args.cmd)
    text = cl.redact(json.dumps(result, indent=1, sort_keys=False, default=str))
    if args.out:
        Path(args.out).write_text(text + "\n")
    print(primary_summary(result) if args.cmd == "primary-stress" else text)
    return 0 if ok else 1
