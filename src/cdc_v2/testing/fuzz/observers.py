"""Consumer loops (SPEC 1.3 and 3.2): observers that read the destination on connections of their own, all the
time, each observation in one snapshot (one statement).

Every check is one SQL statement over the destination (DuckDB dialect: MotherDuck or a local file) and a function
that judges its row. A `ConsumerLoop` runs one check in a thread as fast as `min_interval` allows and reports the
number of observations, the longest gap between two successful ones (an observer that looked ten times proves
nothing), the unavailable reads, and on a violation what it saw, as counts and keys (never row values beyond keys).

Checks:

* `transfers`     the global sum of the transfer tables is constant (the invariant of package O's observer).
* `parent_child`  w_parent.n_children equals the number of its children; no child without its parent.
* `debit_credit`  sum(w_debit.amount) = sum(w_credit.amount).
* `large_marked`  each w_bulk batch is visible completely together with its marker row, or not at all.
* `versions`      count and checksum of one table are always one of the versions the test names (SPEC 3.2:
                  never a missing, empty or partial table); a missing table is a violation here.

A missing table is "unavailable" for the first four (the destination may not have it yet).
"""
from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field
from typing import Callable, Optional


def _q(schema: str, table: str) -> str:
    return '"%s"."%s"' % (schema.replace('"', '""'), table.replace('"', '""'))


@dataclass
class Check:
    name: str
    sql: str
    judge: Callable                                       # row -> (ok, detail dict)
    missing_is_violation: bool = False


def transfers(tables: list, expected_sum: int, schema: str = "public") -> Check:
    sql = "SELECT " + " + ".join("(SELECT coalesce(sum(balance), 0) FROM %s)" % _q(schema, t) for t in tables)

    def judge(row):
        v = int(row[0])
        return v == expected_sum, {"sum": v, "expected": expected_sum}
    return Check("transfers", sql, judge)


def parent_child(schema: str = "public", parent: str = "w_parent", child: str = "w_child") -> Check:
    P, C = _q(schema, parent), _q(schema, child)
    sql = ("WITH n AS (SELECT parent_id, count(*) AS k FROM %s GROUP BY parent_id), "
           "bad AS (SELECT p.id, p.n_children, coalesce(n.k, 0) AS k FROM %s p LEFT JOIN n ON n.parent_id = p.id "
           "WHERE p.n_children <> coalesce(n.k, 0)), "
           "orphan AS (SELECT n.parent_id FROM n LEFT JOIN %s p ON p.id = n.parent_id WHERE p.id IS NULL) "
           "SELECT (SELECT count(*) FROM bad), (SELECT count(*) FROM orphan), "
           "(SELECT list([id, n_children, k] ORDER BY id)[1:5] FROM bad), "
           "(SELECT list(parent_id ORDER BY parent_id)[1:5] FROM orphan), (SELECT count(*) FROM %s)"
           % (C, P, P, P))

    def judge(row):
        bad, orphans = int(row[0]), int(row[1])
        return bad == 0 and orphans == 0, {"parents_with_wrong_count": bad, "orphan_parent_ids": orphans,
                                           "sample_parent_id_count_children": row[2] or [],
                                           "sample_orphans": row[3] or [], "parents": int(row[4])}
    return Check("parent_child", sql, judge)


def debit_credit(schema: str = "public", debit: str = "w_debit", credit: str = "w_credit") -> Check:
    sql = "SELECT (SELECT coalesce(sum(amount), 0) FROM %s), (SELECT coalesce(sum(amount), 0) FROM %s), " \
          "(SELECT count(*) FROM %s), (SELECT count(*) FROM %s)" % (_q(schema, debit), _q(schema, credit),
                                                                  _q(schema, debit), _q(schema, credit))

    def judge(row):
        d, c = int(row[0]), int(row[1])
        return d == c, {"debits": d, "credits": c, "debit_rows": int(row[2]), "credit_rows": int(row[3])}
    return Check("debit_credit", sql, judge)


def large_marked(schema: str = "public", bulk: str = "w_bulk", mark: str = "w_bulk_mark") -> Check:
    B, M = _q(schema, bulk), _q(schema, mark)
    sql = ("WITH b AS (SELECT batch, count(*) AS n FROM %s GROUP BY batch), "
           "j AS (SELECT coalesce(b.batch, m.batch) AS batch, b.n AS rows, m.n AS marked FROM b FULL OUTER JOIN %s m "
           "ON m.batch = b.batch) "
           "SELECT count(*) FILTER (WHERE rows IS DISTINCT FROM marked), count(*), "
           "list([batch, coalesce(rows, 0), coalesce(marked, -1)] ORDER BY batch) FILTER "
           "(WHERE rows IS DISTINCT FROM marked) FROM j" % (B, M))

    def judge(row):
        bad = int(row[0])
        return bad == 0, {"batches_partial_or_unmarked": bad, "batches": int(row[1]),
                          "sample_batch_rows_marked": (row[2] or [])[:5]}
    return Check("large_marked", sql, judge)


def versions(table: str, allowed: Optional[set] = None, schema: str = "public") -> Check:
    """SPEC 3.2: count and checksum of one table; `allowed` = {(count, checksum)} named by the test (None: record
    only). `version_sql` gives the statement a test uses to compute an allowed version."""
    sql = version_sql(table, schema)

    def judge(row):
        v = (int(row[0]), int(row[1]))
        if allowed is None:
            return True, {"version": list(v)}
        return v in allowed, {"version": list(v), "allowed": sorted(list(a) for a in allowed)[:5]}
    return Check("versions", sql, judge, missing_is_violation=True)


def version_sql(table: str, schema: str = "public") -> str:
    return "SELECT count(*), coalesce(sum(hash(t)::HUGEINT), 0) FROM %s AS t" % _q(schema, table)


def checks_for(features, schema: str = "public", transfer_tables=None, transfer_sum=None) -> list:
    """The checks a workload with these `Features` supports."""
    out = []
    if getattr(features, "transfers", False) and transfer_tables:
        out.append(transfers(transfer_tables, transfer_sum, schema))
    if getattr(features, "parent_child", False):
        out.append(parent_child(schema))
    if getattr(features, "ledger", False):
        out.append(debit_credit(schema))
    if getattr(features, "large_marked", False) or getattr(features, "large_rollback", False):
        out.append(large_marked(schema))
    return out


def _is_missing(e: Exception) -> bool:
    s = str(e).lower()
    return "does not exist" in s or "not found" in s or "catalog error" in s


@dataclass
class LoopReport:
    check: str
    observations: int = 0
    unavailable: int = 0
    violations: int = 0
    longest_gap_s: float = 0.0
    seconds: float = 0.0
    first_violations: list = field(default_factory=list)
    errors: list = field(default_factory=list)
    last: Optional[dict] = None

    @property
    def ok(self) -> bool:
        return self.violations == 0

    def to_dict(self) -> dict:
        return {"check": self.check, "observations": self.observations, "unavailable": self.unavailable,
                "violations": self.violations, "longest_gap_s": round(self.longest_gap_s, 3),
                "seconds": round(self.seconds, 3), "first_violations": self.first_violations,
                "errors": self.errors, "last": self.last}


class ConsumerLoop:
    """Runs one check in a thread. `connect()` gives the observer's own connection (it is used by this thread
    only); every observation is one statement on it."""

    def __init__(self, check: Check, connect: Callable, min_interval: float = 0.0, keep: int = 3):
        self.check, self.connect, self.min_interval, self.keep = check, connect, min_interval, keep
        self.report = LoopReport(check.name)
        self._stop = threading.Event()
        self._thread = None
        self._pause = threading.Event()

    def observe_once(self, conn) -> Optional[bool]:
        try:
            row = conn.execute(self.check.sql).fetchone()
        except Exception as e:                             # noqa: BLE001 - counted
            if self.check.missing_is_violation and _is_missing(e):
                self._violation({"missing_table": True})
                return False
            self.report.unavailable += 1
            if len(self.report.errors) < 5 and type(e).__name__ not in self.report.errors:
                self.report.errors.append(type(e).__name__)
            return None
        ok, detail = self.check.judge(row)
        self.report.observations += 1
        self.report.last = detail
        if not ok:
            self._violation(detail)
        return ok

    def _violation(self, detail):
        self.report.violations += 1
        if len(self.report.first_violations) < self.keep:
            self.report.first_violations.append(dict(detail, observation=self.report.observations,
                                                     t=round(time.monotonic() - self._t0, 3)))

    def _loop(self):
        self._t0 = time.monotonic()
        last_ok = None
        conn = None
        try:
            while not self._stop.is_set():
                if self._pause.is_set():
                    self._stop.wait(0.05)
                    last_ok = None                         # a paused observer has no gap
                    continue
                if conn is None:
                    try:
                        conn = self.connect()
                    except Exception as e:                 # noqa: BLE001
                        self.report.unavailable += 1
                        if len(self.report.errors) < 5:
                            self.report.errors.append("connect: %s" % type(e).__name__)
                        self._stop.wait(0.2)
                        continue
                r = self.observe_once(conn)
                now = time.monotonic()
                if r is not None:
                    if last_ok is not None:
                        self.report.longest_gap_s = max(self.report.longest_gap_s, now - last_ok)
                    last_ok = now
                if self.min_interval:
                    self._stop.wait(self.min_interval)
        finally:
            self.report.seconds = time.monotonic() - self._t0
            if conn is not None:
                try:
                    conn.close()
                except Exception:                          # noqa: BLE001
                    pass

    def start(self) -> "ConsumerLoop":
        self._thread = threading.Thread(target=self._loop, name="observer-" + self.check.name, daemon=True)
        self._thread.start()
        return self

    def pause(self):
        self._pause.set()

    def resume(self):
        self._pause.clear()

    def stop(self) -> LoopReport:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=60)
        return self.report


class Observers:
    """Several loops together."""

    def __init__(self, checks: list, connect: Callable, min_interval: float = 0.0):
        self.loops = [ConsumerLoop(c, connect, min_interval) for c in checks]

    def start(self):
        for lp in self.loops:
            lp.start()
        return self

    def pause(self):
        for lp in self.loops:
            lp.pause()

    def resume(self):
        for lp in self.loops:
            lp.resume()

    def stop(self) -> list:
        return [lp.stop().to_dict() for lp in self.loops]

    def violations(self) -> int:
        return sum(lp.report.violations for lp in self.loops)
