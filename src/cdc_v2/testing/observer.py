"""The transfers observer (SPEC 1.3): polls a global sum on the destination and records every observation.

Transfer transactions of the workload move amounts between accounts in several tables, so the sum over all of
them is constant in every committed source state. The observer reads that sum on the destination from its own
connection, one statement (one transaction) per observation. An observation with a different sum shows a state that
never existed on the source: a source transaction was split across destination commits.

Observations that fail (for example because the tables do not exist yet) are counted as unavailable, not as
violations.
"""
from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field
from typing import Callable, Optional

from cdc_v2.deadline import Deadline
from cdc_v2.oracle import mdexec


def sum_query(tables: list, schema: str = "public", column: str = "balance") -> str:
    parts = ['(SELECT coalesce(sum("%s"), 0) FROM "%s"."%s")' % (column, schema, t) for t in tables]
    return "SELECT " + " + ".join(parts)


@dataclass
class ObserverReport:
    expected: int
    observations: int = 0
    unavailable: int = 0
    violations: int = 0
    distinct_sums: list = field(default_factory=list)
    first_violation: Optional[dict] = None
    seconds: float = 0.0
    errors: list = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return self.violations == 0 and self.observations > 0


class TransfersObserver:
    """Start it before the destination receives transfers; `stop()` returns the report.

    connect(): a new connection to the destination (it is used only by the observer thread).
    """

    def __init__(self, connect: Callable, tables: list, expected_sum: int, schema: str = "public",
                 interval: float = 0.0, column: str = "balance", statement_seconds: float = 60.0):
        self.connect = connect
        self.sql = sum_query(tables, schema, column)
        self.report = ObserverReport(expected_sum)
        self.interval = interval
        self.statement_seconds = statement_seconds
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._sums: set = set()

    def observe_once(self, conn) -> Optional[int]:
        try:
            v = mdexec.execute(conn, self.sql, deadline=Deadline.after(self.statement_seconds), fetch="one",
                               what="transfers observation")[0]
        except Exception as e:                       # noqa: BLE001 - counted, not raised
            self.report.unavailable += 1
            if len(self.report.errors) < 5:
                self.report.errors.append(type(e).__name__)
            return None
        v = int(v)
        self.report.observations += 1
        self._sums.add(v)
        if v != self.report.expected:
            self.report.violations += 1
            if self.report.first_violation is None:
                self.report.first_violation = {"sum": v, "expected": self.report.expected,
                                               "observation": self.report.observations}
        return v

    def _loop(self) -> None:
        t0 = time.monotonic()
        conn = self.connect()
        try:
            while not self._stop.is_set():
                self.observe_once(conn)
                if self.interval:
                    self._stop.wait(self.interval)
        finally:
            self.report.seconds = round(time.monotonic() - t0, 3)
            try:
                conn.close()
            except Exception:                        # noqa: BLE001
                pass

    def start(self) -> "TransfersObserver":
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._thread.start()
        return self

    def stop(self) -> ObserverReport:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=60)
        self.report.distinct_sums = sorted(self._sums)[:20]
        return self.report


def apply_transfers(md_conn, transfers: list, split: bool = False, pause: float = 0.0,
                    statement_seconds: float = 120.0) -> int:
    """Apply source transfer transactions to the destination. Test helper standing in for a pipeline.

    transfers: statement lists (without BEGIN/COMMIT). With split=False each source transaction becomes one
    destination transaction (correct). With split=True the first statement commits on its own and the rest in a
    second transaction, `pause` seconds later (the bug the observer must catch).
    """
    n = 0
    for stmts in transfers:
        groups = [stmts[:1], stmts[1:]] if split else [stmts]
        for i, g in enumerate(groups):
            if i and pause:
                time.sleep(pause)
            d = Deadline.after(statement_seconds)
            for s in ["BEGIN TRANSACTION"] + list(g) + ["COMMIT"]:
                mdexec.execute(md_conn, s, deadline=d, fetch=None, what="stand-in apply")
            n += 1
    return n
