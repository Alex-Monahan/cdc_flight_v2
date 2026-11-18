"""The seams the stream offers to packages B (backfill) and T (catalog gate) (task S section 9; contract C10).

The stream calls, in every group (and every idle position step), inside the group's stream-lane transaction and
before the position step:

    tables.on_group(tx, group_end)                                   # package T: enact catalog findings
    changes = backfill.on_group(tx, group_end, change_sets, routes)  # package B: requests, pending logs, swap

and, for every group before it is applied (beside the client-side work, one source round trip at most):

    answer = tables.confirm(group_end, tables, deadline)             # {(schema, table): "same" | "changed"}

`backfill.enact(tx, route, request_id, p0)` enacts one request at once (a reposition of C10 does it for every
table). The stand-ins below answer `same`, do nothing for T, and for B enact a request by setting the table's
routing to `skip` and leaving the request row pending. Packages B and T replace them with `install`.
"""
from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import Callable, Optional

from cdc_v2.stream import state as st

log = logging.getLogger("cdc_v2.stream")

#: Pending requests whose table is not `skip` yet (the stand-in's one read, at most every `request_poll_s`).
_OPEN_REQUESTS_SQL = (
    "SELECT r.request_id, r.schema_name, r.table_name FROM _cdc.backfill_requests r JOIN _cdc.stream_tables t "
    "ON t.schema_name = r.schema_name AND t.table_name = r.table_name WHERE r.state = 'pending' "
    "AND t.routing <> 'skip' ORDER BY r.requested_at, r.request_id")


class StandInBackfill:
    """Package B's stand-in. `on_group` reads the pending requests at most every `poll_s` seconds and enacts each
    one whose table is not `skip` yet; it returns the routing changes {(schema, table): Route}."""

    def __init__(self, poll_s: float = 5.0, clock: Callable[[], float] = time.monotonic):
        self.poll_s = poll_s
        self.clock = clock
        self._last = None

    def enact(self, tx, route: st.Route, request_id: str, p0: int) -> st.Route:
        """Enact one request: the table enters `backfilling` with routing `skip` (the stand-in keeps no pending log
        and never swaps); the request row stays pending for package B."""
        to = "backfilling" if route.state in ("streaming", "quarantined") else route.state
        return st.set_routing(tx, route, to, "skip", "backfill requested (stand-in)", backfill_id=request_id,
                              detail={"p0": p0})

    def on_group(self, tx, group_end: int, change_sets: dict, routes: dict) -> dict:
        now = self.clock()
        if self._last is not None and now - self._last < self.poll_s:
            return {}
        self._last = now
        changes = {}
        for rid, schema, table in tx.fetchall(_OPEN_REQUESTS_SQL):
            r = changes.get((schema, table)) or routes.get((schema, table))
            if r is None or r.routing == "skip":
                continue
            changes[(schema, table)] = self.enact(tx, r, rid, group_end)
        return changes


class StandInTables:
    """Package T's stand-in: every table is the table its destination was built for."""

    def confirm(self, group_end: int, tables, deadline) -> dict:
        return {t: "same" for t in tables}

    def on_group(self, tx, group_end: int) -> None:
        return None


@dataclass
class Seams:
    backfill: object
    tables: object

    def backfill_enact(self, tx, route, request_id, p0):
        return self.backfill.enact(tx, route, request_id, p0)


_current: Optional[Seams] = None


def standins(poll_s: float = 5.0) -> Seams:
    return Seams(StandInBackfill(poll_s), StandInTables())


def install(seams: Optional[Seams]) -> None:
    """Install the seams of packages B and T for this process (None: the stand-ins)."""
    global _current
    _current = seams


def current() -> Seams:
    global _current
    if _current is None:
        _current = standins()
    return _current


__all__ = ["StandInBackfill", "StandInTables", "Seams", "standins", "install", "current"]
