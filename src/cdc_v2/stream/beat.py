"""The source-side heartbeat (SPEC 4.4, 4.6; task S section 8) and two small boards the components share.

`run_beat(handle, ctx)` is a component of its own: every `stream.heartbeat_interval` it writes `heartbeat.beat` on
the primary, on its own connection (package P), and posts the position the beat returned on the `BeatBoard`. The
stream recognises a beat by position: an END whose `ack_position` is at or past it (docs/design/source.md 5). Only
beats that were written count for the marker rule of SPEC 4.6.

When a beat cannot be written (privilege revoked, primary read-only or unreachable), the stream goes on, and one
alert (`source_heartbeat_failed`) says that the slot's position stands still while the replicated tables are idle
and that kept WAL grows with other traffic (SPEC 4.4). It is resolved by the next beat that is written.

`AlertOutbox` carries alerts of components that have no lane of their own to the stream, which records them in its
next transaction (alerts are lane writes, contract C4).
"""
from __future__ import annotations

import logging
import threading
import time
from typing import Callable, Optional

from cdc_v2.deadline import Deadline

log = logging.getLogger("cdc_v2.stream")


class BeatBoard:
    """Beats written on the primary, for the stream's marker rule."""

    def __init__(self, clock: Callable[[], float] = time.monotonic):
        self.clock = clock
        self._lock = threading.Lock()
        self._new: list = []
        self.written = 0
        self.failed = 0
        self._want = threading.Event()

    def post(self, position: int):
        with self._lock:
            self._new.append((position, self.clock()))
            self.written += 1

    def take(self) -> list:
        """The beats posted since the last call: [(position, written_at)]."""
        with self._lock:
            out, self._new = self._new, []
        return out

    def ask(self):
        """Ask for a beat now (a cut that waits for the stream to pass it, task S section 9)."""
        self._want.set()


class AlertOutbox:
    """Alerts waiting for the stream's next transaction: (kind, severity, fields, key)."""

    def __init__(self, clock: Callable[[], float] = time.monotonic):
        self.clock = clock
        self._lock = threading.Lock()
        self._items: list = []
        self._since: Optional[float] = None

    def put(self, kind: str, severity: str, fields: dict, key=()):
        with self._lock:
            self._items.append((kind, severity, dict(fields), tuple(key)))
            if self._since is None:
                self._since = self.clock()

    def peek(self) -> list:
        with self._lock:
            return list(self._items)

    def drop(self, n: int):
        """The first `n` alerts were committed."""
        with self._lock:
            self._items = self._items[n:]
            self._since = self.clock() if self._items else None

    def age(self) -> float:
        with self._lock:
            return 0.0 if self._since is None else self.clock() - self._since


def run_beat(handle, *, beat: Callable[[Deadline], int], board: BeatBoard, outbox: AlertOutbox, interval_s: float,
             statement_s: float = 10.0, slot: str = ""):
    """The heartbeat component. `beat(deadline)` writes one beat and returns its position (package P's
    `heartbeat.beat(primary, deadline, slot=…).position` on a connection the caller manages)."""
    failing = None
    next_at = time.monotonic()
    while not handle.stopping:
        handle.progress("beating")
        delay = next_at - time.monotonic()
        if delay > 0:
            if board._want.wait(min(delay, 1.0)):
                board._want.clear()
            elif time.monotonic() < next_at:
                continue
        if handle.stopping:
            return
        next_at = time.monotonic() + interval_s
        try:
            d = Deadline.after(statement_s)
            with handle.waiting("source_heartbeat_write", d):
                pos = beat(d)
        except Exception as e:                                    # noqa: BLE001 - any failure is reported, never fatal
            board.failed += 1
            kind = type(e).__name__
            if failing != kind:
                failing = kind
                outbox.put("source_heartbeat_failed", "warning",
                           {"slot": slot, "error_class": kind[:64],
                            "note": "slot position stands still while replicated tables are idle"},
                           key=("slot",))
                log.warning("source heartbeat failed", extra={"cdc": {"error_class": kind}})
            continue
        if failing is not None:
            failing = None
            outbox.put("source_heartbeat_restored", "info", {"slot": slot}, key=("slot",))
        board.post(pos)


__all__ = ["BeatBoard", "AlertOutbox", "run_beat"]
