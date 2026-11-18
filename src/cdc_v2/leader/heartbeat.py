"""The pipeline's heartbeat (design 4.3; contracts C11, C12) and the tick to the bootstrap.

A dedicated thread and connection. Every interval:

1. ``ok, phase, progress_at = supervisor.progress()`` — the heartbeat's only input (C11). Not ok: no beat.
2. The connection registers under `control` before its first write and after every change of its client
   connection (`registry.register(conn, "control", epoch, retry_conflict=True)`), so that a take-over can
   interrupt it.
3. `md.schema.heartbeat(conn, epoch, phase, progress_age_us)`: `beat_at` by MotherDuck's clock, `WHERE epoch =
   :mine`. Zero rows changed (`FenceLost`): another run leads; the one reaction (`stop.on_fence_lost`) ends the
   process with 10.
4. After each successful beat, one byte to the tick descriptor (never blocking; a full pipe drops the tick).

A statement that hangs has a deadline (`LeaderSettings.beat_timeout_s`); what ends a run whose heartbeat cannot
be written is the bootstrap's watch: no tick for the hard-exit age. So a run never stays alive without a healthy
heartbeat (SPEC 4.2), whatever this thread does.
"""
from __future__ import annotations

import logging
import os
import threading
import time

from cdc_v2 import crashpoints, md
from cdc_v2.deadline import Deadline, DeadlineExceeded
from cdc_v2.md import registry
from cdc_v2.md import schema as md_schema

from . import stop

log = logging.getLogger("cdc_v2.leader")


class Ticker:
    """The write end of the tick pipe (C12). ``tick()`` writes one byte without blocking: a full pipe is not an
    error (the bootstrap reads all that is there at its next look). A closed read end means that the bootstrap is
    gone, and nothing would bound this run any more: ``on_gone`` (default: end the process with 11)."""

    def __init__(self, fd, on_gone=None):
        self.fd = fd
        self.sent = 0
        self.dropped = 0
        self.on_gone = on_gone or (lambda: stop.stop_process(stop.EXIT_NO_WATCHDOG, "the bootstrap is gone"))
        if fd is not None:
            os.set_blocking(fd, False)

    def tick(self):
        if self.fd is None:
            return False
        try:
            os.write(self.fd, b"t")
            self.sent += 1
            return True
        except BlockingIOError:
            self.dropped += 1
            return False
        except OSError:                          # EPIPE, EBADF: the bootstrap's end is closed
            self.on_gone()
            return False


class Heartbeat:
    """The heartbeat thread of one run. ``supervisor`` has ``progress() -> (ok, phase, progress_at)``, with
    ``progress_at`` on this process's monotonic clock (C11; package S delivers the supervisor)."""

    def __init__(self, ctx, supervisor, md_settings=None, ticker=None, on_lost=None, role="heartbeat"):
        self.ctx = ctx
        self.settings = ctx.settings
        self.supervisor = supervisor
        self.md_settings = md_settings
        self.ticker = ticker if ticker is not None else Ticker(ctx.tick_fd)
        self.on_lost = on_lost or stop.on_fence_lost
        self.role = role
        self.conn = None
        self._registered = None            # (generation, server id) the registration was made for
        self._stop = threading.Event()
        self._thread = None
        self.beats = 0
        self.skipped = 0
        self.errors = 0
        self.last_ok = None
        self.lost = False

    # ------------------------------------------------------------------ one beat
    def beat_once(self):
        """One beat. Returns ``"beat"``, ``"no_progress"``, ``"retry"`` (a transport problem or a conflict: tried
        again soon) or ``"lost"`` (another run leads; ``on_lost`` was called)."""
        ok, phase, progress_at = self.supervisor.progress()
        if not ok:
            self.skipped += 1
            return "no_progress"
        age_us = max(0, int((time.monotonic() - progress_at) * 1e6))
        d = Deadline.after(self.settings.beat_timeout_s)
        try:
            if self.conn is None:
                self.conn = md.connect(self.role, self.md_settings, d)
            self.conn.ensure(d)
            key = (self.conn.generation, self.conn.server_id)
            if self._registered != key:
                registry.register(self.conn, "control", self.ctx.epoch, d, retry_conflict=True)
                self._registered = (self.conn.generation, self.conn.server_id)
            crashpoints.crash_point("leader.heartbeat_sent")
            md_schema.heartbeat(self.conn, self.ctx.epoch, phase, age_us, d)
            crashpoints.crash_point("leader.heartbeat_written")
        except md.FenceLost:
            self.lost = True
            self.on_lost("the heartbeat changed no row")
            return "lost"
        except (md.Conflict, md.ConnectionLost, md.CommitUnknown, md.StatementError, DeadlineExceeded) as e:
            # a beat whose outcome is unknown is not ticked: the bootstrap counts only what is known to be written
            self.errors += 1
            log.warning("heartbeat not written", extra={"cdc": {"error": type(e).__name__, "epoch": self.ctx.epoch}})
            return "retry"
        self.beats += 1
        self.last_ok = time.monotonic()
        self.ticker.tick()
        return "beat"

    # ------------------------------------------------------------------ the thread
    def _loop(self):
        interval = self.settings.heartbeat_interval_s
        while not self._stop.is_set():
            t = time.monotonic()
            out = self.beat_once()
            if out == "lost":
                return
            pause = interval if out in ("beat", "no_progress") else min(interval, 1.0)
            self._stop.wait(max(0.0, pause - (time.monotonic() - t)))

    def start(self):
        self._thread = threading.Thread(target=self._loop, name="cdc_v2-leader-heartbeat", daemon=True)
        self._thread.start()
        return self

    def stop(self, timeout=5.0):
        """Stop beating. A statement in flight ends within its own deadline; the thread is not waited for longer
        than ``timeout``."""
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout)
        if self.conn is not None:
            try:
                self.conn.close()
            except Exception:                    # noqa: BLE001
                pass


def waiting_calls_exceeded(settings):
    """True when the calls that destination connections gave up and that still wait exceed the bound (C11). The
    supervisor ends the process then: such calls hold threads until the network answers (destination section 4)."""
    return md.waiting_calls() > settings.waiting_calls_bound


__all__ = ["Heartbeat", "Ticker", "waiting_calls_exceeded"]
