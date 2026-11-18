"""The one way the pipeline ends itself (contract C12): the reaction to `FenceLost`, and the other ends.

    from cdc_v2 import leader
    try:
        ...
    except md.FenceLost:
        leader.on_fence_lost("stream lane")          # never returns

`stop_process(code, reason)` aborts every destination connection of the process (`md.abort_all`: the call in
flight ends, the server connection is interrupted, so that no open transaction of this run keeps holding rows
against the new leader), waits a bounded moment for those interrupts, and ends the process with `os._exit`. It
writes nothing to the destination: no alert, no release, no log row. Exit status (C12): `10` when the run was
superseded, `0` only after a committed release (`cdc_v2.leader.handover`), anything else is a failure.

The first caller decides; a second caller (another component that met `FenceLost` at the same moment) waits for
the end. Nothing here can wait for ever: the wait for the interrupts has a deadline, and `os._exit` does not wait
for threads.
"""
from __future__ import annotations

import logging
import os
import sys
import threading
import time

from cdc_v2 import crashpoints, md
from cdc_v2.deadline import Deadline

log = logging.getLogger("cdc_v2.leader")

EXIT_RELEASED = 0          # a committed release (planned hand-over)
EXIT_FAILED = 1            # anything else that ends the pipeline
EXIT_SUPERSEDED = 10       # FenceLost, or a heartbeat that changed no row
EXIT_NO_WATCHDOG = 11      # the tick pipe is closed: the bootstrap is gone, so nothing would bound this run

_lock = threading.Lock()
_ending = []
_exit = os._exit           # replaced by tests


def stop_process(code, reason, abort_s=2.0):
    """End the process with ``code``. Never returns."""
    with _lock:
        first = not _ending
        if first:
            _ending.append((code, reason))
    if not first:
        while True:                            # another component is ending the process right now
            time.sleep(0.5)
            if _exit is not os._exit:          # a test's exit function returned: do not hang the test
                return
    log.error("pipeline ends", extra={"cdc": {"exit_code": code, "reason": reason}})
    if code == EXIT_SUPERSEDED:
        crashpoints.crash_point("leader.fence_lost")
    try:
        md.abort_all()
        md.wait_idle(Deadline.after(abort_s))
    except Exception:                          # noqa: BLE001 - ending the process must not fail
        pass
    for stream in (sys.stdout, sys.stderr):
        try:
            stream.flush()
        except Exception:                      # noqa: BLE001
            pass
    _exit(code)


def on_fence_lost(reason="another run took over"):
    """The one reaction to `md.FenceLost` (and to a heartbeat that changed no row): stop every lane, end the
    process with 10, write nothing more."""
    stop_process(EXIT_SUPERSEDED, "fence lost: %s" % reason)


def ending():
    """``(code, reason)`` once a component has started to end the process, else None."""
    return _ending[0] if _ending else None


def _reset_for_tests():
    del _ending[:]


__all__ = ["stop_process", "on_fence_lost", "ending", "EXIT_RELEASED", "EXIT_FAILED", "EXIT_SUPERSEDED",
           "EXIT_NO_WATCHDOG"]
