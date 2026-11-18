"""A stand-in for package S's supervisor (contract C11), for the leadership tests.

    sup = StandInSupervisor(settings, deadline_s=2.0)
    sup.report("toy", "streaming")                 # a component made progress
    with sup.waiting("toy", "commit", deadline):   # a declared wait: progress until its deadline, not after
        ...
    ok, phase, progress_at = sup.progress()        # the heartbeat's only input

Rules (C11, design 4.3): ok while every component reported within ``deadline_s`` or stands in a declared wait
whose deadline has not passed. ``progress_at`` is the oldest of the components' last progress (monotonic clock).
Past the bound of calls that destination connections gave up (`leader.waiting_calls_exceeded`), the process ends
(`leader.stop_process`, status 1): such calls hold threads until the network answers. Package S builds the real
supervisor with the same interface.
"""
import contextlib
import threading
import time

from cdc_v2 import leader


class StandInSupervisor:
    def __init__(self, settings, deadline_s=5.0, stop=None, exceeded=None):
        self.settings = settings
        self.deadline_s = deadline_s
        self.stop = stop or leader.stop_process
        self.exceeded = exceeded or leader.waiting_calls_exceeded
        self.phase = "starting"
        self._last = {}
        self._waits = {}
        self._lock = threading.Lock()
        self.halted = False                     # a test switch: no component reports any more

    def report(self, component, phase=None):
        with self._lock:
            self._last[component] = time.monotonic()
            if phase:
                self.phase = phase

    @contextlib.contextmanager
    def waiting(self, component, reason, deadline):
        key = object()
        with self._lock:
            self._waits[key] = (component, reason, deadline)
            self._last.setdefault(component, time.monotonic())
        try:
            yield
        finally:
            with self._lock:
                self._waits.pop(key, None)
            self.report(component)

    def progress(self):
        if self.exceeded(self.settings):
            self.stop(leader.EXIT_FAILED, "calls that were given up still wait for the destination")
        now = time.monotonic()
        with self._lock:
            waiting = {c for c, _r, d in self._waits.values() if not d.expired}
            if not self._last:
                return True, self.phase, now
            ok = not self.halted
            oldest = now
            for component, at in self._last.items():
                if component in waiting:
                    continue
                if now - at > self.deadline_s:
                    ok = False
                oldest = min(oldest, at)
            return ok, self.phase, oldest
