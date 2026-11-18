"""The supervisor (SPEC 4.5; contract C11; task S section 2): component progress, declared waits, restarts.

    sup = Supervisor(SupervisorSettings())
    sup.add("stream", run_stream, essential=True, primary=True)    # run_stream(handle) runs in its own thread
    sup.start()
    ok, phase, progress_at = sup.progress()                        # the only input of the leader's heartbeat

A component reports progress with `handle.progress(phase)`. `with handle.waiting(reason, deadline):` declares a
wait: until its deadline the component counts as progressing, and its reason is the phase an operator reads in
`_cdc.heartbeat.phase`; after the deadline it does not. `handle.on_close(fn)` registers what closes the component's
resources from outside (the engine's kill, the lane connection's abort, a source connection's close).

`check()` (a monitor thread calls it every `check_interval`) restarts a component that has been silent for longer
than `restart_after` (default 120 s, SPEC 4.5) and is not inside a declared wait: its closers run, its thread gets
`join_timeout` to return, and a new instance starts in a new thread. A component that ends with an error is
restarted the same way. A thread that does not return in time, more than `max_restarts` restarts of one component
within `restart_window`, more calls of the destination layer left waiting than `max_waiting_calls`
(`md.waiting_calls()`, C11), or an error that the component marks as final (`FenceLost`, `LaneMisuse`) put the
supervisor past its limit: `progress()` answers `ok = False` from then on, so the heartbeat stops and the
bootstrap's hard exit ends the run (design 5). The supervisor never waits for ever and never needs an operator.

Times are read from an injectable monotonic clock, so the rules are tested with a fake clock.
"""
from __future__ import annotations

import contextlib
import logging
import threading
import time
from dataclasses import dataclass
from typing import Callable, Optional

log = logging.getLogger("cdc_v2.supervisor")


@dataclass(frozen=True)
class SupervisorSettings:
    restart_after_s: float = 120.0          # SPEC 4.5: a component silent for longer is restarted
    max_restarts: int = 5                   # restarts of one component within `restart_window_s` ...
    restart_window_s: float = 600.0         # ... beyond which the supervisor is past its limit
    join_timeout_s: float = 30.0            # a restarted component's thread must return within this
    check_interval_s: float = 1.0
    max_waiting_calls: int = 8              # calls of the destination layer given up and still waiting (C11)

    def __post_init__(self):
        for n in ("restart_after_s", "restart_window_s", "join_timeout_s", "check_interval_s"):
            v = getattr(self, n)
            if isinstance(v, bool) or not isinstance(v, (int, float)) or not 0 < v < float("inf"):
                raise ValueError("supervisor.%s must be a number of seconds > 0" % n)
        for n in ("max_restarts", "max_waiting_calls"):
            v = getattr(self, n)
            if isinstance(v, bool) or not isinstance(v, int) or v < 1:
                raise ValueError("supervisor.%s must be an integer >= 1" % n)


class Stopping(Exception):
    """Raised inside a component's wait when the supervisor stops or restarts it."""


class Final(Exception):
    """A component ended with an error that no restart can cure (another run leads, a programming error). The
    supervisor goes past its limit at once; `error` is the original exception."""

    def __init__(self, error: BaseException):
        self.error = error
        super().__init__("%s: %s" % (type(error).__name__, error))


class Handle:
    """What one instance of a component sees of the supervisor."""

    def __init__(self, sup: "Supervisor", comp: "_Component", generation: int):
        self._sup = sup
        self._comp = comp
        self.generation = generation
        self.stop_event = threading.Event()
        self._closers: list = []
        self._lock = threading.Lock()

    @property
    def name(self) -> str:
        return self._comp.name

    @property
    def stopping(self) -> bool:
        return self.stop_event.is_set()

    def progress(self, phase: str = "running"):
        """The component made progress; `phase` is a short lower-case word for `_cdc.heartbeat.phase`."""
        if self._comp.handle is not self:
            raise Stopping("this instance of %s was replaced" % self._comp.name)
        now = self._sup.clock()
        self._comp.progress_at = now
        self._comp.phase = phase

    @contextlib.contextmanager
    def waiting(self, reason: str, deadline):
        """A declared wait: progress until `deadline` (a `cdc_v2.deadline.Deadline`), not after it."""
        until = self._sup.clock() + max(0.0, deadline.remaining())
        w = (reason, until)
        with self._sup._lock:
            self._comp.waits.append(w)
        try:
            yield
        finally:
            with self._sup._lock:
                try:
                    self._comp.waits.remove(w)
                except ValueError:
                    pass
            self.progress(self._comp.phase if self._comp.phase else "running")

    def sleep(self, seconds: float, reason: str):
        """Sleep inside a declared wait; ends early (raising `Stopping`) when the component is stopped."""
        from cdc_v2.deadline import Deadline
        d = Deadline.after(max(0.0, seconds))
        with self.waiting(reason, d):
            if self.stop_event.wait(max(0.0, seconds)):
                raise Stopping("%s stopped" % self.name)

    def on_close(self, fn: Callable[[], None]):
        """Register a closer: called from outside when the component is restarted or stopped."""
        with self._lock:
            self._closers.append(fn)

    def remove_closer(self, fn):
        with self._lock:
            with contextlib.suppress(ValueError):
                self._closers.remove(fn)

    def _close(self):
        self.stop_event.set()
        with self._lock:
            closers = list(self._closers)
        for fn in reversed(closers):
            try:
                fn()
            except Exception:                          # noqa: BLE001 - a closer must not stop the others
                log.warning("closing %s raised", self.name, exc_info=True)


class _Component:
    def __init__(self, name, factory, essential, primary):
        self.name = name
        self.factory = factory
        self.essential = essential
        self.primary = primary
        self.handle: Optional[Handle] = None
        self.thread: Optional[threading.Thread] = None
        self.progress_at = None
        self.phase = "starting"
        self.waits: list = []
        self.restarts: list = []               # clock times of restarts
        self.generation = 0
        self.ended = False                     # returned normally (a planned stop)
        self.error: Optional[BaseException] = None


class Supervisor:
    def __init__(self, settings: SupervisorSettings = SupervisorSettings(), clock: Callable[[], float] = time.monotonic,
                 waiting_calls: Optional[Callable[[], int]] = None, start_threads: bool = True):
        self.settings = settings
        self.clock = clock
        self._comps: dict = {}
        self._lock = threading.RLock()
        self._past_limit: Optional[str] = None
        self._final: Optional[BaseException] = None
        self._stopping = False
        self._monitor: Optional[threading.Thread] = None
        self._monitor_stop = threading.Event()
        self._waiting_calls = waiting_calls
        self._start_threads = start_threads
        self.events: list = []                 # (clock, component, event, detail): for tests and the log

    # ------------------------------------------------------------------------------------------------ building

    def add(self, name: str, factory: Callable[[Handle], None], *, essential: bool = True, primary: bool = False):
        """A component: `factory(handle)` runs one instance in its own thread. `primary`: its progress is the
        progress the heartbeat reports (the stream)."""
        if name in self._comps:
            raise ValueError("component %s added twice" % name)
        self._comps[name] = _Component(name, factory, essential, primary)

    def start(self):
        for c in self._comps.values():
            self._launch(c)
        if self._start_threads:
            self._monitor = threading.Thread(target=self._monitor_loop, name="cdc_v2-supervisor", daemon=True)
            self._monitor.start()

    def _launch(self, c: _Component):
        c.generation += 1
        h = Handle(self, c, c.generation)
        c.handle = h
        c.progress_at = self.clock()
        c.phase = "starting"
        c.waits = []
        c.error = None
        self.events.append((self.clock(), c.name, "start", c.generation))
        if self._start_threads:
            t = threading.Thread(target=self._run, args=(c, h), name="cdc_v2-%s-%d" % (c.name, c.generation),
                                 daemon=True)
            c.thread = t
            t.start()

    def _run(self, c: _Component, h: Handle):
        try:
            c.factory(h)
            if c.handle is h:
                c.ended = True
                self.events.append((self.clock(), c.name, "ended", h.generation))
        except Stopping:
            pass
        except Final as e:
            self._go_past_limit("%s: %s" % (c.name, e), e.error)
        except BaseException as e:                     # noqa: BLE001 - every other error is a restart
            if c.handle is h:
                c.error = e
                self.events.append((self.clock(), c.name, "error", type(e).__name__))
                log.warning("component %s ended with %s", c.name, type(e).__name__,
                            extra={"cdc": {"component": c.name, "error": type(e).__name__}})

    # ------------------------------------------------------------------------------------------------ rules

    def _silent(self, c: _Component, now: float) -> bool:
        if c.ended or c.progress_at is None:
            return False
        if any(until > now for _r, until in c.waits):
            return False
        return now - c.progress_at > self.settings.restart_after_s

    def check(self):
        """One pass of the rules (the monitor thread calls it; tests call it with a fake clock)."""
        if self._stopping:
            return
        now = self.clock()
        if self._waiting_calls is not None:
            try:
                n = int(self._waiting_calls())
            except Exception:                          # noqa: BLE001
                n = 0
            if n > self.settings.max_waiting_calls:
                self._go_past_limit("%d calls of the destination layer were given up and still wait" % n)
        for c in list(self._comps.values()):
            if c.ended or self._past_limit:
                continue
            dead = c.thread is not None and not c.thread.is_alive() and not c.ended
            if dead or c.error is not None or self._silent(c, now):
                self.restart(c.name, "ended with %s" % type(c.error).__name__ if c.error is not None
                             else ("thread ended" if dead else "silent beyond restart_after"))

    def restart(self, name: str, why: str):
        """Close the component from outside, give its thread `join_timeout` to return, start a new instance."""
        c = self._comps[name]
        now = self.clock()
        c.restarts = [t for t in c.restarts if now - t <= self.settings.restart_window_s] + [now]
        self.events.append((now, name, "restart", why))
        log.warning("restarting component %s: %s", name, why, extra={"cdc": {"component": name, "why": why}})
        old, thread = c.handle, c.thread
        if old is not None:
            old._close()
        if thread is not None and thread is not threading.current_thread():
            thread.join(self.settings.join_timeout_s)
            if thread.is_alive():
                self._go_past_limit("component %s did not return within %.0f s of its restart"
                                    % (name, self.settings.join_timeout_s))
                return
        if len(c.restarts) > self.settings.max_restarts:
            self._go_past_limit("component %s restarted %d times within %.0f s"
                                % (name, len(c.restarts), self.settings.restart_window_s))
            return
        self._launch(c)

    def _go_past_limit(self, why: str, error: Optional[BaseException] = None):
        with self._lock:
            if self._past_limit is None:
                self._past_limit = why
                self._final = error
                self.events.append((self.clock(), "-", "past_limit", why))
                log.error("supervisor past its limit: %s", why, extra={"cdc": {"why": why}})

    # ------------------------------------------------------------------------------------------------ reading

    @property
    def past_limit(self) -> Optional[str]:
        return self._past_limit

    @property
    def final_error(self) -> Optional[BaseException]:
        return self._final

    def progress(self):
        """(ok, phase, progress_at): `ok` is false when the primary component has not reported within its deadline
        (and is not inside a declared wait), or the supervisor is past its limit. `phase` is the primary
        component's phase or the reason of its declared wait; `progress_at` its last progress (this supervisor's
        clock)."""
        now = self.clock()
        prim = [c for c in self._comps.values() if c.primary] or list(self._comps.values())
        if not prim:
            return (self._past_limit is None, "idle", now)
        c = prim[0]
        if self._past_limit is not None:
            return (False, "past_limit", c.progress_at)
        waits = [r for r, until in c.waits if until > now]
        phase = waits[-1] if waits else c.phase
        ok = not c.ended and not self._silent(c, now)
        return (ok, _phase_word(phase), c.progress_at)

    def component(self, name):
        return self._comps[name]

    # ------------------------------------------------------------------------------------------------ stopping

    def _monitor_loop(self):
        while not self._monitor_stop.wait(self.settings.check_interval_s):
            try:
                self.check()
            except Exception:                          # noqa: BLE001 - the monitor never dies
                log.exception("supervisor check failed")

    def stop(self, deadline):
        """Stop every component: set their stop events, run their closers, join their threads until `deadline`."""
        self._stopping = True
        self._monitor_stop.set()
        for c in self._comps.values():
            if c.handle is not None:
                c.handle._close()
        for c in self._comps.values():
            if c.thread is not None and c.thread is not threading.current_thread():
                c.thread.join(max(0.0, deadline.remaining()))
        return all(c.thread is None or not c.thread.is_alive() for c in self._comps.values())


def _phase_word(text: str) -> str:
    """A phase for `_cdc.heartbeat.phase` (lower-case identifier of at most 32 characters, md.schema)."""
    out = []
    for ch in (text or "running").lower():
        out.append(ch if ch.isalnum() and ch.isascii() else "_")
    word = "".join(out).strip("_")
    while "__" in word:
        word = word.replace("__", "_")
    if not word or not word[0].isalpha():
        word = "p_" + word
    return word[:32].rstrip("_") or "running"


__all__ = ["Supervisor", "SupervisorSettings", "Handle", "Stopping", "Final"]
