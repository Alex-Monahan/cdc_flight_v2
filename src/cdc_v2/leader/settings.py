"""The leader timings (design 4.3, 4.6; contract C6: "every leader timing is a setting").

The bootstrap (`flights/cdc_v2/main.py`) reads the same environment variables before the clone and passes the
values it used on to the pipeline, so both sides of one run work with one set of numbers. A T0 test compares the
bootstrap's table (`TIMINGS`) with `ENV` below: names, variables and defaults.

Order, checked when the settings are built: heartbeat interval at most a third of the stale age, stale age below
the hard-exit age. (Design 4.3 writes "interval < stale age / 3"; its own defaults, 10 s and 30 s, meet that only
with equality, so equality is allowed: three beats fit into the stale age.)
"""
from __future__ import annotations

import os
from dataclasses import dataclass, fields

#: field -> environment variable. The defaults are the dataclass defaults (PLAN §7, design 4).
ENV = {
    "heartbeat_interval_s": "CDC_V2_HEARTBEAT_INTERVAL_S",
    "stale_age_s": "CDC_V2_STALE_AGE_S",
    "hard_exit_age_s": "CDC_V2_HARD_EXIT_AGE_S",
    "startup_timeout_s": "CDC_V2_STARTUP_TIMEOUT_S",
    "takeover_timeout_s": "CDC_V2_TAKEOVER_TIMEOUT_S",
    "late_commit_wait_s": "CDC_V2_LATE_COMMIT_WAIT_S",
    "max_runtime_s": "CDC_V2_RUN_MAX_RUNTIME_S",
    "release_before_boundary_s": "CDC_V2_RELEASE_BEFORE_BOUNDARY_S",
    "minute_s": "CDC_V2_MINUTE_S",
    "finish_budget_s": "CDC_V2_FINISH_BUDGET_S",
    "contender_check_s": "CDC_V2_CONTENDER_CHECK_S",
    "waiting_calls_bound": "CDC_V2_WAITING_CALLS_BOUND",
}


def check_order(interval, stale, hard_exit):
    """What is wrong with the order of the three timings (empty: nothing). The bootstrap has the same function."""
    out = []
    for name, v in (("heartbeat interval", interval), ("stale age", stale), ("hard-exit age", hard_exit)):
        if not v > 0:
            out.append("%s must be > 0" % name)
    if not interval <= stale / 3.0:
        out.append("heartbeat interval %.3f s must be at most a third of the stale age %.3f s" % (interval, stale))
    if not stale < hard_exit:
        out.append("stale age %.3f s must be below the hard-exit age %.3f s" % (stale, hard_exit))
    return out


@dataclass(frozen=True)
class LeaderSettings:
    heartbeat_interval_s: float = 10.0       # design 4.3
    stale_age_s: float = 30.0                # a heartbeat older than this is not healthy (SPEC 4.2)
    hard_exit_age_s: float = 60.0            # no tick for this long: the bootstrap kills the run (SPEC 4.2)
    startup_timeout_s: float = 180.0         # take-over to the pipeline's first tick (run.startup_timeout)
    takeover_timeout_s: float = 45.0         # leader.takeover_timeout
    late_commit_wait_s: float = 2.5          # after an unknown commit outcome, before reading again (> 2 s)
    max_runtime_s: float = 27000.0           # run.max_runtime, 7 h 30 min (50 min on a Lite plan)
    release_before_boundary_s: float = 5.0   # the hand-over ends this long before a minute boundary
    minute_s: float = 60.0                   # the length of the schedule's minute (shorter in tests)
    finish_budget_s: float = 10.0            # time allowed for finishing the group in flight and the release
    contender_check_s: float = 60.0          # how often the leader turns contenders' notices into alerts
    waiting_calls_bound: int = 8             # md.waiting_calls() above this ends the process (contract C11)

    def __post_init__(self):
        for f in fields(self):
            v = getattr(self, f.name)
            if f.name == "waiting_calls_bound":
                if isinstance(v, bool) or not isinstance(v, int) or v < 1:
                    raise ValueError("waiting_calls_bound must be an integer >= 1")
                continue
            if isinstance(v, bool) or not isinstance(v, (int, float)) or not 0 < v < float("inf"):
                raise ValueError("%s must be a number of seconds > 0, got %r" % (f.name, v))
            object.__setattr__(self, f.name, float(v))
        problems = check_order(self.heartbeat_interval_s, self.stale_age_s, self.hard_exit_age_s)
        if problems:
            raise ValueError("; ".join(problems))
        if self.minute_s <= self.release_before_boundary_s:
            raise ValueError("the minute must be longer than release_before_boundary_s")

    @property
    def beat_timeout_s(self):
        """The deadline of one heartbeat statement: half the stale age (at most 20 s), so that a statement that
        hangs leaves time for the next beat before the heartbeat is stale; the hard exit bounds it anyway."""
        return max(0.2, min(self.stale_age_s / 2.0, 20.0))

    @classmethod
    def from_env(cls, environ=None):
        env = os.environ if environ is None else environ
        kw = {}
        for name, var in ENV.items():
            text = (env.get(var) or "").strip()
            if not text:
                continue
            try:
                kw[name] = int(text) if name == "waiting_calls_bound" else float(text)
            except ValueError:
                raise ValueError("%s is not a number" % var) from None
        return cls(**kw)

    def to_env(self):
        """The environment variables that give these settings (for a process the test starts)."""
        return {var: repr(getattr(self, name)) for name, var in ENV.items()}


__all__ = ["LeaderSettings", "ENV", "check_order"]
