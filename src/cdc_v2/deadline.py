"""Deadlines. Every blocking call in this code base takes one (SPEC 4.5); nothing may wait for ever.

A deadline is a point on the monotonic clock of this process. It is never compared with a wall clock and never
sent to another process; a child process gets the remaining seconds and makes its own.
"""
from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Callable

_clock: Callable[[], float] = time.monotonic
_sleep: Callable[[float], None] = time.sleep


class DeadlineExceeded(TimeoutError):
    """A wait reached its deadline. `what` says which wait, in words a log reader understands."""

    def __init__(self, what: str):
        super().__init__(what)
        self.what = what


@dataclass(frozen=True, order=True)
class Deadline:
    at: float

    @classmethod
    def after(cls, seconds: float) -> "Deadline":
        if not seconds >= 0:                      # also refuses NaN
            raise ValueError("a deadline needs a duration >= 0, got %r" % (seconds,))
        if seconds == float("inf"):
            raise ValueError("a deadline cannot be infinite")
        return cls(_clock() + seconds)

    def remaining(self) -> float:
        """Seconds left, never negative."""
        return max(0.0, self.at - _clock())

    @property
    def expired(self) -> bool:
        return _clock() >= self.at

    def check(self, what: str) -> None:
        if self.expired:
            raise DeadlineExceeded(what)

    def within(self, seconds: float) -> "Deadline":
        """A deadline for one step of the work: `seconds` from now, but never later than this one."""
        return min(self, Deadline.after(seconds))

    def sleep(self, seconds: float, what: str) -> None:
        """Sleep `seconds`; raise DeadlineExceeded instead of sleeping past the deadline."""
        left = self.remaining()
        if seconds >= left:
            _sleep(left)
            raise DeadlineExceeded(what)
        _sleep(seconds)
