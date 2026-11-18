"""What the bootstrap hands the pipeline (contract C12): epoch, run identity, tick descriptor, take-over facts.

    ctx = leader.RunContext.from_env()

- ``epoch``: the epoch this run took over with. Every lane of the pipeline is built for it.
- ``run_id``: the identity of the run across the bootstrap and the pipeline: the platform's run id
  (`MOTHERDUCK_FLIGHT_RUN_ID`), which the take-over wrote into `_cdc.leader.run_id`. ``flight_id`` and
  ``run_number`` name the run on the platform (the next leader cancels exactly that run). Package S writes
  ``epoch`` and ``run_id`` into the row it appends to `_cdc.transitions` for every committed group, which is what
  the leadership fuzzer reads to check "never two writers".
- ``tick_fd``: the write end of the tick pipe (`cdc_v2.leader.heartbeat.Ticker`).
- ``takeover``: the facts of the take-over (previous run, its heartbeat's age, the reason, attempts, conflicts,
  interrupted connections, whether the previous run was cancelled), for the alert `takeover` and the leader
  machine's row (`cdc_v2.leader.startup`).
- ``run_elapsed_s``: seconds from the bootstrap's first line to the pipeline's start, so that the hand-over clock
  counts the run's whole life (`cdc_v2.leader.handover`).
"""
from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass, field

from .settings import LeaderSettings


@dataclass(frozen=True)
class RunContext:
    epoch: int
    run_id: str
    flight_id: str
    run_number: int
    tick_fd: object = None
    takeover: dict = field(default_factory=dict)
    run_elapsed_s: float = 0.0
    settings: LeaderSettings = field(default_factory=LeaderSettings)
    started_mono: float = field(default_factory=time.monotonic)

    def __post_init__(self):
        if isinstance(self.epoch, bool) or not isinstance(self.epoch, int) or self.epoch < 1:
            raise ValueError("an epoch is an integer >= 1")
        if not isinstance(self.run_id, str) or not self.run_id:
            raise ValueError("a run id is a non-empty text")

    @classmethod
    def from_env(cls, environ=None):
        """The context the bootstrap put into the environment (C12). Raises `ValueError` when a variable is
        missing: a pipeline without its bootstrap is a programming error, never a leader."""
        env = os.environ if environ is None else environ
        try:
            epoch = int(env["CDC_V2_EPOCH"])
            run_id = env["CDC_V2_RUN_ID"]
            flight_id = env.get("CDC_V2_FLIGHT_ID", "")
            run_number = int(env.get("CDC_V2_RUN_NUMBER") or 0)
        except (KeyError, ValueError):
            raise ValueError("CDC_V2_EPOCH and CDC_V2_RUN_ID come from the bootstrap (contract C12)") from None
        fd = (env.get("CDC_V2_TICK_FD") or "").strip()
        try:
            takeover = json.loads(env.get("CDC_V2_TAKEOVER") or "{}")
        except ValueError:
            takeover = {}
        elapsed = float(env.get("CDC_V2_RUN_ELAPSED_S") or 0.0)
        return cls(epoch=epoch, run_id=run_id, flight_id=flight_id, run_number=run_number,
                   tick_fd=int(fd) if fd.isdigit() else None, takeover=takeover if isinstance(takeover, dict) else {},
                   run_elapsed_s=elapsed, settings=LeaderSettings.from_env(env))

    def run_age_s(self):
        """Seconds since the bootstrap's first line (on this process's monotonic clock)."""
        return self.run_elapsed_s + (time.monotonic() - self.started_mono)


__all__ = ["RunContext"]
