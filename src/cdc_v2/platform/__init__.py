"""What the leader code needs from the Flight platform (contract C6), behind one interface with two implementations:

    platform.identity() -> (flight_id, run_number, run_id)
    platform.run_state(flight_id, run_number) -> 'PENDING' | 'RUNNING' | 'SUCCEEDED' | 'FAILED' | 'CANCELLED' | None
    platform.cancel(flight_id, run_number) -> bool

- `MotherDuckPlatform` (`cdc_v2.platform.motherduck`): MotherDuck's Flight SQL functions, as the run spike used
  them (`docs/spike/run_semantics.md`).
- `LocalPlatform` (`cdc_v2.platform.local`): the simulator for tier T1 and the leadership fuzzer; runs are local
  process trees started from `flights/cdc_v2/main.py`. `LocalPlatformClient` is the three calls above for a
  process of a run.

The bootstrap needs the same three calls before the clone; it carries a small copy of both (compared by a T0 test).
`None` from `run_state` means that the platform knows no such run (or could not say).
"""
from __future__ import annotations

import os

RUN_STATES = ("PENDING", "RUNNING", "SUCCEEDED", "FAILED", "CANCELLED")
ACTIVE = frozenset(("PENDING", "RUNNING"))
TERMINAL = frozenset(("SUCCEEDED", "FAILED", "CANCELLED"))


def normalize_state(status):
    """``RUN_STATUS_RUNNING`` and ``RUNNING`` both become ``RUNNING`` (the DuckDB 1.5.6 client drops the prefix,
    `docs/spike/flight_platform.md` section 6)."""
    if status is None:
        return None
    s = str(status).upper()
    if s.startswith("RUN_STATUS_"):
        s = s[len("RUN_STATUS_"):]
    return s


def identity(environ=None):
    """``(flight_id, run_number, run_id)`` of the run this process belongs to: the variables the platform sets
    (`MOTHERDUCK_FLIGHT_ID`, `MOTHERDUCK_FLIGHT_RUN_NUMBER`, `MOTHERDUCK_FLIGHT_RUN_ID`; measured, not documented,
    `docs/spike/flight_platform.md` section 5). LocalPlatform sets the same names."""
    env = os.environ if environ is None else environ
    number = (env.get("MOTHERDUCK_FLIGHT_RUN_NUMBER") or "").strip()
    return ((env.get("MOTHERDUCK_FLIGHT_ID") or "").strip() or None,
            int(number) if number.isdigit() else None,
            (env.get("MOTHERDUCK_FLIGHT_RUN_ID") or "").strip() or None)


__all__ = ["RUN_STATES", "ACTIVE", "TERMINAL", "normalize_state", "identity"]
