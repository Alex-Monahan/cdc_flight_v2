"""`MotherDuckPlatform`: contract C6 over MotherDuck's Flight SQL functions (`MD_GET_FLIGHT_RUN`,
`MD_CANCEL_FLIGHT_RUN`), as the run spike used them from inside a Flight (`docs/spike/run_semantics.md`, Q4;
`flights/spike_runs/main.py`, mode `leader`).

What the spike measured and this class relies on:

- a run's own identity is in its environment (`identity`);
- `MD_CANCEL_FLIGHT_RUN` works from inside a Flight with the run's own token, also across Flights of one account;
  the record says `CANCELLED` 0.5 to 0.6 s after the call and the run's processes keep running, and committing, for
  2.3 to 2.5 s more (finding 1): the fence, not the cancel, keeps them out;
- cancel exactly the stored run number of the previous leader, after `MD_GET_FLIGHT_RUN` shows it `RUNNING`;
  never "all other runs": the next minute's newcomer is `RUNNING` while it decides, and a pending run is not
  listed (finding 8);
- cancelling a terminal run fails ("already in terminal state"); an unknown run number fails ("does not exist").

Table-function arguments must be constants, so the two values are rendered as literals (a UUID checked by
`uuid.UUID`, an integer). The calls are reads to the destination layer (`SELECT ... FROM MD_...`), on a
connection of their own with the layer's deadlines (`md.connect("platform")`).
"""
from __future__ import annotations

import uuid

from cdc_v2 import md

from . import identity as _identity
from . import normalize_state


def _uuid_literal(value):
    return "'%s'::UUID" % uuid.UUID(str(value))


def run_state_sql(flight_id, run_number):
    """The statement that reads a run's state (the bootstrap holds a copy; a T0 test compares the two)."""
    return "SELECT status FROM MD_GET_FLIGHT_RUN(flight_id := %s, run_number := %d)" % (
        _uuid_literal(flight_id), int(run_number))


def cancel_sql(flight_id, run_number):
    """The statement that cancels one run."""
    return "SELECT * FROM MD_CANCEL_FLIGHT_RUN(flight_id := %s, run_number := %d)" % (
        _uuid_literal(flight_id), int(run_number))


class MotherDuckPlatform:
    """C6 on a real Flight. ``conn`` is an `md.Connection` (default: a connection of its own, ``md.connect(
    "platform", settings)``); every call has the layer's deadline."""

    def __init__(self, conn=None, settings=None, environ=None):
        self._conn = conn
        self._settings = settings
        self._environ = environ

    @property
    def conn(self):
        if self._conn is None:
            self._conn = md.connect("platform", self._settings)
        return self._conn

    def identity(self):
        return _identity(self._environ)

    def run_state(self, flight_id, run_number, deadline=None):
        """The run's state, or None when the platform knows no such run (or its Flight is gone)."""
        try:
            row = self.conn.fetchone(run_state_sql(flight_id, run_number), None, deadline)
        except md.StatementError:
            return None
        return normalize_state(row[0]) if row else None

    def cancel(self, flight_id, run_number, deadline=None):
        """Cancel one run. True when the platform took the cancel; False when it refused it (the run is terminal
        or unknown)."""
        try:
            self.conn.fetchall(cancel_sql(flight_id, run_number), None, deadline)
            return True
        except md.StatementError:
            return False

    def close(self):
        if self._conn is not None:
            self._conn.close()
            self._conn = None


__all__ = ["MotherDuckPlatform", "run_state_sql", "cancel_sql"]
