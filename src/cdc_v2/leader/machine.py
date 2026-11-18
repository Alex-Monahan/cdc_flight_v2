"""The leader machine (SPEC 1.9; design section 9; contract C5).

Two states: `leading` and `released`. Its row is the one row of `_cdc.leader`, which callers may not update (the
layer refuses it), so `state.transition` cannot move it:

- the take-over (`flights/cdc_v2/main.py`, which carries its own copy of this machine) moves it
  `released -> leading` or `leading -> leading` (epoch + 1) and appends this machine's row to `_cdc.transitions` in
  the same transaction when that table exists; at a first run it does not yet, and `cdc_v2.leader.startup`
  appends the row after the migrations;
- the release (`md.schema.release`, in a fenced `control`-lane transaction) moves it `leading -> released`, and
  `release_row` appends the row in that transaction.

Why a run ended (superseded, failed, released) is the cause of the next transition. The enumeration of SPEC 1.9
runs over the real take-over and release (`tests/leader/test_machine.py`), because D's helper writes the state
with `state.transition`; the same helper also runs over a mirror table of this declaration.
"""
from __future__ import annotations

from cdc_v2 import state

LEADER = state.declare(state.Machine(
    name="leader",
    states=("leading", "released"),
    transitions=(("released", "leading"), ("leading", "leading"), ("leading", "released")),
    table="_cdc.leader",
    key_columns=(),
    state_column="state",
    creators=("control",),                 # never created by `state.create`: the leader tables come with the row
    initial=("released",),
    description="The one leader record (SPEC 4.2). Moved by the bootstrap's take-over (released -> leading, "
                "leading -> leading with epoch + 1) and by md.schema.release (leading -> released); the row of "
                "_cdc.transitions is written in the same transaction (package L).",
))

#: Does the take-over of ``epoch`` have its row in `_cdc.transitions`? Parameter: the epoch.
HAS_TAKEOVER_ROW_SQL = ("SELECT count(*) FROM _cdc.transitions WHERE machine = 'leader' AND epoch = ? "
                        "AND to_state = 'leading'")
READ_LEADER_SQL = "SELECT epoch, state, run_id FROM _cdc.leader"


def record(tx, from_state, to_state, cause, detail=None):
    """Append the leader machine's row to `_cdc.transitions` inside ``tx`` (a control-lane transaction)."""
    state.record(tx, LEADER, (), from_state, to_state, cause, detail=detail)


__all__ = ["LEADER", "record", "HAS_TAKEOVER_ROW_SQL", "READ_LEADER_SQL"]
