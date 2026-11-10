"""The destination's applied position, and waiting until it reaches a source position.

Positions are ints (C0); text `X/Y` is parsed and printed only through `cdc_v2.lsn`. Package S owns
`_cdc.stream_state` and provides the real reader, `stream.applied_position(conn, slot_name) -> int | None`
(contract C8). Until it is merged, `stand_in_applied_position` has the same signature: it reads `applied_lsn` of
the row of the named slot (column `slot_name`). Every caller takes the reader as a parameter, so S's function
replaces the stand-in without a change here.
"""
from __future__ import annotations

from typing import Any, Callable, Optional

from cdc_v2 import lsn
from cdc_v2.deadline import Deadline, DeadlineExceeded

AppliedPosition = Callable[[Any, str], Optional[int]]


def as_position(value: Any) -> Optional[int]:
    """An int position from an int, a Decimal/HUGEINT, or text `X/Y`; None stays None."""
    if value is None:
        return None
    if isinstance(value, bool):
        raise ValueError("not a position: %r" % (value,))
    if isinstance(value, int):
        return value
    if hasattr(value, "to_integral_value"):
        return int(value)
    return lsn.parse(str(value).strip())


def stand_in_applied_position(md_conn, slot_name: str) -> Optional[int]:
    """Stand-in for S's reader: `applied_lsn` of the row `slot_name` in `_cdc.stream_state`, or None when the table
    or the row does not exist yet. Accepts the column as UBIGINT (C0) or as text `X/Y`."""
    try:
        row = md_conn.execute("SELECT applied_lsn FROM _cdc.stream_state WHERE slot_name = ?", [slot_name]).fetchone()
    except Exception as e:                             # noqa: BLE001 - a missing table means "not there yet"
        if "does not exist" in str(e) or "not found" in str(e).lower():
            return None
        raise
    return as_position(row[0]) if row else None


def wait_for_applied(md_conn, target: int, deadline: Deadline, slot_name: str,
                     applied_position: AppliedPosition = stand_in_applied_position, poll: float = 0.5) -> int:
    """Block until applied_position(md_conn, slot_name) >= target. Returns the applied position; raises
    `DeadlineExceeded` when the deadline passes first.

    Each read is its own statement (autocommit), so it sees the newest committed state. The target is a position
    from `Workload.quiesce()` / `pause()`; it is reached once the pipeline has applied a transaction that committed
    at or after it (the 10 s heartbeat of SPEC 4.4 provides one when nothing else is written).
    """
    if isinstance(target, str):
        target = lsn.parse(target)
    last = None
    while True:
        last = applied_position(md_conn, slot_name)
        if last is not None and last >= target:
            return last
        deadline.sleep(poll, "destination applied %s, waited for %s (slot %s)" % (
            lsn.format(last) if last is not None else "nothing", lsn.format(target), slot_name))


__all__ = ["AppliedPosition", "DeadlineExceeded", "as_position", "stand_in_applied_position", "wait_for_applied"]
