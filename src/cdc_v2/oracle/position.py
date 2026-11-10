"""WAL positions, and waiting until the destination has applied the source up to a position.

`docs/design/core.md` section 2 stores the applied position in `_cdc.stream_state` (one row per slot), written in
the same MotherDuck transaction as the data. The pipeline does not exist yet, so the lookup is one small function
(`applied_lsn_from_stream_state`) that callers can replace: `wait_for_applied(..., lookup=my_lookup)`.
"""
from __future__ import annotations

import re
import time
from typing import Any, Callable, Optional

_LSN = re.compile(r"^\s*([0-9A-Fa-f]{1,8})/([0-9A-Fa-f]{1,8})\s*$")


class PositionTimeout(Exception):
    pass


def parse_lsn(value: Any) -> Optional[int]:
    """'16/B374D848' -> int; ints and Decimals pass through; None stays None."""
    if value is None:
        return None
    if isinstance(value, bool):
        raise ValueError("not an LSN: %r" % (value,))
    if isinstance(value, int):
        return value
    if hasattr(value, "to_integral_value"):           # Decimal, HUGEINT through DECIMAL
        return int(value)
    m = _LSN.match(str(value))
    if m:
        return (int(m.group(1), 16) << 32) | int(m.group(2), 16)
    s = str(value).strip()
    if s.isdigit():
        return int(s)
    raise ValueError("not an LSN: %r" % (value,))


def format_lsn(value: int) -> str:
    return "%X/%X" % (value >> 32, value & 0xFFFFFFFF)


def applied_lsn_from_stream_state(md_conn) -> Optional[int]:
    """Default lookup: the highest `applied_lsn` in `_cdc.stream_state` (core.md section 2), or None if the table
    does not exist yet or is empty. Accepts the column as text ('X/Y') or as an integer."""
    try:
        rows = md_conn.execute("SELECT applied_lsn FROM _cdc.stream_state").fetchall()
    except Exception as e:                             # noqa: BLE001 - catalog errors mean "not there yet"
        if "does not exist" in str(e) or "not found" in str(e).lower():
            return None
        raise
    values = [parse_lsn(r[0]) for r in rows if r[0] is not None]
    return max(values) if values else None


def wait_for_applied(md_conn, target: Any, timeout: float = 120.0, poll: float = 0.5,
                     lookup: Callable[[Any], Optional[int]] = applied_lsn_from_stream_state,
                     sleep: Callable[[float], None] = time.sleep, clock: Callable[[], float] = time.monotonic) -> int:
    """Block until lookup(md_conn) >= target. Returns the applied position. Raises PositionTimeout.

    Each lookup is its own statement (autocommit), so it sees the newest committed state. The target is usually the
    position returned by a workload quiesce point; it is reached once the pipeline has applied a transaction that
    committed at or after that point (the 10 s heartbeat of SPEC 4.4 provides one when nothing else is written).
    """
    want = parse_lsn(target)
    deadline = clock() + timeout
    last = None
    while True:
        last = lookup(md_conn)
        if last is not None and last >= want:
            return last
        if clock() >= deadline:
            raise PositionTimeout("destination applied %s, waited for %s (%.0f s)" % (
                format_lsn(last) if last is not None else "nothing", format_lsn(want), timeout))
        sleep(poll)
