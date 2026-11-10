"""WAL positions. A position is a Python int (the 64-bit LSN); the text form `X/Y` exists only at the edges."""
from __future__ import annotations

import re

_TEXT = re.compile(r"^([0-9A-Fa-f]{1,8})/([0-9A-Fa-f]{1,8})$")
MAX = (1 << 64) - 1


def parse(text: str) -> int:
    """'16/B374D848' -> 0x16B374D848. Raises ValueError for anything that is not an LSN in text form."""
    m = _TEXT.match(text) if isinstance(text, str) else None
    if m is None:
        raise ValueError("not an LSN: %r" % (text,))
    return (int(m.group(1), 16) << 32) | int(m.group(2), 16)


def format(value: int) -> str:  # noqa: A001 - the name mirrors parse
    """0x16B374D848 -> '16/B374D848', as Postgres prints it."""
    if isinstance(value, bool) or not isinstance(value, int) or not 0 <= value <= MAX:
        raise ValueError("not a position: %r" % (value,))
    return "%X/%X" % (value >> 32, value & 0xFFFFFFFF)
