"""SQL text helpers: identifiers are always quoted, never formatted from untrusted text without these."""
from __future__ import annotations

import re

_SLOT_NAME = re.compile(r"^[a-z0-9_]{1,63}$")


def ident(name: str) -> str:
    """A quoted SQL identifier (double quotes, inner double quotes doubled). Refuses empty names, NUL and names
    over 63 bytes."""
    if not isinstance(name, str) or not name:
        raise ValueError("an identifier must be a non-empty string")
    if "\x00" in name:
        raise ValueError("an identifier cannot contain NUL")
    if len(name.encode("utf-8")) > 63:
        raise ValueError("an identifier is at most 63 bytes")
    return '"' + name.replace('"', '""') + '"'


def qualified(schema: str, name: str) -> str:
    return ident(schema) + "." + ident(name)


def literal(text: str) -> str:
    """A string literal for the few places where parameters cannot be used (replication commands)."""
    if "\x00" in text:
        raise ValueError("a literal cannot contain NUL")
    return "'" + text.replace("'", "''") + "'"


def check_slot_name(name: str) -> str:
    """Slot names are lower-case letters, digits and underscores (Postgres' own rule), at most 63 bytes."""
    if not isinstance(name, str) or not _SLOT_NAME.match(name):
        raise ValueError("bad replication slot name %r" % (name,))
    return name
