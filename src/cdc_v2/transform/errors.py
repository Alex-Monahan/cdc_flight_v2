"""Errors of the transform (plan C3). None of them ever carries a source value.

Every attribute is metadata: the table's schema and name, column positions, a `seq`, a row index and a constant
reason chosen by this package. The DuckDB error that led to an error here is never chained (`__cause__` and
`__context__` stay empty): DuckDB's own error texts quote the value they failed on.
"""
from __future__ import annotations

from typing import Optional


class TransformError(Exception):
    """Base class of the transform's errors."""

    def __init__(self, message: str):
        super().__init__(message)
        self.__cause__ = None
        self.__context__ = None
        self.__suppress_context__ = True


def _table(table) -> str:
    schema, name = table
    return "%s.%s" % (schema, name)


class PlanMismatch(TransformError):
    """The engine's SCHEMA frame and the catalog disagree (column positions, names, transport form, key).
    `differences` lists (position or None, field, catalog side, frame side); all of it is metadata."""

    def __init__(self, table, differences):
        self.table = tuple(table)
        self.differences = tuple(differences)
        shown = "; ".join("%s %s: catalog %r, frame %r" % ("table" if p is None else "position %d" % p, f, c, s)
                          for p, f, c, s in self.differences[:8])
        more = " (+%d more)" % (len(self.differences) - 8) if len(self.differences) > 8 else ""
        super().__init__("plan mismatch for %s: %s%s" % (_table(self.table), shown, more))


class WideningNeeded(TransformError):
    """A batch holds a value that the current form of these columns cannot hold. Nothing of the batch was
    converted. The caller widens the destination columns, takes `plan.widened(...)` for each position and converts
    the batch again."""

    def __init__(self, table, positions):
        self.table = tuple(table)
        self.positions = tuple(sorted(set(int(p) for p in positions)))
        super().__init__("table %s: column position(s) %s need their wide form"
                         % (_table(self.table), ", ".join(str(p) for p in self.positions)))


class ConversionError(TransformError):
    """A value that the plan's form should hold could not be converted: a bug, or an input that breaks the contract
    (C2 transport form, or the text form of the read sessions). `position` and `seq` name the place (`seq` is None
    for a backfill batch, which has none; `row` is the row index in the batch); `reason` is a constant text."""

    def __init__(self, table, position: Optional[int], seq: Optional[int], *, row: Optional[int] = None,
                 column: Optional[str] = None, reason: str = "value could not be converted"):
        self.table = tuple(table)
        self.position = position
        self.seq = seq
        self.row = row
        self.column = column
        self.reason = reason
        where = []
        if column is not None:
            where.append("column %s" % column)
        elif position is not None:
            where.append("position %d" % position)
        if seq is not None:
            where.append("seq %d" % seq)
        if row is not None:
            where.append("row %d" % row)
        super().__init__("table %s, %s: %s" % (_table(self.table), ", ".join(where) or "unknown place", reason))


__all__ = ["TransformError", "PlanMismatch", "WideningNeeded", "ConversionError"]
