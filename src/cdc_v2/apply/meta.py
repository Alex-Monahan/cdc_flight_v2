"""What apply knows about a destination table: its columns by position (contract C3a `meta`) and its name."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

from .types import DType, parse, quote_ident

__all__ = ["Column", "TableMeta", "Target"]


@dataclass(frozen=True)
class Column:
    """One column. `position` names the change-set columns (`a<position>`, `b<…>`, `u<…>`, `k<…>`); `name` is the
    destination column name and only ever appears as a quoted identifier of the target; `type` is the destination
    type as DuckDB SQL."""
    position: int
    name: str
    type: str
    key: bool = False
    unchanged_possible: bool = False
    dtype: DType = field(init=False, repr=False, compare=False)

    def __post_init__(self):
        if isinstance(self.position, bool) or not isinstance(self.position, int) or self.position < 0:
            raise ValueError("a column position is an integer >= 0")
        if not isinstance(self.name, str) or self.name == "":
            raise ValueError("a destination column name is a non-empty string")
        object.__setattr__(self, "dtype", parse(self.type))

    @property
    def a(self) -> str:
        return "a%d" % self.position

    @property
    def b(self) -> str:
        return "b%d" % self.position

    @property
    def u(self) -> str:
        return "u%d" % self.position

    @property
    def k(self) -> str:
        return "k%d" % self.position

    @property
    def x(self) -> str:
        """Name of this column's exact key in apply's own relations."""
        return "x%d" % self.position

    @property
    def qname(self) -> str:
        return quote_ident(self.name)


@dataclass(frozen=True)
class TableMeta:
    """Per column position: destination name and type, key flag, "unchanged possible" flag. Frozen; `has_key`
    is derived from the key flags.

    Refused: duplicate positions; two names that differ only by case (DuckDB compares identifiers without regard
    to case). Any other string is a valid name."""
    columns: tuple

    def __post_init__(self):
        cols = tuple(self.columns)
        if not cols:
            raise ValueError("a table has at least one column")
        if not all(isinstance(c, Column) for c in cols):
            raise TypeError("TableMeta takes Column objects")
        cols = tuple(sorted(cols, key=lambda c: c.position))
        positions = [c.position for c in cols]
        if len(set(positions)) != len(positions):
            raise ValueError("two columns with one position")
        folded = [c.name.casefold() for c in cols]
        if len(set(folded)) != len(folded):
            raise ValueError("two destination column names differ only by case")
        object.__setattr__(self, "columns", cols)

    @property
    def has_key(self) -> bool:
        return any(c.key for c in self.columns)

    @property
    def key_columns(self) -> tuple:
        return tuple(c for c in self.columns if c.key)

    @property
    def value_columns(self) -> tuple:
        return tuple(c for c in self.columns if not c.key)

    @property
    def unchanged_columns(self) -> tuple:
        """Columns that can carry "unchanged" (`u<i>` exists), keys excluded: a key value is always present."""
        return tuple(c for c in self.columns if c.unchanged_possible and not c.key)

    def column(self, position: int) -> Column:
        for c in self.columns:
            if c.position == position:
                return c
        raise KeyError(position)


@dataclass(frozen=True)
class Target:
    """The destination table: schema and table name (C0: the source's names). Always quoted."""
    schema: str
    table: str

    def __post_init__(self):
        for v in (self.schema, self.table):
            if not isinstance(v, str) or v == "":
                raise ValueError("schema and table are non-empty strings")

    def sql(self) -> str:
        return "%s.%s" % (quote_ident(self.schema), quote_ident(self.table))

    def __str__(self) -> str:                       # for messages: names only, never values
        return "%s.%s" % (self.schema, self.table)
