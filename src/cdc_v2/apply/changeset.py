"""The two forms of a change set (contract C3a).

Local form: `rows` (Arrow, columns `seq`, `op`, `a<i>`, `b<i>`, `u<i>`, `k<i>` in destination types, plus `txn`),
`transactions` (per `txn`: `xid`, `commit_lsn`, `commit_ts`) and `meta`. Order (`txn`, `seq`).

Relation form: a `SELECT` (text and parameters) over rows already in MotherDuck, with `commit_lsn`, `xid`,
`commit_ts` in place of `txn`, and `meta`. Order (`commit_lsn`, `seq`). Apply's working tables for this form are
created in a schema `<scratch_prefix>_<random>` inside the caller's transaction and dropped (with the schema)
before `apply_group` returns.

Neither form is reduced: every change is there, and apply reduces.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional, Sequence

import pyarrow as pa

from .meta import TableMeta

__all__ = ["LocalChangeSet", "RelationChangeSet", "ChangeSetError"]


class ChangeSetError(ValueError):
    """The change set does not have the shape of contract C3a (a caller's bug, not a divergence)."""


def _required_columns(meta: TableMeta) -> list:
    cols = ["seq", "op"] + [c.a for c in meta.columns]
    if meta.has_key:
        cols += [c.k for c in meta.key_columns]
    else:
        cols += [c.b for c in meta.columns]
    return cols


@dataclass(frozen=True)
class LocalChangeSet:
    rows: pa.Table
    transactions: Optional[pa.Table]
    meta: TableMeta

    def __post_init__(self):
        if isinstance(self.rows, pa.RecordBatch):
            object.__setattr__(self, "rows", pa.Table.from_batches([self.rows]))
        if not isinstance(self.rows, pa.Table):
            raise ChangeSetError("rows is an Arrow table")
        if not isinstance(self.meta, TableMeta):
            raise ChangeSetError("meta is a TableMeta")
        names = set(self.rows.column_names)
        missing = [c for c in _required_columns(self.meta) + ["txn"] if c not in names]
        if missing:
            raise ChangeSetError("rows lack the columns %s" % ", ".join(missing))

    @property
    def order_column(self) -> str:
        return "txn"

    def u_columns(self) -> list:
        """The non-key columns whose `u<i>` column is present. Presence wins over the meta flag: a `u<i>` that the
        meta does not expect is still honoured, so that an absent value is never written as NULL."""
        names = set(self.rows.column_names)
        return [c for c in self.meta.value_columns if c.u in names]


@dataclass(frozen=True)
class RelationChangeSet:
    sql: str
    params: Sequence = ()
    meta: TableMeta = None
    scratch_prefix: str = "_cdc_apply"
    u_present: Optional[Sequence[int]] = None        # positions whose u<i> column exists; None: every unchanged column

    def __post_init__(self):
        if not isinstance(self.sql, str) or not self.sql.strip():
            raise ChangeSetError("the relation form needs a SELECT")
        if not isinstance(self.meta, TableMeta):
            raise ChangeSetError("meta is a TableMeta")
        if (not isinstance(self.scratch_prefix, str) or not self.scratch_prefix.lower().startswith("_cdc")
                or not self.scratch_prefix.replace("_", "").isalnum()):
            raise ChangeSetError("apply's working schema has a name that starts with _cdc (letters, digits, _)")
        object.__setattr__(self, "params", tuple(self.params or ()))

    @property
    def order_column(self) -> str:
        return "commit_lsn"

    def u_columns(self) -> list:
        if self.u_present is None:
            return list(self.meta.unchanged_columns)
        keep = set(self.u_present)
        return [c for c in self.meta.unchanged_columns if c.position in keep]
