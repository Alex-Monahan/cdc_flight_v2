"""Apply: the statements that change one destination table for one change set, inside a transaction that the
caller opened and will commit (contract C3a, design section 6; the full account is docs/design/apply.md).

    meta = TableMeta(columns=[Column(position, name, type, key, unchanged_possible), ...])
    target = Target(schema, table)
    counts = apply_group(tx, target, LocalChangeSet(rows, transactions, meta), mode="stream")
    truncate(tx, target)

`tx` is an open lane transaction (contract C4): apply calls `tx.execute`, `tx.fetch_arrow` and `tx.register`, and
never begins, commits, rolls back or fences.
"""
from .changeset import ChangeSetError, LocalChangeSet, RelationChangeSet
from .core import (ApplyCounts, ApplyInvariantError, ApplySettings, ApplySettingsError, ApplySpillFull, apply_group,
                   reduce_duplicates, truncate)
from .meta import Column, TableMeta, Target

__all__ = ["Column", "TableMeta", "Target", "LocalChangeSet", "RelationChangeSet", "ChangeSetError", "ApplyCounts",
           "ApplyInvariantError", "ApplySettings", "ApplySettingsError", "ApplySpillFull", "apply_group", "truncate", "reduce_duplicates"]
