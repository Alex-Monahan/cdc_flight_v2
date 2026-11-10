"""The transform (plan C3): the one place where a source value becomes a destination value.

    from cdc_v2 import transform
    plan = transform.plan(source_table, schema_frame=None, server_version_num=180006)
    out = transform.stream(plan, rows)       # engine transport form (C2 ROWS) -> destination types
    out = transform.backfill(plan, rows)     # Postgres text (read sessions, docs/design/source.md 1) -> same
    mask = transform.ambiguous(plan, rows)   # plan C3 "Ambiguous zero interval"; None without such a column

The module functions use the calling thread's own `Transformer` (one local DuckDB connection each); a caller that
needs its own settings (spill area, memory) creates a `Transformer(TransformSettings(...))` and calls its methods.
`docs/design/transform.md` is the full account; `docs/type_mapping.md` the type table.
"""
from __future__ import annotations

from .convert import TransformSettings, Transformer, default
from .convert import ambiguous as _ambiguous
from .errors import ConversionError, PlanMismatch, TransformError, WideningNeeded
from .families import UNCONSTRAINED_NUMERIC_SCALE, arrow_type
from .planning import ColumnPlan, MetaColumn, TableMeta, TablePlan, dest_names, plan


def stream(plan: TablePlan, rows):
    """A ROWS batch in transport form -> destination types, on the calling thread's transformer."""
    return default().stream(plan, rows)


def backfill(plan: TablePlan, rows):
    """Postgres text (a<i>) -> destination types, on the calling thread's transformer."""
    return default().backfill(plan, rows)


def ambiguous(plan: TablePlan, rows):
    """Plan C3: mask of rows whose zero interval is ambiguous, or None when the plan has no such column."""
    return _ambiguous(plan, rows)


__all__ = ["plan", "stream", "backfill", "ambiguous", "Transformer", "TransformSettings", "TablePlan", "ColumnPlan",
           "TableMeta", "MetaColumn", "TransformError", "PlanMismatch", "WideningNeeded", "ConversionError",
           "UNCONSTRAINED_NUMERIC_SCALE", "arrow_type", "dest_names"]
