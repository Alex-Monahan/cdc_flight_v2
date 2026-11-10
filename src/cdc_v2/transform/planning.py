"""The table plan (plan C3): per column position the destination name and type, and how values are converted.

`plan(source_table, schema_frame=None, *, server_version_num)` builds a `TablePlan` from the catalog alone; a
SCHEMA frame of the engine is only checked against it (`PlanMismatch`), so the plan of the backfill and the plan of
the stream of one table are the same value with the same hash.
"""
from __future__ import annotations

import dataclasses
import hashlib
import json
from dataclasses import dataclass, field
from typing import Optional

from . import families as F
from .errors import PlanMismatch


@dataclass(frozen=True)
class ColumnPlan:
    position: int                    # the column's position i in the table (a<i>, b<i>, u<i>, k<i>; C2)
    attnum: int
    source_name: str
    source_type: str                 # as the catalog names it, with modifiers (format_type)
    base_type: str                   # the type that decides the mapping (a domain's base type)
    family: str
    dest_name: str
    native_type: str                 # DuckDB type of the native form
    wide_type: Optional[str]         # DuckDB type of the wide form; None: one form only
    form: str                        # 'native' | 'wide': the form the destination column is in
    key: bool
    unchanged_possible: bool         # the stream may send this column as "unchanged" (u<i>)
    stream_forms: tuple              # transport forms the engine may send (docs/design/engine.md 3)
    numeric: Optional[tuple] = None  # numeric: (width, scale, general)
    step: Optional[str] = None       # SPEC 8.3 seam: SQL over '{x}' (the converted value), applied on both paths

    @property
    def dest_type(self) -> str:
        return self.wide_type if self.form == "wide" else self.native_type

    @property
    def spec(self) -> F.TypeSpec:
        return F.TypeSpec(self.family, self.base_type, self.native_type, self.wide_type, self.stream_forms,
                          self.numeric)


@dataclass(frozen=True)
class MetaColumn:
    position: int
    name: str
    type: str
    key: bool
    unchanged_possible: bool


@dataclass(frozen=True)
class TableMeta:
    """What apply needs of a table (C3a): per column position the destination name and type, the key flag and the
    "unchanged possible" flag."""
    columns: tuple

    @property
    def has_key(self) -> bool:
        return any(c.key for c in self.columns)


@dataclass(frozen=True)
class TablePlan:
    schema: str
    table: str
    oid: int
    server_version_num: int
    columns: tuple                   # ColumnPlan per position, in position order
    has_key: bool
    key_positions: tuple             # in key order
    ambiguous: tuple                 # positions whose zero interval in the stream is ambiguous (plan C3)
    collisions: tuple                # groups of positions whose source names one DuckDB table cannot hold apart
    _hash: str = field(default="", compare=False, repr=False)

    @property
    def qualified(self) -> tuple:
        return (self.schema, self.table)

    @property
    def hash(self) -> str:
        """Stable hash of the plan: everything the destination table and the conversion depend on. The server
        version enters with its major number only (a minor update changes nothing here)."""
        if not self._hash:
            obj = {"schema": self.schema, "table": self.table, "oid": self.oid,
                   "server_major": self.server_version_num // 10000,
                   "columns": [[c.position, c.attnum, c.source_name, c.source_type, c.base_type, c.family,
                                c.dest_name, c.native_type, c.wide_type, c.form, c.key, c.unchanged_possible,
                                list(c.stream_forms), list(c.numeric) if c.numeric else None, c.step]
                               for c in self.columns],
                   "key": list(self.key_positions), "ambiguous": list(self.ambiguous),
                   "collisions": [list(g) for g in self.collisions]}
            h = hashlib.sha256(json.dumps(obj, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
            object.__setattr__(self, "_hash", h)
        return self._hash

    def __hash__(self):
        return hash(self.hash)

    def column(self, position: int) -> ColumnPlan:
        return self.columns[position]

    # -------------------------------------------------------------------------------------- forms and widening

    def widened(self, position: int) -> "TablePlan":
        """The plan with the column at `position` in its wide form (a no-op when it is wide already)."""
        c = self.columns[position]
        if c.wide_type is None:
            raise ValueError("column %d (%s) has one form only" % (position, c.family))
        if c.form == "wide":
            return self
        cols = list(self.columns)
        cols[position] = dataclasses.replace(c, form="wide")
        return dataclasses.replace(self, columns=tuple(cols), _hash="")

    def widen_sql(self, position: int, column_sql: Optional[str] = None) -> str:
        """SQL that carries an existing value of the native form into the wide form, for
        `ALTER TABLE t ALTER COLUMN c TYPE <wide type> USING <this>`. `column_sql` defaults to the quoted destination
        name."""
        c = self.columns[position]
        return F.widen_expression(c.spec, column_sql or quote_ident(c.dest_name))

    def with_step(self, position: int, step: Optional[str]) -> "TablePlan":
        """SPEC 8.3 seam (P4): a per-column SQL step over `{x}` (the converted value), applied on both paths before
        the result leaves the transform. The step must keep NULL as NULL and keep the destination type."""
        cols = list(self.columns)
        cols[position] = dataclasses.replace(cols[position], step=step)
        return dataclasses.replace(self, columns=tuple(cols), _hash="")

    # -------------------------------------------------------------------------------------- for other packages

    def table_meta(self) -> TableMeta:
        return TableMeta(tuple(MetaColumn(c.position, c.dest_name, c.dest_type, c.key, c.unchanged_possible)
                               for c in self.columns))

    def create_columns(self) -> tuple:
        """((quoted destination name, destination type), ...) in position order: the column list of a CREATE TABLE.
        Who creates the table is not the transform's business."""
        return tuple((quote_ident(c.dest_name), c.dest_type) for c in self.columns)

    def column_ddl(self) -> str:
        return ", ".join("%s %s" % pair for pair in self.create_columns())

    def arrow_schema(self, prefix: str = "a"):
        """The Arrow types the transform delivers for `<prefix><i>` (also for b<i> and k<i>)."""
        import pyarrow as pa
        return pa.schema([pa.field("%s%d" % (prefix, c.position), F.arrow_type(c.dest_type)) for c in self.columns])


def quote_ident(name: str) -> str:
    return '"%s"' % name.replace('"', '""')


# ------------------------------------------------------------------------------------------------ names


def _fold(name: str) -> str:
    """The comparison under which two destination column names count as one. DuckDB compares identifiers without
    regard to case; probed on DuckDB 1.5.6, ASCII letters fold ('A'/'a'), while 'É'/'é', the Kelvin sign/'k' and
    'ß'/'ss' are held apart. Unicode case folding is a superset of every probed collision, so a reported collision
    may be one that DuckDB would tolerate, never the other way round."""
    return name.casefold()


def dest_names(names) -> tuple:
    """(destination names, collision groups). A name is kept exactly unless it collides; within a collision group
    (positions whose names fold to the same text) the lowest position keeps its name and every other becomes
    `<name>_<position>`, with more `_` appended until it collides with nothing."""
    groups: dict = {}
    for i, n in enumerate(names):
        groups.setdefault(_fold(n), []).append(i)
    collisions = tuple(tuple(g) for g in groups.values() if len(g) > 1)
    out = list(names)
    taken = {_fold(n) for n in names}
    for g in collisions:
        for i in g[1:]:
            cand = "%s_%d" % (names[i], i)
            while _fold(cand) in taken:
                cand += "_"
            taken.add(_fold(cand))
            out[i] = cand
    return tuple(out), tuple(sorted(collisions))


# ------------------------------------------------------------------------------------------------ plan()


def plan(source_table, schema_frame=None, *, server_version_num: int) -> TablePlan:
    """The plan of one source table (`cdc_v2.pg.catalog.SourceTable`). Positions are the table's live columns in
    `attnum` order (the engine's positions). With `schema_frame` (the engine's SCHEMA frame) the frame is checked
    against the catalog: column count, names, transport forms, key; `PlanMismatch` names every difference."""
    if not isinstance(server_version_num, int) or server_version_num < 90000:
        raise ValueError("server_version_num must be an int such as 180006")
    names = [c.name for c in source_table.columns]
    dnames, collisions = dest_names(names)
    key_attnums = tuple(source_table.key_attnums)
    pos_by_attnum = {c.attnum: i for i, c in enumerate(source_table.columns)}
    key_positions = tuple(pos_by_attnum[a] for a in key_attnums if a in pos_by_attnum)
    cols = []
    for i, c in enumerate(source_table.columns):
        spec = F.resolve(c)
        cols.append(ColumnPlan(
            position=i, attnum=c.attnum, source_name=c.name, source_type=c.type_name, base_type=spec.base_type,
            family=spec.family, dest_name=dnames[i], native_type=spec.native_type, wide_type=spec.wide_type,
            form="native", key=i in key_positions, unchanged_possible=bool(c.toastable),
            stream_forms=spec.stream_forms, numeric=spec.numeric))
    ambiguous = tuple(c.position for c in cols if c.family == "interval") if server_version_num >= 170000 else ()
    p = TablePlan(schema=source_table.schema, table=source_table.name, oid=source_table.oid,
                  server_version_num=server_version_num, columns=tuple(cols), has_key=bool(key_positions),
                  key_positions=key_positions, ambiguous=ambiguous, collisions=collisions)
    if schema_frame is not None:
        check_frame(p, schema_frame)
    return p


def check_frame(p: TablePlan, frame) -> None:
    """Raise PlanMismatch unless the SCHEMA frame describes the same columns as the plan."""
    diffs = []
    if (frame.schema, frame.table) != (p.schema, p.table):
        diffs.append((None, "table", "%s.%s" % (p.schema, p.table), "%s.%s" % (frame.schema, frame.table)))
    fcols = list(frame.columns)
    if len(fcols) != len(p.columns):
        diffs.append((None, "column count", len(p.columns), len(fcols)))
    for i, fc in enumerate(fcols):
        if fc.position != i:
            diffs.append((i, "position", i, fc.position))
        if i >= len(p.columns):
            continue
        c = p.columns[i]
        if fc.name != c.source_name:
            diffs.append((i, "name", c.source_name, fc.name))
        if fc.form not in c.stream_forms:
            diffs.append((i, "transport form", "/".join(c.stream_forms), fc.form))
        if bool(fc.key) != c.key:
            diffs.append((i, "key", c.key, bool(fc.key)))
    if bool(frame.has_key) != p.has_key:
        diffs.append((None, "has key", p.has_key, bool(frame.has_key)))
    if tuple(frame.key_positions) != p.key_positions:
        diffs.append((None, "key positions", p.key_positions, tuple(frame.key_positions)))
    if diffs:
        raise PlanMismatch(p.qualified, diffs)
