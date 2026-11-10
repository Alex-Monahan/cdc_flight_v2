"""The transformer: one local DuckDB connection that converts batches by a plan (plan C3).

    t = Transformer(TransformSettings(threads=1, memory_limit="512MB", temp_directory=..., ...))
    out = t.stream(plan, rows)        # a ROWS batch (C2) in transport form -> destination types
    out = t.backfill(plan, rows)      # a<i> as Postgres text -> destination types
    mask = t.ambiguous(plan, rows)

One transformer per thread; nothing is shared between two of them. The connection is in-memory, has external access
switched off (no ATTACH, no extension loading: it can never reach MotherDuck), `TimeZone=UTC` and DuckDB's lossless
Arrow conversion (UUID, TIMETZ).

Per (plan, direction, input schema) one query is compiled and cached as text. It is not kept as a prepared
statement: a prepared statement keeps reading the Arrow batch that was registered when it was prepared, also after
the name is registered again (measured on DuckDB 1.5.6: the second batch returned the first batch's rows). A column
whose input already has the destination layout is not touched; only the other columns go through DuckDB, in one
statement.
"""
from __future__ import annotations

import collections
import re
import threading
from dataclasses import dataclass
from typing import Optional

import duckdb
import pyarrow as pa
import pyarrow.compute as pc

from . import families as F
from .errors import ConversionError, WideningNeeded
from .planning import TablePlan

_COL = re.compile(r"^([abk])(\d+)$")
_MARK = re.compile(r"cdc_v2:(misfit|bad):([abk][0-9]+)")
ZERO_INTERVAL_TEXT = "0 months 0 days 0 microseconds"
_IN = "cdc_v2_in"
_RN = "cdc_v2_rn"


@dataclass(frozen=True)
class TransformSettings:
    threads: int = 1
    memory_limit: str = "512MB"
    temp_directory: Optional[str] = None             # the spill area (package S sets it, SPEC 5.4)
    max_temp_directory_size: Optional[str] = None
    max_cached_queries: int = 256


@dataclass
class _Compiled:
    plan: TablePlan
    direction: str
    names: tuple                      # output column names, in input order
    actions: tuple                    # per output column: (kind, conv)
    sql_names: tuple                  # columns computed by the query, in query output order
    exprs: tuple                      # their expressions over `src`
    src: Optional[str]                # the layered FROM source over the registered input
    misfit_exprs: tuple               # bool_or(...) per widenable column
    misfit_positions: tuple           # position per misfit expression

    @property
    def sql(self) -> str:
        return "SELECT %s FROM %s" % (", ".join('%s AS "%s"' % (e, n) for n, e in zip(self.sql_names, self.exprs)),
                                      self.src)


class Transformer:
    def __init__(self, settings: TransformSettings = TransformSettings()):
        self.settings = settings
        config = {"threads": settings.threads, "memory_limit": settings.memory_limit,
                  "autoload_known_extensions": False, "autoinstall_known_extensions": False,
                  "arrow_lossless_conversion": True, "enable_external_access": False}
        if settings.temp_directory:
            config["temp_directory"] = settings.temp_directory
        if settings.max_temp_directory_size:
            config["max_temp_directory_size"] = settings.max_temp_directory_size
        self.conn = duckdb.connect(":memory:", config=config)
        self.conn.execute("SET TimeZone = 'UTC'")
        self._cache: "collections.OrderedDict[tuple, _Compiled]" = collections.OrderedDict()
        self._n = 0

    # ------------------------------------------------------------------------------------------------ API

    def stream(self, plan: TablePlan, rows: pa.RecordBatch) -> pa.RecordBatch:
        """A ROWS batch in the engine's transport form -> the same columns in destination types. `seq`, `op` and
        `u<i>` pass through untouched; a value under a set `u<i>` is never looked at."""
        return self._run(plan, rows, "stream")

    def backfill(self, plan: TablePlan, rows: pa.RecordBatch) -> pa.RecordBatch:
        """A batch whose `a<i>` are the texts Postgres prints under the read sessions' settings (real NULLs) ->
        destination types."""
        return self._run(plan, rows, "backfill")

    def ambiguous(self, plan: TablePlan, rows: pa.RecordBatch):
        return ambiguous(plan, rows)

    def close(self):
        try:
            self.conn.close()
        except Exception:                 # noqa: BLE001
            pass

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    # ------------------------------------------------------------------------------------------------ compile

    def _compile(self, plan: TablePlan, rows: pa.RecordBatch, direction: str) -> _Compiled:
        key = (plan.hash, direction, tuple((f.name, str(f.type)) for f in rows.schema))
        c = self._cache.get(key)
        if c is not None:
            self._cache.move_to_end(key)
            return c
        names, actions, sql_cols, misfits = [], [], [], []
        layer_items: list = []
        for f in rows.schema:
            m = _COL.match(f.name)
            names.append(f.name)
            if not m:
                actions.append(("pass", None))           # seq, op, u<i>, txn, ...
                continue
            pos = int(m.group(2))
            if pos >= len(plan.columns):
                raise ConversionError(plan.qualified, pos, None, column=f.name,
                                      reason="the batch has a column the plan does not know")
            col = plan.columns[pos]
            in_type = f.type
            if direction == "stream" and in_type not in [F.FORM_ARROW[x] for x in col.stream_forms]:
                raise ConversionError(plan.qualified, pos, None, column=f.name,
                                      reason="transport type %s is not one of the plan's forms" % in_type)
            if direction == "backfill" and not pa.types.is_string(in_type):
                raise ConversionError(plan.qualified, pos, None, column=f.name,
                                      reason="a backfill column must be text, not %s" % in_type)
            cv = F.conv(col.spec, direction, col.form, in_type)
            if col.step:
                cv = _with_step(cv, col.step, col.dest_type)
            if cv.kind == "pass" and in_type != cv.out_type:
                raise ConversionError(plan.qualified, pos, None, column=f.name,
                                      reason="input type %s does not match %s" % (in_type, cv.out_type))
            actions.append((cv.kind, cv))
            if cv.kind != "sql":
                continue
            p = "__%s_" % f.name
            fmt = {"x": '"%s"' % f.name, "p": p, "c": f.name}
            for depth, layer in enumerate(cv.layers):
                while len(layer_items) <= depth:
                    layer_items.append([])
                for nm, tmpl in layer:
                    layer_items[depth].append("%s AS %s%s" % (tmpl.format(**fmt), p, nm))
            final = cv.final.format(**fmt)
            if direction == "stream" and f.name.startswith("a") and ("u%d" % pos) in rows.schema.names:
                final = 'CASE WHEN "u%d" THEN NULL ELSE %s END' % (pos, final)
            sql_cols.append((f.name, final))
            if cv.misfit:
                misfits.append((pos, "bool_or(%s)" % cv.misfit.format(**fmt)))
        src = None
        if sql_cols:
            src = _IN
            for items in layer_items:
                src = "(SELECT *, %s FROM %s)" % (", ".join(items), src)
        c = _Compiled(plan, direction, tuple(names), tuple(actions), tuple(n for n, _e in sql_cols),
                      tuple(e for _n, e in sql_cols), src, tuple(e for _p, e in misfits),
                      tuple(p for p, _e in misfits))
        self._cache[key] = c
        while len(self._cache) > self.settings.max_cached_queries:
            self._cache.popitem(last=False)
        return c

    # ------------------------------------------------------------------------------------------------ run

    def _run(self, plan: TablePlan, rows: pa.RecordBatch, direction: str) -> pa.RecordBatch:
        if isinstance(rows, pa.Table):
            rows = rows.combine_chunks()
            rows = rows.to_batches()[0] if rows.num_rows else pa.RecordBatch.from_pylist([], schema=rows.schema)
        if direction == "backfill" and any(pa.types.is_large_string(f.type) for f in rows.schema):
            rows = pa.RecordBatch.from_arrays([a.cast(pa.string()) if pa.types.is_large_string(a.type) else a
                                               for a in rows.columns], names=rows.schema.names)
        c = self._compile(plan, rows, direction)
        computed = self._execute(c, rows) if c.src is not None else {}
        out = []
        for name, (kind, cv), arr in zip(c.names, c.actions, rows.columns):
            if kind == "pass":
                out.append(arr)
            elif kind == "view":
                out.append(_view(plan, rows, name, cv, arr))
            else:
                res = computed[name]
                if res.type == F.BOOL8_ARROW:
                    res = _from_bool8(res)
                if cv.post == "time64":
                    res = res.view(pa.time64("us"))
                elif cv.post == "timetz":
                    storage = pa.Array.from_buffers(pa.binary(8), len(res), [res.buffers()[0], res.buffers()[1]],
                                                    offset=res.offset)
                    res = pa.ExtensionArray.from_storage(F.TIMETZ_ARROW, storage)
                if res.type != cv.out_type:
                    raise ConversionError(plan.qualified, int(name[1:]), None, column=name,
                                          reason="DuckDB returned %s instead of %s" % (res.type, cv.out_type))
                out.append(res)
        return pa.RecordBatch.from_arrays(out, names=list(c.names))

    def _execute(self, c: _Compiled, rows: pa.RecordBatch) -> dict:
        marker = None
        failed = False
        result = None
        self.conn.register(_IN, rows)
        try:
            try:
                result = self.conn.execute(c.sql).to_arrow_table(batch_size=max(len(rows), 1))
            except duckdb.Error as e:
                # keep only our own marker; DuckDB's text may quote a value and is dropped here
                m = _MARK.search(str(e))
                marker = (m.group(1), m.group(2)) if m else None
                failed = True
        finally:
            try:
                self.conn.unregister(_IN)
            except duckdb.Error:
                pass
        if failed:
            self._diagnose(c, rows, marker)              # raises; outside any handler, so nothing is chained
        out = {}
        for i, name in enumerate(c.sql_names):
            col = result.column(i)
            if col.num_chunks == 0:
                out[name] = pa.array([], type=col.type)
            else:
                out[name] = col.chunk(0) if col.num_chunks == 1 else col.combine_chunks()
        return out

    def _diagnose(self, c: _Compiled, rows: pa.RecordBatch, marker) -> None:
        """The conversion failed. Decide, without a value reaching any message, whether columns need their wide
        form (WideningNeeded, every such position at once) or a value breaks the contract (ConversionError with
        the first failing column and row, found by running parts of the statement on halves of the batch)."""
        plan = c.plan
        n = len(rows)
        numbered = pa.RecordBatch.from_arrays(list(rows.columns) + [pa.array(range(n), pa.int64())],
                                              names=list(rows.schema.names) + [_RN])
        self.conn.register(_IN, numbered)
        try:
            if c.misfit_exprs:
                flags = self._try_fetch("SELECT %s FROM %s" % (", ".join(c.misfit_exprs), c.src))
                if flags is not None:
                    positions = sorted({p for p, f in zip(c.misfit_positions, flags) if f})
                    if positions:
                        raise WideningNeeded(plan.qualified, positions)
            order = list(range(len(c.exprs)))
            if marker is not None and marker[1] in c.sql_names:
                k = c.sql_names.index(marker[1])
                order.remove(k)
                order.insert(0, k)
            for k in order:
                name, expr = c.sql_names[k], c.exprs[k]
                def q(a, b):
                    return "SELECT %s FROM %s WHERE %s >= %d AND %s < %d" % (expr, c.src, _RN, a, _RN, b)
                if self._try_fetch(q(0, n)) is not None:
                    continue
                lo, hi = 0, n
                while hi - lo > 1:
                    mid = (lo + hi) // 2
                    if self._try_fetch(q(lo, mid)) is None:
                        hi = mid
                    else:
                        lo = mid
                raise ConversionError(plan.qualified, int(name[1:]), _seq(rows, lo), row=lo, column=name,
                                      reason="value could not be converted to %s"
                                      % plan.columns[int(name[1:])].dest_type)
        finally:
            try:
                self.conn.unregister(_IN)
            except duckdb.Error:
                pass
        raise ConversionError(plan.qualified, None, None, reason="conversion failed (no single value found)")

    def _try_fetch(self, sql):
        ok = True
        try:
            r = self.conn.execute(sql).fetchall()
        except duckdb.Error:
            ok = False
        if not ok:
            return None
        return r[0] if r else ()


def _view(plan, rows, name, cv, arr):
    lo, hi = cv.view_range
    if arr.null_count < len(arr):
        mm = pc.min_max(arr)
        if mm["min"].as_py() < lo or mm["max"].as_py() > hi:
            bad = pc.or_(pc.less(arr, lo), pc.greater(arr, hi))
            row = pc.index(bad, True).as_py()
            raise ConversionError(plan.qualified, int(name[1:]), _seq(rows, row), row=row, column=name,
                                  reason="value outside the range of the transport form")
    return arr.view(cv.out_type)


def _from_bool8(arr):
    """DuckDB's lossless Arrow export gives BOOLEAN as the `arrow.bool8` extension (one byte per value); the
    transform hands on the standard bit-packed boolean, which is also what the stream delivers."""
    return pc.not_equal(arr.storage, 0)


def _seq(rows: pa.RecordBatch, row):
    if "seq" in rows.schema.names and row is not None and 0 <= row < len(rows):
        return rows.column(rows.schema.get_field_index("seq"))[row].as_py()
    return None


def _with_step(cv: F.Conv, step: str, dest_type: str) -> F.Conv:
    """SPEC 8.3 seam: `step` is SQL in which `{x}` stands for the converted value (no other braces). A column that
    is passed through goes through SQL when it has a step."""
    if "{" in step.replace("{x}", ""):
        raise ValueError("a step may contain no braces other than {x}")
    if cv.kind == "sql" and not cv.post:
        return F.Conv("sql", cv.in_type, cv.out_type, cv.layers,
                      "CAST((%s) AS %s)" % (step.replace("{x}", "(%s)" % cv.final), dest_type), cv.misfit)
    if cv.kind == "pass":
        return F.Conv("sql", cv.in_type, cv.out_type, (), "CAST((%s) AS %s)" % (step, dest_type))
    raise ValueError("a step on this column kind is not supported in P3")


def ambiguous(plan: TablePlan, rows: pa.RecordBatch):
    """Plan C3: the rows of a stream batch (transport form) whose new image `a<i>` - or old key `k<i>` - holds the
    zero interval in a column of `plan.ambiguous`, while its `u<i>` (if any) is not set. None when the plan has no
    such column. Vectorised; needs no DuckDB."""
    if not plan.ambiguous:
        return None
    names = rows.schema.names
    mask = pa.array([False] * len(rows), pa.bool_())
    for pos in plan.ambiguous:
        u = rows.column(names.index("u%d" % pos)) if ("u%d" % pos) in names else None
        for prefix in ("a", "k"):
            name = "%s%d" % (prefix, pos)
            if name not in names:
                continue
            col = rows.column(names.index(name))
            if not pa.types.is_string(col.type):
                raise TypeError("ambiguous() takes the batch in transport form (interval_text)")
            zero = pc.fill_null(pc.equal(col, ZERO_INTERVAL_TEXT), False)
            if u is not None and prefix == "a":
                zero = pc.and_(zero, pc.invert(pc.fill_null(u, False)))
            mask = pc.or_(mask, zero)
    return mask


# ------------------------------------------------------------------------------------------------ per thread

_local = threading.local()


def default() -> Transformer:
    """The calling thread's own transformer (created on first use, with default settings)."""
    t = getattr(_local, "transformer", None)
    if t is None:
        t = Transformer()
        _local.transformer = t
    return t
