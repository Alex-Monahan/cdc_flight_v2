"""apply_group, truncate and reduce_duplicates (contract C3a; docs/design/apply.md)."""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import List, Optional, Union

import pyarrow as pa

from .backend import LocalBackend, LocalSpill, RemoteBackend
from .changeset import ChangeSetError, LocalChangeSet, RelationChangeSet
from .meta import TableMeta, Target
from .plan import flat_x, keyed_stats_sql, keyed_steps, keyless_stats_sql, keyless_steps
from .statements import Shapes
from .types import affected

__all__ = ["ApplyInvariantError", "ApplySpillFull", "ApplySettingsError", "ApplyCounts", "ApplySettings", "apply_group",
           "truncate", "reduce_duplicates"]

log = logging.getLogger("cdc_v2.apply")

MODES = ("stream", "catch_up")


class ApplyInvariantError(Exception):
    """The destination and the change set disagree in a way that applying cannot cure (the destination has
    diverged, or a keyless delete finds fewer rows than it must remove). The caller rolls back. The message
    names the table and counts, never a value of a row (SPEC 8.3)."""


class ApplySpillFull(Exception):
    """The private DuckDB that plans a local change set ran out of memory and of spill space (SPEC 5.4). Nothing
    was sent to the destination for this change set by the plan; the caller can stage the change set in MotherDuck
    and apply it in the relation form instead. The message carries no value."""


class ApplySettingsError(ValueError):
    """ApplySettings that cannot be used (for example no spill directory for the local form)."""


@dataclass(frozen=True)
class ApplySettings:
    #: rows per DELETE or INSERT statement fed from local Arrow data (design 6.1: bounded hand-over)
    statement_rows: int = 1_000_000
    #: threads of the private DuckDB connection that plans a local change set (None: DuckDB's default)
    local_threads: Optional[int] = 2
    #: memory limit of that connection; beyond it DuckDB spills to `local_temp_directory`
    local_memory_limit: str = "1GB"
    #: the pipeline's spill area for that connection (required for the local form: SPEC 5.4, one spill area with a
    #: limit); it must exist
    local_temp_directory: Optional[str] = None
    #: the most that connection may spill; beyond it the plan fails with ApplySpillFull
    local_max_temp_directory_size: str = "4GB"
    #: stream mode: also check, with one statement, that keys whose first record is a create have no destination
    #: row when no other statement of the change set would see it (an insert-only change set)
    check_new_keys: bool = True


@dataclass
class ApplyCounts:
    records: int = 0
    by_op: dict = field(default_factory=lambda: {"c": 0, "u": 0, "d": 0})
    deleted: int = 0
    inserted: int = 0
    updated: int = 0
    statements: int = 0                      # statements sent to the destination (each is one round trip)
    shapes: List[str] = field(default_factory=list)   # the shape of each statement, in order (K1 ... L2, lookup ...)
    unresolved: Optional[pa.Table] = None    # catch_up: key columns a<i> of the keys not applied; else None


class _Counting:
    """Counts what is sent through the lane transaction and names each statement's shape."""

    def __init__(self, tx, counts: ApplyCounts):
        self._tx, self._counts = tx, counts
        self.shape = "plan"

    def _note(self):
        self._counts.statements += 1
        self._counts.shapes.append(self.shape)

    def execute(self, sql, params=None):
        self._note()
        return self._tx.execute(sql, params)

    def fetch_arrow(self, sql, params=None):
        self._note()
        return self._tx.fetch_arrow(sql, params)

    def register(self, name, data):
        return self._tx.register(name, data)


def _ucols(cs) -> list:
    return [c for c in cs.u_columns() if not c.key]


def apply_group(tx, target: Target, change_set: Union[LocalChangeSet, RelationChangeSet], mode: str = "stream",
                settings: Optional[ApplySettings] = None) -> ApplyCounts:
    """Apply one change set to one destination table inside the open lane transaction `tx` (contract C4). Never
    begins, commits, rolls back or fences. Raises `ApplyInvariantError` (stream: an unresolved key or a divergence;
    both modes: a keyless delete that finds too few rows) and `ChangeSetError` (a change set of the wrong shape)."""
    if mode not in MODES:
        raise ValueError("mode is 'stream' or 'catch_up'")
    if not isinstance(target, Target):
        raise TypeError("target is a Target")
    settings = settings or ApplySettings()
    counts = ApplyCounts()
    ctx = _Counting(tx, counts)
    meta = change_set.meta
    ucols = _ucols(change_set)
    if isinstance(change_set, LocalChangeSet):
        import os
        if not settings.local_temp_directory or not os.path.isdir(settings.local_temp_directory):
            raise ApplySettingsError("ApplySettings.local_temp_directory must name an existing directory (the "
                                     "pipeline's spill area) for a local change set")
        backend = LocalBackend(ctx, target, meta, change_set.rows, settings.statement_rows, settings.local_threads,
                               settings.local_memory_limit, settings.local_temp_directory,
                               settings.local_max_temp_directory_size)
    elif isinstance(change_set, RelationChangeSet):
        backend = RemoteBackend(ctx, target, meta, change_set.sql, change_set.params, change_set.scratch_prefix)
    else:
        raise ChangeSetError("a change set is a LocalChangeSet or a RelationChangeSet")
    try:
        if meta.has_key:
            _keyed(ctx, backend, target, meta, ucols, mode, change_set, counts, settings)
        else:
            _keyless(ctx, backend, target, meta, ucols, change_set, counts)
        ctx.shape = "cleanup"
        backend.finish()
    except LocalSpill:
        backend.discard()
        raise ApplySpillFull("%s: the plan of a local change set of %d records exceeded the memory limit and the "
                             "spill limit" % (target, change_set.rows.num_rows)) from None
    finally:
        backend.close()
    log.debug("applied", extra={"cdc": {"table": str(target), "records": counts.records, "deleted": counts.deleted,
                                        "inserted": counts.inserted, "updated": counts.updated,
                                        "statements": counts.statements}})
    return counts


#: relations that the statements, the counts or the result read after the plan has run
_KEEP = {"r0", "r", "kp", "act", "del", "ins", "upd", "copy", "unres", "neg", "delta", "plus"}


def _run_plan(ctx, backend, steps):
    """Run the steps; in the local form drop each intermediate table after the last step that reads it, so that
    client memory stays near the change set's own size (design 6.1)."""
    last = {}
    for i, st in enumerate(steps):
        text = st.sql + st.ix_sql
        for other in steps[:i]:
            if backend.rel(other.name) in text or (other.kind == "lookup" and backend.rel(other.name) in text):
                last[other.name] = i
        if st.kind == "lookup":
            last[st.source] = i
    for i, st in enumerate(steps):
        ctx.shape = "lookup" if st.kind == "lookup" else "plan"
        backend.run(st)
        for name, j in last.items():
            if j == i and name not in _KEEP:
                backend.drop(name)


def _check_shape(stats, target):
    if stats["n_badop"]:
        raise ChangeSetError("%s: %d records with an op other than c, u, d" % (target, stats["n_badop"]))
    if stats["n_nullorder"]:
        raise ChangeSetError("%s: %d records without a transaction order or seq" % (target, stats["n_nullorder"]))


def _keyed(ctx, backend, target, meta, ucols, mode, cs, counts, settings):
    steps = keyed_steps(meta, backend.src_sql, cs.order_column, ucols, mode, backend.rel, backend.src_params,
                        backend.pos, settings.check_new_keys)
    _run_plan(ctx, backend, steps)
    ctx.shape = "plan"
    stats = backend.query_one(keyed_stats_sql(meta, ucols, backend.rel, mode))
    counts.records = int(stats["n_records"])
    counts.by_op = {"c": int(stats["n_c"]), "u": int(stats["n_u"]), "d": int(stats["n_d"])}
    _check_shape(stats, target)
    if stats["n_dupseq"]:
        raise ChangeSetError("%s: two records with one (transaction, seq)" % target)
    if stats["n_keynull"] or stats["n_okeypartial"]:
        raise ApplyInvariantError("%s: %d records without a key value, %d with a partial old key"
                                  % (target, stats["n_keynull"], stats["n_okeypartial"]))
    if stats["n_samesrc"]:
        raise ApplyInvariantError("%s: %d keys that the change set ends with are one key at the source (-0 and 0, "
                                  "1 mon and 30 days)" % (target, stats["n_samesrc"]))
    if stats["n_bad"]:
        # both modes: records that no source history can produce (a c at a key that holds a row, a u or d at a
        # key that holds none; rule K, H-015)
        raise ApplyInvariantError("%s: %d keys whose records are not a source history (a create at a held key, "
                                  "an update or delete at a free one)" % (target, stats["n_bad"]))
    if mode == "stream":
        if stats["n_unres_final"]:
            raise ApplyInvariantError("%s: %d keys whose unchanged values have no source" % (target,
                                                                                           stats["n_unres_final"]))
    n_copy = int(stats["n_copy"])
    # a copy finds its old row by row id; a column named rowid hides it, so it is renamed around the statements
    with (_rowid_free(ctx, target, meta) if n_copy else _same(meta)) as smeta:
        upos = {c.position for c in ucols}
        _keyed_statements(ctx, backend, target, smeta, [c for c in smeta.columns if c.position in upos], mode,
                          stats, counts, settings)
    if mode == "catch_up":
        ctx.shape = "plan"
        counts.unresolved = backend.fetch_arrow("unres")         # with the key types, also when empty


class _same:
    def __init__(self, meta):
        self.meta = meta

    def __enter__(self):
        return self.meta

    def __exit__(self, et, ev, tb):
        return False


def _keyed_statements(ctx, backend, target, meta, ucols, mode, stats, counts, settings):
    """The statements in their order (docs/design/apply.md 3.4): K1; C0 (the copy roots' row ids, before any row
    moves); K2, K3; K7, K8 (by row id); K4, K5; K6. In stream mode each statement acts on a key only where the
    destination holds what the source held at the start, so that its count is a count of keys (2.5)."""
    stream = mode == "stream"
    sh = Shapes(meta, target, ucols, per_key=stream, new_keys=settings.check_new_keys)
    n_del, n_upd, n_copy, n_ins = (int(stats[k]) for k in ("n_del", "n_upd", "n_copy", "n_ins"))
    if n_del:
        ctx.shape = "K1"
        for rel in backend.statement_relations("del", chunked=True):
            got = ctx.execute(sh.k1_delete(rel)) or 0
            counts.deleted += got
            want = backend.current_sum("p")
            want = int(stats["n_del_held"]) if want is None else want
            if stream and got != want:
                # the destination equals the source at the start: one row per key the source held, none per key it
                # did not hold; K1 removes the rows only when every key of its slice is so (statements.k1_delete)
                raise ApplyInvariantError("%s: K1 removed %d rows for %d keys the source held at the start (a key "
                                          "with another number of rows)" % (target, got, want))
    copy_rel = None
    if n_copy:
        ctx.shape = "C0"
        copy_rel, found, roots = backend.copy_with_rowids("copy", sh)
        if copy_rel is None:
            raise ApplyInvariantError("%s: %d keys to copy, %d rows found at %d of them" % (target, n_copy, found,
                                                                                           roots))
    if n_upd:
        upd_rel = list(backend.statement_relations("upd", chunked=False))[0]
        if sh.sentinel_mover:
            ctx.shape = "K2"
            ctx.execute(sh.k2_sentinel_mover(upd_rel))
        ctx.shape = "K3"
        moved = ctx.execute(sh.k3_move(upd_rel))
        if moved != n_upd:
            raise ApplyInvariantError("%s: %d of %d rows to update found in the destination%s"
                                      % (target, moved or 0, n_upd, " (each alone at its key, and its new key free)"
                                         if sh.per_key else ""))
        counts.updated += moved
    if n_copy:
        ctx.shape = "K7"
        copied = ctx.execute(sh.k7_copy(copy_rel))
        ctx.shape = "K8"
        removed = ctx.execute(sh.k8_delete_copied(copy_rel))
        if copied != n_copy or removed != n_copy:
            raise ApplyInvariantError("%s: %d rows to copy, %s copied, %s removed" % (target, n_copy, copied, removed))
        counts.inserted += copied
        counts.deleted += removed
    if n_upd:
        for c in sh.per_column:
            if c in sh.ucols and not stats["h%d" % c.position]:
                continue
            if c in sh.ucols and affected(c.dtype):
                ctx.shape = "K4.%d" % c.position
                ctx.execute(sh.k4_sentinel_column(c, upd_rel))
            ctx.shape = "K5.%d" % c.position
            ctx.execute(sh.k5_set_column(c, upd_rel))
    if n_ins:
        ctx.shape = "K6"
        got = 0
        for rel in backend.statement_relations("ins", chunked=True):
            got += ctx.execute(sh.k6_insert(rel)) or 0
        if sh.new_keys and got != n_ins:
            raise ApplyInvariantError("%s: %d of %d rows to insert have a free key" % (target, got, n_ins))
        counts.inserted += got


def _keyless(ctx, backend, target, meta, ucols, cs, counts):
    steps = keyless_steps(meta, backend.src_sql, cs.order_column, ucols, backend.rel, backend.src_params,
                          backend.pos)
    _run_plan(ctx, backend, steps)
    ctx.shape = "plan"
    stats = backend.query_one(keyless_stats_sql(backend.rel))
    counts.records = int(stats["n_records"])
    counts.by_op = {"c": int(stats["n_c"]), "u": int(stats["n_u"]), "d": int(stats["n_d"])}
    _check_shape(stats, target)
    if stats["n_dupseq"]:
        raise ChangeSetError("%s: two records with one (transaction, seq)" % target)
    need = int(stats["n_neg_rows"])
    if not need and not stats["n_pos_rows"]:
        return
    with _rowid_free(ctx, target, meta) as smeta:
        _keyless_statements(ctx, backend, target, smeta, ucols, stats, counts)


def _keyless_statements(ctx, backend, target, meta, ucols, stats, counts):
    sh = Shapes(meta, target, ucols)
    need = int(stats["n_neg_rows"])
    if need:
        ctx.shape = "L1"
        got = 0
        for rel in backend.statement_relations("neg", chunked=True):
            got += ctx.execute(sh.l1_delete(rel)) or 0
        if got != need:
            raise ApplyInvariantError("%s: %d rows to delete, %d found" % (target, need, got))
        counts.deleted += got
    if stats["n_pos_rows"]:
        ctx.shape = "L2"
        for rel in backend.statement_relations("plus", chunked=True):
            counts.inserted += ctx.execute(sh.l2_insert(rel)) or 0


class _rowid_free:
    """DuckDB's row id is hidden by a column named `rowid` (any case). The keyless delete and reduce_duplicates
    need the row id, so such a column is renamed for the duration of the statements, inside the caller's
    transaction, and renamed back; the statements use the temporary name (docs/design/apply.md 4.3)."""

    def __init__(self, tx, target: Target, meta: TableMeta):
        self.tx, self.target, self.meta = tx, target, meta
        self.col = next((c for c in meta.columns if c.name.casefold() == "rowid"), None)

    def __enter__(self) -> TableMeta:
        if self.col is None:
            return self.meta
        import secrets
        from .meta import Column
        from .types import quote_ident
        self.tmp = "rowid_cdc_v2_%s" % secrets.token_hex(6)
        if hasattr(self.tx, "shape"):
            self.tx.shape = "R1"
        self.tx.execute("ALTER TABLE %s RENAME COLUMN %s TO %s" % (self.target.sql(), self.col.qname,
                                                                    quote_ident(self.tmp)))
        cols = [Column(c.position, self.tmp, c.type, c.key, c.unchanged_possible) if c is self.col else c
                for c in self.meta.columns]
        return TableMeta(tuple(cols))

    def __exit__(self, et, ev, tb):
        if self.col is None or et is not None:
            return False                           # after an error the caller rolls back, which undoes the rename
        from .types import quote_ident
        if hasattr(self.tx, "shape"):
            self.tx.shape = "R2"
        self.tx.execute("ALTER TABLE %s RENAME COLUMN %s TO %s" % (self.target.sql(), quote_ident(self.tmp),
                                                                    self.col.qname))
        return False


def truncate(tx, target: Target) -> int:
    """Empty the table inside the transaction (a TRUNCATE of the source at this place of the stream)."""
    if not isinstance(target, Target):
        raise TypeError("target is a Target")
    return tx.execute("DELETE FROM %s" % target.sql()) or 0


def reduce_duplicates(tx, target: Target, change_set: Union[LocalChangeSet, RelationChangeSet],
                      settings: Optional[ApplySettings] = None) -> int:
    """For the keys of a change set: several destination rows with one key are reduced to one (design 7.3 step 1;
    which one stays does not matter, the log supplies every value that differs). Returns the rows removed."""
    meta = change_set.meta
    if not meta.has_key:
        raise ValueError("reduce_duplicates is for tables with a key")
    settings = settings or ApplySettings()
    keys = meta.key_columns
    from .types import exact
    xs = ", ".join("%s AS x%d" % (exact(c.a, c.dtype), c.position) for c in keys)
    ks = ", ".join("%s AS x%d" % (exact(c.k, c.dtype), c.position) for c in keys)
    nn = " AND ".join("%s IS NOT NULL" % c.k for c in keys)
    if isinstance(change_set, LocalChangeSet):
        import duckdb
        L = duckdb.connect(":memory:")
        L.execute("SET arrow_lossless_conversion = true")
        try:
            L.register("src_rows", change_set.rows)
            data = L.execute("SELECT DISTINCT * FROM (SELECT %s FROM src_rows UNION ALL SELECT %s FROM src_rows WHERE %s)"
                             % (xs, ks, nn)).to_arrow_table()
        finally:
            L.close()
        if data.num_rows == 0:
            return 0
        tx.register("cdc_v2_apply_keys", data)
        with _rowid_free(tx, target, meta) as smeta:
            return tx.execute(Shapes(smeta, target, []).reduce_duplicates("cdc_v2_apply_keys")) or 0
    rel = ("(SELECT DISTINCT * FROM (SELECT %s FROM (%s) AS s UNION ALL SELECT %s FROM (%s) AS s WHERE %s))"
           % (xs, change_set.sql, ks, change_set.sql, nn))
    params = list(change_set.params) * 2 or None
    with _rowid_free(tx, target, meta) as smeta:
        return tx.execute(Shapes(smeta, target, []).reduce_duplicates(rel), params) or 0
