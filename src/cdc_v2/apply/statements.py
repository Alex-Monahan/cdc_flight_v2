"""The statement shapes (docs/design/apply.md section 3.3 and 4.2). Each function returns one SQL text; its shape
depends on the table's columns only. `n` is the relation the plan produced, `t` the destination table."""
from __future__ import annotations

from typing import List

from .meta import Column, TableMeta, Target
from .plan import key_join
from .types import affected, exact, rewrites_row, sentinel

__all__ = ["Shapes"]


class Shapes:
    def __init__(self, meta: TableMeta, target: Target, ucols: List[Column]):
        self.meta, self.target = meta, target
        self.T = target.sql()
        self.ucols = [c for c in ucols if not c.key]
        upos = {c.position for c in self.ucols}
        self.nonu = [c for c in meta.value_columns if c.position not in upos]
        #: set by the row mover (K3): key columns and the columns that are always present and update in place
        self.mover_cols = [c for c in self.nonu if not rewrites_row(c.dtype)]
        #: set by a statement of their own, only where the value changes: unchanged-possible columns and columns
        #: whose update rewrites the row
        self.per_column = [c for c in self.ucols] + [c for c in self.nonu if rewrites_row(c.dtype)]
        self.sentinel_mover = [c for c in self.mover_cols if affected(c.dtype)]

    # -------------------------------------------------------------------------------------------- keyed

    def k1_delete(self, rel: str) -> str:
        return "DELETE FROM %s AS t USING %s AS n WHERE %s" % (self.T, rel, key_join(self.meta, "t", "n", "x"))

    def _need(self, c: Column) -> str:
        """The new value is not NULL and differs exactly from the stored one."""
        return "(n.%s IS NOT NULL AND %s IS DISTINCT FROM %s)" % (
            c.a, exact("t.%s" % c.qname, c.dtype), exact("n.%s" % c.a, c.dtype))

    def k2_sentinel_mover(self, rel: str) -> str:
        sets = ", ".join("%s = CASE WHEN %s THEN %s ELSE t.%s END" % (c.qname, self._need(c),
                                                                      sentinel("n.%s" % c.a, c.dtype), c.qname)
                         for c in self.sentinel_mover)
        return "UPDATE %s AS t SET %s FROM %s AS n WHERE %s AND (%s)" % (
            self.T, sets, rel, key_join(self.meta, "t", "n", "rx"), " OR ".join(self._need(c) for c in self.sentinel_mover))

    def k3_move(self, rel: str) -> str:
        cols = list(self.meta.key_columns) + self.mover_cols
        sets = ", ".join("%s = n.%s" % (c.qname, c.a) for c in cols)
        return "UPDATE %s AS t SET %s FROM %s AS n WHERE %s" % (self.T, sets, rel, key_join(self.meta, "t", "n", "rx"))

    def k4_sentinel_column(self, c: Column, rel: str) -> str:
        return "UPDATE %s AS t SET %s = %s FROM %s AS n WHERE %s%s AND %s" % (
            self.T, c.qname, sentinel("n.%s" % c.a, c.dtype), rel, key_join(self.meta, "t", "n", "x"),
            self._has(c), self._need(c))

    def k5_set_column(self, c: Column, rel: str) -> str:
        return "UPDATE %s AS t SET %s = n.%s FROM %s AS n WHERE %s%s AND %s IS DISTINCT FROM %s" % (
            self.T, c.qname, c.a, rel, key_join(self.meta, "t", "n", "x"), self._has(c),
            exact("t.%s" % c.qname, c.dtype), exact("n.%s" % c.a, c.dtype))

    def _has(self, c: Column) -> str:
        return " AND n.h%d" % c.position if c in self.ucols else ""

    def k6_insert(self, rel: str) -> str:
        cols = list(self.meta.columns)
        return "INSERT INTO %s (%s) SELECT %s FROM %s" % (self.T, ", ".join(c.qname for c in cols),
                                                           ", ".join(c.a for c in cols), rel)

    def k7_copy(self, rel: str) -> str:
        cols = list(self.meta.columns)
        upos = {c.position for c in self.ucols}
        sel = ", ".join(("CASE WHEN n.h%d THEN n.%s ELSE t.%s END" % (c.position, c.a, c.qname))
                        if c.position in upos else "n.%s" % c.a for c in cols)
        return "INSERT INTO %s (%s) SELECT %s FROM %s AS n JOIN %s AS t ON %s" % (
            self.T, ", ".join(c.qname for c in cols), sel, rel, self.T, key_join(self.meta, "t", "n", "rx"))

    def k8_delete_copied(self, rel: str) -> str:
        return "DELETE FROM %s AS t USING %s AS n WHERE %s" % (self.T, rel, key_join(self.meta, "t", "n", "rx"))

    # -------------------------------------------------------------------------------------------- keyless

    def l1_delete(self, rel: str) -> str:
        cond = " AND ".join("%s IS NOT DISTINCT FROM n.e%d" % (exact("t.%s" % c.qname, c.dtype), c.position)
                            for c in self.meta.columns)
        return ("DELETE FROM {T} WHERE rowid IN (SELECT rid FROM (SELECT t.rowid AS rid, n.n AS need, "
                "row_number() OVER (PARTITION BY n.gid ORDER BY t.rowid) AS rk FROM {T} AS t JOIN {rel} AS n ON {cond}) "
                "AS m WHERE rk <= need)").format(T=self.T, rel=rel, cond=cond)

    def l2_insert(self, rel: str) -> str:
        cols = list(self.meta.columns)
        return "INSERT INTO %s (%s) SELECT %s FROM (SELECT *, unnest(range(n)) AS _r FROM %s) AS p" % (
            self.T, ", ".join(c.qname for c in cols), ", ".join("v%d" % c.position for c in cols), rel)

    # -------------------------------------------------------------------------------------------- both

    def truncate(self) -> str:
        return "DELETE FROM %s" % self.T

    def reduce_duplicates(self, rel: str) -> str:
        """Several rows with one key (after loading under several snapshots, design 7.3 step 1): keep one per key
        of the relation (columns x<i>)."""
        part = ", ".join(exact("t.%s" % c.qname, c.dtype) for c in self.meta.key_columns)
        return ("DELETE FROM {T} WHERE rowid IN (SELECT rid FROM (SELECT t.rowid AS rid, row_number() OVER "
                "(PARTITION BY {part} ORDER BY t.rowid) AS rk FROM {T} AS t JOIN {rel} AS n ON {cond}) AS m "
                "WHERE rk > 1)").format(T=self.T, part=part, rel=rel, cond=key_join(self.meta, "t", "n", "x"))

    def shape_bound(self) -> int:
        """How many statement shapes this table can use (keyed: K1-K8 with one K4/K5 per column)."""
        if not self.meta.has_key:
            return 2
        return 6 + len(self.per_column) + len([c for c in self.ucols if affected(c.dtype)])
