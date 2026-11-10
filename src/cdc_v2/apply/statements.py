"""The statement shapes (docs/design/apply.md section 3.3 and 4.2). Each function returns one SQL text; its shape
depends on the table's columns only. `n` is the relation the plan produced, `t` the destination table."""
from __future__ import annotations

from typing import List

from .meta import Column, TableMeta, Target
from .plan import key_join
from .types import affected, exact, join_wrap, rewrites_row, sentinel

__all__ = ["Shapes"]


class Shapes:
    """`per_key`: stream mode, the statements act on a key only where the destination holds exactly the rows the
    source held at the start (a count per key inside the statement, docs/design/apply.md 2.5). `new_keys`: also
    check inside K3, K6 and K7 that a key the source did not hold has no row (`ApplySettings.check_new_keys`)."""

    def __init__(self, meta: TableMeta, target: Target, ucols: List[Column], per_key: bool = False,
                 new_keys: bool = False):
        self.meta, self.target = meta, target
        self.per_key, self.new_keys = per_key, new_keys and per_key
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

    def _one_row(self, prefix: str) -> str:
        """The destination holds exactly one row at the relation's key `prefix`<i> (evaluated on the table as
        the statement starts)."""
        return " AND (SELECT count(*) FROM %s AS t2 WHERE %s) = 1" % (self.T, key_join(self.meta, "t2", "n", prefix))

    def _free_target(self) -> str:
        """A target key that the source did not hold at the start (xp = 0) has no row."""
        return " AND (n.xp = 1 OR NOT EXISTS (SELECT 1 FROM %s AS t3 WHERE %s))" % (
            self.T, key_join(self.meta, "t3", "n", "x"))

    def k1_delete(self, rel: str) -> str:
        """Stream (`per_key`): the rows of the relation's keys go only if every key holds p rows (p = 1: the
        source held it, p = 0: it did not); otherwise the count differs from the number of keys with p = 1 (none
        go, or, when no key has p = 1, all rows found go), whatever the sums of rows are."""
        cond = ""
        if self.per_key:
            cond = (" AND (NOT EXISTS (SELECT 1 FROM {rel} AS n2 WHERE (SELECT count(*) FROM {T} AS t2 WHERE {j2}) "
                    "<> n2.p) OR NOT EXISTS (SELECT 1 FROM {rel} AS n3 WHERE n3.p = 1))").format(
                rel=rel, T=self.T, j2=key_join(self.meta, "t2", "n2", "x"))
        return "DELETE FROM %s AS t USING %s AS n WHERE %s%s" % (self.T, rel, key_join(self.meta, "t", "n", "x"), cond)

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
        return "UPDATE %s AS t SET %s FROM %s AS n WHERE %s%s%s" % (
            self.T, sets, rel, key_join(self.meta, "t", "n", "rx"), self._one_row("rx") if self.per_key else "",
            self._free_target() if self.new_keys else "")

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
        free = (" WHERE NOT EXISTS (SELECT 1 FROM %s AS t WHERE %s)" % (self.T, key_join(self.meta, "t", "n", "x"))
                if self.new_keys else "")
        return "INSERT INTO %s (%s) SELECT %s FROM %s AS n%s" % (self.T, ", ".join(c.qname for c in cols),
                                                                  ", ".join("n.%s" % c.a for c in cols), rel, free)

    def c0_copy_roots(self, rel: str) -> str:
        """The row id of each copy root (column `ci` of the relation numbers them), read before any statement of
        the call moves or inserts a row (docs/design/apply.md 3.4)."""
        return "SELECT n.ci, t.rowid AS rid FROM %s AS n JOIN %s AS t ON %s" % (rel, self.T,
                                                                               key_join(self.meta, "t", "n", "rx"))

    def k7_copy(self, rel: str) -> str:
        """Insert the copies; the old row is found by its row id (`rid`, from C0), never by its key."""
        cols = list(self.meta.columns)
        upos = {c.position for c in self.ucols}
        sel = ", ".join(("CASE WHEN n.h%d THEN n.%s ELSE t.%s END" % (c.position, c.a, c.qname))
                        if c.position in upos else "n.%s" % c.a for c in cols)
        return "INSERT INTO %s (%s) SELECT %s FROM %s AS n JOIN %s AS t ON t.rowid = n.rid%s" % (
            self.T, ", ".join(c.qname for c in cols), sel, rel, self.T,
            self._free_target().replace(" AND ", " WHERE ", 1) if self.new_keys else "")

    def k8_delete_copied(self, rel: str) -> str:
        return "DELETE FROM %s WHERE rowid IN (SELECT rid FROM %s)" % (self.T, rel)

    # -------------------------------------------------------------------------------------------- keyless

    def l1_delete(self, rel: str) -> str:
        cond = " AND ".join("%s IS NOT DISTINCT FROM %s" % (join_wrap(exact("t.%s" % c.qname, c.dtype), c.dtype),
                                                             join_wrap("n.e%d" % c.position, c.dtype))
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
        """How many statement shapes this table can use (keyed: K1-K8 and C0 with one K4/K5 per column)."""
        if not self.meta.has_key:
            return 2
        return 7 + len(self.per_column) + len([c for c in self.ucols if affected(c.dtype)])
