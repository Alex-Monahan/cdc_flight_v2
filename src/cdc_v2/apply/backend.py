"""Where the plan runs.

* `LocalBackend` (local form): the plan runs in a private in-memory DuckDB connection over the Arrow rows. The
  destination is asked only when the plan needs a fact about it (a lookup, one statement, and only when there is
  something to look up). The relations the statements read are handed to the lane transaction as registered Arrow
  data, in slices of at most `statement_rows` rows for the statements whose rows are independent of each other.
* `RemoteBackend` (relation form): the plan runs inside the caller's transaction, materialised as tables of a
  schema of its own (`<prefix>_<random>`; the tables and the schema are dropped before `apply_group` returns). No row
  comes to the client; only counts and unresolved keys do.
"""
from __future__ import annotations

import itertools
import secrets
import threading
from typing import Iterator, Optional

import duckdb
import pyarrow as pa
import pyarrow.compute  # noqa: F401 - pa.compute

from .meta import TableMeta, Target
from .plan import Step, key_join
from .types import quote_ident

__all__ = ["LocalBackend", "RemoteBackend", "lookup_sql"]

#: Registered names on the lane transaction (D's rule: lower-case identifiers).
VIEW_PREFIX = "cdc_v2_apply_"


def lookup_sql(meta: TableMeta, target: Target, cand: str, by_index: bool = False) -> str:
    """Per candidate exact key: how many destination rows hold it. `by_index`: return the candidate's number `ci`
    instead of its key, so that no key travels back through an Arrow export of the lane connection (which turns
    TIME WITH TIME ZONE into TIME unless that connection converts losslessly)."""
    first = meta.key_columns[0].qname
    sel = "c.ci" if by_index else "c.*"
    return ("SELECT %s, count(t.%s) AS n FROM %s AS c LEFT JOIN %s AS t ON %s GROUP BY ALL"
            % (sel, first, cand, target.sql(), key_join(meta, "t", "c")))


#: Types that a plain Arrow export changes: they are fetched as text and cast back in the planning connection.
_TEXT_FETCH = ("TIME WITH TIME ZONE", "UUID", "JSON")


_local = threading.local()
_calls = itertools.count()


def _planning_db(threads: Optional[int], memory_limit: str, temp_directory: str, max_temp: str):
    """One private in-memory DuckDB database per thread and configuration; each call works in a cursor and a
    schema of its own (a new database costs about 5 ms, a cursor and a schema well under 1 ms). Memory limit, spill
    directory and spill limit are set before the database's first statement; beyond the memory limit DuckDB
    spills to the directory, beyond the spill limit the statement fails (core.ApplySpillFull)."""
    key = (threads, memory_limit, temp_directory, max_temp)
    dbs = getattr(_local, "dbs", None)
    if dbs is None:
        dbs = _local.dbs = {}
    db = dbs.get(key)
    if db is None:
        config = {"memory_limit": memory_limit, "temp_directory": temp_directory, "max_temp_directory_size": max_temp}
        if threads:
            config["threads"] = int(threads)
        db = duckdb.connect(":memory:", config=config)
        dbs[key] = db
    return db, key


def _discard_db(key) -> None:
    """Close a planning database (DuckDB removes its spill files) so that the next call starts afresh."""
    dbs = getattr(_local, "dbs", {})
    db = dbs.pop(key, None)
    if db is not None:
        try:
            db.close()
        except Exception:                                    # noqa: BLE001 - a private connection
            pass


class LocalSpill(Exception):
    """The planning database (not the destination) ran out of memory and spill space."""


class _Guarded:
    """The planning cursor: an out-of-memory error of it becomes LocalSpill, so that it is never confused with an
    error of the destination."""

    def __init__(self, cur):
        self._cur = cur

    def execute(self, sql, params=None):
        try:
            return self._cur.execute(sql, params)
        except duckdb.OutOfMemoryException as e:
            raise LocalSpill(type(e).__name__) from None

    def __getattr__(self, name):
        return getattr(self._cur, name)


class LocalBackend:
    def __init__(self, tx, target: Target, meta: TableMeta, rows: pa.Table, statement_rows: int,
                 threads: Optional[int], memory_limit: str, temp_directory: str, max_temp: str):
        self.tx, self.target, self.meta = tx, target, meta
        self.statement_rows = max(1, int(statement_rows))
        self.planning = True
        db, self.db_key = _planning_db(threads, memory_limit, temp_directory, max_temp)
        self.L = _Guarded(db.cursor())
        # SET SESSION: the database is shared by the calls of this thread (and a plain SET of some options is
        # global); each setting stays with this cursor
        self.L.execute("SET SESSION TimeZone = 'UTC'")
        # UUID, TIME WITH TIME ZONE and JSON leave as Arrow extension types; without this they would become
        # VARCHAR and TIME (the offset lost) on their way to the destination.
        self.L.execute("SET SESSION arrow_lossless_conversion = true")
        self.schema = "p_%d" % next(_calls)
        self.L.execute("CREATE SCHEMA %s" % self.schema)
        # the position of each record: values are read by it at the end (plan.py, r0)
        self.rows = rows
        self.recipes = {}
        self.L.register("src_rows", rows.append_column("pos", pa.array(range(rows.num_rows), type=pa.int64())))
        self.src_sql = "src_rows"
        self.src_params = ()
        self.pos = "pos"

    def rel(self, name: str) -> str:
        return "%s.%s" % (self.schema, quote_ident(name))

    def run(self, step: Step) -> None:
        if step.kind == "sql":
            if step.ix_sql:
                self.recipes[step.name] = step.recipe
                self.L.execute("CREATE TABLE %s AS %s" % (self.rel(step.name + "_ix"), step.ix_sql))
                return
            what = "VIEW" if step.view else "TABLE"
            self.L.execute("CREATE %s %s AS %s" % (what, self.rel(step.name), step.sql), list(step.params) or None)
            return
        cand = self.L.execute("SELECT row_number() OVER () AS ci, * FROM %s" % self.rel(step.source)).to_arrow_table()
        if cand.num_rows == 0:
            self.L.execute("CREATE TABLE %s AS SELECT *, CAST(0 AS BIGINT) AS n FROM %s WHERE false"
                           % (self.rel(step.name), self.rel(step.source)))
            return
        view = VIEW_PREFIX + "cand"
        self.tx.register(view, cand)
        found = self.tx.fetch_arrow(lookup_sql(self.meta, self.target, view, by_index=True))
        self.L.register("lookup_cand", cand)
        self.L.register("lookup_result", found)
        self.L.execute("CREATE TABLE %s AS SELECT c.* EXCLUDE (ci), CAST(coalesce(f.n, 0) AS BIGINT) AS n "
                       "FROM lookup_cand AS c LEFT JOIN lookup_result AS f ON f.ci = c.ci" % self.rel(step.name))
        self.L.unregister("lookup_result")
        self.L.unregister("lookup_cand")

    def drop(self, name: str) -> None:
        self.L.execute("DROP TABLE IF EXISTS %s" % self.rel(name))

    def query_one(self, sql: str):
        cur = self.L.execute(sql)
        row = cur.fetchone()
        return dict(zip([d[0] for d in cur.description], row))

    def fetch_arrow(self, name: str) -> pa.Table:
        return self.L.execute("SELECT * FROM %s" % self.rel(name)).to_arrow_table()

    def assemble(self, name: str) -> pa.Table:
        """An output relation from its positions (`<name>_ix`) and the change set's own Arrow columns. Values are
        copied by Arrow `take`, never by a SQL operator (docs/design/apply.md 3.1)."""
        ix = self.L.execute("SELECT * FROM %s" % self.rel(name + "_ix")).to_arrow_table()
        cols, names = [], []
        for item in self.recipes[name]:
            out, kind = item[0], item[1]
            if kind == "ix":
                arr = ix.column(item[2])
            elif kind == "take":
                arr = self.rows.column(item[2]).take(ix.column(item[3]))
            else:                                            # take2: from a ++ b
                both = pa.chunked_array(self.rows.column(item[2]).chunks + self.rows.column(item[3]).chunks,
                                        type=self.rows.column(item[2]).type)
                arr = both.take(ix.column(item[4]))
            cols.append(arr)
            names.append(out)
        return pa.table(cols, names=names)

    def statement_relations(self, name: str, chunked: bool) -> Iterator[str]:
        """Register the relation `name` on the transaction (whole, or in slices) and yield its name per slice."""
        data = self.assemble(name) if name in self.recipes else self.fetch_arrow(name)
        view = VIEW_PREFIX + name
        if not chunked or data.num_rows <= self.statement_rows:
            self.current = data
            self.tx.register(view, data)
            yield view
            return
        for off in range(0, data.num_rows, self.statement_rows):
            self.current = data.slice(off, self.statement_rows)
            self.tx.register(view, self.current)
            yield view

    def current_sum(self, column: str) -> Optional[int]:
        """The sum of `column` over the slice that the last relation of statement_relations holds."""
        return int(pa.compute.sum(self.current.column(column)).as_py() or 0)

    def copy_with_rowids(self, name: str, sh):
        """C0: the row id of every copy root, in one statement on the destination. Returns (the relation for K7
        and K8 with column `rid`, or None unless each root has exactly one row; rows found; roots found)."""
        data = self.assemble(name) if name in self.recipes else self.fetch_arrow(name)
        data = data.append_column("ci", pa.array(range(data.num_rows), type=pa.int64()))
        roots = VIEW_PREFIX + name + "_roots"
        self.tx.register(roots, data.select(["ci"] + ["rx%d" % c.position for c in self.meta.key_columns]))
        found = self.tx.fetch_arrow(sh.c0_copy_roots(roots))
        cis = found.column("ci").to_pylist()
        if not (len(cis) == len(set(cis)) == data.num_rows):
            return None, len(cis), len(set(cis))
        order = pa.compute.sort_indices(found, sort_keys=[("ci", "ascending")])
        data = data.append_column("rid", found.column("rid").take(order).cast(pa.int64()))
        view = VIEW_PREFIX + name
        self.tx.register(view, data)
        return view, len(cis), len(cis)

    def finish(self) -> None:
        pass

    def discard(self) -> None:
        """After a failure of the planning database itself: close it, so that its spill files go."""
        try:
            self.L.close()
        except Exception:                                    # noqa: BLE001
            pass
        _discard_db(self.db_key)
        self.L = None

    def close(self) -> None:
        if self.L is None:
            return
        try:
            self.L.execute("DROP SCHEMA IF EXISTS %s CASCADE" % self.schema)
            self.L.close()
        except Exception:                                    # noqa: BLE001 - a private connection
            _discard_db(self.db_key)                         # start again with a fresh database


class RemoteBackend:
    def __init__(self, tx, target: Target, meta: TableMeta, src_sql: str, src_params: tuple, prefix: str):
        self.tx, self.target, self.meta = tx, target, meta
        self.schema = "%s_%s" % (prefix, secrets.token_hex(6))
        self.src_sql = "(%s) AS src" % src_sql
        self.src_params = tuple(src_params)
        self.pos = "rowid"                                  # r0 is a table here: its row id is the position
        self.created = []
        self.tx.execute("CREATE SCHEMA %s" % quote_ident(self.schema))

    def rel(self, name: str) -> str:
        return "%s.%s" % (quote_ident(self.schema), quote_ident(name))

    def run(self, step: Step) -> None:
        self.created.append(step.name)
        if step.kind == "sql":
            self.tx.execute("CREATE TABLE %s AS %s" % (self.rel(step.name), step.sql), list(step.params) or None)
            return
        self.tx.execute("CREATE TABLE %s AS %s" % (self.rel(step.name),
                                                     lookup_sql(self.meta, self.target, self.rel(step.source))))

    def drop(self, name: str) -> None:
        pass                                                 # dropped with the others at the end

    def query_one(self, sql: str):
        rows = self.tx.fetch_arrow(sql).to_pylist()
        return rows[0]

    def fetch_arrow(self, name: str) -> pa.Table:
        """A small relation of key columns a<i> (the unresolved keys), with their destination types: columns of
        a type that a plain Arrow export changes travel as text and are cast back locally."""
        keys = self.meta.key_columns
        sel = ", ".join(("CAST(%s AS VARCHAR) AS %s" % (c.a, c.a)) if c.dtype.base in _TEXT_FETCH else c.a
                        for c in keys)
        data = self.tx.fetch_arrow("SELECT %s FROM %s" % (sel, self.rel(name)))
        if not any(c.dtype.base in _TEXT_FETCH for c in keys):
            return data
        L = duckdb.connect(":memory:")                       # a few keys: no spill
        try:
            L.execute("SET SESSION arrow_lossless_conversion = true")
            L.register("fetched", data)
            return L.execute("SELECT %s FROM fetched" % ", ".join("CAST(%s AS %s) AS %s" % (c.a, c.dtype.sql(), c.a)
                                                                 for c in keys)).to_arrow_table()
        finally:
            L.close()

    def statement_relations(self, name: str, chunked: bool) -> Iterator[str]:
        yield self.rel(name)

    def current_sum(self, column: str) -> Optional[int]:
        return None                                          # never sliced: the plan's own count holds

    def copy_with_rowids(self, name: str, sh):
        """C0 (see LocalBackend): a table of the copies with the row id of each root."""
        out = name + "_rid"
        self.created.append(out)
        keys = ", ".join("n.rx%d" % c.position for c in self.meta.key_columns)
        self.tx.execute("CREATE TABLE %s AS SELECT n.*, t.rowid AS rid FROM %s AS n JOIN %s AS t ON %s"
                        % (self.rel(out), self.rel(name), self.target.sql(), key_join(sh.meta, "t", "n", "rx")))
        q = self.query_one("SELECT count(*) AS n, count(DISTINCT struct_pack(%s)) AS k, (SELECT count(*) FROM %s) AS "
                           "want FROM %s AS n" % (keys, self.rel(name), self.rel(out)))
        ok = q["n"] == q["k"] == q["want"]
        return (self.rel(out) if ok else None), q["n"], q["k"]

    def finish(self) -> None:
        # one DROP per table: DuckDB refuses at COMMIT a DROP SCHEMA ... CASCADE of tables created in the same
        # transaction ("another transaction has dropped this table"; docs/design/apply.md 5)
        for name in reversed(self.created):
            self.tx.execute("DROP TABLE %s" % self.rel(name))
        self.tx.execute("DROP SCHEMA %s" % quote_ident(self.schema))

    def close(self) -> None:
        pass
