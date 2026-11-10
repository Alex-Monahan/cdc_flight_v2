"""Where the plan runs.

* `LocalBackend` (local form): the plan runs in a private in-memory DuckDB connection over the Arrow rows. The
  destination is asked only when the plan needs a fact about it (a lookup, one statement, and only when there is
  something to look up). The relations the statements read are handed to the lane transaction as registered Arrow
  data, in slices of at most `statement_rows` rows for the statements whose rows are independent of each other.
* `RemoteBackend` (relation form): the plan runs inside the caller's transaction, materialised as tables of a
  schema of its own (`<prefix>_<random>`, dropped with everything in it before `apply_group` returns). No row
  comes to the client; only counts and unresolved keys do.
"""
from __future__ import annotations

import secrets
from typing import Iterator, Optional

import duckdb
import pyarrow as pa

from .meta import TableMeta, Target
from .plan import Step, key_join
from .types import quote_ident

__all__ = ["LocalBackend", "RemoteBackend", "lookup_sql"]

#: Registered names on the lane transaction (D's rule: lower-case identifiers).
VIEW_PREFIX = "cdc_v2_apply_"


def lookup_sql(meta: TableMeta, target: Target, cand: str) -> str:
    """Per candidate exact key: how many destination rows hold it."""
    first = meta.key_columns[0].qname
    return ("SELECT c.*, count(t.%s) AS n FROM %s AS c LEFT JOIN %s AS t ON %s GROUP BY ALL"
            % (first, cand, target.sql(), key_join(meta, "t", "c")))


class LocalBackend:
    def __init__(self, tx, target: Target, meta: TableMeta, rows: pa.Table, statement_rows: int,
                 threads: Optional[int] = None):
        self.tx, self.target, self.meta = tx, target, meta
        self.statement_rows = max(1, int(statement_rows))
        self.L = duckdb.connect(":memory:")
        self.L.execute("SET TimeZone = 'UTC'")
        # UUID, TIME WITH TIME ZONE and JSON leave as Arrow extension types; without this they would become
        # VARCHAR and TIME (the offset lost) on their way to the destination.
        self.L.execute("SET arrow_lossless_conversion = true")
        if threads:
            self.L.execute("SET threads = %d" % int(threads))
        self.L.register("src_rows", rows)
        self.src_sql = "src_rows"
        self.src_params = ()

    def rel(self, name: str) -> str:
        return quote_ident("p_" + name)

    def run(self, step: Step) -> None:
        if step.kind == "sql":
            self.L.execute("CREATE TABLE %s AS %s" % (self.rel(step.name), step.sql), list(step.params) or None)
            return
        cand = self.L.execute("SELECT * FROM %s" % self.rel(step.source)).to_arrow_table()
        if cand.num_rows == 0:
            self.L.execute("CREATE TABLE %s AS SELECT *, CAST(0 AS BIGINT) AS n FROM %s WHERE false"
                           % (self.rel(step.name), self.rel(step.source)))
            return
        view = VIEW_PREFIX + "cand"
        self.tx.register(view, cand)
        found = self.tx.fetch_arrow(lookup_sql(self.meta, self.target, view))
        self.L.register("lookup_result", found)
        self.L.execute("CREATE TABLE %s AS SELECT * FROM lookup_result" % self.rel(step.name))
        self.L.unregister("lookup_result")

    def query_one(self, sql: str):
        cur = self.L.execute(sql)
        row = cur.fetchone()
        return dict(zip([d[0] for d in cur.description], row))

    def fetch_arrow(self, name: str) -> pa.Table:
        return self.L.execute("SELECT * FROM %s" % self.rel(name)).to_arrow_table()

    def statement_relations(self, name: str, chunked: bool) -> Iterator[str]:
        """Register the relation `name` on the transaction (whole, or in slices) and yield its name per slice."""
        data = self.fetch_arrow(name)
        view = VIEW_PREFIX + name
        if not chunked or data.num_rows <= self.statement_rows:
            self.tx.register(view, data)
            yield view
            return
        for off in range(0, data.num_rows, self.statement_rows):
            self.tx.register(view, data.slice(off, self.statement_rows))
            yield view

    def finish(self) -> None:
        pass

    def close(self) -> None:
        try:
            self.L.close()
        except Exception:                                    # noqa: BLE001 - closing a private connection
            pass


class RemoteBackend:
    def __init__(self, tx, target: Target, meta: TableMeta, src_sql: str, src_params: tuple, prefix: str):
        self.tx, self.target, self.meta = tx, target, meta
        self.schema = "%s_%s" % (prefix, secrets.token_hex(6))
        self.src_sql = "(%s) AS src" % src_sql
        self.src_params = tuple(src_params)
        self.tx.execute("CREATE SCHEMA %s" % quote_ident(self.schema))

    def rel(self, name: str) -> str:
        return "%s.%s" % (quote_ident(self.schema), quote_ident(name))

    def run(self, step: Step) -> None:
        if step.kind == "sql":
            self.tx.execute("CREATE TABLE %s AS %s" % (self.rel(step.name), step.sql), list(step.params) or None)
            return
        self.tx.execute("CREATE TABLE %s AS %s" % (self.rel(step.name),
                                                     lookup_sql(self.meta, self.target, self.rel(step.source))))

    def query_one(self, sql: str):
        rows = self.tx.fetch_arrow(sql).to_pylist()
        return rows[0]

    def fetch_arrow(self, name: str) -> pa.Table:
        return self.tx.fetch_arrow("SELECT * FROM %s" % self.rel(name))

    def statement_relations(self, name: str, chunked: bool) -> Iterator[str]:
        yield self.rel(name)

    def finish(self) -> None:
        self.tx.execute("DROP SCHEMA %s CASCADE" % quote_ident(self.schema))

    def close(self) -> None:
        pass
