"""A stand-in for a lane transaction (contract C4) over a plain DuckDB connection, until package D is merged.

Same calls as D's `Transaction`: `execute(sql, params) -> changed rows or None`, `fetch_arrow`, `fetchall`,
`register(name, data)`. It issues BEGIN and COMMIT/ROLLBACK itself (apply never does); no fence. Test code only.
"""
from __future__ import annotations

import re

import duckdb

_VIEW = re.compile(r"^[a-z_][a-z0-9_]{0,62}$")


class StandInTx:
    def __init__(self, conn, commit: bool = True):
        self.conn = conn
        self.commit = commit
        self.sent = []                       # every statement, in order
        self._views = []

    def __enter__(self):
        self.conn.execute("BEGIN TRANSACTION")
        return self

    def __exit__(self, et, ev, tb):
        for v in self._views:
            try:
                self.conn.unregister(v)
            except Exception:                # noqa: BLE001
                pass
        self.conn.execute("COMMIT" if (et is None and self.commit) else "ROLLBACK")
        return False

    def _check(self, sql):
        stmts = duckdb.extract_statements(sql)
        assert len(stmts) == 1, "exactly one statement per call"
        assert stmts[0].type != duckdb.StatementType.TRANSACTION, "apply never sends BEGIN, COMMIT or ROLLBACK"

    def execute(self, sql, params=None, deadline=None):
        self._check(sql)
        self.sent.append(sql)
        cur = self.conn.execute(sql, params)
        d = cur.description
        if d is not None and len(d) == 1 and d[0][0] == "Count":
            row = cur.fetchone()
            return int(row[0]) if row is not None and row[0] is not None else None
        return None

    def fetchall(self, sql, params=None, deadline=None):
        self._check(sql)
        self.sent.append(sql)
        return self.conn.execute(sql, params).fetchall()

    def fetch_arrow(self, sql, params=None, deadline=None):
        self._check(sql)
        self.sent.append(sql)
        return self.conn.execute(sql, params).to_arrow_table()

    def register(self, name, data, deadline=None):
        assert _VIEW.match(name), name
        self.conn.register(name, data)
        if name not in self._views:
            self._views.append(name)
