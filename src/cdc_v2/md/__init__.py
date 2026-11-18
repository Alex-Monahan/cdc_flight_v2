"""The destination layer (contract C4): connections with deadlines, lanes with the fence, the leader tables, the
connection registry and the migrations. `docs/design/destination.md` describes it.

    from cdc_v2 import md

    md.configure(md.Settings(database="..."))
    conn = md.connect("stream")
    lane = md.Lane(conn, "stream", epoch)
    with lane.transaction(deadline) as tx:
        tx.execute(sql, params)
        tx.execute_one(sql, params)

No other module issues BEGIN, COMMIT, ROLLBACK or the fence statement.
"""
from . import errors, registry, schema
from .connection import (CLOCK_US_SQL, Connection, background_threads, clock_sql, configure, connect,
                         default_settings, open_connections, statement_kind, wait_idle, waiting_calls)
from .errors import (CommitUnknown, Conflict, ConnectionLost, DeadlineExceeded, DestinationError, FenceLost,
                     LaneMisuse, StatementError)
from .lane import FENCE_SQL, Lane, Transaction, crash_points
from .registry import interrupt_earlier
from .schema import SchemaError, Step, ensure_leader_tables, migrate, register_migrations
from .settings import LANES, Settings

__all__ = [
    "Settings", "configure", "default_settings", "connect", "Connection", "Lane", "Transaction", "LANES",
    "clock_sql", "CLOCK_US_SQL", "FENCE_SQL", "crash_points",
    "DestinationError", "FenceLost", "CommitUnknown", "Conflict", "ConnectionLost", "StatementError",
    "DeadlineExceeded", "LaneMisuse",
    "SchemaError", "Step", "register_migrations", "migrate", "ensure_leader_tables", "interrupt_earlier",
    "errors", "registry", "schema", "open_connections", "background_threads", "wait_idle", "statement_kind",
    "waiting_calls",
]
