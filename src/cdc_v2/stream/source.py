"""What the stream needs from the source, on package P's functions (contract C7): the catalog of the node it decodes
from, the slot's facts, the termination of a holder of our slot, the first creation of the slot, and the last line
of SPEC 4.3 (moving our slot past a place that fails again and again).

`PgSource` keeps one SQL connection to the decoding node (replica mode: a standby, SPEC R17) and one to the primary
(only when a step needs it), each opened again after a lost connection. Every call takes a deadline. Every session
carries an `application_name` that starts with `cdc_v2_` (task S section 10). A test stand-in implements the same
methods without Postgres (`tests/stream/fakes.py`).
"""
from __future__ import annotations

import logging
from typing import Optional

from cdc_v2 import lsn as lsnmod
from cdc_v2 import pg
from cdc_v2.crashpoints import crash_point, declare
from cdc_v2.deadline import Deadline
from cdc_v2.pg import errors as pg_errors

log = logging.getLogger("cdc_v2.stream")

CP_SLOT_ADVANCED = declare("stream.slot_advanced", "SPEC 4.3 last line: our slot was moved past the failing place; "
                           "the reposition is not committed yet (reached only on that path)")


class PgSource:
    def __init__(self, settings: pg.SourceSettings, node: str = "replica", *, primary: str = "primary"):
        self.settings = settings
        self.node_name = node if node in settings.nodes else "primary"
        self.primary_name = primary
        self.slot = settings.slot
        self.publication = settings.publication
        self._conn: Optional[pg.PgConnection] = None
        self._primary: Optional[pg.PgConnection] = None
        self.server_version_num: Optional[int] = None

    # -------------------------------------------------------------------------------------------- connections

    def _node(self, deadline: Deadline) -> pg.PgConnection:
        if self._conn is None or self._conn.closed:
            self._conn = pg.connect(self.node_name, deadline, settings=self.settings, application_name="cdc_v2_stream")
            self.server_version_num = int(self._conn.scalar("SHOW server_version_num", deadline=deadline))
        return self._conn

    def _prim(self, deadline: Deadline) -> pg.PgConnection:
        if self.node_name == self.primary_name:
            return self._node(deadline)
        if self._primary is None or self._primary.closed:
            self._primary = pg.connect(self.primary_name, deadline, settings=self.settings,
                                       application_name="cdc_v2_stream_primary")
        return self._primary

    def _call(self, fn, deadline):
        try:
            return fn(self._node(deadline))
        except pg_errors.ConnectionLost:
            self._drop()
            raise

    def _drop(self):
        for c in (self._conn, self._primary):
            if c is not None:
                try:
                    c.close(Deadline.after(2.0))
                except Exception:                         # noqa: BLE001
                    pass
        self._conn = self._primary = None

    def close(self):
        self._drop()

    # -------------------------------------------------------------------------------------------- catalog

    def version(self, deadline: Deadline) -> int:
        self._node(deadline)
        return self.server_version_num

    def tables(self, deadline: Deadline, names=None) -> dict:
        """{(schema, table): SourceTable} of the decoding node: every table, or the named ones."""
        include = None if names is None else [tuple(n) for n in names]
        out = self._call(lambda c: pg.catalog.tables(c, include, publication=self.publication, deadline=deadline),
                         deadline)
        return {t.qualified: t for t in out}

    # -------------------------------------------------------------------------------------------- slot

    def slot_facts(self, deadline: Deadline):
        return self._call(lambda c: pg.slot.facts(c, self.slot, deadline=deadline), deadline)

    def terminate_holder(self, deadline: Deadline, expect=None):
        return self._call(lambda c: pg.slot.terminate_holder(c, self.slot, deadline, expect=expect), deadline)

    def create_slot(self, deadline: Deadline) -> int:
        """The first creation of our slot (task S section 3): returns its consistent point."""
        node = self._node(deadline)
        primary = None if self.node_name == self.primary_name else self._prim(deadline)
        created = pg.slot.create(node, self.slot, deadline, primary=primary)
        return created.consistent_point

    def heartbeat_problems(self, deadline: Deadline) -> list:
        return self._call(lambda c: pg.heartbeat.check(c, publication=self.publication,
                                                       schema=self.settings.heartbeat_schema,
                                                       table=self.settings.heartbeat_table, deadline=deadline),
                          deadline)

    def advance_slot(self, deadline: Deadline) -> int:
        """SPEC 4.3, the last line: move our slot (not held: the engine is stopped) to the node's current position
        and return its new `confirmed_flush_lsn`. On a standby the target is the replay position, on a primary the
        flush position. The runtime role may do it (REPLICATION attribute; docs/design/stream.md)."""
        def run(c):
            target = ("pg_last_wal_replay_lsn()" if c.in_recovery(deadline) else "pg_current_wal_flush_lsn()")
            c.execute("SELECT pg_replication_slot_advance(%%s, %s)" % target, (self.slot,), deadline=deadline,
                      what="slot advance")
            f = pg.slot.facts(c, self.slot, deadline=deadline)
            return f.confirmed_flush_lsn
        pos = self._call(run, deadline)
        crash_point(CP_SLOT_ADVANCED)
        return pos


class PgBeat:
    """`heartbeat.beat` on the primary, on a connection of its own (SPEC 4.4: a separate light connection)."""

    def __init__(self, settings: pg.SourceSettings, primary: str = "primary"):
        self.settings = settings
        self.primary = primary
        self._conn: Optional[pg.PgConnection] = None

    def __call__(self, deadline: Deadline) -> int:
        try:
            if self._conn is None or self._conn.closed:
                self._conn = pg.connect(self.primary, deadline, settings=self.settings,
                                        application_name="cdc_v2_heartbeat")
            w = pg.heartbeat.beat(self._conn, deadline, slot=self.settings.slot,
                                  schema=self.settings.heartbeat_schema, table=self.settings.heartbeat_table)
            return w.position
        except Exception:
            self.close()
            raise

    def close(self):
        if self._conn is not None:
            try:
                self._conn.close(Deadline.after(2.0))
            except Exception:                             # noqa: BLE001
                pass
            self._conn = None


def format_position(p) -> str:
    return lsnmod.format(p) if isinstance(p, int) else str(p)


__all__ = ["PgSource", "PgBeat", "CP_SLOT_ADVANCED"]
