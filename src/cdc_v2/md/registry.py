"""The connection registry `_cdc.connections` (design 4.1, 4.5).

Every lane connection writes its server-side id here before its first transaction. A take-over interrupts the
connections of earlier epochs **from this list only**: an abandoned connection of a dead or frozen run can hold an
open transaction, and with it rows that the take-over must write
(`evidence/p3/destination/registry_takeover.json`). No function of this package lists all connections of the
account.
"""
from __future__ import annotations

import logging

from cdc_v2.deadline import DeadlineExceeded

from .connection import SITE_EXECUTE, _interrupt_local, as_deadline
from .errors import CommitUnknown, ConnectionLost, FenceLost, LaneMisuse
from .settings import LANES

log = logging.getLogger("cdc_v2.md")

#: Adds one row, only while the leader row still holds the epoch. Parameters: lane, connection id, epoch.
REGISTER_SQL = ("INSERT INTO _cdc.connections (epoch, lane, connection_id, registered_at) "
                "SELECT epoch, ?, ?::UUID, uuid_extract_timestamp(uuidv7()) FROM _cdc.leader WHERE epoch = ?")
#: The connection ids of every epoch before the given one. Parameter: epoch (exclusive).
LIST_EARLIER_SQL = "SELECT DISTINCT connection_id::VARCHAR FROM _cdc.connections WHERE epoch < ?"
#: MotherDuck only: interrupts those connections in one statement and counts them. Parameters: epoch (exclusive),
#: the id of the connection that runs the statement (left out: a statement that interrupts its own connection fails).
INTERRUPT_EARLIER_SQL = ("SELECT count(*), count(*) FILTER (WHERE md_interrupt_server_connection(connection_id)) "
                         "FROM (SELECT DISTINCT connection_id FROM _cdc.connections "
                         "WHERE epoch < ? AND connection_id <> ?::UUID)")
#: Stands for "no connection" where `INTERRUPT_EARLIER_SQL` is run by a connection that never registered.
NO_CONNECTION = "00000000-0000-0000-0000-000000000000"
#: Removes the rows of epochs before the previous one. Parameter: the leader's own epoch.
CLEANUP_SQL = "DELETE FROM _cdc.connections WHERE epoch < ? - 1"


def _epoch(epoch):
    if isinstance(epoch, bool) or not isinstance(epoch, int) or epoch < 0:
        raise ValueError("an epoch is an integer >= 0")
    return epoch


def register(conn, lane, epoch, deadline=None):
    """Write the connection's server-side id into `_cdc.connections`, in one autocommit statement that inserts only
    if `_cdc.leader` still holds ``epoch``. No row inserted: `FenceLost`.

    A lane calls this itself before its first transaction and after every replacement of its connection. Other
    connections that write for an epoch (the heartbeat connection) call it once, with the lane name they stand
    for. A lost connection here is `ConnectionLost`: a row may or may not have been written, and one row too many
    does no harm."""
    if lane not in LANES:
        raise ValueError("unknown lane %r" % (lane,))
    d = as_deadline(deadline, conn.settings.statement_deadline_s)
    sess = conn.ensure(d)
    sid = sess.server_id
    try:
        n = conn._run(SITE_EXECUTE, REGISTER_SQL, [lane, sid, _epoch(epoch)], "count", d, writes=True)
    except CommitUnknown as e:
        raise ConnectionLost("lane %s: connection lost while it registered" % lane) from e
    if n != 1:
        raise FenceLost("lane %s: epoch %d no longer leads (the leader row has another epoch)" % (lane, epoch))
    return sid


def interrupt_earlier(conn, epoch, deadline=None):
    """Interrupt every connection that `_cdc.connections` lists for an epoch **before** ``epoch``: a statement in
    flight ends and an open transaction is rolled back on the server. Returns ``(listed, interrupted)``.
    ``listed`` leaves out the connection ``conn`` itself. ``interrupted`` counts the connections that had a
    statement in flight or a transaction open on the server: MotherDuck answers false for a connection that is
    idle outside a transaction, for a closed one and for an id it does not know
    (`evidence/p3/destination/registry_takeover.json`, key ``what_the_server_answers``).

    A take-over that saw epoch ``s`` and meets a conflict calls this with ``s + 1`` and tries again (design 4.5).
    The new leader calls it once more with its own epoch after it won: a connection of the old run can register
    up to one statement after the take-over committed. The ids come from the table and from nowhere else."""
    if conn._in_block:
        raise LaneMisuse("this connection is inside a lane transaction")
    d = as_deadline(deadline, conn.settings.statement_deadline_s)
    sess = conn.ensure(d)
    own = sess.server_id
    if conn.is_motherduck:
        row = conn._run_on(sess, SITE_EXECUTE, INTERRUPT_EARLIER_SQL, [_epoch(epoch), own], "one", d, writes=False)
        listed, done = (int(row[0]), int(row[1] or 0)) if row is not None else (0, 0)
    else:
        rows = conn._run_on(sess, SITE_EXECUTE, LIST_EARLIER_SQL, [_epoch(epoch)], "all", d, writes=False)
        ids = [sid for (sid,) in rows if sid != own]
        listed, done = len(ids), 0
        for sid in ids:
            if d.expired:
                raise DeadlineExceeded("interrupting the connections of earlier epochs")
            if _interrupt_local(sid):
                done += 1
    if listed:
        log.info("interrupted connections of earlier epochs",
                 extra={"cdc": {"before_epoch": epoch, "listed": listed, "interrupted": done}})
    return listed, done


def cleanup(conn_or_tx, epoch, deadline=None):
    """Remove the rows of epochs before the previous one (design 4.1). Called by the leader with its own epoch, on
    a connection or inside a `control`-lane transaction. Returns the number of rows removed."""
    return conn_or_tx.execute(CLEANUP_SQL, [_epoch(epoch)], deadline)
