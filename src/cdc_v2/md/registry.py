"""The connection registry `_cdc.connections` (design 4.1, 4.5).

Every lane connection writes its server-side id here before its first transaction. A take-over interrupts the
connections of earlier epochs **from this list only**: an abandoned connection of a dead or frozen run can hold an
open transaction, and with it rows that the take-over must write
(`evidence/p3/destination/registry_takeover.json`). No function of this package lists all connections of the
account.

A registration is a transaction of its own: it inserts the row and writes the counter `n` of the lane's fence
row with a condition on the epoch (`REGISTER_FENCE_SQL`, then `REGISTER_FENCE_RESTORE_SQL`: `n + 1`, then `n - 1`,
so the counter is unchanged), and commits only if each changed exactly one row. A take-over writes the same
column of the same row, so the two cannot both commit (conflicts are per column, `docs/design/destination.md`
section 6): a registration that read the old epoch fails, or it commits before the take-over, and then the
take-over's interrupt of the earlier epochs finds it. The counter is written with a changed value on purpose:
DuckDB 1.5.6 (and MotherDuck) does not record a write of an unchanged value into a column that has had no update
since it was last written or checkpointed, so ``SET epoch = epoch`` is not seen by a later writer
(`docs/design/destination.md`, section 14.4). (Until the fix round the
registration was one `INSERT ... SELECT ... FROM _cdc.leader WHERE epoch = ?` in autocommit. It read the leader
row and wrote only `_cdc.connections`, so one that read the old epoch could commit after the take-over and after
the new leader's interrupt had read the list.)
"""
from __future__ import annotations

import logging

import random

from cdc_v2.deadline import Deadline, DeadlineExceeded

from . import errors
from .connection import SITE_EXECUTE, _NO_TX, _interrupt_local, as_deadline
from .errors import Conflict, ConnectionLost, FenceLost, LaneMisuse
from .settings import LANES

log = logging.getLogger("cdc_v2.md")

#: Adds one row. Parameters: epoch, lane, connection id. It commits only together with `REGISTER_FENCE_SQL`.
REGISTER_SQL = ("INSERT INTO _cdc.connections (epoch, lane, connection_id, registered_at) "
                "VALUES (?, ?, ?::UUID, uuid_extract_timestamp(uuidv7()))")
#: In the same transaction: must change exactly one row, else `FenceLost`. It writes the column `n` with a changed
#: value; a take-over writes `n` too, so the registration and a take-over cannot both commit. Parameters: lane,
#: epoch. `REGISTER_FENCE_RESTORE_SQL` follows it and puts the counter back.
REGISTER_FENCE_SQL = "UPDATE _cdc.fence SET n = n + 1 WHERE lane = ? AND epoch = ?"
REGISTER_FENCE_RESTORE_SQL = "UPDATE _cdc.fence SET n = n - 1 WHERE lane = ? AND epoch = ?"
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


def register(conn, lane, epoch, deadline=None, retry_conflict=False):
    """Write the connection's server-side id into `_cdc.connections` for ``epoch``, in a transaction of its own
    that also writes the epoch of the fence row of ``lane`` with a condition on it (module text). Returns the id.

    - The fence row holds another epoch (another run took over): `FenceLost`, nothing written.
    - A take-over or a transaction of the same lane writes the fence row at the same moment: `Conflict`, nothing
      written. With ``retry_conflict`` it is tried again after a short pause until ``deadline``.
    - The connection is lost, or the outcome of its `COMMIT` is not known: `ConnectionLost`, and the connection
      is given up, so that the next call registers a new one. (A row may have been written; one row too many
      does no harm: an interrupt of an id that holds nothing answers false.)

    A lane calls this itself before its first transaction and after every replacement of its connection, with
    the statement deadline (`cdc_v2.md.lane`). Another connection that writes for an epoch (the heartbeat
    connection of package L) calls it with the lane name ``control`` and ``retry_conflict=True``: its
    registration writes the fence row of `control`, which a transaction of the control lane holds for the moment
    between its fence statement and its `COMMIT`. It calls it before its first write, and again whenever
    ``conn.generation`` has changed since (its client connection was replaced, so the server knows it under a
    new id). The row holds the id of the client connection that wrote it: the statements run on the connection
    whose id was read, and a connection that is replaced meanwhile is `ConnectionLost`."""
    if lane not in LANES:
        raise ValueError("unknown lane %r" % (lane,))
    if conn._in_block:
        raise LaneMisuse("this connection is inside a lane transaction")
    epoch = _epoch(epoch)
    d = as_deadline(deadline, conn.settings.statement_deadline_s)
    while True:
        try:
            return _register_once(conn, lane, epoch, d)
        except Conflict:
            if not retry_conflict:
                raise
            d.sleep(0.05 + 0.2 * random.random(), "registering a connection")


def _register_once(conn, lane, epoch, d):
    sess = conn.ensure(d)
    sid = sess.server_id
    where = "lane %s: registration" % lane
    cleanup = []

    def budget():
        if not cleanup:
            cleanup.append(Deadline.after(conn.settings.rollback_deadline_s))
        return cleanup[0]

    def rollback():
        if sess.state != "open":
            conn._drop_session(sess)
            return
        out = sess.call(sess.statement(SITE_EXECUTE, "ROLLBACK", None, None, tx=False), budget())
        if out.ok and not out.late:
            return
        if out.sent and out.returned and not out.late and out.error is not None and _NO_TX.search(str(out.error)):
            sess.in_tx = False
            return
        conn._drop_session(sess)

    def failed(out, statement):
        """The error of a step before COMMIT that did not succeed (the rules of `Transaction._failure`)."""
        if not out.sent:
            rollback()
            return DeadlineExceeded("%s: the deadline passed before %s" % (where, statement))
        if out.late or out.error is None:
            conn._drop_session(sess)
            return DeadlineExceeded("%s: %s" % (where, statement))
        kind = errors.kind_of(out.error)
        if kind == "transport":
            conn._drop_session(sess)
            return ConnectionLost("%s: connection lost during %s: %s" % (where, statement, errors.label(out.error)))
        rollback()
        if kind == "conflict":
            return Conflict("%s: write-write conflict in %s" % (where, statement))
        return errors.refused(out.error, where, statement)

    def step(sql, params, fetch, tx, statement):
        try:
            out = sess.call(sess.statement(SITE_EXECUTE, sql, params, fetch, tx=tx), d)
        except BaseException:
            conn._drop_session(sess)                 # the wait was broken off: what the statement did is not known
            raise
        if out.ok and not out.late:
            return out.result
        raise failed(out, statement)

    step("BEGIN", None, None, True, "BEGIN")
    step(REGISTER_SQL, [epoch, lane, sid], None, None, "INSERT")
    for sql in (REGISTER_FENCE_SQL, REGISTER_FENCE_RESTORE_SQL):
        n = step(sql, [lane, epoch], "count", None, "UPDATE")
        if n != 1:
            rollback()
            raise FenceLost("lane %s: epoch %d no longer leads (the fence row has another epoch)" % (lane, epoch))
    job = sess.submit(sess.commit())
    if job is None:
        conn._drop_session(sess)
        raise ConnectionLost("%s: connection closed before COMMIT" % where)
    try:
        out = sess.wait(job, d)
    except BaseException:
        conn._drop_session(sess)
        raise
    if not out.ok:
        err = out.error
        if out.returned and not out.late and err is not None and errors.kind_of(err) == "conflict":
            rollback()
            raise Conflict("%s: write-write conflict at COMMIT" % where)
        conn._drop_session(sess)
        why = errors.label(err) if err is not None else "no answer within the deadline and the grace period"
        raise ConnectionLost("%s: the outcome of COMMIT is unknown (%s); the connection is replaced" % (where, why))
    if out.late:
        conn._drop_session(sess)                     # it did commit; the next call registers a new connection
    if conn._session is not sess or sess.state != "open":
        raise ConnectionLost("%s: the connection was replaced while it registered" % where)
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
            if _interrupt_local(sid, d):
                done += 1
    if listed:
        log.info("interrupted connections of earlier epochs",
                 extra={"cdc": {"before_epoch": epoch, "listed": listed, "interrupted": done}})
    return listed, done


def cleanup(conn_or_tx, epoch, deadline=None):
    """Remove the rows of epochs before the previous one (design 4.1). Called by the leader with its own epoch, on
    a connection or inside a `control`-lane transaction. Returns the number of rows removed."""
    return conn_or_tx.execute(CLEANUP_SQL, [_epoch(epoch)], deadline)
