"""Alert records (SPEC 6.2, contract C4 "Alerts"): packages raise alerts only through this module.

    alerts.emit(tx, "slot_lost", "critical", {"slot": slot_name, "applied_position": pos})      # in a data transaction
    alerts.emit_now(control_lane, "lag_high", "warning", {"lag_seconds": 412.5})                # on its own

`emit` writes **one row** into `_cdc.alerts` inside the caller's lane transaction: the alert exists exactly when the
data change it belongs to is committed. It only inserts, so it can never conflict with another writer. Delivery to
Slack, de-duplication, rate limiting and the "resolved" message (SPEC 6.2) are built later and read this table; the
table has the columns they need (`dedup_key`, `first_at`, `last_at`, `count`, `state`, `delivery_state`,
`delivery_attempts`).

**No row value can reach an alert** (SPEC 8.3). `fields` holds identifiers, positions, counts and durations only:
a flat mapping from a lower-case name to an integer, a finite float, or a string of at most `MAX_STRING` printable
characters. Anything else is refused with `AlertError` before the database is touched.

The table exists after the migrations ran (`md.migrate`); before that an emit fails with `md.StatementError`
(``error_class`` is ``CatalogException``).
"""
from __future__ import annotations

import json
import logging
import random
import re
import uuid

from cdc_v2 import state
from cdc_v2.md import schema as _schema
from cdc_v2.md.connection import as_deadline
from cdc_v2.md.errors import Conflict, ConnectionLost, LaneMisuse
from cdc_v2.md.lane import Transaction

log = logging.getLogger("cdc_v2.alerts")

SEVERITIES = ("info", "warning", "critical")
MAX_FIELDS = state.MAX_FIELDS
MAX_STRING = state.MAX_STRING
DELIVERY_PENDING = "pending"

_KIND = re.compile(r"^[a-z][a-z0-9_]{0,63}$")


class AlertError(state.FieldError):
    """The alert was refused before it reached the database: a bad kind, severity or field."""


#: An alert is open until the condition it reports is over.
ALERT = state.declare(state.Machine(
    name="alert",
    states=("open", "resolved"),
    transitions=(("open", "resolved"),),
    table="_cdc.alerts",
    key_columns=("alert_id",),
    state_column="state",
    creators=state.FRESH_IDS,                      # every alert is a new random id; any lane records one
    initial=("open",),
    description="One alert record (SPEC 6.2). `resolved` means the condition it reported is over.",
))

MIGRATIONS = _schema.register_migrations("alerts", (
    _schema.Step("alerts_table", (
        "CREATE TABLE _cdc.alerts (alert_id VARCHAR, kind VARCHAR, severity VARCHAR, dedup_key VARCHAR, "
        "first_at TIMESTAMPTZ, last_at TIMESTAMPTZ, count BIGINT, state VARCHAR, delivery_state VARCHAR, "
        "delivery_attempts BIGINT, fields JSON, epoch BIGINT, lane VARCHAR)",
    )),
))


def check_fields(fields):
    """Return ``fields`` as a plain dict, or raise `AlertError`. Allowed values: an integer (64-bit, not a bool), a
    finite float, a string of at most `MAX_STRING` printable characters (the rules of `state.check_fields`, which
    the detail of a state transition follows too)."""
    return state.check_fields(fields, AlertError, "alert field")


def dedup_key(kind, fields=None, key=()):
    """The de-duplication key of an alert: its kind, and the values of the fields named in ``key``. Alerts with the
    same key report the same condition."""
    if not isinstance(kind, str) or not _KIND.match(kind):
        raise AlertError("an alert kind is a lower-case identifier")
    fields = check_fields(fields)
    key = (key,) if isinstance(key, str) else tuple(key)
    parts = [kind]
    for name in key:
        if name not in fields:
            raise AlertError("the alert has no field %s for its de-duplication key" % (name,))
        parts.append("%s=%s" % (name, json.dumps(fields[name])))
    return "|".join(parts)


def _checked(kind, severity, fields, key):
    if severity not in SEVERITIES:
        raise AlertError("an alert severity is one of %s" % ", ".join(SEVERITIES))
    fields = check_fields(fields)
    return kind, severity, fields, dedup_key(kind, fields, key)


def _insert(tx, kind, severity, fields, dkey):
    if not isinstance(tx, Transaction):
        raise LaneMisuse("alerts.emit needs an md.Transaction (a lane transaction), got %s" % type(tx).__name__)
    alert_id = uuid.uuid4().hex
    state.create(tx, ALERT, alert_id, "open", "emitted", values={
        "kind": kind, "severity": severity, "dedup_key": dkey, "first_at": state.NOW, "last_at": state.NOW,
        "count": 1, "delivery_state": DELIVERY_PENDING, "delivery_attempts": 0,
        "fields": json.dumps(fields, sort_keys=True), "epoch": tx.epoch, "lane": tx.lane,
    })
    # Written inside the caller's open transaction, so that the alert is seen even when that transaction fails.
    # `_cdc.alerts` is the record of what was committed; the field `committed` says what the line cannot know.
    log.warning("alert", extra={"cdc": {"alert_id": alert_id, "kind": kind, "severity": severity,
                                        "dedup_key": dkey, "fields": fields, "epoch": tx.epoch,
                                        "lane": tx.lane, "committed": False}})
    return alert_id


def emit(tx, kind, severity, fields=None, key=()):
    """Record an alert inside the open lane transaction ``tx``. Returns the alert's id.

    ``kind``: what happened, a lower-case identifier. ``severity``: one of `SEVERITIES`. ``fields``: see
    `check_fields`. ``key``: the names of the fields that, with the kind, tell one condition from another (for
    example ``("table_id",)``); default: the kind alone."""
    return _insert(tx, *_checked(kind, severity, fields, key))


def emit_now(lane, kind, severity, fields=None, key=(), deadline=None):
    """Record an alert in a transaction of its own on ``lane``. Returns the alert's id.

    A conflict or a lost connection before the commit is tried again until the deadline. `FenceLost` (this run no
    longer leads) and `CommitUnknown` (the alert may or may not be recorded) are passed on."""
    args = _checked(kind, severity, fields, key)
    d = as_deadline(deadline, lane.conn.settings.transaction_deadline_s)
    while True:
        try:
            with lane.transaction(d) as tx:
                return _insert(tx, *args)
        except (Conflict, ConnectionLost):
            d.sleep(0.05 + 0.2 * random.random(), "recording an alert")


def resolve(tx, dkey, cause):
    """Mark every open alert with the de-duplication key ``dkey`` (see `dedup_key`) as resolved, inside ``tx``.
    Returns how many there were."""
    rows = tx.fetchall("SELECT alert_id FROM _cdc.alerts WHERE dedup_key = ? AND state = 'open' ORDER BY alert_id",
                       [dkey])
    for (alert_id,) in rows:
        state.transition(tx, ALERT, alert_id, "open", "resolved", cause)
    return len(rows)


__all__ = ["emit", "emit_now", "resolve", "dedup_key", "check_fields", "AlertError", "ALERT", "SEVERITIES",
           "MAX_FIELDS", "MAX_STRING", "DELIVERY_PENDING"]
