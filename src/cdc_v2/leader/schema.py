"""The leader package's own `_cdc` objects (migration list ``leader``) and the contenders' notice.

- `_cdc.leader_record`: a view of the leader row and the heartbeat row, the one "leader record" SPEC 4.2 and 6.1
  name (run id, Flight id, run number, fencing epoch, state, heartbeat time and its age by MotherDuck's clock).
- `_cdc.contenders`: notices of runs of another Flight that found a healthy leader of this destination. Such a run
  is not the leader: it may not migrate, it has no lane and it cannot write `_cdc.alerts` behind the fence. It
  appends one notice (`NOTICE_SQL`, an insert that conflicts with nothing and changes no leader state) and exits
  non-zero; the leader turns notices into the alert `contending_flight` (`cdc_v2.leader.startup.ContenderWatch`).
  Before the first leader migrated, the table does not exist and the contender only logs (its run is `FAILED`
  with exit code 3 on the platform either way).
"""
from __future__ import annotations

from cdc_v2.md import schema as _schema

NOTICE_SQL = ("INSERT INTO _cdc.contenders (noticed_at, flight_id, run_number, run_id, leader_epoch, "
              "leader_flight_id, leader_run_number) VALUES (uuid_extract_timestamp(uuidv7()), ?, ?, ?, ?, ?, ?)")

LEADER_RECORD_VIEW_SQL = (
    "CREATE VIEW _cdc.leader_record AS SELECT l.epoch, l.flight_id, l.run_number, l.run_id, l.state, l.taken_at, "
    "h.epoch AS heartbeat_epoch, h.beat_at, h.progress_at, h.phase, "
    "epoch_ms(uuid_extract_timestamp(uuidv7())) - epoch_ms(h.beat_at) AS heartbeat_age_ms "
    "FROM _cdc.leader AS l, _cdc.heartbeat AS h"
)
CONTENDERS_TABLE_SQL = (
    "CREATE TABLE _cdc.contenders (noticed_at TIMESTAMPTZ, flight_id VARCHAR, run_number BIGINT, run_id VARCHAR, "
    "leader_epoch BIGINT, leader_flight_id VARCHAR, leader_run_number BIGINT)"
)

MIGRATIONS = _schema.register_migrations("leader", (
    _schema.Step("leader_record_view", (LEADER_RECORD_VIEW_SQL,)),
    _schema.Step("contenders_table", (CONTENDERS_TABLE_SQL,)),
))

__all__ = ["NOTICE_SQL", "LEADER_RECORD_VIEW_SQL", "CONTENDERS_TABLE_SQL", "MIGRATIONS"]
