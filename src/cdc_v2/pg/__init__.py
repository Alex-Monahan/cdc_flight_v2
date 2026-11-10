"""Source side of the pipeline (package P, contract C7): connections with deadlines, the catalog and its
observation, the publication and replica identity, the slot and its facts, timeline history, the heartbeat.

    from cdc_v2 import pg
    conn = pg.connect("replica", Deadline.after(10))

See docs/design/source.md.
"""
from cdc_v2.pg import catalog, errors, heartbeat, preflight, publication, replica_identity, slot, timeline
from cdc_v2.pg.conn import PgConnection, SessionSettingsMismatch, connect
from cdc_v2.pg.settings import (FIXED_SESSION_SETTINGS, NodeSettings, Password, SourceSettings, configure,
                                configured)

__all__ = ["catalog", "errors", "heartbeat", "preflight", "publication", "replica_identity", "slot", "timeline",
           "connect", "PgConnection", "SessionSettingsMismatch", "NodeSettings", "Password", "SourceSettings",
           "configure", "configured", "FIXED_SESSION_SETTINGS"]
