"""Package P: pre-flight checks as the runtime role (marker localpg), Postgres 18 and 16."""
from __future__ import annotations

import pytest

from cdc_v2.deadline import Deadline
from cdc_v2.pg import preflight

from .conftest import MAJOR, record

pytestmark = [pytest.mark.localpg, pytest.mark.timeout(300)]


def test_runtime_role_can_read_pg_control_checkpoint(cluster):
    out = {}
    for node in ("primary", "replica"):
        with cluster.connect(node) as conn:
            p = preflight.check(conn, deadline=Deadline.after(10))
        assert p.problems == (), p.problems
        assert p.control_checkpoint_callable and p.control_system_callable and p.replication_role
        assert p.checkpoint_next_xid and p.checkpoint_next_xid > 3
        assert p.in_recovery == (node == "replica")
        out[node] = {"pg_control_checkpoint_callable": True, "checkpoint_next_xid": p.checkpoint_next_xid,
                     "problems": list(p.problems)}
    record("preflight", "runtime_role_pg%d" % MAJOR, out)


def test_preflight_names_the_missing_grant(cluster):
    """Without EXECUTE for PUBLIC (as a provider might configure it), the check names the grant that fixes it."""
    rt = cluster.passwords["runtime"]["user"]
    cluster.boot("primary", "revoke execute on function pg_catalog.pg_control_checkpoint() from public",
                 db=cluster.info["database"])
    try:
        cluster.replay_to_primary()
        with cluster.connect("replica") as conn:
            p = preflight.check(conn, deadline=Deadline.after(10))
        assert not p.control_checkpoint_callable and p.checkpoint_next_xid is None
        assert any("GRANT EXECUTE ON FUNCTION pg_catalog.pg_control_checkpoint()" in x for x in p.problems)
        cluster.boot("primary", "grant execute on function pg_catalog.pg_control_checkpoint() to %s" % rt,
                     db=cluster.info["database"])
        cluster.replay_to_primary()
        with cluster.connect("replica") as conn:
            assert preflight.check(conn, deadline=Deadline.after(10)).problems == ()
    finally:
        cluster.boot("primary", "revoke execute on function pg_catalog.pg_control_checkpoint() from %s" % rt,
                     db=cluster.info["database"])
        cluster.boot("primary", "grant execute on function pg_catalog.pg_control_checkpoint() to public",
                     db=cluster.info["database"])


def test_preflight_on_a_standby_without_feedback(breakable):
    c = breakable
    c.lp("set", "replica", "hot_standby_feedback=off")
    with c.connect("replica") as conn:
        p = preflight.check(conn, deadline=Deadline.after(10))
    assert any("hot_standby_feedback" in x for x in p.problems)
