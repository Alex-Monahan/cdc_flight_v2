"""Fixtures for the tools/localpg self-tests.

The self-tests run on their own home and ports (55460-55467 by default), so they never disturb a development cluster on
55432-55439. Run: tools/with-env uv run pytest -m localpg tests/localpg   (opt-in; the default run deselects it)
"""
import json
import os

import pytest

from localpg_support import PG_BIN, PG_MAJOR, TEST_HOME, Cluster, canonical_topology, lp, wait_for


def pytest_collection_modifyitems(config, items):
    if not os.access(os.path.join(PG_BIN, "postgres"), os.X_OK):
        skip = pytest.mark.skip(reason="Postgres binaries not found in %s" % PG_BIN)
        for item in items:
            if "localpg" in item.keywords:
                item.add_marker(skip)


@pytest.fixture(scope="session", autouse=True)
def stop_clusters_at_end():
    yield
    if os.path.isdir(TEST_HOME):
        lp("down", check=False)


@pytest.fixture
def cluster():
    """A running primary + replica on the self-test home (reset if the topology was changed by an earlier test)."""
    st = json.loads(lp("status", "--json", check=False).stdout) if os.path.isdir(TEST_HOME) else {"nodes": []}
    if st.get("initialized") and canonical_topology(st) and st.get("pg_major") == PG_MAJOR:
        lp("up")
    else:
        lp("reset")
    c = Cluster()
    wait_for(lambda: c.node("replica").get("wal_receiver_status") == "streaming", what="replica streaming")
    return c
