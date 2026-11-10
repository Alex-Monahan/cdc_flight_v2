"""Shared fixtures. The default run is hermetic; tier-T1 fixtures are used only by tests marked `t1`.

Which cluster and which MotherDuck prefix the T1 fixtures use comes from the environment
(docs/design/p3_plan.md section 4, docs/oracle.md section 2):

    export CDC_V2_PG_HOME=~/.cache/cdc_flight_v2/pg-<pkg> CDC_V2_PG_PORT_BASE=<base> CDC_V2_MD_PREFIX=cdc_v2_t_<pkg>_
    tools/with-env uv run pytest -m t1

* `source_db` / `make_source_db` - a private database `cdc_v2_t_<random>` on the cluster named by
  `CDC_V2_PG_HOME` (built on `CDC_V2_PG_PORT_BASE` if needed). The default home (55432/55433) only with
  `CDC_V2_T1_SHARED=1`. The fixture may run `localpg up`; it never stops or changes that cluster.
* `dest_db` / `make_dest_db` - a MotherDuck database `<CDC_V2_MD_PREFIX><random>`, created and dropped here.
* `private_cluster` - a primary and replica of their own on a block of 8 ports leased from 55840-56095, for
  destructive drills; stopped and removed afterwards.
* `evidence` - `evidence("p3/oracle").write_json(name, obj)` / `.write_text(name, text)`: thin helpers over
  `cdc_v2.testing.harness_evidence.write_evidence`, the one writer of evidence files.
"""
import os
from pathlib import Path

import pytest

from cdc_v2.evidence import default_redactor

PG_BIN = os.environ.get("CDC_V2_PG_BIN", "/opt/homebrew/opt/postgresql@18/bin")
ROOT = Path(__file__).resolve().parents[1]


def pytest_configure(config):
    """Temporary files (tmp_path, DuckDB spill files) live under ~/.cache/cdc_flight_v2/tmp, not the system temp."""
    import tempfile
    tmp = Path(os.environ.get("CDC_V2_CACHE", Path.home() / ".cache" / "cdc_flight_v2")) / "tmp"
    tmp.mkdir(parents=True, exist_ok=True)
    os.environ["TMPDIR"] = str(tmp)
    tempfile.tempdir = None


def pytest_collection_modifyitems(config, items):
    missing = []
    if not os.access(os.path.join(PG_BIN, "postgres"), os.X_OK):
        missing.append("Postgres binaries not found in %s" % PG_BIN)
    if not (os.environ.get("motherduck_token") or os.environ.get("MOTHERDUCK_TOKEN")):
        missing.append("no MotherDuck token (run through tools/with-env)")
    if missing:
        skip = pytest.mark.skip(reason="; ".join(missing))
        for item in items:
            if "t1" in item.keywords:
                item.add_marker(skip)


@pytest.fixture(scope="session")
def shared_pg():
    """The cluster of the T1 fixtures (CDC_V2_PG_HOME; see the module docstring). Fails with what to export."""
    from cdc_v2.testing.pg import HarnessError, fixture_cluster
    try:
        cluster = fixture_cluster()
    except HarnessError as e:
        pytest.fail(str(e), pytrace=False)
    for s in cluster.secrets():
        default_redactor().add(s)
    return cluster


@pytest.fixture(scope="session")
def make_source_db(shared_pg):
    from cdc_v2.testing.pg import PrivateDatabase
    made = []

    def make():
        db = PrivateDatabase(shared_pg)
        made.append(db)
        return db

    yield make
    for db in made:
        db.drop()


@pytest.fixture
def source_db(shared_pg):
    from cdc_v2.testing.pg import PrivateDatabase
    db = PrivateDatabase(shared_pg)
    yield db
    db.drop()


@pytest.fixture(scope="session")
def make_dest_db():
    from cdc_v2.testing.md import TestDestination
    made = []

    def make():
        d = TestDestination()
        made.append(d)
        return d

    yield make
    for d in made:
        d.drop()


@pytest.fixture
def dest_db():
    from cdc_v2.testing.md import TestDestination
    d = TestDestination()
    yield d
    d.drop()


@pytest.fixture
def private_cluster():
    from cdc_v2.testing.pg import PrivateCluster
    c = PrivateCluster()
    for s in c.secrets():
        default_redactor().add(s)
    yield c
    c.stop()


class _Evidence:
    """evidence/<item>/<name> through write_evidence (redacted, scrubbed, refused if anything is left)."""

    def __init__(self, item):
        self.dir = ROOT / "evidence" / item

    def write_json(self, name, obj):
        from cdc_v2.testing.harness_evidence import write_evidence
        name = name if name.endswith(".json") else name + ".json"
        return write_evidence(self.dir / name, obj)

    def write_text(self, name, text):
        from cdc_v2.testing.harness_evidence import write_evidence
        return write_evidence(self.dir / name, text if text.endswith("\n") else text + "\n")


@pytest.fixture(scope="session")
def evidence():
    return _Evidence
