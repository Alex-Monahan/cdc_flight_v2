"""Shared fixtures. The default run is hermetic; tier-T1 fixtures are used only by tests marked `t1`:

    tools/with-env uv run pytest -m t1

T1 fixtures give every test (and every one of several test processes running at once) its own source and
destination:

* `source_db` / `make_source_db` - a private database `cdc_v2_t_<random>` on the shared development cluster
  (tools/localpg, primary 55432, replica 55433). The fixture may run `localpg up`; it never stops or changes the
  shared cluster.
* `dest_db` / `make_dest_db` - a MotherDuck database `cdc_v2_t_<random>`, created and dropped by the fixture.
* `private_cluster` - a primary and replica of their own on a leased port block (55520 upwards, steps of 8), for
  destructive drills; stopped and removed afterwards.
* `evidence` - `evidence("p3/oracle")` returns a redacting writer for evidence/<item>/.
"""
import os

import pytest

from cdc_v2.evidence import EvidenceWriter, default_redactor

PG_BIN = os.environ.get("CDC_V2_PG_BIN", "/opt/homebrew/opt/postgresql@18/bin")


def pytest_configure(config):
    """Temporary files (tmp_path, DuckDB spill files) live under ~/.cache/cdc_flight_v2/tmp, not the system temp."""
    import tempfile
    from pathlib import Path
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
    from cdc_v2.testing.pg import shared_cluster
    cluster = shared_cluster()
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


@pytest.fixture(scope="session")
def evidence():
    return lambda item: EvidenceWriter(item)
