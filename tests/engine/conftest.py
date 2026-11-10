"""Fixtures of the engine tests (package E).

Default run: hermetic tests only. Opt-in:
    uv run pytest -m jvm tests/engine                          # local JVM, no Postgres
    uv run pytest -m "jvm and localpg" tests/engine            # the private pair pg-e (ports 55620-55627)
    CDC_V2_E_LONG=1 uv run pytest -m ... tests/engine          # the long variants (1,000 detection seeds, 1M rows)
"""
import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


def pytest_configure(config):
    config.addinivalue_line("markers", "jvm: needs a local JVM and the locked Debezium jars (opt-in: -m jvm)")


def pytest_collection_modifyitems(config, items):
    expr = config.getoption("-m") or ""
    if "jvm" in expr and "not jvm" not in expr:
        return
    skip = pytest.mark.skip(reason="needs a JVM: select with -m jvm")
    for item in items:
        if "jvm" in item.keywords:
            item.add_marker(skip)


@pytest.fixture(scope="session")
def artifacts():
    from cdc_v2.stream import jvm
    return jvm.prepare()


@pytest.fixture(scope="session")
def pair():
    from pgsupport import Pair, lp
    lp("up")
    p = Pair()
    yield p
    if os.environ.get("CDC_V2_E_KEEP_PG") != "1":
        lp("down", check=False)


@pytest.fixture
def scenario(pair):
    from pgsupport import Scenario
    made = []

    def make(*tables_sql):
        s = Scenario(pair, tables_sql)
        made.append(s)
        return s

    yield make
    for s in made:
        s.close()
