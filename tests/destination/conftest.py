"""Fixtures for the destination layer (package D).

Default run (tier T0): local in-memory DuckDB, no network. Every test that uses `dest` also proves that the layer
left no client connection and no thread behind.

Marker `md`: real MotherDuck, databases named `cdc_v2_t_d_*` only, created and dropped per test. Opt-in:

    tools/with-env uv run -q pytest -q -m md tests/destination

`pyproject.toml` does not register the marker and does not deselect it (this package may not edit that file), so
this conftest registers it and deselects `md` tests unless the command line asks for them.
"""
import os
import re

import pytest

from cdc_v2 import crashpoints, faultsites
from cdc_v2.testing import destination as td
from cdc_v2.testing.faults import FaultControl

MD_PREFIX = "cdc_v2_t_d_"
ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
EVIDENCE = os.path.join(ROOT, "evidence", "p3", "destination")


def pytest_configure(config):
    config.addinivalue_line(
        "markers", "md: needs real MotherDuck through tools/with-env; creates and drops databases cdc_v2_t_d_* "
                   "(opt-in: -m md)")


def _wants_md(config):
    expr = config.getoption("markexpr") or ""
    return bool(re.search(r"(?<!not )\bmd\b", expr))


def pytest_collection_modifyitems(config, items):
    if _wants_md(config):
        if not os.environ.get("motherduck_token"):
            skip = pytest.mark.skip(reason="no MotherDuck token in the environment (run through tools/with-env)")
            for item in items:
                if "md" in item.keywords:
                    item.add_marker(skip)
        return
    mine = [i for i in items if "md" in i.keywords]
    if mine:
        config.hook.pytest_deselected(items=mine)
        items[:] = [i for i in items if "md" not in i.keywords]


@pytest.fixture
def no_leftovers():
    """After the test: no client connection and no thread of the layer is left in this process."""
    yield
    assert td.wait_no_leftovers(10.0), "the destination layer left a connection or a thread behind"


@pytest.fixture
def dest(no_leftovers):
    """A private local destination with the leader tables; nobody leads yet (epoch 0)."""
    with td.local_destination() as d:
        yield d


@pytest.fixture
def led(dest):
    """`dest` after a take-over and the migrations. ``led.epoch`` is the leading epoch (1)."""
    dest.epoch = dest.lead()
    return dest


@pytest.fixture
def faults(tmp_path):
    """Fault sites of this process, controlled by the test."""
    fc = FaultControl(tmp_path / "faults")
    faultsites.reconfigure(fc.env())
    try:
        yield fc
    finally:
        fc.clear()
        faultsites.reconfigure({})


@pytest.fixture
def crash_at():
    """``crash_at(point, hit=1)``: make a crash point of this process raise `CrashPointHit`."""
    def arm(point, hit=1):
        crashpoints.reconfigure(crashpoints.env_for(point, hit=hit, action="raise"))
    try:
        yield arm
    finally:
        crashpoints.reconfigure({})


@pytest.fixture
def md_dest(no_leftovers):
    """A MotherDuck database `cdc_v2_t_d_<random>` with the leader tables; dropped after the test."""
    with td.motherduck_destination(MD_PREFIX) as d:
        yield d
