"""Fixtures for the destination layer (package D).

Default run (tier T0): local in-memory DuckDB, no network. Every test that uses `dest` also proves that the layer
left no client connection and no thread behind.

Marker `md`: real MotherDuck, databases named `cdc_v2_t_d_*` only, created and dropped per test. Opt-in:

    tools/with-env uv run -q pytest -q -m md tests/destination

`pyproject.toml` does not register the marker and does not deselect it (this package may not edit that file), so
this conftest registers it and deselects `md` tests unless the command line asks for them.

Marker `d_full`: the rest of the full matrices of this package (fix round, item 7). The default run keeps one case
per fault site of the deadline matrix of `test_lane.py`, one example machine of the enumeration of
`test_state.py` and one shape of the take-over at every point of `test_fix_round.py`; the other cases carry
`d_full` and run only when asked for:

    uv run -q pytest -q -m d_full tests/destination

A MotherDuck run (`-m md`) keeps them too.
"""
import datetime
import json
import os
import platform
import re

import duckdb
import pytest

from cdc_v2 import crashpoints, faultsites
from cdc_v2.testing import destination as td
from cdc_v2.testing.faults import FaultControl
from cdc_v2.testing.harness_evidence import write_evidence

MD_PREFIX = "cdc_v2_t_d_"
ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
EVIDENCE = os.path.join(ROOT, "evidence", "p3", "destination")


def pytest_configure(config):
    config.addinivalue_line(
        "markers", "md: needs real MotherDuck through tools/with-env; creates and drops databases cdc_v2_t_d_* "
                   "(opt-in: -m md)")
    config.addinivalue_line(
        "markers", "d_full: the rest of the full test matrices of package D, left out of the default run "
                   "(opt-in: -m d_full)")


def _wants_md(config):
    expr = config.getoption("markexpr") or ""
    return bool(re.search(r"(?<!not )\bmd\b", expr))


def _wants_full(config):
    expr = config.getoption("markexpr") or ""
    return bool(re.search(r"(?<!not )\bd_full\b", expr))


def pytest_collection_modifyitems(config, items):
    if not (_wants_full(config) or _wants_md(config)):
        full = [i for i in items if "d_full" in i.keywords]
        if full:
            config.hook.pytest_deselected(items=full)
            items[:] = [i for i in items if "d_full" not in i.keywords]
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


def run_command():
    """The command that runs now, as a grader repeats it: the arguments pytest was given (fix round, item 5)."""
    import shlex
    import sys
    return "tools/with-env uv run -q pytest " + " ".join(shlex.quote(a) for a in sys.argv[1:])


def run_commit():
    """The commit of the tree that runs now, and whether the files the tests use differ from it."""
    import subprocess
    try:
        head = subprocess.run(["git", "-C", ROOT, "rev-parse", "--short", "HEAD"], capture_output=True, text=True,
                              timeout=10).stdout.strip()
        dirty = subprocess.run(["git", "-C", ROOT, "status", "--porcelain", "--", "src", "tests", "tools",
                                "pyproject.toml", "uv.lock"], capture_output=True, text=True, timeout=10).stdout.strip()
    except Exception:                                        # noqa: BLE001 - recorded as unknown
        return None
    return head + ("+changes" if dirty else "")


def add_evidence(name, key, value, what=None):
    """Put one result into `evidence/p3/destination/<name>` (a JSON file: a header and ``results``, one entry per
    test). The entries of tests that did not run this time stay as they are. `write_evidence` scrubs and checks."""
    path = os.path.join(EVIDENCE, name)
    try:
        with open(path, encoding="utf-8") as f:
            doc = json.load(f)
    except (FileNotFoundError, ValueError):
        doc = {}
    doc.setdefault("results", {})
    if what:
        doc["what"] = what
    doc["command"] = "see results.<key>.command (each result names the run that wrote it)"
    doc["versions"] = {"duckdb_client": duckdb.__version__, "python": platform.python_version(),
                       "platform": "%s %s" % (platform.system(), platform.machine())}
    doc["machine_note"] = ("10 cores, 16 GB, shared with up to three other workers of the same build (their "
                           "tests, Postgres clusters and JVMs may have been running); times are indications only")
    value = dict(value)
    value["date_utc"] = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M")
    value["commit"] = run_commit()
    value["command"] = run_command()
    doc["results"][key] = value
    write_evidence(path, doc)


@pytest.fixture
def evidence():
    """``evidence(file_name, key, value, what=None)``: record what an `md` test measured."""
    return add_evidence
