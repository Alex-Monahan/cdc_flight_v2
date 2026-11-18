"""Fixtures of the stream's tests (package S). The default run is hermetic: a scripted slot and engine, a local
DuckDB destination through package D's lanes."""
import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


@pytest.fixture
def dest():
    from cdc_v2.testing import destination as td
    with td.local_destination() as d:
        yield d


@pytest.fixture
def rig(dest, tmp_path):
    from rig import Rig
    return Rig(dest, tmp_path)
