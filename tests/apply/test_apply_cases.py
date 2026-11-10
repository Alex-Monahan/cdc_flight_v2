"""Package A, tier T0: the named cases (apply_cases.py) on local DuckDB. Every case runs in the local form; the
relation form (the same plan, run inside the transaction) runs for every third case and for every catch-up and
error case, or for all with CDC_V2_APPLY_FULL=1. All cases run in both forms on MotherDuck in test_apply_md.py."""
from __future__ import annotations

import os

import pytest

from apply_cases import all_cases
from apply_harness import local_conn, run_case

_ALL = all_cases()
_FULL = os.environ.get("CDC_V2_APPLY_FULL") == "1"
CASES = [(c, "local") for c in _ALL] + [(c, "relation") for i, c in enumerate(_ALL)
                                        if _FULL or i % 3 == 0 or c.mode == "catch_up" or c.error]


@pytest.mark.parametrize("case,form", CASES, ids=["%s-%s" % (c.name, f) for c, f in CASES])
def test_case(case, form):
    run_case(case, local_conn(), form)
