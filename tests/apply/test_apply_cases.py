"""Package A, tier T0: the named cases (apply_cases.py) on local DuckDB, in both change-set forms. The same cases
run on MotherDuck in test_apply_md.py (marker md)."""
from __future__ import annotations

import pytest

from apply_cases import all_cases
from apply_harness import local_conn, run_case

CASES = [(c, f) for c in all_cases() for f in c.forms]


@pytest.mark.parametrize("case,form", CASES, ids=["%s-%s" % (c.name, f) for c, f in CASES])
def test_case(case, form):
    run_case(case, local_conn(), form)
