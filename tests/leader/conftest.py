"""Fixtures of the leadership tests (package L).

- Default run (tier T0): local DuckDB in this process, and real child processes that need no network (the watch,
  the local platform). Timings in tenths of seconds.
- Marker `md`: real MotherDuck (databases `cdc_v2_t_l_*` only, created and dropped per test), `LocalPlatform`, the
  real bootstrap and the toy pipeline in real processes:

      tools/with-env uv run -q pytest -q -m md tests/leader

- Marker `l_full`: the long T0 matrices (more repetitions, every watch case); opt-in with `-m l_full`.
"""
import importlib.util
import os
import re
import sys

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(os.path.dirname(HERE))
BOOTSTRAP = os.path.join(ROOT, "flights", "cdc_v2", "main.py")
MD_PREFIX = "cdc_v2_t_l_"


def load_bootstrap(name="cdc_v2_bootstrap"):
    """`flights/cdc_v2/main.py` as a module (it is a script, not part of the package)."""
    if name in sys.modules:
        return sys.modules[name]
    spec = importlib.util.spec_from_file_location(name, BOOTSTRAP)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod


def pytest_configure(config):
    config.addinivalue_line("markers", "l_full: the long matrices of package L (opt-in: -m l_full)")


def _wants(config, marker):
    expr = config.getoption("markexpr") or ""
    return bool(re.search(r"(?<!not )\b%s\b" % marker, expr))


def pytest_collection_modifyitems(config, items):
    if not (_wants(config, "l_full") or _wants(config, "md")):
        full = [i for i in items if "l_full" in i.keywords]
        if full:
            config.hook.pytest_deselected(items=full)
            items[:] = [i for i in items if "l_full" not in i.keywords]
    if _wants(config, "md") and not (os.environ.get("motherduck_token") or os.environ.get("MOTHERDUCK_TOKEN")):
        skip = pytest.mark.skip(reason="no MotherDuck token in the environment (run through tools/with-env)")
        for item in items:
            if "md" in item.keywords:
                item.add_marker(skip)


@pytest.fixture(scope="session")
def boot():
    return load_bootstrap()
