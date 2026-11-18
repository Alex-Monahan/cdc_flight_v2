"""Apply's tests run on package D's real lane (contract C4): `md.Lane` and `md.Transaction` over a destination of
`cdc_v2.testing.destination`, a local DuckDB in memory (tier T0) or a MotherDuck database `cdc_v2_t_a_<random>`.

`LaneDB` is what the tests call a destination:

* `transaction()` opens a block of the lane `stream` and yields D's `md.Transaction` itself; with `check=True` it
  yields `Checked`, which records what apply sends and checks at every call the rules of C4 that apply keeps
  (no deadline of its own; registered names `cdc_v2_apply_<step>`);
* `execute`, `register`, `unregister` (and `raw`) set the table up and read it back on a plain client connection,
  outside the layer, as `cdc_v2.testing.destination.Destination.run` does (the layer's own connections take reads
  only outside a lane transaction).

There is no stand-in any more (`apply_standin.StandInTx` was deleted with the fix round after D's merge).
"""
from __future__ import annotations

import contextlib
import re
import subprocess
from pathlib import Path

from cdc_v2.deadline import Deadline
from cdc_v2.md.connection import counts_rows, statement_kind
from cdc_v2.testing import destination as td

ROOT = Path(__file__).resolve().parents[2]
#: The names apply registers on a lane transaction: `cdc_v2_apply_<step>` (and D's rule for any name).
APPLY_NAME = re.compile(r"cdc_v2_apply_[a-z0-9_]{1,50}")
D_NAME = re.compile(r"[a-z_][a-z0-9_]{0,62}")
#: Statements whose count apply reads: C4 promises a count for exactly these kinds (without RETURNING).
COUNTED_KINDS = {"INSERT", "UPDATE", "DELETE"}

_open = []                                   # every LaneDB that is not closed yet (conftest closes what a test left)


class LaneDB:
    """A destination for apply's tests: D's lane `stream` on its own client connection, and a plain client
    connection for set-up and inspection. A context manager; `close()` removes the destination."""

    def __init__(self, cm, dest, epoch, shared=False):
        self._cm = cm
        self.dest = dest
        self.epoch = epoch
        self.settings = dest.settings
        self.lane = dest.lane("stream", epoch)
        self.raw = dest.raw()
        self.checked = []                    # every Checked block of this destination, in order
        if not shared:                       # a shared one (a module fixture) is closed by its owner
            _open.append(self)

    # ------------------------------------------------------------------ the lane
    @contextlib.contextmanager
    def transaction(self, deadline=None, check=False):
        """One block of D's lane. Leaving it runs D's fence statement and COMMIT; an exception rolls back."""
        with self.lane.transaction(deadline) as tx:
            if check:
                c = Checked(tx)
                self.checked.append(c)
                yield c
            else:
                yield tx

    # ------------------------------------------------------------------ set-up and inspection, outside the layer
    def execute(self, sql, params=None):
        return self.raw.execute(sql, params) if params is not None else self.raw.execute(sql)

    def register(self, name, data):
        self.raw.register(name, data)

    def unregister(self, name):
        self.raw.unregister(name)

    # ------------------------------------------------------------------ life
    def close(self):
        if self in _open:
            _open.remove(self)
        cm, self._cm = self._cm, None
        if cm is None:
            return
        with contextlib.suppress(Exception):
            self.raw.close()
        cm.__exit__(None, None, None)

    def __enter__(self):
        return self

    def __exit__(self, et, ev, tb):
        self.close()
        return False


def _lane_db(cm, shared) -> LaneDB:
    dest = cm.__enter__()
    try:
        epoch = dest.lead(migrate=False)
        return LaneDB(cm, dest, epoch, shared)
    except BaseException:
        cm.__exit__(None, None, None)
        raise


def local_db(shared=False, **overrides) -> LaneDB:
    """A destination in the memory of this process (tier T0), with the leader tables and a lane `stream`.
    `overrides` are `md.Settings` fields (the default deadlines are the short ones of `td.FAST`). A `shared` one
    is not closed by `close_all`: its owner (a module fixture) closes it."""
    return _lane_db(td.local_destination(**overrides), shared)


def md_db(prefix="cdc_v2_t_a_", shared=False, **overrides) -> LaneDB:
    """A MotherDuck database `<prefix><random>` (created and dropped through the test ledger), with the leader
    tables and a lane `stream`."""
    return _lane_db(td.motherduck_destination(prefix, **overrides), shared)


def close_all():
    """Close every LaneDB that is still open (a test that failed before its own `with` ended)."""
    for db in list(_open):
        db.close()


class Checked:
    """D's lane transaction as apply sees it, with a record of what apply sends (`sent`, `names`) and the rules of
    C4 that apply keeps checked at every call: a deadline is None or a `Deadline`; a registered name is
    `cdc_v2_apply_<step>`. Everything else is D's `md.Transaction` itself."""

    def __init__(self, tx):
        self.tx = tx
        self.sent = []
        self.names = []

    @staticmethod
    def _deadline(deadline):
        assert deadline is None or isinstance(deadline, Deadline), "apply passed a deadline of type %s" % type(deadline)

    def execute(self, sql, params=None, deadline=None):
        self._deadline(deadline)
        self.sent.append(sql)
        return self.tx.execute(sql, params, deadline)

    def fetchall(self, sql, params=None, deadline=None):
        self._deadline(deadline)
        self.sent.append(sql)
        return self.tx.fetchall(sql, params, deadline)

    def fetch_arrow(self, sql, params=None, deadline=None):
        self._deadline(deadline)
        self.sent.append(sql)
        return self.tx.fetch_arrow(sql, params, deadline)

    def register(self, name, data, deadline=None):
        self._deadline(deadline)
        assert APPLY_NAME.fullmatch(name) and D_NAME.fullmatch(name), "apply registered the name %r" % name
        self.names.append(name)
        return self.tx.register(name, data, deadline)

    def __getattr__(self, name):
        return getattr(self.tx, name)


@contextlib.contextmanager
def counted_statements(seen=None):
    """While active, every statement whose count apply reads (`_Counting.count`) is checked: the kind that the
    client's parser gives it is INSERT, UPDATE or DELETE and it has no RETURNING (`md.connection.counts_rows`, the
    rule by which D's lane reports a count). `seen` (a list) receives (shape, kind) per statement."""
    from cdc_v2.apply import core
    orig = core._Counting.count

    def count(self, sql, params=None):
        kind = statement_kind(sql)
        assert kind in COUNTED_KINDS and counts_rows(sql), "apply read the count of a %s statement (%s)" % (
            kind, self.shape)
        if seen is not None:
            seen.append((self.shape, kind))
        return orig(self, sql, params)

    core._Counting.count = count
    try:
        yield seen
    finally:
        core._Counting.count = orig


def run_commit():
    """The commit of the tree that runs now, and whether the files the tests use differ from it."""
    try:
        head = subprocess.run(["git", "-C", str(ROOT), "rev-parse", "--short", "HEAD"], capture_output=True,
                              text=True, timeout=10).stdout.strip()
        dirty = subprocess.run(["git", "-C", str(ROOT), "status", "--porcelain", "--", "src", "tests", "tools",
                                "pyproject.toml", "uv.lock"], capture_output=True, text=True, timeout=10).stdout.strip()
    except Exception:                                        # noqa: BLE001 - recorded as unknown
        return None
    return head + ("+changes" if dirty else "")


def run_command(prefix="CDC_V2_MD_PREFIX=cdc_v2_t_a_ tools/with-env uv run pytest"):
    """The command that runs now, as a grader repeats it: the arguments pytest was given."""
    import shlex
    import sys
    return prefix + " " + " ".join(shlex.quote(a) for a in sys.argv[1:])
