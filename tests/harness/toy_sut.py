"""Toy system under test for the crash-harness self-tests. Hermetic: a local source file, a local DuckDB file.

    python toy_sut.py --source SRC --dest DEST.duckdb --variant correct|wrong

The source is an append-only file of lines ``<id>\\t<value>\\n`` with ids 1, 2, 3, ... The toy copies new lines into
the table ``toy_records`` and keeps its position (the last id copied) in ``toy_position``. It runs until it is
killed, like the pipeline.

- ``correct``: position read, rows inserted and position written in one transaction.
- ``wrong``: rows are committed in one transaction and the position in a second one. A crash between the two makes
  the restarted toy copy the same rows again (duplicates). The crash harness must catch this.

The DuckDB file is opened per batch, so several instances can compete: a second instance waits for the file lock.
"""
import argparse
import sys
import time

import duckdb

from cdc_v2.crashpoints import crash_point, declare

POINTS = {
    "toy.schema_ready": ("start-up transaction that creates the destination tables committed", True),
    "toy.begun": ("BEGIN of a batch transaction", False),
    "toy.batch_read": ("position and batch read inside the batch transaction", False),
    "toy.data_written": ("batch rows inserted, not committed", False),
    "toy.data_committed": ("wrong variant: rows committed, position not yet written", False),
    "toy.position_written": ("position updated, not committed", False),
    "toy.committed": ("COMMIT of the position returned", False),
}
for _name, (_desc, _once) in POINTS.items():
    declare(_name, _desc, once=_once)

CORRECT_POINTS = ["toy.schema_ready", "toy.begun", "toy.batch_read", "toy.data_written", "toy.position_written",
                  "toy.committed"]
WRONG_POINTS = ["toy.schema_ready", "toy.begun", "toy.batch_read", "toy.data_written", "toy.data_committed",
                "toy.position_written", "toy.committed"]

LOCK_DEADLINE_S = 30.0
POLL_S = 0.01
BATCH = 7


def connect(path):
    """Open the destination, waiting for another instance's file lock (with a deadline)."""
    deadline = time.monotonic() + LOCK_DEADLINE_S
    while True:
        try:
            return duckdb.connect(path)
        except duckdb.IOException as exc:
            if "lock" not in str(exc).lower() or time.monotonic() >= deadline:
                raise
            time.sleep(0.005)


def read_source(path):
    with open(path, encoding="utf-8") as f:
        data = f.read()
    rows = []
    for line in data.split("\n")[:-1]:          # the last element is empty or a torn line
        i, _, v = line.partition("\t")
        rows.append((int(i), v))
    return rows


def ensure_schema(dest):
    con = connect(dest)
    try:
        con.execute("BEGIN")
        con.execute("CREATE TABLE IF NOT EXISTS toy_records (id BIGINT, val VARCHAR)")
        con.execute("CREATE TABLE IF NOT EXISTS toy_position (k INTEGER, pos BIGINT)")
        if con.execute("SELECT count(*) FROM toy_position").fetchone()[0] == 0:
            con.execute("INSERT INTO toy_position VALUES (1, 0)")
        con.execute("COMMIT")
    finally:
        con.close()
    crash_point("toy.schema_ready")


def batch(source, dest, variant, known_pos):
    """Copy one batch. Returns the position seen afterwards."""
    rows_all = read_source(source)
    if not rows_all or rows_all[-1][0] <= known_pos:
        return known_pos
    con = connect(dest)
    try:
        con.execute("BEGIN")
        crash_point("toy.begun")
        pos = con.execute("SELECT pos FROM toy_position WHERE k = 1").fetchone()[0]
        rows = [r for r in rows_all if r[0] > pos][:BATCH]
        if not rows:
            con.execute("ROLLBACK")
            return pos
        crash_point("toy.batch_read")
        con.executemany("INSERT INTO toy_records VALUES (?, ?)", rows)
        crash_point("toy.data_written")
        if variant == "wrong":
            con.execute("COMMIT")
            crash_point("toy.data_committed")
            con.execute("BEGIN")
        con.execute("UPDATE toy_position SET pos = ? WHERE k = 1", [rows[-1][0]])
        crash_point("toy.position_written")
        con.execute("COMMIT")
        crash_point("toy.committed")
        return rows[-1][0]
    finally:
        con.close()


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--source", required=True)
    ap.add_argument("--dest", required=True)
    ap.add_argument("--variant", choices=("correct", "wrong"), required=True)
    a = ap.parse_args(argv)
    ensure_schema(a.dest)
    print("toy %s started" % a.variant, flush=True)
    pos = -1
    while True:
        pos = batch(a.source, a.dest, a.variant, pos)
        time.sleep(POLL_S)


if __name__ == "__main__":
    sys.exit(main())
