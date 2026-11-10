"""P1 check: the build can reach MotherDuck, and create, write, read and drop a database named cdc_v2_*.

Run:  tools/with-env uv run python tools/checks/motherduck_connectivity.py
Prints a short report without any token, user name or host name.
"""
import os
import sys
import time

import duckdb

DB = "cdc_v2_p1_probe"


def main() -> int:
    if not os.environ.get("motherduck_token"):
        print("FAIL: motherduck_token is not set (run through tools/with-env)")
        return 1
    started = time.time()
    con = duckdb.connect("md:")
    print("duckdb client version:", duckdb.__version__)
    print("connected in %.1f s" % (time.time() - started))
    con.execute(f"DROP DATABASE IF EXISTS {DB}")
    con.execute(f"CREATE DATABASE {DB}")
    con.execute(f"CREATE TABLE {DB}.main.t AS SELECT range AS id, 'row ' || range AS note FROM range(1000)")
    other = duckdb.connect("md:")  # a second connection sees the committed rows
    count, checksum = other.execute(f"SELECT count(*), sum(hash(id, note))::VARCHAR FROM {DB}.main.t").fetchone()
    print("rows visible on a second connection:", count)
    other.close()
    con.execute(f"DROP DATABASE {DB}")
    left = con.execute("SELECT count(*) FROM duckdb_databases() WHERE database_name = ?", [DB]).fetchone()[0]
    print("probe database dropped:", left == 0)
    ok = count == 1000 and left == 0
    print("RESULT:", "PASS" if ok else "FAIL")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
