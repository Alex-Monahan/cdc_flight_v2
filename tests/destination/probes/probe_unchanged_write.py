"""F0 (fix round, item 8): is a write of an unchanged value seen by a later writer of the same column?

An open transaction writes a row (`first`); another transaction then writes the same column of that row and
commits (`second`); then the first commits. Exactly one of the two may commit if they exclude each other. Each
case runs on a fresh table (written by `CREATE TABLE AS`, so its columns have had no update yet), on local DuckDB
and on MotherDuck, with plain client connections. The cases are the statements of the fence protocol:

- the fence statement (`n + 1`, the unchanged `epoch`) and the take-over of design 4.5 (`epoch` alone);
- the fence statement and the take-over as built since the fix round (`epoch` and `n + 1`, then `n - 1`), in both
  orders;
- the first form of the registration of item 2 (`epoch = epoch`) and the registration as built (`n + 1`, then
  `n - 1`), each against the take-over;
- a write of a changed value, as the control that the detection works.

Run:  tools/with-env uv run python tests/destination/probes/probe_unchanged_write.py [rounds]
Creates and drops one database named cdc_v2_t_d_<random>. Writes evidence/p3/destination/unchanged_write.json.
"""
import sys
import uuid

import duckdb

from common import MD_PREFIX, save, want_motherduck
from cdc_v2.testing import destination as td

FENCE = ["UPDATE {t} SET n = n + 1, epoch = 1 WHERE lane = 'stream' AND epoch = 1"]
TAKEOVER_AS_DESIGNED = ["UPDATE {t} SET epoch = 1 + 1"]
TAKEOVER_AS_BUILT = ["UPDATE {t} SET epoch = 1 + 1, n = n + 1", "UPDATE {t} SET n = n - 1"]
REGISTRATION_FIRST_FORM = ["UPDATE {t} SET epoch = epoch WHERE lane = 'stream' AND epoch = 1"]
REGISTRATION_AS_BUILT = ["UPDATE {t} SET n = n + 1 WHERE lane = 'stream' AND epoch = 1",
                         "UPDATE {t} SET n = n - 1 WHERE lane = 'stream' AND epoch = 1"]
CASES = {
    "fence, then take-over as designed (4.5)": (FENCE, TAKEOVER_AS_DESIGNED),
    "fence, then take-over as built": (FENCE, TAKEOVER_AS_BUILT),
    "take-over as built, then fence": (TAKEOVER_AS_BUILT, FENCE),
    "registration first form, then take-over as designed": (REGISTRATION_FIRST_FORM, TAKEOVER_AS_DESIGNED),
    "registration as built, then take-over as built": (REGISTRATION_AS_BUILT, TAKEOVER_AS_BUILT),
    "take-over as built, then registration as built": (TAKEOVER_AS_BUILT, REGISTRATION_AS_BUILT),
    "control: a changed epoch, then take-over as designed": (
        ["UPDATE {t} SET epoch = 7 WHERE lane = 'stream'"], TAKEOVER_AS_DESIGNED),
}
CREATE = ("CREATE TABLE {t} AS SELECT CAST(lane AS VARCHAR) AS lane, CAST(1 AS BIGINT) AS epoch, "
          "CAST(0 AS BIGINT) AS n FROM (VALUES ('stream'), ('control'), ('backfill.0')) AS lanes(lane)")


def one(connect, setup, t, first, second):
    setup.execute(CREATE.format(t=t))
    a, b = connect(), connect()
    try:
        a.execute("BEGIN")
        for q in first:
            a.execute(q.format(t=t))
        try:
            b.execute("BEGIN")
            for q in second:
                b.execute(q.format(t=t))
            b.execute("COMMIT")
            second_committed = True
        except duckdb.Error:
            b.execute("ROLLBACK")
            second_committed = False
        try:
            a.execute("COMMIT")
            first_committed = True
        except duckdb.Error:
            first_committed = False
    finally:
        a.close()
        b.close()
    setup.execute("DROP TABLE %s" % t)
    return first_committed and second_committed


def run(where, connect, rounds):
    setup = connect()
    out = {}
    try:
        for name, (first, second) in CASES.items():
            both = sum(1 for r in range(rounds)
                       if one(connect, setup, "f_%s" % uuid.uuid4().hex[:10], first, second))
            out[name] = {"rounds": rounds, "both_committed": both}
            print(where, name, both, "of", rounds, file=sys.stderr)
    finally:
        setup.close()
    return out


def main():
    rounds = int(sys.argv[1]) if len(sys.argv) > 1 else 6
    body = {"local": run("local", lambda name=":memory:cdc_v2_t_d_probe_%s" % uuid.uuid4().hex[:6]:
                         duckdb.connect(name), rounds)}
    if want_motherduck():
        with td.motherduck_destination(MD_PREFIX, leader_tables=False) as dest:
            cfg = {"custom_user_agent": dest.settings.marker}
            body["motherduck"] = run("motherduck", lambda: duckdb.connect(dest.settings.target, config=cfg), rounds)
    save("unchanged_write.json", "tools/with-env uv run python tests/destination/probes/probe_unchanged_write.py %d"
         % rounds, "F0: a write of an unchanged value into a column without updates is not seen by a later writer "
         "(both_committed counts the rounds in which both transactions committed; 0 means they excluded each "
         "other)", body)


if __name__ == "__main__":
    main()
