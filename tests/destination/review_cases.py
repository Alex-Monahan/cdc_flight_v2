"""Cases taken from the programs of the second review of package D (fix round, items 9 to 11), for the tests of
this package at T0 and on MotherDuck. The reviewer's programs stay where they are; these are copies of the parts
the tests need.

- `MISLEADING`: 21 statements whose error repeats a planted text without quotation marks, and the 8 planted
  texts (`e4_misleading.py`): 168 statements, of which 160 fail. None of them is a lost connection or a conflict.
- `CONFLICT_SHAPES`: DDL on one lane while another lane writes the table (`e1_conflicts.py`, `e1b_raw_texts.py`).
  The second writer gets ``Transaction conflict: ...``; running it again succeeds, except after a `DROP`.
- `MISSING_FILE`: a statement that reads a file that does not exist (`e2_ioerror.py`): an `IOException` every time.
"""
import os

HERE = os.path.dirname(os.path.abspath(__file__))
TEXTS = os.path.join(HERE, "data", "misleading_texts.json")

PLANTED = ["Connection Error", "Conflict on update", "lease expired", "UNAVAILABLE", "connection closed",
           "unavailable_dates", "deadline_exceeded_log", "stock_unavailable"]

MISLEADING = [
    # (label, setup statements, statement with {v} for the planted text and {n} for its number)
    ("json_extract of a text", [], "SELECT json_extract('{v}', '$.a')"),
    ("json cast", [], "SELECT '{v}'::JSON"),
    ("from_json", [], "SELECT from_json('{v}', '{{\"a\":\"INTEGER\"}}')"),
    ("regexp pattern from data", [], "SELECT regexp_matches('a', '({v}')"),
    ("strptime", [], "SELECT strptime('{v}', '%Y-%m-%d')"),
    ("printf", [], "SELECT printf('%d {v}', 'x')"),
    ("enum cast", ["CREATE TYPE IF NOT EXISTS rv_e AS ENUM ('a')"], "SELECT '{v}'::rv_e"),
    ("table name (schema of source)", [], "SELECT * FROM \"{v}\""),
    ("schema name", [], "SELECT * FROM \"{v}\".t"),
    ("column name", ["CREATE TABLE IF NOT EXISTS rv_t (a INTEGER)"], "SELECT \"{v}\" FROM rv_t"),
    ("not null column named by source", ["CREATE TABLE IF NOT EXISTS \"rv_nn_{n}\" (\"{v}\" INTEGER NOT NULL)"],
     "INSERT INTO \"rv_nn_{n}\" VALUES (NULL)"),
    ("unknown function from config", [], "SELECT \"{v}\"(1)"),
    ("union member", [], "SELECT union_extract(union_value(a := 1), '{v}')"),
    ("struct field", [], "SELECT struct_extract({{'a': 1}}, '{v}')"),
    ("map key", [], "SELECT map_extract(MAP {{'a': 1}}, 1)['{v}']"),
    ("set variable", [], "SELECT getvariable('{v}') + 'x'"),
    ("error() of a value", [], "SELECT error('{v}')"),
    ("interval", [], "SELECT '{v}'::INTERVAL"),
    ("timezone name", [], "SELECT timezone('{v}', TIMESTAMPTZ '2020-01-01 00:00:00+00')"),
    ("collation", [], "SELECT 'a' COLLATE \"{v}\""),
    ("attach-like pragma", [], "PRAGMA \"{v}\""),
]


def misleading_statements():
    """``(label, planted, setup statements, statement)`` for all 168."""
    out = []
    for label, setup, stmt in MISLEADING:
        for i, v in enumerate(PLANTED):
            n = "%d" % i
            out.append((label, v, [s.format(v=v, n=n) for s in setup], stmt.format(v=v, n=n)))
    return out


SETUP = (
    "CREATE TABLE rv.{t} (k INTEGER, v VARCHAR, w INTEGER)",
    "INSERT INTO rv.{t} VALUES (1, 'a', 1), (2, 'b', 2), (3, 'c', 3)",
    "CREATE TABLE rv.{t}_stage AS SELECT * FROM rv.{t}",
)
INS = "INSERT INTO rv.{t} (k, v, w) VALUES (9, 'z', 9)"
UPD = "UPDATE rv.{t} SET v = v || 'x' WHERE k = 1"

#: (name, what lane A runs and commits, what lane B runs after it read the table, B again succeeds?)
CONFLICT_SHAPES = [
    ("add_col__ins", ["ALTER TABLE rv.{t} ADD COLUMN x INTEGER"], [INS], True),
    ("add_col__upd", ["ALTER TABLE rv.{t} ADD COLUMN x INTEGER"], [UPD], True),
    ("type__upd", ["ALTER TABLE rv.{t} ALTER w TYPE BIGINT"], [UPD], True),
    ("drop__ins", ["DROP TABLE rv.{t}"], [INS], False),
    ("swap__ins", ["DROP TABLE rv.{t}", "ALTER TABLE rv.{t}_stage RENAME TO {t}"], [INS], True),
]

MISSING_FILE = "INSERT INTO rv.io SELECT * FROM read_parquet('/nonexistent_cdc_v2_t_d/part-0.parquet')"
MISSING_FILE_READ = "SELECT * FROM read_parquet('/nonexistent_cdc_v2_t_d/part-0.parquet')"


def fmt(sqls, t):
    return [s.format(t=t) for s in sqls]
