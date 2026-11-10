"""Baseline type families through a real table, checked value by value (markers jvm, localpg).

For each column the expected transport value is computed by Postgres itself on the primary (exact integer
arithmetic for the temporal forms, the type's text output otherwise) and compared with what the engine delivered.
Floats are compared bit for bit with the binary value psycopg reads.
"""
import math
import struct

import pytest

from cdc_v2.stream.engine import Engine
from stream_helpers import Collector, evidence

pytestmark = [pytest.mark.jvm, pytest.mark.localpg, pytest.mark.timeout(600)]

# name, SQL type, values (SQL literals), expected-transport SQL expression over column c (None: compare natively)
FAMILIES = [
    ("bool", "boolean", ["true", "false"], "case when c then 'True' when not c then 'False' end"),
    ("i2", "smallint", ["-32768", "32767", "0"], "c::text"),
    ("i4", "integer", ["-2147483648", "2147483647", "0"], "c::text"),
    ("i8", "bigint", ["-9223372036854775808", "9223372036854775807", "0"], "c::text"),
    ("oid_", "oid", ["0", "4294967295"], "c::text"),
    ("f4", "real", ["'NaN'", "'Infinity'", "'-Infinity'", "'-0'", "'1.17549435e-38'", "'3.4028235e38'",
                    "'0.1'", "'1e-45'"], None),
    ("f8", "double precision", ["'NaN'", "'Infinity'", "'-Infinity'", "'-0'", "'5e-324'",
                                "'1.7976931348623157e308'", "'0.1'", "'2.2250738585072014e-308'"], None),
    ("num", "numeric", ["1.50", "'NaN'", "'Infinity'", "'-Infinity'", "0.000000000000000000000000000000000001",
                        "123456789012345678901234567890123456789012345.6789", "-0.0", "1e300"], "c::text"),
    ("num_c", "numeric(15,2)", ["1.5", "-9999999999999.99", "0"], "c::text"),
    ("txt", "text", ["''", "'plain'", "E'tab\\tquote\\'back\\\\slash'", "'é中\U0001F600'",
                     "repeat('x', 100000)"], "c::text"),
    ("vc", "varchar(10)", ["'abc'", "''"], "c::text"),
    ("ch", "char(5)", ["'ab'", "'abcde'"], "case when c is not null then format('%s', c) end"),
    ("qchar", "\"char\"", ["'a'", "'\\'"], "c::text"),
    ("name_", "name", ["'n'"], "c::text"),
    ("by", "bytea", ["''::bytea", "'\\x00'::bytea", "'\\xdeadbeef00ff'::bytea",
                     "decode(repeat('ab', 50000), 'hex')"], None),
    ("uu", "uuid", ["'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'"], "c::text"),
    ("js", "json", ["'{\"a\": 12345678901234567890123, \"b\": [1.0e400]}'", "'\"x\"'", "'null'"], "c::text"),
    ("jb", "jsonb", ["'{\"a\": 12345678901234567890123, \"b\": 1.50}'", "'[]'"], "c::text"),
    ("dt", "date", ["'infinity'", "'-infinity'", "'4713-11-24 BC'", "'5874897-12-31'", "'0001-01-01'",
                    "'0001-12-31 BC'", "'10000-01-01'", "'1999-12-31'", "'2000-01-01'"],
     "(case when c = 'infinity' then 2147483647 when c = '-infinity' then -2147483648 "
     "else c - date '2000-01-01' end)::text"),
    ("ts", "timestamp", ["'infinity'", "'-infinity'", "'4713-11-24 00:00:00 BC'",
                         "'294276-12-31 23:59:59.999999'", "'2000-01-01'", "'1970-01-01 00:00:00.000001'",
                         "'0044-03-15 12:00:00 BC'", "'12345-06-07 08:09:10.5'"],
     "(case when c = 'infinity' then 9223372036854775807 when c = '-infinity' then -9223372036854775808 "
     "else (extract(epoch from c - timestamp '2000-01-01') * 1000000)::numeric::bigint end)::text"),
    ("tz", "timestamptz", ["'infinity'", "'-infinity'", "'4713-11-24 00:00:00+00 BC'",
                           "'294276-12-31 23:59:59.999999+00'", "'2000-01-01 00:00:00+00'",
                           "'1999-12-31 23:59:59.999999-08'", "'2024-02-29 12:34:56.789+05:45'"],
     "(case when c = 'infinity' then 9223372036854775807 when c = '-infinity' then -9223372036854775808 "
     "else (extract(epoch from c - timestamptz '2000-01-01 00:00:00+00') * 1000000)::numeric::bigint end)::text"),
    ("tm", "time", ["'00:00:00'", "'24:00:00'", "'23:59:59.999999'", "'12:00:00.5'"],
     "(extract(epoch from c) * 1000000)::numeric::bigint::text"),
    ("tmz", "timetz", ["'24:00:00+14'", "'00:00:00-15:59'", "'12:34:56.789+05:30'"], "c::text"),
    ("iv", "interval", ["'1 year 2 mons 3 days 04:05:06.000078'", "'-1 days +02:00:00'", "'0'",
                        "'-178956970 years -8 mons'", "'1 mon -1 day 00:00:00.000001'"],
     "((extract(year from c) * 12 + extract(month from c))::bigint || ' months ' || extract(day from c)::bigint"
     " || ' days ' || (extract(hour from c) * 3600000000 + extract(minute from c) * 60000000"
     " + extract(microseconds from c))::bigint || ' microseconds')"),
    ("ai", "integer[]", ["'{1,2,NULL}'", "'[0:2]={1,2,3}'", "'{{1,2},{3,4}}'", "'{}'"], "c::text"),
    ("at", "text[]", ["'{\"a b\",\"c\\\"d\",NULL,\"NULL\"}'"], "c::text"),
    ("vb", "varbit", ["B'101'", "B'0101'", "B''"], "c::text"),
    ("bt", "bit(4)", ["B'1010'"], "c::text"),
    ("mn", "money", ["'12.34'", "'-0.01'", "'-92233720368547758.08'"], "c::numeric::text"),
    ("inet_", "inet", ["'10.0.0.1/8'", "'::1'"], "case when c is not null then format('%s', c) end"),
    ("rng", "int4range", ["'[1,5)'", "'empty'"], "c::text"),
]


def test_baseline_types_value_by_value(pair, scenario, artifacts):
    cols = ", ".join("%s %s" % (n, t) for n, t, _v, _e in FAMILIES)
    sc = scenario("create table {s}.ty (id serial primary key, %s)" % cols)
    rows_needed = max(len(v) for _n, _t, v, _e in FAMILIES)
    with pair.connect(pair.primary_port) as c:
        for i in range(rows_needed):
            names = [n for n, _t, v, _e in FAMILIES if i < len(v)]
            vals = [v[i] for n, _t, v, _e in FAMILIES if i < len(v)]
            c.execute("insert into %s.ty (%s) values (%s)" % (sc.schema, ", ".join(names), ", ".join(vals)))
        c.execute("insert into %s.ty default values" % sc.schema)        # every family NULL
    total = rows_needed + 1
    with Engine(sc.settings(), None, artifacts) as eng:
        col = Collector(eng).run(lambda c: len(c.rows_of("ty")) >= total, timeout=120)
        eng.stop(__import__("cdc_v2.deadline", fromlist=["Deadline"]).Deadline.after(20))
    schema = next(v for k, v in col.schemas.items() if k[1] == "ty")
    pos = {c.name: c.position for c in schema.columns}
    got = {r["a%d" % pos["id"]]: r for r in col.rows_of("ty")}
    checked, report = 0, {}
    with pair.connect(pair.primary_port) as c:
        for name, typ, _vals, expr in FAMILIES:
            colinfo = schema.columns[pos[name]]
            fam = report.setdefault(name, {"type": typ, "form": colinfo.form, "values": 0})
            if expr is None:
                rows = c.execute("select id, %s from %s.ty order by id" % (name, sc.schema)).fetchall()
            else:
                rows = c.execute("select id, %s from (select id, %s as c from %s.ty) q order by id"
                                 % (expr, name, sc.schema)).fetchall()
            for rid, want in rows:
                have = got[rid]["a%d" % pos[name]]
                if want is None:
                    assert have is None, (name, rid)
                elif typ in ("real", "double precision"):
                    fmt = "<f" if typ == "real" else "<d"
                    assert (math.isnan(want) and math.isnan(have)) or struct.pack(fmt, want) == struct.pack(fmt, have), \
                        (name, rid, want, have)
                elif typ == "bytea":
                    assert bytes(want) == have, (name, rid)
                else:
                    assert str(have) == want, (name, rid, want, have)
                fam["values"] += 1
                fam["non_null"] = fam.get("non_null", 0) + (want is not None)
                checked += 1
    assert checked >= 100
    evidence("types_baseline.json", {"test": "tests/engine/test_engine_types.py", "values_checked": checked,
                                     "families": report})
