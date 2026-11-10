"""Source datasets for oracle tests: every supported type family with its edge values, a keyless table with
identical rows, and a larger generated table that spans many buckets."""
from __future__ import annotations

TYPES_DDL = """
CREATE TABLE public.types_k (
    id integer PRIMARY KEY,
    c_smallint smallint, c_int integer, c_bigint bigint, c_bool boolean,
    c_num numeric(12,4), c_num_u numeric, c_real real, c_double double precision,
    c_text text, c_varchar varchar(20), c_char char(6), c_bytea bytea,
    c_date date, c_time time, c_ts timestamp, c_tstz timestamptz, c_interval interval,
    c_uuid uuid, c_json json, c_jsonb jsonb,
    c_int_arr integer[], c_text_arr text[], c_num_arr numeric[], c_double_arr double precision[],
    c_tstz_arr timestamptz[]
)"""

# One row per tuple; values are Postgres literals. Row 1 is all NULL. Comments name the edge being covered.
TYPES_ROWS = [
    "(1, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, "
    "NULL, NULL, NULL, NULL, NULL, NULL, NULL)",
    # ordinary values
    "(2, 1, 2, 3, true, 12345.6789, 1.50, 0.1, 0.1, 'hello', 'v', 'ab', '\\xab00', '2024-01-02', '01:02:03.000004', "
    "'2024-01-01 00:00:00.000001', '2024-01-01 00:00:00.000001+00', '1 year 2 mons 3 days 04:05:06.000007', "
    "'6f9619ff-8b86-d011-b42d-00c04fc964ff', '{\"b\": 1,  \"a\":2}', '{\"b\": 1, \"a\": [1, 2.50]}', "
    "'{1,NULL,3}', '{\"x\",\"\",NULL}', '{1.5,NaN}', '{-0,1e300}', '{\"2024-01-01 00:00:00+00\"}')",
    # boundaries, empty values, -0, NaN, infinities
    "(3, -32768, -2147483648, -9223372036854775808, false, -99999999.9999, 'NaN', '-0', '-0', '', '', '', '\\x', "
    "'infinity', '24:00:00', 'infinity', '-infinity', '1 mon', '00000000-0000-0000-0000-000000000000', '[]', '{}', "
    "'{}', '{}', '{}', '{}', '{}')",
    "(4, 32767, 2147483647, 9223372036854775807, true, 0, 'Infinity', 'NaN', 'NaN', 'héllo wörld ✓', "
    "'twenty characters!!!', 'abcdef', '\\x00ff10', '-infinity', '00:00:00', '-infinity', 'infinity', '30 days', "
    "'ffffffff-ffff-ffff-ffff-ffffffffffff', 'null', 'null', '{-1}', '{NULL}', '{NULL}', '{NaN,Infinity,-Infinity}', "
    "'{NULL}')",
    "(5, 0, 0, 0, false, 0.0001, '-Infinity', 'Infinity', 'Infinity', 'a''quote', 'x', 'x', '\\x0a', '1970-01-01', "
    "'23:59:59.999999', '1970-01-01 00:00:00', '1970-01-01 00:00:00+00', '-1 days -00:00:00.000001', "
    "'6F9619FF-8B86-D011-B42D-00C04FC964FF', '\"str\"', '\"str\"', '{0}', '{\"\"}', '{0.000}', '{0}', "
    "'{\"1970-01-01 00:00:00+00\"}')",
    # numbers that need care
    "(6, 7, 8, 9, true, 1.0000, 123456789012345678901234567890.123456789, 3.4028235e38, 1.7976931348623157e308, "
    "'tab\there', 'nl', 'nl', '\\xdeadbeef', '0001-01-01', '12:00:00', '0001-01-01 00:00:00', "
    "'2000-02-29 23:59:59.999999+05:30', '-2 years -3 mons +4 days -05:06:07', "
    "'12345678-1234-1234-1234-123456789abc', '{\"a\":1, \"a\":2}', '{\"a\":1, \"a\":2}', '{2147483647}', "
    "'{\"line\nbreak\"}', '{1e-20,100.00}', '{5e-324,-1.5}', '{infinity,-infinity}')",
    "(7, -1, -1, -1, false, -0.0001, 0.000000000000000000000001, 1e-45, 5e-324, 'end', 'end', 'end  ', '\\x01', "
    "'2262-04-11', '00:00:00.000001', '2262-04-11 23:47:16.854775', '1677-09-21 00:12:43.145225+00', "
    "'00:00:00.000001', 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', '1', '1.0', '{1,2,3,4,5}', '{a,b}', '{1.10}', "
    "'{0.1}', '{\"2262-04-11 23:47:16.854775+00\"}')",
]

DUPS_DDL = "CREATE TABLE public.dups (a integer, b text, c numeric)"
DUPS_ROWS = [
    "(1, 'x', 1.5)", "(1, 'x', 1.5)", "(1, 'x', 1.5)",          # three identical rows
    "(2, NULL, NULL)", "(2, NULL, NULL)",                       # identical rows with NULLs
    "(NULL, '', 0)",                                            # empty string, not NULL
    "(3, 'y', 'NaN')",
]

BIG_DDL = """
CREATE SCHEMA other;
CREATE TABLE other.big (
    l_orderkey bigint, l_linenumber integer, l_partkey bigint, l_suppkey bigint,
    l_quantity numeric(15,2), l_extendedprice numeric(15,2), l_discount numeric(15,2), l_tax numeric(15,2),
    l_returnflag char(1), l_linestatus char(1), l_shipdate date, l_commitdate date, l_receiptdate date,
    l_shipinstruct char(25), l_shipmode char(10), l_comment varchar(44),
    PRIMARY KEY (l_orderkey, l_linenumber)
)"""


def lineitem_insert_sql(table: str, start: int, count: int) -> str:
    """Postgres INSERT of `count` lineitem-shaped rows with keys start..start+count-1 (deterministic values)."""
    return """INSERT INTO {t}
SELECT i, (i % 7 + 1)::int, (i * 7) % 2000000, (i * 13) % 100000,
       ((i % 50) + 1)::numeric(15,2), ((i % 100000) * 1.37)::numeric(15,2),
       ((i % 11) / 100.0)::numeric(15,2), ((i % 9) / 100.0)::numeric(15,2),
       (ARRAY['A','N','R'])[(i % 3) + 1], (ARRAY['F','O'])[(i % 2) + 1],
       DATE '1992-01-01' + (i % 2500)::int, DATE '1992-01-31' + (i % 2500)::int, DATE '1992-01-10' + (i % 2500)::int,
       (ARRAY['DELIVER IN PERSON','COLLECT COD','NONE','TAKE BACK RETURN'])[(i % 4) + 1],
       (ARRAY['TRUCK','MAIL','REG AIR','AIR','FOB','RAIL','SHIP'])[(i % 7) + 1],
       'carefully final packages ' || substr(md5(i::text), 1, 16)
FROM generate_series({a}, {b}) AS g(i)""".format(t=table, a=start, b=start + count - 1)


def lineitem_md_ddl(table: str) -> str:
    return """CREATE TABLE {t} (
    l_orderkey BIGINT, l_linenumber INTEGER, l_partkey BIGINT, l_suppkey BIGINT,
    l_quantity DECIMAL(15,2), l_extendedprice DECIMAL(15,2), l_discount DECIMAL(15,2), l_tax DECIMAL(15,2),
    l_returnflag VARCHAR, l_linestatus VARCHAR, l_shipdate DATE, l_commitdate DATE, l_receiptdate DATE,
    l_shipinstruct VARCHAR, l_shipmode VARCHAR, l_comment VARCHAR)""".format(t=table)


def lineitem_md_insert_sql(table: str, start: int, count: int) -> str:
    """The same rows as `lineitem_insert_sql`, generated on the MotherDuck server (no upload)."""
    return """INSERT INTO {t}
SELECT i, (i % 7 + 1)::INTEGER, (i * 7) % 2000000, (i * 13) % 100000,
       ((i % 50) + 1)::DECIMAL(15,2), ((i % 100000) * 1.37)::DECIMAL(15,2),
       ((i % 11) / 100.0)::DECIMAL(15,2), ((i % 9) / 100.0)::DECIMAL(15,2),
       ['A','N','R'][(i % 3) + 1], ['F','O'][(i % 2) + 1],
       DATE '1992-01-01' + (i % 2500)::INTEGER, DATE '1992-01-31' + (i % 2500)::INTEGER,
       DATE '1992-01-10' + (i % 2500)::INTEGER,
       ['DELIVER IN PERSON','COLLECT COD','NONE','TAKE BACK RETURN'][(i % 4) + 1],
       ['TRUCK','MAIL','REG AIR','AIR','FOB','RAIL','SHIP'][(i % 7) + 1],
       'carefully final packages ' || substr(md5(i::VARCHAR), 1, 16)
FROM range({a}, {b}) AS g(i)""".format(t=table, a=start, b=start + count)


def create_types_dataset(db, big_rows: int = 3000) -> None:
    """Create the oracle test tables in a PrivateDatabase."""
    db.sql(TYPES_DDL)
    db.sql("INSERT INTO public.types_k VALUES " + ", ".join(TYPES_ROWS))
    db.sql(DUPS_DDL)
    db.sql("INSERT INTO public.dups VALUES " + ", ".join(DUPS_ROWS))
    for stmt in BIG_DDL.split(";"):
        if stmt.strip():
            db.sql(stmt)
    db.sql(lineitem_insert_sql("other.big", 1, big_rows))
    db.sql("ANALYZE")
