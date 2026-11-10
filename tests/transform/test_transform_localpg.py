"""Postgres prints (marker localpg, no JVM): seeded random values per family are written to the local Postgres and
read back as text under package P's session settings; `transform.backfill` must give the generating value exactly.
Negative controls: deliberately wrong conversions are reported by the oracle.

    uv run pytest -m localpg tests/transform/test_transform_localpg.py
"""
from __future__ import annotations

import decimal
import os
import random
import struct

import duckdb
import pyarrow as pa
import pytest

from cdc_v2 import lsn
from cdc_v2 import transform as T
from cdc_v2.deadline import Deadline
from cdc_v2.oracle import Destination, OracleConfig, compare
from cdc_v2.pg import catalog
import pgref as R
from pgpair import PG16_BIN, Pair, Scenario, lp
from test_transform_crosspath import read_backfill, write

pytestmark = [pytest.mark.localpg, pytest.mark.timeout(900)]
PG16 = os.environ.get("CDC_V2_Y_PG") == "16"
N = int(os.environ.get("CDC_V2_Y_PG_VALUES", "3000"))
SEED = int(os.environ.get("CDC_V2_Y_SEED", "20261110"))
D = decimal.Decimal


@pytest.fixture(scope="module")
def pair():
    pg_bin = PG16_BIN if PG16 else None
    lp("up", pg_bin=pg_bin)
    p = Pair(pg_bin)
    yield p
    if os.environ.get("CDC_V2_Y_KEEP_PG") != "1":
        lp("down", check=False, pg_bin=pg_bin)


def gen(kind, rnd):
    """(text written to Postgres, expected destination value as a comparable Python value)."""
    if kind == "float8":
        v = struct.unpack("<d", struct.pack("<Q", rnd.getrandbits(64)))[0]
        return R.float8_text(v), ("NaN" if v != v else struct.pack("<d", v).hex())
    if kind == "float4":
        v = R.float4_value(rnd.getrandbits(32))
        return R.float4_text(v), ("NaN" if v != v else struct.pack("<f", v).hex())
    if kind == "numeric":
        t = ("-" if rnd.random() < .5 else "") + str(rnd.randint(0, 10 ** rnd.randint(0, 27))) + "." + "".join(
            rnd.choice("0123456789") for _ in range(rnd.randint(1, 10)))
        return t, D(t)
    if kind == "date":
        d = rnd.randint(R.MIN_DATE_DAYS, R.MAX_DATE_DAYS)
        return R.date_text(d), d
    if kind in ("timestamp", "timestamptz"):
        us = rnd.randint(R.MIN_DATE_DAYS * R.DAY_US, R.DUCK_TS_MAX)
        return R.ts_text(us, kind == "timestamptz"), us
    if kind == "time":
        us = rnd.randint(0, R.DAY_US)
        return R.time_text(us), us
    if kind == "timetz":
        us, off = rnd.randint(0, R.DAY_US), rnd.randint(-57599, 57599)
        return R.timetz_text(us, off), (us, off)
    if kind == "interval":
        m, d, u = rnd.randint(-2 ** 31, 2 ** 31 - 1), rnd.randint(-2 ** 31, 2 ** 31 - 1), rnd.randint(
            -9223372036854775, 9223372036854775)
        return R.interval_transport(m, d, u).replace("months", "mons"), (m, d, u * 1000)
    if kind == "bytea":
        b = bytes(rnd.getrandbits(8) for _ in range(rnd.randint(0, 30)))
        return R.bytea_text(b), b
    raise AssertionError(kind)


def value_of(kind, arr, i):
    v = arr[i]
    if not v.is_valid:
        return None
    if kind in ("float8", "float4"):
        x = v.as_py()
        return "NaN" if x != x else struct.pack("<d" if kind == "float8" else "<f", x).hex()
    if kind in ("date", "timestamp", "timestamptz", "time"):
        width = pa.int32() if kind == "date" else pa.int64()
        return arr.slice(i, 1).view(width)[0].as_py()
    if kind == "timetz":
        bits = struct.unpack("<Q", arr.storage[i].as_py())[0]
        return (bits >> 24, 57599 - (bits & 0xFFFFFF))
    if kind == "interval":
        x = v.as_py()
        return (x.months, x.days, x.nanoseconds)
    return v.as_py()


KINDS = {"float8": "double precision", "float4": "real", "numeric": "numeric", "date": "date",
         "timestamp": "timestamp", "timestamptz": "timestamptz", "time": "time", "timetz": "timetz",
         "interval": "interval", "bytea": "bytea"}


def test_postgres_prints_and_the_backfill_reads_it_back(pair):
    rnd = random.Random(SEED)
    sc = Scenario(pair, ["create table {s}.p_%s (id int primary key, v %s)" % (k, t) for k, t in KINDS.items()])
    report = {}
    try:
        version = pair.server_version_num()
        conn = pair.read_conn()
        try:
            cat = {t.name: t for t in catalog.tables(conn, [(sc.schema, "p_" + k) for k in KINDS],
                                                     publication="none", deadline=Deadline.after(30))}
        finally:
            conn.close(Deadline.after(5))
        for kind in KINDS:
            vals = [gen(kind, rnd) for _ in range(N)]
            with pair.connect(pair.primary_port) as c:
                with c.cursor().copy("COPY %s.p_%s (id, v) FROM STDIN" % (sc.schema, kind)) as cp:
                    for i, (text, _w) in enumerate(vals):
                        cp.write_row((i, text))
                    cp.write_row((N, None))
            pair.wait_replayed()
            batch = read_backfill(pair, sc.schema, "p_" + kind, 2)
            plan = T.plan(cat["p_" + kind], server_version_num=version)
            out = T.backfill(plan, batch)
            got = [value_of(kind, out.column(1), i) for i in range(N + 1)]
            assert got == [w for _t, w in vals] + [None], kind
            report[kind] = {"values": N, "printed_by_postgres_differs_from_ours":
                            sum(1 for (t, _w), p in zip(vals, batch.column(1).to_pylist()) if t != p)}
    finally:
        sc.close()
    write("postgres_prints_pg%d.json" % (version // 10000),
          {"test": "tests/transform/test_transform_localpg.py::test_postgres_prints_and_the_backfill_reads_it_back",
           "seed": SEED, "server_version_num": version, "families": report})


# ------------------------------------------------------------------------------------------------ negative controls


def _oracle(pair, con, schema, table, slot="cdc_v2_y_nc"):
    q = lsn.parse(pair.p("select pg_current_wal_insert_lsn()::text")[0][0])
    con.execute("CREATE SCHEMA IF NOT EXISTS _cdc")
    con.execute("CREATE OR REPLACE TABLE _cdc.stream_state (slot_name VARCHAR, applied_lsn UBIGINT)")
    con.execute("INSERT INTO _cdc.stream_state VALUES (?, ?)", [slot, q])
    return compare(pair.oracle_source(), Destination(conn=con), OracleConfig(tables=["%s.%s" % (schema, table)]),
                   position=q, slot_name=slot, deadline=Deadline.after(120))


def test_negative_controls_are_caught_by_the_oracle(pair):
    sc = Scenario(pair, ["create table {s}.nc (id int primary key, f double precision, d date)"])
    try:
        with pair.connect(pair.primary_port) as c:
            c.execute("insert into %s.nc values (1, 0.30000000000000004, '0044-03-15 BC'), (2, '-0', '2020-01-01'), "
                      "(3, 'NaN', '0001-12-31 BC')" % sc.schema)
        pair.wait_replayed()
        conn = pair.read_conn()
        try:
            st = catalog.tables(conn, [(sc.schema, "nc")], publication="none", deadline=Deadline.after(30))[0]
        finally:
            conn.close(Deadline.after(5))
        plan = T.plan(st, server_version_num=pair.server_version_num())
        batch = read_backfill(pair, sc.schema, "nc", 3)
        texts = [batch.column(i).to_pylist() for i in range(3)]

        def check(f_texts, d_arr):
            con = duckdb.connect()
            con.execute('CREATE SCHEMA "%s"' % sc.schema)
            con.execute('CREATE TABLE "%s".nc (%s)' % (sc.schema, plan.column_ddl()))
            b = T.backfill(plan, pa.record_batch({"a0": pa.array(texts[0]), "a1": pa.array(f_texts),
                                                  "a2": pa.array(texts[2])}))
            cols = [b.column(0), b.column(1), d_arr if d_arr is not None else b.column(2)]
            con.register("src", pa.table({"a%d" % i: c for i, c in enumerate(cols)}))
            con.execute('INSERT INTO "%s".nc SELECT * FROM src' % sc.schema)
            return _oracle(pair, con, sc.schema, "nc").equal

        assert check(texts[1], None)                                           # the right conversion is equal
        too_few = ["%.15g" % float(t) if t not in ("NaN", "-0") else t for t in texts[1]]
        assert not check(too_few, None)                                         # floats with 15 digits
        assert not check(["0" if t == "-0" else t for t in texts[1]], None)     # -0 normalised
        plain = duckdb.connect().execute("SELECT CAST(x AS DATE) FROM (SELECT unnest(?) AS x)",
                                         [texts[2]]).to_arrow_table().column(0).combine_chunks()
        assert not check(texts[1], plain)                                       # BC date through the plain cast
    finally:
        sc.close()
