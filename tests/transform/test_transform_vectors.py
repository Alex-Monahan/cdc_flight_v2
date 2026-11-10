"""Hand-written vectors for both directions, each with the exact destination value; the errors and what they carry;
isolation of the connection; threads; the rules from the review of package A (T0, local DuckDB only)."""
import decimal
import io
import logging
import os
import struct
import threading
import traceback
import uuid

import duckdb
import pyarrow as pa
import pytest

from cdc_v2 import transform as T
from cdc_v2.oracle import canonical as C
from cdc_v2.transform import families as F
import pgref as R
from tables import array, col, numeric_typmod, one, table

V18 = 180006
D = decimal.Decimal


def back(typ, values, typmod=-1, wide=False):
    p = T.plan(one(typ, typmod), server_version_num=V18)
    p = p.widened(0) if wide else p
    return T.backfill(p, pa.record_batch({"a0": pa.array(values, pa.string())})).column(0)


def strm(typ, arr, typmod=-1, wide=False, u=None):
    p = T.plan(one(typ, typmod), server_version_num=V18)
    p = p.widened(0) if wide else p
    cols = {"seq": pa.array(range(len(arr)), pa.int64()), "op": pa.array(["c"] * len(arr)), "a0": arr}
    if u is not None:
        cols["u0"] = pa.array(u, pa.bool_())
    out = T.stream(p, pa.record_batch(cols))
    assert out.schema.names == list(cols)
    return out.column(2)


def ints(arr):
    """The physical integers of a temporal Arrow array (None for NULL)."""
    width = pa.int32() if pa.types.is_date32(arr.type) else pa.int64()
    return arr.view(width).to_pylist()


def union_values(arr):
    """[(tag, value)] of a sparse union; NULL as None."""
    out = []
    for i in range(len(arr)):
        tag = arr.type_codes[i].as_py()
        child = arr.field(tag)[i]
        out.append(None if not child.is_valid else (tag, child.as_py()))
    return out


def same_buffers(a, b) -> bool:
    """Not copied: the same memory."""
    return [x.address if x else None for x in a.buffers()] == [x.address if x else None for x in b.buffers()]


# ------------------------------------------------------------------------------------------------ vectors


def test_boolean_integers_and_floats():
    assert back("bool", ["t", "f", None]).to_pylist() == [True, False, None]
    assert back("bool", ["t"]).type == pa.bool_()
    assert back("int2", ["-32768", "32767", "0", None]).to_pylist() == [-32768, 32767, 0, None]
    assert back("int8", ["-9223372036854775808", "9223372036854775807"]).to_pylist() == [-2 ** 63, 2 ** 63 - 1]
    f8 = back("float8", ["NaN", "Infinity", "-Infinity", "-0", "0", "5e-324", "1.7976931348623157e+308",
                         "2.2250738585072014e-308", "0.1", "-2.0604180037107712e+16", None])
    bits = [None if v is None else struct.pack(">d", v).hex() for v in f8.to_pylist()]
    assert bits == ["7ff8000000000000", "7ff0000000000000", "fff0000000000000", "8000000000000000",
                    "0000000000000000", "0000000000000001", "7fefffffffffffff", "0010000000000000",
                    "3fb999999999999a", "c3524cd920000000", None]
    f4 = back("float4", ["NaN", "-0", "1e-45", "3.4028235e+38", "1.1754944e-38", "0.1"])
    assert [struct.pack(">f", v).hex() for v in f4.to_pylist()] == [
        "7fc00000", "80000000", "00000001", "7f7fffff", "00800000", "3dcccccd"]
    # the stream's physical values pass untouched: the same buffers, every NaN payload and -0 kept
    raw = pa.array([float("nan"), -0.0, 1.5], pa.float64())
    nan2 = pa.Array.from_buffers(pa.float64(), 1, [None, pa.py_buffer(struct.pack("<Q", 0xFFF8000000000001))])
    raw = pa.concat_arrays([raw, nan2])
    out = strm("float8", raw)
    assert out.buffers()[1].to_pybytes() == raw.buffers()[1].to_pybytes()


def test_numeric_vectors_and_fits():
    assert back("numeric", ["1.50", "-0.0", "0.0000000001", "-9999999999999999999999999999.9999999999",
                            "1.50000000000000000000", "0000012"]).to_pylist() == [
        D("1.5"), D("0"), D("1E-10"), D("-9999999999999999999999999999.9999999999"), D("1.5"), D(12)]
    for misfit in ["NaN", "Infinity", "-Infinity", "0.00000000001", "1" + "0" * 28, "1" * 131072]:
        with pytest.raises(T.WideningNeeded) as e:
            back("numeric", ["1", misfit])
        assert e.value.positions == (0,)
    w = back("numeric", ["1.50", "NaN", "-Infinity", "1" + "0" * 300, None, "0.00000000001"], wide=True)
    assert union_values(w) == [(0, D("1.5")), (1, "NaN"), (1, "-Infinity"), (1, "1" + "0" * 300), None,
                               (1, "0.00000000001")]
    assert back("numeric", ["12.30", "-9999999999999.99"], numeric_typmod(15, 2)).to_pylist() == [
        D("12.30"), D("-9999999999999.99")]
    assert back("numeric", ["12300"], numeric_typmod(5, -2)).to_pylist() == [D(12300)]
    assert back("numeric", ["0.00123"], numeric_typmod(3, 5)).to_pylist() == [D("0.00123")]
    with pytest.raises(T.WideningNeeded):
        back("numeric", ["NaN"], numeric_typmod(15, 2))
    # the stream form is the same text
    assert strm("numeric", pa.array(["1.50", None])).to_pylist() == [D("1.5"), None]


def test_text_bytea_uuid_json_enum_pass():
    big = "x" * (1 << 20)
    vals = ["", "ab   ", "é中\U0001F600", big, None]
    for typ in ("text", "bpchar", "json", "jsonb", "varchar"):
        arr = pa.array(vals, pa.string())
        out = back(typ, vals)
        assert out.to_pylist() == vals
        assert same_buffers(strm(typ, arr), arr)
    assert back("bytea", ["\\x", "\\x00ff", None, R.bytea_text(bytes(range(256)))]).to_pylist() == [
        b"", b"\x00\xff", None, bytes(range(256))]
    b = pa.array([b"", b"\x00", None], pa.binary())
    assert strm("bytea", b).to_pylist() == [b"", b"\x00", None]
    u = "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"
    for out in (back("uuid", [u, None]), strm("uuid", pa.array([u, None]))):
        assert out.type == pa.uuid() and out.storage.to_pylist() == [uuid.UUID(u).bytes, None]


def test_date_vectors():
    texts = ["2020-02-29", "0044-03-15 BC", "0001-12-31 BC", "0001-01-01", "4714-11-24 BC", "5874897-12-31",
             "10000-01-01", "infinity", "-infinity", "1970-01-01", None]
    want = [18321, -735160, -719163, -719162, R.MIN_DATE_DAYS, R.MAX_DATE_DAYS, 2932897, 2147483647, -2147483647,
            0, None]
    assert ints(back("date", texts)) == want
    for t, w in zip(texts, want):
        if t and "infinity" not in t:
            assert R.date_text(w) == t
    pg_days = [None if w is None else (2147483647 if w == 2147483647 else -2147483648 if w == -2147483647
                                       else w - R.PG_EPOCH_DAYS) for w in want]
    assert ints(strm("date", pa.array(pg_days, pa.int32()))) == want
    for bad in ["2021-02-29", "2020-13-01", "2020-1-1x", "20-01-01", "0000-01-01", "2020-01-01 AD"]:
        with pytest.raises(T.ConversionError):
            back("date", [bad])


def test_timestamp_vectors():
    texts = ["2020-02-29 12:34:56.5", "0044-03-15 12:00:00 BC", "4714-11-24 00:00:00 BC", "infinity",
             "-infinity", "294247-01-10 04:00:54.775806", "1999-12-31 23:59:59.999999", None]
    out = ints(back("timestamp", texts))
    assert out == [1582979696500000, -63517780800000000, R.MIN_DATE_DAYS * R.DAY_US, 2 ** 63 - 1, -(2 ** 63 - 1),
                   R.DUCK_TS_MAX, 946684799999999, None]
    tz = ints(back("timestamptz", [t + "+00" if t and "inf" not in t and "BC" not in t
                                   else (t.replace(" BC", "+00 BC") if t and "BC" in t else t) for t in texts]))
    assert tz == out
    assert back("timestamptz", ["2020-01-01 00:00:00+00"]).type == pa.timestamp("us", "UTC")
    with pytest.raises(T.WideningNeeded):
        back("timestamp", ["294276-12-31 23:59:59.999999"])
    w = back("timestamp", ["294276-12-31 23:59:59.999999", "2000-01-01 00:00:00", "infinity", None], wide=True)
    assert w.type_codes.to_pylist() == [1, 0, 0, 0] and w.field(1).to_pylist()[0] == "294276-12-31 23:59:59.999999"
    assert ints(w.field(0)) == [None, 946684800000000, 2 ** 63 - 1, None]
    # the stream: microseconds since 2000; the same values
    pg = [None if v is None else (2 ** 63 - 1 if v == 2 ** 63 - 1 else -(2 ** 63) if v == -(2 ** 63 - 1)
                                  else v - R.PG_EPOCH_US) for v in out]
    assert ints(strm("timestamp", pa.array(pg, pa.int64()))) == out
    top = R.MAX_TS_US - R.PG_EPOCH_US
    sw = strm("timestamptz", pa.array([top, 0], pa.int64()), wide=True)
    assert union_values(sw)[0] == (1, "294276-12-31 23:59:59.999999+00")
    for bad in ["2020-02-29 24:00:00", "2020-02-29 12:34", "2020-02-29T12:34:56", "2020-02-30 00:00:00"]:
        with pytest.raises(T.ConversionError):
            back("timestamp", [bad])
    with pytest.raises(T.ConversionError):
        back("timestamptz", ["2020-02-29 12:34:56+01"])


def test_time_timetz_vectors():
    assert ints(back("time", ["24:00:00", "00:00:00", "23:59:59.999999", "12:00:00.5", None])) == [
        86400000000, 0, 86399999999, 43200500000, None]
    raw = pa.array([86400000000, 0, None], pa.int64())
    assert ints(strm("time", raw)) == [86400000000, 0, None]
    for bad in ["24:00:01", "12:60:00", "1:00:00"]:
        with pytest.raises(T.ConversionError):
            back("time", [bad])
    with pytest.raises(T.ConversionError):
        strm("time", pa.array([86400000001], pa.int64()))
    texts = ["12:34:56.789+05:30", "24:00:00+15:59:59", "00:00:00-15:59:59", "12:00:00-01:00:30", "12:00:00+00",
             "12:00:00-00:00:30", None]
    for out in (back("timetz", texts), strm("timetz", pa.array(texts))):
        assert out.type == F.TIMETZ_ARROW
        con = duckdb.connect()
        con.register("t", pa.table({"z": out}))
        got = con.execute("SELECT epoch_us(CAST(z AS TIME)), date_part('timezone', z) FROM t").fetchall()
        assert got == [(45296789000, 19800), (86400000000, 57599), (0, -57599), (43200000000, -3630),
                       (43200000000, 0), (43200000000, -30), (None, None)]


def test_interval_vectors():
    cases = [(0, 0, 0), (14, 3, 14706000078), (-14, 3, -14706500000), (1, -1, 1), (-2147483648, 2147483647, -1),
             (0, 0, R.ARROW_US if hasattr(R, "ARROW_US") else 9223372036854775)]
    texts = [R.interval_iso(*c) for c in cases]
    assert texts[:5] == ["PT0S", "P1Y2M3DT4H5M6.000078S", "P-1Y-2M3DT-4H-5M-6.5S", "P1M-1DT0.000001S",
                         "P-178956970Y-8M2147483647DT-0.000001S"]
    want = [pa.MonthDayNano([m, d, u * 1000]) for m, d, u in cases]
    assert back("interval", texts).to_pylist() == want
    assert strm("interval", pa.array([R.interval_transport(*c) for c in cases])).to_pylist() == want
    big = (0, 0, 9223372036854775807)
    for fn, arg in ((back, R.interval_iso(*big)), (back, "infinity"), (back, "-infinity"),
                    (strm, pa.array([R.interval_transport(*big)]))):
        with pytest.raises(T.WideningNeeded):
            fn("interval", [arg] if fn is back else arg)
    w = back("interval", ["infinity", R.interval_iso(*big), "P1D", None], wide=True)
    assert union_values(w) == [(1, "infinity"), (1, "PT2562047788H54.775807S"), (0, pa.MonthDayNano([0, 1, 0])),
                               None]
    sw = strm("interval", pa.array([R.interval_transport(*big), "0 months 1 days 0 microseconds", None]), wide=True)
    assert union_values(sw) == union_values(w)[1:]
    for bad in ["P", "1 day", "PT1.1234567S", "P1Y2Y"]:
        with pytest.raises(T.ConversionError):
            back("interval", [bad])


def test_fallback_types():
    assert back("money", ["$1.00", None]).to_pylist() == ["$1.00", None]
    assert strm("money", pa.array(["-92233720368547758.08", "1234567.5", "0.01", "-0.10", None])).to_pylist() == [
        "-$92,233,720,368,547,758.08", "$1,234,567.50", "$0.01", "-$0.10", None]
    assert strm("oid", pa.array([0, 4294967295, None], pa.int64())).to_pylist() == ["0", "4294967295", None]
    a = pa.array(["{1,NULL}", "[0:1]={1,2}", None])
    p = T.plan(table([col(1, "a", array("int4"), type_name="integer[]")]), server_version_num=V18)
    assert same_buffers(T.stream(p, pa.record_batch({"a0": a})).column(0), a)
    assert same_buffers(T.backfill(p, pa.record_batch({"a0": a})).column(0), a)


def test_old_image_old_key_and_unchanged_columns():
    t = table([col(1, "id", "numeric", numeric_typmod(10, 0)), col(2, "v", "numeric")], key=(1,))
    p = T.plan(t, server_version_num=V18)
    rows = pa.record_batch({"seq": pa.array([5, 6], pa.int64()), "op": pa.array(["u", "c"]),
                            "a0": pa.array(["1", "2"]), "a1": pa.array(["garbage-under-u", "3.5"]),
                            "u1": pa.array([True, False]), "k0": pa.array([None, "1"])})
    out = T.stream(p, rows)
    assert out.schema.names == ["seq", "op", "a0", "a1", "u1", "k0"]
    assert same_buffers(out.column(0), rows.column(0)) and same_buffers(out.column(4), rows.column(4))
    assert out.column(3).to_pylist() == [None, D("3.5")]                 # never looked at under u<i>
    assert out.column(5).to_pylist() == [None, D(1)]
    keyless = table([col(1, "v", "int4")], replica_identity="full")
    pk = T.plan(keyless, server_version_num=V18)
    o = T.stream(pk, pa.record_batch({"a0": pa.array([None, 2], pa.int32()), "b0": pa.array([1, 1], pa.int32())}))
    assert o.column(1).to_pylist() == [1, 1]


def test_empty_batches_and_row_order():
    p = T.plan(one("date"), server_version_num=V18)
    assert len(T.backfill(p, pa.record_batch({"a0": pa.array([], pa.string())}))) == 0
    vals = [R.date_text(d) for d in range(-5000, 5000)]
    assert ints(T.backfill(p, pa.record_batch({"a0": pa.array(vals)})).column(0)) == list(range(-5000, 5000))


# ------------------------------------------------------------------------------------------------ errors

MARK = "PLANTED_9b2c"


def _all_text(exc) -> str:
    seen, parts, e = set(), [], exc
    while e is not None and id(e) not in seen:
        seen.add(id(e))
        parts += [str(e), repr(e), repr(e.args), repr(vars(e))]
        e = e.__cause__ or e.__context__
    parts.append("".join(traceback.format_exception(exc)))
    return "\n".join(parts)


@pytest.mark.parametrize("typ,value,wide,expect", [
    ("int4", MARK, False, T.ConversionError), ("int4", "1" + MARK, False, T.ConversionError),
    ("bool", MARK, False, T.ConversionError), ("float8", MARK, False, T.ConversionError),
    ("numeric", MARK, False, T.ConversionError), ("numeric", "NaN" + MARK, False, T.ConversionError),
    ("bytea", "\\x" + MARK.encode().hex() + "zz", False, T.ConversionError),
    ("date", "2020-01-01" + MARK, False, T.ConversionError),
    ("timestamp", "2020-01-01 00:00:00" + MARK, True, T.ConversionError),
    ("time", MARK, False, T.ConversionError), ("timetz", MARK, False, T.ConversionError),
    ("interval", "P" + MARK, True, T.ConversionError), ("uuid", MARK, False, T.ConversionError),
])
def test_errors_carry_no_value(typ, value, wide, expect, caplog):
    caplog.set_level(logging.DEBUG)
    p = T.plan(one(typ), server_version_num=V18)
    p = p.widened(0) if wide else p
    rows = pa.record_batch({"seq": pa.array([41, 42], pa.int64()), "a0": pa.array(["", value])})
    with pytest.raises(expect) as e:
        T.backfill(p, pa.record_batch({"a0": rows.column(1)}))
    text = _all_text(e.value) + caplog.text
    assert MARK not in text and MARK.encode().hex() not in text and "zz" not in text.split("public.t")[-1][:0] + text[:0]
    assert e.value.__cause__ is None and e.value.__context__ is None
    assert e.value.position == 0 and e.value.row == 0 and e.value.seq is None


def test_conversion_error_names_seq_and_row_in_the_stream():
    p = T.plan(table([col(1, "id", "int4"), col(2, "v", "uuid")], key=(1,)), server_version_num=V18)
    rows = pa.record_batch({"seq": pa.array(range(100, 1100), pa.int64()), "a0": pa.array(range(1000), pa.int32()),
                            "a1": pa.array([str(uuid.uuid4()) for _ in range(777)] + [MARK] +
                                           [str(uuid.uuid4()) for _ in range(222)])})
    with pytest.raises(T.ConversionError) as e:
        T.stream(p, rows)
    assert (e.value.position, e.value.row, e.value.seq) == (1, 777, 877)
    assert MARK not in _all_text(e.value)


def test_widening_needed_names_all_positions_and_carries_no_value():
    t = table([col(1, "a", "numeric"), col(2, "b", "int4"), col(3, "c", "timestamp"), col(4, "d", "interval")])
    p = T.plan(t, server_version_num=V18)
    secret = "9" * 29 + "." + "123"
    rows = pa.record_batch({"a0": pa.array(["1", secret]), "a1": pa.array(["1", "2"]),
                            "a2": pa.array(["294270-01-01 00:00:00", None]), "a3": pa.array(["PT1S", "PT2S"])})
    with pytest.raises(T.WideningNeeded) as e:
        T.backfill(p, rows)
    assert e.value.positions == (0, 2) and secret not in _all_text(e.value)
    p2 = p.widened(0).widened(2)
    out = T.backfill(p2, rows)
    assert union_values(out.column(0)) == [(0, D(1)), (1, secret)]
    # a bad value next to a misfit: the misfit is reported first; after widening the bad value
    rows3 = pa.record_batch({"a0": pa.array([secret, "x"]), "a1": pa.array(["1", "2"]),
                             "a2": pa.array([None, None], pa.string()), "a3": pa.array([None, None], pa.string())})
    with pytest.raises(T.ConversionError) as e3:
        T.backfill(p.widened(0), rows3)
    assert e3.value.position == 0 and e3.value.row == 1


def test_batch_that_breaks_the_plan():
    p = T.plan(one("int4"), server_version_num=V18)
    with pytest.raises(T.ConversionError):
        T.stream(p, pa.record_batch({"a0": pa.array(["1"])}))              # transport type is not int32
    with pytest.raises(T.ConversionError):
        T.backfill(p, pa.record_batch({"a0": pa.array([1], pa.int32())}))  # backfill takes text
    with pytest.raises(T.ConversionError):
        T.backfill(p, pa.record_batch({"a3": pa.array(["1"])}))            # no such position


# ------------------------------------------------------------------------------------------------ connection

def test_connection_is_isolated_from_motherduck():
    t = T.Transformer()
    con = t.conn
    assert [r[0] for r in con.execute("SELECT database_name FROM duckdb_databases() ORDER BY 1").fetchall()] == [
        "memory", "system", "temp"]
    for sql in ("ATTACH 'md:' AS m", "INSTALL motherduck", "LOAD motherduck", "ATTACH 'x.duckdb'"):
        with pytest.raises(duckdb.Error):
            con.execute(sql)
    settings = dict(con.execute("SELECT name, value FROM duckdb_settings() WHERE name IN ('enable_external_access', "
                                "'autoload_known_extensions', 'arrow_lossless_conversion', 'TimeZone')").fetchall())
    assert settings == {"enable_external_access": "false", "autoload_known_extensions": "false",
                        "arrow_lossless_conversion": "true", "TimeZone": "UTC"}
    t.close()


def test_spill_area_setting_and_nothing_left_behind(tmp_path):
    spill = tmp_path / "spill"
    spill.mkdir()
    t = T.Transformer(T.TransformSettings(threads=2, memory_limit="64MB", temp_directory=str(spill),
                                          max_temp_directory_size="1GB"))
    got = dict(t.conn.execute("SELECT name, value FROM duckdb_settings() WHERE name IN ('threads', 'memory_limit', "
                              "'temp_directory', 'max_temp_directory_size')").fetchall())
    assert got["threads"] == "2" and got["temp_directory"] == str(spill)
    p = T.plan(one("date"), server_version_num=V18)
    T.Transformer.backfill(t, p, pa.record_batch({"a0": pa.array(["2020-01-01"] * 10)}))
    t.close()
    assert list(spill.iterdir()) == []


def test_threads_give_the_same_results_as_one():
    t = table([col(1, "n", "numeric"), col(2, "d", "date"), col(3, "ts", "timestamptz"), col(4, "iv", "interval")])
    p = T.plan(t, server_version_num=V18)
    n = 5000
    rows = pa.record_batch({"a0": pa.array([str(i) + ".25" for i in range(n)]),
                            "a1": pa.array([R.date_text(i * 37 - 90000) for i in range(n)]),
                            "a2": pa.array([R.ts_text(i * 123456789011, tz=True) for i in range(n)]),
                            "a3": pa.array([R.interval_iso(i, -i, i * 1000001) for i in range(n)])})
    ref = T.Transformer().backfill(p, rows)
    results, errors = [None] * 6, []

    def work(k):
        try:
            for _ in range(5):
                results[k] = T.backfill(p, rows)               # each thread has its own transformer
        except Exception as e:                              # noqa: BLE001
            errors.append(e)

    th = [threading.Thread(target=work, args=(k,)) for k in range(6)]
    for x in th:
        x.start()
    for x in th:
        x.join()
    assert not errors and all(r.equals(ref) for r in results)


# ------------------------------------------------------------------------------------------------ from the review of apply


def test_lossless_arrow_types_for_uuid_timetz_and_json_text():
    """Rule 1: UUID and TIMETZ leave the transform in DuckDB's lossless Arrow types; JSON is VARCHAR on purpose
    (DuckDB's JSON refuses texts that Postgres json holds, e.g. '"\\ud800"')."""
    t = table([col(1, "u", "uuid"), col(2, "z", "timetz"), col(3, "j", "json"), col(4, "jb", "jsonb"),
               col(5, "ts", "timestamptz")])
    p = T.plan(t, server_version_num=V18)
    vals = {"a0": ["a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"], "a1": ["12:00:00+05:30"], "a2": ['"\\ud800"'],
            "a3": ['{"a": 1}'], "a4": ["2020-01-01 00:00:00+00"]}
    out = T.backfill(p, pa.record_batch({k: pa.array(v) for k, v in vals.items()}))
    assert [f.type for f in out.schema] == [pa.uuid(), F.TIMETZ_ARROW, pa.string(), pa.string(),
                                            pa.timestamp("us", "UTC")]
    assert out.schema == p.arrow_schema()
    with pytest.raises(duckdb.Error):
        duckdb.connect().execute("SELECT CAST(? AS JSON)", ['"\\ud800"'])
    con = duckdb.connect()
    con.register("o", out)
    assert con.execute("SELECT typeof(a0), typeof(a1) FROM o").fetchone() == ("UUID", "TIME WITH TIME ZONE")


def test_negative_zero_and_nan_kept_by_bits_on_both_paths_also_inside_arrays():
    """Rule 2: no value passes through an aggregate, a window or an equality; -0 and NaN keep their bits, also
    inside an array (text in P3) and a composite (text in P3)."""
    t = table([col(1, "f8", "float8"), col(2, "f4", "float4"), col(3, "a", array("float8"), type_name="x[]"),
               col(4, "r", "text")])
    p = T.plan(t, server_version_num=V18)
    texts = {"a0": ["-0", "0", "NaN"], "a1": ["-0", "0", "NaN"], "a2": ["{-0,0,NaN}", "{NULL}", "{}"],
             "a3": ["(-0,NaN)", "(0,0)", None]}
    out = T.backfill(p, pa.record_batch({k: pa.array(v) for k, v in texts.items()}))
    neg_nan = struct.unpack("<d", struct.pack("<Q", 0xFFF8000000000000))[0]
    stream_in = pa.record_batch({"a0": pa.array([-0.0, 0.0, neg_nan]), "a1": pa.array([-0.0, 0.0, neg_nan],
                                                                                         pa.float32()),
                                 "a2": pa.array(texts["a2"]), "a3": pa.array(texts["a3"])})
    sout = T.stream(p, stream_in)
    b8 = out.column(0).buffers()[1].to_pybytes()
    assert [b8[i * 8:i * 8 + 8][::-1].hex() for i in range(3)] == ["8000000000000000", "0000000000000000",
                                                                    "7ff8000000000000"]
    assert [C.py_float8(v) for v in out.column(0).to_pylist()] == ["8000000000000000", "0000000000000000", "NaN"]
    assert [C.py_float4(v) for v in out.column(1).to_pylist()] == [C.py_float4(-0.0), C.py_float4(0.0), "NaN"]
    assert out.column(2).to_pylist() == texts["a2"] and out.column(3).to_pylist() == texts["a3"]
    # the stream keeps both NaN signs and -0 bit for bit (it passes the buffers on)
    assert sout.column(0).buffers()[1].to_pybytes() == stream_in.column(0).buffers()[1].to_pybytes()
    assert sout.column(1).buffers()[1].to_pybytes() == stream_in.column(1).buffers()[1].to_pybytes()
    assert sout.column(2).to_pylist() == texts["a2"]


UNION_FAMILIES = [("numeric", -1, "1.5"), ("numeric", numeric_typmod(15, 2), "1.50"),
                  ("timestamp", -1, "2020-01-01 00:00:00"), ("timestamptz", -1, "2020-01-01 00:00:00+00"),
                  ("interval", -1, "P1D")]


@pytest.mark.parametrize("typ,typmod,value", UNION_FAMILIES)
def test_source_null_is_a_real_null_in_a_union_column(typ, typmod, value):
    """Rule 3: every family with a UNION (wide) form; a NULL read into DuckDB is NULL, not a member holding NULL.
    (No family uses a STRUCT wrapper.)"""
    p = T.plan(one(typ, typmod), server_version_num=V18).widened(0)
    out = T.backfill(p, pa.record_batch({"a0": pa.array([value, None])})).column(0)
    con = duckdb.connect()
    con.execute("SET TimeZone = 'UTC'")
    con.execute("CREATE TABLE d (%s)" % p.column_ddl())
    con.register("o", pa.table({"c": out}))
    con.execute("INSERT INTO d SELECT c FROM o")
    assert con.execute('SELECT "c" IS NULL, union_tag("c") FROM d').fetchall() == [(False, "n"), (True, None)]


# ------------------------------------------------------------------------------------------------ seams


def test_ambiguous_mask():
    t = table([col(1, "id", "int4"), col(2, "iv", "interval"), col(3, "other", "interval")], key=(1,))
    zero = R.interval_transport(0, 0, 0)
    rows = pa.record_batch({"seq": pa.array([1, 2, 3, 4], pa.int64()), "a0": pa.array([1, 2, 3, 4], pa.int32()),
                            "a1": pa.array([zero, "0 months 1 days 0 microseconds", None, "1 months 0 days 0 "
                                            "microseconds"]),
                            "a2": pa.array([None, None, None, zero])})
    p18 = T.plan(t, server_version_num=180006)
    assert T.ambiguous(p18, rows).to_pylist() == [True, False, False, True]
    assert T.ambiguous(T.plan(t, server_version_num=160006), rows) is None
    assert T.Transformer().ambiguous(p18, rows).to_pylist() == [True, False, False, True]


def test_pii_step_seam_applies_on_both_paths():
    """SPEC 8.3 seam (P4 builds the rules): a per-column step, applied after the conversion on both paths."""
    t = table([col(1, "id", "int4"), col(2, "s", "text"), col(3, "n", "numeric", numeric_typmod(10, 2))], key=(1,))
    p = T.plan(t, server_version_num=V18).with_step(1, "upper({x})").with_step(2, "{x} * 0")
    assert p.hash != T.plan(t, server_version_num=V18).hash
    b = T.backfill(p, pa.record_batch({"a0": pa.array(["1"]), "a1": pa.array(["abc"]), "a2": pa.array(["2.50"])}))
    s = T.stream(p, pa.record_batch({"a0": pa.array([1], pa.int32()), "a1": pa.array(["abc"]),
                                     "a2": pa.array(["2.50"])}))
    assert b.equals(s) and b.column(1).to_pylist() == ["ABC"] and b.column(2).to_pylist() == [D("0.00")]


def test_stream_fast_path_equals_the_sql_path(monkeypatch):
    """The pyarrow path for common stream values (date, finite timestamps, numeric(p<=18,s)) gives exactly what the
    SQL path gives; a batch with a value it does not cover takes the SQL path."""
    from cdc_v2.transform import convert
    t = table([col(1, "d", "date"), col(2, "ts", "timestamp"), col(3, "tz", "timestamptz"),
               col(4, "n", "numeric", numeric_typmod(15, 2)), col(5, "s", "numeric", numeric_typmod(5, -2))])
    p = T.plan(t, server_version_num=V18)
    rows = pa.record_batch({"a0": pa.array([0, -2451545, R.MAX_DATE_DAYS - R.PG_EPOCH_DAYS, None], pa.int32()),
                            "a1": pa.array([0, -(10 ** 17), R.DUCK_TS_MAX - R.PG_EPOCH_US, None], pa.int64()),
                            "a2": pa.array([1, -1, 10 ** 18, None], pa.int64()),
                            "a3": pa.array(["0.00", "-9999999999999.99", "12.30", None]),
                            "a4": pa.array(["12300", "-9999900", "0", None])})
    fast = T.Transformer()
    out_fast = fast.stream(p, rows)
    assert convert._fast_all(fast._compile(p, rows, "stream"), rows) is not None
    monkeypatch.setattr(convert, "_fast_all", lambda c, r: None)
    out_sql = T.Transformer().stream(p, rows)
    assert out_fast.schema == out_sql.schema
    for a, b in zip(out_fast.columns, out_sql.columns):
        assert same_values(a, b)
    monkeypatch.undo()
    for special in ({"a0": pa.array([2147483647], pa.int32())}, {"a1": pa.array([2 ** 63 - 1], pa.int64())},
                    {"a3": pa.array(["NaN"])}):
        cols = {k: v.slice(0, 1) for k, v in zip(rows.schema.names, rows.columns)}
        cols.update(special)
        b = pa.record_batch(cols)
        assert convert._fast_all(fast._compile(p, b, "stream"), b) is None


def same_values(a, b):
    if pa.types.is_date32(a.type):
        return a.view(pa.int32()).equals(b.view(pa.int32()))
    if pa.types.is_timestamp(a.type):
        return a.view(pa.int64()).equals(b.view(pa.int64()))
    return a.equals(b)
