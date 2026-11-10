"""Property tests (hypothesis, local DuckDB only): random values per family are printed as Postgres prints them
(pgref, a printer of our own) and in the engine's transport form; both go through the transform; each result must
equal the generating value exactly, and the two results must be identical. Negative controls show that a wrong
conversion is caught."""
import decimal
import os
import struct

import pyarrow as pa
import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from cdc_v2 import transform as T
import pgref as R
from tables import numeric_typmod, one

V18 = 180006
D = decimal.Decimal
SETTINGS = dict(max_examples=int(os.environ.get("CDC_V2_Y_EXAMPLES", "30")), deadline=None, suppress_health_check=[HealthCheck.too_slow])
LISTS = dict(min_size=1, max_size=40)


def both(typ, texts, transport, ttype, typmod=-1, wide=False):
    p = T.plan(one(typ, typmod), server_version_num=V18)
    p = p.widened(0) if wide else p
    b = T.backfill(p, pa.record_batch({"a0": pa.array(texts, pa.string())})).column(0)
    s = T.stream(p, pa.record_batch({"a0": pa.array(transport, ttype)})).column(0)
    assert identical(b, s), (typ, texts)
    return b


def identical(a, b) -> bool:
    """Same type, same NULLs, same valid values bit for bit."""
    if a.type != b.type or len(a) != len(b):
        return False
    if pa.types.is_union(a.type):
        return (a.type_codes.equals(b.type_codes) and identical(a.field(0), b.field(0))
                and identical(a.field(1), b.field(1)))
    if isinstance(a, pa.ExtensionArray):
        return identical(a.storage, b.storage)
    if a.is_null().to_pylist() != b.is_null().to_pylist():
        return False
    if pa.types.is_floating(a.type):
        w = 8 if pa.types.is_float64(a.type) else 4
        ab, bb = a.buffers()[1].to_pybytes(), b.buffers()[1].to_pybytes()
        return all(ab[(i + a.offset) * w:(i + a.offset + 1) * w] == bb[(i + b.offset) * w:(i + b.offset + 1) * w]
                   for i in range(len(a)) if a[i].is_valid)
    if pa.types.is_date(a.type) or pa.types.is_timestamp(a.type) or pa.types.is_time(a.type):
        width = pa.int32() if pa.types.is_date32(a.type) else pa.int64()
        return a.view(width).equals(b.view(width))
    return a.equals(b)


def phys(arr):
    width = pa.int32() if pa.types.is_date32(arr.type) else pa.int64()
    return arr.view(width).to_pylist()


# ------------------------------------------------------------------------------------------------ families

@settings(**SETTINGS)
@given(st.lists(st.one_of(st.none(), st.integers(-2 ** 63, 2 ** 63 - 1)), **LISTS))
def test_bigint(vals):
    out = both("int8", [None if v is None else str(v) for v in vals], vals, pa.int64())
    assert out.to_pylist() == vals


@settings(**SETTINGS)
@given(st.lists(st.one_of(st.none(), st.integers(0, 2 ** 64 - 1)), **LISTS))
def test_double_bits(bits):
    vals = [None if b is None else struct.unpack("<d", struct.pack("<Q", b))[0] for b in bits]
    # Postgres has one NaN: its text is 'NaN', and the engine (Java's parser) gives the canonical bits
    canon = [None if v is None else (float("nan") if v != v else v) for v in vals]
    out = both("float8", [None if v is None else R.float8_text(v) for v in vals], canon, pa.float64())
    got = out.buffers()[1].to_pybytes()
    for i, v in enumerate(canon):
        if v is not None:
            assert got[i * 8:i * 8 + 8] == struct.pack("<d", v)


@settings(**SETTINGS)
@given(st.lists(st.one_of(st.none(), st.integers(0, 2 ** 32 - 1)), **LISTS))
def test_real_bits(bits):
    vals = [None if b is None else R.float4_value(b) for b in bits]
    canon = [None if v is None else (float("nan") if v != v else v) for v in vals]
    out = both("float4", [None if v is None else R.float4_text(v) for v in vals], canon, pa.float32())
    got = out.buffers()[1].to_pybytes()
    for i, v in enumerate(canon):
        if v is not None:
            assert got[i * 4:i * 4 + 4] == struct.pack("<f", v)


numeric_text = st.one_of(
    st.sampled_from(["NaN", "Infinity", "-Infinity", "0", "0.0", "1.50"]),
    st.builds(lambda neg, ip, fp: ("-" if neg else "") + ip + ("." + fp if fp else ""), st.booleans(),
              st.from_regex(r"[1-9][0-9]{0,40}|0", fullmatch=True), st.from_regex(r"[0-9]{0,16}", fullmatch=True)))


@settings(**SETTINGS)
@given(st.lists(st.one_of(st.none(), numeric_text), **LISTS))
def test_unconstrained_numeric(texts):
    out = both("numeric", texts, texts, pa.string(), wide=True)
    for i, t in enumerate(texts):
        tag = out.type_codes[i].as_py()
        if t is None:
            assert not out.field(tag)[i].is_valid
        elif R.numeric_fits(t):
            assert tag == 0 and out.field(0)[i].as_py() == D(t)
        else:
            assert tag == 1 and out.field(1)[i].as_py() == t
    fits = all(t is None or R.numeric_fits(t) for t in texts)
    p = T.plan(one("numeric"), server_version_num=V18)
    if fits:
        assert T.backfill(p, pa.record_batch({"a0": pa.array(texts, pa.string())})).column(0).to_pylist() == [
            None if t is None else D(t) for t in texts]
    else:
        with pytest.raises(T.WideningNeeded):
            T.backfill(p, pa.record_batch({"a0": pa.array(texts, pa.string())}))


@settings(**SETTINGS)
@given(st.lists(st.one_of(st.none(), st.integers(-10 ** 13 + 1, 10 ** 13 - 1)), **LISTS))
def test_numeric_15_2(cents):
    texts = [None if c is None else "%s%d.%02d" % ("-" if c < 0 else "", abs(c) // 100, abs(c) % 100)
             for c in cents]
    out = both("numeric", texts, texts, pa.string(), numeric_typmod(15, 2))
    assert out.to_pylist() == [None if c is None else D(c).scaleb(-2) for c in cents]


@settings(**SETTINGS)
@given(st.lists(st.one_of(st.none(), st.binary(max_size=64)), **LISTS))
def test_bytea(vals):
    out = both("bytea", [None if v is None else R.bytea_text(v) for v in vals], vals, pa.binary())
    assert out.to_pylist() == vals


dates = st.one_of(st.integers(R.MIN_DATE_DAYS, R.MAX_DATE_DAYS), st.integers(-800000, 3000000),
                  st.sampled_from(["inf", "-inf"]))


def date_forms(v):
    if v is None:
        return None, None, None
    if v == "inf":
        return "infinity", 2147483647, 2147483647
    if v == "-inf":
        return "-infinity", -2147483648, -2147483647
    return R.date_text(v), v - R.PG_EPOCH_DAYS, v


@settings(**SETTINGS)
@given(st.lists(st.one_of(st.none(), dates), **LISTS))
def test_date(vals):
    forms = [date_forms(v) for v in vals]
    out = both("date", [f[0] for f in forms], [f[1] for f in forms], pa.int32())
    assert phys(out) == [f[2] for f in forms]


stamps = st.one_of(st.integers(R.MIN_DATE_DAYS * R.DAY_US, R.MAX_TS_US),
                   st.integers(-(10 ** 17), 10 ** 17), st.integers(R.DUCK_TS_MAX - 10 ** 6, R.MAX_TS_US),
                   st.sampled_from(["inf", "-inf"]))


@pytest.mark.parametrize("tz", [False, True])
@settings(**SETTINGS)
@given(vals=st.lists(st.one_of(st.none(), stamps), **LISTS))
def test_timestamp(tz, vals):
    typ = "timestamptz" if tz else "timestamp"
    texts, transport, want = [], [], []
    for v in vals:
        if v is None:
            texts.append(None); transport.append(None); want.append(None)
        elif v in ("inf", "-inf"):
            texts.append(v.replace("inf", "infinity"))
            transport.append(2 ** 63 - 1 if v == "inf" else -(2 ** 63))
            want.append((0, 2 ** 63 - 1 if v == "inf" else -(2 ** 63 - 1)))
        else:
            texts.append(R.ts_text(v, tz))
            transport.append(v - R.PG_EPOCH_US)
            want.append((0, v) if v <= R.DUCK_TS_MAX else (1, R.ts_text(v, tz)))
    out = both(typ, texts, transport, pa.int64(), wide=True)
    nat = phys(out.field(0))
    for i, w in enumerate(want):
        tag = out.type_codes[i].as_py()
        if w is None:
            assert not out.field(tag)[i].is_valid
        elif w[0] == 0:
            assert tag == 0 and nat[i] == w[1]
        else:
            assert tag == 1 and out.field(1)[i].as_py() == w[1]


@settings(**SETTINGS)
@given(st.lists(st.one_of(st.none(), st.integers(0, R.DAY_US)), **LISTS))
def test_time(vals):
    out = both("time", [None if v is None else R.time_text(v) for v in vals], vals, pa.int64())
    assert phys(out) == vals


@settings(**SETTINGS)
@given(st.lists(st.one_of(st.none(), st.tuples(st.integers(0, R.DAY_US), st.integers(-57599, 57599))), **LISTS))
def test_timetz(vals):
    texts = [None if v is None else R.timetz_text(*v) for v in vals]
    out = both("timetz", texts, texts, pa.string())
    raw = out.storage.buffers()[1].to_pybytes()
    for i, v in enumerate(vals):
        if v is not None:
            bits = struct.unpack("<Q", raw[i * 8:i * 8 + 8])[0]
            assert (bits >> 24, 57599 - (bits & 0xFFFFFF)) == v


intervals = st.tuples(st.integers(-2 ** 31, 2 ** 31 - 1), st.integers(-2 ** 31, 2 ** 31 - 1),
                      st.one_of(st.integers(-(2 ** 63) + 1, 2 ** 63 - 1), st.integers(-10 ** 13, 10 ** 13)))


@settings(**SETTINGS)
@given(st.lists(st.one_of(st.none(), intervals), **LISTS))
def test_interval(vals):
    texts = [None if v is None else R.interval_postgres(*v) for v in vals]
    transport = [None if v is None else R.interval_transport(*v) for v in vals]
    out = both("interval", texts, transport, pa.string(), wide=True)
    for i, v in enumerate(vals):
        tag = out.type_codes[i].as_py()
        if v is None:
            assert not out.field(tag)[i].is_valid
        elif abs(v[2]) <= 9223372036854775:
            assert tag == 0 and out.field(0)[i].as_py() == pa.MonthDayNano([v[0], v[1], v[2] * 1000])
        else:
            assert tag == 1 and out.field(1)[i].as_py() == R.interval_iso(*v)


@settings(**SETTINGS)
@given(st.lists(st.one_of(st.none(), st.uuids()), **LISTS))
def test_uuid(vals):
    texts = [None if v is None else str(v) for v in vals]
    out = both("uuid", texts, texts, pa.string())
    assert out.storage.to_pylist() == [None if v is None else v.bytes for v in vals]


@settings(**SETTINGS)
@given(st.lists(st.one_of(st.none(), st.text(max_size=30)), **LISTS))
def test_text(vals):
    vals = [None if v is None else v.replace("\x00", "") for v in vals]       # Postgres text holds no NUL
    assert both("text", vals, vals, pa.string()).to_pylist() == vals


# ------------------------------------------------------------------------------------------------ negative controls


def test_negative_control_float_printed_with_too_few_digits_is_caught():
    """A conversion that printed a double with 15 significant digits and parsed it back would change the value;
    the comparison by bits catches it."""
    v = 0.1 + 0.2
    wrong = "%.15g" % v
    out = T.backfill(T.plan(one("float8"), server_version_num=V18), pa.record_batch({"a0": pa.array([wrong])}))
    assert struct.pack("<d", out.column(0)[0].as_py()) != struct.pack("<d", v)
    right = T.backfill(T.plan(one("float8"), server_version_num=V18),
                       pa.record_batch({"a0": pa.array([R.float8_text(v)])}))
    assert struct.pack("<d", right.column(0)[0].as_py()) == struct.pack("<d", v)


def test_negative_control_bc_date_through_the_plain_cast_is_caught():
    """DuckDB's own date cast reads '0044-03-15 BC' as 44 AD; the transform's value differs from it, and equals
    the value the stream delivers."""
    import duckdb
    plain = duckdb.connect().execute("SELECT CAST('0044-03-15 BC' AS DATE) - DATE '1970-01-01'").fetchone()[0]
    p = T.plan(one("date"), server_version_num=V18)
    ours = phys(T.backfill(p, pa.record_batch({"a0": pa.array(["0044-03-15 BC"])})).column(0))[0]
    stream = phys(T.stream(p, pa.record_batch({"a0": pa.array([R.days_from_civil(-43, 3, 15) - R.PG_EPOCH_DAYS],
                                                              pa.int32())})).column(0))[0]
    assert ours == stream == R.days_from_civil(-43, 3, 15) and plain != ours


def test_negative_control_normalised_negative_zero_is_caught():
    """A conversion that turned -0 into 0 (an aggregate, a GROUP BY) gives other bits; `identical` notices."""
    good = pa.array([-0.0])
    bad = pa.array([0.0])
    assert good.equals(bad)                                  # Arrow's own equality would miss it
    assert not identical(good, bad)
