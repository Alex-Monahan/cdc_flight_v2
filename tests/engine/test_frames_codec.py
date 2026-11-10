"""Frame codec round trip with the Python encoder (hermetic). Package E, plan C2."""
import math
import struct

import pyarrow as pa
import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from cdc_v2.stream import frames as F
from cdc_v2.testing import frames as T


def payloads(frames):
    return [T.encode_payload(f) for f in frames]


def roundtrip(frames, chunk=None):
    data = T.Encoder().encode_all(frames)
    out = F.decode_all(data, chunk=chunk)
    assert payloads(out) == payloads(frames)
    assert T.Encoder().encode_all(out) == data
    return out


# ------------------------------------------------------------------------------------------------- strategies

texts = st.text(alphabet=st.characters(blacklist_categories=("Cs",)), max_size=30)
u64 = st.integers(0, (1 << 64) - 1)
i64 = st.integers(-(1 << 63), (1 << 63) - 1)
opt_u64 = st.none() | u64
posmaps = st.dictionaries(st.text(min_size=1, max_size=8), st.none() | i64 | texts | st.booleans(), max_size=5)

VALUES = {
    F.P_BOOL: st.booleans(),
    F.P_INT16: st.integers(-(1 << 15), (1 << 15) - 1),
    F.P_INT32: st.integers(-(1 << 31), (1 << 31) - 1),
    F.P_INT64: i64,
    F.P_FLOAT32: st.integers(0, (1 << 32) - 1),          # raw bit patterns (NaN payloads, -0, infinities)
    F.P_FLOAT64: st.integers(0, (1 << 64) - 1),
    F.P_UTF8: texts,
    F.P_BINARY: st.binary(max_size=40),
}


@st.composite
def rows_frames(draw):
    nrows = draw(st.integers(0, 30))
    ncols = draw(st.integers(1, 8))
    names, arrays = [], []
    for i in range(ncols):
        ptype = draw(st.sampled_from(sorted(VALUES)))
        vals = draw(st.lists(st.none() | VALUES[ptype], min_size=nrows, max_size=nrows))
        names.append("c%d" % i)
        arrays.append(T._array(ptype, vals))
    return F.Rows(draw(texts), draw(texts), draw(st.integers(0, (1 << 32) - 1)),
                  pa.RecordBatch.from_arrays(arrays, names=names))


@st.composite
def schema_frames(draw):
    n = draw(st.integers(0, 6))
    cols = tuple(F.Column(i, draw(texts), draw(texts), draw(texts), draw(texts), draw(st.integers(1, 8)), draw(texts),
                          draw(st.integers(-1, 1 << 20)), draw(st.integers(-1, 100)), draw(st.booleans()),
                          draw(st.booleans()), draw(st.booleans())) for i in range(n))
    keys = tuple(draw(st.lists(st.integers(0, 65535), max_size=3)))
    return F.Schema(draw(texts), draw(texts), draw(st.integers(0, (1 << 32) - 1)), draw(st.booleans()), keys, cols)


other_frames = st.one_of(
    st.builds(F.Hello, st.integers(0, (1 << 32) - 1), texts, texts, opt_u64, st.integers(0, (1 << 32) - 1), texts,
              opt_u64, opt_u64, texts),
    st.builds(F.Begin, u64, texts, i64, posmaps, opt_u64),
    st.builds(F.End, u64, texts, i64, posmaps, opt_u64, u64,
              st.lists(st.tuples(texts, u64), max_size=3).map(tuple)),
    st.builds(F.Truncate, u64, texts, texts, posmaps),
    st.builds(F.Message, u64, texts, st.booleans(), st.binary(max_size=60), posmaps),
    st.builds(F.Heartbeat, u64, posmaps),
    st.builds(F.BatchEnd, st.integers(0, (1 << 32) - 1), opt_u64),
    st.builds(F.Fatal, texts, texts, st.lists(st.tuples(texts, texts), max_size=3).map(tuple), posmaps, texts, texts),
)

any_frame = st.one_of(other_frames, rows_frames(), schema_frames())


@settings(max_examples=150, deadline=None, suppress_health_check=[HealthCheck.too_slow])
@given(st.lists(any_frame, max_size=6), st.integers(1, 64))
def test_roundtrip_property(frames, chunk):
    roundtrip(frames, chunk=chunk)


def test_every_frame_type_and_physical_type():
    frames = T.generate(7, 20)
    kinds = {type(f).__name__ for f in frames}
    assert kinds == {"Hello", "Schema", "Begin", "Rows", "Truncate", "Message", "End", "Heartbeat", "BatchEnd",
                     "Fatal"}
    ptypes = {t for f in frames if isinstance(f, F.Rows) for t in f.batch.schema.types}
    assert ptypes == set(F.ARROW_TYPES.values())
    roundtrip(frames)
    roundtrip(frames, chunk=1)      # a frame never has to fit one read


def test_edges_nulls_empty_zero_length_zero_byte_extremes():
    cols = {
        "seq": (F.P_INT64, [0, 1, 2, 3]),
        "i16": (F.P_INT16, [-(1 << 15), (1 << 15) - 1, None, 0]),
        "i32": (F.P_INT32, [-(1 << 31), (1 << 31) - 1, None, 0]),
        "i64": (F.P_INT64, [-(1 << 63), (1 << 63) - 1, None, 0]),
        "f32": (F.P_FLOAT32, [float("nan"), float("inf"), float("-inf"), -0.0]),
        "f64": (F.P_FLOAT64, [float("nan"), 5e-324, 1.7976931348623157e308, -0.0]),
        "t": (F.P_UTF8, ["", "a\0b", None, "\U0010FFFFé"]),
        "b": (F.P_BINARY, [b"", b"\0", None, bytes(range(256))]),
        "bo": (F.P_BOOL, [True, False, None, True]),
        "allnull": (F.P_INT32, [None, None, None, None]),
    }
    r = T.rows("s", "t", 1, cols)
    empty = T.rows("s", "t", 2, {k: (p, []) for k, (p, _v) in cols.items()})
    out = roundtrip([r, empty])
    b = out[0].batch
    assert b.column("t").to_pylist() == ["", "a\0b", None, "\U0010FFFFé"]
    assert b.column("b").to_pylist()[1] == b"\0"
    f = b.column("f64").to_pylist()
    assert math.isnan(f[0]) and f[1] == 5e-324 and math.copysign(1, f[3]) == -1
    assert struct.pack("<f", b.column("f32").to_pylist()[3]) == struct.pack("<f", -0.0)
    assert out[1].batch.num_rows == 0
    assert b.column("allnull").null_count == 4


def test_nan_payload_preserved_bit_for_bit():
    arr = T._array(F.P_FLOAT64, [0x7ff8000000000123, 0xfff0000000000001])
    out = roundtrip([F.Rows("s", "t", 1, pa.RecordBatch.from_arrays([arr], names=["x"]))])
    raw = out[0].batch.column(0).buffers()[1].to_pybytes()
    assert struct.unpack("<2Q", raw) == (0x7ff8000000000123, 0xfff0000000000001)


def test_rows_buffers_are_zero_copy_slices_of_the_payload():
    r = T.rows("s", "t", 1, {"x": (F.P_INT64, list(range(1000))), "y": (F.P_UTF8, ["v%d" % i for i in range(1000)])})
    data = T.Encoder().encode(r)
    p = F.FrameParser()
    (out,) = p.feed(data)
    col = out.batch.column(0)
    assert col.buffers()[1].address % 8 == 0       # aligned, so DuckDB and Arrow kernels can use it as is
    assert out.batch.column(1).buffers()[2].address % 8 == 0


def test_sliced_arrow_input_is_normalised():
    arr = pa.array(["a", None, "ccc", "dd"]).slice(1, 3)
    b = pa.RecordBatch.from_arrays([arr], names=["x"])
    out = roundtrip([F.Rows("s", "t", 1, b)])
    assert out[0].batch.column(0).to_pylist() == [None, "ccc", "dd"]


def test_generator_is_deterministic():
    assert T.Encoder().encode_all(T.generate(5, 3)) == T.Encoder().encode_all(T.generate(5, 3))
    assert T.Encoder().encode_all(T.generate(5)) != T.Encoder().encode_all(T.generate(6))


def test_length_limit_and_reserved_bytes():
    data = T.Encoder().encode(F.Message(0, "p", False, b"x" * 100, {}))
    with pytest.raises(F.EngineBroken, match="above the limit"):
        F.decode_all(data, max_frame_bytes=50)
    bad = bytearray(data)
    bad[6] = 1
    with pytest.raises(F.EngineBroken):
        F.decode_all(bytes(bad))


def test_broken_parser_stays_broken():
    data = T.Encoder().encode_all(T.generate(3))
    bad = bytearray(data)
    bad[0] ^= 1
    p = F.FrameParser()
    with pytest.raises(F.EngineBroken):
        p.feed(bytes(bad))
    with pytest.raises(F.EngineBroken):
        p.feed(data)
    with pytest.raises(F.EngineBroken):
        p.eof()


def test_invalid_utf8_in_a_text_column_is_refused():
    r = T.rows("s", "t", 1, {"x": (F.P_BINARY, [b"\xff\xfe"])})
    data = bytearray(T.Encoder().encode(r))
    # change the physical type from binary (8) to utf8 (7) and fix the checksum: well framed, invalid content
    i = data.index(b"\x01\x00\x00\x00x") + 5
    assert data[i] == F.P_BINARY
    data[i] = F.P_UTF8
    body = bytes(data[:-4])
    import zlib
    data[-4:] = struct.pack("<I", zlib.crc32(body) & 0xFFFFFFFF)
    with pytest.raises(F.EngineBroken, match="invalid column buffers"):
        F.decode_all(bytes(data))


def test_rows_frame_without_columns_is_refused():
    from cdc_v2.testing.frames import envelope
    import pyarrow as pa
    payload = bytearray()
    for s in ("s", "t"):
        payload += struct.pack("<I", 1) + s.encode()
    payload += struct.pack("<IIH", 1, 5, 0)          # 5 rows, 0 columns
    with pytest.raises(F.EngineBroken, match="without columns"):
        F.decode_all(envelope(F.ROWS, 0, bytes(payload)))
