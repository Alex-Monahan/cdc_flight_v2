"""Engine frames in Python: an encoder that produces the bytes the Java engine produces, and the shared generator.

For hermetic tests, the stream fuzzer and the tests of later packages, which must not need a JVM:

    enc = Encoder()                       # frame sequence numbers from 0
    data = enc.encode(Begin(...)) + enc.encode(Rows(...)) + ...
    frames = cdc_v2.stream.frames.decode_all(data)

`generate(seed, count)` produces the same frames as the Java `CdcEngine --framegen <seed> <out> <count>` (both use
the splitmix64 generator below), so `Encoder().encode_all(generate(s, n))` equals the Java output byte for byte.
`rows(...)` builds a ROWS frame from Python values for tests.
"""
from __future__ import annotations

import struct
import zlib

import pyarrow as pa

from cdc_v2.crashpoints import splitmix64
from cdc_v2.stream import frames as F

_M64 = (1 << 64) - 1
_PTYPE_OF = {pa.bool_(): F.P_BOOL, pa.int16(): F.P_INT16, pa.int32(): F.P_INT32, pa.int64(): F.P_INT64,
             pa.float32(): F.P_FLOAT32, pa.float64(): F.P_FLOAT64, pa.string(): F.P_UTF8, pa.binary(): F.P_BINARY}


class _Buf:
    def __init__(self):
        self.b = bytearray()

    def u8(self, v):
        self.b += struct.pack("<B", v)

    def u16(self, v):
        self.b += struct.pack("<H", v)

    def u32(self, v):
        self.b += struct.pack("<I", v & 0xFFFFFFFF)

    def u64(self, v):
        self.b += struct.pack("<Q", v & _M64)

    def str(self, s):
        e = s.encode("utf-8", "surrogatepass")
        self.u32(len(e))
        self.b += e

    def bin(self, data):
        self.u32(len(data))
        self.b += data

    def opt_u64(self, v):
        if v is None:
            self.u8(0)
        else:
            self.u8(1)
            self.u64(v)

    def flag(self, v):
        self.u8(1 if v else 0)

    def pad8(self):
        self.b += b"\0" * ((8 - (len(self.b) & 7)) & 7)

    def buffer(self, data):
        self.u32(len(data))
        self.pad8()
        self.b += data
        self.pad8()

    def posmap(self, m):
        keys = sorted(m, key=lambda k: k.encode("utf-16-be", "surrogatepass"))   # Java TreeMap<String> order
        self.u16(len(keys))
        for k in keys:
            self.str(k)
            v = m[k]
            if v is None:
                self.u8(0)
            elif isinstance(v, bool):
                self.u8(3)
                self.flag(v)
            elif isinstance(v, int):
                self.u8(1)
                self.u64(v)
            else:
                self.u8(2)
                self.str(str(v))


def _bitmap_clean(raw, n):
    """The first ceil(n/8) bytes of a bitmap with the bits at and beyond n cleared."""
    nb = (n + 7) >> 3
    out = bytearray(raw[:nb])
    if n & 7:
        out[-1] &= (1 << (n & 7)) - 1
    return out


def _column_buffers(arr: pa.Array):
    """(ptype, null_count, validity, offsets or None, data) of an Arrow array, as the Java engine writes them:
    NULL slots hold zero bytes (fixed width), a cleared bit (boolean) or an empty value (text, bytes)."""
    ptype = _PTYPE_OF.get(arr.type)
    if ptype is None:
        raise ValueError("no physical type for Arrow type %s" % arr.type)
    if arr.offset:
        arr = pa.concat_arrays([arr])
    n = len(arr)
    nulls = arr.null_count
    bufs = arr.buffers()
    valid_bits = None
    validity = b""
    if nulls:
        validity = bytes(_bitmap_clean(bufs[0].to_pybytes(), n))
        valid_bits = [(validity[i >> 3] >> (i & 7)) & 1 for i in range(n)]
    if ptype in (F.P_UTF8, F.P_BINARY):
        offs = struct.unpack("<%di" % (n + 1), bufs[1].to_pybytes()[:4 * (n + 1)]) if n or bufs[1] else (0,)
        data = bufs[2].to_pybytes() if bufs[2] is not None else b""
        if offs[0] != 0 or valid_bits is not None:
            out_offs, out_data, pos = [0], bytearray(), 0
            for i in range(n):
                if valid_bits is None or valid_bits[i]:
                    piece = data[offs[i]:offs[i + 1]]
                    out_data += piece
                    pos += len(piece)
                out_offs.append(pos)
            offs, data = out_offs, bytes(out_data)
        else:
            data = data[:offs[n]]
        return ptype, nulls, validity, struct.pack("<%di" % (n + 1), *offs), data
    if ptype == F.P_BOOL:
        data = _bitmap_clean(bufs[1].to_pybytes(), n)
        if valid_bits is not None:
            for i in range(n):
                if not valid_bits[i]:
                    data[i >> 3] &= ~(1 << (i & 7)) & 0xFF
        return ptype, nulls, validity, None, bytes(data)
    w = F.WIDTHS[ptype]
    data = bytearray(bufs[1].to_pybytes()[:w * n])
    if valid_bits is not None:
        for i in range(n):
            if not valid_bits[i]:
                data[i * w:(i + 1) * w] = b"\0" * w
    return ptype, nulls, validity, None, bytes(data)


def encode_payload(frame):
    """(type, payload bytes) of a frame object of cdc_v2.stream.frames."""
    b = _Buf()
    if isinstance(frame, F.Hello):
        b.u32(frame.version)
        b.str(frame.debezium_version)
        b.str(frame.slot)
        b.opt_u64(frame.start_position)
        b.u32(frame.jvm_pid)
        b.str(frame.engine_id)
        b.opt_u64(frame.active_pid)
        b.opt_u64(frame.backend_start_us)
        b.str(frame.application_name)
        return F.HELLO, bytes(b.b)
    if isinstance(frame, F.Schema):
        b.str(frame.schema)
        b.str(frame.table)
        b.u32(frame.version)
        b.flag(frame.has_key)
        b.u16(len(frame.key_positions))
        for p in frame.key_positions:
            b.u16(p)
        b.u16(len(frame.columns))
        for c in frame.columns:
            b.u16(c.position)
            b.str(c.name)
            b.str(c.connect_type)
            b.str(c.semantic_name)
            b.str(c.form)
            b.u8(c.physical)
            b.str(c.source_type)
            b.u32(c.length)
            b.u32(c.scale)
            b.flag(c.nullable)
            b.flag(c.key)
            b.flag(c.unchanged)
        return F.SCHEMA, bytes(b.b)
    if isinstance(frame, F.Rows):
        batch = frame.batch
        b.str(frame.schema)
        b.str(frame.table)
        b.u32(frame.version)
        b.u32(batch.num_rows)
        b.u16(batch.num_columns)
        for name, arr in zip(batch.schema.names, batch.columns):
            ptype, nulls, validity, offsets, data = _column_buffers(arr)
            b.str(name)
            b.u8(ptype)
            b.u32(nulls)
            b.buffer(validity)
            if offsets is not None:
                b.buffer(offsets)
            b.buffer(data)
        return F.ROWS, bytes(b.b)
    if isinstance(frame, (F.Begin, F.End)):
        b.u64(frame.seq)
        b.str(frame.txid)
        b.u64(frame.commit_time_ms)
        b.posmap(frame.positions)
        b.opt_u64(frame.ack_position)
        if isinstance(frame, F.End):
            b.u64(frame.event_count)
            b.u32(len(frame.collections))
            for name, count in frame.collections:
                b.str(name)
                b.u64(count)
            return F.END, bytes(b.b)
        return F.BEGIN, bytes(b.b)
    if isinstance(frame, F.Truncate):
        b.u64(frame.seq)
        b.str(frame.schema)
        b.str(frame.table)
        b.posmap(frame.positions)
        return F.TRUNCATE, bytes(b.b)
    if isinstance(frame, F.Message):
        b.u64(frame.seq)
        b.str(frame.prefix)
        b.flag(frame.transactional)
        b.bin(frame.content)
        b.posmap(frame.positions)
        return F.MESSAGE, bytes(b.b)
    if isinstance(frame, F.Heartbeat):
        b.u64(frame.seq)
        b.posmap(frame.positions)
        b.opt_u64(frame.ack_position)
        return F.HEARTBEAT, bytes(b.b)
    if isinstance(frame, F.BatchEnd):
        b.u32(frame.records)
        b.opt_u64(frame.acked)
        return F.BATCH_END, bytes(b.b)
    if isinstance(frame, F.Fatal):
        b.str(frame.error_class)
        b.str(frame.message)
        b.u16(len(frame.causes))
        for cls, msg in frame.causes:
            b.str(cls)
            b.str(msg)
        return F.FATAL, bytes(b.b)
    raise TypeError("not a frame: %r" % (frame,))


def envelope(ftype, seq, payload):
    head = F.HEADER.pack(F.MAGIC, ftype, b"\0\0\0", seq, len(payload))
    return head + payload + F.CRC.pack(zlib.crc32(payload, zlib.crc32(head)) & 0xFFFFFFFF)


class Encoder:
    """Encodes frame objects; frame sequence numbers count from `seq`."""

    def __init__(self, seq=0):
        self.seq = seq

    def encode(self, frame):
        ftype, payload = encode_payload(frame)
        out = envelope(ftype, self.seq, payload)
        self.seq += 1
        return out

    def encode_all(self, frames):
        return b"".join(self.encode(f) for f in frames)


def rows(schema, table, version, columns):
    """A Rows frame from {name: (physical type, [values])} (None = NULL); column order as given."""
    names, arrays = [], []
    for name, (ptype, values) in columns.items():
        names.append(name)
        arrays.append(pa.array(values, type=F.ARROW_TYPES[ptype]))
    return F.Rows(schema, table, version, pa.RecordBatch.from_arrays(arrays, names=names))


# --------------------------------------------------------------------------------- the generator shared with Java

class Gen:
    """Same sequence as cdc_v2.engine.FrameGen: state += 1; value = splitmix64(state)."""

    def __init__(self, seed):
        self.state = seed & _M64

    def next(self):
        self.state = (self.state + 1) & _M64
        return splitmix64(self.state)

    def u(self, n):
        return self.next() % n

    def coin(self):
        return (self.next() & 1) == 1

    def text(self, n):
        out = []
        for _ in range(n):
            k = self.u(5)
            if k == 0:
                cp = self.u(128)
            elif k == 1:
                cp = 0x80 + self.u(0x780)
            elif k == 2:
                cp = 0x800 + self.u(0xD800 - 0x800)
            elif k == 3:
                cp = 0xE000 + self.u(0x2000)
            else:
                cp = 0x10000 + self.u(0x100000)
            out.append(chr(cp))
        return "".join(out)

    def bytes(self, n):
        return bytes(self.next() & 0xFF for _ in range(n))

    def opt_long(self):
        return self.next() if self.coin() else None

    def posmap(self):
        n = self.u(5)
        m = {}
        for i in range(n):
            kind = self.u(4)
            if kind == 0:
                v = None
            elif kind == 1:
                v = _s64(self.next())
            elif kind == 2:
                v = self.text(self.u(6))
            else:
                v = self.coin()
            m["k%d" % i] = v
        return m


def _s64(x):
    return x - (1 << 64) if x & (1 << 63) else x


def _s(x, bits):
    x &= (1 << bits) - 1
    return x - (1 << bits) if x & (1 << (bits - 1)) else x


_E16 = [0, -1, 1, -(1 << 15), (1 << 15) - 1]
_E32 = [0, -1, 1, -(1 << 31), (1 << 31) - 1]
_E64 = [0, -1, 1, -(1 << 63), (1 << 63) - 1]
_F32 = [0x7fc00000, 0x7f800000, 0xff800000, 0x80000000, 0x00000001, 0x7f7fffff]
_F64 = [0x7ff8000000000000, 0x7ff0000000000000, 0xfff0000000000000, 0x8000000000000000, 1, 0x7fefffffffffffff]


def _array(ptype, values):
    """An Arrow array from values; floats are given as raw bit patterns so NaN payloads survive."""
    n = len(values)
    valid = bytearray((n + 7) >> 3)
    nulls = 0
    for i, v in enumerate(values):
        if v is None:
            nulls += 1
        else:
            valid[i >> 3] |= 1 << (i & 7)
    vbuf = pa.py_buffer(bytes(valid)) if nulls else None
    if ptype in (F.P_UTF8, F.P_BINARY):
        offs, data = [0], bytearray()
        for v in values:
            if v is not None:
                data += v.encode("utf-8") if ptype == F.P_UTF8 else v
            offs.append(len(data))
        bufs = [vbuf, pa.py_buffer(struct.pack("<%di" % (n + 1), *offs)), pa.py_buffer(bytes(data))]
    elif ptype == F.P_BOOL:
        data = bytearray((n + 7) >> 3)
        for i, v in enumerate(values):
            if v:
                data[i >> 3] |= 1 << (i & 7)
        bufs = [vbuf, pa.py_buffer(bytes(data))]
    else:
        fmt = {F.P_INT16: "<h", F.P_INT32: "<i", F.P_INT64: "<q", F.P_FLOAT32: "<I", F.P_FLOAT64: "<Q"}[ptype]
        data = b"".join(struct.pack(fmt, 0 if v is None else v) for v in values)
        bufs = [vbuf, pa.py_buffer(data)]
    return pa.Array.from_buffers(F.ARROW_TYPES[ptype], n, bufs, null_count=nulls)


def _value(g, ptype):
    if g.u(5) == 0:
        return None
    if ptype == F.P_BOOL:
        return g.coin()
    if ptype == F.P_INT16:
        return _E16[g.u(5)] if g.u(4) == 0 else _s(g.next(), 16)
    if ptype == F.P_INT32:
        return _E32[g.u(5)] if g.u(4) == 0 else _s(g.next(), 32)
    if ptype == F.P_INT64:
        return _E64[g.u(5)] if g.u(4) == 0 else _s(g.next(), 64)
    if ptype == F.P_FLOAT32:
        return _F32[g.u(6)] if g.u(4) == 0 else g.next() & 0xFFFFFFFF
    if ptype == F.P_FLOAT64:
        return _F64[g.u(6)] if g.u(4) == 0 else g.next()
    if ptype == F.P_UTF8:
        return g.text(g.u(20))
    return g.bytes(g.u(20))


def generate_one(seed):
    g = Gen(seed)
    out = []
    dbz = "gen-%d" % g.u(1000)
    slot = "slot_%d" % g.u(100)
    start = g.opt_long()
    pid = g.u(100000)
    engine = "e%d" % g.u(1000)
    active = g.opt_long()
    bstart = g.opt_long()
    out.append(F.Hello(1, dbz, slot, start, pid, engine, active, bstart, g.text(g.u(12))))
    schema = "s%d" % g.u(3)
    table = "t" + g.text(g.u(6))
    version = 1 + g.u(5)
    has_key = g.coin()
    ncols = 1 + g.u(8)
    cols, ptypes, unchanged = [], [], []
    for i in range(ncols):
        p = 1 + g.u(8)
        ptypes.append(p)
        name = g.text(g.u(10))
        sem = "sem.%d" % g.u(9) if g.coin() else ""
        src = g.text(g.u(8))
        length = g.u(1000) if g.coin() else -1
        scale = g.u(20) if g.coin() else -1
        nullable = g.coin()
        unch = p >= F.P_UTF8 and g.coin()
        unchanged.append(unch)
        cols.append(F.Column(i, name, "T%d" % p, sem, "f%d" % p, p, src, length, scale, nullable,
                             has_key and i == 0, unch))
    out.append(F.Schema(schema, table, version, has_key, (0,) if has_key else (), tuple(cols)))
    seq = g.u(1 << 30)
    txid = g.text(g.u(10))
    ts = _s64(g.next())
    pos = g.posmap()
    out.append(F.Begin(seq, txid, ts, pos, g.opt_long()))
    names = ["seq", "op"] + ["a%d" % i for i in range(ncols)]
    types = [F.P_INT64, F.P_UTF8] + ptypes
    if not has_key:
        names += ["b%d" % i for i in range(ncols)]
        types += ptypes
    for i in range(ncols):
        if unchanged[i]:
            names.append("u%d" % i)
            types.append(F.P_BOOL)
    if has_key:
        names.append("k0")
        types.append(ptypes[0])
    nframes = g.u(4)
    rseq = g.u(1000)
    for _ in range(nframes):
        nrows = g.u(40)
        values = [[] for _ in names]
        for _ in range(nrows):
            values[0].append(rseq)
            rseq += 1
            values[1].append("cud"[g.u(3)])
            for c in range(2, len(names)):
                values[c].append(_value(g, types[c]))
        arrays = [_array(t, v) for t, v in zip(types, values)]
        out.append(F.Rows(schema, table, version, pa.RecordBatch.from_arrays(arrays, names=names)))
    seq = g.u(1 << 30)
    s, t = g.text(g.u(5)), g.text(g.u(5))
    out.append(F.Truncate(seq, s, t, g.posmap()))
    seq = g.u(1 << 30)
    prefix = g.text(g.u(8))
    tx = g.coin()
    content = g.bytes(g.u(30))
    out.append(F.Message(seq, prefix, tx, content, g.posmap()))
    seq = g.u(1 << 30)
    txid = g.text(g.u(10))
    ts = _s64(g.next())
    pos = g.posmap()
    ack = g.opt_long()
    count = g.u(1000000)
    colls = []
    for _ in range(g.u(3)):
        name = g.text(g.u(8))
        colls.append((name, g.u(1000)))
    out.append(F.End(seq, txid, ts, pos, ack, count, tuple(colls)))
    seq = g.u(1 << 30)
    pos = g.posmap()
    out.append(F.Heartbeat(seq, pos, g.opt_long()))
    recs = g.u(10000)
    out.append(F.BatchEnd(recs, g.opt_long()))
    cls = g.text(g.u(20))
    msg = g.text(g.u(40))
    causes = []
    for _ in range(g.u(3)):
        a = g.text(g.u(10))
        causes.append((a, g.text(g.u(10))))
    out.append(F.Fatal(cls, msg, tuple(causes)))
    return out


def generate(seed, count=1):
    """Frames for seeds seed .. seed+count-1, concatenated (as `CdcEngine --framegen seed out count`)."""
    out = []
    for i in range(count):
        out.extend(generate_one(seed + i))
    return out
