"""Engine frames (plan C2, docs/design/engine.md "Byte layout"): the decoder.

A frame on file descriptor 3 of the engine:

    magic b"CDCE" | type u8 | 3 zero bytes | seq u64 | length u32 | payload | crc32 u32

little-endian; the CRC-32 (zlib's) covers every byte before it; `seq` starts at 0 and has no gaps. The decoder
checks magic, reserved bytes, sequence, the length limit and the checksum, and parses the payload completely (every
byte must be used). The first error raises `EngineBroken`; a broken decoder raises it again on every later call and
never yields anything after the error. `ROWS` become `pyarrow.RecordBatch`es whose buffers are slices of the frame
payload (no per-value work in Python).

`FrameParser` is the incremental core (bytes in, frames out; a frame may arrive in any number of pieces).
`FrameReader` reads a file descriptor with deadlines. `decode_all(data)` decodes a complete byte string.
"""
from __future__ import annotations

import os
import select
import struct
import zlib
from dataclasses import dataclass, field
from typing import Any, Optional

import pyarrow as pa

from cdc_v2.deadline import Deadline, DeadlineExceeded

MAGIC = b"CDCE"
HEADER = struct.Struct("<4sB3sQI")          # magic, type, reserved, seq, length
HEADER_SIZE = HEADER.size                   # 20
CRC = struct.Struct("<I")
PROTOCOL_VERSION = 1
DEFAULT_MAX_FRAME_BYTES = (1 << 30) + (16 << 20)

HELLO, SCHEMA, ROWS, BEGIN, END, TRUNCATE, MESSAGE, HEARTBEAT, BATCH_END, FATAL = range(1, 11)
TYPE_NAMES = {HELLO: "HELLO", SCHEMA: "SCHEMA", ROWS: "ROWS", BEGIN: "BEGIN", END: "END", TRUNCATE: "TRUNCATE",
              MESSAGE: "MESSAGE", HEARTBEAT: "HEARTBEAT", BATCH_END: "BATCH_END", FATAL: "FATAL"}

# Physical types of ROWS columns.
P_BOOL, P_INT16, P_INT32, P_INT64, P_FLOAT32, P_FLOAT64, P_UTF8, P_BINARY = range(1, 9)
ARROW_TYPES = {P_BOOL: pa.bool_(), P_INT16: pa.int16(), P_INT32: pa.int32(), P_INT64: pa.int64(),
               P_FLOAT32: pa.float32(), P_FLOAT64: pa.float64(), P_UTF8: pa.string(), P_BINARY: pa.binary()}
WIDTHS = {P_INT16: 2, P_INT32: 4, P_INT64: 8, P_FLOAT32: 4, P_FLOAT64: 8}


class EngineBroken(Exception):
    """The frame stream is broken (bad magic, gap, length, checksum, payload, truncation). Nothing after it counts."""


# ------------------------------------------------------------------------------------------------- frame objects

@dataclass(frozen=True)
class Hello:
    version: int
    debezium_version: str
    slot: str
    start_position: Optional[int]
    jvm_pid: int
    engine_id: str
    active_pid: Optional[int]           # the backend that held the slot when the first batch arrived
    backend_start_us: Optional[int]     # that backend's start, microseconds since 1970 (UTC)
    application_name: str               # that backend's application_name (Debezium: "Debezium Streaming")


@dataclass(frozen=True)
class Column:
    position: int
    name: str
    connect_type: str
    semantic_name: str
    form: str                           # transport form, docs/design/engine.md "Value transport"
    physical: int
    source_type: str
    length: int                         # -1: none
    scale: int                          # -1: none
    nullable: bool
    key: bool
    unchanged: bool                     # values of this column can arrive as "unchanged" (column u<i> exists)


@dataclass(frozen=True)
class Schema:
    schema: str
    table: str
    version: int
    has_key: bool
    key_positions: tuple
    columns: tuple


@dataclass(frozen=True)
class Rows:
    schema: str
    table: str
    version: int
    batch: pa.RecordBatch = field(compare=False)

    def __eq__(self, other):
        return (isinstance(other, Rows) and (self.schema, self.table, self.version)
                == (other.schema, other.table, other.version) and self.batch.equals(other.batch))

    __hash__ = None


@dataclass(frozen=True)
class Begin:
    seq: int
    txid: str
    commit_time_ms: int
    positions: dict
    ack_position: Optional[int]


@dataclass(frozen=True)
class End:
    seq: int
    txid: str
    commit_time_ms: int
    positions: dict
    ack_position: Optional[int]
    event_count: int
    collections: tuple                  # ((data_collection, event_count), ...)


@dataclass(frozen=True)
class Truncate:
    seq: int
    schema: str
    table: str
    positions: dict


@dataclass(frozen=True)
class Message:
    seq: int
    prefix: str
    transactional: bool
    content: bytes
    positions: dict


@dataclass(frozen=True)
class Heartbeat:
    """Information only: the engine is alive. Never acknowledged, carries no ack_position (plan C2)."""
    seq: int
    positions: dict


@dataclass(frozen=True)
class BatchEnd:
    records: int
    acked: Optional[int]                # newest ack_position handed to Debezium in this engine's life


@dataclass(frozen=True)
class Fatal:
    error_class: str
    message: str                        # only text that cannot quote a source value (docs/design/engine.md §7)
    causes: tuple                       # ((class, message), ...)
    positions: dict = field(default_factory=dict)   # position fields of the last record handed over
    schema: str = ""                    # the table of the record being handled, when known
    table: str = ""


Frame = Any


# ------------------------------------------------------------------------------------------------ payload parser

class _Cursor:
    __slots__ = ("mv", "pos", "end")

    def __init__(self, mv):
        self.mv, self.pos, self.end = mv, 0, len(mv)

    def take(self, n):
        if n < 0 or self.pos + n > self.end:
            raise EngineBroken("payload too short")
        p = self.pos
        self.pos += n
        return p

    def u8(self):
        return self.mv[self.take(1)]

    def u16(self):
        return struct.unpack_from("<H", self.mv, self.take(2))[0]

    def u32(self):
        return struct.unpack_from("<I", self.mv, self.take(4))[0]

    def i32(self):
        return struct.unpack_from("<i", self.mv, self.take(4))[0]

    def u64(self):
        return struct.unpack_from("<Q", self.mv, self.take(8))[0]

    def i64(self):
        return struct.unpack_from("<q", self.mv, self.take(8))[0]

    def flag(self):
        b = self.u8()
        if b > 1:
            raise EngineBroken("bad boolean byte %d" % b)
        return b == 1

    def str(self):
        n = self.u32()
        p = self.take(n)
        try:
            return bytes(self.mv[p:p + n]).decode("utf-8")
        except UnicodeDecodeError:
            raise EngineBroken("invalid UTF-8 in a string field") from None

    def bin(self):
        n = self.u32()
        p = self.take(n)
        return bytes(self.mv[p:p + n])

    def opt_u64(self):
        return self.u64() if self.flag() else None

    def pad8(self):
        p = (8 - (self.pos & 7)) & 7
        start = self.take(p)
        if any(self.mv[start:start + p]):
            raise EngineBroken("non-zero padding")

    def buffer(self):
        """u32 length, padding, bytes, padding: returns (offset, length) in the payload."""
        n = self.u32()
        self.pad8()
        off = self.take(n)
        self.pad8()
        return off, n

    def posmap(self):
        n = self.u16()
        out = {}
        for _ in range(n):
            k = self.str()
            kind = self.u8()
            if kind == 0:
                v = None
            elif kind == 1:
                v = self.i64()
            elif kind == 2:
                v = self.str()
            elif kind == 3:
                v = self.flag()
            else:
                raise EngineBroken("bad position field kind %d" % kind)
            if k in out:
                raise EngineBroken("duplicate position field")
            out[k] = v
        return out

    def done(self):
        if self.pos != self.end:
            raise EngineBroken("%d unused payload bytes" % (self.end - self.pos))


def _rows(payload, validate):
    c = _Cursor(memoryview(payload))
    schema, table = c.str(), c.str()
    version, nrows, ncols = c.u32(), c.u32(), c.u16()
    if ncols == 0:
        raise EngineBroken("ROWS frame without columns")
    buf = pa.py_buffer(payload)
    bitmap = (nrows + 7) >> 3
    names, arrays = [], []
    for _ in range(ncols):
        name = c.str()
        ptype = c.u8()
        nulls = c.u32()
        if ptype not in ARROW_TYPES:
            raise EngineBroken("unknown physical type %d" % ptype)
        if nulls > nrows:
            raise EngineBroken("null count above row count")
        voff, vlen = c.buffer()
        if vlen not in (0, bitmap) or (vlen == 0 and nulls and nrows):
            raise EngineBroken("bad validity buffer length")
        validity = buf.slice(voff, vlen) if vlen else None
        if ptype in (P_UTF8, P_BINARY):
            ooff, olen = c.buffer()
            doff, dlen = c.buffer()
            if olen != 4 * (nrows + 1):
                raise EngineBroken("bad offsets buffer length")
            first, last = struct.unpack_from("<i", payload, ooff)[0], struct.unpack_from("<i", payload,
                                                                                         ooff + olen - 4)[0]
            if first != 0 or last != dlen:
                raise EngineBroken("offsets do not span the data buffer")
            bufs = [validity, buf.slice(ooff, olen), buf.slice(doff, dlen)]
        else:
            doff, dlen = c.buffer()
            want = bitmap if ptype == P_BOOL else WIDTHS[ptype] * nrows
            if dlen != want:
                raise EngineBroken("bad data buffer length")
            bufs = [validity, buf.slice(doff, dlen)]
        try:
            arr = pa.Array.from_buffers(ARROW_TYPES[ptype], nrows, bufs, null_count=nulls if vlen else 0)
            if validate:
                arr.validate(full=True)
        except (pa.ArrowException, ValueError) as e:
            raise EngineBroken("invalid column buffers: %s" % e) from None
        names.append(name)
        arrays.append(arr)
    c.done()
    if len(set(names)) != len(names):
        raise EngineBroken("duplicate column name")
    return Rows(schema, table, version, pa.RecordBatch.from_arrays(arrays, names=names))


def parse_payload(ftype, payload, validate=True):
    """Parse one frame payload (bytes or bytearray) into a frame object; raises EngineBroken."""
    if ftype == ROWS:
        return _rows(payload, validate)
    c = _Cursor(memoryview(payload))
    if ftype == HELLO:
        f = Hello(c.u32(), c.str(), c.str(), c.opt_u64(), c.u32(), c.str(), c.opt_u64(), c.opt_u64(), c.str())
    elif ftype == SCHEMA:
        schema, table, version, has_key = c.str(), c.str(), c.u32(), c.flag()
        keys = tuple(c.u16() for _ in range(c.u16()))
        cols = []
        for _ in range(c.u16()):
            cols.append(Column(c.u16(), c.str(), c.str(), c.str(), c.str(), c.u8(), c.str(), c.i32(), c.i32(),
                               c.flag(), c.flag(), c.flag()))
        f = Schema(schema, table, version, has_key, keys, tuple(cols))
    elif ftype == BEGIN:
        f = Begin(c.u64(), c.str(), c.i64(), c.posmap(), c.opt_u64())
    elif ftype == END:
        seq, txid, ts, pos, ack, count = c.u64(), c.str(), c.i64(), c.posmap(), c.opt_u64(), c.u64()
        cols = tuple((c.str(), c.u64()) for _ in range(c.u32()))
        f = End(seq, txid, ts, pos, ack, count, cols)
    elif ftype == TRUNCATE:
        f = Truncate(c.u64(), c.str(), c.str(), c.posmap())
    elif ftype == MESSAGE:
        f = Message(c.u64(), c.str(), c.flag(), c.bin(), c.posmap())
    elif ftype == HEARTBEAT:
        f = Heartbeat(c.u64(), c.posmap())          # an ack_position would be unused bytes: EngineBroken
    elif ftype == BATCH_END:
        f = BatchEnd(c.u32(), c.opt_u64())
    elif ftype == FATAL:
        cls, msg = c.str(), c.str()
        causes = tuple((c.str(), c.str()) for _ in range(c.u16()))
        f = Fatal(cls, msg, causes, c.posmap(), c.str(), c.str())
    else:
        raise EngineBroken("unknown frame type %d" % ftype)
    c.done()
    return f


# --------------------------------------------------------------------------------------------- incremental parser

class FrameParser:
    """Bytes in (`feed`), frames out. Tolerates any chunking. After an error it stays broken."""

    def __init__(self, max_frame_bytes=DEFAULT_MAX_FRAME_BYTES, validate=True):
        self.max_frame_bytes = max_frame_bytes
        self.validate = validate
        self.next_seq = 0
        self.broken = None              # the first error, once broken
        self._hdr = bytearray()
        self._ftype = None
        self._len = 0
        self._payload = None
        self._filled = 0
        self._crc_buf = bytearray()
        self.bytes_in = 0

    def _fail(self, why):
        self.broken = EngineBroken("%s (frame %d, stream byte %d)" % (why, self.next_seq, self.bytes_in))
        raise self.broken

    def feed(self, data):
        """Consume `data`; return the frames completed by it."""
        if self.broken is not None:
            raise EngineBroken("stream already broken: %s" % self.broken)
        out = []
        mv = memoryview(data)
        i, n = 0, len(mv)
        while i < n:
            if self._payload is None and self._ftype is None:
                take = min(HEADER_SIZE - len(self._hdr), n - i)
                self._hdr += mv[i:i + take]
                i += take
                self.bytes_in += take
                if len(self._hdr) < HEADER_SIZE:
                    break
                magic, ftype, reserved, seq, length = HEADER.unpack(self._hdr)
                if magic != MAGIC:
                    self._fail("bad magic")
                if reserved != b"\0\0\0" or ftype not in TYPE_NAMES:
                    self._fail("bad frame type or reserved bytes")
                if seq != self.next_seq:
                    self._fail("sequence gap: got %d" % seq)
                if length > self.max_frame_bytes:
                    self._fail("frame length %d above the limit %d" % (length, self.max_frame_bytes))
                self._ftype, self._len = ftype, length
                try:
                    self._payload = bytearray(length)
                except MemoryError:
                    self._fail("cannot allocate a frame of %d bytes" % length)
                self._filled = 0
                self._crc_buf = bytearray()
                continue
            if self._filled < self._len:
                take = min(self._len - self._filled, n - i)
                self._payload[self._filled:self._filled + take] = mv[i:i + take]
                self._filled += take
                i += take
                self.bytes_in += take
                continue
            take = min(4 - len(self._crc_buf), n - i)
            self._crc_buf += mv[i:i + take]
            i += take
            self.bytes_in += take
            if len(self._crc_buf) < 4:
                break
            want = zlib.crc32(self._payload, zlib.crc32(self._hdr)) & 0xFFFFFFFF
            if CRC.unpack(self._crc_buf)[0] != want:
                self._fail("bad checksum")
            payload, ftype = self._payload, self._ftype
            self._hdr, self._ftype, self._payload, self._crc_buf = bytearray(), None, None, bytearray()
            try:
                frame = parse_payload(ftype, payload, self.validate)
            except EngineBroken as e:
                self._fail("bad %s payload: %s" % (TYPE_NAMES[ftype], e))
            self.next_seq += 1
            out.append(frame)
        return out

    @property
    def at_boundary(self):
        return not self._hdr and self._ftype is None

    def eof(self):
        """End of input: raises EngineBroken unless it falls on a frame boundary."""
        if self.broken is not None:
            raise EngineBroken("stream already broken: %s" % self.broken)
        if not self.at_boundary:
            self._fail("stream ends inside a frame")


def decode_all(data, max_frame_bytes=DEFAULT_MAX_FRAME_BYTES, chunk=None, validate=True):
    """Decode a complete stream; returns the list of frames or raises EngineBroken (also for a truncated end)."""
    p = FrameParser(max_frame_bytes, validate)
    out = []
    step = chunk or max(1, len(data))
    for k in range(0, len(data), step):
        out.extend(p.feed(data[k:k + step]))
    p.eof()
    return out


class FrameReader:
    """Frames from a readable file descriptor, with deadlines. `read(deadline)` returns the next frame, `None` at a
    clean end of the stream, raises `DeadlineExceeded` when the deadline passes first, and `EngineBroken` on any
    error (also a stream that ends inside a frame)."""

    def __init__(self, fd, max_frame_bytes=DEFAULT_MAX_FRAME_BYTES, read_size=1 << 20, validate=True):
        self.fd = fd
        self.parser = FrameParser(max_frame_bytes, validate)
        self.read_size = read_size
        self.pending = []
        self.ended = False
        self.closed = False
        os.set_blocking(fd, False)
        self._poll = select.poll() if hasattr(select, "poll") else None
        if self._poll is not None:
            self._poll.register(fd, select.POLLIN | select.POLLHUP | select.POLLERR)

    def _wait(self, seconds):
        if self._poll is not None:
            return bool(self._poll.poll(max(0, int(seconds * 1000))))
        r, _, _ = select.select([self.fd], [], [], seconds)
        return bool(r)

    def close(self):
        """Close the file descriptor; every later read raises EngineBroken (never reads a reused descriptor)."""
        if not self.closed:
            self.closed = True
            try:
                os.close(self.fd)
            except OSError:
                pass

    def read(self, deadline: Deadline):
        while not self.pending:
            if self.ended:
                return None
            if self.closed:
                raise EngineBroken("the frame stream is closed")
            if self.parser.broken is not None:
                raise EngineBroken("stream already broken: %s" % self.parser.broken)
            try:
                chunk = os.read(self.fd, self.read_size)
            except BlockingIOError:
                if self.closed:
                    raise EngineBroken("the frame stream is closed") from None
                left = deadline.remaining()
                if left <= 0:
                    raise DeadlineExceeded("waiting for the next engine frame")
                self._wait(min(left, 1.0))
                continue
            except OSError as e:
                raise EngineBroken("reading the frame stream failed: %s" % e) from None
            if not chunk:
                self.ended = True
                self.parser.eof()
                return None
            self.pending.extend(self.parser.feed(chunk))
        return self.pending.pop(0)
