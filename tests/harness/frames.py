"""A minimal framed stream for the byte-stream relay tests (not the engine's real frame format).

frame = "CDCF" | seq u32 | len u32 | payload | crc32(header + payload) u32; the last frame has payload "END".
The receiver applies a frame only after it has validated it, in sequence; at the first error it stops. So a fault
is "detected and contained" when the receiver reports an error and has applied only a prefix of the true frames.
"""
import random
import struct
import zlib

MAGIC = b"CDCF"
MAX_LEN = 1 << 16
END = b"END"


def frame(seq, payload):
    hdr = MAGIC + struct.pack(">II", seq, len(payload))
    return hdr + payload + struct.pack(">I", zlib.crc32(hdr + payload))


def make_stream(n, seed):
    rng = random.Random(seed)
    payloads = [bytes(rng.getrandbits(8) for _ in range(rng.randint(0, 200))) for _ in range(n)]
    data = b"".join(frame(i, p) for i, p in enumerate(payloads)) + frame(n, END)
    return data, payloads


def receive(data):
    """Returns (applied payloads, error or None)."""
    applied, pos, seq = [], 0, 0
    while True:
        if pos == len(data):
            return applied, "stream ended without END frame"
        if len(data) - pos < 12:
            return applied, "truncated header at %d" % pos
        if data[pos:pos + 4] != MAGIC:
            return applied, "bad magic at %d" % pos
        s, n = struct.unpack(">II", data[pos + 4:pos + 12])
        if n > MAX_LEN:
            return applied, "frame too long at %d" % pos
        if len(data) - pos < 12 + n + 4:
            return applied, "truncated frame at %d" % pos
        body = data[pos:pos + 12 + n]
        (crc,) = struct.unpack(">I", data[pos + 12 + n:pos + 16 + n])
        if zlib.crc32(body) != crc:
            return applied, "bad checksum at %d" % pos
        if s != seq:
            return applied, "sequence %d, expected %d" % (s, seq)
        payload = data[pos + 12:pos + 12 + n]
        pos += 16 + n
        if payload == END and s == len(applied):
            if pos != len(data):
                return applied, "bytes after END"
            return applied, None
        applied.append(payload)
        seq += 1
