"""The frame reader allocates a payload as its bytes arrive, never ahead by the length field (plan C2, SPEC 5.4):
a header whose length says 1 GiB, then 1 KiB and the end of the stream, gives the typed error, and the reader never
held more than the bytes it read (the buffer is measured, not the process). Hermetic."""
import pytest

from cdc_v2.stream import frames as F


def _header(length, seq=0, ftype=F.ROWS):
    return F.HEADER.pack(F.MAGIC, ftype, b"\0\0\0", seq, length)


def test_a_huge_length_reserves_nothing_and_ends_in_the_typed_error():
    p = F.FrameParser()                                    # default maximum: 1 GiB + 16 MiB
    assert p.feed(_header(1 << 30)) == []
    held = []
    for _ in range(16):                                    # 1 KiB in 64-byte pieces
        assert p.feed(b"\x01" * 64) == []
        held.append(len(p._payload))
    assert held[-1] == 1024 and max(held) == 1024
    assert len(p._payload) <= 1024 + 64                    # never more than what arrived
    with pytest.raises(F.EngineBroken, match="inside a frame"):
        p.eof()


def test_frame_reader_on_a_pipe_with_a_huge_length(tmp_path):
    import os
    from cdc_v2.deadline import Deadline
    r, w = os.pipe()
    os.write(w, _header(1 << 30) + b"\x02" * 1024)
    os.close(w)
    reader = F.FrameReader(r)
    try:
        with pytest.raises(F.EngineBroken):
            reader.read(Deadline.after(5))
        assert len(reader.parser._payload or b"") <= 1024
    finally:
        reader.close()
