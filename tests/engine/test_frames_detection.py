"""Corruption of the frame stream is detected or harmless; nothing wrong is ever yielded (SPEC §4 stream fuzzer).

For a damaged stream the decoder yields frames until its first error. The test holds when every yielded frame is
the true frame at its place (a prefix of the true stream), whether or not an error was raised; an undetected damage
must leave the output identical. Hermetic; CDC_V2_E_LONG=1 runs the 1,000-seed pipefaults campaign and writes its
evidence (CDC_V2_E_EVIDENCE=<dir>).
"""
import json
import os
import random

import pytest

from cdc_v2.stream import frames as F
from cdc_v2.testing import frames as T
from cdc_v2.testing import pipefaults as pf
from conftest import LONG


def true_stream(seed, count=2):
    frames = T.generate(seed, count)
    return frames, T.Encoder().encode_all(frames)


def decode_prefix(data, chunk=None):
    """(frames yielded before the first error, error or None)."""
    p = F.FrameParser()
    out = []
    step = chunk or max(1, len(data))
    try:
        for k in range(0, len(data), step):
            out.extend(p.feed(data[k:k + step]))
        p.eof()
    except F.EngineBroken as e:
        return out, e
    return out, None


def check(true_frames, damaged, chunk=None):
    got, err = decode_prefix(damaged, chunk)
    want = [T.encode_payload(f) for f in true_frames]
    have = [T.encode_payload(f) for f in got]
    assert have == want[:len(have)], "a wrong frame was yielded"
    if err is None and have != want:
        # Undetected and different: allowed only when the damaged stream is exactly a shorter, clean stream
        # (a cut at a frame boundary). No frame is wrong; the end of the stream is reported by the engine's exit,
        # and a transaction without its END is never applied.
        assert damaged == T.Encoder().encode_all(got), "damage went undetected and changed the output"
    return err is not None


def test_every_single_bit_flip_of_a_small_stream():
    frames = [F.Hello(1, "d", "s", 5, 1, "e", 7, 9, "a"), F.Begin(0, "x", 1, {"lsn": 3}, 3),
              T.rows("s", "t", 1, {"seq": (F.P_INT64, [1, 2]), "op": (F.P_UTF8, ["c", "d"]),
                                   "a0": (F.P_UTF8, ["x", None])}),
              F.End(3, "x", 1, {"lsn": 4}, 4, 2, (("s.t", 2),)), F.BatchEnd(4, None)]
    data = T.Encoder().encode_all(frames)
    detected = 0
    for byte in range(len(data)):
        for bit in range(8):
            bad = bytearray(data)
            bad[byte] ^= 1 << bit
            detected += check(frames, bytes(bad))
    assert detected == 8 * len(data)     # CRC-32 detects every single-bit error


def test_every_truncation_point():
    frames, data = true_stream(11, 1)
    for n in range(len(data)):
        check(frames, data[:n])


def test_duplicated_range_and_dropped_frame():
    frames, data = true_stream(12, 2)
    rng = random.Random(1)
    for _ in range(200):
        a = rng.randrange(len(data))
        b = min(len(data), a + rng.randint(1, 600))
        assert check(frames, data[:b] + data[a:b] + data[b:])
    enc = T.Encoder()
    pieces = [enc.encode(f) for f in frames]
    for i in range(len(pieces) - 1):
        assert check(frames, b"".join(pieces[:i] + pieces[i + 1:]))   # a gap in the sequence
    check(frames, b"".join(pieces[:-1]))                              # the last one: a clean, shorter stream


def run_pipefaults(seeds, count=2):
    detected = undetected_harmless = injected = 0
    for seed in seeds:
        frames, data = true_stream(seed, count)
        plan = pf.make_plan(seed, len(data), kinds=("cut", "dup", "truncate", "flip"))
        out, log = pf.apply_bytes(data, plan, chunk=97)
        fired = [e for e in log if e.get("fired")]
        injected += len(fired)
        if check(frames, out, chunk=113):
            detected += 1
        else:
            undetected_harmless += 1
    return {"seeds": len(list(seeds)), "detected": detected, "undetected_output_identical_or_a_clean_shorter_stream": undetected_harmless,
            "faults_injected": injected}


def test_pipefaults_seeds_default():
    r = run_pipefaults(range(60))
    assert r["detected"] + r["undetected_output_identical_or_a_clean_shorter_stream"] == 60


@pytest.mark.skipif(not LONG, reason="opt-in: CDC_V2_E_LONG=1")
def test_pipefaults_seeds_long():
    r = run_pipefaults(range(1000, 2000), count=3)
    r["command"] = "CDC_V2_E_LONG=1 uv run pytest tests/engine/test_frames_detection.py -k long"
    out = os.environ.get("CDC_V2_E_EVIDENCE")
    if out:
        from cdc_v2.testing.harness_evidence import write_evidence
        write_evidence(os.path.join(out, "frames_detection.json"), r)
    assert r["seeds"] == 1000
    print(json.dumps(r))
