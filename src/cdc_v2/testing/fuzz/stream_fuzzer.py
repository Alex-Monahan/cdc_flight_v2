"""The stream fuzzer: faults between the engine process and the applier (contract C2 frames).

Property: what the reader hands on is always a prefix of the true sequence of whole transactions, followed by
either the true continuation or a detected error. Nothing partial, corrupt or repeated is ever applied, and no
position is confirmed beyond what was delivered whole.

Two layers are attacked:

* the frame layer: package E's decoder (`cdc_v2.stream.frames.FrameParser` / `FrameReader`), with byte faults
  (`pipefaults`: flips, duplicated ranges, cuts, truncation; plus targeted flips in lengths, sequence numbers,
  positions and counts, raw frame swaps and repeated frame runs) at random offsets and under random chunking;
* the transaction layer: well-formed frames with valid sequence numbers and checksums that are wrong in meaning
  (an END whose count does not match, a frame of another transaction inserted, an END or BEGIN missing, frames
  swapped, a falling position, a whole transaction delivered again, a HEARTBEAT that carries a position ahead of
  the last END). The frame format cannot see these; the assembler must. Package S's assembler does not exist yet,
  so `assemble` below is the reference that round 2 replaces with S's; its negative control `assemble_trusting`
  must be caught.

A restart of the engine is a new life: its frames start again at sequence 0 with a HELLO, and the transactions it
re-sends are discarded by position (`apply_lives`).

What the format cannot detect (findings for the contract, `UNDETECTABLE`): a clean end of the stream exactly at a
frame boundary (only the process exit status tells it from a STOP); a whole new life (sequence 0) that repeats
transactions (by design; discarded by position); and wrong content that was encoded upstream with a correct
checksum (for example the rows of a ROWS frame replaced by as many other rows with the same record numbers).
A corrupted length field is detected only after `length` further bytes have arrived (the header has no checksum of
its own), so detection waits for later frames, and the reader allocates up to `max_frame_bytes` first.
"""
from __future__ import annotations

import hashlib
import json
import os
import random
import struct
import threading
import time
from collections import Counter
from pathlib import Path
from typing import Optional

from cdc_v2.deadline import Deadline, DeadlineExceeded
from cdc_v2.stream import frames as F
from cdc_v2.testing import frames as FE
from cdc_v2.testing import pipefaults

CORPUS_DIR = Path(__file__).resolve().parents[4] / "tests" / "fuzz" / "corpus"
# faults after which a clean end at a frame boundary is the true outcome (the stream really is shorter)
SHORTENING = ("pipefault", "cut_at_boundary", "raw_drop_frame", "drop_end", "flip_header_length", "huge_length")
UNDETECTABLE = ("clean_end_at_boundary", "new_life_replay", "content_replaced_same_shape")
BYTE_KINDS = ("pipefault", "huge_length", "flip_header_length", "flip_header_seq", "flip_header_type", "flip_payload",
              "raw_swap", "raw_dup_run", "raw_drop_frame", "cut_at_boundary")
SEMANTIC_KINDS = ("end_count", "foreign_rows", "drop_end", "drop_begin", "swap_records", "ack_regress",
                  "dup_txn_same_life", "heartbeat_ahead", "content_replaced_same_shape", "new_life_replay")


class Broken(Exception):
    """The transaction layer found the stream wrong; nothing after the last whole transaction counts."""


# ------------------------------------------------------------------------------------------------- assembly

def _records(f):
    if isinstance(f, F.Rows):
        return [("row", f.schema, f.table, r) for r in f.batch.to_pylist()]
    if isinstance(f, F.Truncate):
        return [("truncate", f.schema, f.table, {"seq": f.seq})]
    if isinstance(f, F.Message):
        return [("message", f.prefix, f.transactional, {"seq": f.seq, "content": f.content.hex()})]
    return []


def _digest(records) -> str:
    return hashlib.sha256(json.dumps(records, sort_keys=True, default=repr).encode()).hexdigest()[:16]


class Assembler:
    """Reference transaction assembler (strict). `feed(frame)` returns the transactions it completes; raises Broken.

    Checks: one HELLO first; record `seq` strictly rising over every record of the life (BEGIN, rows, TRUNCATE,
    MESSAGE, END, HEARTBEAT); BEGIN and END paired; END.event_count = rows + TRUNCATE + transactional MESSAGE
    frames; END.ack_position present and rising; a HEARTBEAT never moves the position; FATAL ends the stream."""

    def __init__(self):
        self.hello = False
        self.cur = None
        self.last_seq = -1
        self.last_ack = -1
        self.position = None

    def _seq(self, s):
        if s <= self.last_seq:
            raise Broken("record seq %d after %d" % (s, self.last_seq))
        self.last_seq = s

    def feed(self, f) -> list:
        if isinstance(f, F.Hello):
            if self.hello:
                raise Broken("second HELLO")
            self.hello = True
            return []
        if not self.hello:
            raise Broken("no HELLO first")
        if isinstance(f, (F.Schema, F.BatchEnd)):
            return []
        if isinstance(f, F.Fatal):
            raise Broken("FATAL")
        if isinstance(f, F.Heartbeat):
            self._seq(f.seq)
            return []
        if isinstance(f, F.Begin):
            if self.cur is not None:
                raise Broken("BEGIN inside a transaction")
            self._seq(f.seq)
            self.cur = {"begin": f, "records": [], "count": 0}
            return []
        if isinstance(f, F.Message) and not f.transactional:
            self._seq(f.seq)
            return []
        if isinstance(f, (F.Rows, F.Truncate, F.Message)):
            if self.cur is None:
                raise Broken("record outside a transaction")
            for rec in _records(f):
                self._seq(rec[3]["seq"])
                self.cur["records"].append(rec)
                self.cur["count"] += 1
            return []
        if isinstance(f, F.End):
            if self.cur is None:
                raise Broken("END without BEGIN")
            self._seq(f.seq)
            if f.event_count != self.cur["count"]:
                raise Broken("END count %d, %d records" % (f.event_count, self.cur["count"]))
            if f.ack_position is None or f.ack_position <= self.last_ack:
                raise Broken("END position missing or not rising")
            self.last_ack = f.ack_position
            txn = {"ack": f.ack_position, "n": self.cur["count"], "digest": _digest(self.cur["records"])}
            self.cur = None
            self.position = f.ack_position
            return [txn]
        raise Broken("unknown frame %r" % type(f).__name__)


class TrustingAssembler(Assembler):
    """Negative control: no checks, and it adopts a HEARTBEAT's position as the applied position."""

    def _seq(self, s):
        self.last_seq = s

    def feed(self, f):
        if isinstance(f, F.Heartbeat):
            pos = f.positions.get("lsn")
            if isinstance(pos, int) and (self.position is None or pos > self.position):
                self.position = pos
            return []
        if isinstance(f, F.End):
            if self.cur is None:
                return []
            txn = {"ack": f.ack_position, "n": self.cur["count"], "digest": _digest(self.cur["records"])}
            self.cur = None
            self.position = f.ack_position
            return [txn]
        if isinstance(f, F.Hello):
            return []
        if isinstance(f, (F.Rows, F.Truncate, F.Message)) and self.cur is None:
            return []
        return super().feed(f) if not isinstance(f, F.Fatal) else []


def truth(frames: list) -> list:
    a = Assembler()
    out = []
    for f in frames:
        out.extend(a.feed(f))
    return out


# ------------------------------------------------------------------------------------------------- streams

def synth_stream(seed: int, txns: int = 12) -> list:
    """A consistent frame stream from a seed (for tests without the recorded corpus): HELLO, SCHEMA, transactions
    with rows of two tables, TRUNCATE, transactional and plain MESSAGE frames, HEARTBEAT frames anywhere (also
    inside a transaction, with positions ahead), BATCH_END."""
    r = random.Random(seed)
    out = [F.Hello(1, "3.7.1.Final", "cdc_v2_slot", None, 4242, "e1", 777, 1_700_000_000_000_000,
                   "Debezium Streaming")]
    cols = (F.Column(0, "id", "INT64", "", "int64", F.P_INT64, "bigint", -1, -1, False, True, False),
            F.Column(1, "v", "STRING", "", "text", F.P_UTF8, "text", -1, -1, True, False, True))
    out.append(F.Schema("public", "t", 1, True, (0,), cols))
    seq, pos = 0, 0x3000000 + r.randrange(1 << 20)
    for i in range(txns):
        pos += r.randint(40, 4000)
        commit = pos + r.randint(10, 400)
        p = {"lsn": pos, "txId": 700 + i, "lsn_commit": commit}
        out.append(F.Begin(seq, "%d:%d" % (700 + i, pos), 1_700_000_000_000 + i, p, commit))
        seq += 1
        n = 0
        for _ in range(r.randint(0, 3)):
            k = r.randint(1, 5)
            rows = {"seq": [seq + j for j in range(k)], "op": [r.choice("cud") for _ in range(k)],
                    "a0": [r.randrange(1 << 40) for _ in range(k)],
                    "a1": [None if r.random() < 0.2 else "v%d" % r.randrange(1000) for _ in range(k)],
                    "u1": [r.random() < 0.1 for _ in range(k)],
                    "k0": [None] * k}
            out.append(FE.rows("public", "t", 1, {"seq": (F.P_INT64, rows["seq"]), "op": (F.P_UTF8, rows["op"]),
                                                  "a0": (F.P_INT64, rows["a0"]), "a1": (F.P_UTF8, rows["a1"]),
                                                  "u1": (F.P_BOOL, rows["u1"]), "k0": (F.P_INT64, rows["k0"])}))
            seq += k
            n += k
            if r.random() < 0.2:
                out.append(F.Heartbeat(seq, {"lsn": commit + 1000}))
                seq += 1
        if r.random() < 0.2:
            out.append(F.Truncate(seq, "public", "t", p))
            seq += 1
            n += 1
        if r.random() < 0.2:
            out.append(F.Message(seq, "cdc_v2_m", True, b"m%d" % i, p))
            seq += 1
            n += 1
        out.append(F.End(seq, "%d:%d" % (700 + i, commit), 1_700_000_000_000 + i, p, commit, n,
                         (("public.t", n),) if n else ()))
        seq += 1
        if r.random() < 0.3:
            out.append(F.Heartbeat(seq, {"lsn": commit}))
            seq += 1
        if r.random() < 0.2:
            out.append(F.Message(seq, "cdc_v2_n", False, b"loose", {"lsn": commit}))
            seq += 1
        if r.random() < 0.4:
            out.append(F.BatchEnd(r.randint(1, 50), commit))
    return out


def load_corpus(directory: Path = CORPUS_DIR) -> list:
    """[(name, bytes)] of the recorded frame streams (generated data only)."""
    out = []
    if directory.exists():
        for p in sorted(directory.glob("*.frames")):
            out.append((p.stem, p.read_bytes()))
    return out


def frame_spans(data: bytes) -> list:
    """(start, end) of every frame of a valid stream."""
    spans, pos = [], 0
    while pos < len(data):
        _m, _t, _r, _s, length = F.HEADER.unpack_from(data, pos)
        end = pos + F.HEADER_SIZE + length + 4
        spans.append((pos, end))
        pos = end
    return spans


# ------------------------------------------------------------------------------------------------- mutations

def _renumber(frames: list) -> bytes:
    return FE.Encoder().encode_all(frames)


def mutate(kind: str, frames: list, data: bytes, r: random.Random) -> tuple:
    """(mutated bytes or list of lives, details). Byte kinds change bytes; semantic kinds re-encode frames so that
    sequence numbers and checksums are valid."""
    spans = frame_spans(data)
    if kind == "pipefault":
        plan = pipefaults.make_plan(r.getrandbits(32), len(data), kinds=("flip", "dup", "cut", "truncate"))
        out, log = pipefaults.apply_bytes(data, plan)
        return out, {"plan": [e for e in log if e.get("fired")]}
    if kind == "huge_length":
        # a length field that claims up to the decoder's maximum, then the stream ends: the reader must not
        # reserve that memory and must end with a typed error (read with the default maximum, below)
        s, e = spans[r.randrange(len(spans))]
        b = bytearray(data[:e])
        struct.pack_into("<I", b, s + 16, r.randint(1 << 24, F.DEFAULT_MAX_FRAME_BYTES))
        return bytes(b), {"frame_start": s}
    if kind.startswith("flip_"):
        s, e = spans[r.randrange(len(spans))]
        if kind == "flip_header_length":
            off = s + 16 + r.randrange(4)
        elif kind == "flip_header_seq":
            off = s + 8 + r.randrange(8)
        elif kind == "flip_header_type":
            off = s + 4
        else:
            off = s + F.HEADER_SIZE + r.randrange(max(1, e - s - F.HEADER_SIZE - 4)) if e - s > F.HEADER_SIZE + 4 \
                else s + r.randrange(e - s)
        b = bytearray(data)
        b[off] ^= 1 << r.randrange(8)
        return bytes(b), {"offset": off}
    if kind == "raw_swap" and len(spans) >= 3:
        i = r.randrange(1, len(spans) - 1)
        (a0, a1), (b0, b1) = spans[i], spans[i + 1]
        return data[:a0] + data[b0:b1] + data[a0:a1] + data[b1:], {"frames": [i, i + 1]}
    if kind == "raw_dup_run" and len(spans) >= 2:
        i = r.randrange(1, len(spans))
        j = min(len(spans), i + r.randint(1, 4))
        a0, a1 = spans[i][0], spans[j - 1][1]
        return data[:a1] + data[a0:a1] + data[a1:], {"frames": [i, j]}
    if kind == "raw_drop_frame" and len(spans) >= 2:
        i = r.randrange(1, len(spans))
        a0, a1 = spans[i]
        return data[:a0] + data[a1:], {"frame": i}
    if kind == "cut_at_boundary":
        i = r.randrange(1, len(spans))
        return data[:spans[i][0]], {"frames_kept": i}
    fr = list(frames)
    ends = [i for i, f in enumerate(fr) if isinstance(f, F.End)]
    begins = [i for i, f in enumerate(fr) if isinstance(f, F.Begin)]
    rows = [i for i, f in enumerate(fr) if isinstance(f, F.Rows)]
    if kind == "end_count" and ends:
        i = r.choice(ends)
        e = fr[i]
        fr[i] = F.End(e.seq, e.txid, e.commit_time_ms, e.positions, e.ack_position,
                      max(0, e.event_count + r.choice([-1, 1])), e.collections)
        return _renumber(fr), {"frame": i}
    if kind == "foreign_rows" and rows and begins:
        src = r.choice(rows)
        tgt = r.choice(begins) + 1
        fr.insert(tgt, fr[src])
        return _renumber(fr), {"copied": src, "into": tgt}
    if kind == "drop_end" and ends:
        i = r.choice(ends)
        del fr[i]
        return _renumber(fr), {"dropped": i}
    if kind == "drop_begin" and begins:
        i = r.choice(begins)
        del fr[i]
        return _renumber(fr), {"dropped": i}
    if kind == "swap_records":
        rec = [i for i, f in enumerate(fr) if isinstance(f, (F.Rows, F.Truncate, F.Message, F.End, F.Begin))]
        if len(rec) >= 2:
            k = r.randrange(len(rec) - 1)
            a, b = rec[k], rec[k + 1]
            fr[a], fr[b] = fr[b], fr[a]
            return _renumber(fr), {"swapped": [a, b]}
    if kind == "ack_regress" and len(ends) >= 2:
        i = r.choice(ends[1:])
        e = fr[i]
        prev = fr[ends[ends.index(i) - 1]].ack_position
        fr[i] = F.End(e.seq, e.txid, e.commit_time_ms, e.positions, prev - r.randint(0, 5), e.event_count,
                      e.collections)
        return _renumber(fr), {"frame": i}
    if kind == "dup_txn_same_life" and begins:
        b = r.choice(begins)
        e = next((i for i in ends if i > b), None)
        if e is not None:
            fr[e + 1:e + 1] = fr[b:e + 1]
            return _renumber(fr), {"txn": [b, e]}
    if kind == "heartbeat_ahead" and ends:
        last = max(fr[i].ack_position for i in ends)
        i = r.randrange(2, len(fr) + 1)
        prev_seq = max([_max_seq(f) for f in fr[:i]] + [-1])
        nxt = min([_min_seq(f) for f in fr[i:] if _min_seq(f) is not None] + [prev_seq + 2])
        if nxt - prev_seq >= 2:
            fr.insert(i, F.Heartbeat(prev_seq + 1, {"lsn": last + r.randint(1, 1 << 20)}))
            return _renumber(fr), {"at": i, "benign": True}
        return _renumber(fr), {"at": None, "benign": True, "no_gap": True}
    if kind == "content_replaced_same_shape" and rows:
        i = r.choice(rows)
        b = fr[i].batch
        d = b.to_pydict()
        for name in d:
            if name.startswith("a") and d[name] and isinstance(d[name][0], (int, str)) and name != "a0":
                d[name] = [("x" + v) if isinstance(v, str) else v for v in d[name]]
        import pyarrow as pa
        fr[i] = F.Rows(fr[i].schema, fr[i].table, fr[i].version,
                       pa.RecordBatch.from_pydict(d, schema=b.schema))
        return _renumber(fr), {"frame": i, "undetectable": True}
    if kind == "new_life_replay" and begins:
        k = r.randrange(len(begins))
        cut = begins[k]
        life1 = _renumber(fr)
        life2 = _renumber([fr[0]] + [f for f in fr[1:] if isinstance(f, F.Schema)] + fr[cut:])
        return [life1, life2], {"replayed_from_txn": k}
    return data, {"no_change": True}


def _seqs(f):
    if isinstance(f, (F.Begin, F.End, F.Heartbeat, F.Truncate, F.Message)):
        return [f.seq]
    if isinstance(f, F.Rows):
        return list(f.batch.column("seq").to_pylist()) if "seq" in f.batch.schema.names else []
    return []


def _max_seq(f):
    s = _seqs(f)
    return max(s) if s else -1


def _min_seq(f):
    s = _seqs(f)
    return min(s) if s else None


# ------------------------------------------------------------------------------------------------- one case

def read_life(data: bytes, r: random.Random, assembler_cls=Assembler, decoder="strict",
              max_frame_bytes: int = 1 << 20) -> dict:
    """Feed one life's bytes in random chunks: whole transactions handed on, and how the life ended."""
    out = {"txns": [], "end": None, "position": None}
    if decoder == "lenient":
        import sys
        here = str(CORPUS_DIR.parent / "toy")
        if here not in sys.path:
            sys.path.insert(0, here)
        import toy
        frames = toy.lenient_frames(data)
        a = assembler_cls()
        try:
            for f in frames:
                out["txns"].extend(a.feed(f))
            out["end"] = "clean"
        except Broken as e:
            out["end"] = "assembler: %s" % e
        out["position"] = a.position
        return out
    p = F.FrameParser(max_frame_bytes=max_frame_bytes)
    a = assembler_cls()
    i = 0
    try:
        while i < len(data):
            n = r.choice([1, 7, 64, 4096, 1 << 20])
            for f in p.feed(data[i:i + n]):
                out["txns"].extend(a.feed(f))
            i += n
        p.eof()
        out["end"] = "clean"
    except F.EngineBroken as e:
        out["end"] = "frame: %s" % str(e)[:80]
    except Broken as e:
        out["end"] = "assembler: %s" % e
    out["position"] = a.position
    return out


def apply_lives(lives: list, r: random.Random, assembler_cls=Assembler, decoder="strict",
                max_frame_bytes: int = 1 << 20) -> dict:
    """Each life gets a new reader (an engine restart); a transaction at or below the applied position is
    discarded as a whole."""
    applied, position, ends = [], None, []
    for data in lives:
        res = read_life(data, r, assembler_cls, decoder, max_frame_bytes)
        ends.append(res["end"])
        for t in res["txns"]:
            if position is None or t["ack"] > position:
                applied.append(t)
                position = t["ack"]
        if res["position"] is not None and (position is None or res["position"] > position):
            position = res["position"]
        if res["end"] != "clean":
            break
    return {"txns": applied, "ends": ends, "position": position}


def check_case(true_txns: list, result: dict, kind: str, details: dict) -> dict:
    """The property. Returns {ok, why, detected, undetected_class}."""
    got = result["txns"]
    prefix = got == true_txns[:len(got)]
    complete = got == true_txns
    last_true = true_txns[len(got) - 1]["ack"] if got else None
    pos_ok = result["position"] is None or (got and result["position"] == got[-1]["ack"]) or \
        (not got and result["position"] is None)
    ended_clean = all(e == "clean" for e in result["ends"])
    detected = not ended_clean
    out = {"kind": kind, "prefix": prefix, "complete": complete, "detected": detected, "ends": result["ends"][:3],
           "handed_on": len(got), "true": len(true_txns), "position_ok": bool(pos_ok)}
    if not prefix:
        out.update(ok=False, why="handed on something that is not a prefix of the true transactions")
    elif not pos_ok:
        out.update(ok=False, why="position beyond the last whole transaction handed on (%s vs %s)"
                   % (result["position"], last_true))
    elif complete or detected:
        out.update(ok=True, why="complete" if complete else "prefix, then a detected error")
    elif kind in SHORTENING:
        out.update(ok=True, why="prefix, then a clean end at a frame boundary", undetected="clean_end_at_boundary")
    else:
        out.update(ok=False, why="a clean end short of the true transactions after a fault that does not shorten "
                                 "the stream")
    if kind == "content_replaced_same_shape" and complete is False and prefix is False:
        out.update(ok=True, why="content replaced upstream with a valid checksum: undetectable by design",
                   undetected="content_replaced_same_shape")
    if kind == "heartbeat_ahead" and not complete:
        out.update(ok=False, why="a heartbeat ahead of the last END changed what was handed on")
    return out


def run_case(seed: int, corpus: list, kinds=None, assembler_cls=Assembler, decoder="strict") -> dict:
    r = random.Random(seed)
    name, data = corpus[r.randrange(len(corpus))]
    frames = F.decode_all(data)
    true = truth(frames)
    kind = r.choice(list(kinds or (BYTE_KINDS + SEMANTIC_KINDS)))
    mutated, details = mutate(kind, frames, data, r)
    lives = mutated if isinstance(mutated, list) else [mutated]
    res = apply_lives(lives, r, assembler_cls, decoder,
                      max_frame_bytes=F.DEFAULT_MAX_FRAME_BYTES if kind == "huge_length" else 1 << 20)
    c = check_case(true, res, kind, details)
    c.update(seed=seed, stream=name)
    return c


def run_offline(seeds, corpus: Optional[list] = None, kinds=None, assembler_cls=Assembler, decoder="strict") -> dict:
    """Many cases against the frame layer and the reference assembler. Returns counts and the failing cases."""
    corpus = corpus or load_corpus() or [("synth%d" % s, FE.Encoder().encode_all(synth_stream(s))) for s in range(8)]
    t0 = time.monotonic()
    by_kind, detected, undetected, fails = Counter(), Counter(), Counter(), []
    n = 0
    for seed in seeds:
        c = run_case(seed, corpus, kinds, assembler_cls, decoder)
        n += 1
        by_kind[c["kind"]] += 1
        if c.get("detected"):
            detected[c["kind"]] += 1
        if c.get("undetected"):
            undetected[c["undetected"]] += 1
        if not c["ok"]:
            fails.append(c)
    return {"cases": n, "seconds": round(time.monotonic() - t0, 3), "by_kind": dict(by_kind),
            "detected": dict(detected), "undetected_by_class": dict(undetected), "failures": len(fails),
            "failing_cases": fails[:20], "corpus": [nm for nm, _ in corpus]}


# ------------------------------------------------------------------------------------------------- live pipe

def run_live(seed: int, data: bytes, read_deadline_s: float = 0.15) -> dict:
    """The byte relay of the harness between a writer thread and FrameReader on real pipes, with stalls longer than
    the reader's deadline and a slow trickle. The reader must report a deadline (never a partial frame), then go
    on, and hand on exactly the true frames when nothing else was injected."""
    r = random.Random(seed)
    plan = [{"kind": "stall", "offset": r.randrange(len(data)), "seconds": round(r.uniform(0.3, 0.6), 3), "id": 0}]
    relay = pipefaults.PipeRelay(plan).start()

    def writer():
        try:
            for i in range(0, len(data), 97):
                os.write(relay.write_fd, data[i:i + 97])
                if r.random() < 0.02:
                    time.sleep(0.01)                       # trickle
        finally:
            os.close(relay.write_fd)
    th = threading.Thread(target=writer, daemon=True)
    th.start()
    reader = F.FrameReader(relay.read_fd)
    frames, deadlines = [], 0
    end = time.monotonic() + 30
    while time.monotonic() < end:
        try:
            f = reader.read(Deadline.after(read_deadline_s))
        except DeadlineExceeded:
            deadlines += 1
            continue
        if f is None:
            break
        frames.append(f)
    th.join(10)
    log = relay.join(10)
    reader.close()
    true = F.decode_all(data)
    return {"ok": len(frames) == len(true) and all(a == b for a, b in zip(frames, true)) and deadlines >= 1,
            "frames": len(frames), "true": len(true), "deadlines": deadlines,
            "stalls_fired": sum(1 for e in log if e.get("kind") == "stall" and e.get("fired"))}
