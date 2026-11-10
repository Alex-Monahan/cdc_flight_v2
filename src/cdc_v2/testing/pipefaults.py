"""Byte-stream fault relay for a one-directional stream between two processes (the JVM engine writes change frames
to a pipe, `docs/spike/jvm_on_flight.md` §8), independent of the frame format. SPEC §4 stream fuzzer.

From a seed, `make_plan` chooses faults at random input offsets:

``stall``     stop relaying for ``seconds`` before the byte at ``offset``.
``cut``       end of stream before the byte at ``offset``: the output is closed (and, by default, the input too, so
              the writer sees EPIPE, like a dead reader).
``dup``       after the bytes ``[offset, offset + length)`` have been relayed, relay them once more.
``truncate``  at the end of the input, drop the last ``length`` bytes (the relay holds that many bytes back while it
              runs, so a live stream is delayed by them).
``flip``      XOR the byte at ``offset`` with ``mask`` (one bit).

The relay records what it injected and where (input and output offset), so a test can check that the receiver
detected every fault (corrupted or truncated input is detected and contained, never applied). The output depends only
on the plan and the input bytes, not on how reads are chunked: the same seed and input give the same bytes and log.

    plan = make_plan(seed=7, size=len(stream))
    out, log = apply_bytes(stream, plan)                     # pure, for unit tests (stalls are only recorded)
    relay = PipeRelay(plan).start()                          # thread: relay.write_fd -> relay.read_fd
    python -m cdc_v2.testing.pipefaults --seed 7 --size N --log L < in > out   # as a process
"""
import json
import os
import random
import sys
import threading
import time

KINDS = ("stall", "cut", "dup", "truncate", "flip")
TERMINAL = ("cut", "truncate")


def make_plan(seed, size, kinds=KINDS, events=None, max_stall=0.3, max_dup=512, max_truncate=64):
    """A reproducible fault plan for a stream of about ``size`` bytes. At most one terminal fault (cut or truncate).
    Offsets beyond the real stream length simply never fire (the log says so)."""
    if size < 1:
        raise ValueError("size must be >= 1")
    for k in kinds:
        if k not in KINDS:
            raise ValueError("unknown fault kind %r" % (k,))
    rng = random.Random(seed)
    n = events if events is not None else rng.randint(1, 3)
    plan, terminal = [], False
    for _ in range(n):
        choices = [k for k in kinds if not (terminal and k in TERMINAL)]
        if not choices:
            break
        kind = rng.choice(choices)
        terminal = terminal or kind in TERMINAL
        off = rng.randrange(size)
        if kind == "stall":
            plan.append({"kind": "stall", "offset": off, "seconds": round(rng.uniform(0.01, max_stall), 3)})
        elif kind == "cut":
            plan.append({"kind": "cut", "offset": off})
        elif kind == "dup":
            plan.append({"kind": "dup", "offset": off, "length": rng.randint(1, max(1, min(max_dup, size - off)))})
        elif kind == "truncate":
            plan.append({"kind": "truncate", "length": rng.randint(1, max(1, min(max_truncate, size)))})
        else:
            plan.append({"kind": "flip", "offset": off, "mask": 1 << rng.randrange(8)})
    for i, e in enumerate(plan):
        e["id"] = i
    return plan


def _trigger(e):
    return e["offset"] + e["length"] if e["kind"] == "dup" else e["offset"]


class Mutator:
    """Applies a plan to a byte stream. ``feed`` and ``finish`` return actions: ``("data", bytes)``,
    ``("stall", seconds)`` or ``("cut",)``. ``log`` lists the faults that fired."""

    _ORDER = {"dup": 0, "stall": 1, "cut": 2, "flip": 3}

    def __init__(self, plan):
        self.events = sorted((e for e in plan if e["kind"] != "truncate"),
                             key=lambda e: (_trigger(e), self._ORDER[e["kind"]], e["id"]))
        trunc = [e for e in plan if e["kind"] == "truncate"]
        self.truncate = trunc[0] if trunc else None
        self.pos = 0                    # input bytes consumed
        self.out = 0                    # output bytes released
        self.history = bytearray()      # input bytes, for dup (bounded below)
        self.hist_start = 0
        self.max_dup = max([e["length"] for e in self.events if e["kind"] == "dup"] or [0])
        self.tail = bytearray()
        self.cut = False
        self.log = []

    def _release(self, actions, data):
        if not data:
            return
        if self.truncate is None:
            actions.append(("data", bytes(data)))
            self.out += len(data)
            return
        self.tail += data
        keep = self.truncate["length"]
        if len(self.tail) > keep:
            n = len(self.tail) - keep
            actions.append(("data", bytes(self.tail[:n])))
            self.out += n
            del self.tail[:n]

    def _remember(self, data):
        if self.max_dup:
            self.history += data
            extra = len(self.history) - self.max_dup
            if extra > 0:
                del self.history[:extra]
                self.hist_start += extra

    def feed(self, chunk):
        actions = []
        if self.cut:
            return actions
        i = 0
        while i < len(chunk) or (self.events and _trigger(self.events[0]) == self.pos):
            if self.events and _trigger(self.events[0]) == self.pos:
                e = self.events.pop(0)
                k = e["kind"]
                if k == "dup":
                    s = e["offset"] - self.hist_start
                    data = bytes(self.history[s:s + e["length"]])
                    self.log.append(dict(e, in_offset=self.pos, out_offset=self.out + len(self.tail), fired=True))
                    self._release(actions, data)
                elif k == "stall":
                    self.log.append(dict(e, in_offset=self.pos, out_offset=self.out + len(self.tail), fired=True))
                    actions.append(("stall", e["seconds"]))
                elif k == "cut":
                    self.log.append(dict(e, in_offset=self.pos, out_offset=self.out + len(self.tail), fired=True))
                    self._release_all_tail(actions)
                    self.cut = True
                    actions.append(("cut",))
                    return actions
                else:                                          # flip the byte at self.pos
                    if i >= len(chunk):
                        self.events.insert(0, e)               # the byte comes with the next chunk
                        break
                    b = chunk[i]
                    nb = b ^ e["mask"]
                    self.log.append(dict(e, in_offset=self.pos, out_offset=self.out + len(self.tail), fired=True,
                                         before=b, after=nb))
                    self._remember(chunk[i:i + 1])
                    self._release(actions, bytes([nb]))
                    self.pos += 1
                    i += 1
                continue
            nxt = _trigger(self.events[0]) if self.events else None
            end = len(chunk) if nxt is None else min(len(chunk), i + (nxt - self.pos))
            piece = chunk[i:end]
            self._remember(piece)
            self._release(actions, piece)
            self.pos += len(piece)
            i = end
        return actions

    def _release_all_tail(self, actions):
        if self.tail:
            actions.append(("data", bytes(self.tail)))
            self.out += len(self.tail)
            self.tail = bytearray()
        if self.truncate is not None:
            self.log.append(dict(self.truncate, fired=False, why="cut came first"))
            self.truncate = None

    def finish(self):
        """End of input: drop the truncated tail; record faults that never fired."""
        actions = []
        if not self.cut and self.truncate is not None:
            self.log.append(dict(self.truncate, in_offset=self.pos - len(self.tail), out_offset=self.out,
                                 dropped=len(self.tail), fired=bool(self.tail)))
            self.tail = bytearray()
            self.truncate = None
        for e in self.events:
            self.log.append(dict(e, fired=False, why="beyond the end of the stream" if not self.cut else "after cut"))
        self.events = []
        return actions

    def injected(self):
        return [e for e in self.log if e.get("fired")]


def apply_bytes(data, plan, chunk=None):
    """Run ``plan`` over ``data``; returns (output bytes, log). ``chunk`` splits the input to show that chunking does
    not matter. Stalls are recorded, not slept."""
    m = Mutator(plan)
    out = bytearray()
    step = chunk or max(1, len(data))
    for i in range(0, len(data), step):
        for a in m.feed(data[i:i + step]):
            if a[0] == "data":
                out += a[1]
        if m.cut:
            break
    for a in m.finish():
        if a[0] == "data":
            out += a[1]
    return bytes(out), m.log


def _write_all(fd, data):
    view = memoryview(data)
    while view:
        n = os.write(fd, view)
        view = view[n:]


def relay_fds(in_fd, out_fd, plan, cut_closes_input=True, sleep=time.sleep, read_size=65536):
    """Relay ``in_fd`` to ``out_fd`` through the plan until EOF, a cut, or the reader closing. Closes ``out_fd``
    (and ``in_fd`` on a cut when ``cut_closes_input``). Returns the log (with an ``end`` entry)."""
    m = Mutator(plan)
    end = "eof"
    try:
        while True:
            chunk = os.read(in_fd, read_size)
            if not chunk:
                break
            for a in m.feed(chunk):
                if a[0] == "data":
                    _write_all(out_fd, a[1])
                elif a[0] == "stall":
                    sleep(a[1])
            if m.cut:
                end = "cut"
                break
        m.finish()
    except BrokenPipeError:
        end = "reader_closed"
        m.finish()
    finally:
        os.close(out_fd)
        if end == "cut" and cut_closes_input:
            os.close(in_fd)
            in_fd = None
    log = list(m.log)
    log.append({"kind": "end", "how": end, "in_bytes": m.pos, "out_bytes": m.out})
    return log


class PipeRelay:
    """Relay in a thread between two new pipes: the producer writes to ``write_fd``, the consumer reads ``read_fd``.
    ``producer_fd()`` / ``consumer_fd()`` give the ends to pass to child processes (``pass_fds``)."""

    def __init__(self, plan, cut_closes_input=True):
        self.plan = plan
        self.cut_closes_input = cut_closes_input
        self._in_r, self.write_fd = os.pipe()
        self.read_fd, self._out_w = os.pipe()
        self.log = None
        self.thread = None

    def start(self):
        def run():
            try:
                self.log = relay_fds(self._in_r, self._out_w, self.plan, self.cut_closes_input)
            finally:
                if not (self.log and self.log[-1].get("how") == "cut" and self.cut_closes_input):
                    try:
                        os.close(self._in_r)
                    except OSError:
                        pass

        self.thread = threading.Thread(target=run, name="pipefaults", daemon=True)
        self.thread.start()
        return self

    def join(self, timeout):
        self.thread.join(timeout)
        if self.thread.is_alive():
            raise TimeoutError("relay still running after %.1f s" % timeout)
        return self.log

    def injected(self):
        return [e for e in (self.log or []) if e.get("fired")]


def main(argv=None):
    import argparse

    ap = argparse.ArgumentParser(prog="python -m cdc_v2.testing.pipefaults",
                                 description="Relay stdin to stdout with seeded byte-stream faults.")
    ap.add_argument("--seed", type=int, required=True)
    ap.add_argument("--size", type=int, required=True, help="expected stream size; offsets are chosen below it")
    ap.add_argument("--kinds", default=",".join(KINDS))
    ap.add_argument("--events", type=int)
    ap.add_argument("--log", help="write the injection log (JSON) here at the end")
    a = ap.parse_args(argv)
    plan = make_plan(a.seed, a.size, kinds=[k for k in a.kinds.split(",") if k], events=a.events)
    log = relay_fds(sys.stdin.fileno(), sys.stdout.fileno(), plan)
    if a.log:
        with open(a.log, "w", encoding="utf-8") as f:
            json.dump({"seed": a.seed, "plan": plan, "log": log}, f, indent=2)
    return 0


if __name__ == "__main__":
    sys.exit(main())
