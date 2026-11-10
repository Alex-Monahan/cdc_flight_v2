"""Spike consumer for the local streaming measurements (PLAN §2 A 5 part 2). Same protocol and behaviour as
spikes/jvm_on_flight/consumer.py (starts the JVM as its child, reads batch frames from a dedicated pipe, acknowledges
each batch on the JVM's stdin), plus:

  --latency FILE   walk every event of a struct payload and append (arrival_ms, source ts_ms, lsn, txId) as four
                   big-endian int64 per event to FILE (for end-to-end delay; the row values are skipped, not built)
  --hold-acks      do not acknowledge (the JVM then blocks after its first batch); used only for restart tests

    python stream_consumer.py --stats S --jvm-log L [--latency F] -- java ... cdc_v2.engine.EngineMain props
"""
import argparse
import json
import os
import signal
import struct
import subprocess
import sys
import threading
import time

MAGIC = 0x43444331
HDR = struct.Struct(">iqii")
EV = struct.Struct(">cqqq")
REC = struct.Struct(">qqqq")


class Stats:
    def __init__(self, path):
        self.path = path
        self.d = {"consumer_pid": os.getpid(), "events": 0, "bytes": 0, "batches": 0, "first_event_at": None,
                  "last_event_at": None, "eof_at": None, "jvm_rc": None}
        self.lock = threading.Lock()

    def write(self):
        with self.lock:
            self.d["written_at"] = time.time()
            tmp = self.path + ".tmp"
            with open(tmp, "w") as f:
                f.write(json.dumps(self.d))
            os.replace(tmp, self.path)

    def start_flusher(self):
        def loop():
            while True:
                time.sleep(0.05)
                try:
                    self.write()
                except RuntimeError:
                    pass
        threading.Thread(target=loop, daemon=True).start()


def read_exact(f, n):
    buf = bytearray(n)
    view = memoryview(buf)
    got = 0
    while got < n:
        k = f.readinto(view[got:])
        if not k:
            return None
        got += k
    return buf


FIXED = {1: 4, 2: 8, 3: 8, 8: 4, 9: 2, 10: 1, 5: 1, 0: 0}


def walk(payload, n, arrival_ms, out):
    """Skip through n struct events; append one REC per event to the bytearray out."""
    off = 0
    u = struct.unpack_from
    for _ in range(n):
        _op, lsn, tx, ts = EV.unpack_from(payload, off)
        off += 25
        tl, = u(">h", payload, off)
        off += 2 + tl
        nc, = u(">h", payload, off)
        off += 2
        for _c in range(nc):
            tag = payload[off]
            off += 1
            f = FIXED.get(tag)
            if f is not None:
                off += f
            elif tag in (4, 11):
                ln, = u(">i", payload, off)
                off += 4 + max(ln, 0)
            elif tag == 6:
                ln, = u(">i", payload, off)
                off += 4 + ln + 2
            elif tag == 7:
                ln, = u(">i", payload, off)
                off += 4 + ln
            else:
                raise ValueError("unknown tag %d" % tag)
        out += REC.pack(arrival_ms, ts, lsn, tx)
    if off != len(payload):
        raise ValueError("payload has %d trailing bytes" % (len(payload) - off))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--stats", required=True)
    ap.add_argument("--jvm-log", required=True)
    ap.add_argument("--latency", default=None)
    ap.add_argument("--hold-acks", action="store_true")
    ap.add_argument("cmd", nargs=argparse.REMAINDER)
    a = ap.parse_args()
    cmd = a.cmd[1:] if a.cmd and a.cmd[0] == "--" else a.cmd
    st = Stats(a.stats)
    log = open(a.jvm_log, "ab", buffering=0)
    st.d["spawn_at"] = time.time()
    rfd, wfd = os.pipe()
    cmd = [c.replace("@FRAMES_FD@", str(wfd)) for c in cmd]
    p = subprocess.Popen(cmd, stdin=subprocess.PIPE, stdout=log, stderr=log, bufsize=0, pass_fds=(wfd,))
    os.close(wfd)
    st.d["jvm_pid"] = p.pid
    st.write()
    st.start_flusher()
    signal.signal(signal.SIGTERM, lambda *_: p.send_signal(signal.SIGTERM))
    lat = open(a.latency, "ab") if a.latency else None
    buf = bytearray()
    out = os.fdopen(rfd, "rb", buffering=1 << 20)
    try:
        while True:
            hdr = read_exact(out, HDR.size)
            if hdr is None:
                break
            magic, seq, n, ln = HDR.unpack(hdr)
            if magic != MAGIC:
                raise SystemExit("bad frame magic %x after %d batches" % (magic, st.d["batches"]))
            payload = read_exact(out, ln) if ln else bytearray()
            if payload is None:
                raise SystemExit("truncated frame %d" % seq)
            now = time.time()
            if lat is not None and n:
                buf.clear()
                walk(payload, n, int(now * 1000), buf)
                lat.write(buf)
            if n and st.d["first_event_at"] is None:
                st.d["first_event_at"] = now
            if n:
                st.d["last_event_at"] = now
            st.d["events"] += n
            st.d["bytes"] += ln
            st.d["batches"] += 1
            if not a.hold_acks:
                try:
                    p.stdin.write(b"A %d\n" % seq)
                    p.stdin.flush()
                except BrokenPipeError:
                    break
    finally:
        st.d["eof_at"] = time.time()
        if lat:
            lat.close()
        st.write()
    try:
        p.stdin.close()
    except OSError:
        pass
    st.d["jvm_rc"] = p.wait()
    st.d["exited_at"] = time.time()
    st.write()
    return 0


if __name__ == "__main__":
    sys.exit(main())
