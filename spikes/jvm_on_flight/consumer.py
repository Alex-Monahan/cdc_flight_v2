"""Spike consumer: the Python side of candidate B. Starts the JVM program as its child, reads its batch frames from
the JVM's stdout, optionally decodes them and writes them to a file sink, and acknowledges each batch on the JVM's
stdin. Writes counters to a JSON stats file (atomically replaced) that the driver polls.

    python consumer.py --stats S --jvm-log L [--decode] [--sink FILE] -- java ... cdc_v2.engine.EngineMain props

Frame: int magic 'CDC1', long seq, int n, int payload length, payload (see jvm/src/cdc_v2/engine/EngineMain.java).
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


class Stats:
    def __init__(self, path):
        self.path = path
        self.d = {"consumer_pid": os.getpid(), "events": 0, "bytes": 0, "batches": 0, "first_event_at": None,
                  "last_event_at": None, "eof_at": None, "jvm_rc": None, "ops": {}, "orderkey_sum": 0,
                  "decoded": 0, "sample": []}
        self.last_write = 0.0
        self.lock = threading.Lock()

    def write(self, force=False):
        now = time.time()
        if not force and now - self.last_write < 0.1:
            return
        with self.lock:
            self.last_write = now
            self.d["written_at"] = now
            text = json.dumps(self.d)
            tmp = self.path + ".tmp"
            with open(tmp, "w") as f:
                f.write(text)
            os.replace(tmp, self.path)

    def start_flusher(self):
        """Rewrite the file every 0.2 s, so the last batches are visible even when no further batch arrives."""
        def loop():
            while True:
                time.sleep(0.2)
                try:
                    self.write(force=True)
                except RuntimeError:  # a dict changed size while being serialised: try again next tick
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


def decode_struct(payload, n, st):
    """Decode a struct-format payload (prototype of the applier's parser; only used for correctness checks)."""
    off = 0
    u = struct.unpack_from
    for _ in range(n):
        op = chr(payload[off]); off += 1
        lsn, tx, ts = u(">qqq", payload, off); off += 24
        tl, = u(">h", payload, off); off += 2
        table = payload[off:off + tl].decode(); off += tl
        nc, = u(">h", payload, off); off += 2
        row = []
        for _c in range(nc):
            tag = payload[off]; off += 1
            if tag == 0:
                v = None
            elif tag == 1:
                v, = u(">i", payload, off); off += 4
            elif tag == 2:
                v, = u(">q", payload, off); off += 8
            elif tag == 3:
                v, = u(">d", payload, off); off += 8
            elif tag == 8:
                v, = u(">f", payload, off); off += 4
            elif tag == 9:
                v, = u(">h", payload, off); off += 2
            elif tag == 10:
                v = payload[off]; off += 1
            elif tag == 5:
                v = bool(payload[off]); off += 1
            elif tag in (4, 11):
                ln, = u(">i", payload, off); off += 4
                v = payload[off:off + ln].decode(); off += ln
            elif tag == 6:
                ln, = u(">i", payload, off); off += 4
                unscaled = int(payload[off:off + ln].decode()); off += ln
                scale, = u(">h", payload, off); off += 2
                v = "%s:%d" % (unscaled, scale)
            elif tag == 7:
                ln, = u(">i", payload, off); off += 4
                v = bytes(payload[off:off + ln]).hex(); off += ln
            else:
                raise ValueError("unknown tag %d" % tag)
            row.append(v)
        st.d["ops"][op] = st.d["ops"].get(op, 0) + 1
        st.d["decoded"] += 1
        if row and isinstance(row[0], int):
            st.d["orderkey_sum"] += row[0]
        if len(st.d["sample"]) < 2:
            st.d["sample"].append({"op": op, "lsn": lsn, "txId": tx, "ts_ms": ts, "table": table, "row": row})
    if off != len(payload):
        raise ValueError("payload has %d trailing bytes" % (len(payload) - off))


def decode_json(payload, n, st):
    off = 0
    for _ in range(n):
        for which in ("key", "value"):
            ln, = struct.unpack_from(">i", payload, off); off += 4
            if ln >= 0:
                doc = json.loads(bytes(payload[off:off + ln]))
                off += ln
                if which == "value":
                    body = doc.get("payload", doc)
                    op = body.get("op", "?")
                    st.d["ops"][op] = st.d["ops"].get(op, 0) + 1
                    st.d["decoded"] += 1
                    row = body.get("after") or body.get("before") or {}
                    st.d["orderkey_sum"] += int(row.get("l_orderkey", 0) or 0)
                    if len(st.d["sample"]) < 1:
                        st.d["sample"].append(doc)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--stats", required=True)
    ap.add_argument("--jvm-log", required=True)
    ap.add_argument("--decode", choices=["none", "struct", "json"], default="none")
    ap.add_argument("--sink", default=None, help="append every payload to this file")
    ap.add_argument("--no-ack", action="store_true")
    ap.add_argument("cmd", nargs=argparse.REMAINDER)
    a = ap.parse_args()
    cmd = a.cmd[1:] if a.cmd and a.cmd[0] == "--" else a.cmd
    st = Stats(a.stats)
    log = open(a.jvm_log, "ab", buffering=0)
    st.d["spawn_at"] = time.time()
    # Frames arrive on a dedicated pipe; the JVM's own stdout (unified logging, JFR messages) goes to the log.
    rfd, wfd = os.pipe()
    cmd = [c.replace("@FRAMES_FD@", str(wfd)) for c in cmd]
    p = subprocess.Popen(cmd, stdin=subprocess.PIPE, stdout=log, stderr=log, bufsize=0, pass_fds=(wfd,))
    os.close(wfd)
    st.d["jvm_pid"] = p.pid
    st.write(force=True)
    st.start_flusher()
    signal.signal(signal.SIGTERM, lambda *_: p.send_signal(signal.SIGTERM))
    sink = open(a.sink, "ab") if a.sink else None
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
            if a.decode == "struct":
                decode_struct(payload, n, st)
            elif a.decode == "json":
                decode_json(payload, n, st)
            if sink:
                sink.write(payload)
            now = time.time()
            if n and st.d["first_event_at"] is None:
                st.d["first_event_at"] = now
            if n:
                st.d["last_event_at"] = now
            st.d["events"] += n
            st.d["bytes"] += ln
            st.d["batches"] += 1
            if not a.no_ack:
                try:
                    p.stdin.write(b"A %d\n" % seq)
                    p.stdin.flush()
                except BrokenPipeError:
                    break
            st.write()
    finally:
        st.d["eof_at"] = time.time()
        st.write(force=True)
        if sink:
            sink.close()
    try:
        p.stdin.close()
    except OSError:
        pass
    st.d["jvm_rc"] = p.wait()
    st.d["exited_at"] = time.time()
    st.write(force=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
