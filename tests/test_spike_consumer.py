"""The spike consumer's frame protocol: frames on a dedicated pipe, acknowledgements on stdin, stdout noise ignored."""
import json
import os
import subprocess
import sys

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CONSUMER = os.path.join(REPO, "spikes", "jvm_on_flight", "consumer.py")

FAKE_ENGINE = r'''
import struct, sys
print("[0.8s][info][jfr,startup] noise on stdout must not reach the frame stream", flush=True)
doc = b'{"payload": {"op": "c", "after": {"l_orderkey": 7}}}'
with open(sys.argv[1], "wb", buffering=0) as f:
    for seq, n in ((1, 0), (2, 1)):
        payload = (struct.pack(">i", 2) + b"{}" + struct.pack(">i", len(doc)) + doc) if n else b""
        f.write(struct.pack(">iqii", 0x43444331, seq, n, len(payload)) + payload)
        ack = sys.stdin.readline().strip()
        if ack != "A %d" % seq:
            sys.exit(5)
'''


def test_frames_on_dedicated_pipe_with_acks(tmp_path):
    engine = tmp_path / "fake_engine.py"
    engine.write_text(FAKE_ENGINE)
    stats, log = tmp_path / "s.json", tmp_path / "jvm.log"
    p = subprocess.run([sys.executable, CONSUMER, "--stats", str(stats), "--jvm-log", str(log), "--decode", "json",
                        "--", sys.executable, str(engine), "/dev/fd/@FRAMES_FD@"], timeout=60)
    assert p.returncode == 0
    st = json.loads(stats.read_text())
    assert st["jvm_rc"] == 0  # the engine saw both acknowledgements in order
    assert st["events"] == 1 and st["batches"] == 2 and st["ops"] == {"c": 1} and st["orderkey_sum"] == 7
    assert "noise on stdout" in log.read_text()


def test_bad_magic_is_refused(tmp_path):
    engine = tmp_path / "bad.py"
    engine.write_text("import sys\nopen(sys.argv[1], 'wb').write(b'[0.8s][info] not a frame......')\n")
    p = subprocess.run([sys.executable, CONSUMER, "--stats", str(tmp_path / "s.json"), "--jvm-log",
                        str(tmp_path / "l.log"), "--", sys.executable, str(engine), "/dev/fd/@FRAMES_FD@"],
                       capture_output=True, text=True, timeout=60)
    assert p.returncode != 0 and "bad frame magic" in p.stderr
