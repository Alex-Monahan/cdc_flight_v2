"""A stand-in for the JVM in hermetic tests of cdc_v2.stream.engine: speaks the same pipes (CONFIG/DURABLE/STOP on
standard input, frames on file descriptor 3). Mode from FAKE_MODE; observations go to FAKE_OUT (JSON lines)."""
import hashlib
import json
import os
import sys
import threading
import time

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), "src"))

from cdc_v2.stream import frames as F          # noqa: E402
from cdc_v2.testing import frames as T          # noqa: E402

mode = os.environ.get("FAKE_MODE", "normal")
outp = os.environ.get("FAKE_OUT")


def note(**kw):
    if outp:
        with open(outp, "a") as f:
            f.write(json.dumps(kw) + "\n")


note(argv=sys.argv, env=dict(os.environ))
print("fake engine noise on stdout", flush=True)
config_line = sys.stdin.readline()
config = json.loads(config_line[len("CONFIG "):])
note(config_keys=sorted(config), password_ok=hashlib.sha256(config.get("database.password", "").encode()).hexdigest()
     == os.environ.get("FAKE_EXPECT_PW_SHA"))
out = open("/dev/fd/3", "wb", buffering=0)
enc = T.Encoder()
durables = []
lock = threading.Lock()


def commands():
    for line in sys.stdin:
        line = line.strip()
        if line.startswith("DURABLE "):
            with lock:
                durables.append(int(line.split()[1]))
            note(durable=int(line.split()[1]))
        elif line == "STOP":
            note(stop=True)
            if mode != "ignore_stop":
                with lock:
                    note(durables_seen=len(durables), last=durables[-1] if durables else None)
                os._exit(0)
    note(eof=True)
    os._exit(4)


threading.Thread(target=commands, daemon=True).start()
out.write(enc.encode(F.Hello(1, "fake", config.get("slot.name", ""), None, os.getpid(),
                             config.get("cdc_v2.engine.id", ""), 1, 2, "Debezium Streaming")))
if mode == "fatal":
    out.write(enc.encode(F.Fatal("x.Boom", "it broke", (("y.Cause", "deeper"),))))
    os._exit(1)
if mode == "exit":
    os._exit(3)
if mode == "garbage":
    out.write(b"[0.8s][info] not a frame at all......")
    time.sleep(60)
if mode == "flood":
    # far more than a pipe holds: the write blocks until the reader reads
    big = T.rows("s", "t", 1, {"seq": (F.P_INT64, list(range(20000))), "op": (F.P_UTF8, ["c"] * 20000)})
    for i in range(50):
        out.write(enc.encode(big))
    with lock:
        note(flood_done=True, durables_seen=len(durables), last=durables[-1] if durables else None)
for i in range(5):
    out.write(enc.encode(F.Heartbeat(i, {"lsn": 100 + i}, 100 + i)))
    out.write(enc.encode(F.BatchEnd(1, None)))
while True:
    time.sleep(0.1)
