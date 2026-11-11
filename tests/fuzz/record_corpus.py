"""Record the stream fuzzer's corpus from the real engine (package E), once: `-m "jvm and localpg"`.

    tools/with-env uv run pytest -q -m "jvm and localpg" tests/fuzz/record_corpus.py

A workload with every frame type runs against a fuzz home's standby slot (pgoutput, as in production): inserts,
updates, deletes, a key change chain and a swap, a keyless table with identical rows, TRUNCATE of one and of two
tables, a transactional and a plain logical message, a transaction of 1,200 rows (several ROWS frames: the frame row
limit is set to 500), an empty transaction, a rolled-back one, idle time for HEARTBEAT frames. The frames the engine
wrote are encoded again with `cdc_v2.testing.frames.Encoder` (byte-identical to the Java encoder, package E's codec
tests) into tests/fuzz/corpus/engine_*.frames. Generated data only.
"""
import threading
import time
from pathlib import Path

import pytest

from cdc_v2.deadline import Deadline
from cdc_v2.stream import frames as F
from cdc_v2.testing import frames as FE
from cdc_v2.testing.fuzz import pool
from cdc_v2.testing.pg import PrivateDatabase

pytestmark = [pytest.mark.jvm, pytest.mark.localpg]
OUT = Path(__file__).resolve().parent / "corpus"

SETUP = ["CREATE TABLE t (id bigint PRIMARY KEY, v text, n integer)",
         "CREATE TABLE k (a integer, b text)", "ALTER TABLE k REPLICA IDENTITY FULL",
         "CREATE TABLE big (id bigint PRIMARY KEY, pad text)",
         "CREATE PUBLICATION cdc_v2_pub FOR TABLE t, k, big",
         "GRANT SELECT ON t, k, big TO cdc_v2_runtime"]
WORK = [["INSERT INTO t VALUES (1, 'a', 1), (2, 'b', 2), (3, NULL, 3)"],
        ["UPDATE t SET v = 'a2', n = n + 1 WHERE id = 1"],
        ["DELETE FROM t WHERE id = 3"],
        ["BEGIN", "UPDATE t SET id = 10 WHERE id = 1", "UPDATE t SET id = 11 WHERE id = 10", "COMMIT"],
        ["INSERT INTO k VALUES (1, 'x'), (1, 'x'), (2, NULL)"],
        ["UPDATE k SET b = 'y' WHERE ctid = (SELECT min(ctid) FROM k WHERE a = 1)"],
        ["DELETE FROM k WHERE ctid = (SELECT min(ctid) FROM k WHERE a = 1)"],
        ["TRUNCATE k"],
        ["BEGIN", "INSERT INTO t VALUES (20, 'm', 0)", "SELECT pg_logical_emit_message(true, 'cdc_v2_m', 'in-tx')",
         "COMMIT"],
        ["SELECT pg_logical_emit_message(false, 'cdc_v2_n', 'loose')"],
        ["INSERT INTO big SELECT g, md5(g::text) FROM generate_series(1, 1200) g"],
        ["BEGIN", "INSERT INTO t VALUES (30, 'r', 0)", "ROLLBACK"],
        ["BEGIN", "COMMIT"],
        ["TRUNCATE t, big"],
        ["INSERT INTO t VALUES (999, 'end', 0)"]]


def test_record_engine_corpus():
    from cdc_v2.stream import jvm
    from cdc_v2.stream.engine import Engine, EngineSettings
    artifacts = jvm.prepare()
    lease = pool.acquire(wait_s=60)
    cl = lease.cluster()
    db = PrivateDatabase(cl)
    try:
        for s in SETUP:
            db.sql(s)
        cl.wait_replica_caught_up()
        stop = threading.Event()

        def nudge():
            with db.connect("runtime") as c:
                while not stop.is_set():
                    c.execute("SELECT pg_log_standby_snapshot()")
                    stop.wait(0.2)
        th = threading.Thread(target=nudge, daemon=True)
        th.start()
        db.sql("SELECT pg_create_logical_replication_slot('cdc_v2_slot', 'pgoutput')", role="runtime",
               node="replica")
        stop.set()
        rt = cl.passwords["runtime"]
        settings = EngineSettings(host="localhost", port=cl.replica_port, dbname=db.name, user=rt["user"],
                                  password=rt["password"], sslrootcert=cl.info["ca_cert"], frame_max_rows=500,
                                  heartbeat_interval_ms=1000)
        frames = []
        with Engine(settings, None, artifacts) as eng:
            for sts in WORK[:-1]:
                with db.connect() as c:
                    for s in sts:
                        c.execute(s)
            time.sleep(3.0)                                # HEARTBEAT frames on an idle source
            with db.connect() as c:
                c.execute(WORK[-1][0])
            end = time.monotonic() + 120
            done = False
            while time.monotonic() < end and not done:
                for f in eng.frames(Deadline.after(2)):
                    frames.append(f)
                    if isinstance(f, F.Rows) and f.table == "t" and 999 in f.batch.column("a0").to_pylist():
                        done = True
                    if done and isinstance(f, F.End):
                        break
                if done:
                    for f in eng.frames(Deadline.after(1)):
                        frames.append(f)
                        if isinstance(f, F.End):
                            break
                    break
            eng.stop(Deadline.after(20))
        while frames and not isinstance(frames[-1], (F.End, F.BatchEnd)):
            frames.pop()
        kinds = {type(f).__name__ for f in frames}
        assert {"Hello", "Schema", "Rows", "Begin", "End", "Truncate", "Message", "Heartbeat",
                "BatchEnd"} <= kinds, kinds
        data = FE.Encoder().encode_all(frames)
        assert F.decode_all(data) == frames
        OUT.mkdir(exist_ok=True)
        (OUT / "engine_all_types.frames").write_bytes(data)
        # a second, shorter stream: the first 40 % of the frames, cut after a whole transaction
        cut = [i for i, f in enumerate(frames) if isinstance(f, F.End)]
        k = cut[max(0, len(cut) * 2 // 5)]
        (OUT / "engine_prefix.frames").write_bytes(FE.Encoder().encode_all(frames[:k + 1]))
    finally:
        db.drop()
        lease.release()
