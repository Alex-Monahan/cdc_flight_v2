"""Item 1 of the streaming spike: JVM, jars, tables, publication, a pgoutput slot on the standby, end-to-end smoke.
Writes evidence/p2/local_streaming/setup.json.

    tools/with-env uv run --group spike python spikes/local_throughput/stream_setup.py
"""
from __future__ import annotations

import json
import subprocess
import time

import common as C
import stream_common as S


def main():
    subprocess.run([str(C.LOCALPG), "up"], check=True, timeout=600)
    ev = {"utc": C.utc(), "load_avg": C.load_avg(), "prepare": S.prepare()}
    S.setup_tables()
    with C.pg_connect("replica", "admin") as r:
        ev["replica_settings"] = dict(r.execute(
            "select name, setting from pg_settings where name in ('logical_decoding_work_mem','wal_level',"
            "'hot_standby_feedback','max_standby_streaming_delay','wal_sender_timeout')").fetchall())
    # slot on the standby: (a) nothing helps, (b) the admin role tries pg_log_standby_snapshot, (c) superuser helps
    for n in ("replica", "primary"):
        S.drop_slot(n)
    with C.pg_connect("primary", "admin") as p:
        try:
            p.execute("select pg_log_standby_snapshot()")
            ev["admin_can_log_standby_snapshot"] = True
        except Exception as e:  # noqa: BLE001
            ev["admin_can_log_standby_snapshot"] = C.redact(e).splitlines()[0]
    C.wait_replica_caught_up()
    ev["slot_unaided_right_after_writes"] = S.create_slot("replica", help_standby=False, timeout=40)
    S.drop_slot("replica")
    time.sleep(20)  # primary idle: the bgwriter logs a running-xacts record only after "important" WAL
    ev["slot_unaided_idle_primary_20s"] = S.create_slot("replica", help_standby=False, timeout=40)
    S.drop_slot("replica")
    time.sleep(20)
    t = time.time()
    with C.pg_connect("primary", "admin") as p:  # one write on the primary while the creation waits
        import threading
        th = threading.Thread(target=lambda: ev.__setitem__(
            "slot_unaided_idle_then_one_write", S.create_slot("replica", help_standby=False, timeout=40)))
        th.start()
        time.sleep(2)
        p.execute("INSERT INTO public.stream_unpub VALUES (1, 'x') ON CONFLICT DO NOTHING")
        th.join()
    ev["slot_unaided_idle_then_one_write"]["write_at_s"] = 2.0
    ev["slot_unaided_idle_then_one_write"]["wall_s"] = round(time.time() - t, 2)
    S.drop_slot("replica")
    ev["slot_with_pg_log_standby_snapshot"] = S.create_slot("replica", help_standby=True)
    # smoke: engine on the replica, 1,000 rows
    S.truncate("stream_li")
    eng = S.Engine("replica", "best", label="smoke")
    t0 = time.time()
    ok = eng.wait_polling(120)
    ev["smoke"] = {"polling_started": ok, "polling_s": round(time.time() - t0, 2)}
    with C.pg_connect("primary", "admin") as p:
        p.execute("INSERT INTO public.stream_li SELECT * FROM public.lineitem LIMIT 1000")
    t1 = time.time()
    ev["smoke"]["events_ok"] = eng.wait_events(1000, 60)
    ev["smoke"]["commit_to_last_event_s"] = round(eng.stats().get("last_event_at", t1) - t1, 3)
    ev["smoke"]["errors"] = eng.errors()
    ev["smoke"]["stop_s"] = round(eng.stop(), 2)
    S.truncate("stream_li")
    print(json.dumps(ev, indent=1))
    S.EVID.mkdir(parents=True, exist_ok=True)
    (S.EVID / "setup.json").write_text(C.redact(json.dumps(ev, indent=1)) + "\n")


if __name__ == "__main__":
    main()
