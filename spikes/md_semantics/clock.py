"""Q6 (SPEC 4.2): where are now(), current_timestamp, get_current_timestamp() evaluated on MotherDuck, and which
expression is certainly evaluated by the server?

Method:
 1. EXPLAIN: MotherDuck marks each operator (L) local or (R) remote. A clock function that the client planner folds
    into a literal shows up as a timestamp literal even inside an (R) operator: then the client clock produced it.
 2. Stale-transaction test: inside a client transaction opened N seconds earlier, now()/current_timestamp return the
    client's transaction start time; a server-evaluated expression returns the current time.
 3. Column DEFAULTs: which clock does a DEFAULT use when the INSERT runs on the server?
 4. Time-zone test: with a deliberately odd session TimeZone, a timestamp-to-text cast inside an (R) operator shows
    the client setting when it was folded on the client.
 5. Skew: server time (from uuidv7() generated on the server) against the client clock, bracketing each round trip.

Run:  tools/with-env uv run python spikes/md_semantics/clock.py
"""
from __future__ import annotations

import re
import time

from common import Evidence, connect, create_db, drop_db, err_class, dist

DB = "cdc_v2_spike_md_clock"

EXPRS = {
    "now()": "now()",
    "current_timestamp": "current_timestamp",
    "get_current_timestamp()": "get_current_timestamp()",
    "transaction_timestamp()": "transaction_timestamp()",
    "current_localtimestamp()": "current_localtimestamp()",
    "epoch_ms(now())": "epoch_ms(now())",
    "uuid_extract_timestamp(uuidv7())": "uuid_extract_timestamp(uuidv7())",
    "gen_random_uuid()": "gen_random_uuid()",
}
LIT = re.compile(r"'\d{4}-\d\d-\d\d \d\d:\d\d:\d\d|\b1[5-9]\d{11}\b")


def remote_projection(plan: str) -> str:
    """Text of the PROJECTION (R) box(es) of a rendered plan."""
    out, grab = [], False
    for line in plan.splitlines():
        if "PROJECTION (R)" in line or "UPDATE (R)" in line or "INSERT (R)" in line:
            grab = True
            continue
        if grab and ("└" in line):
            grab = False
        if grab:
            out.append(line.strip("│ ").strip())
    return " ".join(out)


def main() -> None:
    ev = Evidence("clock")
    create_db(DB)
    con = connect(DB)
    try:
        con.execute("CREATE TABLE one AS SELECT 1 AS x")
        ev.log("client session TimeZone:", con.execute("SELECT current_setting('TimeZone')").fetchone()[0])
        # 1. plans
        ev.log("== 1. EXPLAIN of SELECT <expr> FROM one (a MotherDuck table, so the projection runs remotely)")
        plans = {}
        for name, e in EXPRS.items():
            plan = con.execute(f"EXPLAIN SELECT {e} FROM one").fetchall()[0][1]
            proj = remote_projection(plan)
            folded = bool(LIT.search(proj))
            plans[name] = {"remote_projection": proj, "folded_to_literal_by_client": folded}
            ev.log(f"   {name:36s} remote projection = {proj!r:60s} -> "
                   f"{'FOLDED ON CLIENT (client clock)' if folded else 'evaluated by the server'}")
        for label, sql in [
            ("UPDATE ... SET hb = now()", "EXPLAIN UPDATE one SET x = epoch_ms(now())"),
            ("UPDATE ... SET hb = uuid_extract_timestamp(uuidv7())",
             "EXPLAIN UPDATE one SET x = epoch_ms(uuid_extract_timestamp(uuidv7()))"),
            ("INSERT ... SELECT now()", "EXPLAIN INSERT INTO one SELECT epoch_ms(now())"),
        ]:
            plan = con.execute(sql).fetchall()[0][1]
            proj = remote_projection(plan)
            ev.log(f"   {label:52s} -> remote operators show {proj!r}")
            plans[label] = proj
        ev.put("plans", plans)

        # 2. stale transaction
        ev.log("== 2. inside a client transaction opened 5 s earlier (values as epoch ms; client clock at query time "
               "shown for comparison)")
        con.execute("BEGIN")
        t_begin = time.time()
        con.execute("SELECT count(*) FROM one").fetchall()
        time.sleep(5)
        t_q = time.time()
        r = con.execute("SELECT epoch_ms(now()), epoch_ms(current_timestamp), epoch_ms(get_current_timestamp()), "
                        "epoch_ms(uuid_extract_timestamp(uuidv7())) FROM one").fetchone()
        t_r = time.time()
        con.execute("COMMIT")
        stale = {"client_begin_ms": int(t_begin * 1000), "client_query_sent_ms": int(t_q * 1000),
                 "now_minus_begin_ms": r[0] - int(t_begin * 1000),
                 "current_timestamp_minus_begin_ms": r[1] - int(t_begin * 1000),
                 "get_current_timestamp_minus_query_ms": r[2] - int(t_q * 1000),
                 "server_uuidv7_minus_query_ms": r[3] - int(t_q * 1000), "round_trip_ms": int((t_r - t_q) * 1000)}
        for k, v in stale.items():
            ev.log(f"   {k}: {v}")
        ev.put("stale_transaction", stale)

        # 3. defaults
        ev.log("== 3. column DEFAULTs, inserted inside a transaction opened 5 s earlier")
        con.execute("CREATE TABLE hb (id INT, d_now TIMESTAMPTZ DEFAULT now(), "
                    "d_get TIMESTAMPTZ DEFAULT get_current_timestamp(), d_uuid UUID DEFAULT uuidv7())")
        plan = con.execute("EXPLAIN INSERT INTO hb (id) VALUES (1)").fetchall()[0][1]
        ev.log("   plan of INSERT INTO hb(id) VALUES (1), remote part:", repr(remote_projection(plan)))
        con.execute("BEGIN")
        t_begin = time.time()
        con.execute("SELECT count(*) FROM one").fetchall()
        time.sleep(5)
        t_q = time.time()
        con.execute("INSERT INTO hb (id) VALUES (1)")
        con.execute("COMMIT")
        r = con.execute("SELECT epoch_ms(d_now), epoch_ms(d_get), epoch_ms(uuid_extract_timestamp(d_uuid)) "
                        "FROM hb").fetchone()
        d = {"default_now_minus_begin_ms": r[0] - int(t_begin * 1000),
             "default_get_current_timestamp_minus_insert_ms": r[1] - int(t_q * 1000),
             "default_uuidv7_minus_insert_ms": r[2] - int(t_q * 1000)}
        for k, v in d.items():
            ev.log(f"   {k}: {v}")
        ev.put("defaults", d)

        # 4. time zone
        ev.log("== 4. session TimeZone set to Asia/Kathmandu (+05:45): text rendering inside a remote projection")
        con.execute("SET TimeZone = 'Asia/Kathmandu'")
        v = con.execute("SELECT now()::VARCHAR, uuid_extract_timestamp(uuidv7())::VARCHAR FROM one").fetchone()
        plan = con.execute("EXPLAIN SELECT now()::VARCHAR FROM one").fetchall()[0][1]
        ev.log("   now()::VARCHAR =", v[0][-6:], "(offset only); plan:", repr(remote_projection(plan))[-30:])
        ev.log("   uuid_extract_timestamp(uuidv7())::VARCHAR offset =", v[1][-6:])
        ev.put("timezone", {"now_varchar_offset": v[0][-6:], "uuid_ts_varchar_offset": v[1][-6:]})
        con.execute("RESET TimeZone")

        # 5. skew
        ev.log("== 5. skew = server time (uuidv7 made on the server) - client time at the midpoint of the round trip")
        samples = []
        for _ in range(300):
            t0 = time.time()
            s = con.execute("SELECT epoch_ms(uuid_extract_timestamp(uuidv7())) FROM one").fetchone()[0]
            t1 = time.time()
            samples.append(((t1 - t0) * 1000, s - (t0 + t1) / 2 * 1000, s - t0 * 1000, s - t1 * 1000))
        samples.sort()
        best = samples[: max(10, len(samples) // 10)]  # the fastest round trips bound the skew best
        skews = [x[1] for x in best]
        rtts = [x[0] for x in best]
        outside = sum(1 for x in samples if x[2] < 0 or x[3] > 0)
        ev.log(f"   samples whose server timestamp lies OUTSIDE the client's [send, receive] window: {outside} of "
               f"{len(samples)} (impossible if one clock produced both)")
        ev.put("server_ts_outside_client_window", [outside, len(samples)])
        ev.log("   all samples: rtt ms", dist([x[0] for x in samples]))
        ev.log("   fastest 10%: rtt ms", dist(rtts))
        ev.log("   fastest 10%: skew ms", dist(skews), "(uncertainty +/- rtt/2)")
        ev.put("skew", {"rtt_all_ms": dist([x[0] for x in samples]), "rtt_best_ms": dist(rtts),
                        "skew_best_ms": dist(skews)})
        # heartbeat age entirely on the server
        con.execute("CREATE TABLE leader (hb TIMESTAMPTZ)")
        con.execute("INSERT INTO leader VALUES (uuid_extract_timestamp(uuidv7()))")
        time.sleep(2)
        age = con.execute("SELECT epoch_ms(uuid_extract_timestamp(uuidv7())) - epoch_ms(hb) FROM leader").fetchone()[0]
        ev.log(f"   heartbeat written with the server clock, age read 2 s later with the server clock: {age} ms")
        ev.put("server_side_age_after_2s_ms", age)
    except Exception as e:
        ev.log("ERROR", err_class(e))
        raise
    finally:
        con.close()
        drop_db(DB)
        ev.log("database dropped")
        ev.write()


if __name__ == "__main__":
    main()
