#!/usr/bin/env python3
"""Q10 definition changes under a running Flight.

    tools/with-env .venv/bin/python tools/spike/run_semantics_defchange.py

Flight cdc_v2_spike_runs_def (ticker mode, a row every second tagged with the source version, runs last 240 s):
 1. run A on demand (source v1, config tag c1); while it runs, MD_UPDATE_FLIGHT with source v2 and config tag c2;
    then schedule '* * * * *' and let the next scheduled run S start: which version and config does S use, does A
    keep running on v1?
 2. clear the schedule while A and S run: do they continue?
 3. delete the Flight while A and S run: do they stop?
Evidence: defchange.json.
"""
import time

import run_semantics_common as c

NAME = "cdc_v2_spike_runs_def"
CFG = {"CDC_V2_RUNS_MODE": "ticker", "CDC_V2_RUNS_TICK_S": "1", "CDC_V2_RUNS_DURATION_S": "240",
       "CDC_V2_RUNS_CONFIG_TAG": "c1"}


def rows_after(n, t):
    r = c.db().execute("SELECT count(*), max(epoch_ms(server_ts)) / 1000.0, list(DISTINCT tag) FROM ticks "
                       "WHERE flight = ? AND run_number = ? AND epoch_ms(server_ts) / 1000.0 > ?", [NAME, n, t]).fetchone()
    return {"rows": r[0], "last": c.iso(r[1]), "tags": r[2]}


def start_event(n):
    e = [x for x in c.event_rows(NAME, n) if x["kind"] == "start"]
    return e[0]["detail"] if e else None


def main():
    c.setup_db()
    src_v1 = c.read_src()
    assert 'SOURCE_VERSION = "v1"' in src_v1
    src_v2 = src_v1.replace('SOURCE_VERSION = "v1"', 'SOURCE_VERSION = "v2"')
    s = c.deploy(NAME, config=CFG, source_text=src_v1)
    out = {"flight": NAME, "initial_version": s.get("current_version")}
    a = c.run(NAME)["run_number"]
    c.wait_state(NAME, a)
    time.sleep(15)
    t_upd = time.time()
    upd = c.ctl.update(NAME, source_code=src_v2,
                       config=dict(c.DEFAULT_CONFIG, CDC_V2_RUNS_FLIGHT=NAME, **dict(CFG, CDC_V2_RUNS_CONFIG_TAG="c2")))
    out["update"] = {"at": c.iso(t_upd), "new_version": upd.get("current_version")}
    c.ctl.update(NAME, schedule_cron="* * * * *")
    # wait for the first scheduled run to be RUNNING and ticking
    sched = None
    deadline = time.time() + 150
    while time.time() < deadline and sched is None:
        for r in c.ctl.runs(NAME, 10):
            if r["is_scheduled"] and r["state"] == "RUNNING":
                sched = r["run_number"]
        time.sleep(2)
    time.sleep(15)
    t_unsched = time.time()
    c.ctl.unschedule(NAME)
    time.sleep(30)
    runs_before_delete = [c.slim_run(r) for r in c.ctl.runs(NAME, 20)]
    out["step1"] = {
        "run_A": a, "run_A_record": c.slim_run(c.ctl.get_run(NAME, a)), "run_A_start_event": start_event(a),
        "run_A_rows_after_update": rows_after(a, t_upd),
        "scheduled_run_S": sched, "run_S_record": c.slim_run(c.ctl.get_run(NAME, sched)) if sched else None,
        "run_S_start_event": start_event(sched) if sched else None,
        "run_S_tags": rows_after(sched, 0) if sched else None}
    out["step2_unschedule"] = {"at": c.iso(t_unsched), "run_A_rows_after": rows_after(a, t_unsched + 1),
                               "run_S_rows_after": rows_after(sched, t_unsched + 1) if sched else None,
                               "runs": runs_before_delete}
    active = [r["run_number"] for r in c.ctl.runs(NAME, 20) if c.fc.run_is_active(r)]
    fid = c.ctl.resolve(NAME)["flight_id"]
    t_del = time.time()
    out["step3_delete"] = {"active_runs_at_delete": active, "at": c.iso(t_del), "result": c.ctl.delete(NAME)}
    time.sleep(45)
    try:
        after = c.ctl.query(c.fc.build_list_runs_sql(fid, 20))
        out["step3_delete"]["runs_listed_after_delete"] = [c.slim_run(r) for r in after]
    except c.fc.FlightCtlError as exc:
        out["step3_delete"]["runs_listed_after_delete_error"] = str(exc)[:300]
    out["step3_delete"]["rows_after_delete"] = {n: rows_after(n, t_del + 2) for n in active}
    out["step3_delete"]["last_rows"] = {n: c.tick_summary(NAME, n) for n in active}
    c.save("defchange", out)
    print(out["step3_delete"]["rows_after_delete"], flush=True)


if __name__ == "__main__":
    main()
