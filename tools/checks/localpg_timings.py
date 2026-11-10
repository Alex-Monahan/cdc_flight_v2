#!/usr/bin/env python3
"""Times the tools/localpg lifecycle on a home and prints a Markdown report (for evidence/p1/localpg.md).

Run: tools/with-env python3 tools/checks/localpg_timings.py      (uses $CDC_V2_PG_HOME / $CDC_V2_PG_PORT_BASE)
The home is reset at the start and all of its clusters are stopped at the end. Paths under $HOME are printed as `~`.
"""
import json
import os
import subprocess
import sys
import time

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
LOCALPG = os.path.join(ROOT, "tools", "localpg")
HOME = os.path.expanduser("~")

SETTINGS = ["server_version", "wal_level", "hot_standby", "hot_standby_feedback", "sync_replication_slots",
            "primary_slot_name", "synchronized_standby_slots", "wal_retrieve_retry_interval",
            "max_slot_wal_keep_size", "wal_keep_size",
            "shared_buffers", "max_connections", "max_wal_senders", "max_replication_slots", "max_worker_processes",
            "listen_addresses", "port", "ssl", "ssl_min_protocol_version", "password_encryption", "data_checksums"]


def redact(text):
    return text.replace(HOME, "~")


def lp(*args, check=True):
    t0 = time.time()
    p = subprocess.run([LOCALPG, *args], capture_output=True, text=True, timeout=900)
    dt = time.time() - t0
    if check and p.returncode != 0:
        sys.exit("localpg %s failed: %s" % (" ".join(args), p.stderr.strip()))
    return dt, p


def main():
    rows = []

    def timed(label, *args):
        dt, p = lp(*args)
        rows.append((label, "localpg " + " ".join(args), dt, redact(p.stdout.strip().splitlines()[-1])))
        return p

    lp("down", check=False)
    timed("reset (wipe + build from nothing)", "reset")
    timed("up, already running (no-op)", "up")
    timed("down (2 nodes)", "down")
    timed("up from stopped (2 nodes)", "up")
    timed("add-standby", "add-standby")
    timed("kill primary (SIGKILL)", "kill", "primary")
    timed("up after kill (crash recovery)", "up")
    timed("promote standby2", "promote", "standby2")
    timed("down (3 nodes)", "down")
    timed("reset from a 3-node, 2-primary state", "reset")
    _, st = lp("status")
    settings = {}
    for node in ("primary", "replica"):
        _, p = lp("sql", node, "-c", "select json_object_agg(name, current_setting(name)) from pg_settings "
                                     "where name in (%s)" % ",".join("'%s'" % s for s in SETTINGS))
        settings[node] = json.loads(p.stdout)
    _, p = lp("sql", "primary", "-c", "select json_agg(json_build_object('role', rolname, 'super', rolsuper, "
                                      "'replication', rolreplication, 'createrole', rolcreaterole, 'createdb', "
                                      "rolcreatedb) order by rolname) from pg_roles where rolname like 'cdc_v2%'")
    roles = json.loads(p.stdout)
    timed("down (final)", "down")
    _, final = lp("status", check=False)

    print("| Step | Command | Seconds | Output |\n|---|---|---|---|")
    for label, cmd, dt, out in rows:
        print("| %s | `%s` | %.2f | %s |" % (label, cmd, dt, out.replace("|", "\\|")))
    print("\n`localpg status` after the final reset:\n\n```\n%s\n```\n" % redact(st.stdout.strip()))
    print("Settings in force:\n\n| Setting | primary | replica |\n|---|---|---|")
    for s in SETTINGS:
        print("| `%s` | %s | %s |" % (s, settings["primary"].get(s, "") or "''", settings["replica"].get(s, "") or "''"))
    print("\nRoles:\n\n| Role | superuser | replication | createrole | createdb |\n|---|---|---|---|---|")
    for r in roles:
        print("| `%s` | %s | %s | %s | %s |" % (r["role"], r["super"], r["replication"], r["createrole"],
                                              r["createdb"]))
    print("\n`localpg status` after the final `down` (exit %d):\n\n```\n%s\n```" % (final.returncode,
                                                                                redact(final.stdout.strip())))


if __name__ == "__main__":
    main()
