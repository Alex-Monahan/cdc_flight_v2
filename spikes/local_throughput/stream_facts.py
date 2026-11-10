"""Item 4: what Debezium delivers per change event and per transaction, next to what the server sent.
Writes evidence/p2/local_streaming/facts.json (and facts_sample.jsonl: a few raw records of each kind).

    tools/with-env uv run --group spike python spikes/local_throughput/stream_facts.py [--tx 1200] [--node replica]

1. Debezium's slot (cdc_v2_stream_slot) and a second pgoutput slot (cdc_v2_stream_peek) on the same node, created
   back to back before any workload.
2. FactsMain (stock engine, provide.transaction.metadata=true) dumps every record (offset, source block,
   transaction block, BEGIN/END events) to a JSON-lines file.
3. Workload on the primary: --tx transactions of mixed size and shape (inserts 1-50 rows, updates and deletes on a
   keyed table, multi-table transactions, some with rows in an unpublished table), and special transactions between
   sentinel transactions: empty (BEGIN; COMMIT), xid only (pg_current_xact_id()), unpublished table only, rolled back,
   rolled-back savepoints.
4. pg_logical_slot_peek_binary_changes(peek slot, proto_version 1, publication_names) is parsed (BEGIN: final LSN,
   commit time, xid; COMMIT: flags, commit LSN, end LSN; I/U/D: the row's lsn column) and matched to Debezium's
   output by xid; every candidate equality is counted over all transactions.
"""
from __future__ import annotations

import argparse
import json
import random
import struct
import time

import common as C
import stream_common as S

PEEK = "cdc_v2_stream_peek"


def lsn_int(s) -> int:
    hi, lo = str(s).split("/")
    return (int(hi, 16) << 32) | int(lo, 16)


def lsn_str(n: int) -> str:
    return "%X/%X" % (n >> 32, n & 0xFFFFFFFF)


def peek(node: str) -> list[dict]:
    """Server transactions from the peek slot: [{xid, final_lsn, commit_ts, commit_lsn, end_lsn, flags,
    begin_row_lsn, commit_row_lsn, changes: [(type, row_lsn)]}]."""
    with C.pg_connect(node, "admin") as c:
        rows = c.execute("select lsn::text, xid::text, data from pg_logical_slot_peek_binary_changes(%s, NULL, NULL, "
                         "'proto_version', '1', 'publication_names', %s)", (PEEK, S.PUB)).fetchall()
    txs, cur = [], None
    for lsn, xid, data in rows:
        data = bytes(data)
        t = chr(data[0])
        if t == "B":
            final_lsn, ts, x = struct.unpack_from(">QqI", data, 1)
            cur = {"xid": x, "final_lsn": final_lsn, "commit_ts": ts, "begin_row_lsn": lsn_int(lsn),
                   "row_xid": xid, "changes": []}
        elif t == "C":
            flags, commit_lsn, end_lsn, ts = struct.unpack_from(">BQQq", data, 1)
            cur.update(flags=flags, commit_lsn=commit_lsn, end_lsn=end_lsn, commit_row_lsn=lsn_int(lsn))
            txs.append(cur)
            cur = None
        elif t in "IUD" and cur is not None:
            cur["changes"].append((t, lsn_int(lsn)))
        elif t in "RYOTM":
            pass
    return txs


def workload(n_tx: int, seed: int = 7) -> dict:
    rnd = random.Random(seed)
    specials = {}
    key = [10 ** 9]

    def ins_small(c, k):
        c.execute("INSERT INTO public.stream_small SELECT nextval('public.stream_small_seq'), g, 1, 1, 1, 1, 0, 0, "
                  "'N', 'O', '1996-03-13', '1996-02-12', '1996-03-22', 'X', 'TRUCK', 'facts' "
                  "FROM generate_series(1, %s) g", (k,))

    def sentinel(c, label):
        ins_small(c, 1)
        x = c.execute("select pg_current_xact_id()::text").fetchone()[0]
        c.commit()
        specials.setdefault("sentinels", []).append((label, int(x)))

    with C.pg_connect("primary", "admin", autocommit=False) as c:
        c.execute("INSERT INTO public.upd_keyed SELECT * FROM public.lineitem LIMIT 20000")
        c.commit()
        kinds = {"insert": 0, "update": 0, "delete": 0, "multi": 0, "with_unpublished": 0}
        for i in range(n_tx):
            r = rnd.random()
            if r < 0.5:
                ins_small(c, rnd.randint(1, 50))
                kinds["insert"] += 1
            elif r < 0.7:
                lo = rnd.randint(1, 4000)
                c.execute("UPDATE public.upd_keyed SET l_comment = md5(random()::text) WHERE l_orderkey BETWEEN %s "
                          "AND %s", (lo, lo + rnd.randint(0, 3)))
                kinds["update"] += 1
            elif r < 0.8:
                lo = rnd.randint(4000, 5000)
                c.execute("DELETE FROM public.upd_keyed WHERE ctid IN (SELECT ctid FROM public.upd_keyed WHERE "
                          "l_orderkey >= %s LIMIT %s)", (lo, rnd.randint(1, 5)))
                kinds["delete"] += 1
            else:
                ins_small(c, rnd.randint(1, 10))
                lo = rnd.randint(1, 4000)
                c.execute("UPDATE public.upd_keyed SET l_comment = 'multi' WHERE l_orderkey = %s", (lo,))
                if rnd.random() < 0.5:
                    key[0] += 1
                    c.execute("INSERT INTO public.stream_unpub VALUES (%s, 'u')", (key[0],))
                    kinds["with_unpublished"] += 1
                kinds["multi"] += 1
            c.commit()
        # special transactions, each between two sentinels
        for rep in range(3):
            sentinel(c, f"before_empty_{rep}")
            c.execute("BEGIN")
            c.commit()  # psycopg: nothing sent but BEGIN/COMMIT
            sentinel(c, f"before_xid_only_{rep}")
            x = c.execute("select pg_current_xact_id()::text").fetchone()[0]
            c.commit()
            specials.setdefault("xid_only", []).append(int(x))
            sentinel(c, f"before_unpublished_only_{rep}")
            key[0] += 1
            c.execute("INSERT INTO public.stream_unpub VALUES (%s, 'u')", (key[0],))
            x = c.execute("select pg_current_xact_id()::text").fetchone()[0]
            c.commit()
            specials.setdefault("unpublished_only", []).append(int(x))
            sentinel(c, f"before_rollback_{rep}")
            ins_small(c, 3)
            x = c.execute("select pg_current_xact_id()::text").fetchone()[0]
            c.rollback()
            specials.setdefault("rolled_back", []).append(int(x))
            sentinel(c, f"before_savepoints_{rep}")
            ins_small(c, 1)
            c.execute("SAVEPOINT s1")
            ins_small(c, 2)
            c.execute("ROLLBACK TO SAVEPOINT s1")
            ins_small(c, 1)
            c.execute("SAVEPOINT s2")
            ins_small(c, 4)
            c.execute("RELEASE SAVEPOINT s2")
            x = c.execute("select pg_current_xact_id()::text").fetchone()[0]
            c.commit()
            specials.setdefault("savepoints", []).append(int(x))  # expect 1 + 1 + 4 = 6 rows
            sentinel(c, f"after_{rep}")
    specials["kinds"] = kinds
    return specials


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--tx", type=int, default=1200)
    ap.add_argument("--node", default="replica")
    a = ap.parse_args()
    node = a.node
    ev = {"utc": C.utc(), "load_avg": C.load_avg(), "node": node}
    S.truncate("stream_small", "upd_keyed")
    for n in ("replica", "primary"):
        S.drop_slot(n)
        S.drop_slot(n, PEEK)
    ev["slot"] = S.create_slot(node)
    ev["peek_slot"] = S.create_slot(node, PEEK)
    out = S.WORK / "facts.jsonl"
    if out.exists():
        out.unlink()
    eng = S.Engine(node, "default", extra={"provide.transaction.metadata": "true"}, label="facts",
                   main="cdc_v2.spike.FactsMain", main_args=(str(out),))
    t0 = time.time()
    while "POLLING_STARTED" not in eng.log_text() and time.time() - t0 < 120:
        time.sleep(0.1)
    sp = workload(a.tx)
    last_sentinel = sp["sentinels"][-1][1]
    deadline = time.time() + 300
    recs = []
    while time.time() < deadline:
        recs = [json.loads(x) for x in out.read_text().splitlines() if x.strip()] if out.exists() else []
        if any(r["topic"].endswith(".transaction") and r["value"].get("status") == "END"
               and str(r["value"].get("id", "")).startswith(f"{last_sentinel}:") for r in recs):
            break
        time.sleep(0.5)
    time.sleep(2)
    recs = [json.loads(x) for x in out.read_text().splitlines() if x.strip()]
    eng.consumer.stdin.write(b"STOP\n")
    eng.consumer.stdin.flush()
    eng.consumer.wait(timeout=60)
    ev["errors"] = eng.errors()
    server = peek(node)
    ev["workload"] = sp
    ev.update(analyse(recs, server, sp))
    # samples
    samp = []
    seen = set()
    for r in recs:
        k = (r["topic"].split(".")[-1], r["value"].get("status") or r["value"].get("op"))
        if k not in seen:
            seen.add(k)
            samp.append(r)
    S.EVID.mkdir(parents=True, exist_ok=True)
    (S.EVID / "facts_sample.jsonl").write_text("".join(C.redact(json.dumps(x)) + "\n" for x in samp))
    (S.EVID / "facts.json").write_text(C.redact(json.dumps(ev, indent=1, default=str)) + "\n")
    print(json.dumps({k: v for k, v in ev.items() if k not in ("workload",)}, indent=1, default=str)[:6000])
    for n in ("replica", "primary"):
        S.drop_slot(n, PEEK)


def analyse(recs, server, sp) -> dict:
    by_x = {t["xid"]: t for t in server}
    begins, ends, changes = {}, {}, {}
    for r in recs:
        v = r["value"]
        if r["topic"].endswith(".transaction"):
            x = int(str(v["id"]).split(":")[0])
            (begins if v["status"] == "BEGIN" else ends)[x] = r
        elif v.get("op") in ("c", "u", "d"):
            x = int(v["source"]["txId"])
            changes.setdefault(x, []).append(r)
    checks = {k: [0, 0] for k in (
        "BEGIN.id = '<xid>:<server B.final_lsn = commit record start>'",
        "BEGIN.id = '<xid>:<server C.end_lsn>'",
        "END.id = BEGIN.id",
        "END.event_count = number of change events = server I/U/D count",
        "END.data_collections counts = per-table change events",
        "change source.lsn = server row lsn of that change",
        "change source.txId = server xid",
        "change source.sequence[1] = source.lsn",
        "change source.sequence[0] = previous transaction's commit end LSN (server C.end_lsn)",
        "change offset.lsn = source.lsn",
        "change offset.lsn_proc = source.lsn",
        "change offset.lsn_commit = previous transaction's commit end LSN",
        "change offset.lsn_commit = this transaction's commit end LSN",
        "END offset.lsn_commit = this transaction's server C.end_lsn",
        "END offset.lsn = this transaction's server C.end_lsn",
        "END offset.lsn_proc = this transaction's server C.end_lsn",
        "END offset.lsn_commit = server C.commit_lsn (commit record start)",
        "BEGIN offset.lsn_commit = previous transaction's commit end LSN",
        "any Debezium field = server C.commit_lsn (commit record start)",
        "server C.commit_lsn = B.final_lsn",
        "server commit row lsn (SQL interface) = C.end_lsn",
    )}

    def chk(name, cond):
        checks[name][1] += 1
        checks[name][0] += bool(cond)

    prev_end = None
    matched = 0
    order = sorted(server, key=lambda t: t["end_lsn"])
    for t in order:
        x = t["xid"]
        chk("server C.commit_lsn = B.final_lsn", t["commit_lsn"] == t["final_lsn"])
        chk("server commit row lsn (SQL interface) = C.end_lsn", t["commit_row_lsn"] == t["end_lsn"])
        b, e, ch = begins.get(x), ends.get(x), changes.get(x, [])
        if b and e:
            matched += 1
            bid_lsn = int(str(b["value"]["id"]).split(":")[1])
            chk("BEGIN.id = '<xid>:<server B.final_lsn = commit record start>'", bid_lsn == t["final_lsn"])
            chk("BEGIN.id = '<xid>:<server C.end_lsn>'", bid_lsn == t["end_lsn"])
            chk("END.id = BEGIN.id", e["value"]["id"] == b["value"]["id"])
            chk("END.event_count = number of change events = server I/U/D count",
                e["value"]["event_count"] == len(ch) == len(t["changes"]))
            per = {}
            for r in ch:
                k = r["value"]["source"]["schema"] + "." + r["value"]["source"]["table"]
                per[k] = per.get(k, 0) + 1
            dc = {d["data_collection"]: d["event_count"] for d in e["value"]["data_collections"] or []}
            chk("END.data_collections counts = per-table change events", dc == per)
            eo, bo = e["offset"], b["offset"]
            chk("END offset.lsn_commit = this transaction's server C.end_lsn", eo.get("lsn_commit") == t["end_lsn"])
            chk("END offset.lsn = this transaction's server C.end_lsn", eo.get("lsn") == t["end_lsn"])
            chk("END offset.lsn_proc = this transaction's server C.end_lsn", eo.get("lsn_proc") == t["end_lsn"])
            chk("END offset.lsn_commit = server C.commit_lsn (commit record start)",
                eo.get("lsn_commit") == t["commit_lsn"])
            if prev_end is not None:
                chk("BEGIN offset.lsn_commit = previous transaction's commit end LSN", bo.get("lsn_commit") == prev_end)
            vals = set()
            for r in [b, e] + ch:
                for d in (r["offset"], r["value"].get("source") or {}):
                    for k, v in d.items():
                        if isinstance(v, int):
                            vals.add(v)
                        elif k == "sequence" and v:
                            vals.update(int(s) for s in json.loads(v) if s)
            chk("any Debezium field = server C.commit_lsn (commit record start)", t["commit_lsn"] in vals)
            for i, r in enumerate(ch):
                src, off = r["value"]["source"], r["offset"]
                srv_lsn = t["changes"][i][1] if i < len(t["changes"]) else None
                chk("change source.lsn = server row lsn of that change", src["lsn"] == srv_lsn)
                chk("change source.txId = server xid", int(src["txId"]) == x)
                seq = json.loads(src["sequence"]) if src.get("sequence") else [None, None]
                chk("change source.sequence[1] = source.lsn", seq[1] is not None and int(seq[1]) == src["lsn"])
                if prev_end is not None:
                    chk("change source.sequence[0] = previous transaction's commit end LSN (server C.end_lsn)",
                        seq[0] is not None and int(seq[0]) == prev_end)
                    chk("change offset.lsn_commit = previous transaction's commit end LSN",
                        off.get("lsn_commit") == prev_end)
                chk("change offset.lsn_commit = this transaction's commit end LSN", off.get("lsn_commit") == t["end_lsn"])
                chk("change offset.lsn = source.lsn", off.get("lsn") == src["lsn"])
                chk("change offset.lsn_proc = source.lsn", off.get("lsn_proc") == src["lsn"])
        prev_end = t["end_lsn"]
    # special transactions
    special = {}
    for kind in ("xid_only", "unpublished_only", "rolled_back", "savepoints"):
        special[kind] = [{"xid": x, "server_tx": bool(by_x.get(x)),
                          "server_changes": len(by_x[x]["changes"]) if x in by_x else 0,
                          "debezium_begin": x in begins, "debezium_end": x in ends,
                          "debezium_changes": len(changes.get(x, [])),
                          "end_event_count": ends[x]["value"]["event_count"] if x in ends else None}
                         for x in sp.get(kind, [])]
    # what Debezium delivered between consecutive sentinels around the empty transaction
    sent = dict(sp["sentinels"])
    between = []
    for rep in range(3):
        a, b = sent[f"before_empty_{rep}"], sent[f"before_xid_only_{rep}"]
        idx = [i for i, r in enumerate(recs) if r["topic"].endswith(".transaction")
               and str(r["value"]["id"]).split(":")[0] in (str(a), str(b)) and r["value"]["status"] == "END"]
        between.append({"records_between_sentinel_END_and_next_sentinel": (idx[1] - idx[0] - 1) if len(idx) == 2 else None})
    special["empty_transaction"] = between
    other_topics = sorted({r["topic"] for r in recs})
    n_srv_with_changes = sum(1 for t in server if t["changes"])
    return {"records": len(recs), "topics": other_topics, "server_transactions": len(server),
            "server_transactions_with_changes": n_srv_with_changes, "debezium_begin_events": len(begins),
            "debezium_end_events": len(ends), "matched_transactions": matched,
            "debezium_tx_without_server_tx": sorted(set(ends) - set(by_x))[:10],
            "checks": {k: {"true": v[0], "of": v[1]} for k, v in checks.items()}, "special": special}


if __name__ == "__main__":
    main()
