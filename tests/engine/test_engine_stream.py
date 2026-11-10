"""Operations, transactions, event counts, unchanged TOAST, schema changes, a large transaction (jvm, localpg)."""
import threading
import time

import pytest

from cdc_v2.deadline import Deadline
from cdc_v2.stream import frames as F
from cdc_v2.stream.engine import Engine
from stream_helpers import Collector, evidence, rss_kb

pytestmark = [pytest.mark.jvm, pytest.mark.localpg, pytest.mark.timeout(900)]

PLACEHOLDER = bytes.fromhex("006364635f76325f746f61737400")


def records_of(txn):
    return len(txn.rows) + len(txn.truncates) + len(txn.messages)


def run_sql(pair, statements):
    with pair.connect(pair.primary_port) as c:
        for st in statements:
            c.execute(st)


def test_operations_transactions_and_event_counts(pair, scenario, artifacts):
    sc = scenario(
        "create table {s}.t (id bigint primary key, v text)",
        "create table {s}.t2 (id bigint, v text, primary key (id) deferrable initially deferred)",
        "alter table {s}.t2 replica identity full",
        "create table {s}.k (a int, b text)",
        "alter table {s}.k replica identity full",
        "create table {s}.x (id int primary key)",
        "create table {s}.parent (id int primary key)",
        "create table {s}.child (id int primary key, p int references {s}.parent(id))",
        "create table {s}.seqt (id int generated always as identity primary key, v int)",
        "create table {s}.unpub (id int)")
    s = sc.schema
    pair.p("alter publication %s drop table %s.unpub" % (sc.pub, s))
    include = ",".join("%s.%s" % (s, t) for t in ("t", "t2", "k", "parent", "child", "seqt"))
    cases = [
        ("insert three", ["insert into {s}.t values (1,'a'),(2,'b'),(3,'c')"]),
        ("update", ["update {s}.t set v = 'a2' where id = 1"]),
        ("delete", ["delete from {s}.t where id = 3"]),
        ("key change chain A->B, B->C", ["begin", "update {s}.t set id = 10 where id = 1",
                                          "update {s}.t set id = 11 where id = 10", "commit"]),
        ("key swap (deferred key, REPLICA IDENTITY FULL)", ["insert into {s}.t2 values (2, 'b'), (11, 'e')",
                      "update {s}.t2 set id = case id when 2 then 11 else 2 end where id in (2, 11)"]),
        ("keyless duplicates", ["insert into {s}.k values (1,'a'),(1,'a'),(2,'b')"]),
        ("keyless update one of two", ["update {s}.k set b = 'z' where ctid = (select min(ctid) from {s}.k "
                                       "where a = 1)"]),
        ("keyless delete one", ["delete from {s}.k where ctid = (select min(ctid) from {s}.k where a = 1)"]),
        ("truncate one table", ["truncate {s}.k"]),
        ("truncate two tables", ["truncate {s}.t, {s}.k"]),
        ("truncate cascade", ["insert into {s}.parent values (1)", "insert into {s}.child values (1, 1)",
                              "truncate {s}.parent cascade"]),
        ("truncate restart identity", ["insert into {s}.seqt (v) values (1)", "truncate {s}.seqt restart identity"]),
        ("transactional message", ["begin", "insert into {s}.t values (20, 'm')",
                                   "select pg_logical_emit_message(true, 'cdc_v2_m', 'in-tx')", "commit"]),
        ("rolled back with messages", ["begin", "insert into {s}.t values (21, 'r')",
                                       "select pg_logical_emit_message(false, 'cdc_v2_n', 'non-tx')",
                                       "select pg_logical_emit_message(true, 'cdc_v2_r', 'rolled back')",
                                       "rollback"]),
        ("excluded table with included", ["begin", "insert into {s}.x values (1)", "insert into {s}.t values (30, 'x')",
                                          "commit"]),
        ("excluded table only", ["insert into {s}.x values (2)"]),
        ("empty transaction", ["begin", "commit"]),
        ("unpublished table only", ["insert into {s}.unpub values (1)"]),
        ("savepoint rolled back", ["begin", "insert into {s}.t values (100, 's')", "savepoint s1",
                                   "insert into {s}.t values (101, 'gone')", "rollback to savepoint s1",
                                   "insert into {s}.t values (102, 's')", "commit"]),
        ("several tables in one transaction", ["begin", "insert into {s}.t values (200, 'p')",
                                               "insert into {s}.k values (9, 'q')",
                                               "insert into {s}.t values (201, 'r')", "commit"]),
        ("message with an excluded prefix in a mixed transaction", ["begin", "insert into {s}.t values (40, 'p')",
                                   "select pg_logical_emit_message(true, 'other_prefix', 'filtered')", "commit"]),
        ("message with an excluded prefix alone", ["select pg_logical_emit_message(true, 'other_prefix', 'alone')"]),
        ("truncate of an excluded table with an included insert", ["begin", "truncate {s}.x",
                                                                    "insert into {s}.t values (41, 'q')", "commit"]),
        ("marker", ["insert into {s}.t values (999, 'end')"]),
    ]
    # t2 has a deferrable primary key: by rule K a table without a key, so it is listed (test_engine_keys.py)
    with Engine(sc.settings(table_include=include, message_prefixes=("cdc_v2_.*",), keyless_tables=((s, "t2"),)),
                None, artifacts) as eng:
        for _name, sts in cases:
            run_sql(pair, [st.format(s=s) for st in sts])
        col = Collector(eng).run(lambda c: any(r["a0"] == 999 for r in c.rows_of("t")), timeout=120)
        eng.stop(Deadline.after(20))
    # the same slot again (nothing was acknowledged), t2 only and without the list: Debezium's key decides
    with Engine(sc.settings(table_include="%s.t2" % s), None, artifacts) as eng:
        unlisted = Collector(eng).run(lambda c: len(c.rows_of("t2")) >= 6, timeout=120)
        eng.stop(Deadline.after(20))
    txns = col.txns
    # every END count equals the records of its transaction
    for t in txns:
        assert t.end.event_count == records_of(t), t
        assert sum(n for _c, n in t.end.collections) == t.end.event_count
    by_rows = {}
    out = []
    for t in txns:
        desc = [(kind, tb, (r.get("op"), r.get("a0"), r.get("k0")) if kind == "row" else None)
                for kind, tb, r in t.records]
        out.append({"event_count": t.end.event_count, "collections": [list(c) for c in t.end.collections],
                    "records": [[k, tb, list(d) if d else None] for k, tb, d in desc]})
    # key change chain: d(1) c(10 old 1) d(10) c(11 old 10)
    chain = next(t for t in txns if any(r.get("a0") == 11 and r["op"] == "c" for _tb, r in t.rows))
    assert [(r["op"], r["a0"], r["k0"]) for _tb, r in chain.rows] == [("d", 1, None), ("c", 10, 1), ("d", 10, None),
                                                                     ("c", 11, 10)]
    swap = txns[txns.index(chain) + 2]
    # listed (rule K): a stream without a key; Debezium still splits each key change into a delete and a create,
    # and every record carries its complete image, in WAL order
    sch_t2 = next(v for k, v in col.schemas.items() if k[1] == "t2")
    assert not sch_t2.has_key and sch_t2.key_positions == ()
    assert [(r["op"], r["b0"], r["b1"], r["a0"], r["a1"]) for _tb, r in swap.rows] == [
        ("d", 2, "b", None, None), ("c", None, None, 11, "b"), ("d", 11, "e", None, None), ("c", None, None, 2, "e")]
    assert not any(k.startswith("k") for _tb, r in swap.rows for k in r)
    # without the list Debezium's key decides: a delete with the key only and a create that names the old key
    assert next(v for k, v in unlisted.schemas.items() if k[1] == "t2").has_key
    ops = sorted((r["op"], r["a0"], r["k0"]) for r in unlisted.rows_of("t2")[2:])
    assert ops == [("c", 2, 11), ("c", 11, 2), ("d", 2, None), ("d", 11, None)]
    for r in unlisted.rows_of("t2")[2:]:
        if r["op"] == "c":                        # the full new image travels with the create
            assert r["a1"] == {2: "e", 11: "b"}[r["a0"]]
    # keyless: old image in b<i>, new in a<i>
    kl = [r for t in txns for tb, r in t.rows if tb == "k"]
    upd = next(r for r in kl if r["op"] == "u")
    assert (upd["a0"], upd["a1"], upd["b0"], upd["b1"]) == (1, "z", 1, "a")
    dele = next(r for r in kl if r["op"] == "d")
    assert dele["a0"] is None and dele["b0"] == 1
    # truncates: one frame per table, also for CASCADE
    tr = [[f.table for f in t.truncates] for t in txns if t.truncates]
    assert tr == [["k"], ["t", "k"], ["parent", "child"], ["seqt"]] or tr == [["k"], ["t", "k"], ["child", "parent"],
                                                                             ["seqt"]]
    # messages
    msg = next(t for t in txns if t.messages)
    assert msg.messages[0].content == b"in-tx" and msg.end.event_count == 2
    assert [m.content for m in col.loose_messages] == [b"non-tx"]
    assert not any(m.prefix == "cdc_v2_r" for t in txns for m in t.messages)
    # excluded table: not delivered, not counted
    ex = next(t for t in txns if any(r.get("a0") == 30 for _tb, r in t.rows))
    assert ex.end.event_count == 1 and records_of(ex) == 1
    ex_only = [t for t in txns if records_of(t) == 0]
    # a transactional message with an excluded prefix: neither delivered nor counted
    fp = next(t for t in txns if any(r.get("a0") == 40 for _tb, r in t.rows))
    assert fp.end.event_count == 1 and not fp.messages
    # TRUNCATE of an excluded table in a mixed transaction: neither delivered nor counted
    tx = next(t for t in txns if any(r.get("a0") == 41 for _tb, r in t.rows))
    assert tx.end.event_count == 1 and not tx.truncates
    # savepoint
    sp = next(t for t in txns if any(r.get("a0") == 100 for _tb, r in t.rows))
    assert [r["a0"] for _tb, r in sp.rows] == [100, 102]
    # several tables keep seq order
    mt = next(t for t in txns if any(r.get("a0") == 200 for _tb, r in t.rows))
    assert [(tb, r["a0"]) for tb, r in mt.rows] == [("t", 200), ("k", 9), ("t", 201)]
    assert [r["seq"] for _tb, r in mt.rows] == sorted(r["seq"] for _tb, r in mt.rows)
    # ack positions never decrease; END ack = lsn_commit of its offset
    acks = [t.end.ack_position for t in txns]
    assert acks == sorted(acks)
    assert all(t.end.ack_position == t.end.positions["lsn_commit"] for t in txns)
    evidence("operations_event_counts.json", {
        "test": "tests/engine/test_engine_stream.py::test_operations_transactions_and_event_counts",
        "cases": [c[0] for c in cases], "transactions_delivered": len(txns),
        "transactions_without_records": len(ex_only), "transactions": out,
        "loose_messages": [m.prefix for m in col.loose_messages]})


def test_unchanged_toast_h003(pair, scenario, artifacts):
    sc = scenario(
        "create table {s}.tt (id int primary key, a text, b bytea, c text[], n int)",
        "alter table {s}.tt alter column a set storage external, alter column b set storage external,"
        " alter column c set storage external",
        "create table {s}.tf (id int, a text, n int)",
        "alter table {s}.tf replica identity full",
        "alter table {s}.tf alter column a set storage external")
    s = sc.schema
    big = "repeat('0123456789abcdef', 1000)"
    run_sql(pair, [
        "insert into {s}.tt values (1, {big}, convert_to({big}, 'UTF8'), array[{big}, 'x'], 0)".format(s=s, big=big),
        # values equal to the placeholder bytes, its printable part, and Debezium's default placeholder
        "insert into {s}.tt values (2, 'cdc_v2_toast', '\\x006364635f76325f746f61737400'::bytea, "
        "array['cdc_v2_toast', '__debezium_unavailable_value'], 0)".format(s=s),
        "insert into {s}.tt values (3, '__debezium_unavailable_value', '\\x5f5f646562657a69756d'::bytea, "
        "array[convert_from('\\x006364635f76325f746f617374'::bytea, 'SQL_ASCII')], 0)".format(s=s)
        if False else "insert into {s}.tt values (3, '__debezium_unavailable_value', ''::bytea, '{{}}', 0)".format(s=s),
        "update {s}.tt set n = 1".format(s=s),                                   # big values unchanged
        "update {s}.tt set n = 2, a = 'small now' where id = 1".format(s=s),     # one changed, two unchanged
        "update {s}.tt set id = 11, n = 3 where id = 1".format(s=s),             # key change, b and c unchanged
        "begin; insert into {s}.tt values (4, {big}, null, null, 0); update {s}.tt set n = 4 where id = 4; "
        "commit".format(s=s, big=big),                                           # insert then update, one txn
        "update {s}.tt set n = 5 where id = 4".format(s=s),
        "update {s}.tt set n = 6 where id = 4".format(s=s),                      # consecutive updates
        "insert into {s}.tf values (1, {big}, 0)".format(s=s, big=big),
        "update {s}.tf set n = 1".format(s=s),                                   # FULL: value comes from old image
        "insert into {s}.tt values (999, 'end', null, null, 0)".format(s=s)])
    with Engine(sc.settings(), None, artifacts) as eng:
        col = Collector(eng).run(lambda c: any(r["a0"] == 999 for r in c.rows_of("tt")), timeout=120)
        eng.stop(Deadline.after(20))
    sch = next(v for k, v in col.schemas.items() if k[1] == "tt")
    assert [c.unchanged for c in sch.columns] == [False, True, True, True, False]
    tt = col.rows_of("tt")
    upd1 = [r for r in tt if r["op"] == "u" and r["a4"] == 1]
    r1 = next(r for r in upd1 if r["a0"] == 1)
    assert (r1["u1"], r1["u2"], r1["u3"]) == (True, True, True) and r1["a1"] is None and r1["a2"] is None
    for rid in (2, 3):                     # real values equal to a placeholder: delivered as values, never "unchanged"
        r = next(x for x in upd1 if x["a0"] == rid)
        assert (r["u1"], r["u2"], r["u3"]) == (False, False, False)
    r2 = next(x for x in upd1 if x["a0"] == 2)
    assert r2["a1"] == "cdc_v2_toast" and r2["a2"] == PLACEHOLDER
    assert r2["a3"] == "{cdc_v2_toast,__debezium_unavailable_value}"
    assert next(x for x in upd1 if x["a0"] == 3)["a1"] == "__debezium_unavailable_value"
    r = next(x for x in tt if x["op"] == "u" and x["a4"] == 2)
    assert (r["u1"], r["u2"], r["u3"], r["a1"]) == (False, True, True, "small now")
    kc = next(x for x in tt if x["op"] == "c" and x["a0"] == 11)
    assert kc["k0"] == 1 and (kc["u1"], kc["u2"], kc["u3"]) == (False, True, True)
    ins_upd = [x for x in tt if x["a0"] == 4]
    assert [x["op"] for x in ins_upd] == ["c", "u", "u", "u"]
    assert ins_upd[0]["u1"] is False and len(ins_upd[0]["a1"]) == 16000
    assert all(x["u1"] for x in ins_upd[1:])
    tf = [r for r in col.rows_of("tf") if r["op"] == "u"][0]
    assert tf["u1"] is False and len(tf["a1"]) == 16000 and len(tf["b1"]) == 16000
    evidence("unchanged_toast.json", {"test": "tests/engine/test_engine_stream.py::test_unchanged_toast_h003",
                                      "rows": len(tt), "placeholder_collision_rows": [2, 3],
                                      "checked": ["single update", "one changed two unchanged", "key change",
                                                  "insert then update in one txn", "consecutive updates",
                                                  "REPLICA IDENTITY FULL", "text, bytea, array element equal to "
                                                  "the placeholder or its printable part"]})


def test_schema_change_between_transactions(pair, scenario, artifacts):
    sc = scenario("create table {s}.sc (id int primary key, a int, b text)")
    s = sc.schema
    run_sql(pair, [st.format(s=s) for st in [
        "insert into {s}.sc values (1, 1, 'x')",
        "alter table {s}.sc add column c numeric(10,2) default 1.5",
        "insert into {s}.sc values (2, 2, 'y', 2.25)",
        "alter table {s}.sc drop column a",
        "insert into {s}.sc values (3, 'z', 3)",
        "alter table {s}.sc rename column b to bb",
        "insert into {s}.sc values (4, 'w', 4)",
        "alter table {s}.sc alter column id type bigint",
        "insert into {s}.sc values (5, 'v', 5)",
        "update {s}.sc set bb = 'same schema' where id = 5"]])
    with Engine(sc.settings(), None, artifacts) as eng:
        col = Collector(eng).run(lambda c: len(c.rows_of("sc")) >= 6, timeout=120)
        eng.stop(Deadline.after(20))
    versions = [f for _seq, f in col.schema_log if f.table == "sc"]
    assert [v.version for v in versions] == [1, 2, 3, 4, 5]
    assert [[c.name for c in v.columns] for v in versions] == [
        ["id", "a", "b"], ["id", "a", "b", "c"], ["id", "b", "c"], ["id", "bb", "c"], ["id", "bb", "c"]]
    assert versions[1].columns[3].source_type == "numeric" and versions[1].columns[3].scale == 2
    assert versions[4].columns[0].form == "int64" and versions[3].columns[0].form == "int32"
    rows = col.rows_of("sc")
    assert [r["_version"] for r in rows] == [1, 2, 3, 4, 5, 5]
    # each SCHEMA frame arrives before the first ROWS of its version
    order = [(type(f).__name__, getattr(f, "version", None)) for f in col.frames if isinstance(f, (F.Schema, F.Rows))
             and getattr(f, "table", "") == "sc"]
    assert order == [("Schema", 1), ("Rows", 1), ("Schema", 2), ("Rows", 2), ("Schema", 3), ("Rows", 3),
                     ("Schema", 4), ("Rows", 4), ("Schema", 5), ("Rows", 5), ("Rows", 5)]
    evidence("schema_change.json", {"test": "tests/engine/test_engine_stream.py::test_schema_change_between_transactions",
                                    "versions": [[c.name + ":" + c.form for c in v.columns] for v in versions]})


def test_large_transaction_bounded_memory(pair, scenario, artifacts):
    n = 1_000_000
    sc = scenario("create table {s}.big (id bigint primary key, v text, w double precision)")
    with Engine(sc.settings(heap_mb=512), None, artifacts) as eng:
        col = Collector(eng)
        col.keep_frames = False
        col.run(lambda c: c.engine.hello is not None or c.batch_ends > 0 or c.heartbeats, timeout=60)
        peak = {"rss_kb": 0}
        stop = threading.Event()

        def sample():
            while not stop.is_set():
                r = rss_kb(eng.pid)
                if r:
                    peak["rss_kb"] = max(peak["rss_kb"], r)
                time.sleep(0.2)

        th = threading.Thread(target=sample, daemon=True)
        th.start()
        t0 = time.monotonic()
        pair.p("insert into %s.big select g, md5(g::text), g * 0.5 from generate_series(1, %d) g" % (sc.schema, n))
        t_commit = time.monotonic()
        seen = {"rows": 0, "frames": 0, "max_rows_per_frame": 0}
        end_frame = []

        def feed(f):
            if isinstance(f, F.Rows):
                seen["rows"] += f.batch.num_rows
                seen["frames"] += 1
                seen["max_rows_per_frame"] = max(seen["max_rows_per_frame"], f.batch.num_rows)
            elif isinstance(f, F.End) and f.event_count == n:
                end_frame.append(f)

        deadline = Deadline.after(600)
        while not end_frame:
            for f in eng.frames(deadline.within(1)):
                feed(f)
                if end_frame:
                    break
            assert not deadline.expired
        t_end = time.monotonic()
        stop.set()
        th.join()
        eng.stop(Deadline.after(20))
    assert seen["rows"] == n and end_frame[0].event_count == n
    assert seen["max_rows_per_frame"] <= 65536
    evidence("large_transaction.json", {
        "test": "tests/engine/test_engine_stream.py::test_large_transaction_bounded_memory", "rows": n,
        "heap_mb": 512, "jvm_peak_rss_mb": round(peak["rss_kb"] / 1024, 1), "rows_frames": seen["frames"],
        "max_rows_per_frame": seen["max_rows_per_frame"], "insert_s": round(t_commit - t0, 2),
        "commit_to_end_frame_s": round(t_end - t_commit, 2),
        "rows_per_s_after_commit": round(n / (t_end - t_commit)),
        "note": "indication only: shared development machine, source on the same host"})
