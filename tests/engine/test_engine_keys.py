"""Rule K in the engine (package K): what Debezium calls a key, and what the list of tables without a key changes
(jvm, localpg; the private pair pg-e).

Five table shapes, all with REPLICA IDENTITY FULL, receive the same changes. One engine reads them without the list
of tables without a key, then a second engine reads the same slot (no DURABLE was sent, so the slot did not move)
with all five tables listed. With the list every table is a table without a key, and replaying its records (old
image out, new image in) in arrival order must give exactly the rows the source holds at the end.
"""
import collections

import pytest

from cdc_v2.deadline import Deadline
from cdc_v2.stream.engine import Engine, EngineFatal
from stream_helpers import Collector, evidence

pytestmark = [pytest.mark.jvm, pytest.mark.localpg, pytest.mark.timeout(600)]

#: section 3 of docs/tasks/p3_k_key_rule.md: name -> (what it is, DDL)
CASES = {
    "dpk": ("deferrable primary key",
            "create table {s}.dpk (id int, v text, u int, primary key (id) deferrable initially deferred)"),
    "uix": ("no primary key, unique index on a NOT NULL column",
            "create table {s}.uix (id int not null, v text, u int); create unique index on {s}.uix (id)"),
    "uixn": ("no primary key, unique index on a nullable column",
             "create table {s}.uixn (id int, v text, u int); create unique index on {s}.uixn (id)"),
    "duc": ("no primary key, deferrable unique constraint",
            "create table {s}.duc (id int not null, v text, u int, constraint duc_id unique (id) deferrable)"),
    "none": ("no primary key, no unique index (control)", "create table {s}.none (id int, v text, u int)"),
}

OPERATIONS = [
    ("insert", "insert into {t} values (1, 'a', 1), (2, 'b', 2)"),
    ("update of a non-key column", "update {t} set u = 11 where id = 1"),
    ("update of the key column", "update {t} set id = 10 where id = 1"),
    ("delete", "delete from {t} where id = 2"),
]

#: case 1 only: the two transactions of tests/pg_source/test_pg_keys.py that pass through two rows with one key
DEFERRED = [
    ("rows for the swap", "insert into {t} values (1, 'p', 1), (2, 'q', 2)"),
    ("direct swap", "update {t} set id = 3 - id where id in (1, 2)"),
    ("insert a duplicate, then delete the old row",
     "begin; insert into {t} values (2, 'dup', 0); delete from {t} where v = 'p'; commit"),
]

#: case 3 only: a nullable unique column holds NULL in several rows
NULLS = [
    ("two rows with a NULL key", "insert into {t} values (null, 'n', 0), (null, 'n', 0)"),
    ("delete one of them", "delete from {t} where ctid = (select min(ctid) from {t} where id is null)"),
]


def schema_frame(col, table):
    frames = [f for (s, t, _v), f in col.schemas.items() if t == table]
    assert len(frames) == 1, frames
    return frames[0]


def describe(col, table):
    """The SCHEMA facts and the rows of one table, column sets by name, for the evidence."""
    sch = schema_frame(col, table)
    rows = []
    for r in col.rows_of(table):
        sets = collections.defaultdict(list)
        for name in sorted(k for k in r if k[0] in "abuk" and k[1:].isdigit()):
            sets[name[0]].append(r[name])
        rows.append({"op": r["op"], **{k: v for k, v in sorted(sets.items())}})
    return {"has_key": sch.has_key, "key_positions": list(sch.key_positions),
            "key_flags": [c.key for c in sch.columns], "rows": rows}


def replay(col, table, ncols):
    """Apply the records of a table without a key in arrival order: the old image (b<i>) leaves, the new image
    (a<i>) arrives. Every old image must find its row. Returns the final multiset and the largest number of rows
    that held one key (column 0) after any record."""
    held = collections.Counter()
    worst = 0
    for r in col.rows_of(table):
        new = tuple(r["a%d" % i] for i in range(ncols))
        old = tuple(r["b%d" % i] for i in range(ncols))
        if r["op"] in ("u", "d"):
            assert held[old] > 0, ("old image of a row that is not there", table, r)
            held[old] -= 1
        else:
            assert all(v is None for v in old), r
        if r["op"] in ("c", "u"):
            held[new] += 1
        else:
            assert all(v is None for v in new), r
        per_key = collections.Counter(row[0] for row, n in held.items() for _ in range(n) if row[0] is not None)
        worst = max([worst] + list(per_key.values()))
    return +held, worst


def assert_without_a_key(col, table, source_rows, ncols=3):
    """A stream without a key: has_key 0, no key positions, the old image in b<i>, no k<i>, and a replay of the
    records gives the rows the source holds."""
    sch = schema_frame(col, table)
    assert not sch.has_key and sch.key_positions == () and not any(c.key for c in sch.columns), sch
    for r in col.rows_of(table):
        assert all("b%d" % i in r for i in range(ncols)) and not any(k.startswith("k") for k in r), r
    final, worst = replay(col, table, ncols)
    assert final == collections.Counter(source_rows), (table, final, source_rows)
    return worst


def run_engine(sc, until, **kw):
    with Engine(sc.settings(**kw), None, sc.pair_artifacts) as eng:
        col = Collector(eng).run(until, timeout=120)
        eng.stop(Deadline.after(20))
    return col


def test_a_listed_table_without_an_old_image(pair, scenario, artifacts):
    """A list that names a table whose records do not carry a complete old image (here REPLICA IDENTITY USING
    INDEX; the same holds for DEFAULT). pgoutput sends an old image only when an identity column changes or the row
    is deleted, and then only the identity columns. An update without any old image stops the engine with FATAL
    (naming the table, never a value). An old image that holds only the identity columns cannot be told from one
    with NULLs: the engine passes it on; the prevention is the catalog (publication.check refuses such a table, and
    a change of replica identity is a schema change)."""
    sc = scenario("create table {s}.ri (id int not null, v text, u int)",
                  "create unique index ri_id on {s}.ri (id)",
                  "alter table {s}.ri replica identity using index ri_id")
    s = sc.schema
    with pair.connect(pair.primary_port) as c:
        for sql in ("insert into {s}.ri values (1, 'hidden-value-1', 1), (2, 'hidden-value-2', 2)",
                    "delete from {s}.ri where id = 2", "update {s}.ri set id = 10 where id = 1",
                    "update {s}.ri set u = 3"):
            c.execute(sql.format(s=s))
    with Engine(sc.settings(keyless_tables=((s, "ri"),)), None, artifacts) as eng:
        col = Collector(eng)
        with pytest.raises(EngineFatal) as e:
            col.run(lambda c: False, timeout=120)
    fatal = e.value.frame
    assert fatal.message.startswith("update without an old image in %s.ri, a table without a key" % s), fatal
    assert (fatal.schema, fatal.table) == (s, "ri")
    assert "hidden-value" not in str(e.value) and not any("hidden-value" in line for line in eng.log_lines)
    ri = [(r["op"], [r["b%d" % i] for i in range(3)], [r["a%d" % i] for i in range(3)]) for r in col.rows_of("ri")]
    assert ri == [("c", [None] * 3, [1, "hidden-value-1", 1]), ("c", [None] * 3, [2, "hidden-value-2", 2]),
                  ("d", [2, None, None], [None] * 3),                     # only the identity column
                  ("d", [1, None, None], [None] * 3),                     # the identity change: Debezium's key
                  ("c", [None] * 3, [10, "hidden-value-1", 1])], ri       # splits it into a delete and a create
    evidence("engine_incomplete_old_image.json", {
        "test": "tests/engine/test_engine_keys.py::test_a_listed_table_without_an_old_image",
        "table": "no primary key, REPLICA IDENTITY USING INDEX on a unique NOT NULL column, listed",
        "delete_old_image": ri[2][1], "update_of_the_identity_column": [list(x) for x in ri[3:]],
        "update_of_another_column": {"fatal_class": fatal.error_class,
                                     "fatal_message": fatal.message.replace(s, "<schema>")},
        "note": "an identity-only old image arrives with NULLs in the other columns and is not detectable in the "
                "engine; publication.check refuses a table without a key by rule K that is not REPLICA IDENTITY "
                "FULL"})


def test_what_debezium_calls_a_key_and_the_list(pair, scenario, artifacts):
    sc = scenario(*[ddl for _what, ddl in CASES.values()],
                  *["alter table {s}.%s replica identity full" % t for t in CASES],
                  "create table {s}.m (id int primary key)")
    sc.pair_artifacts = artifacts
    s = sc.schema
    statements = []
    for t in CASES:
        statements += [(t, what, sql.format(t="%s.%s" % (s, t))) for what, sql in OPERATIONS]
    statements += [("dpk", what, sql.format(t="%s.dpk" % s)) for what, sql in DEFERRED]
    statements += [("uixn", what, sql.format(t="%s.uixn" % s)) for what, sql in NULLS]
    statements.append(("m", "marker", "insert into %s.m values (999)" % s))
    with pair.connect(pair.primary_port) as c:
        for _t, _what, sql in statements:
            c.execute(sql)
    source = {t: [tuple(r) for r in pair.p("select id, v, u from %s.%s" % (s, t))] for t in CASES}

    def marker(c):
        return any(r["a0"] == 999 for r in c.rows_of("m"))

    without = run_engine(sc, marker)
    with_list = run_engine(sc, marker, keyless_tables=tuple((s, t) for t in CASES))

    out = {"without_list": {t: describe(without, t) for t in CASES},
           "with_list": {t: describe(with_list, t) for t in CASES}}
    # the same input both times: the slot did not move between the two engines
    assert ([len(without.rows_of(t)) for t in CASES] == [len(with_list.rows_of(t)) for t in CASES])

    # ---- without the list: what Debezium calls a key. Debezium takes the primary key, also a deferrable one, and
    # without a primary key the first unique index, also on a nullable column and also a deferrable constraint
    keyed = {t for t in CASES if out["without_list"][t]["has_key"]}
    assert keyed == {"dpk", "uix", "uixn", "duc"}, keyed
    for t in sorted(keyed):
        d = out["without_list"][t]
        assert d["key_positions"] == [0] and d["key_flags"] == [True, False, False], (t, d)
        assert not any("b" in r for r in d["rows"]), t            # no old image: the engine followed the key
        ops = [(r["op"], r["a"][0], r["k"][0]) for r in d["rows"]]
        # the update of the key column: a delete that carries only the key, and a create that names the old key
        assert ops[:6] == [("c", 1, None), ("c", 2, None), ("u", 1, None), ("d", 1, None), ("c", 10, 1),
                           ("d", 2, None)], (t, ops)
        assert d["rows"][3]["a"] == [1, None, None], t
        # negative control: the check for a stream without a key fails for these tables when the list is left out
        with pytest.raises(AssertionError):
            assert_without_a_key(without, t, source[t])
    nul = [r for r in out["without_list"]["uixn"]["rows"] if r["a"][1] == "n" or r["a"] == [None] * 3]
    assert [(r["op"], r["a"]) for r in nul] == [("c", [None, "n", 0]), ("c", [None, "n", 0]),
                                                ("d", [None, None, None])], nul   # a delete that names no row
    assert_without_a_key(without, "none", source["none"])            # Debezium's own keyless table: as with it
    assert out["with_list"]["none"] == out["without_list"]["none"]

    # ---- with the list: every table is a table without a key, complete images in WAL order
    worst = {t: assert_without_a_key(with_list, t, source[t]) for t in CASES}
    assert worst["dpk"] == 2                      # the stream shows two rows with key 2 inside the swap
    for t in CASES:                               # the update of the key column
        rows = out["with_list"][t]["rows"]
        if t in keyed:                            # Debezium still splits it: a delete and a create, both complete
            assert [(r["op"], r["b"], r["a"]) for r in rows[3:5]] == [
                ("d", [1, "a", 11], [None] * 3), ("c", [None] * 3, [10, "a", 11])], (t, rows)
        else:                                     # one update with both images
            assert [(r["op"], r["b"], r["a"]) for r in rows[3:4]] == [("u", [1, "a", 11], [10, "a", 11])], rows
    rows = out["with_list"]["dpk"]["rows"]
    assert [(r["op"], r["b"], r["a"]) for r in rows[8:12]] == [            # the direct swap
        ("d", [1, "p", 1], [None] * 3), ("c", [None] * 3, [2, "p", 1]),
        ("d", [2, "q", 2], [None] * 3), ("c", [None] * 3, [1, "q", 2])], rows
    assert [(r["op"], r["b"], r["a"]) for r in rows[12:]] == [            # a duplicate, then the old row deleted
        ("c", [None] * 3, [2, "dup", 0]), ("d", [2, "p", 1], [None] * 3)], rows

    evidence("engine_key_cases.json", {
        "test": "tests/engine/test_engine_keys.py::test_what_debezium_calls_a_key_and_the_list",
        "cases": {t: what for t, (what, _ddl) in CASES.items()}, "replica_identity": "full for every case",
        "operations": [[t, what] for t, what, _sql in statements],
        "source_rows_at_the_end": {t: [list(r) for r in v] for t, v in source.items()},
        "largest_number_of_rows_with_one_key_in_the_replayed_stream_with_the_list": worst,
        "negative_control": "assert_without_a_key(dpk) on the stream read without the list raised AssertionError",
        **out})
