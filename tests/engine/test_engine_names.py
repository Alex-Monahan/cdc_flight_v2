"""Names stock Debezium cannot stream (H-051), the residuals of package K's fix round (jvm, localpg; pair pg-e).

Each test builds its own schemas, publication and slot and removes them, also on failure. The outcome of each
shape is asserted as measured with Debezium 3.7.1 on Postgres 18.6.
"""
import uuid

import pytest

from cdc_v2.deadline import Deadline
from cdc_v2.stream.engine import Engine, EngineFatal
from stream_helpers import Collector, evidence

pytestmark = [pytest.mark.jvm, pytest.mark.localpg, pytest.mark.timeout(600)]


def q(name):
    return '"' + name.replace('"', '""') + '"'


class Names:
    """Schemas with arbitrary names, one publication over their tables, one slot on the standby."""

    def __init__(self, pair, schemas_tables):
        self.pair = pair
        self.tag = uuid.uuid4().hex[:8]
        self.pub = "cdc_v2_e_pub_" + self.tag
        self.slot = "cdc_v2_e_slot_" + self.tag
        self.schemas = sorted({s for s, _t in schemas_tables})
        self.tables = list(schemas_tables)
        for s in self.schemas:
            pair.p("create schema %s" % q(s))
            pair.p("grant usage on schema %s to cdc_v2_runtime" % q(s))
        for s, t in self.tables:
            pair.p("create table %s.%s (id int primary key, v text)" % (q(s), q(t)))
            pair.p("grant select on %s.%s to cdc_v2_runtime" % (q(s), q(t)))
        pair.p("create publication %s for table %s" % (self.pub, ", ".join("%s.%s" % (q(s), q(t))
                                                                            for s, t in self.tables)))
        pair.wait_replayed()
        pair.create_slot(self.slot)

    def insert(self, schema, table, *ids):
        self.pair.p("insert into %s.%s values %s" % (q(schema), q(table), ", ".join("(%d, 'v')" % i for i in ids)))

    def close(self):
        try:
            self.pair.drop_slot(self.slot)
        finally:
            self.pair.p("drop publication if exists %s" % self.pub)
            for s in self.schemas:
                self.pair.p("drop schema if exists %s cascade" % q(s))


@pytest.fixture
def names(pair):
    made = []

    def make(schemas_tables):
        n = Names(pair, schemas_tables)
        made.append(n)
        return n

    yield make
    for n in made:
        n.close()


def run(pair, n, artifacts, until, **kw):
    """Read until `until(collector)`; returns (collector, None) or (collector, EngineFatal)."""
    with Engine(pair.settings(n.slot, n.pub, **kw), None, artifacts) as eng:
        col = Collector(eng)
        try:
            col.run(until, timeout=60)
        except EngineFatal as e:
            return col, e
        eng.stop(Deadline.after(20))
    return col, None


def rows_by_table(col):
    out = {}
    for t in col.txns:
        for tb, r in t.rows:
            out.setdefault(tb, []).append(r["a0"])
    return out


def schemas_of(col):
    return sorted({(s, t) for (s, t, _v) in col.schemas})


SCHEMA_SHAPES = {"quote at the start": '"b', "space then a quoted part": 'x "q"', "quoted whole name": '"q"',
                 "quote inside": 'a"b'}


@pytest.mark.parametrize("kind", sorted(SCHEMA_SHAPES))
def test_the_shapes_as_schema_names(pair, names, artifacts, kind):
    tag = uuid.uuid4().hex[:6]
    bad = SCHEMA_SHAPES[kind].replace("b", "b" + tag, 1).replace("q", "q" + tag, 1)
    n = names([(bad, "t"), ("cdc_v2_e_m_" + tag, "m")])
    n.insert(bad, "t", 1, 2)
    n.insert("cdc_v2_e_m_" + tag, "m", 999)
    col, fatal = run(pair, n, artifacts, lambda c: any(r["a0"] == 999 for _t in c.txns for _tb, r in _t.rows))
    got = {"schema": SCHEMA_SHAPES[kind], "fatal": fatal is not None,
           "event_counts": [t.end.event_count for t in col.txns], "tables_seen": [list(x) for x in schemas_of(col)],
           "rows": rows_by_table(col)}
    if kind in ("quote at the start", "space then a quoted part"):          # as table names: FATAL at start
        assert fatal is not None and "IllegalArgumentException" in str(fatal) and not col.txns, got
    elif kind == "quoted whole name":                                       # silently dropped: event count 0
        assert fatal is None and got["event_counts"] == [0, 1] and got["rows"] == {"m": [999]}, got
    else:                                                                   # a quote inside: streams
        assert fatal is None and got["rows"] == {"t": [1, 2], "m": [999]} and (bad, "t") in schemas_of(col), got
    evidence("engine_names_schema_%s.json" % kind.replace(" ", "_"), {
        "test": "tests/engine/test_engine_names.py::test_the_shapes_as_schema_names[%s]" % kind, **got})


def test_quoted_name_next_to_the_plain_name(pair, names, artifacts):
    s = "cdc_v2_e_n_" + uuid.uuid4().hex[:6]
    n = names([(s, '"q"'), (s, "q"), (s, "m")])
    n.insert(s, '"q"', 1, 2)
    n.insert(s, "q", 3)
    n.insert(s, "m", 999)
    col, fatal = run(pair, n, artifacts, lambda c: any(r["a0"] == 999 for _t in c.txns for _tb, r in _t.rows))
    got = {"fatal": fatal is not None, "event_counts": [t.end.event_count for t in col.txns],
           "tables_seen": [[x[1]] for x in schemas_of(col)], "rows": rows_by_table(col)}
    # the worst case: the rows of "q" arrive as rows of q, counted, with no error
    assert fatal is None and got["event_counts"] == [2, 1, 1], got
    assert got["rows"] == {"q": [1, 2, 3], "m": [999]} and got["tables_seen"] == [["m"], ["q"]], got
    evidence("engine_names_quoted_next_to_plain.json", {
        "test": "tests/engine/test_engine_names.py::test_quoted_name_next_to_the_plain_name", **got})


def test_dotted_schema_and_dotted_table_that_print_alike(pair, names, artifacts):
    base = "cdc_v2_e_d_" + uuid.uuid4().hex[:6]
    n = names([(base + ".b", "c"), (base, "b.c"), (base, "m")])
    n.insert(base + ".b", "c", 1)
    n.insert(base, "b.c", 2)
    n.insert(base, "m", 999)
    col, fatal = run(pair, n, artifacts, lambda c: any(r["a0"] == 999 for _t in c.txns for _tb, r in _t.rows))
    got = {"fatal": fatal is not None, "event_counts": [t.end.event_count for t in col.txns],
           "tables_seen": [[x[0].replace(base, "<s>"), x[1]] for x in schemas_of(col)],
           "rows": rows_by_table(col)}
    # kept apart: the SCHEMA and ROWS frames carry schema and table as separate fields
    assert fatal is None and got["rows"] == {"c": [1], "b.c": [2], "m": [999]}, got
    assert got["tables_seen"] == [["<s>", "b.c"], ["<s>", "m"], ["<s>.b", "c"]], got
    evidence("engine_names_dotted_alike.json", {
        "test": "tests/engine/test_engine_names.py::test_dotted_schema_and_dotted_table_that_print_alike", **got})


@pytest.mark.parametrize("bad", ['"b', 'x "q"'])
def test_left_out_of_the_include_list(pair, names, artifacts, bad):
    s = "cdc_v2_e_i_" + uuid.uuid4().hex[:6]
    n = names([(s, bad), (s, "m")])
    n.insert(s, bad, 1)
    n.insert(s, "m", 999)
    col, fatal = run(pair, n, artifacts, lambda c: any(r["a0"] == 999 for _t in c.txns for _tb, r in _t.rows),
                     table_include="%s\\.m" % s)
    got = {"table": bad, "fatal": fatal is not None, "event_counts": [t.end.event_count for t in col.txns],
           "rows": rows_by_table(col)}
    # left out of table.include.list: no FATAL; its transaction arrives with event count 0
    assert fatal is None and got["event_counts"] == [0, 1] and got["rows"] == {"m": [999]}, got
    evidence("engine_names_left_out_%s.json" % ("quote_at_the_start" if bad == '"b' else "space_then_quote"), {
        "test": "tests/engine/test_engine_names.py::test_left_out_of_the_include_list", **got})
