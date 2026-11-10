"""Cross-path test (markers localpg, jvm): the same source rows read once through the engine (transport form ->
transform.stream) and once as text under package P's session settings (transform.backfill). The two Arrow results
must be identical, value for value and bit for bit, and each must equal the source under the oracle's rules
(cdc_v2.oracle with a local DuckDB destination).

    uv run pytest -m "localpg and jvm" tests/transform/test_transform_crosspath.py          # Postgres 18 (pg-y)
    CDC_V2_Y_PG=16 uv run pytest -m "localpg and jvm" tests/transform/test_transform_crosspath.py   # after a reset
    CDC_V2_Y_EVIDENCE=1 ...                                                                  # evidence files
"""
from __future__ import annotations

import os
import random
import struct
import time

import duckdb
import pyarrow as pa
import pytest

from cdc_v2 import transform as T
from cdc_v2.deadline import Deadline
from cdc_v2.oracle import Destination, OracleConfig, compare
from cdc_v2.pg import catalog
from cdc_v2.stream import frames as Fr
import pgref as R
from pgpair import PG16_BIN, Pair, Scenario, lp
from test_transform_property import identical

pytestmark = [pytest.mark.localpg, pytest.mark.jvm, pytest.mark.timeout(1500)]
PG16 = os.environ.get("CDC_V2_Y_PG") == "16"
EVIDENCE = os.environ.get("CDC_V2_Y_EVIDENCE") == "1"
SEED = int(os.environ.get("CDC_V2_Y_SEED", "20261110"))
N_RANDOM = int(os.environ.get("CDC_V2_Y_RANDOM", "200"))


def q(text):
    return "'%s'" % text.replace("'", "''")


def _rand_lits(kind, rnd, n):
    out = []
    for _ in range(n):
        if kind == "int8":
            out.append(str(rnd.randint(-2 ** 63, 2 ** 63 - 1)))
        elif kind == "float8":
            v = struct.unpack("<d", struct.pack("<Q", rnd.getrandbits(64)))[0]
            out.append(q(R.float8_text(v)))
        elif kind == "float4":
            out.append(q(R.float4_text(R.float4_value(rnd.getrandbits(32)))))
        elif kind == "numeric":
            ip = str(rnd.randint(0, 10 ** rnd.randint(0, 40)))
            fp = "".join(rnd.choice("0123456789") for _ in range(rnd.randint(0, 14)))
            out.append(q(("-" if rnd.random() < .5 else "") + ip + ("." + fp if fp else "")))
        elif kind == "numeric152":
            c = rnd.randint(-10 ** 15 + 1, 10 ** 15 - 1)
            out.append(q("%s%d.%02d" % ("-" if c < 0 else "", abs(c) // 100, abs(c) % 100)))
        elif kind == "date":
            out.append(q(R.date_text(rnd.randint(R.MIN_DATE_DAYS, R.MAX_DATE_DAYS))))
        elif kind == "timestamp":
            out.append(q(R.ts_text(rnd.randint(R.MIN_DATE_DAYS * R.DAY_US, R.MAX_TS_US))))
        elif kind == "timestamptz":
            out.append(q(R.ts_text(rnd.randint(R.MIN_DATE_DAYS * R.DAY_US, R.MAX_TS_US), tz=True)))
        elif kind == "time":
            out.append(q(R.time_text(rnd.randint(0, R.DAY_US))))
        elif kind == "timetz":
            out.append(q(R.timetz_text(rnd.randint(0, R.DAY_US), rnd.randint(-57599, 57599))))
        elif kind == "interval":
            # the stream's driver cannot parse an hour part above 2^31 (docs/design/engine.md 10): kept below
            us = rnd.randint(-(2 ** 31 - 1) * 3600000000, (2 ** 31 - 1) * 3600000000)
            out.append(q(R.interval_iso(rnd.randint(-2 ** 31, 2 ** 31 - 1), rnd.randint(-2 ** 31, 2 ** 31 - 1), us)))
        elif kind == "bytea":
            out.append("'\\x%s'::bytea" % bytes(rnd.getrandbits(8) for _ in range(rnd.randint(0, 40))).hex())
        elif kind == "text":
            out.append(q("".join(chr(rnd.choice([rnd.randint(32, 126), rnd.randint(160, 0x2FFF),
                                                  rnd.randint(0x1F300, 0x1F64F)])) for _ in range(rnd.randint(0, 20)))))
        elif kind == "uuid":
            out.append(q("%032x" % rnd.getrandbits(128)))
        elif kind == "bool":
            out.append(rnd.choice(["true", "false"]))
    return out


# name, SQL type, boundary literals, kind of random values
BASELINE = [
    ("bool", "boolean", ["true", "false"], "bool"),
    ("i2", "smallint", ["-32768", "32767", "0"], None),
    ("i4", "integer", ["-2147483648", "2147483647", "0"], None),
    ("i8", "bigint", ["-9223372036854775808", "9223372036854775807", "0"], "int8"),
    ("f4", "real", ["'NaN'", "'Infinity'", "'-Infinity'", "'-0'", "'1.17549435e-38'", "'3.4028235e38'", "'1e-45'",
                    "'0.1'"], "float4"),
    ("f8", "double precision", ["'NaN'", "'Infinity'", "'-Infinity'", "'-0'", "'5e-324'", "'1.7976931348623157e308'",
                                "'0.1'", "'2.2250738585072014e-308'", "'-2.0604180037107712e+16'"], "float8"),
    ("num", "numeric", ["1.50", "'NaN'", "'Infinity'", "'-Infinity'", "0.000000000000000000000000000000000001",
                        "123456789012345678901234567890123456789012345.6789", "-0.0", "1e300", "1e300::numeric * 1e300",
                        "'9999999999999999999999999999.9999999999'", "'0.00000000001'", "'131072.000'"], "numeric"),
    ("num152", "numeric(15,2)", ["1.5", "-9999999999999.99", "0", "'NaN'"], "numeric152"),
    ("numsc", "numeric(3,5)", ["0.00123", "-0.00999"], None),
    ("txt", "text", ["''", "'plain'", "E'tab\\tquote\\'back\\\\slash\\nnl'", "'\u00e9\u4e2d\U0001F600'",
                     "repeat('x', 100000)"], "text"),
    ("vc", "varchar(10)", ["'abc'", "''"], None),
    ("ch", "char(5)", ["'ab'", "'abcde'", "''"], None),
    ("qch", "\"char\"", ["'a'", "'\\'", "'\u00e9'::\"char\""], None),
    ("nm", "name", ["'n'", "repeat('n', 63)"], None),
    ("ci", "citext", ["'MiXeD'"], None),
    ("by", "bytea", ["''::bytea", "'\\x00'::bytea", "'\\xdeadbeef00ff'::bytea", "decode(repeat('ab', 50000), 'hex')"],
     "bytea"),
    ("uu", "uuid", ["'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'", "'00000000-0000-0000-0000-000000000000'"], "uuid"),
    ("js", "json", ["'{\"a\": 12345678901234567890123, \"b\": [1.0e400], \"a\": 2}'", "'\"\\ud800\"'", "'null'",
                    "'  [1 , 2 ]'"], None),
    ("jb", "jsonb", ["'{\"a\": 12345678901234567890123, \"b\": 1.50}'", "'[]'"], None),
    ("dt", "date", ["'infinity'", "'-infinity'", "'4714-11-24 BC'", "'5874897-12-31'", "'0001-01-01'",
                    "'0001-12-31 BC'", "'10000-01-01'", "'1999-12-31'", "'2000-01-01'"], "date"),
    ("ts", "timestamp", ["'infinity'", "'-infinity'", "'4714-11-24 00:00:00 BC'", "'294276-12-31 23:59:59.999999'",
                         "'294247-01-10 04:00:54.775806'", "'294247-01-10 04:00:54.775807'", "'2000-01-01'",
                         "'0044-03-15 12:00:00 BC'", "'12345-06-07 08:09:10.5'"], "timestamp"),
    ("tz", "timestamptz", ["'infinity'", "'-infinity'", "'4714-11-24 00:00:00+00 BC'",
                           "'294276-12-31 23:59:59.999999+00'", "'1999-12-31 23:59:59.999999-08'",
                           "'2024-02-29 12:34:56.789+05:45'"], "timestamptz"),
    ("tm", "time", ["'00:00:00'", "'24:00:00'", "'23:59:59.999999'", "'12:00:00.5'"], "time"),
    ("tmz", "timetz", ["'24:00:00+15:59:59'", "'00:00:00-15:59:59'", "'12:34:56.789+05:30'", "'12:00:00-01:00:30'",
                       "'12:00:00+00'"], "timetz"),
    ("iv", "interval", ["'1 year 2 mons 3 days 04:05:06.000078'", "'-1 days +02:00:00'", "'0'",
                        "'-178956970 years -8 mons'", "'1 mon -1 day 00:00:00.000001'", "'100000000 hours'",
                        "'-2147483647 hours -59 minutes -59.999999 seconds'"], "interval"),
    ("en", "{s}.mood", ["'happy'", "'sad'"], None),
]
if not PG16:
    BASELINE.append(("numneg", "numeric(5,-2)", ["12345", "-99999"], None))

# Every fallback type of docs/research/debezium.md section 7 (and some more): text in P3
FALLBACK = [
    ("mn", "money", ["'12.34'", "'-0.01'", "'-92233720368547758.08'", "'1234567.5'"]),
    ("bt", "bit(4)", ["B'1010'"]),
    ("vb", "varbit", ["B'101'", "B'0101'", "B''"]),
    ("ai", "integer[]", ["'{1,2,NULL}'", "'[0:2]={1,2,3}'", "'{{1,2},{3,4}}'", "'{}'"]),
    ("at", "text[]", ["'{\"a b\",\"c\\\"d\",NULL,\"NULL\"}'"]),
    ("af", "float8[]", ["'{-0,0,NaN,Infinity,1e-300}'"]),
    ("an", "numeric[]", ["'{1.50,NaN,-0.0}'"]),
    ("atz", "timestamptz[]", ["'{\"2020-01-01 00:00:00+00\",infinity}'"]),
    ("aby", "bytea[]", ["ARRAY['\\x00ff'::bytea, NULL]"]),
    ("aiv", "interval[]", ["'{\"1 day\",-1}'" if PG16 else "'{\"1 day\",infinity}'", "'{\"1 year 2 mons\"}'"]),
    ("hs", "hstore", ["'a=>1, b=>NULL, \"c d\"=>\"x\"'"]),
    ("cp", "{s}.pair", ["ROW(1, 'x', '1 day')::{s}.pair", "ROW(NULL, '', NULL)::{s}.pair"]),
    ("r4", "int4range", ["'[1,5)'", "'empty'"]),
    ("rtz", "tstzrange", ["'[\"2020-01-01 00:00:00+00\",infinity)'"]),
    ("mr", "int4multirange", ["'{[1,3),[5,7)}'"]),
    ("pt", "point", ["'(1,2)'", "'(1.5,-0.1)'", "'(1e-300,3.4e38)'"]),
    ("bx", "box", ["'((0,0),(1,1))'"]),
    ("ine", "inet", ["'10.0.0.1/8'", "'::1'"]),
    ("cid", "cidr", ["'10.0.0.0/8'"]),
    ("mac", "macaddr", ["'08:00:2b:01:02:03'"]),
    ("mac8", "macaddr8", ["'08:00:2b:01:02:03:04:05'"]),
    ("xm", "xml", ["'<a b=\"1\">x</a>'"]),
    ("tsv", "tsvector", ["'a fat cat'"]),
    ("tsq", "tsquery", ["'fat & cat'"]),
    ("lsn", "pg_lsn", ["'0/16B3748'"]),
    ("oi", "oid", ["0", "4294967295"]),
    ("lt", "ltree", ["'a.b.c'"]),
    ("dn", "{s}.money2", ["12.5"]),
    ("da", "{s}.ints", ["'{1,2}'"]),
]

# fallback columns whose stream text is not the Postgres text of the read sessions: findings for package E
KNOWN_STREAM_DIFFERENCES = {
    "aiv": "interval inside an array: the walsender session prints IntervalStyle=postgres, the read sessions "
           "iso_8601",
    "cp": "interval inside a composite: same cause as aiv",
    "pt": "geometric: Debezium passes pgjdbc's PGpoint, printed with Java's float formatting",
    "bx": "geometric: same cause as pt",
}

# tables whose values the oracle (package O) cannot judge as delivered in P3: findings for package O
ORACLE_CANNOT_JUDGE = {
    "f_ai": "array as text: the oracle's array rule wants a LIST destination",
    "f_at": "same", "f_af": "same", "f_an": "same", "f_atz": "same", "f_aby": "same", "f_aiv": "same",
    "f_da": "domain over an array: same",
    "f_cp": "composite as text: the oracle reads the source under its own session (IntervalStyle not pinned)",
    "f_ine": "inet: the oracle's fallback uses inet::text ('::1/128'), not the output function ('::1')",
    "f_tmz": "timetz as TIMETZ: the oracle compares CAST(TIMETZ AS VARCHAR), and DuckDB prints -3630 s as '-01:30'",
}

DDL_TYPES = ["create type {s}.mood as enum ('happy', 'sad')",
             "create type {s}.pair as (a int, b text, c interval)",
             "create domain {s}.money2 as numeric(10,2)",
             "create domain {s}.ints as int[]"]


def tables_ddl():
    ddl = list(DDL_TYPES)
    for name, typ, _v, *_ in BASELINE + FALLBACK:
        ddl.append("create table {s}.f_%s (id int primary key, v %s)" % (name, typ))
    ddl.append("create table {s}.f_ivhuge (id int primary key, v interval)")          # read by the backfill only
    ddl.append("create table {s}.mix (id int primary key, n numeric, d date, tz timestamptz, iv interval, "
               "t text, b bytea)")
    ddl.append("create table {s}.keyless (rid int, f float8, iv interval, t text)")
    ddl.append("alter table {s}.keyless replica identity full")
    return ddl


def write_rows(pair, s, rnd):
    """Insert boundary and random values; then updates, deletes, a key change, keyless changes. Returns the
    number of row records the stream must deliver per table."""
    counts = {}
    with pair.connect(pair.primary_port) as c:
        for name, typ, vals, *rest in BASELINE + FALLBACK:
            kind = rest[0] if rest else None
            lits = [v.replace("{s}", s) for v in vals] + (_rand_lits(kind, rnd, N_RANDOM) if kind else [])
            rows = ["(%d, %s)" % (i + 1, v) for i, v in enumerate(lits)] + ["(%d, NULL)" % (len(lits) + 1)]
            try:
                c.execute("insert into %s.f_%s values %s" % (s, name, ", ".join(rows)))
            except Exception as e:
                raise AssertionError("insert into f_%s failed: %s" % (name, type(e).__name__)) from None
            counts["f_" + name] = len(rows)
        if not PG16:
            c.execute("insert into %s.f_iv values (100001, 'infinity'), (100002, '-infinity')" % s)
            counts["f_iv"] += 2
        c.execute("insert into %s.f_ivhuge values (1, '2562047788 hours'), (2, '1 day')" % s)
        # mix: inserts, an update of some columns, a key change, a delete, a large text left unchanged (TOAST)
        c.execute("insert into %s.mix select g, (g || '.25')::numeric, date '2000-01-01' + g, "
                  "timestamptz '2020-01-01 00:00:00+00' + g * interval '1 hour', g * interval '1 day 1 second', "
                  "repeat(md5(g::text), 400), decode(repeat('00ff', g), 'hex') from generate_series(1, 50) g" % s)
        c.execute("update %s.mix set n = 'NaN', d = 'infinity' where id = 3" % s)
        c.execute("update %s.mix set id = 1000 where id = 4" % s)
        c.execute("update %s.mix set n = 0.5 where id = 5" % s)                 # t is TOASTed: sent unchanged
        c.execute("delete from %s.mix where id = 6" % s)
        with c.transaction():
            c.execute("update %s.mix set id = 2000 where id = 7" % s)
            c.execute("update %s.mix set id = 7 where id = 8" % s)
        c.execute("insert into %s.keyless values (1, -0.0, '1 mon', 'a'), (2, 'NaN', '30 days', 'b'), "
                  "(3, 0.0, '0', 'c')" % s)
        c.execute("update %s.keyless set f = 1.5 where rid = 1" % s)
        c.execute("delete from %s.keyless where rid = 2" % s)
        counts["mix"] = 50 + 1 + 2 + 1 + 1 + 4
        counts["keyless"] = 3 + 1 + 1
    return counts


# ------------------------------------------------------------------------------------------------ the two paths


def read_stream(pair, sc, counts, artifacts, timeout=300):
    """Run the engine until every expected row record has arrived; returns ({table: [Rows]}, {table: Schema})."""
    from cdc_v2.stream.engine import Engine
    total = sum(counts.values())
    got, rows, schemas = 0, {}, {}
    include = ",".join("%s\\.%s" % (sc.schema, t) for t in counts)
    with Engine(pair.engine_settings(sc.slot, sc.pub, table_include=include), None, artifacts) as eng:
        end = Deadline.after(timeout)
        while got < total:
            assert not end.expired, "stream delivered %d of %d records" % (got, total)
            for f in eng.frames(end.within(1.0)):
                if isinstance(f, Fr.Schema):
                    schemas[f.table] = f
                elif isinstance(f, Fr.Rows):
                    rows.setdefault(f.table, []).append(f)
                    got += f.batch.num_rows
        eng.stop(Deadline.after(20))
    return rows, schemas


def read_backfill(pair, s, table, ncols, order="id"):
    conn = pair.read_conn()
    try:
        with conn.guard(Deadline.after(120), "copy"):
            cols = [[] for _ in range(ncols)]
            with conn.raw.cursor().copy("COPY (SELECT * FROM %s.%s ORDER BY %s) TO STDOUT" % (s, table, order)) as cp:
                for row in cp.rows():
                    for i, v in enumerate(row):
                        cols[i].append(v)
    finally:
        conn.close(Deadline.after(5))
    return pa.record_batch({"a%d" % i: pa.array(c, pa.string()) for i, c in enumerate(cols)})


def convert(fn, plan, batch):
    """Convert, widening as a caller does; returns (plan, result)."""
    for _ in range(len(plan.columns) + 1):
        try:
            return plan, fn(plan, batch)
        except T.WideningNeeded as e:
            for p in e.positions:
                plan = plan.widened(p)
    raise AssertionError("widening did not converge")


def apply_records(plan, batches):
    """The final table state from stream records (C2 semantics), as a list of (key, {position: 1-row array})."""
    state, gone = {}, {}
    keyed = plan.has_key
    for b in batches:
        names = b.schema.names
        for r in range(b.num_rows):
            op = b.column(names.index("op"))[r].as_py()
            img = {i: b.column(names.index("a%d" % i)).slice(r, 1) for i in range(len(plan.columns))}
            for i in range(len(plan.columns)):
                un = "u%d" % i
                if un in names and b.column(names.index(un))[r].as_py():
                    img[i] = None                                   # unchanged: taken from the earlier version
            if keyed:
                key = tuple(img[p].to_pylist()[0] for p in plan.key_positions)
                old = None
                kcols = ["k%d" % p for p in plan.key_positions]
                if all(k in names for k in kcols) and b.column(names.index(kcols[0]))[r].is_valid:
                    old = tuple(b.column(names.index(k))[r].as_py() for k in kcols)
                prev = state.get(old if old is not None else key)
                if prev is None and old is not None:
                    prev = gone.get(old)                            # the delete of the old key came first
                if op == "d":
                    if key in state:
                        gone[key] = state.pop(key)
                    continue
                if old is not None:
                    state.pop(old, None)
                for i, v in img.items():
                    if v is None:
                        img[i] = prev[i]
                state[key] = img
            else:
                bimg = {i: b.column(names.index("b%d" % i)).slice(r, 1) for i in range(len(plan.columns))}
                if op in ("u", "d"):
                    rid = bimg[0].to_pylist()[0]
                    state.pop(rid, None)
                if op in ("c", "u"):
                    state[img[0].to_pylist()[0]] = img
    keys = sorted(state, key=lambda k: (k is None, k))
    return [state[k] for k in keys]


def columns_of(images, n):
    return [pa.concat_arrays([im[i] for im in images]) if images else None for i in range(n)]


# ------------------------------------------------------------------------------------------------ the test


@pytest.fixture(scope="module")
def pair():
    pg_bin = PG16_BIN if PG16 else None
    if PG16:
        lp("reset", pg_bin=pg_bin)
    lp("up", pg_bin=pg_bin)
    p = Pair(pg_bin)
    yield p
    if os.environ.get("CDC_V2_Y_KEEP_PG") != "1":
        lp("down", check=False, pg_bin=pg_bin)


@pytest.fixture(scope="module")
def artifacts():
    from cdc_v2.stream import jvm
    return jvm.prepare()


def run_paths(pair, artifacts, sc, counts):
    version = pair.server_version_num()
    rows, schemas = read_stream(pair, sc, counts, artifacts)
    conn = pair.read_conn()
    try:
        cat = {t.name: t for t in catalog.tables(conn, [(sc.schema, t) for t in list(counts) + ["f_ivhuge"]],
                                                 publication=sc.pub, deadline=Deadline.after(30))}
    finally:
        conn.close(Deadline.after(5))
    out = {}
    for table in list(counts) + ["f_ivhuge"]:
        st = cat[table]
        plan = T.plan(st, schemas.get(table), server_version_num=version)
        n = len(plan.columns)
        order = "rid" if table == "keyless" else "id"
        bf_in = read_backfill(pair, sc.schema, table, n, order)
        plan, bf = convert(T.backfill, plan, bf_in)
        sb = []
        for fr in rows.get(table, []):
            plan2, conv = convert(T.stream, plan, fr.batch)
            if plan2.hash != plan.hash:                       # a later batch widened: convert everything again
                plan = plan2
                plan, bf = convert(T.backfill, plan, bf_in)
                sb = [convert(T.stream, plan, x.batch)[1] for x in rows[table][:len(sb)]]
            sb.append(conv)
        masks = [T.ambiguous(plan, fr.batch) for fr in rows.get(table, [])]
        out[table] = dict(plan=plan, backfill=bf, stream=sb, masks=masks, frames=rows.get(table, []))
    return out, version


def compare_paths(out):
    """{table: [differing positions]} between the backfill result and the stream's final state; plus details."""
    diffs, detail = {}, {}
    for table, d in out.items():
        plan = d["plan"]
        n = len(plan.columns)
        bf = [d["backfill"].column(i) for i in range(n)]
        if not d["stream"]:
            diffs[table] = ["no stream rows"]
            continue
        st = columns_of(apply_records(plan, d["stream"]), n)
        bad = []
        for i in range(n):
            if st[i] is None or len(st[i]) != len(bf[i]) or not identical(bf[i], st[i]):
                bad.append(i)
                rows = []
                if st[i] is not None and len(st[i]) == len(bf[i]):
                    rows = [r for r in range(len(bf[i])) if not identical(bf[i].slice(r, 1), st[i].slice(r, 1))]
                detail[(table, i)] = rows
        if bad:
            diffs[table] = bad
    return diffs, detail


def oracle_exclusions(out):
    """{table: [ids]} of rows the oracle cannot judge: a text member of a wide timestamp or interval column (the
    oracle accepts VARCHAR members only for numeric, and its Postgres timestamp form overflows bigint above
    294247 AD). Those rows are compared by the cross-path identity only."""
    ex = {}
    for table, d in out.items():
        plan, bf = d["plan"], d["backfill"]
        ids = bf.column(0).to_pylist()
        bad = set()
        for c in plan.columns:
            if c.form == "wide" and c.family in ("timestamp", "timestamptz", "interval"):
                arr = bf.column(c.position)
                bad |= {ids[r] for r in range(len(arr)) if arr.type_codes[r].as_py() == 1
                        and arr.field(1)[r].is_valid}
        if bad:
            ex[table] = sorted(bad)
    return ex


def make_oracle_copies(pair, schema, out, ex):
    """Source copies `<t>_o` without the excluded rows; returns the copies' names."""
    for table, ids in ex.items():
        pair.p("create table %s.%s_o (like %s.%s)" % (schema, table, schema, table))
        pair.p("insert into %s.%s_o select * from %s.%s where id <> all(%%s)" % (schema, table, schema, table),
               (ids,))
    return {t: t + "_o" for t in ex}


def load_duck(out, which, schema, ex=None):
    con = duckdb.connect()
    con.execute("SET TimeZone = 'UTC'")
    con.execute('CREATE SCHEMA "%s"' % schema)
    con.execute("CREATE SCHEMA _cdc")
    con.execute("CREATE TABLE _cdc.stream_state (slot_name VARCHAR, applied_lsn UBIGINT)")
    for table, d in out.items():
        plan = d["plan"]
        n = len(plan.columns)
        if which == "backfill":
            cols = [d["backfill"].column(i) for i in range(n)]
        else:
            if not d["stream"]:
                continue
            cols = columns_of(apply_records(plan, d["stream"]), n)
        name = table
        if ex and table in ex and cols[0] is not None:
            keep = pa.array([v not in set(ex[table]) for v in cols[0].to_pylist()])
            cols = [c.filter(keep) for c in cols]
            name = table + "_o"
        con.execute('CREATE TABLE "%s"."%s" (%s)' % (schema, name, plan.column_ddl()))
        if cols[0] is not None and len(cols[0]):
            con.register("src", pa.table({"a%d" % i: c for i, c in enumerate(cols)}))
            con.execute('INSERT INTO "%s"."%s" SELECT * FROM src' % (schema, name))
            con.unregister("src")
    return con


def oracle(pair, con, schema, tables, q, slot):
    con.execute("INSERT INTO _cdc.stream_state VALUES (?, ?)", [slot, q])
    res = compare(pair.oracle_source(), Destination(conn=con), OracleConfig(tables=["%s.%s" % (schema, t)
                                                                                   for t in tables]),
                  position=q, slot_name=slot, deadline=Deadline.after(600))
    con.execute("DELETE FROM _cdc.stream_state")
    return res


def test_cross_path(pair, artifacts):
    rnd = random.Random(SEED)
    sc = Scenario(pair, tables_ddl(), extensions=("hstore", "citext", "ltree"))
    t0 = time.monotonic()
    try:
        sc.start()
        counts = write_rows(pair, sc.schema, rnd)
        from cdc_v2 import lsn
        q = lsn.parse(pair.p("select pg_current_wal_insert_lsn()::text")[0][0])
        out, version = run_paths(pair, artifacts, sc, counts)
        diffs, detail = compare_paths(out)
        stream_tables = [t for t in counts]
        # 1. the two paths are identical for every baseline family, the mix and the keyless table
        expected = {"f_" + k for k in KNOWN_STREAM_DIFFERENCES}
        gap_rows = {}
        if not PG16:                         # the ambiguous zero interval: only the two infinity rows of f_iv
            assert diffs.pop("f_iv", None) == [1]
            ids = out["f_iv"]["backfill"].column(0).to_pylist()
            gap_rows["f_iv"] = [ids[r] for r in detail[("f_iv", 1)]]
            assert gap_rows["f_iv"] == [100001, 100002]
        diffs.pop("f_ivhuge")                # read by the backfill only (the driver cannot parse it)
        assert set(diffs) == expected, diffs
        # 2. the mask marks exactly the infinity rows (and the real zeros), never on 16
        marked = []
        for fr, m in zip(out["f_iv"]["frames"], out["f_iv"]["masks"]):
            if m is not None:
                marked += [fr.batch.column(fr.batch.schema.names.index("a0"))[i].as_py()
                           for i, v in enumerate(m.to_pylist()) if v]
        zero_ids = [r for r, t in zip(out["f_iv"]["backfill"].column(0).to_pylist(),
                                      out["f_iv"]["backfill"].column(1).to_pylist()) if t == pa.MonthDayNano([0, 0, 0])]
        if PG16:
            assert all(m is None for m in out["f_iv"]["masks"]) and out["f_iv"]["plan"].ambiguous == ()
        else:
            assert sorted(marked) == sorted(zero_ids + [100001, 100002]) and out["f_iv"]["plan"].ambiguous == (1,)
        # 3. the backfill result equals the source under the oracle (all tables)
        ex = oracle_exclusions(out)
        copies = make_oracle_copies(pair, sc.schema, out, ex)
        q = lsn.parse(pair.p("select pg_current_wal_insert_lsn()::text")[0][0])
        all_tables = [copies.get(t, t) for t in out]
        con_b = load_duck(out, "backfill", sc.schema, ex)
        rb = oracle(pair, con_b, sc.schema, all_tables, q, sc.slot)
        unequal_b = {t.source.split(".")[-1] for t in rb.tables if not t.equal}
        assert unequal_b == set(ORACLE_CANNOT_JUDGE), rb.summary()
        # 4. the stream result equals the source except the known gaps
        con_s = load_duck(out, "stream", sc.schema, ex)
        rs = oracle(pair, con_s, sc.schema, [copies.get(t, t) for t in stream_tables], q, sc.slot)
        unequal = {t.source.split(".")[-1] for t in rs.tables if not t.equal}
        # f_cp: the stream's composite text (IntervalStyle postgres) happens to match the oracle's own session;
        # the infinity rows of f_iv are left out of f_iv_o (the oracle has no rule for the text member)
        want_unequal = (expected | set(ORACLE_CANNOT_JUDGE)) - {"f_cp"}
        assert unequal == want_unequal, rs.summary()
        forms = {t: {c.position: c.form for c in d["plan"].columns if c.form == "wide"} for t, d in out.items()}
        evidence = {
            "test": "tests/transform/test_transform_crosspath.py::test_cross_path",
            "server_version_num": version, "seed": SEED, "random_values_per_family": N_RANDOM,
            "tables": len(out), "records_streamed": sum(counts.values()),
            "values_compared": sum(d["backfill"].num_rows * len(d["plan"].columns) for d in out.values()),
            "identical_tables": sorted(set(stream_tables) - set(diffs) - {"f_iv"}),
            "stream_differences_fallback": {k: KNOWN_STREAM_DIFFERENCES[k] for k in KNOWN_STREAM_DIFFERENCES},
            "ambiguous_zero_interval_rows": gap_rows, "stream_marked_ids": sorted(marked),
            "stream_side_gap": {"f_ivhuge": "interval '2562047788 hours': the engine's driver cannot parse it; "
                                            "the backfill delivers it (wide form)"},
            "widened": {t: f for t, f in forms.items() if f},
            "oracle_backfill_unequal_tables": sorted(unequal_b), "oracle_cannot_judge": ORACLE_CANNOT_JUDGE,
            "oracle_rows_compared_backfill": rb.rows_compared,
            "oracle_cannot_judge_rows": ex, "oracle_stream_unequal_tables": sorted(unequal),
            "seconds": round(time.monotonic() - t0, 1)}
        if EVIDENCE:
            write(name="crosspath_pg%d.json" % (version // 10000), obj=evidence)
    finally:
        sc.close()


def write(name, obj):
    from cdc_v2.testing.harness_evidence import write_evidence
    write_evidence(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "evidence", "p3", "transform",
                                name), obj)


ROLE_SETTINGS = {"DateStyle": "SQL, DMY", "IntervalStyle": "sql_standard", "extra_float_digits": "-15",
                 "bytea_output": "escape", "TimeZone": "Asia/Kathmandu", "lc_monetary": "en_US.UTF-8"}
SESSION_TABLES = ["f_f8", "f_f4", "f_dt", "f_ts", "f_tz", "f_tm", "f_tmz", "f_iv", "f_by", "f_mn", "f_num", "f_aiv",
                  "f_atz", "f_af", "f_cp", "f_rtz", "mix"]


def _results(pair, artifacts, seed):
    sc = Scenario(pair, tables_ddl(), extensions=("hstore", "citext", "ltree"))
    try:
        sc.start()
        counts = write_rows(pair, sc.schema, random.Random(seed))
        counts = {t: n for t, n in counts.items() if t in SESSION_TABLES}
        out, _v = run_paths(pair, artifacts, sc, counts)
        return {t: (d["backfill"], columns_of(apply_records(d["plan"], d["stream"]), len(d["plan"].columns)))
                for t, d in out.items() if t in SESSION_TABLES}
    finally:
        sc.close()


def test_role_level_session_settings_change_nothing(pair, artifacts):
    """ALTER ROLE ... SET of DateStyle, IntervalStyle, extra_float_digits, bytea_output, TimeZone and lc_monetary for
    the runtime role (which both paths use) changes no value on either path: package P pins them for reads,
    package E for the walsender."""
    seed = SEED + 1
    base = _results(pair, artifacts, seed)
    try:
        for k, v in ROLE_SETTINGS.items():
            pair.p("alter role cdc_v2_runtime set %s = %s" % (k, q(v)), role="bootstrap")
        assert pair.r("select current_setting('IntervalStyle')", role="runtime")[0][0] == "sql_standard"
        changed = _results(pair, artifacts, seed)
    finally:
        pair.p("alter role cdc_v2_runtime reset all", role="bootstrap")
    same = {}
    for t in base:
        b0, s0 = base[t]
        b1, s1 = changed[t]
        same[t] = all(identical(b0.column(i), b1.column(i)) and identical(s0[i], s1[i])
                      for i in range(b0.num_columns))
    assert all(same.values()), same
    if EVIDENCE:
        write("session_settings_pg%d.json" % (pair.server_version_num() // 10000),
              {"test": "tests/transform/test_transform_crosspath.py::test_role_level_session_settings_change_nothing",
               "role_settings": ROLE_SETTINGS, "tables": sorted(same), "identical": same})
