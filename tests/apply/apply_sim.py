"""Test model for package A: a simulated source table that emits change records the way the engine does
(docs/design/engine.md section 2; contract C2), and the canonical comparison of the oracle.

The simulator is the reference: it applies source operations one by one to its own rows (a list of row objects,
keys unique at every transaction end), records what Debezium would send, and its final rows are the expected
destination. Values are DuckDB SQL literals, so every destination type and every special value (-0, NaN, -NaN,
infinities, `1 mon` versus `30 days`, a UNION whose member holds NULL ...) is written exactly. Equality of
destination and model is decided by the oracle's canonical forms (`cdc_v2.oracle.canonical`), never by Python's
`==`.
"""
from __future__ import annotations

import random
import uuid as _uuid
from collections import Counter
from dataclasses import dataclass, field
from typing import Dict, List, Optional

import duckdb
import pyarrow as pa

from cdc_v2.apply import Column, TableMeta
from cdc_v2.apply.types import parse, quote_ident
from cdc_v2.oracle import canonical as oc

NZ = "CAST('-0' AS DOUBLE)"
NZF = "CAST('-0' AS FLOAT)"


# ------------------------------------------------------------------------------------------------ types


@dataclass(frozen=True)
class TSpec:
    type: str
    values: tuple                       # literal pool (SQL); NULL is added for value columns
    keys: tuple = ()                    # literal pool for key columns: canonically distinct values
    toast: bool = False                 # can be sent as "unchanged" (the engine's "unchanged possible")


def _ints(lo, hi):
    return ("0", "1", "-1", str(lo), str(hi), "42", "-7")


SPECS: Dict[str, TSpec] = {
    "int": TSpec("INTEGER", _ints(-2147483648, 2147483647), tuple(str(i) for i in range(-3, 40))),
    "bigint": TSpec("BIGINT", _ints(-9223372036854775808, 9223372036854775807),
                    ("-9223372036854775808", "9223372036854775807") + tuple(str(i * 1000003) for i in range(30))),
    "decimal": TSpec("DECIMAL(18,3)", ("1.500", "1.5", "-0.001", "0", "999999999999999.999", "-999999999999999.999"),
                     tuple("%d.%03d" % (i, i * 7 % 1000) for i in range(30)), toast=True),
    "double": TSpec("DOUBLE", (NZ, "0.0::DOUBLE", "'nan'::DOUBLE", "(-('nan'::DOUBLE))", "'inf'::DOUBLE",
                               "'-inf'::DOUBLE", "1.5::DOUBLE", "-2.25::DOUBLE", "1e-300::DOUBLE", "0.1::DOUBLE"),
                    (NZ, "0.0::DOUBLE", "'nan'::DOUBLE", "'inf'::DOUBLE", "'-inf'::DOUBLE")
                    + tuple("%d.25::DOUBLE" % i for i in range(1, 30))),
    "float": TSpec("FLOAT", (NZF, "0.0::FLOAT", "'nan'::FLOAT", "(-('nan'::FLOAT))", "'inf'::FLOAT", "0.1::FLOAT",
                             "16777217::FLOAT")),
    "varchar": TSpec("VARCHAR", ("''", "'a'", "'A'", "chr(0) || 'z'", "'it''s'", "'é'", "repeat('x', 3000)",
                                 "' '", "'NULL'", "'\\'"),
                     ("''", "chr(0)", "'a'", "'A'", "'a '", "'é'", "'it''s'") + tuple("'k%d'" % i for i in range(30)),
                     toast=True),
    "blob": TSpec("BLOB", ("''::BLOB", "from_hex('00')", "from_hex('00ff')", "from_hex('ff')", "repeat('ab', 2000)::BLOB"),
                  ("''::BLOB", "from_hex('00')", "from_hex('0000')") + tuple("from_hex('%02x')" % i for i in range(1, 30)),
                  toast=True),
    "json": TSpec("JSON", ("'{\"a\":1}'::JSON", "'{\"a\": 1}'::JSON", "'[]'::JSON", "'null'::JSON"), toast=True),
    "uuid": TSpec("UUID", tuple("'%s'::UUID" % _uuid.UUID(int=i * 0x1234567 + 1) for i in range(4)),
                  tuple("'%s'::UUID" % _uuid.UUID(int=i * 0x9e3779b97f4a7c15 + 7) for i in range(30))),
    "bool": TSpec("BOOLEAN", ("true", "false")),
    "date": TSpec("DATE", ("'2020-01-01'::DATE", "'infinity'::DATE", "'-infinity'::DATE", "'0001-01-01'::DATE"),
                  ("'infinity'::DATE", "'-infinity'::DATE") + tuple("'2020-01-%02d'::DATE" % i for i in range(1, 29))),
    "ts": TSpec("TIMESTAMP", ("'2020-01-01 00:00:00'::TIMESTAMP", "'2020-01-01 00:00:00.000001'::TIMESTAMP",
                              "'infinity'::TIMESTAMP", "'-infinity'::TIMESTAMP"),
                ("'infinity'::TIMESTAMP", "'-infinity'::TIMESTAMP")
                + tuple("'2020-01-01 00:00:%02d.5'::TIMESTAMP" % i for i in range(30))),
    "tstz": TSpec("TIMESTAMPTZ", ("'2020-01-01 00:00:00+00'::TIMESTAMPTZ", "'2020-01-01 01:00:00+01'::TIMESTAMPTZ",
                                  "'infinity'::TIMESTAMPTZ", "'2021-06-01 12:00:00.123456+00'::TIMESTAMPTZ")),
    "time": TSpec("TIME", ("'00:00:00'::TIME", "'24:00:00'::TIME", "'23:59:59.999999'::TIME")),
    "timetz": TSpec("TIMETZ", ("'12:00:00+01'::TIMETZ", "'11:00:00+00'::TIMETZ", "'24:00:00-15:59'::TIMETZ"),
                    ("'12:00:00+01'::TIMETZ", "'11:00:00+00'::TIMETZ", "'12:00:00+00'::TIMETZ")
                    + tuple("'10:%02d:00+02'::TIMETZ" % i for i in range(30)), toast=True),
    "interval": TSpec("INTERVAL", ("INTERVAL '1 month'", "INTERVAL '30 days'", "INTERVAL '720 hours'",
                                   "INTERVAL '1 month -30 days'", "INTERVAL '0 seconds'", "INTERVAL '-1 microsecond'",
                                   "INTERVAL '1 year 2 months 3 days 04:05:06.000007'", "INTERVAL '1 day'",
                                   "INTERVAL '24 hours'"),
                      ("INTERVAL '1 month'", "INTERVAL '30 days'", "INTERVAL '720 hours'", "INTERVAL '1 month -30 days'",
                       "INTERVAL '0 seconds'", "INTERVAL '1 day'", "INTERVAL '24 hours'")
                      + tuple("INTERVAL '%d minutes'" % i for i in range(1, 30)), toast=True),
    "intlist": TSpec("INTEGER[]", ("[1, NULL, 3]", "[]::INTEGER[]", "[NULL]::INTEGER[]", "[2]"), toast=True),
    "dbllist": TSpec("DOUBLE[]", ("[%s]" % NZ, "[0.0::DOUBLE]", "[%s, 'nan'::DOUBLE]" % NZ, "[NULL]::DOUBLE[]"), toast=True),
    "struct": TSpec("STRUCT(a DOUBLE, \"b c\" VARCHAR)",
                    ("{'a': %s, 'b c': 'x'}" % NZ, "{'a': 0.0, 'b c': 'x'}", "{'a': NULL, 'b c': NULL}",
                     "{'a': 'inf'::DOUBLE, 'b c': ''}"), toast=True),
    "ivstruct": TSpec("STRUCT(i INTERVAL)", ("{'i': INTERVAL '1 month'}", "{'i': INTERVAL '30 days'}")),
    "union": TSpec("UNION(n DECIMAL(38,10), s VARCHAR)",
                   ("union_value(n := 1.5)", "union_value(s := 'NaN')", "union_value(s := NULL::VARCHAR)",
                    "union_value(s := '1.5')", "union_value(n := 0)"), toast=True),
    "dblunion": TSpec("UNION(d DOUBLE, i BIGINT)", ("union_value(d := %s)" % NZ, "union_value(d := 0.0::DOUBLE)",
                                                     "union_value(i := 0)")),
    "map": TSpec("MAP(VARCHAR, DOUBLE)", ("MAP {'a': %s}" % NZ, "MAP {'a': 0.0::DOUBLE}", "MAP {'b': 1.0::DOUBLE}"),
                 toast=True),
}

KEY_KINDS = [k for k, s in SPECS.items() if s.keys]
VALUE_KINDS = list(SPECS)

WEIRD_NAMES = ["id", 'a"b', "with space", "select", "ä€", " ", "a.b", "x0", "a3", "n", "t", "Count", "rowid", "k1",
               "u2", "h1", "rx0", "gid", "e0", "order", "NULL", "'q'", "tab\tname"]


def cast(lit: Optional[str], typ: str) -> str:
    return "CAST(NULL AS %s)" % typ if lit is None else "CAST(%s AS %s)" % (lit, typ)


# ------------------------------------------------------------------------------------------------ canonical


def _pgtype(t) -> Optional[oc.PgColumnType]:
    m = {"INTEGER": "integer", "BIGINT": "bigint", "SMALLINT": "smallint", "DECIMAL": "numeric",
         "DOUBLE": "double precision", "FLOAT": "real", "VARCHAR": "text", "BLOB": "bytea", "JSON": "json",
         "UUID": "uuid", "BOOLEAN": "boolean", "DATE": "date", "TIMESTAMP": "timestamp without time zone",
         "TIMESTAMP WITH TIME ZONE": "timestamp with time zone", "TIME": "time without time zone",
         "INTERVAL": "interval"}
    if t.base in m:
        return oc.PgColumnType(m[t.base])
    if t.base == "LIST" and t.elem.base in m:
        return oc.PgColumnType(m[t.elem.base] + "[]", elem=m[t.elem.base])
    return None


def canon(expr: str, typ: str, depth: int = 0) -> str:
    """SQL: a token text for a value of destination type `typ` that is equal for two values exactly when the oracle
    calls them equal; UNION values carry their member; NULL is `N`."""
    t = parse(typ)
    pg = _pgtype(t)
    if pg is not None:
        inner = oc.md_canon(expr, pg, oc.parse_md_type(t.sql()))
    elif t.base == "STRUCT":
        inner = "'{' || %s || '}'" % " || ',' || ".join(canon("struct_extract(%s, '%s')" % (expr, n.replace("'", "''")),
                                                               ft.sql(), depth + 1) for n, ft in t.fields)
    elif t.base == "UNION":
        inner = "CAST(union_tag(%s) AS VARCHAR) || ':' || (CASE CAST(union_tag(%s) AS VARCHAR) %s END)" % (
            expr, expr, " ".join("WHEN '%s' THEN %s" % (n, canon("union_extract(%s, '%s')" % (expr, n), ft.sql(), depth + 1))
                                 for n, ft in t.fields))
    elif t.base == "MAP":
        v = "_m%d" % depth
        inner = "array_to_string(list_transform(map_entries(%s), lambda %s: %s || '=' || %s), ',')" % (
            expr, v, canon("%s.key" % v, t.key.sql(), depth + 1), canon("%s.value" % v, t.value.sql(), depth + 1))
    elif t.base == "LIST":
        v = "_l%d" % depth
        inner = "'[' || array_to_string(list_transform(%s, lambda %s: %s), ',') || ']'" % (
            expr, v, canon(v, t.elem.sql(), depth + 1))
    else:                                            # TIME WITH TIME ZONE: DuckDB's text keeps the offset
        inner = "CAST(%s AS VARCHAR)" % expr
    return "(CASE WHEN %s IS NULL THEN 'N' ELSE 'V' || %s END)" % (expr, inner)


def canon_rows(conn, table_sql: str, cols: List[Column]) -> Counter:
    sql = "SELECT %s FROM %s" % (", ".join(canon(quote_ident(c.name), c.type) for c in cols), table_sql)
    return Counter(tuple(r) for r in conn.execute(sql).fetchall())


def values_table(cols: List[Column], rows: List[Dict[int, Optional[str]]]) -> pa.Table:
    """Rows given as literals, as an Arrow table with the destination column names and types. Values travel through
    Arrow as they do in the pipeline (a UNION whose member holds NULL arrives as NULL, the oracle's rule)."""
    con = duckdb.connect()
    con.execute("SET TimeZone = 'UTC'")
    con.execute("SET arrow_lossless_conversion = true")
    sel = lambda r: "SELECT %s" % ", ".join("%s AS %s" % (cast(r.get(c.position), c.type), quote_ident(c.name))
                                            for c in cols)
    if rows:
        sql = " UNION ALL ".join(sel(r) for r in rows)
    else:
        sql = sel({}) + " WHERE false"
    t = con.execute(sql).to_arrow_table()
    con.close()
    return t


def literal_rows_canon(cols: List[Column], rows: List[Dict[int, Optional[str]]]) -> Counter:
    """Canonical multiset of rows given as literals (computed in a private local DuckDB)."""
    con = duckdb.connect()
    con.execute("SET TimeZone = 'UTC'")
    con.execute("CREATE TABLE e (%s)" % ", ".join("%s %s" % (quote_ident(c.name), c.type) for c in cols))
    con.register("vals", values_table(cols, rows))
    con.execute("INSERT INTO e SELECT * FROM vals")
    out = canon_rows(con, "e", cols)
    con.close()
    return out


def lit_canon(lit: Optional[str], typ: str, _cache={}) -> str:
    key = (lit, typ)
    if key not in _cache:
        con = duckdb.connect()
        con.execute("SET TimeZone = 'UTC'")
        _cache[key] = con.execute("SELECT %s" % canon(cast(lit, typ), typ)).fetchone()[0]
        con.close()
    return _cache[key]


# ------------------------------------------------------------------------------------------------ records


@dataclass
class Rec:
    txn: int
    seq: int
    op: str
    a: Dict[int, Optional[str]]
    u: Dict[int, bool] = field(default_factory=dict)
    k: Dict[int, Optional[str]] = field(default_factory=dict)
    b: Dict[int, Optional[str]] = field(default_factory=dict)


def change_set_table(meta: TableMeta, recs: List[Rec], txn_base: int = 0) -> pa.Table:
    """Arrow rows of contract C3a for the records (destination types, built by DuckDB from the literals)."""
    cols = list(meta.columns)
    ucols = [c for c in cols if c.unchanged_possible and not c.key]
    names = ["txn", "seq", "op"] + [c.a for c in cols] + [c.u for c in ucols]
    if meta.has_key:
        names += [c.k for c in meta.key_columns]
    else:
        names += [c.b for c in cols]
    con = duckdb.connect()
    con.execute("SET TimeZone = 'UTC'")
    con.execute("SET arrow_lossless_conversion = true")
    exprs = []
    for r in recs:
        vals = ["CAST(%d AS BIGINT)" % (r.txn - txn_base), "CAST(%d AS BIGINT)" % r.seq, "'%s'" % r.op]
        vals += [cast(r.a.get(c.position), c.type) for c in cols]
        vals += ["CAST(%s AS BOOLEAN)" % ("true" if r.u.get(c.position) else "false") for c in ucols]
        if meta.has_key:
            vals += [cast(r.k.get(c.position), c.type) for c in meta.key_columns]
        else:
            vals += [cast(r.b.get(c.position), c.type) for c in cols]
        exprs.append("SELECT %s" % ", ".join("%s AS %s" % (v, quote_ident(n)) for v, n in zip(vals, names)))
    if not exprs:
        empty = ["CAST(NULL AS BIGINT)", "CAST(NULL AS BIGINT)", "CAST(NULL AS VARCHAR)"]
        empty += ["CAST(NULL AS %s)" % c.type for c in cols] + ["CAST(NULL AS BOOLEAN)" for c in ucols]
        empty += ["CAST(NULL AS %s)" % c.type for c in (meta.key_columns if meta.has_key else cols)]
        exprs = ["SELECT %s WHERE false" % ", ".join("%s AS %s" % (v, quote_ident(n)) for v, n in zip(empty, names))]
    t = con.execute(" UNION ALL ".join(exprs)).to_arrow_table()
    con.close()
    return t


def create_table(conn, schema: str, table: str, meta: TableMeta, rows: List[Dict[int, Optional[str]]]):
    cols = list(meta.columns)
    conn.execute("CREATE SCHEMA IF NOT EXISTS %s" % quote_ident(schema))
    full = "%s.%s" % (quote_ident(schema), quote_ident(table))
    conn.execute("CREATE TABLE %s (%s)" % (full, ", ".join("%s %s" % (quote_ident(c.name), c.type) for c in cols)))
    if rows:
        conn.register("cdc_v2_test_vals", values_table(cols, rows))
        conn.execute("INSERT INTO %s SELECT * FROM cdc_v2_test_vals" % full)
        conn.unregister("cdc_v2_test_vals")
    return full


# ------------------------------------------------------------------------------------------------ simulators


class _Row:
    __slots__ = ("vals", "toasted", "rid")
    _n = 0

    def __init__(self, vals, toasted):
        self.vals = dict(vals)
        self.toasted = set(toasted)
        _Row._n += 1
        self.rid = _Row._n


class KeyedSource:
    """A source table with a key. `ops_txn` runs one source transaction of random statements and returns its
    records. Keys are unique at every transaction end; a deferred permutation holds duplicates inside one."""

    def __init__(self, meta: TableMeta, rng: random.Random, kinds: Dict[int, str], toast_rate: float = 0.5):
        self.meta, self.rng, self.kinds = meta, rng, kinds
        self.keys = meta.key_columns
        self.vals = [c for c in meta.value_columns]
        self.toast_rate = toast_rate
        self.rows: List[_Row] = []
        self.seq = 0
        self.txn = 0
        self.key_pool = self._key_pool()

    def _key_pool(self):
        pools = [SPECS[self.kinds[c.position]].keys for c in self.keys]
        out = []
        for i in range(max(len(p) for p in pools) * 2):
            out.append(tuple(p[(i * (j + 1) + j) % len(p)] for j, p in enumerate(pools)))
        # canonically distinct tuples only
        seen, uniq = set(), []
        for kt in out:
            ck = tuple(lit_canon(l, c.type) for l, c in zip(kt, self.keys))
            if ck not in seen:
                seen.add(ck)
                uniq.append(kt)
        return uniq

    def kcanon(self, row_or_vals):
        vals = row_or_vals.vals if isinstance(row_or_vals, _Row) else row_or_vals
        return tuple(lit_canon(vals[c.position], c.type) for c in self.keys)

    def _rand_val(self, c: Column):
        pool = SPECS[self.kinds[c.position]].values
        return None if self.rng.random() < 0.12 else self.rng.choice(pool)

    def _toastable(self, c: Column):
        return c.unchanged_possible and not c.key

    def _new_row(self, kt):
        vals = {c.position: l for c, l in zip(self.keys, kt)}
        toasted = set()
        for c in self.vals:
            vals[c.position] = self._rand_val(c)
            if self._toastable(c) and vals[c.position] is not None and self.rng.random() < self.toast_rate:
                toasted.add(c.position)
        return _Row(vals, toasted)

    def _free_key(self, extra_taken=()):
        taken = {self.kcanon(r) for r in self.rows} | set(extra_taken)
        free = [kt for kt in self.key_pool if tuple(lit_canon(l, c.type) for l, c in zip(kt, self.keys)) not in taken]
        return self.rng.choice(free) if free else None

    def _emit(self, recs, op, a, u=None, k=None):
        recs.append(Rec(self.txn, self.seq, op, dict(a), dict(u or {}), dict(k or {})))
        self.seq += 1

    def _image(self, row, changed):
        """The new image as pgoutput sends it: an unchanged value that is stored out of line is "unchanged"."""
        a, u = {}, {}
        for c in self.meta.columns:
            p = c.position
            if self._toastable(c) and p not in changed and p in row.toasted:
                a[p], u[p] = None, True
            else:
                a[p] = row.vals[p]
        return a, u

    def _update_values(self, row):
        changed = set()
        for c in self.vals:
            if self.rng.random() < 0.5:
                row.vals[c.position] = self._rand_val(c)
                changed.add(c.position)
                row.toasted.discard(c.position)
                if self._toastable(c) and row.vals[c.position] is not None and self.rng.random() < self.toast_rate:
                    row.toasted.add(c.position)
        return changed

    def insert(self, recs):
        kt = self._free_key()
        if kt is None:
            return
        row = self._new_row(kt)
        self.rows.append(row)
        self._emit(recs, "c", row.vals)

    def delete(self, recs, row=None):
        if not self.rows:
            return
        row = row or self.rng.choice(self.rows)
        self.rows.remove(row)
        self._emit(recs, "d", {c.position: row.vals[c.position] for c in self.keys})

    def update(self, recs):
        if not self.rows:
            return
        row = self.rng.choice(self.rows)
        changed = self._update_values(row)
        a, u = self._image(row, changed)
        self._emit(recs, "u", a, u)

    def move(self, recs, row=None, new_key=None):
        """UPDATE that changes the key (immediate constraint): d(old) + c(new image, k=old)."""
        if not self.rows:
            return
        row = row or self.rng.choice(self.rows)
        kt = new_key or self._free_key()
        if kt is None:
            return
        old = {c.position: row.vals[c.position] for c in self.keys}
        changed = self._update_values(row) if self.rng.random() < 0.5 else set()
        for c, l in zip(self.keys, kt):
            row.vals[c.position] = l
            changed.add(c.position)
        a, u = self._image(row, changed)
        self._emit(recs, "d", old)
        self._emit(recs, "c", a, u, old)

    def move_same_key(self, recs):
        """A key change back to the same key (UPDATE SET id = id, seen by Debezium as a key change only when the
        representation differs; here the record pair is sent as for any key change)."""
        if not self.rows:
            return
        row = self.rng.choice(self.rows)
        self.move(recs, row, tuple(row.vals[c.position] for c in self.keys))

    def permute(self, recs):
        """One UPDATE that permutes the keys of several rows (deferred unique constraint): each row's d + c in
        heap order; duplicates exist between the records of one statement."""
        if len(self.rows) < 2:
            return
        group = self.rng.sample(self.rows, self.rng.randint(2, min(4, len(self.rows))))
        keys = [tuple(r.vals[c.position] for c in self.keys) for r in group]
        perm = keys[1:] + keys[:1]
        order = list(range(len(group)))
        self.rng.shuffle(order)
        for i in order:
            row = group[i]
            old = {c.position: row.vals[c.position] for c in self.keys}
            changed = self._update_values(row) if self.rng.random() < 0.3 else set()
            for c, l in zip(self.keys, perm[i]):
                row.vals[c.position] = l
                changed.add(c.position)
            a, u = self._image(row, changed)
            self._emit(recs, "d", old)
            self._emit(recs, "c", a, u, old)

    def swap_via_temp(self, recs):
        """A <-> B through a third key, three immediate statements."""
        if len(self.rows) < 2:
            return
        r1, r2 = self.rng.sample(self.rows, 2)
        k1 = tuple(r1.vals[c.position] for c in self.keys)
        k2 = tuple(r2.vals[c.position] for c in self.keys)
        tmp = self._free_key()
        if tmp is None:
            return
        self.move(recs, r1, tmp)
        self.move(recs, r2, k1)
        self.move(recs, r1, k2)

    def chain(self, recs):
        """B -> new, then A -> B (the workload's chain), or A -> B -> C on one row."""
        if not self.rows:
            return
        if len(self.rows) >= 2 and self.rng.random() < 0.5:
            a, b = self.rng.sample(self.rows, 2)
            kb = tuple(b.vals[c.position] for c in self.keys)
            self.move(recs, b)
            self.move(recs, a, kb)
        else:
            r = self.rng.choice(self.rows)
            self.move(recs, r)
            self.move(recs, r)

    def temp_row(self, recs):
        """Insert and delete one row inside the transaction (possibly updating it between)."""
        kt = self._free_key()
        if kt is None:
            return
        row = self._new_row(kt)
        self.rows.append(row)
        self._emit(recs, "c", row.vals)
        if self.rng.random() < 0.5:
            changed = self._update_values(row)
            a, u = self._image(row, changed)
            self._emit(recs, "u", a, u)
        self.delete(recs, row)

    def delete_then_reuse(self, recs):
        """Delete B, then move A onto B's key (a key change onto a key that another record deleted)."""
        if len(self.rows) < 2:
            return
        a, b = self.rng.sample(self.rows, 2)
        kb = tuple(b.vals[c.position] for c in self.keys)
        self.delete(recs, b)
        self.move(recs, a, kb)

    OPS = ["insert", "insert", "update", "update", "update", "delete", "move", "move_same_key", "permute",
           "swap_via_temp", "chain", "temp_row", "delete_then_reuse"]

    def ops_txn(self, n_ops: int, ops=None) -> List[Rec]:
        self.txn += 1
        if self.rng.random() < 0.2:
            self.seq = 0                         # a new engine instance: seq starts again
        recs: List[Rec] = []
        for _ in range(n_ops):
            getattr(self, self.rng.choice(ops or self.OPS))(recs)
        return recs

    def snapshot(self) -> List[Dict[int, Optional[str]]]:
        return [dict(r.vals) for r in self.rows]

    def seed_rows(self, n: int):
        for _ in range(n):
            kt = self._free_key()
            if kt is None:
                break
            self.rows.append(self._new_row(kt))


class KeylessSource:
    """A source table without a key (REPLICA IDENTITY FULL): records carry the old image in b<i>."""

    def __init__(self, meta: TableMeta, rng: random.Random, kinds: Dict[int, str], toast_rate: float = 0.5):
        self.meta, self.rng, self.kinds = meta, rng, kinds
        self.cols = list(meta.columns)
        self.toast_rate = toast_rate
        self.rows: List[_Row] = []
        self.seq = 0
        self.txn = 0
        # a small value pool per column, so that identical rows are common
        self.pool = {c.position: [None] + list(SPECS[kinds[c.position]].values[:3]) for c in self.cols}

    def _val(self, c):
        return self.rng.choice(self.pool[c.position])

    def _new_row(self):
        vals = {c.position: self._val(c) for c in self.cols}
        toasted = {c.position for c in self.cols if c.unchanged_possible and vals[c.position] is not None
                   and self.rng.random() < self.toast_rate}
        return _Row(vals, toasted)

    def _emit(self, recs, op, a, b, u=None):
        recs.append(Rec(self.txn, self.seq, op, dict(a), dict(u or {}), {}, dict(b)))
        self.seq += 1

    def insert(self, recs):
        row = self._new_row()
        if self.rows and self.rng.random() < 0.4:                 # an identical copy of an existing row
            src = self.rng.choice(self.rows)
            row = _Row(src.vals, src.toasted)
        self.rows.append(row)
        self._emit(recs, "c", row.vals, {})

    def delete(self, recs):
        if not self.rows:
            return
        row = self.rng.choice(self.rows)
        self.rows.remove(row)
        self._emit(recs, "d", {}, row.vals)

    def update(self, recs):
        if not self.rows:
            return
        row = self.rng.choice(self.rows)
        old = dict(row.vals)
        changed = set()
        for c in self.cols:
            if self.rng.random() < 0.5:
                row.vals[c.position] = self._val(c)
                changed.add(c.position)
                row.toasted.discard(c.position)
        a, u = {}, {}
        for c in self.cols:
            p = c.position
            if c.unchanged_possible and p not in changed and p in row.toasted:
                a[p], u[p] = None, True
            else:
                a[p] = row.vals[p]
        self._emit(recs, "u", a, old, u)

    OPS = ["insert", "insert", "delete", "update", "update"]

    def ops_txn(self, n_ops: int) -> List[Rec]:
        self.txn += 1
        if self.rng.random() < 0.2:
            self.seq = 0
        recs: List[Rec] = []
        for _ in range(n_ops):
            getattr(self, self.rng.choice(self.OPS))(recs)
        return recs

    def snapshot(self):
        return [dict(r.vals) for r in self.rows]

    def seed_rows(self, n):
        for _ in range(n):
            self.rows.append(self._new_row())


def random_meta(rng: random.Random, keyed: bool, n_values: int, value_kinds=None, key_kinds=None, n_keys=None,
                names=None):
    """A table: key columns (if keyed) and value columns of random kinds, with awkward destination names."""
    names = list(names or WEIRD_NAMES)
    if keyed:
        pass
    else:
        names = [n for n in names if n.casefold() != "rowid"]
    rng.shuffle(names)
    kinds, cols, pos = {}, [], 0
    positions = rng.sample(range(0, 40), (n_keys or 0) + n_values + (1 if keyed and not n_keys else 0))
    nk = (n_keys or rng.choice([1, 1, 1, 2])) if keyed else 0
    positions = rng.sample(range(0, 40), nk + n_values)
    for i in range(nk):
        kind = rng.choice(key_kinds or KEY_KINDS)
        p = positions[i]
        kinds[p] = kind
        cols.append(Column(p, names.pop(), SPECS[kind].type, key=True, unchanged_possible=SPECS[kind].toast))
    for i in range(n_values):
        kind = rng.choice(value_kinds or VALUE_KINDS)
        p = positions[nk + i]
        kinds[p] = kind
        cols.append(Column(p, names.pop(), SPECS[kind].type, key=False, unchanged_possible=SPECS[kind].toast))
    return TableMeta(tuple(cols)), kinds
