"""Destination types as apply needs them: exact equality, writes that DuckDB skips, writes that rewrite a row.

Three facts about DuckDB 1.5.6 (local and MotherDuck, `evidence/p3/apply/duckdb_semantics.json`) decide the
statements of this package:

1. `=`, `GROUP BY`, `PARTITION BY` and `DISTINCT` treat some different values as one: `-0` and `0`, NaN and -NaN
   (FLOAT, DOUBLE), and intervals that normalise to the same length (`1 mon`, `30 days`, `720 hours`), also inside
   lists, structs, maps and unions. Apply compares through `exact(expr, type)`, which keeps those values apart and
   treats every NaN as one value (the oracle's rule, `docs/oracle.md` 1.2).
2. An `UPDATE` stores nothing when the new value compares equal to the value in the column's data slot (C++ `==`,
   which also says `-0 == 0`; for intervals the normalised comparison). The data slot of a NULL keeps the value it
   had before, so `NULL -> 0` over a slot holding `-0` reads back `-0`. Affected: FLOAT, DOUBLE, INTERVAL, and
   STRUCTs that hold them (`affected(type)`). `sentinel(expr, type)` gives a value that compares unequal to `expr`;
   writing it first makes the second write store the new value.
3. An `UPDATE` that sets a LIST, ARRAY, MAP or UNION column is executed as a delete and an insert of the whole row
   (`rewrites_row(type)`): every column of the row is copied. Apply sets such a column only on rows whose value
   really changes.

The parser is this package's own (the oracle shares no code with apply, `docs/design/p3_plan.md` C8).
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Optional

__all__ = ["DType", "parse", "exact", "needs_exact", "affected", "rewrites_row", "sentinel", "quote_ident",
           "quote_literal"]

#: The bits of the one NaN that `exact` uses for every NaN (0x7ff8000000000000 and 0x7fc00000).
NAN_BITS_DOUBLE = 9221120237041090560
NAN_BITS_FLOAT = 2143289344


def quote_ident(name: str) -> str:
    """A DuckDB identifier for any string: double quotes, inner quotes doubled."""
    if not isinstance(name, str):
        raise TypeError("an identifier is a string")
    return '"%s"' % name.replace('"', '""')


def quote_literal(text: str) -> str:
    return "'%s'" % text.replace("'", "''")


@dataclass(frozen=True)
class DType:
    """A parsed DuckDB type. `base` is upper case and canonical (`TIMESTAMP WITH TIME ZONE`, `LIST`, `STRUCT` ...)."""
    base: str
    args: tuple = ()                                  # DECIMAL (p, s), VARCHAR (n)
    elem: Optional["DType"] = None                    # LIST, ARRAY element
    size: Optional[int] = None                        # ARRAY size
    fields: tuple = ()                                # STRUCT / UNION ((name, DType), ...)
    key: Optional["DType"] = None                     # MAP
    value: Optional["DType"] = None                   # MAP
    text: str = field(default="", compare=False)

    def sql(self) -> str:
        """The type as DuckDB SQL, with every field name quoted."""
        if self.base == "LIST":
            return self.elem.sql() + "[]"
        if self.base == "ARRAY":
            return "%s[%d]" % (self.elem.sql(), self.size)
        if self.base in ("STRUCT", "UNION"):
            return "%s(%s)" % (self.base, ", ".join("%s %s" % (quote_ident(n), t.sql()) for n, t in self.fields))
        if self.base == "MAP":
            return "MAP(%s, %s)" % (self.key.sql(), self.value.sql())
        if self.args:
            return "%s(%s)" % (self.base, ", ".join(str(a) for a in self.args))
        return self.base

    def children(self):
        if self.elem is not None:
            yield self.elem
        for _, t in self.fields:
            yield t
        if self.key is not None:
            yield self.key
            yield self.value


_ALIASES = {
    "INT": "INTEGER", "INT4": "INTEGER", "SIGNED": "INTEGER", "INT8": "BIGINT", "LONG": "BIGINT", "INT2": "SMALLINT",
    "SHORT": "SMALLINT", "INT1": "TINYINT", "FLOAT4": "FLOAT", "REAL": "FLOAT", "FLOAT8": "DOUBLE",
    "NUMERIC": "DECIMAL", "STRING": "VARCHAR", "TEXT": "VARCHAR", "CHAR": "VARCHAR", "BPCHAR": "VARCHAR",
    "BYTEA": "BLOB", "BINARY": "BLOB", "VARBINARY": "BLOB", "BOOL": "BOOLEAN", "LOGICAL": "BOOLEAN",
    "TIMESTAMPTZ": "TIMESTAMP WITH TIME ZONE", "TIMETZ": "TIME WITH TIME ZONE", "DATETIME": "TIMESTAMP",
    "TIMESTAMP_US": "TIMESTAMP",
}


class _Parser:
    def __init__(self, text: str):
        self.s = text
        self.i = 0

    def ws(self):
        while self.i < len(self.s) and self.s[self.i].isspace():
            self.i += 1

    def peek(self, ch: str) -> bool:
        self.ws()
        return self.s.startswith(ch, self.i)

    def take(self, ch: str):
        if not self.peek(ch):
            raise ValueError("cannot parse type %r at %d: expected %r" % (self.s, self.i, ch))
        self.i += len(ch)

    def name(self) -> str:
        """A field name: a quoted identifier or a bare word."""
        self.ws()
        if self.s.startswith('"', self.i):
            out, j = [], self.i + 1
            while True:
                k = self.s.index('"', j)
                if self.s.startswith('""', k):
                    out.append(self.s[j:k] + '"')
                    j = k + 2
                    continue
                out.append(self.s[j:k])
                self.i = k + 1
                return "".join(out)
        m = re.compile(r"[^\s,()\[\]]+").match(self.s, self.i)
        if not m:
            raise ValueError("cannot parse type %r at %d: expected a name" % (self.s, self.i))
        self.i = m.end()
        return m.group(0)

    def typ(self) -> DType:
        self.ws()
        start = self.i
        m = re.compile(r"[A-Za-z_][A-Za-z0-9_]*(?:\s+(?:WITH|WITHOUT)\s+TIME\s+ZONE)?", re.I).match(self.s, self.i)
        if not m:
            raise ValueError("cannot parse type %r at %d" % (self.s, self.i))
        self.i = m.end()
        word = " ".join(m.group(0).upper().split())
        word = word.replace(" WITHOUT TIME ZONE", "")
        base = _ALIASES.get(word, word)
        t: DType
        if base in ("STRUCT", "UNION"):
            self.take("(")
            fields = []
            while True:
                n = self.name()
                fields.append((n, self.typ()))
                if self.peek(","):
                    self.take(",")
                    continue
                break
            self.take(")")
            t = DType(base, fields=tuple(fields))
        elif base == "MAP":
            self.take("(")
            k = self.typ()
            self.take(",")
            v = self.typ()
            self.take(")")
            t = DType("MAP", key=k, value=v)
        elif self.peek("("):
            self.take("(")
            j = self.s.index(")", self.i)
            args = tuple(int(a) if a.strip().lstrip("-").isdigit() else a.strip() for a in self.s[self.i:j].split(","))
            self.i = j + 1
            t = DType(base, args=args)
        else:
            t = DType(base)
        while self.peek("["):
            self.take("[")
            self.ws()
            m = re.compile(r"\d*").match(self.s, self.i)
            self.i = m.end()
            self.take("]")
            t = DType("ARRAY", elem=t, size=int(m.group(0))) if m.group(0) else DType("LIST", elem=t)
        return DType(t.base, t.args, t.elem, t.size, t.fields, t.key, t.value, self.s[start:self.i].strip())


def parse(text: str) -> DType:
    if isinstance(text, DType):
        return text
    p = _Parser(text)
    t = p.typ()
    p.ws()
    if p.i != len(p.s):
        raise ValueError("cannot parse type %r: unexpected %r" % (text, p.s[p.i:]))
    return t


# ------------------------------------------------------------------------------------------------ exact equality

_FLOATS = ("FLOAT", "DOUBLE")


def needs_exact(t: DType) -> bool:
    """True when DuckDB's `=` merges values of this type that the source keeps apart."""
    if t.base in _FLOATS or t.base == "INTERVAL":
        return True
    return any(needs_exact(c) for c in t.children())


def exact(expr: str, t: DType, depth: int = 0) -> str:
    """SQL: a value whose DuckDB equality (and GROUP BY) is the exact equality of `expr`: -0 differs from 0, every
    NaN is one value, `1 mon` differs from `30 days`; NULL stays NULL. The identity for every type whose `=` is
    already exact."""
    if not needs_exact(t):
        return expr
    if t.base == "DOUBLE":
        return "(CASE WHEN isnan(%s) THEN %d ELSE CAST(CAST(%s AS BIT) AS BIGINT) END)" % (expr, NAN_BITS_DOUBLE, expr)
    if t.base == "FLOAT":
        return "(CASE WHEN isnan(%s) THEN %d ELSE CAST(CAST(%s AS BIT) AS INTEGER) END)" % (expr, NAN_BITS_FLOAT, expr)
    if t.base == "INTERVAL":
        # DuckDB renders months, days and microseconds separately and never normalises them in the text.
        return "CAST(%s AS VARCHAR)" % expr
    v = "_ax%d" % depth
    if t.base in ("LIST", "ARRAY"):
        return "list_transform(%s, lambda %s: %s)" % (expr, v, exact(v, t.elem, depth + 1))
    if t.base == "MAP":
        return ("list_transform(map_entries(%s), lambda %s: struct_pack(k := %s, v := %s))"
                % (expr, v, exact("%s.key" % v, t.key, depth + 1), exact("%s.value" % v, t.value, depth + 1)))
    if t.base == "STRUCT":
        parts = ", ".join("%s := %s" % (quote_ident(n), exact("struct_extract(%s, %s)" % (expr, quote_literal(n)),
                                                                  ft, depth + 1))
                          for n, ft in t.fields)
        return "(CASE WHEN %s IS NULL THEN NULL ELSE struct_pack(%s) END)" % (expr, parts)
    if t.base == "UNION":
        parts = ", ".join("%s := %s" % (quote_ident("m_" + n), exact("union_extract(%s, %s)" % (expr, quote_literal(n)),
                                                                        ft, depth + 1))
                          for n, ft in t.fields)
        return ("(CASE WHEN %s IS NULL THEN NULL ELSE struct_pack(_tag := CAST(union_tag(%s) AS VARCHAR), %s) END)"
                % (expr, expr, parts))
    raise ValueError("no exact form for %s" % t.sql())        # pragma: no cover - needs_exact covers the rest


# ------------------------------------------------------------------------------------------------ writes


def affected(t: DType) -> bool:
    """True when an UPDATE of this type can store nothing although the value changed (fact 2 of the module)."""
    if t.base in _FLOATS or t.base == "INTERVAL":
        return True
    if t.base == "STRUCT":
        return any(affected(ft) for _, ft in t.fields)
    return False


def rewrites_row(t: DType) -> bool:
    """True when an UPDATE that sets this type is executed as delete + insert of the whole row (fact 3)."""
    if t.base in ("LIST", "ARRAY", "MAP", "UNION"):
        return True
    return any(rewrites_row(c) for c in t.children())


def sentinel(expr: str, t: DType) -> str:
    """SQL: a value of type t that never compares equal (C++ `==`, per stored slot) to `expr` where `expr` is not
    NULL. Written before the real value, it makes DuckDB store the real value (fact 2)."""
    if t.base in _FLOATS:
        return "CAST(CASE WHEN %s = 1 THEN 2 ELSE 1 END AS %s)" % (expr, t.base)
    if t.base == "INTERVAL":
        return ("(CASE WHEN %s = INTERVAL '1 microsecond' THEN INTERVAL '2 microseconds' "
                "ELSE INTERVAL '1 microsecond' END)" % expr)
    if t.base == "STRUCT":
        parts = []
        for n, ft in t.fields:
            child = "struct_extract(%s, %s)" % (expr, quote_literal(n))
            parts.append("%s := %s" % (quote_ident(n), sentinel(child, ft) if affected(ft) else child))
        return "(CASE WHEN %s IS NULL THEN NULL ELSE CAST(struct_pack(%s) AS %s) END)" % (expr, ", ".join(parts),
                                                                                         t.sql())
    raise ValueError("type %s needs no sentinel" % t.sql())
