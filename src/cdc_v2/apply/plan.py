"""The plan: from a change set to the relations that the statements read (docs/design/apply.md section 3).

The plan is a list of SQL steps. Each step materialises one relation from the change set and earlier relations.
The same text runs in a local DuckDB connection over Arrow data (local form) and inside the caller's MotherDuck
transaction over the rows already there (relation form); a `Backend` decides where (`backend.py`).

Tables with a key: the model is "the source table, one key at a time" (docs/design/apply.md 2). Per exact key,
records are ordered by (transaction, seq). `c` adds a row version (an *arrival*) at the back of the key's queue,
`d` removes the front one, `u` changes the front one; the row the destination holds when the change set starts is
arrival 0 if the source had it then. Whether it had it (`p`) follows from the records almost always; where it does
not (a `c` and a later `d` in one transaction, with nothing before), stream mode asks the destination and catch-up
mode reports the key as unresolved. A key change (`d` of the old key directly followed by `c` with `k<i>` set)
links the new arrival to the arrival that `d` removed; unchanged values are found by following these links back.

One rule for every step (docs/design/apply.md 5): a value of a row never passes through an aggregate, a window or
QUALIFY. DuckDB normalises -0 and NaN inside nested values there (and in a CREATE TABLE AS with QUALIFY, even
inside a STRUCT). Those operators see only integers and exact keys (`types.exact`, which hold no FLOAT, DOUBLE or
INTERVAL); a value is fetched at the end, by a join on the record number that supplies it.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, List

from .meta import Column, TableMeta
from .types import exact, needs_exact

__all__ = ["Step", "keyed_steps", "keyless_steps", "key_join", "flat_x", "keyed_stats_sql", "keyless_stats_sql"]


@dataclass
class Step:
    """`kind` is 'sql' (materialise `sql` as `name`) or 'lookup' (count destination rows per exact key of the
    relation `source`, into `name` with columns x<i>..., n)."""
    name: str
    kind: str
    sql: str = ""
    source: str = ""
    params: tuple = ()


def struct_of(parts: list) -> str:
    return "struct_pack(%s)" % ", ".join("%s := %s" % (n, e) for n, e in parts)


def flat_x(meta: TableMeta, struct_expr: str, prefix: str = "x") -> str:
    """The exact-key struct as columns x<i> (or rx<i> ...)."""
    return ", ".join("%s.x%d AS %s%d" % (struct_expr, c.position, prefix, c.position) for c in meta.key_columns)


def key_join(meta: TableMeta, t_alias: str, rel_alias: str, prefix: str = "x") -> str:
    """Join condition of a destination row with a relation's exact key columns: ex(t.k) = n.x<i> for every key
    column. For a key type whose `=` is exact this is a plain column comparison (a hash join on the raw column)."""
    return " AND ".join("%s = %s.%s%d" % (exact("%s.%s" % (t_alias, c.qname), c.dtype), rel_alias, prefix, c.position)
                        for c in meta.key_columns)


def _ordered(src_sql: str, order_col: str, cols: str, params: tuple) -> List[Step]:
    """r0: the records with a record number in (order column, seq) order. The number is computed on the order
    columns alone and joined back, so that no value passes through the window."""
    return [
        Step("r0", "sql", "SELECT {oc} AS tord, seq, CAST(op AS VARCHAR) AS op, {cols} FROM {src}".format(
            oc=order_col, cols=cols, src=src_sql), params=tuple(params)),
    ]


# ------------------------------------------------------------------------------------------------ keyed


def keyed_steps(meta: TableMeta, src_sql: str, order_col: str, ucols: List[Column], mode: str,
                rel: Callable[[str], str], src_params: tuple = ()) -> List[Step]:
    keys = meta.key_columns
    ucols = [c for c in ucols if not c.key]
    upos = {c.position for c in ucols}
    nonu = [c for c in meta.value_columns if c.position not in upos]
    allcols = list(meta.columns)
    R = rel
    kx = struct_of([("x%d" % c.position, exact(c.a, c.dtype)) for c in keys])
    okx_null = " AND ".join("%s IS NULL" % c.k for c in keys)
    okx = "CASE WHEN %s THEN NULL ELSE %s END" % (okx_null, struct_of([("x%d" % c.position, exact(c.k, c.dtype))
                                                                         for c in keys]))
    kv = struct_of([(c.a, c.a) for c in keys])
    src_cols = ", ".join([c.a for c in allcols] + [c.u for c in ucols] + [c.k for c in keys])
    steps = _ordered(src_sql, order_col, src_cols, src_params)

    # r: the records with their record number and exact keys (values pass through a join only).
    steps.append(Step("r", "sql", """
SELECT o.rn, q.tord, q.seq, q.op, {kx} AS kx, {okx} AS okx, {kv} AS kv,
       ({kn}) AS key_null, (NOT ({okn}) AND ({okp})) AS okey_partial, {a}{u}
FROM {r0} AS q
JOIN (SELECT tord, seq, row_number() OVER (ORDER BY tord, seq) AS rn FROM {r0}) AS o
  ON o.tord = q.tord AND o.seq = q.seq
""".format(kx=kx, okx=okx, kv=kv, a=", ".join("q.%s" % c.a for c in allcols),
           u="".join(", coalesce(q.%s, false) AS %s" % (c.u, c.u) for c in ucols), r0=R("r0"),
           kn=" OR ".join("q.%s IS NULL" % c.a for c in keys), okn=okx_null,
           okp=" OR ".join("q.%s IS NULL" % c.k for c in keys))))

    # w: running sums per key, and the record just before (a key change is a d directly followed by its c).
    steps.append(Step("w", "sql", """
SELECT *, lag(op) OVER o AS prev_op, lag(kx) OVER o AS prev_kx, lag(tord) OVER o AS prev_tord,
       lag(d_before) OVER o AS prev_dbefore
FROM (
  SELECT rn, tord, op, kx, okx,
         CAST(sum(CASE op WHEN 'c' THEN 1 WHEN 'd' THEN -1 ELSE 0 END) OVER wk AS BIGINT) AS s_after,
         CAST(coalesce(sum(CASE WHEN op = 'd' THEN 1 ELSE 0 END) OVER wb, 0) AS BIGINT) AS d_before,
         CAST(coalesce(sum(CASE WHEN op = 'c' THEN 1 ELSE 0 END) OVER wb, 0) AS BIGINT) AS c_before,
         rn = max(rn) OVER (PARTITION BY kx, tord) AS txn_last
  FROM {r}
  WINDOW wk AS (PARTITION BY kx ORDER BY rn ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),
         wb AS (PARTITION BY kx ORDER BY rn ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)
) AS s
WINDOW o AS (ORDER BY rn)
""".format(r=R("r"))))

    # kb: per key, the bounds of p (did the source hold the key when the change set starts?).
    steps.append(Step("kb", "sql", """
SELECT kx,
       greatest(0, coalesce(max(CASE WHEN op = 'd' THEN -s_after WHEN op = 'u' THEN 1 - s_after END), 0),
                coalesce(max(CASE WHEN txn_last THEN -s_after END), 0)) AS lo,
       least(1, min(CASE WHEN txn_last THEN 1 - s_after END)) AS hi,
       CAST(sum(CASE WHEN op = 'c' THEN 1 ELSE 0 END) AS BIGINT) AS n_c,
       CAST(sum(CASE WHEN op = 'd' THEN 1 ELSE 0 END) AS BIGINT) AS n_d,
       min(rn) AS first_rn
FROM {w} GROUP BY kx
""".format(w=R("w"))))

    if mode == "stream":
        steps.append(Step("cand_p", "sql", "SELECT %s FROM %s WHERE lo < hi" % (flat_x(meta, "kx"), R("kb"))))
        steps.append(Step("present", "lookup", source="cand_p"))
        undet = "CASE WHEN pr.n > 0 THEN 1 ELSE 0 END"
        pjoin = "LEFT JOIN %s AS pr ON %s" % (R("present"), " AND ".join("pr.x%d = kb.kx.x%d" % (c.position, c.position)
                                                                       for c in keys))
    else:
        undet, pjoin = "NULL", ""
    steps.append(Step("kp", "sql", """
SELECT kb.*, CAST(CASE WHEN lo = hi THEN lo WHEN lo < hi THEN {undet} END AS BIGINT) AS p, lo > hi AS bad
FROM {kb} AS kb {pjoin}
""".format(undet=undet, kb=R("kb"), pjoin=pjoin)))

    # ev: every record with its arrival index (c: the arrival it creates; d: the one it removes; u: the one it
    # changes) and, for the c of a key change, the link to the d before it.
    steps.append(Step("ev", "sql", """
SELECT w.rn, w.op, w.kx, w.okx, kp.p,
       CASE WHEN w.op = 'c' THEN kp.p + w.c_before ELSE w.d_before END AS idx,
       CASE WHEN w.op <> 'c' OR w.okx IS NULL THEN 'none'
            WHEN w.prev_op = 'd' AND w.prev_kx = w.okx AND w.prev_tord = w.tord THEN 'ok' ELSE 'broken' END AS plink,
       w.prev_dbefore AS pidx
FROM {w} AS w JOIN {kp} AS kp ON kp.kx = w.kx
WHERE kp.p IS NOT NULL
""".format(w=R("w"), kp=R("kp"))))

    # arr: one row per arrival that a record creates or changes: its newest record (lrn, which carries every
    # column that cannot be unchanged) and, per unchanged-possible column, the newest record that carries it.
    srcs = "".join(", max(CASE WHEN NOT r.%s THEN e.rn END) OVER wa AS s%d" % (c.u, c.position) for c in ucols)
    steps.append(Step("arr", "sql", """
WITH s AS (
  SELECT e.kx, e.idx, e.p, e.rn AS lrn, max(CASE WHEN e.op = 'c' THEN e.rn END) OVER wa AS crn{srcs}
  FROM {ev} AS e JOIN {r} AS r ON r.rn = e.rn
  WHERE e.op IN ('c', 'u')
  WINDOW wa AS (PARTITION BY e.kx, e.idx)
  QUALIFY row_number() OVER (PARTITION BY e.kx, e.idx ORDER BY e.rn DESC) = 1
)
SELECT s.*, c.plink AS c_plink, c.okx AS pkx, c.pidx AS pidx
FROM s LEFT JOIN {ev} AS c ON c.rn = s.crn
""".format(srcs=srcs, ev=R("ev"), r=R("r"))))

    snames = ["s%d" % c.position for c in ucols]
    steps.append(Step("arrx", "sql", """
SELECT kx, idx, p, CASE WHEN idx < p THEN 'dest' ELSE c_plink END AS plink, pkx, pidx, crn, lrn{scopy} FROM {arr}
UNION ALL BY NAME
SELECT kp.kx, CAST(0 AS BIGINT) AS idx, kp.p, 'dest' AS plink, NULL AS pkx, NULL AS pidx, NULL AS crn,
       NULL AS lrn{snull}
FROM {kp} AS kp
WHERE kp.p = 1 AND NOT EXISTS (SELECT 1 FROM {arr} AS a WHERE a.kx = kp.kx AND a.idx = 0)
""".format(scopy="".join(", " + s for s in snames), snull="".join(", CAST(NULL AS BIGINT) AS " + s for s in snames),
           arr=R("arr"), kp=R("kp"))))

    # fin: the arrival each key holds when the change set ends.
    steps.append(Step("fin", "sql", """
SELECT a.*, kp.first_rn FROM {kp} AS kp JOIN {arrx} AS a ON a.kx = kp.kx AND a.idx = kp.n_d
WHERE kp.p IS NOT NULL AND kp.p + kp.n_c - kp.n_d = 1
""".format(kp=R("kp"), arrx=R("arrx"))))

    # res: where each unchanged column's value comes from, following key-change links back to a destination row.
    if ucols:
        any_null = " OR ".join("%s.s%d IS NULL" % ("{t}", c.position) for c in ucols)
        steps.append(Step("res", "sql", """
WITH RECURSIVE res AS (
  SELECT f.kx AS fkx, f.kx AS ckx, f.idx AS cidx, f.plink AS clink, f.pkx AS cpkx, f.pidx AS cpidx,
         0 AS depth{sbase}
  FROM {fin} AS f WHERE {fnull}
  UNION ALL
  SELECT res.fkx, a.kx, a.idx, a.plink, a.pkx, a.pidx, res.depth + 1{sstep}
  FROM res JOIN {arrx} AS a ON a.kx = res.cpkx AND a.idx = res.cpidx
  WHERE res.clink = 'ok' AND ({rnull})
)
SELECT * FROM res QUALIFY row_number() OVER (PARTITION BY fkx ORDER BY depth DESC) = 1
""".format(sbase="".join(", f.s%d" % c.position for c in ucols),
           sstep="".join(", coalesce(res.s%d, a.s%d) AS s%d" % (c.position, c.position, c.position) for c in ucols),
           fin=R("fin"), arrx=R("arrx"), fnull=any_null.replace("{t}", "f"), rnull=any_null.replace("{t}", "res"))))
        all_set = " AND ".join("res.s%d IS NOT NULL" % c.position for c in ucols)
        res_join = "LEFT JOIN %s AS res ON res.fkx = f.kx" % R("res")
        how = ("CASE WHEN res.fkx IS NULL OR ({all}) THEN 'complete' WHEN res.clink = 'dest' THEN 'ref' "
               "ELSE 'unres' END".format(all=all_set))
        rkx = "CASE WHEN res.fkx IS NOT NULL AND NOT ({all}) AND res.clink = 'dest' THEN res.ckx END".format(all=all_set)
        svals = "".join(", CASE WHEN res.fkx IS NULL THEN f.s{p} ELSE res.s{p} END AS s{p}".format(p=c.position)
                        for c in ucols)
    else:
        res_join, how, rkx, svals = "", "'complete'", "CASE WHEN false THEN f.kx END", ""
    steps.append(Step("dec", "sql", """
SELECT f.kx, f.crn, f.first_rn, f.lrn, {how} AS how, {rkx} AS rkx{svals}
FROM {fin} AS f {res_join}
""".format(how=how, rkx=rkx, svals=svals, fin=R("fin"), res_join=res_join)))

    # act: what happens to each final key. Catch-up checks the destination rows it would take values from.
    if mode == "catch_up":
        steps.append(Step("cand_r", "sql", """
SELECT DISTINCT {fx} FROM (
  SELECT rkx AS kx FROM {dec} WHERE how = 'ref'
  UNION ALL SELECT kx FROM {dec} WHERE how IN ('ref', 'unres') AND crn IS NOT NULL AND crn = first_rn
) AS c
""".format(fx=flat_x(meta, "kx"), dec=R("dec"))))
        steps.append(Step("rootcnt", "lookup", source="cand_r"))
        rc = lambda alias, sx: " AND ".join("%s.x%d = %s.x%d" % (alias, c.position, sx, c.position) for c in keys)
        steps.append(Step("act0", "sql", """
SELECT d.*,
  CASE WHEN d.how = 'complete' THEN 'ins'
       WHEN d.how = 'ref' AND r2.n = 1 AND kr.n_c = 0 THEN 'upd'
       WHEN d.how IN ('ref', 'unres') AND d.crn IS NOT NULL AND d.crn = d.first_rn AND r3.n = 1 THEN 'upd3'
       ELSE 'unres' END AS action0
FROM {dec} AS d
LEFT JOIN {rc} AS r2 ON {j2}
LEFT JOIN {kp} AS kr ON kr.kx = d.rkx
LEFT JOIN {rc} AS r3 ON {j3}
""".format(dec=R("dec"), rc=R("rootcnt"), kp=R("kp"), j2=rc("r2", "d.rkx"), j3=rc("r3", "d.kx"))))
        root = "CASE WHEN action0 = 'upd3' THEN kx ELSE rkx END"
        action = "CASE WHEN action0 = 'upd3' THEN 'upd' ELSE action0 END"
        src_act = R("act0")
    else:
        root = "rkx"
        action = "CASE how WHEN 'complete' THEN 'ins' WHEN 'ref' THEN 'upd' ELSE 'unres' END"
        src_act = R("dec")
    if any(needs_exact(c.dtype) for c in keys):
        # a key that changes only in representation (-0 -> 0, 1 mon -> 30 days) cannot be moved by UPDATE: the
        # stored key would stay; such a row is copied (docs/design/apply.md 3.4). Raw keys compared with `=`.
        rep = " AND ".join("rr.%s = rf.%s" % (c.a, c.a) for c in keys)
        act_sql = """
SELECT a.* REPLACE (CASE WHEN a.action = 'upd' AND a.root <> a.kx AND {rep} THEN 'copy' ELSE a.action END AS action)
FROM (SELECT *, {root} AS root, {action} AS action FROM {src}) AS a
LEFT JOIN {kp} AS kr ON kr.kx = a.root
LEFT JOIN {r} AS rr ON rr.rn = kr.first_rn
LEFT JOIN {r} AS rf ON rf.rn = a.lrn
""".format(rep=rep, root=root, action=action, src=src_act, kp=R("kp"), r=R("r"))
    else:
        act_sql = "SELECT *, {root} AS root, {action} AS action FROM {src}".format(root=root, action=action, src=src_act)
    steps.append(Step("act", "sql", act_sql))

    # outputs: values are read from the records by record number
    # stream: only keys the source held when the change set started (p = 1) can have a destination row; catch-up:
    # every key of the log, because the table was loaded under other snapshots.
    steps.append(Step("del", "sql", """
SELECT {fx} FROM {kp} AS kp
WHERE kp.p IS NOT NULL{pfilter}
  AND NOT EXISTS (SELECT 1 FROM {act} AS a WHERE a.action IN ('upd', 'copy') AND a.root = kp.kx)
  AND NOT EXISTS (SELECT 1 FROM {act} AS a WHERE a.action = 'unres' AND a.kx = kp.kx)
""".format(fx=flat_x(meta, "kp.kx"), kp=R("kp"), act=R("act"), pfilter=" AND kp.p = 1" if mode == "stream" else "")))
    vjoins = "JOIN {r} AS rl ON rl.rn = a.lrn".format(r=R("r")) + "".join(
        " LEFT JOIN {r} AS r{p} ON r{p}.rn = a.s{p}".format(r=R("r"), p=c.position) for c in ucols)
    vcols = (["rl.%s AS %s" % (c.a, c.a) for c in keys] + ["rl.%s AS %s" % (c.a, c.a) for c in nonu]
             + ["r%d.%s AS %s" % (c.position, c.a, c.a) for c in ucols])
    steps.append(Step("ins", "sql", "SELECT %s FROM %s AS a %s WHERE a.action = 'ins'"
                      % (", ".join(vcols), R("act"), vjoins)))
    upd_cols = ([flat_x(meta, "a.root", "rx"), flat_x(meta, "a.kx")] + vcols
                + ["(a.s%d IS NOT NULL) AS h%d" % (c.position, c.position) for c in ucols])
    for name in ("upd", "copy"):
        steps.append(Step(name, "sql", "SELECT %s FROM %s AS a %s WHERE a.action = '%s'"
                          % (", ".join(upd_cols), R("act"), vjoins, name)))
    if mode == "catch_up":
        kvs = ", ".join("q.%s AS %s" % (c.a, c.a) for c in keys)
        steps.append(Step("unres", "sql", """
SELECT {kvs} FROM {act} AS a JOIN {r} AS q ON q.rn = a.lrn WHERE a.action = 'unres'
UNION ALL SELECT {kvs} FROM {kp} AS kp JOIN {r} AS q ON q.rn = kp.first_rn WHERE kp.p IS NULL
""".format(kvs=kvs, act=R("act"), kp=R("kp"), r=R("r"))))
    return steps


def keyed_stats_sql(meta: TableMeta, ucols: List[Column], rel: Callable[[str], str], mode: str) -> str:
    """One row: records by op, problems, rows per output, and per unchanged column how many updates carry it."""
    ucols = [c for c in ucols if not c.key]
    parts = [
        "(SELECT count(*) FROM {r}) AS n_records",
        "(SELECT count(*) FROM {r} WHERE op = 'c') AS n_c",
        "(SELECT count(*) FROM {r} WHERE op = 'u') AS n_u",
        "(SELECT count(*) FROM {r} WHERE op = 'd') AS n_d",
        "(SELECT count(*) FROM {r} WHERE op NOT IN ('c', 'u', 'd') OR op IS NULL) AS n_badop",
        "(SELECT count(*) FROM {r} WHERE key_null) AS n_keynull",
        "(SELECT count(*) FROM {r} WHERE okey_partial) AS n_okeypartial",
        "(SELECT count(*) FROM (SELECT tord, seq FROM {r0} GROUP BY ALL HAVING count(*) > 1)) AS n_dupseq",
        "(SELECT count(*) FROM {kp} WHERE bad) AS n_bad",
        "(SELECT count(*) FROM {act} WHERE action = 'unres') AS n_unres_final",
        "(SELECT count(*) FROM {del}) AS n_del",
        "(SELECT count(*) FROM {ins}) AS n_ins",
        "(SELECT count(*) FROM {upd}) AS n_upd",
        "(SELECT count(*) FROM {copy}) AS n_copy",
    ]
    if mode == "catch_up":
        parts.append("(SELECT count(*) FROM {unres}) AS n_unres")
    for c in ucols:
        parts.append("(SELECT count(*) FROM {upd} WHERE h%d) AS h%d" % (c.position, c.position))
    names = {n: rel(n) for n in ("r0", "r", "kp", "act", "del", "ins", "upd", "copy", "unres")}
    return "SELECT " + ", ".join(p.format(**names) for p in parts)


# ------------------------------------------------------------------------------------------------ keyless


def keyless_steps(meta: TableMeta, src_sql: str, order_col: str, ucols: List[Column],
                  rel: Callable[[str], str], src_params: tuple = ()) -> List[Step]:
    """delta: per exact row image, +1 for every new image and -1 for every old image (docs/design/apply.md 4).
    The images are grouped by their exact form; a representative record is chosen by number and its values are
    read by a join."""
    cols = list(meta.columns)
    upos = {c.position for c in ucols}
    newv = {c.position: ("CASE WHEN coalesce(%s, false) THEN %s ELSE %s END" % (c.u, c.b, c.a)
                         if c.position in upos else c.a) for c in cols}
    src_cols = ", ".join([c.a for c in cols] + [c.b for c in cols] + [c.u for c in ucols])
    ecols = ", ".join("e%d" % c.position for c in cols)
    steps = _ordered(src_sql, order_col, src_cols, src_params)
    steps.append(Step("r", "sql", """
SELECT o.rn, q.* FROM {r0} AS q
JOIN (SELECT tord, seq, row_number() OVER (ORDER BY tord, seq) AS rn FROM {r0}) AS o ON o.tord = q.tord AND o.seq = q.seq
""".format(r0=rel("r0"))))
    old_e = ", ".join("%s AS e%d" % (exact(c.b, c.dtype), c.position) for c in cols)
    new_e = ", ".join("%s AS e%d" % (exact(newv[c.position], c.dtype), c.position) for c in cols)
    steps.append(Step("img", "sql", """
SELECT rn * 2 AS iid, -1 AS w, {old_e} FROM {r} WHERE op IN ('d', 'u')
UNION ALL
SELECT rn * 2 + 1 AS iid, 1 AS w, {new_e} FROM {r} WHERE op IN ('c', 'u')
""".format(old_e=old_e, new_e=new_e, r=rel("r"))))
    steps.append(Step("delta", "sql", """
SELECT row_number() OVER () AS gid, * FROM (
  SELECT {ecols}, CAST(sum(w) AS BIGINT) AS n, min(iid) AS iid FROM {img} GROUP BY {ecols} HAVING sum(w) <> 0
) AS g
""".format(ecols=ecols, img=rel("img"))))
    steps.append(Step("neg", "sql", "SELECT gid, -n AS n, %s FROM %s WHERE n < 0" % (ecols, rel("delta"))))
    def _new(c, q):
        if c.position in upos:
            return "CASE WHEN coalesce({q}.{u}, false) THEN {q}.{b} ELSE {q}.{a} END".format(q=q, u=c.u, b=c.b, a=c.a)
        return "%s.%s" % (q, c.a)
    vals = ", ".join("CASE WHEN d.iid %% 2 = 0 THEN q.%s ELSE %s END AS v%d" % (c.b, _new(c, "q"), c.position)
                     for c in cols)
    steps.append(Step("pos", "sql", "SELECT d.n, %s FROM %s AS d JOIN %s AS q ON q.rn = d.iid // 2 WHERE d.n > 0"
                      % (vals, rel("delta"), rel("r"))))
    return steps


def keyless_stats_sql(rel: Callable[[str], str]) -> str:
    return ("SELECT (SELECT count(*) FROM {r}) AS n_records, (SELECT count(*) FROM {r} WHERE op = 'c') AS n_c, "
            "(SELECT count(*) FROM {r} WHERE op = 'u') AS n_u, (SELECT count(*) FROM {r} WHERE op = 'd') AS n_d, "
            "(SELECT count(*) FROM {r} WHERE op NOT IN ('c', 'u', 'd') OR op IS NULL) AS n_badop, "
            "(SELECT count(*) FROM (SELECT tord, seq FROM {r0} GROUP BY ALL HAVING count(*) > 1)) AS n_dupseq, "
            "(SELECT count(*) FROM {neg}) AS n_neg_groups, (SELECT coalesce(sum(n), 0) FROM {neg}) AS n_neg_rows, "
            "(SELECT count(*) FROM {pos}) AS n_pos_groups, (SELECT coalesce(sum(n), 0) FROM {pos}) AS n_pos_rows"
            ).format(r0=rel("r0"), r=rel("r"), neg=rel("neg"), pos=rel("pos"))
