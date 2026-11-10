"""Drive apply against a destination (local DuckDB or MotherDuck) and compare with the simulator."""
from __future__ import annotations

import random
from typing import List, Optional

import duckdb

from apply_sim import (KeyedSource, KeylessSource, Rec, canon_rows, change_set_table, create_table,
                       literal_rows_canon, random_meta)
from apply_standin import StandInTx
from cdc_v2.apply import (ApplySettings, LocalChangeSet, RelationChangeSet, Target, apply_group,
                          reduce_duplicates)
from cdc_v2.apply.types import quote_ident

SCHEMA = "s p.ä"


def local_conn():
    """A destination on local DuckDB (a database of its own: tests create the same schema names)."""
    con = duckdb.connect()
    con.execute("SET TimeZone = 'UTC'")
    return con


def split_by_txn(recs: List[Rec], cuts: List[int]) -> List[List[Rec]]:
    """Split records into change sets at transaction boundaries: `cuts` are transaction numbers that start a new
    change set."""
    out, cur = [], []
    cuts = set(cuts)
    last = None
    for r in recs:
        if cur and r.txn != last and r.txn in cuts:
            out.append(cur)
            cur = []
        cur.append(r)
        last = r.txn
    if cur:
        out.append(cur)
    return out


def apply_sets(conn, target, meta, sets: List[List[Rec]], mode="stream", form="local", one_tx=False,
               settings: Optional[ApplySettings] = None, stage_conn=None):
    """Apply each change set (one transaction each, or all in one). Returns the list of ApplyCounts."""
    out = []

    def one(tx, recs, i):
        rows = change_set_table(meta, recs)
        if form == "local":
            cs = LocalChangeSet(rows, None, meta)
        else:
            name = "_cdc_stage_test_%d" % i
            # stage the rows as a pending log would: commit_lsn instead of txn
            conn.register("stage_src", rows)
            conn.execute("CREATE SCHEMA IF NOT EXISTS _cdc_stage")
            conn.execute("CREATE OR REPLACE TABLE _cdc_stage.%s AS SELECT * EXCLUDE (txn), "
                         "CAST(txn * 16 + 1000 AS UBIGINT) AS commit_lsn, CAST(txn AS UBIGINT) AS xid, "
                         "TIMESTAMPTZ '2026-01-01 00:00:00+00' AS commit_ts FROM stage_src" % name)
            conn.unregister("stage_src")
            cs = RelationChangeSet("SELECT * FROM _cdc_stage.%s WHERE commit_lsn >= ?" % name, (0,), meta)
        return apply_group(tx, target, cs, mode=mode, settings=settings)

    if one_tx:
        with StandInTx(conn) as tx:
            for i, s in enumerate(sets):
                out.append(one(tx, s, i))
    else:
        for i, s in enumerate(sets):
            with StandInTx(conn) as tx:
                out.append(one(tx, s, i))
    return out


def dest_canon(conn, target, meta):
    return canon_rows(conn, target.sql(), list(meta.columns))


# ------------------------------------------------------------------------------------------------ named cases


def key_canon(meta, table):
    if table is None or table.num_rows == 0:
        return set()
    from apply_sim import canon
    con = local_conn()
    con.register("u", table)
    out = {tuple(r) for r in con.execute("SELECT %s FROM u" % ", ".join(canon(c.a, c.type)
                                                                         for c in meta.key_columns)).fetchall()}
    con.close()
    return out


def run_case(case, conn, form="local", table="t"):
    """Run a named case on `conn` (local DuckDB or MotherDuck). Returns a dict for the evidence; asserts."""
    import pytest
    from apply_sim import lit_canon
    from cdc_v2.apply import ApplyInvariantError, ChangeSetError
    target = Target(SCHEMA, table)
    create_table(conn, SCHEMA, table, case.meta, case.start)
    before = dest_canon(conn, target, case.meta)
    info = {"case": case.name, "form": form, "mode": case.mode}
    if case.error:
        exc = {"invariant": ApplyInvariantError, "changeset": ChangeSetError}[case.error]
        with pytest.raises(exc):
            apply_sets(conn, target, case.meta, case.sets, mode=case.mode, form=form, one_tx=case.one_tx)
        assert dest_canon(conn, target, case.meta) == before, "the caller's roll-back leaves the table as it was"
        info["error"] = case.error
        return info
    counts = apply_sets(conn, target, case.meta, case.sets, mode=case.mode, form=form, one_tx=case.one_tx)
    got = dest_canon(conn, target, case.meta)
    want = literal_rows_canon(list(case.meta.columns), case.expect)
    assert got == want, "%s/%s: destination differs:\n extra %s\n missing %s" % (case.name, form, got - want,
                                                                                 want - got)
    info["statements"] = [c.statements for c in counts]
    info["shapes"] = [c.shapes for c in counts]
    if case.max_statements is not None and form == "local":
        assert all(c.statements <= case.max_statements for c in counts), info
    if case.unresolved is not None:
        got_u = set().union(*[key_canon(case.meta, c.unresolved) for c in counts])
        want_u = {tuple(lit_canon(l, c.type) for l, c in zip(k, case.meta.key_columns)) for k in case.unresolved}
        assert got_u == want_u, (got_u, want_u)
        info["unresolved"] = len(got_u)
    if case.twice:
        apply_sets(conn, target, case.meta, case.sets, mode=case.mode, form=form, one_tx=case.one_tx)
        again = dest_canon(conn, target, case.meta)
        assert again == got, "%s/%s: a second catch-up changed the table" % (case.name, form)
        info["idempotent"] = True
    return info


# ------------------------------------------------------------------------------------------------ scenarios


def stream_scenario(conn, seed: int, keyed: bool, form: str = "local", table: str = "t", n_txn: Optional[int] = None,
                    max_ops: int = 5, one_tx: Optional[bool] = None, value_kinds=None, key_kinds=None):
    """A random table and history; the history cut into change sets at random transaction boundaries and applied
    in stream mode, one transaction per change set or all in one. Returns a ledger entry; raises AssertionError
    when the destination differs from the source."""
    rng = random.Random(seed)
    meta, kinds = random_meta(rng, keyed, rng.randint(1, 4), value_kinds=value_kinds, key_kinds=key_kinds)
    src = (KeyedSource if keyed else KeylessSource)(meta, rng, kinds)
    src.seed_rows(rng.randint(0, 8))
    start = src.snapshot()
    n_txn = n_txn or rng.randint(1, 6)
    recs = []
    for _ in range(n_txn):
        recs += src.ops_txn(rng.randint(1, max_ops))
    target = Target(SCHEMA, table)
    create_table(conn, SCHEMA, table, meta, start)
    sets = split_by_txn(recs, [t for t in range(1, n_txn + 1) if rng.random() < 0.35])
    one_tx = rng.random() < 0.3 if one_tx is None else one_tx
    counts = apply_sets(conn, target, meta, sets, form=form, one_tx=one_tx)
    got = dest_canon(conn, target, meta)
    want = literal_rows_canon(list(meta.columns), src.snapshot())
    entry = {"seed": seed, "keyed": keyed, "form": form, "mode": "stream", "records": len(recs), "sets": len(sets),
             "one_tx": one_tx, "statements": sum(c.statements for c in counts),
             "types": sorted({c.type for c in meta.columns}), "ok": got == want}
    assert got == want, "seed %d: destination differs\n extra %s\n missing %s" % (seed, got - want, want - got)
    return entry


def catch_up_scenario(conn, seed: int, form: str = "local", table: str = "t", n_txn: int = 6, max_ops: int = 4,
                      value_kinds=None, key_kinds=None, rid_offset: int = 0, verbose: bool = False,
                      early_split: bool = False):
    """A history, a destination loaded under several snapshots (rows read twice or not at all when they moved
    between chunks), the log replayed in mode catch_up after reducing duplicates, in one call and split into two
    calls; then once more. Every key that is not reported unresolved must equal the source; the second replay
    must not change any such key; unresolved keys are keys of the log."""
    import zlib
    from collections import Counter
    from apply_sim import lit_canon
    rng = random.Random(seed)
    meta, kinds = random_meta(rng, True, rng.randint(1, 3), value_kinds=value_kinds, key_kinds=key_kinds)
    src = KeyedSource(meta, rng, kinds)
    import apply_sim
    apply_sim._Row._n = rid_offset                 # row ids decide which snapshot loads a row
    src.seed_rows(rng.randint(2, 10))
    snaps = [src.snapshot_full()]
    recs = []
    for _ in range(n_txn):
        recs += src.ops_txn(rng.randint(1, max_ops))
        snaps.append(src.snapshot_full())
    start = rng.randint(0, n_txn - 1)
    log = [r for r in recs if r.txn > start]
    m = rng.randint(1, 3)
    times = [rng.randint(start, n_txn) for _ in range(m)]
    dest_rows = [vals for j, s in enumerate(times) for rid, ver, vals in snaps[s]
                 if zlib.crc32(b"%d:%d" % (rid, ver)) % m == j]
    target = Target(SCHEMA, table)
    create_table(conn, SCHEMA, table, meta, dest_rows)
    txns = sorted({r.txn for r in log})
    # Precondition of mode catch_up (docs/design/apply.md 2.6): the first call reaches past every snapshot the
    # table was loaded under. A split is therefore at a transaction after the newest snapshot; `early_split` breaks
    # the precondition on purpose (test_catch_up_split_before_the_newest_snapshot shows what happens then).
    after = [t for t in txns if t > max(times)] if not early_split else txns[1:]
    split = rng.choice(after) if after and rng.random() < 0.5 else None
    parts = split_by_txn(log, [split] if split else [])
    with StandInTx(conn) as tx:
        cs_all = LocalChangeSet(change_set_table(meta, log), None, meta)
        removed = reduce_duplicates(tx, target, cs_all)
    counts = apply_sets(conn, target, meta, parts, mode="catch_up", form=form, one_tx=rng.random() < 0.5)
    d1 = dest_canon(conn, target, meta)
    again = apply_sets(conn, target, meta, [log], mode="catch_up", form=form)
    d2 = dest_canon(conn, target, meta)
    U1 = set().union(*[key_canon(meta, c.unresolved) for c in counts])
    U2 = set().union(*[key_canon(meta, c.unresolved) for c in again])
    final = literal_rows_canon(list(meta.columns), src.snapshot())
    kidx = [i for i, c in enumerate(meta.columns) if c.key]
    keyof = lambda row: tuple(row[i] for i in kidx)
    restrict = lambda cnt, U: Counter({r: n for r, n in cnt.items() if keyof(r) not in U})
    logkeys = ({tuple(lit_canon(r.a[c.position], c.type) for c in meta.key_columns) for r in log}
               | {tuple(lit_canon(r.k[c.position], c.type) for c in meta.key_columns) for r in log if r.k})
    if verbose:
        print("meta", [(c.position, c.name, c.type, c.key, c.unchanged_possible) for c in meta.columns])
        print("start txn", start, "times", times, "split", split)
        for r in log:
            print("  ", r)
        print("loaded", dest_rows)
        print("final", src.snapshot())
        print("d1-final", d1 - final, "\nfinal-d1", final - d1, "\nU1", U1)
    entry = {"seed": seed, "form": form, "mode": "catch_up", "records": len(log), "snapshots": m,
             "loaded_rows": len(dest_rows), "duplicates_removed": removed, "split": bool(split),
             "unresolved": len(U1), "unresolved_second": len(U2), "types": sorted({c.type for c in meta.columns})}
    assert restrict(d1, U1) == restrict(final, U1), "seed %d: catch-up differs outside the unresolved keys" % seed
    assert restrict(d2, U1 | U2) == restrict(d1, U1 | U2), "seed %d: a second catch-up changed the table" % seed
    assert U1 <= logkeys and U2 <= logkeys, "seed %d: an unresolved key that is not in the log" % seed
    entry["ok"] = True
    return entry
