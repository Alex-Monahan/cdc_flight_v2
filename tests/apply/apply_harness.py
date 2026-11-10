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
