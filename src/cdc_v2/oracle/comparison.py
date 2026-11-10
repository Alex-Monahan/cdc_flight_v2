"""Compare Postgres source tables with MotherDuck destination tables.

How one comparison runs:

0. **Position.** A comparison is tied to a source position `Q` (from `Workload.quiesce()` / `pause()`). Before
   the source snapshot is taken, the source must be at or past `Q`: a standby's `pg_last_wal_replay_lsn()` is
   awaited (within `position_wait_s`), a primary's `pg_current_wal_insert_lsn()` is checked. Inside the
   destination's read transaction its applied position (S's reader, by slot name) must be at or past `Q`; it is
   not awaited there. Either side short of `Q` raises `PositionNotReached`: the comparison is refused, never
   reported equal. A comparison without a position must be asked for with `unpositioned=True`; its result says so.
1. The source is read under **one snapshot**: a coordinator transaction (`REPEATABLE READ, READ ONLY`) exports its
   snapshot, and every worker connection imports it (`SET TRANSACTION SNAPSHOT`). The destination is read inside
   **one transaction** on one connection. Every MotherDuck statement has a deadline; a standby that cancels the
   snapshot for a recovery conflict (SQLSTATE 40001) makes the whole comparison start again, a bounded number of
   times.
2. Catalogs of both sides are read in that snapshot and that transaction. A table or column that is missing or extra
   on the destination is a difference. Destination schemas `_cdc` and `_cdc_stage*` are ignored.
3. **Checksum pass.** Every row is turned into a row text: the canonical form of each compared column
   (`canonical.py`), each wrapped in a token that encodes NULL and the text's length. The row hash is the first 60
   bits of md5(row text). Rows fall into buckets by a hash of their key text (the primary key's tokens; for a table
   without a primary key, the whole row text, so identical rows share a bucket). Both servers compute
   `count(*)` and `sum(row hash)` per bucket in one scan; sums are exact (numeric / HUGEINT), so the result is
   independent of row order and counts duplicates: tables are compared as multisets. The source scan is split into
   `ctid` ranges and runs on several snapshot connections at once; the destination scan runs at the same time.
4. **Row pass**, only for buckets whose count or sum differ (at most `max_diff_buckets` per table, `bucket_batch` at
   a time): the destination's row hashes in those buckets are counted (integers only); the source rows of those
   buckets are streamed and matched against the counter, so only unmatched rows are kept (missing rows); the hashes
   left over are extra rows, and only their values are fetched from the destination. For tables with a key, a
   missing and an extra row with the same key are reported as one row whose values differ.

Memory: the client holds one (count, sum) per bucket, the destination row hashes of at most `bucket_batch` buckets,
and the values of differing rows only (at most `row_fetch_limit` per side and batch; counts stay exact beyond it).
"""
from __future__ import annotations

import concurrent.futures as cf
import math
import re
import threading
import time
from collections import Counter
from dataclasses import asdict, dataclass, field
from typing import Any, Callable, Optional

from cdc_v2 import lsn
from cdc_v2.deadline import Deadline, DeadlineExceeded

from . import canonical as C
from . import mdexec
from .catalog import (MD_IGNORED_SCHEMAS, PG_SYSTEM_SCHEMAS, DestTable, SourceTable, find_column, matches,
                      read_dest, read_source, server_version_num)
from .position import AppliedPosition, stand_in_applied_position

DB_NAME = re.compile(r"^cdc_v2_[a-z0-9_]+$")
B, H = "_cdc_v2_oracle_b", "_cdc_v2_oracle_h"


class OracleError(Exception):
    pass


class PositionNotReached(OracleError):
    """The source snapshot or the destination is not at or past the position the comparison was asked for."""


@dataclass
class OracleConfig:
    chunk_rows: int = 100_000          # target rows per bucket
    max_buckets: int = 65_536
    sample_limit: int = 10             # rows per category in a table's sample
    max_diff_buckets: int = 16         # differing buckets examined row by row, per table
    bucket_batch: int = 4              # buckets fetched together in the row pass
    row_fetch_limit: int = 10_000      # differing rows per side and batch whose values are kept (counts stay exact)
    source_workers: int = 4            # snapshot connections that scan ctid ranges in parallel
    min_blocks_per_range: int = 256    # tables smaller than this are scanned by one connection
    tables: Optional[list] = None      # ['schema.table', ...]; default: every user table of the source
    exclude_source_schemas: tuple = ()
    ignore_dest_schemas: tuple = MD_IGNORED_SCHEMAS
    check_extra_tables: Optional[bool] = None    # default: True when `tables` is None
    table_map: Optional[Callable[[str, str], tuple]] = None   # source (schema, table) -> destination (schema, table)
    statement_timeout_ms: int = 3_600_000          # Postgres statement_timeout (never more than the deadline)
    timeout_s: float = 3_600.0                       # the whole comparison, when no deadline is passed
    md_statement_s: float = 1_800.0                  # one MotherDuck statement
    position_wait_s: float = 60.0                    # waiting for a standby to replay up to the position
    conflict_retries: int = 3                        # new attempts after SQLSTATE 40001 (standby recovery conflict)


# ------------------------------------------------------------------------------------------------ connections


class Source:
    """Postgres connection parameters (psycopg keyword arguments). Every connection names its port explicitly."""

    def __init__(self, **params):
        if "port" not in params and "conninfo" not in params:
            raise OracleError("the source port must be named explicitly")
        if str(params.get("port")) == "5432":
            raise OracleError("port 5432 is not this build's Postgres")
        self.params = params

    def connect(self):
        import psycopg
        params = dict(self.params)
        conninfo = params.pop("conninfo", "")
        params.setdefault("connect_timeout", 10)
        return psycopg.connect(conninfo, autocommit=True, **params)


class Destination:
    """A MotherDuck database (named cdc_v2_*), or an existing DuckDB connection.

    With `conn` given and `in_transaction=True`, the oracle reads inside the caller's open transaction and neither
    begins nor ends it (tests use this to compare a mutation and roll it back).
    """

    def __init__(self, database: Optional[str] = None, conn=None, in_transaction: bool = False):
        if conn is None:
            if not database or not DB_NAME.match(database):
                raise OracleError("destination database must be named cdc_v2_*")
        self.database = database
        self.conn = conn
        self.in_transaction = in_transaction
        self._own = False

    def open(self, deadline: Deadline):
        if self.conn is None:
            self.conn = mdexec.connect("md:%s?attach_mode=single" % self.database, deadline=deadline)
            self._own = True
        mdexec.execute(self.conn, "SET TimeZone = 'UTC'", deadline=deadline, fetch=None)
        return self.conn

    def close(self):
        if self._own and self.conn is not None:
            self.conn.close()
            self.conn = None
            self._own = False


# ------------------------------------------------------------------------------------------------ result


@dataclass
class TableResult:
    source: str
    destination: Optional[str]
    equal: bool = False
    source_rows: Optional[int] = None
    destination_rows: Optional[int] = None
    missing_on_destination: bool = False
    missing_columns: list = field(default_factory=list)
    extra_columns: list = field(default_factory=list)
    compared_columns: list = field(default_factory=list)
    key: list = field(default_factory=list)
    fallback_columns: list = field(default_factory=list)
    buckets: int = 0
    differing_buckets: int = 0
    examined_buckets: int = 0
    missing_rows: int = 0
    extra_rows: int = 0
    different_rows: int = 0
    truncated: bool = False
    sample: dict = field(default_factory=lambda: {"missing": [], "extra": [], "different": []})
    seconds: float = 0.0


@dataclass
class Result:
    equal: bool
    tables: list
    missing_tables: list
    extra_tables: list
    positioned: bool                   # False only for a comparison asked for with unpositioned=True
    position: Optional[str]            # Q, the position the comparison was asked for
    source_position: Optional[str]     # replay (standby) or insert (primary) position read before the snapshot
    destination_position: Optional[str]  # applied position read inside the destination's read transaction
    slot_name: Optional[str]
    attempts: int
    source_in_recovery: Optional[bool]
    source_server_version: Optional[int]
    rows_compared: int
    seconds: float
    rows_per_second: float
    config: dict

    def to_dict(self) -> dict:
        return asdict(self)

    def summary(self) -> str:
        bad = [t for t in self.tables if not t.equal]
        lines = ["oracle: %s (%d tables, %d rows, %.1f s, %.0f rows/s; %s)" % (
            "EQUAL" if self.equal else "DIFFERENT", len(self.tables), self.rows_compared, self.seconds,
            self.rows_per_second,
            "position %s, source at %s, destination at %s" % (self.position, self.source_position,
                                                              self.destination_position)
            if self.positioned else "UNPOSITIONED, not a gate result")]
        for name in self.missing_tables:
            lines.append("  missing table on destination: %s" % name)
        for name in self.extra_tables:
            lines.append("  extra table on destination: %s" % name)
        for t in bad:
            lines.append("  %s: source %s rows, destination %s rows, missing %d, extra %d, different %d%s%s%s" % (
                t.source, t.source_rows, t.destination_rows, t.missing_rows, t.extra_rows, t.different_rows,
                ", missing columns %s" % t.missing_columns if t.missing_columns else "",
                ", extra columns %s" % t.extra_columns if t.extra_columns else "",
                " (row pass truncated)" if t.truncated else ""))
        return "\n".join(lines)


# ------------------------------------------------------------------------------------------------ SQL


@dataclass
class Plan:
    """What is compared for one table pair, and the SQL for both sides."""
    src: SourceTable
    dst: DestTable
    columns: list          # [(name, PgColumnType, MdType)]
    key: list              # key column names (all compared) or []
    buckets: int


def _qual(schema: str, name: str) -> str:
    return "%s.%s" % (C.quote_ident(schema), C.quote_ident(name))


def _pg_exprs(plan: Plan, ctx: C.Ctx):
    canon = {n: C.pg_canon(C.quote_ident(n), t, ctx) for n, t, _ in plan.columns}
    row = " || ".join(C.token_pg(canon[n]) for n, _, _ in plan.columns) or "''"
    key = " || ".join(C.token_pg(canon[n]) for n in plan.key) if plan.key else None
    return canon, row, key


def _md_exprs(plan: Plan):
    canon = {n: C.md_canon(C.quote_ident(n), t, mt) for n, t, mt in plan.columns}
    row = " || ".join(C.token_md(canon[n]) for n, _, _ in plan.columns) or "''"
    key = " || ".join(C.token_md(canon[n]) for n in plan.key) if plan.key else None
    return canon, row, key


def _inner(hash_fn, row: str, key: Optional[str], buckets: int, table_sql: str, where: str, keep_columns: bool):
    h = hash_fn(row)
    b = ("(%s %% %d)" % (hash_fn(key), buckets)) if key else ("(%s %% %d)" % (H, buckets))
    if key:
        sel = "%s AS %s, %s AS %s" % (b, B, h, H)
        if keep_columns:
            sel += ", _t.*"
        return "SELECT %s FROM %s AS _t%s OFFSET 0" % (sel, table_sql, where)
    # keyless: the bucket comes from the row hash; compute the hash once in an inner query
    inner = "SELECT %s AS %s%s FROM %s AS _t%s OFFSET 0" % (h, H, ", _t.*" if keep_columns else "", table_sql, where)
    return "SELECT %s AS %s, _s.* FROM (%s) AS _s OFFSET 0" % (b, B, inner)


def checksum_sql(inner: str) -> str:
    return "SELECT %s, count(*), sum(%s) FROM (%s) AS _q GROUP BY %s" % (B, H, inner, B)


def rows_sql(inner: str, canon: dict, columns: list, buckets: list, hashes: Optional[list] = None) -> str:
    """Row hash and canonical columns of the rows in `buckets` (and, if given, with one of `hashes`)."""
    cols = ", ".join(canon[n] for n, _, _ in columns) or "NULL"
    where = "%s IN (%s)" % (B, ", ".join(str(int(b)) for b in buckets))
    if hashes is not None:
        where += " AND %s IN (%s)" % (H, ", ".join(str(int(h)) for h in hashes) or "NULL")
    return "SELECT %s, %s FROM (%s) AS _q WHERE %s" % (H, cols, inner, where)


def hashes_sql(inner: str, buckets: list) -> str:
    return "SELECT %s FROM (%s) AS _q WHERE %s IN (%s)" % (H, inner, B, ", ".join(str(int(b)) for b in buckets))


def ctid_ranges(blocks: int, workers: int, min_blocks: int) -> list:
    """WHERE clauses that split a heap of `blocks` pages into ranges; the last range is open-ended."""
    if blocks < min_blocks * 2 or workers <= 1:
        return [""]
    n = min(workers * 2, max(1, blocks // min_blocks))
    step = math.ceil(blocks / n)
    out = []
    for i in range(n):
        lo, hi = i * step, (i + 1) * step
        if i == n - 1:
            out.append(" WHERE ctid >= '(%d,0)'::tid" % lo)
        else:
            out.append(" WHERE ctid >= '(%d,0)'::tid AND ctid < '(%d,0)'::tid" % (lo, hi))
    return out


# ------------------------------------------------------------------------------------------------ comparison


class _SourceSnapshot:
    """A coordinator transaction that exports its snapshot, and a pool of worker connections that import it."""

    def __init__(self, source: Source, workers: int, timeout_ms: int, position: Optional[int],
                 wait: Deadline):
        self.source = source
        self.timeout_ms = timeout_ms
        self.workers = []
        self._free: list = []
        self._lock = threading.Lock()
        self.coord = source.connect()
        try:
            self._open(source, workers, position, wait)
        except BaseException:
            self.close()
            raise

    def _open(self, source, workers, position, wait):
        self._setup(self.coord)
        self.in_recovery = bool(self.coord.execute("SELECT pg_is_in_recovery()").fetchone()[0])
        # the position is read before the snapshot exists: the snapshot then contains at least that much
        while True:
            pos = lsn.parse(self.coord.execute(
                "SELECT (CASE WHEN pg_is_in_recovery() THEN pg_last_wal_replay_lsn() "
                "ELSE pg_current_wal_insert_lsn() END)::text").fetchone()[0])
            if position is None or pos >= position:
                break
            if not self.in_recovery:
                raise PositionNotReached("source primary is at %s, before the position %s" % (
                    lsn.format(pos), lsn.format(position)))
            if wait.expired:
                raise PositionNotReached("source standby replayed only up to %s, not the position %s" % (
                    lsn.format(pos), lsn.format(position)))
            time.sleep(min(0.1, wait.remaining()))
        self.position = pos
        self.coord.execute("BEGIN ISOLATION LEVEL REPEATABLE READ, READ ONLY")
        self.snapshot = self.coord.execute("SELECT pg_export_snapshot()").fetchone()[0]
        self.version = server_version_num(self.coord)
        for _ in range(max(1, workers)):
            c = source.connect()
            self._setup(c)
            c.execute("BEGIN ISOLATION LEVEL REPEATABLE READ, READ ONLY")
            c.execute("SET TRANSACTION SNAPSHOT '%s'" % self.snapshot.replace("'", ""))
            self.workers.append(c)
            self._free.append(c)

    def _setup(self, c):
        c.execute("SET extra_float_digits = 1")
        c.execute("SET TimeZone = 'UTC'")
        c.execute("SET DateStyle = 'ISO, YMD'")
        c.execute("SET IntervalStyle = 'postgres'")
        c.execute("SET bytea_output = 'hex'")
        c.execute("SET statement_timeout = %d" % int(self.timeout_ms))
        c.execute("SET application_name = 'cdc_v2_oracle'")

    def run(self, sql: str) -> list:
        with self._lock:
            c = self._free.pop()
        try:
            return c.execute(sql).fetchall()
        finally:
            with self._lock:
                self._free.append(c)

    def stream(self, sql: str, consume: Callable[[list], None], batch: int = 5000) -> None:
        """Run sql on a snapshot connection and hand the rows to consume() in batches, without holding the whole
        result. Uses libpq's single-row mode (`Cursor.stream`), not a server-side cursor: Postgres never runs a
        DECLARE CURSOR query in parallel, which made the row pass about twice as slow."""
        with self._lock:
            c = self._free.pop()
        try:
            buf: list = []
            for row in c.cursor().stream(sql):
                buf.append(row)
                if len(buf) >= batch:
                    consume(buf)
                    buf = []
            if buf:
                consume(buf)
        finally:
            with self._lock:
                self._free.append(c)

    def close(self):
        for c in self.workers + [self.coord]:
            try:
                c.execute("ROLLBACK")
            except Exception:                  # noqa: BLE001 - closing anyway
                pass
            c.close()


def _plan(src: SourceTable, dst: DestTable, cfg: OracleConfig, result: TableResult) -> Plan:
    cols = []
    for c in src.columns:
        d = find_column(dst.columns, c.name)
        if d is None:
            result.missing_columns.append(c.name)
            continue
        cols.append((c.name, c.type, d.type))
        if c.type.rule is C.FALLBACK:
            result.fallback_columns.append("%s (%s)" % (c.name, c.full_type))
    src_names = {c.name for c in src.columns}
    result.extra_columns = [d.name for d in dst.columns if d.name not in src_names]
    compared = {n for n, _, _ in cols}
    key = list(src.key) if src.key and all(k in compared for k in src.key) else []
    est = src.est_rows if src.est_rows and src.est_rows > 0 else src.blocks * 50
    buckets = int(min(cfg.max_buckets, max(1, math.ceil(est / max(1, cfg.chunk_rows)))))
    result.compared_columns = [n for n, _, _ in cols]
    result.key = key
    result.buckets = buckets
    return Plan(src, dst, cols, key, buckets)


def _merge(parts) -> dict:
    out: dict = {}
    for rows in parts:
        for b, n, s in rows:
            n0, s0 = out.get(b, (0, 0))
            out[int(b)] = (n0 + int(n), s0 + int(s or 0))
    return out


def _show(t: C.PgColumnType, canon):
    show = t.rule.show if not t.is_array else None
    return show(canon) if show and canon is not None else canon


def _display(columns, key, values) -> dict:
    return {n: _show(t, values[i]) for i, (n, t, _) in enumerate(columns)}


def _row_diff(plan: Plan, src_rows: list, dst_rows: list, res: TableResult, limit: int):
    """Multiset difference of rows (hash, canon...) from both sides; fills counts and the bounded sample."""
    def index(rows):
        cnt, vals = Counter(), {}
        for r in rows:
            h = int(r[0])
            cnt[h] += 1
            vals.setdefault(h, tuple(r[1:]))
        return cnt, vals

    sc, sv = index(src_rows)
    dc, dv = index(dst_rows)
    _diff_counts(plan, sc - dc, sv, dc - sc, dv, res, limit)


def _diff_counts(plan: Plan, missing: Counter, sv: dict, extra: Counter, dv: dict, res: TableResult, limit: int):
    """Fill counts and the sample from the multiset difference: missing / extra row hashes with their counts, and
    the canonical values of (at least some of) those rows. Rows whose values were not fetched are counted but
    neither paired by key nor sampled; the result is then marked truncated."""
    missing, extra = Counter(missing), Counter(extra)
    if any(h not in sv for h in missing) or any(h not in dv for h in extra):
        res.truncated = True
    names = [n for n, _, _ in plan.columns]
    kpos = [names.index(k) for k in plan.key]

    if kpos:
        def key_of(vals):
            return tuple(vals[i] for i in kpos)
        extra_by_key: dict = {}
        for h, n in extra.items():
            if h in dv:
                extra_by_key.setdefault(key_of(dv[h]), []).append(h)
        for h in list(missing):
            if h not in sv:
                continue
            k = key_of(sv[h])
            cands = extra_by_key.get(k)
            while cands and missing[h] > 0:
                h2 = cands[0]
                res.different_rows += 1
                if len(res.sample["different"]) < limit:
                    a, b = sv[h], dv[h2]
                    res.sample["different"].append({
                        "key": {plan.key[j]: k[j] for j in range(len(k))},
                        "columns": {names[i]: {"source": _show(plan.columns[i][1], a[i]),
                                               "destination": _show(plan.columns[i][1], b[i])}
                                    for i in range(len(names)) if a[i] != b[i]}})
                missing[h] -= 1
                extra[h2] -= 1
                if extra[h2] <= 0:
                    cands.pop(0)
            if missing[h] <= 0:
                del missing[h]
        extra = +extra
    for h, n in missing.items():
        res.missing_rows += n
        for _ in range(n):
            if len(res.sample["missing"]) < limit and h in sv:
                res.sample["missing"].append(_display(plan.columns, plan.key, sv[h]))
    for h, n in extra.items():
        res.extra_rows += n
        for _ in range(n):
            if len(res.sample["extra"]) < limit and h in dv:
                res.sample["extra"].append(_display(plan.columns, plan.key, dv[h]))


def _row_pass(plan: Plan, snap: "_SourceSnapshot", md, cfg: OracleConfig, res: TableResult, pool, batch: list,
              ranges: list, pg: tuple, mdq: tuple) -> None:
    """Row-level comparison of some buckets with bounded memory.

    1. The destination's row hashes in the buckets (integers only) go into a counter.
    2. The source rows of the buckets are streamed (hash and canonical columns); a row whose hash the counter still
       holds is matched and dropped, any other row is kept as missing (values for at most `row_fetch_limit` hashes).
    3. Hashes left in the counter are extra rows; only their values are fetched from the destination.
    """
    pcanon, prow, pkey, src_sql = pg
    mcanon, mrow, mkey, dst_sql = mdq
    dst = Counter()

    def count(rows):
        for (h,) in rows:
            dst[int(h)] += 1

    md(hashes_sql(_inner(C.hash60_md, mrow, mkey, plan.buckets, dst_sql, "", False), batch), "row hashes",
       fetch="stream", consume=count)
    lock = threading.Lock()
    missing, svals = Counter(), {}

    def consume(rows):
        with lock:
            for r in rows:
                h = int(r[0])
                if dst[h] > 0:
                    dst[h] -= 1
                    continue
                missing[h] += 1
                if h not in svals and len(svals) < cfg.row_fetch_limit:
                    svals[h] = tuple(r[1:])

    jobs = [pool.submit(snap.stream, rows_sql(_inner(C.hash60_pg, prow, pkey, plan.buckets, src_sql, w, True),
                                              pcanon, plan.columns, batch), consume) for w in ranges]
    for f in jobs:
        f.result()
    extra = +dst
    dvals = {}
    if extra:
        want = sorted(extra)[:cfg.row_fetch_limit]
        for r in md(rows_sql(_inner(C.hash60_md, mrow, mkey, plan.buckets, dst_sql, "", True), mcanon,
                             plan.columns, batch, want), "extra rows"):
            dvals.setdefault(int(r[0]), tuple(r[1:]))
    _diff_counts(plan, missing, svals, extra, dvals, res, cfg.sample_limit)


def _compare_table(plan: Plan, snap: _SourceSnapshot, md, cfg: OracleConfig, res: TableResult, pool):
    ctx = C.Ctx(snap.version)
    pcanon, prow, pkey = _pg_exprs(plan, ctx)
    mcanon, mrow, mkey = _md_exprs(plan)
    src_sql = _qual(plan.src.schema, plan.src.name)
    dst_sql = _qual(plan.dst.schema, plan.dst.name)
    ranges = [""] if plan.src.kind != "r" else ctid_ranges(plan.src.blocks, cfg.source_workers,
                                                            cfg.min_blocks_per_range)
    src_jobs = [pool.submit(snap.run, checksum_sql(_inner(C.hash60_pg, prow, pkey, plan.buckets, src_sql, w, False)))
                for w in ranges]
    dst_rows = md(checksum_sql(_inner(C.hash60_md, mrow, mkey, plan.buckets, dst_sql, "", False)), "checksums")
    sb = _merge(f.result() for f in src_jobs)
    db = _merge([dst_rows])
    res.source_rows = sum(n for n, _ in sb.values())
    res.destination_rows = sum(n for n, _ in db.values())
    differing = sorted(b for b in set(sb) | set(db) if sb.get(b, (0, 0)) != db.get(b, (0, 0)))
    res.differing_buckets = len(differing)
    examine = differing[:cfg.max_diff_buckets]
    res.truncated = len(differing) > len(examine)
    for i in range(0, len(examine), max(1, cfg.bucket_batch)):
        batch = examine[i:i + cfg.bucket_batch]
        _row_pass(plan, snap, md, cfg, res, pool, batch, ranges, (pcanon, prow, pkey, src_sql),
                  (mcanon, mrow, mkey, dst_sql))
        res.examined_buckets += len(batch)
    res.equal = (not differing and not res.missing_columns and not res.extra_columns)


def _is_conflict(e: BaseException) -> bool:
    """SQLSTATE 40001: on a standby, a replayed lock or vacuum cancelled our snapshot (recovery conflict)."""
    return getattr(e, "sqlstate", None) == "40001"


def compare(source: Source, destination: Destination, config: Optional[OracleConfig] = None, *,
            position: Optional[int] = None, slot_name: Optional[str] = None,
            applied_position: AppliedPosition = stand_in_applied_position, unpositioned: bool = False,
            deadline: Optional[Deadline] = None) -> Result:
    """Compare source and destination at `position` (see the module docstring).

    Returns a Result for equal and for different data. Raises `PositionNotReached` when a side is short of the
    position, `OracleError` for a comparison that compares nothing, and `DeadlineExceeded` when the deadline passes.
    `position` and `slot_name` (whose applied position the destination must have reached) are required unless
    `unpositioned=True` is given by name; an unpositioned result is never a gate result (contract C8).
    """
    cfg = config or OracleConfig()
    if isinstance(position, str):
        position = lsn.parse(position)
    if unpositioned:
        if position is not None:
            raise OracleError("give a position or unpositioned=True, not both")
    elif position is None or not slot_name:
        raise OracleError("compare() needs position=Q and slot_name (or unpositioned=True, asked for by name)")
    if cfg.tables is not None and not cfg.tables:
        raise OracleError("an empty table list compares nothing")
    deadline = deadline or Deadline.after(cfg.timeout_s)
    attempt = 0
    while True:
        attempt += 1
        try:
            return _compare_once(source, destination, cfg, position, slot_name, applied_position, deadline, attempt)
        except Exception as e:                       # noqa: BLE001 - only recovery conflicts are retried
            if not _is_conflict(e) or attempt > cfg.conflict_retries:
                raise
            deadline.check("comparison retried after a recovery conflict")


def _compare_once(source, destination, cfg, position, slot_name, applied_position, deadline, attempt) -> Result:
    t0 = time.monotonic()
    timeout_ms = int(min(cfg.statement_timeout_ms, max(1.0, deadline.remaining()) * 1000))
    snap = _SourceSnapshot(source, cfg.source_workers, timeout_ms, position, deadline.within(cfg.position_wait_s))
    md = None
    began = False

    def run_md(sql, what="", fetch="all", **kw):
        return mdexec.execute(md, sql, deadline=deadline.within(cfg.md_statement_s), what=what or sql[:60],
                              fetch=fetch, **kw)

    try:
        md = destination.open(deadline.within(cfg.md_statement_s))
        if not destination.in_transaction:
            run_md("BEGIN TRANSACTION", fetch=None)
            began = True
        dest_pos = None
        if position is not None:
            with mdexec.guarded(md, deadline.within(cfg.md_statement_s), "applied position of %s" % slot_name):
                dest_pos = applied_position(md, slot_name)
            if dest_pos is None or dest_pos < position:
                raise PositionNotReached("destination applied %s for slot %s, before the position %s" % (
                    lsn.format(dest_pos) if dest_pos is not None else "nothing", slot_name, lsn.format(position)))
        src_tables = read_source(snap.coord, tuple(cfg.exclude_source_schemas) + PG_SYSTEM_SCHEMAS)
        dst_tables = read_dest(lambda sql: run_md(sql, "destination catalog"), cfg.ignore_dest_schemas)
        if cfg.tables is not None:
            wanted = set(cfg.tables)
            unknown = wanted - {"%s.%s" % k for k in src_tables}
            if unknown:
                raise OracleError("tables not found on the source: %s" % ", ".join(sorted(unknown)))
            src_tables = {k: v for k, v in src_tables.items() if "%s.%s" % k in wanted}
        check_extra = cfg.check_extra_tables if cfg.check_extra_tables is not None else cfg.tables is None
        if not src_tables:
            raise OracleError("the source has no table to compare")
        tmap = cfg.table_map or (lambda s, t: (s, t))
        results, mapped, missing_tables = [], set(), []
        with cf.ThreadPoolExecutor(max_workers=max(1, cfg.source_workers)) as pool:
            for (schema, name), st in sorted(src_tables.items()):
                ds, dn = tmap(schema, name)
                res = TableResult(st.qualified, "%s.%s" % (ds, dn))
                dt = dst_tables.get((ds, dn))
                tt = time.monotonic()
                if dt is None:
                    res.missing_on_destination = True
                    res.destination = None
                    missing_tables.append(st.qualified)
                    res.source_rows = int(snap.run("SELECT count(*) FROM %s" % _qual(schema, name))[0][0])
                else:
                    mapped.add((ds, dn))
                    _compare_table(_plan(st, dt, cfg, res), snap, run_md, cfg, res, pool)
                res.seconds = round(time.monotonic() - tt, 3)
                results.append(res)
        extra_tables = sorted("%s.%s" % k for k in dst_tables if k not in mapped) if check_extra else []
    finally:
        if began:
            try:
                run_md("ROLLBACK", fetch=None)
            except Exception:                   # noqa: BLE001
                pass
        destination.close()
        snap.close()
    seconds = time.monotonic() - t0
    rows = sum(r.source_rows or 0 for r in results)          # rate = source rows compared per second
    equal = all(r.equal for r in results) and not extra_tables and not missing_tables
    cfg_out = {k: v for k, v in asdict(cfg).items() if k != "table_map"}
    cfg_out["table_map"] = "custom" if cfg.table_map else "identity"
    fmt = (lambda v: lsn.format(v) if v is not None else None)
    return Result(equal, results, missing_tables, extra_tables, position is not None, fmt(position),
                  fmt(snap.position), fmt(dest_pos), slot_name, attempt, snap.in_recovery, snap.version, rows,
                  round(seconds, 3), round(rows / seconds, 1) if seconds > 0 else 0.0, cfg_out)
