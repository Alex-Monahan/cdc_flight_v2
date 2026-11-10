"""The toy replicator: a real, small, crash-only replicator that the fuzzers attack before the pipeline exists.

    python toy.py --config CONFIG.json --tag TAG [--run-id ID]

It reads a logical slot (output plugin `test_decoding`) of a local cluster through the SQL functions
(`pg_logical_slot_peek_changes`, then `pg_replication_slot_advance` after the destination commit), turns each batch
of whole source transactions into engine frames (contract C2 layout, `cdc_v2.testing.frames`), reads them back with
the engine's decoder (`cdc_v2.stream.frames`), and applies every source transaction whole to a DuckDB file. The
applied position (`_cdc.stream_state.applied_lsn`, the commit end LSN of the last applied transaction) is written in
the same destination transaction as the data; the slot is confirmed only up to what is durable.

It has a leader epoch and a fence of its own (`_cdc.toy_leader`, `_cdc.toy_fence`), so several instances can
compete: an instance takes over when no healthy heartbeat exists, and every data transaction checks its epoch on the
fence row. Every data transaction also appends (epoch, run, commit position, transaction id) to `_cdc.toy_log`, the
fact the leadership fuzzer uses for "never two writers".

When its slot is missing, invalid, or a resynchronisation is pending, it marks the destination, creates the slot
again with a snapshot (`CREATE_REPLICATION_SLOT ... (SNAPSHOT 'use')` on a replication connection) and loads every
table from that snapshot in one destination transaction, at the slot's consistent point. The first start of a fresh
destination takes this path too.

Variants (one switch each, a real class of bug; docs/testing/fuzzers.md has the table): correct, ack_early,
position_apart, split, reorder, no_fence, skip_on_reconnect, swallow, trust_frames.

It is test code: simple and obviously right beats fast. Exit codes: 0 healthy leader found (guard mode `exit`) or
planned release; 10 superseded (fence lost); 1 crash; 3 could not take over in time.
"""
from __future__ import annotations

import argparse
import json
import os
import random
import re
import socket
import sys
import threading
import time
import zlib

import duckdb
import psycopg
import pyarrow as pa

from cdc_v2 import lsn
from cdc_v2.crashpoints import crash_point, declare
from cdc_v2.faultsites import FaultInjected, declare_site, fault_site
from cdc_v2.stream import frames as F
from cdc_v2.testing import frames as FE
from cdc_v2.testing import pipefaults

VARIANTS = ("correct", "ack_early", "position_apart", "split", "reorder", "no_fence", "skip_on_reconnect",
            "swallow", "trust_frames", "keyless_toast_null", "deferrable_by_key")

# name -> (description, once, variants that reach it (None = all))
POINTS = {
    "toyrep.schema_ready": ("start-up transaction that creates the destination tables committed", True, None),
    "toyrep.taken_over": ("take-over transaction committed", True, None),
    "toyrep.resync_marked": ("resynchronisation marked pending in the destination", True, None),
    "toyrep.slot_created": ("slot created with its snapshot, destination not loaded yet", True, None),
    "toyrep.resync_committed": ("tables loaded from the snapshot and position set, committed", True, None),
    "toyrep.heartbeat_written": ("heartbeat transaction committed", False, None),
    "toyrep.begun": ("BEGIN of a group transaction", False, None),
    "toyrep.applied": ("every change of the group applied, not committed", False, None),
    "toyrep.data_committed": ("position_apart: data committed, position not yet written", False, ("position_apart",)),
    "toyrep.position_written": ("applied position updated, not committed", False, None),
    "toyrep.fenced": ("fence statement executed, not committed", False, None),
    "toyrep.acked_early": ("ack_early: slot confirmed before the destination commit", False, ("ack_early",)),
    "toyrep.commit_sent": ("immediately before COMMIT of the group (run with kill_later: COMMIT in flight)", False, None),
    "toyrep.committed": ("COMMIT of the group returned", False, None),
    "toyrep.slot_advanced": ("slot confirmed up to the durable position", False, None),
    "toyrep.released": ("planned release committed", True, None),
}
for _n, (_d, _once, _v) in POINTS.items():
    declare(_n, _d, once=_once)

SITES = {
    "md.connect": "open a destination connection",
    "md.execute": "one destination statement",
    "md.fence": "the fence statement of a data transaction",
    "md.commit": "COMMIT of a destination transaction",
    "toyrep.slot_peek": "read changes from the slot",
    "toyrep.slot_advance": "confirm the slot up to a position",
    "toyrep.source_connect": "open the source connection",
}
for _n, _d in SITES.items():
    declare_site(_n, _d)


def points_for(variant: str, include_release: bool = False) -> list:
    out = [n for n, (_d, _o, v) in POINTS.items() if v is None or variant in v]
    return [p for p in out if include_release or p != "toyrep.released"]


class FenceLost(Exception):
    pass


class ToyConflict(Exception):
    """A simulated write-write conflict of the destination (MotherDuck reports these in normal operation; C4)."""


class ApplyInvariantError(Exception):
    pass


class DestBusy(Exception):
    pass


class StreamBroken(Exception):
    pass


UNCHANGED = object()
TYPE_MAP = {"bigint": "BIGINT", "integer": "INTEGER", "smallint": "SMALLINT", "text": "VARCHAR",
            "boolean": "BOOLEAN", "timestamp with time zone": "TIMESTAMPTZ", "date": "DATE"}


def dd_type(pg_type: str) -> str:
    if pg_type in TYPE_MAP:
        return TYPE_MAP[pg_type]
    m = re.match(r"^numeric\((\d+),(\d+)\)$", pg_type)
    if m and int(m.group(1)) <= 38:
        return "DECIMAL(%s,%s)" % m.groups()
    if pg_type.startswith("character varying"):
        return "VARCHAR"
    raise ValueError("the toy does not support the type %r" % pg_type)


def qi(name: str) -> str:
    return '"%s"' % name.replace('"', '""')


def qt(table: str) -> str:
    s, t = table.split(".", 1)
    return "%s.%s" % (qi(s), qi(t))


# ------------------------------------------------------------------------------------------------- test_decoding

_HEAD = re.compile(r"^table (.+?): (INSERT|UPDATE|DELETE|TRUNCATE):(.*)$", re.S)


def _unquote_ident(s: str) -> str:
    return s[1:-1].replace('""', '"') if s.startswith('"') else s


def parse_name(text: str) -> str:
    """`public.t` or `"S"."t"` -> `schema.table` (unquoted)."""
    parts, i, cur = [], 0, ""
    while i < len(text):
        ch = text[i]
        if ch == '"':
            j = i + 1
            while True:
                j = text.index('"', j)
                if j + 1 < len(text) and text[j + 1] == '"':
                    j += 2
                    continue
                break
            cur += text[i:j + 1]
            i = j + 1
            continue
        if ch == ".":
            parts.append(_unquote_ident(cur))
            cur = ""
        else:
            cur += ch
        i += 1
    parts.append(_unquote_ident(cur))
    return ".".join(parts)


def parse_tuple(text: str) -> dict:
    """`a[integer]:1 b[text]:'x''y' c[text]:null d[text]:unchanged-toast-datum` -> {name: value}."""
    out, i, n = {}, 0, len(text)
    while i < n:
        while i < n and text[i] == " ":
            i += 1
        if i >= n:
            break
        if text[i] == '"':
            j = i + 1
            while True:
                j = text.index('"', j)
                if j + 1 < n and text[j + 1] == '"':
                    j += 2
                    continue
                break
            name = text[i + 1:j].replace('""', '"')
            i = j + 1
        else:
            j = text.index("[", i)
            name = text[i:j]
            i = j
        j = text.index("]:", i)
        i = j + 2
        if i < n and text[i] == "'":
            j, buf = i + 1, []
            while True:
                k = text.index("'", j)
                buf.append(text[j:k])
                if k + 1 < n and text[k + 1] == "'":
                    buf.append("'")
                    j = k + 2
                    continue
                i = k + 1
                break
            out[name] = "".join(buf)
        else:
            j = text.find(" ", i)
            j = n if j < 0 else j
            raw = text[i:j]
            i = j
            out[name] = None if raw == "null" else UNCHANGED if raw == "unchanged-toast-datum" else raw
    return out


def parse_change(data: str) -> dict:
    m = _HEAD.match(data)
    if not m:
        raise ValueError("unparsable change")
    names, kind, rest = m.group(1), m.group(2), m.group(3).strip()
    if kind == "TRUNCATE":
        return {"kind": "T", "tables": [parse_name(t.strip()) for t in names.split(", ")]}
    table = parse_name(names)
    old = new = None
    if rest.startswith("old-key:"):
        a, _, b = rest[len("old-key:"):].partition(" new-tuple:")
        old, new = parse_tuple(a.strip()), parse_tuple(b.strip())
    elif kind == "DELETE":
        old = parse_tuple(rest) if rest != "(no-tuple-data)" else {}
    else:
        new = parse_tuple(rest)
    return {"kind": kind[0], "table": table, "old": old, "new": new}


def parse_batch(rows) -> list:
    """Peeked rows (lsn text, xid text, data) -> whole transactions [{xid, commit_lsn, changes}] in commit order."""
    txns, cur = [], None
    for lsn_text, xid, data in rows:
        if data.startswith("BEGIN"):
            cur = {"xid": int(xid), "changes": []}
        elif data.startswith("COMMIT"):
            if cur is not None:
                cur["commit_lsn"] = lsn.parse(lsn_text)
                txns.append(cur)
            cur = None
        elif cur is not None:
            cur["changes"].append(parse_change(data))
    return txns


# ------------------------------------------------------------------------------------------------- frames

def _strs(values):
    return pa.array(values, type=pa.string())


def encode_batch(txns: list, meta: dict) -> bytes:
    """Whole transactions -> engine frames (C2 layout): SCHEMA per table, then BEGIN, ROWS / TRUNCATE, END."""
    enc, seq, out = FE.Encoder(), 0, []
    for t, m in sorted(meta.items()):
        s, n = t.split(".", 1)
        cols = tuple(F.Column(i, c, "STRING", "", "text", F.P_UTF8, typ, -1, -1, True, i in m["key"], True)
                     for i, (c, typ) in enumerate(m["columns"]))
        out.append(enc.encode(F.Schema(s, n, 1, bool(m["key"]), tuple(m["key"]), cols)))
    for txn in txns:
        pos = {"lsn": txn["commit_lsn"], "txId": txn["xid"]}
        out.append(enc.encode(F.Begin(seq, str(txn["xid"]), 0, pos, txn["commit_lsn"])))
        seq += 1
        count, per = 0, {}
        for ch in txn["changes"]:
            if ch["kind"] == "T":
                for t in ch["tables"]:
                    if t in meta:
                        s, n = t.split(".", 1)
                        out.append(enc.encode(F.Truncate(seq, s, n, pos)))
                        seq += 1
                        count += 1
                        per[t] = per.get(t, 0) + 1
                continue
            t = ch["table"]
            if t not in meta:
                continue
            m = meta[t]
            ncol = len(m["columns"])
            names = [c for c, _ in m["columns"]]
            new, old = ch["new"] or {}, ch["old"] or {}
            cols = {"seq": pa.array([seq], type=pa.int64()),
                    "op": _strs([{"I": "c", "U": "u", "D": "d"}[ch["kind"]]])}
            src = new if ch["kind"] != "D" else old
            for i in range(ncol):
                v = src.get(names[i])
                cols["a%d" % i] = _strs([None if v is UNCHANGED else v])
            if not m["key"]:
                for i in range(ncol):
                    cols["b%d" % i] = _strs([old.get(names[i]) if ch["kind"] != "I" else None])
            for i in range(ncol):
                cols["u%d" % i] = pa.array([new.get(names[i]) is UNCHANGED], type=pa.bool_())
            for i in m["key"]:
                cols["k%d" % i] = _strs([old.get(names[i]) if (m["key"] and ch["kind"] == "U" and old) else None])
            s, n = t.split(".", 1)
            out.append(enc.encode(F.Rows(s, n, 1, pa.RecordBatch.from_pydict(cols))))
            seq += 1
            count += 1
            per[t] = per.get(t, 0) + 1
        out.append(enc.encode(F.End(seq, str(txn["xid"]), 0, pos, txn["commit_lsn"], count,
                                    tuple(sorted(per.items())))))
        seq += 1
    return b"".join(out)


def _rows_of(frame: F.Rows) -> list:
    d = frame.batch.to_pydict()
    return [{k: d[k][i] for k in d} for i in range(frame.batch.num_rows)]


def assemble(frames: list, strict: bool = True) -> list:
    """Frames -> whole transactions [{xid, commit_lsn, items: [(table, row) | (table, 'T')]}].

    strict (the correct toy): BEGIN/END pairing, END event count, record order and rising positions are checked; a
    transaction without its END is dropped; any violation raises StreamBroken. Not strict (variant trust_frames):
    nothing is checked, and a transaction cut off before its END is handed on with the position its BEGIN named."""
    out, cur, last_seq, last_pos = [], None, -1, -1
    for f in frames:
        if isinstance(f, (F.Schema, F.Heartbeat, F.Hello, F.BatchEnd)):
            continue
        if isinstance(f, F.Begin):
            if strict and (cur is not None or f.ack_position is None or f.seq <= last_seq):
                raise StreamBroken("BEGIN out of place")
            cur = {"xid": int(f.txid), "commit_lsn": f.ack_position, "items": [], "count": 0}
            last_seq = f.seq
            continue
        if isinstance(f, (F.Rows, F.Truncate)):
            if cur is None:
                if strict:
                    raise StreamBroken("change outside a transaction")
                continue
            t = "%s.%s" % (f.schema, f.table)
            if isinstance(f, F.Truncate):
                if strict and f.seq <= last_seq:
                    raise StreamBroken("record order")
                last_seq = f.seq
                cur["items"].append((t, "T"))
                cur["count"] += 1
            else:
                for r in _rows_of(f):
                    if strict and r["seq"] <= last_seq:
                        raise StreamBroken("record order")
                    last_seq = r["seq"]
                    cur["items"].append((t, r))
                    cur["count"] += 1
            continue
        if isinstance(f, F.End):
            if cur is None:
                if strict:
                    raise StreamBroken("END without BEGIN")
                continue
            if strict and (f.event_count != cur["count"] or f.ack_position != cur["commit_lsn"]
                           or f.ack_position <= last_pos or f.seq <= last_seq):
                raise StreamBroken("END does not match its transaction")
            last_pos, last_seq = f.ack_position, f.seq
            out.append(cur)
            cur = None
            continue
        if isinstance(f, F.Fatal) and strict:
            raise StreamBroken("FATAL")
    if cur is not None and not strict:
        out.append(cur)
    return out


def lenient_frames(data: bytes) -> list:
    """trust_frames: frames by their length fields only; no checksum, no sequence check, bad payloads skipped."""
    out, pos = [], 0
    while pos + F.HEADER_SIZE <= len(data):
        magic, ftype, _res, _seq, length = F.HEADER.unpack_from(data, pos)
        if magic != F.MAGIC:
            pos += 1
            continue
        payload = bytes(data[pos + F.HEADER_SIZE:pos + F.HEADER_SIZE + length])
        pos += F.HEADER_SIZE + length + 4
        try:
            out.append(F.parse_payload(ftype, payload, validate=False))
        except Exception:                                  # noqa: BLE001 - trusting means skipping, not stopping
            continue
    return out


# ------------------------------------------------------------------------------------------------- the toy


class Toy:
    def __init__(self, cfg: dict, tag: str, run_id: str):
        self.cfg, self.tag, self.run_id = cfg, tag, run_id
        self.variant = cfg.get("variant", "correct")
        if self.variant not in VARIANTS:
            raise ValueError("unknown variant %r" % self.variant)
        self.slot = cfg["slot"]
        self.tables = list(cfg["tables"])
        self.epoch = None
        self.src = None
        self.src_lost = False
        self.meta = None
        self.last_beat = 0.0
        self.started = time.monotonic()
        self.attempt = 0
        self.batch_no = 0
        self.rng = random.Random("%s/%s/%d" % (cfg.get("seed", 0), run_id, os.getpid()))
        self.events_path = cfg.get("events")
        self.spill_dir = cfg.get("spill_dir")
        self._spill_waiting = False
        self.quarantined = set()
        self.invariant_errors = {}

    # ------------------------------------------------------------------ observability
    def event(self, kind: str, **fields):
        line = dict(fields, kind=kind, tag=self.tag, run=self.run_id, pid=os.getpid(), t=round(time.time(), 6))
        print("toy event %s" % json.dumps(line, sort_keys=True), flush=True)
        if self.events_path:
            fd = os.open(self.events_path, os.O_WRONLY | os.O_APPEND | os.O_CREAT, 0o600)
            try:
                os.write(fd, (json.dumps(line, sort_keys=True) + "\n").encode())
            finally:
                os.close(fd)

    # ------------------------------------------------------------------ destination
    def dconnect(self):
        fault_site("md.connect")
        deadline = time.monotonic() + float(self.cfg.get("dest_lock_s", 30))
        while True:
            try:
                con = duckdb.connect(self.cfg["dest_path"])
                con.execute("SET TimeZone = 'UTC'")
                return con
            except duckdb.IOException as e:
                if "lock" not in str(e).lower() or time.monotonic() >= deadline:
                    raise DestBusy(str(e)[:120]) from None
                time.sleep(0.002 + random.random() * 0.004)

    @staticmethod
    def dx(con, sql, params=None):
        fault_site("md.execute")
        return con.execute(sql, params) if params is not None else con.execute(sql)

    def fence(self, con):
        if self.variant == "no_fence":
            return
        fault_site("md.fence")
        n = self.dx(con, "UPDATE _cdc.toy_fence SET n = n + 1 WHERE k = 1 AND epoch = ?", [self.epoch]).fetchone()[0]
        if n != 1:
            raise FenceLost("fence row holds another epoch")

    @staticmethod
    def commit(con):
        fault_site("md.commit")
        con.execute("COMMIT")

    def ensure_schema(self):
        con = self.dconnect()
        try:
            con.execute("BEGIN")
            for sql in ["CREATE SCHEMA IF NOT EXISTS _cdc",
                        "CREATE TABLE IF NOT EXISTS _cdc.stream_state (slot_name VARCHAR, applied_lsn UBIGINT, "
                        "resync BOOLEAN)",
                        "CREATE TABLE IF NOT EXISTS _cdc.toy_leader (k INTEGER, epoch BIGINT, run_id VARCHAR, "
                        "state VARCHAR, beat_at TIMESTAMP)",
                        "CREATE TABLE IF NOT EXISTS _cdc.toy_fence (k INTEGER, epoch BIGINT, n BIGINT)",
                        "CREATE SEQUENCE IF NOT EXISTS _cdc.toy_log_seq",
                        "CREATE TABLE IF NOT EXISTS _cdc.toy_log (n BIGINT, epoch BIGINT, run_id VARCHAR, "
                        "commit_lsn UBIGINT, xid BIGINT, kind VARCHAR)",
                        "CREATE TABLE IF NOT EXISTS _cdc.toy_quarantine (tbl VARCHAR, quarantined_at TIMESTAMP, epoch BIGINT)"]:
                con.execute(sql)
            if con.execute("SELECT count(*) FROM _cdc.stream_state").fetchone()[0] == 0:
                con.execute("INSERT INTO _cdc.stream_state VALUES (?, 0, true)", [self.slot])
            if con.execute("SELECT count(*) FROM _cdc.toy_leader").fetchone()[0] == 0:
                con.execute("INSERT INTO _cdc.toy_leader VALUES (1, 0, '', 'released', TIMESTAMP '2000-01-01')")
                con.execute("INSERT INTO _cdc.toy_fence VALUES (1, 0, 0)")
            con.execute("COMMIT")
        finally:
            con.close()
        crash_point("toyrep.schema_ready")

    # ------------------------------------------------------------------ leadership
    def guard(self):
        mode = self.cfg.get("guard", "wait")
        stale = float(self.cfg.get("stale_s", 2.0))
        deadline = time.monotonic() + float(self.cfg.get("takeover_timeout_s", 120))
        while True:
            try:
                con = self.dconnect()
            except DestBusy:
                if time.monotonic() > deadline:
                    sys.exit(3)
                continue
            try:
                con.execute("BEGIN")
                epoch, state, age = con.execute(
                    "SELECT epoch, state, epoch_ms(now()::TIMESTAMP) - epoch_ms(beat_at) FROM _cdc.toy_leader "
                    "WHERE k = 1").fetchone()
                if state == "leading" and age < stale * 1000:
                    con.execute("ROLLBACK")
                    if mode == "exit":
                        self.event("healthy_leader_found", epoch=epoch, age_ms=age)
                        sys.exit(0)
                    if time.monotonic() > deadline:
                        sys.exit(3)
                    time.sleep(0.1)
                    continue
                n = con.execute("UPDATE _cdc.toy_leader SET epoch = epoch + 1, run_id = ?, state = 'leading', "
                                "beat_at = now()::TIMESTAMP WHERE k = 1 AND epoch = ?", [self.run_id, epoch]).fetchone()[0]
                if n != 1:
                    con.execute("ROLLBACK")
                    continue
                con.execute("UPDATE _cdc.toy_fence SET epoch = ? WHERE k = 1", [epoch + 1])
                # the take-over is a fact of the writer log too (leadership fuzzer: never two writers)
                con.execute("INSERT INTO _cdc.toy_log SELECT nextval('_cdc.toy_log_seq'), ?, ?, applied_lsn, 0, "
                            "'takeover' FROM _cdc.stream_state WHERE slot_name = ?", [epoch + 1, self.run_id, self.slot])
                con.execute("COMMIT")
                self.epoch = epoch + 1
                self.last_beat = time.monotonic()
                self.event("took_over", epoch=self.epoch, previous_state=state, previous_age_ms=age)
                crash_point("toyrep.taken_over")
                return
            except duckdb.Error:
                time.sleep(0.05)
            finally:
                con.close()

    def heartbeat(self, force=False):
        if not force and time.monotonic() - self.last_beat < float(self.cfg.get("hb_interval_s", 0.5)):
            return
        con = self.dconnect()
        try:
            con.execute("BEGIN")
            if self.variant == "no_fence":
                self.dx(con, "UPDATE _cdc.toy_leader SET beat_at = now()::TIMESTAMP WHERE k = 1")
            else:
                n = self.dx(con, "UPDATE _cdc.toy_leader SET beat_at = now()::TIMESTAMP WHERE k = 1 AND epoch = ? "
                                 "AND state = 'leading'", [self.epoch]).fetchone()[0]
                if n != 1:
                    con.execute("ROLLBACK")
                    raise FenceLost("heartbeat changed no row")
            self.commit(con)
        finally:
            con.close()
        self.last_beat = time.monotonic()
        crash_point("toyrep.heartbeat_written")

    def release(self):
        con = self.dconnect()
        try:
            con.execute("BEGIN")
            self.dx(con, "UPDATE _cdc.toy_leader SET state = 'released' WHERE k = 1 AND epoch = ?", [self.epoch])
            self.fence(con)
            self.commit(con)
        finally:
            con.close()
        self.event("released", epoch=self.epoch)
        crash_point("toyrep.released")
        sys.exit(0)

    # ------------------------------------------------------------------ source
    def src_params(self, replication=False):
        s = self.cfg["source"]
        p = dict(host=s["host"], port=int(s["port"]), dbname=s["dbname"], user=s["user"], passfile=s["passfile"],
                 sslmode="verify-full", sslrootcert=s["sslrootcert"], connect_timeout=5, gssencmode="disable",
                 application_name="cdc_v2_toy_%s" % self.tag,
                 options="-c TimeZone=UTC -c DateStyle=ISO -c statement_timeout=%d"
                         % int(1000 * float(self.cfg.get("statement_timeout_s", 10))))
        if replication:
            p["replication"] = "database"
        return p

    def sconnect(self):
        fault_site("toyrep.source_connect")
        if int(self.cfg["source"]["port"]) == 5432:
            raise RuntimeError("refusing port 5432")
        conn = psycopg.connect(autocommit=True, **self.src_params())
        if self.src_lost:
            self.event("source_reconnected")
            if self.variant == "skip_on_reconnect":
                # the bug: continue from the slot's newest position instead of from what is durable
                here = conn.execute("SELECT CASE WHEN pg_is_in_recovery() THEN pg_last_wal_replay_lsn() "
                                    "ELSE pg_current_wal_lsn() END::text").fetchone()[0]
                try:
                    conn.execute("SELECT pg_replication_slot_advance(%s, %s::pg_lsn)", (self.slot, here))
                    self.event("skipped_to_newest", position=here)
                except psycopg.Error:
                    pass
            self.src_lost = False
        return conn

    def sq(self, sql, params=None, timeout=None):
        """One source statement with a client-side deadline: the socket is shut down when it passes (a black hole
        through the proxy is found by this deadline, docs/testing/harness.md section 7)."""
        if self.src is None:
            self.src = self.sconnect()
        conn = self.src
        fired = threading.Event()

        def shut():
            fired.set()
            try:
                fd = conn.pgconn.socket
                s = socket.socket(fileno=os.dup(fd))
                try:
                    s.shutdown(socket.SHUT_RDWR)
                finally:
                    s.close()
            except Exception:                              # noqa: BLE001
                pass

        timer = threading.Timer(timeout or float(self.cfg.get("client_timeout_s", 8)), shut)
        timer.daemon = True
        timer.start()
        try:
            cur = conn.execute(sql, params)
            return cur.fetchall() if cur.description else None
        except psycopg.OperationalError:
            self.drop_source("deadline" if fired.is_set() else "connection")
            raise
        finally:
            timer.cancel()

    def drop_source(self, why):
        if self.src is not None:
            try:
                self.src.close()
            except Exception:                              # noqa: BLE001
                pass
        self.src = None
        self.src_lost = True
        self.event("source_lost", why=why)

    def load_meta(self):
        meta = {}
        for t in self.tables:
            s, n = t.split(".", 1)
            cols = self.sq("SELECT a.attname, format_type(a.atttypid, a.atttypmod) FROM pg_attribute a "
                           "JOIN pg_class c ON c.oid = a.attrelid JOIN pg_namespace ns ON ns.oid = c.relnamespace "
                           "WHERE ns.nspname = %s AND c.relname = %s AND a.attnum > 0 AND NOT a.attisdropped "
                           "ORDER BY a.attnum", (s, n))
            if not cols:
                raise RuntimeError("source table %s not found" % t)
            key = self.sq("SELECT a.attname FROM pg_index i JOIN pg_class c ON c.oid = i.indrelid "
                          "JOIN pg_namespace ns ON ns.oid = c.relnamespace JOIN pg_attribute a ON a.attrelid = c.oid "
                          "AND a.attnum = ANY(i.indkey) WHERE i.indisprimary AND ns.nspname = %s AND c.relname = %s"
                          + ("" if self.variant == "deferrable_by_key" else " AND i.indimmediate"), (s, n))
            # rule K (core design 6): a deferrable key is not a key; the variant deferrable_by_key applies it by key
            names = [c for c, _ in cols]
            meta[t] = {"columns": [(c, typ) for c, typ in cols], "types": [dd_type(typ) for _, typ in cols],
                       "key": sorted(names.index(k) for (k,) in key)}
        self.meta = meta

    def slot_state(self):
        rows = self.sq("SELECT confirmed_flush_lsn::text, wal_status, conflicting, invalidation_reason "
                       "FROM pg_replication_slots WHERE slot_name = %s", (self.slot,))
        if not rows:
            return None
        cf, wal, conflicting, reason = rows[0]
        return {"confirmed": lsn.parse(cf) if cf else 0, "invalid": wal == "lost" or bool(conflicting) or bool(reason)}

    def advance(self, position: int):
        fault_site("toyrep.slot_advance")
        try:
            self.sq("SELECT pg_replication_slot_advance(%s, %s::pg_lsn)", (self.slot, lsn.format(position)))
        except psycopg.errors.ObjectInUse:
            return False
        return True

    # ------------------------------------------------------------------ resynchronisation (slot lost or new)
    def resync(self, why):
        self.event("resync_start", why=why)
        con = self.dconnect()
        try:
            con.execute("BEGIN")
            self.dx(con, "UPDATE _cdc.stream_state SET resync = true WHERE slot_name = ?", [self.slot])
            self.fence(con)
            self.commit(con)
        finally:
            con.close()
        crash_point("toyrep.resync_marked")
        rows = self.sq("SELECT active_pid FROM pg_replication_slots WHERE slot_name = %s", (self.slot,))
        if rows:
            if rows[0][0]:
                self.sq("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE pid = %s AND "
                        "application_name LIKE 'cdc_v2_toy_%%'", (rows[0][0],))
                time.sleep(0.2)
            self.sq("SELECT pg_drop_replication_slot(%s)", (self.slot,))
        if self.meta is None:
            self.load_meta()
        nudge_stop = threading.Event()
        nudger = None
        if self.cfg.get("nudge"):
            nudger = threading.Thread(target=self._nudge, args=(nudge_stop,), daemon=True)
            nudger.start()
        rep = psycopg.connect(autocommit=True, **self.src_params(replication=True))
        try:
            pg = rep.pgconn

            def ex(sql):
                r = pg.exec_(sql.encode())
                if r.error_message:
                    raise RuntimeError("replication command failed: %s" % r.error_message.decode()[:200])
                return [[None if r.get_value(i, j) is None else r.get_value(i, j).decode()
                         for j in range(r.nfields)] for i in range(r.ntuples)]

            ex("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
            created = ex("CREATE_REPLICATION_SLOT %s LOGICAL test_decoding (SNAPSHOT 'use')" % self.slot)
            nudge_stop.set()
            consistent = lsn.parse(created[0][1])
            crash_point("toyrep.slot_created")
            data = {}
            for t, m in self.meta.items():
                cols = ", ".join("%s::text" % qi(c) for c, _ in m["columns"])
                data[t] = ex("SELECT %s FROM %s" % (cols, qt(t)))
            ex("COMMIT")
        finally:
            nudge_stop.set()
            rep.close()
        con = self.dconnect()
        try:
            con.execute("BEGIN")
            self.ensure_tables(con)
            for t, rows_ in data.items():
                self.dx(con, "DELETE FROM %s" % qt(t))
                for r in rows_:
                    self.insert(con, t, r)
            self.dx(con, "UPDATE _cdc.stream_state SET applied_lsn = ?, resync = false WHERE slot_name = ?",
                    [consistent, self.slot])
            self.dx(con, "INSERT INTO _cdc.toy_log VALUES (nextval('_cdc.toy_log_seq'), ?, ?, ?, 0, 'resync')",
                    [self.epoch, self.run_id, consistent])
            self.fence(con)
            self.commit(con)
        finally:
            con.close()
        self.event("resync_done", position=lsn.format(consistent), rows=sum(len(v) for v in data.values()))
        crash_point("toyrep.resync_committed")

    def _nudge(self, stop):
        try:
            with psycopg.connect(autocommit=True, **dict(self.src_params(), **self.cfg["nudge"])) as c:
                while not stop.is_set():
                    c.execute("SELECT pg_log_standby_snapshot()")
                    stop.wait(0.2)
        except Exception as e:                             # noqa: BLE001 - slot creation still ends without it
            self.event("nudge_failed", error=type(e).__name__)

    def ensure_tables(self, con):
        for t, m in self.meta.items():
            s, _ = t.split(".", 1)
            con.execute("CREATE SCHEMA IF NOT EXISTS %s" % qi(s))
            con.execute("CREATE TABLE IF NOT EXISTS %s (%s)" % (
                qt(t), ", ".join("%s %s" % (qi(c), ty) for (c, _), ty in zip(m["columns"], m["types"]))))

    # ------------------------------------------------------------------ apply
    def insert(self, con, t, values):
        m = self.meta[t]
        self.dx(con, "INSERT INTO %s VALUES (%s)" % (qt(t), ", ".join("CAST(? AS %s)" % ty for ty in m["types"])),
                list(values))

    def _where(self, m, idx, values):
        conds, params = [], []
        for i, v in zip(idx, values):
            c = qi(m["columns"][i][0])
            if v is None:
                conds.append("%s IS NULL" % c)
            else:
                conds.append("%s = CAST(? AS %s)" % (c, m["types"][i]))
                params.append(v)
        return " AND ".join(conds) or "true", params

    def apply_row(self, con, t, row):
        m = self.meta[t]
        n = len(m["columns"])
        a = [row["a%d" % i] for i in range(n)]
        op = row["op"]
        if not m["key"]:
            if op == "u" and self.variant != "keyless_toast_null":
                for i in range(n):                 # an unchanged (TOASTed) value: the old image of FULL holds it
                    if row.get("u%d" % i):
                        a[i] = row["b%d" % i]
            if op in ("d", "u"):
                b = [row["b%d" % i] for i in range(n)]
                where, params = self._where(m, range(n), b)
                got = self.dx(con, "DELETE FROM %s WHERE rowid = (SELECT rowid FROM %s WHERE %s LIMIT 1)"
                              % (qt(t), qt(t), where), params).fetchone()[0]
                if got != 1:
                    raise ApplyInvariantError("keyless %s found %d rows to remove" % (t, got))
            if op in ("c", "u"):
                self.insert(con, t, a)
            return
        key = m["key"]
        if op == "d":
            where, params = self._where(m, key, [a[i] for i in key])
            self.dx(con, "DELETE FROM %s WHERE %s" % (qt(t), where), params)
            return
        old_key = [row.get("k%d" % i) for i in key]
        if op != "u" or all(v is None for v in old_key):
            old_key = [a[i] for i in key]
        where, params = self._where(m, key, old_key)
        missing = [i for i in range(n) if row.get("u%d" % i)]
        if missing:
            got = self.dx(con, "SELECT %s FROM %s WHERE %s" % (", ".join("%s::VARCHAR" % qi(m["columns"][i][0])
                                                                        for i in missing), qt(t), where),
                          params).fetchone()
            if got is None:
                raise ApplyInvariantError("unchanged value of %s has no earlier row" % t)
            for i, v in zip(missing, got):
                a[i] = v
        self.dx(con, "DELETE FROM %s WHERE %s" % (qt(t), where), params)
        if old_key != [a[i] for i in key]:
            w2, p2 = self._where(m, key, [a[i] for i in key])
            self.dx(con, "DELETE FROM %s WHERE %s" % (qt(t), w2), p2)
        self.insert(con, t, a)

    def conflict_draw(self, group):
        """The simulated destination conflict of this attempt: (transaction index, table) or None."""
        rate = float(self.cfg.get("conflict_rate", 0.0))
        if not rate or not group:
            return None
        r = random.Random("%s/%d/%d/%d" % (self.cfg.get("seed", 0), group[0]["commit_lsn"], self.attempt,
                                           os.getpid()))
        if r.random() >= rate:
            return None
        i = r.randrange(len(group))
        tabs = sorted({t for t, _ in group[i]["items"] if t in self.meta})
        return (i, r.choice(tabs)) if tabs else None

    def apply_group(self, group, prev):
        order = list(range(len(group)))
        if self.variant == "reorder" and len(order) >= 2:
            order[0], order[-1] = order[-1], order[0]              # the bug: two transactions swapped
        end = group[-1]["commit_lsn"]
        conflict = self.conflict_draw(group)
        con = self.dconnect()
        try:
            con.execute("BEGIN")
            crash_point("toyrep.begun")
            cur = self.dx(con, "SELECT applied_lsn FROM _cdc.stream_state WHERE slot_name = ?", [self.slot]).fetchone()
            if cur is None or int(cur[0]) != prev:
                con.execute("ROLLBACK")
                return False
            for idx in order:
                txn = group[idx]
                applied_here = 0
                skipped = set()
                for t, row in txn["items"]:
                    if t not in self.meta or t in skipped or t in self.quarantined:
                        continue
                    try:
                        if conflict == (idx, t):
                            raise ToyConflict("write-write conflict on %s" % t)
                        if row == "T":
                            self.dx(con, "DELETE FROM %s" % qt(t))
                        else:
                            self.apply_row(con, t, row)
                    except ApplyInvariantError as e:
                        e.table = t
                        raise
                    except (ToyConflict, FaultInjected, duckdb.Error):
                        if self.variant != "swallow":
                            raise
                        skipped.add(t)                              # the bug: the error is ignored
                        self.event("apply_error_ignored", table=t)
                        continue
                    applied_here += 1
                    if self.variant == "split" and applied_here == 1 and len(txn["items"]) > 1:
                        # the bug: one source transaction committed in two destination transactions
                        self.commit(con)
                        con.close()
                        time.sleep(float(self.cfg.get("split_gap_s", 0.02)))
                        con = self.dconnect()
                        con.execute("BEGIN")
                self.dx(con, "INSERT INTO _cdc.toy_log VALUES (nextval('_cdc.toy_log_seq'), ?, ?, ?, ?, 'txn')",
                        [self.epoch, self.run_id, txn["commit_lsn"], txn["xid"]])
            crash_point("toyrep.applied")
            if self.variant == "position_apart":
                self.fence(con)
                self.commit(con)                                    # the bug: data and position apart
                crash_point("toyrep.data_committed")
                con.execute("BEGIN")
                prev = int(self.dx(con, "SELECT applied_lsn FROM _cdc.stream_state WHERE slot_name = ?",
                                   [self.slot]).fetchone()[0])
            n = self.dx(con, "UPDATE _cdc.stream_state SET applied_lsn = ? WHERE slot_name = ? AND applied_lsn = ?",
                        [end, self.slot, prev]).fetchone()[0]
            if n != 1:
                con.execute("ROLLBACK")
                return False
            crash_point("toyrep.position_written")
            self.fence(con)
            crash_point("toyrep.fenced")
            if self.variant == "ack_early":
                self.advance(end)                                   # the bug: confirmed before the commit
                crash_point("toyrep.acked_early")
            crash_point("toyrep.commit_sent")
            self.commit(con)
            crash_point("toyrep.committed")
        except BaseException:
            try:
                con.execute("ROLLBACK")
            except Exception:                                      # noqa: BLE001
                pass
            raise
        finally:
            con.close()
        return True

    # ------------------------------------------------------------------ the stream path
    def through_frames(self, txns):
        """Encode, spill, inject the configured byte faults, decode and assemble (the engine-to-applier path)."""
        data = encode_batch(txns, self.meta)
        spill = self.spill(data)
        try:
            rate = float(self.cfg.get("stream_fault_rate", 0.0))
            self.batch_no += 1
            fr = random.Random("%s/stream/%d/%d" % (self.cfg.get("stream_seed", 0), txns[0]["commit_lsn"],
                                                    self.batch_no))
            injected = []
            if rate and fr.random() < rate:
                plan = pipefaults.make_plan(fr.getrandbits(32), len(data), kinds=("cut", "dup", "truncate", "flip"))
                data, log = pipefaults.apply_bytes(data, plan)
                injected = [e["kind"] for e in log if e.get("fired")]
                if injected:
                    self.event("stream_fault", kinds=injected)
            if self.variant == "trust_frames":
                return assemble(lenient_frames(data), strict=False)
            try:
                frames = F.decode_all(data, max_frame_bytes=64 << 20)
                return assemble(frames, strict=True)
            except (F.EngineBroken, StreamBroken) as e:
                self.event("stream_broken", error=str(e)[:80], injected=injected)
                return None
        finally:
            if spill:
                try:
                    os.unlink(spill)
                except OSError:
                    pass

    def spill(self, data):
        if not self.spill_dir:
            return None
        quota = int(self.cfg.get("spill_quota", 8 << 20))
        while True:
            used = 0
            for name in os.listdir(self.spill_dir):
                try:
                    used += os.stat(os.path.join(self.spill_dir, name)).st_size
                except OSError:
                    pass
            if used + len(data) <= quota:
                break
            if not self._spill_waiting:
                self.event("spill_full", used=used, need=len(data), quota=quota)
                self._spill_waiting = True
            self.heartbeat()                                        # it waits and stays alive; it never crashes
            time.sleep(0.1)
        if self._spill_waiting:
            self.event("spill_free")
            self._spill_waiting = False
        path = os.path.join(self.spill_dir, "%s-%d-%d.bin" % (self.tag, os.getpid(), self.batch_no))
        with open(path, "wb") as f:
            f.write(data)
        return path

    def applied(self):
        con = self.dconnect()
        try:
            return con.execute("SELECT applied_lsn, resync FROM _cdc.stream_state WHERE slot_name = ?",
                               [self.slot]).fetchone()
        finally:
            con.close()

    def step(self):
        if self.cfg.get("max_runtime_s") and time.monotonic() - self.started > float(self.cfg["max_runtime_s"]):
            self.release()
        self.heartbeat()
        prev, pending = self.applied()
        prev = int(prev)
        state = self.slot_state()
        if pending or state is None or state["invalid"]:
            self.resync("pending" if pending else "slot missing" if state is None else "slot invalid")
            return
        if self.meta is None:
            self.load_meta()
        fault_site("toyrep.slot_peek")
        try:
            rows = self.sq("SELECT lsn::text, xid::text, data FROM pg_logical_slot_peek_changes(%s, NULL, %s, "
                           "'include-xids', '1', 'skip-empty-xacts', '1')",
                           (self.slot, int(self.cfg.get("batch_changes", 400))))
        except psycopg.errors.ObjectInUse:
            time.sleep(0.05)
            return
        txns = [t for t in parse_batch(rows) if t["commit_lsn"] > prev]
        if not txns:
            if state["confirmed"] < prev:
                self.advance(prev)                                 # confirm what is durable already
            time.sleep(float(self.cfg.get("poll_s", 0.05)))
            return
        group = self.through_frames(txns)
        if not group:
            time.sleep(0.05)
            return
        group = [g for g in group if g["commit_lsn"] > prev]
        if not group:
            return
        self.attempt += 1
        if self.apply_group(group, prev):
            end = group[-1]["commit_lsn"]
            if self.advance(end):
                crash_point("toyrep.slot_advanced")

    def quarantine(self, table):
        """A table that fails to apply max_table_attempts times leaves the stream (contract C10): recorded in a fenced
        transaction, with an event (the alert); the stream goes on without it."""
        con = self.dconnect()
        try:
            con.execute("BEGIN")
            self.dx(con, "INSERT INTO _cdc.toy_quarantine VALUES (?, now()::TIMESTAMP, ?)", [table, self.epoch])
            self.fence(con)
            self.commit(con)
        finally:
            con.close()
        self.quarantined.add(table)
        self.event("quarantined", table=table)

    def run(self):
        self.ensure_schema()
        con = self.dconnect()
        try:
            self.quarantined = {r[0] for r in con.execute("SELECT tbl FROM _cdc.toy_quarantine").fetchall()}
        finally:
            con.close()
        self.guard()
        if self.spill_dir:
            os.makedirs(self.spill_dir, exist_ok=True)
            for name in os.listdir(self.spill_dir):
                if name.startswith(self.tag + "-"):
                    os.unlink(os.path.join(self.spill_dir, name))
        self.event("started", epoch=self.epoch, variant=self.variant)
        backoff = 0.05
        while True:
            try:
                self.step()
                backoff = 0.05
            except FenceLost as e:
                self.event("superseded", why=str(e))
                sys.exit(10)
            except psycopg.Error as e:
                if self.src is not None and (self.src.closed or self.src.broken):
                    self.drop_source(type(e).__name__)
                self.event("source_error", error=type(e).__name__)
                time.sleep(backoff)
                backoff = min(1.0, backoff * 2)
            except ApplyInvariantError as e:
                self.event("retry", error="ApplyInvariantError", detail=str(e)[:100])
                self.invariant_errors[getattr(e, "table", None)] = self.invariant_errors.get(
                    getattr(e, "table", None), 0) + 1
                if getattr(e, "table", None) and self.invariant_errors[e.table] >= int(
                        self.cfg.get("max_table_attempts", 3)):
                    try:
                        self.quarantine(e.table)
                    except (FenceLost, SystemExit):
                        raise
                    except Exception:                      # noqa: BLE001 - tried again with the next error
                        pass
                time.sleep(backoff)
                backoff = min(1.0, backoff * 2)
            except (ToyConflict, FaultInjected, DestBusy, duckdb.Error, RuntimeError, OSError) as e:
                self.event("retry", error=type(e).__name__, detail=str(e)[:100])
                time.sleep(backoff)
                backoff = min(1.0, backoff * 2)


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", required=True)
    ap.add_argument("--tag", default="a")
    ap.add_argument("--run-id", default=None)
    ap.add_argument("--guard", choices=("wait", "exit"), default=None)
    ap.add_argument("--max-runtime", type=float, default=None)
    a = ap.parse_args(argv)
    with open(a.config, encoding="utf-8") as f:
        cfg = json.load(f)
    if a.guard:
        cfg["guard"] = a.guard
    if a.max_runtime is not None:
        cfg["max_runtime_s"] = a.max_runtime
    Toy(cfg, a.tag, a.run_id or "%s-%d" % (a.tag, os.getpid())).run()


if __name__ == "__main__":
    sys.exit(main())
