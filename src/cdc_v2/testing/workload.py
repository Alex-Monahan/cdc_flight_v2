"""Seeded, reproducible workload against a Postgres source (tier T1).

A workload runs in **rounds**. In each round every writer runs `txns_per_round` transactions on its own connection,
all writers at the same time; after the round the driver may run one DDL group or one TRUNCATE while the writers
wait. Between rounds the source is quiet: that is a **quiesce point**, where `quiesce()` returns the source's WAL
position and the oracle compares.

Reproducibility: statements are generated from the seed and an in-memory model, never from what the database
returns. Writer w owns the keys `k * writers + w + 1` and the keyless rows whose column `w` is w, so writers never
touch each other's rows (transfers excepted: they only add amounts, which commute). The final state after a round
therefore does not depend on how the writers interleave, and the same seed always produces the same statements per
writer. Every executed transaction and driver step is appended to a JSON-lines **replay log**; `replay()` runs a log
again, in log order, on one connection.

Feature groups (`Features`) switch each kind of work on or off, so early phases run only what the pipeline supports.
"""
from __future__ import annotations

import copy
import dataclasses
import decimal
import json
import random
import threading
import time
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Iterable, Optional

from cdc_v2 import lsn

ALPHABET = "abcdefghijklmnopqrstuvwxyz ABC'é✓_-"


@dataclass
class Features:
    single_row: bool = True          # transactions with one row change
    multi_row: bool = True           # several rows of one table
    multi_table: bool = True         # rows of several tables in one transaction
    savepoints: bool = True          # SAVEPOINT ... ROLLBACK TO SAVEPOINT inside a committed transaction
    rollbacks: bool = True           # transactions that end with ROLLBACK
    large_txn: bool = False          # one large transaction per writer now and then (large_txn_rows rows)
    inserts: bool = True
    updates: bool = True
    deletes: bool = True
    pk_updates: bool = True          # primary-key updates, including chains (A->B, B->C) and swaps
    toast_unchanged: bool = True     # updates that leave a large TOASTed text column unchanged
    keyless: bool = True             # a table without a primary key, with identical rows
    truncate: bool = False           # driver TRUNCATE between rounds (single and multi-table)
    ddl: bool = False                # driver DDL group between rounds
    transfers: bool = False          # transfer transactions with a constant global sum
    # Shapes for the consumer loops of SPEC 1.3 (package F). Each keeps an invariant across two tables that holds
    # in every committed source state, so an observer of the destination sees a split transaction.
    parent_child: bool = False       # w_parent.n_children = number of its rows in w_child, changed together
    ledger: bool = False             # sum(w_debit.amount) = sum(w_credit.amount): entries booked on both sides
    large_marked: bool = False       # a large insert into w_bulk plus its marker row in w_bulk_mark, one transaction
    large_rollback: bool = False     # the same large insert, rolled back (it must never be visible)

    @classmethod
    def only(cls, **on) -> "Features":
        """All groups off except the named ones."""
        f = cls(**{k.name: False for k in dataclasses.fields(cls)})
        for k, v in on.items():
            setattr(f, k, v)
        return f


@dataclass
class WorkloadConfig:
    seed: int
    writers: int = 2
    txns_per_round: int = 20
    keyed_tables: int = 2
    features: Features = field(default_factory=Features)
    schema: str = "public"
    large_txn_rows: int = 10_000
    large_txn_prob: float = 0.02     # per transaction, when large_txn is on
    truncate_prob: float = 0.15      # per round, when truncate is on
    ddl_prob: float = 0.5            # per round, when ddl is on
    toast_bytes: int = 20_000
    transfer_tables: int = 3
    transfer_accounts: int = 8       # per transfer table
    transfer_balance: int = 1_000    # initial balance of every account

    def to_dict(self) -> dict:
        return dataclasses.asdict(self)

    @classmethod
    def from_dict(cls, d: dict) -> "WorkloadConfig":
        d = dict(d)
        d["features"] = Features(**d.get("features", {}))
        return cls(**d)


# ------------------------------------------------------------------------------------------------ literals


def lit(v) -> str:
    if v is None:
        return "NULL"
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, (int, decimal.Decimal)):
        return str(v)
    if isinstance(v, str):
        return "'" + v.replace("'", "''") + "'"
    raise TypeError(type(v))


def ident(name: str) -> str:
    return '"%s"' % name.replace('"', '""')


# ------------------------------------------------------------------------------------------------ model

COLUMN_TYPES = ["integer", "bigint", "text", "numeric(12,2)", "timestamptz", "boolean"]
WIDENINGS = {"integer": "bigint", "numeric(12,2)": "numeric(16,4)", "text": "varchar(200)"}


@dataclass
class TableModel:
    name: str
    columns: list                    # [(name, type)] without the key column "id"
    created_by_ddl: bool = False


@dataclass
class Model:
    """What the generator believes the source holds. Changed only by planned statements."""
    keyed: dict = field(default_factory=dict)          # name -> TableModel
    keys: dict = field(default_factory=dict)           # (writer, table) -> set of keys
    next_key: dict = field(default_factory=dict)       # writer -> counter
    keyless: dict = field(default_factory=dict)        # writer -> Counter of (a, b)
    toast: dict = field(default_factory=dict)          # writer -> set of keys
    parents: dict = field(default_factory=dict)        # writer -> {parent id: list of child ids}
    entries: dict = field(default_factory=dict)        # writer -> {entry id: (debit ids, credit ids)}
    batches: dict = field(default_factory=dict)        # writer -> {batch id: (first index, rows)}
    ddl_seq: int = 0


@dataclass
class Txn:
    writer: int
    round: int
    seq: int
    shape: str
    statements: list
    end: str                          # 'commit' or 'rollback'


class Workload:
    def __init__(self, config: WorkloadConfig, connect: Optional[Callable] = None,
                 log_path: Optional[Path] = None):
        """connect(): a new autocommit psycopg connection to the source database (None for planning only)."""
        self.cfg = config
        self.connect = connect
        self.log_path = Path(log_path) if log_path else None
        self.model = Model()
        self.rngs = [random.Random("%d/w%d" % (config.seed, w)) for w in range(config.writers)]
        self.driver_rng = random.Random("%d/driver" % config.seed)
        self.round = 0
        self.seq = [0] * config.writers
        self._log_lock = threading.Lock()
        self._log = None
        self.stats = Counter()
        self._bg: Optional[threading.Thread] = None
        self._pause = threading.Event()
        self._paused = threading.Event()
        self._stop = threading.Event()
        self._bg_error: Optional[BaseException] = None
        self.last_quiesce_lsn: Optional[int] = None

    # ---------------------------------------------------------------- names and setup

    def q(self, table: str) -> str:
        return "%s.%s" % (ident(self.cfg.schema), ident(table))

    @property
    def transfer_table_names(self) -> list:
        return ["w_xfer_%d" % i for i in range(self.cfg.transfer_tables)]

    @property
    def transfer_sum(self) -> int:
        return self.cfg.transfer_tables * self.cfg.transfer_accounts * self.cfg.transfer_balance

    def setup_statements(self) -> list:
        f, s = self.cfg.features, []
        s.append("CREATE SCHEMA IF NOT EXISTS %s" % ident(self.cfg.schema))
        for i in range(self.cfg.keyed_tables):
            name = "w_k%d" % i
            cols = [("a", "integer"), ("b", "text"), ("c", "numeric(12,2)"), ("d", "timestamptz"), ("e", "boolean")]
            self.model.keyed[name] = TableModel(name, list(cols))
            s.append("CREATE TABLE %s (id bigint PRIMARY KEY, %s)" % (
                self.q(name), ", ".join("%s %s" % (ident(c), t) for c, t in cols)))
        if f.keyless:
            s.append("CREATE TABLE %s (w integer NOT NULL, a integer, b text)" % self.q("w_nk"))
            s.append("ALTER TABLE %s REPLICA IDENTITY FULL" % self.q("w_nk"))
        if f.toast_unchanged:
            s.append("CREATE TABLE %s (id bigint PRIMARY KEY, n integer NOT NULL, big text NOT NULL)" % self.q("w_toast"))
        if f.transfers:
            for t in self.transfer_table_names:
                s.append("CREATE TABLE %s (id integer PRIMARY KEY, balance bigint NOT NULL)" % self.q(t))
                s.append("INSERT INTO %s SELECT g, %d FROM generate_series(0, %d) AS g" % (
                    self.q(t), self.cfg.transfer_balance, self.cfg.transfer_accounts - 1))
        if f.parent_child:
            s.append("CREATE TABLE %s (id bigint PRIMARY KEY, w integer NOT NULL, n_children integer NOT NULL)"
                     % self.q("w_parent"))
            s.append("CREATE TABLE %s (id bigint PRIMARY KEY, parent_id bigint NOT NULL, w integer NOT NULL, v text)"
                     % self.q("w_child"))
        if f.ledger:
            for t in ("w_debit", "w_credit"):
                s.append("CREATE TABLE %s (id bigint PRIMARY KEY, entry bigint NOT NULL, amount bigint NOT NULL)"
                         % self.q(t))
        if f.large_marked or f.large_rollback:
            s.append("CREATE TABLE %s (id bigint PRIMARY KEY, batch bigint NOT NULL, v integer)" % self.q("w_bulk"))
            s.append("CREATE TABLE %s (batch bigint PRIMARY KEY, n integer NOT NULL)" % self.q("w_bulk_mark"))
        for w in range(self.cfg.writers):
            self.model.next_key[w] = 0
            self.model.keyless[w] = Counter()
            self.model.toast[w] = set()
            self.model.parents[w] = {}
            self.model.entries[w] = {}
            self.model.batches[w] = {}
        return s

    def table_names(self) -> list:
        """The tables the workload creates at setup (DDL may add and drop more), as `schema.table`."""
        f, out = self.cfg.features, ["w_k%d" % i for i in range(self.cfg.keyed_tables)]
        out += ["w_nk"] if f.keyless else []
        out += ["w_toast"] if f.toast_unchanged else []
        out += self.transfer_table_names if f.transfers else []
        out += ["w_parent", "w_child"] if f.parent_child else []
        out += ["w_debit", "w_credit"] if f.ledger else []
        out += ["w_bulk", "w_bulk_mark"] if f.large_marked or f.large_rollback else []
        return ["%s.%s" % (self.cfg.schema, t) for t in out]

    # ---------------------------------------------------------------- value generation

    def _value(self, r: random.Random, typ: str):
        if r.random() < 0.1:
            return None
        base = typ.split("(")[0]
        if base == "integer":
            return r.randint(-100000, 100000)
        if base == "bigint":
            return r.randint(-10 ** 12, 10 ** 12)
        if base in ("text", "varchar"):
            return "".join(r.choice(ALPHABET) for _ in range(r.randint(0, 24)))
        if base == "numeric":
            scale = int(typ.split(",")[1].rstrip(")")) if "," in typ else 2
            return decimal.Decimal(r.randint(-10 ** 9, 10 ** 9)).scaleb(-scale)
        if base == "timestamptz":
            us = r.randint(0, 10 ** 15)
            return "TS:%d" % us
        if base == "boolean":
            return r.random() < 0.5
        raise ValueError(typ)

    @staticmethod
    def _vlit(v) -> str:
        if isinstance(v, str) and v.startswith("TS:"):
            return "(TIMESTAMPTZ '2000-01-01 00:00:00+00' + %d * INTERVAL '1 microsecond')" % int(v[3:])
        return lit(v)

    def _new_key(self, w: int) -> int:
        k = self.model.next_key[w] * self.cfg.writers + w + 1
        self.model.next_key[w] += 1
        return k

    # ---------------------------------------------------------------- row operations (on a transaction-local state)

    def _op_keyed(self, r, w, st, kind: str, table: Optional[str] = None) -> list:
        tables = sorted(self.model.keyed)
        if not tables:
            return []
        t = table or r.choice(tables)
        tm = self.model.keyed[t]
        keys = st["keys"].setdefault(t, set(self.model.keys.get((w, t), set())))
        if kind == "update" and not keys or kind == "delete" and not keys:
            kind = "insert"
        if kind == "insert":
            k = self._new_key(w)
            keys.add(k)
            cols = ["id"] + [c for c, _ in tm.columns]
            vals = [str(k)] + [self._vlit(self._value(r, typ)) for _, typ in tm.columns]
            return ["INSERT INTO %s (%s) VALUES (%s)" % (self.q(t), ", ".join(ident(c) for c in cols),
                                                       ", ".join(vals))]
        k = r.choice(sorted(keys))
        if kind == "delete":
            keys.discard(k)
            return ["DELETE FROM %s WHERE id = %d" % (self.q(t), k)]
        if not tm.columns:
            return []
        chosen = r.sample(tm.columns, r.randint(1, min(3, len(tm.columns))))
        sets = ", ".join("%s = %s" % (ident(c), self._vlit(self._value(r, typ))) for c, typ in chosen)
        return ["UPDATE %s SET %s WHERE id = %d" % (self.q(t), sets, k)]

    def _op_pk(self, r, w, st) -> list:
        tables = sorted(self.model.keyed)
        if not tables:
            return []
        t = r.choice(tables)
        keys = st["keys"].setdefault(t, set(self.model.keys.get((w, t), set())))
        if len(keys) < 2:
            return self._op_keyed(r, w, st, "insert", t) + self._op_keyed(r, w, st, "insert", t)
        a, b = r.sample(sorted(keys), 2)
        q = self.q(t)
        if r.random() < 0.5:
            # chain inside one transaction: b -> fresh, a -> b
            fresh = self._new_key(w)
            keys.discard(a)
            keys.add(fresh)
            self.stats["pk_chain"] += 1
            return ["UPDATE %s SET id = %d WHERE id = %d" % (q, fresh, b),
                    "UPDATE %s SET id = %d WHERE id = %d" % (q, b, a)]
        # swap a <-> b through a temporary key
        tmp = self._new_key(w)
        self.stats["pk_swap"] += 1
        return ["UPDATE %s SET id = %d WHERE id = %d" % (q, tmp, a),
                "UPDATE %s SET id = %d WHERE id = %d" % (q, a, b),
                "UPDATE %s SET id = %d WHERE id = %d" % (q, b, tmp)]

    def _op_keyless(self, r, w, st) -> list:
        rows: Counter = st.setdefault("keyless", Counter(self.model.keyless[w]))
        q = self.q("w_nk")
        kind = r.choice(["insert", "insert", "delete", "update"])
        if kind != "insert" and not rows:
            kind = "insert"

        def match(a, b):
            return "w = %d AND a IS NOT DISTINCT FROM %s AND b IS NOT DISTINCT FROM %s" % (w, lit(a), lit(b))

        if kind == "insert":
            a, b = r.randint(0, 3), r.choice(["x", "y", None, ""])
            n = r.randint(1, 3)                             # identical rows on purpose
            rows[(a, b)] += n
            return ["INSERT INTO %s (w, a, b) VALUES %s" % (q, ", ".join(["(%d, %s, %s)" % (w, lit(a), lit(b))] * n))]
        a, b = r.choice(sorted(rows, key=repr))
        rows[(a, b)] -= 1
        if rows[(a, b)] <= 0:
            del rows[(a, b)]
        one = "ctid = (SELECT ctid FROM %s WHERE %s LIMIT 1)" % (q, match(a, b))
        if kind == "delete":
            return ["DELETE FROM %s WHERE %s" % (q, one)]
        a2 = r.randint(0, 3)
        rows[(a2, b)] += 1
        return ["UPDATE %s SET a = %d WHERE %s" % (q, a2, one)]

    def _op_toast(self, r, w, st) -> list:
        keys = st.setdefault("toast", set(self.model.toast[w]))
        q = self.q("w_toast")
        if not keys or r.random() < 0.3:
            k = self._new_key(w)
            keys.add(k)
            # a large, poorly compressible value: stored out of line (TOAST)
            return ["INSERT INTO %s (id, n, big) VALUES (%d, 0, (SELECT string_agg(md5(%s || g::text), '') "
                    "FROM generate_series(1, %d) AS g))" % (q, k, lit("%d/%d" % (self.cfg.seed, k)),
                                                             max(1, self.cfg.toast_bytes // 32))]
        k = r.choice(sorted(keys))
        if r.random() < 0.1 and self.cfg.features.deletes:
            keys.discard(k)
            return ["DELETE FROM %s WHERE id = %d" % (q, k)]
        self.stats["toast_unchanged_update"] += 1
        return ["UPDATE %s SET n = n + 1 WHERE id = %d" % (q, k)]

    def _op_transfer(self, r, w, st) -> list:
        cfg = self.cfg
        n = r.randint(2, 4)
        picks = set()
        while len(picks) < n:
            picks.add((r.randrange(cfg.transfer_tables), r.randrange(cfg.transfer_accounts)))
        picks = sorted(picks)                         # one global lock order: no deadlocks
        amounts = [r.randint(1, 50) for _ in picks[1:]]
        amounts = [-sum(amounts)] + amounts
        order = list(zip(picks, amounts))
        self.stats["transfer"] += 1
        return ["UPDATE %s SET balance = balance + (%d) WHERE id = %d" % (self.q("w_xfer_%d" % t), amt, a)
                for (t, a), amt in order]

    def _op_large(self, r, w, st) -> list:
        tables = sorted(self.model.keyed)
        if not tables:
            return []
        t = r.choice(tables)
        keys = st["keys"].setdefault(t, set(self.model.keys.get((w, t), set())))
        n = self.cfg.large_txn_rows
        first = self.model.next_key[w]
        self.model.next_key[w] += n
        W = self.cfg.writers
        new = {(first + i) * W + w + 1 for i in range(n)}
        keys.update(new)
        lo, hi = first * W + w + 1, (first + n - 1) * W + w + 1
        tm = self.model.keyed[t]
        stmts = ["INSERT INTO %s (id) SELECT g * %d + %d FROM generate_series(%d, %d) AS g" % (
            self.q(t), W, w + 1, first, first + n - 1)]
        intcol = next((c for c, typ in tm.columns if typ in ("integer", "bigint")), None)
        if intcol:
            stmts.append("UPDATE %s SET %s = 7 WHERE id BETWEEN %d AND %d AND id %% %d = %d" % (
                self.q(t), ident(intcol), lo, hi, W, (w + 1) % W))
        self.stats["large_txn"] += 1
        return stmts

    def _op_family(self, r, w, st) -> list:
        """Parent and children: children are inserted and deleted only together with the update of the parent's
        n_children, in one transaction (SPEC 1.3 consumer loop)."""
        fam = st.setdefault("parents", copy.deepcopy(self.model.parents[w]))
        P, C = self.q("w_parent"), self.q("w_child")
        kind = r.choice(["new", "grow", "grow", "shrink", "remove"]) if fam else "new"
        self.stats["family_" + kind] += 1
        if kind == "new":
            pid = self._new_key(w)
            kids = [self._new_key(w) for _ in range(r.randint(0, 4))]
            fam[pid] = kids
            out = ["INSERT INTO %s (id, w, n_children) VALUES (%d, %d, %d)" % (P, pid, w, len(kids))]
            if kids:
                out.append("INSERT INTO %s (id, parent_id, w, v) VALUES %s" % (C, ", ".join(
                    "(%d, %d, %d, %s)" % (k, pid, w, lit("c%d" % k)) for k in kids)))
            return out
        pid = r.choice(sorted(fam))
        kids = fam[pid]
        if kind == "remove":
            del fam[pid]
            return ["DELETE FROM %s WHERE parent_id = %d" % (C, pid), "DELETE FROM %s WHERE id = %d" % (P, pid)]
        if kind == "shrink" and kids:
            gone = sorted(r.sample(kids, r.randint(1, len(kids))))
            fam[pid] = [k for k in kids if k not in gone]
            return ["DELETE FROM %s WHERE id IN (%s)" % (C, ", ".join(str(k) for k in gone)),
                    "UPDATE %s SET n_children = n_children - %d WHERE id = %d" % (P, len(gone), pid)]
        new = [self._new_key(w) for _ in range(r.randint(1, 3))]
        fam[pid] = kids + new
        ins = "INSERT INTO %s (id, parent_id, w, v) VALUES %s" % (C, ", ".join(
            "(%d, %d, %d, %s)" % (k, pid, w, lit("c%d" % k)) for k in new))
        upd = "UPDATE %s SET n_children = n_children + %d WHERE id = %d" % (P, len(new), pid)
        return [upd, ins] if r.random() < 0.5 else [ins, upd]       # both orders of the two tables occur

    def _op_ledger(self, r, w, st) -> list:
        """Debits and credits: every entry is booked on both sides with equal totals, in one transaction."""
        ent = st.setdefault("entries", copy.deepcopy(self.model.entries[w]))
        D, K = self.q("w_debit"), self.q("w_credit")
        kind = r.choice(["book", "book", "rebook", "cancel"]) if ent else "book"
        self.stats["ledger_" + kind] += 1
        if kind == "cancel":
            e = r.choice(sorted(ent))
            del ent[e]
            return ["DELETE FROM %s WHERE entry = %d" % (K, e), "DELETE FROM %s WHERE entry = %d" % (D, e)]
        if kind == "rebook":
            e = r.choice(sorted(ent))
            d_ids, c_ids = ent[e]
            delta = r.randint(1, 500)
            return ["UPDATE %s SET amount = amount + %d WHERE id = %d" % (D, delta, d_ids[0]),
                    "UPDATE %s SET amount = amount + %d WHERE id = %d" % (K, delta, r.choice(c_ids))]
        e = self._new_key(w)
        total = r.randint(1, 10_000)
        parts = [total] if r.random() < 0.5 else [total // 2, total - total // 2]
        d_id = self._new_key(w)
        c_ids = [self._new_key(w) for _ in parts]
        ent[e] = ([d_id], c_ids)
        out = ["INSERT INTO %s (id, entry, amount) VALUES (%d, %d, %d)" % (D, d_id, e, total)]
        out += ["INSERT INTO %s (id, entry, amount) VALUES (%d, %d, %d)" % (K, c, e, a) for c, a in zip(c_ids, parts)]
        return out if r.random() < 0.5 else out[1:] + out[:1]

    def _op_bulk(self, r, w, st, rollback: bool) -> list:
        """A large insert into w_bulk and its marker row: visible not at all or completely. Committed batches are
        deleted again later (also a large transaction), so the table stays bounded."""
        bat = st.setdefault("batches", dict(self.model.batches[w]))
        B, M = self.q("w_bulk"), self.q("w_bulk_mark")
        if bat and not rollback and r.random() < 0.3:
            b = r.choice(sorted(bat))
            del bat[b]
            self.stats["bulk_delete"] += 1
            return ["DELETE FROM %s WHERE batch = %d" % (B, b), "DELETE FROM %s WHERE batch = %d" % (M, b)]
        n = self.cfg.large_txn_rows
        b = self._new_key(w)
        first = self.model.next_key[w]
        self.model.next_key[w] += n
        W = self.cfg.writers
        bat[b] = (first, n)
        self.stats["bulk_rollback" if rollback else "bulk_insert"] += 1
        return ["INSERT INTO %s (id, batch, v) SELECT g * %d + %d, %d, g %% 97 FROM generate_series(%d, %d) AS g" % (
            B, W, w + 1, b, first, first + n - 1),
            "INSERT INTO %s (batch, n) VALUES (%d, %d)" % (M, b, n)]

    # ---------------------------------------------------------------- transactions

    def _row_ops(self, r, w, st, n: int, table: Optional[str] = None) -> list:
        f = self.cfg.features
        kinds = [k for k, on in (("insert", f.inserts), ("update", f.updates), ("delete", f.deletes)) if on]
        out = []
        for _ in range(n):
            choices = []
            if kinds and self.model.keyed:
                choices += ["keyed"] * 4
            if f.keyless and table is None:
                choices.append("keyless")
            if f.toast_unchanged and table is None:
                choices.append("toast")
            if not choices:
                break
            c = r.choice(choices)
            if c == "keyed":
                out += self._op_keyed(r, w, st, r.choice(kinds), table)
            elif c == "keyless":
                out += self._op_keyless(r, w, st)
            else:
                out += self._op_toast(r, w, st)
        return out

    def _commit_state(self, w, st) -> None:
        for t, keys in st.get("keys", {}).items():
            if t in self.model.keyed:
                self.model.keys[(w, t)] = keys
        if "keyless" in st:
            self.model.keyless[w] = st["keyless"]
        if "toast" in st:
            self.model.toast[w] = st["toast"]
        for name in ("parents", "entries", "batches"):
            if name in st:
                getattr(self.model, name)[w] = st[name]

    def plan_txn(self, w: int) -> Optional[Txn]:
        f, r = self.cfg.features, self.rngs[w]
        shapes = []
        row_work = (f.inserts or f.updates or f.deletes) and self.model.keyed or f.keyless or f.toast_unchanged
        if row_work:
            shapes += [s for s, on in (("single", f.single_row), ("multi_row", f.multi_row),
                                       ("multi_table", f.multi_table), ("savepoint", f.savepoints),
                                       ("rollback", f.rollbacks)) if on]
        if f.pk_updates and self.model.keyed:
            shapes.append("pk_update")
        if f.transfers:
            shapes += ["transfer", "transfer"]
        if f.parent_child:
            shapes += ["family", "family"]
        if f.ledger:
            shapes += ["ledger", "ledger"]
        bulk = [k for k, on in (("large_marked", f.large_marked), ("large_rollback", f.large_rollback)) if on]
        if not shapes and not bulk:
            return None
        if f.large_txn and self.model.keyed and r.random() < self.cfg.large_txn_prob:
            shape = "large"
        elif bulk and (not shapes or r.random() < self.cfg.large_txn_prob):
            shape = r.choice(bulk)
        else:
            shape = r.choice(shapes)
        st: dict = {"keys": {}}
        body, end = [], "commit"
        if shape == "single":
            body = self._row_ops(r, w, st, 1)
        elif shape == "multi_row":
            tables = sorted(self.model.keyed)
            body = self._row_ops(r, w, st, r.randint(2, 8), r.choice(tables) if tables else None)
        elif shape == "multi_table":
            body = self._row_ops(r, w, st, r.randint(3, 8))
            if f.transfers and r.random() < 0.3:
                body += self._op_transfer(r, w, st)
        elif shape == "savepoint":
            body = self._row_ops(r, w, st, r.randint(1, 3))
            saved = copy.deepcopy(st)
            body += ["SAVEPOINT s1"] + self._row_ops(r, w, st, r.randint(1, 3)) + ["ROLLBACK TO SAVEPOINT s1"]
            st = saved
            body += self._row_ops(r, w, st, r.randint(1, 3))
            self.stats["savepoint_rollback"] += 1
        elif shape == "rollback":
            saved_next = self.model.next_key[w]
            body = self._row_ops(r, w, st, r.randint(1, 5))
            end = "rollback"
            st = {"keys": {}}
            self.model.next_key[w] = saved_next
        elif shape == "pk_update":
            body = self._op_pk(r, w, st)
        elif shape == "transfer":
            body = self._op_transfer(r, w, st)
        elif shape == "large":
            body = self._op_large(r, w, st)
        elif shape == "family":
            body = self._op_family(r, w, st)
        elif shape == "ledger":
            body = self._op_ledger(r, w, st)
        elif shape == "large_marked":
            body = self._op_bulk(r, w, st, rollback=False)
        elif shape == "large_rollback":
            saved_next = self.model.next_key[w]
            body = self._op_bulk(r, w, st, rollback=True)
            end = "rollback"
            st = {"keys": {}}
            self.model.next_key[w] = saved_next
        if not [s for s in body if not s.startswith(("SAVEPOINT", "ROLLBACK TO"))]:
            return None
        if end == "commit":
            self._commit_state(w, st)
        self.seq[w] += 1
        self.stats["txn_" + shape] += 1
        return Txn(w, self.round, self.seq[w], shape, ["BEGIN"] + body + ["COMMIT" if end == "commit" else "ROLLBACK"],
                   end)

    def plan_round(self) -> list:
        """Per writer, the list of transactions of the next round (pure: no database access)."""
        plans = []
        for w in range(self.cfg.writers):
            txns = []
            for _ in range(self.cfg.txns_per_round):
                t = self.plan_txn(w)
                if t is not None:
                    txns.append(t)
            plans.append(txns)
        return plans

    # ---------------------------------------------------------------- driver steps (between rounds)

    def plan_driver_step(self) -> Optional[dict]:
        f, r = self.cfg.features, self.driver_rng
        if f.truncate and r.random() < self.cfg.truncate_prob:
            cands = sorted(self.model.keyed) + (["w_nk"] if f.keyless else []) + (
                ["w_toast"] if f.toast_unchanged else [])
            if cands:
                chosen = sorted(r.sample(cands, r.randint(1, min(3, len(cands)))))
                for t in chosen:
                    if t == "w_nk":
                        for w in self.model.keyless:
                            self.model.keyless[w] = Counter()
                    elif t == "w_toast":
                        for w in self.model.toast:
                            self.model.toast[w] = set()
                    else:
                        for key in [k for k in self.model.keys if k[1] == t]:
                            self.model.keys[key] = set()
                self.stats["truncate"] += 1
                return {"kind": "truncate", "statements": ["TRUNCATE %s" % ", ".join(self.q(t) for t in chosen)]}
        if f.ddl and r.random() < self.cfg.ddl_prob:
            return self._plan_ddl(r)
        return None

    def _plan_ddl(self, r) -> Optional[dict]:
        m = self.model
        kinds = ["add_column", "add_column_default", "drop_column", "rename_column", "alter_type", "create_table",
                 "drop_table"]
        kind = r.choice(kinds)
        tables = sorted(m.keyed)
        if kind == "create_table" or not tables:
            m.ddl_seq += 1
            name = "w_ddl%d" % m.ddl_seq
            m.keyed[name] = TableModel(name, [("a", "integer"), ("b", "text")], created_by_ddl=True)
            self.stats["ddl_create_table"] += 1
            return {"kind": "ddl", "op": "create_table",
                    "statements": ["CREATE TABLE %s (id bigint PRIMARY KEY, a integer, b text)" % self.q(name)]}
        t = r.choice(tables)
        tm = m.keyed[t]
        q = self.q(t)
        stmts: list = []
        if kind == "drop_table":
            if len(tables) <= 1:
                kind = "add_column"
            else:
                del m.keyed[t]
                for key in [k for k in m.keys if k[1] == t]:
                    del m.keys[key]
                stmts = ["DROP TABLE %s" % q]
        if kind == "drop_column":
            if len(tm.columns) <= 1:
                kind = "add_column"
            else:
                c, _ = tm.columns.pop(r.randrange(len(tm.columns)))
                stmts = ["ALTER TABLE %s DROP COLUMN %s" % (q, ident(c))]
        if kind == "rename_column" and tm.columns:
            i = r.randrange(len(tm.columns))
            c, typ = tm.columns[i]
            m.ddl_seq += 1
            new = "%s_r%d" % (c.split("_r")[0], m.ddl_seq)
            tm.columns[i] = (new, typ)
            stmts = ["ALTER TABLE %s RENAME COLUMN %s TO %s" % (q, ident(c), ident(new))]
        if kind == "alter_type":
            widen = [(i, c, typ) for i, (c, typ) in enumerate(tm.columns) if typ in WIDENINGS]
            if not widen:
                kind = "add_column"
            else:
                i, c, typ = r.choice(widen)
                tm.columns[i] = (c, WIDENINGS[typ])
                stmts = ["ALTER TABLE %s ALTER COLUMN %s TYPE %s" % (q, ident(c), WIDENINGS[typ])]
        if kind in ("add_column", "add_column_default"):
            m.ddl_seq += 1
            typ = r.choice(COLUMN_TYPES)
            c = "x%d" % m.ddl_seq
            tm.columns.append((c, typ))
            default = ""
            if kind == "add_column_default":
                v = self._value(r, typ)
                if v is not None:
                    default = " DEFAULT %s" % self._vlit(v)
            stmts = ["ALTER TABLE %s ADD COLUMN %s %s%s" % (q, ident(c), typ, default)]
        self.stats["ddl_" + kind] += 1
        return {"kind": "ddl", "op": kind, "statements": stmts}

    # ---------------------------------------------------------------- execution

    def _write_log(self, entry: dict) -> None:
        if self.log_path is None:
            return
        with self._log_lock:
            if self._log is None:
                self.log_path.parent.mkdir(parents=True, exist_ok=True)
                self._log = self.log_path.open("a", encoding="utf-8")
            self._log.write(json.dumps(entry, sort_keys=True) + "\n")
            self._log.flush()

    def _exec(self, conn, statements: Iterable[str]) -> None:
        for s in statements:
            conn.execute(s)

    def setup(self) -> None:
        stmts = self.setup_statements()
        self._write_log({"type": "header", "seed": self.cfg.seed, "config": self.cfg.to_dict()})
        if self.connect is not None:
            with self.connect() as c:
                self._exec(c, stmts)
        self._write_log({"type": "setup", "statements": stmts})

    def run_round(self) -> dict:
        """Plan and execute one round, then the driver step. Returns what ran."""
        if self.connect is None:
            raise RuntimeError("no source connection: use plan_round() for planning only")
        plans = self.plan_round()
        errors: list = []

        def writer(txns):
            try:
                with self.connect() as c:
                    for t in txns:
                        self._exec(c, t.statements)
                        self._write_log({"type": "txn", "writer": t.writer, "round": t.round, "seq": t.seq,
                                         "shape": t.shape, "end": t.end, "statements": t.statements})
            except BaseException as e:            # noqa: BLE001 - re-raised below
                errors.append(e)

        threads = [threading.Thread(target=writer, args=(p,), daemon=True) for p in plans]
        for th in threads:
            th.start()
        for th in threads:
            th.join()
        if errors:
            raise errors[0]
        step = self.plan_driver_step()
        if step:
            with self.connect() as c:
                self._exec(c, step["statements"])
            self._write_log(dict(step, type="driver", round=self.round))
        self.round += 1
        return {"round": self.round - 1, "txns": sum(len(p) for p in plans), "driver": step}

    def quiesce(self) -> int:
        """Between rounds every writer is idle: return the source's WAL insert position (an int, C0). Every
        transaction of the workload so far ends at or before it; `compare(..., position=Q)` checks both sides."""
        with self.connect() as c:
            text = c.execute("SELECT pg_current_wal_insert_lsn()::text").fetchone()[0]
        pos = lsn.parse(text)
        self.last_quiesce_lsn = pos
        self._write_log({"type": "quiesce", "round": self.round, "lsn": text})
        return pos

    def run(self, rounds: int, on_quiesce: Optional[Callable[[str, int], None]] = None, quiesce_every: int = 0):
        """Run `rounds` rounds; with quiesce_every=n, call on_quiesce(position, round) after every n-th round."""
        for i in range(rounds):
            self.run_round()
            if quiesce_every and (i + 1) % quiesce_every == 0:
                pos = self.quiesce()
                if on_quiesce:
                    on_quiesce(pos, self.round)

    # continuous mode: rounds back to back in a background thread, paused at a round boundary

    def start(self) -> None:
        def loop():
            try:
                while not self._stop.is_set():
                    if self._pause.is_set():
                        self._paused.set()
                        time.sleep(0.01)
                        continue
                    self._paused.clear()
                    self.run_round()
            except BaseException as e:            # noqa: BLE001
                self._bg_error = e
                self._paused.set()

        self._stop.clear()
        self._pause.clear()
        self._bg = threading.Thread(target=loop, daemon=True)
        self._bg.start()

    def pause(self, timeout: float = 300.0) -> int:
        """Let the current round finish, keep all writers idle, and return the WAL position (a quiesce point)."""
        self._pause.set()
        if not self._paused.wait(timeout):
            raise TimeoutError("writers did not pause within %.0f s" % timeout)
        if self._bg_error:
            raise self._bg_error
        return self.quiesce()

    def resume(self) -> None:
        self._paused.clear()
        self._pause.clear()

    def stop(self) -> None:
        self._stop.set()
        self._pause.clear()
        if self._bg:
            self._bg.join(timeout=300)
        if self._bg_error:
            raise self._bg_error

    def close(self) -> None:
        if self._log is not None:
            self._log.close()
            self._log = None

    # ---------------------------------------------------------------- planning-only run (determinism checks)

    def plan(self, rounds: int) -> list:
        """The entries a run of `rounds` rounds would execute, without a database: per writer and driver step."""
        out = [{"type": "setup", "statements": self.setup_statements()}]
        for _ in range(rounds):
            for txns in self.plan_round():
                out += [{"type": "txn", "writer": t.writer, "round": t.round, "seq": t.seq, "shape": t.shape,
                         "end": t.end, "statements": t.statements} for t in txns]
            step = self.plan_driver_step()
            if step:
                out.append(dict(step, type="driver", round=self.round))
            self.round += 1
        return out


def read_log(path: Path) -> list:
    with Path(path).open(encoding="utf-8") as f:
        return [json.loads(line) for line in f if line.strip()]


def replay(path: Path, connect: Callable, on_quiesce: Optional[Callable[[dict], None]] = None) -> Counter:
    """Run a replay log again, in log order, on one connection. Returns counts per entry type."""
    seen: Counter = Counter()
    with connect() as c:
        for e in read_log(path):
            seen[e["type"]] += 1
            if e["type"] in ("setup", "txn", "driver"):
                for s in e["statements"]:
                    c.execute(s)
            elif e["type"] == "quiesce" and on_quiesce:
                on_quiesce(e)
    return seen


def per_writer_statements(entries: Iterable[dict]) -> dict:
    """{writer: [statements...]} from log or plan entries (interleaving removed), plus driver steps per round."""
    out: dict = {}
    for e in entries:
        if e["type"] == "txn":
            out.setdefault(e["writer"], []).append(e["statements"])
        elif e["type"] in ("driver", "setup"):
            out.setdefault(e["type"], []).append(e["statements"])
    return out
