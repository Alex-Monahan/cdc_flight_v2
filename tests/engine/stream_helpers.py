"""Turn engine frames into transactions for the tests."""
import os
import subprocess
import time

from cdc_v2.deadline import Deadline, DeadlineExceeded
from cdc_v2.stream import frames as F

LONG = os.environ.get("CDC_V2_E_LONG") == "1"      # the long variants of the engine tests


class Txn:
    def __init__(self, begin):
        self.begin = begin
        self.rows = []          # (table, row dict)
        self.truncates = []
        self.messages = []
        self.end = None
        self.records = []       # every record in seq order: ("row", table, row) / ("truncate", ...) / ("message", ...)

    @property
    def xid(self):
        return self.begin.positions.get("txId")

    def __repr__(self):
        return "Txn(xid=%s rows=%d end=%s)" % (self.xid, len(self.rows), self.end and self.end.ack_position)


class Collector:
    """Reads frames and keeps transactions, schemas, heartbeats and non-transactional messages."""

    def __init__(self, engine):
        self.engine = engine
        self.txns = []
        self.schemas = {}               # (schema, table, version) -> Schema
        self.schema_log = []            # Schema frames in arrival order, with the seq of the next record
        self.heartbeats = []
        self.loose_messages = []        # non-transactional messages
        self.frames = []
        self.current = None
        self.last_seq = -1
        self.batch_ends = 0
        self.keep_frames = True

    def _seq(self, s):
        assert s > self.last_seq, "seq not increasing: %d after %d" % (s, self.last_seq)
        self.last_seq = s

    def feed(self, f):
        if self.keep_frames:
            self.frames.append(f)
        if isinstance(f, F.Schema):
            self.schemas[(f.schema, f.table, f.version)] = f
            self.schema_log.append((self.last_seq, f))
        elif isinstance(f, F.Begin):
            assert self.current is None, "BEGIN inside a transaction"
            self._seq(f.seq)
            self.current = Txn(f)
        elif isinstance(f, F.Rows):
            assert self.current is not None, "ROWS outside a transaction"
            for row in f.batch.to_pylist():
                self._seq(row["seq"])
                row["_version"] = f.version
                self.current.rows.append((f.table, row))
                self.current.records.append(("row", f.table, row))
        elif isinstance(f, F.Truncate):
            self._seq(f.seq)
            self.current.truncates.append(f)
            self.current.records.append(("truncate", f.table, f))
        elif isinstance(f, F.Message):
            self._seq(f.seq)
            if f.transactional:
                self.current.messages.append(f)
                self.current.records.append(("message", f.prefix, f))
            else:
                self.loose_messages.append(f)
        elif isinstance(f, F.End):
            self._seq(f.seq)
            assert self.current is not None
            self.current.end = f
            self.txns.append(self.current)
            self.current = None
        elif isinstance(f, F.Heartbeat):
            self._seq(f.seq)
            self.heartbeats.append(f)
        elif isinstance(f, F.BatchEnd):
            self.batch_ends += 1

    def run(self, until, timeout=60, step=0.5):
        """Read until `until(self)` is true; raises DeadlineExceeded."""
        end = Deadline.after(timeout)
        while not until(self):
            if end.expired:
                raise DeadlineExceeded("collector condition not reached; have %d transactions" % len(self.txns))
            for f in self.engine.frames(end.within(step)):
                self.feed(f)
                if until(self):
                    return self
        return self

    def rows_of(self, table):
        return [r for t in self.txns for (tb, r) in t.rows if tb == table]


def rss_kb(pid):
    out = subprocess.run(["ps", "-o", "rss=", "-p", str(pid)], capture_output=True, text=True).stdout.strip()
    return int(out) if out else None


def wait_for(fn, timeout=30, interval=0.1, what="condition"):
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        last = fn()
        if last:
            return last
        time.sleep(interval)
    raise AssertionError("timed out after %ss waiting for %s (last %r)" % (timeout, what, last))


def lsn(text):
    from cdc_v2 import lsn as L
    return L.parse(text) if text else None


EVIDENCE = os.environ.get("CDC_V2_E_EVIDENCE")


def evidence(name, obj):
    """Write evidence when CDC_V2_E_EVIDENCE names a directory (redacted by write_evidence)."""
    if EVIDENCE:
        from cdc_v2.testing.harness_evidence import write_evidence
        write_evidence(os.path.join(EVIDENCE, name), obj)
