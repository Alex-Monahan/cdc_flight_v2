"""Crash-runner adapter for the toy system under test (`toy_sut.py`)."""
import os
import random
import sys
import time

import duckdb

import toy_sut
from cdc_v2.testing import procs
from cdc_v2.testing.crash import Adapter

HERE = os.path.dirname(os.path.abspath(__file__))
SRC = os.path.join(os.path.dirname(os.path.dirname(HERE)), "src")


class ToyAdapter(Adapter):
    variant = "correct"

    def __init__(self):
        self.name = "toy-%s" % self.variant
        self.dir = self.source = self.dest = None
        self.next_id = 1

    def crash_points(self):
        return list(toy_sut.CORRECT_POINTS if self.variant == "correct" else toy_sut.WRONG_POINTS)

    def setup(self, workdir, seed):
        self.dir = str(workdir)
        os.makedirs(self.dir, exist_ok=True)
        self.source = os.path.join(self.dir, "source.tsv")
        self.dest = os.path.join(self.dir, "dest.duckdb")
        open(self.source, "w").close()
        self.next_id = 1

    def start(self, tag, env):
        env = dict(env)
        env["PYTHONPATH"] = SRC + os.pathsep + os.environ.get("PYTHONPATH", "")
        return procs.start([sys.executable, os.path.join(HERE, "toy_sut.py"), "--source", self.source, "--dest",
                            self.dest, "--variant", self.variant], env=env, name=tag,
                           log_path=os.path.join(self.dir, tag + ".log"))

    def workload(self, step, seed):
        rng = random.Random(seed * 1_000_003 + step)
        lines = []
        for _ in range(rng.randint(1, 5)):
            lines.append("%d\t%s\n" % (self.next_id, "%08x" % rng.getrandbits(32)))
            self.next_id += 1
        fd = os.open(self.source, os.O_WRONLY | os.O_APPEND)
        try:
            os.write(fd, "".join(lines).encode())
        finally:
            os.close(fd)

    def _read_dest(self, query):
        """Run a read-only query, waiting for a writer's file lock (bounded by the caller's deadline)."""
        con = duckdb.connect(self.dest, read_only=True)
        try:
            return con.execute(query).fetchall()
        finally:
            con.close()

    def position(self):
        try:
            return self._read_dest("SELECT pos FROM toy_position WHERE k = 1")[0][0]
        except (duckdb.Error, IndexError):
            return None

    def wait_caught_up(self, timeout):
        target = self.next_id - 1
        deadline = time.monotonic() + timeout
        last = None
        while time.monotonic() < deadline:
            last = self.position()
            if last is not None and last >= target:
                return
            time.sleep(0.02)
        raise procs.DeadlineExceeded("position %r, source has %d rows, after %.0f s" % (last, target, timeout))

    def check(self):
        source = dict(toy_sut.read_source(self.source))
        rows = self._read_dest("SELECT id, count(*), min(val), max(val) FROM toy_records GROUP BY id")
        pos = self.position()
        got = {r[0]: r for r in rows}
        missing = sorted(set(source) - set(got))
        extra = sorted(set(got) - set(source))
        dup = sorted(i for i, r in got.items() if r[1] > 1)
        wrong = sorted(i for i, r in got.items() if i in source and (r[2] != source[i] or r[3] != source[i]))
        ok = not (missing or extra or dup or wrong) and pos == len(source)
        reason = "" if ok else "missing %d, duplicated %d, extra %d, wrong %d, position %r of %d" % (
            len(missing), len(dup), len(extra), len(wrong), pos, len(source))
        return {"ok": ok, "reason": reason, "source_rows": len(source), "dest_ids": len(got),
                "missing": missing[:10], "duplicated": dup[:10], "extra": extra[:10], "wrong": wrong[:10],
                "position": pos}


class CorrectToy(ToyAdapter):
    variant = "correct"


class WrongToy(ToyAdapter):
    variant = "wrong"
