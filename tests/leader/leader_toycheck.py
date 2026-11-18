"""The source file of the toy pipeline and the check of its destination (exactly once, never two writers).

The check does not rest on the fence it tests: it reads `toyl.writer_log`, which every data transaction of the toy
writes itself (epoch, run id, the positions it moved between, and `seq`, the order of commit on the stream lane).
"""
import json
import threading


class SourceWriter:
    def __init__(self, path):
        self.path = str(path)
        self.count = 0
        self._lock = threading.Lock()
        open(self.path, "ab").close()

    def add(self, n=1, mark="e"):
        with self._lock:
            lines = []
            for _ in range(n):
                self.count += 1
                lines.append(json.dumps({"seq": self.count, "payload": "%s-%d" % (mark, self.count)}))
            with open(self.path, "ab") as f:
                f.write(("\n".join(lines) + "\n").encode())
            return self.count


def read_events(path):
    out = []
    try:
        with open(path, "rb") as f:
            for line in f.read().splitlines():
                if line.strip():
                    out.append(json.loads(line)["payload"])
    except FileNotFoundError:
        pass
    return out


def read_state(raw):
    """Position, rows and writer log, read in one transaction (one snapshot)."""
    raw.execute("BEGIN")
    try:
        pos, n = raw.execute("SELECT pos, n FROM toyl.position").fetchone()
        rows = raw.execute("SELECT seq, payload, epoch, run_id FROM toyl.data ORDER BY seq, epoch").fetchall()
        log = raw.execute("SELECT epoch, run_id, from_pos, to_pos, seq FROM toyl.writer_log ORDER BY seq").fetchall()
    finally:
        raw.execute("ROLLBACK")
    return int(pos), int(n), [(int(a), b, int(c), d) for a, b, c, d in rows], \
        [(int(a), b, int(c), int(d), int(e)) for a, b, c, d, e in log]


def check(raw, source_path):
    """``{"ok", "caught_up", "pos", "n", "rows", "doubles", "commits", "epochs", "problems"}``.

    - exactly once: the data rows are the events 1 .. pos, each once, with the source's payload; n = pos;
    - never two writers: ordered by `seq`, each writer-log row starts where the one before ended, `seq` is unique,
      epochs never fall, a run writes under one epoch and an epoch belongs to one run, and every data row carries
      the epoch and run of the log row that covers it."""
    events = read_events(source_path)
    pos, n, rows, log = read_state(raw)
    problems = []
    seqs = [r[0] for r in rows]
    doubles = sorted({q for q in seqs if seqs.count(q) > 1})
    if doubles:
        problems.append("%d events applied more than once (first %r)" % (len(doubles), doubles[:3]))
    if pos > len(events):
        problems.append("position %d is beyond the source (%d events)" % (pos, len(events)))
    if n != pos:
        problems.append("counter %d differs from position %d" % (n, pos))
    if not doubles and [(r[0], r[1]) for r in rows] != list(enumerate(events[:pos], start=1)):
        problems.append("data rows differ from the events 1 to %d" % pos)
    last_to, last_epoch, run_epoch, epoch_run, seen_seq = 0, 0, {}, {}, set()
    cover = {}
    for epoch, run_id, from_pos, to_pos, seq in log:
        if seq in seen_seq:
            problems.append("seq %d twice in the writer log (two writers)" % seq)
        seen_seq.add(seq)
        if from_pos != last_to:
            problems.append("writer log: a transaction from %d follows one that ended at %d" % (from_pos, last_to))
        if epoch < last_epoch:
            problems.append("writer log: epoch %d after epoch %d (two writers)" % (epoch, last_epoch))
        if run_epoch.setdefault(run_id, epoch) != epoch:
            problems.append("run %s wrote under two epochs" % run_id[:8])
        if epoch_run.setdefault(epoch, run_id) != run_id:
            problems.append("epoch %d belongs to two runs" % epoch)
        for q in range(from_pos + 1, to_pos + 1):
            cover[q] = (epoch, run_id)
        last_to, last_epoch = to_pos, max(last_epoch, epoch)
    if last_to != pos:
        problems.append("writer log ends at %d, position is %d" % (last_to, pos))
    for seq, _payload, epoch, run_id in rows:
        if cover.get(seq) != (epoch, run_id):
            problems.append("row %d carries epoch %d of a run the writer log does not name" % (seq, epoch))
            break
    return {"ok": not problems, "caught_up": pos == len(events), "pos": pos, "n": n, "rows": len(rows),
            "events": len(events), "doubles": len(doubles), "commits": len(log),
            "epochs": sorted(epoch_run), "problems": problems[:10]}
