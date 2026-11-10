"""Test-side control of run-time fault sites (`cdc_v2.faultsites`).

    fc = FaultControl(tmp_path / "faults")
    proc = procs.start([...], env=fc.env(tag="a"))
    fc.raise_at("md.commit", "ConnectionError", hits=2)
    ...
    assert fc.fired(site="md.commit")          # the fault log proves the fault was in effect
    fc.clear()

The control file is replaced atomically, and every write changes the file's inode and modification time, so a running
process sees the change at its next call of the site.
"""
import json
import os
import time
import uuid

from cdc_v2 import faultsites


class FaultControl:
    def __init__(self, directory, name="faults"):
        os.makedirs(directory, exist_ok=True)
        self.path = os.path.join(str(directory), name + ".json")
        self.log_path = os.path.join(str(directory), name + ".log.jsonl")
        self.rules = []
        self._last_mtime = None
        self._n = 0
        self._write()

    def env(self, tag=""):
        """Environment variables for a process that should obey this control file."""
        return {faultsites.ENV_FILE: self.path, faultsites.ENV_LOG: self.log_path, faultsites.ENV_TAG: tag}

    def _write(self):
        doc = {"rules": self.rules}
        faultsites.validate_rules(doc)
        tmp = "%s.%s.tmp" % (self.path, uuid.uuid4().hex[:8])
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(doc, f)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, self.path)
        st = os.stat(self.path)
        if self._last_mtime is not None and st.st_mtime_ns <= self._last_mtime:
            ns = self._last_mtime + 1000
            os.utime(self.path, ns=(ns, ns))
            st = os.stat(self.path)
        self._last_mtime = st.st_mtime_ns

    def _id(self, rid):
        if rid:
            return rid
        self._n += 1
        return "r%d" % self._n

    def add(self, site, action, rid=None, **fields):
        """Append one rule; returns its id. ``window=(start_offset_s, end_offset_s)`` is relative to now."""
        window = fields.pop("window", None)
        rule = {"id": self._id(rid), "site": site, "action": action}
        if window is not None:
            now = time.time()
            rule["start"], rule["end"] = now + window[0], now + window[1]
        rule.update({k: v for k, v in fields.items() if v is not None})
        self.rules.append(rule)
        try:
            self._write()
        except Exception:
            self.rules.pop()
            raise
        return rule["id"]

    def raise_at(self, site, exception=None, message=None, hits=None, skip=None, window=None, tag=None, rid=None):
        return self.add(site, "raise", rid=rid, exception=exception, message=message, hits=hits, skip=skip,
                        window=window, tag=tag)

    def delay_at(self, site, seconds, hits=None, skip=None, window=None, tag=None, rid=None):
        return self.add(site, "delay", rid=rid, seconds=seconds, hits=hits, skip=skip, window=window, tag=tag)

    def block_at(self, site, seconds=None, hits=None, skip=None, window=None, tag=None, rid=None):
        return self.add(site, "block", rid=rid, seconds=seconds, hits=hits, skip=skip, window=window, tag=tag)

    def remove(self, rid):
        self.rules = [r for r in self.rules if r["id"] != rid]
        self._write()

    def clear(self):
        self.rules = []
        self._write()

    def log(self):
        out = []
        try:
            with open(self.log_path, encoding="utf-8") as f:
                for line in f:
                    if line.endswith("\n"):
                        try:
                            out.append(json.loads(line))
                        except ValueError:
                            pass
        except FileNotFoundError:
            pass
        return out

    def fired(self, site=None, rule=None, action=None):
        """Fault log entries of faults that fired (not 'unblock' or error lines), filtered."""
        return [e for e in self.log() if "site" in e and e.get("action") in faultsites.ACTIONS
                and (site is None or e["site"] == site) and (rule is None or e.get("rule") == rule)
                and (action is None or e.get("action") == action)]
