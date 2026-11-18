"""Seeded workloads for the scripted slot: keyed and keyless tables, unchanged TOAST values, key changes, deletes
and re-inserts of one key, transactions over several tables, empty transactions, heartbeat transactions,
truncates, and (optionally) a transaction larger than a group."""
from __future__ import annotations

import random

from fakes import HB, K, N, dele, ins, keychange, upd


def main(slot, seed=1, txns=40, large_rows=0, big_text=0):
    """A mixed workload. With `large_rows`, one transaction of that many inserts is in the middle (staging when the
    group's byte limit is small); `big_text` makes its values that long."""
    r = random.Random(seed)
    keyed = {}
    keyless = []
    next_id = [1]

    def new_id():
        next_id[0] += 1
        return next_id[0]

    last = None
    for i in range(txns):
        parts = []
        kind = r.random()
        if large_rows and i == txns // 2:
            recs = []
            for _ in range(large_rows):
                k = new_id()
                v = ("L%d" % k) * max(1, big_text // 4) if big_text else "L%d" % k
                keyed[k] = v
                recs.append(ins(k, v))
            last = slot.txn([("rows", K, recs)])
            continue
        if kind < 0.08:
            last = slot.txn([])                                    # empty: END with count 0
            continue
        if kind < 0.16:
            last = slot.txn([("rows", HB, [ins("cdc_v2_slot", i)])])   # heartbeat: not replicated
            continue
        if kind < 0.20 and keyless:
            parts.append(("truncate", N))
            keyless.clear()
        recs = []
        for _ in range(r.randint(1, 4)):
            op = r.random()
            if op < 0.4 or not keyed:
                k = new_id()
                keyed[k] = "v%d" % k
                recs.append(ins(k, keyed[k]))
            elif op < 0.6:
                k = r.choice(sorted(keyed))
                if r.random() < 0.5:
                    recs.append(upd(k, None, unchanged=(1,)))    # unchanged TOAST value
                else:
                    keyed[k] = "u%d_%d" % (k, i)
                    recs.append(upd(k, keyed[k]))
            elif op < 0.75:
                k = r.choice(sorted(keyed))
                del keyed[k]
                recs.append(dele(k))
                if r.random() < 0.5:                             # delete and re-insert of the same key
                    keyed[k] = "re%d" % k
                    recs.append(ins(k, keyed[k]))
            elif op < 0.9:
                k = r.choice(sorted(keyed))
                n = new_id()
                v = keyed.pop(k)
                keyed[n] = v
                recs.extend(keychange(k, n, v))
            else:
                k = r.choice(sorted(keyed))
                del keyed[k]
                recs.append(dele(k))
        parts.append(("rows", K, recs))
        if r.random() < 0.6:
            nrecs = []
            for _ in range(r.randint(1, 3)):
                op = r.random()
                if op < 0.5 or not keyless:
                    row = (r.randint(1, 4), "x%d" % r.randint(1, 3))      # duplicates on purpose
                    keyless.append(row)
                    nrecs.append(ins(*row))
                elif op < 0.75:
                    old = keyless.pop(r.randrange(len(keyless)))
                    nrecs.append(dele(*old, keyless=True))
                else:
                    j = r.randrange(len(keyless))
                    old = keyless[j]
                    if r.random() < 0.5:
                        new = (old[0] + 10, old[1])
                        keyless[j] = new
                        nrecs.append(upd(new[0], None, unchanged=(1,), old=old))
                    else:
                        new = (old[0], old[1] + "!")
                        keyless[j] = new
                        nrecs.append(upd(*new, old=old))
            parts.append(("rows", N, nrecs))
        if r.random() < 0.1:
            parts.append(("rows", HB, [ins("cdc_v2_slot", i)]))
        last = slot.txn(parts)
    return last
