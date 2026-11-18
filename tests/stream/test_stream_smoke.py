from fakes import HB, K, N, dele, ins, keychange, upd


def test_a_few_transactions_reach_the_destination(rig):
    s = rig.slot
    s.txn([("rows", K, [ins(1, "a"), ins(2, "b")]), ("rows", N, [ins(10, "x"), ins(10, "x")])])
    s.txn([("rows", K, [upd(1, None, unchanged=(1,))]), ("rows", N, [dele(10, "x", keyless=True)])])
    s.txn([("rows", K, keychange(2, 3, "c"))])
    s.txn([("rows", HB, [ins("cdc_v2_slot", 1)])])
    last = s.txn([("truncate", N), ("rows", N, [ins(5, "y")])])
    with rig.running() as (comp, sup):
        rig.wait_applied(last.commit_lsn, sup=sup)
    rig.check_equal()
