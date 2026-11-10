"""Oracle rate on a lineitem-width table (T1, opt-in benchmark).

    CDC_V2_ORACLE_BENCH_ROWS=5000000 tools/with-env uv run pytest -m t1 tests/t1/test_oracle_rate_t1.py

The same rows are generated on both servers by SQL (no upload): Postgres with generate_series, MotherDuck with
range(). The oracle then compares the equal tables (checksum pass only), and again after one row was changed on the
destination (checksum pass plus the row pass for one bucket). Laptop numbers over the public internet; the source
is the local primary.
"""
import os
import resource
import time

import pytest

from cdc_v2.oracle import OracleConfig, compare
from cdc_v2.testing import datasets

pytestmark = [pytest.mark.t1, pytest.mark.timeout(3600)]
ROWS = int(os.environ.get("CDC_V2_ORACLE_BENCH_ROWS", "0"))


@pytest.mark.skipif(ROWS <= 0, reason="set CDC_V2_ORACLE_BENCH_ROWS to run the oracle benchmark")
def test_oracle_rate(source_db, dest_db, evidence):
    t0 = time.monotonic()
    source_db.sql(datasets.BIG_DDL.split(";")[0])
    source_db.sql(datasets.BIG_DDL.split(";")[1])
    step = 1_000_000
    for start in range(1, ROWS + 1, step):
        source_db.sql(datasets.lineitem_insert_sql("other.big", start, min(step, ROWS - start + 1)))
    source_db.sql("VACUUM ANALYZE other.big")
    t_src = time.monotonic() - t0
    pg_bytes = source_db.sql("SELECT pg_total_relation_size('other.big')")[0][0]

    t0 = time.monotonic()
    c = dest_db.conn
    c.execute("CREATE SCHEMA other")
    c.execute(datasets.lineitem_md_ddl("other.big"))
    c.execute(datasets.lineitem_md_insert_sql("other.big", 1, ROWS))
    t_dst = time.monotonic() - t0

    cfg = OracleConfig()                                  # defaults: 100,000 rows per bucket, 4 source workers
    r1 = compare(source_db.source(), dest_db.destination(), cfg)
    assert r1.equal, r1.summary()

    c.execute("UPDATE other.big SET l_extendedprice = l_extendedprice + 0.01 WHERE l_orderkey = %d" % (ROWS // 2))
    r2 = compare(source_db.source(), dest_db.destination(), cfg)
    t2 = r2.tables[0]
    assert not r2.equal and (t2.missing_rows, t2.extra_rows, t2.different_rows) == (0, 0, 1)
    assert t2.differing_buckets == 1 and t2.examined_buckets == 1

    rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss       # bytes on macOS
    evidence("p3/oracle").write_json("rate", {
        "what": "oracle rate on a TPC-H lineitem-width table (16 columns), laptop, source = local primary, "
                "destination = MotherDuck over the public internet",
        "rows": ROWS, "source_table_bytes": pg_bytes,
        "setup_seconds": {"postgres_generate": round(t_src, 1), "motherduck_generate": round(t_dst, 1)},
        "equal": {"seconds": r1.seconds, "rows_per_second": r1.rows_per_second, "buckets": r1.tables[0].buckets},
        "one_row_changed": {"seconds": r2.seconds, "rows_per_second": r2.rows_per_second,
                            "differing_buckets": t2.differing_buckets, "sample": t2.sample["different"]},
        "client_peak_rss_mb": round(rss / (1024 * 1024 if rss > 10 ** 7 else 1024), 1),
        "config": {"chunk_rows": cfg.chunk_rows, "source_workers": cfg.source_workers},
        "machine": {"cpus": os.cpu_count()}})
