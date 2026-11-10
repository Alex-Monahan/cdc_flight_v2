"""Q7 (SPEC 2.4, 2.5, R9): type machinery on MotherDuck — in-place widening and UNION columns.

Every check is run, recorded as OK (with its result) or ERROR (with the error class and message), and never stops the
script. Part T times the type changes on a 10M-row table (server side).

Run:  tools/with-env uv run --with pyarrow python spikes/md_semantics/types.py [--rows 10000000]
"""
from __future__ import annotations

import argparse
import time

from common import Evidence, connect, create_db, drop_db, err_class, tmp_dir

DB = "cdc_v2_spike_md_types"

U2 = "UNION(i BIGINT, f DOUBLE)"
U3 = "UNION(i BIGINT, f DOUBLE, s VARCHAR)"
UDEC = "UNION(n DECIMAL(38,10), s VARCHAR)"


class Checks:
    def __init__(self, ev: Evidence, con):
        self.ev, self.con, self.results = ev, con, {}

    def run(self, name: str, stmts, fetch: str | None = None, expect=None):
        """Execute stmts (str or list); optionally fetch a query and compare with expect."""
        if isinstance(stmts, str):
            stmts = [stmts]
        try:
            for s in stmts:
                self.con.execute(s)
            out = None
            if fetch:
                out = self.con.execute(fetch).fetchall()
            status = "OK"
            if expect is not None and out != expect:
                status = "UNEXPECTED"
            self.results[name] = {"status": status, "result": out}
            self.ev.log(f"   [{status}] {name}: {out if out is not None else ''}")
        except Exception as e:
            try:
                self.con.execute("ROLLBACK")
            except Exception:
                pass
            self.results[name] = {"status": "ERROR", "error": err_class(e)}
            self.ev.log(f"   [ERROR] {name}: {err_class(e)}")
        return self.results[name]

    def coltype(self, table: str, col: str) -> str:
        return self.con.execute("SELECT data_type FROM duckdb_columns() WHERE database_name = current_database() "
                                "AND table_name = ? AND column_name = ?", [table, col]).fetchone()[0]


def widening(c: Checks) -> None:
    c.ev.log("== 1. in-place widening inside a transaction with DML")
    for name, old, new, val_old, val_new in [
        ("int_to_bigint", "INTEGER", "BIGINT", "2147483647", "9223372036854775807"),
        ("float_to_double", "FLOAT", "DOUBLE", "1.5", "1.0000000000000002"),
        ("decimal_10_2_to_38_2", "DECIMAL(10,2)", "DECIMAL(38,2)", "12345678.91", "123456789012345678901234567890.12"),
        ("decimal_18_4_to_38_10", "DECIMAL(18,4)", "DECIMAL(38,10)", "12345678901234.5678",
         "1234567890123456789012345678.0123456789"),
        ("varchar_n_to_varchar", "VARCHAR(5)", "VARCHAR", "'abcde'", "'abcdefghij'"),
        ("smallint_to_integer", "SMALLINT", "INTEGER", "32767", "2147483647"),
        ("timestamp_to_timestamptz", "TIMESTAMP", "TIMESTAMPTZ", "TIMESTAMP '2020-01-01 00:00:00'",
         "TIMESTAMPTZ '2021-01-01 00:00:00+00'"),
    ]:
        t = f"w_{name}"
        c.run(f"{name}: ALTER ... TYPE {new} + INSERT + UPDATE in one transaction, committed",
              [f"CREATE OR REPLACE TABLE {t} (id INTEGER, v {old})", f"INSERT INTO {t} VALUES (1, {val_old})",
               "BEGIN", f"ALTER TABLE {t} ALTER COLUMN v TYPE {new}", f"INSERT INTO {t} VALUES (2, {val_new})",
               f"UPDATE {t} SET id = id + 10 WHERE id = 1", "COMMIT"],
              fetch=f"SELECT id, v::VARCHAR, typeof(v) FROM {t} ORDER BY id")
    # PK column widening
    c.run("int_to_bigint on a PRIMARY KEY column",
          ["CREATE OR REPLACE TABLE w_pk (id INTEGER PRIMARY KEY, v VARCHAR)", "INSERT INTO w_pk VALUES (1, 'a')",
           "ALTER TABLE w_pk ALTER COLUMN id TYPE BIGINT"], fetch="SELECT typeof(id) FROM w_pk")
    c.run("int_to_bigint on a column used by a non-PK index (CREATE INDEX)",
          ["CREATE OR REPLACE TABLE w_ix (id INTEGER, v VARCHAR)", "CREATE INDEX w_ix_i ON w_ix(id)",
           "ALTER TABLE w_ix ALTER COLUMN id TYPE BIGINT"], fetch="SELECT typeof(id) FROM w_ix")
    c.run("rollback of ALTER TYPE restores the old type",
          ["CREATE OR REPLACE TABLE w_rb (id INTEGER, v INTEGER)", "INSERT INTO w_rb VALUES (1, 1)", "BEGIN",
           "ALTER TABLE w_rb ALTER COLUMN v TYPE BIGINT", "INSERT INTO w_rb VALUES (2, 9223372036854775807)",
           "ROLLBACK"], fetch="SELECT count(*), any_value(typeof(v)) FROM w_rb")


def unions(c: Checks) -> None:
    c.ev.log("== 2. ALTER ... TYPE UNION(old, new), inside a transaction with DML")
    c.run("bigint -> UNION(i BIGINT, f DOUBLE), implicit cast, then insert a DOUBLE, committed",
          ["CREATE OR REPLACE TABLE u1 (id INTEGER, v BIGINT)", "INSERT INTO u1 VALUES (1, 42), (2, -7)", "BEGIN",
           f"ALTER TABLE u1 ALTER COLUMN v TYPE {U2}", "INSERT INTO u1 VALUES (3, 2.5::DOUBLE)",
           "UPDATE u1 SET v = 1e300::DOUBLE WHERE id = 2", "COMMIT"],
          fetch="SELECT id, v::VARCHAR, union_tag(v) FROM u1 ORDER BY id")
    c.run("bigint -> UNION with explicit USING union_value(i := v)",
          ["CREATE OR REPLACE TABLE u1b (id INTEGER, v BIGINT)", "INSERT INTO u1b VALUES (1, 42)",
           f"ALTER TABLE u1b ALTER COLUMN v TYPE {U2} USING union_value(i := v)"],
          fetch="SELECT v::VARCHAR, union_tag(v) FROM u1b")
    c.run("double -> UNION(f DOUBLE, i BIGINT) (float column receives a bigint)",
          ["CREATE OR REPLACE TABLE u1c (id INTEGER, v DOUBLE)", "INSERT INTO u1c VALUES (1, 0.1)",
           "ALTER TABLE u1c ALTER COLUMN v TYPE UNION(f DOUBLE, i BIGINT)",
           "INSERT INTO u1c VALUES (2, 9007199254740993::BIGINT)"],
          fetch="SELECT id, v::VARCHAR, union_tag(v) FROM u1c ORDER BY id")
    c.run("text -> UNION(s VARCHAR, i INTEGER)",
          ["CREATE OR REPLACE TABLE u2 (id INTEGER, v VARCHAR)", "INSERT INTO u2 VALUES (1, 'abc'), (2, '17')",
           "BEGIN", "ALTER TABLE u2 ALTER COLUMN v TYPE UNION(s VARCHAR, i INTEGER)",
           "INSERT INTO u2 VALUES (3, 17::INTEGER)", "COMMIT"],
          fetch="SELECT id, v::VARCHAR, union_tag(v) FROM u2 ORDER BY id")
    c.run("DECIMAL(38,10) -> UNION(n DECIMAL(38,10), s VARCHAR) and a NaN / huge numeric as text",
          ["CREATE OR REPLACE TABLE u3 (id INTEGER, v DECIMAL(38,10))", "INSERT INTO u3 VALUES (1, 3.1415926535)",
           "BEGIN", f"ALTER TABLE u3 ALTER COLUMN v TYPE {UDEC}",
           "INSERT INTO u3 VALUES (2, 'NaN'::VARCHAR), (3, '1e400'::VARCHAR), (4, 12.5::DECIMAL(38,10))",
           "COMMIT"],
          fetch="SELECT id, v::VARCHAR, union_tag(v) FROM u3 ORDER BY id")
    c.run("second change: add a member to an existing union (UNION(i,f) -> UNION(i,f,s))",
          ["BEGIN", f"ALTER TABLE u1 ALTER COLUMN v TYPE {U3}", "INSERT INTO u1 VALUES (4, 'text'::VARCHAR)",
           "COMMIT"],
          fetch="SELECT id, v::VARCHAR, union_tag(v) FROM u1 ORDER BY id")
    c.run("rollback of ALTER ... TYPE UNION restores the old type and rows",
          ["CREATE OR REPLACE TABLE u_rb (id INTEGER, v BIGINT)", "INSERT INTO u_rb VALUES (1, 1)", "BEGIN",
           f"ALTER TABLE u_rb ALTER COLUMN v TYPE {U2}", "INSERT INTO u_rb VALUES (2, 0.5::DOUBLE)", "ROLLBACK"],
          fetch="SELECT count(*), any_value(typeof(v)) FROM u_rb")
    c.ev.log("   column type of u1.v now:", c.coltype("u1", "v"))


def union_behaviour(c: Checks) -> None:
    c.ev.log("== 3. UNION values in DML, keys, joins, comparisons, checksums")
    c.con.execute(f"CREATE OR REPLACE TABLE ub (k {U2}, v VARCHAR)")
    c.con.execute("INSERT INTO ub VALUES (1::BIGINT, 'int one'), (1.0::DOUBLE, 'double one'), "
                  "(2.5::DOUBLE, 'double 2.5'), (NULL, 'null')")
    c.con.execute("CREATE OR REPLACE TABLE stage_d (k DOUBLE, v VARCHAR)")
    c.con.execute("INSERT INTO stage_d VALUES (7.25, 'staged double')")
    c.con.execute("CREATE OR REPLACE TABLE stage_i (k BIGINT, v VARCHAR)")
    c.con.execute("INSERT INTO stage_i VALUES (8, 'staged bigint')")
    c.run("INSERT ... SELECT from a DOUBLE staging column into the UNION column", "INSERT INTO ub SELECT * FROM stage_d",
          fetch="SELECT k::VARCHAR, union_tag(k) FROM ub WHERE v = 'staged double'")
    c.run("INSERT ... SELECT from a BIGINT staging column into the UNION column", "INSERT INTO ub SELECT * FROM stage_i",
          fetch="SELECT k::VARCHAR, union_tag(k) FROM ub WHERE v = 'staged bigint'")
    c.run("UPDATE sets a value of the other member", "UPDATE ub SET k = 99::BIGINT WHERE v = 'double 2.5'",
          fetch="SELECT k::VARCHAR, union_tag(k) FROM ub WHERE v = 'double 2.5'")
    c.run("WHERE k = 1 (plain integer literal) matches which rows", [],
          fetch="SELECT v FROM ub WHERE k = 1 ORDER BY v")
    c.run("WHERE k = 1::BIGINT matches which rows", [], fetch="SELECT v FROM ub WHERE k = 1::BIGINT ORDER BY v")
    c.run("WHERE k = 1.0::DOUBLE matches which rows", [], fetch="SELECT v FROM ub WHERE k = 1.0::DOUBLE ORDER BY v")
    c.run(f"k = union_value(i := 1) equality (union vs union, same tag)", [],
          fetch=f"SELECT v FROM ub WHERE k = union_value(i := 1::BIGINT)::{U2} ORDER BY v")
    c.run("ORDER BY a union column (across members)", [], fetch="SELECT k::VARCHAR FROM ub ORDER BY k NULLS LAST")
    c.run("GROUP BY / DISTINCT a union column: is int 1 the same group as double 1.0?", [],
          fetch="SELECT count(DISTINCT k) FROM ub WHERE v IN ('int one', 'double one')")
    c.run("hash(): union BIGINT 1 vs plain BIGINT 1 vs union DOUBLE 1.0", [],
          fetch=f"SELECT hash(1::BIGINT) = hash(1::BIGINT::{U2}), hash(1::BIGINT::{U2}) = hash(1.0::DOUBLE::{U2})")
    c.run("hash() stable across server and client (same value hashed remotely and locally)", [],
          fetch="SELECT hash(k) FROM ub WHERE v = 'int one'")
    local = c.con.execute(f"SELECT hash(1::BIGINT::{U2})").fetchone()[0]
    c.ev.log("      local hash of union BIGINT 1:", local)
    c.run("join on union columns", [f"CREATE OR REPLACE TABLE ub2 (k {U2}, w VARCHAR)",
                                    "INSERT INTO ub2 VALUES (1::BIGINT, 'j-int'), (1.0::DOUBLE, 'j-dbl')"],
          fetch="SELECT a.v, b.w FROM ub a JOIN ub2 b ON a.k = b.k ORDER BY 1, 2")
    c.run("PRIMARY KEY on a UNION column", [f"CREATE OR REPLACE TABLE upk (k {U2} PRIMARY KEY, v VARCHAR)",
                                            "INSERT INTO upk VALUES (1::BIGINT, 'a'), (1.0::DOUBLE, 'b')"],
          fetch="SELECT count(*) FROM upk")
    c.run("ALTER a PRIMARY KEY column to UNION", ["CREATE OR REPLACE TABLE upk2 (k BIGINT PRIMARY KEY, v VARCHAR)",
                                                  "INSERT INTO upk2 VALUES (1, 'a')",
                                                  f"ALTER TABLE upk2 ALTER COLUMN k TYPE {U2}"],
          fetch="SELECT typeof(k) FROM upk2")
    c.run("MERGE keyed on a union column", [
        f"CREATE OR REPLACE TABLE mt (k {U2}, v VARCHAR)", "INSERT INTO mt VALUES (1::BIGINT, 'old')",
        f"CREATE OR REPLACE TABLE ms (k {U2}, v VARCHAR)",
        "INSERT INTO ms VALUES (1::BIGINT, 'new'), (2.5::DOUBLE, 'ins')",
        "MERGE INTO mt USING ms ON mt.k = ms.k WHEN MATCHED THEN UPDATE SET v = ms.v "
        "WHEN NOT MATCHED THEN INSERT VALUES (ms.k, ms.v)"],
          fetch="SELECT k::VARCHAR, v FROM mt ORDER BY v")
    c.run("DELETE ... USING keyed on a union column",
          "DELETE FROM mt USING ms WHERE mt.k = ms.k AND ms.v = 'ins'", fetch="SELECT count(*) FROM mt")
    c.run("canonical text for an oracle: k::VARCHAR per member", [],
          fetch="SELECT union_tag(k), k::VARCHAR FROM ub ORDER BY v")


def arrow_parquet(c: Checks) -> None:
    c.ev.log("== 4. UNION through Arrow and Parquet")
    try:
        import pyarrow as pa  # noqa: F401
    except ImportError:
        c.ev.log("   pyarrow not installed: run with `uv run --with pyarrow`")
        return
    c.con.execute(f"CREATE OR REPLACE TABLE ua (id INTEGER, k {U3})")
    c.con.execute("INSERT INTO ua VALUES (1, 1::BIGINT), (2, 2.5::DOUBLE), (3, 'x'::VARCHAR), (4, NULL)")
    try:
        tbl = c.con.execute("SELECT * FROM ua ORDER BY id").fetch_arrow_table()
        c.ev.log("   [OK] fetch as Arrow: arrow type of k =", tbl.schema.field("k").type)
        c.results["arrow_fetch"] = {"status": "OK", "type": str(tbl.schema.field("k").type)}
        c.con.register("arrow_in", tbl)
        c.run("INSERT INTO <md table> SELECT * FROM <arrow table with a union column>",
              [f"CREATE OR REPLACE TABLE ua2 (id INTEGER, k {U3})", "INSERT INTO ua2 SELECT * FROM arrow_in"],
              fetch="SELECT id, k::VARCHAR, union_tag(k) FROM ua2 ORDER BY id")
        c.con.unregister("arrow_in")
    except Exception as e:
        c.results["arrow_fetch"] = {"status": "ERROR", "error": err_class(e)}
        c.ev.log("   [ERROR] Arrow:", err_class(e))
    path = tmp_dir() / "union_test.parquet"
    c.run("COPY (SELECT ... union column) TO local Parquet, read back",
          f"COPY (SELECT * FROM ua ORDER BY id) TO '{path}' (FORMAT parquet)",
          fetch=f"SELECT id, k::VARCHAR, typeof(k) FROM read_parquet('{path}') ORDER BY id")
    c.run("INSERT INTO union table SELECT FROM that Parquet file",
          [f"CREATE OR REPLACE TABLE ua3 (id INTEGER, k {U3})",
           f"INSERT INTO ua3 SELECT * FROM read_parquet('{path}')"],
          fetch="SELECT id, k::VARCHAR, union_tag(k) FROM ua3 ORDER BY id")
    path.unlink(missing_ok=True)


def timing(c: Checks, rows: int) -> None:
    c.ev.log(f"== 5. timing of type changes on a {rows:,}-row table (server side), each in its own transaction "
             "together with a 1,000-row INSERT")
    con = c.con
    con.execute(f"CREATE OR REPLACE TABLE big AS SELECT range::INTEGER AS id, (range % 1000000)::INTEGER AS a, "
                f"(range * 0.5)::FLOAT AS f, (range / 7)::DECIMAL(18,4) AS d, md5(range::VARCHAR) AS s "
                f"FROM range({rows})")
    times = {}
    for name, stmts in [
        ("INTEGER -> BIGINT", ["ALTER TABLE big ALTER COLUMN a TYPE BIGINT",
                               "INSERT INTO big (id, a) SELECT range, 9223372036854775807 FROM range(1000)"]),
        ("FLOAT -> DOUBLE", ["ALTER TABLE big ALTER COLUMN f TYPE DOUBLE",
                             "INSERT INTO big (id, f) SELECT range, 0.1 FROM range(1000)"]),
        ("DECIMAL(18,4) -> DECIMAL(38,10)", ["ALTER TABLE big ALTER COLUMN d TYPE DECIMAL(38,10)",
                                             "INSERT INTO big (id, d) SELECT range, 1e20 FROM range(1000)"]),
        ("DECIMAL(38,10) -> UNION(n DECIMAL(38,10), s VARCHAR)",
         [f"ALTER TABLE big ALTER COLUMN d TYPE {UDEC}",
          "INSERT INTO big (id, d) SELECT range, 'NaN'::VARCHAR FROM range(1000)"]),
        ("BIGINT -> UNION(i BIGINT, f DOUBLE)", [f"ALTER TABLE big ALTER COLUMN a TYPE {U2}",
                                                 "INSERT INTO big (id, a) SELECT range, 0.5::DOUBLE FROM range(1000)"]),
        ("UNION(i,f) -> UNION(i,f,s) (add a member)",
         [f"ALTER TABLE big ALTER COLUMN a TYPE {U3}",
          "INSERT INTO big (id, a) SELECT range, 'x'::VARCHAR FROM range(1000)"]),
        ("VARCHAR -> UNION(s VARCHAR, i INTEGER)",
         ["ALTER TABLE big ALTER COLUMN s TYPE UNION(s VARCHAR, i INTEGER)",
          "INSERT INTO big (id, s) SELECT range, 5::INTEGER FROM range(1000)"]),
    ]:
        t0 = time.time()
        try:
            con.execute("BEGIN")
            for s in stmts:
                con.execute(s)
            con.execute("COMMIT")
            times[name] = round(time.time() - t0, 2)
            c.ev.log(f"   {name}: {times[name]} s")
        except Exception as e:
            try:
                con.execute("ROLLBACK")
            except Exception:
                pass
            times[name] = "ERROR " + err_class(e)
            c.ev.log(f"   {name}: ERROR {err_class(e)}")
    n, chk = con.execute("SELECT count(*), sum(hash(a)) FROM big").fetchone()
    c.ev.log(f"   rows after: {n:,}; final types:",
             con.execute("SELECT column_name, data_type FROM duckdb_columns() WHERE database_name = "
                         "current_database() AND table_name = 'big' ORDER BY column_index").fetchall())
    c.results["timing_s"] = times
    con.execute("DROP TABLE big")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--rows", type=int, default=10_000_000)
    ap.add_argument("--skip-timing", action="store_true")
    a = ap.parse_args()
    ev = Evidence("types")
    create_db(DB)
    con = connect(DB)
    c = Checks(ev, con)
    try:
        widening(c)
        unions(c)
        union_behaviour(c)
        arrow_parquet(c)
        if not a.skip_timing:
            timing(c, a.rows)
    finally:
        ev.put("checks", c.results)
        con.close()
        drop_db(DB)
        ev.log("database dropped")
        ev.write()


if __name__ == "__main__":
    main()
