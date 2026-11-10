#!/usr/bin/env python3
"""Q3 supplement: is billing granularity or a minimum charge visible to a SQL connection?

    tools/with-env .venv/bin/python tools/spike/run_semantics_billing.py

Lists the names of the MotherDuck SQL functions (duckdb_functions(), names only; no database is read) and flags
any whose name mentions billing, usage, cost, credit or compute. Evidence: billing_visibility.json.
"""
import duckdb

import run_semantics_common as c

KEYWORDS = ("bill", "usage", "cost", "credit", "compute", "invoice", "price", "meter")


def main():
    con = duckdb.connect("md:%s?attach_mode=single" % c.DB)
    names = sorted({r[0] for r in con.execute(
        "SELECT function_name FROM duckdb_functions() WHERE starts_with(upper(function_name), 'MD_')").fetchall()})
    flagged = [n for n in names if any(k in n.lower() for k in KEYWORDS)]
    c.save("billing_visibility", {
        "md_functions": names, "functions_matching_billing_keywords": flagged, "keywords": list(KEYWORDS),
        "documented_md_information_schema_views": ["DATABASES", "OWNED_SHARES", "SHARED_WITH_ME",
                                                   "DATABASE_SNAPSHOTS", "STORAGE_INFO", "RECENT_QUERIES",
                                                   "QUERY_HISTORY"],
        "sources": ["https://motherduck.com/docs/about-motherduck/billing/flight-sizes",
                    "https://motherduck.com/docs/sql-reference/motherduck-sql-reference/md_information_schema/"
                    "introduction/"],
        "note": "MD_INFORMATION_SCHEMA itself was not queried (PLAN §9: only cdc_v2_* databases are touched); its "
                "documented views were read from the documentation."})


if __name__ == "__main__":
    main()
