"""The oracle: decides whether Postgres source tables and MotherDuck destination tables are equal (SPEC §3).

    from cdc_v2.oracle import Source, Destination, compare
    result = compare(Source(host="localhost", port=55432, dbname=..., user=..., password=...),
                     Destination("cdc_v2_t_..."))
    result.equal, result.to_dict()

Equality is defined per type by the canonical forms in `canonical.py` (documented in docs/oracle.md). This package
imports nothing from the pipeline.
"""
from .canonical import RULES, FALLBACK, MdType, PgColumnType, parse_md_type, py_canon, rule_for
from .comparison import Destination, OracleConfig, OracleError, Result, Source, TableResult, compare
from .position import (PositionTimeout, applied_lsn_from_stream_state, format_lsn, parse_lsn, wait_for_applied)

__all__ = ["RULES", "FALLBACK", "MdType", "PgColumnType", "parse_md_type", "py_canon", "rule_for", "Destination",
           "OracleConfig", "OracleError", "Result", "Source", "TableResult", "compare", "PositionTimeout",
           "applied_lsn_from_stream_state", "format_lsn", "parse_lsn", "wait_for_applied"]
