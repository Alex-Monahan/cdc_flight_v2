"""The oracle: decides whether Postgres source tables and MotherDuck destination tables are equal (SPEC §3, C8).

    from cdc_v2.oracle import Source, Destination, OracleConfig, compare
    result = compare(source, Destination("cdc_v2_t_..."), OracleConfig(tables=[...]), position=Q, slot_name=...)
    result.equal, result.to_dict()

Equality is defined per type by the canonical forms in `canonical.py` (documented in docs/oracle.md). This package
shares no conversion or apply code with the pipeline, and the pipeline never imports it. It asks one question of
the pipeline's library: which server versions export defective snapshots on a standby
(`cdc_v2.pg.cut.export_defect`, the one place that knows them), so that its own source reader keeps the same rule.
"""
from .canonical import FALLBACK, RULES, MdType, PgColumnType, parse_md_type, py_canon, rule_for
from .comparison import (Destination, OracleConfig, OracleError, PositionNotReached, Result, Source, TableResult,
                         compare)
from .position import AppliedPosition, as_position, stand_in_applied_position, wait_for_applied

__all__ = ["RULES", "FALLBACK", "MdType", "PgColumnType", "parse_md_type", "py_canon", "rule_for", "Destination",
           "OracleConfig", "OracleError", "PositionNotReached", "Result", "Source", "TableResult", "compare",
           "AppliedPosition", "as_position", "stand_in_applied_position", "wait_for_applied"]
