"""Test only: narrowing a standby cut's ``unknown`` by the start of the commit record (``S``).

Raw pgoutput gives ``S`` (BEGIN ``final_lsn``).  Stock Debezium does not: no field of a Debezium event equals it
(docs/spike/local_streaming.md section 4; docs/design/engine.md section 5).  So the pipeline cannot use this
rule and ``cdc_v2.pg.cut`` does not contain it.  It lives here for two purposes only: the measurement in
docs/design/cut.md section 5 (how much the rule would narrow, if a stream ever delivered ``S``), and the model
check, which shows that the rule is sound.

Nothing under ``cdc_v2.testing`` is imported by the pipeline, and the names here say what they need
(``..._with_start``), so production code cannot reach this rule by passing one more argument to the library.
"""

from __future__ import annotations

from typing import Optional

from cdc_v2.pg.cut import OUTSIDE, UNKNOWN, StandbyCut, Verdict

#: Size of a WAL page header at the start of a segment (``SizeOfXLogLongPHD``).  A record that follows a page
#: boundary starts this many bytes (or 24, on other pages) after the boundary.
WAL_LONG_PAGE_HEADER = 40
WAL_SHORT_PAGE_HEADER = 24
WAL_BLOCK_SIZE_DEFAULT = 8192

SQL_WAL_BLOCK_SIZE = "SELECT current_setting('wal_block_size')::int"


def wal_block_size(conn) -> int:
    row = conn.execute(SQL_WAL_BLOCK_SIZE).fetchone()
    return int(row[0])


def starts_after_r1(cut: StandbyCut, s: int, wal_block_size: int = WAL_BLOCK_SIZE_DEFAULT) -> bool:
    """True when a commit record that starts at ``s`` cannot have been in replay when the snapshot was taken.

    When the snapshot was taken, the replay position was at most ``R1``, and the only record whose replay may
    have been under way is the one that starts at the replay position.  A record starts exactly at the replay
    position, except when that position is a WAL page boundary: then a page header (24 or 40 bytes) comes first.
    So a commit record that starts after ``R1``, and not within one page header of an ``R1`` that is a page
    boundary, was not in replay.
    """
    if s <= cut.r1:
        return False
    if cut.r1 % wal_block_size == 0 and s - cut.r1 <= WAL_LONG_PAGE_HEADER:
        return False
    return True


def classify_with_start(cut: StandbyCut, e: int, xid: Optional[int], s: int,
                        wal_block_size: int = WAL_BLOCK_SIZE_DEFAULT) -> Verdict:
    """The library's answer, with ``unknown`` narrowed to ``outside`` when the commit record starts after ``R1``."""
    verdict = cut.classify(e, xid)
    if verdict is UNKNOWN and starts_after_r1(cut, s, wal_block_size):
        return OUTSIDE
    return verdict
