"""Timeline history (H-029, H-045): parse a server's history file and decide whether a position lies on the history
of the server's current timeline.

A history file of timeline T lists every ancestor timeline with the position at which the next one forked from it
(the switch point), one line each: `<parent tli>\\t<switch point>\\t<reason>`. Timeline 1 has no history file.
WAL of an ancestor timeline up to its switch point is shared with every later timeline; WAL after the switch point
on the ancestor exists only there (a server that kept writing after a promotion of its standby).
"""
from __future__ import annotations

from dataclasses import dataclass

from cdc_v2 import lsn as lsnmod


@dataclass(frozen=True)
class TimelineEntry:
    timeline: int        # an ancestor timeline
    switch: int          # the position where the next timeline begins; WAL on `timeline` up to here is shared


@dataclass(frozen=True)
class TimelineHistory:
    timeline: int                          # the server's current timeline
    entries: tuple[TimelineEntry, ...]     # ancestors, oldest first; empty on timeline 1
    system_identifier: str | None = None
    position: int | None = None            # IDENTIFY_SYSTEM xlogpos when read


def parse_history(text: str, current: int) -> TimelineHistory:
    """Parse the content of a `<tli>.history` file of timeline `current`. Comment and empty lines are skipped.
    Raises ValueError on a malformed or non-increasing history."""
    entries = []
    for raw in text.splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        parts = line.split("\t") if "\t" in line else line.split(None, 2)
        if len(parts) < 2:
            raise ValueError("malformed timeline history line: %r" % raw)
        tli = int(parts[0])
        switch = lsnmod.parse(parts[1].strip())
        if entries and (tli <= entries[-1].timeline or switch < entries[-1].switch):
            raise ValueError("timeline history is not increasing at %r" % raw)
        entries.append(TimelineEntry(tli, switch))
    if entries and entries[-1].timeline >= current:
        raise ValueError("timeline history of %d lists timeline %d" % (current, entries[-1].timeline))
    return TimelineHistory(timeline=current, entries=tuple(entries))


def contains(history: TimelineHistory, position: int, timeline: int) -> bool:
    """Whether `position`, written on `timeline`, lies on the history of the server's current timeline.

    True when `timeline` is the current timeline, or an ancestor and `position` ≤ its switch point (a record that
    ends exactly at the switch point is shared). False for a timeline the history does not know (another server,
    or a newer timeline than the server's), and for a position past the ancestor's switch point (written on the
    old timeline after the fork: diverged)."""
    if isinstance(position, bool) or not isinstance(position, int) or position < 0:
        raise ValueError("position must be an int LSN")
    if timeline == history.timeline:
        return True
    for e in history.entries:
        if e.timeline == timeline:
            return position <= e.switch
    return False
