"""Crash points of leadership (contract C1; plan 6.2, "From the review of F").

Two lists:

- the pipeline's points, declared here and called in this package (`heartbeat`, `handover`, `stop`, `startup`);
- the bootstrap's points. `flights/cdc_v2/main.py` cannot import this package before the clone, so it carries the
  same list (`BOOT_POINTS`) and its own implementation of the environment contract of `docs/testing/harness.md`
  (the same variables, hit counting, seed rule, actions and hit-log line). They are declared here as well, so that
  `python -m cdc_v2.crashpoints --import cdc_v2.leader` lists them and the crash runner's adapter can name them. A
  T0 test compares the two lists and the two implementations.

The lane points of the `control` lane (`control.fenced`, `control.commit_sent`, `control.committed`) frame the
release and the leader's own transactions; they are package D's.
"""
from cdc_v2 import crashpoints

# The pipeline side. Each description says when a point is reached only on some paths.
crashpoints.declare("leader.heartbeat_sent", "pipeline: immediately before the heartbeat statement (run with kill_later)")
crashpoints.declare("leader.heartbeat_written", "pipeline: the heartbeat changed its row; the tick is not sent yet")
crashpoints.declare("leader.recorded", "pipeline: the leader's first transaction committed (take-over row, alert, "
                    "clean-up of _cdc.connections)", once=True)
crashpoints.declare("leader.release_ready", "pipeline: the release and its row of _cdc.transitions are written, the "
                    "fence and COMMIT follow (only on a planned hand-over)", once=True)
crashpoints.declare("leader.released", "pipeline: the release committed; the run ends with 0 (only on a planned "
                    "hand-over)", once=True)
crashpoints.declare("leader.fence_lost", "pipeline: the one reaction to FenceLost starts; nothing more is written "
                    "(only when another run took over)", once=True)

#: The bootstrap's points, as `flights/cdc_v2/main.py` holds them: (name, description, once).
BOOT_POINTS = (
    ("leader.tables_created", "bootstrap: the leader tables were absent and are now created (a first run only)",
     True),
    ("leader.takeover_sent", "bootstrap: immediately before COMMIT of the take-over (run with kill_later)", True),
    ("leader.taken_over", "bootstrap: COMMIT of the take-over returned; this run leads", True),
    ("leader.interrupted", "bootstrap: the connections of earlier epochs were interrupted after the take-over",
     True),
    ("leader.previous_cancelled", "bootstrap: the previous run was cancelled (only when it was still running)",
     True),
    ("leader.startup_beat", "bootstrap: a start-up heartbeat committed", False),
    ("leader.pipeline_started", "bootstrap: the pipeline process was started", True),
    ("leader.first_tick", "bootstrap: the pipeline's first tick arrived; the start-up heartbeat stops", True),
)
for _name, _description, _once in BOOT_POINTS:
    crashpoints.declare(_name, _description, once=_once)

PIPELINE_POINTS = ("leader.heartbeat_sent", "leader.heartbeat_written", "leader.recorded", "leader.release_ready",
                   "leader.released", "leader.fence_lost")


def all_points():
    """Every leadership point: the bootstrap's and the pipeline's."""
    return [p[0] for p in BOOT_POINTS] + list(PIPELINE_POINTS)


__all__ = ["BOOT_POINTS", "PIPELINE_POINTS", "all_points"]
