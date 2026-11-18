"""Leadership inside the pipeline (SPEC 4.2, R18; design 4 and 5; contracts C5, C11, C12).

The bootstrap (`flights/cdc_v2/main.py`) runs the guard and the take-over and watches the pipeline. Inside the
pipeline this package owns:

- `RunContext`: epoch, run identity, tick descriptor and take-over facts from the bootstrap (C12);
- `Heartbeat`: the heartbeat thread, whose only input is `supervisor.progress()` (C11), and the tick (`Ticker`);
- `on_fence_lost`, `stop_process`: the one reaction to `md.FenceLost` and the way the pipeline ends itself;
- `hand_over`, `release`, `HandoverClock`, `schedule`: the planned hand-over at `run.max_runtime` (design 4.6);
- `on_leading`, `ContenderWatch`: the leader's first transaction (the take-over's row, the alert `takeover`, the
  clean-up of `_cdc.connections`) and the alert `contending_flight`;
- `LEADER`: the leader machine; its migration list `leader` (`_cdc.leader_record`, `_cdc.contenders`).

`docs/design/leadership.md` describes the protocol as built.
"""
from . import points  # noqa: F401 - declares the crash points
from .context import RunContext
from .handover import HandoverClock, hand_over, release, schedule
from .heartbeat import Heartbeat, Ticker, waiting_calls_exceeded
from .machine import LEADER
from .schema import NOTICE_SQL
from .settings import LeaderSettings, check_order
from .startup import ContenderWatch, on_leading, takeover_alert_fields
from .stop import (EXIT_FAILED, EXIT_NO_WATCHDOG, EXIT_RELEASED, EXIT_SUPERSEDED, on_fence_lost,
                   stop_process)

__all__ = ["RunContext", "Heartbeat", "Ticker", "waiting_calls_exceeded", "on_fence_lost", "stop_process",
           "hand_over", "release", "HandoverClock", "schedule", "on_leading", "ContenderWatch",
           "takeover_alert_fields", "LEADER", "NOTICE_SQL", "LeaderSettings", "check_order",
           "EXIT_RELEASED", "EXIT_FAILED", "EXIT_SUPERSEDED", "EXIT_NO_WATCHDOG"]
