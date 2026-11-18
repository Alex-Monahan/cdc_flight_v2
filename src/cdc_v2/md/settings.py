"""Settings of the destination layer (contract C0: a small frozen dataclass per package).

The token is not a setting. The client library reads it from the environment by itself (``motherduck_token``); this
package never reads, stores, prints or logs it, so no object of this package can leak it.
"""
from __future__ import annotations

import re
from dataclasses import dataclass

_MD_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,62}$")
_MEM_NAME = re.compile(r"^:memory:[A-Za-z0-9_]*$")
_MARKER = re.compile(r"^[A-Za-z0-9_.-]{1,64}$")
SHARED_MEMORY_NAME = ":memory:cdc_v2_shared"

#: The fixed set of lanes (design 4.2). A fence row exists for each of them from the first initialisation on.
LANES = ("stream", "control") + tuple("backfill.%d" % i for i in range(16))


@dataclass(frozen=True)
class Settings:
    """Where the destination is and how long its calls may take.

    ``database`` is one of

    - a plain name (letters, digits, underscore): a MotherDuck database, opened alone (``attach_mode=single``);
    - ``:memory:`` or ``:memory:<name>``: an in-memory DuckDB database of this process. Every connection made with
      the same text reaches the same database (plain ``:memory:`` is one shared database per process), which is what
      tier T0 needs; the database disappears when its last connection closes;
    - anything else: the path of a local DuckDB file.
    """
    database: str
    marker: str = "cdc_v2"                 # custom_user_agent of every connection of this pipeline
    connect_deadline_s: float = 30.0       # one attempt to connect, the first statements included
    connect_attempts: int = 3              # attempts of connect() inside the caller's deadline
    connect_retry_pause_s: float = 0.5
    statement_deadline_s: float = 60.0     # a call that names no deadline
    transaction_deadline_s: float = 300.0  # lane.transaction(None)
    rollback_deadline_s: float = 5.0       # ROLLBACK after an error; past it the connection is abandoned
    grace_s: float = 2.0                   # wait after the interrupt before a connection is abandoned
    interrupt_deadline_s: float = 5.0      # one call of the server's interrupt function
    max_waiting_calls: int = 2             # per connection: calls it gave up that have not returned; one more
    #                                        fails at once with ConnectionLost (each holds a thread and a client
    #                                        connection while the network answers nothing)

    def __post_init__(self):
        if not isinstance(self.database, str) or not self.database or "\n" in self.database:
            raise ValueError("Settings.database must be a database name, ':memory:[name]' or a file path")
        if self.database.startswith(":memory:") and not _MEM_NAME.match(self.database):
            raise ValueError("an in-memory database is ':memory:' or ':memory:<letters, digits, underscore>'")
        if self.database.lower().startswith(("md:", "motherduck:")) or "?" in self.database:
            raise ValueError("Settings.database is a plain MotherDuck database name, not a connection string")
        if not isinstance(self.marker, str) or not _MARKER.match(self.marker):
            raise ValueError("Settings.marker: letters, digits, '_', '.', '-', at most 64 characters")
        for name in ("connect_deadline_s", "statement_deadline_s", "transaction_deadline_s", "rollback_deadline_s",
                     "grace_s", "interrupt_deadline_s", "connect_retry_pause_s"):
            v = getattr(self, name)
            if isinstance(v, bool) or not isinstance(v, (int, float)) or not 0 < v < float("inf"):
                raise ValueError("Settings.%s must be a number of seconds > 0, got %r" % (name, v))
        if isinstance(self.connect_attempts, bool) or not isinstance(self.connect_attempts, int) \
                or self.connect_attempts < 1:
            raise ValueError("Settings.connect_attempts must be an integer >= 1")
        if isinstance(self.max_waiting_calls, bool) or not isinstance(self.max_waiting_calls, int) \
                or self.max_waiting_calls < 1:
            raise ValueError("Settings.max_waiting_calls must be an integer >= 1")

    @property
    def is_motherduck(self) -> bool:
        return bool(_MD_NAME.match(self.database))

    @property
    def target(self) -> str:
        """The text passed to ``duckdb.connect``. It never holds a token."""
        if self.is_motherduck:
            return "md:%s?attach_mode=single" % self.database
        if self.database == ":memory:":
            return SHARED_MEMORY_NAME
        return self.database
