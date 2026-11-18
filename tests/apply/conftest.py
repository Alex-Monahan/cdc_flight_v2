"""Package A's tests: every test runs with the check that apply reads a count only from an INSERT, UPDATE or DELETE
without RETURNING (`apply_lane.counted_statements`), and a destination that a failed test left open is closed
(the layer's connections must not outlive the test: `tests/destination` counts them)."""
import pytest


@pytest.fixture(autouse=True)
def _apply_on_the_lane():
    import apply_lane
    with apply_lane.counted_statements():
        yield
    apply_lane.close_all()
