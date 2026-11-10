import pytest

from cdc_v2 import deadline as d


@pytest.fixture
def clock(monkeypatch):
    state = {"now": 100.0, "slept": []}

    def sleep(seconds):
        state["slept"].append(seconds)
        state["now"] += seconds

    monkeypatch.setattr(d, "_clock", lambda: state["now"])
    monkeypatch.setattr(d, "_sleep", sleep)
    return state


def test_remaining_and_expiry(clock):
    dl = d.Deadline.after(5)
    assert dl.remaining() == 5 and not dl.expired
    dl.check("x")
    clock["now"] += 5
    assert dl.remaining() == 0 and dl.expired
    with pytest.raises(d.DeadlineExceeded) as e:
        dl.check("waiting for the commit")
    assert e.value.what == "waiting for the commit" and isinstance(e.value, TimeoutError)
    clock["now"] += 50
    assert dl.remaining() == 0


def test_within_never_extends(clock):
    outer = d.Deadline.after(5)
    assert outer.within(2).remaining() == 2
    assert outer.within(60) == outer


def test_sleep_stops_at_the_deadline(clock):
    dl = d.Deadline.after(5)
    dl.sleep(2, "x")
    assert clock["slept"] == [2] and dl.remaining() == 3
    with pytest.raises(d.DeadlineExceeded):
        dl.sleep(3, "x")
    assert clock["slept"] == [2, 3] and dl.expired


@pytest.mark.parametrize("bad", [-1, float("nan"), float("inf")])
def test_refuses_durations_that_are_not_a_deadline(clock, bad):
    with pytest.raises(ValueError):
        d.Deadline.after(bad)
