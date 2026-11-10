import pytest
from hypothesis import given, strategies as st

from cdc_v2 import lsn


def test_text_forms():
    assert lsn.parse("0/0") == 0
    assert lsn.parse("16/B374D848") == 0x16B374D848
    assert lsn.parse("ffffffff/ffffffff") == lsn.MAX
    assert lsn.format(0x16B374D848) == "16/B374D848"
    assert lsn.format(0x100000000) == "1/0"


@given(st.integers(min_value=0, max_value=lsn.MAX))
def test_round_trip(value):
    assert lsn.parse(lsn.format(value)) == value


@pytest.mark.parametrize("bad", ["", "16", "16/", "/1", "1/2/3", " 1/2", "1/2 ", "g/1", "123456789/1", None, 5])
def test_parse_refuses(bad):
    with pytest.raises(ValueError):
        lsn.parse(bad)


@pytest.mark.parametrize("bad", [-1, lsn.MAX + 1, True, "1/2", 1.0])
def test_format_refuses(bad):
    with pytest.raises(ValueError):
        lsn.format(bad)
