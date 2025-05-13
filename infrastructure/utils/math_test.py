import pytest

from .math import round_half_up, round_up


def test_simple():
    assert round_half_up(-1.055, precision=2) == -1.06


params_round_half_up = [(-2.1, 0, -2), (-2.5, 0, -3), ("-3.5", 0, -4), (4.2, 0, 4), (4.5, 0, 5),
                        (-6.034543, 4, -6.0345), (-7.034443, 4, -7.0344)]


@pytest.mark.parametrize("unrounded, precision, expected", params_round_half_up)
def test_round_half_up(unrounded, precision, expected):
    """Test round_half_up()

    This utility function takes an un-rounded scalar value and rounds
    it to the nearest integer, with ties going away from zero, using 'precision'
    to signify the decimal place to round.
    """
    assert round_half_up(unrounded, precision=precision) == expected


params_round_up = [(-2.1, 0, -3), (-2.5, 0, -3), ("-3.5", 0, -4), (4.2, 0, 5), (4.5, 0, 5),
                   (-6.034543, 4, -6.0346), (-7.034443, 4, -7.0345)]


@pytest.mark.parametrize("unrounded, precision, expected", params_round_up)
def test_round_up(unrounded, precision, expected):
    """Test round_up()

    This utility function takes an un-rounded scalar value and rounds
    it to the nearest integer away from zero, using 'precision' to signify
    the decimal place to round.
    """
    assert round_up(unrounded, precision=precision) == expected
