"""Unit test for h/heuristics/linefinder.py module."""
import pytest
from . linefinder import HeuristicsLineFinder
from typing import List, NoReturn

#spectrum_0 = [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 5.0, 15.0, 
#            30.0, 15.0, 5.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 
#            0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]
#spectrum_1 = [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 5.0, 15.0, 
#            30.0, 15.0, 5.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 
#            10.0, 20.0, 40.0, 20.0, 10.0, 0.0, 0.0, 0.0, 0.0, 0.0]
spectrum_0 = [100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 105.0, 115.0, 
            130.0, 115.0, 105.0, 100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 
            100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 100.0]
spectrum_1 = [100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 105.0, 115.0, 
            130.0, 115.0, 105.0, 100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 
            110.0, 120.0, 140.0, 120.0, 110.0, 100.0, 100.0, 100.0, 100.0, 100.0]
spectrum_2 = [100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 
            90.0, 80.0, 70.0, 60.0, 50.0, 60.0, 70.0, 80.0, 90.0, 100.0, 
            100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 100.0, 100.0]

@pytest.mark.parametrize("spectrum, threshold, tweak, mask, edge, expected", 
                         [
                             (spectrum_0, 7.0, False, [], None, [8, 12]),
                             (spectrum_1, 7.0, False, [], None, [8, 12, 20, 24]),
                             (spectrum_1, 7.0, True, [], None, [8, 21, 11, 24]),
                             (spectrum_1, 7.0, False, [], [7, 5], []),
                             (spectrum_2, 7.0, False, [], None, [10, 18]),
                         ])

def test_linefinder(spectrum, threshold, tweak, mask, edge, expected) -> NoReturn:
    """
    Unit test for calculate method.

    Args:
        spectrum: list of spectrum data
        threshold: a factor of threshold of line detection with respect to MAD.
        tweak: if True, spectral line ranges are extended to cover line edges.
        mask: mask of spectrum array. An elements of spectrum is valid when 
              the corresponding element of mask is True. If False, invalid. 
              The length of list should be equal to that of spectrum.
        edge: the number of elements in left and right edges of spectrum to be
              excluded from line detection.
        expected: expected result
    Returns:
        NoReturn
    Raises:
        AssertationError if tests fail
    """
    s = HeuristicsLineFinder()
    assert s.calculate(spectrum, threshold, tweak, mask, edge) == expected

@pytest.mark.parametrize("spectrum, ranges, edge, n_ignore, expected", 
                         [
                             (spectrum_0, [8, 12], [0, 0], 1, [8, 12]),
                             (spectrum_1, [8, 12], [0, 0], 1, [8, 12]),
                             (spectrum_1, [20, 24], [0, 0], 1, [11, 24]),
                             (spectrum_1, [12, 20], [0, 0], 1, [11, 20]),
                             (spectrum_2, [11, 18], [0, 0], 1, [9, 18]),                             
                         ])
def test_tweak_lines(spectrum, ranges, edge, n_ignore, expected) -> NoReturn:
    """
    Unit test for tweak_lines method.

    Args:
        spectrum: list of spectrum data.
        ranges: list of indice of line range.
        edge: a list of minimum and maximum indices of spectrum to consider 
        (excluding edge channels).
        n_ignore: the maximum number of channels to extend line ranges in 
        each direction.
        expected: expected result
    Returns:
        NoReturn
    Raises:
        AssertationError if tests fail
    """
    s = HeuristicsLineFinder()
    assert s.tweak_lines(spectrum, ranges, edge, n_ignore) == expected
