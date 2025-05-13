"""
scorecalculator_test.py : Unit tests for "qa/scorecalculator.py".

Unit tests for "qa/scorecalculator.py"
"""
from typing import List, Tuple

import pytest

import pipeline.qa.scorecalculator as qacalc


@pytest.mark.parametrize(
    'lines, expected',
    [
        ([(10, 5, True)], [(8, 13)]),
        ([(10, 5, False)], []),
        # round-off tests
        ([(5.0, 10, True)], [(0, 10)]),
        ([(5.2, 10.1, True)], [(0, 10)]),
        ([(5.6, 10.1, True)], [(1, 11)])
    ]
)
def test_get_line_ranges(lines: List[Tuple[int, int, bool]], expected: List[Tuple[int, int]]):
    line_ranges = qacalc.get_line_ranges(lines)
    assert line_ranges == expected


@pytest.mark.parametrize(
    'lines, nchan, edge, expected',
    [
        # line not extend to the edge
        ([(1020, 1030)], 2048, (0, 0), False),
        ([(1, 11)], 1024, (0, 0), False),
        ([(800, 1022)], 1024, (0, 0), False),
        # narrow edge lines
        ([(0, 10)], 1024, (0, 0), True),
        ([(0, 203)], 1024, (0, 0), True),
        ([(990, 1023)], 1024, (0, 0), True),
        ([(-10, 200)], 1024, (0, 0), True),
        ([(900, 2047)], 1024, (0, 0), True),
        # wide edge lines
        ([(0, 204)], 1024, (0, 0), True),
        ([(800, 1023)], 1024, (0, 0), True),
        ([(800, 1024)], 1024, (0, 0), True),
        ([(0, 1023)], 1024, (0, 0), True),
        ([(-10, 210)], 1024, (0, 0), True),
        ([(800, 2047)], 1024, (0, 0), True),
        # edge excuded
        ([(2, 11)], 1024, (2, 0), True),
        ([(2, 11)], 1024, (1, 0), False),
        ([(800, 1021)], 1024, (0, 2), True),
        ([(800, 1021)], 1024, (0, 1), False),
    ]
)
def test_examine_sd_edge_lines(lines: List[Tuple[int, int]], nchan: int, edge: Tuple[int, int], expected: float):
    score = qacalc.examine_sd_edge_lines(lines, nchan, edge)
    assert score == expected


@pytest.mark.parametrize(
    'lines, nchan, edge, expected',
    [
        # single narrow line
        ([(1020, 1030)], 2048, (0, 0), False),
        ([(500, 840)], 1024, (0, 0), False),
        ([(1000, 1681)], 2048, (0, 0), False),
        # single wide line that lowers QA score
        ([(0, 1023)], 1024, (0, 0), True),
        ([(0, 2047)], 1024, (0, 0), True),
        ([(500, 841)], 1024, (0, 0), True),
        ([(1000, 1682)], 2048, (0, 0), True),
        # multiple lines, less coverage than threshold
        ([(100, 199), (300, 399), (500, 599), (700, 740)], 1024, (0, 0), False),
        # multiple lines, enough coverage to lower QA score
        ([(100, 199), (300, 399), (500, 599), (700, 741)], 1024, (0, 0), True),
        # edge excluded
        ([(501, 840)], 1024, (3, 2), True),
        ([(501, 840)], 1024, (5, 0), True),
        ([(501, 840)], 1024, (0, 5), True),
    ]
)
def test_examine_sd_wide_lines(lines: List[Tuple[int, int]], nchan: int, edge: Tuple[int, int], expected: float):
    score = qacalc.examine_sd_wide_lines(lines, nchan, edge)
    assert score == expected


@pytest.mark.parametrize(
    'edge, nchan, sideband, ranges, expected',
    [
        ((0, 0), 16, 1, [(3, 5), (9, 12)], [(3, 5), (9, 12)]),
        ((0, 0), 16, -1, [(3, 5), (9, 12)], [(3, 6), (10, 12)]),
        ((1, 0), 16, 1, [(3, 5), (9, 12)], [(2, 4), (8, 11)]),
        ((1, 1), 16, 1, [(3, 5), (9, 12)], [(2, 4), (8, 11)]),
        ((1, 0), 16, -1, [(3, 5), (9, 12)], [(3, 6), (10, 12)]),
        ((1, 1), 16, -1, [(3, 5), (9, 12)], [(2, 5), (9, 11)]),
    ]
)
def test_channel_ranges_for_image(edge: Tuple[int, int], nchan: int, sideband: int, ranges: List[Tuple[int, int]], expected: List[Tuple[int, int]]):
    ranges_image = qacalc.channel_ranges_for_image(edge, nchan, sideband, ranges)
    assert ranges_image == expected
