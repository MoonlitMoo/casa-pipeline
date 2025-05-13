"""
Test for heuristics defined in rasterscan.py.

Configuration of Raster scan:
  - raster scan consists of two raster maps
  - each raster map consists of two raster rows
  - configuration options:
    - row scan direction: round-trip or one-way
    - map scan direction: round-trip or one-way
    - scan angle: horizontal, vertical, arbitrary angle
    - ratio of raster row interval to pointing interval: <1, 1, >1
    - pointing error: 0 to 10% of pointing interval

Failure Cases:
  - non-raster pattern (PSW)
  - large pointing error: pointing error comparable to pointing interval
"""
import math
from typing import Tuple

import numpy as np
import pytest

from . import rasterscan


def random_noise(n: int, mean: int = 0, amp: int = 1, rs: np.random.mtrand.RandomState = None) -> np.ndarray:
    """Generate random noise.

    Generate random noise with given mean and maximum amplitude.
    Seed for random noise can be specified.

    Args:
        n (int): number of random noise
        mean (int, optional): mean value of random noise. Defaults to 0.
        amp (int, optional): maximum amplitude of random noise. Defaults to 1.
        rs (np.random.mtrand.RandomState, optional): seed for random noise. Defaults to None.

    Returns:
        np.ndarray: random noise
    """
    if rs is None:
        r = np.random.rand(n)
    else:
        r = rs.rand(n)
    return (r - (0.5 - mean)) * amp / 0.5


def generate_position_data_psw() -> Tuple[np.ndarray, np.ndarray]:
    """Generate position data for simulated position-switch observation.

    Generate position data for simulated position-switch observatin.
    The observation consists of four positions in 4x4 grids. Each
    position has ten data that contain random noise around commanded position.

    Returns:
        tuple: two-tuple consisting of the list of x (R.A.) and
               y (Dec.) directions
    """
    xlist = [0, 1, 2, 3]
    ylist = [0, 1, 2, 3]
    rs = np.random.RandomState(seed=1234567)
    noise_mean = 0
    noise_amp = 0.02
    num_pointings_per_grid = 10
    ra_list = []
    dec_list = []
    for x in xlist:
        for y in ylist:
            xnoise = random_noise(num_pointings_per_grid, noise_mean, noise_amp, rs)
            ynoise = random_noise(num_pointings_per_grid, noise_mean, noise_amp, rs)
            ra_list.append(xnoise + x)
            dec_list.append(ynoise + y)
    ra_list = np.concatenate(ra_list)
    dec_list = np.concatenate(dec_list)

    return ra_list, dec_list


def generate_position_data_raster(
    oneway_row: bool = False,
    oneway_map: bool = True,
    scan_angle: float = 0.0,
    interval_factor: float = 1.0,
    pointing_error: float = 0.1
) -> Tuple[np.ndarray, np.ndarray]:
    """Generate position data for simulated OTF raster observation.

    Generate position data for simulated OTF raster observation
    along scan direction specified by scan_angle. Default scan
    angle is along the x-direction (R.A.). The observation consists
    of two raster rows. Each row has twenty continuously taken data
    that contains random noise around commanded position. Scanning
    directions are opposite in these two rows if oneway_row is False
    (default).

        y

        |
        |    <--------------------------------
      1 -  + + + + + + + + + + + + + + + + + + + +
        |
        |
      0 -  + + + + + + + + + + + + + + + + + + + +
        |    -------------------------------->
        |
        -----------------------------------------------  x

    Args:
        oneway_row: all raster rows goes the same direction specified
                    by scan angle if True. Otherwise, next raster row
                    goes opposite direction to the current row.
                    defaults to False.
        oneway_map: all raster maps start from the same point if True.
                    Otherwise, next raster map starts from the point
                    diagonal to the starting point of the current map.
                    defaults to True.
        scan_angle: scan angle in degree with respect to x-direction.
                    defaults to 0.
        interval_factor: spacing between raster rows as a fraction of
                         pointing interval. Larger value corresponds
                         to coarse spacing between rows. defaults to 1.0.
        pointing_error: pointing error factor as a fraction of pointing
                        interval. Larger value corresponds to larger error.
                        defaults to 0.1.
    Returns:
        tuple: two-tuple consisting of the list of x (R.A.) and
               y (Dec.) directions
    """
    x_interval = 0.1
    y_interval = x_interval * interval_factor
    print(f'pointing interval = {x_interval}, row interval = {y_interval}')
    xlist = np.arange(0, 1, x_interval)
    xlist2 = xlist if oneway_row is True else xlist[::-1]
    xlist = [xlist, xlist2] * 8
    ylist = np.arange(0, (len(xlist) + 0.1) * y_interval, y_interval)
    rs = np.random.RandomState(seed=1234567)
    noise_mean = 0
    noise_amp = x_interval * pointing_error
    print(f'Noise Amplitude = {noise_amp}')
    ra_list = []
    dec_list = []
    for x, y in zip(xlist, ylist):
        ndata = len(x)
        ra_list.append(x)
        dec_list.append(np.zeros(ndata, dtype=float) + y)
    ra_list = np.concatenate(ra_list)
    dec_list = np.concatenate(dec_list)

    if oneway_map is True:
        ra_list2 = ra_list
        dec_list2 = dec_list
    else:
        ra_list2 = ra_list[::-1]
        dec_list2 = dec_list[::-1]
    map_ra = np.concatenate((ra_list, ra_list2))
    map_dec = np.concatenate((dec_list, dec_list2))

    ndata = len(map_ra)
    xnoise = random_noise(ndata, noise_mean, noise_amp, rs)
    ynoise = random_noise(ndata, noise_mean, noise_amp, rs)
    map_ra += xnoise
    map_dec += ynoise

    angle_rad = math.radians(scan_angle)
    cost = math.cos(angle_rad)
    sint = math.sin(angle_rad)
    rot_map_ra = map_ra * cost - map_dec * sint
    rot_map_dec = map_ra * sint + map_dec * cost

    return rot_map_ra, rot_map_dec


@pytest.mark.parametrize(
    'oneway_row, oneway_map, scan_angle, interval_factor, pointing_error',
    [
        (False, True, 0.0, 1.0, 0.1),
        (False, True, 30.0, 1.0, 0.1),
        (False, True, 90.0, 1.0, 0.1),
        (True, True, 0.0, 1.0, 0.1),
        (True, True, 30.0, 1.0, 0.1),
        (True, True, 90.0, 1.0, 0.1),
        (True, False, 0.0, 1.0, 0.1),
        (True, False, 30.0, 1.0, 0.1),
        (True, False, 90.0, 1.0, 0.1),
        (False, False, 0.0, 1.0, 0.1),
        (False, False, 30.0, 1.0, 0.1),
        (False, False, 90.0, 1.0, 0.1),
        (True, True, 0.0, 0.5, 0.1),
        (False, True, 0.0, 0.5, 0.1),
        (False, True, 0.0, 1.0, 0.5),
    ]
)
def test_rasterscan_heuristic(oneway_row, oneway_map, scan_angle, interval_factor, pointing_error):
    """Test rasterscan heuristic."""
    raster_row = 'one-way' if oneway_row is True else 'round-trip'
    raster_map = 'one-way' if oneway_map is True else 'round-trip'
    print()
    print('===================')
    print('TEST CONFIGURATION:')
    print('  raster row:   {}'.format(raster_row))
    print('  raster map:   {}'.format(raster_map))
    print('  scan angle:   {:.1f}deg'.format(scan_angle))
    print('  row interval: {:.2f}'.format(interval_factor))
    print('  pointing error: {:.2f}'.format(pointing_error))
    print('===================')
    print()

    ra, dec = generate_position_data_raster(
        oneway_row=oneway_row,
        oneway_map=oneway_map,
        scan_angle=scan_angle,
        interval_factor=interval_factor,
        pointing_error=pointing_error
    )

    h = rasterscan.RasterScanHeuristic()

    if oneway_map is False or pointing_error > 0.1:
        # should raise RasterScanHeuristicsFailure
        with pytest.raises(rasterscan.RasterScanHeuristicsFailure):
            gaptable, gaplist = h(ra, dec)
    else:
        gaptable, gaplist = h(ra, dec)

        # total number of data is 320
        ndata = 320
        # total number of data per row is 10
        nrow = 10
        # each raster map consists of 16 raster rows
        nmap = nrow * 16
        gaptable_expected_small = [
            np.arange(s, s + nrow) for s in range(0, ndata, nrow)
        ]
        gaptable_expected_large = [
            np.arange(s, s + nmap) for s in range(0, ndata, nmap)
        ]
        gaptable_small = gaptable[0]
        gaptable_large = gaptable[1]
        assert len(gaptable_small) == len(gaptable_expected_small)
        assert len(gaptable_large) == len(gaptable_expected_large)
        for result, ref in zip(gaptable_small, gaptable_expected_small):
            assert np.array_equal(result, ref)
        for result, ref in zip(gaptable_large, gaptable_expected_large):
            assert np.array_equal(result, ref)

        gaplist_expected_small = np.arange(nrow, ndata, nrow, dtype=int)
        gaplist_expected_large = np.arange(nmap, ndata, nmap, dtype=int)
        gaplist_small = gaplist[0]
        gaplist_large = gaplist[1]
        assert np.array_equal(gaplist_small, gaplist_expected_small)
        assert np.array_equal(gaplist_large, gaplist_expected_large)


def test_rasterscan_heuristic_fail_psw():
    """Test rasterscan heuristic (expected to fail)."""
    print()
    print('===================')
    print('TEST CONFIGURATION:')
    print('  expected to fail')
    print('  attempt to analyze PSW pattern')
    print('===================')
    print()

    ra, dec = generate_position_data_psw()
    h = rasterscan.RasterScanHeuristic()

    # should raise RasterScanHeuristicsFailure
    with pytest.raises(rasterscan.RasterScanHeuristicsFailure):
        gaptable, gaplist = h(ra, dec)


@pytest.mark.parametrize(
    'x, y',
    [
        ((0, 0), (1, 1)),
        ((1.23, 1.24), (2.99, 8.38)),
    ]
)
def test_distance(x, y):
    """Test distance."""
    d = rasterscan.distance(x[0], y[0], x[1], y[1])
    dx = x[1] - x[0]
    dy = y[1] - y[0]
    expected = math.sqrt(dx * dx + dy * dy)
    assert d == expected


def test_generate_histogram():
    """Test generate_histogram."""
    arr = np.arange(10) + 0.5
    binw = 1
    left_edge = 0
    right_edge = 10
    result = rasterscan.generate_histogram(arr, binw, left_edge, right_edge)
    expected_data = np.ones(10)
    expected_bin = np.arange(11)
    assert np.array_equal(result[0], expected_data)
    assert np.array_equal(result[1], expected_bin)

    arr += 50
    binw = 10
    left_edge = 0
    right_edge = 100
    result = rasterscan.generate_histogram(arr, binw, left_edge, right_edge)
    expected_data = np.zeros(10)
    expected_data[5] = 10
    expected_bin = np.arange(0, 101, 10)
    assert np.array_equal(result[0], expected_data)
    assert np.array_equal(result[1], expected_bin)


def test_detect_peak():
    """Test detect_peak."""
    arr = np.zeros(10)
    arr[5] = 3
    result = rasterscan.detect_peak(arr)
    expected = (3, 5, 5, 5)
    assert result == expected

    arr[6] = 1
    arr[2:5] = 1
    result = rasterscan.detect_peak(arr)
    expected = (7, 5, 2, 6)
    assert result == expected

    # test with mask: True is invalid while False is valid
    msk = np.ones(10, dtype=bool)
    arr[8] = 2
    msk[7:] = False
    result = rasterscan.detect_peak(arr, msk)
    expected = (2, 8, 8, 8)
    assert result == expected


def test_find_histogram_peak():
    """Test find_histogram_peak."""
    arr = np.zeros(10)
    arr[5] = 3
    result = rasterscan.find_histogram_peak(arr)
    expected = [5]
    assert result == expected

    arr[6] = 1
    arr[2:5] = 1
    result = rasterscan.find_histogram_peak(arr)
    expected = [5]
    assert result == expected

    arr[:] = 0
    arr[5] = 6
    arr[8] = 8
    arr[1] = 7
    result = rasterscan.find_histogram_peak(arr)
    expected = [8, 1]
    assert result == expected

    arr += 1
    # should raise RasterScanHeuristicsFailure
    with pytest.raises(rasterscan.RasterScanHeuristicsFailure):
        result = rasterscan.find_histogram_peak(arr)


def test_shift_angle():
    """Test shift_angle."""
    shift = 10
    angle = 90
    result = rasterscan.shift_angle(angle, shift)
    expected = angle + shift
    assert result == expected

    angle = np.array([90, 270])
    result = rasterscan.shift_angle(angle, shift)
    expected = np.array(angle) + shift
    assert np.array_equal(result, expected)

    angle = np.array([90, -20])
    result = rasterscan.shift_angle(angle, shift)
    expected = np.array([90, 340]) + shift
    assert np.array_equal(result, expected)

    angle = np.array([-190, -10])
    result = rasterscan.shift_angle(angle, shift)
    expected = np.array([180, 0])
    assert np.array_equal(result, expected)


def test_find_most_frequent():
    """Test find_most_frequent."""
    arr = np.ones(10)
    result = rasterscan.find_most_frequent(arr)
    expected = 1
    assert result == expected

    arr[0] = 10
    result = rasterscan.find_most_frequent(arr)
    expected = 1
    assert result == expected

    arr[:5] = 10
    result = rasterscan.find_most_frequent(arr)
    expected = 10
    assert result == expected


def test_refine_gaps():
    """Test refine_gaps."""
    expected = np.arange(0, 101, 10)

    # insert unreasonable gap
    gaplist = np.append(expected, [9])
    gaplist.sort()

    result = rasterscan.refine_gaps(gaplist, 100)
    assert np.array_equal(result, expected)


@pytest.mark.parametrize(
    'peaks, width, expected',
    [
        ([90], 45, [(45, 135)]),
        ([10], 45, [(0, 55), (325, 360)]),
        ([340], 45, [(295, 360), (0, 25)]),
    ]
)
def test_create_range(peaks, width, expected):
    """Test create_range."""
    angle_min = 0
    angle_max = 360

    result = rasterscan.create_range(peaks, width, angle_min, angle_max)
    assert len(result) == len(expected)
    for val, ref in zip(result, expected):
        assert len(val) == 2
        assert val[0] == ref[0]
        assert val[1] == ref[1]


def test_find_angle_gap_by_range():
    """Test find_angle_gap_by_range."""
    arr = np.ones(10) * 10
    ranges = [(0, 20)]
    result = rasterscan.find_angle_gap_by_range(arr, ranges)
    expected = np.array([])
    assert np.array_equal(result, expected)

    arr[5] = 55
    result = rasterscan.find_angle_gap_by_range(arr, ranges)
    expected = np.array([6])
    assert np.array_equal(result, expected)

    arr[:] = 10
    ranges = [(20, 30)]
    result = rasterscan.find_angle_gap_by_range(arr, ranges)
    expected = np.arange(1, len(arr) + 1)
    assert np.array_equal(result, expected)




@pytest.mark.parametrize(
    'oneway_row, oneway_map, scan_angle, interval_factor, pointing_error',
    [
        (False, True, 0.0, 1.0, 0.1),
        (False, True, 30.0, 1.0, 0.1),
        (False, True, 90.0, 1.0, 0.1),
        (True, True, 0.0, 1.0, 0.1),
        (True, True, 30.0, 1.0, 0.1),
        (True, True, 90.0, 1.0, 0.1),
        (True, False, 0.0, 1.0, 0.1),
        (True, False, 30.0, 1.0, 0.1),
        (True, False, 90.0, 1.0, 0.1),
        (False, False, 0.0, 1.0, 0.1),
        (False, False, 30.0, 1.0, 0.1),
        (False, False, 90.0, 1.0, 0.1),
    ]
)
def test_find_distance_gap(oneway_row, oneway_map, scan_angle, interval_factor, pointing_error):
    """Test find_distance_gap."""
    raster_row = 'one-way' if oneway_row is True else 'round-trip'
    raster_map = 'one-way' if oneway_map is True else 'round-trip'
    print()
    print('===================')
    print('TEST CONFIGURATION:')
    print('  raster row:   {}'.format(raster_row))
    print('  raster map:   {}'.format(raster_map))
    print('  scan angle:   {:.1f}deg'.format(scan_angle))
    print('  row interval: {:.2f}'.format(interval_factor))
    print('  pointing error: {:.2f}'.format(pointing_error))
    print('===================')
    print()

    ra, dec = generate_position_data_raster(
        oneway_row=oneway_row,
        oneway_map=oneway_map,
        scan_angle=scan_angle,
        interval_factor=interval_factor,
        pointing_error=pointing_error
    )

    ndata = len(ra)
    dra = np.diff(ra)
    ddec = np.diff(dec)
    result = rasterscan.find_distance_gap(dra, ddec)
    gaplist = result[1]
    print(f'gaplist={gaplist}')
    if oneway_row is False:
        if oneway_map is True:
            expected = np.array([ndata // 2])
        else:
            expected = np.array([])
    else:
        if oneway_map is True:
            expected = np.arange(10, ndata, 10, dtype=int)
        else:
            expected = np.arange(10, ndata, 10, dtype=int)
            ngap = len(expected)
            expected = np.delete(expected, ngap // 2)

    assert np.array_equal(gaplist, expected)


def test_find_angle_gap():
    """Test find_angle_gap."""
    angle = np.zeros(100)
    angle[9::10] = 90
    gaplist = rasterscan.find_angle_gap(angle)
    expected = np.arange(10, 101, 10)
    assert np.array_equal(gaplist, expected)

    angle += 30
    gaplist = rasterscan.find_angle_gap(angle)
    expected = np.arange(10, 101, 10)
    assert np.array_equal(gaplist, expected)

    angle[:] = 60
    angle[10] = 100
    angle[20] = 120
    gaplist = rasterscan.find_angle_gap(angle)
    expected = np.array([21])
    assert np.array_equal(gaplist, expected)



@pytest.mark.parametrize(
    'oneway_row, oneway_map, scan_angle, interval_factor, pointing_error',
    [
        (False, True, 0.0, 1.0, 0.1),
        (False, True, 30.0, 1.0, 0.1),
        (False, True, 90.0, 1.0, 0.1),
        (True, True, 0.0, 1.0, 0.1),
        (True, True, 30.0, 1.0, 0.1),
        (True, True, 90.0, 1.0, 0.1),
        (True, False, 0.0, 1.0, 0.1),
        (True, False, 30.0, 1.0, 0.1),
        (True, False, 90.0, 1.0, 0.1),
        (False, False, 0.0, 1.0, 0.1),
        (False, False, 30.0, 1.0, 0.1),
        (False, False, 90.0, 1.0, 0.1),
        (True, True, 0.0, 0.5, 0.1),
        (False, True, 0.0, 0.5, 0.1),
        (False, True, 0.0, 1.0, 0.5),
    ]
)
def test_find_raster_row(oneway_row, oneway_map, scan_angle, interval_factor, pointing_error):
    """Test find_raster_row."""
    raster_row = 'one-way' if oneway_row is True else 'round-trip'
    raster_map = 'one-way' if oneway_map is True else 'round-trip'
    print()
    print('===================')
    print('TEST CONFIGURATION:')
    print('  raster row:   {}'.format(raster_row))
    print('  raster map:   {}'.format(raster_map))
    print('  scan angle:   {:.1f}deg'.format(scan_angle))
    print('  row interval: {:.2f}'.format(interval_factor))
    print('  pointing error: {:.2f}'.format(pointing_error))
    print('===================')
    print()

    ra, dec = generate_position_data_raster(
        oneway_row=oneway_row,
        oneway_map=oneway_map,
        scan_angle=scan_angle,
        interval_factor=interval_factor,
        pointing_error=pointing_error
    )

    if pointing_error > 0.1:
        # should raise RasterScanHeuristicsFailure
        with pytest.raises(rasterscan.RasterScanHeuristicsFailure):
            gaplist = rasterscan.find_raster_row(ra, dec)
    else:
        gaplist = rasterscan.find_raster_row(ra, dec)
        expected = np.arange(0, len(ra) + 1, 10)
        assert np.array_equal(gaplist, expected)


@pytest.mark.parametrize(
    'oneway_row, oneway_map, scan_angle, interval_factor',
    [
        (False, True, 0.0, 1.0),
        (False, True, 30.0, 1.0),
        (False, True, 90.0, 1.0),
        (True, True, 0.0, 1.0),
        (True, True, 30.0, 1.0),
        (True, True, 90.0, 1.0),
        (True, False, 0.0, 1.0),
        (True, False, 30.0, 1.0),
        (True, False, 90.0, 1.0),
        (False, False, 0.0, 1.0),
        (False, False, 30.0, 1.0),
        (False, False, 90.0, 1.0),
        (True, True, 0.0, 0.5),
        (False, True, 0.0, 0.5),
        (False, True, 0.0, 1.0),
    ]
)
def test_get_raster_distance(oneway_row, oneway_map, scan_angle, interval_factor):
    """Test get_raster_distance."""
    raster_row = 'one-way' if oneway_row is True else 'round-trip'
    raster_map = 'one-way' if oneway_map is True else 'round-trip'
    pointing_error = 0
    print()
    print('===================')
    print('TEST CONFIGURATION:')
    print('  raster row:   {}'.format(raster_row))
    print('  raster map:   {}'.format(raster_map))
    print('  scan angle:   {:.1f}deg'.format(scan_angle))
    print('  row interval: {:.2f}'.format(interval_factor))
    print('  pointing error: {:.2f}'.format(pointing_error))
    print('===================')
    print()

    ra, dec = generate_position_data_raster(
        oneway_row=oneway_row,
        oneway_map=oneway_map,
        scan_angle=scan_angle,
        interval_factor=interval_factor,
        pointing_error=pointing_error
    )

    nmap = 2
    ndata = len(ra)
    idx = np.arange(ndata)
    nrow = ndata // 10
    rowlist = idx.reshape((nrow, 10))

    distance_list = rasterscan.get_raster_distance(ra, dec, rowlist)

    distance_list_for_map = np.arange(nrow // nmap) * interval_factor * 0.1

    if oneway_map is True:
        expected = np.concatenate(
            (distance_list_for_map, distance_list_for_map)
        )
    else:
        expected = np.concatenate(
            (distance_list_for_map, distance_list_for_map[::-1])
        )

    print(f'distance_list={distance_list}')

    assert np.allclose(distance_list, expected)


@pytest.mark.parametrize(
    'oneway_row, oneway_map, scan_angle, interval_factor, pointing_error',
    [
        (False, True, 0.0, 1.0, 0.1),
        (False, True, 30.0, 1.0, 0.1),
        (False, True, 90.0, 1.0, 0.1),
        (True, True, 0.0, 1.0, 0.1),
        (True, True, 30.0, 1.0, 0.1),
        (True, True, 90.0, 1.0, 0.1),
        (True, False, 0.0, 1.0, 0.1),
        (True, False, 30.0, 1.0, 0.1),
        (True, False, 90.0, 1.0, 0.1),
        (False, False, 0.0, 1.0, 0.1),
        (False, False, 30.0, 1.0, 0.1),
        (False, False, 90.0, 1.0, 0.1),
        (True, True, 0.0, 0.5, 0.1),
        (False, True, 0.0, 0.5, 0.1),
        (False, True, 0.0, 1.0, 0.5),
    ]
)
def test_find_raster_gap(oneway_row, oneway_map, scan_angle, interval_factor, pointing_error):
    """Test find_raster_gap."""
    raster_row = 'one-way' if oneway_row is True else 'round-trip'
    raster_map = 'one-way' if oneway_map is True else 'round-trip'
    print()
    print('===================')
    print('TEST CONFIGURATION:')
    print('  raster row:   {}'.format(raster_row))
    print('  raster map:   {}'.format(raster_map))
    print('  scan angle:   {:.1f}deg'.format(scan_angle))
    print('  row interval: {:.2f}'.format(interval_factor))
    print('  pointing error: {:.2f}'.format(pointing_error))
    print('===================')
    print()

    ra, dec = generate_position_data_raster(
        oneway_row=oneway_row,
        oneway_map=oneway_map,
        scan_angle=scan_angle,
        interval_factor=interval_factor,
        pointing_error=pointing_error
    )

    ndata = len(ra)
    idx = np.arange(ndata)
    nrow = ndata // 10
    rowlist = idx.reshape((nrow, 10))
    expected = np.array([0, nrow // 2, nrow])

    if oneway_map is False:
        # should raise RasterScanHeuristicsFailure
        with pytest.raises(rasterscan.RasterScanHeuristicsFailure):
            gaplist = rasterscan.find_raster_gap(ra, dec, rowlist)
    else:
        gaplist = rasterscan.find_raster_gap(ra, dec, rowlist)
        assert np.array_equal(gaplist, expected)
