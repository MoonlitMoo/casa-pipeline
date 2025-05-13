"""Test for heuristics defined in grouping2.py."""
from typing import Tuple

import numpy as np
import pytest

from pipeline.infrastructure import casa_tools
from .grouping2 import GroupByPosition2
from .grouping2 import GroupByTime2
from .grouping2 import MergeGapTables2
from .grouping2 import ThresholdForGroupByTime

qa = casa_tools.quanta


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
    The observation consists of four positions in 2x2 grids, (0,0),
    (0,1), (1,0), and (1,1). Each position has ten data that contains
    random noise around commanded position.

      y

      |  (0,1)   (1,1)
      |  +       +
      |
      |  (0,0)   (1,0)
      |  +       +
      |
      -----------------  x

    Returns:
        tuple: two-tuple consisting of the list of x (R.A.) and
               y (Dec.) directions
    """
    xlist = [0, 1]
    ylist = [0, 1]
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


def generate_time_data_psw() -> np.ndarray:
    """Generate time series for simulated position-switch observation.

    Generate time series for simulated position-switch observation.
    Observation consists of four fixed positions and each position
    contains ten continuous integrations. Integration time is assumed
    to be 1sec. There are time gaps between positions: 10sec, 60sec,
    and 10sec, respectively.

      0  1 ...  8  9   gap   10 ... 19   gap   ... 30 ... 39
    |--|--|...|--|--|-------|--|...|--|-------|...|--|...|--|
    |   POSITION 0  |       |  POS 1  |           |  POS 3  |

    Returns:
        np.ndarray: time series
    """
    time_list = np.arange(40, dtype=float)
    for gap, incr in [(10, 9), (20, 59), (30, 9)]:
        time_list[gap:] += incr
    return time_list


def generate_position_data_raster() -> Tuple[np.ndarray, np.ndarray]:
    """Generate position data for simulated OTF raster observation.

    Generate position data for simulated OTF raster observatin
    along x-direction (R.A.). The observation consists of two raster
    rows. Each row has twenty continuously taken data that contains
    random noise around commanded position. Scanning directions are
    opposite in these two rows.

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

    Returns:
        tuple: two-tuple consisting of the list of x (R.A.) and
               y (Dec.) directions
    """
    xlist = np.arange(0, 1, 0.05)
    xlist = [xlist, xlist[::-1]]
    ylist = [0, 1]
    rs = np.random.RandomState(seed=1234567)
    noise_mean = 0
    noise_amp = 0.00001
    ra_list = []
    dec_list = []
    for x, y in zip(xlist, ylist):
        ndata = len(x)
        xnoise = random_noise(ndata, noise_mean, noise_amp, rs)
        ynoise = random_noise(ndata, noise_mean, noise_amp, rs)
        ra_list.append(xnoise + x)
        dec_list.append(ynoise + y)
    ra_list = np.concatenate(ra_list)
    dec_list = np.concatenate(dec_list)

    return ra_list, dec_list


def generate_time_data_raster() -> np.ndarray:
    """Generate time series for simulated OTF raster observation.

    Generate time series for simulated OTF raster observation.
    Observation consists of two raster rows and each row contains
    twenty continuous integrations. Integration time is assumed to
    be 1sec. There are time gap of 10 sec between rows.

      0  1  2 ... 17 18 19   gap   20 21 ... 38 39
    |--|--|--|...|--|--|--|-------|--|--|...|--|--|
    |     RASTER ROW 0    |       |  RASTER ROW 1 |

    Returns:
        np.ndarray: time series
    """
    time_list = np.arange(40, dtype=float)
    time_list[20:] += 9
    return time_list


@pytest.mark.parametrize(
    "combine_radius, allowance_radius",
    [
        (0.4, 0.05),
        (qa.quantity(0.4, 'deg'), 0.05),
        (0.4, qa.quantity(0.05, 'deg')),
        (qa.quantity(0.4, 'deg'), qa.quantity(0.05, 'deg')),
        # unit conversion: rad -> deg
        (qa.quantity(0.4 * np.pi / 180, 'rad'), 0.05),
        # unit conversion: arcsec -> deg
        (qa.quantity(0.4 * 3600, 'arcsec'), 0.05),
    ]
)
def test_group_by_position_psw(combine_radius, allowance_radius):
    """Test grouping by position on position switch pattern."""
    ra_list, dec_list = generate_position_data_psw()
    h = GroupByPosition2()
    posdict, posgap = h.calculate(ra_list, dec_list, combine_radius, allowance_radius)
    expected_posdict = {
        0: [0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
        1: [-1, 0], 2: [-1, 0], 3: [-1, 0],
        4: [-1, 0], 5: [-1, 0], 6: [-1, 0],
        7: [-1, 0], 8: [-1, 0], 9: [-1, 0],
        10: [10, 11, 12, 13, 14, 15, 16, 17, 18, 19],
        11: [-1, 10], 12: [-1, 10], 13: [-1, 10],
        14: [-1, 10], 15: [-1, 10], 16: [-1, 10],
        17: [-1, 10], 18: [-1, 10], 19: [-1, 10],
        20: [20, 21, 22, 23, 24, 25, 26, 27, 28, 29],
        21: [-1, 20], 22: [-1, 20], 23: [-1, 20],
        24: [-1, 20], 25: [-1, 20], 26: [-1, 20],
        27: [-1, 20], 28: [-1, 20], 29: [-1, 20],
        30: [30, 31, 32, 33, 34, 35, 36, 37, 38, 39],
        31: [-1, 30], 32: [-1, 30], 33: [-1, 30],
        34: [-1, 30], 35: [-1, 30], 36: [-1, 30],
        37: [-1, 30], 38: [-1, 30], 39: [-1, 30],
    }
    expected_posgap = [10, 20, 30]
    assert posdict == expected_posdict
    assert posgap == expected_posgap


@pytest.mark.parametrize(
    "combine_radius, allowance_radius",
    [
        (0.249, 0.01),
        (qa.quantity(0.249, 'deg'), 0.01),
        (0.249, qa.quantity(0.01, 'deg')),
        (qa.quantity(0.249, 'deg'), qa.quantity(0.01, 'deg')),
    ]
)
def test_group_by_position_raster(combine_radius, allowance_radius):
    """Test grouping by position on raster pattern including some edge cases."""
    ra_list, dec_list = generate_position_data_raster()
    h = GroupByPosition2()
    posdict, posgap = h.calculate(ra_list, dec_list, combine_radius, allowance_radius)
    expected_posdict = {
        0: [0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
        1: [-1, 0], 2: [-1, 0], 3: [-1, 0],
        4: [-1, 0], 5: [-1, 0], 6: [-1, 0],
        7: [-1, 0], 8: [-1, 0], 9: [-1, 0],
        10: [10, 11, 12, 13, 14, 15, 16, 17, 18, 19],
        11: [-1, 10], 12: [-1, 10], 13: [-1, 10],
        14: [-1, 10], 15: [-1, 10], 16: [-1, 10],
        17: [-1, 10], 18: [-1, 10], 19: [-1, 10],
        20: [20, 21, 22, 23, 24, 25, 26, 27, 28, 29],
        21: [-1, 20], 22: [-1, 20], 23: [-1, 20],
        24: [-1, 20], 25: [-1, 20], 26: [-1, 20],
        27: [-1, 20], 28: [-1, 20], 29: [-1, 20],
        30: [30, 31, 32, 33, 34, 35, 36, 37, 38, 39],
        31: [-1, 30], 32: [-1, 30], 33: [-1, 30],
        34: [-1, 30], 35: [-1, 30], 36: [-1, 30],
        37: [-1, 30], 38: [-1, 30], 39: [-1, 30],
    }
    expected_posgap = [20]
    assert posdict == expected_posdict
    assert posgap == expected_posgap


def test_group_by_position_too_large_allowance_radius():
    """Test grouping by position: too large allowance radius -> all gaps are detected."""
    ra_list, dec_list = generate_position_data_raster()
    h = GroupByPosition2()
    combine_radius = 0.249
    allowance_radius = 10
    posdict, posgap = h.calculate(ra_list, dec_list, combine_radius, allowance_radius)
    expected_posdict = {
        0: [0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
        1: [-1, 0], 2: [-1, 0], 3: [-1, 0],
        4: [-1, 0], 5: [-1, 0], 6: [-1, 0],
        7: [-1, 0], 8: [-1, 0], 9: [-1, 0],
        10: [10, 11, 12, 13, 14, 15, 16, 17, 18, 19],
        11: [-1, 10], 12: [-1, 10], 13: [-1, 10],
        14: [-1, 10], 15: [-1, 10], 16: [-1, 10],
        17: [-1, 10], 18: [-1, 10], 19: [-1, 10],
        20: [20, 21, 22, 23, 24, 25, 26, 27, 28, 29],
        21: [-1, 20], 22: [-1, 20], 23: [-1, 20],
        24: [-1, 20], 25: [-1, 20], 26: [-1, 20],
        27: [-1, 20], 28: [-1, 20], 29: [-1, 20],
        30: [30, 31, 32, 33, 34, 35, 36, 37, 38, 39],
        31: [-1, 30], 32: [-1, 30], 33: [-1, 30],
        34: [-1, 30], 35: [-1, 30], 36: [-1, 30],
        37: [-1, 30], 38: [-1, 30], 39: [-1, 30],
    }
    expected_posgap = [
        1, 2, 3, 4, 5, 6, 7, 8, 9,
        10, 11, 12, 13, 14, 15, 16, 17, 18, 19,
        20, 21, 22, 23, 24, 25, 26, 27, 28, 29,
        30, 31, 32, 33, 34, 35, 36, 37, 38, 39
    ]
    assert posdict == expected_posdict
    assert posgap == expected_posgap


def test_group_by_position_moderate_allowance_radius():
    """Test grouping by position: moderate allowance radius -> no gap is detected."""
    ra_list, dec_list = generate_position_data_raster()
    h = GroupByPosition2()
    combine_radius = 0.249
    allowance_radius = 1
    posdict, posgap = h.calculate(ra_list, dec_list, combine_radius, allowance_radius)
    expected_posdict = {
        0: [0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
        1: [-1, 0], 2: [-1, 0], 3: [-1, 0],
        4: [-1, 0], 5: [-1, 0], 6: [-1, 0],
        7: [-1, 0], 8: [-1, 0], 9: [-1, 0],
        10: [10, 11, 12, 13, 14, 15, 16, 17, 18, 19],
        11: [-1, 10], 12: [-1, 10], 13: [-1, 10],
        14: [-1, 10], 15: [-1, 10], 16: [-1, 10],
        17: [-1, 10], 18: [-1, 10], 19: [-1, 10],
        20: [20, 21, 22, 23, 24, 25, 26, 27, 28, 29],
        21: [-1, 20], 22: [-1, 20], 23: [-1, 20],
        24: [-1, 20], 25: [-1, 20], 26: [-1, 20],
        27: [-1, 20], 28: [-1, 20], 29: [-1, 20],
        30: [30, 31, 32, 33, 34, 35, 36, 37, 38, 39],
        31: [-1, 30], 32: [-1, 30], 33: [-1, 30],
        34: [-1, 30], 35: [-1, 30], 36: [-1, 30],
        37: [-1, 30], 38: [-1, 30], 39: [-1, 30],
    }
    expected_posgap = []
    assert posdict == expected_posdict
    assert posgap == expected_posgap


def test_group_by_position_too_small_combine_radius():
    """Test grouping by position: too small combine radius -> all data are separated."""
    ra_list, dec_list = generate_position_data_raster()
    h = GroupByPosition2()
    combine_radius = 1e-5
    allowance_radius = 0.01
    posdict, posgap = h.calculate(ra_list, dec_list, combine_radius, allowance_radius)
    expected_posdict = {
        0: [0], 1: [1], 2: [2], 3: [3], 4: [4],
        5: [5], 6: [6], 7: [7], 8: [8], 9: [9],
        10: [10], 11: [11], 12: [12], 13: [13], 14: [14],
        15: [15], 16: [16], 17: [17], 18: [18], 19: [19],
        20: [20], 21: [21], 22: [22], 23: [23], 24: [24],
        25: [25], 26: [26], 27: [27], 28: [28], 29: [29],
        30: [30], 31: [31], 32: [32], 33: [33], 34: [34],
        35: [35], 36: [36], 37: [37], 38: [38], 39: [39],
    }
    expected_posgap = [20]
    assert posdict == expected_posdict
    assert posgap == expected_posgap


def test_group_by_position_too_large_combine_radius():
    """Test grouping by position: too large combine radius -> only one group."""
    ra_list, dec_list = generate_position_data_raster()
    h = GroupByPosition2()
    combine_radius = 10
    allowance_radius = 0.01
    posdict, posgap = h.calculate(ra_list, dec_list, combine_radius, allowance_radius)
    expected_posdict = {
        0: [0, 1, 2, 3, 4, 5, 6, 7, 8, 9,
            10, 11, 12, 13, 14, 15, 16, 17, 18, 19,
            20, 21, 22, 23, 24, 25, 26, 27, 28, 29,
            30, 31, 32, 33, 34, 35, 36, 37, 38, 39],
        1: [-1, 0], 2: [-1, 0], 3: [-1, 0], 4: [-1, 0],
        5: [-1, 0], 6: [-1, 0], 7: [-1, 0], 8: [-1, 0], 9: [-1, 0],
        10: [-1, 0], 11: [-1, 0], 12: [-1, 0], 13: [-1, 0], 14: [-1, 0],
        15: [-1, 0], 16: [-1, 0], 17: [-1, 0], 18: [-1, 0], 19: [-1, 0],
        20: [-1, 0], 21: [-1, 0], 22: [-1, 0], 23: [-1, 0], 24: [-1, 0],
        25: [-1, 0], 26: [-1, 0], 27: [-1, 0], 28: [-1, 0], 29: [-1, 0],
        30: [-1, 0], 31: [-1, 0], 32: [-1, 0], 33: [-1, 0], 34: [-1, 0],
        35: [-1, 0], 36: [-1, 0], 37: [-1, 0], 38: [-1, 0], 39: [-1, 0],
    }
    expected_posgap = [20]
    assert posdict == expected_posdict
    assert posgap == expected_posgap


def test_group_by_posiition_error():
    """Test grouping by position: error cases."""
    ra_list, dec_list = generate_position_data_psw()
    h = GroupByPosition2()

    # GroupByPosition2 only accepts numpy.ndarray for RA/DEC data
    with pytest.raises(AttributeError):
        posdict, posgap = h.calculate(ra_list.tolist(), dec_list, 0.4, 0.05)

    with pytest.raises(AttributeError):
        posdict, posgap = h.calculate(ra_list, dec_list.tolist(), 0.4, 0.05)


@pytest.mark.parametrize(
    'time_list, expected_gaps',
    [
        (generate_time_data_psw(), (5.0, 50.0)),
        (generate_time_data_raster(), (5.0, 50.0)),
    ]
)
def test_threshold_for_time(time_list, expected_gaps):
    """Test evaluation of threshold for time grouping."""
    h = ThresholdForGroupByTime()
    gaps = h.calculate(time_list)
    assert gaps == expected_gaps


@pytest.mark.parametrize(
    'time_list',
    [
        (generate_time_data_psw()),
        # list input is allowed
        (generate_time_data_psw().tolist()),
    ]
)
def test_group_by_time_psw(time_list):
    """Test grouping by time for position switch pattern."""
    h = GroupByTime2()
    time_list_np = np.asarray(time_list)
    delta_np = time_list_np[1:] - time_list_np[:-1]

    expected_time_table = [
        [[0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
         [10, 11, 12, 13, 14, 15, 16, 17, 18, 19],
         [20, 21, 22, 23, 24, 25, 26, 27, 28, 29],
         [30, 31, 32, 33, 34, 35, 36, 37, 38, 39]],
        [[0, 1, 2, 3, 4, 5, 6, 7, 8, 9,
          10, 11, 12, 13, 14, 15, 16, 17, 18, 19],
         [20, 21, 22, 23, 24, 25, 26, 27, 28, 29,
          30, 31, 32, 33, 34, 35, 36, 37, 38, 39]]
    ]
    expected_time_gap = [[10, 20, 30], [20]]

    # delta can be either np.ndarray or list
    for delta in [delta_np, delta_np.tolist()]:
        time_table, time_gap = h.calculate(time_list, delta)
        assert time_table == expected_time_table
        assert time_gap == expected_time_gap


@pytest.mark.parametrize(
    'time_list',
    [
        (generate_time_data_raster()),
        # list input is allowed
        (generate_time_data_raster().tolist()),
    ]
)
def test_group_by_time_raster(time_list):
    """Test grouping by time for raster pattern."""
    h = GroupByTime2()
    time_list_np = np.asarray(time_list)
    delta_np = time_list_np[1:] - time_list_np[:-1]

    expected_time_table = [
        [[0, 1, 2, 3, 4, 5, 6, 7, 8, 9,
          10, 11, 12, 13, 14, 15, 16, 17, 18, 19],
         [20, 21, 22, 23, 24, 25, 26, 27, 28, 29,
          30, 31, 32, 33, 34, 35, 36, 37, 38, 39]],
        [[0, 1, 2, 3, 4, 5, 6, 7, 8, 9,
          10, 11, 12, 13, 14, 15, 16, 17, 18, 19,
          20, 21, 22, 23, 24, 25, 26, 27, 28, 29,
          30, 31, 32, 33, 34, 35, 36, 37, 38, 39]]
    ]
    expected_time_gap = [[20], []]

    # delta can be either np.ndarray or list
    for delta in [delta_np, delta_np.tolist()]:
        time_table, time_gap = h.calculate(time_list, delta)
        assert time_table == expected_time_table
        assert time_gap == expected_time_gap


def test_merge_gap_tables_psw():
    """Test merging gap tables for position switch pattern."""
    time_table = [
        [[0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
         [10, 11, 12, 13, 14, 15, 16, 17, 18, 19],
         [20, 21, 22, 23, 24, 25, 26, 27, 28, 29],
         [30, 31, 32, 33, 34, 35, 36, 37, 38, 39]],
        [[0, 1, 2, 3, 4, 5, 6, 7, 8, 9,
          10, 11, 12, 13, 14, 15, 16, 17, 18, 19],
         [20, 21, 22, 23, 24, 25, 26, 27, 28, 29,
          30, 31, 32, 33, 34, 35, 36, 37, 38, 39]]
    ]
    time_gap = [[10, 20, 30], [20]]
    position_gap = [10, 20, 30]
    beam_np = np.zeros(40, dtype=int)
    h = MergeGapTables2()

    # beam can be either np.ndarray or list
    for beam in (beam_np, beam_np.tolist()):
        merge_table, merge_gap = h.calculate(time_gap, time_table, position_gap, beam)
        assert merge_table == time_table
        assert merge_gap == [position_gap, time_gap[1]]


def test_merge_gap_tables_raster():
    """Test merging gap tables for raster pattern."""
    time_table = [
        [[0, 1, 2, 3, 4, 5, 6, 7, 8, 9,
          10, 11, 12, 13, 14, 15, 16, 17, 18, 19],
         [20, 21, 22, 23, 24, 25, 26, 27, 28, 29,
          30, 31, 32, 33, 34, 35, 36, 37, 38, 39]],
        [[0, 1, 2, 3, 4, 5, 6, 7, 8, 9,
          10, 11, 12, 13, 14, 15, 16, 17, 18, 19,
          20, 21, 22, 23, 24, 25, 26, 27, 28, 29,
          30, 31, 32, 33, 34, 35, 36, 37, 38, 39]]
    ]
    time_gap = [[20], []]
    position_gap = [10, 20, 30]
    beam_np = np.zeros(40, dtype=int)

    expected_time_table = [
        [[0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
         [10, 11, 12, 13, 14, 15, 16, 17, 18, 19],
         [20, 21, 22, 23, 24, 25, 26, 27, 28, 29],
         [30, 31, 32, 33, 34, 35, 36, 37, 38, 39]],
        [[0, 1, 2, 3, 4, 5, 6, 7, 8, 9,
          10, 11, 12, 13, 14, 15, 16, 17, 18, 19,
          20, 21, 22, 23, 24, 25, 26, 27, 28, 29,
          30, 31, 32, 33, 34, 35, 36, 37, 38, 39]]
    ]

    h = MergeGapTables2()

    # beam can be either np.ndarray or list
    for beam in (beam_np, beam_np.tolist()):
        merge_table, merge_gap = h.calculate(time_gap, time_table, position_gap, beam)
        assert merge_table == expected_time_table
        assert merge_gap == [position_gap, time_gap[1]]
