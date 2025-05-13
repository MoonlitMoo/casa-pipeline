"""
Heuristics to detect raster rows and maps.

Examine spatial observing pattern in celestial coordinate
(R.A./Dec. etc.) to distinguish each raster row and each
raster map, which is a set of raster rows to cover whole
observing region.

Note that it does not check if observing pattern is raster.
"""
# import standard modules
from typing import Dict, List, Optional, Tuple, Union

# import 3rd party modules
import numpy as np
import numpy.typing as npt

# import pipeline submodules
from pipeline.domain.measurementset import MeasurementSet
import pipeline.infrastructure.api as api
import pipeline.infrastructure.utils.compatibility as compatibility
import pipeline.infrastructure.logging as logging

LOG = logging.get_logger(__name__)


class RasterScanHeuristicsFailure(Exception):
    """Indicates failure of RasterScanHeuristics."""


class HeuristicsParameter(object):
    """Holds tunable parameters for RasterScanHeuristic."""

    # Threshold for contiguous sequence
    AngleThreshold = 45

    # Thresholds for histogram
    AngleHistogramThreshold = 0.3
    AngleHistogramSparseness = 0.5
    AngleHistogramBinWidth = 1

    # Angle tolerance for "turn-around scan"
    TurnaroundAngleTolerance = 10

    # Distance threshold factor
    # for std
    # DistanceThresholdFactor = 5
    # for MAD
    DistanceThresholdFactor = 75

    # Distance threshold factor for raster gap detection
    RasterGapThresholdFactor = 0.05

    # Threshold for "Round-trip" raster scan detection
    RoundTripRasterScanThresholdFactor = 0.6


def distance(x0: float, y0: float, x1: float, y1: float) -> np.ndarray:
    """
    Compute distance between two points (x0, y0) and (x1, y1).

    Args:
        x0: x-coordinate value for point 0
        y0: y-coordinate value for point 0
        x1: x-coordinate value for point 1
        y1: y-coordinate value for point 1

    Returns: distance between two points
    """
    return np.hypot(x1 - x0, y1 - y0)


def generate_histogram(
    arr: np.ndarray,
    bin_width: float,
    left_edge: float,
    right_edge: float
) -> Tuple[np.ndarray, np.ndarray]:
    """Generate histogram for given array.

    Generates histogram for given array, arr, with the configuration specified by
    the parameters, bin_width, left_edge, and right_edge.

    Args:
        arr: data for histogram
        bin_width: width of the bin
        left_edge: lower limit of the histogram bin
        right_edge: upper limit of the histogram bin

    Returns:
        histogram and bin configuration as two tuples
    """
    nbin = int(np.ceil((right_edge - left_edge) / bin_width))
    return np.histogram(arr, bins=nbin, range=(left_edge, right_edge))


def detect_peak(
    hist: np.ndarray,
    mask: Optional[np.ndarray] = None
) -> Tuple[int, int, int, int]:
    """Detect peak in the given histogram.

    Detects single peak and find its range. Also compute the
    number of data included in the range.

    Args:
        hist: histogram to be examined
        mask: boolean mask array. setting True excludes the corresponding
              array element in hist from the examination. Defaults to None.

    Returns:
        Four ints, total numlber of data included in the range, and
        indices of the peak, left edge, and right edge.
    """
    if mask is None:
        mh = hist
    else:
        mh = np.ma.masked_array(hist, mask)

    ipeak = mh.argmax()
    total = mh[ipeak]
    imin = ipeak
    imax = ipeak
    LOG.trace('Detected peak %s', ipeak)
    for i in range(ipeak - 1, -1, -1):
        x = mh[i]
        if np.ma.is_masked(x) or x == 0:
            LOG.trace('break left wing %s', imin)
            break
        total += x
        imin = i
        LOG.trace('peak %s left wing %s', ipeak, i)
    for i in range(ipeak + 1, len(hist)):
        x = mh[i]
        if np.ma.is_masked(x) or x == 0:
            LOG.trace('break right wing %s', imax)
            break
        total += x
        imax = i
        LOG.trace('peak %s right wing %s', ipeak, i)
    return total, ipeak, imin, imax


def find_histogram_peak(hist: np.ndarray) -> List[int]:
    """Find histogram peaks.

    Repeatedly calls detect_peak until fraction of data included in
    the peak region reaches HeuristicsParameter.AngleHistogramThreshold.

    Args:
        hist: histogram data.

    Raises:
        RasterScanHeuristicsFailure:
            fraction of detected peak region exceeded the threshold
            specified by HeuristicsParameter.AngleHistogramSparseness

    Returns:
        List of peak indices.
    """
    total = hist.sum()
    threshold = HeuristicsParameter.AngleHistogramThreshold
    sparseness = HeuristicsParameter.AngleHistogramSparseness
    fraction = 1.0
    peak_indices = []
    mask = np.zeros(len(hist), dtype=bool)
    ss = 0
    LOG.info('hist = %s', hist.tolist())
    while fraction > threshold:
        v, ip, il, ir = detect_peak(hist, mask)
        LOG.info('Found peak: peak %s, range %s~%s, number of data %s', ip, il, ir, v)
        ss += v
        fraction = (total - ss) / total
        LOG.trace('cumulative number %s fraction %s', ss, fraction)
        mask[il:ir + 1] = True
        peak_indices.append(ip)
        LOG.info('thre %s, frac %s', threshold, fraction)
    LOG.info('peak_indices = %s', peak_indices)
    # if angle distribution is too wide, fail the heuristics
    if np.count_nonzero(mask) / len(mask) > sparseness:
        msg = 'Angle distribution is not likely to be the one for raster scan'
        LOG.warning(msg)
        raise RasterScanHeuristicsFailure(msg)
    return peak_indices


def shift_angle(
    angle: Union[float, np.ndarray],
    delta: float
) -> Union[float, np.ndarray]:
    """Shift angle or angle array by the value, delta.

    All angles must be the value in degree.

    Args:
        angle: angle value or angle array in degree.
        delta: angle shift in degree.

    Returns:
        shifted angle in degree
    """
    return (angle + 360 + delta) % 360


def find_most_frequent(v: np.ndarray) -> int:
    """
    Return the most frequent value in an input array.

    Args:
        v: data

    Returns:
        most frequent value
    """
    values, counts = np.unique(v, return_counts=True)
    max_count = counts.max()
    LOG.trace(f'max count: {max_count}')
    modes = values[counts == max_count]
    LOG.trace(f'modes: {modes}')
    if len(modes) > 1:
        mode = modes.max()
    else:
        mode = modes[0]
    LOG.trace(f'mode = {mode}')

    return mode


def refine_gaps(gap_list: List[int], num_data: int) -> np.ndarray:
    """Refine gap list by eliminating unreasonable gaps.

    When there is relatively large pointing error right before
    the turnaround of raster row, turnaround effectively spread
    two data points so that individual angles become shallow.
    As a result, additional gap is introduced at the end of raster
    row and edge point is isolated by the additional gap.
    Such isolation can be compensated by refine_gaps.

    Args:
        gap_list: original gap list
        num_data: total number of data

    Returns:
        refined gap list
    """
    gaps = gap_list[:]
    if gap_list[0] != 0:
        gaps = np.concatenate(([0], gaps))
    if gap_list[-1] != num_data:
        gaps = np.concatenate((gaps, [num_data]))
    num_data_per_row = np.diff(gaps)

    # remove gaps that causes unnatural division of data sequence
    most_frequent = find_most_frequent(num_data_per_row)

    refined_gaps = [gaps[0]]
    ntmp = 0
    for i in range(len(gaps) - 1):
        n = num_data_per_row[i]
        gap = gaps[i + 1]
        LOG.trace('idx %s, gap %s, n %s', i, gap, n)
        ntmp += n
        if ntmp >= most_frequent:
            LOG.trace('most_frequent %s, n %s, ntmp %s, registering %s', most_frequent, n, ntmp, gap)
            refined_gaps.append(gap)
            ntmp = 0
        else:
            LOG.trace('adding %s to ntmp %s', n, ntmp)
    if refined_gaps[-1] != gaps[-1]:
        refined_gaps.append(gaps[-1])

    return np.asarray(refined_gaps)


def create_range(
    peak_values: List[float],
    acceptable_deviation: float,
    angle_min: float,
    angle_max: float
) -> List[Tuple[float, float]]:
    """Create angle ranges (in degree) from peak values and width of the ranges.

    It takes into account periodic property of the angle so that
    ranges exceeding the range [angle_min, angle_max] are wrapped
    around.

    Args:
        peak_values: List of peak values in degree that become
                     the center of the ranges
        acceptable_deviation: (half-)width of the range in degree
        angle_min: minimum angle in degree
        angle_max: maximum angle in degree

    Raises:
        RuntimeError: inconsistent angle setup

    Returns:
        List of angle ranges in degree
    """
    acceptable_ranges = []
    for p in peak_values:
        upper = p + acceptable_deviation
        lower = p - acceptable_deviation
        if angle_min <= lower and upper <= angle_max:
            acceptable_ranges.append((lower, upper))
        elif angle_max < upper:
            upper = angle_min + (upper - angle_max)
            acceptable_ranges.append((lower, angle_max))
            acceptable_ranges.append((angle_min, upper))
        elif lower < angle_min:
            lower = angle_max - (angle_min - lower)
            acceptable_ranges.append((angle_min, upper))
            acceptable_ranges.append((lower, angle_max))
        else:
            msg = 'Inconsistent angle range. Aborting.'
            raise RuntimeError(msg)
    return acceptable_ranges


def find_angle_gap_by_range(
    angle_deg: np.ndarray,
    acceptable_ranges: List[Tuple[float, float]]
) -> np.ndarray:
    """Find angle gap using the range.

    Given the angle array in degree, angle_deg, find angle using the
    range specified by acceptable_ranges. When a certan angle is out of
    ranges in acceptable_ranges, it is regarded as a gap.

        ASCII illustration for angle gap index

                |                 *
         gap idx|   0   1   2   3 | 4 (gap)
                | *---*---*---*---*
        data idx| 0   1   2   3   4


    Args:
        angle_deg: angle array in degree
        acceptable_ranges: angle ranges to detect gaps.

    Returns:
        list of indices of angle gaps
    """
    mask = np.empty(len(angle_deg), dtype=bool)
    mask[:] = False
    for lower, upper in acceptable_ranges:
        in_range = np.logical_and(lower <= angle_deg, angle_deg <= upper)
        mask = np.logical_or(mask, in_range)

    angle_gap = np.where(mask == False)[0] + 1

    return angle_gap


def find_distance_gap(delta_ra: np.ndarray, delta_dec: np.ndarray) -> np.ndarray:
    """Find distance gap.

    Distance gap is detected by the condition below:

        abs(distance - median(distance)) > X * MAD(distance)

    where X is HeuristicsParameter.DistanceThresholdFactor.
    Detection process is performed three times with clipping.

        ASCII illustration for distance gap

                             (gap)
         gap idx |   0   1     2     3
                 | *---*---*-------*---*
        data idx | 0   1   2       3   4


    Args:
        delta_ra: horisontal position separation
        delta_dec: vertical position separation

    Returns:
        list of indices of distance gaps
    """
    distance = np.hypot(delta_ra, delta_dec).flatten()
    factor = HeuristicsParameter.DistanceThresholdFactor
    num_loop = 3
    distance_gap = np.zeros(0, dtype=int)
    mask = np.ones(len(distance), dtype=bool)
    num_gap_prev = 0
    for i in range(num_loop):
        dist = distance[mask]
        dmed = np.median(dist)
        # TODO: replace the following two lines with scipy function call:
        #
        # dstd = scipy.stats.median_abs_deviation(dist, scale='normal')
        #
        # This should be done after we drop support of CASA py3.6 release.
        compute_mad = compatibility.get_scipy_function_for_mad()
        dstd = compute_mad(dist)
        distance_threshold = factor * dstd
        tmp = np.abs(distance - dmed) <= distance_threshold
        idx = np.where(tmp == False)[0] + 1
        LOG.info('LOOP %s. Threshold %s. found %s gaps', i, distance_threshold, len(idx))
        if len(idx) != num_gap_prev:
            distance_gap = idx
            num_gap_prev = len(idx)
        else:
            break
        mask = np.logical_and(mask, tmp)
    distance_gap = np.unique(np.asarray(distance_gap, dtype=int))
    LOG.info('distance gap: %s', distance_gap)
    return distance, distance_gap, distance_threshold


def find_angle_gap(angle_deg: np.ndarray) -> List[int]:
    """Find angle gap.

    Find angle gap by using angle histogram. For raster scan,
    there should be one (one-way raster) or two (round-trip raster)
    angle peaks in the histogram. When deviation from the peaks exceeds
    45deg, it is regarded as angle gap. Currently the algorithm doesn't
    support one-way raster scan that all the raster rows are the same.

    Args:
        angle_deg: angle array in degree

    Raises:
        RasterScanHeuristicsFailure:
            scan pattern is not likely to be raster scan

    Returns:
        list of indices of angle gaps
    """
    bin_width = HeuristicsParameter.AngleHistogramBinWidth
    hist, bin_edges = generate_histogram(angle_deg, bin_width=bin_width,
                                         left_edge=-180, right_edge=180)
    peak_indices = find_histogram_peak(hist)
    num_peak = len(peak_indices)
    LOG.info('original angle: found %s peaks', num_peak)
    if num_peak == 3:
        # should be raster scan, but one of the raster direction is along 180deg
        # need angle shifting
        delta = 70
        angle_deg = shift_angle(angle_deg, delta)
        hist, bin_edges = generate_histogram(angle_deg, bin_width=bin_width,
                                             left_edge=0, right_edge=360 + delta)
        peak_indices = find_histogram_peak(hist)
        num_peak = len(peak_indices)
        LOG.info('shifted angle: found %s peaks', num_peak)

    peak_values = [(bin_edges[i] + bin_edges[i + 1]) / 2 for i in peak_indices]
    LOG.info('peak_values = %s', peak_values)

    # fail if scan pattern is not likely to be raster
    # check if angle distribution is
    # check if the pattern is one-way raster
    is_one_way_raster = num_peak == 1
    # check if the pattern is round-trip rasetr
    # 1. check if number of peaks in angle histogram is 2 (assuming round-trip scan)
    # 2. difference of peak angles is around 180deg
    tolerance = HeuristicsParameter.TurnaroundAngleTolerance
    is_round_trip_raster = ((num_peak == 2) and (abs(180 - abs(peak_values[1] - peak_values[0]))) <= tolerance)
    if not (is_one_way_raster or is_round_trip_raster):
        msg = 'Scan pattern is not likely to be raster scan.'
        LOG.warning(msg)
        raise RasterScanHeuristicsFailure(msg)

    # acceptable angle deviation from peak angle in degree
    acceptable_deviation = HeuristicsParameter.AngleThreshold
    angle_min = bin_edges.min()
    angle_max = bin_edges.max()
    acceptable_ranges = create_range(peak_values, acceptable_deviation, angle_min, angle_max)
    angle_gap = find_angle_gap_by_range(angle_deg, acceptable_ranges)
    LOG.info('angle gap: %s', angle_gap)

    #return angle_deg, angle_gap, hist, bin_edges, peak_indices, acceptable_ranges
    return angle_gap


def find_raster_row(ra: np.ndarray, dec: np.ndarray) -> np.ndarray:
    """Find raster rows using angle gap as primary and distance gap as secondary.

    The function tries to find angle gap with tight condition and distance
    gap with loose condition. Then, these gaps are combined by taking union.
    Finally, gap list is refined.

    Args:
        ra: horizontal position sequence
        dec: vertical position sequence

    Returns:
        list of indices that separate individual raster rows
    """
    delta_ra = ra[1:] - ra[:-1]
    delta_dec = dec[1:] - dec[:-1]

    # heuristics based on distances between integrations
    distance, distance_gap, distance_threshold = find_distance_gap(delta_ra, delta_dec)

    # heuristics based on scan direction
    angle_rad = np.arctan2(delta_dec, delta_ra).flatten()
    angle_deg = angle_rad * 180 / np.pi
    #shifted_angle, angle_gap, hist, bin_edges, peak_indices, ranges = find_angle_gap(angle_deg)
    angle_gap = find_angle_gap(angle_deg)
    merged_gap = np.union1d(angle_gap, distance_gap)
    num_data = len(ra)
    merged_gap = np.concatenate(([0], merged_gap, [num_data]))
    LOG.info('merged gap: %s', merged_gap)
    refined_gap = refine_gaps(merged_gap, num_data)
    LOG.info('final gap list: %s', refined_gap)

    #return shifted_angle, distance, refined_gap, angle_gap, distance_gap, (hist, bin_edges), peak_indices, ranges, distance_threshold
    return refined_gap


def get_raster_distance(ra: np.ndarray, dec: np.ndarray, dtrow_list: List[List[int]]) -> np.ndarray:
    """
    Compute distances between raster rows and the first row.

    Compute distances between representative positions of raster rows and that of the first raster row.
    Origin of the distance is the first raster row.
    The representative position of each raster row is the mid point (mean position) of R.A. and Dec.

    Args:
        ra: np.ndarray of RA
        dec: np.ndarray of Dec
        dtrow_list: list of row ids for datatable rows per data chunk indicating
                    single raster row.

    Returns:
        np.ndarray of the distances.
    """
    i1 = dtrow_list[0]
    x1 = ra[i1].mean()
    y1 = dec[i1].mean()

    distance_list = np.fromiter(
        (distance(ra[i].mean(), dec[i].mean(), x1, y1) for i in dtrow_list),
        dtype=float)

    return distance_list


class RasterScanHeuristicsResult():
    """Result class of raster scan analysis."""

    def __init__(self, ms: MeasurementSet):
        """Initialize an object.

        Args:
            ms (MeasurementSet): an MeasurementSet object related to the instance.
        """
        self.__ms = ms
        self.__antenna = {}
    
    @property
    def ms(self):
        return self.__ms

    @property
    def antenna(self):
        return self.__antenna
    
    def set_result_fail(self, antid: int, spwid: int, fieldid: int):
        """Set False means raster analysis failure to self.antenna dictionary that is used for getting fail antennas.

        Args:
            antid (int): An Antenna ID of the result
            spwid (int): An SpW ID of the result
            fieldid (int): A field ID of the result
        """
        self._set_result(antid, spwid, fieldid, False)

    def _set_result(self, antid: int, spwid: int, fieldid: int, result: bool):
        """Set boolean that appears the result of raster analysis to self.antenna dictionary that is used for getting fail antennas.

        Args:
            antid (int): An Antenna ID of the result
            spwid (int): An SpW ID of the result
            fieldid (int): A field ID of the result
            result (bool): result of raster analysis
        """
        field = self.antenna.setdefault(antid, {}) \
                            .setdefault(spwid, {}) \
                            .setdefault(fieldid, {})
        field[fieldid] = result

    def get_antennas_rasterscan_failed(self) -> List[str]:
        """Get antennas which have had some error in raster scan analysis of the self.ms.

        Returns:
            List[str]: List of antenna names
        """
        def _contains_fail(value:Union[str, Dict[str, Dict]]):
            if isinstance(value, dict):
                return any(_contains_fail(v) for v in value.values())
            return value is False
        
        antenna_ids = np.unique([k for k, v in self.__antenna.items() if _contains_fail(v)])
        return sorted([self.ms.antennas[id].name for id in antenna_ids])


def find_raster_gap(ra: npt.NDArray[np.float64], dec: npt.NDArray[np.float64], dtrow_list: List[npt.NDArray[np.int64]]) \
        -> npt.NDArray[np.int64]:
    """
    Find gaps between individual raster map.

    Returned list should be used in combination with timetable.
    Here is an example to plot RA/DEC data per raster map:

    Example:
    >>> import maplotlib.pyplot as plt
    >>> import numpy as np
    >>> gap = find_raster_gap(ra, dec, dtrow_list)
    >>> for s, e in zip(gap[:-1], gap[1:]):
    >>>     idx = np.concatenate(dtrow_list[s:e])
    >>>     plt.plot(ra[idx], dec[idx], '.')

    Args:
        ra: np.ndarray of RA
        dec: np.ndarray of Dec
        dtrow_list: List of np.ndarray holding array indices for ra and dec.
                    Each index array is supposed to represent single raster row.

    Raises:
        RasterScanHeuristicsFailure:
            irregular row input or unsupported raster mapping

    Returns:
        np.ndarray of index for dtrow_list indicating boundary between raster maps
    """
    msg = 'Failed to identify gap between raster map iteration.'

    if len(dtrow_list) == 0:
        raise RasterScanHeuristicsFailure(msg)

    distance_list = get_raster_distance(ra, dec, dtrow_list)
    delta_distance = distance_list[1:] - distance_list[:-1]
    threshold = HeuristicsParameter.RasterGapThresholdFactor * np.median(delta_distance)
    LOG.debug('delta_distance = %s', delta_distance)
    LOG.debug('threshold = %s', threshold)
    idx = np.where(delta_distance < threshold)[0] + 1
    raster_gap = np.concatenate([[0], idx, [len(dtrow_list)]])
    delta = raster_gap[1:] - raster_gap[:-1]
    LOG.debug('idx = %s, raster_gap = %s', idx, raster_gap)
    LOG.debug('delta = %s', delta)
    # check if the direction of raster mapping is one-way (go back to
    # the start position of previous raster frame) or round-trip
    # (start from the end position of previous raster frame)
    # For the latter case, all raster rows in the raster frames taken
    # along returning direction are separated by the above threshold
    # so that most of the elements in delta become 1
    ndelta1 = len(np.where(delta == 1)[0])
    LOG.debug('ndelta1 = %s', ndelta1)
    if ndelta1 > HeuristicsParameter.RoundTripRasterScanThresholdFactor * len(raster_gap):
        # possibly round-trip raster mapping which is not supported
        raise RasterScanHeuristicsFailure(msg)
    return raster_gap


class RasterScanHeuristic(api.Heuristic):
    """Heuristic to analyze raster scan pattern."""

    def calculate(self, ra: npt.NDArray[np.float64], dec: npt.NDArray[np.float64]) \
                      -> Tuple[List[npt.NDArray[np.int64]], List[npt.NDArray[np.int64]]]:
        """Detect gaps that separate individual raster rows and raster maps.

        Detected gaps are transrated into TimeTable and TimeGap described below.

        Args:
            ra: horizontal position list
            dec: vertical position list

        Raises:
            RasterScanHeuristicsFailure (raised from find_raster_row() or find_raster_gap()):
                scan pattern is not likely to be raster scan or
                irregular row input, or unsupported raster mapping

        Returns:
            Two-tuple containing information on group membership
            (TimeTable) and boundaries between groups (TimeGap).

            TimeTable is the "list-of-list" whose items are the set
            of indices for each group. TimeTable[0] is the groups
            separaged by "small" gap while TimeTable[1] is for
            groups separated by "large" gap. They are used for
            baseline subtraction (hsd_baseline) and subsequent
            flagging (hsd_blflag).

            TimeTable:
                [[[ismall00,...,ismall0M],[...],...,[ismallX0,...,ismallXN]],
                 [[ilarge00,...,ilarge0P],[...],...,[ilargeY0,...,ilargeYQ]]]
            TimeTable[0]: separated by small gaps
            TimeTable[1]: separated by large gaps

            TimeGap is the list of indices which indicate boundaries
            for "small" and "large" gaps. The "small" gap is a merged
            list of gaps for groups separated by small time gaps and
            the ones grouped by positions. These are used for plotting.

            TimeGap: [[rowX1, rowX2,...,rowXN], [rowY1, rowY2,...,rowYN]]
            TimeGap[0]: small gap
            TimeGap[1]: large gap
        """
        gaplist_row = find_raster_row(ra, dec)
        # gaplist_row = ret[2]
        idx_iter = zip(gaplist_row[:-1], gaplist_row[1:])
        dtrow_list = [np.arange(s, e, dtype=int) for s, e in idx_iter]
        gaplist_map = find_raster_gap(ra, dec, dtrow_list)
        LOG.info('large gap list: %s', gaplist_map)

        # construct return value that is compatible with grouping2 heuristics
        # - gaps for raster row correspond to "small" time gap
        # - gaps for raster map correspond to "large" time gap
        gap_small = gaplist_row[1:-1]
        gap_large = [gaplist_row[i] for i in gaplist_map[1:-1]]
        gaplist = [gap_small, gap_large]

        table_small = dtrow_list
        idx_iter = zip(gaplist_map[:-1], gaplist_map[1:])
        table_large = [np.concatenate(dtrow_list[s:e]) for s, e in idx_iter]
        gaptable = [table_small, table_large]
        return gaptable, gaplist
