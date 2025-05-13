"""Extract various informations of raster."""
import argparse
import collections
import glob
import itertools
import math
import os
import sys
from operator import sub
from matplotlib.animation import FuncAnimation, ImageMagickWriter
from matplotlib.lines import Line2D
import matplotlib.pyplot as plt
import numpy as np
import pipeline.domain.datatable as datatable
from pipeline.domain.datatable import DataTableImpl
import pipeline.infrastructure.logging as logging
from pipeline.infrastructure import casa_tools
from typing import TYPE_CHECKING, Generator, List, Optional, Tuple

from ...heuristics import rasterscan

if TYPE_CHECKING:
    from pipeline.domain.measurementset import MeasurementSet


LOG = logging.get_logger(__name__)


MetaDataSet = collections.namedtuple(
    'MetaDataSet',
    ['timestamp', 'dtrow', 'field', 'spw', 'antenna', 'ra', 'dec', 'srctype', 'pflag'])


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
    _dx = x1 - x0
    _dy = y1 - y0
    return np.hypot(_dx, _dy)


def is_multi_beam(datatable: DataTableImpl) -> bool:
    """
    Check if given dataset is multi-beam or not.

    Args:
        datatable (DataTableImpl): datatable instance

    Returns:
        bool: True if multi-beam dataset, otherwise False
    """
    return len(np.unique(datatable.getcol('BEAM'))) != 1


def extract_dtrow_list(timetable: List[List[List[int]]], for_small_gap: bool = True) -> List[np.ndarray]:
    """Convert timetable into datatable row id list."""
    tt_idx = 0 if for_small_gap else 1
    return [np.asarray(x[1]) for x in timetable[tt_idx]]


def read_readonly_data(table: DataTableImpl) -> Tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray, np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    """
    Extract necerrary data from datatable instance.

    Args:
        table: datatable instance

    Returns:
        A tuple that stores arrays of time stamps, row IDs,
        R.A., Dec., source types, antenna, field, and spw IDs
        of all rows in datable.
    """
    timestamp = table.getcol('TIME')
    dtrow = np.arange(len(timestamp))
    ra = table.getcol('OFS_RA')
    dec = table.getcol('OFS_DEC')
    srctype = table.getcol('SRCTYPE')
    antenna = table.getcol('ANTENNA')
    field = table.getcol('FIELD_ID')
    spw = table.getcol('IF')
    return timestamp, dtrow, ra, dec, srctype, antenna, field, spw


def read_readwrite_data(table: DataTableImpl) -> np.ndarray:
    """
    Extract necessary data from datatable instance.

    Args:
        table: datatable instance

    Returns:
        pflag: np.ndarray of online flag status
    """
    pflags = table.getcol('FLAG_PERMANENT')
    pflag = np.any(pflags[:, datatable.OnlineFlagIndex, :], axis=0)
    return pflag


def read_datatable(datatable: DataTableImpl) -> MetaDataSet:
    """
    Extract necessary data from datatable instance.

    Args:
        datatable: datatable instance

    Returns:
        metadata: A MetaDataSet which stores arrays of time stamps,
        row IDs, R.A., Dec., source types, antenna and field IDs
        (each is in ndarray of column values taken from datatable).
    """
    timestamp, dtrow, ra, dec, srctype, antenna, field, spw = read_readonly_data(datatable)
    pflag = read_readwrite_data(datatable)
    metadata = MetaDataSet(
        timestamp=timestamp,
        dtrow=dtrow,
        field=field,
        spw=spw,
        antenna=antenna,
        ra=ra, dec=dec,
        srctype=srctype,
        pflag=pflag)

    return metadata


def from_context(context_dir: str) -> MetaDataSet:
    """
    Read DataTable located in the context directory.

    NOTE: only one DataTable will be loaded for multi-EB run

    Args:
        context_dir: path to the pipeline context directory

    Returns:
        metadata: A MetaDataSet which stores arrays of time stamps,
        row IDs, R.A., Dec., source types, antenna and field IDs
        (each is in ndarray of column values taken from datatable).
    """
    datatable_dir = os.path.join(context_dir, 'MSDataTable.tbl')
    rotable = glob.glob(f'{datatable_dir}/*.ms/RO')[0]
    rwtable = glob.glob(f'{datatable_dir}/*.ms/RW')[0]

    tb = casa_tools.table

    tb.open(rotable)
    try:
        timestamp, dtrow, ra, dec, srctype, antenna, field, spw = read_readonly_data(tb)
    finally:
        tb.close()

    tb.open(rwtable)
    try:
        pflag = read_readwrite_data(tb)
    finally:
        tb.close()

    metadata = MetaDataSet(
        timestamp=timestamp,
        dtrow=dtrow,
        field=field,
        spw=spw,
        antenna=antenna,
        ra=ra, dec=dec,
        srctype=srctype,
        pflag=pflag)

    return metadata


def get_science_target_fields(metadata: MetaDataSet) -> np.ndarray:
    """
    Get a list of unique field IDs of science targets.

    Args:
        metadata: MetaDataSet extracted from a datatable

    Returns:
        np.ndarray of field ids for science targets
    """
    return np.unique(metadata.field[metadata.srctype == 0])


def get_science_spectral_windows(metadata: MetaDataSet) -> np.ndarray:
    """
    Get a list of unique spw IDs of science targets.

    Args:
        metadata: MetaDataSet extracted from a datatable

    Returns:
        np.ndarray of spw ids for science targets
    """
    return np.unique(metadata.spw[metadata.srctype == 0])


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


def flag_incomplete_raster(raster_index_list: List[np.ndarray], nd_raster: int, nd_row: int) -> np.ndarray:
    """
    Return IDs of incomplete raster map.

    N: number of data per raster map
    M: number of data per raster row
    MN: median of N => typical number of data per raster map
    MM: median of M => typical number of data per raster row
    logic:
      - if N[x] <= MN - MM then flag whole data in raster map x
      - if N[x] >= MN + MM then flag whole data in raster map x and later

    Args:
        raster_index_list: list of indices for metadata arrays per raster map
        nd_raster: typical number of data per raster map (MN)
        nd_row: typical number of data per raster row (MM)

    Returns:
        np.ndarray of index for raster map to flag.
    """
    nd = np.fromiter(map(len, raster_index_list), dtype=int)
    assert nd_raster >= nd_row
    upper_threshold = nd_raster + nd_row
    lower_threshold = nd_raster - nd_row
    LOG.debug('There are %s raster maps', len(nd))
    LOG.debug('number of data points per raster map: %s', nd)

    # nd exceeds upper_threshold
    test_upper = nd >= upper_threshold
    idx = np.where(test_upper)[0]
    if len(idx) > 0:
        test_upper[idx[-1]:] = True
    LOG.debug('test_upper=%s', test_upper)

    # nd is less than lower_threshold
    test_lower = nd <= lower_threshold
    LOG.debug('test_lower=%s', test_lower)

    idx = np.where(np.logical_or(test_upper, test_lower))[0]

    return idx


def flag_worm_eaten_raster(meta: MetaDataSet, raster_index_list: List[np.ndarray], nd_row: int) -> np.ndarray:
    """
    Return IDs of raster map where number of continuous flagged data exceeds upper limit given by nd_row.

    M: number of data per raster row
    MM: median of M => typical number of data per raster row
    L: maximum length of continuous flagged data
    logic:
      - if L[x] > MM then flag whole data in raster map x

    Args:
        meta: input MetaDataSet to analyze
        raster_index_list: list of indices for metadata arrays per raster map
        nd_row: typical number of data per raster row (MM)

    Returns:
        np.ndarray of index for raster map to flag.
    """
    # check if there are at least MM continuously flagged data
    # where MM is number of typical data points for one raster row
    #
    # flag
    # 1: valid, 0: invalid
    flag_raster = [meta.pflag[idx] for idx in raster_index_list]
    LOG.debug('Typical number of data per raster row: %s', nd_row)
    flag_continuous = [
        np.fromiter(
            map(sum, (f[i:i + nd_row] for i in range(len(f) - nd_row + 1))),
            dtype=int
        )
        for f in flag_raster
    ]
    min_count = np.fromiter(
        (x.min() for x in flag_continuous),
        dtype=int
    )
    LOG.debug('Minimum number of continuous valid data: %s', min_count)
    test = min_count == 0
    LOG.debug('test_result=%s', test)

    idx = np.where(test)[0]

    return idx


def get_raster_flag_list(flagged1: List[int], flagged2: List[int], raster_index_list: List[np.ndarray]) -> np.ndarray:
    """
    Merge flag result and convert raster id to list of data index.

    Args:
        flagged1: list of flagged raster id
        flagged2: list of flagged raster id
        raster_index_list: list of indices for metadata arrays per raster map

    Returns:
        np.ndarray of data ids to be flagged
    """
    flagged = set(flagged1).union(set(flagged2))
    g = (raster_index_list[i] for i in flagged)
    data_ids = np.fromiter(itertools.chain(*g), dtype=int)
    return data_ids


def flag_raster_map(datatable: DataTableImpl, ms: 'MeasurementSet', rasterscan_heuristics_result: rasterscan.RasterScanHeuristicsResult) -> List[int]:
    """
    Return list of index to be flagged by flagging heuristics for raster scan.

    Args:
        datatable (DataTableImpl): input datatable to analyze
        ms (MeasurementSet): MeasurementSet domain object
        rasterscan_heuristics_result (RasterScanHeuristicsResult): Result object of RasterScanHeuristics

    Returns:
        per-antenna list of indice to be flagged
    """
    rowdict = {}

    # rasterutil doesn't support multi-beam data
    if is_multi_beam(datatable):
        LOG.warning('Currently rasterutil does not support multi-beam data. Raster flag is not applied.')
        return rowdict

    metadata = read_datatable(datatable)
    vis = datatable.getkeyword('FILENAME')
    basename = os.path.basename(vis)
    field_list = get_science_target_fields(metadata)
    spw_list = get_science_spectral_windows(metadata)
    antenna_list = np.unique(metadata.antenna)

    # use timetable (output of grouping heuristics) to distinguish raster rows
    dtrowdict = {}
    ndrowdict = {}
    for field_id, spw_id, antenna_id in itertools.product(field_list, spw_list, antenna_list):
        try:
            timetable = datatable.get_timetable(ant=antenna_id, spw=spw_id, pol=None, ms=basename, field_id=field_id)
        except Exception:
            continue
        dtrow_list = extract_dtrow_list(timetable, for_small_gap=True)
        key = (field_id, spw_id, antenna_id)
        dtrowdict[key] = dtrow_list

        # typical number of data per raster row
        # result is consolicated per field and spectralspec (PIPE-1990)
        num_data_per_raster_row = [len(x) for x in dtrow_list]
        spectralspec = str(ms.get_spectral_window(spw_id).spectralspec)
        result_key = (field_id, spectralspec)
        ndrowdict.setdefault(result_key, []).extend(num_data_per_raster_row)

    # rastergapdict stores list of datatable row ids per raster map
    rastergapdict = {}
    ndmapdict = {}
    for key, dtrow_list in dtrowdict.items():
        # Here, we need to re-execute find_raster_gap because we cannot
        # rely on large gap stored in datatable to separate raster map.
        # That is because there might be malformed raster scan which
        # cannot be handled by rasterscan heuristic. In such case,
        # traditional grouping2 heuristic, which is time-domain heuristic,
        # is triggered.
        try:
            raster_gap = rasterscan.find_raster_gap(metadata.ra, metadata.dec, dtrow_list)
        except rasterscan.RasterScanHeuristicsFailure as e:
            LOG.debug('{} This often happens when pointing pattern deviates from regular raster. You may want to check the pointings in observation.'.format(e))
            rasterscan_heuristics_result.set_result_fail(key[2], key[1], key[0])
            # exclude combination of (field_id, spw_id, antenna_id) held by key
            # from the subsequent analysis
            continue

        idx_list = [
            np.concatenate(dtrow_list[s:e]) for s, e in zip(raster_gap[:-1], raster_gap[1:])
        ]
        rastergapdict[key] = idx_list

        # compute number of data per raster map
        # result is consolidated per field and spectralspec (PIPE-1990)
        field_id, spw_id, _ = key
        spectralspec = str(ms.get_spectral_window(spw_id).spectralspec)
        result_key = (field_id, spectralspec)
        ndmapdict.setdefault(result_key, []).extend(list(map(len, idx_list)))

    repmapdict = {}
    for key, ndmap in ndmapdict.items():
        ndrow = ndrowdict[key]
        field_id, spectralspec = key
        LOG.trace('FIELD %s spectralSpec %s: Number of data per raster row = %s', field_id, spectralspec, ndrow)
        nd_per_row_rep = find_most_frequent(ndrow)
        LOG.debug('FIELD %s spectralSpec %s: number of raster row = %s', field_id, spectralspec, len(ndrow))
        LOG.debug('FIELD %s spectralSpec %s: most frequent # of data per raster row = %s', field_id, spectralspec, nd_per_row_rep)
        nd_per_raster_rep = find_most_frequent(ndmap)
        LOG.debug('FIELD %s spectralSpec %s: number of raster map = %s', field_id, spectralspec, len(ndmap))
        LOG.debug('FIELD %s spectralSpec %s: most frequent # of data per raster map = %s', field_id, spectralspec, nd_per_raster_rep)
        LOG.debug('FIELD %s spectralSpec %s: nominal number of row per raster map = %s', field_id, spectralspec, nd_per_raster_rep // nd_per_row_rep)
        repmapdict[key] = {
            'row': nd_per_row_rep,
            'map': nd_per_raster_rep
        }

    for key, idx_list in rastergapdict.items():
        LOG.debug('Processing FIELD %s, SPW %s, ANTENNA %s', *key)

        # nominal number of data per row and per raster map
        # these numbers depend on field as well as spectral spec (PIPE-1990)
        field_id, spw_id, antenna_id = key
        spectralspec = str(ms.get_spectral_window(spw_id).spectralspec)
        result_key = (field_id, spectralspec)
        nd_per_raster_rep = repmapdict[result_key]['map']
        nd_per_row_rep = repmapdict[result_key]['row']

        # flag incomplete raster map
        flag_raster1 = flag_incomplete_raster(idx_list, nd_per_raster_rep, nd_per_row_rep)

        # flag raster map if it contains continuous flagged data
        # whose length is larger than the number of data per raster row
        flag_raster2 = flag_worm_eaten_raster(metadata, idx_list, nd_per_row_rep)

        # merge flag result and convert raster id to list of data index
        flag_list = get_raster_flag_list(flag_raster1, flag_raster2, idx_list)
        LOG.trace(flag_list)

        # convert to row list
        row_list = metadata.dtrow[flag_list]

        # key for rowdict is (spw_id, antenna_id) tuple
        new_key = (spw_id, antenna_id)
        val = rowdict.get(new_key, np.zeros(0, metadata.dtrow.dtype))
        rowdict[new_key] = np.append(val, row_list)

    # sort row numbers
    for k, v in rowdict.items():
        rowdict[k] = np.sort(v)

    return rowdict


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


def get_aspect(ax: plt.Axes) -> float:
    """
    Compute aspect ratio of matplotlib figure.

    Args:
        ax: Axes object to examine

    Returns: aspect ratio
    """
    # Total figure size
    figW, figH = ax.get_figure().get_size_inches()
    # Axis size on figure
    _, _, w, h = ax.get_position().bounds
    # Ratio of display units
    disp_ratio = (figH * h) / (figW * w)
    # Ratio of data units
    # Negative over negative because of the order of subtraction
    data_ratio = sub(*ax.get_ylim()) / sub(*ax.get_xlim())

    return disp_ratio / data_ratio


def get_angle(dx: float, dy: float, aspect_ratio: float=1) -> float:
    """
    Compute tangential angle taking into account aspect ratio.

    Args:
        dx: length along x-axis
        dy: length along y-axis
        aspect_ratio: aspect_ratio, defaults to 1

    Returns:
        tangential angle in degrees
    """
    offset = 30
    theta = math.degrees(math.atan2(dy * aspect_ratio, dx))
    return offset + theta


def anim_gen(ra: np.ndarray, dec: np.ndarray, dtrow_list: List[np.ndarray], dist_list: np.ndarray, cmap: Tuple[float, float, float, float]) -> Generator[Tuple[Optional[np.ndarray], Optional[np.ndarray], Tuple[float, float, float, float], bool], None, None]:
    """
    Generate position, color and boolean flag for generate_animation.

    Args:
        ra: np.ndarray of RA
        dec: np.ndarray of Dec
        dtrow_list: list of row ids for datatable rows per data chunk indicating
                    single raster row.
        dist_list: np.ndarray of distance
        cmap: color map

    Yields:
        position, color, and boolean flag to clear existing plot
    """
    dist_prev = 0
    cidx = 0
    raster_flag = False
    for idx, dist in zip(dtrow_list, dist_list):
        print('{} - {} = {}'.format(dist, dist_prev, dist - dist_prev))
        if dist - dist_prev < 0:
            print('updating cidx {}'.format(cidx))
            cidx = (cidx + 1) % cmap.N
            raster_flag = True
        color = cmap(cidx)
        yield ra[idx], dec[idx], color, raster_flag
        dist_prev = dist
        raster_flag = False

    raster_flag = True
    cidx = 0
    color = cmap(cidx)
    yield None, None, color, raster_flag


def animate(i: Tuple[np.ndarray, np.ndarray, Tuple[float, float, float, float], bool]) -> List[Line2D]:
    """
    Generate plot corresponding to single frame.

    Args:
        i: position, color, and boolean flag to clear existing plot

    Returns:
        lines for this frame
    """
    ra, dec, c, flag = i
    print(c)
    if flag is True:
        # clear existing raster scan
        for l in plt.gca().get_lines()[1:]:
            l.remove()
    if ra is None:
        return []

    dx = np.median(ra[1:] - ra[:-1])
    dy = np.median(dec[1:] - dec[:-1])
    aspect = get_aspect(plt.gca())
    angle = get_angle(dx, dy, aspect)
    lines = plt.plot(ra, dec, marker=(3, 0, angle), color=c, linewidth=1, markersize=6)
    return lines


def generate_animation(ra: np.ndarray, dec: np.ndarray, dtrow_list: List[np.ndarray], figfile: str = 'movie.gif') -> None:
    """
    Generate animation GIF file to illustrate observing pattern.

    Args:
        ra: np.ndarray of RA
        dec: np.ndarray of Dec
        dtrow_list: list of row ids for datatable rows per data chunk indicating
                    single raster row.
        figfile: output file name, defaults to 'movie.gif'
    """
    row_distance = get_raster_distance(ra, dec, dtrow_list)
    cmap = plt.get_cmap('tab10')
    all_rows = np.concatenate(dtrow_list)

    fig = plt.figure()
    plt.clf()
    anim = FuncAnimation(
        fig, animate,
        anim_gen(ra, dec, dtrow_list, row_distance, cmap),
        init_func=lambda: plt.plot(ra[all_rows], dec[all_rows], '.', color='gray', markersize=2),
        blit=True)
    anim.save(figfile, writer=ImageMagickWriter(fps=2))


if __name__ == '__main__':

    parser = argparse.ArgumentParser(
        description='Generate gif animation of raster pattern. Matplotlib backend must be "Agg".'
    )
    parser.add_argument('context_dir', type=str, help='context directory')
    parser.add_argument('-a', '--antenna', action='store', dest='antenna', type=int, default=0)
    parser.add_argument('-f', '--field', action='store', dest='field', type=int, default=-1)
    args = parser.parse_args()
    print('DEBUG: antenna={}'.format(args.antenna))
    print('DEBUG: field={}'.format(args.field))
    print('DEBUG: context_dir="{}"'.format(args.context_dir))

    datatable_root_dir = os.path.join(args.context_dir, 'MSDataTable.tbl')
    datatable_subdir_list = glob.glob(f'{datatable_root_dir}/*.ms')
    if len(datatable_subdir_list) == 0:
        print('Failed to find DataTable in {}'.format(args.context_dir))
        sys.exit(1)
    datatable_path = datatable_subdir_list[0]
    dt = DataTableImpl(datatable_path)
    metadata = read_datatable(dt)

    _, ms_name = os.path.split(datatable_path)
    print(f'DEBUG: ms_name="{ms_name}"')

    # pick one science spw from the list
    science_spws = get_science_spectral_windows(metadata)
    if len(science_spws) == 0:
        print(f'ERROR: no science spws exist')
        sys.exit(1)
    spw = science_spws[0]

    # Field ID to process
    science_targets = get_science_target_fields(metadata)
    print(f'DEBUG: science target list: {science_targets}')
    if args.field == -1:
        field = science_targets[0]
    else:
        field = args.field

    if field not in science_targets:
        print(f'ERROR: science target field {field} does not exist')
        sys.exit(1)

    antenna = args.antenna if args.antenna >= 0 else 0
    antenna_list = np.unique(dt.getcol('ANTENNA'))
    if antenna not in antenna_list:
        print(f'ERROR: antnena {antenna} does not exist')
        sys.exit(1)

    timetable = dt.get_timetable(
        ant=args.antenna, spw=spw, pol=None, ms=ms_name, field_id=field
    )

    ra = dt.getcol('OFS_RA')
    dec = dt.getcol('OFS_DEC')
    dtrow_list = extract_dtrow_list(timetable, for_small_gap=True)
    figfile = 'pointing.field{}ant{}.gif'.format(field, args.antenna)
    generate_animation(ra, dec, dtrow_list, figfile=figfile)
