"""A collection of Single Dish utility methods and classes."""
import collections
import contextlib
import datetime
import functools
import os
import sys
import time
from logging import Logger as pyLogger
from typing import Any, Callable, Dict, Generator, Iterable, List, NewType, Optional, Sequence, Union, Tuple

from astropy.time import Time

# Imported for annotation pupose only. Use table in casa_tools in code.
from casatools import table as casa_table

import numpy

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.logging as logging
from pipeline.domain import DataTable, Field, MeasurementSet, ObservingRun
from pipeline.domain.datatable import OnlineFlagIndex
from pipeline.infrastructure import Context
from pipeline.infrastructure import casa_tools
from pipeline.infrastructure.utils import absolute_path, relative_path
from . import compress

LOG = infrastructure.get_logger(__name__)

TableLike = NewType('TableLike',
                    Union[casa_tools._logging_table_cls, casa_table])


def profiler(func: Callable):
    """
    Measure execution time of a decorated function.

    Args:
        func: A function to be decorated.
    """
    @functools.wraps(func)
    def wrapper(*args, **kw):
        """Measure execution time of a function and print it to a logger."""
        start = time.time()
        result = func(*args, **kw)
        end = time.time()

        LOG.info('#PROFILE# %s: elapsed %s sec' % (func.__name__, end - start))

        return result
    return wrapper


def require_virtual_spw_id_handling(observing_run: ObservingRun) -> bool:
    """
    Test if SpW IDs vary across MeasurementSets.

    Args:
        observing_run: An ObservingRun instance to investigate.

    Returns:
        True if SpW IDs across MeasurementSets in the ObservingRun.
    """
    return numpy.any([spw.id != observing_run.real2virtual_spw_id(spw.id, ms) for ms in observing_run.measurement_sets
                      for spw in ms.get_spectral_windows(science_windows_only=True)])


def convert_spw_virtual2real(context, spw_in: Union[str, int],
                             mses: List[MeasurementSet] = []) -> Dict[str, str]:
    """Convert virtual spw selection into real spw selection.

    Real spw selection can be different among MSes. This method returns
    the dictionary of real spw selections for each MS.

    Args:
        context: pipeline context
        spw_in: virtual spw selection string (a comma separated list of
            virtual spw IDs) or an integer ID
        mses: list of measurementset domain objects (optional)
    Returns:
        real spw selection per MS
    """
    spw_out = {}
    observing_run = context.observing_run
    ms_list = mses if mses else context.observing_run.measurement_sets
    if isinstance(spw_in, int):
        spw_in = str(spw_in)
    elif not isinstance(spw_in, str):
        raise TypeError('spw_in must be string or integer.')
    # construct per MS real SpW ID selection dictionary
    if len(spw_in) == 0:
        spw_out = dict((ms.name, '') for ms in ms_list)
    else:
        # only supports comma-separated spw id list
        vspw_list = [int(v) for v in spw_in.split(',')]
        for ms in ms_list:
            origin_ms = observing_run.get_ms(ms.origin_ms)
            spw_list = [
                observing_run.virtual2real_spw_id(vspw, origin_ms)
                for vspw in vspw_list
            ]
            spw_out[ms.name] = ','.join(map(str, spw_list))

    return spw_out


def is_nro(context: Context) -> bool:
    """
    Test if processing Nobeyama data or not.

    This methods identifies Nobeyama data if all antennas in all
    MeasurementSets are Nobeyama ones.

    Args:
        context: A Pipeline Context to be tested.

    Returns:
         True if identified as Nobeyama data.
    """
    mses = context.observing_run.measurement_sets
    return numpy.all([ms.antenna_array.name == 'NRO' for ms in mses])


def asdm_name_from_ms(ms_domain: MeasurementSet) -> str:
    """
    Parse a name of MeasurementSet (MS) and return the ASDM name.

    Return the name of original ASDM from which a given MS is created.
    Assumptions are:
       - MS is generated from an ASDM
       - MS name is <uid>.ms

    Args:
        ms_doemain: An MS domain object.

    Returtns:
        The name of ASDM.
    """
    ms_basename = ms_domain.basename
    index_for_suffix = ms_basename.rfind('.')
    asdm = ms_basename[:index_for_suffix] if index_for_suffix > 0 else ms_basename
    return asdm

def get_ms_idx(context: Context, msname: str) -> int:
    """
    Return an index of a given MeasurementSet (MS) in Pipeline Context.

    Args:
        context: A Pipeline Context to be investigated.
        msname: A name of MS to look into.

    Returns:
        An index of MS in Context. The return value is -1 if no match is found.
    """
    mslist = context.observing_run.measurement_sets
    idx_found = -1
    for idx in range(len(mslist)):
        msobj = mslist[idx]
        search_list = [msobj.name, msobj.basename]
        if msname in search_list:
            idx_found = idx
            break
    return idx_found

def get_data_table_path(context: Context, msobj: MeasurementSet) -> str:
    """
    Return the path of DataTable.

    Args:
        context: A Pipeline Context
        msobj: An MS domain object to get DataTable path for

    Returns:
        A relative path of DataTable of a given msobj
    """
    origin_ms_name = os.path.basename(msobj.origin_ms)
    return relative_path(os.path.join(context.observing_run.ms_datatable_name, origin_ms_name))

def match_origin_ms(ms_list: List[MeasurementSet], origin_name: str) -> MeasurementSet:
    """
    Return an MS domain object that has the same origin_ms as origin_name.

    Args:
        ms_list: List of MS domain objects to match.
        origin_name: The name of origin MS

    Returns:
        MS domain object of the first matching MS. Return None if not match is
        found.
    """
    for ms in ms_list:
        if ms.origin_ms == origin_name:
            return ms
    return None


class ProgressTimer(object):
    """
    Show the progress bar on the console.

    The progress bar is shown only if a given LogLevel is higher than INFO.

    Attributes:
        currentLevel: A current progress (w.r.t. the length of pregress bar).
        curCount: A current count (w.r.t. the maximum count).
        LogLevel: A log level of progress bar.
        maxCount: The maximum number of count to be considered as 100%.
        scale: A scale factor used to calculate progress.
    """

    def __init__(self, length: int=80, maxCount: int=80,
                 LogLevel: Union[int, str]='info'):
        """
        Initialize ProgressTimer class.

        Args:
            length: The length of the progress bar in the numebr of characters.
            macCount: The maximum number of count to be considered as 100%.
            LogLevel: The log level of progress bar.
        """
        self.currentLevel = 0
        self.maxCount = maxCount
        self.curCount = 0
        self.scale = float(length)/float(maxCount)
        if isinstance(LogLevel, str):
            self.LogLevel = logging.LOGGING_LEVELS[LogLevel] if LogLevel in logging.LOGGING_LEVELS else logging.INFO
        else:
            self.LogLevel = LogLevel
        if self.LogLevel >= logging.INFO:
            print('\n|{} 100% {}|'.format('=' * ((length - 8) // 2), '=' * ((length - 8) // 2)))

    def __del__(self):
        """Destructor of ProgressTimer."""
        if self.LogLevel >= logging.INFO:
            print('\n')

    def count(self, increment: int=1):
        """
        Advance progress bar.

        Args:
            increment: A count to be prgressed.
        """
        if self.LogLevel >= logging.INFO:
            self.curCount += increment
            newLevel = int(self.curCount * self.scale)
            if newLevel != self.currentLevel:
                print('\b{}'.format('*' * (newLevel - self.currentLevel)))
                sys.stdout.flush()
                self.currentLevel = newLevel


# parse edge parameter to tuple
def parseEdge(edge: Union[float, List[float]]) -> Tuple[float, float]:
    """
    Convert a given edge value to a two-element-tuple.

    Args:
        edge: An edge value.

    Returns:
        A given edge is converted to a tuple with two elements each indicates
        the left and right edge (channels), respectively.

    Examples:
        >>> parseEdge(100)
        (100, 100)
        >>> parseEdge([50, 70])
        (50, 70)
        >>> parseEdge([0, 1, 2])
        (0, 1)
    """
    if isinstance(edge, int) or isinstance(edge, float):
        EdgeL = edge
        EdgeR = edge
    elif len(edge) == 0:
        EdgeL = 0
        EdgeR = 0
    elif len(edge) == 1:
        EdgeL = edge[0]
        EdgeR = edge[0]
    else:
        (EdgeL, EdgeR) = edge[:2]
    return EdgeL, EdgeR


def mjd_to_datetime(val: float) -> datetime.datetime:
    """Convert MJD to datetime instance.

    Args:
        val: MJD value in day.

    Returns:
        datetime.datetime: datetime instance
    """
    t = Time(val, format='mjd')
    date_time = t.datetime
    return date_time


def mjd_to_datestring(t: float, unit: str='sec') -> str:
    """
    Convert a given Modified Julian Date (MJD) to a date string.

    Args:
        t: An MJD in UTC.
        unit: The unit of t. Supported units are 'sec' and 'day'.

    Returns:
        A date string. Returns the origin of MJD if unsupported unit is given.

    Examples:
        The default unit ('sec') example
        >>> mjd_to_datestring(5113612512.0)
        'Wed Dec  2 07:55:12 2020 UTC'

        >>> mjd_to_datestring(59185.33, 'day')
        'Wed Dec  2 07:55:12 2020 UTC'

        Invalid unit example
        >>> mjd_to_datestring(85226875.2, 'min')
        'Wed Nov 17 00:00:00 1858 UTC'
    """
    if unit in ['sec', 's']:
        # 1 day = 24hour * 60min * 60sec = 86400sec
        mjd = t / 86400.0
    elif unit in ['day', 'd']:
        mjd = t
    else:
        mjd = 0.0
    date_time = mjd_to_datetime(mjd)
    mjdstr = time.asctime(date_time.timetuple()) + ' UTC'
    return mjdstr


def to_list(s: Any) -> Optional[Sequence[Any]]:
    """
    Convert the input argument to a list.

    Args:
        s: Target of conversion.

    Retruns:
        A list. No conversion is done if input is numpy.ndarray or None.

    Examples:
        >>> to_list(5)
        [5]
        >>> to_list([2.5, 5])
        [2.5, 5]
        >>> import numpy
        >>> to_list(numpy.array([2.5, 5]))
        array([2.5, 5. ])
        >>> to_list('5')
        [5.0]
        >>> to_list('[2.5, 5]')
        [2.5, 5]
        >>> to_list('pipeline')
        ['pipeline']
        >>> to_list('[a,b,c]')
        ['a', 'b', 'c']
        >>> to_list('pipeline,casa')
        ['pipeline,casa']
        >>> to_list(dict(a=1, b=2))
        [{'a': 1, 'b': 2}]
        >>> to_list((2.5, 5))
        [(2.5, 5)]
    """
    if s is None:
        return None
    elif isinstance(s, list) or isinstance(s, numpy.ndarray):
        return s
    elif isinstance(s, str):
        if s.startswith('['):
            if s.lstrip('[')[0].isdigit():
                return eval(s)
            else:
                # maybe string list
                return eval(s.replace('[', '[\'').replace(']', '\']').replace(',', '\',\''))
        else:
            try:
                return [float(s)]
            except:
                return [s]
    else:
        return [s]


def to_bool(s: Any) -> Union[bool, str, None]:
    """
    Convert the input argument to a bool.

    Args:
        s: Target of conversion.

    Retruns:
        A bool. No conversion is done if input is None or string that does not
        interpret as True/False.

    Examples:
        >>> to_bool(False)
        False
        >>> to_bool('True')
        True
        >>> to_bool(1.5)
        True
        >>> to_bool('Some_string')
        'Some_string'
    """
    if s is None:
        return None
    elif isinstance(s, bool):
        return s
    elif isinstance(s, str):
        if s.upper() == 'FALSE':
            return False
        elif s.upper() == 'TRUE':
            return True
        else:
            return s
    else:
        return bool(s)


def to_numeric(s: Any) -> Any:
    """
    Convert the input argument to a number.

    Args:
        s: Target of conversion.

    Retruns:
        A value converted to a number. No conversion is done if input is not a
        string.

    Examples:
        >>> to_numeric(5)
        5
        >>> to_numeric('5')
        5.0
    """
    if s is None:
        return None
    elif isinstance(s, str):
        try:
            return float(s)
        except:
            return s
    else:
        return s


def get_mask_from_flagtra(flagtra: Sequence[int]) -> numpy.ndarray:
    """
    Convert a flag array (0=valid, 1=flagged) to mask (1=valid, 0=flagged).

    Args:
        flagtra: A flag array (0=valid, 1=flagged).

    Retruns:
        An integer array of mask (1=valid, 0=flagged).

    Example:
        >>> get_mask_from_flagtra([1, 0, 0, 1])
        array([0, 1, 1, 0])
    """
    return (numpy.asarray(flagtra) == 0).astype(int)


def iterate_group_member(group_desc: dict,
                         member_id_list: List[int]
                         ) -> Iterable[Tuple[MeasurementSet, int, int, int]]:
    """
    Yeild reduction group members.

    Args:
        group_desc: A reduction group dictionary. Keys of the dictionary are
            group IDs and values are
            pipeline.domain.singledish.MSReductionGroupDesc instances.
        member_id_list: A list of member IDs in group_desc to yield

    Yields:
        A tuple of MeasurementSet instance, field, antenna, and SpW IDs.
    """
    for mid in member_id_list:
        member = group_desc[mid]
        yield member.ms, member.field_id, member.antenna_id, member.spw_id


def get_index_list_for_ms(datatable: DataTable, origin_vis_list: List[str],
                          antennaid_list: List[int], fieldid_list: List[int],
                          spwid_list: List[int])  -> numpy.ndarray:
    """
    Return an array of row IDs in datatable that matches selection.

    Args:
        datatable: A datatable instance.
        origin_vis_list: A list of origin MeasurementSet (MS) name.
        antennaid_list: A list of antenna IDs to select for a correspoinding
            elements of vis_list.
        fieldid_list: A list of field IDs to select for a correspoinding
            elements of vis_list.
        spwid_list: A list of SpW IDs to select for a correspoinding
            elements of vis_list.

    Retruns:
        An array of row IDs in datatable
    """
    return numpy.fromiter(_get_index_list_for_ms(datatable, origin_vis_list,
                                                 antennaid_list, fieldid_list,
                                                spwid_list), dtype=numpy.int64)


def _get_index_list_for_ms(datatable: DataTable, origin_vis_list: List[str],
                           antennaid_list: List[int], fieldid_list: List[int],
                           spwid_list: List[int]
                           ) -> Generator[int, None, None]:
    """
    Yield row IDs in datatable that matches given selection criteria.

    Args:
        datatable: A datatable instance.
        origin_vis_list: A list of origin MeasurementSet (MS) name.
        antennaid_list: A list of antenna IDs to select for a correspoinding
            elements of vis_list.
        fieldid_list: A list of field IDs to select for a correspoinding
            elements of vis_list.
        spwid_list: A list of SpW IDs to select for a correspoinding
            elements of vis_list.

    Yields:
        Row IDs in datatable
    """
    # use time_table instead of data selection
    #online_flag = datatable.getcolslice('FLAG_PERMANENT', [0, OnlineFlagIndex], [-1, OnlineFlagIndex], 1)[0]
    #LOG.info('online_flag=%s'%(online_flag))
    for (_vis, _field, _ant, _spw) in zip(origin_vis_list, fieldid_list, antennaid_list, spwid_list):
        try:
            time_table = datatable.get_timetable(_ant, _spw, None, os.path.basename(_vis), _field)
        except RuntimeError as e:
            # data could be missing. just skip.
            LOG.warning('Exception reported from datatable.get_timetable:')
            LOG.warning(str(e))
            continue
        # time table separated by large time gap
        the_table = time_table[1]
        for group in the_table:
            for row in group[1]:
                permanent_flag = datatable.getcell('FLAG_PERMANENT', row)
                online_flag = permanent_flag[:, OnlineFlagIndex]
                if any(online_flag == 1):
                    yield row


def get_index_list_for_ms2(datatable_dict: dict, group_desc: dict,
                           member_list: List[int]) -> collections.defaultdict:
    """
    Return row IDs of datatable of selected reductions group members.

    Args:
        datatable_dict: A dictionary that stores DataTable (values) of each
            MeasurementSet (MS). Keys of the dictionary is the name of MS.
        group_desc: A reduction group dictionary. Keys of the dictionary are
            group IDs and values are
            pipeline.domain.singledish.MSReductionGroupDesc instances.
        member_id_list: A list of member IDs in group_desc to yield.

    Returns:
        Keys of the returned dictionary are names of origin MSes and values are
        numpy arrays of row IDs in corresponding datatables.
    """
    # use time_table instead of data selection
    index_dict = collections.defaultdict(list)
    for (_ms, _field, _ant, _spw) in iterate_group_member(group_desc, member_list):
        print('{0} {1} {2} {3}'.format(_ms.basename, _field, _ant, _spw))
        origin_ms_basename = os.path.basename(_ms.origin_ms)
        datatable = datatable_dict[origin_ms_basename]
        time_table = datatable.get_timetable(_ant, _spw, None,
                                             origin_ms_basename, _field)
        # time table separated by large time gap
        the_table = time_table[1]
        def _g():
            for group in the_table:
                for row in group[1]:
                    permanent_flag = datatable.getcell('FLAG_PERMANENT', row)
                    online_flag = permanent_flag[:, OnlineFlagIndex]
                    if any(online_flag == 1):
                        yield row
        arr = numpy.fromiter(_g(), dtype=numpy.int64)
        index_dict[origin_ms_basename].extend(arr)
    for vis in index_dict:
        index_dict[vis] = numpy.asarray(index_dict[vis])
    return index_dict


# TODO (ksugimoto): refactor get_valid_ms_members
def get_valid_ms_members(group_desc: dict, msname_filter: List[str],
                         ant_selection: str, field_selection: str,
                         spw_selection: Union[str, dict]) -> Generator[int, None, None]:
    """
    Yield IDs of reduction groups that matches selection criteria.

    Args:
        group_desc: A reduction group dictionary. Keys of the dictionary are
            group IDs and values are
            pipeline.domain.singledish.MSReductionGroupDesc instances.
        msname_filter: Names of MeasurementSets to select.
        ant_selection: Antenna selection syntax.
        field_selection: Field selection syntax.
        spw_selection: SpW selection syntax. It can be string or dictionary
                       containing per-MS spw selection string. Keys for the
                       dictionary should be absolute path to the MS.

    Yields:
        IDs of reduction group.
    """
    for member_id in range(len(group_desc)):
        member = group_desc[member_id]
        spw_id = member.spw_id
        field_id = member.field_id
        ant_id = member.antenna_id
        msobj = member.ms
        if absolute_path(msobj.name) in [absolute_path(name) for name in msname_filter]:
            _field_selection = field_selection
            try:
                nfields = len(msobj.fields)
                if len(field_selection) == 0:
                    # fine, go ahead
                    pass
                elif not field_selection.isdigit():
                    # selection by name, bracket by ""
                    LOG.debug('non-digit field selection')
                    if not _field_selection.startswith('"'):
                        _field_selection = '"{}"'.format(field_selection)
                else:
                    tmp_id = int(field_selection)
                    LOG.debug('field_id = {}'.format(tmp_id))
                    if tmp_id < 0 or nfields <= tmp_id:
                        # could be selection by name consisting of digits, bracket by ""
                        LOG.debug('field name consisting digits')
                        if not _field_selection.startswith('"'):
                            _field_selection = '"{}"'.format(field_selection)
                LOG.debug('field_selection = "{}"'.format(_field_selection))

                if isinstance(spw_selection, str):
                    _spw_selection = spw_selection
                elif isinstance(spw_selection, dict):
                    _spw_selection = spw_selection.get(msobj.name, '')
                else:
                    _spw_selection = ''
                LOG.debug(f'spw_selection = {_spw_selection}')

                mssel = casa_tools.ms.msseltoindex(vis=msobj.name, spw=_spw_selection,
                                                   field=_field_selection, baseline=ant_selection)
            except RuntimeError as e:
                LOG.trace('RuntimeError: {0}'.format(str(e)))
                LOG.trace('vis="{0}" field_selection: "{1}"'.format(msobj.name, _field_selection))
                continue
            spwsel = mssel['spw']
            fieldsel = mssel['field']
            antsel = mssel['antenna1']
            if ((len(spwsel) == 0 or spw_id in spwsel) and
                    (len(fieldsel) == 0 or field_id in fieldsel) and
                    (len(antsel) == 0 or ant_id in antsel)):
                yield member_id


# TODO (ksugimoto): Move this to casa_tools module.
@contextlib.contextmanager
def TableSelector(name: str, query: str) -> casa_table:
    """
    Retun a CASA table tool instance of selected rows of a table.

    Select a table rows with a query string and return a CASA table tool
    instance with selection.

    Args:
        name: A path to table to be selected.
        query: A query string to select table rows.

    Returns:
        CASA table tool instance with row selection.
    """
    with casa_tools.TableReader(name) as tb:
        tsel = tb.query(query)
        yield tsel
        tsel.close()


class EchoDictionary(dict):
    """Dictionary that always returns key."""

    def __getitem__(self, x):
        """Destructor of EchoDictionary class."""
        return x

def make_row_map_between_ms(src_ms: MeasurementSet, derived_vis: str,
                            table_container=None) -> dict:
    """
    Make row mapping between source and derived MSes.

    Args:
        src_ms: An MS domain object of source MS.
        derived_vis: A name of derived MS
        table_container: A container class that stores table tool instances
            of calibrated and associating MS.

    Returns:
        A row mapping dictionary. A key is row ID of source MS and
        a corresponding value is that of derived MS.
    """
    src_tb = None
    derived_tb = None
    if table_container is not None:
        src_tb = table_container.tb1
        derived_tb = table_container.tb2

    return make_row_map(src_ms, derived_vis, src_tb, derived_tb)

#@profiler
def make_row_map(src_ms: MeasurementSet, derived_vis: str,
                 src_tb: Optional[TableLike]=None,
                 derived_tb: Optional[TableLike]=None) -> dict:
    """
    Make row mapping between a source and a derived MeasurementSet (MS).

    Args:
        src_ms: An MS domain object of source MS.
        derived_vis: A name of the MS that derives from the source MS.
        src_tb: A table tool instance of a source MS.
            The src_ms is used if not specified.
        derived_tb: A table tool instance of a derived MS.
            The derived_vis is used if not specified.

    Returns:
        A row mapping dictionary. A key is row ID of calibrated MS and
        a corresponding value is that of baselined MS.
    """
    vis0 = src_ms.name
    vis1 = derived_vis

    rowmap = {}

    if vis0 == vis1:
        return EchoDictionary()

    # make polarization map between src MS and derived MS
    to_derived_polid = make_polid_map(vis0, vis1)
    LOG.trace('to_derived_polid=%s' % to_derived_polid)

    # make spw map between src MS and derived MS
    to_derived_spwid = make_spwid_map(vis0, vis1)
    LOG.trace('to_derived_spwid=%s' % to_derived_spwid)

    # make a map between (polid, spwid) pair and ddid for derived MS
    derived_ddid_map = make_ddid_map(vis1)
    LOG.trace('derived_ddid_map=%s' % derived_ddid_map)

    scans = src_ms.get_scans(scan_intent='TARGET')
    scan_numbers = [s.id for s in scans]
    fields = {}
    states = {}
    for scan in scans:
        fields[scan.id] = [f.id for f in scan.fields if 'TARGET' in f.intents]
        states[scan.id] = [s.id for s in scan.states if 'TARGET' in s.intents]
    field_values = list(fields.values())
    is_unique_field_set = True
    for v in field_values:
        if v != field_values[0]:
            is_unique_field_set = False
    state_values = list(states.values())
    is_unique_state_set = True
    for v in state_values:
        if v != state_values[0]:
            is_unique_state_set = False
    if is_unique_field_set and is_unique_state_set:
        taql = 'ANTENNA1 == ANTENNA2 && SCAN_NUMBER IN %s && FIELD_ID IN %s && STATE_ID IN %s' % (scan_numbers, field_values[0], state_values[0])
    else:
        taql = 'ANTENNA1 == ANTENNA2 && (%s)' % (' || '.join(['(SCAN_NUMBER == %s && FIELD_ID IN %s && STATE_ID IN %s)' % (scan, fields[scan], states[scan]) for scan in scan_numbers]))
    LOG.trace('taql=\'%s\'' % (taql))

    with casa_tools.TableReader(os.path.join(vis0, 'OBSERVATION')) as tb:
        nrow_obs0 = tb.nrows()
    with casa_tools.TableReader(os.path.join(vis0, 'PROCESSOR')) as tb:
        nrow_proc0 = tb.nrows()
    with casa_tools.TableReader(os.path.join(vis1, 'OBSERVATION')) as tb:
        nrow_obs1 = tb.nrows()
    with casa_tools.TableReader(os.path.join(vis1, 'PROCESSOR')) as tb:
        nrow_proc1 = tb.nrows()

    assert nrow_obs0 == nrow_obs1
    assert nrow_proc0 == nrow_proc1

    is_unique_observation_id = nrow_obs0 == 1
    is_unique_processor_id = nrow_proc0 == 1

    if src_tb is None:
        with casa_tools.TableReader(vis0) as tb:
            tsel = tb.query(taql)
            try:
                if is_unique_observation_id:
                    observation_id_list0 = None
                    observation_id_set = {0}
                else:
                    observation_id_list0 = tsel.getcol('OBSERVATION_ID')
                    observation_id_set = set(observation_id_list0)
                if is_unique_processor_id:
                    processor_id_list0 = None
                    processor_id_set = {0}
                else:
                    processor_id_list0 = tsel.getcol('PROCESSOR_ID')
                    processor_id_set = set(processor_id_list0)
                scan_number_list0 = tsel.getcol('SCAN_NUMBER')
                field_id_list0 = tsel.getcol('FIELD_ID')
                antenna1_list0 = tsel.getcol('ANTENNA1')
                state_id_list0 = tsel.getcol('STATE_ID')
                data_desc_id_list0 = tsel.getcol('DATA_DESC_ID')
                time_list0 = tsel.getcol('TIME')
                rownumber_list0 = tsel.rownumbers()
            finally:
                tsel.close()
    else:
        tsel = src_tb.query(taql)
        try:
            if is_unique_observation_id:
                observation_id_list0 = None
                observation_id_set = {0}
            else:
                observation_id_list0 = tsel.getcol('OBSERVATION_ID')
                observation_id_set = set(observation_id_list0)
            if is_unique_processor_id:
                processor_id_list0 = None
                processor_id_set = {0}
            else:
                processor_id_list0 = tsel.getcol('PROCESSOR_ID')
                processor_id_set = set(processor_id_list0)
            scan_number_list0 = tsel.getcol('SCAN_NUMBER')
            field_id_list0 = tsel.getcol('FIELD_ID')
            antenna1_list0 = tsel.getcol('ANTENNA1')
            state_id_list0 = tsel.getcol('STATE_ID')
            data_desc_id_list0 = tsel.getcol('DATA_DESC_ID')
            time_list0 = tsel.getcol('TIME')
            rownumber_list0 = tsel.rownumbers()
        finally:
            tsel.close()

    if derived_tb is None:
        with casa_tools.TableReader(vis1) as tb:
            tsel = tb.query(taql)
            try:
                if is_unique_observation_id:
                    observation_id_list1 = None
                else:
                    observation_id_list1 = tsel.getcol('OBSERVATION_ID')
                if is_unique_processor_id:
                    processor_id_list1 = None
                else:
                    processor_id_list1 = tsel.getcol('PROCESSOR_ID')
                scan_number_list1 = tsel.getcol('SCAN_NUMBER')
                field_id_list1 = tsel.getcol('FIELD_ID')
                antenna1_list1 = tsel.getcol('ANTENNA1')
                state_id_list1 = tsel.getcol('STATE_ID')
                data_desc_id_list1 = tsel.getcol('DATA_DESC_ID')
                time_list1 = tsel.getcol('TIME')
                rownumber_list1 = tsel.rownumbers()
            finally:
                tsel.close()
    else:
        tsel = derived_tb.query(taql)
        try:
            if is_unique_observation_id:
                observation_id_list1 = None
            else:
                observation_id_list1 = tsel.getcol('OBSERVATION_ID')
            if is_unique_processor_id:
                processor_id_list1 = None
            else:
                processor_id_list1 = tsel.getcol('PROCESSOR_ID')
            scan_number_list1 = tsel.getcol('SCAN_NUMBER')
            field_id_list1 = tsel.getcol('FIELD_ID')
            antenna1_list1 = tsel.getcol('ANTENNA1')
            state_id_list1 = tsel.getcol('STATE_ID')
            data_desc_id_list1 = tsel.getcol('DATA_DESC_ID')
            time_list1 = tsel.getcol('TIME')
            rownumber_list1 = tsel.rownumbers()
        finally:
            tsel.close()

    for processor_id in processor_id_set:

        LOG.trace('PROCESSOR_ID %s' % processor_id)

        for observation_id in observation_id_set:
            LOG.trace('OBSERVATION_ID %s' % observation_id)

            for scan_number in scan_numbers:
                LOG.trace('SCAN_NUMBER %s' % scan_number)

                if scan_number not in states:
                    LOG.trace('No target states in SCAN %s' % scan_number)
                    continue

                for field_id in fields[scan_number]:
                    LOG.trace('FIELD_ID %s' % field_id)

                    for antenna in src_ms.antennas:
                        antenna_id = antenna.id
                        LOG.trace('ANTENNA_ID %s' % antenna_id)

                        for spw in src_ms.get_spectral_windows(science_windows_only=True):
                            data_desc = src_ms.get_data_description(spw=spw)
                            data_desc_id = data_desc.id
                            pol_id = data_desc.pol_id
                            spw_id = spw.id
                            LOG.trace('START PROCESSOR %s SCAN %s DATA_DESC_ID %s ANTENNA %s FIELD %s' %
                                      (processor_id, scan_number, data_desc_id, antenna_id, field_id))
                            derived_pol_id = to_derived_polid[pol_id]
                            derived_spw_id = to_derived_spwid[spw_id]
                            derived_dd_id = derived_ddid_map[(derived_pol_id, derived_spw_id)]
                            LOG.trace('SRC DATA_DESC_ID %s (SPW %s)' % (data_desc_id, spw_id))
                            LOG.trace('DERIVED DATA_DESC_ID %s (SPW %s)' % (derived_dd_id, derived_spw_id))

                            tmask0 = numpy.logical_and(
                                data_desc_id_list0 == data_desc_id,
                                numpy.logical_and(antenna1_list0 == antenna_id,
                                                  numpy.logical_and(field_id_list0 == field_id,
                                                                    scan_number_list0 == scan_number)))
                            if not is_unique_processor_id:
                                numpy.logical_and(tmask0, processor_id_list0 == processor_id, out=tmask0)
                            if not is_unique_observation_id:
                                numpy.logical_and(tmask0, observation_id_list0 == observation_id, out=tmask0)

                            tmask1 = numpy.logical_and(
                                data_desc_id_list1 == derived_dd_id,
                                numpy.logical_and(antenna1_list1 == antenna_id,
                                                  numpy.logical_and(field_id_list1 == field_id,
                                                                    scan_number_list1 == scan_number)))
                            if not is_unique_processor_id:
                                numpy.logical_and(tmask1, processor_id_list1 == processor_id, out=tmask1)
                            if not is_unique_observation_id:
                                numpy.logical_and(tmask1, observation_id_list1 == observation_id, out=tmask1)

                            if numpy.all(tmask0 == False) and numpy.all(tmask1 == False):
                                # no corresponding data (probably due to PROCESSOR_ID for non-science windows)
                                LOG.trace('SKIP PROCESSOR %s SCAN %s DATA_DESC_ID %s ANTENNA %s FIELD %s' %
                                          (processor_id, scan_number, data_desc_id, antenna_id, field_id))
                                continue

                            tstate0 = state_id_list0[tmask0]
                            tstate1 = state_id_list1[tmask1]
                            ttime0 = time_list0[tmask0]
                            ttime1 = time_list1[tmask1]
                            trow0 = rownumber_list0[tmask0]
                            trow1 = rownumber_list1[tmask1]
                            sort_index0 = numpy.lexsort((tstate0, ttime0))
                            sort_index1 = numpy.lexsort((tstate1, ttime1))
                            LOG.trace('scan %s' % (scan_number)
                                      + ' actual %s' % (list(set(tstate0)))
                                      + ' expected %s' % (states[scan_number]))
                            assert numpy.all(ttime0[sort_index0] == ttime1[sort_index1])
                            assert numpy.all(tstate0[sort_index0] == tstate1[sort_index1])
                            # assert set(tstate0) == set(states[scan_number])
                            assert set(tstate0).issubset(set(states[scan_number]))

                            for (i0, i1) in zip(sort_index0, sort_index1):
                                r0 = trow0[i0]
                                r1 = trow1[i1]
                                rowmap[r0] = r1

                            LOG.trace('END PROCESSOR %s SCAN %s DATA_DESC_ID %s ANTENNA %s FIELD %s' %
                                      (processor_id, scan_number, data_desc_id, antenna_id, field_id))

    return rowmap


class SpwSimpleView(object):
    """
    A simple class that holds an spectral windpw (SpW) ID and Name pair.

    Attributes:
        id: A SpW ID.
        name: A SpW name.
    """

    def __init__(self, spwid: int, name: str):
        """Initialize SpwSimpleView class."""
        self.id = spwid
        self.name = name


class SpwDetailedView(object):
    """
    A class to store Spestral Window (SpW) settings.

    Attributes:
        id: An SpW ID.
        name: A SpW name.
        num_channels: A number of channels in SpW.
        ref_frequency: The reference frequency of SpW.
        min_frequency: The minimum frequency of SpW.
        max_frequency: The maximum frequency of SpW.
    """

    def __init__(self, spwid: int, name: str, num_channels: int,
                 ref_frequency: float, min_frequency: float,
                 max_frequency: float):
        """
        Initialize SpwDetailedView class.

        Args:
            id: A spectral windpw (SpW) ID.
            name: A SpW name.
            num_channels: A number of channels in SpW.
            ref_frequency: The reference frequency of SpW.
            min_frequency: The minimum frequency of SpW.
            max_frequency: The maximum frequency of SpW.
        """
        self.id = spwid
        self.name = name
        self.num_channels = num_channels
        self.ref_frequency = ref_frequency
        self.min_frequency = min_frequency
        self.max_frequency = max_frequency


def get_spw_names(vis: str) -> List[SpwSimpleView]:
    """
    Return a list of SpWSimpleView of all spectral windpws in a MeasurementSet.

    Args:
        vis: A path to MeasurementSet.

    Returns:
        A list of SpWSimpleView instances of all spectral windpw in vis.
    """
    with casa_tools.TableReader(os.path.join(vis, 'SPECTRAL_WINDOW')) as tb:
        gen = (SpwSimpleView(i, tb.getcell('NAME', i)) for i in range(tb.nrows()))
        spws = list(gen)
    return spws


def get_spw_properties(vis: str) -> List[SpwDetailedView]:
    """
    Return a list of SpwDetailedView of all spectral windpws in a MeasurementSet.

    Args:
        vis: A path to MeasurementSet.

    Returns:
        A list of SpwDetailedView instances of all spectral windpw in vis.
    """
    with casa_tools.TableReader(os.path.join(vis, 'SPECTRAL_WINDOW')) as tb:
        spws = []
        for irow in range(tb.nrows()):
            name = tb.getcell('NAME', irow)
            nchan = tb.getcell('NUM_CHAN', irow)
            ref_freq = tb.getcell('REF_FREQUENCY', irow)
            chan_freq = tb.getcell('CHAN_FREQ', irow)
            chan_width = tb.getcell('CHAN_WIDTH', irow)
            min_freq = chan_freq.min() - abs(chan_width[0]) / 2
            max_freq = chan_freq.max() + abs(chan_width[0]) / 2
            spws.append(SpwDetailedView(irow, name, nchan, ref_freq, min_freq, max_freq))
    return spws


# @profiler
def __read_table(reader: Optional[Callable], method: Callable,
                 vis: Any) -> Any:
    # Returns results of either method(reader(vis)) or method(vis).
    if reader is None:
        result = method(vis)
    else:
        with reader(vis) as readerobj:
            result = method(readerobj)
    return result


# @profiler
def make_spwid_map(srcvis: str, dstvis: str) -> dict:
    """
    Make mapping of spectral windpw IDs in two MeasurementSets (MS).

    Args:
        srcvis: A path to source MS.
        dstvis: A path to the other MS.

    Returns:
        A spectral windpw (SpW) mapping dictionary. A key is SpW ID of srcvis
        and the value is that of dstvis.
    """
    src_spws = __read_table(None, get_spw_properties, srcvis)
    dst_spws = __read_table(None, get_spw_properties, dstvis)

    for spw in src_spws:
        LOG.trace('SRC SPWID %s NAME %s' % (spw.id, spw.name))
    for spw in dst_spws:
        LOG.trace('DST SPWID %s NAME %s' % (spw.id, spw.name))

    map_byname = collections.defaultdict(list)
    for src_spw in src_spws:
        for dst_spw in dst_spws:
            if src_spw.name == dst_spw.name:
                map_byname[src_spw].append(dst_spw)

    spwid_map = {}
    for src, dst in map_byname.items():
        LOG.trace('map_byname src spw %s: dst spws %s' % (src.id, [spw.id for spw in dst]))
        if len(dst) == 0:
            continue
        elif len(dst) == 1:
            # mapping by name worked
            spwid_map[src.id] = dst[0].id
        else:
            # need more investigation
            for spw in dst:
                if (src.num_channels == spw.num_channels and
                        src.ref_frequency == spw.ref_frequency and
                        src.min_frequency == spw.min_frequency and
                        src.max_frequency == spw.max_frequency):
                    if src.id in spwid_map:
                        raise RuntimeError('Failed to create spw map for MSs \'%s\' and \'%s\'' % (srcvis, dstvis))
                    spwid_map[src.id] = spw.id
    return spwid_map

PolarizationData = Tuple[int, int, List[int], List[int], bool]

def _read_polarization_table(vis: str) -> List[PolarizationData]:
    """
    Read the POLARIZATION table of a given MeasurementSet.

    This function used to be part of tablereader, which has since moved from
    direct table reading to using the MSMD tool.

    Args:
        vis: A path to MeasurementSet.

    Retruns:
        A list PolarizationData extracted from each row of POLARIZATION table.
    """
    LOG.debug('Analysing POLARIZATION table')
    polarization_table = os.path.join(vis, 'POLARIZATION')
    with casa_tools.TableReader(polarization_table) as table:
        num_corrs = table.getcol('NUM_CORR')
        vcorr_types = table.getvarcol('CORR_TYPE')
        vcorr_products = table.getvarcol('CORR_PRODUCT')
        flag_rows = table.getcol('FLAG_ROW')

        rowids = []
        corr_types = []
        corr_products = []
        for i in range(table.nrows()):
            rowids.append(i)
            corr_types.append(vcorr_types['r%s' % (i + 1)])
            corr_products.append(vcorr_products['r%s' % (i + 1)])

        rows = list(zip(rowids, num_corrs, corr_types, corr_products, flag_rows))
        return rows


# @profiler
def make_polid_map(srcvis: str, dstvis: str) -> dict:
    """
    Make mapping of Polarization IDs in two MeasurementSets (MS).

    Args:
        srcvis: A path to source MS.
        dstvis: A path to the other MS.

    Returns:
        A polarization mapping dictionary. A key is polarization ID of srcvis
        and the value is that of dstvis.
    """
    src_rows = _read_polarization_table(srcvis)
    dst_rows = _read_polarization_table(dstvis)
    for (src_polid, src_numpol, src_poltype, _, _) in src_rows:
        LOG.trace('SRC: POLID %s NPOL %s POLTYPE %s' % (src_polid, src_numpol, src_poltype))
    for (dst_polid, dst_numpol, dst_poltype, _, _) in dst_rows:
        LOG.trace('DST: POLID %s NPOL %s POLTYPE %s' % (dst_polid, dst_numpol, dst_poltype))
    polid_map = {}
    for (src_polid, src_numpol, src_poltype, _, _) in src_rows:
        for (dst_polid, dst_numpol, dst_poltype, _, _) in dst_rows:
            if src_numpol == dst_numpol and numpy.all(src_poltype == dst_poltype):
                polid_map[src_polid] = dst_polid
    LOG.trace('polid_map = %s' % polid_map)
    return polid_map


# @profiler
def make_ddid_map(vis: str) -> dict:
    """
    Map polarization and spwctral window IDs to data description ID.

    Args:
        vis: A name of MeasurementSet.

    Returns:
        A dictionary that maps polarization (pol) and spectral windpw (SpW) IDs
        to data description ID. A key of dictionary is a tuple of
        (pol ID, SpW ID) and a value is the corresponding data description ID.
    """
    with casa_tools.TableReader(os.path.join(vis, 'DATA_DESCRIPTION')) as tb:
        pol_ids = tb.getcol('POLARIZATION_ID')
        spw_ids = tb.getcol('SPECTRAL_WINDOW_ID')
        num_ddids = tb.nrows()
    ddid_map = {}
    for ddid in range(num_ddids):
        ddid_map[(pol_ids[ddid], spw_ids[ddid])] = ddid
    return ddid_map


def get_datacolumn_name(vis: str) -> str:
    """
    Return a name of column that stores spectral or visibility data.

    Args:
        vis: A path to MeasurementSet to analyze.

    Returns:
        A name of data column. The CORRECTED_DATA is prioritied when multiple
        data columns exists.
    """
    colname_candidates = ['CORRECTED_DATA', 'FLOAT_DATA', 'DATA']
    with casa_tools.TableReader(vis) as tb:
        colnames = tb.colnames()
    colname = None
    for name in colname_candidates:
        if name in colnames:
            colname = name
            break
    assert colname is not None
    return colname


def get_restfrequency(vis: str, spwid: int,
                      source_id: int) -> Optional[numpy.ndarray]:
    """
    Obtain the rest frequency of a given source and spectral windpw (SpW).

    Args:
        vis: A path to MeasurementSet.
        spwid: A SpW ID to select.
        source_id: A source ID to select.

    Returns:
        The first entry of the rest frequency in SOURCE table that matches
        selection.
    """
    source_table = os.path.join(vis, 'SOURCE')
    with casa_tools.TableReader(source_table) as tb:
        tsel = tb.query('SOURCE_ID == {} && SPECTRAL_WINDOW_ID == {}'.format(source_id, spwid))
        try:
            if tsel.nrows() == 0:
                return None
            else:
                if tsel.iscelldefined('REST_FREQUENCY', 0):
                    return tsel.getcell('REST_FREQUENCY', 0)[0]
                else:
                    return None
        finally:
            tsel.close()


class RGAccumulator(object):
    """
    Accumulate metadata information of a reduction group.

    Attributes:
        field: A list of field IDs.
        antenna: A list of antenna IDs.
        spw: A list of spectral windpw IDs.
        pols: A list of polarizations.
        grid_table: A list of compressed grid tables.
        channelmap_range: A list of channel map ranges.
    """

    def __init__(self):
        """Initialize RGAccumurator class."""
        self.field = []
        self.antenna = []
        self.spw = []
        self.pols = []
        self.grid_table = []
        self.channelmap_range = []

    def append(self, field_id: int, antenna_id: int, spw_id: int,
               pol_ids: Union[List[int], List[str], None]=None,
               grid_table: Union[dict, compress.CompressedObj, None]=None,
               channelmap_range: Optional[List[int]]=None):
        """
        Add an entry to class.

        Args:
            field_id: A field ID.
            antenna_id: An antenna ID.
            spw_id: A spectral windpw ID.
            pol_ids: Polarizations.
            grid_table: A grid table.
            channelmap_range: Channel map ranges.
        """
        self.field.append(field_id)
        self.antenna.append(antenna_id)
        self.spw.append(spw_id)
        self.pols.append(pol_ids)
        if isinstance(grid_table, compress.CompressedObj) or grid_table is None:
            self.grid_table.append(grid_table)
        else:
            self.grid_table.append(compress.CompressedObj(grid_table))
        self.channelmap_range.append(channelmap_range)

#         def extend(self, field_id_list, antenna_id_list, spw_id_list):
#             self.field.extend(field_id_list)
#             self.antenna.extend(antenna_id_list)
#             self.spw.extend(spw_id_list)
#
    def get_field_id_list(self):
        """Return a list of field IDs registered to the class."""
        return self.field

    def get_antenna_id_list(self):
        """Return a list of antenna IDs registered to the class."""
        return self.antenna

    def get_spw_id_list(self):
        """Return a list of spectral windpw IDs registered to the class."""
        return self.spw

    def get_pol_ids_list(self):
        """Return a list of polarization IDs registered to the class."""
        return self.pols

    def get_grid_table_list(self):
        """Return a list of compressed grid tables registered to the class."""
        return self.grid_table

    def get_channelmap_range_list(self):
        """Return a list of channel map ranges registered to the class."""
        return self.channelmap_range

    def iterate_id(self) -> Generator[Tuple[int, int, int], None, None]:
        """Yield field, antenna, and spectral window registered."""
        assert len(self.field) == len(self.antenna)
        assert len(self.field) == len(self.spw)
        assert len(self.field) == len(self.pols)
        for v in zip(self.field, self.antenna, self.spw):
            yield v

    def iterate_all(
            self
            ) -> Generator[Tuple[int, int, int, Optional[dict], List[int]],
                           None, None]:
        """
        Yield metadata registered.

        Returns:
            A tuple of field, antenna, and spectral window, grid table and
            channel map range.
        """
        assert len(self.field) == len(self.antenna)
        assert len(self.field) == len(self.spw)
        assert len(self.field) == len(self.pols)
        assert len(self.field) == len(self.grid_table)
        assert len(self.field) == len(self.channelmap_range)
        for f, a, s, g, c in zip(self.field, self.antenna, self.spw, self.grid_table, self.channelmap_range):
            _g = g.decompress()
            yield f, a, s, _g, c
            del _g

    def get_process_list(
            self, withpol: bool=False
            ) -> Union[Tuple[int, int, int],
                       Tuple[int, int, int, Union[List[int], List[str]]]]:
        """
        Obtain a list of metadata registered.

        Args:
            withpol: If True, polarizations will be returned in addtion to the
                field, antenna, and spectral window IDs.

        Returns:
            Lists of the field, antenna and spwctral IDs, and optionally
            polarizations.
        """
        field_id_list = self.get_field_id_list()
        antenna_id_list = self.get_antenna_id_list()
        spw_id_list = self.get_spw_id_list()

        assert len(field_id_list) == len(antenna_id_list)
        assert len(field_id_list) == len(spw_id_list)

        if withpol == True:
            pol_ids_list = self.get_pol_ids_list()
            assert len(field_id_list) == len(pol_ids_list)
            return field_id_list, antenna_id_list, spw_id_list, pol_ids_list
        else:
            return field_id_list, antenna_id_list, spw_id_list


def sort_fields(context: Context) -> List[Field]:
    """
    Obtain a set of field objects registered to a context.

    Args:
        context: A Pipeline context to analyze.

    Retruns:
        A list of unduplicated field objects in the other of MeasurementSet and
        fields that appears in Pipeline Context.
    """
    mses = context.observing_run.measurement_sets
    sorted_names = []
    sorted_fields = []
    for ms in mses:
        fields = ms.get_fields(intent='TARGET')
        for f in fields:
            if f.name not in sorted_names:
                sorted_fields.append(f)
                sorted_names.append(f.name)
    return sorted_fields


def get_brightness_unit(vis: str, defaultunit: str='Jy/beam') -> str:
    """
    Obtain a unit of data column of MeasurementSet.

    Arg:
        vis: A path to MeasurementSet.
        defaultunit: A default unit in case unit is not available in any data
            column.

    Returns:
        A unit of a data column in vis. The value of defaultunit is returned
        if not unit is available. CORRECTED_DATA is prioritized when multiple
        data columns are in the vis.
    """
    with casa_tools.TableReader(vis) as tb:
        colnames = tb.colnames()
        target_columns = ['CORRECTED_DATA', 'FLOAT_DATA', 'DATA']
        bunit = defaultunit
        for col in target_columns:
            if col in colnames:
                keys = tb.getcolkeywords(col)
                if 'UNIT' in keys:
                    _bunit = keys['UNIT']
                    if len(_bunit) > 0:
                        # should be K or Jy
                        # update bunit only when UNIT is K
                        if _bunit == 'K':
                            bunit = 'K'
                        break
    return bunit
