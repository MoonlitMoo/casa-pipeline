"""
The conversion module contains utility functions.

The conversion module contains utility functions that convert between data
types and assist in formatting objects as strings for presentation to the
user.
"""
import collections
import decimal
import math
import os
import re
import string
import typing
from datetime import datetime, timedelta
from numbers import Number
from typing import Any, Dict, Iterator, List, Optional, Sequence, Tuple, Union, Iterable

import astropy.units as u
import cachetools
import numpy as np
import pyparsing
from astropy.coordinates import SkyCoord

from .. import casa_tools, logging

LOG = logging.get_logger(__name__)

__all__ = ['commafy', 'flatten', 'mjd_seconds_to_datetime', 'get_epoch_as_datetime', 'range_to_list', 'to_CASA_intent',
           'to_pipeline_intent', 'field_arg_to_id', 'spw_arg_to_id', 'ant_arg_to_id', 'safe_split', 'dequote',
           'format_datetime', 'format_timedelta', 'record_to_quantity']

# By default we use CASA to parse arguments into spw/field/ant IDs. However, this
# requires access to the data. Setting this property to False uses the pipeline's
# legacy parsing routines, which operate off context data. This is useful when
# importing a remote context for debugging purposes, when we don't have access to
# the data.
USE_CASA_PARSING_ROUTINES = True


class LoggingLRUCache(cachetools.LRUCache):
    """'Least recently used' cache that logs when cache entries are evicted.

    Underestimating the required cache size leads to poor performance, as seen
    in PIPE-327, where a lack of cached entries for the 33 EBs leads to
    millions of 'unnecessary' MS tool open calls, each open() taking several
    tens of milliseconds. Hence, we want to be notified when the cache size
    limit is hit.
    """

    def __init__(self, name: str, *args, **kwargs):
        self.name = name
        super().__init__(*args, **kwargs)

    def popitem(self):
        """Remove and return the (key, value) pair least recently used.

        Override popitem method to create a log entry when a cache entry is
        evicted.
        """
        key, value = super().popitem()
        LOG.info(f'Evicting cache entry for {self.name}. '
                 f'Cache size ({self.maxsize}) is too small!')
        LOG.trace(f'Key {key} evicted with value {value}')
        return key, value


# Cache for ms.msselectedindices calls. Without this cache, the MS tool would
# open and close the measurement set on each query, which is an expensive
# operation.
MSTOOL_SELECTEDINDICES_CACHE: typing.Dict[str, LoggingLRUCache] = {}


def commafy(l: Iterable, quotes: bool = True, multi_prefix: str = '', separator: str = ', ',
            conjunction: str = 'and') -> str:
    """Convert the string list into the textual description.

    Example:
    >>> commafy(['a','b','c'])
    "'a', 'b' and 'c'"

    Args:
        l: Python string list.
        quotes: If quote is True, 'l' arg elements are enclosed in quotes by each.
        multi_prefix: If the 'l' arg has three or more elements, the 'multi_prefix'
            attach to the head.
        separator: The 'separator' arg is used as separator instead of ','.
        conjunction: The 'conjunction' arg is used as conjunction instead of 'and'.

    Return:
        The textual description of the given list.
    """
    if not isinstance(l, list) and isinstance(l, collections.abc.Iterable):
        l = [i for i in l]

    # turn 's' into 's '
    if multi_prefix:
        multi_prefix += ' '

    length = len(l)
    if length == 0:
        return ''
    if length == 1:
        if multi_prefix:
            prefix = ' '
        else:
            prefix = ''

        if quotes:
            return '%s\'%s\'' % (prefix, l[0])
        else:
            return '%s%s' % (prefix, l[0])
    if length == 2:
        if quotes:
            return '%s\'%s\' %s \'%s\'' % (multi_prefix, l[0], conjunction, l[1])
        else:
            return '%s%s %s %s' % (multi_prefix, l[0], conjunction, l[1])
    else:
        if quotes:
            return '%s\'%s\'%s%s' % (
                multi_prefix, l[0], separator,
                commafy(l[1:], separator=separator, quotes=quotes, conjunction=conjunction))
        else:
            return '%s%s%s%s' % (
                multi_prefix, l[0], separator,
                commafy(l[1:], separator=separator, quotes=quotes, conjunction=conjunction))


def flatten(l: Sequence[Any]) -> Iterator[Any]:
    """Flatten a list of lists into a single list without pipelineaq.QAScore.

    Example:
    >>> obj = flatten([1,2,[3,4,[5,6]],7])
    >>> obj.__next__()
    1
    >>> obj.__next__()
    2
    >>> obj.__next__()
    3

    >>> list(flatten([1,2,['c',4,['e',6]],7]))
    [1, 2, 'c', 4, 'e', 6, 7]

    Args:
        l: A list with list or any object.
    Yields:
        Single list.
    """
    for el in l:
        if isinstance(el, collections.abc.Iterable) and not isinstance(el, str):
            for sub in flatten(el):
                yield sub
        else:
            yield el


def unix_seconds_to_datetime(unix_secs: Sequence[Number]) -> Union[datetime, List[datetime]]:
    """Convert list of UNIX epoch times to a list of equivalent datetime objects.

    Args:
        unix_secs: list of elapsed seconds since 1970-01-01.
    Returns:
        List of equivalent Python datetime objects.
    """
    return [datetime.utcfromtimestamp(s) for s in unix_secs]


def mjd_seconds_to_datetime(mjd_secs: Sequence[Number]) -> List[datetime]:
    """Convert list of MJD seconds to a list of equivalent datetime objects.

    Convert the input list of elapsed seconds since MJD epoch to the
    equivalent Python datetime objects.

    Args:
        mjd_secs: list of elapsed seconds since MJD epoch.
    Returns:
        List of equivalent Python datetime objects.
    """
    # 1970-01-01 is JD 40587. 86400 = seconds in a day
    unix_offset = 40587 * 86400
    mjd_secs_with_offsets = [s - unix_offset for s in mjd_secs]
    return unix_seconds_to_datetime(mjd_secs_with_offsets)


def get_epoch_as_datetime(epoch: Number) -> datetime:
    """Convert a CASA epoch measure into a Python datetime.

    Args:
        epoch: CASA epoch measure.
    Returns:
        The equivalent Python datetime.
    """
    mt = casa_tools.measures
    qt = casa_tools.quanta

    # calculate UTC standard offset
    datetime_base = mt.epoch('UTC', '40587.0d')
    base_time = mt.getvalue(datetime_base)['m0']
    base_time = qt.convert(base_time, 'd')
    base_time = qt.floor(base_time)

    # subtract offset from UTC equivalent time
    epoch_utc = mt.measure(epoch, 'UTC')
    t = mt.getvalue(epoch_utc)['m0']
    t = qt.sub(t, base_time)
    t = qt.convert(t, 's')
    t = datetime.utcfromtimestamp(qt.getvalue(t)[0])

    return t


def range_to_list(arg: str) -> List[int]:
    """Expand a numeric range expressed in CASA syntax to the list of integer.

    Expand a numeric range expressed in CASA syntax to the equivalent Python
    list of integers.

    Example:
    >>> range_to_list('1~5,7~9')
    [1, 2, 3, 4, 5, 7, 8, 9]

    Args:
        arg: The numeric range expressed in CASA syntax.
    Returns:
        The equivalent Python list of integers.
    """
    if arg == '':
        return []

    # recognise but suppress the mode-switching tokens
    TILDE = pyparsing.Suppress('~')

    # recognise '123' as a number, converting to an integer
    number = pyparsing.Word(pyparsing.nums).setParseAction(lambda tokens: int(tokens[0]))

    # convert '1~10' to a range
    rangeExpr = number('start') + TILDE + number('end')
    rangeExpr.setParseAction(lambda tokens: list(range(tokens.start, tokens.end + 1)))

    casa_chars = ''.join([c for c in string.printable
                          if c not in ',;"/' + string.whitespace])
    textExpr = pyparsing.Word(casa_chars)

    # numbers can be expressed as ranges or single numbers
    atomExpr = rangeExpr | number | textExpr

    # we can have multiple items separated by commas
    atoms = pyparsing.delimitedList(atomExpr, delim=',')('atoms')

    return atoms.parseString(str(arg)).asList()


def to_CASA_intent(ms, intents: str) -> str:
    """Convert pipeline intents back to the equivalent intents recorded in the measurement set.

    Example:
    > to_CASA_intent(ms, 'PHASE,BANDPASS')
    'CALIBRATE_PHASE_ON_SOURCE,CALIBRATE_BANDPASS_ON_SOURCE'

    Args:
        ms: MeasurementSet object.
        intents: pipeline intents to convert.
    Returns:
        The CASA intents recorded.
    """
    obs_modes = ms.get_original_intent(intents)
    return ','.join(obs_modes)


def to_pipeline_intent(ms, intents: str) -> str:
    """Convert CASA intents to pipeline intents.

    Args:
        ms: MeasurementSet object.
        intents: CASA intents to convert.
    Returns:
        The pipeline intents.
    """
    casa_intents = {i.strip('*') for i in intents.split(',') if i is not None}

    state = ms.states[0]

    pipeline_intents = {pipeline_intent for casa_intent in casa_intents
                        for obsmode, pipeline_intent in state.obs_mode_mapping.items()
                        if casa_intent in obsmode}

    return ','.join(pipeline_intents)


def field_arg_to_id(ms_path: str, field_arg: Union[str, int], all_fields) -> List[int]:
    """Convert a string to the corresponding field IDs.

    Args:
        ms_path: A path to the measurement set.
        field_arg: A field selection in CASA format.
        all_fields: All Field objects, for use when CASA msselect is not used.
    Returns:
        A list of field IDs.
    """
    if USE_CASA_PARSING_ROUTINES:
        try:
            all_indices = _convert_arg_to_id('field', ms_path, str(field_arg))
            return all_indices['field'].tolist()
        except RuntimeError:
            # SCOPS-1666
            # msselect throws exceptions with numeric field names beginning with
            # zero. Try again, encapsulating the argument in quotes.
            quoted_arg = '"%s"' % str(field_arg)
            all_indices = _convert_arg_to_id('field', ms_path, quoted_arg)
            return all_indices['field'].tolist()
    else:
        return _parse_field(field_arg, all_fields)


def spw_arg_to_id(ms_path: str, spw_arg: Union[str, int], all_spws) -> List[Tuple[int, int, int, int]]:
    """Convert a string to spectral window IDs and channels.

    Args:
        ms_path: A path to the measurement set.
        spw_arg: A spw selection in CASA format.
        all_spws: List of all SpectralWindow objects, for use when CASA msselect
            is not used.
    Returns:
        A list of (spw, chan_start, chan_end, step) lists.
    """
    if USE_CASA_PARSING_ROUTINES:
        all_indices = _convert_arg_to_id('spw', ms_path, str(spw_arg))
        # filter out channel tuples whose spw is not in the spw entry
        return [(spw, start, end, step)
                for (spw, start, end, step) in all_indices['channel']
                if spw in all_indices['spw']]

    else:
        atoms = _parse_spw(spw_arg, all_spws)
        spws = []
        for atom in atoms:
            spw = [spw for spw in all_spws if spw.id == atom.spw].pop()
            spws.append((spw.id, 0, len(spw.channels.chan_freqs), 1))
        return spws


def ant_arg_to_id(ms_path: str, ant_arg: Union[str, int], all_antennas) -> List[int]:
    """Convert a string to the corresponding antenna IDs.

    Args
        ms_path: A path to the measurement set.
        ant_arg: A antenna selection in CASA format.
        all_antennas: All antenna domain objects for use when CASA msselect is disabled.
    Returns
        A list of antenna IDs.
    """
    if USE_CASA_PARSING_ROUTINES:
        all_indices = _convert_arg_to_id('baseline', ms_path, str(ant_arg))
        return all_indices['antenna1'].tolist()
    else:
        return _parse_antenna(ant_arg, all_antennas)


def _convert_arg_to_id(arg_name: str, ms_path: str, arg_val: str) -> Dict[str, np.ndarray]:
    """Parse the CASA input argument and return the matching IDs.

    Originally the cache was set on this function with the cache size fixed at
    import time (originally 1000). In PIPE-327 this cache size proved too
    small due to the number of EBs (33) and data shape and so we need a way to
    scale the cache with the input data. Hence, a way to scale the cache at
    runtime was created (via the MSSelectedIndicesCache class) and this
    function delegates to the instance held in the module namespace.

    Args:
        arg_name: Name of selection argument to use in MS selection query
        ms_path: A path to the measurement set
        arg_val: Value for selection argument to use in MS selection query,
            formatted with CASA syntax.
    Returns:
        A list of IDs matching the input selection.
    """
    ms_abspath = os.path.abspath(ms_path)
    if ms_abspath not in MSTOOL_SELECTEDINDICES_CACHE:
        # PIPE-327:
        # Historically, a cache size of 1000 entries per EB has been
        # sufficient to avoid cache eviction. It would be possible to
        # calculate a more accurate required cache size (some function of
        # spws, fields, field combinations, etc.) but it's probably not
        # worth the effort as cache entries left unoccupied should take
        # minimal space.
        # PIPE-1008:
        # increase maxsize to 40k entries for VLASS calibration
        # A typical VLASS observation can have 15-20k fields
        MSTOOL_SELECTEDINDICES_CACHE[ms_abspath] = LoggingLRUCache(ms_abspath, maxsize=40000)

    cache_for_ms = MSTOOL_SELECTEDINDICES_CACHE[ms_abspath]
    cache_key = (arg_name, arg_val)

    try:
        return cache_for_ms[cache_key]
    except KeyError:
        taql = {arg_name: str(arg_val)}
        LOG.trace('Executing msselect({%r:%r} on %s', arg_name, arg_val, ms_path)
        with casa_tools.MSReader(ms_path) as ms:
            ms.msselect(taql, onlyparse=True)
            result = ms.msselectedindices()

        cache_for_ms[cache_key] = result
    return result


def safe_split(fields: str) -> List[str]:
    """Split a string containing field names into a list.

    Split a string containing field names into a list, taking account of field
    names within quotes.

    Args:
        fields: A string containing field names.
    Returns:
        A list, taking account of field names within quotes.
    """
    return pyparsing.pyparsing_common.comma_separated_list.parseString(str(fields)).asList()


def dequote(s: str) -> str:
    """Remove any kind of quotes from a string to facilitate comparisons.

    Args:
        s: A string.
    Returns:
        String removed any kind of quotes.
    """
    return s.replace('"', '').replace("'", "")


def format_datetime(dt: datetime, dp: int = 0) -> str:
    """Convert a datetime to a formatted string representation.

    Convert a Python datetime object into a string representation, including
    microseconds to the requested precision.

    Args:
        dt: Python datetime.
        dp: A number of decimal places for microseconds (0=do not show).
    Returns:
        Formatted string representation of datetime.
    """
    s = dt.strftime('%Y-%m-%d %H:%M:%S')
    if dp > 6:
        raise ValueError('Cannot exceed 6 decimal places as datetime stores to microsecond precision')
    elif 0 < dp <= 6:
        microsecs = dt.microsecond / 1e6
        f = '{0:.%sf}' % dp
        return s + f.format(microsecs)[1:]
    else:
        return s


def format_timedelta(td: timedelta, dp: int = 0) -> str:
    """Convert a timedelta to a formatted string representation.

    Convert a Python timedelta object into a string representation, including
    microseconds to the requested precision.

    Args
        td: A timedelta object.
        dp: A number of decimal places for microseconds (0=do not show).
            The number should be natural number with 0.
    Returns:
        Formatted string representation of timedelta.
    """
    secs = decimal.Decimal(td.seconds)
    microsecs = decimal.Decimal(td.microseconds) / decimal.Decimal('1e6')
    rounded_secs = (secs + microsecs).quantize(decimal.Decimal(10) ** -dp)
    rounded = timedelta(days=td.days, seconds=math.floor(rounded_secs))
    # get rounded number of microseconds as an integer
    rounded_microsecs = int((rounded_secs % 1).shift(6))
    # .. which we can pad with zeroes..
    str_microsecs = '{0:06d}'.format(rounded_microsecs)
    # .. which we can append onto the end of the default timedelta string
    # representation
    if dp > 6:
        raise ValueError('Cannot exceed 6 decimal places as datetime stores to microsecond precision')
    elif 0 < dp <= 6:
        fraction = str_microsecs[0:dp]
        return str(rounded) + '.' + str(fraction)
    else:
        return str(rounded)


def _parse_spw(task_arg: str, all_spw_ids: tuple = None):
    """Convert the CASA-style spw argument to a list of spw IDs.

    Channel limits are also parsed in this function but are not currently
    used. The channel limits may be found as the channels property of an
    atom.

    Example:
    > _parse_spw('0:0~6^2,2:6~38^4 (0, 1, 4, 5, 6, 7)')
    <result>
    <atom>
      <spws>
        <ITEM>0</ITEM>
      </spws>
      <channels>
        <ITEM>0</ITEM>
        <ITEM>2</ITEM>
        <ITEM>4</ITEM>
        <ITEM>6</ITEM>
      </channels>
    </atom>
    <atom>
      <spws>
        <ITEM>2</ITEM>
      </spws>
      <channels>
        <ITEM>6</ITEM>
        <ITEM>10</ITEM>
        <ITEM>14</ITEM>
        <ITEM>18</ITEM>
        <ITEM>22</ITEM>
        <ITEM>26</ITEM>
        <ITEM>30</ITEM>
        <ITEM>34</ITEM>
        <ITEM>38</ITEM>
      </channels>
    </atom>
    </result>

    Args:
        task_arg:
        all_spw_ids:
    Returns:
    """
    if task_arg in (None, ''):
        return all_spw_ids
    if all_spw_ids is None:
        all_spw_ids = []

    # recognise but suppress the mode-switching tokens
    TILDE, LESSTHAN, CARET, COLON, ASTERISK = list(map(pyparsing.Suppress, '~<^:*'))

    # recognise '123' as a number, converting to an integer
    number = pyparsing.Word(pyparsing.nums).setParseAction(lambda tokens: int(tokens[0]))

    # convert '1~10' to a range
    rangeExpr = number('start') + TILDE + number('end')
    rangeExpr.setParseAction(lambda tokens: list(range(tokens.start, tokens.end + 1)))

    # convert '1~10^2' to a range with the given step size
    rangeWithStepExpr = number('start') + TILDE + number('end') + CARET + number('step')
    rangeWithStepExpr.setParseAction(lambda tokens: list(range(tokens.start, tokens.end + 1, tokens.step)))

    # convert <10 to a range
    ltExpr = LESSTHAN + number('max')
    ltExpr.setParseAction(lambda tokens: list(range(0, tokens.max)))

    # convert * to all spws
    allExpr = ASTERISK.setParseAction(lambda tokens: all_spw_ids)

    # spw and channel components can be any of the above patterns
    numExpr = rangeWithStepExpr | rangeExpr | ltExpr | allExpr | number

    # recognise and group multiple channel definitions separated by semi-colons
    channelsExpr = pyparsing.Group(pyparsing.delimitedList(numExpr, delim=';'))

    # group the number so it converted to a node, spw in this case
    spwsExpr = pyparsing.Group(numExpr)

    # the complete expression is either spw or spw:chan
    atomExpr = pyparsing.Group(spwsExpr('spws') + COLON + channelsExpr('channels') | spwsExpr('spws'))

    # and we can have multiple items separated by commas
    finalExpr = pyparsing.delimitedList(atomExpr('atom'), delim=',')('result')

    parse_result = finalExpr.parseString(str(task_arg))

    results = {}
    for atom in parse_result.result:
        for spw in atom.spws:
            if spw not in results:
                results[spw] = set(atom.channels)
            else:
                results[spw].update(atom.channels)

    Atom = collections.namedtuple('Atom', ['spw', 'channels'])
    return [Atom(spw=k, channels=v) for k, v in results.items()]


def _parse_field(task_arg: Optional[str], fields=None) -> List[int]:
    """Convert the field section in CASA format to list of field IDs.

    Inner method.

    Args:
        task_arg: The field selection in CASA format.
        fields: Field objects
    Returns:
        A list of field IDs that matches field selection criteria
    """
    if task_arg in (None, ''):
        return [f.id for f in fields]
    if fields is None:
        fields = []

    # recognise but suppress the mode-switching tokens
    TILDE = pyparsing.Suppress('~')

    # recognise '123' as a number, converting to an integer
    number = pyparsing.Word(pyparsing.nums).setParseAction(lambda tokens: int(tokens[0]))

    # convert '1~10' to a range
    rangeExpr = number('start') + TILDE + number('end')
    rangeExpr.setParseAction(lambda tokens: list(range(tokens.start, tokens.end + 1)))

    boundary = ''.join([c for c in pyparsing.printables if c not in (' ', ',')])
    field_id = pyparsing.WordStart(boundary) + (rangeExpr | number) + pyparsing.WordEnd(boundary)

    casa_chars = ''.join([c for c in string.printable
                          if c not in string.whitespace])
    field_name = pyparsing.Word(casa_chars + ' ')

    def get_ids_for_matching(tokens):
        search_term = tokens[0]
        if '*' in search_term:
            regex = search_term.replace('*', '.*') + '$'
            return [f.id for f in fields if re.match(regex, f.name)]
        return [f.id for f in fields if f.name == search_term]

    field_name.setParseAction(get_ids_for_matching)

    results = set()
    for atom in pyparsing.pyparsing_common.comma_separated_list.parseString(str(task_arg)):
        for parser in [field_name('fields'), field_id('fields')]:
            for match in parser.searchString(atom):
                results.update(match.asList())

    return sorted(list(results))


def _parse_antenna(task_arg: Optional[str], antennas: Optional[Dict[str, np.ndarray]] = None) -> List[int]:
    """Convert the antenna selection in CASA format to a list of antenna IDs.

    Inner method.

    Args:
        task_arg: The antenna selection in CASA format.
        antennas: Antenna domain objects.
    Returns:
        List of antenna IDs that matches antenna selection criteria.
    """
    if task_arg in (None, ''):
        return [a.id for a in antennas]
    if antennas is None:
        antennas = []

    # recognise but suppress the mode-switching tokens
    TILDE = pyparsing.Suppress('~')

    # recognise '123' as a number, converting to an integer
    number = pyparsing.Word(pyparsing.nums).setParseAction(lambda tokens: int(tokens[0]))

    # convert '1~10' to a range
    rangeExpr = number('start') + TILDE + number('end')
    rangeExpr.setParseAction(lambda tokens: list(range(tokens.start, tokens.end + 1)))

    # antenna-oriented 'by ID' expressions can be any of the above patterns
    boundary = ''.join([c for c in pyparsing.printables if c not in (' ', ',')])
    numExpr = pyparsing.WordStart(boundary) + (rangeExpr | number) + pyparsing.WordEnd(boundary)

    # group the number so it converted to a node, fields in this case
    antenna_id_expr = pyparsing.Group(numExpr)

    casa_chars = ''.join([c for c in string.printable
                          if c not in ',;"/' + string.whitespace])
    antenna_name = pyparsing.Word(casa_chars)

    def get_antenna(tokens):
        search_term = tokens[0]
        if '*' in search_term:
            regex = search_term.replace('*', '.*') + '$'
            return [a.id for a in antennas if re.match(regex, a.name)]
        return [a.id for a in antennas if a.name == search_term]

    antenna_name.setParseAction(get_antenna)

    antenna_name_expr = pyparsing.Group(antenna_name)

    # the complete expression
    atomExpr = pyparsing.Group(antenna_id_expr('antennas') | antenna_name_expr('antennas'))

    results = set()
    for substr in pyparsing.pyparsing_common.comma_separated_list.parseString(str(task_arg)):
        atoms = atomExpr.parseString(substr)
        for atom in atoms:
            for ant in atom.antennas:
                results.add(ant)

    return sorted(list(results))


def record_to_quantity(record: Union[Dict, List[Dict], Tuple[Dict]]) -> Union[u.Quantity, List[u.Quantity], Tuple[u.Quantity]]:
    """Convert a CASA record to an Astropy quantity.

    Optionally, the input can be a list/tuple in which each element is a CASA record.
    """

    if isinstance(record, (list, tuple)):
        quantities = [record_to_quantity(r) for r in record]
        if isinstance(record, tuple):
            return tuple(quantities)
        return quantities

    return record['value'] * u.Unit(record['unit'])


def phasecenter_to_skycoord(phasecenter):
    """Convert a CASA-style coordinate string to an Astropy SkyCoord object."""

    phasecenter_list = phasecenter.split()
    if len(phasecenter_list) == 2:
        ra = phasecenter_list[0]
        dec = phasecenter_list[1]
        refcode = 'icrs'
    elif len(phasecenter_list) == 3:
        ra = phasecenter_list[1]
        dec = phasecenter_list[2]
        refcode = phasecenter_list[0]
    else:
        LOG.error('cannot parse the phasecenter string %s', phasecenter)
        return

    dec = dec.replace('.', ':', 2)
    frame = refcode_to_skyframe(refcode)
    coord = SkyCoord(ra, dec, unit=(u.hourangle, u.deg), frame=frame)

    return coord


def refcode_to_skyframe(refcode):
    """Conver a CASA coordsysy refcode to an Astropy SkyCoord frame name.
    
    Limitations:
    
    Currently, it only handles the common cases, e.g. J2000, B1950, ICRS
    
    To get a list of built-in astropy.coordinates frame names:
        from astropy.coordinates import frame_transform_graph
        print(frame_transform_graph.get_names())
    To get a list of CASA csys reference code:
        csys = cs.newcoordsys(direction=True)
        clist = csys.referencecode('dir', True)
    
    """

    frame = refcode.lower()
    if frame == 'j2000':
        frame = 'fk5'
    if frame == 'b1950':
        frame = 'fk4'

    return frame
