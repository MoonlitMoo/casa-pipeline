"""
The imaging module contains utility functions used by the imaging tasks.

TODO These utility functions should migrate to hif.tasks.common
"""
import re

from .. import casa_tools
from .. import logging
import numpy
from typing import Union, Tuple, List, Dict, Any, Generator

from .. import utils

LOG = logging.get_logger(__name__)

__all__ = ['chan_selection_to_frequencies', 'freq_selection_to_channels', 'spw_intersect', 'update_sens_dict',
           'update_beams_dict', 'set_nested_dict', 'intersect_ranges', 'intersect_ranges_by_weight', 'merge_ranges', 'equal_to_n_digits',
           'velocity_to_frequency', 'frequency_to_velocity',
           'predict_kernel']


def _get_cube_freq_axis(img: str) -> Tuple[float, float, str, float, int]:
    """
    Get CASA image/cube frequency axis.

    Args:
        img: CASA image/cube name

    Returns:
        Tuple of frequency axis components
        (reference frequency, delta frequency per channel, frequency unit,
         reference pixel, number of pixels of frequency axis)
    """
    iaTool = casa_tools.image

    # Get frequency axis
    iaTool.open(img)
    imInfo = iaTool.summary()
    iaTool.close()

    fIndex = imInfo['axisnames'].tolist().index('Frequency')
    refFreq = imInfo['refval'][fIndex]
    deltaFreq = imInfo['incr'][fIndex]
    freqUnit = imInfo['axisunits'][fIndex]
    refPix = imInfo['refpix'][fIndex]
    numPix = imInfo['shape'][fIndex]

    return refFreq, deltaFreq, freqUnit, refPix, numPix


def chan_selection_to_frequencies(img: str, selection: str, unit: str = 'GHz') -> Union[List[float], List[str]]:
    """
    Convert channel selection to frequency tuples for a given CASA cube.

    Args:
        img:       CASA cube name
        selection: Channel selection string using CASA selection syntax
        unit:      Frequency unit

    Returns:
        List of pairs of frequency values (float) in the desired units
    """

    if selection in ('NONE', 'ALL', 'ALLCONT'):
        return [selection]

    frequencies = []
    if selection != '':
        qaTool = casa_tools.quanta

        # Get frequency axis
        try:
            refFreq, deltaFreq, freqUnit, refPix, numPix = _get_cube_freq_axis(img)
        except:
            LOG.error('No frequency axis found in %s.' % (img))
            return ['NONE']

        for crange in selection.split(';'):
            c0, c1 = list(map(float, crange.split('~')))
            # Make sure c0 is the lower channel so that the +/-0.5 channel
            # adjustments below go in the right direction.
            if (c1 < c0):
                c0, c1 = c1, c0

            # Convert the channel range (c0-c1) to the corresponding frequency range
            # that spans between the outer edges of this channel range. I.e., from
            # the lower frequency edge of c0 to the upper frequency edge of c1.
            f0 = qaTool.convert({'value': refFreq + (c0 - 0.5 - refPix) * deltaFreq, 'unit': freqUnit}, unit)
            f1 = qaTool.convert({'value': refFreq + (c1 + 0.5 - refPix) * deltaFreq, 'unit': freqUnit}, unit)
            if qaTool.lt(f0, f1):
                frequencies.append((f0['value'], f1['value']))
            else:
                frequencies.append((f1['value'], f0['value']))
    else:
        frequencies = ['NONE']

    return frequencies


def freq_selection_to_channels(img: str, selection: str) -> Union[List[int], List[str]]:
    """
    Convert frequency selection to channel tuples for a given CASA cube.

    Args:
        img:       CASA cube name
        selection: Frequency selection string using CASA syntax

    Returns:
        List of pairs of channel values (int)
    """

    if selection in ('NONE', 'ALL', 'ALLCONT'):
        return [selection]

    channels = []
    if selection != '':
        qaTool = casa_tools.quanta

        # Get frequency axis
        try:
            refFreq, deltaFreq, freqUnit, refPix, numPix = _get_cube_freq_axis(img)
        except:
            LOG.error('No frequency axis found in %s.' % (img))
            return ['NONE']

        p = re.compile(r'([\d.]*)(~)([\d.]*)(\D*)')
        for frange in p.findall(selection.replace(';', '')):
            f0 = qaTool.convert('%s%s' % (frange[0], frange[3]), freqUnit)['value']
            f1 = qaTool.convert('%s%s' % (frange[2], frange[3]), freqUnit)['value']
            # It is assumed here that the frequency ranges are given from
            # the lower edge of the lowest frequency channel to the upper
            # edge of the highest frequency channel, while the reference frequency
            # is specified at the center of the reference pixel (channel). To calculate
            # the corresponding channel range, we need to add 0.5 to the lower channel,
            # and subtract 0.5 from the upper channel.
            c0 = (f0 - refFreq) / deltaFreq
            c1 = (f1 - refFreq) / deltaFreq

            # Avoid stepping outside possible channel range
            c0 = max(c0, 0)
            c0 = min(c0, numPix - 1)
            c0 = int(utils.round_half_up(c0 + 0.5))
            c0 = max(c0, 0)
            c0 = min(c0, numPix - 1)

            c1 = max(c1, 0)
            c1 = min(c1, numPix - 1)
            c1 = int(utils.round_half_up(c1 - 0.5))
            c1 = max(c1, 0)
            c1 = min(c1, numPix - 1)

            if c0 < c1:
                channels.append((c0, c1))
            else:
                channels.append((c1, c0))
    else:
        channels = ['NONE']

    return channels


def spw_intersect(spw_range: List[float], line_regions: List[List[float]]) -> List[List[float]]:
    """
    This utility function takes a frequency range (as numbers with arbitrary
    but common units) and computes the intersection with a list of frequency
    ranges defining the regions of spectral lines. It returns the remaining
    ranges excluding the line frequency ranges.

    Args:
        spw_range:    List of two numbers defining the spw frequency range
        line_regions: List of lists of pairs of numbers defining frequency ranges
                      to be excluded

    Returns:
        List of lists of pairs of numbers defining the remaining frequency ranges
    """
    spw_sel_intervals = []
    for line_region in line_regions:
        if (line_region[0] <= spw_range[0]) and (line_region[1] >= spw_range[1]):
            spw_sel_intervals = []
            spw_range = []
            break
        elif (line_region[0] <= spw_range[0]) and (line_region[1] >= spw_range[0]):
            spw_range = [line_region[1], spw_range[1]]
        elif (line_region[0] >= spw_range[0]) and (line_region[1] < spw_range[1]):
            spw_sel_intervals.append([spw_range[0], line_region[0]])
            spw_range = [line_region[1], spw_range[1]]
        elif line_region[0] >= spw_range[1]:
            spw_sel_intervals.append(spw_range)
            spw_range = []
            break
        elif (line_region[0] >= spw_range[0]) and (line_region[1] >= spw_range[1]):
            spw_sel_intervals.append([spw_range[0], line_region[0]])
            spw_range = []
            break
    if spw_range != []:
        spw_sel_intervals.append(spw_range)

    return spw_sel_intervals


def update_sens_dict(dct: Dict, udct: Dict) -> None:
    """
    Update a sensitivity dictionary. All generic solutions
    tried so far did not do the job. So this method assumes
    an explicit dictionary structure of
    ['<MS name>']['<field name']['<intent>'][<spw>]: {<sensitivity result>}.

    Args:
        dct:  Sensitivities dictionary
        udct: Sensitivities update dictionary

    Returns:
        None. The main dictionary is modified in place.
    """
    for msname in udct:
        # Exclude special primary keys that are not MS names
        if msname not in ['recalc', 'robust', 'uvtaper']:
            if msname not in dct:
                dct[msname] = {}
            for field in udct[msname]:
                if field not in dct[msname]:
                    dct[msname][field] = {}
                for intent in udct[msname][field]:
                    if intent not in dct[msname][field]:
                        dct[msname][field][intent] = {}
                    for spw in udct[msname][field][intent]:
                        if spw not in dct[msname][field][intent]:
                            dct[msname][field][intent][spw] = {}
                        dct[msname][field][intent][spw] = udct[msname][field][intent][spw]


def update_beams_dict(dct: Dict, udct: Dict) -> None:
    """
    Update a beams dictionary. All generic solutions
    tried so far did not do the job. So this method assumes
    an explicit dictionary structure of
    ['<field name']['<intent>'][<spwids>]: {<beam>}.

    Args:
        dct:  Beams dictionary
        udct: Beams update dictionary

    Returns:
        None. The main dictionary is modified in place.
    """
    for field in udct:
        # Exclude special primary keys that are not MS names
        if field not in ['recalc', 'robust', 'uvtaper']:
            if field not in dct:
                dct[field] = {}
            for intent in udct[field]:
                if intent not in dct[field]:
                    dct[field][intent] = {}
                for spwids in udct[field][intent]:
                    if spwids not in dct[field][intent]:
                        dct[field][intent][spwids] = {}
                    dct[field][intent][spwids] = udct[field][intent][spwids]


def set_nested_dict(dct: Dict, keys: Tuple[Any], value: Any) -> None:
    """
    Set a hierarchy of dictionaries with given keys and value
    for the lowest level key.

    >>> d = {}
    >>> set_nested_dict(d, ('key1', 'key2', 'key3'), 1)
    >>> print(d)
    {'key1': {'key2': {'key3': 1}}}

    Args:
        dct:   Any dictionary
        keys : List of keys to build hierarchy
        value: Value for lowest level key

    Returns:
        None. The dictionary is modified in place.
    """
    for key in keys[:-1]:
        dct = dct.setdefault(key, {})
    dct[keys[-1]] = value


def intersect_ranges(ranges: List[Tuple[Union[float, int]]]) -> Tuple[Union[float, int]]:
    """
    Compute intersection of ranges.

    Args:
        ranges: List of tuples defining (frequency) intervals

    Returns:
        intersect_range: Tuple of two numbers defining the intersection
    """
    if len(ranges) == 0:
        return ()
    elif len(ranges) == 1:
        return ranges[0]
    else:
        intersect_range = ranges[0]
        for myrange in ranges[1:]:
            i0 = max(intersect_range[0], myrange[0])
            i1 = min(intersect_range[1], myrange[1])
            if i0 <= i1:
                intersect_range = (i0, i1)
            else:
                return ()

        return intersect_range


def intersect_ranges_by_weight(ranges: List[Tuple[Union[float, int]]], delta: float, threshold: float) -> Tuple[float]:
    """
    Compute intersection of ranges through weight arrays and a threshold.

    Args:
        ranges:    List of tuples defining frequency intervals
        delta:     Frequency step to be used for the intersection
        threshold: Threshold to be used for the intersection

    Returns:
        intersect_range: Tuple of two numbers defining the intersection
    """
    if len(ranges) == 0:
        return ()
    elif len(ranges) == 1:
        return ranges[0]
    else:
        min_v = min(numpy.array(ranges).flatten())
        max_v = max(numpy.array(ranges).flatten())
    max_range = numpy.arange(min_v, max_v+delta, delta)
    range_weights = numpy.zeros(max_range.shape, 'd')
    for myrange in ranges:
        range_weights += numpy.where((max_range >= myrange[0]) & (max_range <= myrange[1]), 1.0, 0.0)
    range_weights /= len(ranges)
    valid_indices = numpy.where(range_weights >= threshold)[0]
    if valid_indices.shape != (0,):
        return (max_range[valid_indices[0]], max_range[valid_indices[-1]])
    else:
        return ()


def merge_ranges(ranges: List[Tuple[Union[float, int]]]) -> Generator[List[Tuple[float]], None, None]:
    """
    Merge overlapping and adjacent ranges and yield the merged ranges
    in order. The argument must be an iterable of pairs (start, stop).

    Args:
        ranges: List of tuples of two numbers defining ranges
    Returns:
        Generator yielding tuples of merged ranges

    >>> list(merge_ranges([(5,7), (3,5), (-1,3)]))
    [(-1, 7)]
    >>> list(merge_ranges([(5,6), (3,4), (1,2)]))
    [(1, 2), (3, 4), (5, 6)]
    >>> list(merge_ranges([]))
    []

    (c) Gareth Rees 02/2013

    """
    ranges = iter(sorted(ranges))
    try:
        current_start, current_stop = next(ranges)
    except StopIteration:
        return
    for start, stop in ranges:
        if start > current_stop:
            # Gap between segments: output current segment and start a new one.
            yield current_start, current_stop
            current_start, current_stop = start, stop
        else:
            # Segments adjacent or overlapping: merge.
            current_stop = max(current_stop, stop)
    yield current_start, current_stop


def equal_to_n_digits(x: float, y: float, numdigits: int = 7) -> bool:
    """
    Approximate equality check up to a given number of digits.

    Args:
        x: First floating point number
        y: Second floating point number
        numdigits: Number of digits to check

    Returns:
        Boolean
    """
    try:
        numpy.testing.assert_approx_equal(x, y, numdigits)
        return True
    except:
        return False


def velocity_to_frequency(velocity: Union[Dict, str], restfreq: Union[Dict, str]) -> Union[Dict, str]:
    """
    Convert radial velocity to frequency using radio convention.

    f = f_rest * (1 - v/c)

    Args:
        velocity: velocity quantity
        restfreq: rest frequency quantity

    Returns:
        Frequency quantity in units of restfreq
    """

    cqa = casa_tools.quanta
    light_speed = float(cqa.getvalue(cqa.convert(cqa.constants('c'), 'km/s')))
    velocity = float(cqa.getvalue(cqa.convert(cqa.quantity(velocity), 'km/s')))
    val = float(cqa.getvalue(restfreq)) * (1 - velocity / light_speed)
    unit = cqa.getunit(restfreq)
    frequency = cqa.tos(cqa.quantity(val, unit))
    return frequency


def frequency_to_velocity(frequency: Union[Dict, str], restfreq: Union[Dict, str]) -> Union[Dict, str]:
    """
    Convert frequency to radial velocity using radio convention.

    v = c * (f_rest - f) / f_rest

    Args:
        frequency: frequency quantity
        restfreq: rest frequency quantity

    Returns:
        Velocity quantity in units of km/s
    """

    cqa = casa_tools.quanta
    light_speed = float(cqa.getvalue(cqa.convert(cqa.constants('c'), 'km/s')))
    restfreq = float(cqa.getvalue(cqa.convert(restfreq, 'MHz')))
    freq = float(cqa.getvalue(cqa.convert(frequency, 'MHz')))
    val = light_speed * ((restfreq - freq) / restfreq)
    velocity = cqa.tos(cqa.quantity(val, 'km/s'))
    return velocity


def predict_kernel(beam, target_beam, pstol=1e-6, patol=1e-3):
    """Predict the required convolution kernel to each a target restoring beam.
    
    pstol: the tolerance in arcsec for original vs. target bmaj/bmin identical or kernel "point source" like. 
    patol: the tolerance in degree for original vs. target bpa identical 

    return_code:
        0:  sucess, the target beam can be reached with a valid convolution kernel
        1:  fail, "point source" like
        2:  fail, unable to reach the target beam shape, and the original beam is probably already too large in a certain direction.

    Note:
        Although ia.deconvolvefrombeam() can also predict convolution kernel sizes, its return can be misleading
        in some circumstances (see CAS-13804). Therefore, we use ia.beamforconvolvedsize() here even though we have to catch the CASA runtime error messages.
    """
    cqa = casa_tools.quanta
    cia = casa_tools.image
    clog = casa_tools.casalog

    # default return code and kernel: fail (code=2) and a dummy kernel
    rt_kernel = {'major': {'unit': 'arcsec', 'value': 0.0},
                 'minor': {'unit': 'arcsec', 'value': 0.0},
                 'pa': {'unit': 'deg', 'value': 0.0}}
    rt_code = 2

    # ia.restoringbeam() return bpa under the key 'positionangle' while ia.commombeam() return bpa under 'pa'
    # we search the exact key here so both versions will work.
    t_bpa_key = 'positionangle' if 'positionangle' in target_beam else 'pa'
    bpa_key = 'positionangle' if 'positionangle' in beam else 'pa'

    t_bmaj = cqa.convert(target_beam['major'], 'arcsec')['value']
    t_bmin = cqa.convert(target_beam['minor'], 'arcsec')['value']
    t_bpa = cqa.convert(target_beam[t_bpa_key], 'deg')['value']
    bmaj = cqa.convert(beam['major'], 'arcsec')['value']
    bmin = cqa.convert(beam['minor'], 'arcsec')['value']
    bpa = cqa.convert(beam[bpa_key], 'deg')['value']

    if abs(t_bmaj-bmaj) < pstol and abs(t_bmin-bmin) < pstol and abs(t_bpa-bpa) < patol:
        LOG.info(
            'The target beam is identical or close to the original beam under the specified tolerance: ' +
            f'pstol = {pstol} arcsec and patol = {patol} deg.')
        rt_code = 1
    else:
        target_bm = [cqa.tos(target_beam['major']), cqa.tos(target_beam['minor']), cqa.tos(target_beam[t_bpa_key])]
        origin_bm = [cqa.tos(beam['major']), cqa.tos(beam['minor']), cqa.tos(beam[bpa_key])]

        # filter out the potential runtime error message when ia.beamforconvolvedsize() fails.
        with logging.log_filtermsg('Unable to reach target resolution of major'):
            try:
                rt_kernel = cia.beamforconvolvedsize(source=origin_bm, convolved=target_bm)
                if cqa.convert(rt_kernel['major'], 'arcsec')['value'] < pstol:
                    LOG.info('The kernel from ia.deconvolvefrombeam() is considered as a point-source under the tolerance ' +
                            f'pstol = {pstol} arcsec.')
                    rt_code = 1
                else:
                    LOG.info(f'The convolution kernel prediced by ia.deconvolvefrombeam is {rt_kernel} and larger than the tolerence ' +
                            f'pstol = {pstol} arcsec')
                    rt_code = 0
            except RuntimeError as e:
                LOG.info("Unable to reach the target beam shape because the original beam is probably already too large.")
                rt_code = 2


    return rt_kernel, rt_code
