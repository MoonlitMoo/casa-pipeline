"""A utility module to handle newly generated MeasurementSets."""
import os
from typing import Dict, Optional, Tuple

import pipeline.domain.measures as measures
import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.tablereader as tablereader
from pipeline.domain import MeasurementSet, ObservingRun
from pipeline.domain.singledish import MSReductionGroupDesc
from pipeline.infrastructure import casa_tools
from pipeline.infrastructure.utils import relative_path

from ... import heuristics

LOG = infrastructure.get_logger(__name__)

def generate_ms(name: str, ref_ms: MeasurementSet) -> MeasurementSet:
    """
    Generate MeasurementSet (MS) domain object of a new MS.

    This method generate a new MS domain object of a give name and transfer
    some information from the reference MS.
    The following MS information is copied from the reference MS to new MS
    assuming metadata indices are the same between new and reference MS:
    session, origin_ms, observing_pattern, calibration_strategy,
    is_known_eph_obj

    Args:
        name: The name of new MS
        ref_ms: The reference MS domain object from which information is
            transferred from.

    Returns:
        An MS domain object of the new MS.
    """
    # Check for existence of the output vis.
    if not os.path.exists(name):
        raise ValueError('Could not find {}'.format(os.path.basename(name)))

    # Import the new measurement set.
    to_import = relative_path(name)
    observing_run = tablereader.ObservingRunReader.get_observing_run(to_import)
    assert len(observing_run.measurement_sets) == 1

    # Transfer information from the source measurement set. The assumption is
    # that IDs would not change between ref and new MSes.
    new_ms = observing_run.measurement_sets[0]
    LOG.debug('Setting session to %s for %s', ref_ms.session, new_ms.basename)
    new_ms.session = ref_ms.session
    LOG.debug('Setting origin_ms.')
    new_ms.origin_ms = ref_ms.origin_ms
    LOG.debug('Setting observing_pattern, calibration_strategy and beam_size.')
    new_ms.observing_pattern = ref_ms.observing_pattern
    new_ms.calibration_strategy = ref_ms.calibration_strategy
    set_beam_size(new_ms)
    if hasattr(ref_ms, 'k2jy_factor'):
        LOG.debug('Copying k2jy_factor.')
        new_ms.k2jy_factor = ref_ms.k2jy_factor
    LOG.debug('Copying flags of ephemeris objects')
    __transfer_ephemeris_flag(new_ms, ref_ms)
    LOG.debug('Copying deviation mask.')
    if hasattr(ref_ms, 'deviation_mask'):
        new_ms.deviation_mask = ref_ms.deviation_mask
    LOG.debug('Copying org_direction')
    for f in ref_ms.fields:
        if hasattr(f.source, 'org_direction'):
            field = new_ms.get_fields(field_id = f.id)[0]
            if field.name == f.name:
                field.source.org_direction = f.source.org_direction

    return new_ms

def __transfer_ephemeris_flag(new_ms: MeasurementSet, ref_ms: MeasurementSet):
    """
    Copy is_known_eph_obj and is_eph_obj from ref_ms to new_ms.

    Args:
        new_ms: The new MS domain object from which information is
            transferred to.
        ref_ms: The reference MS domain object from which information is
            transferred from.
    """
    for f in new_ms.fields:
        ref_f = ref_ms.get_fields(field_id=f.id)[0]
        assert ref_f.source.name == f.source.name
        f.source.is_known_eph_obj = ref_f.source.is_known_eph_obj
        f.source.is_eph_obj = ref_f.source.is_eph_obj

def set_beam_size(ms: MeasurementSet):
    """
    Set beam size of each antenna and spwctral window pair to MS domain object.

    The beam size of each spectral window in MS is set as an attribute, 'beam_sizes'.
    The beam_sizes is a dictionary in the form beam_sizes[antenna ID][spw ID].

    Args:
        ms: An MS domain object to calculate and set beam size
    """
    beam_size_heuristic = heuristics.SingleDishBeamSize()
    beam_sizes = {}
    for antenna in ms.antennas:
        diameter = antenna.diameter
        antenna_id = antenna.id
        beam_size_for_antenna = {}
        for spw in ms.spectral_windows:
            spw_id = spw.id
            center_frequency = float(spw.centre_frequency.convert_to(measures.FrequencyUnits.GIGAHERTZ).value)
            beam_size = beam_size_heuristic(diameter=diameter, frequency=center_frequency)
            beam_size_quantity = casa_tools.quanta.quantity(beam_size, 'arcsec')
            beam_size_for_antenna[spw_id] = beam_size_quantity
        beam_sizes[antenna_id] = beam_size_for_antenna
    ms.beam_sizes = beam_sizes

def merge_reduction_group(observing_run: ObservingRun,
                          reduction_group: Dict[int, MSReductionGroupDesc]):
    """
    Merge a reduction group description to observing_run in context.

    Arg:
        observing_run: An ObservingRun associated with a context.
        reduction_group: A reduction group dictionary to merge. Key of
            dictionary are group IDs and values are reduction group
            descriptions.
    """
    if not hasattr(observing_run, 'ms_reduction_group'):
        LOG.info('Adding ms_reduction_group to observing_run')
        observing_run.ms_reduction_group = {}

    # merge reduction group
    for myid, mydesc in reduction_group.items():
        matched_id = -1
        for group_id, group_desc in observing_run.ms_reduction_group.items():
            if group_desc == mydesc:
                LOG.info('merge input group %s to group %s' % (myid, group_id))
                matched_id = group_id
                LOG.info('number of members before merge: %s' % (len(group_desc)))
                group_desc.merge(mydesc)
                LOG.info('number of members after merge: %s' % (len(group_desc)))
        if matched_id == -1:
            LOG.info('add new group')
            key = len(observing_run.ms_reduction_group)
            observing_run.ms_reduction_group[key] = mydesc

def inspect_reduction_group(ms: MeasurementSet) -> Dict[int, MSReductionGroupDesc]:
    """
    Inspect MS and define Reduction Group Description of the MS.

    Args:
        ms: An MS domain object to inspect

    Retruns:
        A dictionary of reduction group ID (key) and reduction group
        description (value).
    """
    reduction_group = {}
    group_spw_names = {}
    ms = ms
    science_windows = ms.get_spectral_windows(science_windows_only=True)
    if hasattr(ms, 'calibration_strategy'):
        field_list = ms.calibration_strategy['field_strategy'].keys()
    else:
        field_list = [f.id for f in ms.get_fields(intent='TARGET')]
    for field_id in field_list:
        fields = ms.get_fields(field_id)
        assert len(fields) == 1
        field = fields[0]
        field_name = field.name
        for spw in science_windows:
            spw_name = spw.name
            nchan = spw.num_channels
            min_frequency = float(spw._min_frequency.value)
            max_frequency = float(spw._max_frequency.value)
            if len(spw_name) > 0:
                # grouping by name
                match =__find_match_by_name(spw_name, field_name, group_spw_names)
            else:
                # grouping by frequency range
                match = __find_match_by_coverage(nchan, min_frequency, max_frequency,
                                                      reduction_group, fraction=0.99, field_name=field_name)
            if match == False:
                # add new group
                key = len(reduction_group)
                group_spw_names[key] = (spw_name, field_name)
                newgroup = MSReductionGroupDesc(spw_name=spw_name,
                                                min_frequency=min_frequency,
                                                max_frequency=max_frequency,
                                                nchan=nchan, field=field)
                reduction_group[key] = newgroup
            else:
                key = match
            ### Check existance of antenna, spw, field combination in MS
            ddid = ms.get_data_description(spw=spw)
            with casa_tools.TableReader(ms.name) as tb:
                subtb = tb.query('DATA_DESC_ID==%d && FIELD_ID==%d' % (ddid.id, field.id),
                                 columns='ANTENNA1')
                valid_antid = set(subtb.getcol('ANTENNA1'))
                subtb.close()
            for ant_id in valid_antid:
                reduction_group[key].add_member(ms, ant_id, spw.id, field_id)

    return reduction_group

def __find_match_by_name(spw_name: str, field_name: str,
                         group_names: Dict[int, Tuple[int, int]]) -> int:
    """
    Return a group ID that matches given spw and field.

    Args:
        spw_name: Name of spectral window to match
        field_name: Name of field to match
        group_names: Group information dictionary to search. Keys of dictionary
            are group IDs and values are a tuple of spw and field names.

    Returns:
        A group ID that matches spw_name and field_name.
    """
    match = False
    for group_key, names in group_names.items():
        group_spw_name = names[0]
        group_field_name = names[1]
        if group_spw_name == '':
            raise RuntimeError("Got empty group spectral window name")
        elif spw_name == group_spw_name and field_name == group_field_name:
            match = group_key
            break
    return match

def __find_match_by_coverage(nchan: int, min_frequency: float,
                             max_frequency: float,
                             reduction_group: Dict[int, MSReductionGroupDesc],
                             fraction: float=0.99,
                             field_name: Optional[str]=None) -> int:
    """
    Return a group ID that matches search criteria.

    Search reduction_group and find a reduction group that matches field_name
    and spw criteria (number of channels and frequency overlap). If field_name
    is not given, the first group ID that match spw criteria is retruned.

    Args:
        nchan: Number of channels in an spw to match
        min_frequency: The minimum frequency of the spw in the unit of Hz
        max_frequency: The maximum frequency of the spw in the unit of Hz
        reduction_group: A reduction group dictionary to search for match. Key
            of dictionary are group IDs and values are reduction group
            descriptions.
        fraction: The minimum overlapping fraction of spw
        field_name: Name of field to match

    Return:
        A reduction group ID.
    """
    if fraction <= 0 or fraction > 1.0:
        raise ValueError("overlap fraction should be between 0.0 and 1.0")
    LOG.warning("Creating reduction group by frequency overlap. This may not be proper if observation dates extend"
                " over long period.")
    match = False
    for group_key, group_desc in reduction_group.items():
        group_field_name = group_desc.field
        if field_name is not None and group_field_name != field_name:
            continue
        group_range = group_desc.frequency_range
        group_nchan = group_desc.nchan
        overlap = max(0.0, min(group_range[1], max_frequency) - max(group_range[0], min_frequency))
        width = max(group_range[1], max_frequency) - min(group_range[0], min_frequency)
        coverage = overlap/width
        if nchan == group_nchan and coverage >= fraction:
            match = group_key
            break
    return match
