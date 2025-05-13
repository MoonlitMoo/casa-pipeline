import collections
import datetime
import itertools
import operator
import os
import re
import xml.etree.ElementTree as ElementTree
from bisect import bisect_left
from functools import reduce
from typing import List, Tuple

import cachetools
import numpy

import pipeline.domain as domain
import pipeline.domain.measures as measures
import pipeline.infrastructure.utils as utils
from . import casa_tools
from . import logging
from .casa_tools import MSMDReader
from ..domain import Antenna, AntennaArray

LOG = logging.get_logger(__name__)


def find_EVLA_band(frequency, bandlimits=None, BBAND='?4PLSCXUKAQ?'):
    """identify VLA band"""
    if bandlimits is None:
        bandlimits = [0.0e6, 150.0e6, 700.0e6, 2.0e9, 4.0e9, 8.0e9, 12.0e9, 18.0e9, 26.5e9, 40.0e9, 56.0e9]
    i = bisect_left(bandlimits, frequency)

    return BBAND[i]


def _get_ms_name(ms):
    return ms.name if isinstance(ms, domain.MeasurementSet) else ms


def _get_ms_basename(ms):
    return ms.basename if isinstance(ms, domain.MeasurementSet) else ms


def _get_science_goal_value(science_goals, goal_keyword):
    value = None
    for science_goal in science_goals:
        keyword = science_goal.split('=')[0].replace(' ', '')
        if keyword != goal_keyword:
            continue
        if keyword == 'representativeSource':
            value = science_goal.split('=')[1].lstrip().rstrip()
        else:
            value = science_goal.split('=')[1].replace(' ', '')
        return value
    return value


class ObservingRunReader(object):
    @staticmethod
    def get_observing_run(ms_files):
        if isinstance(ms_files, str):
            ms_files = [ms_files]

        observing_run = domain.ObservingRun()
        for ms_file in ms_files:
            ms = MeasurementSetReader.get_measurement_set(ms_file)
            observing_run.add_measurement_set(ms)
        return observing_run


class MeasurementSetReader(object):
    @staticmethod
    def get_scans(msmd, ms):
        LOG.debug('Analysing scans in {0}'.format(ms.name))
        with casa_tools.TableReader(ms.name) as openms:
            scan_number_col = openms.getcol('SCAN_NUMBER')
            time_col = openms.getcol('TIME')
            antenna1_col = openms.getcol('ANTENNA1')
            antenna2_col = openms.getcol('ANTENNA2')
            data_desc_id_col = openms.getcol('DATA_DESC_ID')
            time_colkeywords = openms.getcolkeywords('TIME')

        # get columns and tools needed to create scan times
        time_unit = time_colkeywords['QuantumUnits'][0]
        time_ref = time_colkeywords['MEASINFO']['Ref']
        mt = casa_tools.measures
        qt = casa_tools.quanta

        # For each Observation ID, retrieve info on scans.
        scans = []

        data_desc_mask_dict = {}
        for dd in ms.data_descriptions:
            data_desc_mask_dict[dd.id] = (data_desc_id_col == dd.id)

        for obs_id in range(msmd.nobservations()):
            statesforscans = msmd.statesforscans(obsid=obs_id)
            fieldsforscans = msmd.fieldsforscans(obsid=obs_id, arrayid=0, asmap=True)
            spwsforscans = msmd.spwsforscans(obsid=obs_id)

            for scan_id in msmd.scannumbers(obsid=obs_id):
                states = [s for s in ms.states
                          if s.id in statesforscans[str(scan_id)]]

                intents = reduce(lambda s, t: s.union(t.intents), states, set())

                fields = [f for f in ms.fields if f.id in fieldsforscans[str(scan_id)]]

                # can't use msmd.timesforscan as we need unique times grouped
                # by spw
                # scan_times = msmd.timesforscan(scan_id)

                exposures = {spw_id: msmd.exposuretime(scan=scan_id, spwid=spw_id, obsid=obs_id)
                             for spw_id in spwsforscans[str(scan_id)]}

                scan_mask = (scan_number_col == scan_id)

                # get the antennas used for this scan 
                LOG.trace('Calculating antennas used for scan %s', scan_id)
                antenna_ids = set()
                scan_antenna1 = antenna1_col[scan_mask]
                scan_antenna2 = antenna2_col[scan_mask] 
                antenna_ids.update(scan_antenna1)
                antenna_ids.update(scan_antenna2)
                antennas = [o for o in ms.antennas if o.id in antenna_ids]

                # get the data descriptions for this scan
                LOG.trace('Calculating data descriptions used for scan %s', scan_id)
                scan_data_desc_id = set(data_desc_id_col[scan_mask])
                data_descriptions = [o for o in ms.data_descriptions if o.id in scan_data_desc_id]

                # times are specified per data description, so we must
                # re-mask and calculate times per dd
                scan_times = {}  
                LOG.trace('Processing scan times for scan %s', scan_id)
                for dd in data_descriptions:
                    dd_mask = scan_mask & data_desc_mask_dict[dd.id]

                    raw_midpoints = list(time_col[dd_mask])
                    epoch_midpoints = [mt.epoch(time_ref, qt.quantity(o, time_unit))
                                       for o in (numpy.min(raw_midpoints), numpy.max(raw_midpoints))]

                    scan_times[dd.spw.id] = list(zip(epoch_midpoints, itertools.repeat(exposures[dd.spw.id])))

                LOG.trace('Creating domain object for scan %s', scan_id)
                scan = domain.Scan(id=scan_id, states=states, fields=fields, data_descriptions=data_descriptions,
                                   antennas=antennas, scan_times=scan_times, intents=intents)
                scans.append(scan)

                LOG.trace('{0}'.format(scan))

        return scans

    @staticmethod
    def add_band_to_spws(ms: domain.MeasurementSet) -> None:
        """
        Sets spw.band, which is a string describing a band. 
        """
        observatory = ms.antenna_array.name.upper()
        
        # This dict is only populated if the spw's band number cannot be determined from its name for ALMA.
        alma_receiver_band = {}

        for spw in ms.spectral_windows:
            if spw.type == 'WVR':
                spw.band = 'WVR'
                LOG.debug("For MS {}, SpW {}, setting band to WVR.".format(ms.name, spw.id))
                continue

            # Determining the band number for ALMA data 
            #
            # First, try to determine the band number from the spw name
            # The expected format is something like ALMA_RB_03#BB_1#SW-01#FULL_RES
            # If this doesn't work and this is ALMA data, try to get the band number from the ASDM_RECEIVER table
            # If this also fails, then set the band number using a look-up-table. 
            #
            # See: PIPE-140 or PIPE-1078
            #
            alma_band_regex = r'ALMA_RB_(?P<band>\d+)'
            match_found = re.search(alma_band_regex, spw.name)
            if match_found:
                band_str = match_found.groupdict()['band']
                band_num = int(band_str)
                spw.band = 'ALMA Band %s' % band_num
                LOG.debug("For MS {}, SpW {}, setting band to {}, based on the SPW name.".format(ms.name, spw.id, spw.band))
                continue
            elif observatory == 'ALMA': 
                if not alma_receiver_band:
                    alma_receiver_band = SpectralWindowTable.get_receiver_info(ms, get_band_info=True)
                if spw.id in alma_receiver_band:
                    match_receiver_band = re.search(alma_band_regex, alma_receiver_band[spw.id])
                    if match_receiver_band:
                        band_str = match_receiver_band.groupdict()['band']
                        band_num = int(band_str)
                        spw.band = 'ALMA Band %s' % band_num 
                        LOG.debug("For MS {}, SpW {}, setting band to {}, based on ASDM_RECEIVER table.".format(ms.name, spw.id, spw.band))
                        continue
                else:
                    LOG.debug("For MS {}, SpW {}, could not find band number information in ALMA_RECEIVER table.".format(ms.name, spw.id))

            # If both fail for ALMA, set the band number as follows: 
            spw.band = BandDescriber.get_description(spw.ref_frequency, observatory=ms.antenna_array.name)
            LOG.debug("For MS {}, SpW {}, setting band to {}, based on Pipeline internal look-up table.".format(ms.name, spw.id, spw.band))

            # Used EVLA band name from spw instead of frequency range

            if observatory in ('VLA', 'EVLA'):
                spw2band = ms.get_vla_spw2band()

                try:
                    EVLA_band = spw2band[spw.id]
                except:
                    LOG.info('Unable to get band from spw id - using reference frequency instead')
                    freqHz = float(spw.ref_frequency.value)
                    EVLA_band = find_EVLA_band(freqHz)

                EVLA_band_dict = {'4': '4m (4)',
                                  'P': '90cm (P)',
                                  'L': '20cm (L)',
                                  'S': '13cm (S)',
                                  'C': '6cm (C)',
                                  'X': '3cm (X)',
                                  'U': '2cm (Ku)',
                                  'K': '1.3cm (K)',
                                  'A': '1cm (Ka)',
                                  'Q': '0.7cm (Q)'}

                spw.band = EVLA_band_dict[EVLA_band]

    @staticmethod
    def add_spectralspec_spwmap(ms):
        ms.spectralspec_spwmap = utils.get_spectralspec_to_spwid_map(ms.spectral_windows)

    @staticmethod
    def add_spectralspec_to_spws(ms):
        # For ALMA, extract spectral spec from spw name.
        for spw in ms.spectral_windows:
            if 'ALMA' in spw.name:
                i = spw.name.find('#')
                if i != -1:
                    spw.spectralspec = spw.name[:i]

    @staticmethod
    def link_intents_to_spws(msmd, ms):
        # we can't use msmd.intentsforspw directly as we may have a modified
        # obsmode mapping

        scansforspws = msmd.scansforspws()

        # with one scan containing many spws, many of the statesforscan
        # arguments are repeated. This cache speeds up the subsequent
        # duplicate calls.
        class StatesCache(cachetools.LRUCache):
            def __missing__(self, key):
                return msmd.statesforscan(key)

        cached_states = StatesCache(1000)

        for spw in ms.spectral_windows:
            scan_ids = scansforspws[str(spw.id)]
            state_ids = [cached_states[i] for i in scan_ids]
            state_ids = set(itertools.chain(*state_ids))
            states = [s for s in ms.states if s.id in state_ids] 

            for state in states:
                spw.intents.update(state.intents)

            LOG.trace('Intents for spw #{0}: {1}'.format(spw.id, ','.join(spw.intents)))

    @staticmethod
    def link_fields_to_states(msmd, ms):
        # for each field..

        class StatesCache(cachetools.LRUCache):
            def __missing__(self, key):
                return msmd.statesforscan(key)

        cached_states = StatesCache(1000)

        for field in ms.fields:
            # Find the state IDs for the field by first identifying the scans
            # for the field, then finding the state IDs for those scans

            try:
                scan_ids = msmd.scansforfield(field.id)
            except:
                LOG.debug("Field " + str(field.id) + " not in scansforfields dictionary.")
                continue

            state_ids = [cached_states[i] for i in scan_ids]
            # flatten the state IDs to a 1D list
            state_ids = set(itertools.chain(*state_ids))
            states = [ms.get_state(i) for i in state_ids]

            # some scans may have multiple fields and/or intents so
            # it is necessary to distinguish which intents belong to
            # each field
            obs_modes_for_field = set(msmd.intentsforfield(field.id))
            states_for_field = [s for s in states if not obs_modes_for_field.isdisjoint(s.obs_mode.split(','))]

            field.states.update(states_for_field)
            for state in states_for_field:
                field.intents.update(state.intents)

    @staticmethod
    def link_fields_to_sources(msmd, ms):        
        for source in ms.sources:
            field_ids = msmd.fieldsforsource(source.id, False)
            fields = [f for f in ms.fields if f.id in field_ids]

            source.fields[:] = fields
            for field in fields:
                field.source = source

    @staticmethod
    def link_spws_to_fields(msmd, ms):
        spwsforfields = msmd.spwsforfields()
        for field in ms.fields:
            try:
                spws = [spw for spw in ms.spectral_windows if spw.id in spwsforfields[str(field.id)]]
                field.valid_spws.update(spws)
            except:
                LOG.debug("Field "+str(field.id) + " not in spwsforfields dictionary.")

    @staticmethod
    def get_measurement_set(ms_file: str) -> domain.MeasurementSet:
        LOG.info('Analysing {0}'.format(ms_file))
        ms = domain.MeasurementSet(ms_file)

        # populate ms properties with results of table readers
        with casa_tools.MSMDReader(ms_file) as msmd:
            LOG.info('Populating ms.antenna_array...')
            ms.antenna_array = AntennaTable.get_antenna_array(msmd)
            LOG.info('Populating ms.spectral_windows...')
            ms.spectral_windows = RetrieveByIndexContainer(SpectralWindowTable.get_spectral_windows(msmd, ms))
            LOG.info('Populating ms.states...')
            ms.states = RetrieveByIndexContainer(StateTable.get_states(msmd))
            LOG.info('Populating ms.fields...')
            ms.fields = RetrieveByIndexContainer(FieldTable.get_fields(msmd))
            LOG.info('Populating ms.sources...')
            ms.sources = RetrieveByIndexContainer(SourceTable.get_sources(msmd))
            LOG.info('Populating ms.data_descriptions...')
            ms.data_descriptions = RetrieveByIndexContainer(DataDescriptionTable.get_descriptions(msmd, ms))
            LOG.info('Populating ms.polarizations...')
            ms.polarizations = PolarizationTable.get_polarizations(msmd)
            LOG.info('Populating ms.correlator_name...')
            ms.correlator_name = MeasurementSetReader._get_correlator_name(ms)

            # For now the SBSummary table is ALMA specific
            if 'ALMA' in msmd.observatorynames():
                sbinfo = SBSummaryTable.get_sbsummary_info(ms, msmd.observatorynames())

                if sbinfo.repSource is None:
                    LOG.attention('Unable to identify representative target for %s. Will try to fall back to existing'
                                  ' science target sources in the imaging tasks.' % ms.basename)
                else:
                    if sbinfo.repSource == 'none':
                        LOG.warning('Representative target for %s is set to "none". Will try to fall back to existing'
                                    ' science target sources or calibrators in the imaging tasks.' % ms.basename)
                    LOG.info('Populating ms.representative_target ...')
                    ms.representative_target = (sbinfo.repSource, sbinfo.repFrequency, sbinfo.repBandwidth)
                    ms.representative_window = sbinfo.repWindow

                LOG.info('Populating ms.observing_modes...')
                ms.observing_modes = SBSummaryTable.get_observing_modes(ms)

                LOG.info('Populating ms.science_goals...')
                if sbinfo.minAngResolution is None and sbinfo.maxAngResolution is None:
                    # Only warn if the number of 12m antennas is greater than the number of 7m antennas
                    # and if the observation is not single dish
                    if len([a for a in ms.get_antenna() if a.diameter == 12.0]) > \
                            len([a for a in ms.get_antenna() if a.diameter == 7.0]) \
                            and 'Standard Single Dish' not in ms.observing_modes:
                        LOG.warning('Undefined angular resolution limits for %s' % ms.basename)
                    ms.science_goals = {'minAcceptableAngResolution': '0.0arcsec',
                                        'maxAcceptableAngResolution': '0.0arcsec'}
                else:
                    ms.science_goals = {'minAcceptableAngResolution': sbinfo.minAngResolution,
                                        'maxAcceptableAngResolution': sbinfo.maxAngResolution}

                if sbinfo.maxAllowedBeamAxialRatio is None:
                    ms.science_goals['maxAllowedBeamAxialRatio'] = '0.0'
                else:
                    ms.science_goals['maxAllowedBeamAxialRatio'] = sbinfo.maxAllowedBeamAxialRatio

                if sbinfo.sensitivity is None:
                    ms.science_goals['sensitivity'] = '0.0mJy'
                else:
                    ms.science_goals['sensitivity'] = sbinfo.sensitivity

                if sbinfo.dynamicRange is None:
                    ms.science_goals['dynamicRange'] = '1.0'
                else:
                    ms.science_goals['dynamicRange'] = sbinfo.dynamicRange

                ms.science_goals['spectralDynamicRangeBandWidth'] = sbinfo.spectralDynamicRangeBandWidth

                ms.science_goals['sbName'] = sbinfo.sbName
            
                # Populate the online ALMA Control Software names
                LOG.info('Populating ms.acs_software_version and ms.acs_software_build_version...')
                ms.acs_software_version, ms.acs_software_build_version = \
                    MeasurementSetReader.get_acs_software_version(ms, msmd)
                    
            LOG.info('Populating ms.array_name...')
            # No MSMD functions to help populating the ASDM_EXECBLOCK table
            ms.array_name = ExecblockTable.get_execblock_info(ms)

            with casa_tools.MSReader(ms.name) as openms:
                for dd in ms.data_descriptions:
                    openms.selectinit(reset=True)
                    # CAS-11207: from ~CASA 5.3pre89 onwards, getdata fails if
                    # the data selection does not select any datadd is
                    # missing. To compensate for this, selectinit now returns
                    # a boolean that indicates the status of data selection
                    # (True = selection contains data); this can be used to
                    # check that the subsequent getdata call will succeed.
                    if openms.selectinit(datadescid=dd.id):
                        ms_info = openms.getdata(['axis_info', 'time'])

                        dd.obs_time = numpy.mean(ms_info['time'])
                        dd.chan_freq = ms_info['axis_info']['freq_axis']['chan_freq'].tolist()
                        dd.corr_axis = ms_info['axis_info']['corr_axis'].tolist()

            # now back to pure MSMD calls
            LOG.info('Linking fields to states...')
            MeasurementSetReader.link_fields_to_states(msmd, ms)
            LOG.info('Linking fields to sources...')
            MeasurementSetReader.link_fields_to_sources(msmd, ms)
            LOG.info('Linking intents to spws...')
            MeasurementSetReader.link_intents_to_spws(msmd, ms)
            LOG.info('Linking spectral windows to fields...')
            MeasurementSetReader.link_spws_to_fields(msmd, ms)
            LOG.info('Populating ms.scans...')
            ms.scans = MeasurementSetReader.get_scans(msmd, ms)

            (observer, project_id, schedblock_id, execblock_id) = ObservationTable.get_project_info(msmd)

        # Update spectral windows in ms with band and spectralspec.
        MeasurementSetReader.add_band_to_spws(ms)
        MeasurementSetReader.add_spectralspec_to_spws(ms)

        # Populate mapping of spectralspecs to spws.
        MeasurementSetReader.add_spectralspec_spwmap(ms)

        # work around NumPy bug with empty strings
        # http://projects.scipy.org/numpy/ticket/1239
        ms.observer = str(observer)
        ms.project_id = str(project_id)

        ms.schedblock_id = schedblock_id
        ms.execblock_id = execblock_id

        return ms

    @staticmethod
    def _get_range(filename, column):
        with casa_tools.MSReader(filename) as ms:
            data = ms.range([column])
            return list(data.values())[0]

    @staticmethod
    def _get_correlator_name(ms: domain.MeasurementSet) -> str:
        """
        Get correlator name information from the PROCESSOR table in the MS. 
        
        The name is set to the value of the SUB_TYPE for the first row with
        CORRELATOR for its TYPE value. 

        :param ms: the measurements set to get the correlator name for
        :return: the correlator name
        """
        correlator_name = None
        try:
            with casa_tools.TableReader(ms.name + '/PROCESSOR') as table:
                tb1 = table.query("TYPE=='CORRELATOR'")
                sub_types_col = tb1.getcol('SUB_TYPE')
                tb1.close()

            if len(sub_types_col) > 0:
                correlator_name = str(sub_types_col[0])
            else:
                msg = "No correlator name could be found for {}".format(ms.basename)
                LOG.warning(msg)

        except Exception as e:
            correlator_name = None
            msg = "Error while populating correlator name for {}, error: {}".format(ms.basename, str(e))
            LOG.warning(msg)
            
        return correlator_name

    @staticmethod
    def get_acs_software_version(ms, msmd) -> Tuple[str, str]:
        """
        Retrieve the ALMA Common Software version and build version from the ASDM_ANNOTATION table. 

        Returns: 
            A tuple containing a string with the ACS software version, then a string with 
            the ACS software build version.
        """
        acs_software_version = "Unknown"
        acs_software_build_version = "Unknown"

        annotation_table = os.path.join(msmd.name(), 'ASDM_ANNOTATION') 
        try:
            with casa_tools.TableReader(annotation_table) as table:
                acs_software_details = table.getcol('details')[0]

                # This value can be "WARNING: No ACS_TAG available" in the ASDM_ANNOTATION table.
                if "WARNING" in acs_software_details: 
                    acs_software_version = "Unknown"
                else: 
                    acs_software_version = acs_software_details

                acs_software_build_version = table.getcol('details')[1]
        except: 
            LOG.info("Unable to read Annotation table information for MS {}".format(_get_ms_basename(ms)))

        return (acs_software_version, acs_software_build_version)


class SpectralWindowTable(object):
    @staticmethod
    def get_spectral_windows(msmd, ms):
        # map spw ID to spw type
        spw_types = {i: 'FDM' for i in msmd.fdmspws()}
        spw_types.update({i: 'TDM' for i in msmd.tdmspws()})
        spw_types.update({i: 'WVR' for i in msmd.wvrspws()})
        spw_types.update({i: 'CHANAVG' for i in msmd.chanavgspws()})
        spw_types.update({i: 'SQLD' for i in msmd.almaspws(sqld=True)})

        # these msmd functions don't need a spw argument. They return a list of
        # values, one for each spw
        spw_names = msmd.namesforspws()            
        bandwidths = msmd.bandwidths()

        # We need the first TARGET source ID to get the correct transitions
        try:
            first_target_field_id = msmd.fieldsforintent('*TARGET*')[0]
            first_target_source_id = msmd.sourceidforfield(first_target_field_id)
        except:
            first_target_source_id = 0

        target_spw_ids = msmd.spwsforintent('*TARGET*')

        # Read in information on receiver for current MS.
        receiver_info = SpectralWindowTable.get_receiver_info(ms)

        # Read in information about the SDM_NUM_BIN column for the current ms
        sdm_num_bins = SpectralWindowTable.get_sdm_num_bin_info(ms, msmd)

        # PIPE-1538: Compute median feed receptor angle.
        receptor_angle_info = SpectralWindowTable.get_receptor_angle(ms)

        spws = []
        for i, spw_name in enumerate(spw_names):
            # get this spw's values from our precalculated lists and dicts
            bandwidth = bandwidths[i]
            spw_type = spw_types.get(i, 'UNKNOWN')

            # the following msmd functions need a spw argument, so they have
            # to be contained within the spw loop
            mean_freq = msmd.meanfreq(i)
            chan_freqs = msmd.chanfreqs(i)
            chan_widths = msmd.chanwidths(i)         
            chan_effective_bws = msmd.chaneffbws(i)
            sideband = msmd.sideband(i)
            # BBC_NO column is optional
            if 'NRO' in msmd.observatorynames():
                # For Nobeyama (TODO: how to define BBC_NO for NRO)
                baseband = i
            else:
                baseband = msmd.baseband(i)

            ref_freq = msmd.reffreq(i)
            
            # Read transitions for target spws.

            # PIPE-2124: Missing of the TRANSITION column (e.g. old data) or lack of (sourceid, spwid) entries
            # in the SOURCE table might cause dubious "SEVERE" messages. Here we temporarily filter out them and
            # later replace with generic messages of missing the transition metadata in the MS subtable.
            transitions = False
            with logging.log_filtermsg('SOURCE table does not contain a row'):
                if i in target_spw_ids:
                    try:
                        # The msmd.transitions(..) call below can return a boolean value of False or
                        # a Numpy array with dtype=numpy.str_ , e.g.,
                        #   CASA <15>: msmd.transitions(sourceid=2,spw=16)
                        #   Out[15]: array(['N2H__v_0_J_1_0(ID=3925982)'], dtype='<U26')
                        # For invalid source/spw combinations, the call could also trigger an exception with a RuntimeError.
                        # Also see: https://casadocs.readthedocs.io/en/latest/api/tt/casatools.msmetadata.html#casatools.msmetadata.msmetadata.transitions
                        transitions = msmd.transitions(sourceid=first_target_source_id, spw=i)
                    except RuntimeError:
                        pass

            if transitions is False:
                LOG.info('No transition info available for SOURCE_ID=%s and SPECTRAL_WINDOW_ID=%s', first_target_source_id, i)
                transitions = ['Unknown']

            # Create simple name for spectral window if none was provided.
            if spw_name in [None, '']:
                spw_name = 'spw_%s' % str(i)

            # Extract receiver type and LO frequencies for current spw.
            try:
                receiver, freq_lo = receiver_info[i]
            except KeyError:
                LOG.info("No receiver info available for MS {} spw id {}".format(_get_ms_basename(ms), i))
                receiver, freq_lo = None, None

            # Extract feed receptor angle for current spw.
            try:
                median_receptor_angle = receptor_angle_info[i]
            except KeyError:
                LOG.info("No feed info available for MS {} spw id {}".format(_get_ms_basename(ms), i))
                median_receptor_angle = None

            # If the earlier get_sdm_num_bin_info call returned None, need to set sdm_num_bin value to None for each spw
            if sdm_num_bins is None: 
                sdm_num_bin = None
            else: 
                sdm_num_bin = sdm_num_bins[i]

            # Fetch and add correlation bits information
            correlation_bits = msmd.corrbit(i)

            spw = domain.SpectralWindow(i, spw_name, spw_type, bandwidth, ref_freq, mean_freq, chan_freqs, chan_widths,
                                        chan_effective_bws, sideband, baseband, receiver, freq_lo,
                                        transitions=transitions, sdm_num_bin=sdm_num_bin, correlation_bits=correlation_bits,
                                        median_receptor_angle=median_receptor_angle)
            spws.append(spw)

        return spws

    @staticmethod
    def get_receptor_angle(ms):
        """
        Extract information about the feed receptor angle from the FEED table's
        RECEPTOR_ANGLE column, and compute an average value over all antennas
        for each SpW.
        Return: a dict in which each MS SpW corresponds to an array of angles,
        or an empty dict in case of error.
        """
        # Get mapping of ASDM spectral window id to MS spectral window id.
        asdm_to_ms_spw_map = SpectralWindowTable.get_asdm_to_ms_spw_mapping(ms)

        # Construct path to FEED table.
        msname = _get_ms_name(ms)
        feed_table = os.path.join(msname, 'FEED')

        angle_info = {}
        try:
            with casa_tools.TableReader(feed_table) as tb:
                # Extract the ASDM spw ids column.
                asdm_spwids = sorted(set(tb.getcol('SPECTRAL_WINDOW_ID')))

                # Go through the table row-by-row, and extract info for each
                # ASDM spwid encountered:
                for asdm_spwid in asdm_spwids:
                    # Get MS spwid corresponding to the current ASDM spwid.
                    ms_spwid = asdm_to_ms_spw_map[asdm_spwid]

                    # Compute median feed angle, discarding non-linear polarizations.
                    tsel = tb.query(f"SPECTRAL_WINDOW_ID == {ms_spwid}")
                    angle = tsel.getcol('RECEPTOR_ANGLE')
                    pol = tsel.getcol('POLARIZATION_TYPE')
                    use = numpy.logical_or(pol == 'X', pol == 'Y')
                    angle[~use] = numpy.nan
                    if not numpy.all(numpy.isnan(angle)):
                        angle_info[ms_spwid] = numpy.degrees(numpy.nanmedian(angle, axis=1))
                    tsel.close()
        except Exception as ex:
            LOG.info("Unable to read feed info for MS {}: {}".format(_get_ms_basename(ms), ex))

        return angle_info

    @staticmethod
    def get_sdm_num_bin_info(ms, msmd):
        """
        Extract information about the online spectral averaging from the SPECTRAL_WINDOW
        table's SDM_NUM_BIN column.
        
        :param ms: measurement set to inspect
        :param msmd: msmetadata (casa_tools.MSMDReader) for the measurement set.
        :return: list of values for sdm_num_bin
        """
        # Read and return the online spectral averaging information if available
        sdm_num_bin = None
        if 'ALMA' in msmd.observatorynames() or 'EVLA' in msmd.observatorynames():
            with casa_tools.TableReader(ms.name + '/SPECTRAL_WINDOW') as table:
                if 'SDM_NUM_BIN' in table.colnames():
                    sdm_num_bin = table.getcol('SDM_NUM_BIN')
                else:
                    LOG.info(f"SDM_NUM_BIN does not exist in the SPECTRAL_WINDOW Table of MS {_get_ms_basename(ms)}")
        return sdm_num_bin

    @staticmethod
    def get_receiver_info(ms, get_band_info=False):
        """
        Extract information about the receiver from the ASDM_RECEIVER table.
        The following properties are extracted by default:
        * receiver type (e.g.: TSB, DSB, NOSB)
        * local oscillator frequencies

        If get_band_info is set to True, instead, only the frequency 
        band information is extracted. 

        If multiple entries are present for the same ASDM spwid, then keep

        :param ms: measurement set to inspect
        :return: dict of MS spw: (receiver_type, freq_lo) or MS spw: frequency_band
        """
        # Get mapping of ASDM spectral window id to MS spectral window id.
        asdm_to_ms_spw_map = SpectralWindowTable.get_asdm_to_ms_spw_mapping(ms)

        # Construct path to ASDM_RECEIVER table.
        msname = _get_ms_name(ms)
        receiver_table = os.path.join(msname, 'ASDM_RECEIVER')

        receiver_info = {}
        try:
            # Read in required columns from table.
            with casa_tools.TableReader(receiver_table) as tb:
                # Extract the ASDM spw ids column.
                spwids = tb.getcol('spectralWindowId')

                # Go through the table row-by-row, and extract info for each
                # ASDM spwid encountered:
                for i, spwid in enumerate(spwids):
                    # Assume that ASDM spectral windows are stored as a string
                    # such as "SpectralWindow_<nn>", where <nn> is the ID integer.
                    _, asdm_spwid = spwid.split('_')

                    # Get MS spwid corresponding to the current ASDM spwid.
                    ms_spwid = asdm_to_ms_spw_map[int(asdm_spwid)]

                    if get_band_info: 
                        # Add and return frequency band information stored in receiver table 
                        if ms_spwid not in receiver_info:
                            receiver_info[ms_spwid] = tb.getcell("frequencyBand", i)
                    else:
                        # Add the information from the current row if either:
                        #  a.) no info for the current spwid was stored yet.
                        #  b.) info was already stored for the current spwid, but
                        #      this info was not for receiver type of "TSB" or "DSB".
                        # This will store one entry for each ASDM spwid encountered,
                        # preferentially the first TSB/DSB row in the table
                        # corresponding to the spwid, but otherwise the first
                        # non-TSB/DSB row corresponding to the spwid.
                        if ms_spwid not in receiver_info or receiver_info[ms_spwid][0] not in ["TSB", "DSB"]:
                            receiver_info[ms_spwid] = (tb.getcell("receiverSideband", i), tb.getcell("freqLO", i))
        except:
            LOG.info("Unable to read receiver info for MS {}".format(_get_ms_basename(ms)))
            receiver_info = {}

        return receiver_info

    @staticmethod
    def parse_spectral_window_ids_from_xml(xml_path):
        """
        Extract the spectral window ID element from each row of an XML file.

        :param xml_path: path for XML file
        :return: list of integer spectral window IDs
        """
        ids = []
        try:
            root_element = ElementTree.parse(xml_path)

            for row in root_element.findall('row'):
                element = row.findtext('spectralWindowId')
                _, str_id = element.split('_')
                ids.append(int(str_id))
        except IOError:
            LOG.info("Could not parse XML at: {}".format(xml_path))

        return ids

    @staticmethod
    def get_data_description_spw_ids(ms):
        """
        Extract a list of spectral window IDs from the DataDescription XML for an
        ASDM.

        This function assumes the XML has been copied across to the measurement
        set directory.

        :param ms: measurement set to inspect
        :return: list of integers corresponding to ASDM spectral window IDs
        """
        result = []
        xml_path = os.path.join(ms.name, 'DataDescription.xml')
        if not os.path.exists(xml_path):
            LOG.info("No DataDescription XML found at {}.".format(xml_path))
        else:
            result = SpectralWindowTable.parse_spectral_window_ids_from_xml(xml_path)

        return result

    @staticmethod
    def get_spectral_window_spw_ids(ms):
        """
        Extract a list of spectral window IDs from the SpectralWindow XML for an
        ASDM.

        This function assumes the XML has been copied across to the measurement
        set directory.

        :param ms: measurement set to inspect
        :return: list of integers corresponding to ASDM spectral window IDs
        """
        result = []
        xml_path = os.path.join(ms.name, 'SpectralWindow.xml')
        if not os.path.exists(xml_path):
            LOG.info("No SpectralWindow XML found at {}.".format(xml_path))
        else:
            result = SpectralWindowTable.parse_spectral_window_ids_from_xml(xml_path)

        return result

    @staticmethod
    def get_asdm_to_ms_spw_mapping(ms):
        """
        Get the mapping of ASDM spectral window ID to Measurement Set spectral
        window ID.

        This function requires the SpectralWindow and DataDescription ASDM XML
        files to have been copied across to the measurement set directory.

        :param ms: measurement set to inspect
        :return: dict of ASDM spw: MS spw
        """
        dd_spws = SpectralWindowTable.get_data_description_spw_ids(ms)
        spw_spws = SpectralWindowTable.get_spectral_window_spw_ids(ms)
        asdm_ids = [i for i in spw_spws if i in dd_spws] + [i for i in spw_spws if i not in dd_spws]
        return {k: v for k, v in zip(asdm_ids, spw_spws)}


class ObservationTable(object):
    @staticmethod
    def get_project_info(msmd):
        project_id = msmd.projects()[0]
        observer = msmd.observers()[0]

        schedblock_id = 'N/A'
        execblock_id = 'N/A'

        obsnames = msmd.observatorynames()

        if 'ALMA' in obsnames or 'VLA' in obsnames or 'EVLA' in obsnames:
            # TODO this would break if > 1 observation in an EB. Can that
            # ever happen?
            d = {}
            for cell in msmd.schedule(0):
                key, val = cell.split()
                d[key] = val

            schedblock_id = d.get('SchedulingBlock', 'N/A')
            execblock_id = d.get('ExecBlock', 'N/A')

        return observer, project_id, schedblock_id, execblock_id


class AntennaTable(object):
    @staticmethod
    def get_antenna_array(msmd: MSMDReader) -> AntennaArray:
        position = msmd.observatoryposition()            
        names = set(msmd.observatorynames())
        assert len(names) == 1
        name = names.pop()
        antennas = AntennaTable.get_antennas(msmd)
        return domain.AntennaArray(name, position, antennas)

    @staticmethod
    def get_antennas(msmd: MSMDReader) -> List[Antenna]:
        antenna_table = os.path.join(msmd.name(), 'ANTENNA')
        LOG.trace('Opening ANTENNA table to read ANTENNA.FLAG_ROW')
        with casa_tools.TableReader(antenna_table) as table:
            flags = table.getcol('FLAG_ROW')

        antennas = []
        for (i, name, station) in zip(msmd.antennaids(), msmd.antennanames(), msmd.antennastations()):
            # omit this antenna if it has been flagged
            if flags[i]:
                continue

            position = msmd.antennaposition(i)
            offset = msmd.antennaoffset(i)
            diameter_m = casa_tools.quanta.convert(msmd.antennadiameter(i), 'm')
            diameter = casa_tools.quanta.getvalue(diameter_m)[0]

            antenna = domain.Antenna(i, name, station, position, offset, diameter)
            antennas.append(antenna)

        return antennas


class DataDescriptionTable(object):
    @staticmethod
    def get_descriptions(msmd, ms):
        spws = ms.spectral_windows
        # read the data descriptions table and create the objects
        descriptions = [DataDescriptionTable._create_data_description(spws, *row) 
                        for row in DataDescriptionTable._read_table(msmd)]

        return descriptions            

    @staticmethod
    def _create_data_description(spws, dd_id, spw_id, pol_id):
        # find the SpectralWindow matching the given spectral window ID
        matching_spws = [spw for spw in spws if spw.id == spw_id]
        spw = matching_spws[0]

        return domain.DataDescription(dd_id, spw, pol_id)

    @staticmethod
    def _read_table(msmd):
        """
        Read the DATA_DESCRIPTION table of the given measurement set.
        """
        LOG.debug('Analysing DATA_DESCRIPTION table')

        dd_ids = msmd.datadescids()
        spw_ids = msmd.spwfordatadesc()
        pol_ids = msmd.polidfordatadesc()

        return list(zip(dd_ids, spw_ids, pol_ids))


SBSummaryInfo = collections.namedtuple(
    'SBSummaryInfo', 'repSource repFrequency repBandwidth repWindow minAngResolution maxAngResolution '
                     'maxAllowedBeamAxialRatio sensitivity dynamicRange spectralDynamicRangeBandWidth sbName')


class SBSummaryTable(object):
    @staticmethod
    def get_sbsummary_info(ms, obsnames):
        try:
            sbsummary_info = [SBSummaryTable._create_sbsummary_info(*row) for row in SBSummaryTable._read_table(ms)]
            return sbsummary_info[0]
        except Exception as ex:
            if 'ALMA' in obsnames:
                LOG.warning('Error reading science goals for %s: %s' % (ms.basename, ex))
            return SBSummaryInfo(repSource=None, repFrequency=None, repBandwidth=None, repWindow=None,
                                 minAngResolution=None, maxAngResolution=None, maxAllowedBeamAxialRatio=None,
                                 sensitivity=None, dynamicRange=None, spectralDynamicRangeBandWidth=None, sbName=None)

    @staticmethod
    def get_observing_modes(ms: domain.MeasurementSet) -> List[str]:
        msname = _get_ms_name(ms)
        sbsummary_table = os.path.join(msname, 'ASDM_SBSUMMARY')
        observing_modes = []
        try:
            with casa_tools.TableReader(sbsummary_table) as tb:
                observing_mode = tb.getcol('observingMode')
                for irow in range(tb.nrows()):
                    cell = observing_mode[:, irow]
                    for mode in cell:
                        if mode not in observing_modes:
                            observing_modes.append(mode)
        except:
            LOG.warning('Error reading observing modes for %s' % ms.basename)

        return observing_modes

    @staticmethod
    def _create_sbsummary_info(repSource, repFrequency, repBandwidth, repWindow, minAngResolution, maxAngResolution,
                               maxAllowedBeamAxialRatio, sensitivity, dynamicRange, spectralDynamicRangeBandWidth, sbName):
        return SBSummaryInfo(repSource=repSource, repFrequency=repFrequency, repBandwidth=repBandwidth,
                             repWindow=repWindow, minAngResolution=minAngResolution, maxAngResolution=maxAngResolution,
                             maxAllowedBeamAxialRatio=maxAllowedBeamAxialRatio, sensitivity=sensitivity,
                             dynamicRange=dynamicRange, spectralDynamicRangeBandWidth=spectralDynamicRangeBandWidth, sbName=sbName)

    @staticmethod
    def _read_table(ms):
        """
        Read the ASDM_SBSummary table
        For all practical purposes this table consists of a single row
        but handle the more general case
        """
        LOG.debug('Analysing ASDM_SBSummary table')
        qa = casa_tools.quanta
        msname = _get_ms_name(ms)
        sbsummary_table = os.path.join(msname, 'ASDM_SBSUMMARY')        
        with casa_tools.TableReader(sbsummary_table) as table:
            scienceGoals = table.getcol('scienceGoal')
            numScienceGoals = table.getcol('numScienceGoal')

            # shouldn't happen in a well-formed XML
            if len(scienceGoals) != numScienceGoals:
                LOG.warning(f'{_get_ms_basename(ms)}: number of science goals found in the SB summary'
                            f' ({len(scienceGoals)}) are fewer than the number that were declared ({numScienceGoals}).')

            repSources = []
            repFrequencies = []
            repBandWidths = []
            repWindows = []
            minAngResolutions = []
            maxAngResolutions = []
            maxAllowedBeamAxialRatios = []
            sensitivities = []
            dynamicRanges = []
            spectralDynamicRangeBandWidths = []
            sbNames = []

            for i in range(table.nrows()):

                # Create source
                repSource = _get_science_goal_value(scienceGoals[0:numScienceGoals[i], i], 'representativeSource')
                repSources.append(repSource)

                # Create frequency
                repFrequencyGoal = _get_science_goal_value(scienceGoals[0:numScienceGoals[i], i], 'representativeFrequency')
                if repFrequencyGoal is not None:
                    repFrequency = qa.quantity(repFrequencyGoal)
                else:
                    repFrequency = qa.quantity(0.0)
                if repFrequency['value'] <= 0.0 or repFrequency['unit'] == '':
                    repFrequency = None
                repFrequencies.append(repFrequency)

                # Create representative bandwidth
                repBandWidthGoal = _get_science_goal_value(scienceGoals[0:numScienceGoals[i], i], 'representativeBandwidth')
                if repBandWidthGoal is not None:
                    repBandWidth = qa.quantity(repBandWidthGoal)
                else:
                    repBandWidth = qa.quantity(0.0)
                if repBandWidth['value'] <= 0.0 or repBandWidth['unit'] == '':
                    repBandWidth = None
                repBandWidths.append(repBandWidth)

                # Create window
                repWindow = _get_science_goal_value(scienceGoals[0:numScienceGoals[i], i], 'representativeWindow')
                if repWindow in ('none', ''):
                    repWindow = None
                repWindows.append(repWindow)

                # Create minimum and maximum angular resolution
                minAngResolutionGoal = _get_science_goal_value(scienceGoals[0:numScienceGoals[i], i], 'minAcceptableAngResolution')
                maxAngResolutionGoal = _get_science_goal_value(scienceGoals[0:numScienceGoals[i], i], 'maxAcceptableAngResolution')
                if minAngResolutionGoal is not None:
                    minAngResolution = qa.quantity(minAngResolutionGoal)
                else:
                    minAngResolution = qa.quantity(0.0)
                if maxAngResolutionGoal is not None:
                    maxAngResolution = qa.quantity(maxAngResolutionGoal)
                else:
                    maxAngResolution = qa.quantity(0.0)

                # There are cases with minAngResolutionGoal being set to 0 arcsec
                # while maxAngResolutionGoal has a non-zero value (PIPE-593).
                if (minAngResolution['value'] <= 0.0 and maxAngResolution['value'] <= 0.0) or minAngResolution['unit'] == '':
                    minAngResolution = None
                if maxAngResolution['value'] <= 0.0 or maxAngResolution['unit'] == '':
                    maxAngResolution = None

                minAngResolutions.append(minAngResolution)
                maxAngResolutions.append(maxAngResolution)

                # Create maximum allowed beam axial ratio
                maxAllowedBeamAxialRatioGoal = _get_science_goal_value(scienceGoals[0:numScienceGoals[i], i], 'maxAllowedBeamAxialRatio')
                if maxAllowedBeamAxialRatioGoal is not None:
                    maxAllowedBeamAxialRatio = qa.quantity(maxAllowedBeamAxialRatioGoal)
                else:
                    maxAllowedBeamAxialRatio = qa.quantity(0.0)
                if maxAllowedBeamAxialRatio['value'] <= 0.0 or maxAllowedBeamAxialRatio['value'] >= 999.:
                    maxAllowedBeamAxialRatio = None
                maxAllowedBeamAxialRatios.append(maxAllowedBeamAxialRatio)

                # Create sensitivity goal
                sensitivityGoal = _get_science_goal_value(scienceGoals[0:numScienceGoals[i], i], 'sensitivityGoal')
                if sensitivityGoal is not None:
                    sensitivity = qa.quantity(sensitivityGoal)
                else:
                    sensitivity = qa.quantity(0.0)
                if sensitivity['value'] <= 0.0 or sensitivity['unit'] == '':
                    sensitivity = None
                sensitivities.append(sensitivity)

                # Create dynamic range goal
                dynamicRangeGoal = _get_science_goal_value(scienceGoals[0:numScienceGoals[i], i], 'dynamicRange')
                if dynamicRangeGoal is not None:
                    dynamicRange = qa.quantity(dynamicRangeGoal)
                else:
                    dynamicRange = qa.quantity(0.0)
                dynamicRanges.append(dynamicRange)

                # Create spectral dynamic range bandwidth goal
                spectralDynamicRangeBandWidthGoal = _get_science_goal_value(scienceGoals[0:numScienceGoals[i], i], 'spectralDynamicRangeBandWidth')
                if spectralDynamicRangeBandWidthGoal not in (None, 'None', 'none'):
                    spectralDynamicRangeBandWidth = qa.quantity(spectralDynamicRangeBandWidthGoal)
                else:
                    spectralDynamicRangeBandWidth = qa.quantity(0.0)
                if spectralDynamicRangeBandWidth['value'] <= 0.0 or spectralDynamicRangeBandWidth['unit'] == '':
                    spectralDynamicRangeBandWidth = None
                spectralDynamicRangeBandWidths.append(spectralDynamicRangeBandWidth)

                sbName = _get_science_goal_value(scienceGoals[0:numScienceGoals[i], i], 'SBName')
                sbNames.append(sbName)

        rows = list(zip(repSources, repFrequencies, repBandWidths, repWindows, minAngResolutions, maxAngResolutions,
                        maxAllowedBeamAxialRatios, sensitivities, dynamicRanges, spectralDynamicRangeBandWidths, sbNames))
        return rows


class ExecblockTable(object):
    @staticmethod
    def get_execblock_info(ms):
        try:
            execblock_info = [ExecblockTable._create_execblock_info(*row) for row in ExecblockTable._read_table(ms)]
            if execblock_info[0][0] == 'ALMA':
                if execblock_info[0][1] == 'A':
                    return None
                else:
                    return execblock_info[0][1]             
            else:
                return execblock_info[0][1]             
        except:
            return None

    @staticmethod
    def _create_execblock_info(telescopeName, configName):
        return telescopeName, configName

    @staticmethod
    def _read_table(ms):
        """
        Read the ASDM_EXECBLOCK table
        For all practical purposes this table consists of a single row
        but handle the more general case
        """
        LOG.debug('Analysing ASDM_EXECBLOCK table')
        msname = _get_ms_name(ms)
        execblock_table = os.path.join(msname, 'ASDM_EXECBLOCK')        
        with casa_tools.TableReader(execblock_table) as table:
            telescope_names = table.getcol('telescopeName')
            config_names = table.getcol('configName')

        # In case multiple columns are extracted at some point
        # in which case rows would be constructed from the zipped
        # columns
        rows = list(zip(telescope_names, config_names))
        return rows


class PolarizationTable(object):
    @staticmethod
    def get_polarizations(msmd):
        pol_ids = sorted({int(i) for i in msmd.polidfordatadesc()})

        num_corrs = [msmd.ncorrforpol(i) for i in pol_ids]
        corr_types = [msmd.corrtypesforpol(i) for i in pol_ids]
        corr_products = [msmd.corrprodsforpol(i) for i in pol_ids]

        return [PolarizationTable._create_pol_description(*row)
                for row in zip(pol_ids, num_corrs, corr_types, corr_products)]

    @staticmethod
    def _create_pol_description(id, num_corr, corr_type, corr_product):
        return domain.Polarization(id, num_corr, corr_type, corr_product)


class SourceTable(object):
    @staticmethod
    def get_sources(msmd):
        rows = SourceTable._read_table(msmd)

        # duplicate source entries may be present due to duplicate entries
        # differing by non-essential columns, such as spw
        key_fn = operator.itemgetter(0)
        data = sorted(rows, key=key_fn)
        grouped_by_source_id = []
        for _, g in itertools.groupby(data, key_fn):
            grouped_by_source_id.append(list(g))

        no_dups = [s[0] for s in grouped_by_source_id]

        return [SourceTable._create_source(*row) for row in no_dups]

    @staticmethod
    def _create_source(source_id, name, direction, proper_motion, is_eph_obj, table_names, avg_spacings):
        return domain.Source(source_id, name, direction, proper_motion, is_eph_obj, table_names, avg_spacings)

    @staticmethod
    def _read_table(msmd):
        """
        Read the SOURCE table of the given measurement set.
        """
        LOG.debug('Analysing SOURCE table')
        ids = msmd.sourceidsfromsourcetable()
        sourcenames = msmd.sourcenames()
        directions = [v for _, v in sorted(msmd.sourcedirs().items(), key=lambda d: int(d[0]))]
        propermotions = [v for _, v in sorted(msmd.propermotions().items(), key=lambda pm: int(pm[0]))]
        eph_sourcenames, ephemeris_tables, avg_spacings = SourceTable._get_eph_sourcenames(msmd.name())
        is_eph_objs = [sourcename in eph_sourcenames for sourcename in sourcenames]

        table_list = []
        spacings_list = []
        for sourcename in sourcenames:
            if sourcename in eph_sourcenames:
                table_list.append(ephemeris_tables[sourcename])
                spacings_list.append(avg_spacings[sourcename])
            else: 
                table_list.append("")
                spacings_list.append("")

        all_sources = list(zip(ids, sourcenames, directions, propermotions, is_eph_objs, table_list, spacings_list))

        # Only return sources for which scans are present.
        # Create a mapping of source id to a boolean of whether any
        # scans are present for that source, using fields to link
        # between sources and scans.
        source_id_to_scans = {}
        for source_id in set(msmd.sourceidsfromsourcetable()):
            fields_for_source = set(msmd.fieldsforsource(source_id))
            # Fields do not necessarily have associated scans. If only the
            # first field is tested and gives a negative result, CAS-9499
            # results (AttributeError: 'Field' object has no attribute
            # 'source'). Prevent this by testing all fields for the
            # presence of scans.
            source_id_to_scans[source_id] = any([len(msmd.scansforfield(field_id)) != 0
                                                 for field_id in fields_for_source])

        return [row for row in all_sources if source_id_to_scans.get(row[0], False)]

    @staticmethod
    def _get_eph_sourcenames(msname):
        ephemeris_tables = utils.glob_ordered(msname+'/FIELD/EPHEM*.tab')

        eph_sourcenames = []
        avg_spacings = {}
        ephemeris_table_names = {}
        for ephemeris_table in ephemeris_tables:
            with casa_tools.TableReader(ephemeris_table) as tb:
                keywords = tb.getkeywords()
                eph_sourcename = keywords['NAME']
                eph_sourcenames.append(eph_sourcename)
                # Add the average spacing in minutes of the MJD column of the ephemeris table (see PIPE-627).
                if 'MJD' in tb.colnames():
                    mjd = tb.getcol('MJD')
                    avg_spacings[eph_sourcename] = numpy.diff(mjd).mean()*1440 # Convert fractional day to minutes
                # Return file names (not whole paths) for the ephemeris tables (see PIPE-627)
                ephemeris_table_names[eph_sourcename] = os.path.splitext(os.path.basename(ephemeris_table))[0]

        return eph_sourcenames, ephemeris_table_names, avg_spacings


class StateTable(object):
    @staticmethod
    def get_states(msmd):
        state_factory = StateTable.get_state_factory(msmd)

        LOG.trace('Opening STATE table to read STATE.OBS_MODE')
        state_table = os.path.join(msmd.name(), 'STATE')
        with casa_tools.TableReader(state_table) as table:
            obs_modes = table.getcol('OBS_MODE')

        states = []
        for i in range(msmd.nstates()):
            obs_mode = obs_modes[i]
            state = state_factory.create_state(i, obs_mode)
            states.append(state)
        return states

    @staticmethod
    def get_state_factory(msmd):
        names = set(msmd.observatorynames())
        assert len(names) == 1
        facility = names.pop()

        first_scan = min(msmd.scannumbers())
        scan_start = min(msmd.timesforscan(first_scan))

        LOG.trace('Opening MS to read TIME keyword to avoid '
                  'msmd.timesforscan() units ambiguity')
        with casa_tools.TableReader(msmd.name()) as table:
            time_colkeywords = table.getcolkeywords('TIME')
            time_unit = time_colkeywords['QuantumUnits'][0]
            time_ref = time_colkeywords['MEASINFO']['Ref']    

        me = casa_tools.measures
        qa = casa_tools.quanta

        epoch_start = me.epoch(time_ref, qa.quantity(scan_start, time_unit))
        str_start = qa.time(epoch_start['m0'], form=['fits'])[0]
        dt_start = datetime.datetime.strptime(str_start, '%Y-%m-%dT%H:%M:%S')

        return domain.state.StateFactory(facility, dt_start)        


class FieldTable(object):
    @staticmethod
    def _read_table(msmd):
        num_fields = msmd.nfields()
        field_ids = list(range(num_fields))
        field_names = msmd.namesforfields()
        times = [msmd.timesforfield(i) for i in field_ids]
        phase_centres = [msmd.phasecenter(i) for i in field_ids]
        source_ids = [msmd.sourceidforfield(i) for i in field_ids]

        LOG.trace('Opening FIELD table to read FIELD.SOURCE_TYPE')
        field_table = os.path.join(msmd.name(), 'FIELD')
        with casa_tools.TableReader(field_table) as table:
            # TODO can this old code be removed? We've not handled non-APDMs
            # for a *long* time!
            #
            # FIELD.SOURCE_TYPE contains the intents in non-APDM MS
            if 'SOURCE_TYPE' in table.colnames():
                source_types = table.getcol('SOURCE_TYPE')
            else:
                source_types = [None] * num_fields

        all_fields = list(zip(field_ids, field_names, source_ids, times, source_types, phase_centres))

        # only return sources for which scans are present
        # create a mapping of source id to a boolean of whether any scans are present for that source
        field_id_to_scans = {field_id: (len(msmd.scansforfield(field_id)) != 0) for field_id in set(field_ids)}

        return [row for row in all_fields if field_id_to_scans.get(row[0], False)]

    @staticmethod
    def get_fields(msmd):
        return [FieldTable._create_field(*row) for row in FieldTable._read_table(msmd)]

    @staticmethod
    def _create_field(field_id, name, source_id, time, source_type, phase_centre):
        field = domain.Field(field_id, name, source_id, time, phase_centre)

        if source_type:
            field.set_source_type(source_type)

        return field


def _make_range(f_min, f_max):
    return measures.FrequencyRange(measures.Frequency(f_min),
                                   measures.Frequency(f_max))


class BandDescriber(object):
    alma_bands = {'ALMA Band 1': _make_range(35, 50),
                  'ALMA Band 2': _make_range(67, 90),
                  'ALMA Band 3': _make_range(84, 116),
                  'ALMA Band 4': _make_range(125, 163),
                  'ALMA Band 5': _make_range(163, 211),
                  'ALMA Band 6': _make_range(211, 275),
                  'ALMA Band 7': _make_range(275, 373),
                  'ALMA Band 8': _make_range(385, 500),
                  'ALMA Band 9': _make_range(602, 720),
                  'ALMA Band 10': _make_range(787, 950)}

    # From original EVLA pipeline script
    # FLOW = [ 0.0e6, 150.0e6, 700.0e6, 2.0e9, 4.0e9, 8.0e9, 12.0e9, 18.0e9, 26.5e9, 40.0e9 ]
    # FHIGH = [ 150.0e6, 700.0e6, 2.0e9, 4.0e9, 8.0e9, 12.0e9, 18.0e9, 26.5e9, 40.0e9, 56.0e9 ]
    # BBAND = [ '4', 'P', 'L', 'S', 'C', 'X', 'U', 'K', 'A', 'Q' ]

    evla_bands = {'20cm (L)': _make_range(0.7, 2.0),
                  '13cm (S)': _make_range(2.0, 4.0),
                  '6cm (C)': _make_range(4, 8),
                  '3cm (X)': _make_range(8, 12),
                  '2cm (Ku)': _make_range(12, 18),
                  '1.3cm (K)': _make_range(18, 26.5),
                  '1cm (Ka)': _make_range(26.5, 40),
                  '0.7cm (Q)': _make_range(40, 56.0)}

    unknown = {'Unknown': measures.FrequencyRange()}

    @staticmethod
    def get_description(f, observatory='ALMA'):
        if observatory.upper() in ('ALMA',):
            bands = BandDescriber.alma_bands
        elif observatory.upper() in ('VLA', 'EVLA'):
            bands = BandDescriber.evla_bands
        else:
            bands = BandDescriber.unknown

        for description, rng in bands.items():
            if rng.contains(f):
                return description

        return 'Unknown'


class RetrieveByIndexContainer:
    """
    RetrieveByIndexContainer is a container for items whose numeric index or
    other unique identifier is stored in an instance attribute.

    Retrieving by index from this container matches and returns the item with
    matching index attribute, which may differ from the natural position of
    the item in the underlying list backing store. For instance, getting item
    3 with container[3] returns the item with index attribute == 3, not the
    item at position 3.
    """

    def __init__(self, items, index_fn=operator.attrgetter('id')):
        """
        Create a new RetrieveByIndexContainer.

        The list of items passed as the 'items' argument is set as an instance
        attribute (i.e., a copy or deep copy is not made). No changes should be
        made to the list after passing it to this constructor.

        :param items: the list of indexable items to wrap
        :param index_fn: function that returns the index of an item instance
        """
        self.__items = items
        self.__index_fn = index_fn

    def __iter__(self):
        return iter(self.__items)

    def __len__(self):
        return len(self.__items)

    def __getitem__(self, index):
        try:
            index = int(index)
        except ValueError:
            raise TypeError(
                'list indices must be integers, not {}'.format(index.__class__.__name__))

        with_id = [i for i in self.__items if self.__index_fn(i) == index]
        if not with_id:
            raise IndexError('list index out of range: {}'.format(index))
        if len(with_id) > 1:
            raise IndexError('more than one object found with ID {}'.format(index))
        return with_id.pop()

    def __str__(self):
        return '<RetrieveByIndexContainer({})>'.format(str(self.__items))
