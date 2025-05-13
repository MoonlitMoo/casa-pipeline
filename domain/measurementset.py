"""Provide a class to store logical representation of MeasurementSet."""
import collections
import contextlib
import inspect
import itertools
import operator
import os
import re
from typing import TYPE_CHECKING, List, Optional, Tuple, Union

import numpy as np

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.utils as utils
from pipeline.infrastructure import casa_tools

if TYPE_CHECKING:  # Avoid circular import. Used only for type annotation.
    from pipeline.infrastructure.tablereader import RetrieveByIndexContainer

from pipeline.infrastructure import logging

from . import measures, spectralwindow
from .antennaarray import AntennaArray
from .datatype import DataType

LOG = infrastructure.get_logger(__name__)


class MeasurementSet(object):
    """
    A class to store logical representation of a MeasurementSet (MS).

    Attributes:
        name: A path to MeasurementSet
        session: Session name of MS
        antenna_array: Antenna array information
        array_name: Name of array configuration
        derived_fluxes: Flux measurements
        flagcmds: A list of flag commands
        filesize: Disk size of MS
        representative_target: A tuple of the name of representative source,
            frequency and bandwidth.
        representative_window: A representative spectral window name
        science_goals: A science goal information consists of min/max
            acceptable angular resolution, max allowed beam ratio, sensitivity,
            dynamic range, spectral dynamic range bandwidth (Cycle 10+), and SB name.
        data_descriptions: A list of DataDescription objects associated with MS
        spectral_windows: A list of SpectralWindow objects associated with MS
        phasecal_mapping: A dictionary mapping phase calibrator fields to
            corresponding fields with TARGET or CHECK intent.
        spectralspec_spwmap: A dictionary mapping SpectralSpec to corresponding
            spectral window IDs.
        fields: A list of Field objects associated with MS
        scans: A list of Scan objects associated with MS
        sources: A list of Source objects associated with MS
        states: A list of State objects associated with MS
        spwmaps: Spectral window mapping to use for combining/mapping, split by
            (intent, field), used in ALMA interferometry calibration tasks.
        reference_spwmap: Reference spectral window map
        data_column: A dictionary to store data type (key) and corresponding
            data column (value)
        data_types_per_source_and_spw: A dictionary to store a list of
            available data types (values) in this MS per (source,spw) tuples
            (keys)
        observing_modes: The name(s) of the Observing Mode(s) used for this MS
            (empty list if not ALMA).
        reference_antenna_locked: If True, reference antenna is locked to
            prevent modification
        origin_ms: A path to the first generation MeasurementSet from which
            the current MS is generated.
        acs_software_version: ALMA Common Software version used to create this MS. (None if not ALMA.)
        acs_software_build_version: ALMA Common Software build version used to create this MS. (None if not ALMA.)
        phase_calapps_for_check_sources : The phase calapps for the check sources 
            from hifa_gfluxscale
        phaseup_caltable_for_phase_rms : The bandpass phaseup caltable name from hifa_bandpass
        fluxscale_fluxes: flux measurements derived by CASA's fluxscale; used
            in ALMA interferometry polarisation calibration.
        correlator_name: the name of the correlator, for example: ALMA_ACA, ALMA_BASELINE
    """

    def __init__(self, name: str, session: Optional[str] = None):
        """
        Initialize a MeasurementSet object.

        Args:
            name: A path to MS
            session: Session name of MS
        """
        self.name: str = name
        self.session: Optional[str] = session
        self.antenna_array: Optional[AntennaArray] = None
        self.array_name: str = ''
        self.derived_fluxes: Optional[collections.defaultdict] = None
        self.flagcmds: List[str] = []
        self.filesize: measures.FileSize = self._calc_filesize()
        self.representative_target: Tuple[Optional[str], Optional[dict],
                                          Optional[dict]] = (None, None, None)
        self.representative_window: Optional[str] = None
        self.science_goals: dict = {}
        self.data_descriptions: Union[RetrieveByIndexContainer, list] = []
        self.spectral_windows: Union[RetrieveByIndexContainer, list] = []
        self.fields: Union[RetrieveByIndexContainer, list] = []
        self.scans: list = []
        self.sources: Union[RetrieveByIndexContainer, list] = []
        self.states: Union[RetrieveByIndexContainer, list] = []
        self.reference_spwmap: Optional[List[int]] = None
        self.origin_ms: str = name
        self.data_column: dict = {}
        self.exclude_num_chans: Tuple[int, int] = (1, 4)

        # The ALMA Common Software version used to create this MS, if ALMA. Otherwise, None
        # (PIPE-132)
        self.acs_software_version = None

        # The ALMA Common Software build version used to create this MS, if ALMA. Otherwise, None.
        # (PIPE-132)
        self.acs_software_build_version = None

        self.data_types_per_source_and_spw: dict = {}

        # ALMA only: the name(s) of the Observing Mode(s) selected for this MS,
        # populated from the ASDM_SBSUMMARY table (PIPE-2084).
        self.observing_modes: List[str] = []

        # Dictionary mapping phase calibrator fields to corresponding fields
        # with TARGET / CHECK intents (PIPE-1154).
        self.phasecal_mapping: dict = {}

        # Dictionary to map each SpectralSpec to list of corresponding spectral
        # window IDs (PIPE-1132).
        self.spectralspec_spwmap: dict = {}

        # Dictionary with collections of spectral window maps for mapping or
        # combining spws, split by (intent, field). This is used in several
        # ALMA ('hifa') calibration tasks (PIPE-1154).
        self.spwmaps: dict = {}

        # Polarisation calibration requires the refant list be frozen, after
        # which subsequent gaincal calls are executed with
        # refantmode='strict'.
        #
        # To meet this requirement we make the MS refant list lockable. When
        # locked, the refant list cannot be changed. Additionally, gaincal
        # checks the lock status to know whether to set refantmode to strict.
        #
        # The backing property for the refant list.
        self._reference_antenna: Optional[str] = None
        # The refant lock. Setting reference_antenna_locked to True prevents
        # the reference antenna list from being modified. I would have liked
        # to put the lock on a custom refant list class, but some tasks check
        # the type of reference_antenna directly which prevents that approach.
        self.reference_antenna_locked: bool = False

        # This contains the phase calapps for the check sources from hifa_gfluxscale.
        # Added for ALMA IF to support PIPE-1377
        # These calapps are saved off from hifa_gfluxscale and saved here 
        # so they can be added to the Diagnostic Phase Vs Time plots for hifa_timegaincal
        self.phase_calapps_for_check_sources = []

        # Added for ALMA IF to support PIPE-1624
        # This phaseup caltable name is saved in hifa_bandpass
        # so it can be used for the phase RMS stability assessment
        self.phaseup_caltable_for_phase_rms = []
        
        # This contains flux measurements derived by CASA's fluxscale as
        # derived during hifa_gfluxscale. Added for ALMA IF as part of
        # PIPE-1776 to support polarisation calibration.
        self.fluxscale_fluxes: Optional[collections.defaultdict] = None

        # This contains the name of the correlator from the PROCESSOR table
        # Example values: ALMA_ACA, ALMA_BASELINE
        self.correlator_name: Optional[str] = None

    def _calc_filesize(self):
        """
        Calculate the disk usage of this measurement set.
        """
        total_bytes = 0
        for dirpath, _, filenames in os.walk(self.name):
            for f in filenames:
                fp = os.path.join(dirpath, f)
                total_bytes += os.path.getsize(fp)

        return measures.FileSize(total_bytes,
                                 measures.FileSizeUnits.BYTES)

    def __str__(self):
        return 'MeasurementSet({0})'.format(self.name)

    @property
    def intents(self):
        intents = set()
        # we look to field rather than state as VLA datasets don't have state
        # entries
        for field in self.fields:
            intents.update(field.intents)
        return intents

    @property
    def antennas(self):
        # return a copy rather than the underlying list
        return list(self.antenna_array.antennas)

    @property
    def basename(self):
        return os.path.basename(self.name)

    @property
    def is_band_to_band(self) -> bool:
        # PIPE-2084: declares whether this MS is for band-to-band
        # interferometry. This is deemed True if either the observing mode
        # declares it as band-to-band, or if the diffgain intent + SpW setup are
        # consistent with band-to-band (latter covers datasets that don't have
        # correct observing mode set in their metadata).
        return "BandToBand Interferometry" in self.observing_modes or self.get_diffgain_mode() == "B2B"

    def get_antenna(self, search_term=''):
        if search_term == '':
            return self.antennas

        return [a for a in self.antennas
                if a.id in utils.ant_arg_to_id(self.name, search_term, self.antennas)]

    def get_state(self, state_id=None):
        match = [state for state in self.states if state.id == state_id]
        if match:
            return match[0]
        else:
            return None

    def get_scans(self, scan_id=None, scan_intent=None, field=None, spw=None):
        pool = self.scans

        if scan_id is not None:
            # encase raw numbers in a tuple
            if not isinstance(scan_id, collections.Sequence):
                scan_id = (scan_id,)
            pool = [s for s in pool if s.id in scan_id]

        if scan_intent is not None:
            if isinstance(scan_intent, str):
                if scan_intent in ('', '*'):
                    # empty string equals all intents for CASA
                    scan_intent = ','.join(self.intents)
                scan_intent = scan_intent.split(',')
            scan_intent = set(scan_intent)
            pool = [s for s in pool if not s.intents.isdisjoint(scan_intent)]

        if field is not None:
            fields_with_name = frozenset(self.get_fields(task_arg=field))
            pool = [s for s in pool if not fields_with_name.isdisjoint(s.fields)]

        if spw is not None:
            if not isinstance(spw, collections.Sequence):
                spw = (spw,)
            if isinstance(spw, str):
                if spw in ('', '*'):
                    spw = ','.join(str(spw.id) for spw in self.spectral_windows)
                if '~' in spw:
                    spw = utils.range_to_list(spw)
                else:
                    spw = spw.split(',')
            spw = {int(i) for i in spw}
            pool = {scan for scan in pool for scan_spw in scan.spws if scan_spw.id in spw}
            pool = sorted(pool, key=lambda s: s.id)

        return pool

    def get_data_description(self, spw=None, id=None):
        match = None
        if spw is not None:
            if isinstance(spw, spectralwindow.SpectralWindow):
                match = [dd for dd in self.data_descriptions
                         if dd.spw is spw]
            elif isinstance(spw, int):
                match = [dd for dd in self.data_descriptions
                         if dd.spw.id == spw]
        if id is not None:
            match = [dd for dd in self.data_descriptions if dd.id == id]

        if match:
            return match[0]
        else:
            return None

    def get_representative_source_spw(self, source_name: Optional[str] = None, source_spwid: Optional[int] = None) -> \
            Tuple[Optional[str], Optional[int]]:
        """
        Get the representative target source object:
            Use user name if source_name is supplied by user and it has TARGET intent.
            Otherwise use the source defined in the ASDM SBSummary table if it has TARGET intent.
            Otherwise use the first source in the source list with TARGET intent.
        Arguments:
            source_name:  a string with the source name, or None for an automatic selection.
            source_spwid:  an int with the spw id, or None for an automatic selection.
        Returns:
            a tuple with representative source name and spw id;
            if such a source cannot be identified, return (None, None),
            and if an spw cannot be determined, return (name, None).
        """
        qa = casa_tools.quanta
        cme = casa_tools.measures

        # PIPE-1504: only issue certain messages at the WARNING level if they are executed by hifa_imageprecheck
        if 'hifa_imageprecheck' in [fn_name for (_, _, _, fn_name, _, _) in inspect.stack()]:
            log_level = logging.WARNING
        else:
            log_level = logging.INFO

        if source_name:
            # Use the first target source that matches the user defined name
            target_sources = [source for source in self.sources
                              if source.name == source_name
                              and 'TARGET' in source.intents]
            if len(target_sources) > 0:
                LOG.info('Selecting user defined representative target source %s for data set %s' %
                         (source_name, self.basename))
                target_source = target_sources[0]
            else:
                LOG.warning('User defined representative target source %s not found in data set %s' %
                            (source_name, self.basename))
                target_source = None
        elif self.representative_target[0]:
            # Use the first target source that matches the representative source name in the ASDM
            # SBSummary table
            source_name = self.representative_target[0]
            target_sources = [source for source in self.sources
                              if source.name == source_name
                              and 'TARGET' in source.intents]
            if len(target_sources) > 0:
                LOG.info('Selecting representative target source %s for data set %s' %
                         (self.representative_target[0], self.basename))
                target_source = target_sources[0]
            else:
                LOG.warning('Representative target source %s not found in data set %s' %
                            (self.representative_target[0], self.basename))
                # Try to fall back first target source
                target_sources = [source for source in self.sources
                                  if 'TARGET' in source.intents]
                if len(target_sources) > 0:
                    target_source = target_sources[0]
                    LOG.info('Falling back to first target source (%s) for data set %s' %
                             (target_source.name, self.basename))
                else:
                    LOG.warning('No target sources observed for data set %s' % self.basename)
                    target_source = None
        else:
            # Use first target source no matter what it is
            target_sources = [source for source in self.sources
                              if 'TARGET' in source.intents]
            if len(target_sources) > 0:
                target_source = target_sources[0]
                LOG.info('Undefined representative target source, defaulting to first target source (%s) for data set %s' %
                         (target_source.name, self.basename))
            else:
                LOG.warning('No target sources observed for data set %s' % self.basename)
                target_source = None

        # Target source not found
        if target_source is None:
            return (None, None)

        # Target source name
        target_source_name = target_source.name

        # Check the user defined spw and return it if it was observed for the
        # representative source. If it was not observed quit.
        if source_spwid:
            found = False
            for f in target_source.fields:
                valid_spwids = [spw.id for spw in list(f.valid_spws)]
                if source_spwid in valid_spwids:
                    found = True
                    break
            if found:
                LOG.info('Selecting user defined representative spw %s for data set %s' %
                         (str(source_spwid), self.basename))
                return (target_source_name, source_spwid)
            else:
                LOG.warning('No target source data for representative spw %s in data set %s' %
                            (str(source_spwid), self.basename))
                return (target_source_name, None)

        target_spwid = None
        # Check for representative spw from ASDM (>= Cycle 7)
        if self.representative_window is not None:
            try:
                target_spwid = [s.id for s in self.get_spectral_windows() if s.name == self.representative_window][0]
            except:
                LOG.log(log_level, 'Could not translate spw name %s to ID. Trying frequency matching heuristics.' %
                        self.representative_window)

        if target_spwid is not None:
            return (target_source_name, target_spwid)

        # Get the representative bandwidth
        #     Return if there isn't one
        if not self.representative_target[2]:
            if self.antenna_array.name not in ('VLA', 'EVLA'):
                LOG.warning('Undefined representative bandwidth for data set %s' % self.basename)
            return (target_source_name, None)
        
        target_bw = cme.frequency('TOPO',
            qa.quantity(qa.getvalue(self.representative_target[2]),
            qa.getunit(self.representative_target[2])))

        # Get the representative frequency
        #     Return if there isn't one
        if not self.representative_target[1]:
            LOG.warning('Undefined representative frequency for data set %s' % self.basename)
            return (target_source_name, None)
        target_frequency = cme.frequency('BARY',
            qa.quantity(qa.getvalue(self.representative_target[1]),
            qa.getunit(self.representative_target[1])))

        # Convert BARY frequency to TOPO
        #    Use the start time of the first target source scan
        #    Note some funny business with source and field names
        cme.doframe(cme.observatory(self.antenna_array.name))
        cme.doframe(target_source.direction)
        target_scans = [
            scan for scan in self.get_scans(scan_intent='TARGET')
            if target_source.id in [f.source_id for f in scan.fields]]
        if len(target_scans) > 0:
            cme.doframe(target_scans[0].start_time)
            target_frequency_topo = cme.measure(target_frequency, 'TOPO')
        else:
            LOG.error('Unable to convert representative frequency to TOPO for data set %s' % self.basename)
            target_frequency_topo = None
        cme.done()

        # No representative frequency
        if not target_frequency_topo:
            return (target_source_name, None)

        # Find the science spw 

        # Get the science spw ids
        science_spw_ids = [spw.id for spw in self.get_spectral_windows()]

        # Get the target science spws observed for the target source
        target_spws = {spw for f in target_source.fields for spw in f.valid_spws
                       if spw.id in science_spw_ids}

        # Now find all the target_spws that have a channel width less than
        # or equal to the representative bandwidth
        # Note that the representative bandwidth can be slightly smaller than the actual channel width.
        # This is due to the details of online Hanning smoothing and is technically correct. We thus
        # apply a 1% margin (see CAS-11710).
        target_spws_bw = [spw for spw in target_spws
                          if spw.channels[0].getWidth().to_units(measures.FrequencyUnits.HERTZ) <= 1.01 * target_bw['m0']['value']]

        if len(target_spws_bw) <= 0:
            LOG.warning('No target spws have channel width <= representative bandwidth in data set %s' % self.basename)

            # Now find all the target spws that contain the representative frequency.
            target_spws_freq = [spw for spw in target_spws
                                if spw.min_frequency.value <= target_frequency_topo['m0']['value'] <= spw.max_frequency.value]

            if len(target_spws_freq) <= 0:
                LOG.warning('No target spws overlap the representative frequency in data set %s' % self.basename)

                # Now find the closest match to the center frequency
                max_freqdiff = np.finfo('d').max
                for spw in target_spws:
                    freqdiff = abs(float(spw.centre_frequency.value) - target_frequency_topo['m0']['value'])
                    if freqdiff < max_freqdiff:
                        target_spwid = spw.id
                        max_freqdiff = freqdiff
                LOG.info('Selecting spw %s which is closest to the representative frequency in data set %s' %
                         (str(target_spwid), self.basename))
            else:
                min_chanwidth = None
                for spw in target_spws_freq:
                    chanwidth = spw.channels[0].getWidth().to_units(measures.FrequencyUnits.HERTZ)
                    if not min_chanwidth or chanwidth < min_chanwidth:
                        target_spwid = spw.id
                LOG.info('Selecting the narrowest chanwidth spw id %s which overlaps the representative frequency in data set %s' %
                         (str(target_spwid), self.basename))

            return (target_source_name, target_spwid)

        # Now find all the spw that contain the representative frequency
        target_spws_freq = [spw for spw in target_spws_bw
                            if spw.min_frequency.value <= target_frequency_topo['m0']['value'] <= spw.max_frequency.value]
        if len(target_spws_freq) <= 0:
            LOG.log(log_level,
                    'No target spws with channel spacing <= representative bandwith overlap the representative frequency in data set %s' %
                    self.basename)
            max_chanwidth = None
            for spw in target_spws_bw:
                chanwidth = spw.channels[0].getWidth().to_units(measures.FrequencyUnits.HERTZ)
                if (max_chanwidth is None) or (chanwidth > max_chanwidth):
                    target_spwid = spw.id
            LOG.info('Selecting widest channel width spw id {} with channel width <= representative bandwidth in data'
                     ' set {}'.format(str(target_spwid), self.basename))

            return (target_source_name, target_spwid)

        # For all the spws with channel width less than or equal
        # to the representative bandwidth which contain the
        # representative frequency select the one with the spw
        # with the greatest bandwidth.
        bestspw = None
        for spw in sorted(target_spws_freq, key=lambda x: x.id):
            if not bestspw:
                bestspw = spw
            elif spw.bandwidth.value > bestspw.bandwidth.value:
                bestspw = spw
        target_spwid = bestspw.id

        return target_source_name, target_spwid

    def get_fields(self, task_arg=None, field_id=None, name=None, intent=None):
        """
        Get Fields from this MeasurementSet matching the given criteria. If no
        criteria are given, all Fields in the MeasurementSet will be returned.

        Arguments can be given as either single items of the expected type,
        sequences of the expected type, or in the case of name or intent, as
        comma separated strings. For instance, name could be 'HOIX', 
        'HOIX,0841+708' or ('HOIX','0841+708').

        :param field_id: field ID(s) to match
        :param name: field name(s) to match
        :param intent: observing intent(s) to match
        :rtype: a (potentially empty) list of :class:`~pipeline.domain.field.Field` \
             objects
        """
        pool = self.fields

        if task_arg not in (None, ''):
            field_id_for_task_arg = utils.field_arg_to_id(self.name, task_arg, self.fields)
            pool = [f for f in pool if f.id in field_id_for_task_arg]

        if field_id is not None:
            # encase raw numbers in a tuple
            if not isinstance(field_id, collections.Sequence):
                field_id = (field_id,)
            pool = [f for f in pool if f.id in field_id]

        if name is not None:
            if isinstance(name, str):
                name = name.split(',')
            name = set(name)
            pool = [f for f in pool if f.name in name]

        if intent is not None:
            if isinstance(intent, str):
                if intent in ('', '*'):
                    # empty string equals all intents for CASA
                    intent = ','.join(self.intents)
                intent = intent.split(',')
            intent = set(intent)
            pool = [f for f in pool if not f.intents.isdisjoint(intent)]

        return pool

    def get_spectral_window(self, spw_id):
        if spw_id is not None:
            spw_id = int(spw_id)
            match = [spw for spw in self.spectral_windows
                     if spw.id == spw_id]
            if match:
                return match[0]
            else:
                raise KeyError('No spectral window with ID \'{0}\' found in '
                               '{1}'.format(spw_id, self.basename))

    def get_spectral_windows(self, task_arg='', with_channels=False, num_channels=(), science_windows_only=True,
                             spectralspecs=None, intent=None):
        """
        Return the spectral windows corresponding to the given CASA-style spw
        argument, filtering out windows that may not be science spectral 
        windows (WVR windows, channel average windows etc.).
        """
        spws = self.get_all_spectral_windows(task_arg, with_channels)

        # if requested, filter spws by number of channels
        if num_channels:
            spws = [w for w in spws if w.num_channels in num_channels]

        # If requested, filter spws by spectral specs.
        if spectralspecs is not None:
            spws = [w for w in spws if w.spectralspec in spectralspecs]

        # If requested, filter spws by intent.
        if intent is not None:
            spws = [w for w in spws if not set(intent.split(',')).isdisjoint(w.intents)]

        if not science_windows_only:
            return spws

        if self.antenna_array.name == 'ALMA':
            science_intents = {'TARGET', 'PHASE', 'BANDPASS', 'AMPLITUDE',
                               'POLARIZATION', 'POLANGLE', 'POLLEAKAGE',
                               'CHECK', 'DIFFGAINREF', 'DIFFGAINSRC'}
            return [w for w in spws if w.num_channels not in self.exclude_num_chans
                    and not science_intents.isdisjoint(w.intents)]

        if self.antenna_array.name in ('VLA', 'EVLA'):
            science_intents = {'TARGET', 'PHASE', 'BANDPASS', 'AMPLITUDE',
                               'POLARIZATION', 'POLANGLE', 'POLLEAKAGE',
                               'CHECK'}
            return [w for w in spws if w.num_channels not in self.exclude_num_chans
                    and not science_intents.isdisjoint(w.intents) and 'POINTING' not in w.intents]

        if self.antenna_array.name == 'NRO':
            science_intents = {'TARGET'}
            return [w for w in spws if not science_intents.isdisjoint(w.intents)]

        return spws

    def get_spectral_specs(self) -> List[str]:
        """Return list of all spectral specs used in the MS."""
        return list(self.spectralspec_spwmap.keys())

    def get_all_spectral_windows(self, task_arg: str = '', with_channels: bool = False) \
            -> List[Union[spectralwindow.SpectralWindow, spectralwindow.SpectralWindowWithChannelSelection]]:
        """Return the spectral windows corresponding to the given CASA-style
        spw argument.
        """
        # we may have more spectral windows in our MeasurementSet than have
        # data in the measurement set on disk. Ask for all 
        if task_arg in (None, ''):
            task_arg = '*'

        # expand spw tuples into a range per spw, eg. spw9 : 1,2,3,4,5
        selected = collections.defaultdict(set)
        for (spw, start, end, step) in utils.spw_arg_to_id(self.name, task_arg, self.spectral_windows):
            selected[spw].update(set(range(start, end+1, step)))

        if not with_channels:
            return [spw for spw in self.spectral_windows if spw.id in selected]

        spws = []
        for spw_id, channels in selected.items():
            spw_obj = self.get_spectral_window(spw_id)
            proxy = spectralwindow.SpectralWindowWithChannelSelection(spw_obj, channels)
            spws.append(proxy)
        return spws

    def get_diffgain_mode(self) -> Optional[str]:
        """
        Determine if the intents and SpW setup in this measurement set are
        consistent with an observing mode that uses a differential gain
        calibrator: BandToBand (B2B) or BandwidthSwitching (BWSW).

        Returns:
            'B2B' if the ratio of frequencies between on-source and reference spws is above 1.661;
            'BWSW' if the ratio of bandwidths between reference and on-source is above 1.5;
            None if there are no DIFFGAIN* intents in this MS.

        Raises:
            ValueError if the DIFFGAIN* intents are present in the MS, but either
            a. the MS is missing diffgain reference or on-source SpWs
            b. the SpW setup does not match either BandToBand or BandwidthSwitching.
        """
        if 'DIFFGAINREF' in self.intents or 'DIFFGAINSRC' in self.intents:
            # Retrieve diffgain reference and diffgain on-source SpWs.
            dg_refspws = self.get_spectral_windows(intent='DIFFGAINREF')
            if not dg_refspws:
                raise ValueError(f'DIFFGAINREF intent missing in calibration sources for dataset {self.basename}')
            dg_srcspws = self.get_spectral_windows(intent='DIFFGAINSRC')
            if not dg_srcspws:
                raise ValueError(f'DIFFGAINSRC intent missing in science sources for dataset {self.basename}')

            # Retrieve the center frequency and bandwidth from the first
            # diffgain on-source SpW and first diffgain reference SpW. The setup
            # is expected to be the same for all SpWs.
            refcent = dg_refspws[0].centre_frequency.value
            scicent = dg_srcspws[0].centre_frequency.value
            refwidth = dg_refspws[0].bandwidth.value
            sciwidth = dg_srcspws[0].bandwidth.value

            # [at least] one of the above conditions has to be satisfied
            if scicent/refcent > 1.661:  # simple ratio to be out of band Cycle 10
                return 'B2B'
            elif refwidth/sciwidth > 1.5:
                return 'BWSW'

            raise ValueError(f'DIFFGAIN intent with an unsupported spectral setup for dataset {self.basename}')

        return None

    def get_original_intent(self, intent=None):
        """
        Get the original obs_modes that correspond to the given pipeline
        observing intents.
        """
        obs_modes = [state.get_obs_mode_for_intent(intent)
                     for state in self.states]
        return set(itertools.chain(*obs_modes))

    def get_alma_cycle_number(self) -> Optional[int]:
        """
        Get the ALMA cycle number from the ALMA control software version that this MeasurementSet was acquired with.

        Returns:
            int cycle_number or None if not found
        """
        match = re.search(r"CYCLE(\d+)", self.acs_software_build_version)
        if match:
            cycle_number = int(match.group(1))
            return cycle_number
        else:
            return None

    @property
    def start_time(self):
        earliest, _ = min([(scan, utils.get_epoch_as_datetime(scan.start_time)) for scan in self.scans],
                          key=operator.itemgetter(1))
        return earliest.start_time

    @property
    def end_time(self):
        latest, _ = max([(scan, utils.get_epoch_as_datetime(scan.end_time)) for scan in self.scans],
                        key=operator.itemgetter(1))
        return latest.end_time

    def get_vla_max_integration_time(self):
        """Get the integration time used by the original VLA scripts

        Args:
            None

        Returns:
            int_time: int value of the max integration time used
        """

        # with casa_tools.TableReader(vis + '/FIELD') as table:
        #     numFields = table.nrows()
        #     field_positions = table.getcol('PHASE_DIR')
        #     field_ids = range(numFields)
        #     field_names = table.getcol('NAME')

        # with casa_tools.TableReader(vis) as table:
        #     scanNums = sorted(np.unique(table.getcol('SCAN_NUMBER')))
        #     field_scans = []
        #     for ii in range(0,numFields):
        #         subtable = table.query('FIELD_ID==%s'%ii)
        #         field_scans.append(list(np.unique(subtable.getcol('SCAN_NUMBER'))))
        #         subtable.close()

        # field_scans is now a list of lists containing the scans for each field.
        # so, to access all the scans for the fields, you'd:
        #
        # for ii in range(0,len(field_scans)):
        #    for jj in range(0,len(field_scans[ii]))
        #
        # the jj'th scan of the ii'th field is in field_scans[ii][jj]

        # Figure out integration time used

        with casa_tools.MSReader(self.name) as ms:
            scan_summary = ms.getscansummary()
        # startdate=float(ms_summary['BeginTime'])

        integ_scan_list = []
        for scan in scan_summary:
            integ_scan_list.append(int(scan))
        sorted_scan_list = sorted(integ_scan_list)

        # find max and median integration times
        #
        integration_times = []
        for ii in sorted_scan_list:
            integration_times.append(scan_summary[str(ii)]['0']['IntegrationTime'])

        maximum_integration_time = max(integration_times)
        median_integration_time = np.median(integration_times)

        int_time = maximum_integration_time

        return int_time

    def get_vla_datadesc(self):
        """Generate VLA data description index
            Use the original VLA buildscans function to return a dd index

        Args:
            None

        Returns:
            ddindex: indexed list of dictionaries with VLA metadata

        """

        cordesclist = ['Undefined', 'I', 'Q', 'U', 'V',
                       'RR', 'RL', 'LR', 'LL',
                       'XX', 'XY', 'YX', 'YY',
                       'RX', 'RY', 'LX', 'LY',
                       'XR', 'XL', 'YR', 'YL',
                       'PP', 'PQ', 'QP', 'QQ',
                       'RCircular', 'LCircular',
                       'Linear', 'Ptotal',
                       'Plinear', 'PFtotal',
                       'PFlinear', 'Pangle']

        # From Steve Myers buildscans function
        with casa_tools.TableReader(self.name + '/DATA_DESCRIPTION') as table:
            ddspwarr = table.getcol("SPECTRAL_WINDOW_ID")
            ddpolarr = table.getcol("POLARIZATION_ID")

        ddspwlist = ddspwarr.tolist()
        ddpollist = ddpolarr.tolist()
        ndd = len(ddspwlist)

        with casa_tools.TableReader(self.name + '/SPECTRAL_WINDOW') as table:
            nchanarr = table.getcol("NUM_CHAN")
            spwnamearr = table.getcol("NAME")
            reffreqarr = table.getcol("REF_FREQUENCY")

        nspw = len(nchanarr)
        spwlookup = {}
        for isp in range(nspw):
            spwlookup[isp] = {}
            spwlookup[isp]['nchan'] = nchanarr[isp]
            spwlookup[isp]['name'] = str(spwnamearr[isp])
            spwlookup[isp]['reffreq'] = reffreqarr[isp]

        with casa_tools.TableReader(self.name + '/POLARIZATION') as table:
            ncorarr = table.getcol("NUM_CORR")
            npols = len(ncorarr)
            polindex = {}
            poldescr = {}
            for ip in range(npols):
                cort = table.getcol("CORR_TYPE", startrow=ip, nrow=1)
                (nct, nr) = cort.shape
                cortypes = []
                cordescs = []
                for ict in range(nct):
                    cct = cort[ict][0]
                    cde = cordesclist[cct]
                    cortypes.append(cct)
                    cordescs.append(cde)
                polindex[ip] = cortypes
                poldescr[ip] = cordescs

        ddindex = {}
        ncorlist = ncorarr.tolist()
        for idd in range(ndd):
            ddindex[idd] = {}
            isp = ddspwlist[idd]
            ddindex[idd]['spw'] = isp
            ddindex[idd]['spwname'] = spwlookup[isp]['name']
            ddindex[idd]['nchan'] = spwlookup[isp]['nchan']
            ddindex[idd]['reffreq'] = spwlookup[isp]['reffreq']
            #
            ipol = ddpollist[idd]
            ddindex[idd]['ipol'] = ipol
            ddindex[idd]['npol'] = ncorlist[ipol]
            ddindex[idd]['corrtype'] = polindex[ipol]
            ddindex[idd]['corrdesc'] = poldescr[ipol]

        return ddindex

    def get_vla_corrstring(self):
        """Get correlation string for VLA

        Args:
            None

        Returns:
            corrstring: string value of correlation

        """

        # Prep string listing of correlations from dictionary created by method buildscans
        # For now, only use the parallel hands.  Cross hands will be implemented later.

        ddindex = self.get_vla_datadesc()

        corrstring_list = ddindex[0]['corrdesc']
        removal_list = ['RL', 'LR', 'XY', 'YX']
        corrstring_list = sorted(set(corrstring_list).difference(set(removal_list)))
        corrstring = ','.join(corrstring_list)

        return corrstring

    def get_vla_corrlist_from_spw(self, spw=None):
        """Get all VLA correlation labels as a list of string from selected spw(s).

        Args:
            spw: a spw selection string or None. Defaults to None.

        Returns:
            list: a list of correlation labels.
        """
        ddindex = self.get_vla_datadesc()
        corrs = set()
        for dd in ddindex.values():
            if spw in ('', '*', None) or (isinstance(spw, str) and str(dd['spw']) in spw.split(',')):
                corrs = corrs.union(dd['corrdesc'])

        return sorted(corrs)

    def get_alma_corrstring(self):
        """Get correlation string for ALMA for the science windows

        Args:
            None

        Returns:
            corrstring: string value of correlation

        """

        sci_spwlist = self.get_spectral_windows(science_windows_only=True)
        sci_spwids = [spw.id for spw in sci_spwlist]

        datadescs = [dd for dd in self.data_descriptions if dd.spw.id in sci_spwids]

        numpols = len(datadescs[0].polarizations)

        if numpols == 1:
            corrstring = 'XX'
        else:
            corrstring = 'XX,YY'

        return corrstring

    def get_vla_spw2band(self):
        """Find field spws for VLA

        Args:
            None

        Returns:
            spw2band: dictionary with each string key spw index giving a single letter string value of the band

        """

        ddindex = self.get_vla_datadesc()

        spw2band = {}

        for spw in ddindex:

            strelems = list(ddindex[spw]['spwname'])
            bandname = strelems[5]
            if bandname in '4PLSCXUKAQ':
                spw2band[spw] = strelems[5]
            # Check for U / KU
            if strelems[5] == 'K' and strelems[6] == 'U':
                spw2band[spw] = 'U'
            if strelems[5] == 'K' and strelems[6] == 'A':
                spw2band[spw] = 'A'

        return spw2band

    def vla_minbaselineforcal(self):
        """Min baseline for cal

        Args:
            None

        Returns:
            Constant value

        """

        # Old determination before it was changed to a constant value of 4
        # return max(4, int(len(self.antennas) / 2.0))
        return 4

    def get_vla_field_spws(self, spwlist=[]):
        """Find field spws for VLA

        Args:
            spwlist (List, optional): list of string spws   ['1', '2', '3']

        Returns:
            field_spws: List of dictionaries

        """

        # Map field IDs to spws
        field_spws = []

        spwlistint = [int(spw) for spw in spwlist]

        with casa_tools.MSMDReader(self.name) as msmd:
            spwsforfieldsall = msmd.spwsforfields()

            if spwlist != []:
                spwsforfields = {}
                for field, spws in spwsforfieldsall.items():
                    spwsforfields[field] = [spw for spw in spws if spw in spwlistint]
            else:
                spwsforfields = spwsforfieldsall

            spwfieldkeys = sorted([int(i) for i in spwsforfields])
            spwfieldkeys = [str(i) for i in spwfieldkeys]

            for key in spwfieldkeys:
                field_spws.append(spwsforfields[key])

        return field_spws

    def get_vla_numchan(self):
        """Get number of channels for VLA

        Args:
            None

        Returns:
            channels:  NUM_CHAN column from spectral window table

        """

        vis = self.name

        with casa_tools.TableReader(vis+'/SPECTRAL_WINDOW') as table:
            channels = table.getcol('NUM_CHAN')

        return channels

    def get_vla_tst_bpass_spw(self, spwlist=[]):
        """Get VLA test bandpass or delay spws
            This function replaced functionality for get_vla_tst_delay_spw - PIPE-1325

        Args:
            spwlist (List, optional): list of string spws   ['1', '2', '3']


        Returns:
            tst_bpass_spws: CASA argument format of spws:channels    '0:10~80, 1:15~60, 2:30~70'
        """

        tst_delay_spw = ''

        channels = self.get_vla_numchan()

        ispwlist = [int(spw) for spw in spwlist]
        for ispw in ispwlist:
            endch1 = int(channels[ispw]/3.0)
            endch2 = int(2.0*channels[ispw]/3.0)+1
            if ispw < max(ispwlist):
                tst_delay_spw = tst_delay_spw+str(ispw)+':'+str(endch1)+'~'+str(endch2)+','
            else:
                tst_delay_spw = tst_delay_spw+str(ispw)+':'+str(endch1)+'~'+str(endch2)

        tst_bpass_spw = tst_delay_spw

        return tst_bpass_spw

    def get_vla_critfrac(self):
        """Identify bands/basebands/spws

        Args:
            None

        Returns:
            critical fraction

        """

        vis = self.name

        with casa_tools.TableReader(vis+'/SPECTRAL_WINDOW') as table:
            spw_names = table.getcol('NAME')

        # If the dataset is too old to have the bandname in it, assume that
        # either there are 8 spws per baseband (and allow for one or two for
        # pointing), or that this is a dataset with one spw per baseband

        if len(spw_names) >= 8:
            critfrac = 0.9/int(len(spw_names)/8.0)
        else:
            critfrac = 0.9/float(len(spw_names))

        if '#' in spw_names[0]:
            #
            # i assume that if any of the spw_names have '#', they all do...
            #
            bands_basebands_subbands = []
            for spw_name in spw_names:
                receiver_name, baseband, subband = spw_name.split('#')
                receiver_band = (receiver_name.split('_'))[1]
                bands_basebands_subbands.append([receiver_band, baseband, int(subband)])
            spws_info = [[bands_basebands_subbands[0][0], bands_basebands_subbands[0][1], [], []]]
            bands = [bands_basebands_subbands[0][0]]
            for ii in range(len(bands_basebands_subbands)):
                band, baseband, subband = bands_basebands_subbands[ii]
                found = -1
                for jj in range(len(spws_info)):
                    oband, obaseband, osubband, ospw_list = spws_info[jj]
                    if band == oband and baseband == obaseband:
                        osubband.append(subband)
                        ospw_list.append(ii)
                        found = jj
                        break
                if found >= 0:
                    spws_info[found] = [oband, obaseband, osubband, ospw_list]
                else:
                    spws_info.append([band, baseband, [subband], [ii]])
                    bands.append(band)
            # logprint("Bands/basebands/spws are:", logfileout='logs/msinfo.log')
            for spw_info in spws_info:
                spw_info_string = spw_info[0] + '   ' + spw_info[1] + '   [' + ','.join(["%d" % ii for ii in spw_info[2]]) + ']   [' + ','.join(["%d" % ii for ii in spw_info[3]]) + ']'
                # logprint(spw_info_string, logfileout='logs/msinfo.log')

            # Critical fraction of flagged solutions in delay cal to avoid an
            # entire baseband being flagged on all antennas
            critfrac = 0.9/float(len(spws_info))
        elif ':' in spw_names[0]:
            print("old spw names with :")
            # logprint("old spw names with :", logfileout='logs/msinfo.log')
        else:
            print("unknown spw names")

        return critfrac

    def get_vla_baseband_spws(self, science_windows_only=True,
                              return_select_list=True, warning=True):
        """Get the SPW information from individual VLA band/baseband.

        Args:
            science_windows_only (bool, optional): Defaults to True.
            return_select_list (bool, optional): return spw list of each baseband. Defaults to True.
            warning (bool, optional): Defaults to True.

        Returns:
            baseband_spws: spws info of individual basebands as baseband_spws[band][baseband]
            baseband_spws_list: spw_list of individual basebands
                e.g., [[0,1,2,3],[4,5,6,7]]
        """

        baseband_spws = collections.defaultdict(lambda: collections.defaultdict(list))

        for spw in self.get_spectral_windows(science_windows_only=science_windows_only):
            try:
                band = spw.name.split('#')[0].split('_')[1]
                baseband = spw.name.split('#')[1]
                min_freq = spw.min_frequency
                max_freq = spw.max_frequency
                mean_freq = spw.mean_frequency
                chan_width = spw.channels[0].getWidth()
                baseband_spws[band][baseband].append({spw.id: (min_freq, max_freq, mean_freq, chan_width)})
            except Exception as ex:
                if warning:
                    LOG.warning("Exception: Baseband name cannot be parsed. {!s}".format(str(ex)))
                else:
                    pass

        if return_select_list:
            baseband_spws_list = []
            for band in baseband_spws.values():
                for baseband in band.values():
                    baseband_spws_list.append([[*spw_info][0] for spw_info in baseband])
            return baseband_spws, baseband_spws_list
        else:
            return baseband_spws

    def get_median_integration_time(self, intent=None):
        """Get the median integration time used to get data for the given
        intent.

        Args:
            intent (str, optional): The intent of the data of interest.

        Returns:
            The median integration time used.
        """
        LOG.debug('inefficiency - MSFlagger reading file to get integration '
                  'time')

        # get the field IDs and state IDs for fields in the measurement set,
        # filtering by intent if necessary
        if intent:
            field_ids = [field.id for field in self.fields
                         if intent in field.intents]
            state_ids = [state.id for state in self.states
                         if intent in state.intents]
#        if intent:
#            re_intent = intent.replace('*', '.*')
#            re_intent = re.compile(re_intent)
#            field_ids = [field.id for field in self.fields
#                         if re_intent.match(str(field.intents))]
#            state_ids = [state.id for state in self.states
#                         if re_intent.match(str(state.intents))]
        else:
            field_ids = [field.id for field in self.fields]
            state_ids = [state.id for state in self.states]

        # VLA datasets have an empty STATE table; in the main table such rows
        # have a state ID of -1.
        if not state_ids:
            state_ids = [-1]

        with casa_tools.TableReader(self.name) as table:
            taql = '(STATE_ID IN %s AND FIELD_ID IN %s)' % (state_ids, field_ids)
            with contextlib.closing(table.query(taql)) as subtable:
                integration = subtable.getcol('INTERVAL')
            return np.median(integration)

    def get_median_science_integration_time(self, intent=None, spw=None):
        """Get the median integration time for science targets used to get data for the given
        intent.

        Keyword arguments:
        intent  -- The intent of the data of interest.
        spw     -- spw string list - '1,7,11,18'

        Returns -- The median integration time used.
        """
        LOG.debug('inefficiency - MSFlagger reading file to get median integration '
                  'time for science targets')

        if spw is None:
            spws = self.spectral_windows
        else:

            try:
                # Put csv string of spws into a list
                spw_string_list = spw.split(',')

                # Get all spw objects
                all_spws = self.spectral_windows

                # Filter out the science spw objects
                spws = [ispw for ispw in all_spws if str(ispw.id) in spw_string_list]
            except:
                LOG.error("Incorrect spw string format.")

        # now get the science spws, those used for scientific intent
        science_spws = [
            ispw for ispw in spws
            if ispw.num_channels not in self.exclude_num_chans
            and not ispw.intents.isdisjoint(['BANDPASS', 'AMPLITUDE', 'PHASE',
                                             'TARGET'])]
        LOG.debug('science spws are: %s' % [ispw.id for ispw in science_spws])

        # and the science fields/states
        science_field_ids = [
            field.id for field in self.fields
            if not set(field.intents).isdisjoint(['BANDPASS', 'AMPLITUDE',
                                                  'PHASE', 'TARGET'])]
        science_state_ids = [
            state.id for state in self.states
            if not set(state.intents).isdisjoint(['BANDPASS', 'AMPLITUDE',
                                                  'PHASE', 'TARGET'])]

        science_spw_dd_ids = [self.get_data_description(spw).id for spw in science_spws]

        with casa_tools.TableReader(self.name) as table:
            taql = '(STATE_ID IN %s AND FIELD_ID IN %s AND DATA_DESC_ID in %s)' % (science_state_ids, science_field_ids, science_spw_dd_ids)
            with contextlib.closing(table.query(taql)) as subtable:
                integration = subtable.getcol('INTERVAL')
            return np.median(integration)

    def get_times_on_source_per_field_id(self, field: str, intent: str):
        """
        :param field: field name
        :param intent: field intent
        returns dictionary of on source times for selected field IDs
        """
        field_ids = [field.id for field in self.fields if intent in field.intents]
        state_ids = [state.id for state in self.states if intent in state.intents]
        scan_ids = [s.id for s in self.get_scans(field=field, scan_intent=intent)]
        # Need to select just one cross correlation row between antenna 1 and 2
        ant1 = self.antennas[0].id
        ant2 = self.antennas[1].id
        # Just a single spw since all spws are observed together
        first_science_spw_dd_id = [self.get_data_description(spw_id).id for spw_id in [s.id for s in self.get_spectral_windows()]][0]
        times_on_source_per_field_id = dict()
        for field_id in field_ids:
            with casa_tools.TableReader(self.name) as table:
                taql = '(STATE_ID IN %s AND FIELD_ID IN [%s] AND DATA_DESC_ID in [%s] AND SCAN_NUMBER in [%s] AND ANTENNA1=%s AND ANTENNA2=%s)' % (state_ids, field_id, first_science_spw_dd_id, scan_ids, ant1, ant2)
                with contextlib.closing(table.query(taql)) as subtable:
                    integration = subtable.getcol('INTERVAL')
                times_on_source_per_field_id[field_id] = np.sum(integration)
        return times_on_source_per_field_id

    @property
    def reference_antenna(self):
        """
        Get the reference antenna list for this MS. The refant value is
        a comma-separated string.

        Example: 'DV01,DV02,DV03'
        """
        return self._reference_antenna

    @reference_antenna.setter
    def reference_antenna(self, value):
        """
        Set the reference antenna list for this MS.

        If this property is in R/O mode, signified by reference_antenna_locked
        being set True, an AttributeError will be raised.
        """
        if self.reference_antenna_locked:
            # AttributeError is raised for R/O properties, which seems
            # appropriate for this scenario
            raise AttributeError(f'Refant list for {self.basename} is locked')
        self._reference_antenna = value

    def update_reference_antennas(self, ants_to_demote=None, ants_to_remove=None):
        """Update the reference antenna list by demoting and/or removing
        specified antennas.

        If the same antenna is specified to be demoted and to be removed, it
        is removed.

        :param ants_to_demote: list of antenna names to demote
        :param ants_to_remove: list of antenna names to remove
        """
        if ants_to_demote is None:
            ants_to_demote = []
        if ants_to_remove is None:
            ants_to_remove = []

        # Return early if no refants are registered (None, or empty string).
        if not (self.reference_antenna and self.reference_antenna.strip()):
            LOG.warning('No reference antennas registered set for MS {} ({}), '
                        'cannot update its reference antenna list.'.
                        format(self.basename, hex(id(self))))
            return

        previous_reference_antenna = self.reference_antenna

        # Create updated refant list.
        refants_to_keep = []
        refants_to_move = []
        for ant in self.reference_antenna.split(','):
            if ant not in ants_to_remove:
                if ant in ants_to_demote:
                    refants_to_move.append(ant)
                else:
                    refants_to_keep.append(ant)
        refants_to_keep.extend(refants_to_move)

        # Update refant list.
        self.reference_antenna = ','.join(refants_to_keep)

        LOG.info('ms.update_reference_antennas for MS {} ({}): previous list={}, removed={}, demoted={}, new list={}'.
                 format(self.basename, hex(id(self)), previous_reference_antenna,
                        ','.join(sorted(ants_to_remove)) if ants_to_remove else 'none',
                        ','.join(sorted(ants_to_demote)) if ants_to_remove else 'none',
                        self.reference_antenna))

    @property
    def session(self):
        return self._session

    @session.setter
    def session(self, value):
        if value is None:
            value = 'session_1'
        self._session = value

    def all_colnames(self):
        """
        Return all available column names for this MS.
        """
        with casa_tools.TableReader(self.name) as table:
            colnames = table.colnames()
        return colnames

    def data_colnames(self):
        """
        Return all data column names for this MS.
        """
        return [colname for colname in self.all_colnames() if colname in ('DATA', 'FLOAT_DATA', 'CORRECTED_DATA')]

    def set_data_column(self, dtype: DataType, column: str,
                        source: Optional[str] = None,
                        spw: Optional[str] = None,
                        overwrite: bool = False):
        """
        Set data type and column.

        Set data type and column to MS domain object and record the available
        data types per (source,spw) tuple. If source or spw are unset, they
        will be expanded to all available values.

        Args:
            dtype: data type to set
            column: name of column in MS associated with the data type
            source: source name selection string (comma separated names). If
                unset, all sources will be used.
            spw: real spectral window selection string (string of comma
                separated IDs). If unset, all real spw IDs will be used.
            overwrite: if True existing data colum is overwritten by the new
                column. If False and if type is already associated with other
                column, the function raises ValueError.

        Raises:
            ValueError: An error raised when the column does not exist
                or the type is already associated with a column or the
                column is already assigned to a type and would not
                be overwritten.
        """
        # Check existence of the column
        colnames = self.data_colnames()
        if column not in colnames:
            raise ValueError('Column {} does not exist in {}'.format(column, self.basename))

        # Check if data type is already associated with another column
        if not overwrite and dtype in self.data_column and self.get_data_column(dtype) != column:
            raise ValueError('Data type {} is already associated with column {} in {}'.format(
                dtype, self.get_data_column(dtype), self.basename))

        # Check if column is already assigned to another data type
        if not overwrite and column in self.data_column.values() and self.get_data_column(dtype) != column:
            raise ValueError('Column {} is already associated with data type {} in {}'.format(
                column, [k for k, v in self.data_column.items() if v == column][0], self.basename))

        source_name_list = self._source_select_to_list(source)
        spw_id_list = self._spw_select_to_list(spw)

        # Update data types per (source,spw) selection
        for source_name in source_name_list:
            for spw_id in spw_id_list:
                key = (source_name, spw_id)
                if key in self.data_types_per_source_and_spw:
                    if dtype not in self.data_types_per_source_and_spw[key]:
                        self.data_types_per_source_and_spw[key].append(dtype)
                else:
                    self.data_types_per_source_and_spw[key] = [dtype]

        # Check for existing column registration and remove it
        column_keys = [k for k, v in self.data_column.items() if v == column]
        if column_keys != []:
            for k in column_keys:
                del (self.data_column[k])

        # Update MS domain object
        if dtype not in self.data_column:
            self.data_column[dtype] = column
            LOG.info('Updated data column information of {}. Set {} to column {}'.format(self.basename, dtype, column))

    def get_data_column(self, dtype: DataType, source: Optional[str] = None, spw: Optional[str] = None) -> Optional[str]:
        """
        Return the column name associated with a DataType in an MS domain object.

        Args:
            dtype: DataType to fetch column name for
            source: source names (comma separated name selection string) to filter for. 
                    If unset, all sources will be used.
            spw: spectral windows (comma separated real spw ID selection string) to filter for. 
                 If unset, all real spw IDs will be used.

            If source and spw are both unset, the method will just look
            at the MS data type and column information. If one or both
            parameters are set, it will require all (source,spw)
            combinations to have data of the requested data type.

        Returns:
            A name of column of a dtype. Returns None if dtype is not defined
            in the MS.
        """
        if dtype not in self.data_column.keys():
            return None

        if source is None and spw is None:
            return self.data_column[dtype]

        source_name_list = self._source_select_to_list(source)
        spw_id_list = self._spw_select_to_list(spw)

        # Check all (source,spw) combinations
        data_exists_for_all_source_spw_combinations = True
        for source_name in source_name_list:
            for spw_id in spw_id_list:
                key = (source_name, spw_id)
                if dtype not in self.data_types_per_source_and_spw.get(key, []):
                    data_exists_for_all_source_spw_combinations = False

        if data_exists_for_all_source_spw_combinations:
            return self.data_column[dtype]
        else:
            return None

    def get_data_type(self, column: str, source: Optional[str] = None, spw: Optional[str] = None) -> Optional[DataType]:
        """
        Return the DataType associated with a column in an MS domain object.

        Args:
            column: name of column in MS
            source: source names (comma separated name selection string) to filter for. 
                    If unset, all sources will be used.
            spw: spectral windows (comma separated real spw ID selection string) to filter for. 
                 If unset, all real spw IDs will be used.

            If source and spw are both unset, the method will just look
            at the MS data type and column information. If one or both
            parameters are set, it will require all (source,spw)
            combinations to have data of the requested data type.

        Returns:
            The DataType associated with the column name. Returns None
            if dtype is not defined in the MS or in the source/spw
            selection.
        """
        if column not in self.data_column.values():
            return None

        # Invert dictionary. This should not lead to wrong mappings
        # because data types and columns have a 1:1 relation.
        data_type = {v: k for k, v in self.data_column.items()}

        if source is None and spw is None:
            return data_type[column]

        source_name_list = self._source_select_to_list(source)
        spw_id_list = self._spw_select_to_list(spw)

        # Check all (source,spw) combinations
        data_exists_for_all_source_spw_combinations = True
        column_dtype = data_type[column]
        for source_name in source_name_list:
            for spw_id in spw_id_list:
                key = (source_name, spw_id)
                if column_dtype not in self.data_types_per_source_and_spw.get(key, []):
                    data_exists_for_all_source_spw_combinations = False

        if data_exists_for_all_source_spw_combinations:
            return column_dtype
        else:
            return None

    def _source_select_to_list(self, source_select: Union[str, None]) -> List[str]:
        """
        Convert a CASA-style source selection string to a list of source names.

        Args:
            source_select: source string to convert

        Returns:
            A list of source names (as strings)
        """

        if source_select is None or not source_select.strip():
            # if None or empty or blank selection string, use all sources
            source_list = [utils.dequote(s.name) for s in self.sources]
        else:
            source_list = [utils.dequote(s.strip()) for s in source_select.split(',')]

        return source_list

    def _spw_select_to_list(self, spw_select: Union[str, None]) -> List[int]:
        """
        Convert a CASA-style spw selection string to a list of spw IDs.

        Args:
            spw_select: spw selection string to convert

        Returns:
            A list of spw IDs (as integers)
        """

        if spw_select is None or not spw_select.strip():
            # if None or empty or blank selection string, use all spws
            spw_list = [s.id for s in self.spectral_windows]
        else:
            spw_list = utils.range_to_list(spw_select)

        return spw_list
