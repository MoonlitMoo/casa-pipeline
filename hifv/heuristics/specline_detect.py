"""
Heuristic for identifying and/or defining the SPW designation, either as a continuum window or for spectral line analysis
"""
from typing import Type

import pipeline.infrastructure.logging as logging
from pipeline.domain.spectralwindow import SpectralWindow
from pipeline.domain.measurementset import MeasurementSet
from pipeline.infrastructure.utils.conversion import range_to_list, commafy
from pipeline.infrastructure.utils import find_ranges

LOG = logging.get_logger(__name__)


def detect_spectral_lines(mset: Type[MeasurementSet], specline_spws: str='auto') -> None:
    """Handles assignment of spectral windows

    Args:
        mset(object): MeasurementSet object containing MS information
        specline_spws(str): tells pipeline how to handle spectral line assignment; 'auto', 'none', or
            user-defined spectral line spws in CASA format (e.g. '2,3,4~9,23')
    """

    spw2band = mset.get_vla_spw2band()
    spwobjlist = mset.get_spectral_windows(science_windows_only=True)
    spec_windows = list()
    if specline_spws == 'auto':
        LOG.info("Spectral line detection set to auto. "
                 "The pipeline will determine which science spws will be used for spectral line analysis.")
        for spw in spwobjlist:
            _auto_detector(spw, spw2band[spw.id])
    elif specline_spws == 'none':
        LOG.info("Spectral line assignment is turned off. All science spws will be regarded as continuum.")
    elif specline_spws == 'all':
        LOG.info("Spectral line assignment is set to all.")
        spec_windows = [spw.id for spw in spwobjlist]
    else:
        spec_windows = range_to_list(specline_spws)
        LOG.debug('User-defined spectral windows for spectral line analysis: %s', spec_windows)
        if not all([isinstance(x, int) for x in spec_windows]):
            LOG.error("Invalid input for user-defined spws: %s", spec_windows)
            raise Exception("Invalid input for user-defined spws: %s" % spec_windows)
        LOG.info("Spectral line assignment defined by user.")
        non_sci = list(set(spec_windows).difference([x.id for x in spwobjlist]))
        if non_sci:
            message = commafy([str(x) for x in non_sci], quotes=False)
            LOG.info("Non-science windows %s defined by the user will be skipped.", message)
            spec_windows = list(set(spec_windows).intersection([x.id for x in spwobjlist]))
            specline_spws = find_ranges(spec_windows)
        LOG.info('The user defined the following spws for spectral line analysis: %s. '
                 'All other spws will be regarded as continuum.', specline_spws)
    for spw in spwobjlist:
        if int(spw.id) in spec_windows:
            spw.specline_window = True

def _auto_detector(spw: Type[SpectralWindow], band: str) -> None:
    """Determines if a spectral window should be designated as a spectral line window using the following logic:
       - frequency above L-band (greater than 1GHz) and
         - L- or S-band and window more narrow than 32 MHz and more than 64 channels or
         - window more narrow than 64MHz or more than 128 channels or channel widths less than 0.5 MHz

    Args:
        spw(object): spectral window object stored in pipeline context
        band(str): string containing the VLA band name
    """

    spectral_line_spw = False
    spw_type = "continuum"
    if spw.min_frequency.value < 1000000000.0:
        LOG.info("Frequencies below 1GHz are not currently supported. Regarding spw %s as a continuum window." % spw.id)
    else:
        if band in ["L", "S"]:
            if (spw.bandwidth.value / spw.num_channels) < 250000.0:
                LOG.debug("New criteria met for spw %d.", spw.id)
                spectral_line_spw = True
                spw_type = "spectral line"
        else:
            if any([spw.bandwidth.value < 64000000.0,
                    spw.num_channels > 128,
                    (spw.bandwidth.value / spw.num_channels) < 500000.0]):
                spectral_line_spw = True
                spw_type = "spectral line"
        LOG.info("Spw %d has been identified as a %s window.", spw.id, spw_type)
    spw.specline_window = spectral_line_spw
        
