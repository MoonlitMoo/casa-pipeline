import numpy
import os
from typing import List, Tuple, Optional, Union

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.utils as utils
from pipeline.h.tasks.common.commonhelpermethods import get_corr_products
from pipeline.domain.measurementset import MeasurementSet
from pipeline.infrastructure import casa_tools

LOG = infrastructure.get_logger(__name__)


def read_channel_averaged_data_from_ms(ms: MeasurementSet,
                                       fieldid: Union[str, int],
                                       spwid: Union[str, int],
                                       intent: str,
                                       items: List[str],
                                       baseline_set: Optional[List] = None) -> dict:
    """
    Read the channel-averaged visibility data from a MS for the given field, spw, intent,
    and optionally a subset of baselines.
    Arguments:
        ms:       MeasurementSet.
        fieldid:  string or int representing the field ID.
        spwid:    string or int representing the SPW ID.
        intent:   intent or list of intents.
        items:    list of data arrays to read, e.g., ['corrected_data', 'flag', 'antenna1', 'antenna2'].
        baseline_set:  list of baselines (default: all).
    Returns:
        a dict with the data array for each element in items.
    """
    # Identify scans for given data selection.
    scans_with_data = ms.get_scans(scan_intent=intent, field=str(fieldid), spw=str(spwid))
    if not scans_with_data:
        LOG.info('No data expected for {} {} intent, field {}, spw {}. Continuing...'.
                 format(ms.basename, intent, fieldid, spwid))
        return {}

    # Initialize data selection.
    data_selection = {'field': str(fieldid),
                      'scanintent': '*%s*' % utils.to_CASA_intent(ms, intent),
                      'spw': str(spwid)}

    # Add baseline set to data selection if provided; log selection.
    if baseline_set:
        LOG.info('Reading data for {}, intent {}, field {}, spw {}, and {} baselines ({})'.
                 format(os.path.basename(ms.name), intent, fieldid, spwid, baseline_set[0], baseline_set[1]))
        data_selection['baseline'] = baseline_set[1]
    else:
        LOG.info('Reading data for {}, intent {}, field {}, spw {}, and all baselines'.
                 format(os.path.basename(ms.name), intent, fieldid, spwid))

    # Get number of channels for this spw.
    nchans = ms.get_all_spectral_windows(str(spwid))[0].num_channels

    # Read in data from MS.
    with casa_tools.MSReader(ms.name) as openms:
        try:
            # Apply data selection, and set channel selection to take the
            # average of all channels.
            openms.msselect(data_selection)
            openms.selectchannel(1, 0, nchans, 1)

            # Extract data from MS.
            data = openms.getdata(items)
        except:
            LOG.warning('Unable to read data for intent {}, field {}, spw {}'.format(intent, fieldid, spwid))
            data = {}

    return data


def compute_mean_flux(ms: MeasurementSet,
                      fieldid: Union[str, int],
                      spwid: Union[str, int],
                      intent: str) -> Tuple[float, float]:
    """
    Compute the mean flux and its standard deviation (averaged across available polarizations)
    for the given MS, field, spw and intent.
    Arguments:
        ms:  MeasurementSet.
        fieldid:  string or int representing the field ID.
        spwid:    string or int representing the SPW ID.
        intent:   intent or list of intents.
    Returns:
        a tuple of two values -- mean flux and its standard deviation, or a tuple of two zeros if data not available.
    """

    # Read in data from the MS, for specified intent, field, and spw.
    try:
        data = read_channel_averaged_data_from_ms(ms, fieldid, spwid, intent, ['corrected_data', 'flag', 'antenna1', 'antenna2', 'weight'])
    except Exception as ex:
        LOG.warn('Cannot retrieve data for MS {}, field {}, spw {}, intent {}: {}'.
                 format(ms.basename, fieldid, spwid, intent, ex))
        data = {}

    # Return zero if no valid data were read.
    if not data:
        return 0.0, 0.0

    # Get number of correlations for this spw.
    corr_type = get_corr_products(ms, int(spwid))
    ncorrs = len(corr_type)

    # Select which correlations to consider for computing the mean
    # visibility flux:
    #  - for single and dual pol, consider all correlations.
    #  - for ncorr == 4 with linear correlation products (XX, XY, etc)
    #    select the XX and YY columns.
    #  - for other values of ncorrs, or e.g. circular correlation (LL, RL)
    #    raise a warning that these are not handled.
    if ncorrs in [1, 2]:
        columns_to_select = range(ncorrs)
    elif ncorrs == 4 and set(corr_type) == {'XX', 'XY', 'YX', 'YY'}:
        columns_to_select = [corr_type.index('XX'), corr_type.index('YY')]
    else:
        LOG.warning("Unexpected polarisations found for MS {}, unable to compute mean visibility fluxes.".
                    format(ms.basename))
        columns_to_select = []

    # Derive mean flux and variance for each polarisation.
    mean_fluxes = []
    variances = []
    for col in columns_to_select:
        # Select data for current polarisation.
        ampdata = numpy.squeeze(data['corrected_data'], axis=1)[col]
        flagdata = numpy.squeeze(data['flag'], axis=1)[col]
        weightdata = data['weight'][col]

        # Select for non-flagged data and non-NaN data.
        id_nonbad = numpy.where(numpy.logical_and(numpy.logical_not(flagdata), numpy.isfinite(ampdata)))
        amplitudes = ampdata[id_nonbad]
        weights = weightdata[id_nonbad]

        # If no valid data are available, skip to the next polarisation.
        if len(amplitudes) == 0:
            continue

        # Determine number of non-flagged antennas, baselines, and
        # integrations.
        ant1 = data['antenna1'][id_nonbad]
        ant2 = data['antenna2'][id_nonbad]
        n_ants = len(set(ant1) | set(ant2))
        n_baselines = n_ants * (n_ants - 1) // 2
        n_ints = len(amplitudes) // n_baselines

        # PIPE-644: Determine scale factor for variance.
        var_scale = numpy.mean([len(amplitudes), n_ints * n_ants])

        # Compute mean flux and stdev for current polarisation.
        mean_flux = numpy.abs(numpy.average(amplitudes, weights=weights))
        variance = numpy.average((numpy.abs(amplitudes) - mean_flux)**2, weights=weights) / var_scale

        # Store for this polarisation.
        mean_fluxes.append(mean_flux)
        variances.append(variance)

    # Compute mean flux and mean stdev for all polarisations.
    if mean_fluxes:
        mean_flux = numpy.mean(mean_fluxes)
        std_flux = numpy.mean(variances)**0.5
    # If no valid data was found for any polarisation, then set flux and
    # uncertainty to zero.
    else:
        LOG.debug("No valid data found for MS {}, field {}, spw {}, unable to compute mean visibility flux; flux"
                  " and uncertainty will be set to zero.".format(ms.basename, fieldid, spwid))
        mean_flux = 0.0
        std_flux = 0.0

    return mean_flux, std_flux
