import ast
import collections
import functools
import operator
import re
from decimal import Decimal
from math import sqrt
from typing import Iterable, List, Optional, Tuple

import numpy

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.pipelineqa as pqa
import pipeline.infrastructure.utils as utils
import pipeline.qa.scorecalculator as qacalc
from pipeline.domain import Field, FluxMeasurement, MeasurementSet, SpectralWindow
from pipeline.domain.measures import FluxDensity, FluxDensityUnits, Frequency, FrequencyUnits
from pipeline.h.tasks.common import commonfluxresults
from pipeline.h.tasks.importdata.fluxes import ORIGIN_ANALYSIS_UTILS, ORIGIN_XML
from pipeline.hifa.heuristics.snr import ALMA_BANDS, ALMA_SENSITIVITIES, ALMA_TSYS
from pipeline.hifa.tasks.importdata.dbfluxes import ORIGIN_DB
from pipeline.infrastructure import casa_tools
from pipeline.infrastructure.launcher import Context
from . import gcorfluxscale

LOG = infrastructure.get_logger(__name__)

COLSHAPE_FORMAT = re.compile(r'\[(?P<num_pols>\d+), (?P<num_rows>\d+)\]')

# Defines some characteristic values for each ALMA receiver band.
# sensitivity = mJy (for 16*12m antennas, 1 minute, 8 GHz, 2pol)
BandInfo = collections.namedtuple('BandInfo', 'name number nominal_tsys sensitivity')
BAND_INFOS = [BandInfo(name=ALMA_BANDS[i], number=i+1, nominal_tsys=ALMA_TSYS[i],
                       sensitivity=FluxDensity(ALMA_SENSITIVITIES[i], FluxDensityUnits.MILLIJANSKY))
              for i in range(len(ALMA_BANDS))]

# External flux providers
EXTERNAL_SOURCES = (ORIGIN_ANALYSIS_UTILS, ORIGIN_DB, ORIGIN_XML)

# Trusted flux providers. Using untrusted providers will result in a warning.
TRUSTED_SOURCES = (ORIGIN_ANALYSIS_UTILS, ORIGIN_DB)


class GcorFluxscaleQAHandler(pqa.QAPlugin):
    result_cls = commonfluxresults.FluxCalibrationResults
    child_cls = None
    generating_task = gcorfluxscale.GcorFluxscale

    def handle(self, context, result):
        vis = result.inputs['vis']
        ms = context.observing_run.get_ms(vis)

        # Check for existence of field / spw combinations for which
        # the derived fluxes are missing.
        score1 = self._missing_derived_fluxes(ms, result.inputs['transfer'], result.inputs['transintent'],
                                              result.measurements)
        score2 = self._low_snr_fluxes(ms, result.measurements)
        scores = [score1, score2]

        scores.extend(score_kspw(context, result))

        result.qa.pool.extend(scores)

    @staticmethod
    def _missing_derived_fluxes(ms, field, intent, measurements):
        """
        Check whether there are missing derived fluxes. 
        """
        return qacalc.score_missing_derived_fluxes(ms, field, intent, measurements)

    @staticmethod
    def _low_snr_fluxes(ms, measurements):
        """
        Check whether there are low SNR derived fluxes. 
        """
        return qacalc.score_derived_fluxes_snr(ms, measurements)


class GcorFluxscaleListQAHandler(pqa.QAPlugin):
    """
    QA handler for a list containing FluxCalibrationResults.
    """
    result_cls = collections.abc.Iterable
    child_cls = commonfluxresults.FluxCalibrationResults
    generating_task = gcorfluxscale.GcorFluxscale

    def handle(self, context, result):
        # collate the QAScores from each child result, pulling them into our
        # own QAscore list
        collated = utils.flatten([r.qa.pool for r in result])
        result.qa.pool[:] = collated

        mses = [r.inputs['vis'] for r in result]
        longmsg = 'No missing derived fluxes in %s' % utils.commafy(mses, quotes=False, conjunction='or')
        result.qa.all_unity_longmsg = longmsg


def score_kspw(context, result):
    # Spec from CAS-10792:
    #
    # QA score 1: "internal spw-spw consistency":
    #
    # Use a ratio of the (gflux flux for the SPW in question) / (catalog
    # flux for the SPW in question) / ( the same thing calculated for the
    # highest SNR wide [>= 1 GHz] SPW). More precisely, defining
    # r_SPW = (gflux flux for SPW) / (catalog flux for SPW), I suggest
    # using K_SPW = r_spw / r_max_snr_spw as the metric. If there are no
    # >= 1 GHz SPWs, use the highest SNR SPW which has a bandwidth greater
    # than or equal to the median bandwidth of all SPWs. SNR is to be
    # calculated from au.gaincalSNR() or equivalent, not from the SNR
    # implied by the weblog, which is less reliable in general.

    # Retrieve the MS.
    ms = context.observing_run.get_ms(result.inputs['vis'])

    # Gather SpW IDs for all fields with flux measurements in the result.
    fluxmeas_spwids = {fd.spw_id for measurements in result.measurements.values() for fd in measurements}

    # Compute SNR info per SpW for the phase calibrator field; exit early if
    # no SNR info is available.
    snr_info = _compute_snr_info(context, ms, fluxmeas_spwids)
    if not snr_info:
        return []

    # Check whether the SNR info could be computed for all SpWs for which there
    # are flux measurements. If not, then log an error and return early.
    snr_info_spwids = {k for k in snr_info if k in fluxmeas_spwids}
    if not snr_info_spwids.issuperset(fluxmeas_spwids):
        LOG.error('Error calculating internal spw-spw consistency: could not identify highest SNR spectral window')
        return []

    # Create QA scores for each field with flux measurements.
    all_scores = []
    for field_id, measurements in result.measurements.items():
        # get domain object for the field.
        fields = ms.get_fields(task_arg=field_id)
        assert len(fields) == 1
        field = fields[0]

        # these strings will be used repeatedly in log messages
        msg_intents = ','.join(field.intents)
        msg_fieldname = utils.dequote(field.name)

        # Among SpWs in flux measurements for this field, identify the SpW with
        # the highest SNR. If no SpW could be determined, then skip this field
        # and continue with the next one.
        highest_snr_spw = _get_highest_snr_spw(snr_info, measurements, ms, msg_fieldname, msg_intents)
        if highest_snr_spw is None:
            continue

        # Get the flux measurement with highest SNR and corresponding ratio of
        # derived flux over catalog flux. If no flux ratio could be determined,
        # then skip this field and continue with the next one.
        highest_snr_measurement, r_snr = _get_highest_snr_measurement_and_flux_ratio(
            field, highest_snr_spw, measurements, msg_fieldname, msg_intents)
        if r_snr is None:
            continue

        # Identify the other, non-highest-SNR, flux measurements that need to
        # be scored.
        other_measurements = [m for m in measurements if m is not highest_snr_measurement]

        # Compute "k_spw" for the "other" measurements for current field. If no
        # "k_spw" could be computed, then skip this field and continue with the
        # next one.
        k_spws = _compute_k_spws_for_flux_measurements(field, other_measurements, r_snr, msg_fieldname, msg_intents)
        if k_spws is None:
            continue

        # Compute QA score based on "k_spw" for current field, and add to list
        # of all QA scores.
        field_qa_scores = [qacalc.score_gfluxscale_k_spw(ms.basename, field, spw_id, k_spw, highest_snr_spw.id)
                           for spw_id, k_spw in k_spws]
        all_scores.extend(field_qa_scores)

    return all_scores


def _compute_snr_info(context: Context, ms: MeasurementSet, spwids: Iterable[int]) -> dict:
    """
    Compute and return the per-antenna, per-SpW SNR for given measurement set
    and spectral window IDs.

    Args:
        context: pipeline Context
        ms: MeasurementSet domain object
        spwids: IDs of SpWs to compute SNR for

    Returns:
        Dictionary of SNR info keyed by spectral window ID.
    """
    # Get the path to the Tsys caltable for the current MS. If no matching
    # Tsys caltable was found, that is ok, gaincalSNR will do without.
    caltable_path = _get_tsys_caltable_path(context, ms.basename)

    # PIPE-2083: for standard (non-BandToBand) datasets, a single SNR assessment
    # using the PHASE field should cover all Science SpWs:
    if not ms.is_band_to_band:
        spws = ms.get_spectral_windows(task_arg=','.join(str(s) for s in spwids))
        LOG.info(f"{ms.basename}: computing SNR info for phase calibrator.")
        snr_info = _compute_snr_info_for_intent(context, ms, caltable_path, spws, intent='PHASE')
    # For a BandToBand dataset, the PHASE field will only cover the diffgain
    # reference SpWs. To also obtain SNR estimates for the diffgain on-source
    # SpWs, a separate SNR assessment needs to be run using the CHECK source
    # field.
    else:
        # First split the requested SpW IDs into diffgain reference and
        # on-source SpWs.
        spw_str = ','.join(str(s) for s in spwids)
        dg_refspws = ms.get_spectral_windows(task_arg=spw_str, intent='DIFFGAINREF')
        dg_srcspws = ms.get_spectral_windows(task_arg=spw_str, intent='DIFFGAINSRC')

        # Compute the SNR estimates for the diffgain reference SpWs.
        LOG.info(f"{ms.basename} is a BandToBand project: computing SNR info for diffgain reference SpWs using the"
                 f" phase calibrator.")
        snr_info_dg_ref = _compute_snr_info_for_intent(context, ms, caltable_path, dg_refspws, intent='PHASE')

        # Compute the SNR estimates for the diffgain on-source SpWs.
        LOG.info(f"{ms.basename} is a BandToBand project: computing SNR info for diffgain on-source SpWs using the"
                 f" check source.")
        snr_info_dg_src = _compute_snr_info_for_intent(context, ms, caltable_path, dg_srcspws, intent='CHECK')

        # Merge the resulting dictionaries, keeping only the SpW keys.
        snr_info = {k: v for k, v in snr_info_dg_ref.items() if k in spwids}
        snr_info.update({k: v for k, v in snr_info_dg_src.items() if k in spwids})

    return snr_info


def _compute_snr_info_for_intent(context: Context, ms: MeasurementSet, caltable_path: str,
                                 spws: Iterable[SpectralWindow], intent: str = "PHASE") -> dict:
    """
    Compute and return the per-antenna, per-SpW SNR for given measurement set,
    intent, and spectral windows.

    This function first identifies what field to analyse for given intent, then
    retrieves the fluxes for that field, and finally calls "gaincalSNR" to
    compute the SNR info.

    Args:
        context: pipeline Context
        ms: MeasurementSet domain object
        caltable_path: path to the Tsys caltable
        spws: SpectralWindow objects to compute SNR for
        intent: observing intent to use for the calibrator

    Returns:
        Dictionary of SNR info keyed by spectral window ID.
    """
    # Identify what field to analyse; exit early if none was found.
    field = _get_field_to_analyse(ms, intent)
    if not field:
        LOG.warning(f'Error calculating internal spw-spw consistency: no field found for {intent} intent.')
        return {}

    # Retrieve fluxes for selected field; exit early if none are found.
    fluxes = _get_fluxes_for_field(ms, field)
    if not fluxes:
        LOG.error(f'Error calculating internal spw-spw consistency: no flux densities found for {intent} intent'
                  f' (field: {utils.dequote(field.name)})')
        return {}

    # Run gaincalSNR to compute SNR info per SpW for current field. Warn if no
    # SNR info was returned.
    gaincalsnr_output = gaincalSNR(context, ms, caltable_path, fluxes, field, spws, intent=intent)
    if not gaincalsnr_output:
        LOG.warning('Error calculating internal spw-spw consistency: no result from gaincalSNR function')

    return gaincalsnr_output


def _compute_k_spws_for_flux_measurements(field: Field, flux_measurements: Iterable[FluxMeasurement], r_snr: float,
                                          msg_fieldname: str, msg_intents: str) -> Optional[list]:
    """
    Compute the "k_spw" metric for given list of flux measurements and given
    flux ratio for highest SNR SpW.

    If no catalog fluxes are available, return early with None.

    Args:
        field: field to retrieve catalog fluxes for
        flux_measurements: flux measurements to compute k_spw for
        r_snr: flux ratio for highest SNR SpW
        msg_fieldname: pre-formatted name of field, for log messages
        msg_intents: pre-formatted name of intents, for log messages

    Returns:
        List of 'k_spw' for given flux measurements, or None.
    """
    k_spws = []
    for m in flux_measurements:
        # Retrieve catalog fluxes for current flux measurement; exit early if
        # none could be found.
        catalogue_fluxes = [f for f in field.flux_densities if f.origin in EXTERNAL_SOURCES and f.spw_id == m.spw_id]
        if not catalogue_fluxes:
            LOG.info('No catalogue measurement for {} ({}) spw {}'.format(msg_fieldname, msg_intents, m.spw_id))
            return None

        assert (len(catalogue_fluxes) == 1)
        catalogue_flux = catalogue_fluxes[0]

        # Compute ratio of derived flux over catalog flux for current
        # measurement.
        r_spw = m.I.to_units(FluxDensityUnits.JANSKY) / catalogue_flux.I.to_units(FluxDensityUnits.JANSKY)

        # Compute "k_spw" as the ratio of the flux ratio for current measurement
        # over the ratio for the highest SNR measurement.
        k_spw = r_spw / r_snr
        k_spws.append((m.spw_id, k_spw))

    # Sort "k_spw" by SpW ID.
    k_spws.sort(key=operator.itemgetter(0))

    return k_spws


def _get_field_to_analyse(ms: MeasurementSet, intent: str) -> Field:
    """
    Identify and return which field to analyse for the "k_spw" score.

    If there is more than one field for given intent calibrator, then pick the
    first one that does NOT also have observe_target intent. If all have both
    intents, then continue to use the first one observed.

    Args:
        ms: MeasurementSet to find field for
        intent: intent to find field for

    Returns:
        Field to analyse.
    """
    candidate_fields = [f for f in ms.get_fields(intent=intent) if 'TARGET' not in f.intents]
    if not candidate_fields:
        candidate_fields = ms.get_fields(intent=intent)
    field = min(candidate_fields, key=lambda f: f.time.min())
    return field


def _get_fluxes_for_field(ms: MeasurementSet, field: Field) -> List[Tuple[int, float, float]]:
    """
    Return flux information for given measurement set and field.

    Select flux measurements that are from external sources, and return for each
    the SpW ID, SpW mean frequency, and total intensity.

    Args:
        ms: MeasurementSet to retrieve fluxes for
        field: field to retrieve fluxes for

    Returns:
        List of 3-tuples, containing:
            SpW ID for flux measurement
            Mean frequency of SpW in Hertz
            Total intensity in Jansky in flux measurement.
    """
    fluxes = []
    for fm in [fm for fm in field.flux_densities if fm.origin in EXTERNAL_SOURCES]:
        spw = ms.get_spectral_window(fm.spw_id)
        fluxes.append((spw.id, float(spw.mean_frequency.to_units(FrequencyUnits.HERTZ)),
                       float(fm.I.to_units(FluxDensityUnits.JANSKY))))
    return fluxes


def _get_highest_snr_measurement_and_flux_ratio(field: Field, highest_snr_spw: SpectralWindow,
                                                measurements: Iterable[FluxMeasurement], msg_fieldname: str,
                                                msg_intents: str) -> Tuple[FluxMeasurement, Optional[float]]:
    """
    Retrieve flux measurement for given "highest SNR" SpW, compute ratio of
    derived over catalog flux, and return both flux measurement and flux ratio.

    If no catalog fluxes are available, then the flux ratio is returned as None.

    Args:
        field: field to retrieve catalog fluxes for
        highest_snr_spw: SpW with highest SNR
        measurements: derived flux measurements for given field
        msg_fieldname: pre-formatted name of field, for log messages
        msg_intents: pre-formatted name of intents, for log messages

    Returns:
        Tuple containing flux measurement and flux ratio for highest SNR SpW
    """
    # Retrieve flux measurement for highest SNR SpW.
    highest_snr_measurement = [m for m in measurements if m.spw_id == highest_snr_spw.id]
    assert (len(highest_snr_measurement) == 1)
    highest_snr_measurement = highest_snr_measurement[0]
    highest_snr_i = highest_snr_measurement.I

    # Retrieve the catalogue flux for the highest SNR spw.
    catalogue_fluxes = [f for f in field.flux_densities
                        if f.origin in EXTERNAL_SOURCES
                        and f.spw_id == highest_snr_measurement.spw_id]
    if not catalogue_fluxes:
        LOG.warning('Cannot calculate internal spw-spw consistency for {} ({}): no catalogue measurement for '
                    'highest SNR spw ({})'.format(msg_fieldname, msg_intents, highest_snr_measurement.spw_id))
        return highest_snr_measurement, None
    assert (len(catalogue_fluxes) == 1)
    catalogue_flux = catalogue_fluxes[0]

    # Compute ratio of derived flux to catalogue flux for highest SNR spw.
    r_snr = highest_snr_i.to_units(FluxDensityUnits.JANSKY) / catalogue_flux.I.to_units(FluxDensityUnits.JANSKY)

    return highest_snr_measurement, r_snr


def _get_highest_snr_spw(snr_info: dict, flux_measurements: Iterable[FluxMeasurement], ms: MeasurementSet,
                         msg_fieldname: str, msg_intents: str) -> Optional[SpectralWindow]:
    """
    Return the highest SNR SpW for flux measurements based on SNR information.

    Args:
        snr_info: dictionary with output from the gaincal SNR assessment
        flux_measurements: flux measurements to consider
        ms: measurement set to consider
        msg_fieldname: pre-formatted name of field, for log messages
        msg_intents: pre-formatted name of intents, for log messages

    Returns:
        Spectral window with highest SNR
    """
    # Define 1 GHZ for bandwidth comparisons.
    one_ghz = Frequency(1, FrequencyUnits.GIGAHERTZ)

    # Determine which SpWs to consider based on what SpWs are present in the
    # flux measurements and the given intent(s).
    spws = ms.get_spectral_windows(','.join(str(m.spw_id) for m in flux_measurements))
    # PIPE-2083: for BandToBand datasets, override the selection if the intents
    # cover either PHASE or CHECK, to restrict to just the diffgain reference
    # SpWs for PHASE intent or diffgain on-source SpWs for CHECK intent.
    if ms.is_band_to_band:
        if 'PHASE' in msg_intents:
            spws = ms.get_spectral_windows(task_arg=','.join(str(m.spw_id) for m in flux_measurements),
                                           intent='DIFFGAINREF')
        elif 'CHECK' in msg_intents:
            spws = ms.get_spectral_windows(task_arg=','.join(str(m.spw_id) for m in flux_measurements),
                                           intent='DIFFGAINSRC')

    # discard narrow windows < 1GHz
    spw_snr_candidates = [spw for spw in spws if spw.bandwidth >= one_ghz]

    # fall back to median bandwidth selection if all the windows are narrow
    if not spw_snr_candidates:
        LOG.info('No wide (>= 1 GHz) spectral windows identified for {} ({})'.format(msg_fieldname, msg_intents))

        # find median bandwidth of all spws...
        bandwidths = [spw.bandwidth.to_units(FrequencyUnits.HERTZ) for spw in spws]
        median_bandwidth = Frequency(numpy.median(bandwidths), FrequencyUnits.HERTZ)

        # ... and identify SNR spw candidates accordingly
        LOG.info('Taking highest SNR window from spws with bandwidth >= {}'.format(median_bandwidth))
        spw_snr_candidates = [spw for spw in spws if spw.bandwidth >= median_bandwidth]

    # PIPE-2083: if no SpW SNR candidates could be found, log error and return
    # early.
    if not spw_snr_candidates:
        LOG.error(f'{ms.basename}: Error calculating internal spw-spw consistency for {msg_fieldname}'
                  f' ({msg_intents}): could not identify candidate spectral windows.')
        return None

    # Identify the spw with the highest SNR.
    highest_snr_spw = max(spw_snr_candidates, key=lambda spw: snr_info[spw.id]['snr'])

    return highest_snr_spw


def _get_tsys_caltable_path(context: Context, vis: str) -> str:
    """
    Identify the Tsys caltable for the current vis, and return the path to this
    table.

    Args:
        context: pipeline context
        vis: MS to look up Tsys caltable for

    Returns:
        Path to the Tsys caltable.
    """
    # Go through all caltables registered in callibrary looking for a Tsys
    # caltable matching this vis.
    for caltable_path in context.callibrary.active.get_caltable(caltypes='tsys'):
        with casa_tools.TableReader(caltable_path) as table:
            msname = table.getkeyword('MSName')
        if msname in vis:
            break
    else:
        caltable_path = ''
    return caltable_path


def gaincalSNR(context: Context, ms: MeasurementSet, tsysTable: str, flux: Iterable[Tuple[int, float, float]],
               field: Field, spws: Iterable[SpectralWindow], intent='PHASE', required_snr: float = 25,
               edge_fraction: float = 0.03125, min_snr: float = 10) -> dict:
    """
    Computes the per-antenna SNR expected for gaincal(solint='inf') on a
    per-spw basis and recommends whether bandwidth transfer and/or
    combine='spw' is needed.

    This function is based upon the analysisUtils gaincalSNR code by Todd Hunter.

    :param context: pipeline Context
    :param ms: MeasurementSet domain object
    :param tsysTable: path to Tsys caltable
    :type tsysTable: str
    :param flux: the list of flux measurements to use
    :type flux: [FluxDensity, ...]
    :param field: the field to use
    :type field: Field
    :param spws: the spectral windows to make predictions for
    :type spws: [SpectralWindow, ...]
    :param intent: observing intent to use for the calibrator
    :type intent: str
    :param required_snr: threshold for which to make decisions (default=25)
    :param edge_fraction: the fraction of bandwidth to ignore on each edge of a
        TDM window (default=0.03125)
    :param min_snr: threshold for when even aggregate bandwidth is expected to
        fail (default=10)
    :return: a dictionary keyed by spectral window ID
    """
    # Set maximum effective bandwidth per baseband.
    max_effective_bandwidth_per_baseband = Frequency(2.0 * (1 - 2 * edge_fraction), FrequencyUnits.GIGAHERTZ)

    # 1) Get the number of antennas in the dataset. In principle, this should
    #    be the number of unflagged antennas on the PHASE calibrator. Here we
    #    have the simpler option to compute the number of completely unflagged
    #    antennas.
    num_antennas = len(ms.antennas)
    seven_metres_majority = (len([a for a in ms.antennas if a.diameter == 7.0]) / float(num_antennas)) > 0.5
    if seven_metres_majority:
        LOG.info('This is an ACA 7m dataset.')

    # 2) Get the calibrator and target object spw(s) to process.
    spw_types = ('TDM', 'FDM')
    all_gaincal_spws = {spw for spw in ms.spectral_windows if intent in spw.intents and spw.type in spw_types}
    all_target_spws = {spw for spw in ms.spectral_windows if 'TARGET' in spw.intents and spw.type in spw_types}
    all_spws = all_gaincal_spws.union(spws)

    num_basebands = len({spw.baseband for spw in all_spws})
    aggregate_bandwidth = compute_aggregate_bandwidth(all_gaincal_spws)
    widest_spw = max(all_target_spws, key=operator.attrgetter('bandwidth'))

    # 3) Identify scans of the gaincal target for each gaincal spw, then
    #    compute the median time on-source for these scans.
    scans = {spw: [scan for scan in ms.scans if intent in scan.intents and spw in scan.spws and field in scan.fields]
             for spw in all_gaincal_spws}

    # compute the median length of a "solint='inf', combine=''" scan. In
    # principle, this should be the time weighted by percentage of unflagged
    # data. Also, the method below will include sub-scan latency.
    time_on_source = {spw: numpy.median([scan.exposure_time(spw.id) for scan in scans[spw]])
                      for spw in all_gaincal_spws}

    spw_to_flux_density = {spw_id: FluxDensity(flux_jy, FluxDensityUnits.JANSKY) for spw_id, _, flux_jy in flux}

    gaincal_spw_ids = {spw.id for spw in all_gaincal_spws}
    phase_spw_to_tsys_spw = {ms.spectral_windows[i]: ms.spectral_windows[v]
                             for i, v in enumerate(utils.get_calfroms(context, ms.basename, 'tsys')[0].spwmap)
                             if i in gaincal_spw_ids}
    # map CALIBRATE_PHASE spw to Tsys scans for the corresponding Tsys spw
    phase_spw_to_tsys_scans = {
        phase_spw: [scan for scan in ms.scans if 'ATMOSPHERE' in scan.intents and tsys_spw in scan.spws]
        for phase_spw, tsys_spw in phase_spw_to_tsys_spw.items()
    }

    wrapper = CaltableWrapperFactory.from_caltable(tsysTable)

    # keys: CALIBRATE_PHASE spws, values: corresponding Tsys values
    get_snr_info = False  # Flag value to get SNR info or not
    median_tsys = {}
    for phase_spw, tsys_scans in phase_spw_to_tsys_scans.items():
        # If there are multiple scans for an spw, then simply use the Tsys of the first scan
        first_tsys_scan = min(tsys_scans, key=operator.attrgetter('id'))
        tsys_spw = phase_spw_to_tsys_spw[phase_spw]
        scan_data = wrapper.filter(spw=tsys_spw.id, scan=first_tsys_scan.id)
        if numpy.all(scan_data['FPARAM'].mask):  # Assign NaN if everything is masked
            median_tsys[phase_spw.id] = numpy.NaN
            get_snr_info = True
        else:
            median_tsys[phase_spw.id] = numpy.ma.median(scan_data['FPARAM'])

    # PIPE-1208: If any scan is fully masked, attempt to retrieve the info on
    # estimated SNRs that was derived during hifa_spwphaseup.
    snr_info = None
    if get_snr_info:
        # Check if a SpW mapping was registered for current field and intent.
        spwmap = ms.spwmaps.get((intent, field.name), None)
        if spwmap:
            # If a direct match exists, then use the corresponding SNR info.
            snr_info = spwmap.snr_info
        else:
            # Otherwise, retrieve SNR info from the first SpW mapping that
            # matches the current intent.
            for (spwmap_intent, _), spwmap in ms.spwmaps.items():
                if spwmap_intent == intent:
                    snr_info = spwmap.snr_info
                    break
        # Report if the retrieval of SNR info from hifa_spwphaseup failed.
        if snr_info is None:
            LOG.error(f"{ms.basename}: Estimated SNR from hifa_spwphaseup could not be retrieved.")

    # 6) compute the expected channel-averaged SNR
    # TODO Ask Todd if this is an error or a confusingly-named variable
    num_baselines = num_antennas - 1  # for an antenna-based solution

    diffgain_mode = {}
    mydict = {}

    eight_ghz = Frequency(8, FrequencyUnits.GIGAHERTZ)
    for spw in all_target_spws:
        obsspw = spw
        if spw not in all_gaincal_spws:
            # If this spw was not observed on the phase calibrator, then use the widest
            # spw from the same baseband that *was* observed on the phase calibrator
            # Ignore band-2-band possibility for now
            alt_spw = max([w for w in all_gaincal_spws if spw.baseband == w.baseband],
                          key=operator.attrgetter('bandwidth'))
            LOG.debug(f"This is a BandToBand or BandwidthSwitching project: spw {spw.id} matched to spw"
                      f" {alt_spw.id}.")
            spw = alt_spw

        mydict[spw.id] = {}
        diffgain_mode[obsspw] = spw
        band_info = [b for b in BAND_INFOS if b.name == spw.band].pop()
        relative_tsys = median_tsys[spw.id] / band_info.nominal_tsys
        time_factor = 1 / sqrt(time_on_source[spw].total_seconds() / 60.0)
        array_size_factor = sqrt(16 * 15 / 2.) / sqrt(num_baselines)

        area_factor = 1.0
        if seven_metres_majority:
            # scale by antenna collecting area
            area_factor = (12./7.)**2

        # scale by chan bandwidth
        bandwidth_factor = sqrt(eight_ghz / min([spw.bandwidth, max_effective_bandwidth_per_baseband]))
        # scale to single polarization solutions
        polarization_factor = sqrt(2)
        factor = relative_tsys * time_factor * array_size_factor * area_factor * bandwidth_factor * polarization_factor
        sensitivity = band_info.sensitivity * Decimal(factor)

        aggregate_bandwidth_factor = sqrt(eight_ghz / aggregate_bandwidth)
        factor = relative_tsys * time_factor * array_size_factor * area_factor * aggregate_bandwidth_factor * polarization_factor
        aggregate_bandwidth_sensitivity = band_info.sensitivity * Decimal(factor)

        snr_per_spw = spw_to_flux_density[spw.id] / sensitivity
        # PIPE-1208: Use the estimated SNR from hifa_spwphaseup if the data is fully masked
        if numpy.isnan(median_tsys.get(spw.id)):
            if snr_info is not None and str(spw.id) in snr_info:
                snr_value = snr_info[str(spw.id)]
                mydict[spw.id]['snr'] = Decimal(snr_value)
                mydict[spw.id]['snr_aggregate'] = Decimal(
                    snr_value * sqrt(
                        min([aggregate_bandwidth, max_effective_bandwidth_per_baseband * num_basebands]) /
                        min([spw.bandwidth, max_effective_bandwidth_per_baseband]))
                )
                LOG.info(f"{ms.basename}: for SpW {spw.id} SNR extracted from hifa_spwphaseup ({snr_value:.1f}).")
            else:
                snr_value = 0.0
                mydict[spw.id]['snr'] = Decimal('0.0')
                mydict[spw.id]['snr_aggregate'] = Decimal('0.0')
                LOG.error(f"{ms.basename}: for SpW {spw.id} SNR could not be extracted from hifa_spwphaseup, SNR set to"
                          f" 0.")
        else:
            snr_value = snr_per_spw
            mydict[spw.id]['snr'] = snr_per_spw
            mydict[spw.id]['snr_aggregate'] = spw_to_flux_density[spw.id] / aggregate_bandwidth_sensitivity
        mydict[spw.id]['meanFreq'] = spw.mean_frequency
        mydict[spw.id]['medianTsys'] = median_tsys[spw.id]
        mydict[spw.id]['Tsys_spw'] = phase_spw_to_tsys_spw[spw].id
        mydict[spw.id]['bandwidth'] = spw.bandwidth
        mydict[spw.id]['bandwidth_effective'] = min([spw.bandwidth, max_effective_bandwidth_per_baseband])
        mydict[spw.id]['calibrator_flux_density'] = spw_to_flux_density[spw.id]
        mydict[spw.id]['solint_inf_seconds'] = time_on_source[spw].total_seconds()
        mydict['aggregate_bandwidth'] = min([aggregate_bandwidth, max_effective_bandwidth_per_baseband * num_basebands])
        mydict['calibrator'] = field.name

        if spw == obsspw:
            # Then it is not a DIFFGAIN mode (B2B or BWSW) dataset, so compute
            # snr in widest spw.
            widest_spw_bandwidth_factor = sqrt(eight_ghz / widest_spw.bandwidth)
            factor = relative_tsys * time_factor * array_size_factor * area_factor * widest_spw_bandwidth_factor * polarization_factor
            widest_spw_bandwidth_sensitivity = band_info.sensitivity * Decimal(factor)
            if numpy.isnan(median_tsys.get(spw.id)):
                mydict[spw.id]['snr_widest_spw'] = Decimal(
                    snr_value * sqrt(
                        min([widest_spw.bandwidth, max_effective_bandwidth_per_baseband]) /
                        min([spw.bandwidth, max_effective_bandwidth_per_baseband])
                    )
                )
            else:
                mydict[spw.id]['snr_widest_spw'] = spw_to_flux_density[spw.id] / widest_spw_bandwidth_sensitivity
            mydict[spw.id]['widest_spw_bandwidth'] = widest_spw.bandwidth
        else:
            mydict[spw.id]['snr_widest_spw'] = 0

    for spw in all_target_spws:
        calspw = diffgain_mode[spw]
        if mydict[calspw.id]['snr'] >= required_snr:
            # PIPE-2083 clarified the status, which I believe is anyway never used
            if spw != calspw:
                if ms.get_diffgain_mode() == 'B2B':
                    mydict[calspw.id]['status'] = 'b2b_diffgain_mode'
                elif ms.get_diffgain_mode() == 'BWSW':
                    mydict[calspw.id]['status'] = 'bwsw_diffgain_mode'
            else:
                mydict[calspw.id]['status'] = 'normal'
            msg = ('science spw {} ({}) calibrated by spw {} has sufficient S/N: {:.1f}'
                   ''.format(spw.id, spw.bandwidth, calspw.id, mydict[calspw.id]['snr']))
        elif mydict[calspw.id]['snr_widest_spw'] >= required_snr:
            mydict[calspw.id]['status'] = 'spwmap'
            msg = ('science spw {} {} calibrated by widest spw ({}: bandwidth={}) has sufficient S/N: {:.1f}'
                   ''.format(spw.id, spw.bandwidth, widest_spw.id, widest_spw.bandwidth,
                             mydict[calspw.id]['snr_widest_spw']))
        elif mydict[calspw.id]['snr_aggregate'] >= min_snr:
            mydict[calspw.id]['status'] = 'combine_spw'
            msg = ('science spw {} ({}) calibrated by aggregate bandwidth ({}) has sufficient S/N: {:.1f}'
                   ''.format(spw.id, spw.bandwidth, aggregate_bandwidth, mydict[calspw.id]['snr_aggregate']))
        elif mydict[calspw.id]['medianTsys'] <= 0:
            msg = ('science spw {} ({}) has a negative median Tsys: there must be a problem in the data'
                   ''.format(spw.id, spw.bandwidth))
        else:
            msg = ('science spw {} ({}): Even aggregate bandwidth is insufficient (SNR<{:.0f}). QA2 Fail!'
                   ''.format(spw.id, spw.bandwidth, min_snr))
            mydict[calspw.id]['status'] = 'starved'
        LOG.info(msg)

    return mydict


def compute_aggregate_bandwidth(spws):
    """
    Computes the aggregate bandwidth for a list of spws of a measurement set.
    Accounts correctly for overlap.  Called by gaincalSNR().
    spw: an integer list, or comma-delimited string list of spw IDs
    """
    min_spw = min(spws, key=operator.attrgetter('min_frequency'))

    aggregate = [frequency_min_max_after_aliasing(min_spw)]

    spws_by_frequency = sorted(spws, key=operator.attrgetter('min_frequency'))
    for spw in spws_by_frequency[1:]:
        spw_fmin, spw_fmax = frequency_min_max_after_aliasing(spw)
        if spw_fmin < aggregate[-1][1] < spw_fmax:
            # spw begins before current max window ends, so extend the current
            # max window
            aggregate[-1] = (aggregate[-1][0], spw_fmax)

        elif spw_fmin > aggregate[-1][1] < spw_fmax:
            # interval is disjoint with existing, so add a new interval
            aggregate.append((spw_fmin, spw_fmax))

    bw = functools.reduce(lambda x, f_range: x + f_range[1] - f_range[0], aggregate, Frequency(0))
    return bw


def frequency_min_max_after_aliasing(spw):
    two_ghz = Frequency(2, FrequencyUnits.GIGAHERTZ)
    if spw.type == 'TDM' and spw.bandwidth == two_ghz:
        # 125Mhz of bandwidth is flagged (=8 channels are flagged, 4 at the
        # top of the band and 4 at the bottom, due to the anti-aliasing
        # filters reducing the sensitivity beyond that point.
        low_channel = spw.channels[4]
        high_channel = spw.channels[-5]
        # LSB channels could be in descending frequency order
        if low_channel.low > high_channel.low:
            low_channel, high_channel = high_channel, low_channel
        return low_channel.low, high_channel.high
    else:
        return spw.min_frequency, spw.max_frequency


class CaltableWrapperFactory(object):
    @staticmethod
    def from_caltable(filename):
        LOG.trace('CaltableWrapperFactory.from_caltable(%r)', filename)
        return CaltableWrapperFactory.create_param_wrapper(filename)

    @staticmethod
    def create_param_wrapper(path):
        with casa_tools.TableReader(path) as tb:
            colnames = tb.colnames()
            scalar_cols = [c for c in colnames if tb.isscalarcol(c)]
            var_cols = [c for c in colnames if tb.isvarcol(c)]

            dtypes = {c: get_dtype(tb, c) for c in colnames}
            readable_var_cols = [c for c in dtypes if c in var_cols and dtypes[c] is not None]
            col_dtypes = [dtypes[c] for c in dtypes if dtypes[c] is not None]

            data = numpy.ma.empty(tb.nrows(), dtype=col_dtypes)
            for c in scalar_cols:
                data[c] = tb.getcol(c)

            for c in readable_var_cols:
                records = tb.getvarcol(c)
                # results in a list of numpy arrays, one for each row in the
                # caltable. The shape of each numpy array is number of
                # correlations, number of channels, number of values for that
                # correlation/channel combination - which is always 1. Squeeze out
                # the unnecessary dimension and swap the channel and correlation
                # axes.
                row_data = [records['r{}'.format(k + 1)].squeeze(2) for k in range(len(records))]

                # Different spectral windows can have different numbers of
                # channels, e.g., TDM vs FDM. NumPy doesn't support jagged
                # arrays, so the code below ensures the data are uniformly
                # sized. Spectral windows with fewer channels are coerced to
                # the same size as the most detailed windows by adding masked
                # values to the 'end' of the data for that row.

                # Defines required envelope of dimensions, with the number of
                # rows (unused), number of polarisations, maximum number of
                # channels
                _, x_dim, y_dim = data[c].shape
                column_dtype = data[c].dtype

                coerced_rows = []
                for row in row_data:
                    data_channels = numpy.ma.masked_array(data=row, dtype=column_dtype)
                    row_x_dim, row_y_dim = row.shape
                    fake_channels = numpy.ma.masked_all((x_dim, y_dim-row_y_dim), dtype=column_dtype)
                    coerced_rows.append(numpy.ma.hstack((data_channels, fake_channels)))
                data[c] = numpy.ma.masked_array(data=coerced_rows, dtype=column_dtype)

            table_keywords = tb.getkeywords()
            column_keywords = {c: tb.getcolkeywords(c) for c in colnames}

        # convert to NumPy MaskedArray if FLAG column is present
        if 'FLAG' in readable_var_cols:
            mask = data['FLAG']
            var_cols_to_mask = [c for c in readable_var_cols if c != 'FLAG']
            for c in var_cols_to_mask:
                data[c] = numpy.ma.MaskedArray(data=data[c], mask=mask)

        return CaltableWrapper(path, data, table_keywords, column_keywords)


# maps CASA data types to their numpy equivalent. This dict is used by the
# get_dtype function below.
CASA_DATA_TYPES = {
    'int': numpy.int32,
    'boolean': bool,
    'float': numpy.float64,
    'double': numpy.float64,
    'complex': numpy.complex128
}


def get_dtype(tb, col):
    """
    Get the numpy data type for a CASA caltable column.

    :param tb: CASA table tool with caltable open.
    :param col: name of column to process
    :return: 3-tuple of column name, NumPy dtype, column shape
    """
    col_dtype = tb.coldatatype(col)

    if tb.isscalarcol(col):
        return col, CASA_DATA_TYPES[col_dtype]

    elif tb.isvarcol(col):
        # PIPE-1323: pre-check if the first row of this column is an empty cell, a scenario in which
        # the RuntimeError exception and a CASA 'SEVERE' message will be triggered.
        if not tb.iscelldefined(col, 0):
            return None
        try:
            shapes_string = tb.getcolshapestring(col)
        except RuntimeError:
            return None
        else:
            # spectral windows can have different shapes, e.g., TDM vs FDM,
            # therefore the shape needs to be a list of shapes.
            shapes = [ast.literal_eval(s) for s in shapes_string]

            x_dimensions = {shape[0] for shape in shapes}
            assert(len(x_dimensions) == 1)

            # find the maximum dimensions of a row
            max_row_shape = max(shapes)

            return col, CASA_DATA_TYPES[col_dtype], max_row_shape


class CaltableWrapper(object):
    def __init__(self, filename, data, table_keywords, column_keywords):
        self.filename = filename
        self.data = data
        self.table_keywords = table_keywords
        self.column_keywords = column_keywords

    def __getitem__(self, key):
        return self.data[key]

    def __contains__(self, key):
        return key in self.data.dtype.names

    def __len__(self):
        return len(self.data)

    def __iter__(self):
        return (i for i in self.data)

    def _get_mask(self, allowed, column):
        try:
            iter(allowed)
        except TypeError:
            allowed = [allowed]
        mask = numpy.zeros(len(self))
        for a in allowed:
            if a not in self.data[column]:
                raise KeyError('{} column {} value not found: {}'.format(self.filename, column, a))
            mask = (mask == 1) | (self[column] == a)
        return mask

    def filter(self, spw=None, antenna=None, scan=None, field=None, **kwargs):
        mask_args = dict(kwargs)

        # create a mask that lets all data through for columns that are not
        # specified as arguments, or just the specified values through for
        # columns that are specified as arguments
        def passthrough(k, column_name):
            if k is None:
                if column_name not in kwargs:
                    mask_args[column_name] = numpy.unique(self[column_name])
            else:
                mask_args[column_name] = k

        for arg, column_name in [(spw, 'SPECTRAL_WINDOW_ID'), (antenna, 'ANTENNA1'), (field, 'FIELD_ID'),
                                 (scan, 'SCAN_NUMBER')]:
            passthrough(arg, column_name)

        # combine masks to create final data selection mask
        mask = numpy.ones(len(self))
        for k, v in mask_args.items():
            mask = (mask == 1) & (self._get_mask(v, k) == 1)

        # find data for the selection mask
        data = self[mask]

        # create new object for the filtered data
        return CaltableWrapper(self.filename, data, self.table_keywords, self.column_keywords)
