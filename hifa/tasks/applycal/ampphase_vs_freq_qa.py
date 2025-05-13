import collections
import copy
import functools
import operator
import warnings
from typing import List

import numpy as np
import scipy.optimize

import pipeline.infrastructure.logging as logging
from . import mswrapper


MEMORY_CHUNK_SIZE = 1  # Size of the memory chunk when loading the MS (in GB)
LOG = logging.get_logger(__name__)


# AntennaFit is used to associate ant/pol metadata with the amp/phase best fits
AntennaFit = collections.namedtuple(
    'AntennaFit',
    ['ant', 'pol', 'amp', 'phase']
)
# LinearFitParameters is a struct to hold best fit parameters for a linear model
LinearFitParameters = collections.namedtuple(
    'LinearFitParameters',
    ['slope', 'intercept']
)
# Outlier describes an outlier data selection with why it's an outlier, and by how much
Outlier = collections.namedtuple(
    'Outlier',
    ['vis', 'intent', 'scan', 'spw', 'ant', 'pol', 'num_sigma', 'phase_offset_gt90deg', 'reason']
)
# ValueAndUncertainty is a simple 2-tuple to hold a value and the uncertainty in that value
ValueAndUncertainty = collections.namedtuple(
    'ValueAndUncertainty',
    ['value', 'unc']
)


# nsigma thresholds for marking deviating fits as an outlier
AMPLITUDE_SLOPE_THRESHOLD = 25.0
AMPLITUDE_INTERCEPT_THRESHOLD = 53.0
PHASE_SLOPE_THRESHOLD = 40.0
PHASE_INTERCEPT_THRESHOLD = 60.5


def score_all_scans(ms, intent: str, flag_all: bool = False, memory_gb: int = MEMORY_CHUNK_SIZE) -> List[Outlier]:
    """
    Calculate best fits for amplitude vs frequency and phase vs frequency
    for time-averaged visibilities, score each fit by comparison against a
    reference value, and return outliers.

    Outliers are returned as a list of Outlier objects.

    By default, outliers are measured against a PWG-defined threshold for
    each fit type. Set flag_all to True to make classify all fits as outliers.
    This is useful when testing QA plugin score roll-up functionality.

    :param ms: MeasurementSet to process
    :param intent: data intent to process
    :param memory_gb: maximum chunk size (in GB) used when loading the MeasurementSet
    :param flag_all: (optional) True if all fits should be classified as
        outliers
    :return: outliers that deviate from a reference fit
    """
    outliers = []
    wrappers = {}
    scans = sorted(ms.get_scans(scan_intent=intent), key=operator.attrgetter('id'))
    for scan in scans:
        spws = sorted([spw for spw in scan.spws if spw.type in ('FDM', 'TDM')],
                      key=operator.attrgetter('id'))
        for spw in spws:
            LOG.info('Applycal QA analysis: processing {} scan {} spw {}'.format(ms.basename, scan.id, spw.id))

            wrapper = mswrapper.MSWrapper.create_averages_from_ms(ms.name, scan.id, spw.id, memory_gb)
            if spw.id not in wrappers:
                wrappers[spw.id] = []
            wrappers[spw.id].append(wrapper)

            fits = get_best_fits_per_ant(wrapper)

            outlier_fn = functools.partial(
                Outlier,
                vis={ms.basename, },
                intent={intent, },
                spw={spw.id, },
                scan={scan.id, }
            )

            outliers.extend(score_all(fits, outlier_fn, flag_all))

    # Score all scans for a given spw
    for spw_id in wrappers.keys():
        if len(wrappers[spw_id]) > 1:
            LOG.info('Applycal QA analysis: processing {} scan average spw {}'.format(ms.basename, spw_id))
            # Average wrappers
            average_wrapper = mswrapper.MSWrapper.create_averages_from_combination(wrappers[spw_id])
            average_fits = get_best_fits_per_ant(average_wrapper)
            outlier_fn = functools.partial(
                Outlier,
                vis={ms.basename, },
                intent={intent, },
                spw={spw_id, },
                scan={-1, }
            )

            # Score average
            outliers.extend(score_all(average_fits, outlier_fn, flag_all))
        else:
            LOG.info('Applycal QA analysis: skipping {} scan average spw {} due to single scan'.format(ms.basename, spw_id))

    return outliers


def get_best_fits_per_ant(wrapper):
    """
    Calculate and return the best amp/phase vs freq fits for data in the input
    MSWrapper.

    This function calculates an independent best fit per polarisation per
    antenna, returning a list of AntennaFit objects that characterise the fit
    parameters and fit uncertainties per fit.

    :param wrapper: MSWrapper to process
    :return: a list of AntennaFit objects
    """
    V_k = wrapper.V

    corrected_data = V_k['corrected_data']
    sigma = V_k['sigma']

    num_antennas, _, num_chans = corrected_data.shape
    # Filter cross-pol data
    pol_indices = tuple(np.where((wrapper.corr_axis=='XX') | (wrapper.corr_axis=='YY'))[0])

    all_fits = []

    for ant in range(num_antennas):
        frequencies = V_k['chan_freq'][ant]

        bandwidth = np.ma.max(frequencies) - np.ma.min(frequencies)
        band_midpoint = (np.ma.max(frequencies) + np.ma.min(frequencies)) / 2.0
        frequency_scale = 1.0 / bandwidth

        amp_model_fn = get_linear_function(band_midpoint, frequency_scale)
        ang_model_fn = get_angular_linear_function(band_midpoint, frequency_scale)

        for pol in pol_indices:
            visibilities = corrected_data[ant, pol, :]
            ta_sigma = sigma[ant, pol, :]

            if visibilities.count() == 0:
                LOG.info('Could not fit ant {} pol {}: data is completely flagged'.format(ant, pol))
                continue

            # PIPE-884: as of NumPy ver1.20, ma.abs() doesn't convert MaskedArray fill_value to float automatically. This 
            # introduces a casting-related ComplexWarning when the result is passed to ma.median(). We mitigate the warning 
            # by using the real part of filled value explicitly. See also below.
            median_sn = np.ma.median(np.ma.abs(visibilities).real / np.ma.abs(ta_sigma).real)
            if median_sn > 3:  # PIPE-401: Check S/N and either fit or use average
                # Fit the amplitude
                try:
                    amp_fit, amp_err = get_amp_fit(amp_model_fn, frequencies, visibilities, ta_sigma)
                    amplitude_fit = to_linear_fit_parameters(amp_fit, amp_err)
                except TypeError:
                    # Antenna probably flagged..
                    LOG.info('Could not fit phase vs frequency for ant {} pol {} (high S/N; amp. vs frequency)'.format(
                        ant, pol))
                    continue
                # Fit the phase
                try:
                    phase_fit, phase_err = get_phase_fit(amp_model_fn, ang_model_fn, frequencies, visibilities, ta_sigma)
                    phase_fit = to_linear_fit_parameters(phase_fit, phase_err)
                except TypeError:
                    # Antenna probably flagged..
                    LOG.info('Could not fit phase vs frequency for ant {} pol {} (high S/N; phase vs frequency)'.format(
                        ant, pol))
                    continue
            else:
                LOG.debug('Low S/N for ant {} pol {}'.format(ant, pol))
                # 'Fit' the amplitude
                try:  # NOTE: PIPE-401 This try block may not be necessary
                    amp_vis = np.ma.abs(visibilities).real
                    n_channels_unmasked = np.sum(~amp_vis.mask)
                    if n_channels_unmasked != 0:
                        amplitude_fit = LinearFitParameters(
                            slope=ValueAndUncertainty(value=0., unc=1.0e06),
                            intercept=ValueAndUncertainty(
                                value=np.ma.median(amp_vis),
                                unc=np.ma.std(amp_vis)/np.sqrt(n_channels_unmasked))
                        )
                    else:
                        LOG.info(
                            'Could not fit phase vs frequency for ant {} pol {} (low S/N; amp. vs frequency)'.format(
                                ant, pol))
                        continue
                except TypeError:
                    # Antenna probably flagged..
                    LOG.info(
                        'Could not fit phase vs frequency for ant {} pol {} (low S/N; amp. vs frequency)'.format(
                            ant, pol))
                    continue
                # 'Fit' the phase
                try:  # NOTE: PIPE-401 This try block may not be necessary
                    phase_vis = np.ma.angle(visibilities).real
                    n_channels_unmasked = np.sum(~phase_vis.mask)
                    if n_channels_unmasked != 0:
                        phase_fit = LinearFitParameters(
                            slope=ValueAndUncertainty(value=0., unc=1.0e06),
                            intercept=ValueAndUncertainty(
                                value=np.ma.median(phase_vis),
                                unc=np.ma.std(phase_vis)/np.sqrt(n_channels_unmasked)
                            )
                        )
                    else:
                        LOG.info(
                            'Could not fit phase vs frequency for ant {} pol {} (low S/N; phase vs frequency)'.format(
                                ant, pol))
                        continue
                except TypeError:
                    # Antenna probably flagged..
                    LOG.info('Could not fit phase vs frequency for ant {} pol {} (low S/N; phase vs frequency)'.format(
                        ant, pol))
                    continue

            fit_obj = AntennaFit(ant=ant, pol=pol, amp=amplitude_fit, phase=phase_fit)
            all_fits.append(fit_obj)

    return all_fits


def score_all(all_fits, outlier_fn, flag_all: bool = False):
    """
    Compare and score the calculated best fits based on how they deviate from
    a reference value.

    For all amplitude or slope vs frequency fits, score the slope or intercept
    of the fit against the slope or intercept of the median best fit or a value
    of 0, marking fits that deviate by sigma_threshold from the median dispersion
    as outliers. Identified outliers are returned as a list of Outlier object
    returned by the outlier_fn.

    The outlier_fn argument should be a function that returns Outlier objects.
    In practice, this function should be a partially-applied Outlier
    constructor that requires a more required arguments to be
    supplied for an Outlier instance to be created.

    Setting the test argument flag_all to True sets all fits as outliers. This
    is useful for testing the QA score roll-up and summary functions in the QA
    plugin.

    :param all_fits: list of all AntennaFit best fit parameters for all metrics
    :param outlier_fn: a function returning Outlier objects
    :param flag_all: True if all fits should be classed as outliers
    :return: list of Outlier objects
    """
    # Dictionary for the different cases to consider. Each is defined by a tuple
    #  with: attr, ref_value_fn, sigma_threshold
    score_definitions = {
        "amp_slope": ('amp.slope', get_median_fit, AMPLITUDE_SLOPE_THRESHOLD),
        "amp_intercept": ('amp.intercept', get_median_fit, AMPLITUDE_INTERCEPT_THRESHOLD),
        "phase_slope": ('phase.slope', PHASE_REF_FN, PHASE_SLOPE_THRESHOLD),
        "phase_intercept": ('phase.intercept', PHASE_REF_FN, PHASE_INTERCEPT_THRESHOLD),
    }

    outliers = []
    for k, v in score_definitions.items():
        threshold = 0.0 if flag_all else v[2]
        scores = score_X_vs_freq_fits(all_fits, v[0], v[1], outlier_fn, threshold)
        outliers.extend(scores)

    return outliers


def get_median_fit(all_fits, accessor):
    """
    Get the median best fit from a list of best fits.

    The accessor argument should be a function that, when given the list of
    all fits, returns fits of the desired type (amp.slope, phase.intercept,
    etc.)

    :param all_fits: mixed list of best fits
    :type: list of AntennaFit instances
    :param accessor: function to filter best fits
    :return: median value and uncertainty of fits of selected type
    """
    pol_slopes = [accessor(f) for f in all_fits]
    values = [f.value for f in pol_slopes]
    median, median_sigma = robust_stats(values)
    return ValueAndUncertainty(value=median, unc=median_sigma)


def score_X_vs_freq_fits(all_fits, attr, ref_value_fn, outlier_fn, sigma_threshold):
    """
    Score a set of best fits, comparing the fits identified by the 'attr'
    attribute against a reference value calculated by the ref_value_fn,
    marking outliers that deviate by more than sigma_threshold from this
    reference value as outliers, to be returned as Outlier objects created by
    the outlier_fn.

    :param all_fits: a list of fit parameters
    :param attr: identifier of the fits to consider, e.g., 'amp.slope'
    :param ref_value_fn: a function that takes a list of fits and returns a
        value to be used as a reference value in fit comparisons
    :param outlier_fn: a function returning Outlier objects
    :param sigma_threshold: the nsigma threshold to be considered an outlier
    :return: list of Outlier objects
    """
    # convert linear fit metadata to a reason that identifies this fit as
    # originating from this metric in a wider context, e.g., from 'amp.slope'
    # to 'amp_vs_freq.slope'
    y_axis, fit_parameter = attr.split('.')
    reason = f'{y_axis}_vs_freq.{fit_parameter}'
    outlier_fn = functools.partial(outlier_fn, reason={reason, })

    accessor = operator.attrgetter(attr)
    outliers = score_fits(all_fits, ref_value_fn, accessor, outlier_fn, sigma_threshold)

    # Check for >90deg phase offsets which should have extra QA messages
    if y_axis == 'phase':
        for i in range(len(outliers)):
            if outliers[i].phase_offset_gt90deg:
                outliers[i] = Outlier(vis=outliers[i].vis,
                                      intent=outliers[i].intent,
                                      scan=outliers[i].scan,
                                      spw=outliers[i].spw,
                                      ant=outliers[i].ant,
                                      pol=outliers[i].pol,
                                      num_sigma=outliers[i].num_sigma,
                                      phase_offset_gt90deg=outliers[i].phase_offset_gt90deg,
                                      reason={f'gt90deg_offset_{y_axis}_vs_freq.{fit_parameter}', })
    return outliers


def score_fits(all_fits, reference_value_fn, accessor, outlier_fn, sigma_threshold):
    """
    Score a list of best fit parameters against a reference value, identifying
    outliers as fits that deviate by more than sigma_threshold * std dev from
    the reference value.

    :param all_fits: list of AntennaFits
    :param reference_value_fn: function that returns a reference
        ValueAndUncertainty from a list of these objects
    :param accessor: function that returns one LinearFitParameters from an
        AntennaFit
    :param outlier_fn: function that returns an Outlier instance
    :param sigma_threshold: threshold nsigma deviation for comparisons
    :return: list of Outliers
    """
    median_cor_factor = np.sqrt(np.pi / 2.)  # PIPE-401: factor due to using the median instead of the mean
    outliers = []

    pols = {f.pol for f in all_fits}
    for pol in pols:
        pol_fits = [f for f in all_fits if f.pol == pol]
        n_antennas = len(pol_fits)

        # get reference val. Usually median, could be zero for phase
        reference_val, sigma_sample = reference_value_fn(pol_fits, accessor)
        reference_sigma = median_cor_factor * sigma_sample / np.sqrt(n_antennas)

        for fit in pol_fits:
            ant = fit.ant
            unc = accessor(fit).unc
            value = accessor(fit).value
            this_sigma = np.sqrt(reference_sigma ** 2 + unc ** 2)
            num_sigma = np.abs((value - reference_val) / this_sigma)
            if num_sigma > sigma_threshold:
                outlier = outlier_fn(ant={ant, }, pol={pol, }, num_sigma=num_sigma, phase_offset_gt90deg=abs(fit.phase.intercept.value) > 0.5 * np.pi)
                outliers.append(outlier)

    return outliers


def to_linear_fit_parameters(fit, err):
    """
    Convert tuples from the best fit evaluation into a LinearFitParameters
    namedtuple.

    :param fit: 2-tuple of (slope, intercept) best-fit parameters
    :param err: 2-tuple of uncertainty in (slope, intercept) parameters
    :return:
    """
    return LinearFitParameters(slope=ValueAndUncertainty(value=fit[0], unc=err[0]),
                               intercept=ValueAndUncertainty(value=fit[1], unc=err[1]))


def get_amp_fit(amp_model_fn, frequencies, visibilities, sigma):
    """
    Fit a linear amplitude vs frequency model to a set of time-averaged
    visibilities.

    :param amp_model_fn: the amplitude linear model to optimise
    :param frequencies: numpy array of channel frequencies
    :param visibilities: numpy array of time-averaged visibilities
    :param sigma: numpy array of uncertainies in time-averaged visibilities
    :return: tuple of best fit params, uncertainty tuple
    """
    # calculate amplitude and phase from visibility, inc. std. deviations for each
    amp = np.ma.abs(visibilities)
    # angle of complex argument, in radians
    sigma_amp = np.ma.sqrt((visibilities.real * sigma.real) ** 2 + (visibilities.imag * sigma.imag) ** 2) / amp
    sigma_phase = np.ma.sqrt((visibilities.imag * sigma.real) ** 2 + (visibilities.real * sigma.imag) ** 2) / (
            amp ** 2)

    # curve_fit doesn't handle MaskedArrays, so mask out all bad data and
    # convert to standard NumPy arrays
    mask = np.ma.all([amp.mask, sigma_amp <= 0, sigma_phase <= 0], axis=0)
    trimmed_frequencies = frequencies[~mask]
    trimmed_amp = amp.data[~mask]
    trimmed_sigma_amp = sigma_amp.data[~mask]

    Cinit = np.ma.median(trimmed_amp)

    amp_fit, amp_cov = scipy.optimize.curve_fit(amp_model_fn, trimmed_frequencies, trimmed_amp,
                                                p0=[0.0, Cinit], sigma=trimmed_sigma_amp, absolute_sigma=True)

    amp_err = np.sqrt(np.diag(amp_cov))

    return amp_fit, amp_err


def get_phase_fit(amp_model_fn, ang_model_fn, frequencies, visibilities, sigma):
    """
    Fit a linear model for phase vs frequency to a set of time-averaged
    visibilities.

    :param amp_model_fn: model function for amplitude
    :param ang_model_fn: model function for phase angle
    :param frequencies: numpy array of channel frequencies
    :param visibilities: numpy array of time-averaged visibilities
    :param sigma: numpy array of uncertainies in time-averaged visibilities
    :return: tuple of best fit params, uncertainty tuple
    """
    # calculate amplitude and phase from visibility, inc. std. deviations for each
    amp = np.ma.abs(visibilities)
    phase = np.ma.angle(visibilities)

    zeroamp = (amp.data <= 0.0)
    amp.mask[zeroamp] = True
    phase.mask[zeroamp] = True

    sigma_amp = np.ma.sqrt((visibilities.real * sigma.real) ** 2 + (
            visibilities.imag * sigma.imag) ** 2) / amp
    sigma_phase = np.ma.sqrt((visibilities.imag * sigma.real) ** 2 + (
            visibilities.real * sigma.imag) ** 2) / (amp ** 2)

    # curve_fit doesn't handle MaskedArrays, so mask out all bad data and
    # convert to standard NumPy arrays
    mask = np.ma.all([amp.mask, sigma_amp <= 0, sigma_phase <= 0], axis=0)
    trimmed_frequencies = frequencies[~mask]
    trimmed_phase = phase.data[~mask]
    trimmed_sigma_phase = sigma_phase.data[~mask]

    phi_init = np.ma.median(trimmed_phase)

    # normalise visibilities by amplitude to fit linear angular model
    normalised_visibilities = np.ma.divide(visibilities, amp)
    normalised_sigma = np.ma.divide(sigma, amp)

    ang_fit_res = fit_angular_model(ang_model_fn, frequencies, normalised_visibilities, normalised_sigma)

    # Detrend phases using fit
    detrend_model = ang_model_fn(frequencies, -ang_fit_res['x'][0], -ang_fit_res['x'][1])
    detrend_data = normalised_visibilities * detrend_model
    detrend_phase = np.ma.angle(detrend_data)[~mask]

    # Refit phases to obtain errors from the same curve_fit method
    zerophasefit, phasecov = scipy.optimize.curve_fit(amp_model_fn, trimmed_frequencies, detrend_phase,
                                                      p0=[0.0, phi_init - ang_fit_res['x'][1]],
                                                      sigma=trimmed_sigma_phase, absolute_sigma=True)
    # Final result is detrending model + new fit (close to zero)
    phase_fit = ang_fit_res['x'] + zerophasefit

    phase_err = np.sqrt(np.diag(phasecov))

    return phase_fit, phase_err


def get_linear_function(midpoint, x_scale):
    """
    Return a scaled linear function (a function of slope and intercept).

    :param midpoint:
    :param x_scale:
    :return:
    """

    def f(x, slope, intercept):
        return np.ma.multiply(
            np.ma.multiply(slope, np.ma.subtract(x, midpoint)),
            x_scale
        ) + intercept

    return f


def get_angular_linear_function(midpoint, x_scale):
    """
    Angular linear model to fit phases only.

    :param midpoint:
    :param x_scale:
    :return:
    """
    linear_fn = get_linear_function(midpoint, x_scale)

    def f(x, slope, intercept):
        return np.ma.exp(1j * linear_fn(x, slope, intercept))

    return f


def get_chi2_ang_model(angular_model, nu, omega, phi, angdata, angsigma):
    m = angular_model(nu, omega, phi)
    diff = angdata - m
    aux = (np.square(diff.real / angsigma.real) + np.square(diff.imag / angsigma.imag))[~angdata.mask]
    return float(np.sum(aux.real))


def fit_angular_model(angular_model, nu, angdata, angsigma):
    f_aux = lambda omega_phi: get_chi2_ang_model(angular_model, nu, omega_phi[0], omega_phi[1], angdata, angsigma)
    angle = np.ma.angle(angdata[~angdata.mask])
    with warnings.catch_warnings():
        warnings.simplefilter('ignore', np.ComplexWarning)
        phi_init = np.ma.median(angle)
    fitres = scipy.optimize.minimize(f_aux, np.array([0.0, phi_init]), method='L-BFGS-B')
    return fitres


def robust_stats(a):
    """
    Return median and estimate standard deviation
    of numpy array A using median statistics

    :param a:
    :return:
    """
    # correction factor for obtaining sigma from MAD
    madfactor = 1.482602218505602

    n = len(a)
    # Fitting parameters for sample size correction factor b(n)
    if n % 2 == 0:
        alpha = 1.32
        beta = -1.5
    else:
        alpha = 1.32
        beta = -0.9
    bn = 1.0 - 1.0 / (alpha * n + beta)
    mu = np.median(a)
    sigma = (1.0 / bn) * madfactor * np.median(np.abs(a - mu))
    return mu, sigma


def consolidate_data_selections(outliers):
    """
    Consolidate a list of Outliers into a smaller set of equivalent Outliers
    by consolidating their data selection arguments.

    This function works by merging Outliers that have the same list of
    reasons.

    :param outliers: an iterable of Outliers
    :return: an equivalent consolidated list of Outliers
    """
    # dict mapping an reason hash to the reason itself:
    hash_to_reason = {}
    # dict mapping from object hash to corresponding list of Outliers
    reason_hash_to_outliers = collections.defaultdict(list)

    # create our maps of hashes, which we need to test for overlapping data
    # selections
    for outlier in outliers:
        # create a tuple, as lists are not hashable
        reason_hash = tuple([hash(reason) for reason in outlier.reason])
        reason_hash_to_outliers[reason_hash].append(outlier)

        if reason_hash not in hash_to_reason:
            hash_to_reason[reason_hash] = outlier.reason

    # dict that maps holds accepted data selections and their reasons
    accepted = {}
    for reason_hash, outliers in reason_hash_to_outliers.items():
        # assemble the other outliers which we will compare for conflicts
        other_outliers = []
        for v in [v for k, v in reason_hash_to_outliers.items() if k != reason_hash]:
            other_outliers.extend(v)

        for outlier_to_merge in outliers:
            if reason_hash not in accepted:
                # first time round for this outlier, therefore it can always
                # be added as there will be nothing to merge
                accepted[reason_hash] = [copy.deepcopy(outlier_to_merge)]
                continue

            for existing_outlier in accepted[reason_hash]:
                proposed_outlier = copy.deepcopy(existing_outlier)

                proposed_outlier.vis.update(outlier_to_merge.vis)
                proposed_outlier.intent.update(outlier_to_merge.intent)
                proposed_outlier.spw.update(outlier_to_merge.spw)
                proposed_outlier.scan.update(outlier_to_merge.scan)
                proposed_outlier.ant.update(outlier_to_merge.ant)
                proposed_outlier.pol.update(outlier_to_merge.pol)

                # if the merged outlier does not conflict with any of the
                # explicitly registered outliers that require a different
                # reason, then it is safe to add the merged outlier and
                # discard the unmerged data selection
                if not any((data_selection_contains(proposed_outlier, other) for other in other_outliers)):
                    LOG.trace('No conflicting outlier detected')
                    LOG.trace('Accepting merged outlier: {!s}'.format(proposed_outlier))
                    LOG.trace('Discarding unmerged outlier: {!s}'.format(outlier_to_merge))
                    accepted[reason_hash] = [proposed_outlier]
                    break

            else:
                # we get here if all of the proposed outliers conflict with
                # the outlier in hand. In this case, it should be added as it
                # stands, completely unaltered.
                LOG.trace('Merged outlier conflicts with other registrations')
                LOG.trace('Abandoning proposed outlier: {!s}'.format(proposed_outlier))
                LOG.trace('Appending new unmerged outlier: {!s}'.format(outlier_to_merge))
                unmergeable = outlier_to_merge
                accepted[reason_hash].append(unmergeable)

    # dict values are lists, which we need to flatten into a single list
    result = []
    for l in accepted.values():
        result.extend(l)
    return result


def data_selection_contains(proposed, ds_args):
    """
    Return True if one data selection is contained within another.

    :param proposed: data selection 1
    :param ds_args: data selection 2
    :return: True if data selection 2 is contained within data selection 1
    """
    return all([not proposed.vis.isdisjoint(ds_args.vis),
                not proposed.intent.isdisjoint(ds_args.intent),
                not proposed.scan.isdisjoint(ds_args.scan),
                not proposed.spw.isdisjoint(ds_args.spw),
                not proposed.ant.isdisjoint(ds_args.ant),
                not proposed.pol.isdisjoint(ds_args.pol)])


# The function used to create the reference value for phase vs frequency best fits
# Select this to compare fits against the median
# PHASE_REF_FN = get_median_fit
# Select this to compare fits against zero
PHASE_REF_FN = lambda _, __: ValueAndUncertainty(0, 0)
