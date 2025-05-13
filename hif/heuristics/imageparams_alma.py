import re
from typing import List, Optional, Union

import numpy as np

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.utils as utils
from pipeline.infrastructure import casa_tools

from .imageparams_base import ImageParamsHeuristics

LOG = infrastructure.get_logger(__name__)


class ImageParamsHeuristicsALMA(ImageParamsHeuristics):

    def __init__(self, vislist, spw, observing_run, imagename_prefix='', proj_params=None, contfile=None,
                 linesfile=None, imaging_params={}):
        ImageParamsHeuristics.__init__(self, vislist, spw, observing_run, imagename_prefix, proj_params, contfile,
                                       linesfile, imaging_params)
        self.imaging_mode = 'ALMA'

    def robust(self, specmode=None):
        """robust parameter heuristic."""
        if 'robust' in self.imaging_params:
            robust = self.imaging_params['robust']
            LOG.info('ALMA robust heuristics: Using imageprecheck value of robust=%.1f' % robust)
            return robust
        else:
            return 0.5

    def uvtaper(self, beam_natural=None, protect_long=3, beam_user=None, tapering_limit=None, repr_freq=None):
        """Adjustment of uvtaper parameter based on desired resolution or representative baseline length."""

        # Disabled heuristic for ALMA Cycle 6
        return []

        if 'uvtaper' in self.imaging_params:
            uvtaper = self.imaging_params['uvtaper']
            LOG.info('ALMA uvtaper heuristics: Using imageprecheck value of uvtaper=%s' % (str(uvtaper)))
            return uvtaper

        if (beam_natural is None) and (protect_long is None):
            return []

        cqa = casa_tools.quanta

        repr_target, repr_source, repr_spw, repr_freq, reprBW_mode, real_repr_target, minAcceptableAngResolution, maxAcceptableAngResolution, maxAllowedBeamAxialRatio, sensitivityGoal = self.representative_target()

        # Protection against spurious long baselines
        if protect_long is not None:
            l80, min_diameter = self.calc_percentile_baseline_length(80.)
            LOG.info('ALMA uvtaper heuristic: L80 baseline length is %.1f meter' % (l80))

            c = cqa.getvalue(cqa.convert(cqa.constants('c'), 'm/s'))[0]
            uvtaper_value = protect_long * l80 / cqa.getvalue(cqa.convert(cqa.constants('c'), 'm/s'))[0] * cqa.getvalue(cqa.convert(repr_freq, 'Hz'))[0]
            uvtaper = ['%.2fklambda' % utils.round_half_up(uvtaper_value/1000., 2)]

            return uvtaper
        else:
            return []

        # # Original Cycle 5 heuristic follows below for possible later use.
        # if not real_repr_target:
        #     LOG.info('ALMA uvtaper heuristic: No representative target found. Using uvtaper=[]')
        #     return []
        #
        # try:
        #     beam_natural_v = math.sqrt(cqa.getvalue(cqa.convert(beam_natural['major'], 'arcsec')) * cqa.getvalue(cqa.convert(beam_natural['minor'], 'arcsec')))
        # except Exception as e:
        #     LOG.error('ALMA uvtaper heuristic: Cannot get natural beam size: %s. Using uvtaper=[]' % (e))
        #     return []
        #
        # minAR_v = cqa.getvalue(cqa.convert(minAcceptableAngResolution, 'arcsec'))
        # maxAR_v = cqa.getvalue(cqa.convert(maxAcceptableAngResolution, 'arcsec'))
        #
        # if beam_natural_v < 1.1 * maxAR_v:
        #     beam_taper = math.sqrt(maxAR_v ** 2 - beam_natural_v ** 2)
        #     uvtaper = ['%.3garcsec' % (beam_taper)]
        # else:
        #     uvtaper = []
        #
        # return uvtaper

    def dr_correction(self, threshold, dirty_dynamic_range, residual_max, intent, tlimit, drcorrect):
        """Adjustment of cleaning threshold due to dynamic range limitations."""

        qaTool = casa_tools.quanta
        maxEDR_used = False
        DR_correction_factor = 1.0

        diameter = self.observing_run.get_measurement_sets()[0].antennas[0].diameter
        old_threshold = qaTool.convert(threshold, 'Jy')['value']

        if drcorrect not in (None, -999):
            if isinstance(drcorrect, (float, int)) and drcorrect > 0.0:
                new_threshold = old_threshold*drcorrect
                DR_correction_factor = drcorrect
                LOG.info('DR correction: Modified threshold from {:.3g} Jy to {:.3g} Jy based on the user input correction factor: {}'.format(
                    old_threshold, new_threshold, DR_correction_factor))
                return '%.3gJy' % (new_threshold), DR_correction_factor, maxEDR_used
            else:
                raise Exception(f'Got an invalid input value for the DR correction factor: {drcorrect}')

        if intent == 'TARGET' or intent == 'CHECK':
            n_dr_max = 2.5
            if diameter == 12.0:
                if dirty_dynamic_range > 150.:
                    maxSciEDR = 150.0
                    new_threshold = max(n_dr_max * old_threshold, residual_max / maxSciEDR * tlimit)
                    LOG.info('DR heuristic: Applying maxSciEDR(Main array)=%s' % maxSciEDR)
                    maxEDR_used = True
                else:
                    if dirty_dynamic_range > 100.:
                        n_dr = 2.5
                    elif 50. < dirty_dynamic_range <= 100.:
                        n_dr = 2.0
                    elif 20. < dirty_dynamic_range <= 50.:
                        n_dr = 1.5
                    elif dirty_dynamic_range <= 20.:
                        n_dr = 1.0
                    LOG.info('DR heuristic: N_DR=%s' % n_dr)
                    new_threshold = old_threshold * n_dr
            else:
                numberEBs = len(self.vislist)
                if numberEBs == 1:
                    # single-EB 7m array datasets have limited dynamic range
                    maxSciEDR = 30
                    dirtyDRthreshold = 30
                    n_dr_max = 2.5
                else:
                    # multi-EB 7m array datasets will have better dynamic range and can be cleaned somewhat deeper
                    maxSciEDR = 55
                    dirtyDRthreshold = 75
                    n_dr_max = 3.5

                if dirty_dynamic_range > dirtyDRthreshold:
                    new_threshold = max(n_dr_max * old_threshold, residual_max / maxSciEDR * tlimit)
                    n_dr_effective = new_threshold / old_threshold
                    LOG.info('DR heuristic: Applying maxSciEDR(ACA)=%s (for %d EB) effective_N_DR=%.2f' %
                             (maxSciEDR, numberEBs, n_dr_effective))
                    maxEDR_used = True
                else:
                    if dirty_dynamic_range > 40.:
                        n_dr = 3.0
                    elif dirty_dynamic_range > 20.:
                        n_dr = 2.5
                    elif 10. < dirty_dynamic_range <= 20.:
                        n_dr = 2.0
                    elif 4. < dirty_dynamic_range <= 10.:
                        n_dr = 1.5
                    elif dirty_dynamic_range <= 4.:
                        n_dr = 1.0
                    LOG.info('DR heuristic: N_DR=%s' % n_dr)
                    new_threshold = old_threshold * n_dr
        else:
            # Calibrators are usually dynamic range limited. The sensitivity from apparentsens
            # is not a valid estimate for the threshold. Use a heuristic based on the dirty peak
            # and some maximum expected dynamic range (EDR) values.
            if diameter == 12.0:
                maxCalEDR = 1000.0
                veryHighCalEDR = 3000.0  # use shallower slope above this value
                LOG.info('DR heuristic: Applying maxCalEDR=%s, veryHighCalEDR=%s' % (maxCalEDR, veryHighCalEDR))
                matchPoint = veryHighCalEDR/maxCalEDR - 1  # will be 2 for 3000/1000
                highDR = tlimit * residual_max / maxCalEDR / old_threshold  # will use this value up to 3000
                veryHighDR = matchPoint + tlimit * residual_max / veryHighCalEDR / old_threshold  # will use this value above 3000
                n_dr = max(1.0, min(highDR, veryHighDR))
                LOG.info('DR heuristic: Calculating N_DR as max of (1.0, min of (%f, %f)) = %f'
                         '' % (highDR, veryHighDR, n_dr))
                new_threshold = old_threshold * n_dr
            else:
                maxCalEDR = 200.0
                LOG.info('DR heuristic: Applying maxCalEDR=%s' % maxCalEDR)
                new_threshold = max(old_threshold, residual_max / maxCalEDR * tlimit)

            if new_threshold != old_threshold:
                maxEDR_used = True

        if new_threshold != old_threshold:
            LOG.info('DR heuristic: Modified threshold from %.3g Jy to %.3g Jy based on dirty dynamic range calculated'
                     ' from dirty peak / final theoretical sensitivity: %.1f'
                     '' % (old_threshold, new_threshold, dirty_dynamic_range))
            DR_correction_factor = new_threshold / old_threshold

        return '%.3gJy' % (new_threshold), DR_correction_factor, maxEDR_used

    def niter_correction(self, niter, cell, imsize, residual_max, threshold, residual_robust_rms, mask_frac_rad=0.0, intent='TARGET'):
        """Adjustment of number of cleaning iterations due to mask size.

        See base class method for parameter description."""
        if mask_frac_rad == 0.0:
            mask_frac_rad = 0.45    # ALMA specific parameter

        new_niter = super().niter_correction(niter, cell, imsize, residual_max, threshold, residual_robust_rms,
                                             mask_frac_rad=mask_frac_rad, intent=intent)

        # Limit ALMA calibrator niter to 3000
        if intent != 'TARGET' and new_niter > 3000:
            LOG.info('niter heuristic: Modified niter from %d to 3000 due to calibrator intent'
                     '' % (new_niter))
            new_niter = 3000

        return new_niter

    def calc_length_of_nth_baseline(self, n: int):
        """Calculate the length of the nth baseline for the vis list used in the heuristics instance."""
        baseline_lengths = []
        for msname in self.vislist:
            ms_do = self.observing_run.get_ms(msname)
            baselines_m = ms_do.antenna_array.baselines_m
            if n > len(baselines_m):
                continue

            baseline_lengths.append(np.sort(baselines_m)[n-1])

        if len(baseline_lengths) > 0:
            return np.median(baseline_lengths)
        else:
            return None

    def get_autobox_params(
        self,
        iteration: int,
        intent: str,
        specmode: str,
        robust: float,
        rms_multiplier: Optional[Union[int, float]] = None,
    ) -> tuple:
        """Default auto-boxing parameters for ALMA main array and ACA."""

        # Start with generic defaults
        sidelobethreshold = None
        noisethreshold = None
        lownoisethreshold = None
        minbeamfrac = None
        growiterations = None
        dogrowprune = None
        minpercentchange = None

        repBaselineLength, min_diameter = self.calc_percentile_baseline_length(75.)
        LOG.info('autobox heuristic: Representative baseline length is %.1f meter' % repBaselineLength)

        baselineThreshold = 400

        # PIPE-307
        if min_diameter == 12.0 and repBaselineLength > baselineThreshold:
            fastnoise = True
        else:
            fastnoise = False

        if 'TARGET' in intent:
            if min_diameter == 12.0:
                if repBaselineLength < 300:
                    sidelobethreshold = 2.0
                    noisethreshold = 4.25
                    lownoisethreshold = 1.5
                    minbeamfrac = 0.3
                    dogrowprune = True
                    minpercentchange = 1.0

                    if specmode == 'cube':
                        negativethreshold = 15.0
                        growiterations = 50
                    else:
                        negativethreshold = 0.0
                        growiterations = 75
                else:
                    if repBaselineLength < baselineThreshold:
                        sidelobethreshold = 2.0
                    else:
                        sidelobethreshold = 2.5  # was 3.0
                    noisethreshold = 5.0
                    lownoisethreshold = 1.5
                    minbeamfrac = 0.3
                    dogrowprune = True
                    minpercentchange = 1.0
                    if specmode == 'cube':
                        negativethreshold = 7.0
                        growiterations = 50
                    else:
                        negativethreshold = 0.0
                        growiterations = 75
            elif min_diameter == 7.0:
                sidelobethreshold = 1.25
                noisethreshold = 5.0
                lownoisethreshold = 2.0
                minbeamfrac = 0.1
                growiterations = 75
                negativethreshold = 0.0
                dogrowprune = True
                minpercentchange = 1.0
        elif 'CHECK' in intent:
            if min_diameter == 12.0:
                if repBaselineLength < 300:
                    sidelobethreshold = 2.0
                    noisethreshold = 4.25
                    lownoisethreshold = 1.5
                    minbeamfrac = 0.3
                    dogrowprune = True
                    minpercentchange = 1.0

                    if specmode == 'cube':
                        negativethreshold = 15.0
                        growiterations = 50
                    else:
                        negativethreshold = 0.0
                        growiterations = 75
                else:
                    sidelobethreshold = 3.0
                    noisethreshold = 5.0
                    lownoisethreshold = 1.5
                    minbeamfrac = 0.3
                    dogrowprune = True
                    minpercentchange = 1.0
                    if specmode == 'cube':
                        negativethreshold = 7.0
                        growiterations = 50
                    else:
                        negativethreshold = 0.0
                        growiterations = 75
            elif min_diameter == 7.0:
                sidelobethreshold = 1.25
                noisethreshold = 5.0
                lownoisethreshold = 2.0
                minbeamfrac = 0.1
                growiterations = 75
                negativethreshold = 0.0
                dogrowprune = True
                minpercentchange = 1.0
        else:
            if min_diameter == 12.0:
                sidelobethreshold = 2.0
                noisethreshold = 7.0
                lownoisethreshold = 3.0
                minbeamfrac = 0.1
                growiterations = 75
                negativethreshold = 0.0
                dogrowprune = True
                minpercentchange = 1.0
            elif min_diameter == 7.0:
                sidelobethreshold = 1.5
                noisethreshold = 6.0
                lownoisethreshold = 2.0
                minbeamfrac = 0.1
                growiterations = 75
                negativethreshold = 0.0
                dogrowprune = True
                minpercentchange = 1.0

        return sidelobethreshold, noisethreshold, lownoisethreshold, negativethreshold, minbeamfrac, growiterations, dogrowprune, minpercentchange, fastnoise

    def warn_missing_cont_ranges(self):

        return True

    def nterms(self, spwspec):

        return 2

    def _tlimit_cyclefactor_heuristic(self, iteration, field=None, intent=None, specmode=None, iter0_dirty_dynamic_range=None):
        gridder = self.gridder(intent, field)

        times_on_source_per_field = []
        for vis in self.vislist:
            ms_do = self.observing_run.get_ms(vis)
            times_on_source_per_field.extend(ms_do.get_times_on_source_per_field_id(field, intent).values())

        if not times_on_source_per_field:
            return False

        min_time_on_source_per_field = min(times_on_source_per_field)

        if (gridder == 'mosaic'
            and specmode in ('cube', 'repBW')
            and min_time_on_source_per_field <= 60.0
            and iter0_dirty_dynamic_range >= 30.0
            and iteration > 0):
            return True
        else:
            return False

    def tlimit(self, iteration, field=None, intent=None, specmode=None, iter0_dirty_dynamic_range=None):
        if field is None or intent is None or specmode is None or iter0_dirty_dynamic_range is None:
            return 2.0
        if self._tlimit_cyclefactor_heuristic(iteration, field, intent, specmode, iter0_dirty_dynamic_range):
            return 5.0
        else:
            return 2.0

    def cyclefactor(self, iteration, field=None, intent=None, specmode=None, iter0_dirty_dynamic_range=None):
        if field is None or intent is None or specmode is None or iter0_dirty_dynamic_range is None:
            # Use CASA default
            return None
        if self._tlimit_cyclefactor_heuristic(iteration, field, intent, specmode, iter0_dirty_dynamic_range):
            return 3.0
        else:
            return None

    def mosweight(self, intent, field):

        if self.gridder(intent, field) == 'mosaic':
            return True
        else:
            return False

    def tclean_stopcode_ignore(self, iteration, hm_masking):
        """tclean stop code(s) to be ignored for warning messages (PIPE-1319)."""
        return [1, 5, 6]

    def keep_iterating(self, iteration, hm_masking, tclean_stopcode, dirty_dynamic_range, residual_max,
                       residual_robust_rms, field, intent, spw, specmode):
        """Determine if another tclean iteration is necessary."""

        if iteration == 0:
            keep_iterating = True
            hm_masking = hm_masking
        else:
            keep_iterating = False
            # Check for zero automask
            if (hm_masking == 'auto') and (tclean_stopcode == 7):
                if intent in ('BANDPASS', 'PHASE', 'AMPLITUDE', 'POLARIZATION', 'POLANGLE', 'POLLEAKAGE'):
                    if residual_max / residual_robust_rms > 10.0:
                        LOG.attention('No automatic clean mask was found despite clean residual peak / scaled MAD > 10, '
                                      'switched to pb-based mask and tlimit=4. '
                                      'Field %s Intent %s SPW %s' % (field, intent, spw))
                    else:
                        LOG.attention('No automatic clean mask was found, switched to pb-based mask and tlimit=4. Field %s '
                                      'Intent %s SPW %s' % (field, intent, spw))
                    # If no automask is found, always try the simple circular mask for calibrators
                    hm_masking = 'centralregion'
                    keep_iterating = True
                elif intent in ('CHECK', 'TARGET'):
                    if residual_max / residual_robust_rms > 10.0:
                        if (specmode == 'cube') or (dirty_dynamic_range <= 30.0):
                            LOG.attention('No automatic clean mask was found despite clean residual peak / scaled MAD > 10, '
                                          'check the results. '
                                          'Field %s Intent %s SPW %s' % (field, intent, spw))
                        else:
                            LOG.attention('No automatic clean mask was found despite clean residual peak / scaled MAD > 10, '
                                          'switched to pb-based mask and tlimit=4. '
                                          'Field %s Intent %s SPW %s' % (field, intent, spw))
                            # If no automask is found, try the simple circular mask for high DR continuum
                            hm_masking = 'centralregion'
                            keep_iterating = True

        return keep_iterating, hm_masking

    def threshold(self, iteration, threshold, hm_masking):

        cqa = casa_tools.quanta

        if iteration == 0:
            return '0.0mJy'
        elif iteration == 1:
            maxthreshold = self.imaging_params.get('maxthreshold', None)
            if maxthreshold and threshold and cqa.gt(threshold, maxthreshold):
                LOG.info(
                    f'Switching to use the pre-defined threshold upper limit value of {maxthreshold} instead of {threshold}')
                return maxthreshold
            else:
                return threshold
        else:
            # Fallback to circular mask if auto-boxing fails.
            # CAS-10489: old centralregion option needs higher threshold
            return '%sJy' % (cqa.getvalue(cqa.mul(threshold, 2.0))[0])

    def intent(self):
        return 'TARGET'

    def stokes(self, intent: str = '', joint_intents: str = '') -> str:
        if intent == 'POLARIZATION' and joint_intents == 'POLARIZATION':
            return 'IQUV'
        else:
            return 'I'

    def reffreq(self, deconvolver: Optional[str] = None, specmode: Optional[str] = None, spwsel: Optional[dict] = None) -> Optional[str]:
        """PIPE-1838: Tclean reffreq parameter heuristics."""

        if deconvolver != 'mtmfs' or specmode != 'cont':
            return None

        if spwsel in (None, ''):
            LOG.attention('Cannot calculate reference frequency for mtmfs cleaning.')
            return None

        qaTool = casa_tools.quanta

        n_sum = 0.0
        d_sum = 0.0
        p = re.compile(r'([\d.]*\s*)(~\s*)([\d.]*\s*)([A-Za-z]*\s*)(;?)')
        freqRangeFound = False
        for spwsel_k, spwsel_v in spwsel.items():
            try:
                if spwsel_v not in ('', 'NONE'):
                    freq_ranges, frame = spwsel_v.rsplit(' ', maxsplit=1)
                    freqRangeFound = True
                    freq_intervals = p.findall(freq_ranges)
                    LOG.debug('ALMA reffreq heuristics: spwsel - key:value - %s:%s', spwsel_k, spwsel_v)
                    for freq_interval in freq_intervals:
                        f_low = qaTool.quantity(float(freq_interval[0]), freq_interval[3])
                        f_low_v = float(qaTool.getvalue(qaTool.convert(f_low, 'GHz')))
                        f_high = qaTool.quantity(float(freq_interval[2]), freq_interval[3])
                        f_high_v = float(qaTool.getvalue(qaTool.convert(f_high, 'GHz')))
                        LOG.debug('ALMA reffreq heuristics: aggregating interval: f_low_v / f_high_v: %s / %s GHz', f_low_v, f_high_v)
                        n_sum += f_high_v**2-f_low_v**2
                        d_sum += f_high_v-f_low_v
            except:
                LOG.attention('Cannot calculate reference frequency for mtmfs cleaning.')
                return None
        if not freqRangeFound:
            LOG.info('No continuum frequency ranges found. No reference frequency calculated.')
            return None
        d_sum *= 2

        if d_sum != 0.0:
            return f'{n_sum/d_sum}GHz'
        else:
            LOG.attentation('Reference frequency calculation led to zero denominator.')
            return None

    def arrays(self, vislist: Optional[List[str]] = None):

        """Return the array descriptions."""

        if vislist is None:
            local_vislist = self.vislist
        else:
            local_vislist = vislist

        antenna_diameters = self.antenna_diameters(local_vislist)
        array_descs = []
        if 12.0 in antenna_diameters:
            array_descs.append('12m')
        if 7.0 in antenna_diameters:
            array_descs.append('7m')
        return ''.join(array_descs)
