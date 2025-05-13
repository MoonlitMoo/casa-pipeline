import os

import pipeline.infrastructure as infrastructure
from pipeline.hif.heuristics import cleanbox as cbheuristic

from .resultobjects import BoxResult

LOG = infrastructure.get_logger(__name__)


class BaseCleanSequence:

    def __init__(self, multiterm=None, gridder='', threshold='0.0mJy', sensitivity=0.0, niter=0):
        """Constructor.
        """
        self.result = BoxResult()

        self.flux = None

        self.residuals = []
        self.multiterm = multiterm
        self.gridder = gridder
        self.threshold = threshold
        self.sensitivity = sensitivity
        self.dr_corrected_sensitivity = sensitivity
        self.niter = niter

    def iteration_result(self, model, restored, residual,
                         flux, cleanmask, pblimit_image=0.2, pblimit_cleanmask=0.3,
                         cont_freq_ranges=None):
        """This method sets the iteration counter and returns statistics for that iteration."""

        self.flux = flux

        (residual_cleanmask_rms,
         residual_non_cleanmask_rms,
         residual_min,
         residual_max,
         nonpbcor_image_non_cleanmask_rms_min,
         nonpbcor_image_non_cleanmask_rms_max,
         nonpbcor_image_non_cleanmask_rms,
         pbcor_image_min, pbcor_image_max,
         residual_robust_rms,
         nonpbcor_image_robust_rms_and_spectra,
         pbcor_image_min_iquv,
         pbcor_image_max_iquv,
         nonpbcor_image_non_cleanmask_rms_iquv) = \
            cbheuristic.analyse_clean_result(self.multiterm, model, restored,
                                             residual, flux, cleanmask,
                                             pblimit_image, pblimit_cleanmask,
                                             cont_freq_ranges)

        peak_over_rms = residual_max/residual_robust_rms
        LOG.info('Residual peak: %s', residual_max)
        LOG.info('Residual scaled MAD: %s', residual_robust_rms)
        LOG.info('Residual peak / scaled MAD: %s', peak_over_rms)

        # Append the statistics.
        self.residuals.append(residual)

        return residual_cleanmask_rms, \
               residual_non_cleanmask_rms, \
               residual_min, \
               residual_max, \
               nonpbcor_image_non_cleanmask_rms_min, \
               nonpbcor_image_non_cleanmask_rms_max, \
               nonpbcor_image_non_cleanmask_rms, \
               pbcor_image_min, pbcor_image_max, \
               residual_robust_rms, \
               nonpbcor_image_robust_rms_and_spectra, \
               pbcor_image_min_iquv, \
               pbcor_image_max_iquv, \
               nonpbcor_image_non_cleanmask_rms_iquv

    def iteration(self):
        """The base boxworker allows only one iteration."""

        return self.result

    def get_rootname(self):
        """Return the rootname of imaging products.

        e.g. oussid.s2_2.04287+1801_sci.spw16_18_20_22_24_26_28_30.cont.I
        """
        rootname = None
        if self.residuals:
            rootname, _ = os.path.splitext(self.residuals[0])
            rootname, _ = os.path.splitext(rootname)
        return rootname
