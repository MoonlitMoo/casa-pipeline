# This imaging mode is presently only used by hif_mageprecheck. It was
# added in PIPE-1712 to support merging hifas_imageprecheck with
# hifa_imageprecheck. It could be expanded to a fully-fledged imaging mode
# in the future.

import math

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.utils as utils
from pipeline.infrastructure import casa_tools

from .imageparams_alma import ImageParamsHeuristicsALMA

LOG = infrastructure.get_logger(__name__)


class ImageParamsHeuristicsALMASrdp(ImageParamsHeuristicsALMA):

    def __init__(self, vislist, spw, observing_run, imagename_prefix='', proj_params=None, contfile=None, linesfile=None, imaging_params={}):
        super().__init__(vislist, spw, observing_run, imagename_prefix, proj_params, contfile, linesfile, imaging_params)
        self.imaging_mode = 'ALMA-SRDP'

    def uvtaper(self, beam_natural=None, protect_long=3, beam_user=None, tapering_limit=None, repr_freq=None):
        """
        This code will take a given beam and a desired beam size and calculate the necessary
        UV-tapering parameters needed for tclean to recreate that beam.

        UV-tapering parameter larger than the 80 percentile baseline is not allowed.

        :param beam_natural: natural beam, dictionary with major, minor and positionangle keywords
        :param beam_user: desired beam, dictionary with major, minor and positionangle keywords
        :param tapering_limit: 190th baseline in meters. uvtaper larger than this baseline is not allowed
        :param repr_freq: representative frequency, dictionary with unit and value keywords.
        :return: uv_taper needed to recreate user_beam in tclean
        """
        if beam_natural is None:
            return []
        if beam_user is None:
            return []
        if tapering_limit is None:
            return []
        if repr_freq is None:
            return []

        # Determine uvtaper based on equations from Ryan Loomis,
        # https://open-confluence.nrao.edu/display/NAASC/Data+Processing%3A+Imaging+Tips
        # See PIPE-704.
        cqa = casa_tools.quanta

        bmajor = 1.0 / cqa.getvalue(cqa.convert(beam_natural['major'], 'arcsec'))
        bminor = 1.0 / cqa.getvalue(cqa.convert(beam_natural['minor'], 'arcsec'))

        des_bmajor = 1.0 / cqa.getvalue(cqa.convert(beam_user['major'], 'arcsec'))
        des_bminor = 1.0 / cqa.getvalue(cqa.convert(beam_user['minor'], 'arcsec'))

        if (des_bmajor > bmajor) or (des_bminor > bminor):
            LOG.warning('uvtaper cannot be calculated for beam_user (%.2farcsec) larger than beam_natural (%.2farcsec)' % (
                1.0 / des_bmajor, 1.0 / bmajor))
            return []

        tap_bmajor = 1.0 / (bmajor * des_bmajor / (math.sqrt(bmajor ** 2 - des_bmajor ** 2)))
        tap_bminor = 1.0 / (bminor * des_bminor / (math.sqrt(bminor ** 2 - des_bminor ** 2)))

        # Assume symmetric beam
        tap_angle = math.sqrt((tap_bmajor ** 2 + tap_bminor ** 2) / 2.0)

        # Convert angle to baseline
        ARCSEC_PER_RAD = 206264.80624709636
        uvtaper_value = ARCSEC_PER_RAD / tap_angle
        LOG.info('uvtaper needed to achive user specified angular resolution is %.2fklambda' %
                 utils.round_half_up(uvtaper_value / 1000., 2))

        # Determine maximum allowed uvtaper
        # PIPE-1104: Limit such that the image includes the baselines from at least 20 antennas.
        # Limit should be set to: (N*(N-1)/2)=the length of the 190th baseline.
        uvtaper_limit = tapering_limit / cqa.getvalue(cqa.convert(cqa.constants('c'), 'm/s'))[0] * \
            cqa.getvalue(cqa.convert(repr_freq, 'Hz'))[0]
        # Limit uvtaper
        if uvtaper_value < uvtaper_limit:
            uvtaper_value = uvtaper_limit
            LOG.warning('uvtaper is smaller than allowed limit of %.2fklambda, the length of the 190th baseline, using the limit value' %
                        utils.round_half_up(uvtaper_limit / 1000., 2))
        return ['%.2fklambda' % utils.round_half_up(uvtaper_value / 1000., 2)]
