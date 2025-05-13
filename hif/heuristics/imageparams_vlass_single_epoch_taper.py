import re
from typing import Union, Tuple, Optional

import numpy

import pipeline.infrastructure as infrastructure
from pipeline.infrastructure import casa_tools
from .imageparams_base import ImageParamsHeuristics

LOG = infrastructure.get_logger(__name__)


class ImageParamsHeuristicsVlassSeTaper(ImageParamsHeuristics):

    def __init__(self, vislist, spw, observing_run, imagename_prefix='', proj_params=None, contfile=None, linesfile=None, imaging_params={}):
        ImageParamsHeuristics.__init__(self, vislist, spw, observing_run, imagename_prefix, proj_params, contfile, linesfile, imaging_params)
        self.imaging_mode = 'VLASS-SE-TAPER'

    # niter
    def niter_correction(self, niter, cell, imsize, residual_max, threshold, residual_robust_rms, mask_frac_rad=0.0, intent='TARGET') -> int:
        """Adjust niter value between cleaning iteration steps based on imaging parameters, mask and residual"""
        if niter:
            return int(niter)
        else:
            return 20000

    def niter(self) -> int:
        """Tclean niter parameter heuristics."""
        return self.niter_correction(None, None, None, None, None, None)

    def deconvolver(self, specmode, spwspec, intent: str = '', stokes: str = '') -> str:
        """Tclean deconvolver parameter heuristics."""
        return 'mtmfs'

    def robust(self, specmode=None) -> float:
        """Tclean robust parameter heuristics."""
        return 1.0

    def gridder(self, intent, field) -> str:
        """Tclean gridder parameter heuristics."""
        return 'mosaic'

    def cell(self, beam=None, pixperbeam=None) -> Union[str, list]:
        """Tclean cell parameter heuristics."""
        return ['1.8arcsec']

    def imsize(self, fields=None, cell=None, primary_beam=None, sfpblimit=None, max_pixels=None, centreonly=None,
               vislist=None, spwspec=None, intent: str = '', joint_intents: str = '', specmode=None) -> Union[list, int]:
        """Tclean imsize parameter heuristics."""
        return [4050, 4050]

    def reffreq(self, deconvolver: Optional[str]=None, specmode: Optional[str]=None, spwsel: Optional[dict]=None) -> Optional[str]:
        """Tclean reffreq parameter heuristics."""
        return '3.0GHz'

    def cyclefactor(self, iteration: int, field=None, intent=None, specmode=None, iter0_dirty_dynamic_range=None) -> float:
        """Tclean cyclefactor parameter heuristics."""
        return 3.0

    def cycleniter(self, iteration: int ) -> int:
        """Tclean cycleniter parameter heuristics."""
        return 2000

    def scales(self, iteration: Union[int, None] = None) -> list:
        """Tclean scales parameter heuristics."""
        return [0]

    def uvtaper(self, beam_natural=None, protect_long=None, beam_user=None, tapering_limit=None, repr_freq=None) -> Union[str, list]:
        """Tclean uvtaper parameter heuristics."""
        return ['7.0arcsec']

    def uvrange(self, field=None, spwspec=None, specmode=None) -> tuple:
        """Tclean uvrange parameter heuristics."""
        return None, None

    def mask(self, hm_masking=None, rootname=None, iteration=None, mask=None,
             results_list: Union[list, None] = None) -> str:
        return ''

    def buffer_radius(self) -> float:
        return 1000.

    def specmode(self) -> str:
        """Tclean specmode parameter heuristics."""
        return 'mfs'

    def intent(self) -> str:
        """Tclean intent parameter heuristics."""
        return 'TARGET'

    def nterms(self, spwspec) -> int:
        """Tclean nterms parameter heuristics."""
        return 2

    def stokes(self, intent: str = '', joint_intents: str = '') -> str:
        """Tclean stokes parameter heuristics."""
        return 'I'

    def pb_correction(self) -> bool:
        return False

    def conjbeams(self) -> bool:
        """Tclean conjbeams parameter heuristics."""
        return False

    def get_sensitivity(self, ms_do, field, intent, spw, chansel, specmode, cell, imsize, weighting, robust, uvtaper):
        return 0.0, None, None, None

    def find_fields(self, distance='0deg', phase_center=None, matchregex=''):

        # Created STM 2016-May-16 use center direction measure
        # Returns list of fields from msfile within a rectangular box of size distance

        qa = casa_tools.quanta
        me = casa_tools.measures
        tb = casa_tools.table

        msfile = self.vislist[0]

        fieldlist = []

        phase_center = phase_center.split()
        center_dir = me.direction(phase_center[0], phase_center[1], phase_center[2])
        center_ra = center_dir['m0']['value']
        center_dec = center_dir['m1']['value']

        try:
            qdist = qa.toangle(distance)
            qrad = qa.convert(qdist, 'rad')
            maxrad = qrad['value']
        except:
            LOG.error('cannot parse distance {}'.format(distance))
            return

        try:
            tb.open(msfile + '/FIELD')
        except:
            LOG.error('could not open {}/FIELD'.format(msfile))
            return
        field_dirs = tb.getcol('PHASE_DIR')
        field_names = tb.getcol('NAME')
        tb.close()

        (nd, ni, nf) = field_dirs.shape
        LOG.info('Found {} fields'.format(nf))

        # compile field dictionaries
        ddirs = {}
        flookup = {}
        for i in range(nf):
            fra = field_dirs[0, 0, i]
            fdd = field_dirs[1, 0, i]
            rapos = qa.quantity(fra, 'rad')
            decpos = qa.quantity(fdd, 'rad')
            ral = qa.angle(rapos, form=["tim"], prec=9)
            decl = qa.angle(decpos, prec=10)
            fdir = me.direction('J2000', ral[0], decl[0])
            ddirs[i] = {}
            ddirs[i]['ra'] = fra
            ddirs[i]['dec'] = fdd
            ddirs[i]['dir'] = fdir
            fn = field_names[i]
            ddirs[i]['name'] = fn
            if fn in flookup:
                flookup[fn].append(i)
            else:
                flookup[fn] = [i]
        LOG.info('Cataloged {} fields'.format(nf))

        # Construct offset separations in ra,dec
        LOG.info('Looking for fields with maximum separation {}'.format(distance))
        nreject = 0
        skipmatch = matchregex == '' or matchregex == []
        for i in range(nf):
            dd = ddirs[i]['dir']
            dd_ra = dd['m0']['value']
            dd_dec = dd['m1']['value']
            sep_ra = abs(dd_ra - center_ra)
            if sep_ra > numpy.pi:
                sep_ra = 2.0 * numpy.pi - sep_ra
            # change the following to use dd_dec 2017-02-06
            sep_ra_sky = sep_ra * numpy.cos(dd_dec)

            sep_dec = abs(dd_dec - center_dec)

            ddirs[i]['offset_ra'] = sep_ra_sky
            ddirs[i]['offset_ra'] = sep_dec

            if sep_ra_sky <= maxrad:
                if sep_dec <= maxrad:
                    if skipmatch:
                        fieldlist.append(i)
                    else:
                        # test regex against name
                        foundmatch = False
                        fn = ddirs[i]['name']
                        for rx in matchregex:
                            mat = re.findall(rx, fn)
                            if len(mat) > 0:
                                foundmatch = True
                        if foundmatch:
                            fieldlist.append(i)
                        else:
                            nreject += 1

        LOG.info('Found {} fields within {}'.format(len(fieldlist), distance))
        if not skipmatch:
            LOG.info('Rejected {} distance matches for regex'.format(nreject))

        return fieldlist

    def keep_iterating(self, iteration, hm_masking, tclean_stopcode, dirty_dynamic_range, residual_max,
                       residual_robust_rms, field, intent, spw, specmode) -> Tuple[bool, str]:
        """Determine if another tclean iteration is necessary."""
        if iteration == 0:
            return True, hm_masking
        elif iteration == 1:
            LOG.info('Final VLASS single epoch tclean call with no mask')
            return True, 'none'
        else:
            return False, hm_masking

    def threshold(self, iteration: int, threshold: Union[str, float], hm_masking: str) -> Union[str, float]:
        """Tclean threshold parameter heuristics."""
        if hm_masking == 'auto':
            return '0.0mJy'
        elif hm_masking == 'none':
            if iteration in [0, 1]:
                return threshold
            else:
                return '0.0mJy'
        else:
            return threshold

    def nsigma(
        self, iteration: int, hm_nsigma: float, hm_masking: str, rms_multiplier: Optional[Union[int, float]] = None
    ) -> Union[float, None]:
        """Tclean nsigma parameter heuristics."""
        if hm_nsigma:
            return hm_nsigma

        if iteration == 0:
            return None
        elif iteration == 1:
            return 3.0
        else:
            return 4.5

    def savemodel(self, iteration: int) -> Union[str, None]:
        """Tclean savemodel parameter heuristics."""
        if iteration == 2:
            return 'modelcolumn'
        else:
            return None
