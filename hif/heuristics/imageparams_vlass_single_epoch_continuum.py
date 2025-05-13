import os
import re
import shutil
import uuid
from typing import Dict, Optional, Tuple, Union

import numpy
from casatasks.private.imagerhelpers.imager_parallel_continuum import PyParallelContSynthesisImager
from casatasks.private.imagerhelpers.input_parameters import ImagerParameters

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.mpihelpers as mpihelpers
import pipeline.infrastructure.utils as utils
from pipeline.infrastructure import casa_tools

from .imageparams_base import ImageParamsHeuristics

LOG = infrastructure.get_logger(__name__)


class ImageParamsHeuristicsVlassSeCont(ImageParamsHeuristics):

    def __init__(self, vislist, spw, observing_run, imagename_prefix='', proj_params=None, contfile=None,
                 linesfile=None, imaging_params={}):
        ImageParamsHeuristics.__init__(self, vislist, spw, observing_run, imagename_prefix, proj_params, contfile,
                                       linesfile, imaging_params)
        self.imaging_mode = 'VLASS-SE-CONT'
        # Update it explicitly when populating context.clean_list_pending (i.e. in hif_editimlist)
        self.vlass_stage = 0
        # Allow user specified cycleniter that affects only cleaning without user mask in the final imaging stage.
        # Value is None or float.
        self.user_cycleniter_final_image_nomask = None

    # niter
    def niter_correction(self, niter, cell, imsize, residual_max, threshold, residual_robust_rms,
                         mask_frac_rad=0.0, intent='TARGET') -> int:
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
        if self.vlass_stage == 3:
            return 1.0
        else:
            return -2.0

    def gridder(self, intent, field) -> str:
        """Tclean gridder parameter heuristics."""
        return 'awproject'

    def cell(self, beam=None, pixperbeam=None) -> Union[str, list]:
        """Tclean cell parameter heuristics."""
        return ['0.6arcsec']

    def imsize(self, fields=None, cell=None, primary_beam=None, sfpblimit=None, max_pixels=None, centreonly=None,
               vislist=None, spwspec=None, intent: str = '', joint_intents: str = '', specmode=None) -> Union[list, int]:
        """Tclean imsize parameter heuristics."""
        return [16384, 16384]

    def reffreq(self, deconvolver: Optional[str]=None, specmode: Optional[str]=None, spwsel: Optional[dict]=None) -> Optional[str]:
        """Tclean reffreq parameter heuristics."""
        return '3.0GHz'

    def cyclefactor(self, iteration: int, field=None, intent=None, specmode=None, iter0_dirty_dynamic_range=None) -> float:
        """Tclean cyclefactor parameter heuristics."""
        return 3.0

    def cycleniter(self, iteration: int) -> int:
        """Tclean cycleniter parameter heuristics."""
        # Special cases: cleaning without mask in 1st stage
        if self.vlass_stage == 1 and iteration > 1:
            return 500
        # Cleaning without mask 3rd imaging stage, allow user to set value
        elif self.vlass_stage == 3 and iteration > 2:
            if self.user_cycleniter_final_image_nomask:
                LOG.info("Using user specified cycleniter = {} for cleaning without "
                         "user mask (pbmask only).".format(self.user_cycleniter_final_image_nomask))
                return self.user_cycleniter_final_image_nomask
            else:
                return 500
        # Special case: 3rd imaging stage
        elif self.vlass_stage == 3 and iteration > 0:
            return 3000
        else:
            return 5000

    def nmajor(self, iteration: int) -> Union[None, int]:
        """Tclean nmajor parameter heuristics."""
        if iteration == 0:
            return None
        else:
            # PIPE-1745: default value of nmajor=220 for all editimlist stages of the VLASS QL/SE imaging workflow
            return 220

    def scales(self, iteration: Union[int, None] = None) -> Union[list, None]:
        """Tclean scales parameter heuristics."""
        if not iteration:
            return None
        if self.vlass_stage == 3 and iteration in [1, 2, 3]:
            return [0, 5, 12]
        else:
            return [0]

    def uvtaper(self, beam_natural=None, protect_long=None, beam_user=None, tapering_limit=None, repr_freq=None) -> Union[str, list]:
        """Tclean uvtaper parameter heuristics."""
        if self.vlass_stage == 3:
            return ''
        else:
            # PIPE-1679: the previous default value of '3arcsec' has been changed to '3/(pi/(4ln(2)))arcsec'
            # since CASA ver>=6.5.3 to maintain the beam size consistency due to the math correction from CAS-13260.
            return ['2.6476arcsec']

    def uvrange(self, field=None, spwspec=None, specmode=None) -> tuple:
        """Tclean uvrange parameter heuristics."""
        return '<12km', None

    def mask(self, hm_masking=None, rootname=None, iteration=None, mask=None,
             results_list: Union[list, None] = None, clean_no_mask=None) -> Union[str, list]:
        """Tier-1 mask name to be used for computing Tier-1 and Tier-2 combined mask.

            Obtain the mask name from the latest MakeImagesResult object in context.results.
            If not found, then set empty string (as base heuristics)."""
        mask_list = ''
        if results_list and type(results_list) is list:
            for result in results_list:
                result_meta = result.read()
                if hasattr(result_meta, 'pipeline_casa_task') and result_meta.pipeline_casa_task.startswith(
                        'hifv_vlassmasking'):
                    mask_list = result_meta.combinedmask

        # Add 'pb' string as a placeholder for cleaning without mask (pbmask only, see PIPE-977). This should
        # always stand at the last place in the mask list.
        # On request for first imaging stage (selfcal image) and automatically for the final imaging stage.
        if (clean_no_mask and self.vlass_stage == 1) or self.vlass_stage == 3:
            if clean_no_mask:
                LOG.info('Cleaning without user mask is performed in pre-self calibration imaging stage '
                         '(clean_no_mask_selfcal_image=True)')
            if type(mask_list) is list:
                mask_list.append('pb')
            elif mask_list != '':  # mask is non-empty string
                mask_list = [mask_list, 'pb']
            else:
                mask_list = 'pb'

        # In case hif_makeimages result was not found or results_list was not provided
        return mask_list

    def buffer_radius(self) -> float:
        return 1000.

    def specmode(self) -> str:
        """Tclean specmode parameter heuristics.
        See PIPE-1060"""
        return 'cont'

    def intent(self) -> str:
        """Tclean intent parameter heuristics."""
        return 'TARGET'

    def nterms(self, spwspec) -> Union[int, None]:
        """Tclean nterms parameter heuristics."""
        return 2

    def stokes(self, intent: str = '', joint_intents: str = '') -> str:
        """Tclean stokes parameter heuristics."""
        return 'I'

    def pb_correction(self) -> bool:
        return False

    def pblimits(self, pb: Union[None, str], specmode: Optional[str] = None) -> Tuple[float, float]:
        """Tclean pblimit parameter and cleanmask pblimit heuristics."""

        pblimit_image, pblimit_cleanmask = super().pblimits(pb)

        # Overwrite pblimit_image (to be used in tclean as pblimit parameter) with
        # the VLASS-SE-CONT-MOSAIC specific value.
        return 0.02, pblimit_cleanmask

    def conjbeams(self) -> bool:
        """Tclean conjbeams parameter heuristics."""
        return True

    def get_sensitivity(self, ms_do, field, intent, spw, chansel, specmode, cell, imsize, weighting, robust, uvtaper) \
            -> Tuple[float, None, None, None]:
        return 0.0, None, None, None

    def find_fields(self, distance: str = '0deg', phase_center: bool = None, matchregex: str = '') -> list:

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
        """Determine whether another tclean iteration is necessary."""
        if iteration == 0:
            return True, 'auto'
        elif iteration == 1:
            LOG.info('Final VLASS single epoch tclean call with no mask')
            return True, 'user'
        else:
            return False, 'user'

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
        # PSF and dirty image
        if iteration == 0:
            return 2.0
        # Cleaning with pb mask in 3rd imaging stage
        elif self.vlass_stage == 3 and iteration == 3:
            return 4.5
        # First imaging stage
        elif self.vlass_stage == 1 and iteration >= 1:
            return 10.0
        # Second imaging stage
        elif self.vlass_stage == 2 and iteration >= 1:
            return 5.0
        # Cleaning with user mask in 3rd imaging stage
        else:
            return 3.0

    def savemodel(self, iteration: int) -> Union[str, None]:
        """Tclean savemodel parameter heuristics."""
        # Model is saved in first imaging cycle last iteration
        if self.vlass_stage == 1:
            return 'modelcolumn'
        else:
            return None

    def datacolumn(self) -> str:
        """Column parameter to be used as tclean argument"""
        # First imaging stage use data column
        if self.vlass_stage == 1:
            return 'data'
        # Subsequent stages use the self-calibrated and corrected column
        else:
            return 'corrected'

    def wprojplanes(self, gridder=None, spwspec=None) -> int:
        """Tclean wprojplanes parameter heuristics."""
        return 32

    def rotatepastep(self) -> float:
        """Tclean rotatepastep parameter heuristics."""
        return 5.0

    def get_autobox_params(
        self,
        iteration: int,
        intent: str,
        specmode: str,
        robust: float,
        rms_multiplier: Optional[Union[int, float]] = None,
    ) -> tuple:
        """Default auto-boxing parameters."""

        sidelobethreshold = None
        noisethreshold = None
        lownoisethreshold = None
        negativethreshold = None
        if iteration == 0:
            minbeamfrac = 0.3
        else:
            minbeamfrac = 0.1
        growiterations = None
        dogrowprune = None
        minpercentchange = None
        fastnoise = None

        return (
            sidelobethreshold,
            noisethreshold,
            lownoisethreshold,
            negativethreshold,
            minbeamfrac,
            growiterations,
            dogrowprune,
            minpercentchange,
            fastnoise,
        )

    def usepointing(self) -> bool:
        """clean flag to use pointing table."""
        return True

    def get_cfcaches(self, cfcache: str) -> list:
        """Parses comma separated cfcache string

        Comma separated list is used to input frequency dependent and independent A-term cfcaches in
        VLASS-SE-CONT imaging mode.
        """
        if ',' in cfcache:
            return [cfch.strip() for cfch in cfcache.split(',')][0:2]
        else:
            return [cfcache, None]

    def set_user_cycleniter_final_image_nomask(self, cycleniter_final_image_nomask: Union[int, None] = None) -> None:
        """Sets class variable controlling the cycleniter parameter of the last clean step (cleaning without user mask,
        pbmask only) in the third (final) VLASS-SE-CONT imaging stage."""
        if self.vlass_stage == 3 and cycleniter_final_image_nomask != None:
            LOG.info("Setting user specified cycleniter = {} for cleaning without "
                     "user mask (pbmask only).".format(cycleniter_final_image_nomask))
        self.user_cycleniter_final_image_nomask = cycleniter_final_image_nomask

    def smallscalebias(self) -> float:
        """A numerical control to bias the scales when using multi-scale or mtmfs algorithms"""
        return 0.4

    def restoringbeam(self) -> Union[list, str, bool]:
        """Tclean parameter"""
        return ''

    def pointingoffsetsigdev(self) -> list:
        """Tclean parameter"""
        return [300, 30]

    def pbmask(self) -> float:
        """Tclean pbmask parameter heuristics.
        Cleaning with only primary beam mask is used on request (via editimlist)."""
        return 0.4

    def get_parallel_cont_synthesis_imager_csys(self, phasecenter=None, imsize=None, cell=None,
                                                parallel='automatic') -> Union[None, Dict]:
        """
        This method creates an image with PyParallelContSynthesisImager and returns it's phase centre.

        The purpose of this method is to temporarily (until CASA fixes the issue) reduce the phase centre accuracy of
        VLASS-SE-CONT masks computed in hifv_makeimages() to the accuracy used in the parallel imager (in the
        hif_makeimages() stages).

        Tclean 6.1 truncates phase center coordinates at ~1E-7 precision. When a mask is provided to tclean
        with higher precision reference coordinate, the truncation may lead to the interpolated mask to shift
        by a pixel, resulting in slightly different tclean input and output mask.

        The method also records the phase centre difference in the CASA log.

        See CAS-13338 and PIPE-728
        """
        qaTool = casa_tools.quanta
        do_parallel = mpihelpers.parse_mpi_input_parameter(parallel)

        if not phasecenter:
            LOG.error(f"No phasecenter is provided.")

        if do_parallel:
            LOG.info(
                "Determining exact value of image phase center empirically due to CASA precision issue in parallel mode (CAS-13338)")
            tmp_psf_filename = str(uuid.uuid4())
            paramList = ImagerParameters(msname=self.vislist,
                                         phasecenter=phasecenter,
                                         imagename=tmp_psf_filename,
                                         imsize=imsize,
                                         cell=cell,
                                         stokes='I',
                                         # gridder=self.gridder(None, None),
                                         # cfcache=cfcache,
                                         parallel=do_parallel
                                         )
            makepsf_imager = PyParallelContSynthesisImager(params=paramList)

            makepsf_imager.initializeImagers()
            makepsf_imager.initializeNormalizers()
            makepsf_imager.setWeighting()
            makepsf_imager.makePSF()
            makepsf_imager.deleteTools()

            # Obtain image reference and header
            with casa_tools.ImageReader('{}.psf'.format(tmp_psf_filename)) as image:
                csys_image = image.coordsys()
            csys_record = csys_image.torecord()
            csys_image.done()

            tmp_psf_images = utils.glob_ordered('%s.*' % tmp_psf_filename)
            for tmp_psf_image in tmp_psf_images:
                shutil.rmtree(tmp_psf_image)

            # Report coordinate difference:
            ra_str = qaTool.convert(phasecenter.split()[1], 'arcsec')['value']
            dec_str = qaTool.convert(phasecenter.split()[2], 'arcsec')['value']

            ra_psf = qaTool.convert('%s %s' % (csys_record['direction0']['crval'][0],
                                               csys_record['direction0']['units'][0]), 'arcsec')['value']
            dec_psf = qaTool.convert('%s %s' % (csys_record['direction0']['crval'][1],
                                                csys_record['direction0']['units'][1]), 'arcsec')['value']

            LOG.info('Corrected difference between requested phase center and parallel synthesis imager phase '
                     'center is delta_ra = {:.4e} arcsec, delta_dec = {:.4e} arcsec'.format(ra_psf - ra_str,
                                                                                            dec_psf - dec_str))
        else:
            csys_record = None

        return csys_record

    def get_outmaskratio(self, iteration: int,  image: str, pbimage: str, cleanmask: str,
                         pblimit: float = 0.4, frac_lim: float = 0.2) -> Union[None, float]:
        """Determine fractional flux in final image outside cleanmask, only in first imaging stage final image.

        A threshold of 10x sigma (measured on image) and a pblimit of 0.4 is applied.
        """
        # Only for first imaging stage, restoration with QL mask, and if there is a mask
        if self.vlass_stage == 1 and iteration == 1 and cleanmask != '':
            # Check if files exist
            warn_message = '%s does not exist, flux fraction outside mask cannot be computed.'
            if not os.path.exists(image):
                LOG.warning(warn_message % image)
                return None
            if not os.path.exists(pbimage):
                LOG.warning(warn_message % pbimage)
                return None
            if not os.path.exists(cleanmask):
                LOG.warning(warn_message % cleanmask)
                return None

            # threshold for sigma clipping, measure sigma in the image and use 10x the value
            with casa_tools.ImageReader(image) as im:
                threshold = im.statistics(robust=False)['sigma'].item() * 10.0
                thr_unit = im.brightnessunit()

            LOG.info(f"Measuring flux fraction outside clean mask with 10 sigma threshold = {threshold} "
                     f"{thr_unit} and pblimit = {pblimit}")

            # Image flux within mask, above threshold and pb level
            expression = f'iif("{image}" > {threshold}, "{image}", 0.0) ' \
                         f'* iif("{pbimage}" >= {pblimit}, 1.0, 0.0) ' \
                         f'* "{cleanmask}"'
            image_inmask = casa_tools.image.imagecalc(outfile="", pixels=expression, imagemd=f"{image}")
            inmask_flux = image_inmask.statistics()['sum'].item()
            image_inmask.done()

            # Image flux outside mask, above threshold and pb level
            expression = f'iif("{image}" > {threshold}, "{image}", 0.0) ' \
                         f'* iif("{pbimage}" >= {pblimit}, 1.0, 0.0) ' \
                         f'* (("{cleanmask}" - 1.0) * -1.0)'

            image_outmask = casa_tools.image.imagecalc(outfile="", pixels=expression, imagemd=f"{image}")
            outmask_flux = image_outmask.statistics()['sum'].item()
            image_outmask.done()

            # calculate ratio of total flux inside and outside mask
            outmaskratio = outmask_flux / (inmask_flux + outmask_flux)

            if outmaskratio > frac_lim:
                LOG.warning(f'Flux fraction outside cleanmask is {"%.3g" % outmaskratio}, '
                            f'exceeds {frac_lim} limit!')
            else:
                LOG.info(f'Flux fraction outside clean mask ({cleanmask}) is {"%.3g" % outmaskratio}')

            return outmaskratio
        else:
            return None


class ImageParamsHeuristicsVlassSeContAWPP001(ImageParamsHeuristicsVlassSeCont):
    """
    Special heuristics case when gridder is awproject and the wprojplanes parameter
    is set to 1 (in parent class it is 32).
    """

    def __init__(self, vislist, spw, observing_run, imagename_prefix='', proj_params=None, contfile=None,
                 linesfile=None, imaging_params={}):
        ImageParamsHeuristicsVlassSeCont.__init__(self, vislist, spw, observing_run, imagename_prefix, proj_params,
                                                  contfile, linesfile, imaging_params)
        self.imaging_mode = 'VLASS-SE-CONT-AWP-P001'
        # Update it explicitly when populating context.clean_list_pending (i.e. in hif_editimlist)
        self.vlass_stage = 0

    def wprojplanes(self, gridder=None, spwspec=None) -> int:
        """Tclean wprojplanes parameter heuristics."""
        return 1

    def gridder(self, intent, field) -> str:
        """Tclean gridder parameter heuristics."""
        return 'awproject'


class ImageParamsHeuristicsVlassSeContMosaic(ImageParamsHeuristicsVlassSeCont):
    """
    Special heuristics case when gridder is awproject and the wprojplanes parameter
    is set to 1 (in parent class it is 32).
    """

    def __init__(self, vislist, spw, observing_run, imagename_prefix='', proj_params=None, contfile=None,
                 linesfile=None, imaging_params={}):
        ImageParamsHeuristicsVlassSeCont.__init__(self, vislist, spw, observing_run, imagename_prefix, proj_params,
                                                  contfile, linesfile, imaging_params)
        self.imaging_mode = 'VLASS-SE-CONT-MOSAIC'
        # Update it explicitly when populating context.clean_list_pending (i.e. in hif_editimlist)
        self.vlass_stage = 0
        self.user_cycleniter_final_image_nomask = None

    def imsize(self, fields=None, cell=None, primary_beam=None, sfpblimit=None, max_pixels=None, centreonly=None,
               vislist=None, spwspec=None, intent: str = '', joint_intents: str = '', specmode=None) -> Union[list, int]:
        """Tclean imsize parameter heuristics."""
        return [12500, 12500]

    def mosweight(self, intent, field) -> bool:
        """tclean flag to use mosaic weighting."""

        # Currently only ALMA has decided to use this flag (CAS-11840). So
        # the default is set to False here.
        return False

    def wprojplanes(self, gridder=None, spwspec=None) -> int:
        """Tclean wprojplanes parameter heuristics."""
        return 1

    def gridder(self, intent, field) -> str:
        """Tclean gridder parameter heuristics."""
        return 'mosaic'

    def cycleniter(self, iteration) -> int:
        """Tclean cycleniter parameter heuristics."""
        # Special cases: cleaning without mask in 1st stage
        if self.vlass_stage == 1 and iteration > 1:
            return 100
        # Cleaning without mask 3rd imaging stage, allow user to set value
        elif self.vlass_stage == 3 and iteration > 2:
            if self.user_cycleniter_final_image_nomask:
                LOG.info("Using user specified cycleniter = {} for cleaning without "
                         "user mask (pbmask only).".format(self.user_cycleniter_final_image_nomask))
                return self.user_cycleniter_final_image_nomask
            else:
                return 100
        else:
            return 500

    def conjbeams(self) -> bool:
        """Tclean conjbeams parameter heuristics."""
        # Might change to True based on stackholder feedback
        return False

    def pblimits(self, pb: Union[None, str], specmode: Optional[str] = None) -> Tuple[float, float]:
        """Tclean pblimit parameter and cleanmask pblimit heuristics."""
        pblimit_image, pblimit_cleanmask = super().pblimits(pb)

        # Overwrite pblimit_image (to be used in tclean as pblimit parameter) with
        # the VLASS-SE-CONT-MOSAIC specific value.
        return 0.1, pblimit_cleanmask

    def usepointing(self) -> bool:
        """clean flag to use pointing table."""
        return False
