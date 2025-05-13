from .imageparams_alma import ImageParamsHeuristicsALMA
from .imageparams_alma_scal import ImageParamsHeuristicsALMAScal
from .imageparams_alma_srdp import ImageParamsHeuristicsALMASrdp
from .imageparams_vlass_quick_look import ImageParamsHeuristicsVlassQl
from .imageparams_vlass_single_epoch_continuum import ImageParamsHeuristicsVlassSeCont, ImageParamsHeuristicsVlassSeContAWPP001, ImageParamsHeuristicsVlassSeContMosaic
from .imageparams_vlass_single_epoch_taper import ImageParamsHeuristicsVlassSeTaper
from .imageparams_vlass_single_epoch_cube import ImageParamsHeuristicsVlassSeCube
from .imageparams_vla import ImageParamsHeuristicsVLA
from .imageparams_vla_scal import ImageParamsHeuristicsVLAScal

class ImageParamsHeuristicsFactory(object):

    '''Imaging heuristics factory class.'''

    @staticmethod
    def getHeuristics(vislist, spw, observing_run, imagename_prefix='', proj_params=None, contfile=None, linesfile=None, imaging_params={}, imaging_mode='ALMA'):
        if imaging_mode == 'ALMA':
            return ImageParamsHeuristicsALMA(vislist, spw, observing_run, imagename_prefix, proj_params, contfile, linesfile, imaging_params)
        if imaging_mode == 'ALMA-SCAL':
            return ImageParamsHeuristicsALMAScal(vislist, spw, observing_run, imagename_prefix, proj_params, contfile, linesfile, imaging_params)
        if imaging_mode == 'ALMA-SRDP':
            return ImageParamsHeuristicsALMASrdp(vislist, spw, observing_run, imagename_prefix, proj_params, contfile, linesfile, imaging_params)
        elif imaging_mode == 'VLASS-QL':  # quick look
            return ImageParamsHeuristicsVlassQl(vislist, spw, observing_run, imagename_prefix, proj_params, contfile, linesfile, imaging_params)
        elif imaging_mode in ['VLASS-SE-CONT', 'VLASS-SE-CONT-AWP-P032']:  # single epoch continuum
            return ImageParamsHeuristicsVlassSeCont(vislist, spw, observing_run, imagename_prefix, proj_params, contfile, linesfile, imaging_params)
        elif imaging_mode == 'VLASS-SE-CONT-AWP-P001': # single epoch continuum, gridder=awproject, wprojplanes=1
            return ImageParamsHeuristicsVlassSeContAWPP001(vislist, spw, observing_run, imagename_prefix, proj_params, contfile, linesfile, imaging_params)
        elif imaging_mode == 'VLASS-SE-CONT-MOSAIC': # single epoch continuum, gridder=mosaic
            return ImageParamsHeuristicsVlassSeContMosaic(vislist, spw, observing_run, imagename_prefix, proj_params, contfile, linesfile, imaging_params)
        elif imaging_mode == 'VLASS-SE-TAPER':  # single epoch taper
            return ImageParamsHeuristicsVlassSeTaper(vislist, spw, observing_run, imagename_prefix, proj_params, contfile, linesfile, imaging_params)
        elif imaging_mode == 'VLASS-SE-CUBE':  # single epoch cube
            return ImageParamsHeuristicsVlassSeCube(vislist, spw, observing_run, imagename_prefix, proj_params, contfile, linesfile, imaging_params)
        elif imaging_mode in ['VLA', 'JVLA', 'EVLA']:  # VLA but not VLASS
            return ImageParamsHeuristicsVLA(vislist, spw, observing_run, imagename_prefix, proj_params, contfile, linesfile, imaging_params)
        elif imaging_mode in ['VLA-SCAL', 'JVLA-SCAL', 'EVLA-SCAL']:  # VLA but not VLASS
            return ImageParamsHeuristicsVLAScal(vislist, spw, observing_run, imagename_prefix, proj_params, contfile, linesfile, imaging_params)            
        else:
            raise Exception('Unknown imaging mode: %s' % imaging_mode)
