from .imageparams_alma import ImageParamsHeuristicsALMA


class ImageParamsHeuristicsALMAScal(ImageParamsHeuristicsALMA):
    """Class for determining image parameters heuristics for ALMA-SCAL.

    Inherits from ImageParamsHeuristicsALMA and offers specialized heuristics 
    for the VLA-SCAL imaging mode. This class facilitates the initialization 
    of Self-calibration CleanTarget objects used by the hif_selfcal(). 
    It cannot be used directly with the hif_makeimages or hif_tclean.

    Attributes:
        imaging_mode (str): The imaging mode, set to 'ALMA-SCAL'.
        selfcal (bool): A flag indicating if self-calibration is enabled.
    """

    def __init__(self, vislist, spw, observing_run, imagename_prefix='', proj_params=None, contfile=None,
                 linesfile=None, imaging_params={}):
        """Initializes the class with the provided parameters.

        Args:
            vislist (list): List of visibility data.
            spw (str): Spectral window.
            observing_run (str): Observing run identifier.
            imagename_prefix (str, optional): Prefix for the image name. Defaults to ''.
            proj_params (dict, optional): Project parameters. Defaults to None.
            contfile (str, optional): Continuum file. Defaults to None.
            linesfile (str, optional): Lines file. Defaults to None.
            imaging_params (dict, optional): Imaging parameters. Defaults to {}.
        """
        super().__init__(vislist, spw, observing_run, imagename_prefix, proj_params, contfile, linesfile, imaging_params)
        self.imaging_mode = 'ALMA-SCAL'
        self.selfcal = True

    def deconvolver(self, specmode, spwspec, intent: str = '', stokes: str = '') -> str:
        """Returns the deconvolver type for ALMA-SCAL.

        Args:
            specmode (str): Spectral mode.
            spwspec (str): Spectral window specification.
            intent (str, optional): Observation intent. Defaults to ''.
            stokes (str, optional): Stokes parameters. Defaults to ''.

        Returns:
            str: The deconvolver type.
        """
        return 'mtmfs'
