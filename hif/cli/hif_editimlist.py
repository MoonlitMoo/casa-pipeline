import sys

import pipeline.h.cli.utils as utils

   
@utils.cli_wrapper
def hif_editimlist(imagename=None,
                   search_radius_arcsec=None,
                   cell=None,
                   cfcache=None,
                   conjbeams=None,
                   cyclefactor=None,
                   cycleniter=None,
                   nmajor=None,
                   datatype=None,
                   datacolumn=None,
                   deconvolver=None,
                   editmode=None,
                   field=None,
                   imaging_mode=None,
                   imsize=None,
                   intent=None,
                   gridder=None,
                   mask=None,
                   pbmask=None,
                   nbin=None,
                   nchan=None,
                   niter=None,
                   nterms=None,
                   parameter_file=None,
                   pblimit=None,
                   phasecenter=None,
                   reffreq=None,
                   restfreq=None,
                   robust=None,
                   scales=None,
                   specmode=None,
                   spw=None,
                   start=None,
                   stokes=None,
                   sensitivity=None,
                   threshold=None,
                   nsigma=None,
                   uvtaper=None,
                   uvrange=None,
                   width=None,
                   vlass_plane_reject_ms=None):
    """
    hif_editimlist ---- Add to a list of images to be produced with hif_makeimages()


    Add to a list of images to be produced with hif_makeimages(), which uses hif_tclean() to invoke CASA tclean.
    Many of the hif_editimlist() inputs map directly to tclean parameters.

    The results object for the pipeline task is returned.


    --------- parameter descriptions ---------------------------------------------

    imagename              Prefix for output image names.
    search_radius_arcsec   Size of the field finding beam search radius in arcsec.
    cell                   Image X and Y cell size(s) with units or pixels per beam.
                           Single value same for both. \'<number>ppb\' for pixels per beam.
                           Compute cell size based on the UV coverage of all the fields
                           to be imaged and use a 5 pix per beam sampling.
                           The pix per beam specification uses the above default cell size
                           ('5ppb') and scales it accordingly.
                           example: ['0.5arcsec', '0.5arcsec'] '3ppb'
    cfcache                Convolution function cache directory name
    conjbeams              Use conjugate frequency in tclean for wideband A-terms.
    cyclefactor            Controls the depth of clean in minor cycles based on PSF.
    cycleniter             Controls max number of minor cycle iterations in a single major cycle.
    nmajor                 Controls the maximum number of major cycles to evaluate.
    datatype               Data type(s) to image. The default '' selects the best
                           available data type (e.g. selfcal over regcal) with
                           an automatic fallback to the next available data type.
                           With the ``datatype`` parameter of 'regcal' or 'selfcal', one
                           can force the use of only given data type(s).
                           Note that this parameter is only for non-VLASS data when the datacolumn
                           is not explictly set by user or imaging heuristics.
    datacolumn             Data column to image; this will take precedence over the datatype parameter.
    deconvolver            Minor cycle algorithm (multiscale or mtmfs)
    editmode               The edit mode of the task ('add' or 'replace'). Defaults to 'add'.
    field                  Set of data selection field names or ids.
    imaging_mode           Identity of product type (e.g. VLASS quick look) desired.  This will determine the heuristics used.
    imsize                 Image X and Y size(s) in pixels or PB level (single fields), \'\' for default. Single value same for both. \'<number>pb\' for PB level.
    intent                 Set of data selection intents
    gridder                Name of the gridder to use with tclean
    mask                   Used to declare whether to use a predefined mask for tclean.
    pbmask                 Used to declare primary beam gain level for cleaning with primary beam mask (usemask='pb'), used only for VLASS-SE-CONT imaging mode.
    nbin                   Channel binning factor.
    nchan                  Number of channels, -1 = all
    niter                  The max total number of minor cycle iterations allowed for tclean
    nterms                 Number of Taylor coefficients in the spectral model
    parameter_file         keyword=value text file as alternative method of input parameters
    pblimit                PB gain level at which to cut off normalizations
    phasecenter            The default phase center is set to the mean of the field
                           directions of all fields that are to be image together.
                           example: 0, 'J2000 19h30m00 -40d00m00'
    reffreq                Reference frequency of the output image coordinate system
    restfreq               List of rest frequencies or a rest frequency in a string for output image.
    robust                 Briggs robustness parameter for tclean
    scales                 The scales for multi-scale imaging.
    specmode               Spectral gridding type (mfs, cont, cube, \'\' for default)
    spw                    Set of data selection spectral window/channels, \'\' for all
    start                  First channel for frequency mode images.
                           Starts at first input channel of the spw.
                           example: '22.3GHz'
    stokes                 Stokes Planes to make
    sensitivity            Theoretical sensitivity (override internal calculation)
    threshold              Stopping threshold (number in units of Jy, or string)
    nsigma                 Multiplicative factor for rms-based threshold stopping
    uvtaper                Used to set a uv-taper during clean.
    uvrange                Set of data selection uv ranges, \'\' for all.
    width                  Channel width
    vlass_plane_reject_ms  Only used for the 'VLASS-SE-CUBE' imaging mode. default: True
                           If True, reject VLASS Coarse Cube planes with high flagging percentages (see the heuristics details below)
                           If False, do not perform flagging-based VLASS Coarse Cube plane rejection.
                           If the input value is a dictionary, the plane rejection heuristics will be performed with custom thresholds.
                           The optional keys are:
                           - exclude_spw, default: ''
                              Spectral windows to be excluded from the VLASS Coarse Cube plane rejection consideration, i.e. always preserve.
                           - flagpct_thresh, default: 0.9
                              Flagging percentage threshold per field for the plane rejection.
                           - nfield_thresh: default: 12 
                              A minimal number of fields above the flagging percentage threshold is required for the plane rejection.

    --------- examples -----------------------------------------------------------





    """


    ##########################################################################
    #                                                                        #
    #  CASA task interface boilerplate code starts here. No edits should be  #
    #  needed beyond this point.                                             #
    #                                                                        #
    ##########################################################################

    # create a dictionary containing all the arguments given in the
    # constructor
    all_inputs = vars()

    # get the name of this function for the weblog, eg. 'hif_flagdata'
    task_name = sys._getframe().f_code.co_name

    # get the context on which this task operates
    context = utils.get_context()

    # execute the task
    results = utils.execute_task(context, task_name, all_inputs)

    return results
