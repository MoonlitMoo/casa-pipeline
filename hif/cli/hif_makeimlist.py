import sys

import pipeline.h.cli.utils as utils


@utils.cli_wrapper
def hif_makeimlist(vis=None, imagename=None, intent=None, field=None,
                   spw=None, contfile=None, linesfile=None, uvrange=None,
                   specmode=None, outframe=None, hm_imsize=None, hm_cell=None,
                   calmaxpix=None, phasecenter=None,
                   nchan=None, start=None, width=None, nbins=None,
                   robust=None, uvtaper=None, clearlist=None, per_eb=None,
                   per_session=None, calcsb=None, datatype=None, datacolumn=None,
                   allow_wproject=None, parallel=None):

    """
    hif_makeimlist ---- Compute list of clean images to be produced


    Generate a list of images to be cleaned. By default, the list will include
    one image per science target per spw. Calibrator targets can be selected
    by setting appropriate values for ``intent``.

    By default, the output image cell size is set to the minimum cell size
    consistent with the UV coverage.

    By default, the image size in pixels is set to values determined by the
    cell size and the primary beam size. If a calibrator is being
    imaged (intents 'PHASE', 'BANDPASS', 'FLUX' or 'AMPLITUDE') then the
    image dimensions are limited to 'calmaxpix' pixels.

    By default, science target images are cubes and calibrator target images
    are mfs. Science target images may be mosaics or single fields.

    Output

    results -- The results object for the pipeline task is returned.

    --------- parameter descriptions ---------------------------------------------

    vis           The list of input MeasurementSets. Defaults to the list of
                  MeasurementSets specified in the h_init or hif_importdata task.
                  "": use all MeasurementSets in the context

                  Examples: 'ngc5921.ms', ['ngc5921a.ms', ngc5921b.ms', 'ngc5921c.ms']
    imagename     Prefix for output image names, "" for automatic.
    intent        Select intents for which associated fields will be imaged.
                  Possible choices are PHASE, BANDPASS, AMPLITUDE, CHECK and
                  TARGET or combinations thereof.

                  Examples: 'PHASE,BANDPASS', 'TARGET'
    field         Select fields to image. Use field name(s) NOT id(s). Mosaics
                  are assumed to have common source / field names.  If intent is
                  specified only fields with data matching the intent will be
                  selected. The fields will be selected from MeasurementSets in
                  "vis".
                  "" Fields matching intent, one image per target source.
    spw           Select spectral windows to image.
                  "": Images will be computed for all science spectral windows.
    contfile      Name of file with frequency ranges to use for continuum images.
    linesfile     Name of file with line frequency ranges to exclude for continuum images.
    uvrange       Select a set of uv ranges to image.
                  "": All uv data is included

                  Examples: '0~1000klambda', ['0~100klambda', 100~1000klambda]
    specmode      Frequency imaging mode, 'mfs', 'cont', 'cube', 'repBW'. ''
                  defaults to 'cube' if ``intent`` parameter includes 'TARGET'
                  otherwise 'mfs'.

                  specmode='mfs' produce one image per source and spw
                  specmode='cont' produce one image per source and aggregate
                           over all specified spws
                  specmode='cube' produce an LSRK frequency cube, channels are
                           specified in frequency
                  specmode='repBW' produce an LSRK frequency cube at
                           representative channel width
    outframe      velocity frame of output image (LSRK, '' for automatic)
                  (not implemented)
    hm_imsize     Image X and Y size in pixels or PB level for single fields.
                  The explicit sizes must be even and divisible by 2,3,5,7 only.
                  The default values are derived as follows:
                    1. Determine phase center and spread of field centers
                       around it.
                    2. Set the size of the image to cover the spread of field
                       centers plus a border of width 0.75 * beam radius, to
                       first null.
                    3. Divide X and Y extents by cell size to arrive at the
                       number of pixels required.
                  The PB level setting for single fields leads to an imsize
                  extending to the specified level plus 5% padding in all
                  directions.

                  Examples: '0.3pb', [120, 120]
    hm_cell       Image X and Y cell sizes. "" computes the cell size based on
                  the UV coverage of all the fields to be imaged and uses a
                  5 pix per beam sampling.
                  The pix per beam specification ('<number>ppb') uses the above
                  default cell size ('5ppb') and scales it accordingly.
                  The cells can also be specified as explicit measures.

                  Examples: '3ppb', ['0.5arcsec', '0.5arcsec']
    calmaxpix     Maximum image X or Y size in pixels if a calibrator is being
                  imaged ('PHASE', 'BANDPASS', 'AMPLITUDE' or 'FLUX' intent).
    phasecenter   Direction measure or field id of the image center.
                  The default phase center is set to the mean of the field
                  directions of all fields that are to be image together.

                  Examples: 'J2000 19h30m00 -40d00m00', 0
    nchan         Total number of channels in the output image(s)
                  -1 selects enough channels to cover the data selected by
                  spw consistent with start and width.
    start         Start of image frequency axis as frequency or velocity.
                  "" selects start frequency automatically.
    width         Output channel width.
                  Difference in frequency between 2 selected channels for
                  frequency mode images.
                  'pilotimage' for 15 MHz / 8 channel heuristic
    nbins         Channel binning factors for each spw.
                  Format: 'spw1:nb1,spw2:nb2,...' with optional wildcards: '*:nb'

                  Examples: '9:2,11:4,13:2,15:8', '*:2'
    robust        Briggs robustness parameter
                  Values range from -2.0 (uniform) to 2.0 (natural)
    uvtaper       uv-taper on outer baselines
    clearlist     Clear any existing target list
    per_eb        Make an image target per EB
    per_session   Make an image target per session
    calcsb        Force (re-)calculation of sensitivities and beams
    datatype      Data type(s) to image. The default '' selects the best
                  available data type (e.g. selfcal over regcal) with
                  an automatic fallback to the next available data type.
                  With the ``datatype`` parameter one can force the use of only
                  given data type(s) without a fallback. The data type(s) are
                  specified as comma separated string of keywords. Accepted
                  values are the standard data types such as
                  'REGCAL_CONTLINE_ALL', 'REGCAL_CONTLINE_SCIENCE',
                  'SELFCAL_CONTLINE_SCIENCE', 'REGCAL_LINE_SCIENCE',
                  'SELFCAL_LINE_SCIENCE'. The shortcuts 'regcal' and 'selfcal'
                  are also accepted. They are expanded into the full data types
                  using the ``specmode`` parameter and the available data types for
                  the given MSes. In addition the strings 'best' and 'all' are
                  accepted, where 'best' means the above mentioned automatic
                  mode and 'all' means all available data types for a given
                  specmode. The data type strings are case insensitive.

                  Examples: 'selfcal', 'regcal, 'selfcal,regcal',
                            'REGCAL_LINE_SCIENCE,selfcal_line_science'
    datacolumn    Data column to image. Only to be used for manual overriding
                  when the automatic choice by data type is not appropriate.
    allow_wproject Allow the wproject heuristics for imaging
    parallel      Use MPI cluster where possible

    --------- examples -----------------------------------------------------------


    1. Make a list of science target images to be cleaned, one image per science
    spw.

    >>> hif_makeimlist()

    2. Make a list of PHASE and BANDPASS calibrator targets to be imaged,
    one image per science spw.

    >>> hif_makeimlist(intent='PHASE,BANDPASS')

    3. Make a list of PHASE calibrator images observed in spw 1, images limited to
    50 pixels on a side.

    >>> hif_makeimlist(intent='PHASE',spw='1',calmaxpix=50)


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
