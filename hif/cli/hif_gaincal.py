import sys

import pipeline.h.cli.utils as utils


@utils.cli_wrapper
def hif_gaincal(vis=None, caltable=None, field=None, intent=None, spw=None, antenna=None, hm_gaintype=None,
                calmode=None, solint=None, combine=None, refant=None, refantmode=None, solnorm=None, minblperant=None,
                minsnr=None, smodel=None, splinetime=None, npointaver=None, phasewrap=None):

    """
    hif_gaincal ---- Determine temporal gains from calibrator observations


    The complex gains are derived from the data column (raw data) divided by the
    model column (usually set with hif_setjy). The gains are obtained for a
    specified solution interval, spw combination and field combination.

    Good candidate reference antennas can be determined using the hif_refant
    task.

    Previous calibrations that have been stored in the pipeline context are
    applied on the fly. Users can interact with these calibrations via the
    h_export_calstate and h_import_calstate tasks.

    Output

    results -- The results object for the pipeline task is returned.

    --------- parameter descriptions ---------------------------------------------

    vis           The list of input MeasurementSets. Defaults to the list of
                  MeasurementSets specified in the h_init or hif_importdata task.
                  '': use all MeasurementSets in the context

                  Examples: 'ngc5921.ms', ['ngc5921a.ms', ngc5921b.ms', 'ngc5921c.ms']
    caltable      The list of output calibration tables. Defaults to the
                  standard pipeline naming convention.

                  Example: caltable=['M82.gcal', 'M82B.gcal']
    field         The list of field names or field ids for which gain solutions
                  are to be computed. Defaults to all fields with the standard
                  intent.

                  Example: field='3C279', field='3C279, M82'
    intent        A string containing a comma delimited list of intents against
                  which the selected fields are matched. Defaults to `*PHASE*`.

                  Examples: intent='', intent='`*AMP*,*PHASE*`'
    spw           The list of spectral windows and channels for which gain
                  solutions are computed. Defaults to all science spectral
                  windows.

                  Examples: spw='21', spw='21, 23'
    antenna       Set of data selection antenna ids
    hm_gaintype   The type of gain calibration. The options are 'gtype' and
                  'gspline' for CASA gain types = 'G' and 'GSPLINE' respectively.
    calmode       Type of solution. The options are 'ap' (amp and phase), 'p'
                  (phase only) and 'a' (amp only).

                  Examples: calmode='p', calmode='a', calmode='ap'
    solint        Time solution intervals in CASA syntax. Works for
                  hm_gaintype='gtype' only.

                  Examples: solint='inf', solint='int', solint='100sec'
    combine       Data axes to combine for solving. Options are  '', 'scan',
                  'spw', 'field' or any comma-separated combination. Works for
                  hm_gaintype='gtype' only.
    refant        Reference antenna name(s) in priority order. Defaults to most
                  recent values set in the pipeline context. If no reference
                  antenna is defined in the pipeline context use the CASA
                  defaults.

                  Examples: refant='DV01', refant='DV05,DV07'
    refantmode    Controls how the refant is applied. Currently available
                  choices are 'flex', 'strict', and the default value of ''.
                  Setting to '' allows the pipeline to select the appropriate
                  mode based on the state of the reference antenna list.

                  Examples: refantmode='strict', refantmode=''
    solnorm       Normalize average solution amplitudes to 1.0
    minblperant   Minimum number of baselines required per antenna for each
                  solve. Antennas with fewer baselines are excluded from
                  solutions. Works for hm_gaintype='gtype' only.
    minsnr        Solutions below this SNR are rejected. Works for
                  hm_gaintype='channel' only.
    smodel        Point source Stokes parameters for source model (experimental).
                  Defaults to using standard MODEL_DATA column data.

                  Example: smodel=[1,0,0,0]  - (I=1, unpolarized)
    splinetime    Spline timescale (sec). Used for hm_gaintype='gspline'.
                  Typical splinetime should cover about 3 to 5 calibrator scans.
    npointaver    Tune phase-unwrapping algorithm. Used for
                  hm_gaintype='gspline'. Keep at default value.
    phasewrap     Wrap the phase for changes larger than this amount (degrees).
                  Used for hm_gaintype='gspline'. Keep at default value.

    --------- examples -----------------------------------------------------------


    Compute standard per scan gain solutions that will be used to calibrate
    the target:

    >>> hif_gaincal()


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
