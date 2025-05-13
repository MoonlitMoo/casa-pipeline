import sys

import pipeline.h.cli.utils as utils


@utils.cli_wrapper
def hifa_timegaincal(vis=None, calamptable=None, calphasetable=None, offsetstable=None, targetphasetable=None,
                     amptable=None, field=None, spw=None, antenna=None, calsolint=None, targetsolint=None, refant=None,
                     refantmode=None, solnorm=None, minblperant=None, calminsnr=None, targetminsnr=None, smodel=None):
    """
    hifa_timegaincal ---- Determine temporal gains from calibrator observations


    The time-dependent complex gains for each antenna/spwid are determined from
    the raw data (DATA column) divided by the model (MODEL column), for the
    specified fields. The gains are computed according to the spw-combination
    model determined in hifa_spwphaseup.

    Previous calibrations are applied on the fly.

    The complex gains are derived from the data column (raw data) divided by the
    model column (usually set with hif_setjy). The gains are obtained for the
    specified solution intervals, spw combination and field combination. One
    gain solution is computed for the science targets and one for the calibrator
    targets.

    Good candidate reference antennas can be determined using the hif_refant
    task.

    Previous calibrations that have been stored in the pipeline context are
    applied on the fly. Users can interact with these calibrations via the
    h_export_calstate and h_import_calstate tasks.

    Output:

        results -- The results object for the pipeline task is returned.

    --------- parameter descriptions ---------------------------------------------

    vis
                     The list of input MeasurementSets. Defaults to the list of
                     MeasurementSets specified in the pipeline context.

                     Example: vis=['M82A.ms', 'M82B.ms']
    calamptable
                     The list of output diagnostic calibration amplitude tables for
                     the calibration targets. Defaults to the standard pipeline naming
                     convention.

                     Example: calamptable=['M82.gacal', 'M82B.gacal']
    offsetstable
                     The list of output diagnostic phase offset tables for the
                     calibration targets. Defaults to the standard pipeline naming convention.

                     Example: offsetstable=['M82.offsets.gacal', 'M82B.offsets.gacal']
    calphasetable
                     The list of output calibration phase tables for the
                     calibration targets. Defaults to the standard pipeline naming convention.

                     Example: calphasetable=['M82.gpcal', 'M82B.gpcal']
    targetphasetable
                     The list of output phase calibration tables for the science
                     targets. Defaults to the standard pipeline naming convention.

                     Example: targetphasetable=['M82.gpcal', 'M82B.gpcal']
    amptable
                     The list of output calibration amplitude tables for the
                     calibration and science targets.
                     Defaults to the standard pipeline naming convention.

                     Example: amptable=['M82.gacal', 'M82B.gacal']
    field
                     The list of field names or field ids for which gain solutions are to
                     be computed. Defaults to all fields with the standard intent.

                     Example: field='3C279', field='3C279, M82'
    spw
                     The list of spectral windows and channels for which gain solutions are
                     computed. Defaults to all science spectral windows.

                     Example: spw='11', spw='11, 13'
    antenna
                     The selection of antennas for which gains are computed. Defaults to all.
    calsolint
                     Time solution interval in CASA syntax for calibrator source
                     solutions.

                     Example: calsolint='inf', calsolint='int', calsolint='100sec'
    targetsolint
                     Time solution interval in CASA syntax for target source
                     solutions.

                     Example: targetsolint='inf', targetsolint='int', targetsolint='100sec'
    refant
                     Reference antenna name(s) in priority order. Defaults to most recent
                     values set in the pipeline context. If no reference antenna is defined in
                     the pipeline context use the CASA defaults.

                     example: refant='DV01', refant='DV05,DV07'
    refantmode
                     Controls how the refant is applied. Currently available
                     choices are 'flex', 'strict', and the default value of ''.
                     Setting to '' allows the pipeline to select the appropriate
                     mode based on the state of the reference antenna list.

                     Examples: refantmode='strict', refantmode=''
    solnorm
                     Normalise the gain solutions.
    minblperant
                     Minimum number of baselines required per antenna for each solve.
                     Antennas with fewer baselines are excluded from solutions.

                     Example: minblperant=2
    calminsnr
                     Solutions below this SNR are rejected for calibrator solutions.
    targetminsnr
                     Solutions below this SNR are rejected for science target
                     solutions.
    smodel
                     Point source Stokes parameters for source model (experimental)
                     Defaults to using standard MODEL_DATA column data.

                     Example: smodel=[1,0,0,0]  - (I=1, unpolarized)

    --------- examples -----------------------------------------------------------

    1. Compute standard per scan gain solutions that will be used to calibrate
    the target:

    >>> hifa_timegaincal()

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
