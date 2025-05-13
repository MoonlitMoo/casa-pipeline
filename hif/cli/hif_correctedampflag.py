import sys

import pipeline.h.cli.utils as utils


@utils.cli_wrapper
def hif_correctedampflag(
        vis=None, intent=None, field=None, spw=None, antnegsig=None,
        antpossig=None, tmantint=None,
        tmint=None, tmbl=None, antblnegsig=None,
        antblpossig=None, relaxed_factor=None, niter=None):

    """
    hif_correctedampflag ---- Flag corrected - model amplitudes based on calibrators.


    Flag corrected - model amplitudes based on calibrators.

    This task computes the flagging heuristics on a calibrator by calling hif_correctedampflag
    which looks for outlier visibility points by statistically examining the scalar
    difference of corrected amplitudes minus model amplitudes, and flags those outliers.
    The philosophy is that only outlier data points that have remained outliers after
    calibration will be flagged. The heuristic works equally well on resolved calibrators
    and point sources because it is not performing a vector difference, and thus is not
    sensitive to nulls in the flux density vs. uvdistance domain. Note that the phase of
    the data is not assessed.

    Output

    results -- The results object for the pipeline task is returned.

    --------- parameter descriptions ---------------------------------------------

    vis            The list of input MeasurementSets. Defaults to the list of
                   MeasurementSets specified in the h_init or hif_importdata task.
                   '': use all MeasurementSets in the context

                   Examples: 'ngc5921.ms', ['ngc5921a.ms', ngc5921b.ms', 'ngc5921c.ms']
    intent         A string containing a comma delimited list of intents against
                   which the selected fields are matched. If undefined (default),
                   it will select all data with the BANDPASS intent.

                   Example: intent='`*PHASE*`'
    field          The list of field names or field ids for which bandpasses are
                   computed. If undefined (default), it will select all fields.

                   Examples: field='3C279', '3C279, M82'
    spw            The list of spectral windows and channels for which bandpasses
                   are computed. If undefined (default), it will select all
                   science spectral windows.

                   Example: spw='11,13,15,17'
    antnegsig      Lower sigma threshold for identifying outliers as a result of bad antennas within individual timestamps
    antpossig      Upper sigma threshold for identifying outliers as a result of bad antennas within individual timestamps
    tmantint       Threshold for maximum fraction of timestamps that are allowed to contain outliers
    tmint          Initial threshold for maximum fraction of "outlier timestamps" over "total timestamps" that a baseline may be a part of
    tmbl           Initial threshold for maximum fraction of "bad baselines" over "all timestamps" that an antenna may be a part of
    antblnegsig    Lower sigma threshold for identifying outliers as a result of "bad baselines" and/or "bad antennas" within baselines (across all timestamps)
    antblpossig    Upper sigma threshold for identifying outliers as a result of "bad baselines" and/or "bad antennas" within baselines (across all timestamps)
    relaxed_factor Relaxed value to set the threshold scaling factor to under certain conditions (see task description)
    niter          Maximum number of times to iterate on evaluation of flagging
                   heuristics. If an iteration results in no new flags, then
                   subsequent iterations are skipped.

    --------- examples -----------------------------------------------------------


    Run default flagging on bandpass calibrator with recommended settings:

    >>> hif_correctedampflag()


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
