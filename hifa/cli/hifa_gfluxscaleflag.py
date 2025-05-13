import sys

import pipeline.h.cli.utils as utils


@utils.cli_wrapper
def hifa_gfluxscaleflag(vis=None, intent=None, phaseupsolint=None, solint=None, minsnr=None, refant=None,
                        antnegsig=None, antpossig=None, tmantint=None, tmint=None, tmbl=None, antblnegsig=None,
                        antblpossig=None, relaxed_factor=None, niter=None):
    """
    hifa_gfluxscaleflag ---- Flag the flux, diffgain, phase calibrators and check source


    This task computes the flagging heuristics on the flux, diffgain, and phase
    calibrators and the check source, by calling hif_correctedampflag which
    looks for outlier visibility points by statistically examining the scalar
    difference of corrected amplitudes minus model amplitudes, and flags those
    outliers. The philosophy is that only outlier data points that have remained
    outliers after calibration will be flagged. The heuristic works equally well
    on resolved calibrators and point sources because it is not performing a
    vector difference, and thus is not sensitive to nulls in the flux density
    vs. uvdistance domain. Note that the phase of the data is not assessed.

    In further detail, the workflow is as follows: a snapshot of the flagging
    state is preserved at the start, a preliminary phase and amplitude gaincal
    solution is solved and applied, the flagging heuristics are run and
    any outliers are marked for flagging, the flagging state is restored from the
    snapshot. If any outliers were found, then these are flagged. Plots are
    generated at two points in this workflow: after preliminary phase and
    amplitude calibration but before flagging heuristics are run, and after
    flagging heuristics have been run and applied. If no points were flagged,
    the 'after' plots are not generated or displayed. The score for this stage
    is the standard data flagging score, which depends on the fraction of data
    flagged.

    Output:

        results -- The results object for the pipeline task is returned.

    --------- parameter descriptions ---------------------------------------------

    vis
                   The list of input MeasurementSets. Defaults to the list of
                   MeasurementSets specified in the pipeline context.

                   Example: vis=['M51.ms']
    intent
                   A string containing a comma delimited list of intents against
                   which the selected fields are matched. If undefined (default), it
                   will select all data with the AMPLITUDE, PHASE, and CHECK intents,
                   except for one case: if one of the AMPLITUDE intent fields was also
                   used for BANDPASS, then this task will select only data with PHASE
                   and CHECK intents.

                   Example: intent='`*PHASE*`'
    phaseupsolint
                   The phase correction solution interval in CASA syntax.

                   Example: phaseupsolint='300s'
    solint
                   Time and channel solution intervals in CASA syntax.

                   Example: solint='inf,10ch', solint='inf'
    minsnr
                   Solutions below this SNR are rejected.
    refant
                   Reference antenna names. Defaults to the value(s) stored in the
                   pipeline context. If undefined in the pipeline context defaults to
                   the CASA reference antenna naming scheme.

                   Example: refant='DV01', refant='DV06,DV07'
    antnegsig
                   Lower sigma threshold for identifying outliers as a result of
                   bad antennas within individual timestamps.

                   Example: antnegsig=4.0
    antpossig
                   Upper sigma threshold for identifying outliers as a result of
                   bad antennas within individual timestamps.

                   Example: antpossig=4.6
    tmantint
                   Threshold for maximum fraction of timestamps that are allowed to
                   contain outliers.

                   Example: tmantint=0.063
    tmint
                   Threshold for maximum fraction of "outlier timestamps" over
                   "total timestamps" that a baseline may be a part of.

                   Example: tmint=0.085
    tmbl
                   Initial threshold for maximum fraction of "bad baselines" over "all
                   baselines" that an antenna may be a part of.

                   Example: tmbl=0.175
    antblnegsig
                   Lower sigma threshold for identifying outliers as a result of
                   "bad baselines" and/or "bad antennas" within baselines, across all
                   timestamps.

                   Example: antblnegsig=3.4
    antblpossig
                   Threshold for identifying outliers as a result of
                   "bad baselines" and/or "bad antennas" within baselines, across all
                   timestamps.

                   Example: antblpossig=3.2
    relaxed_factor
                   Relaxed value to set the threshold scaling factor to under
                   certain conditions (see task description).

                   Example: relaxed_factor=2.0
    niter
                   Maximum number of times to iterate on evaluation of flagging
                   heuristics. If an iteration results in no new flags, then subsequent
                   iterations are skipped.

                   Example: niter=2

    --------- examples -----------------------------------------------------------

    1. run with recommended settings to create flux scale calibration with flagging
    using recommended thresholds:

    >>> hifa_gfluxscaleflag()

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
