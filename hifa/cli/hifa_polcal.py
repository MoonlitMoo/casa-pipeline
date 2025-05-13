import sys

import pipeline.h.cli.utils as utils


def hifa_polcal(vis=None, minpacov=None, solint_chavg=None, vs_stats=None, vs_thresh=None):
    """
    hifa_polcal ---- Derive instrumental polarization calibration for ALMA.


    Derive the instrumental polarization calibrations for ALMA using the
    polarization calibrators.

    Output:

        results -- The results object for the pipeline task is returned.

    --------- parameter descriptions ---------------------------------------------

    vis
                    The list of input MeasurementSets. Defaults to the list of
                    MeasurementSets specified in the pipeline context.

                    Example: ['M32A.ms', 'M32B.ms']
    minpacov
                    Minimum Parallactic Angle Coverage (degrees) required for
                    Q, U estimation in task polfromgain. This enables avoiding
                    pathological cases of antennas with insufficient parallactic
                    angle coverage which can yield spurious source polarization
                    solutions or cause polfromgain to fail to find any
                    solutions.

                    Default: 30.0
    solint_chavg
                    Channel averaging to include in solint for gaincal steps
                    producing cross-hand delay, cross-hand phase, and leakage
                    (D-terms) solutions.

                    Default: '5MHz'
    vs_stats
                    List of visstat statistics to use for diagnostic comparison
                    between the concatenated session MS and individual MSes in
                    that session after applying polarization calibration tables.

                    Default: ['min','max','mean']
    vs_thresh
                    Threshold to use in diagnostic comparison of visstat
                    statistics; relative differences larger than this threshold
                    are reported in the CASA log.

                    Default: 1e-3

    --------- examples -----------------------------------------------------------

    1. Compute the polarization calibrations:

    >>> hifa_polcal()

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
