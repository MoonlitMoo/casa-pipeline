import sys

import pipeline.h.cli.utils as utils


@utils.cli_wrapper
def hifa_session_refant(vis=None, phase_threshold=None):
    """
    hifa_session_refant ---- Select best reference antenna for session(s)


    This task re-evaluates the reference antenna lists from all measurement sets
    within a session and combines these to select a single common reference
    antenna (per session) that is to be used by any subsequent pipeline stages.

    Output:

        results -- The results object for the pipeline task is returned.

    --------- parameter descriptions ---------------------------------------------

    vis
                    List of input MeasurementSets. Defaults to the list of
                    MeasurementSets specified in the pipeline context.

                    Example: vis=['ngc5921.ms']
    phase_threshold
                    Threshold (in degrees) used to identify absolute phase
                    solution outliers in caltables.

                    Example: phase_threshold=0.005

    --------- examples -----------------------------------------------------------

    1. Compute a single common reference antenna per session:

    >>> hifa_session_refant()

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
