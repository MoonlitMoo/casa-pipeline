import sys

import pipeline.h.cli.utils as utils


@utils.cli_wrapper
def hifa_unlock_refant(vis=None):
    """
    hifa_unlock_refant ---- Unlock reference antenna list

    hifa_unlock_refant marks the reference antenna list as "unlocked" for
    specified measurement sets, allowing the list to be modified by subsequent
    tasks.

    After executing hifa_unlock_refant, all subsequent gaincal calls will by
    default be executed with refantmode='flex'.

    The refant list can be locked with the hifa_lock_refant task.

    Output:

        results -- The results object for the pipeline task is returned.

    --------- parameter descriptions ---------------------------------------------

    vis
                  List of input MeasurementSets. Defaults to the list of
                  MeasurementSets specified in the pipeline context.

                  Example: vis=['ngc5921.ms']

    --------- examples -----------------------------------------------------------

    1. Unlock the refant list for all MSes in pipeline context:

    >>> hifa_unlock_refant()

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
