import sys

import pipeline.h.cli.utils as utils


@utils.cli_wrapper
def hifv_plotsummary(vis=None):

    """
    hifv_plotsummary ---- Create pipeline summary plots

    Create pipeline summary plots

    Output:

    results -- The results object for the pipeline task is returned.

    --------- parameter descriptions ---------------------------------------------

    vis           List of input visibility data

    --------- examples -----------------------------------------------------------


    1. Execute the pipeline plotting task.

    >>> hifv_plotsummary()


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
