import sys

import pipeline.h.cli.utils as utils


@utils.cli_wrapper
def hifv_gaincurves(vis=None, caltable=None):

    """
    hifv_gaincurves ---- Runs gencal in gc mode

    Runs gencal in gc mode

    Output:

    results -- The results object for the pipeline task is returned.


    --------- parameter descriptions ---------------------------------------------

    vis           List of input visibility data
    caltable      String name of caltable

    --------- examples -----------------------------------------------------------


    1. Load an ASDM list in the ../rawdata subdirectory into the context.

    >>> hifv_gaincurves()





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
