import sys

import pipeline.h.cli.utils as utils


@utils.cli_wrapper
def hifv_hanning(vis=None, maser_detection=None):

    """
    hifv_hanning ---- Hanning smoothing on a dataset

    The hifv_hanning task will hanning smooth a VLA dataset



    Output:

    results -- The results object for the pipeline task is returned.


    --------- parameter descriptions ---------------------------------------------

    vis              The list of input MeasurementSets. Defaults to the list of MeasurementSets
                     specified in the h_init or hifv_importdata task.
    maser_detection  Run maser detect algorithm on spectral line windows. Defaults to True.

    --------- examples -----------------------------------------------------------


    1. Run the task to execute hanning smoothing on a VLA CASA pipeline loaded MeasurementSet.

    >>> hifv_hanning()


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
