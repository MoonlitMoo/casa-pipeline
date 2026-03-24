import sys

import pipeline.h.cli.utils as utils


@utils.cli_wrapper
def hifv_aoflagger(vis=None, flag_target=None, aoflagger_file=None, datacolumn=None):
    """
    hifv_aoflagger ---- Run RFI flagging using aoflagger

    Run RFI flagging using aoflagger

    Output:

    results -- The results object for the pipeline task is returned.

    --------- parameter descriptions ---------------------------------------------

    vis                The list of input MeasurementSets. Defaults to the list of MeasurementSets
                       specified in the h_init or hifv_importdata task.
    flag_target        The field specified to run flagging on.
    aoflagger_file     The Lua file to use for flagging
    datacolumn         The column to use for flagging, [data, corrected, residual].

    --------- examples -----------------------------------------------------------


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
