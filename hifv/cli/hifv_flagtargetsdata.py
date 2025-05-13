import sys

import pipeline.h.cli.utils as utils


@utils.cli_wrapper
def hifv_flagtargetsdata(vis=None, template=None, filetemplate=None, flagbackup=None):

    """
    hifv_flagtargetsdata ---- Apply a flagtemplate to target data prior to running imaging pipeline tasks

    Apply a flagtemplate to target data prior to running imaging pipeline tasks

    Output:

    results -- The results object for the pipeline task is returned.

    --------- parameter descriptions ---------------------------------------------

    vis           The list of input MeasurementSets. Defaults to the list
                  of MeasurementSets defined in the pipeline context.
    template      Apply flagging templates.
    filetemplate  The name of a text file that contains the flagging
                  template for issues with the science target data etc.
                  If the template flags files is undefined a name of the
                  form 'msname_flagtargetstemplate.txt' is assumed.
    flagbackup    Back up any pre-existing flags.

    --------- examples -----------------------------------------------------------


    1. Basic flagtargetsdata task

    >>> hifv_flagtargetsdata()


    """


    #                                                                        #
    #  CASA task interface boilerplate code starts here. No edits should be  #
    #  needed beyond this point.                                             #
    #                                                                        #

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
