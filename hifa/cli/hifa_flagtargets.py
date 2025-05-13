import sys

import pipeline.h.cli.utils as utils


@utils.cli_wrapper
def hifa_flagtargets(vis=None, template=None, filetemplate=None, flagbackup=None):
    """
    hifa_flagtargets ---- Do science target flagging


    The hifa_flagtargets task performs basic flagging operations on a list of
    science target MeasurementSets, including:

    - applying a flagging template

    Output:

        results -- The results object for the pipeline task is returned.

    --------- parameter descriptions ---------------------------------------------

    vis
                  The list of input MeasurementSets. Defaults to the list
                  of MeasurementSets defined in the pipeline context.
    template
                  Apply flagging templates; defaults to True.
    filetemplate
                  The name of a text file that contains the flagging
                  template for issues with the science target data etc.
                  If the template flags files is undefined a name of the
                  form 'msname_flagtargetstemplate.txt' is assumed.
    flagbackup
                  Back up any pre-existing flags; defaults to False.

    --------- examples -----------------------------------------------------------

    1. Do basic flagging on a science target MeasurementSet:

    >>> hifa_flagtargets()

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
