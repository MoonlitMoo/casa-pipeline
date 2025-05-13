import sys

import pipeline.h.cli.utils as utils


@utils.cli_wrapper
def hifv_testBPdcals(vis=None, weakbp=None, refantignore=None, doflagundernspwlimit=None, refant=None):

    """
    hifv_testBPdcals ---- Runs initial delay and bandpass calibration to setup for RFI flagging

    Runs initial delay and bandpass calibration to setup for RFI flagging

    Output:

    results -- The results object for the pipeline task is returned.
    --------- parameter descriptions ---------------------------------------------

    vis                  The list of input MeasurementSets. Defaults to the list of MeasurementSets
                         specified in the h_init or hifv_importdata task.
    weakbp               Activate weak bandpass heuristics
    refantignore         String list of antennas to ignore
                         Example:  refantignore='ea02,ea03'
    doflagundernspwlimit If the number of bad spws is greater than zero, and the keyword is True, then spws are flagged individually.
    refant               A csv string of reference antenna(s). When used, disables ``refantignore``.
                         Example: refant = 'ea01, ea02'

    --------- examples -----------------------------------------------------------


    1. Initial delay calibration to set up heuristic flagging.

    >>> hifv_testBPdcals()


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
