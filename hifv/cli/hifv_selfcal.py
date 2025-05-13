import sys

import pipeline.h.cli.utils as utils


@utils.cli_wrapper
def hifv_selfcal(vis=None, refantignore=None,
                 combine=None, selfcalmode=None, refantmode=None, overwrite_modelcol=None):

    """
    hifv_selfcal ---- Perform phase-only self-calibration, per scan row, on VLASS SE images

    Perform phase-only self-calibration, per scan row, on VLASS SE images

    Output:
    results -- The results object for the pipeline task is returned.

    --------- parameter descriptions ---------------------------------------------

    vis                The list of input MeasurementSets. Defaults to the list of MeasurementSets
                       specified in the h_init or hifv_importdata task.
    refantignore       String list of antennas to ignore
    combine            Data axes which to combine for solve
                       Options: '','obs','scan','spw',field', or any
                       comma-separated combination in a single string

                       Example: combine='scan,spw' - Extend solutions
                       over scan boundaries (up to the solint), and
                       combine spws for solving.

                       In selfcalmode='VLASS-SE' use the default value.
    selfcalmode        Heuristics mode selection. Known modes are 'VLASS' and 'VLASS-SE'.
                       Default value is 'VLASS'.
    refantmode         Reference antenna mode
    overwrite_modelcol Always write the model column, even if it already exists

    --------- examples -----------------------------------------------------------


    1. Basic selfcal task

    >>> hifv_selfcal()

    2. VLASS-SE selfcal usage

    >>> hifv_selfcal(selfcalmode='VLASS-SE', combine='field,spw')


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
