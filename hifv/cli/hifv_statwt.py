import sys

import pipeline.h.cli.utils as utils


@utils.cli_wrapper
def hifv_statwt(vis=None, datacolumn=None, overwrite_modelcol=None, statwtmode=None):

    """
    hifv_statwt ---- Compute statistical weights and write them to measurement set

    Compute statistical weights and write them to measurement set


    Output:

    results -- The results object for the pipeline task is returned.

    --------- parameter descriptions ---------------------------------------------

    vis                The list of input MeasurementSets. Defaults to the list of MeasurementSets
                       specified in the h_init or hifv_importdata task.
    datacolumn         Data column used to compute weights. Supported values are
                       "data", "corrected", "residual", and "residual_data"
                       (case insensitive, minimum match supported).
    overwrite_modelcol Always write the model column, even if it already exists
    statwtmode         Sets the weighting parameters for general VLA ('VLA') or VLASS
                       Single Epoch ('VLASS-SE') use case. Note that the 'VLASS-SE'
                       mode is meant to be used with datacolumn='residual_data'.
                       Default is 'VLA'.

    --------- examples -----------------------------------------------------------


    1. Statistical weighting of the visibilities:

    >>> hifv_statwt()

    2. Statistical weighting of the visibilities in the Very Large Array Sky Survey Single Epoch use case:

    >>> hifv_statwt(mode='vlass-se', datacolumn='residual_data')


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
