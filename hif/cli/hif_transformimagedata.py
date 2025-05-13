import sys

import pipeline.h.cli.utils as utils


@utils.cli_wrapper
def hif_transformimagedata(vis=None, outputvis=None, field=None, intent=None, spw=None, datacolumn=None, chanbin=None,
                           timebin=None, replace=None, clear_pointing=None, modify_weights=None, wtmode=None):

    """
    hif_transformimagedata ---- Extract fields for the desired VLASS image to a new MS and reset weights if desired

    Extract fields for the desired VLASS image to a new MS and reset weights if desired

    Output:

    results -- The results object for the pipeline task is returned.

    --------- parameter descriptions ---------------------------------------------

    vis            List of visibility data files. These may be ASDMs, tar files of ASDMs,
                   MSs, or tar files of MSs, If ASDM files are specified, they will be
                   converted  to MS format.

                   example: vis=['X227.ms', 'asdms.tar.gz']
    outputvis      The output MeasurementSet.
    field          Set of data selection field names or ids, \'\' for all.
    intent         Set of data selection intents, \'\' for all.
    spw            Set of data selection spectral window ids \'\' for all.
    datacolumn     Select spectral windows to split. The standard CASA options are
                   supported

                   example: 'data', 'model'
    chanbin        Bin width for channel averaging.
    timebin        Bin width for time averaging.
    replace        If a split was performed delete the parent MS and remove it from the context.

                   example: True or False
    clear_pointing Clear the pointing table.
    modify_weights Re-initialize the weights.
    wtmode         optional weight initialization mode when modify_weights=True

    --------- examples -----------------------------------------------------------


    1. Basic transformimagedata task

    >>> hif_transformimagedata()


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
