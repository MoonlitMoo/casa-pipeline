import sys

import pipeline.h.cli.utils as utils


@utils.cli_wrapper
def hifv_vlassmasking(vis=None, vlass_ql_database=None, maskingmode=None, catalog_search_size=None):

    """
    hifv_vlassmasking ---- Create clean masks for VLASS Single Epoch (SE) images

    Create clean masks for VLASS SE images

    Output:

    results -- The results object for the pipeline task is returned.


    --------- parameter descriptions ---------------------------------------------

    vis                 The list of input MeasurementSets. Defaults to the list of MeasurementSets
                        specified in the h_init or hifv_importdata task.
    vlass_ql_database   vlass_ql_database - usage in Socorro: /home/vlass/packages/VLASS1Q.fits
    maskingmode         maskingmode options are vlass-se-tier-1 or vlass-se-tier-2
    catalog_search_size catalog_search_size in units of degrees

    --------- examples -----------------------------------------------------------


    1. Basic vlassmasking task

    >>> hifv_vlassmasking()


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
