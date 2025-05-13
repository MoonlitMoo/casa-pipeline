import sys

import pipeline.h.cli.utils as utils


@utils.cli_wrapper
def hif_makecutoutimages(vis=None, offsetblc=None, offsettrc=None):

    """
    hif_makecutoutimages ---- Cutout central 1 sq. degree from VLASS QL, SE, and Coarse Cube images


    Cutout central 1 sq. degree from VLASS QL, SE, and Coarse Cube images

    Output:

    results -- The results object for the pipeline task is returned.

    --------- parameter descriptions ---------------------------------------------

    vis           List of visibility data files.
                  These may be ASDMs, tar files of ASDMs, MSs,
                  or tar files of MSs.
                  If ASDM files are specified, they will be converted to
                  MS format.

                  example: vis=['X227.ms', 'asdms.tar.gz']
    offsetblc     -x and -y offsets to the bottom lower corner (blc)
                  in arcseconds
    offsettrc     +x and +y offsets to the top right corner (trc)
                  in arcseconds

    --------- examples -----------------------------------------------------------


    1. Basic makecutoutimages task

    >>> hif_makecutoutimages()


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
