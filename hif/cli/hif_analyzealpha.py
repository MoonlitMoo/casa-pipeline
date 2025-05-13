import sys

import pipeline.h.cli.utils as utils


@utils.cli_wrapper
def hif_analyzealpha(vis=None, image=None, alphafile=None, alphaerrorfile=None):

    """
    hif_analyzealpha ---- Extract spectral index from intensity peak in VLA/VLASS images

    Extract spectral index from intensity peak in VLA/VLASS images

    The results object for the pipeline task is returned.

    --------- parameter descriptions ---------------------------------------------

    vis            List of visisbility  data files. These may be ASDMs, tar files of ASDMs,
                   MSs, or tar files of MSs, If ASDM files are specified, they will be
                   converted  to MS format.
                   example: vis=['X227.ms', 'asdms.tar.gz']
    image          Restored subimage
    alphafile      Input spectral index map
    alphaerrorfile Input spectral index error map

    --------- examples -----------------------------------------------------------



    1. Basic analyzealpha task

    >>> hif_analyzealpha()


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
