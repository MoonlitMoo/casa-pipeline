import sys

from . import utils


@utils.cli_wrapper
def h_tsyscal(vis=None, caltable=None, chantol=None):

    """
    h_tsyscal ---- Derive a Tsys calibration table


    Derive the Tsys calibration for list of ALMA MeasurementSets.

    Output:

    results -- The results object for the pipeline task is returned.

    --------- parameter descriptions ---------------------------------------------

    vis           List of input visibility files.
                  example: vis=['ngc5921.ms']
    caltable      Name of output gain calibration tables.
                  example: caltable='ngc5921.gcal'
    chantol       The tolerance in channels for mapping atmospheric calibration
                  windows (TDM) to science windows (FDM or TDM).
                  example: chantol=5

    --------- examples -----------------------------------------------------------

    1. Standard call

    >>> h_tsyscal()

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
