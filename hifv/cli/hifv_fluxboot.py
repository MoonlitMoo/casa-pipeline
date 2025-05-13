import sys

import pipeline.h.cli.utils as utils


@utils.cli_wrapper
def hifv_fluxboot(vis=None, caltable=None, fitorder=None, refantignore=None, refant=None):

    """
    hifv_fluxboot ---- Fluxboot

    Determine flux density bootstrapping for gain calibrators relative to flux calibrator.

    Output:

    results -- The results object for the pipeline task is returned.

    --------- parameter descriptions ---------------------------------------------

    vis           The list of input MeasurementSets. Defaults to the list of MeasurementSets
                  specified in the h_init or hifv_importdata task.
    caltable      String name of the flagged caltable
    fitorder      Polynomial order of the spectral fitting for valid flux densities
                  with multiple spws.  The default value of -1 means that the heuristics determine the fit order based on
                  fractional bandwidth and receiver bands present in the observation.
                  An override value of 1,2,3 or 4 may be specified by the user.
                  Spectral index (1) and, if applicable, curvature (2) are reported in the weblog.
                  If no determination can be made by the heuristics, a fitorder of 1 will be used.
    refantignore  String list of antennas to ignore
                  Example:  refantignore='ea02,ea03'
    refant        A csv string of reference antenna(s). When used, disables ``refantignore``.
                  Example: refant = 'ea01, ea02'

    --------- examples -----------------------------------------------------------


    1. VLA CASA pipeline flux density bootstrapping.

    >>> hifv_fluxboot()


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
