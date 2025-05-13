import sys

import pipeline.h.cli.utils as utils


@utils.cli_wrapper
def hifv_circfeedpolcal(vis=None, Dterm_solint=None, refantignore=None, leakage_poltype=None, mbdkcross=None,
                        clipminmax=None, refant=None, run_setjy=None):

    """
    hifv_circfeedpolcal ---- Perform polarization calibration for VLA circular feeds.

    Perform polarization calibration for VLA circular feeds.

    Only validated for VLA sky survey data in S-band continuum mode with 3C138
    or 3C286 as polarization angle. Requires that all polarization intents are
    properly set during observation.

    Output:

    results -- The results object for the pipeline task is returned.

    --------- parameter descriptions ---------------------------------------------

    vis             List of input visibility data
    Dterm_solint    D-terms spectral averaging.  Example:  refantignore='ea02,ea03'
    refantignore    String list of antennas to ignore
    leakage_poltype poltype to use in first polcal execution - blank string means use default heuristics
    mbdkcross       Run gaincal KCROSS grouped by baseband
    clipminmax      Acceptable range for leakage amplitudes, values outside will be flagged.
    refant          A csv string of reference antenna(s). When used, disables ``refantignore``.
                    Example: refant = 'ea01, ea02'
    run_setjy       Run setjy for amplitude/flux calibrator, default set to True.
    --------- examples -----------------------------------------------------------


    1. Basic circfeedpolcal task

    >>> hifv_circfeedpolcal()


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
