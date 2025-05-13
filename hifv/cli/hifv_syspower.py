import sys

import pipeline.h.cli.utils as utils


@utils.cli_wrapper
def hifv_syspower(vis=None, clip_sp_template=None, antexclude=None, apply=None, do_not_apply=None):

    """
    hifv_syspower ---- Determine amount of gain compression affecting VLA data below Ku-band

    Determine amount of gain compression affecting VLA data below Ku-band

    Output:

    results -- The results object for the pipeline task is returned.

    --------- parameter descriptions ---------------------------------------------

    vis              List of input visibility data
    clip_sp_template Acceptable range for Pdiff data; data are clipped outside this range and flagged
    antexclude       dictionary in the format of:
                     {'L': {'ea02': {'usemedian': True}, 'ea03': {'usemedian': False}},
                      'X': {'ea02': {'usemedian': True}, 'ea03': {'usemedian': False}},
                      'S': {'ea12': {'usemedian': False}, 'ea22': {'usemedian': False}}}
                     If antexclude is specified with 'usemedian': False, the template values are replaced with 1.0.
                     If 'usemedian': True, the template values are replaced with the median of the good antennas.
    apply            Apply task results to RQ table
    do_not_apply     csv string of band names to not apply. Example: 'L,X,S'

    --------- examples -----------------------------------------------------------


    1. Basic syspower task

    >>> hifv_syspower()


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
