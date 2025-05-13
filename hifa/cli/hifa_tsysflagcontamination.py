import sys

import pipeline.h.cli.utils as utils


@utils.cli_wrapper
def hifa_tsysflagcontamination(
    vis=None,
    caltable=None,
    filetemplate=None,
    logpath=None,
    remove_n_extreme=None,
    relative_detection_factor=None,
    diagnostic_plots=None,
    continue_on_failure=None,
):
    """
    hifa_tsysflagcontamination ---- Flag line contamination in ALMA interferometric Tsys caltables


    This task flags all line contamination detected through an analysis of the
    Tsys and bandpass caltables.

    The general idea for the detection algorithm is to discern features which
    appear in the Tsys calibration tables of the scans taken in the vicinity
    of the source field in comparison with the Tsys calibration tables of the
    scans taken toward the bandpass. The bandpass scan should be clean of
    astrophysical line features.


    Output:

        results -- The results object for the pipeline task is returned.

    --------- parameter descriptions ---------------------------------------------

    vis
                    List of input MeasurementSets (Not used).
    caltable
                    List of input Tsys calibration tables.

                    Default: [] - Use the table currently stored in the pipeline context.

                    Example: caltable=['X132.ms.tsys.s2.tbl']

    filetemplate
                    output file to which regions to flag will be written

    logpath
                    output file to which heuristic log statements will be
                    written

    remove_n_extreme
                    expert parameter for contamination heuristic

                    Default: 2

    relative_detection_factor
                    expert parameter for contamination detection heuristic

                    Default: 0.005

    diagnostic_plots
                    create diagnostic plots for the line contamination heuristic

                    Default: True

    continue_on_failure
                    controls whether pipeline execution continues if a failure
                    occurs in the underlying contamination detection heuristic.

                    Default: True

    --------- examples -----------------------------------------------------------

    1. Flag Tsys line contamination using currently recommended parameters:

    >>> hifa_tsysflagcontamination()

    2. Halt pipeline execution if a failure occurs in the underlying heuristic:

    >>> hifa_tsysflagcontamination(continue_on_failure=False)

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
