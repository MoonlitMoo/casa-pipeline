import sys

import pipeline.h.cli.utils as utils


@utils.cli_wrapper
def hifa_renorm(vis=None, createcaltable=None, threshold=None, spw=None, excludechan=None,
                atm_auto_exclude=None, bwthreshspw=None, parallel=None):
    """
    hifa_renorm ---- ALMA renormalization task

    This task makes an assessment, and optionally applies a correction, to data
    suffering from incorrect amplitude normalization caused by bright
    astronomical lines detected in the autocorrelations of some target sources.

    For a full description of the effects of bright emission lines and the
    correction heuristics used in this task, please see the Pipeline User Guide.

    Output:

        results -- The results object for the pipeline task is returned.

    --------- parameter descriptions ---------------------------------------------

    vis
                     List of input MeasurementSets. Defaults to the list of
                     MeasurementSets specified in the pipeline context.

                     Example: vis=['ngc5921.ms']
    createcaltable
                     Boolean to select whether to create the renormalization
                     correction cal table (True), or only run the assessment
                     (False, default).

                     Example: createcaltable=True
    threshold
                     Apply correction if max correction is above this threshold
                     value and ``apply`` = True. Default is 1.02 (i.e. 2%).

                     Example: threshold=1.02
    spw
                     The list of real (not virtual - i.e. the actual spwIDs in
                     the MS) spectral windows to evaluate.

                     Set to spw='' by default, which means the task will select
                     all relevant (science FDM) spectral windows.

                     Note that for data with multiple MSs, a list with the
                     correct spectral window selection for each MS can be
                     provided.

                     Examples:

                     spw="11,13,15,17"

                     spw=["11,13,15,17", "5,7,11,13"]
    excludechan
                     Channels to exclude in either channel or frequency space
                     (TOPO, GHz), specifying the real (not virtual) spectral
                     window per selection.

                     Note that for data with multiple MSs, a list of
                     dictionaries with the correct selection for each MS can be
                     provided.

                     Examples:

                     excludechan={'22':'100~150;800~850', '24':'100~200'}

                     excludechan={'22':'230.1GHz~230.2GHz'}

                     excludechan=[{'22':'100~150'}, {'15':'100~150'}]
    atm_auto_exclude
                     Automatically find and exclude regions with atmospheric
                     features. Default is False
    bwthreshspw
                     Bandwidth beyond which a SPW is split into chunks to fit
                     separately. The default value for all SPWs is 120e6, and
                     this parameter allows one to override it for specific SPWs,
                     due to needing potentially various 'nsegments' when EBs
                     have very different SPW bandwidths.

                     Example: bwthreshspw={'16: 64e6, '22: 64e6}
    parallel
                     Execute using CASA HPC functionality, if available.

    --------- examples -----------------------------------------------------------

    1. Run with recommended settings to assess the need for an ALMA amplitude
    renormalization correction.

    >>> hifa_renorm()

    2. Run to assess the necessary ALMA amplitude renormalization correction,
    and apply this correction if it exceeds a threshold of 3% (1.03).

    >>> hifa_renorm(createcaltable=True, threshold=1.03)

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
