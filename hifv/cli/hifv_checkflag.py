import sys

import pipeline.h.cli.utils as utils


@utils.cli_wrapper
def hifv_checkflag(vis=None, checkflagmode=None, growflags=None, overwrite_modelcol=None):

    """
    hifv_checkflag ---- Run RFI flagging using flagdata in various modes

    Run RFI flagging using flagdata in various modes

    Output:

    results -- The results object for the pipeline task is returned.

    --------- parameter descriptions ---------------------------------------------

    vis                The list of input MeasurementSets. Defaults to the list of MeasurementSets
                       specified in the h_init or hifv_importdata task.
    checkflagmode      -- Standard VLA modes with improved RFI flagging heuristics: 'bpd-vla', 'allcals-vla', 'target-vla'
                       -- blank string default use of rflag on bandpass and delay calibrators
                       -- use string 'semi' after hifv_semiFinalBPdcals() for executing rflag on calibrators
                       -- use string 'bpd', for the bandpass and delay calibrators:
                            execute rflag on all calibrated cross-hand corrected data;
                            extend flags to all correlations
                            execute rflag on all calibrated parallel-hand residual data;
                            extend flags to all correlations
                            execute tfcrop on all calibrated cross-hand corrected data,
                              per visibility; extend flags to all correlations
                            execute tfcrop on all calibrated parallel-hand corrected data,
                              per visibility; extend flags to all correlations
                       -- use string 'allcals', for all the other calibrators, with delays and BPcal applied:
                             similar procedure as 'bpd' mode, but uses corrected data throughout
                       -- use string 'target', for the target data:
                             similar procedure as 'allcals' mode, but with a higher SNR cutoff
                             for rflag to avoid flagging data due to source structure, and
                             with an additional series of tfcrop executions to make up for
                             the higher SNR cutoff in rflag
                       -- VLASS specific modes include 'bpd-vlass', 'allcals-vlass', and 'target-vlass'
                             which calculate thresholds to use per spw/field/scan (action='calculate', then,
                             per baseband/field/scan, replace all spw thresholds above the median with the median,
                             before re-running rflag with the new thresholds.  This has the effect of
                             lowering the thresholds for spws with RFI to be closer to the RFI-free
                             thresholds, and catches more of the RFI.
                       -- Mode 'vlass-imaging' is similar to 'target-vlass', except that it executes on the split off target
                             data, intent='*TARGET', datacolumn='data' and uses a timedevscale of 4.0.
    growflags          Grow flags in time at the end of the following checkflagmodes:
                       default=True, for 'bpd-vla', 'allcals-vla', 'bpd', and 'allcals'
                       default=False, for '' and 'semi'
    overwrite_modelcol Always write the model column, even if it already exists

    --------- examples -----------------------------------------------------------


    1. Run RFLAG with associated heuristics in the VLA CASA pipeline.

    >>> hifv_checkflag()


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
