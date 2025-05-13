import sys

import pipeline.h.cli.utils as utils


@utils.cli_wrapper
def hifa_targetflag(vis=None):
    """
    hifa_targetflag ---- Flag target source outliers


    This task flags very obvious target source outliers. The calibration tables and
    flags accumulated in the cal library up to this point are pre-applied, then
    hif_correctedampflag is called for just the TARGET intent. Any resulting
    flags are applied and the calibration library is restored to the state before
    calling this task.

    Because science targets are generally not point sources, the flagging algorithm
    needs to be more clever than for point source calibrators. The algorithm identifies
    outliers by examining statistics within successive overlapping radial uv bins,
    allowing it to adapt to an arbitrary uv structure. Outliers must appear to be a
    potential outlier in two bins in order to be declared an outlier.  To further avoid
    overflagging of good data, only the highest threshold levels are used (+12/-13 sigma).
    This stage does can add significant processing time, particularly in making the plots.
    So to save time, the amp vs. time plots are created only if flags are generated, and
    the amp vs. uv distance plots are made for only those spws that generated flags.
    Also, to avoid confusion in mosaics and single field surveys, the amp vs. uv distance
    plots only show field IDs with new flags.

    Output:

        results -- The results object for the pipeline task is returned.

    --------- parameter descriptions ---------------------------------------------

    vis
                  The list of input MeasurementSets. Defaults to the list of
                  MeasurementSets specified in the h_init or hif_importdata task.
                  '': use all MeasurementSets in the context

                  Examples: 'ngc5921.ms', ['ngc5921a.ms', ngc5921b.ms', 'ngc5921c.ms']

    --------- examples -----------------------------------------------------------

    1. Run with recommended settings to flag outliers in science target(s):

    >>> hifa_targetflag()

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
