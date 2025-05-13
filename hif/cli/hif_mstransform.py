import sys

import pipeline.h.cli.utils as utils


@utils.cli_wrapper
def hif_mstransform(vis=None, outputvis=None, field=None, intent=None, spw=None, chanbin=None, timebin=None):

    """
    hif_mstransform ---- Create new MeasurementSets for science target imaging


    Create new MeasurementSets for imaging from the corrected column of the input
    MeasurementSet via a single call to mstransform with all data selection parameters.
    By default, all science target data is copied to the new MS. The
    new MeasurementSet is not re-indexed to the selected data and the new MS will
    have the same source, field, and spw names and ids as it does in the parent MS.

    Output

    results -- The results object for the pipeline task is returned.

    --------- parameter descriptions ---------------------------------------------

    vis           The list of input MeasurementSets. Defaults to the list of
                  MeasurementSets specified in the h_init or hif_importdata task.
                  '': use all MeasurementSets in the context

                  Examples: 'ngc5921.ms', ['ngc5921a.ms', ngc5921b.ms', 'ngc5921c.ms']
    outputvis     The list of output transformed MeasurementSets to be used for
                  imaging. The output list must be the same length as the input
                  list. The default output name defaults to
                  <msrootname>_targets.ms

                  Examples: 'ngc5921.ms',
                            ['ngc5921a.ms', ngc5921b.ms', 'ngc5921c.ms']
    field         Select fields name(s) or id(s) to transform. Only fields with
                  data matching the intent will be selected.

                  Examples: '3C279', 'Centaurus*', '3C279,J1427-421'
    intent        Select intents for which associated fields will be imaged.
                  By default only TARGET data is selected.

                  Examples: 'PHASE,BANDPASS'
    spw           Select spectral window/channels to image. By default all
                  science spws for which the specified intent is valid are
                  selected.
    chanbin       Width (bin) of input channels to average to form an output
                  channel. If chanbin > 1 then chanaverage is automatically
                  switched to True.
    timebin       Bin width for time averaging. If timebin > 0s then
                  timeaverage is automatically switched to True.

    --------- examples -----------------------------------------------------------


    1. Create a science target MS from the corrected column in the input MS.

    >>> hif_mstransform()

    2. Make a phase and bandpass calibrator targets MS from the corrected
    column in the input MS.

    >>> hif_mstransform(intent='PHASE,BANDPASS')


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
