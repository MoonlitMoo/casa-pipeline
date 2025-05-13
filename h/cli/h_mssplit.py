import sys

from . import utils


@utils.cli_wrapper
def h_mssplit(vis=None, outputvis=None, field=None, intent=None, spw=None, datacolumn=None, chanbin=None, timebin=None,
              replace=None):

    """
    h_mssplit ---- Select data from calibrated MS(s) to form new MS(s) for imaging

    Select data from calibrated MS(s) to form new MS(s) for imaging

    Create new MeasurementSets for imaging from the corrected column of the input
    MeasurementSet. By default all science target data is copied to the new MS. The new
    MeasurementSet is not re-indexed to the selected data in the new MS will have the
    same source, field, and spw names and ids as it does in the parent MS.

    The results object for the pipeline task is returned.

    --------- parameter descriptions ---------------------------------------------

    vis           The list of input MeasurementSets to be transformed. Defaults to the
                  list of MeasurementSets specified in the pipeline import data task.
                  default '': Split all MeasurementSets in the context.
                  example: 'ngc5921.ms', ['ngc5921a.ms', ngc5921b.ms', 'ngc5921c.ms']
    outputvis     The list of output split MeasurementSets. The output list must
                  be the same length as the input list and the output names must be different
                  from the input names.
                  default '', The output name defaults to <msrootname>_split.ms
                  example: 'ngc5921.ms', ['ngc5921a.ms', ngc5921b.ms', 'ngc5921c.ms']
    field         Set of data selection field names or ids, \'\' for all
    intent        Select intents to split
                  default: '', All data is selected.
                  example: 'TARGET'
    spw           Select spectral windows to split.
                  default: '', All spws are selected
                  example: '9', '9,13,15'
    datacolumn    Select spectral windows to split. The standard CASA options are supported.
                  example: 'corrected', 'model'
    chanbin       The channel binning factor. 1 for no binning, otherwise 2, 4, 8, or 16.
                  example: 2, 4
    timebin       The time binning factor. '0s' for no binning
                  example: '10s' for 10 second binning
    replace       If a split was performed delete the parent MS and remove it from the context.

    --------- examples -----------------------------------------------------------



    1. Create a 4X channel smoothed output MS from the input MS

    >>> h_mssplit(chanbin=4)


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
