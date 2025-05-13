import sys

from . import utils


@utils.cli_wrapper
def h_applycal(vis=None, field=None, intent=None, spw=None, antenna=None, parang=None, applymode=None, flagbackup=None,
               flagsum=None, flagdetailedsum=None, parallel=None):

    """
    h_applycal ---- Apply the calibration(s) to the data


    Apply precomputed calibrations to the data.

    h_applycal applies the precomputed calibration tables stored in the pipeline
    context to the set of visibility files using predetermined field and
    spectral window maps and default values for the interpolation schemes.

    Users can interact with the pipeline calibration state using the tasks
    h_export_calstate and h_import_calstate.

    Output:

    results -- The results object for the pipeline task is returned

    --------- parameter descriptions ---------------------------------------------

    vis             The list of input MeasurementSets. Defaults to the list of MeasurementSets
                    in the pipeline context.
                    example: ['X227.ms']
    field           A string containing the list of field names or field ids to which
                    the calibration will be applied. Defaults to all fields in the pipeline
                    context.
                    example: '3C279', '3C279, M82'
    intent          A string containing the list of intents against which the
                    selected fields will be matched. Defaults to all supported intents
                    in the pipeline context.
                    example: `'*TARGET*'`
    spw             The list of spectral windows and channels to which the calibration
                    will be applied. Defaults to all science windows in the pipeline
                    context.
                    example: '17', '11, 15'
    antenna         The selection of antennas to which the calibration will be applied.
                    Defaults to all antennas. Not currently supported.
    parang          Apply parallactic angle correction
    applymode       Calibration apply mode
                    'calflag': calibrate data and apply flags from solutions
                    'calflagstrict': (default) same as above except flag spws for which calibration is
                        unavailable in one or more tables (instead of allowing them to pass
                        uncalibrated and unflagged)
                    'trial': report on flags from solutions, dataset entirely unchanged
                    'flagonly': apply flags from solutions only, data not calibrated
                    'flagonlystrict': same as above except flag spws for which calibration is
                        unavailable in one or more tables
                    'calonly': calibrate data only, flags from solutions NOT applied
    flagbackup      Backup the flags before the apply
    flagsum         Compute before and after flagging summary statistics
    flagdetailedsum Compute detailed before and after flagging statistics summaries.
                    Parameter available only when if flagsum is True.
    parallel        Execute using CASA HPC functionality, if available.
                    options: 'automatic', 'true', 'false', True, False
                    default: None (equivalent to False)

    --------- examples -----------------------------------------------------------



    1. Apply the calibration to the target data

    >>> hif_applycal (intent='TARGET')


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
