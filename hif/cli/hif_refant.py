import sys

import pipeline.h.cli.utils as utils


@utils.cli_wrapper
def hif_refant(vis=None, field=None, spw=None, intent=None, hm_refant=None,
               refant=None, geometry=None, flagging=None, parallel=None,
               refantignore=None):

    """
    hif_refant ---- Select the best reference antennas


    The hif_refant task selects a list of reference antennas and stores them
    in the pipeline context in priority order.

    The priority order is determined by a weighted combination of scores derived
    by the antenna selection heuristics. In manual mode the reference antennas
    can be set by hand.

    Output:

    results -- The results object for the pipeline task is returned.

    --------- parameter descriptions ---------------------------------------------

    vis           The list of input MeasurementSets. Defaults to the list of MeasurementSets
                  in the pipeline context.
                  example: ['M31.ms']
    field         The comma delimited list of field names or field ids for which flagging
                  scores are computed if hm_refant='automatic' and  flagging = True
                  example: '' (Default to fields with the specified intents), '3C279', '3C279,M82'
    spw           A string containing the comma delimited list of spectral window ids for
                  which flagging scores are computed if hm_refant='automatic' and  flagging = True.
                  example: '' (all spws observed with the specified intents), '11,13,15,17'
    intent        A string containing a comma delimited list of intents against
                  which the selected fields are matched. Defaults to all supported
                  intents.
                  example: 'BANDPASS', 'AMPLITUDE,BANDPASS,PHASE,POLARIZATION'
    hm_refant     The heuristics method or mode for selection the reference
                  antenna. The options are 'manual' and 'automatic. In manual
                  mode a user supplied reference antenna refant is supplied.
                  In 'automatic' mode the antennas are selected automatically.
    refant        The user supplied reference antenna for hm_refant='manual'. If
                  no antenna list is supplied an empty list is returned.
                  example: 'DV05'
    geometry      Score antenna by proximity to the center of the array. This
                  option is quick as only the ANTENNA table must be read.
                  Parameter is available when ``hm_refant``='automatic'.
    flagging      Score antennas by percentage of unflagged data.  This option
                  requires computing flagging statistics.
                  Parameter is available when ``hm_refant``='automatic'.
    parallel      Execute using CASA HPC functionality, if available.
                  options: 'automatic', 'true', 'false', True, False
                  default: None (equivalent to False)
    refantignore  string list to be ignored as reference antennas.
                  example:  refantignore='ea02,ea03'

    --------- examples -----------------------------------------------------------



    1. Compute the references antennas to be used for bandpass and gain calibration.

    >>> hif_refant()


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
