import sys

import pipeline.h.cli.utils as utils


@utils.cli_wrapper
def hifv_flagdata(vis=None, autocorr=None, shadow=None, scan=None,
                  scannumber=None, quack=None, clip=None, baseband=None,
                  intents=None, edgespw=None, fracspw=None,
                  online=None, fileonline=None, template=None,
                  filetemplate=None, hm_tbuff=None, tbuff=None,
                  flagbackup=None):

    """
    hifv_flagdata ---- Do basic deterministic flagging of a list of MeasurementSets

    The hifv_flagdata task performs basic flagging operations on a list of MeasurementSets including:

    - autocorrelation data flagging
    - shadowed antenna data flagging
    - scan based flagging
    - edge channel flagging
    - baseband edge flagging
    - applying online flags
    - applying a flagging template
    - quack, shadow, and basebands
    - Antenna not-on-source (ANOS)

    Output:

    results -- The results object for the pipeline task is returned.

    --------- parameter descriptions ---------------------------------------------

    vis           The list of input MeasurementSets. Defaults to the list of MeasurementSets
                  specified in the h_init or hifv_importdata task.
    autocorr      Flag autocorrelation data
    shadow        Flag shadowed antennas
    scan          Flag specified scans
    scannumber    A string containing a  comma delimited list of scans to be flagged.
                  example: '3,5,6'
    quack         Quack scans
    clip          Clip mode
    baseband      Flag 20MHz of each edge of basebands
    intents       A string containing a comma delimited list of intents against
                  which the scans to be flagged are matched.
                  example: `'*BANDPASS*'`
    edgespw       Fraction of the baseline correlator TDM edge channels to be flagged.
    fracspw       Fraction of baseline correlator edge channels to be flagged
    online        Apply the online flags
    fileonline    File containing the online flags. These are computed by the
                  h_init or hif_importdata data tasks. If the online flags files
                  are undefined a name of the form 'msname.flagonline.txt' is assumed.
    template      Apply a flagging template
    filetemplate  The name of a text file that contains the flagging template
                  for RFI, birdies, telluric lines, etc.  If the template flags files
                  is undefined a name of the form 'msname.flagtemplate.txt' is assumed.
    hm_tbuff      The time buffer computation heuristic
    tbuff         List of time buffers (sec) to pad timerange in flag commands
    flagbackup    Backup pre-existing flags before applying new ones.

    --------- examples -----------------------------------------------------------


    1. Do basic flagging on a MeasurementSet

    >>> hifv_flagdata()

    2. Do basic flagging on a MeasurementSet as well as flag pointing and
    atmosphere data

    >>> hifv_flagdata(scan=True intent='*BANDPASS*')


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
