import sys

import pipeline.h.cli.utils as utils


@utils.cli_wrapper
def hsd_flagdata(vis=None, autocorr=None, shadow=None, scan=None,
                 scannumber=None, intents=None, edgespw=None, fracspw=None,
                 fracspwfps=None, online=None, fileonline=None, template=None,
                 filetemplate=None, pointing=None, filepointing=None, incompleteraster=None,
                 hm_tbuff=None, tbuff=None, qa0=None, qa2=None, parallel=None,
                 flagbackup=None):

    """
    hsd_flagdata ---- Do basic flagging of a list of MeasurementSets


    The hsd_flagdata data performs basic flagging operations on a list of
    MeasurementSets including:

    - applying online flags
    - applying a flagging template
    - shadowed antenna data flagging
    - scan-based flagging by intent or scan number
    - edge channel flagging

    Output:

    results -- The results object for the pipeline task is returned.

    --------- parameter descriptions ---------------------------------------------

    vis              The list of input MeasurementSets. Defaults to the list of MeasurementSets
                     defined in the pipeline context.
    autocorr         Flag autocorrelation data.
    shadow           Flag shadowed antennas.
    scan             Flag a list of scans and intents specified by scannumber and intents.
    scannumber       A string containing a comma delimited list of scans to be
                     flagged.
    intents          A string containing a comma delimited list of intents against
                     which the scans to be flagged are matched.

                     example: `'*BANDPASS*'`
    edgespw          Flag the edge spectral window channels.
    fracspw          Fraction of the baseline correlator TDM edge channels to be flagged.
    fracspwfps       Fraction of the ACS correlator TDM edge channels to be flagged.
    online           Apply the online flags.
    fileonline       File containing the online flags. These are computed by the
                     h_init or hif_importdata data tasks. If the online flags files
                     are undefined a name of the form 'msname.flagonline.txt' is assumed.
    template         Apply a flagging template.
    filetemplate     The name of a text file that contains the flagging template
                     for RFI, birdies, telluric lines, etc.  If the template flags files
                     is undefined a name of the form 'msname.flagtemplate.txt' is assumed.
    pointing         Apply a flagging template for pointing flag.
    filepointing     The name of a text file that contains the flagging template
                     for pointing flag. If the template flags files is undefined a name of
                     the form 'msname.flagpointing.txt' is assumed.
    incompleteraster Apply commands to flag incomplete raster sequence.
                     If this is False, relevant commands in filepointing are
                     simply commented out.
    hm_tbuff         The heuristic for computing the default time interval padding parameter.
                     The options are 'halfint' and 'manual'. In 'halfint' mode tbuff is set to
                     half the maximum of the median integration time of the science and calibrator target
                     observations.
    tbuff            The time in seconds used to pad flagging command time intervals if
                     hm_tbuff='manual'.
    qa0              QA0 flags
    qa2              QA2 flags
    parallel         Execute using CASA HPC functionality, if available.
                     options: 'automatic', 'true', 'false', True, False
                     default: None (equivalent to 'automatic')
    flagbackup       Back up any pre-existing flags before applying new ones.

    --------- examples -----------------------------------------------------------

    1. Do basic flagging on a MeasurementSet

    >>> hsd_flagdata()

    2. Do basic flagging on a MeasurementSet flagging additional scans selected
    by number as well.

    >>> hsd_flagdata(scannumber='13,18')

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
