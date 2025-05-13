import sys

import pipeline.h.cli.utils as utils


@utils.cli_wrapper
def hifa_flagdata(vis=None, autocorr=None, shadow=None, tolerance=None, scan=None, scannumber=None, intents=None,
                  edgespw=None, fracspw=None, fracspwfps=None, online=None, partialpol=None, lowtrans=None,
                  mintransrepspw=None, mintransnonrepspws=None, fileonline=None, template=None, filetemplate=None,
                  hm_tbuff=None, tbuff=None, qa0=None, qa2=None, flagbackup=None):
    """
    hifa_flagdata ---- Do metadata based flagging of a list of MeasurementSets.


    The hifa_flagdata data performs basic flagging operations on a list of
    measurements including:

    - applying online flags
    - applying a flagging template
    - partial polarization flagging
    - autocorrelation data flagging
    - shadowed antenna data flagging
    - scan-based flagging by intent or scan number
    - edge channel flagging, as needed
    - low atmospheric transmission flagging

    About the spectral window edge channel flagging:

    - For TDM spectral windows, a number of edge channels are always flagged,
      according to the ``fracspw`` and ``fracspwfps`` parameters (the latter
      operates only on spectral windows with 62, 124, or 248 channels). With the
      default setting of ``fracspw``, the number of channels flagged on each edge
      is 2, 4, or 8 for 64, 128, or 256-channel spectral windows, respectively.

    - For most FDM spectral windows, no edge flagging is done. The only exceptions
      are ACA spectral windows that encroach too close to the baseband edge.
      Channels that lie closer to the baseband edge than the following values are
      flagged: 62.5, 40, 20, 10, and 5 MHz for spectral windows with bandwidths of
      1000, 500, 250, 125, and 62.5 MHz, respectively. A warning is generated in
      the weblog if flagging occurs due to proximity to the baseband edge.
      By definition, 2000 MHz spectral windows always encroach the baseband edge on
      both sides of the spectral window, and thus are always flagged on both sides
      in order to achieve 1875 MHz bandwidth (in effect, they are flagged by
      62.5 MHz on each side), and thus no warning is generated.

    Output:

        results -- The results object for the pipeline task is returned.

    --------- parameter descriptions ---------------------------------------------

    vis
                       The list of input MeasurementSets. Defaults to the list of
                       MeasurementSets defined in the pipeline context.
    autocorr
                       Flag autocorrelation data.
    shadow
                       Flag shadowed antennas.
    tolerance
                       Amount of antenna shadowing tolerated, in meters. A positive number
                       allows antennas to overlap in projection. A negative number forces antennas
                       apart in projection. Zero implies a distance of radius_1+radius_2 between
                       antenna centers.
    scan
                       Flag a list of specified scans.
    scannumber
                       A string containing a comma delimited list of scans to be
                       flagged.

                       Example: scannumber='3,5,6'
    intents
                       A string containing a comma delimited list of intents against
                       which the scans to be flagged are matched.

                       Example: intents='`*BANDPASS*`'
    edgespw
                       Flag the edge spectral window channels.
    fracspw
                       Fraction of channels to flag at both edges of TDM spectral windows.
    fracspwfps
                       Fraction of channels to flag at both edges of ACA TDM
                       spectral windows that were created with the earlier (original)
                       implementation of the frequency profile synthesis (FPS) algorithm.
    online
                       Apply the online flags.
    partialpol
                       Identify integrations in multi-polarisation data where part
                       of the polarization products are already flagged, and flag the other
                       polarization products in those integrations.
    lowtrans
                       Flag spectral windows for which a significant fraction of
                       the channels have atmospheric transmission below the
                       threshold (``mintransrepspw``, ``mintransnonrepspws``).
    mintransnonrepspws
                       This atmospheric transmissivity threshold is used to flag
                       a non-representative science spectral window when more than 60% of
                       its channels have a transmissivity below this level.
    mintransrepspw
                       This atmospheric transmissivity threshold is used to flag the
                       representative science spectral window when more than 60% of its channels
                       have a transmissivity below this level.
    fileonline
                       File containing the online flags. These are computed by the
                       h_init or hif_importdata data tasks. If the online flags files
                       are undefined a name of the form 'msname.flagonline.txt' is assumed.
    template
                       Apply flagging templates
    filetemplate
                       The name of a text file that contains the flagging template
                       for RFI, birdies, telluric lines, etc. If the template flags files
                       is undefined a name of the form 'msname.flagtemplate.txt' is assumed.
    hm_tbuff
                       The heuristic for computing the default time interval padding
                       parameter. The options are 'halfint' and 'manual'. In 'halfint' mode tbuff
                       is set to half the maximum of the median integration time of the science
                       and calibrator target observations. The value of 0.048 seconds is
                       subtracted from the lower time limit to accommodate the behavior of the
                       ALMA Control system.
    tbuff
                       The time in seconds used to pad flagging command time
                       intervals if ``hm_tbuff`` = 'manual'. The default in
                       manual mode is no flagging.
    qa0
                       QA0 flags.
    qa2
                       QA2 flags.
    flagbackup
                       Back up any pre-existing flags.

    --------- examples -----------------------------------------------------------

    1. Do basic flagging on a MeasurementSet:

    >>> hifa_flagdata()

    2. Do basic flagging on a MeasurementSet flagging additional scans selected
    by number as well:

    >>> hifa_flagdata(scannumber='13,18')

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
