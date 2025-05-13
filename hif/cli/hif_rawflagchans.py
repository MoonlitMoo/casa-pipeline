import sys

import pipeline.h.cli.utils as utils


@utils.cli_wrapper
def hif_rawflagchans(vis=None, spw=None, intent=None,
  flag_hilo=None, fhl_limit=None, fhl_minsample=None,
  flag_bad_quadrant=None, fbq_hilo_limit=None,
  fbq_antenna_frac_limit=None, fbq_baseline_frac_limit=None):

    """
    hif_rawflagchans ---- Flag deviant baseline/channels in raw data


    hif_rawflagchans flags deviant baseline/channels in the raw data.

    The flagging views used are derived from the raw data for the specified
    intent - default is BANDPASS.

    Bad baseline/channels are flagged for all intents, not just the
    one that is the basis of the flagging views.

    For each spectral window the flagging view is a 2d image with axes
    'channel' and 'baseline'. The pixel for each channel, baseline is the
    time average of the underlying unflagged raw data.

    The baseline axis is labeled by numbers of form id1.id2 where id1 and id2
    are the IDs of the baseline antennas. Both id1 and id2 run over all
    antenna IDs in the observation. This means that each baseline is shown
    twice but has the benefit that 'bad' antennas are easily identified by
    eye.

    Three flagging methods are available:

    If parameter ``flag_hilo`` is set True then outliers from the median
    of each flagging view will be flagged.

    If parameter ``flag_bad_quadrant`` is set True then a simple 2 part
    test is used to check for bad antenna quadrants and/or bad baseline
    quadrants. Here a 'quadrant' is defined simply as one quarter of the
    channel axis. The first part of the test is to note as 'suspect' those
    points further from the view median than ``fbq_hilo_limit`` * MAD.
    The second part is to flag entire antenna/quadrants if their
    fraction of suspect points exceeds ``fbq_antenna_frac_limit``.
    Failing that, entire baseline/quadrants may be flagged if their
    fraction of suspect points exceeds ``fbq_baseline_frac_limit``.
    Suspect points are not flagged unless as part of a bad antenna or
    baseline quadrant.


    Output

    results -- The results object for the pipeline task is returned.

    --------- parameter descriptions ---------------------------------------------

    vis                     List of input MeasurementSets.

                            default: [] - Use the MeasurementSets currently known to the pipeline
                            context.
    spw                     The list of spectral windows and channels to which the calibration
                            will be applied. Defaults to all science windows in the pipeline
                            context.

                            example: spw='17', spw='11, 15'
    intent                  A string containing the list of intents to be checked for antennas
                            with deviant gains. The default is blank, which causes the task to select
                            the 'BANDPASS' intent.

                            example: intent='`*BANDPASS*`'
    flag_hilo               True to flag channel/baseline data further from the view
                            median than fhl_limit * MAD.
    fhl_limit               If flag_hilo is True then flag channel/baseline data
                            further from the view median than fhl_limit * MAD.
    fhl_minsample           Do no flagging if the view median and MAD are derived
                            from fewer than fhl_minsample view pixels.
    flag_bad_quadrant       True to search for and flag bad antenna quadrants
                            and baseline quadrants. Here a /'quadrant/' is one
                            quarter of the channel axis.
    fbq_hilo_limit          If flag_bad_quadrant is True then channel/baselines further
                            from the view median than fbq_hilo_limit * MAD will be noted as
                            'suspect'. If there are enough of them to indicate that an antenna or
                            baseline quadrant is bad then all channel/baselines in that quadrant will
                            be flagged.
    fbq_antenna_frac_limit  If flag_bad_quadrant is True and the fraction of
                            suspect channel/baselines in a particular antenna/quadrant exceeds
                            fbq_antenna_frac_limit then all data for that antenna/quadrant will
                            be flagged.
    fbq_baseline_frac_limit If flag_bad_quadrant is True and the fraction of
                            suspect channel/baselines in a particular baseline/quadrant exceeds
                            fbq_baseline_frac_limit then all data for that baseline/quadrant will
                            be flagged.

    --------- examples -----------------------------------------------------------


    1. Flag bad quadrants and wild outliers, default method:

    >>> hif_rawflagchans()

    equivalent to:

    >>> hif_rawflagchans(flag_hilo=True, fhl_limit=20, flag_bad_quadrant=True, fbq_hilo_limit=8, 
    ...                  fbq_antenna_frac_limit=0.2, fbq_baseline_frac_limit=1.0)


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
