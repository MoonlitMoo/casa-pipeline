import sys

import pipeline.h.cli.utils as utils


@utils.cli_wrapper
def hifa_bandpassflag(vis=None, caltable=None, intent=None, field=None, spw=None, antenna=None, hm_phaseup=None,
                      phaseupsolint=None, phaseupbw=None, phaseupsnr=None, phaseupnsols=None, hm_bandpass=None,
                      solint=None, maxchannels=None, evenbpints=None, bpsnr=None, minbpsnr=None, bpnsols=None,
                      combine=None, refant=None, minblperant=None, minsnr=None, solnorm=None, antnegsig=None,
                      antpossig=None, tmantint=None, tmint=None, tmbl=None, antblnegsig=None, antblpossig=None,
                      relaxed_factor=None, niter=None, hm_auto_fillgaps=None):
    """
    hifa_bandpassflag ---- Bandpass calibration flagging


    This task performs a preliminary phased-up bandpass solution and temporarily
    applies it, then computes the flagging heuristics by calling
    hif_correctedampflag which looks for outlier visibility points by statistically
    examining the scalar difference of the corrected amplitudes minus model
    amplitudes, and then flags those outliers. The philosophy is that only outlier
    data points that have remained outliers after calibration will be flagged. Note
    that the phase of the data is not assessed.

    Plots are generated at two points in this workflow: after bandpass calibration
    but before flagging heuristics are run, and after flagging heuristics have been
    run and applied. If no points were flagged, the 'after' plots are not generated
    or displayed.

    Output:

        results -- The results object for the pipeline task is returned.

    --------- parameter descriptions ---------------------------------------------

    vis
                   List of input MeasurementSets. Defaults to the list of
                   MeasurementSets specified in the pipeline context.

                   Example: vis=['ngc5921.ms']
    caltable
                   List of names for the output calibration tables. Defaults
                   to the standard pipeline naming convention.

                   Example: caltable=['ngc5921.gcal']
    intent
                   A string containing a comma delimited list of intents against
                   which the selected fields are matched. Set to intent='' by default, which
                   means the task will select all data with the BANDPASS intent.

                   Example: intent='`*PHASE*`'
    field
                   The list of field names or field ids for which bandpasses are
                   computed. Set to field='' by default, which means the task
                   will select all fields.

                   Example: field='3C279', field='3C279,M82'
    spw
                   The list of spectral windows and channels for which bandpasses are
                   computed. Set to spw='' by default, which means the task will select all
                   science spectral windows.

                   Example: spw='11,13,15,17'
    antenna
                   Set of data selection antenna IDs
    hm_phaseup
                   The pre-bandpass solution phaseup gain heuristics. The options are:

                   'snr': compute solution required to achieve the specified SNR

                   'manual': use manual solution parameters

                   '': skip phaseup

                   Example: hm_phaseup='manual'
    phaseupsolint
                   The phase correction solution interval in CASA syntax.
                   Used when ``hm_phaseup`` = 'manual' or as a default if the
                   ``hm_phaseup`` = 'snr' heuristic computation fails.

                   Example: phaseupsolint='300s'
    phaseupbw
                   Bandwidth to be used for phaseup. Used when
                   ``hm_phaseup`` = 'manual'.

                   Example:

                   phaseupbw='' to use entire bandpass

                   phaseupbw='500MHz' to use central 500MHz
    phaseupsnr
                   The required SNR for the phaseup solution. Used only if
                   hm_phaseup='snr'.

                   Example: phaseupsnr=10.0
    phaseupnsols
                   The minimum number of phaseup gain solutions. Used only if
                   hm_phaseup='snr'.

                   Example: phaseupnsols=4
    hm_bandpass
                   The bandpass solution heuristics. The options are:

                   'snr': compute the solution required to achieve the specified SNR

                   'smoothed': simple 'smoothing' i.e. spectral solint>1chan

                   'fixed': use the user defined parameters for all spws
    solint
                   Time and channel solution intervals in CASA syntax.
                   Default is solint='inf', which is used when
                   ``hm_bandpass`` = 'fixed'.

                   If ``hm_bandpass`` = 'snr', then the task will attempt to
                   compute and use an optimal SNR-based solint (and warn if this
                   solint is not good enough).

                   If ``hm_bandpass`` = 'smoothed', the task will override the
                   spectral solint with bandwidth/maxchannels.
    maxchannels
                   The bandpass solution 'smoothing' factor in channels, i.e.
                   spectral solint will be set to bandwidth/maxchannels
                   Set to 0 for no smoothing.
                   Used if ``hm_bandpass`` = 'smoothed'.

                   Example: maxchannels=240
    evenbpints
                   Force the per spw frequency solint to be evenly divisible
                   into the spw bandpass if ``hm_bandpass`` = 'snr'.

                   Example: evenbpints=False
    bpsnr
                   The required SNR for the bandpass solution. Used only if
                   ``hm_bandpass`` = 'snr'.

                   Example: bpsnr=30.0
    minbpsnr
                   The minimum required SNR for the bandpass solution
                   when strong atmospheric lines exist in Tsys spectra.
                   Used only if ``hm_bandpass`` = 'snr'.

                   Example: minbpsnr=10.0
    bpnsols
                   The minimum number of bandpass solutions. Used only if
                   ``hm_bandpass`` = 'snr'.

                   Example: bpnsols=8
    combine
                   Data axes to combine for solving. Axes are '', 'scan', 'spw',
                   'field' or any comma-separated combination.

                   Example: combine='scan,field'
    refant
                   List of reference antenna names. Defaults to the value(s) stored in the
                   pipeline context. If undefined in the pipeline context defaults to
                   the CASA reference antenna naming scheme.

                   Example: refant='DV06,DV07'
    minblperant
                   Minimum number of baselines required per antenna for each solve.
                   Antennas with fewer baselines are excluded from solutions.

                   Example: minblperant=4
    minsnr
                   Solutions below this SNR are rejected.

                   Example: minsnr=3.0
    solnorm
                   Normalise the bandpass solution; defaults to True.
    antnegsig
                   Lower sigma threshold for identifying outliers as a result of
                   bad antennas within individual timestamps.

                   Example: antnegsig=4.0
    antpossig
                   Upper sigma threshold for identifying outliers as a result of
                   bad antennas within individual timestamps.

                   Example: antpossig=4.6
    tmantint
                   Threshold for maximum fraction of timestamps that are allowed
                   to contain outliers.

                   Example: tmantint=0.063
    tmint
                   Initial threshold for maximum fraction of 'outlier timestamps'
                   over 'total timestamps' that a baseline may be a part of.

                   Example: tmint=0.085
    tmbl
                   Initial threshold for maximum fraction of 'bad baselines' over
                   'all baselines' that an antenna may be a part of.

                   Example: tmbl=0.175
    antblnegsig
                   Lower sigma threshold for identifying outliers as a result of
                   'bad baselines' and/or 'bad antennas' within baselines (across all
                   timestamps).

                   Example: antblnegsig=3.4
    antblpossig
                   Upper sigma threshold for identifying outliers as a result of
                   'bad baselines' and/or 'bad antennas' within baselines (across all
                   timestamps).

                   Example: antblpossig=3.2
    relaxed_factor
                   Relaxed value to set the threshold scaling factor to under
                   certain conditions (see documentation of the underlying correctedampflag task).

                   Example: relaxed_factor=2.0
    niter
                   Maximum number of times to iterate on evaluation of flagging
                   heuristics. If an iteration results in no new flags, then subsequent
                   iterations are skipped.

                   Example: niter=2
    hm_auto_fillgaps
                   If True, then the ``hm_bandpass`` = 'snr' or 'smoothed'
                   modes, that solve bandpass per SpW, are performed with
                   CASA bandpass task parameter 'fillgaps' set to a quarter
                   of the respective SpW bandwidth (in channels).

                   If False, then these bandpass solves will use
                   fillgaps=0.

                   The ``hm_bandpass`` = 'fixed' mode is unaffected by
                   ``hm_auto_fillgaps`` and always uses fillgaps=0.

    --------- examples -----------------------------------------------------------

    1. run with recommended settings to create bandpass solution with flagging
    using recommended thresholds:

    >>> hifa_bandpassflag()

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
