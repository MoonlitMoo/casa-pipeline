import sys

import pipeline.h.cli.utils as utils


@utils.cli_wrapper
def hifa_wvrgcalflag(vis=None, caltable=None, offsetstable=None, hm_toffset=None, toffset=None, segsource=None,
                     sourceflag=None, hm_tie=None, tie=None, nsol=None, disperse=None, wvrflag=None, hm_smooth=None,
                     smooth=None, scale=None, maxdistm=None, minnumants=None, mingoodfrac=None, refant=None,
                     flag_intent=None, qa_intent=None, qa_bandpass_intent=None, accept_threshold=None, flag_hi=None,
                     fhi_limit=None, fhi_minsample=None, ants_with_wvr_thresh=None):
    """
    hifa_wvrgcalflag ----
    Generate a gain table based on Water Vapor Radiometer data, interpolating over
    antennas with bad radiometers.


    This task will first identify for each vis whether it includes at least 3
    antennas with Water Vapor Radiometer (WVR) data, and that the fraction of
    WVR antennas / all antennas exceeds the minimum threshold
    (ants_with_wvr_thresh).

    If there are not enough WVR antennas by number and/or fraction, then no WVR
    caltable is created and no WVR calibration will be applied to the corresponding
    vis. If there are enough WVR antennas, then the task proceeds as follows for
    each valid vis:

    First, generate a gain table based on the Water Vapor Radiometer data for
    each vis.

    Second, apply the WVR calibration to the data specified by 'flag_intent',
    calculate flagging 'views' showing the ratio
    'phase-rms with WVR / phase-rms without WVR' for each scan. A ratio < 1
    implies that the phase noise is improved, a ratio > 1 implies that it
    is made worse.

    Third, search the flagging views for antennas with anomalous high values.
    If any are found then recalculate the WVR calibration with the 'wvrflag'
    parameter set to ignore their data and interpolate results from other
    antennas according to 'maxdistm' and 'minnumants'.

    Fourth, after flagging, if the remaining unflagged antennas with WVR number
    fewer than 3, or represent a smaller fraction of antennas than the minimum
    threshold (ants_with_wvr_thresh), then the WVR calibration file is rejected
    and will not be merged into the context, i.e. not be used in subsequent
    calibration.

    Fifth, if the overall QA score for the final WVR correction of a vis file
    is greater than the value in 'accept_threshold' then make available the
    wvr calibration file for merging into the context and use in the
    subsequent reduction.

    Output:

        results -- The results object for the pipeline task is returned.

    --------- parameter descriptions ---------------------------------------------

    vis
                         List of input visibility files.

                         Default: none, in which case the vis files to be used
                         will be read from the context.

                         Example: vis=['ngc5921.ms']
    caltable
                         List of output gain calibration tables.

                         Default: none, in which case the names of the caltables
                         will be generated automatically.

                         Example: caltable='ngc5921.wvr'
    offsetstable
                         List of input temperature offsets table files to
                         subtract from WVR measurements before calculating phase
                         corrections.

                         Default: none, in which case no offsets are applied.

                         Example: offsetstable=['ngc5921.cloud_offsets']
    hm_toffset
                         If 'manual', set the 'toffset' parameter to the user-specified
                         value. If 'automatic', set the 'toffset' parameter according to the
                         date of the MeasurementSet; toffset=-1 if before 2013-01-21T00:00:00
                         toffset=0 otherwise.
    toffset
                         Time offset (sec) between interferometric and WVR data.
    segsource
                         If True calculate new atmospheric phase correction
                         coefficients for each source, subject to the
                         constraints of the ``tie`` parameter. ``segsource`` is
                         forced to be True if the ``tie`` parameter is set to a
                         non-empty value by the user or by the automatic
                         heuristic.
    sourceflag
                         Flag the WVR data for these source(s) as bad and do not
                         produce corrections for it. Requires
                         ``segsource`` = True.

                         Example: sourceflag=['3C273']
    hm_tie
                         If 'manual', set the ``tie`` parameter to the
                         user-specified value. If 'automatic', set the ``tie``
                         parameter to include with the target all calibrators
                         that are within 15 degrees of it: if no calibrators are
                         that close then ``tie`` is left empty.
    tie
                         Use the same atmospheric phase correction coefficients
                         when calculating the WVR correction for all sources in
                         the ``tie``. If ``tie`` is not empty then ``segsource``
                         is forced to be True. Ignored unless ``hm_tie`` = 'manual'.

                         Example: tie=['3C273,NGC253', 'IC433,3C279']
    nsol
                         Number of solutions for phase correction coefficients during this
                         observation, evenly distributed in time throughout the observation. It
                         is used only if segsource=False because if segsource=True then the
                         coefficients are recomputed whenever the telescope moves to a new
                         source (within the limits imposed by 'tie').
    disperse
                         Apply correction for dispersion.
                         (Deprecated; will be removed)
    wvrflag
                         Flag the WVR data for these antenna(s) as bad and replace its data
                         with interpolated values.

                         Example: wvrflag=['DV03','DA05','PM02']
    hm_smooth
                         If 'manual' set the 'smooth' parameter to the user-specified value.
                         If 'automatic', run the wvrgcal task with the range of 'smooth' parameters
                         required to match the integration time of the WVR data to that of the
                         interferometric data in each spectral window.
    smooth
                         Smooth WVR data on this timescale before calculating the correction.
                         Ignored unless hm_smooth='manual'.
    scale
                         Scale the entire phase correction by this factor.
    maxdistm
                         Distance in meters of an antenna used for interpolation
                         from a flagged antenna.

                         Default: -1 (automatically set to 100m if >50% of
                         antennas are 7m antennas without WVR and otherwise set
                         to 500m).

                         Example: maxdistm=550
    minnumants
                         Minimum number of nearby antennas (up to 3) used for
                         interpolation from a flagged antenna.

                         Example: minnumants=3
    mingoodfrac
                         Minimum fraction of good data per antenna.

                         Example: mingoodfrac=0.7
    refant
                         Ranked comma delimited list of reference antennas.

                         Example: refant='DV02,DV06'
    flag_intent
                         The data intent(s) on whose WVR correction results the
                         search for bad WVR antennas is to be based.

                         A 'flagging view' will be calculated for each specified
                         intent, in each spectral window in each vis file.

                         Each 'flagging view' will consist of a 2-d image with dimensions
                         ['ANTENNA', 'TIME'], showing the phase noise after the WVR
                         correction has been applied.

                         If flag_intent is left blank, the default, the flagging views will be
                         derived from data with the default bandpass calibration intent i.e.
                         the first in the list BANDPASS, PHASE, AMPLITUDE for which the
                         MeasurementSet has data.
    qa_intent
                         The list of data intents on which the WVR correction is to be
                         tried as a means of estimating its effectiveness.

                         A QA 'view' will be calculated for each specified intent, in each spectral
                         window in each vis file.

                         Each QA 'view' will consist of a pair of 2-d images with dimensions
                         ['ANTENNA', 'TIME'], one showing the data phase-noise before the
                         WVR application, the second showing the phase noise after (both 'before'
                         and 'after' images have a bandpass calibration applied as well).

                         An overall QA score is calculated for each vis file, by dividing the
                         'before' images by the 'after' and taking the median of the result. An
                         overall score of 1 would correspond to no change in the phase noise,
                         a score > 1 implies an improvement.

                         If the overall score for a vis file is less than the value in
                         'accept_threshold' then the WVR calibration file is not made available for
                         merging into the context for use in the subsequent reduction.
    qa_bandpass_intent
                         The data intent to use for the bandpass calibration in
                         the qa calculation. The default is blank to allow the underlying bandpass
                         task to select a sensible intent if the dataset lacks BANDPASS data.
    accept_threshold
                         The phase-rms improvement ratio
                         (rms without WVR / rms with WVR) above which the wrvg file will be
                         accepted into the context for subsequent application.
    flag_hi
                         True to flag high figure of merit outliers.
    fhi_limit
                         Flag figure of merit values higher than limit * MAD.
    fhi_minsample
                         Minimum number of samples for valid MAD estimate.
    ants_with_wvr_thresh
                         This threshold sets the minimum fraction of antennas
                         that should have WVR data for WVR calibration and flagging to proceed; the
                         same threshold is used to determine, after flagging, whether there remain
                         enough unflagged antennas with WVR data for the WVR calibration to be
                         applied.

                         Example: ants_with_wvr_thresh=0.5

    --------- examples -----------------------------------------------------------

    1. Compute the WVR calibration for all the MeasurementSets:

    >>> hifa_wvrgcalflag(hm_tie='automatic')

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
