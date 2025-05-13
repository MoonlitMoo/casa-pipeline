import sys

import pipeline.h.cli.utils as utils


@utils.cli_wrapper
def hifa_wvrgcal(vis=None, caltable=None, offsetstable=None, hm_toffset=None, toffset=None, segsource=None,
                 sourceflag=None, hm_tie=None, tie=None, nsol=None, disperse=None, wvrflag=None, hm_smooth=None,
                 smooth=None, scale=None, maxdistm=None, minnumants=None, mingoodfrac=None, refant=None, qa_intent=None,
                 qa_bandpass_intent=None, qa_spw=None, accept_threshold=None):
    """
    hifa_wvrgcal ----
    Generate a gain table based on Water Vapor Radiometer data, and calculate
    a QA score based on its effect on the interferometric data.


    Generate a gain table based on the Water Vapor Radiometer data in each vis
    file. By applying the wvr calibration to the data specified by ``qa_intent``
    and ``qa_spw``, calculate a QA score to indicate its effect on
    interferometric data; a score > 1 implies that the phase noise is improved,
    a score < 1 implies that it is made worse. If the score is less than
    ``accept_threshold`` then the wvr gain table is not accepted into the
    context for subsequent use.

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
                       List of input temperature offsets table files to subtract
                       from WVR measurements before calculating phase corrections.

                       Default: none, in which case no offsets are applied.

                       Example: offsetstable=['ngc5921.cloud_offsets']
    hm_toffset
                       If 'manual', set the ``toffset`` parameter to the user-specified value.
                       If 'automatic', set the ``toffset`` parameter according to the
                       date of the MeasurementSet; toffset=-1 if before 2013-01-21T00:00:00
                       toffset=0 otherwise.
    toffset
                       Time offset (sec) between interferometric and WVR data.
    segsource
                       If True calculate new atmospheric phase correction coefficients
                       for each source, subject to the constraints of the ``tie`` parameter.
                       'segsource' is forced to be True if the ``tie`` parameter is set to a
                       non-empty value by the user or by the automatic heuristic.
    sourceflag
                       Flag the WVR data for these source(s) as bad and do not
                       produce corrections for it. Requires ``segsource`` = True.

                       Example: ['3C273']
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
                       Apply correction for dispersion. (Deprecated; will be removed)
    wvrflag
                       Flag the WVR data for the listed antennas as bad and replace
                       their data with values interpolated from the 3 nearest antennas with
                       unflagged data.

                       Example: ['DV03','DA05','PM02']
    hm_smooth
                       If 'manual' set the ``smooth`` parameter to the user-specified value.
                       If 'automatic', run the wvrgcal task with the range of ``smooth`` parameters
                       required to match the integration time of the wvr data to that of the
                       interferometric data in each spectral window.
    smooth
                       Smooth WVR data on this timescale before calculating the correction.
                       Ignored unless hm_smooth='manual'.
    scale
                       Scale the entire phase correction by this factor.
    maxdistm
                       Maximum distance in meters of an antenna used for interpolation
                       from a flagged antenna.

                       Default: -1  (automatically set to 100m if >50% of
                       antennas are 7m antennas without WVR and otherwise set to
                       500m).

                       Example: maxdistm=550
    minnumants
                       Minimum number of nearby antennas (up to 3) used for
                       interpolation from a flagged antenna.

                       Example: minnumants=3
    mingoodfrac
                       Minimum fraction of good data per antenna.
    refant
                       Ranked comma delimited list of reference antennas.

                       Example: refant='DV01,DV02'
    qa_intent
                       The list of data intents on which the wvr correction is to be
                       tried as a means of estimating its effectiveness.

                       A QA 'view' will be calculated for each specified intent, in each spectral
                       window in each vis file.

                       Each QA 'view' will consist of a pair of 2-d images with dimensions
                       ['ANTENNA', 'TIME'], one showing the data phase-noise before the
                       wvr application, the second showing the phase noise after (both 'before'
                       and 'after' images have a bandpass calibration applied as well).

                       An overall QA score is calculated for each vis file, by dividing the
                       'before' images by the 'after' and taking the median of the result. An
                       overall score of 1 would correspond to no change in the phase noise,
                       a score > 1 implies an improvement.

                       If the overall score for a vis file is less than the value in
                       'accept_threshold' then the wvr calibration file is not made available
                       for merging into the context for use in the subsequent reduction.

                       If you do not want any QA calculations then set qa_intent=''.

                       example: qa_intent='PHASE'
    qa_bandpass_intent
                       The data intent to use for the bandpass calibration in
                       the qa calculation. The default is blank to allow the underlying bandpass
                       task to select a sensible intent if the dataset lacks BANDPASS data.
    qa_spw
                       The SpW(s) to use for the qa calculation, in the order that they
                       should be tried. Input as a comma-separated list. The default is blank, in
                       which case the task will try SpWs in order of decreasing median sky
                       opacity.
    accept_threshold
                       The phase-rms improvement ratio
                       (rms without wvr / rms with wvr) above which the wrvg file will be
                       accepted into the context for subsequent application.

    --------- examples -----------------------------------------------------------

    1. Compute the WVR calibration for all the MeasurementSets:

    >>> hifa_wvrgcal(hm_tie='automatic')

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
