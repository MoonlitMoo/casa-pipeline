import sys

import pipeline.h.cli.utils as utils


@utils.cli_wrapper
def hifa_bpsolint(vis=None, field=None, intent=None, spw=None, phaseupsnr=None, minphaseupints=None, evenbpints=None,
                  bpsnr=None, minbpsnr=None, minbpnchan=None, hm_nantennas=None, maxfracflagged=None):
    """
    hifa_bpsolint ---- Compute optimal bandpass calibration solution intervals

    The optimal bandpass phaseup time and frequency solution intervals required
    to achieve the required signal-to-noise ratio is estimated based on nominal
    ALMA array characteristics the metadata associated with the observation.

    The phaseup gain time and bandpass frequency intervals are determined as
    follows:

    - For each data set the list of source(s) to use for bandpass solution
      signal-to-noise estimation is compiled based on the values of the
      ``field``, ``intent``, and ``spw`` parameters.

    - Source fluxes are determined for each spw and source combination.

    - Fluxes in Jy are derived from the pipeline context.

    - Pipeline context fluxes are derived from the online flux calibrator
      catalog, the ASDM, or the user via the flux.csv file.

    - If no fluxes are available the task terminates.

    - Atmospheric calibration and observations scans are determined for each
      spw and source combination.

    - If ``intent`` is set to 'PHASE' are there are no atmospheric scans
      associated with the 'PHASE' calibrator, 'TARGET' atmospheric scans will be
      used instead.

    - If atmospheric scans cannot be associated with any of the spw and source
      combinations the task terminates.

    - Science spws are mapped to atmospheric spws for each science spw and
      source combination.

    - If mappings cannot be determined for any of the spws the task terminates.

    - The median Tsys value for each atmospheric spw and source combination is
      determined from the SYSCAL table. Medians are computed first by channel,
      then by antenna, in order to reduce sensitivity to deviant values.

    - The science spw parameters, exposure time(s), and integration time(s) are
      determined.

    - The phase-up time interval, in time units and number of integrations
      required to meet the ``phaseupsnr`` are computed, along with the
      sensitivity in mJy and the signal-to-noise per integration. Nominal Tsys
      and sensitivity values per receiver band provided by the ALMA project are
      used for this estimate.

    - Warnings are issued if estimated phase-up gain time solution would contain
      fewer than ``minphaseupints`` solutions.

    - The frequency interval, in MHz and number of channels required to meet the
      ``bpsnr`` are computed, along with the per channel sensitivity in mJy and
      the per channel signal-to-noise. Nominal Tsys and sensitivity values per
      receiver band provided by the ALMA project are used for this estimate.

    - Warnings are issued if estimated bandpass solution would contain fewer
      than ``minbpnchan`` solutions.

    - If strong atmospheric features are detected in the Tsys spectrum for a
      given spw, the frequency interval of bandpass solution is recalculated to
      meet the lower threshold, ``minbpsnr`` - i.e. a lower snr is tolerated in
      order to preserve enough frequency intervals to capture the atmospheric
      line.

    Output:

        results -- The results object for the pipeline task is returned.

    --------- parameter descriptions ---------------------------------------------

    vis
                   The list of input MeasurementSets. Defaults to the list of
                   MeasurementSets specified in the pipeline context.

                   example: vis=['M82A.ms', 'M82B.ms']
    field
                   The list of field names of sources to be used for
                   signal-to-noise estimation. Defaults to all fields with the
                   standard intent.

                   example: field='3C279'
    intent
                   A string containing a comma delimited list of intents against
                   which the selected fields are matched. Defaults to
                   'BANDPASS'.

                   example: intent='PHASE'
    spw
                   The list of spectral windows and channels for which gain
                   solutions are computed. Defaults to all the science spectral
                   windows for which there are both 'intent' and TARGET intents.

                   example: spw='13,15'
    phaseupsnr
                   The required phase-up gain time interval solution
                   signal-to-noise.

                   example: phaseupsnr=10.0
    minphaseupints
                   The minimum number of time intervals in the phase-up gain
                   solution.

                   example: minphaseupints=4
    evenbpints
                   Use a bandpass frequency solint that is an integer divisor of
                   the spw bandwidth, to prevent the occurrence of one narrower
                   fractional frequency interval.
    bpsnr
                   The required bandpass frequency interval solution
                   signal-to-noise.

                   example: bpsnr=30.0
    minbpsnr
                   The minimum required bandpass frequency interval solution
                   signal-to-noise when strong atmospheric lines exist in Tsys
                   spectra.

                   example: minbpsnr=10.0
    minbpnchan
                   The minimum number of frequency intervals in the bandpass
                   solution.

                   example: minbpnchan=16
    hm_nantennas
                   The heuristics for determines the number of antennas to use
                   in the signal-to-noise estimate. The options are 'all' and
                   'unflagged'. The 'unflagged' options is not currently
                   supported.

                   example: hm_nantennas='unflagged'
    maxfracflagged
                   The maximum fraction of an antenna that can be flagged before
                   it is excluded from the signal-to-noise estimate.

                   example: maxfracflagged=0.80

    --------- examples -----------------------------------------------------------

    1. Estimate the phaseup gain time interval and the bandpass frequency
    interval required to match the desired signal-to-noise for bandpass
    solutions:

    >>> hifa_bpsolint()

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
