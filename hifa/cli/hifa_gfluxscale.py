import sys

import pipeline.h.cli.utils as utils


@utils.cli_wrapper
def hifa_gfluxscale(vis=None, reference=None, transfer=None, refintent=None, transintent=None, refspwmap=None,
                    reffile=None, phaseupsolint=None, solint=None, minsnr=None, refant=None, hm_resolvecals=None,
                    antenna=None, peak_fraction=None, amp_outlier_sigma=None):
    """
    hifa_gfluxscale ---- Derive flux density scales from standard calibrators


    Derive flux densities for point source transfer calibrators using flux models
    for reference calibrators.

    Flux values are determined by:

    - computing phase only solutions for all the science spectral windows
      using the calibrator data selected by the ``reference`` and ``refintent``
      parameters and the ``transfer`` and ``transintent`` parameters, the value
      of the ``phaseupsolint`` parameter, and any spw combination determined
      in hifa_spwphaseup.

    - computing complex amplitude only solutions for all the science spectral
      windows using calibrator data selected with ``reference`` and
      ``refintent`` parameters and the ``transfer`` and ``transintent``
      parameters, the value of the ``solint`` parameter.

    - examining the amplitude-only solutions for obvious outliers and flagging
      them in the caltable.

    - transferring the flux scale from the reference calibrators to the transfer
      calibrators using ``refspwmap`` for windows without data in the reference
      calibrators.

    - inserting the computed flux density values into the MODEL_DATA column.

    Resolved calibrators are handled via antenna selection either automatically
    (``hm_resolvedcals`` = 'automatic') or manually (``hm_resolvedcals`` =
    'manual'). In the former case, antennas closer to the reference antenna than
    the uv distance where visibilities fall to ``peak_fraction`` of the peak are
    used. In manual mode, the antennas specified in ``antenna`` are used.

    Note that the flux corrected calibration table computed internally is
    not currently used in later pipeline apply calibration steps.

    Output:

        results -- The results object for the pipeline task is returned.

    --------- parameter descriptions ---------------------------------------------

    vis
                    The list of input MeasurementSets. Defaults to the list of
                    MeasurementSets specified in the pipeline context.

                    Example: ['M32A.ms', 'M32B.ms']
    reference
                    A string containing a comma delimited list of field names
                    defining the reference calibrators. Defaults to field names with
                    intent '`*AMP*`'.

                    Example: reference='M82,3C273'
    transfer
                    A string containing a comma delimited list of field names
                    defining the transfer calibrators. Defaults to field names with
                    intent '`*PHASE*`'.

                    Example: transfer='J1328+041,J1206+30'
    refintent
                    A string containing a comma delimited list of intents
                    used to select the reference calibrators. Defaults to 'AMPLITUDE'.

                    Example: refintent='', refintent='AMPLITUDE'
    transintent
                    A string containing a comma delimited list of intents
                    defining the transfer calibrators. Defaults to
                    'PHASE,BANDPASS,CHECK,POLARIZATION,POLANGLE,POLLEAKAGE'.

                    Example: transintent='', transintent='PHASE,BANDPASS'
    refspwmap
                    Vector of spectral window ids enabling scaling across
                    spectral windows. Defaults to no scaling.

                    Example: refspwmap=[1,1,3,3] - (4 spws, reference fields in 1 and 3, transfer
                    fields in 0,1,2,3
    reffile
                    Path to a file containing flux densities for calibrators.
                    Setjy will be run for any that have both reference and transfer intents.
                    Values given in this file will take precedence over MODEL column values
                    set by previous tasks. By default, the path is set to the CSV file created
                    by hifa_importdata, consisting of catalogue fluxes extracted from the ASDM
                    and / or edited by the user.

                    example: reffile='', reffile='working/flux.csv'
    phaseupsolint
                    Time solution intervals in CASA syntax for the phase solution.

                    example: phaseupsolint='inf', phaseupsolint='int', phaseupsolint='100sec'
    solint
                    Time solution intervals in CASA syntax for the amplitude solution.

                    example: solint='inf', solint='int', solint='100sec'
    minsnr
                    Minimum signal-to-noise ratio for gain calibration solutions.

                    example: minsnr=1.5, minsnr=0.0
    refant
                    A string specifying the reference antenna(s). By default,
                    this is read from the context.

                    Example: refant='DV05'
    hm_resolvedcals
                    Heuristics method for handling resolved calibrators. The
                    options are 'automatic' and 'manual'. In automatic mode,
                    antennas closer to the reference antenna than the uv
                    distance where visibilities fall to ``peak_fraction`` of the
                    peak are used. In manual mode, the antennas specified in
                    ``antenna`` are used.
    antenna
                    A comma delimited string specifying the antenna names or ids
                    to be used for the fluxscale determination. Used in
                    ``hm_resolvedcals`` = 'manual' mode.

                    Example: antenna='DV16,DV07,DA12,DA08'
    peak_fraction
                    The limiting UV distance from the reference antenna for
                    antennas to be included in the flux calibration. Defined as
                    the point where the calibrator visibilities have fallen to
                    ``peak_fraction`` of the peak value.
    amp_outlier_sigma
                    Sigma threshold used to identify outliers in the amplitude
                    caltable. Default: 50.0

                    Example: amp_outlier_sigma=30.0

    --------- examples -----------------------------------------------------------

    1. Compute flux values for the phase calibrator using model data from
    the amplitude calibrator:

    >>> hifa_gfluxscale()

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
