import sys

import pipeline.h.cli.utils as utils


@utils.cli_wrapper
def hif_bandpass(vis=None, caltable=None, field=None, intent=None, spw=None, antenna=None, phaseup=None,
                 phaseupsolint=None, phaseupbw=None, solint=None, combine=None, refant=None, solnorm=None,
                 minblperant=None, minsnr=None):

    """
    hif_bandpass ---- Compute bandpass calibration solutions


    Compute amplitude and phase as a function of frequency for each spectral
    window in each MeasurementSet.

    Previous calibration can be applied on the fly.

    hif_bandpass computes a bandpass solution for every specified science
    spectral window. By default a 'phaseup' pre-calibration is performed
    and applied on the fly to the data, before the bandpass is computed.

    The hif_refant task may be used to precompute a prioritized list of
    reference antennas.


    Output

    results -- The results object for the pipeline task is returned.

    --------- parameter descriptions ---------------------------------------------

    vis           The list of input MeasurementSets. Defaults to the list of
                  MeasurementSets specified in the h_init or hif_importdata task.
                  '': use all MeasurementSets in the context

                  Examples: 'ngc5921.ms', ['ngc5921a.ms', ngc5921b.ms', 'ngc5921c.ms']
    caltable      The list of output calibration tables. Defaults to the
                  standard pipeline naming convention.

                  Example: caltable=['M82.gcal', 'M82B.gcal']
    field         The list of field names or field ids for which bandpasses are
                  computed. Defaults to all fields.

                  Examples: field='3C279', field='3C279, M82'
    intent        A string containing a comma delimited list of intents against
                  which the selected fields are matched.  Defaults to all data
                  with bandpass intent.

                  Example: intent='`*PHASE*`'
    spw           The list of spectral windows and channels for which bandpasses
                  are computed. Defaults to all science spectral windows.

                  Example: spw='11,13,15,17'
    antenna       Set of data selection antenna IDs
    phaseup       Do a phaseup on the data before computing the bandpass
                  solution.
    phaseupsolint The phase correction solution interval in CASA syntax.
                  Used when phaseup is True.

                  Example: phaseupsolint=300
    phaseupbw     Bandwidth to be used for phaseup. Defaults to 500MHz.
                  Used when phaseup is True.

                  Examples: phaseupbw='' to use entire bandpass
                            phaseupbw='500MHz' to use central 500MHz
    solint        Time and channel solution intervals in CASA syntax.

                  Examples: solint='inf,10ch', 'inf'
    combine       Data axes to combine for solving. Axes are '', 'scan', 'spw',
                  'field' or any comma-separated combination.

                  Example: combine='scan,field'
    refant        Reference antenna names. Defaults to the value(s) stored in
                  the pipeline context. If undefined in the pipeline context
                  defaults to the CASA reference antenna naming scheme.

                  Examples: refant='DV01', refant='DV06,DV07'
    solnorm       Normalise the bandpass solution
    minblperant   Minimum number of baselines required per antenna for each
                  solve. Antennas with fewer baselines are excluded from
                  solutions.
    minsnr        Reject solutions below this SNR

    --------- examples -----------------------------------------------------------


    1. Compute a channel bandpass for all visibility files in the pipeline
    context using the CASA reference antenna determination scheme:

    >>> hif_bandpass()

    2. Same as the above but precompute a prioritized reference antenna list:

    >>> hif_refant()
    >>> hif_bandpass()

    --------- issues -----------------------------------------------------------

    The specified minsnr parameter is currently applied to the bandpass
    solution computation but not the 'phaseup' computation. Some noisy
    solutions in the phaseup may not be properly rejected.

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
