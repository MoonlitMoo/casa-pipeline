import sys

import pipeline.h.cli.utils as utils


@utils.cli_wrapper
def hif_setmodels(vis=None, reference=None, refintent=None, transfer=None, transintent=None, reffile=None,
                  normfluxes=None, scalebychan=None):

    """
    hif_setmodels ---- Set calibrator source models


    Set model fluxes values for calibrator reference and transfer sources using lookup
    values. By default the reference sources are the flux calibrators and the transfer
    sources are the bandpass, phase, and check source calibrators. Reference sources
    which are also in the transfer source list are removed from the transfer source list.

    Built-in lookup tables are used to compute models for solar system object calibrators.
    Point source models are used for other calibrators with flux densities provided in the reference file.
    Normalized fluxes are computed for transfer sources if the ``normfluxes`` parameter is
    set to True.

    The default reference file is 'flux.csv' in the current working directory.
    This file is usually created in the importdata stage. The file is in
    'csv' format and contains the following comma delimited columns.

    vis,fieldid,spwid,I,Q,U,V,pix,comment


    Output:

    results -- The results object for the pipeline task is returned

    --------- parameter descriptions ---------------------------------------------

    vis           The list of input MeasurementSets. Defaults to the list of
                  MeasurementSets specified in the pipeline context.

                  example: ['M32A.ms', 'M32B.ms']
    reference     A string containing a comma delimited list of  field names
                  defining the reference calibrators. Defaults to field names with
                  intent 'AMPLITUDE'.

                  example: 'M82,3C273'
    refintent     A string containing a comma delimited list of intents
                  used to select the reference calibrators. Defaults to 'AMPLITUDE'.

                  example: 'BANDPASS'
    transfer      A string containing a comma delimited list of  field names
                  defining the transfer calibrators. Defaults to field names with
                  intent ''.

                  example: 'J1328+041,J1206+30'
    transintent   A string containing a comma delimited list of intents
                  defining the transfer calibrators. Defaults to 'BANDPASS,PHASE,CHECK'.
                  '' stands for no transfer sources.

                  example: 'PHASE'
    reffile       The reference file containing a lookup table of point source models
                  This file currently defaults to 'flux.csv' in the working directory. This
                  file must conform to the standard pipeline 'flux.csv' format
                  example: 'myfluxes.csv'
    normfluxes    Normalize the transfer source flux densities.
    scalebychan   Scale the flux density on a per channel basis or else on a per spw basis

    --------- examples -----------------------------------------------------------


    1. Set model fluxes for the flux, bandpass, phase, and check sources.

    >>> hif_setmodels()


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
