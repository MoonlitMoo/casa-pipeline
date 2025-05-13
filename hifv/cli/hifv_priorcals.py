import sys

import pipeline.h.cli.utils as utils


@utils.cli_wrapper
def hifv_priorcals(vis=None, show_tec_maps=None, apply_tec_correction=None, apply_gaincurves=None, apply_opcal=None, apply_rqcal=None,
                   apply_antpos=None, apply_swpowcal=None, swpow_spw=None, ant_pos_time_limit=None):

    """
    hifv_priorcals ---- Runs gaincurves, opacities, requantizer gains, antenna position corrections, tec_maps, switched power.

    Runs gaincurves, opacities, requantizer gains, antenna position corrections, tec_maps, switched power.

    Output:

    results -- The results object for the pipeline task is returned.
    --------- parameter descriptions ---------------------------------------------

    vis                  List of visibility data files. These may be ASDMs, tar files of ASDMs,
                         MSes, or tar files of MSes, If ASDM files are specified, they will be
                         converted  to MS format.
                         example: vis=['X227.ms', 'asdms.tar.gz']
    show_tec_maps        Plot tec maps
    apply_tec_correction Apply tec correction
    apply_gaincurves     Apply gain curves correction, default True
    apply_opcal          Apply opacities correction, default True
    apply_rqcal          Apply requantizer gains correction, default True
    apply_antpos         Apply antenna position corrections, default True.
    apply_swpowcal       Apply switched power table, default False.  If set True,
                         ``apply_rqcal`` is ignored and no requantizer gain correction will be applied.
    swpow_spw            Spectral-window(s) for plotting: "" ==>all, spw="6,14"
    ant_pos_time_limit   Antenna position time limit in days, default to 150 days
    --------- examples -----------------------------------------------------------


    1. Run gaincurves, opacities, requantizer gains and antenna position corrections.

    >>> hifv_priorcals()


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
