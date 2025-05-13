'''
Created on 2013/06/23

@author: kana
'''
import sys

import pipeline.h.cli.utils as utils


@utils.cli_wrapper
def hsd_blflag(iteration=None, edge=None, flag_tsys=None, tsys_thresh=None,
                 flag_prfre=None, prfre_thresh=None,
                 flag_pofre=None, pofre_thresh=None,
                 flag_prfr=None, prfr_thresh=None,
                 flag_pofr=None, pofr_thresh=None,
                 flag_prfrm=None, prfrm_thresh=None, prfrm_nmean=None,
                 flag_pofrm=None, pofrm_thresh=None, pofrm_nmean=None,
                 plotflag=None, parallel=None,
                 infiles=None, antenna=None,
                 field=None, spw=None, pol=None):

    """
    hsd_blflag ---- Flag spectra based on predefined criteria of single dish pipeline


    Data are flagged based on several flagging rules. Available rules are:
    expected rms, calculated rms, and running mean of both pre-fit and
    post-fit spectra. Tsys flagging is also available.

    In addition, the heuristics script creates many plots for each stage.
    Those plots are included in the weblog.

    Output:

    results -- The results object for the pipeline task is returned.

    --------- parameter descriptions ---------------------------------------------

    iteration     Number of iterations to perform sigma clipping to
                  calculate threshold value of flagging.
    edge          Number of channels to be dropped from the edge.
                  The value must be a list of integer with length of one or
                  		     two. If list length is one, same number will be applied
                  both side of the band.

                  example: [10,20], [10]
    flag_tsys     Activate (True) or deactivate (False) Tsys flag.
    tsys_thresh   Threshold value for Tsys flag.
    flag_prfre    Activate (True) or deactivate (False) flag by expected
                  rms of pre-fit spectra.
    prfre_thresh  Threshold value for flag by expected rms of pre-fit
                  spectra.
    flag_pofre    Activate (True) or deactivate (False) flag by expected
                  rms of post-fit spectra.
    pofre_thresh  Threshold value for flag by expected rms of post-fit
                  spectra.
    flag_prfr     Activate (True) or deactivate (False) flag by rms of
                  pre-fit spectra.
    prfr_thresh   Threshold value for flag by rms of pre-fit spectra.
    flag_pofr     Activate (True) or deactivate (False) flag by rms of
                  post-fit spectra.
    pofr_thresh   Threshold value for flag by rms of post-fit spectra.
    flag_prfrm    Activate (True) or deactivate (False) flag by running
                  mean of pre-fit spectra.
    prfrm_thresh  Threshold value for flag by running mean of pre-fit
                  spectra.
    prfrm_nmean   Number of channels for running mean of pre-fit spectra.
    flag_pofrm    Activate (True) or deactivate (False) flag by running
                  mean of post-fit spectra.
    pofrm_thresh  Threshold value for flag by running mean of post-fit
                  spectra.
    pofrm_nmean   Number of channels for running mean of post-fit spectra.
    plotflag      True to plot result of data flagging.
    parallel      Execute using CASA HPC functionality, if available.
                  options: 'automatic', 'true', 'false', True, False
                  default: None (equivalent to 'automatic')
    infiles       ASDM or MS files to be processed. This parameter behaves
                  as data selection parameter. The name specified by
                  infiles must be registered to context before you run
                  hsd_blflag.
    antenna       Data selection by antenna names or ids.
                  example: 'PM03,PM04'
                           '' (all antennas)
    field         Data selection by field names or ids.
                  example: '`*Sgr*,M100`'
                           '' (all fields)
    spw           Data selection by spw ids.
                  example: '3,4' (spw 3 and 4)
                           '' (all spws)
    pol           Data selection by polarizations.
                  example: 'XX,YY' (correlation XX and YY)
                           '' (all polarizations)

    --------- examples -----------------------------------------------------------

    1. flagging with all rules

    >>> hsd_blflag()

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
