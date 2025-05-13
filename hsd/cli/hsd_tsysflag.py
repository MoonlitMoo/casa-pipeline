import sys

import pipeline.h.cli.utils as utils


@utils.cli_wrapper
def hsd_tsysflag(vis=None, caltable=None,
                 flag_nmedian=None, fnm_limit=None, fnm_byfield=None,
                 flag_derivative=None, fd_max_limit=None,
                 flag_edgechans=None, fe_edge_limit=None,
                 flag_fieldshape=None, ff_refintent=None, ff_max_limit=None,
                 flag_birdies=None, fb_sharps_limit=None,
                 flag_toomany=None, tmf1_limit=None, tmef1_limit=None,
                 metric_order=None, normalize_tsys=None, filetemplate=None):

    """
    hsd_tsysflag ---- Flag deviant system temperature measurements


    Flag deviant system temperature measurements for single dish measurements. This is done by running a
    sequence of flagging sub-tasks (tests), each looking for a different type of possible error.

    If a file with manual Tsys flags is provided with the 'filetemplate'
    parameter, then these flags are applied prior to the evaluation of the
    flagging heuristics listed below.

    The tests are:

      1. Flag Tsys spectra with high median values

      2. Flag Tsys spectra with high median derivatives. This is meant to spot
        spectra that are 'ringing'.

      3. Flag the edge channels of the Tsys spectra in each SpW.

      4. Flag Tsys spectra whose shape is different from that associated with
        the BANDPASS intent.

      5. Flag 'birdies'.

      6. Flag the Tsys spectra of all antennas in a timestamp and spw if
        proportion of antennas already flagged in this timestamp and spw exceeds
        a threshold, and flag Tsys spectra for all antennas and all timestamps
        in a spw, if proportion of antennas that are already entirely flagged
        in all timestamps exceeds a threshold.


    Output

    results -- The results object for the pipeline task is returned.

    --------- parameter descriptions ---------------------------------------------

    vis             List of input MeasurementSets (Not used)
    caltable        List of input Tsys calibration tables.
                    
                    default: [] - Use the table currently stored in the pipeline context.
                    example: caltable=['X132.ms.tsys.s2.tbl']
    flag_nmedian    True to flag Tsys spectra with high median value.
    fnm_limit       Flag spectra with median value higher than fnm_limit * median
                    of this measure over all spectra.
    fnm_byfield     Evaluate the nmedian metric separately for each field.
    flag_derivative True to flag Tsys spectra with high median derivative.
    fd_max_limit    Flag spectra with median derivative higher than
                    fd_max_limit * median of this measure over all spectra.
    flag_edgechans  True to flag edges of Tsys spectra.
    fe_edge_limit   Flag channels whose channel to channel difference >
                    fe_edge_limit * median across spectrum.
    flag_fieldshape True to flag Tsys spectra with a radically different
                    shape to those of the ff_refintent.
    ff_refintent    Data intent that provides the reference shape for 'flag_fieldshape'.
    ff_max_limit    Flag Tsys spectra with 'fieldshape' metric values >
                    ff_max_limit.
    flag_birdies    True to flag channels covering sharp spectral features.
    fb_sharps_limit Flag channels bracketing a channel to channel
                    difference > fb_sharps_limit.
    flag_toomany    True to flag Tsys spectra for which a proportion of
                    antennas for given timestamp and/or proportion of antennas that are
                    entirely flagged in all timestamps exceeds their respective thresholds.
    tmf1_limit      Flag Tsys spectra for all antennas in a timestamp and spw if
                    proportion of antennas already flagged in this timestamp and spw exceeds
                    tmf1_limit.
    tmef1_limit     Flag Tsys spectra for all antennas and all timestamps
                    in a spw, if proportion of antennas that are already entirely flagged
                    in all timestamps exceeds tmef1_limit.
    metric_order    Order in which to evaluate the flagging metrics that are
                    enabled. Disabled metrics are skipped.
    normalize_tsys  True to create a normalized Tsys table that is used to
                    evaluate the Tsys flagging metrics. All newly found flags are also applied
                    to the original Tsys caltable that continues to be used for subsequent
                    calibration.
    filetemplate    The name of a text file that contains the manual Tsys flagging
                    template. If the template flags file is undefined, a name of the form
                    'msname.flagtsystemplate.txt' is assumed.

    --------- examples -----------------------------------------------------------


    1. Flag Tsys measurements using currently recommended tests:

    >>> hsd_tsysflag()

    2. Flag Tsys measurements using all recommended tests apart from that
    using the 'fieldshape' metric:

    >>> hsd_tsysflag(flag_fieldshape=False)


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
