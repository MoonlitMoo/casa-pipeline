import sys

import pipeline.h.cli.utils as utils


@utils.cli_wrapper
def hsd_restoredata(vis=None, session=None, products_dir=None, copytoraw=None, rawdata_dir=None, lazy=None,
                    bdfflags=None, ocorr_mode=None, asis=None, hm_rasterscan=None):

    """
    hsd_restoredata ---- Restore flagged and calibration single dish data from a pipeline run

    The hsd_restoredata task restores flagged and calibrated MeasurementSets
    from archived ASDMs and pipeline flagging and calibration data products.

    hsd_restoredata assumes that the ASDMs to be restored are present in the
    directory specified by the ``rawdata_dir`` (default: '../rawdata').

    By default (``copytoraw`` = True), hsd_restoredata assumes that for each
    ASDM in the input list, the corresponding pipeline flagging and calibration
    data products (in the format produced by the hsd_exportdata task) are
    present in the directory specified by ``products_dir`` (default: '../products').
    At the start of the task, these products are copied from the ``products_dir``
    to the ``rawdata_dir``.

    If ``copytoraw`` = False, hsd_restoredata assumes that these products are
    to be found in ``rawdata_dir`` along with the ASDMs.

    The expected flagging and calibration products (for each ASDM) include:

        - a compressed tar file of the final flagversions file, e.g.
          uid___A002_X30a93d_X43e.ms.flagversions.tar.gz

        - a text file containing the applycal instructions, e.g.
          uid___A002_X30a93d_X43e.ms.calapply.txt

        - a compressed tar file containing the caltables for the parent session,
          e.g. uid___A001_X74_X29.session_3.caltables.tar.gz

    hsd_restoredata performs the following operations:

    - imports the ASDM(s)
    - removes the default MS.flagversions directory created by the filler
    - restores the final MS.flagversions directory stored by the pipeline
    - restores the final set of pipeline flags to the MS
    - restores the final calibration state of the MS
    - restores the final calibration tables for each MS
    - applies the calibration tables to each MS

    When importing the ASDM and converting it to a Measurement Set (MS), if the
    output MS already exists in the output directory, then the importasdm
    conversion step is skipped, and the existing MS will be imported instead.

    Output:

    results -- The results object for the pipeline task is returned.

    --------- parameter descriptions ---------------------------------------------

    vis           List of raw visibility data files to be restored. Assumed to be
                  in the directory specified by rawdata_dir.
                  
                  example: vis=['uid___A002_X30a93d_X43e']
    session       List of sessions one per visibility file.
                  
                  example: session=['session_3']
    products_dir  Name of the data products directory to copy calibration
                  products from.
                  Default: '../products'
                  
                  The parameter is effective only when ``copytoraw`` = True.
                  When ``copytoraw`` = False, calibration products in
                  ``rawdata_dir`` will be used.
                  
                  example: products_dir='myproductspath'
    copytoraw     Copy calibration and flagging tables from ``products_dir`` to
                  ``rawdata_dir`` directory.
                  Default: True
                  
                  example: copytoraw=False
    rawdata_dir   Name of the raw data directory.
                  Default: '../rawdata'
                  
                  example: rawdata_dir='myrawdatapath'
    lazy          Use the lazy filler option
                  Default: False
                  
                  example: lazy=True
    bdfflags      Set the BDF flags
                  Default: True
                  
                  example: bdfflags=False
    ocorr_mode    Set ocorr_mode
                  Default: 'ao'
                  
                  example: ocorr_mode='ca'
    asis          Creates verbatim copies of the ASDM tables in the output MS.
                  The value given to this option must be a list of table names separated by space characters.
                  Default: 'SBSummary ExecBlock Annotation Antenna Station Receiver Source CalAtmosphere CalWVR'
                  
                  example: asis='Source Receiver'
    hm_rasterscan Heuristics method for raster scan analysis. Two analysis modes,
                  time-domain analysis ('time') and direction analysis ('direction'), are available.
                  Default: 'time'

    --------- examples -----------------------------------------------------------


    1. Restore the pipeline results for a single ASDM in a single session

    >>> hsd_restoredata (vis=['uid___A002_X30a93d_X43e'], session=['session_1'], ocorr_mode='ao')


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
