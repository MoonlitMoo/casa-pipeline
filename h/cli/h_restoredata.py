import sys

from . import utils


@utils.cli_wrapper
def h_restoredata(vis=None, session=None, products_dir=None, copytoraw=None, rawdata_dir=None, lazy=None, bdfflags=None,
                  ocorr_mode=None, asis=None):

    """
    h_restoredata ---- Restore flags and calibration state from a pipeline run


    The h_restoredata restores flagged and calibrated data from archived ASDMs and
    pipeline flagging and calibration data products. Pending archive retrieval
    support h_restoredata assumes that the required products are available in the
    rawdata_dir in the format produced by the h_exportdata task.

    h_restoredata assumes that the following entities are available in the
    rawdata_dir directory:

    - the ASDMs to be restored
    - for each ASDM in the input list:

        - a compressed tar file of the final flagversions file, e.g.,
          uid___A002_X30a93d_X43e.ms.flagversions.tar.gz
        - a text file containing the applycal instructions, e.g.,
          uid___A002_X30a93d_X43e.ms.calapply.txt
        - a compressed tar file containing the caltables for the parent
          session, e.g., uid___A001_X74_X29.session_3.caltables.tar.gz

    h_restore data performs the following operations:

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

    vis           List of raw visibility data files to be restored. Assumed to
                  be in the directory specified by rawdata_dir.

                  example: vis=['uid___A002_X30a93d_X43e']

    session       List of sessions, one per visibility file.

                  example: session=['session_3']

    products_dir  Path to the data products directory, used to copy calibration
                  products from. The parameter is effective only when
                  copytoraw=True. When copytoraw=False, calibration products in
                  rawdata_dir will be used.

                  example: products_dir='/path/to/my/products'

    copytoraw     Copy calibration and flagging tables from products_dir to
                  rawdata_dir directory.

                  example: copytoraw=False

    rawdata_dir   Path to the rawdata subdirectory.

                  example: rawdata_dir='/path/to/my/rawdata'

    lazy          Use the lazy filler option

                  example: lazy=True

    bdfflags      Set the BDF flags

                  example: bdfflags=False

    ocorr_mode    Set correlation import mode

                  example: ocorr_mode='ca'

    asis          Creates verbatim copies of the ASDM tables in the output MS.
                  The value given to this option must be a list of table names separated by space characters.

                  example: ocorr_mode='Source Receiver'

    --------- examples -----------------------------------------------------------


    1. Restore the pipeline results for a single ASDM in a single session

    >>> h_restoredata (vis=['uid___A002_X30a93d_X43e'], session=['session_1'], ocorr_mode='ca')


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
