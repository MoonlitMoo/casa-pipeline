import sys

import pipeline.h.cli.utils as utils


@utils.cli_wrapper
def hifa_exportdata(vis=None, session=None, imaging_products_only=None, exportmses=None, tarms=None, pprfile=None, calintents=None,
                    calimages=None, targetimages=None, products_dir=None):
    """
    hifa_exportdata ---- Prepare interferometry data for export


    Prepare interferometry data for export.

    The hifa_exportdata task for ALMA CASA pipeline exports the data defined
    in the pipeline context and exports it to the data products directory,
    converting and or packing it as necessary.

    The current version of the task exports the following products

    - an XML file containing the pipeline processing request
    - a tar file per ASDM / MS containing the final flags version
    - a text file per ASDM / MS containing the final calibration apply list
    - a FITS image for each selected calibrator source image
    - a FITS image for each selected science target source image
    - a tar file per session containing the caltables for that session
    - a tar file containing the file web log
    - a text file containing the final list of CASA commands
    - an XML "manifest" file listing the products
    - an XML "aquareport" file listing the QA scores and sub-scores,
      image sensitivities, and other numerical information

    Output:

        results -- The results object for the pipeline task is returned.

    --------- parameter descriptions ---------------------------------------------

    vis
                          List of visibility data files for which flagging and calibration
                          information will be exported. Defaults to the list maintained in the
                          pipeline context.

                          Example: vis=['X227.ms', 'X228.ms']
    session
                          List of sessions one per visibility file. Currently defaults
                          to a single virtual session containing all the visibility files in vis.
                          In the future, this will default to the set of observing sessions defined
                          in the context.

                          Example: session=['session1', 'session2']
    imaging_products_only
                          Export science target imaging products only
    exportmses
                          Export the final MeasurementSets instead of the final flags,
                          calibration tables, and calibration instructions.
    tarms                 Tar final MeasurementSets, default: True                          
    pprfile
                          Name of the pipeline processing request to be exported. Defaults
                          to a file matching the template 'PPR_*.xml'.

                          Example: pprfile=['PPR_GRB021004.xml']
    calintents
                          List of calibrator image types to be exported. Defaults to
                          all standard calibrator intents, 'BANDPASS', 'PHASE', 'FLUX'.

                          Example: 'PHASE'
    calimages
                          List of calibrator images to be exported. Defaults to all
                          calibrator images recorded in the pipeline context.

                          Example: calimages=['3C454.3.bandpass', '3C279.phase']
    targetimages
                          List of science target images to be exported. Defaults to all
                          science target images recorded in the pipeline context.

                          Example: targetimages=['NGC3256.band3', 'NGC3256.band6']
    products_dir
                          Name of the data products subdirectory. Defaults to './'.

                          Example: products_dir='../products'

    --------- examples -----------------------------------------------------------

    1. Export the pipeline results for a single session to the data products
    directory:

    >>> !mkdir ../products
    >>> hif_exportdata(products_dir='../products')

    2. Export the pipeline results to the data products directory specify that
    only the gain calibrator images be saved:

    >>> !mkdir ../products
    >>> hif_exportdata(products_dir='../products', calintents='*PHASE*')

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
