import sys

import pipeline.h.cli.utils as utils


@utils.cli_wrapper
def hsd_skycal(calmode=None, fraction=None, noff=None,
                 width=None, elongated=None, parallel=None,
                 infiles=None, field=None,
                 spw=None, scan=None):

    """
    hsd_skycal ---- Calibrate data

    The hsd_skycal generates a caltable for sky calibration that stores
    reference spectra, which is to be subtracted from on-source spectra to filter
    out non-source contribution.

    Output:

    results -- The results object for the pipeline task is returned.

    --------- parameter descriptions ---------------------------------------------

    calmode       Calibration mode. Available options are 'auto' (default),
                  'ps', 'otf', and 'otfraster'. When 'auto' is set, the task will
                  use preset calibration mode that is determined by inspecting data.
                  'ps' mode is simple position switching using explicit reference
                  scans. Other two modes, 'otf' and 'otfraster', will generate
                  reference data from scans at the edge of the map. Those modes
                  are intended for OTF observation and the former is defined for
                  generic scanning pattern such as Lissajous, while the latter is
                  specific use for raster scan.

                  options: 'auto', 'ps', 'otf', 'otfraster'
    fraction      Sub-parameter for calmode. Edge marking parameter for
                  'otf' and 'otfraster' mode. It specifies a number of OFF scans
                  as a fraction of total number of data points.

                  options: String style like '20%', or float value less than 1.0.
                  For 'otfraster' mode, you can also specify 'auto'.
    noff          Sub-parameter for calmode. Edge marking parameter for 'otfraster'
                  mode. It is used to specify a number of OFF scans near edge directly
                  instead to specify it by fractional number by 'fraction'. If it is
                  set, the value will come before setting by 'fraction'.

                  options: any positive integer value
    width         Sub-parameter for calmode. Edge marking parameter for 'otf'
                  mode. It specifies pixel width with respect to a median spatial
                  separation between neighboring two data in time. Default will
                  be fine in most cases.

                  options: any float value
    elongated     Sub-parameter for calmode. Edge marking parameter for
                  'otf' mode. Please set True only if observed area is elongated
                  in one direction.

    parallel      Execute using CASA HPC functionality, if available.
                  options: 'automatic', 'true', 'false', True, False
                  default: None (equivalent to 'automatic')

    infiles       List of data files. These must be a name of MeasurementSets that
                  are registered to context via hsd_importdata task.

                  example: vis=['X227.ms', 'X228.ms']
    field         Data selection by field name.
    spw           Data selection by spw. (defalut all spws)

                  example: '3,4' (generate caltable for spw 3 and 4)
                          ['0','2'] (spw 0 for first data, 2 for second)
    scan          Data selection by scan number. (default all scans)

                  example: '22,23' (use scan 22 and 23 for calibration)
                          ['22','24'] (scan 22 for first data, 24 for second)

    --------- examples -----------------------------------------------------------


    1. Generate caltables for all data managed by context.

    >>> default(hsd_skycal)
    >>> hsd_skycal()


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
