import sys

import pipeline.h.cli.utils as utils


@utils.cli_wrapper
def hifa_importdata(vis=None, session=None, asis=None, process_caldevice=None, overwrite=None, nocopy=None,
                    bdfflags=None, datacolumns=None, lazy=None, dbservice=None, ocorr_mode=None, createmms=None,
                    minparang=None, parallel=None):
    """
    hifa_importdata ---- Imports data into the interferometry pipeline


    The hifa_importdata task loads the specified visibility data into the pipeline
    context unpacking and / or converting it as necessary.

    If the ``overwrite`` input parameter is set to False and the task is asked
    to convert an input ASDM input to an MS, then when the output MS already
    exists in the output directory, the importasdm conversion step is skipped,
    and the existing MS will be imported instead.

    Output:

        results -- The results object for the pipeline task is returned.

    --------- parameter descriptions ---------------------------------------------

    vis
                      List of visibility data files. These may be ASDMs, tar
                      files of ASDMs, MSes, or tar files of MSes. If ASDM files
                      are specified, they will be converted to MS format.

                      Example: vis=['X227.ms', 'asdms.tar.gz']
    session
                      List of session names, one for each visibility dataset,
                      used to group the MSes into sessions.

                      Example: session=['session_1', 'session_2']
    asis
                      Creates verbatim copies of the ASDM tables in the output MS.
                      The value given to this option must be a list of table names
                      separated by space characters.
    process_caldevice
                      Import the caldevice table from the ASDM.
    overwrite
                      Overwrite existing files on import; defaults to False.
                      When converting ASDM to MS, if overwrite=False and the MS
                      already exists in the output directory, then this existing
                      MS dataset will be used instead.

                      Example: overwrite=True
    nocopy
                      Disable copying of MS to working directory; defaults to
                      False.

                      Example: nocopy=True
    bdfflags
                      Apply BDF flags on import.
    datacolumns
                      Dictionary defining the data types of existing columns.
                      The format is:

                      {'data': 'data type 1'}

                      or

                      {'data': 'data type 1', 'corrected': 'data type 2'}

                      For ASDMs the data type can only be RAW and one
                      can only specify it for the data column.

                      For MSes one can define two different data types
                      for the DATA and CORRECTED_DATA columns and they
                      can be any of the known data types (RAW,
                      REGCAL_CONTLINE_ALL, REGCAL_CONTLINE_SCIENCE,
                      SELFCAL_CONTLINE_SCIENCE, REGCAL_LINE_SCIENCE,
                      SELFCAL_LINE_SCIENCE, BASELINED, ATMCORR). The
                      intent selection strings _ALL or _SCIENCE can be
                      skipped. In that case the task determines this
                      automatically by inspecting the existing intents
                      in the dataset.

                      Usually, a single datacolumns dictionary is used
                      for all datasets. If necessary, one can define a
                      list of dictionaries, one for each EB, with
                      different setups per EB.

                      If no types are specified,
                      {'data':'raw','corrected':'regcal_contline'}
                      or {'data':'raw'} will be assumed, depending on
                      whether the corrected column exists or not.
    lazy
                      Use the lazy filler import.
    dbservice
                      Use the online flux catalog.
    ocorr_mode
                      ALMA default set to ca.
    createmms
                      Create an MMS.
    minparang
                      Minimum required parallactic angle range for polarisation
                      calibrator, in degrees. The default of 0.0 is used for
                      non-polarisation processing.
    parallel
                      Execute using CASA HPC functionality, if available.

    --------- examples -----------------------------------------------------------

    1. Load an ASDM list in the ../rawdata subdirectory into the context:

    >>> hifa_importdata(vis=['../rawdata/uid___A002_X30a93d_X43e', '../rawdata/uid_A002_x30a93d_X44e'])

    2. Load an MS in the current directory into the context:

    >>> hifa_importdata(vis=['uid___A002_X30a93d_X43e.ms'])

    3. Load a tarred ASDM in ../rawdata into the context:

    >>> hifa_importdata(vis=['../rawdata/uid___A002_X30a93d_X43e.tar.gz'])

    4. Import a list of MeasurementSets:

    >>> myvislist = ['uid___A002_X30a93d_X43e.ms', 'uid_A002_x30a93d_X44e.ms']
    >>> hifa_importdata(vis=myvislist)

    5. Run with explicit setting of data column types:

    >>> hifa_importdata(vis=['uid___A002_X30a93d_X43e_targets.ms'], datacolumns={'data': 'regcal_contline'})
    >>> hifa_importdata(vis=['uid___A002_X30a93d_X43e_targets_line.ms'], datacolumns={'data': 'regcal_line', 'corrected': 'selfcal_line'})

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
