import sys

import pipeline.h.cli.utils as utils


@utils.cli_wrapper
def hifv_importdata(vis=None, session=None, asis=None, overwrite=None, nocopy=None, createmms=None,
                    ocorr_mode=None, datacolumns=None, specline_spws=None, parallel=None):

    """
    hifv_importdata ---- Imports data into the VLA pipeline

    The hifv_importdata task loads the specified visibility data into the pipeline
    context unpacking and / or converting it as necessary.

    Output:

    results -- The results object for the pipeline task is returned.

    --------- parameter descriptions ---------------------------------------------

    vis           List of visibility data files. These may be ASDMs, tar files of ASDMs,
                  MSes, or tar files of MSes, If ASDM files are specified, they will be
                  converted  to MS format.
                  example: vis=['X227.ms', 'asdms.tar.gz']
    session       List of sessions to which the visibility files belong. Defaults
                  to a single session containing all the visibility files, otherwise
                  a session must be assigned to each vis file.
                  example: session=['Session_1', 'Sessions_2']
    asis          Creates verbatim copies of the ASDM tables in the output MS.
                  The value given to this option must be a list of table names
                  separated by space characters.
                  examples: 'Receiver CalAtmosphere'
                  'Receiver', ''
    overwrite     Overwrite existing files on import.
    nocopy        When importing an MS, disable copying of the MS to the working directory.
    createmms     Create a multi-MeasurementSet ('true') ready for parallel
                  processing, or a standard MeasurementSet ('false'). The default setting
                  ('automatic') creates an MMS if running in a cluster environment.
    ocorr_mode    Read in cross- and auto-correlation data(ca), cross-
                  correlation data only (co), or autocorrelation data only (ao).
    datacolumns   Dictionary defining the data types of
                  existing columns. The format is:

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
    specline_spws String indicating how the pipeline should determine whether a spw should
                  be processed as a spectral line window or continuum. The default setting of 
                  'auto' will use defined heuristics to determine this definition. Accepted
                  values are 'auto', 'none' (no spws will be defined as spectral line), or 
                  a string of spw definitions in the CASA format
                  example: specline_spws='2,3,4~9,23'
    parallel      Execute using CASA HPC functionality, if available.

    --------- examples -----------------------------------------------------------


    1. Load an ASDM list in the ../rawdata subdirectory into the context.

    >>> hifv_importdata (vis=['../rawdata/uid___A002_X30a93d_X43e', '../rawdata/uid_A002_x30a93d_X44e'])

    2. Load an MS in the current directory into the context.

    >>> hifv_importdata (vis=['uid___A002_X30a93d_X43e.ms'])

    3. Load a tarred ASDM in ../rawdata into the context.

    >>> hifv_importdata (vis=['../rawdata/uid___A002_X30a93d_X43e.tar.gz'])

    4. Check the hifv_importdata inputs, then import the data

    >>> myvislist = ['uid___A002_X30a93d_X43e.ms', 'uid_A002_x30a93d_X44e.ms']
    >>> hifv_importdata(vis=myvislist)

    5. Run with explicit setting of data column types:

    >>> hifv_importdata(vis=['uid___A002_X30a93d_X43e_targets.ms'], datacolumns={'data': 'regcal_contline'})
    >>> hifv_importdata(vis=['uid___A002_X30a93d_X43e_targets_line.ms'], datacolumns={'data': 'regcal_line', 'corrected': 'selfcal_line'})


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
