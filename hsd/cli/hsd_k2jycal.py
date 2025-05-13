import sys

import pipeline.h.cli.utils as utils


@utils.cli_wrapper
def hsd_k2jycal(dbservice=None, endpoint=None, reffile=None,
                infiles=None, caltable=None):

    """
    hsd_k2jycal ---- Derive Kelvin to Jy calibration tables


    Derive the Kelvin to Jy calibration for list of MeasurementSets.

    results -- The results object for the pipeline task is returned.

    --------- parameter descriptions ---------------------------------------------

    dbservice     Whether or not accessing Jy/K DB to retrieve conversion factors.
    endpoint      Which endpoints to use for query
                  options: 'asdm', 'model-fit', 'interpolation'
    reffile       Path to a file containing Jy/K factors for science data, which
                  must be provided by associating calibrator reduction or the observatory
                  measurements. Jy/K factor must take into account all efficiencies, i.e.,
                  it must be a direct conversion factor from Ta* to Jy. The file must be
                  in either MS-based or session-based format. The MS-based format must
                  be in an CSV format with five fields: MS name, antenna name, spectral
                  window id, polarization string, and Jy/K conversion factor. Example for
                  the file is as follows:

                      MS,Antenna,Spwid,Polarization,Factor
                      uid___A002_X316307_X6f.ms,CM03,5,XX,10.0
                      uid___A002_X316307_X6f.ms,CM03,5,YY,12.0
                      uid___A002_X316307_X6f.ms,PM04,5,XX,2.0
                      uid___A002_X316307_X6f.ms,PM04,5,YY,5.0

                  The first line in the above example is a header which may or may not
                  exist. Example for the session-based format is as follows:

                      #OUSID=XXXXXX
                      #OBJECT=Uranus
                      #FLUXJY=yy,zz,aa
                      #FLUXFREQ=YY,ZZ,AA
                      #sessionID,ObservationStartDate(UTC),ObservationEndDate(UTC),
                      Antenna,BandCenter(MHz),BandWidth(MHz),POL,Factor
                      1,2011-11-11 01:00:00,2011-11-11 01:30:00,CM02,86243.0,500.0,I,10.0
                      1,2011-11-11 01:00:00,2011-11-11 01:30:00,CM02,86243.0,1000.0,I,30.0
                      1,2011-11-11 01:00:00,2011-11-11 01:30:00,CM03,86243.0,500.0,I,50.0
                      1,2011-11-11 01:00:00,2011-11-11 01:30:00,CM03,86243.0,1000.0,I,70.0
                      1,2011-11-11 01:00:00,2011-11-11 01:30:00,ANONYMOUS,86243.0,500.0,I,30.0
                      1,2011-11-11 01:00:00,2011-11-11 01:30:00,ANONYMOUS,86243.0,1000.0,I,50.0
                      2,2011-11-13 01:45:00,2011-11-13 02:15:00,PM04,86243.0,500.0,I,90.0
                      2,2011-11-13 01:45:00,2011-11-13 02:15:00,PM04,86243.0,1000.0,I,110.0
                      2,2011-11-13 01:45:00,2011-11-13 02:15:00,ANONYMOUS,86243.0,500.0,I,90.0
                      2,2011-11-13 01:45:00,2011-11-13 02:15:00,ANONYMOUS,86243.0,1000.0,I,110.0

                  Lines starting with '#' are meta data and header.
                  The header must exist. The factor to apply is identified by matching the
                  session ID, antenna name, frequency and polarization of data in each line of
                  the file. Note the observation date is supplementary information and not used
                  for the matching so far. The lines whose antenna name is 'ANONYMOUS' are used
                  when there is no measurement for specific antenna in the session. In the above
                  example, if science observation of session 1 contains the antenna PM04, Jy/K
                  factor for ANONYMOUS antenna will be applied since there is no measurement for
                  PM04 in session 1.
                  If no file name is specified or specified file doesn't exist, all Jy/K factors
                  are set to 1.0.

                  example: reffile='', reffile='working/jyperk.csv'
    infiles       List of input MeasurementSets.
                  
                  example: vis='ngc5921.ms'
    caltable      Name of output gain calibration tables.
                  
                  example: caltable='ngc5921.gcal'

    --------- examples -----------------------------------------------------------


    1. Compute the Kevin to Jy calibration tables for a list of MeasurementSets:

    >>> hsd_k2jycal()


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
