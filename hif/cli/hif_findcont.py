import sys

import pipeline.h.cli.utils as utils


@utils.cli_wrapper
def hif_findcont(vis=None, target_list=None, hm_mosweight=None, hm_perchanweightdensity=None, hm_weighting=None,
                 datacolumn=None, parallel=None):

    """
    hif_findcont ---- Find continuum frequency ranges


    Find continuum frequency ranges for a list of specified targets.

    If the cont.dat file is not already present in the working directory, then dirty image
    cubes are created for each spectral window of each science target at the native channel
    resolution unless the ``nbins`` parameter was used in the preceding hif_makeimlist stage.
    Robust=1 Briggs weighting is used for optimal line sensitivity, even if a different
    robust had been chosen in hifa_imageprecheck to match the PI requested angular resolution.
    Using moment0 and moment8 images of each cube, SNR-based masks are created, and the mean
    spectrum of the joint mask is computed and evaluated with extensive heuristics to find the
    channel ranges that are likely to be free of line emission.  Warnings are generated if
    the channel ranges contain a small fraction of the bandwidth, or sample only a limited
    extent of the spectrum.

    If the cont.dat file already exists in the working directory before this task is executed,
    then it will first examine the contents. For any spw that already has frequency ranges
    defined in this file, it will not perform the analysis described above in favor of the
    a priori ranges. For spws not listed in a pre-existing file, it will analyze them as
    normal and update the file. In either case, the cont.dat file is used by the subsequent
    hif_uvcontsub and hif_makeimages stages.

    results -- The results object for the pipeline task is returned.

    --------- parameter descriptions ---------------------------------------------

    vis                     The list of input MeasurementSets. Defaults to the list of
                            MeasurementSets specified in the h_init or hif_importdata task.
                            \'\': use all MeasurementSets in the context

                            Examples: 'ngc5921.ms', ['ngc5921a.ms', ngc5921b.ms', 'ngc5921c.ms']
    target_list             Dictionary specifying targets to be imaged; blank will read list from context
    hm_mosweight            Mosaic weighting

                            Defaults to '' to enable the automatic heuristics calculation.
                            Can be set to True or False manually.
    hm_perchanweightdensity Calculate the weight density for each channel independently

                            Defaults to '' to enable the automatic heuristics calculation.
                            Can be set to True or False manually.
    hm_weighting            Weighting scheme (natural,uniform,briggs,briggsabs[experimental],briggsbwtaper[experimental])
    datacolumn              Data column to image. Only to be used for manual overriding
                            when the automatic choice by data type is not appropriate.
    parallel                Use MPI cluster where possible

    --------- examples -----------------------------------------------------------

    1. Perform continuum frequency range detection for all science targets and spws:

    >>> hif_findcont()


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
