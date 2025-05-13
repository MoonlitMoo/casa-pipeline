import sys

import pipeline.h.cli.utils as utils


@utils.cli_wrapper
def hif_checkproductsize(vis=None, maxcubesize=None, maxcubelimit=None, maxproductsize=None, maximsize=None,
                         calcsb=None, parallel=None):

    """
    hif_checkproductsize ---- Check imaging product size


    Check interferometry imaging product size and try to mitigate to maximum
    allowed values. The task implements a mitigation cascade computing the largest
    cube size and tries to reduce it below a given limit by adjusting the ``nbins``,
    ``hm_imsize`` and ``hm_cell`` parameters. If this step succeeds, it also checks the
    overall imaging product size and if necessary reduces the number of fields to
    be imaged.

    Alternatively, if ``maximsize`` is set, the image product pixel count is
    mitigated by trying to adjust ``hm_cell`` parameter. If the pixel count is still
    greater than ``maximsize`` at ``hm_cell`` of 4ppb, then this value is kept and
    the image field is truncated around the phase center by forcing ``hm_imsize``
    = ``maximsize``.

    Note that mitigation for image pixel count and for the product size currently
    are mutually exclusive, with maximsize taking precedence if set.

    Output:

    results -- The results object for the pipeline task is returned.

    --------- parameter descriptions ---------------------------------------------

    vis            The list of input MeasurementSets. Defaults to the list of
                   MeasurementSets specified in the h_init or hif_importdata task.
                   \'\': use all MeasurementSets in the context

                   Examples: 'ngc5921.ms', ['ngc5921a.ms', ngc5921b.ms', 'ngc5921c.ms']
    maxcubesize    Maximum allowed cube size in gigabytes (mitigation goal)
                   -1: automatic from performance parameters
    maxcubelimit   Maximum allowed cube limit in gigabytes (mitigation failure
                   limit)
                   -1: automatic from performance parameters
    maxproductsize Maximum allowed product size in gigabytes (mitigation goal and
                   failure limit)
                   -1: automatic from performance parameters
    maximsize      Maximum allowed image count size (mitigation goal and hard
                   maximum).
                   Parameter ``maximsize`` must be even and divisible by 2,3,5,7 only.
                   Note that ``maximsize`` is disabled by default and cannot be set at
                   the same time as ``maxcubesize``, ``maxcubelimit`` and ``maxproductsize``!
                   -1: disables mitigation for this parameter
    calcsb         Force (re-)calculation of sensitivities and beams
    parallel       Use MPI cluster where possible

    --------- examples -----------------------------------------------------------

    1. Basic call to check the product sizes using internal defaults

    >>> hif_checkproductsize()

    2. Typical ALMA call

    >>> hif_checkproductsize(maxcubesize=40.0, maxcubelimit=60.0, maxproductsize=350.0)


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
