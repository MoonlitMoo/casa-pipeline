import sys

import pipeline.h.cli.utils as utils


@utils.cli_wrapper
def hifa_fluxcalflag(vis=None, field=None, intent=None, spw=None, threshold=None, appendlines=None, linesfiles=None,
                     applyflags=None):
    """
    hifa_fluxcalflag ---- Locate and flag line regions in solar system flux calibrators


    Search the built-in solar system flux calibrator line catalog for overlaps with
    the science spectral windows. Generate a list of line overlap regions and
    flagging commands.

    Output:

        results -- The results object for the pipeline task is returned.

    --------- parameter descriptions ---------------------------------------------

    vis
                  The list of input MeasurementSets. Defaults to the list of
                  MeasurementSets defined in the pipeline context.
    field
                  The list of field names or field ids for which the models are
                  to be set. Defaults to all fields with intent 'AMPLITUDE'.

                  Example: field='3C279', field='3C279, M82'
    intent
                  A string containing a comma delimited list of intents against
                  which the selected fields are matched. Defaults to all data
                  with amplitude intent.

                  Example: intent='AMPLITUDE'
    spw
                  Spectral windows and channels for which bandpasses are
                  computed. Defaults to all science spectral windows.

                  Example: spw='11,13,15,17'
    threshold
                  If the fraction of a spectral window occupied by line regions
                  is greater than this threshold value, then flag the entire
                  spectral window.
    appendlines
                  Append user defined line regions to the line dictionary.
    linesfile
                  Read in a file containing lines regions and append it to the
                  builtin dictionary. Blank lines and comments beginning with #
                  are skipped. The data is contained in 4 whitespace delimited
                  fields containing the solar system object field name, e.g.
                  'Callisto', the molecular species name, e.g. '13CO', and the
                  starting and ending frequency in GHz.
    applyflags
                  Boolean for whether to apply the generated flag commands. (default True)

    --------- examples -----------------------------------------------------------

    1. Locate known lines in any solar system object flux calibrators:

    >>> hifa_fluxcalflag()

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
