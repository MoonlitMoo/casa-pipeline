import sys

from . import utils


def h_show_calstate():
    """
    h_show_calstate ---- Show the current pipeline calibration state

    h_show_calstate displays the current on-the-fly calibration state
    of the pipeline as a set of equivalent applycal calls.

    --------- parameter descriptions ---------------------------------------------

    --------- examples -----------------------------------------------------------

    1. Show the current on-the-fly pipeline calibration state.

    >>> h_show_calstate()

    """
    context = utils.get_context()
    sys.stdout.write('Current on-the-fly calibration state:\n\n')
    sys.stdout.write(str(context.callibrary.active))
    sys.stdout.write('\n')
