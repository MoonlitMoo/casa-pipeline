from . import utils


def h_import_calstate(filename):
    """
    h_import_calstate ---- Import a calibration state from disk

    h_import_calstate clears and then recreates the pipeline calibration state
    based on the set of applycal calls given in the named file. The applycal
    statements are interpreted in additive fashion; for identically specified
    data selection targets, caltables specified in later statements will be added
    to the state created by earlier calls.

    --------- parameter descriptions ---------------------------------------------

    filename
                Name of the saved calibration state

    --------- examples -----------------------------------------------------------

    1. Import a calibration state from disk.

    >>> h_import_calstate(filename='aftergaincal.calstate')

    """
    context = utils.get_context()
    context.callibrary.import_state(filename)
