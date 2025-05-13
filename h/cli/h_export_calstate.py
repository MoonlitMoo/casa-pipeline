from . import utils


def h_export_calstate(filename=None, state=None):
    """
    h_export_calstate ---- Save the pipeline calibration state to disk

    h_export_calstate saves the current pipeline calibration state to disk
    in the form of a set of equivalent applycal calls.

    If ``filename`` is not given, h_export_calstate saves the calibration state to
    disk with a filename based on the pipeline context creation time, using the
    extension '.calstate'

    One of two calibration states can be exported: either the active calibration
    state (those calibrations currently applied on-the-fly but scheduled for
    permanent application to the MeasurementSet in a subsequent hif_applycal
    call) or the applied calibration state (calibrations that were previously
    applied to the MeasurementSet using hif_applycal). The default is to export
    the active calibration state.

    --------- parameter descriptions ---------------------------------------------

    filename
                Name for saved calibration state.
    state
                The calibration state to export. If undefined, the active
                calibration state will be exported. If set to 'applied', the
                applied calibration state will be exported instead.

    --------- examples -----------------------------------------------------------

    1. Save the calibration state:

    >>> h_export_calstate()

    2. Save the active calibration state with a custom filename:

    >>> h_export_calstate(filename='afterbandpass.calstate')

    3. Save the applied calibration state with a custom filename:

    >>> h_export_calstate(filename='applied.calstate', state='applied')

    --------- issues -----------------------------------------------------------

    If run several times in one pipeline session does the automatic export
    file naming scheme, overwrite previous versions?
    """
    context = utils.get_context()
    if state == 'applied':
        context.callibrary.export_applied(filename)
    else:
        context.callibrary.export(filename)
