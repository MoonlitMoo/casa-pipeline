from . import cli
import pipeline.infrastructure.launcher as launcher


def h_resume(filename=None):

    """
    h_resume ---- Restore a save pipeline state

    h_resume restores a name pipeline state from disk, allowing a
    suspended pipeline reduction session to be resumed.

    --------- parameter descriptions ---------------------------------------------

    filename Name of the saved pipeline state. Setting filename to 'last'
             restores the most recently saved pipeline state whose name
             begins with 'context*'.

    --------- examples -----------------------------------------------------------


    1. Resume the last saved session

    >>> h_resume()

    2. Resume the named saved session

    >>> h_resume(filename='context.s3.2012-02-13T10:49:11')


    """

    _filename = 'last' if filename is None else filename
    pipeline = launcher.Pipeline(context=_filename)

    cli.stack[cli.PIPELINE_NAME] = pipeline

    return pipeline.context
