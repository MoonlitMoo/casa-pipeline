from pipeline import show_weblog


def h_weblog(relpath=None):
    """
    h_weblog ---- Open the pipeline weblog in a browser

    h_weblog opens the weblog in a new browser tab or window.

    --------- parameter descriptions ---------------------------------------------

    relpath
                Relative path to the weblog index file. This file must be located
                in a child directory of the CASA working directory. If relpath
                is left unspecified, the most recent weblog will be located and
                displayed.

    --------- examples -----------------------------------------------------------

    1. Open pipeline weblog in a browser:

    >>> h_weblog()

    """
    show_weblog(index_path=relpath)
