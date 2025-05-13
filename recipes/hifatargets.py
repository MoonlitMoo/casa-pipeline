# General imports

import os
import sys
import traceback
import inspect

# Pipeline imports
import pipeline
from pipeline.infrastructure import casa_tools

# Make pipeline tasks available in local name space
pipeline.initcli(locals())

IMPORT_ONLY = 'Import only'


# Run the procedure
def hifatargets(vislist, importonly=False, interactive=True):

    echo_to_screen = interactive
    casa_tools.post_to_log("Beginning pipeline target imaging run ...")

    try:
        # Initialize the pipeline
        h_init()

        # Load the data
        hifa_importdata(vis=vislist, dbservice=True)
        if importonly:
            raise Exception(IMPORT_ONLY)

        # Split out the target data
        hif_mstransform()

        # Flag the target data
        hifa_flagtargets()

        # Check imaging parameters against PI specified values
        hifa_imageprecheck()

        # Check product size limits and mitigate imaging parameters
        hif_checkproductsize(maxcubesize=40.0, maxcubelimit=60.0, maxproductsize=500.0)

        # Make a list of expected targets to be cleaned in mfs mode (used for continuum subtraction)
        hif_makeimlist(specmode='mfs')

        # Find continuum frequency ranges
        hif_findcont()

        # Fit and subtract the continuum using frequency ranges from hif_findcont
        hif_uvcontsub()

        # Make clean mfs images for the selected targets
        hif_makeimages()

        # Make a list of expected targets to be cleaned in cont (aggregate over all spws) mode
        hif_makeimlist(specmode='cont')

        # Make clean cont images for the selected targets
        hif_makeimages()

        # Make a list of expected targets to be cleaned in continuum subtracted cube mode
        hif_makeimlist(specmode='cube')

        # Make clean continuum subtracted cube images for the selected targets
        hif_makeimages()

        # Make a list of expected targets to be cleaned in continuum subtracted PI cube mode
        hif_makeimlist(specmode='repBW')

        # Make clean continuum subtracted PI cube
        hif_makeimages()

        # Export the data
        hifa_exportdata()

    except Exception as e:
        if str(e) == IMPORT_ONLY:
            casa_tools.post_to_log("Exiting after import step ...", echo_to_screen=echo_to_screen)
        else:
            casa_tools.post_to_log("Error in procedure execution ...", echo_to_screen=echo_to_screen)
            errstr = traceback.format_exc()
            casa_tools.post_to_log(errstr, echo_to_screen=echo_to_screen)

    finally:

        # Save the results to the context
        h_save()

        casa_tools.post_to_log("Terminating procedure execution ...", echo_to_screen=echo_to_screen)
