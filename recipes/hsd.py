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

ITERATION = 2


# Run the procedure
def hsdms(vislist, importonly=False, interactive=True):

    echo_to_screen = interactive
    casa_tools.post_to_log("Beginning pipeline run ...")

    try:
        # Initialize the pipeline
        h_init()

        # Load the data
        hsd_importdata(vis=vislist)
        if importonly:
            raise Exception(IMPORT_ONLY)

        # Deterministic flagging
        hsd_flagdata()

        # Tsys calibration
        h_tsyscal()

        # Flag system temperature calibration
        hsd_tsysflag()

        # Compute the sky calibration
        hsd_skycal()

        # Compute the Kelvin to Jansky calibration
        hsd_k2jycal()

        # Apply the calibrations
        hsd_applycal()

        # Calibration of residual atmospheric transmission
        hsd_atmcor()

        # # Improve line mask for baseline subtraction by executing 
        # # hsd_baseline and hsd_blflag iteratively
        for i in range(ITERATION):

            # Baseline subtraction with automatic line detection
            hsd_baseline()

            # Flag data based on baseline quality
            hsd_blflag()

        # Imaging
        hsd_imaging()

        # Export the data
        hsd_exportdata()

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
