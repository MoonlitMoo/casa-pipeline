import traceback

import pipeline
from pipeline.infrastructure import casa_tools

# Make pipeline tasks available in local name space
pipeline.initcli(locals())

IMPORT_ONLY = 'Import only'


# Run the procedure
def hifv_contimage(vislist, importonly=False, interactive=True):

    echo_to_screen = interactive
    casa_tools.post_to_log("Beginning VLA pipeline continuum imaging run ...")

    try:
        # Initialize the pipeline
        h_init(plotlevel='summary')

        # Load the data
        hifv_importdata(vis=vislist)
        if importonly:
            raise Exception(IMPORT_ONLY)

        # Flag target data
        hifv_flagtargetsdata()

        # Split out the target data
        hif_mstransform()

        # Check product size limits and mitigate image size
        hif_checkproductsize(maximsize=16384)

        # Make a list of expected targets to be cleaned in cont (aggregate over all spws) mode
        hif_makeimlist(specmode='cont')

        # Make clean cont images for the selected targets
        hif_makeimages(hm_cyclefactor=3.0)

        # apply a primary beam correction on target images
        hifv_pbcor()

        # Export the data
        hifv_exportdata(imaging_products_only=True)

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

        casa_tools.post_to_log("VLA CASA Pipeline finished.  Terminating procedure execution ...",
                               echo_to_screen=echo_to_screen)
