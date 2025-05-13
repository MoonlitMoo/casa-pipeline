# General imports

import traceback

import pipeline
from pipeline.infrastructure import casa_tools

# Make pipeline tasks available in local name space
pipeline.initcli(locals())

# IMPORT_ONLY = 'Import only'
IMPORT_ONLY = ''


# Run the procedure
def vlassQLIP(vislist, editimlist_infile, importonly=False, interactive=True):

    echo_to_screen = interactive
    casa_tools.post_to_log("Beginning VLA Sky Survey quick look imaging pipeline run ...")

    try:
        # Initialize the pipeline
        h_init(plotlevel='summary')

        # Load the data
        hifv_importdata(vis=vislist, nocopy=True)
        if importonly:
            raise Exception(IMPORT_ONLY)

        # add imaging target
        hif_editimlist(parameter_file=editimlist_infile, imaging_mode='VLASS-QL')

        # split out selected target data from full MS
        hif_transformimagedata(datacolumn="corrected", clear_pointing=True, modify_weights=False)

        # run tclean and create images
        hif_makeimages(hm_cleaning='manual', hm_masking='none')

        # apply a primary beam correction on images before rms and cutouts
        hifv_pbcor()

        # make uncertainty (rms) image
        hif_makermsimages()

        # make sub-images of final, primary beam, rms and psf images.
        hif_makecutoutimages()

        # Measure of flagged data in the imaging run.
        hifv_flagdata(quack=False, edgespw=False, clip=False, scan=False, autocorr=False, hm_tbuff='manual', template=False, online=False, baseband=False)

        # Export FITS images of primary beam corrected tt0 and RMS cutout images
        hifv_exportvlassdata()

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

        casa_tools.post_to_log("VLASS quick look imaging pipeline finished.  Terminating procedure execution ...",
                               echo_to_screen=echo_to_screen)
