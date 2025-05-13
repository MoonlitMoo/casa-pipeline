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
def hifacal(vislist, importonly=True, dbservice=True, interactive=True):

    echo_to_screen = interactive
    casa_tools.post_to_log("Beginning ALMA calibration pipeline run ...")

    try:
        # Initialize the pipeline
        h_init()

        # Load the data
        hifa_importdata(vis=vislist, dbservice=dbservice)
        if importonly:
            raise Exception(IMPORT_ONLY)

        # Flag known bad data
        hifa_flagdata()

        # Flag lines in solar system calibrators and compute the default
        # reference spectral window map.
        hifa_fluxcalflag()

        # Flag bad channels in the raw data
        hif_rawflagchans()

        # Compute the prioritized lists of reference antennas
        hif_refant()

        # Compute the system temperature calibration
        h_tsyscal()

        # Flag system temperature calibration
        hifa_tsysflag()

        # flag line contamination
        hifa_tsysflagcontamination()

        # Flag system temperature calibration
        hifa_antpos()

        # Compute the WVR calibration, flag and interpolate over bad antennas
        hifa_wvrgcalflag()

        # Flag antennas with low gain
        hif_lowgainflag()

        # Set the flux calibrator model
        hif_setmodels()

        # Derive and temporarily apply a preliminary bandpass calibration,
        # and flag outliers in corrected - model amplitudes for bandpass
        # calibrator.
        hifa_bandpassflag()

        # Compute the bandpass calibration.
        hifa_bandpass()

        # Compute phase calibration spw map and per spw phase offsets.
        hifa_spwphaseup()

        # Derive the flux density scale from standard calibrators, and flag
        # outliers in corrected - model amplitudes for flux and phase
        # calibrators.
        hifa_gfluxscaleflag()

        # Determine flux values for the bandpass and gain calibrators
        # assuming point sources and set their model fluxes
        hifa_gfluxscale()

        # Compute the time dependent gain calibration
        hifa_timegaincal()

        # Run renormalization
        hifa_renorm(createcaltable=True, atm_auto_exclude=True)

        # Flag ultrahigh calibrated target data
        hifa_targetflag()

        # Apply the calibrations
        hif_applycal()

        # Make a list of expected point source calibrators to be cleaned
        hif_makeimlist(intent='PHASE,BANDPASS,AMPLITUDE')

        # Make clean images for the selected calibrators
        hif_makeimages()

        # Make a list of check source calibrators to be cleaned
        hif_makeimlist(intent='CHECK', per_eb=True)

        # Make clean images for the selected calibrators
        hif_makeimages()

        # Check imaging parameters against PI specified values
        hifa_imageprecheck()

        # Check product size limits and mitigate imaging parameters
        hif_checkproductsize(maxcubesize=40.0, maxcubelimit=60.0, maxproductsize=500.0)

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
