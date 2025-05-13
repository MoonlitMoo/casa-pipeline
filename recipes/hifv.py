# General imports

import traceback

import pipeline
from pipeline.infrastructure import casa_tools

# Make pipeline tasks available in local name space
pipeline.initcli(locals())

# IMPORT_ONLY = 'Import only'
IMPORT_ONLY = ''


# Run the procedure
def hifv (vislist, importonly=False, interactive=True):

    echo_to_screen = interactive
    casa_tools.post_to_log("Beginning VLA pipeline calibration run ...")

    try:
        # Initialize the pipeline
        h_init(plotlevel='summary')
        # h_init(loglevel='trace', plotlevel='summary')

        # Load the data
        hifv_importdata (vis=vislist)
        if importonly:
            raise Exception(IMPORT_ONLY)

        # Hanning smooth the data
        hifv_hanning()

        # Flag known bad data
        hifv_flagdata(scan=True, hm_tbuff='1.5int', fracspw=0.01,
                      intents='*POINTING*,*FOCUS*,*ATMOSPHERE*,*SIDEBAND_RATIO*, *UNKNOWN*, *SYSTEM_CONFIGURATION*, *UNSPECIFIED#UNSPECIFIED*')

        # Fill model columns for primary calibrators
        hifv_vlasetjy()

        # Gain curves, opacities, antenna position corrections, 
        # requantizer gains (NB: requires CASA 4.1!)
        hifv_priorcals()

        # Syspower task
        hifv_syspower()

        # Initial test calibrations using bandpass and delay calibrators
        # Identify and flag basebands with bad deformatters or rfi based on
        # bp table amps and phases
        hifv_testBPdcals()

        # Flag possible RFI on BP calibrator using rflag
        hifv_checkflag(checkflagmode='bpd-vla')

        # DO SEMI-FINAL DELAY AND BANDPASS CALIBRATIONS
        # (semi-final because we have not yet determined the spectral index of the bandpass calibrator)
        hifv_semiFinalBPdcals()

        # Use flagdata rflag mode again on calibrators
        hifv_checkflag(checkflagmode='allcals-vla')

        # Re-run semi-final delay and bandpass calibrations
        # hifv_semiFinalBPdcals()

        # Determine solint for scan-average equivalent
        hifv_solint()

        # Do the flux density boostrapping -- fits spectral index of
        # calibrators with a heuristics determined fit order
        hifv_fluxboot()

        # Make the final calibration tables
        hifv_finalcals()

        # Polarization calibration
        # hifv_circfeedpolcal()

        # Apply all the calibrations and check the calibrated data
        hifv_applycals()

        # Now run all calibrated data, including the target, through rflag/tfcropflag/extendflag
        hifv_checkflag(checkflagmode='target-vla')

        # Calculate data weights based on standard deviation within each spw
        hifv_statwt()

        # Plotting Summary
        hifv_plotsummary()

        # Make a list of expected point source calibrators to be cleaned
        hif_makeimlist(intent='PHASE,BANDPASS', specmode='cont')

        # Make clean images for the selected calibrators
        hif_makeimages(hm_masking='centralregion')

        # Export the data
        # hifv_exportdata()

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
