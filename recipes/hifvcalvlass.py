# General imports

import traceback

import pipeline
from pipeline.infrastructure import casa_tools

# Make pipeline tasks available in local name space
pipeline.initcli(locals())

# IMPORT_ONLY = 'Import only'
IMPORT_ONLY = ''


# Run the procedure
def hifvcalvlass(vislist, importonly=False, interactive=True):

    echo_to_screen = interactive
    casa_tools.post_to_log("Beginning VLA Sky Survey pipeline calibration run ...")

    try:
        # Initialize the pipeline
        h_init(plotlevel='summary')
        # h_init(loglevel='trace', plotlevel='summary')

        # Load the data
        hifv_importdata(vis=vislist)
        if importonly:
            raise Exception(IMPORT_ONLY)

        # Hanning smooth the data
        hifv_hanning()

        # Flag known bad data
        hifv_flagdata(intents='*POINTING*,*FOCUS*,*ATMOSPHERE*,*SIDEBAND_RATIO*, *UNKNOWN*, *SYSTEM_CONFIGURATION*, *UNSPECIFIED#UNSPECIFIED*',
                      flagbackup=False, scan=True, baseband=False, clip=True, autocorr=True,
                      hm_tbuff='manual', template=True, online=True, tbuff=0.225, fracspw=0.0,
                      shadow=True, quack=False, edgespw=False)

        # Fill model columns for primary calibrators
        hifv_vlasetjy()

        # Gain curves, opacities, antenna position corrections,
        # requantizer gains (NB: requires CASA 4.1!)
        # tecmaps default is False
        hifv_priorcals(show_tec_maps=True, apply_tec_correction=False, swpow_spw='6,14')

        # Syspower task
        # hifv_syspower()

        # Initial test calibrations using bandpass and delay calibrators
        hifv_testBPdcals()

        # Identify and flag basebands with bad deformatters or rfi based on
        # bp table amps and phases
        # hifv_flagbaddef(doflagundernspwlimit=False)

        # Flag possible RFI on BP calibrator using rflag with mode=bpd
        hifv_checkflag(checkflagmode='bpd-vlass')

        # DO SEMI-FINAL DELAY AND BANDPASS CALIBRATIONS
        # (semi-final because we have not yet determined the spectral index of the bandpass calibrator)
        hifv_semiFinalBPdcals()

        # Use mode=allcals again on calibrators
        hifv_checkflag(checkflagmode='allcals-vlass')

        # Determine solint for scan-average equivalent
        hifv_solint(limit_short_solint='0.45')

        # Do the flux density bootstrapping -- fits spectral index of
        # calibrators with a power-law and puts fit in model column
        hifv_fluxboot(fitorder=2)

        # Make the final calibration tables
        hifv_finalcals()

        # Polarization calibration
        hifv_circfeedpolcal()

        # Flag the finalampgaincal.g calibration table
        hifv_flagcal()

        # Apply all the calibrations and check the calibrated data
        hifv_applycals(flagsum=False, flagdetailedsum=False, gainmap=True)

        # Flag possible RFI on BP calibrator using rflag with mode=bpd
        hifv_checkflag(checkflagmode='target-vlass')

        # Calculate data weights based on standard deviation within each spw
        hifv_statwt()

        # Plotting Summary
        hifv_plotsummary()

        # Apply time offsets to the pointing table
        hifv_fixpointing()

        # Make a list of expected point source calibrators to be cleaned
        # hif_makeimlist(intent='PHASE,BANDPASS')

        # Make clean images for the selected calibrators
        # hif_makeimages()

        # Export the data
        hifv_exportdata(gainmap=True, exportmses=False, exportcalprods=True)

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
