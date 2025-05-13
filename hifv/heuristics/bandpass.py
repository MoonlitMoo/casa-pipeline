import os

import numpy as np

import pipeline.infrastructure.utils as utils
import pipeline.infrastructure as infrastructure
from pipeline.infrastructure import casa_tasks
from pipeline.infrastructure import casa_tools
from . import uvrange

LOG = infrastructure.get_logger(__name__)


def removeRows(caltable, spwids):
    """Remove rows from specified spwid from a CASA caltable."""

    with casa_tools.TableReader(caltable, nomodify=False) as tb:
        for spwid in spwids:
            subtb = tb.query('SPECTRAL_WINDOW_ID == '+str(spwid))
            flaggedrows = subtb.rownumbers()
            if len(flaggedrows) > 0:
                LOG.debug('removing rows from table '+caltable+' for spw='+str(spwid))
                tb.removerows(flaggedrows)
            subtb.close()

    with casa_tools.TableReader(caltable+'/SPECTRAL_WINDOW', nomodify=False) as tb:
        for spwid in spwids:
            temparray = tb.getcol('FLAG_ROW')
            temparray[spwid] = True
            tb.putcol('FLAG_ROW', temparray)


def computeChanFlag(vis, caltable, context):

    m = context.observing_run.get_ms(vis)
    channels = m.get_vla_numchan()

    with casa_tools.TableReader(caltable) as table:
        spwVarCol = table.getvarcol('SPECTRAL_WINDOW_ID')
        dataVarCol = table.getvarcol('CPARAM')
        flagVarCol = table.getvarcol('FLAG')
        rowlist = sorted(dataVarCol.keys())

        spwids = []

        largechunk = False

        for rrow in rowlist:
            dataArr = dataVarCol[rrow]
            flagArr = flagVarCol[rrow]
            spwArr = spwVarCol[rrow]

            ispw = spwArr[0]
            fivepctch = int(0.05*channels[ispw])
            startch1 = 0
            startch2 = fivepctch - 1
            endch1 = channels[ispw] - fivepctch
            endch2 = channels[ispw] - 1
            if (fivepctch < 3):
                startch2=2
                endch1=channels[ispw]-3

            # Find flagged ranges in both polarizations
            rangeA = utils.flagged_intervals(flagArr[0,:].flatten())
            rangeB = utils.flagged_intervals(flagArr[1,:].flatten())

            # print rangeA, rangeB

            # If no solutions are found, only one tuple is returned and make note
            '''
            try:
                if ((rangeA[0][0] == 0 and rangeA[0][1] == len(flagArr[0])-1) or (rangeB[0][0] == 0 and rangeB[0][1] == len(flagArr[1])-1)):
                    LOG.warning("channel pre-averaging bandpass calibration heuristic could not recover solutions for spw="+str(spwArr[0]))
                    print rangeA, rangeB
                    spwids.append(spwArr[0])
                    largechunk = True
            except:
                LOG.warning("Problem with using channel pre-averaging bandpass calibration heuristic - check CASA log")
            '''

            # Determine contiguous lengths of failed solutions for both polarizations, but ignoring edge flagging
            for row in rangeA[1:-1]:
                length = row[-1]-row[0]
                spwids.append(spwArr[0])
                LOG.info('WEAKBP FAILED SOLUTION: SPW '+str(spwArr[0])+': '+str(row[0])+'~'+str(row[-1]))
                if length > len(flagArr[0])/32.0:
                    largechunk = True

            for row in rangeB[1:-1]:
                length = row[-1]-row[0]
                spwids.append(spwArr[0])
                LOG.info('WEAKBP FAILED SOLUTION: SPW '+str(spwArr[0])+': '+str(row[0])+'~'+str(row[-1]))
                if length > len(flagArr[1])/32.0:
                    largechunk = True

            # print rrow.rjust(4), 'Pol A:', str(np.sum(flagArr[0])).rjust(4),' Pol B:',
                    # str(np.sum(flagArr[1])).rjust(4),
                    # ' / ',str(len(flagArr[0])),
                    # ' chan flagged ', '(', 100.0*float(np.sum(flagArr[0]))/len(flagArr[0]),
                    # '%,  ',                                 100.0*float(np.sum(flagArr[1]))/len(flagArr[1]), '%)'

    spwids = np.unique(spwids)
    spwids = list(spwids)

    return (largechunk, spwids)


def do_bandpass(vis, caltable, context=None, RefAntOutput=None, spw=None, ktypecaltable=None,
                bpdgain_touse=None, solint=None, append=None, executor=None):
    """Run CASA task bandpass"""

    m = context.observing_run.get_ms(vis)
    bandpass_field_select_string = context.evla['msinfo'][m.name].bandpass_field_select_string
    bandpass_scan_select_string = context.evla['msinfo'][m.name].bandpass_scan_select_string
    minBL_for_cal = m.vla_minbaselineforcal()

    try:
        setjy_results = context.results[0].read()[0].setjy_results
    except Exception as e:
        setjy_results = context.results[0].read().setjy_results

    BPGainTables = sorted(context.callibrary.active.get_caltable())
    BPGainTables.append(ktypecaltable)
    BPGainTables.append(bpdgain_touse)

    bandpass_task_args = {'vis': vis,
                          'caltable': caltable,
                          'field': '',
                          'spw': spw,
                          'intent': '',
                          'selectdata': True,
                          'uvrange': '',
                          'scan': bandpass_scan_select_string,
                          'solint': solint,
                          'combine': 'scan',
                          'refant': ','.join(RefAntOutput),
                          'minblperant': minBL_for_cal,
                          'minsnr': 5.0,
                          'solnorm': False,
                          'bandtype': 'B',
                          'fillgaps': 0,
                          'smodel': [],
                          'append': append,
                          'docallib': False,
                          'gaintable': BPGainTables,
                          'gainfield': [''],
                          'interp': [''],
                          'spwmap': [],
                          'parang': True}

    bpscanslist = list(map(int, bandpass_scan_select_string.split(',')))
    scanobjlist = m.get_scans(scan_id=bpscanslist)
    allfieldidlist = []
    for scanobj in scanobjlist:
        fieldobj, = scanobj.fields
        if str(fieldobj.id) not in allfieldidlist:
            allfieldidlist.append(str(fieldobj.id))

    # See vlascanheuristics - only use the first bandpass calibrator
    fieldidlist = [fieldid for fieldid in allfieldidlist if fieldid in bandpass_field_select_string]

    for fieldidstring in fieldidlist:
        fieldid = int(fieldidstring)
        uvrangestring = uvrange(setjy_results, fieldid)
        bandpass_task_args['field'] = fieldidstring
        bandpass_task_args['uvrange'] = uvrangestring
        if os.path.exists(caltable):
            bandpass_task_args['append'] = True

        job = casa_tasks.bandpass(**bandpass_task_args)

        executor.execute(job)

    return True


def do_bandpassweakbp(vis, caltable, context=None, RefAntOutput=None, spw=None, ktypecaltable=None,
                      bpdgain_touse=None, solint=None, append=None):
    """Run CASA task bandpass"""

    m = context.observing_run.get_ms(vis)
    bandpass_field_select_string = context.evla['msinfo'][m.name].bandpass_field_select_string
    bandpass_scan_select_string = context.evla['msinfo'][m.name].bandpass_scan_select_string
    minBL_for_cal = m.vla_minbaselineforcal()

    BPGainTables = sorted(context.callibrary.active.get_caltable())
    BPGainTables.append(ktypecaltable)
    BPGainTables.append(bpdgain_touse)

    bandpass_task_args = {'vis': vis,
                          'caltable': caltable,
                          'field': bandpass_field_select_string,
                          'spw': spw,
                          'intent': '',
                          'selectdata': True,
                          'uvrange': '',
                          'scan': bandpass_scan_select_string,
                          'solint': solint,
                          'combine': 'scan',
                          'refant': ','.join(RefAntOutput),
                          'minblperant': minBL_for_cal,
                          'minsnr': 5.0,
                          'solnorm': False,
                          'bandtype': 'B',
                          'fillgaps': 0,
                          'smodel': [],
                          'append': append,
                          'docallib': False,
                          'gaintable': BPGainTables,
                          'gainfield': [''],
                          'interp': [''],
                          'spwmap': [],
                          'parang': True}

    job = casa_tasks.bandpass(**bandpass_task_args)

    return job


def weakbp(vis, caltable, context=None, RefAntOutput=None, ktypecaltable=None,
           bpdgain_touse=None, solint=None, append=None, executor=None, spw=''):

    m = context.observing_run.get_ms(vis)
    channels = m.get_vla_numchan()  # Number of channels before averaging

    bpjob = do_bandpassweakbp(vis, caltable, context=context, spw=spw, RefAntOutput=RefAntOutput,
                              ktypecaltable=ktypecaltable, bpdgain_touse=bpdgain_touse, solint='inf', append=False)
    executor.execute(bpjob)
    (largechunk, spwids) = computeChanFlag(vis, caltable, context)
    # print largechunk, spwids
    if not largechunk and spwids == []:
        # All solutions found - proceed as normal with the pipeline
        interp = ''
        return interp

    LOG.warning("Solutions for all channels not obtained.  Using weak bandpass calibration heuristic.")
    cpa = 2  # Channel pre-averaging
    while largechunk:

        LOG.info("Removing rows in table " + caltable + " for spws="+','.join([str(i) for i in spwids]))
        removeRows(caltable, spwids)
        solint = 'inf,' + str(cpa) + 'ch'
        LOG.warning("Largest contiguous set of channels with no BP solution is greater than maximum " +
                    "allowable 1/32 fractional bandwidth for spw="+','.join([str(i) for i in spwids])
                    + "." + "  Using solint=" + solint)
        LOG.info('Weak bandpass calibration heuristic.  Using solint='+solint)
        bpjob = do_bandpassweakbp(vis, caltable, context=context, RefAntOutput=RefAntOutput,
                                  spw=','.join([str(i) for i in spwids]),
                                  ktypecaltable=ktypecaltable, bpdgain_touse=bpdgain_touse, solint=solint, append=True)
        executor.execute(bpjob)
        (largechunk, spwids) = computeChanFlag(vis, caltable, context)
        for spw in spwids:
            preavgnchan = channels[spw]/float(cpa)
            LOG.debug("CPA: " + str(cpa) + "   NCHAN: "+str(preavgnchan)+"    NCHAN/32: "+str(preavgnchan/32.0))
            if cpa > preavgnchan/32.0:
                LOG.warning("Limiting pre-averaging to maximum 1/32 fractional bandwidth for spw="+str(spw)
                            + ". Interpolation in applycal will need to extend over greater " +
                            "than 1/32 fractional bandwidth, which may fail to capture significant bandpass structure.")
                largechunk = False  # This will break the while loop and move onto applycal
        cpa = cpa * 2

    LOG.warning("Channel gaps in bandpass solutions will be linearly interpolated over in applycal.")
    LOG.warning("Accuracy of bandpass solutions will be slightly degraded at interpolated channels, " +
                "particularly if these fall at spectral window edges where applycal will " +
                "perform 'nearest' extrapolation.")
    interp = 'nearest'
    return interp
