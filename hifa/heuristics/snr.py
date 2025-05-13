import collections
import math
import os
import sys
from copy import deepcopy

import numpy as np

import pipeline.infrastructure as infrastructure
from pipeline.h.tasks.importdata.fluxes import ORIGIN_XML, ORIGIN_ANALYSIS_UTILS
from pipeline.hifa.tasks.importdata.dbfluxes import ORIGIN_DB
from pipeline.infrastructure import casa_tasks
from pipeline.infrastructure import casa_tools
from pipeline.infrastructure.utils.math import round_up

LOG = infrastructure.get_logger(__name__)


"""
The ALMA receiver band, nominal tsys, and sensitivity info.
    This information should go elsewhere in the next release
    The ALMA receiver bands are defined per pipeline convention
"""
ALMA_BANDS = ['ALMA Band 1', 'ALMA Band 2', 'ALMA Band 3', 'ALMA Band 4', 'ALMA Band 5', 'ALMA Band 6',
              'ALMA Band 7', 'ALMA Band 8', 'ALMA Band 9', 'ALMA Band 10']
ALMA_TSYS = [56.0, 65.0, 75.0, 86.0, 120.0, 90.0, 150.0, 387.0, 1200.0, 1515.0]
# Sensitivities in mJy (for 16*12 m antennas, 1 minute, 8 GHz, 2pol)
ALMA_SENSITIVITIES = [0.16, 0.19, 0.20, 0.24, 0.37, 0.27, 0.50, 1.29, 5.32, 8.85]
ALMA_FIDUCIAL_NUM_ANTENNAS = 16
ALMA_FIDUCIAL_EXP_TIME = 1.0  # minutes
ALMA_FIDUCIAL_BANDWIDTH = 8.0e9  # Hz

# origins with smaller numbers are preferred over those with larger numbers
# this is a dict rather than a list so that a default preference order can be
# provided for origins not listed here (setjy, etc.)
# Note the str() - this is so we get the value of the constant, not the name
# of the constant!
PREFERRED_ORIGIN_ORDER = {
    str(ORIGIN_ANALYSIS_UTILS): 1,
    str(ORIGIN_DB): 2,
    str(ORIGIN_XML): 3
}


def estimate_gaincalsnr(ms, fieldlist, intent, spwidlist, compute_nantennas,
                        max_fracflagged, edge_fraction):

    """Estimate the signal to noise of the phase measurements and return it
    in the form of a dictionary.

    Input Parameters
                   ms: The pipeline context ms object
        fieldnamelist: The list of field names to be selected
               intent: The intent of the fields to be selected
            spwidlist: The list of spw ids to be selected
    compute_nantennas: The algorithm for computing the number of unflagged antennas ('all', 'flagged')
      max_fracflagged: The maximum fraction of an antenna can be flagged, e.g. 0.90

    The output SNR dictionary

    The SNR dictionary keys and values
        key: the spw id     value: The science spw id as an integer

    The SNR dictionary keys and values
        TBD

    """

    # Get the flux dictionary from the pipeline context
    flux_dict = get_fluxinfo(ms, fieldlist, intent, spwidlist)
    if not flux_dict:
        LOG.info('No flux values')
        return {}

    # Get the Tsys dictionary
    #    This dictionary defines the science to Tsys scan mapping and the
    #    science spw to Tsys spw mapping.
    #    Return if there are no Tsys spws for the bandpass calibrator.
    tsys_dict = get_tsysinfo(ms, fieldlist, intent, spwidlist)
    if not tsys_dict:
        LOG.info('No Tsys spws')
        return {}

    # Construct the Tsys spw list and the associated phase scan list.
    # from the Tsys dictionary
    tsys_spwlist, scan_list = make_tsyslists(spwidlist, tsys_dict)
    tsystemp_dict = get_mediantemp(ms, tsys_spwlist, scan_list, antenna='', temptype='tsys')
    if not tsystemp_dict:
        LOG.info('No Tsys estimates')
        return {}

    # Get the observing characteristics dictionary as a function of spw
    #    This includes the spw configuration, time on source and
    #    integration information
    obs_dict = get_obsinfo(ms, fieldlist, intent, spwidlist, compute_nantennas=compute_nantennas,
                           max_fracflagged=max_fracflagged)
    if not obs_dict:
        LOG.info('No observation scans')
        return {}

    # Combine all the dictionariies
    spw_dict = join_dicts(spwidlist, tsys_dict, flux_dict, tsystemp_dict, obs_dict)

    # Compute the gain SNR values for each spw
    gaincalsnr_dict = compute_gaincalsnr(ms, spwidlist, spw_dict, edge_fraction=edge_fraction)

    return gaincalsnr_dict


def estimate_bpsolint(ms, fieldlist, intent, spwidlist, compute_nantennas, max_fracflagged, phaseupsnr,
                      minphaseupints, bpsnr, minbpnchan, evenbpsolints=False):
    """Estimate the optimal solint for the selected bandpass data and return
    the solution in the form of a dictionary.

    Input Parameters
                   ms: The pipeline context ms object
        fieldnamelist: The list of field names to be selected
               intent: The intent of the fields to be selected
            spwidlist: The list of spw ids to be selected
    compute_nantennas: The algorithm for computing the number of unflagged antennas ('all', 'flagged')
      max_fracflagged: The maximum fraction of an antenna can be flagged, e.g. 0.90
           phaseupsnr: The desired phaseup gain solution SNR, e.g. 20.0
       minphaseupints: The minimum number of phaseup solution intervals, e.g. 2
                bpsnr: The desired bandpass solution SNR, e.g. 50.0
           minbpnchan: The minimum number of bandpass solution intervals, e.g. 8

    The output solution interval dictionary

    The bandpass preaveraging dictionary keys and values
        key: the spw id     value: The science spw id as an integer

    The preaveraging parameter dictionary keys and values
        key: 'band'               value: The ALMA receiver band
        key: 'frequency_Hz'       value: The frequency of the spw
        key: 'nchan_total'        value: The total number of channels
        key: 'chanwidth_Hz'       value: The median channel width in Hz

        key: 'tsys_spw'           value: The tsys spw id as an integer
        key: 'median_tsys'        value: The median tsys value

        key: 'flux_Jy'            value: The flux of the source in Jy
        key: 'exptime_minutes'    value: The exposure time in minutes
        key: 'snr_per_channel'    value: The signal to noise per channel
        key: 'sensitivity_per_channel_mJy'    value: The sensitivity in mJy per channel

        key: 'bpsolint'           value: The frequency solint in MHz
    """

    # Get the flux dictionary from the pipeline context
    flux_dict = get_fluxinfo(ms, fieldlist, intent, spwidlist)
    if not flux_dict:
        LOG.info('No flux values')
        return {}

    # Get the Tsys dictionary
    #    This dictionary defines the science to Tsys scan mapping and the
    #    science spw to Tsys spw mapping.
    #    Return if there are no Tsys spws for the bandpass calibrator.
    tsys_dict = get_tsysinfo(ms, fieldlist, intent, spwidlist)
    if not tsys_dict:
        LOG.info('No Tsys spws')
        return {}

    # Construct the Tsys spw list and the associated bandpass scan list.
    # from the Tsys dictionary
    tsys_spwlist, scan_list = make_tsyslists(spwidlist, tsys_dict)
    tsystemp_dict = get_mediantemp(ms, tsys_spwlist, scan_list, antenna='', temptype='tsys')
    if not tsystemp_dict:
        LOG.info('No Tsys estimates')
        return {}

    # Get the observing characteristics dictionary as a function of spw
    #    This includes the spw configuration, time on source and
    #    integration information
    obs_dict = get_obsinfo(ms, fieldlist, intent, spwidlist, compute_nantennas=compute_nantennas,
                           max_fracflagged=max_fracflagged)
    if not obs_dict:
        LOG.info('No observation scans')
        return {}

    # Combine all the dictionariies
    spw_dict = join_dicts(spwidlist, tsys_dict, flux_dict, tsystemp_dict, obs_dict)

    # Compute the bandpass solint parameters and return a solution
    # dictionary
    solint_dict = compute_bpsolint(ms, spwidlist, spw_dict, phaseupsnr, minphaseupints, bpsnr, minbpnchan,
                                   evenbpsolints=evenbpsolints)

    return solint_dict


def get_fluxinfo(ms, fieldnamelist, intent, spwidlist):
    """Retrieve the fluxes of selected sources from the pipeline context
    as a function of spw id and return the results in a dictinary indexed
    by spw id.

    The input parameters
               ms: The pipeline context ms object
    fieldnamelist: The list of field names to be selected
           intent: The intent of the fields to be selected
          spwlist: The list of spw ids to be selected

    The output flux dictionary fluxdict

    The flux dictionary key and value
        key: The spw id      value: The source flux dictionary

    The source flux dictionary keys and values
        key: 'field_name'    value: The name of the source, e.g. '3C286'
        key: 'flux'          value: The flux of the source in Jy, e.g. 1.53
    """

    # Initialize the flux dictionary as an ordered dictionary
    fluxdict = collections.OrderedDict()
    LOG.info('Finding sources fluxes')

    # Loop over the science spectral windows
    for spwid in spwidlist:

        # Get the spectral window object.
        try:
            spw = ms.get_spectral_window(spwid)
        except KeyError:
            continue

        # Loop over field names. There is normally only one.
        for fieldname in fieldnamelist:

            # Get fields associated with the name and intent.
            #    There should be only one. If there is more
            #    than one pick the first field.
            fields = ms.get_fields(name=fieldname, intent=intent)
            if len(fields) <= 0:
                continue
            field = fields[0]

            # Check for flux densities
            if len(field.flux_densities) <= 0:
                continue

            # Find the flux for the spw, sorted by origin
            try:
                flux = min([fd for fd in field.flux_densities if fd.spw_id == spw.id],
                           key=lambda f: PREFERRED_ORIGIN_ORDER.get(f.origin, 1e9))
            except ValueError:
                # no flux for this spectral window
                continue

            I, _, _, _ = flux.casa_flux_density
            fluxdict[spw.id] = collections.OrderedDict()
            fluxdict[spw.id]['field_name'] = fieldname
            fluxdict[spw.id]['flux'] = I
            LOG.info('Setting flux for field {} spw {} to {:0.4f} Jy'.format(fieldname, spw.id, I))

    return fluxdict


def get_tsysinfo(ms, fieldnamelist, intent, spwidlist):
    """Get the tsys information as functions of spw and return a dictionary.

    Input parameters
               ms: The pipeline context ms object
    fieldnamelist: The list of field names to be selected
           intent: The intent of the fields to be selected
       spwidlist: The list of spw ids to be selected

    The output dictionary

    The tsys dictionary tsysdict keys and values
        key: The spw id       value: The Tsys source dictionary

    The tsys source dictionary keys and values
        key: 'tsys_field_name'  value: The name of the Tsys field source, e.g. '3C286' (was 'atm_field_name')
        key: 'intent'           value: The intent of the selected source, e.g. 'BANDPASS'
        key: 'snr_scan'         value: The scan associated with Tsys used to compute the SNR, e.g. 4
        key: 'tsys_scan'        value: The Tsys scan to be used for Tsys computation, e.g. 3
        key: 'tsys_spw'         value: The Tsys spw associated with the science spw id, e.g. 13
    """

    # Initialize
    tsysdict = collections.OrderedDict()
    LOG.info('Matching spws')

    ##### Helper function
    def get_scans_for_field_intent(msobj, fieldname_list, scan_intent):
        """
        Return a list of scan object that matches an intent and
        the list of field names

        Inputs:
            msobj: Measurementset object
            fieldname_list: a list of field names
            scan_intent: a Pipeline scan intent string
        Retruns: a list of scan objects
        """
        # Get the list of unique field names
        fieldset = set(fieldname_list)

        # Get scans with the intent associated with the field name list
        scans = []
        for scan in msobj.get_scans(scan_intent=scan_intent):
            # Remove scans not associated with the input field names
            scanfieldset = {field.name for field in scan.fields}
            if len(fieldset.intersection(scanfieldset)) == 0:
                continue
            scans.append(scan)
        return scans
    ###### End: helper function

    # Get atmospheric scans associated with the field name list
    atmscans = get_scans_for_field_intent(ms, fieldnamelist, 'ATMOSPHERE')

    # If no atmospheric scans were found, and the intent specifies a phase
    # calibrator or check source, then try to find atmospheric scans associated
    # with the science target fields.
    if not atmscans and intent in ['CHECK', 'PHASE']:
        fieldlist = [f.name for f in ms.get_fields(intent='TARGET')]
        if fieldlist:
            atmscans = get_scans_for_field_intent(ms, fieldlist, 'ATMOSPHERE')

    # If still no atmospheric scans were found, and the intent specifies a
    # check source, then try to find atmospheric scans associated with the
    # phase calibrator fields.
    if not atmscans and intent == 'CHECK':
        fieldlist = [f.name for f in ms.get_fields(intent='PHASE')]
        if fieldlist:
            atmscans = get_scans_for_field_intent(ms, fieldlist, 'ATMOSPHERE')

    # Still no atmospheric scans found
    #    Return
    if not atmscans:
        return tsysdict

    # Get the scans associated with the field name list and intent
    obscans = get_scans_for_field_intent(ms, fieldnamelist, intent)

    # No data scans found
    if not obscans:
        return tsysdict

    # Loop over the science spws
    for spwid in spwidlist:

        # Get spectral window
        try:
            spw = ms.get_spectral_window(spwid)
        except:
            continue

        # Find best atmospheric spw
        #    This dictionary is created only if the spw id is valid
        ftsysdict = collections.OrderedDict()
        for atmscan in atmscans:

            # Get field name
            scanfieldlist = [field.name for field in atmscan.fields]
            fieldname = scanfieldlist[0]

            # Get tsys spws and spw ids
            scanspwlist = [scanspw for scanspw in list(atmscan.spws)
                           if scanspw.num_channels not in ms.exclude_num_chans]
            scanspwidlist = [scanspw.id for scanspw in list(atmscan.spws)
                             if scanspw.num_channels not in ms.exclude_num_chans]

            # Match the Tsys spw to the science spw
            #   Match first by id then by frequency
            bestspwid = None
            if spw.id in scanspwidlist:
                # bestspwid = scanspw.id
                bestspwid = spw.id
            else:
                mindiff = sys.float_info.max
                for scanspw in scanspwlist:
                    if spw.band != scanspw.band:
                        continue
                    if spw.baseband != scanspw.baseband:
                        continue
                    diff = abs(spw.centre_frequency.value - scanspw.centre_frequency.value)
                    if diff < mindiff:
                        bestspwid = scanspw.id
                        mindiff = diff

            # No spw match found
            if bestspwid is None:
                continue

            # Create dictionary entry based on first scan matched.
            ftsysdict['tsys_field_name'] = fieldname
            ftsysdict['intent'] = intent

            # Pick the first obs scan following the Tsys scan
            #    This should deal with the shared phase / science target
            #    scans
            for obscan in obscans:
                if obscan.id > atmscan.id:
                    ftsysdict['snr_scan'] = obscan.id
                    break

            # PIPE-1154: if no scan for field name list and intent occurred
            # after the Tsys scan, then fall back to picking the most recent
            # scan that occurred before the Tsys scan (assuming obscans are
            # sorted chronologically).
            if 'snr_scan' not in ftsysdict:
                for obscan in obscans:
                    if obscan.id < atmscan.id:
                        ftsysdict['snr_scan'] = obscan.id

            ftsysdict['tsys_scan'] = atmscan.id
            ftsysdict['tsys_spw'] = bestspwid
            break

        # Update the spw dictionary
        if ftsysdict:
            LOG.info('    Matched spw %d to a Tsys spw %d' % (spwid, bestspwid))
            tsysdict[spwid] = ftsysdict
        else:
            LOG.warning('    Cannot match spw %d to a Tsys spw in MS %s' % (spwid, ms.basename))

    return tsysdict


def make_tsyslists(spwlist, tsysdict):
    """Utility routine for constructing the tsys spw list and the observing
    scan list from the tysdict produced by get_tsysinfo.

    Input Parameters
         spwlist: The science spw list, e.g. [13, 15]
        tsysdict: The Tsys dictionary created by get_tsysinfo

    Returned values
        tsys_spwlist: The Tsys spw id list corresponding to spwlist
            scanlist: The list of snr scans for each Tsys window
    """

    tsys_spwlist = []
    scan_list = []
    for spw in spwlist:
        if spw not in tsysdict:
            continue
        if 'tsys_spw' not in tsysdict[spw]:
            continue
        tsys_spwlist.append(tsysdict[spw]['tsys_spw'])
        scan_list.append(tsysdict[spw]['snr_scan'])

    return tsys_spwlist, scan_list


def get_mediantemp(ms, tsys_spwlist, scan_list, antenna='', temptype='tsys'):
    """Get median Tsys, Trx, or Tsky temperatures as a function of spw and return
    a dictionary

    Input parameters
              ms: The pipeline measurement set object
    tsys_spwlist: The list of Tsys spw ids, e.g. [9,11,13,15]
       scan_list: The list of associated observation scan numbers, e.g. [4,8]
         antenna: The antenna selectionm '' for all antennas, or a single antenna id or name
        temptype: The temperature type 'tsys' (default), 'trx' or 'tsky'

    The output dictionary

    The median temperature dictionary keys and values
        key: the spw id         value: The median Tsys temperature in degrees K
    """
    # PIPE-775: Output the call to the function. The second and third arguments (tsys_spwlist and scan_list)
    #  should have the same length
    LOG.trace("Called get_mediantemp({}, {}, {}, antenna='{}', temptype='{}')".format(
        ms, tsys_spwlist, scan_list, antenna, temptype))

    # Initialize
    medtempsdict = collections.OrderedDict()
    LOG.info('Estimating Tsys temperatures')

    # Temperature type must be one of 'tsys' or 'trx' or 'tsky'
    if temptype not in ['tsys', 'trx', 'tsky']:
        return medtempsdict

    # Get list of unique scan ids.
    unique_scans = sorted(set(scan_list))

    # Get the associated spws for each scan id
    scans_spws = {scan: [spw.id for spw in ms.get_scans(scan_id=scan)[0].spws] for scan in unique_scans}
    LOG.debug("Scan spws: {}".format(scans_spws))

    # Determine the start and end times for each unique scan
    begin_scan_times = []
    end_scan_times = []
    for scan in unique_scans:
        reqscan = ms.get_scans(scan_id=scan)
        if not reqscan:
            LOG.warning('Cannot find observation scan %d in MS %s' % (scan, ms.basename))
            return medtempsdict
        start_time = reqscan[0].start_time
        end_time = reqscan[0].end_time
        begin_scan_times.append(start_time)
        end_scan_times.append(end_time)
        LOG.debug('scan %d start %s end %s' % (scan, start_time, end_time))

    # Get the syscal table meta data.
    with casa_tools.TableReader(os.path.join(ms.name, 'SYSCAL')) as table:

        # Get the antenna ids
        tsys_antennas = table.getcol('ANTENNA_ID')
        if len(tsys_antennas) < 1:
            LOG.warning('The SYSCAL table is blank in MS %s' % ms.basename)
            return medtempsdict

        # Get columns and tools needed to understand the tsys times
        time_colkeywords = table.getcolkeywords('TIME')
        time_unit = time_colkeywords['QuantumUnits'][0]
        time_ref = time_colkeywords['MEASINFO']['Ref']
        mt = casa_tools.measures
        qt = casa_tools.quanta

        # Get time and intervals
        tsys_times = table.getcol('TIME')
        tsys_intervals = table.getcol('INTERVAL')

        # Compute the time range of validity for each tsys measurement
        #    Worry about memory efficiency later
        # PIPE-775: This considers the time to be the central time
        tsys_start_times = tsys_times - 0.5 * tsys_intervals
        tsys_end_times = tsys_start_times + tsys_intervals

        # Get the spw ids
        tsys_spws = table.getcol('SPECTRAL_WINDOW_ID')
        tsys_uniqueSpws = np.unique(tsys_spws)

        # Create a scan id array and populate it with zeros
        scanids = np.zeros(len(tsys_start_times), dtype=np.int32)

        # Determine if a tsys measurement matches the scan interval
        #    If it does  set the scan to the scan id
        nmatch = 0
        for i in range(len(tsys_start_times)):

            # Time conversions
            #    Not necessary if scan begin and end times are not converted
            tstart = mt.epoch(time_ref, qt.quantity(tsys_start_times[i], time_unit))
            tend = mt.epoch(time_ref, qt.quantity(tsys_end_times[i], time_unit))
            LOG.debug('row %d start %s end %s' % (i, tstart, tend))

            # Scan starts after end of validity interval or ends before
            # the beginning of the validity interval.
            for j, scan in enumerate(unique_scans):
                if (begin_scan_times[j]['m0']['value'] > tend['m0']['value'] or
                    end_scan_times[j]['m0']['value'] < tstart['m0']['value']):
                    continue
                if scanids[i] <= 0:
                    scanids[i] = unique_scans[j]
                    nmatch = nmatch + 1

        if nmatch <= 0:
            LOG.warning('No SYSCAL table row matches for scans %s tsys spws %s in MS %s' %
                        (unique_scans, tsys_spwlist, ms.basename))
            return medtempsdict
        else:
            LOG.info('    SYSCAL table row matches for scans %s Tsys spws %s %d / %d' %
                     (unique_scans, tsys_spwlist, nmatch, len(tsys_start_times)))

    # Get a list of unique antenna ids.
    if antenna == '':
        unique_antenna_ids = [a.id for a in ms.get_antenna()]
    else:
        unique_antenna_ids = [ms.get_antenna(search_term=antenna)[0].id]

    # Loop over the spw and scan list which have the same length
    for spw, scan in zip(tsys_spwlist, scan_list):

        # If no Tsys data skip to the next window
        if spw not in tsys_uniqueSpws:
            LOG.warning('Tsys spw %d is not in the SYSCAL table for MS %s' %
                        (spw, ms.basename))
            continue
            # return medtempsdict

        # Loop over the rows
        medians = []
        with casa_tools.TableReader(os.path.join(ms.name, 'SYSCAL')) as table:
            for i in range(len(tsys_antennas)):
                if tsys_spws[i] != spw:
                    continue
                if tsys_antennas[i] not in unique_antenna_ids:
                    continue
                if scan != scanids[i]:
                    continue
                if temptype == 'tsys':
                    tsys = table.getcell('TSYS_SPECTRUM', i)
                elif temptype == 'trx':
                    tsys = table.getcell('TRX_SPECTRUM', i)
                elif temptype == 'tsky':
                    tsys = table.getcell('TSKY_SPECTRUM', i)
                medians.append(np.median(tsys))

        if len(medians) > 0:
            medtempsdict[spw] = np.median(medians)
            LOG.info("    Median Tsys %s value for Tsys spw %2d = %.1f K" % (temptype, spw, medtempsdict[spw]))
        else:
            LOG.warning('    No Tsys data for spw %d scan %d in MS %s' % (spw, scan, ms.basename))

    # Return median temperature per spw and scan.
    return medtempsdict


def _get_unflagged_antennas(vis, scanidlist, ants12m, ants7m, max_fracflagged=0.90):
    """Internal method for determining the number of unflagged 12m and 7m
    antennas.

    Loop over the scans in scanlist. Compute the list of unflagged
    and flagged 12m and 7m antennas for each scan. In most cases
    there will be only one scan. Return the number of unflagged
    12m and 7m antennas

    Input Parameters
                vis: The name of the MS
         scanidlist: The input scan id list, e.g. [3,4,5]
            ants12m: The list of 12m antennas
             ants7m: The list of 7m antennas
    max_fracflagged:

    Return values
        nunflagged_12mantennas: number of unflagged 12m antennas
        nunflagged_7mantennas: number of unflagged 7m antennas
    """

    # Execute the CASA flagdata task for the specified bandpass scans
    #     Format the id list for CASA
    #     Execute task
    scanidstr = ','.join([str(scanid) for scanid in scanidlist])
    flagdata_task = casa_tasks.flagdata(vis=vis, scan=scanidstr, mode='summary')
    flagdata_result = flagdata_task.execute()

    # Initialize the statistics per scan
    unflagged_12mantennas = []
    flagged_12mantennas = []
    unflagged_7mantennas = []
    flagged_7mantennas = []

    # Add up the antennas
    for antenna in sorted(flagdata_result['antenna']):
        points = flagdata_result['antenna'][antenna]
        fraction = points['flagged']/points['total']
        if antenna in ants12m:
            if fraction < max_fracflagged:
                unflagged_12mantennas.append(antenna)
            else:
                flagged_12mantennas.append(antenna)
        elif antenna in ants7m:
            if fraction < max_fracflagged:
                unflagged_7mantennas.append(antenna)
            else:
                flagged_7mantennas.append(antenna)

    # Compute the number of unflagged antennas per scan
    nunflagged_12mantennas = len(unflagged_12mantennas)
    nunflagged_7mantennas = len(unflagged_7mantennas)

    # nflagged_12mantennas = len(flagged_12mantennas)
    # nflagged_7mantennas = len(flagged_7mantennas)

    # Return the number of unflagged antennas
    return nunflagged_12mantennas, nunflagged_7mantennas


def get_obsinfo(ms, fieldnamelist, intent, spwidlist, compute_nantennas='all', max_fracflagged=0.90):
    """Get the observing information as a function of spw id  and return a dictionary.

    Input parameters
                   ms: The pipeline context ms object
        fieldnamelist: The list of field names to be selected
               intent: The intent of the fields to be selected
            spwidlist: The list of spw ids to be selected
    compute_nantennas: The algorithm for computing the number of unflagged antennas ('all', 'flagged')
                       (was 'hm_nantennas')
      max_fracflagged: The maximum fraction of an antenna can be flagged

    The output observing dictionary obsdict

    The observing dictionary key and value
        key: the spw id         value: The observing scans dictionary

    The observing scans dictionary keys and values
        key: 'snr_scans'        value: The list of snr source scans, e.g. [4,8]
        key: 'num_12mantenna'   value: The max number of 12m antennas, e.g. 32
        key: 'num_7mantenna'    value: The max number of 7m antennas, e.g. 7
        key: 'exptime'          value: The exposure time in minutes, e.g. 6.32
        key: 'integrationtime'  value: The mean integration time in minutes, e.g. 0.016
        key: 'band'             value: The ALMA receiver band, e.g. 'ALMA Band 3'
        key: 'bandcenter'       value: The receiver band center frequency in Hz, e.g. 9.6e9
        key: 'bandwidth'        value: The band width in Hz, e.g. 2.0e9
        key: 'nchan'            value: The number of channels, e.g. 28
        key: 'chanwidths'       value: The median channel width in Hz, e.g. 7.3e7
    """

    obsdict = collections.OrderedDict()
    LOG.info('Observation summary')
    fieldset = set(fieldnamelist)

    # Get the scans associated with the field name list and intent
    obscans = []
    for scan in ms.get_scans(scan_intent=intent):
        # Remove scans not associated with the input field names
        scanfieldset = {field.name for field in scan.fields}
        if len(fieldset.intersection(scanfieldset)) == 0:
            continue
        obscans.append(scan)

    # No data scans found
    if not obscans:
        return obsdict

    # Loop over the spws
    prev_spwid = None
    prev_scanids = []
    for spwid in spwidlist:

        # Get spectral window
        try:
            spw = ms.get_spectral_window(spwid)
        except:
            continue

        # Find scans associated with the spw. They may be different from
        # one spw to the next
        spwscans = []
        for obscan in obscans:
            scanspwset = {scanspw.id for scanspw in list(obscan.spws)
                          if scanspw.num_channels not in ms.exclude_num_chans}
            if len({spwid}.intersection(scanspwset)) == 0:
                continue
            spwscans.append(obscan)
        if not spwscans:
            continue

        # Limit the scans per spw to those for the first field
        #    in the scan sequence.
        fieldnames = [field.name for field in spwscans[0].fields]
        fieldname = fieldnames[0]
        fscans = []
        for scan in spwscans:
            fnames = [field.name for field in scan.fields]
            if fieldname != fnames[0]:
                continue
            fscans.append(scan)
        if not fscans:
            continue

        obsdict[spwid] = collections.OrderedDict()
        scanids = [scan.id for scan in fscans]
        obsdict[spwid]['snr_scans'] = scanids

        # Figure out the number of 7m and 12 m antennas
        #   Note comparison of floating point numbers is tricky ...
        #
        if compute_nantennas == 'all':
            # Use numbers from the scan with the minimum number of
            # antennas
            n7mant = np.iinfo('i').max
            n12mant = np.iinfo('i').max
            for scan in fscans:
                n7mant = min(n7mant, len([a for a in scan.antennas
                                          if a.diameter == 7.0]))
                n12mant = min(n12mant, len([a for a in scan.antennas
                                            if a.diameter == 12.0]))
        elif len(set(scanids).difference(set(prev_scanids))) > 0:
            # Get the lists of unique 7m and 12m antennas
            ant7m = []
            ant12m = []
            for scan in fscans:
                ant7m.extend([a.name for a in scan.antennas if a.diameter == 7.0])
                ant12m.extend([a.name for a in scan.antennas if a.diameter == 12.0])
            ant12m = list(set(ant12m))
            ant7m = list(set(ant7m))
            # Get the number of unflagged antennas
            n12mant, n7mant = _get_unflagged_antennas(ms.name, scanids, ant12m, ant7m, max_fracflagged=max_fracflagged)
        else:
            # Use values from previous spw
            n7mant = obsdict[prev_spwid]['num_7mantenna']
            n12mant = obsdict[prev_spwid]['num_12mantenna']

        obsdict[spwid]['num_12mantenna'] = n12mant
        obsdict[spwid]['num_7mantenna'] = n7mant

        # Retrieve total exposure time and mean integration time in minutes
        #    Add to dictionary
        exposureTime = 0.0
        meanInterval = 0.0
        for scan in fscans:
            # scanTime = float (scan.time_on_source.total_seconds()) / 60.0
            scanTime = scan.exposure_time(spw.id).total_seconds() / 60.0
            exposureTime = exposureTime + scanTime
            # intTime = scan.mean_interval(spw.id).total_seconds() / 60.0
            intTime = scan.mean_interval(spw.id)
            intTime = (intTime.seconds + intTime.microseconds * 1.0e-6) / 60.0
            meanInterval = meanInterval + intTime
        obsdict[spw.id]['exptime'] = exposureTime
        obsdict[spw.id]['integrationtime'] = meanInterval / len(fscans)

        # Retrieve spw characteristics
        #    Receiver band, center frequency, bandwidth, number of
        #    channels, and median channel width
        #    Add to dictionary
        obsdict[spwid]['band'] = spw.band
        obsdict[spwid]['bandcenter'] = float(spw.centre_frequency.value)
        obsdict[spwid]['bandwidth'] = float(spw.bandwidth.value)
        obsdict[spwid]['nchan'] = spw.num_channels
        channels = spw.channels
        chanwidths = np.zeros(spw.num_channels)
        for i in range(spw.num_channels):
            chanwidths[i] = (channels[i].high - channels[i].low).value
        obsdict[spwid]['chanwidths'] = np.median(chanwidths)

        LOG.info('For field %s spw %2d scans %s' % (fieldname, spwid, scanids))
        LOG.info('    %2d 12m antennas  %2d 7m antennas  exposure %0.3f minutes  interval %0.3f minutes' %
                 (obsdict[spwid]['num_12mantenna'], obsdict[spwid]['num_7mantenna'], exposureTime,
                  meanInterval / len(fscans)))

        prev_spwid = spwid
        prev_scanids = scanids

    return obsdict


def join_dicts(spwlist, tsys_dict, flux_dict, tsystemp_dict, obs_dict):
    """Combine all the input dictionaries and output the spw dictionary.
    This dictionary contains all the information needed to compute the SNR
    estimates.

    The input parameters

    The output dictionary spw_dict

    The spw dictionary spw_dict  key and value
        key: The spw id       value: The spw source dictionary

    The spw source dictionary keys and values
        key: 'tsys_field_name'  value: The name of the Tsys field source, e.g. '3C286'
        key: 'intent'           value: The intent of the field source, e.g. 'BANDPASS'
        key: 'snr_scan'         value: The scan associated with Tsys used to compute the SNR, e.g. 4
        key: 'tsys_scan'        value: The Tsys scan to be used for Tsys computation, e.g. 3
        key: 'tsys_spw'         value: The Tsys spw associated with the science spw id, e.g. 13

        key: 'field_name'       value: The name of the field source, e.g. '3C286'
        key: 'flux'             value: The flux of the field source in Jy, e.g. 5.305

        key: 'median_tsys'      value: The median Tsys value in degrees K, e.g. 45.5

        key: 'snr_scans'        value: The list of snr source scans, e.g. [4,8]
        key: 'num_12mantenna'   value: The max number of 12m antennas, e.g. 32
        key: 'num_7mantenna'    value: The max number of 7m antennas, e.g. 7
        key: 'exptime'          value: The exposure time in minutes, e.g. 6.32
        key: 'integrationtime'  value: The mean integration time in minutes, e.g. 0.016
        key: 'band'             value: The ALMA receiver band, e.g. 'ALMA Band 3'
        key: 'bandcenter'       value: The receiver band center frequency in Hz, e.g. 9.6e9
        key: 'bandwidth'        value: The band width in Hz, e.g. 2.0e9
        key: 'nchan'            value: The number of channels, e.g. 28
        key: 'chanwidths'       value: The median channel width in Hz, e.g. 7.3e7
    """

    # Initialize the spw dictionary from the Tsys dictionary
    #    Make a deep copy of this dictionary
    spw_dict = deepcopy(tsys_dict)

    # Transfer flux information to the spw dictionary.
    _transfer_fluxes(spwlist, spw_dict, flux_dict)

    # Transfer the tsys temperature information to the spw dictionary
    _transfer_temps(spwlist, spw_dict, tsystemp_dict)

    # Transfer the observing information to the spw dictionary
    _transfer_obsinfo(spwlist, spw_dict, obs_dict)

    return spw_dict


def _transfer_fluxes(spwlist, spw_dict, flux_dict):
    """
    Transfer flux information from the flux dictionary to the spw dictionary.
    """
    for spw in spwlist:
        if spw not in flux_dict:
            continue
        if spw not in spw_dict:
            continue
        # if spw_dict[spw]['tsys_field_name'] != flux_dict[spw]['field_name']:
        #     continue
        spw_dict[spw]['field_name'] = flux_dict[spw]['field_name']
        spw_dict[spw]['flux'] = flux_dict[spw]['flux']


def _transfer_temps(spwlist, spw_dict, tsystemp_dict):
    """
    Transfer the tsys temp information to the spw dictionary.
    """
    for spw in spwlist:
        if spw not in spw_dict:
            continue
        if spw_dict[spw]['tsys_spw'] not in tsystemp_dict:
            continue
        spw_dict[spw]['median_tsys'] = \
            tsystemp_dict[spw_dict[spw]['tsys_spw']]


def _transfer_obsinfo(spwlist, spw_dict, obs_dict):
    """
    Transfer the observing information to the spw dictionary.
    """
    for spw in spwlist:
        if spw not in spw_dict:
            continue
        if spw not in obs_dict:
            continue
        spw_dict[spw]['snr_scans'] = obs_dict[spw]['snr_scans']
        spw_dict[spw]['exptime'] = obs_dict[spw]['exptime']
        spw_dict[spw]['integrationtime'] = obs_dict[spw]['integrationtime']
        spw_dict[spw]['num_7mantenna'] = obs_dict[spw]['num_7mantenna']
        spw_dict[spw]['num_12mantenna'] = obs_dict[spw]['num_12mantenna']
        spw_dict[spw]['band'] = obs_dict[spw]['band']
        spw_dict[spw]['bandcenter'] = obs_dict[spw]['bandcenter']
        spw_dict[spw]['bandwidth'] = obs_dict[spw]['bandwidth']
        spw_dict[spw]['nchan'] = obs_dict[spw]['nchan']
        spw_dict[spw]['chanwidths'] = obs_dict[spw]['chanwidths']


def compute_gaincalsnr(ms, spwlist, spw_dict, edge_fraction):
    """Compute the gain to signal-to-noise given the spw list and the spw
    dictionary.

    This code assumes that the science spws are observed in both the
    calibrator and the science target.

    The input parameters
        spwlist                     The list of spw ids
        spw_dict                    The spw dictionary
        edge_fraction               Fraction of the edge that is flagged

    The output SNR dictionary.

    The SNR dictionary keys and values
        key: the spw id     value: The science spw id as an integer

    The preaveraging parameter dictionary keys and values
        key: 'band'                       value: The ALMA receiver band
        key: 'frequency_Hz'               value: The frequency of the spw
        key: 'nchan_total'                value: The total number of channels
        key: 'chanwidth_Hz'               value: The median channel width in Hz

        key: 'tsys_spw'                   value: The tsys spw id as an integer
        key: 'median_tsys'                value: The median tsys value

        key: 'flux_Jy'                    value: The flux of the source in Jy
        key: 'scantime_minutes'           value: The exposure time in minutes
        key: 'inttime_minutes'            value: The exposure time in minutes
        key: 'sensitivity_per_scan_mJy    value: The sensitivity per scan in mJy
        key: 'snr_per_scan                value: The snr per scan
    """

    # Initialize the output solution interval dictionary
    snr_dict = collections.OrderedDict()

    maxEffectiveBW = 2.0e9 * (1.0 - 2.0 * edge_fraction)

    # PIPE-788: retrieve the fraction of flagged data
    scans = set(np.hstack([spw_dict[spwid]['snr_scans'] for spwid in spwlist]))
    flag_task = casa_tasks.flagdata(vis=ms.name, scan=','.join(map(str, list(scans))), mode='summary')
    flag_result = flag_task.execute()

    LOG.info('Signal to noise summary')
    for spwid in spwlist:

        # Determine the receiver band
        bandidx = ALMA_BANDS.index(spw_dict[spwid]['band'])

        # Compute the various generic SNR factors
        if spw_dict[spwid]['median_tsys'] <= 0.0:
            relativeTsys = 1.0
            LOG.warning('Spw %d <= 0K in MS %s assuming nominal Tsys' % (spwid, ms.basename))
        else:
            relativeTsys = spw_dict[spwid]['median_tsys'] / ALMA_TSYS[bandidx]
        nbaselines = spw_dict[spwid]['num_7mantenna'] + spw_dict[spwid]['num_12mantenna'] - 1
        arraySizeFactor = np.sqrt(ALMA_FIDUCIAL_NUM_ANTENNAS * (ALMA_FIDUCIAL_NUM_ANTENNAS-1) / 2.0 / nbaselines)
        if spw_dict[spwid]['num_7mantenna'] == 0:
            areaFactor = 1.0
        elif spw_dict[spwid]['num_12mantenna'] == 0:
            areaFactor = (12.0 / 7.0) ** 2
        else:
            # general case:  eq. 6 in arXiv:2306.07420
            areaFactor = (spw_dict[spwid]['num_12mantenna'] + spw_dict[spwid]['num_7mantenna'] * (12./7)**2) / \
                         (spw_dict[spwid]['num_12mantenna'] + spw_dict[spwid]['num_7mantenna'])
        polarizationFactor = np.sqrt(2.0)

        # SNR computation
        timeFactor = ALMA_FIDUCIAL_EXP_TIME / np.sqrt(spw_dict[spwid]['exptime'] / len(spw_dict[spwid]['snr_scans']))
        bandwidthFactor = np.sqrt(ALMA_FIDUCIAL_BANDWIDTH / min(spw_dict[spwid]['bandwidth'], maxEffectiveBW))
        # PIPE-788: multiply the exposure time by the fraction of unflagged data
        flagFactor = 1.0 / np.sqrt(1 - flag_result['spw'][str(spwid)]['flagged'] / flag_result['spw'][str(spwid)]['total'])
        factor = relativeTsys * timeFactor * arraySizeFactor * \
            areaFactor * bandwidthFactor * polarizationFactor * flagFactor
        sensitivity = ALMA_SENSITIVITIES[bandidx] * factor
        if 'flux' in spw_dict[spwid]:
            snrPerScan = spw_dict[spwid]['flux'] * 1000.0 / sensitivity  # factor of 1000 converting from Jy to mJy
        else:
            snrPerScan = None

        # Fill in the dictionary
        snr_dict[spwid] = collections.OrderedDict()

        # Science spw info
        #    Channel information probably not required
        snr_dict[spwid]['band'] = spw_dict[spwid]['band']
        snr_dict[spwid]['frequency_Hz'] = spw_dict[spwid]['bandcenter']
        snr_dict[spwid]['bandwidth'] = spw_dict[spwid]['bandwidth']
        snr_dict[spwid]['nchan_total'] = spw_dict[spwid]['nchan']
        snr_dict[spwid]['chanwidth_Hz'] = spw_dict[spwid]['chanwidths']

        # Tsys spw info
        snr_dict[spwid]['tsys_spw'] = spw_dict[spwid]['tsys_spw']
        snr_dict[spwid]['median_tsys'] = spw_dict[spwid]['median_tsys']

        # Sensitivity info
        if 'flux' in spw_dict[spwid]:
            snr_dict[spwid]['flux_Jy'] = spw_dict[spwid]['flux']
        else:
            snr_dict[spwid]['flux_Jy'] = None
        snr_dict[spwid]['inttime_minutes'] = spw_dict[spwid]['integrationtime']
        snr_dict[spwid]['scantime_minutes'] = spw_dict[spwid]['exptime'] / len(spw_dict[spwid]['snr_scans'])
        snr_dict[spwid]['sensitivity_per_scan_mJy'] = sensitivity
        if not snrPerScan:
            snr_dict[spwid]['snr_per_scan'] = None
            LOG.info("Spw %3d  scan (minutes) %6.3f  integration (minutes) %6.3f  sensitivity (mJy) %7.3f  SNR unknown" %
                     (spwid,
                      snr_dict[spwid]['scantime_minutes'],
                      snr_dict[spwid]['inttime_minutes'],
                      snr_dict[spwid]['sensitivity_per_scan_mJy']))
        else:
            snr_dict[spwid]['snr_per_scan'] = snrPerScan
            LOG.info("Spw %3d  scan (minutes) %6.3f  integration (minutes) %6.3f  sensitivity (mJy) %7.3f  SNR %10.3f" %
                     (spwid,
                      snr_dict[spwid]['scantime_minutes'],
                      snr_dict[spwid]['inttime_minutes'],
                      snr_dict[spwid]['sensitivity_per_scan_mJy'],
                      snr_dict[spwid]['snr_per_scan']))

    return snr_dict


def compute_bpsolint(ms, spwlist, spw_dict, reqPhaseupSnr, minBpNintervals, reqBpSnr, minBpNchan, evenbpsolints=False):
    """Compute the optimal bandpass frequency solution intervals given the spw list
    and the spw dictionary.

    The input parameters
        spwlist             The list of spw ids
        spw_dict            The spw dictionary
        reqPhaseupSnr       The requested phaseup SNR
        minBpNintervals     The minimum number of phase up time intervals, e.g. 2
        reqBpSnr            The requested bandpass SNR
        minBpNchan          The minimum number of bandpass channel solutions, e.g. 8

    The output solution interval dictionary.

    The bandpass preaveraging dictionary keys and values
        key: the spw id     value: The science spw id as an integer

    The preaveraging parameter dictionary keys and values
        key: 'band'               value: The ALMA receiver band
        key: 'frequency_Hz'       value: The frequency of the spw
        key: 'nchan_total'        value: The total number of channels
        key: 'chanwidth_Hz'       value: The median channel width in Hz

        key: 'tsys_spw'           value: The tsys spw id as an integer
        key: 'median_tsys'        value: The median tsys value

        key: 'flux_Jy'            value: The flux of the source in Jy
        key: 'exptime_minutes'    value: The exposure time in minutes
        key: 'snr_per_channel'    value: The signal to noise per channel
        key: 'sensitivity_per_channel_mJy'    value: The sensitivity in mJy per channel

        key: 'bpsolint'           value: The frequency solint in MHz
        key: 'nchan_bpsolint'     value: The total number of solint channels
    """

    if evenbpsolints:
        LOG.info("Forcing bandpass frequency solint to divide evenly into bandpass")

    # Initialize the output solution interval dictionary
    solint_dict = collections.OrderedDict()

    for spwid in spwlist:

        # Determine the receiver band
        bandidx = ALMA_BANDS.index(spw_dict[spwid]['band'])

        # Compute the various SNR factors
        #    The following are shared between the phaseup time solint and
        #    the bandpass frequency solint
        if spw_dict[spwid]['median_tsys'] <= 0.0:
            relativeTsys = 1.0
            LOG.warning('Spw %d <= 0K in MS %s assuming nominal Tsys' % (spwid, ms.basename))
        else:
            relativeTsys = spw_dict[spwid]['median_tsys'] / ALMA_TSYS[bandidx]
        nbaselines = spw_dict[spwid]['num_7mantenna'] + spw_dict[spwid]['num_12mantenna'] - 1

        # PIPE-408: do not continue if there are no unflagged baselines for current spw; this will cause this spw
        # to be absent from the solution interval dictionary that is returned.
        if nbaselines < 0:
            LOG.warning("Cannot compute optimal bandpass frequency solution interval for spw {} in MS {}; no (unflagged)"
                        " baselines were found".format(spwid, ms.basename))
            continue

        arraySizeFactor = np.sqrt(16 * 15 / 2.0) / np.sqrt(nbaselines)
        if spw_dict[spwid]['num_7mantenna'] == 0:
            areaFactor = 1.0
        elif spw_dict[spwid]['num_12mantenna'] == 0:
            areaFactor = (12.0 / 7.0) ** 2
        else:
            # Not sure this is correct
            ntotant = spw_dict[spwid]['num_7mantenna'] + spw_dict[spwid]['num_12mantenna']
            areaFactor = (spw_dict[spwid]['num_12mantenna'] + (12.0 / 7.0)**2 * spw_dict[spwid]['num_7mantenna']) / \
                ntotant
        polarizationFactor = np.sqrt(2.0)

        # Phaseup bandpasstime solint
        putimeFactor = 1.0 / np.sqrt(spw_dict[spwid]['integrationtime'])
        pubandwidthFactor = np.sqrt(8.0e9 / spw_dict[spwid]['bandwidth'])
        pufactor = relativeTsys * putimeFactor * arraySizeFactor * \
            areaFactor * pubandwidthFactor * polarizationFactor
        pusensitivity = ALMA_SENSITIVITIES[bandidx] * pufactor
        snrPerIntegration = spw_dict[spwid]['flux'] * 1000.0 / pusensitivity
        requiredIntegrations = (reqPhaseupSnr / snrPerIntegration) ** 2

        # Bandpass frequency solint
        bptimeFactor = 1.0 / np.sqrt(spw_dict[spwid]['exptime'])
        bpbandwidthFactor = np.sqrt(8.0e9 / spw_dict[spwid]['chanwidths'])
        bpfactor = relativeTsys * bptimeFactor * arraySizeFactor * \
            areaFactor * bpbandwidthFactor * polarizationFactor
        bpsensitivity = ALMA_SENSITIVITIES[bandidx] * bpfactor
        snrPerChannel = spw_dict[spwid]['flux'] * 1000.0 / bpsensitivity
        requiredChannels = (reqBpSnr / snrPerChannel) ** 2
        LOG.info("spw={}, band={}, alma_sensitivity={}, bpfactor={}".format(
            spwid, bandidx+1, ALMA_SENSITIVITIES[bandidx], bpfactor))
        LOG.info("requiredChannels={}, repBpSnr={}, snrPerChannel={}, spw flux={}, bpsensitivity={}".format(
            requiredChannels, reqBpSnr, snrPerChannel, spw_dict[spwid]['flux'], bpsensitivity))
        evenChannels = nextHighestDivisibleInt(spw_dict[spwid]['nchan'], int(np.ceil(requiredChannels)))

        # Fill in the dictionary
        solint_dict[spwid] = collections.OrderedDict()

        # Science spw info
        solint_dict[spwid]['band'] = spw_dict[spwid]['band']
        solint_dict[spwid]['frequency_Hz'] = spw_dict[spwid]['bandcenter']
        solint_dict[spwid]['bandwidth'] = spw_dict[spwid]['bandwidth']
        solint_dict[spwid]['nchan_total'] = spw_dict[spwid]['nchan']
        solint_dict[spwid]['chanwidth_Hz'] = spw_dict[spwid]['chanwidths']

        # Tsys spw info
        solint_dict[spwid]['tsys_spw'] = spw_dict[spwid]['tsys_spw']
        solint_dict[spwid]['median_tsys'] = spw_dict[spwid]['median_tsys']

        # Sensitivity info
        solint_dict[spwid]['flux_Jy'] = spw_dict[spwid]['flux']
        solint_dict[spwid]['integration_minutes'] = spw_dict[spwid]['integrationtime']
        solint_dict[spwid]['sensitivity_per_integration_mJy'] = pusensitivity
        solint_dict[spwid]['snr_per_integration'] = snrPerIntegration
        solint_dict[spwid]['exptime_minutes'] = spw_dict[spwid]['exptime']
        solint_dict[spwid]['snr_per_channel'] = snrPerChannel
        solint_dict[spwid]['sensitivity_per_channel_mJy'] = bpsensitivity

        # Phaseup bandpass solution info
        if requiredIntegrations <= 1.0:
            solint_dict[spwid]['phaseup_solint'] = 'int'
            solint_dict[spwid]['nint_phaseup_solint'] = 1
        else:
            solint_dict[spwid]['phaseup_solint'] = '%fs' % (solint_dict[spwid]['integration_minutes'] *
                                                            requiredIntegrations * 60.0)
            solint_dict[spwid]['nint_phaseup_solint'] = int(np.ceil(requiredIntegrations))
        solInts = int(np.ceil(solint_dict[spwid]['exptime_minutes'] / solint_dict[spwid]['integration_minutes'])) // int(np.ceil(requiredIntegrations))
        if solInts < minBpNintervals:
            tooFewIntervals = True
            asterisks = '***'
        else:
            tooFewIntervals = False
            asterisks = ''
        LOG.info("%sspw %2d (%6.3fmin) requires phaseup solint='%0.3gsec' (%d time intervals in solution) to reach S/N=%.0f" %
                 (asterisks,
                  spwid,
                  solint_dict[spwid]['exptime_minutes'],
                  60.0 * requiredIntegrations * solint_dict[spwid]['integration_minutes'],
                  solInts,
                  reqPhaseupSnr))
        solint_dict[spwid]['nphaseup_solutions'] = solInts
        if tooFewIntervals:
            LOG.warning('%s Spw %d would have less than %d time intervals in its solution in MS %s' %
                        (asterisks, spwid, minBpNintervals, ms.basename))

        # Bandpass solution
        #    Determine frequency interval in MHz
        #
        # Get number of channels.
        if requiredChannels > 1.0:
            if evenbpsolints:
                nchan = evenChannels
            else:
                nchan = requiredChannels
        else:
            nchan = 1

        # PIPE-2036: work-around for potential issue caused by:
        #   * PL converts nr. of channels to frequency interval
        #   * the frequency interval is passed with limited precision (typically
        #     in MHz with 6 decimals, i.e. a precision of Hz)
        #   * CASA's bandpass converts the frequency interval back to nr. of
        #     channels and then take the floor
        #
        # This could have resulted in e.g. a required nr. of channels of 5
        # corresponding to 4.8828125 MHz but getting passed as 4.882812 MHz,
        # then converted back to 4.999999 channels, and floored to 4.
        #
        # As a work-around, check whether the converted frequency interval would
        # trigger this, and if so, then round *up* the frequency interval to
        # nearest Hz.
        solint = nchan * solint_dict[spwid]['chanwidth_Hz']
        if round(solint) / solint_dict[spwid]['chanwidth_Hz'] < math.floor(nchan):
            solint_dict[spwid]['bpsolint'] = f"{round_up(solint) * 1.e-6:f}MHz"
        else:
            solint_dict[spwid]['bpsolint'] = f"{solint * 1.e-6:f}MHz"

        # Determine the number of channels in the bandpass
        # solution and the number of solutions
        if evenbpsolints:
            solint_dict[spwid]['nchan_bpsolint'] = int(np.ceil(evenChannels))
            solChannels = solint_dict[spwid]['nchan_total'] // int(np.ceil(evenChannels))
        else:
            solint_dict[spwid]['nchan_bpsolint'] = int(np.ceil(requiredChannels))
            solChannels = solint_dict[spwid]['nchan_total'] // int(np.ceil(requiredChannels))

        if solChannels < minBpNchan:
            tooFewChannels = True
            asterisks = '***'
        else:
            tooFewChannels = False
            asterisks = ''
        #LOG.info("%sspw %2d (%4.0fMHz) requires solint='%0.3gMHz' (%d channels intervals in solution) to reach S/N=%.0f" % \
        LOG.info("%sspw %2d (%4.0fMHz) requires solint='%s' (%d channels intervals in solution) to reach S/N=%.0f" %
                 (asterisks,
                  spwid,
                  solint_dict[spwid]['bandwidth']*1.0e-6,
                  solint_dict[spwid]['bpsolint'],
                  solChannels,
                  reqBpSnr))
        solint_dict[spwid]['nbandpass_solutions'] = solChannels
        if tooFewChannels:
            LOG.warning('%s Spw %d would have less than %d channels in its solution in MS %s' %
                        (asterisks, spwid, minBpNchan, ms.basename))

    return solint_dict


def nextHighestDivisibleInt(n, d):
    """
    Checks whether an integer is evenly divisible by a second
    integer, and if not, finds the next higher integer that is.

    n: larger integer
    d: smaller integer
    """

    dd = d
    while n % dd != 0 and dd < n:
        dd += 1

    return dd
