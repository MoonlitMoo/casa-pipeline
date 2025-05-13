from typing import Dict, List, Optional, Tuple

import copy
import numpy as np

import pipeline.infrastructure as infrastructure
from pipeline.infrastructure import casa_tools
from pipeline.hifa.heuristics import atm as atm_heuristic
from pipeline.hifa.heuristics import phasespwmap

LOG = infrastructure.get_logger(__name__)

#################
# Adapted from PIPE692.py::SSFanalyis. Original history:
# L T Maud - ESO 
#          - April 2020 0RIG
#          - May 2021 full CASA 6 version mockup
#          - March 2022 CASA 6.4 'class' version for PL
#          - July 2022 Edit for PL2022 implementation
#            includes logger output messages
#            included bl-name, bl-len, phase RMS in log
# K Berry - NRAO
#          -  July 2023 moved into PL
##################


class PhaseStabilityHeuristics(object):
    def __init__(self, inputsin, outlier_limit, flag_tolerance, max_poor_ant):
        self.vis = inputsin.ms.basename
        self.outlierlimit = outlier_limit
        self.flag_tolerance = flag_tolerance
        self.maxpoorant = max_poor_ant

        self.spw = None
        self.field = None
        self.scan = None
        self.refantid = None
        self.caltable = []
       
        context = inputsin.context
        ms = context.observing_run.get_ms(self.vis)
        self.caltable = sorted(ms.phaseup_caltable_for_phase_rms)

        # Fetch the bandpass phaseup caltable
        if len(self.caltable) > 0:
            self.caltable = self.caltable[-1]
        else:
            msg = "For {}, missing bandpass phaseup caltable and cannot perform decoherence assessment.".format(self.vis)
            LOG.error(msg)
            raise Exception(msg)
        
        self.refant = inputsin.refant.split(',')[0]  # this is the NAME not the index
        self.antlist = []
        # loop ant list for id of refant
        for ant in inputsin.ms.antennas:
            if ant.name == self.refant:
                self.refantid = ant.id
            self.antlist.append(ant.name)

        # Select which SpW to use for phase stability analysis.
        spw_candidates = self._get_spw_candidates(inputsin)

        ##################
        # PIPE-1661 related
        # phasecal field ID out of the ms context passed - need for later flag assessment
        ph_fieldids = [f.id for f in inputsin.ms.get_fields(intent='PHASE')]
        # traditionally we should only have one phase calibrator
        # but still pull this as a list and use in the _getblflags function
        self.ph_ids = ph_fieldids

        # future note, if we have multiple phase calibrators and these have
        # a different spectral spec, should we do this as per the bandpass
        # i.e. get the scans, then find the id of the field for that scan?
        # we can loop the first scan of a scan list and find the associated 
        # field id - as below. 
        # bnot expected to be used in standard observing practice 
        ####################

        # Get the bandpass, using the first of the SpW candidates.
        targeted_scans = inputsin.ms.get_scans(scan_intent='BANDPASS', spw=str(spw_candidates[0]))
        bp_scan = []
        bp_field = []
        bp_id = []
        for elm in targeted_scans:
            bp_scan.append(elm.id)  # take the ID
            for elms in elm.fields:
                bp_field.append(elms.name)  # fields is a dict again...
                bp_id.append(elms.id)  # PIPE-1661

        self.scan = bp_scan[0]  # we only want the first scan, in case there are multiple for the BP (e.g. spectral scan data - NEEDS TESTING ON) - might want to do a sorted just to be sure. 
        self.field = bp_field[0]
        self.fieldId = bp_id[0]  # PIPE-1661 needs ID not a name 

        # Select which SpW to use for phase stability analysis, and retrieve
        # corresponding baseline flagging information.
        self.spw, self.blflags = self._get_final_spw_and_blflags(inputsin, spw_candidates)

        # Check to see if data are ACA or not
        self.PMinACA = self._pm_in_aca()

        # for BP scan only values from function for given scan/field/spw only
        # this is keyed by blname, e.g.  self.baselines['DA42-DA42'] 
        self.baselines = self._getbaselinesproj()

        # Get time/length of the bandpass scan 
        self.totaltime, self.difftime = self._get_bandpass_scan_time()

        # getcycletime will use a lookup if there is 1 or less phase cal scans (PIPE-1848)
        self.cycletime = self._getcycletime()

        # PIPE-2081: for BandToBand MSes, also retrieve flagging information for
        # the diffgain reference SpW associated with the selected SpW; to be
        # used to assess fully flagged baseline for the phase calibrator(s).
        if inputsin.ms.is_band_to_band:
            dg_refspws = inputsin.ms.get_spectral_windows(task_arg=inputsin.spw, intent='DIFFGAINREF')
            dg_srcspws = inputsin.ms.get_spectral_windows(task_arg=inputsin.spw, intent='DIFFGAINSRC')
            spwmap = phasespwmap.update_spwmap_for_band_to_band([], dg_refspws, dg_srcspws)
            spw_ref = spwmap[self.spw]
            self.blflagsref = self._getblflags(spw=spw_ref)
        else:
            # For any other MS, the assessment of fully flagged baselines for
            # phase calibrator(s) can re-use the same flagging information
            # retrieved for the SpW selected to analyse.
            self.blflagsref = self.blflags

        # Set holder for outlier antennas related to plotting and scores later
        self.outlier_antennas = []

        # Setup the main analysis dictionary
        self.allResult = {}

        # Write the seutp info to the log
        self._log_setup_info()

    def analysis(self) -> Tuple[Dict, float, float, List]:
        """
        Do the phase RMS calculation, outlier analysis,
        and return everything required for qa scores, plotting, weblog
        """
        self._do_analysis()
        return copy.deepcopy(self.allResult), self.cycletime, self.totaltime, copy.deepcopy(self.outlier_antennas)

    def _do_analysis(self):
        """
        Do the phase RMS calculation, outlier analysis,
        and fill the dictionary with everything required for later scoring and 
        plotting

        inputs used are:
                  self.cycletime, self.totaltime, self.PMinACA,
                  self.outlierlimit, self.outlier_antennas, self.maxpoorant 

        uses functions:
                  self._phase_rms_caltab, self.mad

        fills:
                  self.allResults

        dict keys are:
                  blphaserms, blphasermscycle, bllen, blname, blout,
                  blphasermsbad, blphasermscyclebad, bllenbad,
                  antphaserms, antphasermscycle, antname,
                  phasermsP80, phasermscycleP80, blP80, blP80orig
        """
        
        # Call to phase_rms_caltab
        # gets baseline based phase RMS and antenna based phase RMS for
        # the total time (i.e. length of BP scan) and 
        # the cycle time (time it takes to cycle the start of a phase cal scan to the next - ties with a 'decohernce time' over the target)
        if self.cycletime < self.totaltime:
            self.allResult = self._phase_rms_caltab(timeScale=self.cycletime)  # if cycle time is shorter we pass the option so it gets assessed
        else:
            self.allResult = self._phase_rms_caltab()  # otherwise no options added and cycletime value will equal total time value

        # Check the cycle time calculation is not all nans - might happen if there are considerable flags
        # i.e. if a large fraction of data > self.flag_tolerance are flagged
        cycleResultFinite = len(np.array(self.allResult['blphasermscycle'])[np.isfinite(self.allResult['blphasermscycle'])])

        # if it is all flagged, theory to scale down total time with 'lower' scaling constant power (Maud et al. 2022)
        if cycleResultFinite == 0:
            self.allResult['blphasermscycle'] = np.array(self.allResult['blphaserms'])*(self.cycletime/self.totaltime)**0.3

        if self.PMinACA:
            # we need to exclude PM antennas from the calculation as they 'can' be too 'long' baselines
            # and not really useful for the understanding of the target likely phase RMS 
            # where only CM used in the images. Thus cut the PM from the upper 80 cut
            blACAid = np.array([blid for blid in range(len(self.allResult['blname'])) if 'PM' not in self.allResult['blname'][blid]])
            antACAid = np.array([antid for antid in range(len(self.allResult['antname'])) if 'PM' not in self.allResult['antname'][antid]])
 
            # Reset allResult to exclude the PM baselines, and PM ants only
            bl_keys = ['blphaserms', 'blphasermscycle', 'bllen', 'blname'] # PIPE-1633
            ant_keys = ['antphaserms', 'antphasermscycle', 'antname'] # PIPE-1633
            for bl_key in bl_keys:
                self.allResult[bl_key] = np.array(self.allResult[bl_key])[blACAid]
            for ant_key in ant_keys:
                self.allResult[ant_key] = np.array(self.allResult[ant_key])[antACAid]
            
        # Get 80th percentile baseline length and ID of all baselines above it
        self.allResult['blP80orig'] = np.percentile(self.allResult['bllen'], 80)  # was xy

        # PIPE-1662 P80 to acount for flagged antennas, i.e work from P80 of the 'good' data
        self.allResult['blP80'] = np.percentile(np.array(self.allResult['bllen'])[np.isfinite(np.array(self.allResult['blphaserms']))], 80)
        #####

        ID_all80 = np.where(np.array(self.allResult['bllen'])> self.allResult['blP80'])[0]  # was xy

        # First calculation of the representative phase RMS values
        self.allResult['phasermsP80'] = np.median(np.array(self.allResult['blphaserms'])[ID_all80[np.isfinite(np.array(self.allResult['blphaserms'])[ID_all80])]])
        self.allResult['phasermscycleP80'] = np.median(np.array(self.allResult['blphasermscycle'])[ID_all80[np.isfinite(np.array(self.allResult['blphasermscycle'])[ID_all80])]])
        phaseRMScycleP80mad = self.mad(np.array(self.allResult['blphasermscycle'])[ID_all80[np.isfinite(np.array(self.allResult['blphasermscycle'])[ID_all80])]])
        
        # Begin the outlier checks
        # first check if any antennas are just above 100 deg phase RMS (this means ~pure phase noise for phases between -180 to +180 deg)
        # so sensible to identify these antennas - over total time
        ID_poorant = np.where(np.array(self.allResult['antphaserms'])[np.isfinite(self.allResult['antphaserms'])]>self.outlierlimit)[0]
        
        if len(ID_poorant) > 0:
            for antout in ID_poorant:
                self.outlier_antennas.append(np.array(self.allResult['antname'])[np.isfinite(self.allResult['antphaserms'])][antout])

        # for what are yellow or red scores where phase RMS >50deg
        # we check for statistical outliers, and then clip these out and re-calculate the phaseRMS
        # in the case PL missed bad antennas were causing the high phase RMS
        # self.outlier_antennas is used/passed in the score function as it adjusts the score and message too
        if self.allResult['phasermscycleP80'] > 50.:
            statsoutlierlimit = self.allResult['phasermscycleP80'] + 4.*phaseRMScycleP80mad # tested limit works well
            # ant phase on cycle time !
            ID_poorant = np.where(np.array(self.allResult['antphasermscycle'])[np.isfinite(self.allResult['antphasermscycle'])] > statsoutlierlimit)[0]
            if len(ID_poorant)>0:
                for antout in ID_poorant:
                    self.outlier_antennas.append(np.array(self.allResult['antname'])[np.isfinite(self.allResult['antphaserms'])][antout])
        
            # max limit on number of 'bad' antennas to be allowed to exclude from calculations (set at 11 good as tested in PIPE692)
            # we clip them out and recalculate - score function also tracks and gives a warning
            # if >11, basically the data is rubbish, so don't clip and let scores be low
            if len(self.outlier_antennas) > 0 and len(self.outlier_antennas) < self.maxpoorant:

                # crude way to get the index values is a loop over the baselines
                # is there a way to better search allResult['blname'] to check if any self.outlier_antennas are (or are not) in them
                ID_goodbl=[]
                ID_badbl=[]
                for idg, bln in enumerate(self.allResult['blname']):
                    if (bln.split('-')[0] not in self.outlier_antennas) and (bln.split('-')[1] not in self.outlier_antennas):
                        ID_goodbl.append(idg)
                    else: # assume bad
                        ID_badbl.append(idg)

                # now simply make a common list of IDs from good ones and the ID_all80 list - which are want we want to assess
                ID_all80 = np.array(np.sort(np.array(list(set(ID_goodbl).intersection(ID_all80)))))

                # recalculate the phase RMS that is bl >P80 and in the 'good' list
                self.allResult['phasermsP80'] = np.median(np.array(self.allResult['blphaserms'])[ID_all80[np.isfinite(np.array(self.allResult['blphaserms'])[ID_all80])]])
                self.allResult['phasermscycleP80'] = np.median(np.array(self.allResult['blphasermscycle'])[ID_all80[np.isfinite(np.array(self.allResult['blphasermscycle'])[ID_all80])]])

                # store outliers for passing to plot function - will be plotted in 'shade/alpha'
                self.allResult['blphasermsbad'] = np.array(self.allResult['blphaserms'])[np.array(ID_badbl)]
                self.allResult['blphasermscyclebad'] = np.array(self.allResult['blphasermscycle'])[np.array(ID_badbl)]
                self.allResult['bllenbad'] = np.array(self.allResult['bllen'])[np.array(ID_badbl)]
            else:
                # none the 'bad' entires in dict 
                self.allResult['blphasermsbad'] = None
                self.allResult['blphasermscyclebad'] = None
                self.allResult['bllenbad'] = None
        else:
            # this else is for <50deg phase RMS where we do not recalcualte the phase RMS as its low already
            # but we still want to identify any outliers to notify in the messages
            statsoutlierlimit = np.max([self.allResult['phasermscycleP80'] + 6.0 * phaseRMScycleP80mad, 2.0 * self.allResult['phasermscycleP80']])
            # outlier on cycle time 
            ID_poorant = np.where(np.array(self.allResult['antphasermscycle'])[np.isfinite(self.allResult['antphasermscycle'])]>statsoutlierlimit)[0]
            # add them to the list so score code picks them up if required and changes the messages
            if len(ID_poorant)>0:
                for antout in ID_poorant:
                    self.outlier_antennas.append(np.array(self.allResult['antname'])[np.isfinite(self.allResult['antphaserms'])][antout])

            # none the 'bad' entires in dict 
            self.allResult['blphasermsbad'] =  None
            self.allResult['blphasermscyclebad'] = None
            self.allResult['bllenbad'] = None

        # now loop over and log write the phase RMS parameters
        # sort by baseline len
        LOG.info(' Phase RMS calculated over the cycle time as function of baseline length')
        for bnblph in list(zip(np.array(self.allResult['blname'])[np.array(self.allResult['bllen']).argsort()],np.array(self.allResult['bllen'])[np.array(self.allResult['bllen']).argsort()],
                               np.array(self.allResult['blphasermscycle'])[np.array(self.allResult['bllen']).argsort()])):

            # add simple text to flagged (i.e. we made nan for the calculation), or outlier antennas (outlier comes first)
            if str(bnblph[0].split('-')[0]) in self.outlier_antennas or str(bnblph[0].split('-')[1]) in self.outlier_antennas:
                LOG.info(str(bnblph)+' - outlier')
            elif not np.isfinite(bnblph[2]):
                LOG.info(str(bnblph)+' - flagged')
            else:
                LOG.info(str(bnblph))

        if len(self.outlier_antennas) > 0:
            antoutstr = ",".join(self.outlier_antennas)
            LOG.info(" Possible high phase RMS on antenna(s): {}".format(antoutstr))

        # Save off spw, scan, field
        self.allResult['spw'] = self.spw
        self.allResult['scan'] = self.scan
        self.allResult['field'] = self.field

    # Methods used by __init__
    def _pm_in_aca(self) -> bool:
        """
        Check if the array is ACA and has PM antennas

        input used:
                 self.antlist

        returns: 
                Bool based on PM with CM antennas
        """
        antUse = self.antlist
        antUse = ",".join(antUse)  # See: PIPE-1633

        if 'CM' in antUse and 'PM' in antUse and 'DA' not in antUse and 'DV' not in antUse:
            PMincACA = True
        else:
            PMincACA = False

        return PMincACA

    def _getbaselinesproj(self, field_id: Optional[int] = None) -> Dict[str, float]:
        """
        Code to get the projected baseline from the openend 
        visibilitiy file already - these are ordered in 
        terms of antennas. This is a modified stand alone version
        similar to the getProjectedBaselines from Todd Hunter's AUs.

        returns a dict of lengths which are BL name keyed
        e.g. bllens[DA41-DA42] - key is name ant 1 - dash - name ant 2
        """
        with casa_tools.MSMDReader(self.vis) as msmd:
            spwchan = msmd.nchan(self.spw)
            datadescid = msmd.datadescids(spw=self.spw)[0]
        
        with casa_tools.MSReader(self.vis) as ms:
            ms.selectinit(datadescid=datadescid)
            ms.select({'uvdist': [1e-9, 1e12]})  # avoid auto corr
            ms.selectchannel(1, 0, spwchan, 1)  # data structure related
            ms.select({'scan_number': int(self.scan)})
            if field_id:
                ms.select({'field_id': int(field_id)})
            alldata = ms.getdata(['uvdist', 'antenna1', 'antenna2'])

        # The length of e.g. alldata['uvdist'] is > total no. of Bls - it loops over all time stamps of the BP 
        # we need a mean of the unique values (as Todd's aU) otherwise we just get the first time entry in the below
        bldict = {}
        uniBl = []
        baselineLen = {}
        for allID in range(len(alldata['uvdist'])):
            myBl = '%s-%s' % (self.antlist[alldata['antenna1'][allID]], self.antlist[alldata['antenna2'][allID]])
            thelen = alldata['uvdist'][allID]
            if myBl not in bldict:
                bldict[myBl] = []
            bldict[myBl].append(thelen)
            uniBl.append(myBl)

        uniBl = np.unique(uniBl)

        for myBl in uniBl:
            baselineLen[myBl] = np.mean(bldict[myBl])  # this has a list for each
        
        # order irrelavant as keyed here with BL Name
        return baselineLen
    
    def _get_bandpass_scan_time(self) -> Tuple[float, float]:
        """
        Read a caltable file and return time
        shifted to start at zero seconds starting time.
        Note these are the recored times in the caltable

        uses inputs:
               self.caltable, self.spw, self.scan, self.antlist
        
        :returns: total time of baseline scan, average integration time 
        :rtype: float, float
        """
        with casa_tools.TableReader(self.caltable) as tb:
            nant = len(self.antlist)
            timeX = []
            antid = 0
            while len(timeX) < 2:
                tb1 = tb.query("ANTENNA1 == %s  && SPECTRAL_WINDOW_ID == %s && SCAN_NUMBER == %s" % (antid, self.spw, self.scan))
                timeX = tb1.getcol('TIME')
                antid += 1
                if antid == nant - 1:
                    break
            tb1.close()

        # If there is no time found (should never happen), throw.
        if len(timeX) < 2:
            raise Exception("No times found in bandpass caltable: {}".format(self.caltable))

        timeX = np.array(timeX) - timeX[0]
        total_bp_scan_time = timeX[-1]
        avg_integration_time = np.median(np.diff(timeX))

        return total_bp_scan_time, avg_integration_time

    def _getcycletime(self) -> float:
        """
        Computes the median time (in seconds) between visits to the specified intent.
        Note that other parts of the ALMA project consider the "cycleTime" to be the 
        scan duration on the science target before going back to the phase calibrator,
        i.e. ignoring the duration of the phasecal scan, the ATM cal scans, the 
        checksource, and all the slewing and overhead.

        If the cycle time is not found, diverts to a lookup table (See: PIPE-1848)
       
        This method is adapted from:
        - Todd Hunter ORIG in analysis Utils (cycleTime)
        - LM edited for this code

        input used:
            self.vis

        return: the cycletime (float)
        """
        with casa_tools.MSMDReader(self.vis) as msmd:
            scans = msmd.scansforintent('*PHASE*')

            # PIPE-1848: if there is no phase calibrator, we cannot get cycle time either
            if len(scans) == 0:
                # No phase calibrator in the data
                LOG.warning("Using lookup Cycle Time as there is no PHASE intent in {}".format(self.vis))
                usecycle = self._lookupcycle()
                return usecycle

            # Use the lookup if there is only one phase calibrator scan. See: PIPE-1848
            if len(scans) == 1:
                LOG.warning("Using lookup Cycle Time as there is only 1 PHASE calibrator scan for {}".format(self.vis))
                usecycle = self._lookupcycle()
                return usecycle

            # all correctly formed data should go here
            # now get the times for the scans and work out the cycle time 
            times = []

            for scan in scans:
                times.append(np.min(msmd.timesforscan(scan)))

        if len(times) == 1:
            LOG.warning("There was only 1 scan with this intent.")

        diffs = np.diff(times)
        return np.median(diffs)

    def _lookupcycle(self) -> float:
        """
        Look up the nearest default cycle time
        using best practices for Baseline length and
        the frequency band. This is only needed when the
        returned cycle time is None, which only happens
        for malformed data with only one PHASE cal scan,
        that ultimately should not be coming to pipeline at all.

        See: PIPE-1848
        """
        if self.PMinACA:
            config = 0
        else:
            config = self._getconfig()  # Configs run 1 to 10, 0 is ACA

        bandu = self._getband()  # Gets freq then band 1 to 10

        # List of lists for cycle times for Cycle 10
        # [config][band]
        # Band index 0 doesn't exist padded with 999

        cycletimes = [[999, 660, 999, 660, 660, 480, 540, 480, 480, 360, 360],
                      [999, 660, 999, 630, 630, 450, 510, 390, 390, 270, 270],
                      [999, 660, 999, 630, 630, 450, 510, 390, 390, 270, 270],
                      [999, 660, 999, 630, 630, 450, 510, 270, 270, 210, 210],
                      [999, 660, 999, 630, 630, 450, 510, 270, 270, 210, 210],
                      [999, 390, 999, 390, 390, 270, 210, 130, 130, 130, 130],
                      [999, 390, 999, 390, 390, 270, 210, 130, 130, 130, 130],
                      [999, 80, 999, 80, 80, 80, 80, 80, 80, 80, 80],
                      [999, 80, 999, 80, 80, 80, 80, 80, 65, 58, 45],
                      [999, 80, 999, 80, 80, 80, 80, 80, 65, 58, 45],
                      [999, 80, 999, 80, 80, 80, 80, 80, 65, 58, 45]]

        cycletime = cycletimes[config][bandu]

        if cycletime == 999:
            msg = "For {}, unable to find back-up cycle time.".format(self.vis)
            raise Exception(msg)

        return float(cycletime)
    
    def _getfreq(self) -> float:
        """ 
        Get the median frequency for the spw.

        The return is in Hz used for getting the band.

        See: PIPE-1848
        """
        # TODO: update to fetch from context
        with casa_tools.MSMDReader(self.vis) as msmd:
            freqval = np.median(msmd.chanfreqs(self.spw))  # otherwise all channels 
    
        return freqval
    
    def _getband(self) -> int:
        """
        Identify the Band for specific frequency (in GHz)
        PIPE-1848
        """

        freq = self._getfreq()
        
        lo = np.array([35, 67, 84, 125, 157, 211, 275, 385, 602, 787])*1e9
        hi = np.array([51, 85, 116, 163, 212, 275, 373, 500, 720, 950])*1e9
        # Set Band 2 to stop at current B3

        bandsel = np.arange(1, len(lo)+1)[(freq > lo) & (freq < hi)][0]

        return bandsel

    def _getconfig(self) -> int:
        ''' Identify the configuration based on 
        baseline length - returns as an int to 
        allow a table search for cycle time
        PIPE-1848
        '''

        # self.baselines is a dict, rule is max on a dict returns a
        # key for the max so this is max baseline to use 
        maxbl = self.baselines[max(self.baselines)]
        shortbl = np.array([0, 44, 160, 313, 499, 783, 1399, 2499, 3599, 8499, 13899])
        longbl = np.array([45, 161, 314, 500, 784, 1400, 2500, 3600, 8500, 13900, 20000])  # upper limit greater than baseline len 
            
        config = np.arange(0, len(longbl))[(maxbl > shortbl) & (maxbl < longbl)][0]

        return config
    
    def _getblflags(self, spw: int, ant1: Optional[str] = None, ant2: Optional[str] = None) \
            -> Tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
        """
        Code to open and close the table for the MS 
        and get the baseline based flags in one lump

        we only pass the spw as previously established
        here the assumption is that any phase RMS issue with
        baseline length will be flagged across all the data,
        true atmospheric things would not be spectral window based.
        phase cal field id already known to class.

        Added to support PIPE-1661

        Args:
            spw: spectral window to retrieve flagging info for
            ant1: if provided (together with ant2), then retrieve flagging info
                only for baseline defined by ant1, ant2
            ant2: if provided (together with ant1), then retrieve flagging info
                only for baseline defined by ant1, ant2

        Returns:
             4-tuple containing Numpy arrays for flags, antenna1, antenna2, and field.
        """
        # MS reads datadescid not spw id
        # need to do the conversion to make sure we use
        # the correct value
        with casa_tools.MSMDReader(self.vis) as msmd:
            datadescid = msmd.datadescids(spw=spw)[0]

        with casa_tools.TableReader(self.vis) as tb:
            if (ant1 is not None) and (ant2 is not None):
                tb1 = tb.query("ANTENNA1 == %s && ANTENNA2 == %s && DATA_DESC_ID == %s " % (ant1, ant2, datadescid))
            else:
                # Speed up - pull everything in one go not per antena, divide out later in main function
                tb1 = tb.query("DATA_DESC_ID == %s " % (datadescid))

            flags = tb1.getcol('FLAG')  # index is [pol][chan][integration]
            a1s = tb1.getcol('ANTENNA1')
            a2s = tb1.getcol('ANTENNA2') 
            field = tb1.getcol('FIELD_ID')
            tb1.close()

        return flags, a1s, a2s, field  

    def _log_setup_info(self): 
        """
        Prints information about the setup of the SSF analysis to the log at the "info" level
        """
        LOG.info('*** Phase RMS vs Baseline assessment setup ***')
        LOG.info(' Working on the MS {}'.format(self.vis))
        LOG.info(' Selected scan {}'.format(self.scan))
        LOG.info(' Selected spw {}'.format(self.spw))
        LOG.info(' Using field {}'.format(self.field))
        LOG.info(' Using caltab {}'.format(self.caltable))
        LOG.info(' The refant is {} id {}'.format(self.refant, self.refantid))
        LOG.info(' Is ACA with PM data {}'.format(self.PMinACA))
        LOG.info(' Total BP scan time {}'.format(self.totaltime))
        LOG.info(' Phase referencing cycle time {}'.format(self.cycletime))
        LOG.info(' The median integration time {}'.format(self.difftime))

    # Methods used by _do_analysis()
    def _get_cal_phase(self, ant: int) -> np.ndarray:
        """
        Read a caltable file and select the
        phases from one pol (tested in PIPE692 as sufficient).
        If those phase data have a flag, the phase
        value is set to nan, which is dealt with in the correct
        way during the rest of the calulation. The phases 
        are unwrapped, i.e. solved for the 2PI ambiguitiy 

        input required:
                 ant (int)  - the antenna to get the phases for 

        uses inputs:
                 self.caltable, self.refantid, self.spw, self.scan
  

        returns: float array of the phases of an antenna
        """
        with casa_tools.TableReader(self.caltable) as tb:
            tb1 = tb.query("ANTENNA1 == %s && ANTENNA2 == %s && SPECTRAL_WINDOW_ID == %s && SCAN_NUMBER == %s "%(ant, self.refantid, self.spw, self.scan))
            cal_phases = tb1.getcol('CPARAM')
            cal_phases = np.angle(cal_phases[0][0])  ## in radians one pol only

            # code works fine as tested in PIPE 692 with only XX pol extractions
            # not 'really' required to do both pols, but could do so, and also get an average
            # single and full-pol to check, this single pull will work, but with more then the order differs (c.f. renorm pol code)

            # Exclude flagged data as it is extracted from the gaintable
            # this is antenna based only   
            flags = tb1.getcol('FLAG')  # [0][0]
            tb1.close()

        cal_phases = [val if not flags[0][0][id_u] else np.nan for id_u, val in enumerate(cal_phases)]  # so only one pol used 'X'
        cal_phases = np.array(cal_phases)

        # Correct wraps in phase stream
        # Note: everything is in radians
        cal_phases = self.phase_unwrap(cal_phases)
        return cal_phases

    def _phase_rms_caltab(self, antout: list=[], timeScale: float=None) -> Dict[str, str]:
        """
        Run the loop over the caltable, work out the baseline based phases,
        and calculate the phase RMS. Also get the Phase RMS per antenna (with
        respect to the refant - i.e. ant based phase RMS).
        
        inputs used:
              self.antlist, self.flag_tolerance, self.difftime, self.baselines

        calls functions:
              self._get_cal_phase, self.ave_phase, self.std_overlapping_avg
 
        returns:  rms_results{}
        dict keys: blphaserms, bphasermscycle, bllen, blname,
                   antphaserms, antphasermscycle, antname
        """

        # setup the result list
        rms_results = {}
        rms_results['blphaserms'] = []
        rms_results['blphasermscycle'] = []
        rms_results['bllen'] = []
        rms_results['blname'] = []
        rms_results['antphaserms'] = []
        rms_results['antphasermscycle'] = []
        rms_results['antname'] = []

        # PIPE-1661 baseline based flag awareness - these store flags 
        # that we read in - just in case we 'need' this information
        rms_results['blflags'] = []

        nant = len(self.antlist)
        iloop = np.arange(nant-1)

        for i in iloop:
            # Ant based parameters
            pHant1 = self._get_cal_phase(i)
            rms_results['antname'].append(self.antlist[i])

            # Make an assessment of flagged data for that antenna
            if len(pHant1[np.isnan(pHant1)]) > self.flag_tolerance*len(pHant1):
                rms_results['antphaserms'].append(np.nan)
                rms_results['antphasermscycle'].append(np.nan)
                
            else:
                # Do averaing -> 10s
                pHant_ave = self.ave_phase(pHant1, self.difftime, over=10.0)  # for thermal/short term noise
                rmspHant_ave = np.std(np.array(pHant_ave)[np.isfinite(pHant_ave)])
                rms_results['antphaserms'].append(rmspHant_ave)
                if timeScale:
                    rmspHant_ave_cycle = self.std_overlapping_avg(pHant_ave, self.difftime, over=timeScale)
                    rms_results['antphasermscycle'].append(rmspHant_ave_cycle)
                else:
                    rms_results['antphasermscycle'].append(rmspHant_ave)

            jloop = np.arange(i+1, nant)  # baseline loop
            for j in jloop:
                # Get the phases - needs to be the single read in table 
                
                # pHant1 is read in above already
                pHant2 = self._get_cal_phase(j)
                # phases from cal table come in an order, baseline then is simply the subtraction
                pH = pHant1 - pHant2
                    
                # fill baseline information now
                rms_results['blname'].append(self.antlist[i]+'-'+self.antlist[j])
                # OLD (new) WAY from context - average all as overview
                # not direcrtly the BP only
                # rms_results['bllen'].append(float(self.baselines.get_baseline(i,j).length.value)) # from context input
                
                rms_results['bllen'].append(float(self.baselines[self.antlist[i]+'-'+self.antlist[j]])) # from function

                # PIPE-1661 - get baseline based flags
                blisflagged = self._isblflagged(i, j)

                rms_results['blflags'].append(blisflagged)
                ######

                # make assessment if this is a bad antenna
                if self.antlist[i] in antout or self.antlist[j] in antout:
                    rms_results['blphaserms'].append(np.nan)
                    rms_results['blphasermscycle'].append(np.nan)

                # PIPE-1661 assessment (could tie with if above but separated for clarity
                elif blisflagged:  # can only be T/F bool
                    rms_results['blphaserms'].append(np.nan)
                    rms_results['blphasermscycle'].append(np.nan)
                ###############
              
                # make an assessment of flagged data
                elif len(pH[np.isnan(pH)]) > self.flag_tolerance*len(pH):
                    rms_results['blphaserms'].append(np.nan)
                    rms_results['blphasermscycle'].append(np.nan)

                else:
                    # do averaing -> 10s
                    # need to get time and then rebin
                    pH_ave = self.ave_phase(pH, self.difftime, over=10.0)  # for thermal/short term noise
                    rmspH_ave = np.std(np.array(pH_ave)[np.isfinite(pH_ave)])  
                    rms_results['blphaserms'].append(rmspH_ave)
                    if timeScale:
                        rmspH_ave_cycle = self.std_overlapping_avg(pH_ave, self.difftime, over=timeScale)
                        rms_results['blphasermscycle'].append(rmspH_ave_cycle)
                    else:
                        rms_results['blphasermscycle'].append(rmspH_ave)

        # set RMS output in degrees as we want
        for key_res in ['blphaserms', 'blphasermscycle', 'antphaserms', 'antphasermscycle']:
            rms_results[key_res]= np.degrees(rms_results[key_res])
        
        return rms_results
    
    def _isblflagged(self, ant1: int, ant2: int):
        """
        function to make the assessment of 
        the flags that are saved and return if the
        full baseline is flagged or not
       
        requires the self.blflags
        """

        # in checking the order of data read associated with each antenna
        # the ordering system is not 100% preordained it seems,
        # i.e. we have to select each time the index of bls in agreement with 
        # the antenna pair and antenna list - then check the flags
 
        # simply need to select the correct cut for the baseline in question
        # start with the bandpass, if totally flagged assume all data flagged 
        # for that baseline

        # speed assumption is we will hit a false before a true
        # flag for good data, so assume bad, set to good
        flaggedbl = True

        idbl = np.where((self.blflags[1]==ant1) & (self.blflags[2]==ant2) & (self.blflags[3]==self.fieldId))[0]
        # loop over correct ant, integration time index 
        for iduse in idbl:

            if self.blflags[0].shape[0]*self.blflags[0].shape[1]!= np.sum(self.blflags[0][:,:,iduse]):
                flaggedbl = False
                break

        # else here for Phase cal check if not flagged in BP
        if not flaggedbl:
            flaggedbl = True
            for phid in self.ph_ids:  # usually one phase cal anyway but loop incase multiple
                idbl = np.where((self.blflagsref[1] == ant1) & (self.blflagsref[2] == ant2) & (self.blflagsref[3] == phid))[0]
                for iduse in idbl:
                    if self.blflagsref[0].shape[0] * self.blflagsref[0].shape[1] != np.sum(self.blflagsref[0][:, :, iduse]):
                        # if there is unflagged data in this antenna pair and time then
                        # that baseline is, in fact, not fully flagged
                        flaggedbl = False
                        break

        return flaggedbl

    def _get_final_spw_and_blflags(self, inputsin, qa_spw_candidates) \
            -> Tuple[int, Tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]]:
        """
        Select the best candidate SpW for the phase decoherence analysis based
        on ranked list and baseline flagging information.

        Args:
            inputsin: inputs from caller task (expected to be hifa_spwphaseup)

        Returns:
            2-Tuple containing:
                - selected SpW ID (integer)
                - 4-tuple representing baseline flagging info for selected SpW
        """
        # PIPE-1871: from the ranked list of SpWs, pick the first SpW for which
        # the baselines are not fully flagged.
        spwid, blflags = None, None
        for qa_spw in qa_spw_candidates:
            candidate_spwid = int(qa_spw)
            LOG.debug(f"{inputsin.ms.basename}: assessing baseline flags for SpW {candidate_spwid}.")

            # Retrieve flagging information for the selected SpW (PIPE-1661).
            blflags = self._getblflags(spw=candidate_spwid)

            # Check whether for current spw (and bandpass field), all
            # corresponding baselines rows are entirely flagged in all pol and
            # all channels:
            # - blflags[3] represents the field, with shape (nrow).
            # - blflags[0] represents the flag, with shape (npol, nchan, nrow).
            # - np.all(blflags[0], (0, 1)) performs an AND on flags in each
            #   (npol, nchan) plane, i.e. checking for each row if all pol, chan
            #   are flagged.
            # If the baselines are not entirely flagged, then keep this Spw as
            # the one to analyse, and stop looking.
            if not np.all(np.all(blflags[0], (0, 1))[blflags[3] == self.fieldId]):
                spwid = candidate_spwid
                break
            else:
                LOG.info(f"{inputsin.ms.basename}: SpW {candidate_spwid} appears to be fully flagged.")

        if spwid is None:
            raise Exception(f"{inputsin.ms.basename}: unable to identify a SpW for assessing phase decoherence; all"
                            f" candidate SpWs appear to be fully flagged.")

        return spwid, blflags

    # Static methods
    @staticmethod
    def _get_spw_candidates(inputsin) -> List[str]:
        """
        Retrieves a list of spectral window candidates for the phase decoherence
        analysis, ranked based on atmosphere heuristics.

        Args:
            inputsin: inputs from caller task (expected to be hifa_spwphaseup)

        Returns:
            List of SpW IDs (string)
        """
        # Retrieve SpWs to consider.
        if inputsin.ms.is_band_to_band:
            # For a BandToBand MS, restrict to diffgain on-source SpWs.
            spws = inputsin.ms.get_spectral_windows(intent='DIFFGAINSRC')
        else:
            # For all other MSes, use all science SpWs.
            spws = inputsin.ms.get_spectral_windows(task_arg=inputsin.spw)

        # PIPE-2081: rank the SpWs by Tsys and bandwidth using atmosphere
        # heuristics module.
        atmheuristics = atm_heuristic.AtmHeuristics(context=inputsin.context, vis=inputsin.vis, spw=spws)
        qa_spw_list = atmheuristics.spwid_rank_by_tsys_and_bandwidth('BANDPASS')
        if qa_spw_list is None:
            # If ranking by Tsys failed (e.g. no Tsys table, or due to
            # flagging), then fall back to ranking by opacity and bandwidth:
            LOG.info(f"Ranking spws by bandwidth and Tsys failed for {inputsin.ms.basename}; will rank by bandwidth"
                     f" and opacity instead.")
            qa_spw_list = atmheuristics.spwid_rank_by_opacity_and_bandwidth()

        if not qa_spw_list:
            raise Exception(f"{inputsin.ms.basename}: unable to identify a SpW for assessing phase decoherence; no"
                            f" candidate SpWs found.")

        # PIPE-1871: as potential fall-back candidates, keep only those SpWs
        # that have the same SpectralSpec as the top candidate.
        qa_spws = [inputsin.ms.get_spectral_window(spwid) for spwid in qa_spw_list]
        qa_spws = [inputsin.ms.get_spectral_window(spwid) for spwid in qa_spw_list]
        qa_spw_list = [str(spw.id) for spw in qa_spws if spw.spectralspec == qa_spws[0].spectralspec]

        return qa_spw_list

    @staticmethod
    def phase_unwrap(phase: np.ndarray) -> np.ndarray:
        """
        Unwraps the phases to solve for 2PI ambiguities
        Input phases may contain np.nan. 

        :param phase: phase in radians
        :type phase: float array
        :returns: unwrapped phase-array
        :rtype: float array
        """
        working_phase = phase.copy()
        working_phase[~np.isnan(working_phase)] = np.unwrap(working_phase[~np.isnan(working_phase)])
        return working_phase
    
    @staticmethod
    def mad(data: np.ndarray, axis: Optional[int] = None) -> float:
        """
        This calculates the MAD - median absolute deviation from the median
        The input must be nan free, i.e. finite data
        
        :param data: input data stream
        :type data: list or array
        
        :returns: median absolute deviation (from the median)
        :rtyep: float
        """
        return np.median(np.abs(data - np.median(data, axis)), axis)

    @staticmethod
    def std_overlapping_avg(phase: np.ndarray, diffTime: float, over: float=120.0) -> float:
        """
        Calculate STD over a set time and return the average of all overlapping
        values of the standard deviation - overlapping estimator. This acts
        upon a phase-time stream of data. Will consider only finite values in 
        the phase-time stream (i.e. phase array). The phase array must be 
        unwrapped, i.e. continous, no breaks or 2PI ambiguities
    
        This is the standard deviation, so it takes out the
        mean value. RMS with mean or fit removed provides
        the same value for zero-centered phases.

        :param phase: any unwrapped input phase
        :type phase: array
        :param diffTime: the time between each data integration
        :type diffTime: float
        :param over: time in seconds to calculate the SD over
        :type over: float
        :returns: average standard deviation for the dataset calcualted over the input timescale 
        :rtype: float
        """

        # Overlap in elements
        over = int(np.round(over / diffTime))
        
        # Use nan versions of numpy functions as some elements of 'phase' might be np.nan to indicate that it was flagged
        std_hold = []

        # Loop over the data in range rounded to size of time step
        for i in range(len(phase) - over):
            std_hold.append(
                np.nanstd(np.array(phase[i:i+over]))
            )

        std_mean = np.nanmean(std_hold)

        return std_mean

    @staticmethod
    def ave_phase(phase: np.ndarray, diffTime: np.ndarray, over: float=1.0) -> np.ndarray:
        """
        Do an averaging/smoopthing on the phase data
        The default for ALMA for phase statistics should be 10s.
        For default 6s integration times this will average 2 values.
        If input phases from the gain table have integration(diffTime) > 10s
        then no averaging is made.

        :param phase: phase series of the data to average
        :type phase: array
        :param diffTime: the average difference in time between each data value, i.e. each phase
        :type diffTime: float
        :param over: the time to average over - default is 1s
        :type over: float
        :returns: array of averaged phases
        :rtype: array
        """
        over = int(np.round(over / diffTime))
        # Make an int for using elements
        # There will be slight but minimal inaccuracies due to long timegaps in the data
        if over > 1.0:
            mean_hold = []
            for i in range(len(phase) - over):
                # Average/smooth the data and ignore the nan values
                mean_hold.append(
                    np.mean(np.array(phase[i:i+over])[~np.isnan(phase[i:i+over])])
                    )
        else:
            mean_hold = phase
            
        return mean_hold
