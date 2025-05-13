########################################################################
# almarenorm.py
#
#  Script to calculate and optionally apply spectrally-resolved
#  normalization correction for ALMA in the strong-line case.  This is
#  needed when Tsys measurements inadequately recover these lines
#  either because Tsys is measured off-source (12m array and ACA)
#  and/or at insufficient spectral resolution (12m array only).
#
# Usage (from CASA 5.x or CASA 6.1.1-15):
#
#  (assumes this python file exists in the current working directory) 
#  
#  sys.path.append(os.getcwd())  
#  from almarenorm import *    # import 
#  RN=ACreNorm(<msname>)       # create ACreNorm tool 
#
### To examine Tsys spectra for spurious lines - BY EYE:
#
#  RN.plotRelTsysSpectra(fthresh=0.01) - Luke's default 0.01 (1%) 
#                                        old default 0.002 is a very 
#                                        low trigger theshold 0.2% level
#
#  this will also write a Tsys template file but it should always be checked
#  as TDM (12m) Tsys are hard to fit and sometime diverage at edges suggesting
#  to incorrectly flag a broad range of Tsys channel 
#
#
### To evaluate the need for the correction on all FDM science spws 
#   and scans (excluding spws as specified), but making NO change in
#   MS:
#
#  RN.renormalize(docorr=False)
#
# (The renormalize function will nominally auto-detect all FDM and
#  science scans to evaluate - without extra options runs default paramaters)
#
### To make plots (if hardcopy=True, unique names constructed from <msname>
#   will be generated for each plot):
#
#  RN.plotSpectra(hardcopy=True)     # Scan-averaged Renorm spectra
#  RN.plotSpwStats(hardcopy=True)    # Scan/Ant-averaged peak stats
#  RN.plotScanStats(hardcopy=True)   # Scan-dep peak stats
#
## To apply the correction to fully-calibrated data (this scales the
#  CORRECTED_DATA by default, applies the renormalization correction and writes
#  it back (plot* commands can also be run after this):
#
#  RN.renormalize(docorr=True)
#  
## Close the ACreNorm tool (detaches from MS, etc.)
#
#  RN.close()
#
########################################################################
#
#  Notes for future development - from some discussion with PLWG 
#
#
# - PLWG might want some information to store in MS keywords
#
#
# - the plotSpwStats and plotScanStats coded by George I did not use 
#   very much - the way these stats are currently built is that
#   they only pass the last field of a scan, i.e. the values are 
#   stored in the RN.rnstat dictionary which never had 'field' 
#   coded in. plotSpectra I have modified to do the correct 
#   cumulative average and takes all fields - even this could be 
#   split out into a per field basis
#
#
#########################################################################

import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

import os
from math import *
from typing import List, Union, Dict
from datetime import datetime
import numpy as np
import matplotlib.pyplot as plt
from scipy import signal
import copy

try:
    from taskinit import tbtool,msmdtool,qatool,attool, mstool, casalog, metool
except:
    from casatools import table as tbtool
    from casatools import msmetadata as msmdtool
    from casatools import quanta as qatool
    from casatools import atmosphere as attool
    from casatools import ms as mstool
    from casatasks import casalog 
    from casatools import measures as metool

mytb=tbtool()
myms=mstool()


# Interface function for use by ALMA Pipeline renormalization task (PIPE-1687).
def alma_renorm(vis: str, spw: List[int], apply: bool, threshold: Union[None, float], excludechan: Dict,
                correct_atm: bool, atm_auto_exclude: bool, bwthreshspw: Dict) -> tuple:   ## PIPE-1469 for bwthreshspw
    """
    Interface function for ALMA Pipeline: this runs the ALMA renormalization
    heuristic for input vis, and returns statistics and metadata required by
    the Pipeline renormalization task.

    Args:
        vis: path to the measurement set to renormalize.
        spw: Spectral Window IDs to process.
        apply: apply the correction or not.
        threshold: the threshold above which the scaling must exceed for the
            renormalization correction to be applied. If set to None, then use
            a pre-defined threshold appropriate for the ALMA Band.
        excludechan: dictionary with keys set to SpW IDs (as string), and
            values set to channels to exclude for that SpW.
        correct_atm: this will get the transmission profiles for the SPW
            bandpass (or phase) and target(s) because the BP (or phase) and
            target(s) autocorr are compared in establishing the scaling in
            cases spectra, there can be issues where transmission is low and
            the source elevations are tens of degrees different. Often the ATM
            is not divided out well and leads to incorrect scaling in ATM
            regions. This option acts to fix this discrepancy so ATM are
            handled correctly.
        atm_auto_exclude: if True, then spectral regions found during the
            "check for false positives" algorithm will be excluded from the
             spectrum.
        bwthreshspw: disctionary with keys set to SpW IDs (as string), 
            and values set to a bandwidth (in Hz) to use for dividing 
            up the SpWs, e.g. 120e6 

    Returns: 8-tuple containing:
        - boolean declaring whether data are all TDM
        - ATM exclusion command
        - ATM warning
        - boolean declaring whether the renormalization correction was found
          to have already been applied.
        - boolean declaring whether a corrected data column was found.
        - boolean declaring whether the renormalization correction was applied.
        - dictionary containing factors for Pipeline QA.
        - dictionary containing renormalization statistics indexed by source
          and SpW.
    """
    # Initialize the renormalization object for input vis.
    print(' Doing the initialize in ALMARENORM itself\n')
    rn = ACreNorm(vis)

    # Initialize return variables.
    stats = {}
    rnstats = {}
    atmWarning = {}
    atmExcludeCmd = {}
    renorm_applied = False

    # Store "all TDM" information before trying the renormalization so
    # that the weblog is rendered properly.
    alltdm = rn.tdm_only

    # Check if the correction has already been applied.
    corrApplied = rn.checkApply()

    # Check whether the dataset has a corrected data column.
    corrColExists = rn.correxists

    # Only attempt the correction if the data are not all TDM.
    if not alltdm:
        # Run the renormalisation.
        rn.renormalize(docorr=apply, docorrThresh=threshold, correctATM=correct_atm, spws=spw, excludechan=excludechan,
                       atmAutoExclude=atm_auto_exclude, bwthreshspw=bwthreshspw) # add bwthreshspw

        # Create spectra plots.
        rn.plotSpectra(includeSummary=False)

        # Check if the renormalisation has been applied. This flag will be
        # passed back to caller, for future export to the manifest and use
        # during restore.
        if apply and rn.checkApply():
            renorm_applied = True

        # Only populate the following variables used for QA and to populate the
        # weblog if this was run in a 'valid' way: on data which has not
        # already been corrected (not corrApplied) and on data which has a
        # corrected column. If apply=False, it doesn't matter if the corrected
        # column exists or if the data has already been corrected, because the
        # correction isn't actually done.
        if (corrColExists or not apply) and (not corrApplied or not apply):
            # Get stats (dictionary) indexed by source, spw
            stats = rn.rnpipestats
            # Get all factors for QA
            rnstats = rn.stats()
            # get information related to detecting false positives caused by
            # atmospheric features, also needed for QA.
            atmWarning = rn.atmWarning
            atmExcludeCmd = rn.atmExcludeCmd

    # Clean up.
    rn.close()

    return alltdm, atmExcludeCmd, atmWarning, corrApplied, corrColExists, renorm_applied, rnstats, stats  



class ACreNorm(object):


    def __init__(self,msname):

        # Version

        self.RNversion='v1.6.1-2023/08/09-PLWG'

        self.msname=msname
        casalog.post('Opening ms: '+str(self.msname))

        self.msmeta=msmdtool()
        self.msmeta.open(self.msname)

        mytb.open(self.msname)
        self.correxists=mytb.colnames().count('CORRECTED_DATA')>0

        self.wtspexists=False    
        # George's code was already commented - with forward look to figuing out weights....
        # mytb.colnames().count('WEIGHT_SPECTRUM')>0
        # Need to check it is initialized (contains values!)!
        #try:
        #    # Try to extract value from first row
        #    wtsp=mytb.getcol('WEIGHT_SPECTRUM',0,1,1)
        #except:
        #    self.wtspexists=False
        #    pass

        mytb.close()
        casalog.post('CORRECTED_DATA exists = '+str(self.correxists))

        self.AntName = self.msmeta.antennanames()
        self.nAnt = self.msmeta.nantennas()
        casalog.post('Found '+str(self.nAnt)+' antennas')

        self.AntOut={} # LM Added initiate for outlier antennas channels
        self.replacedCorr={} # LM added 
        # for tracking of outlier ants per spw, scan, field 
        # that were deemed to need more than 10 channels correcting
        self.birdiechan={} # LM added - actually is set but not assessed - was for testing 
        self.atmtrans= {}  # LM added initiated for atmopsheric transmission regions
                           # options in main code to exclude or "fit out"
        self.corrATM = False # global variable as if we set to True but then cannot find
                           # the PWV value (for whatever reason)
                           # correctATM is input in renormalize function

        # ALMA Bands     0       1     2    3    4     5     6     7     8    9    10
        self.usePWV =  [99.0, 5.186,5.186,2.748,2.748,1.796,1.262,0.913,0.658,0.472,0.472] 

        self.scalingValues={}  # for PL to read the levels at SPW, Scan, Field - max scaling value 

        self.TsysReturn={}  # LM added so Tsys flag channel are already passed and stored
        # useful for ALMA production 

        # ALMA Bands        0    1   2    3    4    5    6    7    8    9    10
        self.bandThresh=[99.0, 1.02,1.02,1.02,1.02,1.02,1.02,1.02,1.02,1.02,1.02] 
        # B9/B10 set high now to now correct
        # set a B0 as its way easier just to then index this array with Band

        self.docorrApply={} # - this is to record if the field in a given SPW is
        # above the threshold for application, in which case that field in that
        # SPW will always be corrected and a boolean can be stored in this dictionary
        # one caveat is what about boarderline data around the threshold
        # and some scans are above, and some are below? Do we add a buffer? - it would be wrong
        # to scale e.g. only half the scans for a field. Code is iterative, so doesn't
        # know about later scans in advanced..so must be based on first scan assessment of the field only
        # usually scalng per scan for same fields is roughtly similar/constant 
        
 
        self.states=[]

        self.nfit=5
        self.fthresh=0.001
        
        self.rnstats={}
        myms.open(self.msname)
        spwInfo = myms.getspectralwindowinfo()
        myms.close()

        self.fdmspws=self.msmeta.fdmspws()
        # Make sure there are FDM windows, if not, work around that.
        if len(self.fdmspws) != 0:
            self.tdm_only = False
            bandFreq = spwInfo[str(self.fdmspws[0])]['Chan1Freq']
            # Maybe a better way?
            self.num_corrs = self.msmeta.ncorrforpol(self.msmeta.polidfordatadesc(self.msmeta.datadescids(spw=self.fdmspws[0])[0]))
            # AC data will only use the parallel hands, so if 4 correlations are detected in the FDM
            # window, we set the full_pol flag to True and reset num_corrs to 2.
            if self.num_corrs == 4:
                self.full_pol = True
                self.num_corrs = 2
            else:
                self.full_pol = False
            #mytb.open(self.msname+'/SPECTRAL_WINDOW')
            # try to get the reference frequency directly but REF_FREQUENCY doesn't exist for
            # older data so in those cases we just take the mean of the spw. 
            #try: 
            #    bandFreq = mytb.getcol('REF_FREQUENCY')[self.fdmspws[0]]
            #except RuntimeError:
            #    bandFreq = np.mean(mytb.getcell('CHAN_FREQ',[self.fdmswps[0]]))
            #mytb.close()
        else:
            casalog.post('No FDM windows found! Renormalization unnecessary.')
            self.tdm_only = True
            bandFreq = spwInfo['0']['Chan1Freq']
            self.num_corrs = self.msmeta.ncorrforpol(self.msmeta.polidfordatadesc(self.msmeta.datadescids(spw=self.msmeta.tdmspws()[-1])[0]))
        
        self.Band = int(self.getband(bandFreq))

        # warnings that give nan slice back or empty mean
        warnings.filterwarnings(action='ignore', message='All-NaN slice encountered')
        warnings.filterwarnings(action='ignore', message='Mean of empty slice')

    def __del__(self):
        casalog.post('Closing msmd tool.')
        plt.close(11)
        plt.close(12)
        plt.close(13)
        plt.close(14)
        plt.close(15)
        self.msmeta.close()

    def close(self):
        self.rnstats=[]
        self.__del__()

    def chanfreqs(self,ispw):
        return self.msmeta.chanfreqs(ispw)
        
    def getBscan(self,spw,verbose=False):
        Bspw=[]
        Bscans=self.msmeta.scansforintent('*BANDPASS*')
        Spwscans=self.msmeta.scansforspw(spw)
        for iB in Bscans:
            mask=Spwscans==iB
            for iS in Spwscans[mask]:
                Bspw.append(iS)
        if verbose:
            casalog.post(" Bandpass scan(s): "+str(Bspw))

        return Bspw


    def getPhscan(self,spw,verbose=False):
        PHscan=[]
        Phscans=self.msmeta.scansforintent('*PHASE*')
        Spwscans=self.msmeta.scansforspw(spw)
        for iPh in Phscans:
            mask=Spwscans==iPh
            for iS in Spwscans[mask]:
                PHscan.append(iS)
        if verbose:
            casalog.post(" Phase calibrator scan(s): "+str(PHscan))

        return PHscan
        
    def getStateIdsForIntent(self,intent):
        if len(self.states)==0:
            mytb.open(self.msname+'/STATE')
            self.states=mytb.getcol('OBS_MODE')
            mytb.close()
        
        return list(np.array(range(len(self.states)))[np.transpose([intent in y for y in self.states])])


    def getBtsysscan(self,spw, verbose=True): 
        
        Bflds=list(self.msmeta.fieldsforintent('*BANDPASS*'))  # Bandpass field(s)
        Bscans=[]
        for iBfld in Bflds:   # handle multiple B calibrators (needed?)
            Bscans=Bscans+list(self.msmeta.scansforfield(iBfld))     # Scans on Bandpass field(s)
        TsysScans=list(self.msmeta.scansforintent('*ATM*'))  # all Tsys scans
        SpwScans=list(self.msmeta.scansforspw(spw)) # Scans in specified spw
        
        # [scan in [scans on Bfld among Tsys scans] among spw scans]
        BTsysScans=[iscan for iscan in [iscan1 for iscan1 in TsysScans if iscan1 in Bscans] if iscan in SpwScans]

        if len(BTsysScans)<1:
            casalog.post('Could not find Tsys scan on B calibrator in spw='+str(spw))
            return None  
        
        if verbose:
            casalog.post('Tsys scan(s) for bandpass calibrator(s) {0} in spw={1} are: {2}'.format(Bflds, spw, BTsysScans))

        # already a list
        return BTsysScans

    def getACdata(self,scan,spw,field,rowave=False,stateid=[]):
        sortlist=''
        if rowave:
            sortlist='ANTENNA1'

        mytb.open(self.msname)
        ddid=str(list(self.msmeta.datadescids(spw)))
        if field is not None:  # i.e. science data
            quer='SCAN_NUMBER IN ['+str(scan)+'] && DATA_DESC_ID IN '+ddid+' && ANTENNA1==ANTENNA2 && FIELD_ID=='+str(field) 
        else:
            # because the getTsysSpectra passes scans but wont pass the field or Bandpass doesn't pass field actually
            quer='SCAN_NUMBER IN ['+str(scan)+'] && DATA_DESC_ID IN '+ddid+' && ANTENNA1==ANTENNA2' 

        if len(stateid)>0:
            quer=quer+'&& STATE_ID IN '+str(stateid)
        st=mytb.query(quer,sortlist=sortlist)

        d=st.getcol('DATA').real
        # If we want to incorporate the XML flags then need to grab them
        #f=st.getcol('FLAG')

        # need a check here for failed data read - happens for aborted data (e.g. in Mosaics ALMA-IMF)
        # aborted means in a given scan the 'last' fiels is not necessarily recorded
        # simply

        if len(d)>0:
            if d.shape[0] == 4:
                d=d[0::(d.shape[0]-1),:,:]   #  parallel hands only for full pol data
            a1=st.getcol('ANTENNA1')  ## WARNING will CASA 6.2 obey row ordering if getcol ANT is not called with DATA ??
            st.close()
            mytb.close()

            dsh=d.shape
            if rowave and dsh[2]%self.nAnt==0:
                dsh2=(dsh[0],dsh[1],self.nAnt,int(dsh[2]/self.nAnt)) 
                d=np.mean(d.reshape(dsh2),3)
                a1=np.sum(a1.reshape(dsh2[2:]),1)//dsh2[3]

                ## Some details for how to incorporate flagging if that was wanted in the future:
                # if (f == True).any(): #if there are no flags then just do as above
                # d = d.reshape(dsh2)
                # f = f.reshape(dsh2)
                # d = np.ma.array(d, mask=f, fill_value=np.nan)
                # d = np.nanmean(d,3)
                # 
                # Note that if this is run after the PL flagging (stage 2) then EVERYTHING is
                # flagged because the PL flags autocorrs. 
        else:
            d = None # new return which is analysed in the renorm code now 
            mytb.close()
        return d

    def getXCdata(self,scan,spw,field,datacolumn='CORRECTED_DATA'):
        casalog.post('  Extracting CROSS-correlation data from spw='+str(spw)+' and scan='+str(scan)+' and field='+str(field))

        mytb.open(self.msname)

        if mytb.colnames().count(datacolumn)==0:
            mytb.close()
            casalog.post('ERROR: '+str(datacolumn)+' does NOT exist!')
            raise RuntimeError(str(datacolumn)+' does not exist.')

        ddid=str(list(self.msmeta.datadescids(spw)))
        st=mytb.query('SCAN_NUMBER IN ['+str(scan)+'] && DATA_DESC_ID IN '+ddid+' && ANTENNA1!=ANTENNA2 && FIELD_ID =='+str(field))
        cd=st.getcol(datacolumn)  ## WARNING CASA6.2 might not obey row order 
        a1=st.getcol('ANTENNA1')
        a2=st.getcol('ANTENNA2')
        st.close()
        mytb.close()

        return (a1,a2,cd)

    # Mimics part of getXCdata, but gets the FLAG column
    def getXCflags(self,scan,spw,field,datacolumn='FLAG', verbose=False):
        if verbose:
            casalog.post('  Extracting CROSS-correlation flags from spw='+str(spw)+' and scan='+str(scan)+' and field='+str(field))
            
        mytb.open(self.msname)

        if mytb.colnames().count(datacolumn)==0:
            mytb.close()
            casalog.post('ERROR: '+str(datacolumn)+' does NOT exist!')
            raise RuntimeError(str(datacolumn)+' does not exist.')

        ddid=str(list(self.msmeta.datadescids(spw)))
        st=mytb.query('SCAN_NUMBER IN ['+str(scan)+'] && DATA_DESC_ID IN '+ddid+' && ANTENNA1!=ANTENNA2 && FIELD_ID =='+str(field))
        cd=st.getcol(datacolumn) 
        a1=st.getcol('ANTENNA1')
        a2=st.getcol('ANTENNA2')
        st.close()
        mytb.close()

        # idea is actually to return a list of fully flagged antennas

        # each row in the XC flags is a unique antenna pair
        # loop the rows and make a list of those that are NOT fully flagged, then use exclusion 
        # comparing to the list of all antennas

        gdants=[]
        for irow in range(cd.shape[2]):
            if np.sum(cd[:,:,irow]) != cd.shape[0]*cd.shape[1]: # i.e here we are summing all corrs and the spectral axis
                gdants.append(a1[irow])
                gdants.append(a2[irow])

        # now make an exclusion cause
        aout = [iant for iant in range(self.nAnt) if iant not in gdants]

        # probably this could be coded better - instead of the loop??? - but is relatively fast as robust 

        return (aout)

    def putXCdata(self,scan,spw,field,cd,datacolumn='CORRECTED_DATA'):
        casalog.post('  Writing CROSS-correlation data to spw='+str(spw)+' and scan='+str(scan)+' and field='+str(field))

        mytb.open(self.msname,nomodify=False)
        ddid=str(list(self.msmeta.datadescids(spw)))
        st=mytb.query('SCAN_NUMBER IN ['+str(scan)+'] && DATA_DESC_ID IN '+ddid+' && ANTENNA1!=ANTENNA2 && FIELD_ID =='+str(field))
        d=st.putcol(datacolumn,cd)
        st.close()
        mytb.close()


    def getTsysSpectra(self,scan,spw):

        # get the sky
        atmoff=self.getACdata(scan,spw,None,True,self.getStateIdsForIntent('ATMOSPHERE#OFF'))

        # get hot
        atmhot=self.getACdata(scan,spw,None,True,self.getStateIdsForIntent('ATMOSPHERE#HOT'))

        # divide sky by hot (removes bandpass, etc.)
        Tatm=atmoff/atmhot

        # norm on Chan axis:
        Tatm=self.normChanAxis(Tatm)

        return Tatm


    def normChanAxis(self,A):
        # this is for the Tsys checking part/plotting 
        (n0,n1,n2)=A.shape

        # Normalize each spectrum by median
        for i2 in range(n2):
            for i0 in range(n0):
                A[i0,:,i2]/=np.median(A[i0,:,i2])
                
        return A


    def xyplots(self,N):
        nY=int(floor(sqrt(N)+0.5))
        nX=int(ceil(float(N)/nY))
        return nX,nY

        
    def plotSpws(self,hardcopy=True):  # unchanged George's code (expect figure output name nad hardcopy=True -- I've not really used (L.Maud)

        casalog.post('Discerning spw intents...')

        specspws=list(self.msmeta.almaspws(tdm=True,fdm=True))

        scispws=list(self.msmeta.spwsforintent('*TARGET*'))
        scispecspws=[ispw for ispw in scispws if ispw in specspws ]
        casalog.post('Found resolved Science spws: '+str(scispecspws))

        tsysspws=list(self.msmeta.spwsforintent('*ATM*'))
        tsysspecspws=[ispw for ispw in tsysspws if ispw in specspws ]
        casalog.post('Found resolved Tsys spws = '+str(tsysspecspws))
        
        nSpw=len(tsysspecspws)
        plt.ioff()
        pfig=plt.figure(15,figsize=(14,9))
        plt.ioff()
        pfig.clf()

        for ispw in scispecspws:
            f=self.msmeta.chanfreqs(ispw,'GHz')
            plt.plot(f,len(f)*[ispw+0.01],'r-',lw=3)
            plt.text(max(f),ispw+0.01,str(ispw),fontsize=10)

        for ispw in tsysspecspws:
            f=self.msmeta.chanfreqs(ispw,'GHz')
            plt.plot(f,np.array(len(f)*[ispw])-0.1,'b-',lw=3)
            plt.text(min(f),ispw-0.1,str(ispw)+'-Tsys',ha='right',va='top',fontsize=10)
            #plt.text(min(f),ispw,'Tsys',ha='right',va='center',fontsize=10)


        flo,fhi,spwlo,spwhi=plt.axis()

        tdmspws=self.msmeta.almaspws(tdm=True)
        for ispw in tsysspecspws:
            if ispw in tdmspws:
                f=self.msmeta.chanfreqs(ispw,'GHz')
                plt.plot([f[0]]*2,[spwlo,spwhi],'b:')
                plt.plot([f[-1]]*2,[spwlo,spwhi],'b:')

        plt.xlabel('Frequency (GHz)')
        plt.ylabel('Spw Id')
        plt.title(self.msname,{'horizontalalignment': 'left', 'fontsize': 'medium','verticalalignment': 'bottom'})
        plt.ticklabel_format(style='plain', useOffset=False)
        if hardcopy:
            if not os.path.exists('RN_plots'):
                os.mkdir('RN_plots')
            fname=self.msname+'_ReNormSpwVsFreq.png'
            casalog.post('Saving hardcopy plot: '+fname)
            plt.savefig('./RN_plots/'+fname)
            plt.close()
        else:
            plt.show()

                   
    def plotRelTsysSpectra(self,spws=[],scans=[],normByB=True,fthresh=0.01,bwthresh=64e6,bwdiv=None,nfit=5,edge=0.025,hardcopy=True,retflchan=False, plotATM=True, verbose=False):
        # LM edited:
        #         made log message 
        #         CASA 6 fix
        #         default alarm/trigger changed to 0.01, i.e. 1% trigger not 0.002 (0.2%) as before
        #         output channels are saved as class value - for later tsys template 
        #         verbose option
        #         plotATM - will also show ATM transmission profile on the Tsys spectra to help DRs
        #         REMOVED fitTDM  - tired to fit TDM spws in chunks, but this diverges at breaks and just is a mess - don't use
    

        # LM Note - the bwthresh, bwdiv etc all work similarly to those for the main renorm code, but are not shared
        #           so the default here is usually kept are per George's setup
        #           this is probably ok, as anyway this is used only to identify possible channels
        #           that need flagging. IF we are to rely on this code later and automate the channels to 
        #           flag in the Tsys, we need very careful testing as the fitting can really diverge at the edges and
        #           causes upswings in the returned profile at the edges of TDM SPWs which are then triggering flags

        usefthresh=self.fthresh
        if fthresh>0.0:
            usefthresh=fthresh

        casalog.post('')
        casalog.post('Using fractional alarm threshold for Tsys Spectra='+str(usefthresh))

        self.nfit=nfit

        if type(spws)!=list:
            casalog.post('Please specify spws as a list.')
            raise TypeError('input parameter "spws" must be a list')

        if type(scans)!=list:
            casalog.post('Please specify scans as a list.')
            raise TypeError('input parameter "scans" must be a list')

        # the spws to process (Tsys spws)
        tsysspecspws=[]
        if len(spws)==0:
            specspws=list(self.msmeta.almaspws(tdm=True,fdm=True))
            tsysspws=list(self.msmeta.spwsforintent('*ATM*'))
            tsysspecspws=[ispw for ispw in tsysspws if ispw in specspws ]
            #spws=list(self.msmeta.almaspws(fdm=True))
            casalog.post('Found Resolved Tsys spws = '+str(tsysspecspws))
        
        else:
            tsysspecspws=spws
            casalog.post('User supplied Tsys spws = '+str(tsysspecspws))

        # global list of target scans
        targtsysscans=[]
        if len(scans)==0:
            tsysscans=list(self.msmeta.scansforintent('*ATM*'))
            bandpassfields=list(self.msmeta.fieldsforintent('*BAND*'))
            fldforscans=self.msmeta.fieldsforscans(tsysscans,False,0,0,asmap=True)
            targtsysscans = [iscan for iscan in tsysscans if fldforscans[str(iscan)][0] not in bandpassfields ]
            #targscans=list(self.msmeta.scansforintent('*TARGET*'))
            casalog.post('Found science Tsys scans = '+str(targtsysscans))

        else:
            targtsysscans=scans
            casalog.post('User supplied Tsys scans = '+str(targtsysscans))

        nSpw=len(tsysspecspws)

        nXspw,nYspw = self.xyplots(nSpw)

        plt.ioff()
        pfig=plt.figure(14,figsize=(14,9))
        plt.ioff()
        pfig.clf()

        cols=['b','g','r','c','m']
        ncol=len(cols)
        k=1

        flch={}

        for ispw in tsysspecspws:

            if normByB:
                # discern the B TSYS scan
                Bscan=self.getBtsysscan(ispw,verbose=verbose)
                if Bscan is None:
                    return
                # and get corr- and ant-dep B, time-averaged 
                Btsys=self.getTsysSpectra(scan=Bscan,spw=ispw)
        
            # Calculate channel chunks                
            (nseg,dNchan) = self.calcChanRanges(ispw,bwthresh,bwdiv,verbose=True) # not an option for Tsys (yet - April 2)

            # George's code for chan range - comes later in plots and Tsys edges are masked too 
            nCha=self.msmeta.nchan(ispw)    # Btsys.shape[1]
            chlo=int(ceil(edge*nCha))
            chhi=nCha-1-chlo
            chans=range(chlo,nCha-chlo)

            if plotATM:
                if type(Bscan) is list:
                    Bscanatm=Bscan[0]
                else:
                    Bscanatm=Bscan
                ATMprof = self.ATMtrans(Bscanatm, ispw, verbose=verbose)
                # mask edges same as the Tsys below,i.e. TDM coes from 128 to 120 channels
                ATMprof=ATMprof[chlo:(nCha-chlo)]

            plt.subplot(nYspw,nXspw,k)
            plt.ioff()
            plt.ticklabel_format(style='plain', useOffset=False)
            if (k-1)%nXspw==0:
                plt.ylabel('Relative Tsys')

            if k>(nSpw-nXspw):
                plt.xlabel('Channel')

            if k==1:
                plt.title(self.msname+' <Nant='+str(self.nAnt)+'> Nscan='+str(len(targtsysscans)),{'horizontalalignment': 'left', 'fontsize': 'medium','verticalalignment': 'bottom'})
            k+=1

            c=-1

            for iscan in targtsysscans:
                c+=1  # new color each scan

                # this scan's Tsys
                Tsys=self.getTsysSpectra(iscan,ispw)

                # Divid by Btsys to remove atm, mostly
                if normByB:
                    Tsys/=Btsys
                
                # normalize on channel axis
                Tsys=self.normChanAxis(Tsys)
                
                # average over corrs, antennas
                Tsys=np.mean(Tsys,(0,self.num_corrs))

                # mask edges
                Tsys=Tsys[chlo:(nCha-chlo)]

                # remove broad non-linearity (usually atm) -- this does not trigger for TDM at present  
                if self.nfit>0:
                    
                    for iseg in range(nseg):
                        # Tsys was masked so rubbish edges are not fitted 
                        lochan=max(iseg*dNchan-chlo,0)
                        hichan=min((iseg+1)*dNchan-chlo,len(Tsys))
                        if lochan<hichan:
                            self.calcReNorm1(Tsys[lochan:hichan],False)

                #for iant in range(Tsys.shape[1]):
                #    plt.plot(Tsys[:,iant])

                plt.plot(chans,Tsys,cols[c%ncol]+'-')


                TsysMax=Tsys.max()
                TsysMedDev=np.median(np.absolute(Tsys-1.0))
                TsysSfrac=np.mean(Tsys)-1.0
                alarm='   '
                flchans=[]
                flchanstr=''
                if TsysMax>=(1.0+usefthresh):
                    flchans=np.array(np.where(Tsys>(1.0+usefthresh))[0])
                    nflchan=len(flchans)
                    if nflchan>0:
                        flchans+=chlo
                        spwkey='spw='+str(ispw)
                        #if not flch.has_key(spwkey):  ## HAS_KEY IS NOT IN CASA6 - PYTHON3
                        #    flch[spwkey]={}

                        if not spwkey in flch.keys():
                            flch[spwkey]={}

                        scankey='scan='+str(iscan)
                        flch[spwkey][scankey]=flchans
                        if verbose:
                            flchanstr=' Found '+str(nflchan)+' channels in SPW '+str(ispw)+' execeeding fractional threshold ('+str(usefthresh)+')' 
                        plt.plot(flchans,[0.999]*nflchan,'ks',markersize=4)
                    alarm=' ***'
                    plt.plot([3*nCha/8,5*nCha/8],[TsysMax]*2,cols[c%ncol]+'-')
                    note1='Peak='+str(floor(TsysMax*10000.0)/10000.0)
                    plt.text(3*nCha/8,TsysMax,note1,ha='right',va='center',color=cols[c%ncol],size='x-small')
                    note2='Intg~'+str(floor(TsysSfrac*10000.0)/10000.0)
                    plt.text(5*nCha/8,TsysMax,note2,ha='left',va='center',color=cols[c%ncol],size='x-small')
                

    
                pstr=" Science Tsys(spw={0:2d},scan={1:3d}): PEAK Frac Line Contrib={2:.4f}{3}  INTEGRATED Frac Line Contrib={4:.4f}"
                if verbose:
                    casalog.post(pstr.format(ispw,iscan,TsysMax,alarm,TsysSfrac))

                if len(flchanstr)>0:
                    casalog.post(flchanstr)
                    
            lims=list(plt.axis())
            lims[0]=chlo-1
            lims[1]=nCha-chlo
            lims[2]=min(0.9985,lims[2])
            lims[3]=max(1.15*lims[3]-0.15*lims[2],1.02)
            plt.axis(lims)

            dy=lims[2]*0.1+lims[3]*0.9
            plt.text(nCha/2,dy,'Spw='+str(ispw),ha='center')

            # want second axis for ATM line 
            # this is to aid DR in understading there is not a problem here
            # similar to pipeline Tsys plots with transmission shown
            # but keep simple with 0 to 100% shown only
            # and plot only once (while looping Target fields) - BP ATM profile is show
            # as the way Geogre coded the plots we can just called plt.twin and 
            # thus 'shifts' to only registering about the new ATM 'axis'
            if plotATM:
                plt.twinx()
                plt.plot(chans,100.*ATMprof,c='m',linestyle='-',linewidth=2)
                plt.ylim(0,100)
                if (k-1)%nXspw==0: 
                    plt.ylabel('ATM transmission (%)')
        
        if nXspw == 3:
            plt.subplots_adjust(wspace=0.35)
        else:
            plt.subplots_adjust(wspace=0.3)

        if hardcopy:
            if not os.path.exists('RN_plots'):
                os.mkdir('RN_plots')
            fname=self.msname+'_RelTsysSpectra.png'
            casalog.post('Saving hardcopy plot: '+fname)
            plt.savefig('./RN_plots/'+fname)
            plt.close()
        else:
            plt.show()

        # keep the channels in a dictionary 
        self.TsysReturn = flch

        if retflchan:
            return flch



    # LM added / edited lots
    def renormalize(self, spws=[], excludespws=[], targscans=[], 
            excludeants=[], excludechan={}, excflagged=True, fixOutliers=True, mededge=0.01,  
            atmAutoExclude=True, checkFalsePositives=True, correctATM=False, limATM=0.85, plotATM=True, 
            bwdiv='odd', bwthresh=120e6, bwthreshspw={}, checkLineForest=True, nfit=5, useDynamicSegments=True,
            datacolumn='CORRECTED_DATA', docorr=False, docorrThresh=None, usePhaseAC=False,
            antHeuristicsSpectra=True, diagSpectra=True, fthresh=0.01, 
            verbose=False):
        """
        Purpose:
            This is the main function of the ACreNorm class. It takes the 
            autocorrelations and evaluates them to see if a rescaling of the
            crosscorrelation data is necessary. It does this by looping over
            the science targets on a per spw, per scan, per field, per antenna,
            per correlation basis. It takes the autocorrelations of the target
            and divides them by a calibrator's autocorrelations (by default the
            bandpass) and any peaks above 2% in the resultant spectrum are 
            caused by actual detections of the science target and therefore 
            need to be corrected in the crosscorrelation data. 

        Inputs:
            This function has many options for evaluating the autocorrelations
            and applying solutions if necessary but no inputs are necessary
            for operation. Some options are highly experimental and not 
            recommended for blind usage, they have been noted below.

            --------------
            Data Selection
            --------------

            excludespws : list of integers
                If not empty, any spw specified will be removed from analysis
                if that spw is one of the science spws. 
                Example: excludespws = [21]
                Default: excludespws = []

             spws : list of integers
                If not empty, only perform renormalization on the spws that
                are specified. Note that these must be science spws or they
                will be ignored.
                Example: spws = [21,23]
                Default: spws = []

            targscans : list of integers
                If not empty, only perform renormalization on the target scans
                that are specified. Note that these scans must contain science
                data or they will be ignored. 
                Example: targscans = [18,21,24,27]
                Default: targscans = []

            --------------
            Data Reduction
            --------------

            excludeants : list of integers
                Specified antennas will be ignored in all analysis and 
                application. This is intended to be used if any antenna are 
                problematic in the interferometric data or need manually 
                flagging. This is normally done via the excflagged option but 
                if the data were just imported then flags are not available 
                and this can be used instead. Excluded antennas have their
                scaling spectra set to 1.0.
                Example: excludeants = [10,32]
                Default: excludeants = []

            excludechan : dictionary 
                If problematic data is found across a series of channels (often
                from atmospheric features) then the spw and channel ranges can
                be specified in either channel or frequency space (TOPO, GHz).
                Excluded channels have their scaling spectra set to 1.0.
                Example: excludechan={'22':'100~150;800~850', '24':'100~200'}
                         excludechan={'22':'230.1GHz~230.2GHz'}
                Default: excludechan={}

            excflagged : boolean
                Excluded flagged antenna by reading the crosscorrelation flag
                tables per spw, per scan, and per field. Antennas are excluded
                if fully flagged. Only useful after the data are fully 
                calibrated. Fully flagged antenna will have thier scaling 
                spectra set to 1.0.
                Default: excflagged = True

            fixOutliers : boolean
                Check antennas individually per scan, field, and correlation
                for outlier data compared to the median spectrum across all 
                antennas for that correlation on a channel-by-channel basis. 
                Outlier data is set to the median spectrum value of the 
                corresponding correlation.
                Default: fixOutliers = True

            mededge : float
                Percentage of edge channels to set to the median spectrum 
                value to prevent edge effects from influencing results.
                Default: mededge = 0.01     (i.e. 1%)

            ---------------------
            Atmosphere Mitigation
            ---------------------

            atmAutoExclude : boolean
                This option works in conjunction with the checkFalsePositives
                option to identify problematic sections of the spectrum where
                the atmosphere may be causing issues. If set to True, this will
                automatically exclude those sections of the spectrum where 
                there are atmospheric features using the same method as the
                excludechan option.
                Default: atmAutoExclude = True
            
            checkFalsePositives : boolean
                This will automatically find atmospheric lines in the spectrum 
                and check those regions to see if there is a renorm signal 
                above the threshold. If there is, it is assumed to be a "false 
                positive" signal caused by the ATM feature and will trigger a
                warning as well as a suggested region to exclude from analysis.
                Default: checkFalsePositives = True

            correctATM : boolean : EXPERIMENTAL - NOT RECOMMENDED
                This will get the transmission profiles for the bandpass (or
                phase) and target(s) and create a ratio of those profiles and
                apply it to the scaling spectrum. Becuase the bandpass (or
                phase) and target(s) autocorrelations are compared in 
                establishing the scaling spectra, there can be issues where 
                transmission is low and/or the source elevations are different 
                by a significant amount resulting in very different atmospheric 
                effects. This option acts to fix this discrpancy so that 
                atmospheric features are handled correction. 
                NOTE: Although a good idea, in practice it appears to have 
                little effect and sometimes does more harm.
                Default: correctATM = False

            limATM : float
                Set the transmission limit for atmospheric features so that
                only when features are below the set limit are they mitigated.
                This helps with computation time as there are diminishing 
                returns on dealing with minor atmospheric features.
                Default: limATM = 0.85

            plotATM : boolean
                If set to True, plot the atmospheric transmission curve on a
                secondary y-axis.
                Default: plotATM = True

            ----------
            Baselining
            ----------

            bwdiv : None, 'odd', int
                If a spw needs to be broken up into chunks (see bwthresh), this
                option determines the behavior of the division process. The 
                input here will be applied to all spws. There are three input
                options available: 
                    1. None - This will use powers of 2 based on taking the 
                        full spw bandwidth and dividing by bwthresh option.
                    2. 'odd' - This follows the process of (1) but attempts to
                        force an odd number of segments of equal size.
                    3. int - This will divide the spw bandwidth by the input 
                        integer to create segments of equal size. 

                NOTE: For options (2) and (3) there is a possiblity that the
                total number of channels does not evenly divide into an 
                integer number of channels. For example, 2048 channels divided
                by 3 segments is 682 channels per segment with a remainder of 2
                channels. In these cases, the mededge option is considered as
                this will flag a number of edge channels anyway. If the number
                of channels in the remainder is smaller than the flagged region
                then the solution is kept, otherwise the solution defaults to
                option (1). 

                NOTE: The total number of segments is capped at 7 which will
                override any input here.

                Examples: bwdiv = None
                          bwdiv = 'odd'
                          bwdiv = 4
                Default: bwdiv = 'odd'


            bwthresh : float
                The maximum bandwidth (in Hz) to fit a baseline to, beyond this
                limit the spw will be broken into chunks to fit separately.
                NOTE: The maximum number of segments is capped at 7. This will
                override this setting for wide spws. 
                Default: bwthresh = 120e6

            bwthreshspw : dictionary
                This allows the user to input specific settings of the bwthresh
                option on a per spw basis, spws not specifed here will default 
                to the bwthresh option setting.
                Example: bwthreshspw = {'22':'250e6', '24':'500e6'}
                Default: {}

            checkLineForest : boolean
                Each segment goes through a process of masking to find and 
                ignore potential lines. Atmospheric features are also ignored. 
                After this masking, this option will enable to a check to make
                sure that >50% of the remaining segment bandwidth is available
                for fitting a baseline to. If not, then a warning is issued and
                if useDynamicSegments is also set to True, then the segment 
                size will be increased to include more bandwidth.
                NOTE: This only works if useDynamicSegments is also True.
                Default: True

            nfit : int
                This is the order of the polynomial that is used to fit the
                scaling spectrum in order to flatten or baseline it.
                Default: nfit = 5

            useDynamicSegments : boolean
                If set to True, then after fitting the scaling spectrum, check
                for issues at the segment boundaries by looking for values 
                >1.05 in the resultant scaling spectrum that are within 10 
                channels or 10 MHz (whichever is larger). If any values are 
                found, then decrease the number of segments by 2 and try again.
                This process is repeated until a good solution is found or
                only 1 segment remains.
                Default: True

            ---------------
            Data Correction
            ---------------

            datacolumn : string
                Specify the data column of the MS to apply the corrections to.
                If this is run post-calibration, then the CORRECTED_DATA column
                will exist and will need to be corrected by the scaling 
                spectrum calculated here. This is preferred as the code will be
                able to take advantage of any flagging performed during 
                calibration (if the excflagged option is set to True). 
                Alternatively, if this is run pre-calibration, then the DATA
                column of the MS can be selected, this however will modify the
                raw data directly so it is not recommended. 
                Default: datacolumn = 'CORRECTED_DATA'

            docorr : boolean
                Specify whether or not to apply the calculated scaling spectrum
                to the MS. If a correction is applied, a history note is 
                written into the MS indicating application, this is checked for
                before application to prevent double application. 
                Default: docorr=False

            docorrThresh : None or float
                The threshold above which to apply the scaling spectrum to the 
                data. It has been determined by ALMA that datasets that do not
                have any residuals in their scaling spectra above 2% do not 
                need to be corrected. As such, the default is set to 1.02 
                across all Bands.
                Example: docorrThresh = 1.01
                Default: docorrThresh = None   (this results in 1.02 being set)

            usePhaseAC : boolean : EXPERIMENTAL - USE WITH CAUTION
                The bandpass calibrator is used by default to create the 
                scaling spectrum as it, by definition, should have no spectral
                features. However, because it can be very far away in elevation
                it can cause significant residuals due to atmospheric effects.
                In those cases, it may be useful to use the Phase calibrator
                instead as it must be within 3 degrees of the target. Note 
                however that the phase calibrator is not guaranteed to be free
                of line emission or be free of wide-spread emission from the
                target itself for cases like Orion where CO is pervasive. 
                If this option is set to True, then the phase calibrator will
                be used to create the scaling spectra instead of the bandpass.
                Default: False

            -------------
            Data Analysis
            -------------

            antHeuristicsSpectra : boolean
                Create additional heuristics plots per antenna for diagnosing
                and fixing bad autocorrelation data as described by the
                fixOutliers option.
                Default: True

            diagSpectra : boolean
                Create additional diagnostic spectra for each spw, scan, and 
                field with both X and Y correlations and all antennas on the 
                same plot along with the median spectra per correlation. These
                plots show the actual correction that is applied per antenna.
                Defualt: True

            fthresh : float
                This is the threshold above which to show an alarm trigger in
                print statements and logs as "***". Stands for "fractional
                alarm" threshold.
                Default: 0.01   (i.e. 1%)

            -----
            Misc.
            -----

            verbose : boolean
                Print additional messages to the terminal that normally only go
                into the log file along with some "debug" type statements.
                Default: False
        
        Outputs:
            Varies by the options selected above but at minimum the scaling 
            spectra are calculated and optionally applied directly to the data.
        """
        # Starting CASA logger message
        casalog.post('*** ALMA almarenorm.py ***', 'INFO', 'ReNormalize')   
        casalog.post('*** '+str(self.RNversion)+' ***', 'INFO', 'ReNormalize')   
        casalog.post('*** Beginning renormalization run ***', 'INFO', 'ReNormalize')   

        casalog.post('')
        casalog.post('*** ALMA almarenorm.py renormalize '+str(self.RNversion)+' ***')

        usefthresh=self.fthresh
        if fthresh>0.0:
            usefthresh=fthresh

        casalog.post('')
        casalog.post('Using fractional alarm indication threshold for ReNorm = '+str(usefthresh*100)+'%')

        if correctATM:
            self.corrATM = True
            casalog.post('Will account for any ATM lines within the SPWs')

        # Handle correction request
        if docorr:
            if datacolumn=='CORRECTED_DATA' and not self.correxists:
                casalog.post('Correction of CORRECTED_DATA requested, but column does not exist! Cannot procede.')
                casalog.post('*** Terminating renormalization run ***', 'INFO', 'ReNormalize')   
                raise RuntimeError('Correction of CORRECTED_DATA requested but column does not exist.')

            casalog.post('The '+str(datacolumn)+' column will be corrected!')

            # Check of the MS history as the renorm code now writes in that application was made
            alreadyApp = self.checkApply()
            if alreadyApp:
                casalog.post('')
                casalog.post('Correction requested, but these data have already been ReNormalized! Cannot procede')
                casalog.post('                      set docorr=False for plots only')
                casalog.post('')
                casalog.post('*** Terminating renormalization run ***', 'INFO', 'ReNormalize')   
                raise Exception('Correction requested but these data have already been renormalized.')

        else:
            casalog.post('No corrections will be applied (docorr=False)!')

        # Check if docorrThresh is set correctly
        if docorrThresh is not None:
            if type(docorrThresh) is not float:
                casalog.post('Correction of CORRECTED_DATA requested, but docorrThresh is set incorrectly! Cannot procede.')
                casalog.post(' set to None for automatic thresholding during apply, or input a float to use') 
                casalog.post('*** Terminating renormalization run ***', 'INFO', 'ReNormalize')   
                raise TypeError('Correction of CORRECTED_DATA requested, but docorrThresh is set incorrectly. Use None or float.')
            if docorrThresh > 1.5:
                casalog.post('WARNING: Correction of CORRECTED_DATA requested, but docorrThresh is set very high')
                casalog.post('         docorrThresh is a factor above which to apply the ReNormalization')
                casalog.post('         '+str(docorrThresh)+' is very high and it is likely that no data will pass that limit') 
                hardLim = docorrThresh
            else:
                hardLim = docorrThresh
        else:  
            hardLim = self.bandThresh[self.Band]
        
        if docorr:
            casalog.post('####################')
            casalog.post('Using Application threshold for ReNorm ='+str(hardLim))
            casalog.post('Only spws where fields exceed this will be corrected')
            casalog.post('####################')
        else:
            casalog.post('Using threshold limit of '+str(hardLim)+' for renormalization determination')

        self.nfit=nfit
        self.fthresh=fthresh

        self.rnstats={}
        self.rnstats['inputs'] = {}
        self.rnstats['inputs']['bwthresh'] = bwthresh
        self.rnstats['inputs']['bwdiv'] = bwdiv
        self.rnstats['inputs']['bwthreshspw'] = bwthreshspw

        # the spws to process (FDM only)
        if len(spws)==0:
            spws=list(self.msmeta.almaspws(fdm=True)) 
            casalog.post('Found FDM spws = '+str(spws))
        else:
            # Force input list to be of type int
            if type(spws[0]) is str:
                for i in range(len(spws)):
                    spws[i] = int(spws[i])
            casalog.post('User supplied spws = '+str(spws))
            if not any(uspw in spws for uspw in list(self.fdmspws)):
                casalog.post('User supplied spw(s) are not in the list of FDM spws => '+str(self.fdmspws))
                raise SyntaxError('User supplied spw(s) are not in the list of FDM spws => '+str(self.fdmspws))

        if len(excludespws)>0:
            casalog.post('Will exclude spws='+str(excludespws))
            for espw in excludespws:
                if spws.count(espw)>0:
                    spws.remove(espw)

        casalog.post('Will process spws = '+str(spws))

        self.rnstats['spws']=spws

        # List of target scans if user didn't input any
        if not targscans:
            targscans=list(self.msmeta.scansforintent('*TARGET*'))
        
        casalog.post('Will process science target scans='+str(targscans))

        self.nScan=len(targscans)
        
        # This sets up rnstats for later summary plots
        self.rnstats['scans']=targscans
        self.rnstats['rNmax']=np.zeros((self.num_corrs,self.nAnt,len(spws),len(targscans)))
        self.rnstats['rNmdev']=np.zeros((self.num_corrs,self.nAnt,len(spws),len(targscans)))
        self.rnstats['N']={}
        self.rnstats['N_atm']={}
        self.rnstats['N_thresh']={} # same as N except only populated when the hardLim is reached

        # Excludeants function
        if len(excludeants) > 0:
            # check type
            if type(excludeants) is str:
                casalog.post(' excludeants requires a list of antenna ID(s) or antenna Name(s)')
                casalog.post(' e.g. [0,1] or ["DA44","DA45"]')
                casalog.post('*** Terminating renormalization run ***', 'INFO', 'ReNormalize')   
                raise TypeError('excludeants requires a list of antenna ID(s) or antenna name(s) (e.g. [0,1] or ["DA44", "DA55"]')
            else:  # note this does not check if the Antenna is actually in the Antenna Names list
                if type(excludeants[0]) is str:
                    # convert to antenna ID
                    casalog.post('Will exclude antennas = '+str((',').join(excludeants)))
                    excludeants=[excn for excn,exca in enumerate(self.AntName) if exca in excludeants]
                else:
                    casalog.post('Will exclude antennas = '+str((',').join(list(self.AntName[[excludeants]]))))

        if excflagged:
            casalog.post('For each spw, scan, field will exclude fully flagged antennas')

        if diagSpectra:
            casalog.post('Will plot diagnostic spectra per spw, scan, field')

        if checkFalsePositives:
            casalog.post('Will check for false positive renormalization triggers from atmospheric features.')
            self.atmMask={}
            self.atmWarning={}
            self.atmExcludeCmd={}

        if atmAutoExclude:
            casalog.post('Regions of the spectrum where atmospheric lines are found will be excluded.')
            checkFalsePositives = True

        if excludechan:
            # checkformats sucessively for fail modes
            if type(excludechan) is not dict:
                casalog.post(' excludechan requires a string dict input')
                casalog.post(' e.g. {"22":"100~150"}')
                casalog.post('*** Terminating renormalization run ***', 'INFO', 'ReNormalize')   
                raise TypeError("excludechan parameter requires a string dict input.")
            for excch in excludechan.keys():
                if type(excch) is not str:
                    casalog.post(' excludechan requires a string dict input')
                    casalog.post(' e.g. {"22":"100~150"}')
                    casalog.post('*** Terminating renormalization run ***', 'INFO', 'ReNormalize')   
                    raise TypeError('excludechan parameter requires a string dict input')
                if int(excch) not in spws:
                    casalog.post(' excludechan specified SPW '+excch+' is not a SPW of this dataset')
                    casalog.post('*** Terminating renormalization run ***', 'INFO', 'ReNormalize')   
                    raise SyntaxError('Inconsistent input parameters: excludechan contains spws not in spw parameter')
                if type(excludechan[excch]) is not str:
                    casalog.post(' excludechan requires a string dict input for channels')
                    casalog.post(' e.g. {"22":"100~150"}')
                    casalog.post('*** Terminating renormalization run ***', 'INFO', 'ReNormalize')   
                    raise TypeError('excludechan requires a stringdict input for channels')
                if '~' not in excludechan[excch]:
                    casalog.post(' excludechan requires a channel range separator of "~"')
                    casalog.post(' e.g. {"22":"100~150"}')
                    casalog.post('*** Terminating renormalization run ***', 'INFO', 'ReNormalize')   
                    raise SyntaxError('excludechan requires a channel range separator of "~" (tilde)')
        elif excludechan is None:
            excludechan={}
        elif type(excludechan) is str:
            excludechan={}
        # PIPE-1612: make a copy of the excludechan input so that it's not modified in the upstream code
        self.rnstats['inputs']['excludechan'] = copy.deepcopy(excludechan)
        
        if useDynamicSegments:
            casalog.post('Will examine segment boundaries for possible issues and automatically resize.')

        # Bwthreshspw (dictionary)
        if bwthreshspw:
            # check formats sucessively for fail modes
            if type(bwthreshspw) is not dict:
                casalog.post(' bwthreshspw requires a string dict input')
                casalog.post(' e.g. {"22":120e6}')
                casalog.post('*** Terminating renormalization run ***', 'INFO', 'ReNormalize')   
                raise TypeError('bwthreshspw requires a string dict input')
            for spwth in bwthreshspw.keys():
                if type(spwth) is not str:
                    casalog.post(' bwthreshspw requires the spw as a string input')
                    casalog.post(' e.g. {"22":120e6}')
                    casalog.post('*** Terminating renormalization run ***', 'INFO', 'ReNormalize')   
                    raise TypeError('bwthreshspw requires the spw as a string input')
                if int(spwth) not in spws:
                    casalog.post(' bwthreshspw SPW specified is not a SPW of this dataset')
                    casalog.post('*** Terminating renormalization run ***', 'INFO', 'ReNormalize')   
                    raise SyntaxError('bwthreshspw SPW specified is not a SPW of this dataset')
                if type(bwthreshspw[spwth]) is not float:
                    casalog.post(' bwthreshspw requires a float for the bw-threshold')
                    casalog.post(' e.g. {"22":120e6}')
                    casalog.post('*** Terminating renormalization run ***', 'INFO', 'ReNormalize')   
                    raise TypeError('bwthreshspw requires a float for the bw threshold')
            if useDynamicSegments:
                casalog.post(' bwthreshspw specified but useDynamicSegments was also specified.')
                casalog.post(' I will use the user input and turn off dynamic segmentation for specified spws.')

        # Send all input options to the log file so it's clear what has been entered
        casalog.post('Equivalent manual input:')
        casalog.post('\nself.renormalize(excludespws='+str(excludespws)+', spws='+str(spws) \
                + ', targscans='+str(targscans))
        casalog.post('    excludeants='+str(excludeants)+', excludechan='+str(excludechan)\
                +', excflagged='+str(excflagged)+', fixOutliers='+str(fixOutliers)+', mededge='+str(mededge))
        casalog.post('    atmAutoExclude='+str(atmAutoExclude)+', checkFalsePositives='+str(checkFalsePositives) \
                +', correctATM='+str(correctATM)+', limATM='+str(limATM)+', plotATM='+str(plotATM)+',')
        casalog.post('    bwdiv='+str(bwdiv)+', bwthresh='+str(bwthresh)+', bwthreshspw='+str(bwthreshspw) \
                +', checkLineForest='+str(checkLineForest)+', nfit='+str(nfit)+', useDynamicSegments=' \
                +str(useDynamicSegments))
        casalog.post('    datacolumn='+str(datacolumn)+', docorr='+str(docorr)+', docorrThresh='+str(docorrThresh) \
                +', usePhaseAC='+str(usePhaseAC))
        casalog.post('    antHeuristicsSpectra='+str(antHeuristicsSpectra)+', diagSpectra='+str(diagSpectra) \
                +', fthresh='+str(fthresh))
        casalog.post('    verbose='+str(verbose)+')')

        # Want to loop over sources so we can disentangle fields and sources and better plot what is happening
        # for mosaics and multi-target observations. 
        target_list = np.unique(self.msmeta.namesforfields(self.msmeta.fieldsforintent('*TARGET*')))
        casalog.post('\nFound targets: '+str(target_list))
        for target in target_list:
            self.rnstats['N'][target] = {}
            self.rnstats['N_atm'][target] = {}
            self.rnstats['N_thresh'][target] = {}
            self.rnstats['inputs'][target] = {}
            self.docorrApply[target] = {} 

            casalog.post('\n Processing Target='+str(target)+' ******************************')
      
            # process each spw
            dospws = np.intersect1d(spws, self.msmeta.spwsforfield(target))
            for ispw in dospws:
                # Not all targets are in all scans, we need to iterate over only those scans containing the target
                target_scans = np.intersect1d(self.msmeta.scansforintent('*TARGET*'), self.msmeta.scansforfield(target))

                # Make an additional cut to catch only those scans which contain the current spw (usually only relevant
                # for spectral scan datasets)
                target_scans = np.intersect1d(target_scans, self.msmeta.scansforspw(ispw))

                # If user input list of scans to use, cross check those with the list of all scans on targets to make
                # sure it's necessary to perform this loop. 
                target_scans = np.intersect1d(target_scans, targscans)
                
                # if there is no intersection of the input scan list and the list of scans with this target, break 
                # out of the spw loop and continue on to the next target.
                if len(target_scans) == 0:
                    casalog.post('\n Target '+str(target)+' is not contained in the input scan list '+str(targscans)+'. Moving to next target.\n')
                    break

                self.docorrApply[target][str(ispw)] = None # instantiating the spw dictionary for this target

                casalog.post('\n Processing spw='+str(ispw)+' (nchan='+str(self.msmeta.nchan(ispw))+') ******************************')

                # Setup for phase cal AC rather than Bandpass AC
                if usePhaseAC:
                    # just pick up the full list of phase cal scans
                    # later work out which one we get the AC from within the loop over the target scans
                    casalog.post('Will use PHASE calibrator AutoCorr for comparisons')
                    Phscan=self.getPhscan(ispw,verbose)
                    # still need Bandpass scan for any ATM plots - these are only for illustration
                    Bscan=self.getBscan(ispw,verbose)
                else:
                    # discern the B scan(s) which will be used - otherwise for using the phase AC we get it later
                    casalog.post('Will use BANDPASS source AutoCorr for comparisons') 
                    Bscan=self.getBscan(ispw,verbose)
                    # and get corr- and ant-dep B, time-averaged 
                    B=self.getACdata(Bscan,ispw,None,True)

                # If correctATM then we need to get the ATM transmission for the bandpass
                # this can happen outside the scan loop below where we get for the target
                # and optionally for the phase cal
                if correctATM:
                    if type(Bscan) is list:
                        Bscanatm = Bscan[0]
                    else:
                        Bscanatm = Bscan
                    if 'BandPass' not in self.atmtrans.keys():
                        # make initiation check as we loop spw then
                        self.atmtrans['BandPass']={}
                    if str(Bscanatm) not in self.atmtrans['BandPass'].keys():
                        self.atmtrans['BandPass'][str(ispw)]={}
                    self.atmtrans['BandPass'][str(ispw)][str(Bscanatm)]=self.ATMtrans(Bscanatm,ispw,verbose=True)
                    # here will make first call to ATMtrans, which can access resetting the correctATM
                    # global variable in case the PWV cannot be found for the data

                # global list of scans with the current spw
                spwscans=list(self.msmeta.scansforspw(ispw))

                if bwthreshspw:
                    # check if the spw is ispw and use different bwthreshold
                    if str(ispw) in bwthreshspw.keys():
                        casalog.post(' Using SPW specific bwthresh of '+str(bwthreshspw[str(ispw)])+' for '+str(ispw)) 
                        (nseg,dNchan) = self.calcChanRanges(ispw,bwthreshspw[str(ispw)],bwdiv,edge=mededge,verbose=verbose)
                    else:
                        # spw not in the bwthreshspw keys list - calculate channel chunks with defaults
                        (nseg,dNchan) = self.calcChanRanges(ispw,bwthresh,bwdiv,edge=mededge,verbose=verbose)
                else:
                    # no bwthreshspw option - calculate channel chunks with defaults as normal
                    (nseg,dNchan) = self.calcChanRanges(ispw,bwthresh,bwdiv,edge=mededge,verbose=verbose)
                
                self.rnstats['inputs'][target][str(ispw)] = {}
                self.rnstats['inputs'][target][str(ispw)]['num_segments'] = []
                self.rnstats['inputs'][target][str(ispw)]['dNchan'] = []

                # init Norm spectra for this spw
                self.rnstats['N'][target][str(ispw)]= np.zeros((self.num_corrs,self.msmeta.nchan(ispw),self.nAnt))
                self.rnstats['N_thresh'][target][str(ispw)] = np.zeros((self.num_corrs, self.msmeta.nchan(ispw), self.nAnt))

                # process each target scan
                ngoodscan=0
                ngoodscan_thresh=0

                if fixOutliers:
                    self.AntOut[str(ispw)]={}
                    self.replacedCorr[str(ispw)]=[]
                    self.birdiechan[str(ispw)]=[]
                    # sets for list of known ants with channel outliers 
                
                casalog.post('Target is in the following scans: '+str(target_scans))

                # We want to apply over all fields even if only one field of a mosaic is over the limit. 
                # This will also catch anything that wobbles around the limit and make sure it is applied.
                if docorr: 
                    num_passes = 2
                else:
                    num_passes = 1
                second_pass = False
                second_pass_required = False
                for npass in range(num_passes):
                    # if num_passes = 2, then npass will be set to 0 on first loop and 1 on second loop (then stops when 2 is reached).
                    # So, if npass == 1, it's the second loop and if second_pass was *not* set to True at the end of the loop, we don't
                    # need to go through the loop again.
                    if npass==1 and not second_pass_required:
                        casalog.post('Threshold limit not reached for any field/scan of spw '+str(ispw)+' of target '+target+'.')
                        continue
                    # Same as previous but if second_pass_required is set to True, we need to apply the correction and run through the scan loop again.
                    elif npass==1 and second_pass_required:
                        second_pass = True
                        casalog.post('\nThreshold limit was reached for one or more fields/scans of spw '+str(ispw)+' of target '+target+'. Applying renormalization correction to all scans, fields, and polarizations.')
                    else:
                        pass

                    for iscan in target_scans:
                        casalog.post(' Processing scan='+str(iscan)+'------------------------------')

                        # LM added
                        # here we will get the Phasecal AC if requested
                        # this will be from the scan preceeding the target scan
                        # get the existing phase cal scan numerically lower than the target scan 'iscan'
                        if usePhaseAC:
                            scanIdx = int(np.where(np.array(Phscan)<iscan)[0][-1])
                            casalog.post('**************** using the phase cal scan '+str(Phscan[scanIdx])+' *************************')
                            B=self.getACdata(Phscan[scanIdx],ispw,None,True)
                            if correctATM: 
                                if 'PhaseCal' not in self.atmtrans.keys():
                                    self.atmtrans['PhaseCal']={}
                                if str(ispw) not in self.atmtrans['PhaseCal'].keys():
                                    self.atmtrans['PhaseCal'][str(ispw)]={}
                                if str(iscan) not in self.atmtrans['PhaseCal'][str(ispw)].keys():
                                    # now we know this field, spw and scan is not filled and we will calc it
                                    # otherwise we just use what's there - i.e for a mosaic it doesn't redo for each ifld
                                    # because the atm trans model reads scan level only
                                    self.atmtrans['PhaseCal'][str(ispw)][str(Phscan[scanIdx])]=self.ATMtrans(Phscan[scanIdx],ispw,verbose=True)

                        # get the fields to process in this scan - i.e. mosaics have many fields per scan
                        Tarfld = list(self.msmeta.fieldsforscan(iscan))
                        casalog.post(' Will process science target field(s) '+str(Tarfld)+' within this scan')

                        # holder for a max value per scan to print out 
                        scanNmax=[]

                        # Now loop over the target fields
                        for ifld in Tarfld:
                            if verbose:
                                casalog.post(' Processing field='+str(ifld)+'-----------------------------')

                            # step over target scans that don't have the current spw
                            if spwscans.count(iscan)==0:
                                if verbose:
                                    casalog.post('Scan='+str(iscan)+' is not a target scan in spw='+str(ispw))
                                continue

                            # initiate the self.scalingValues dictionary
                            if str(ispw) not in self.scalingValues.keys():
                                self.scalingValues[str(ispw)]={}
                            if str(iscan) not in self.scalingValues[str(ispw)].keys():
                                self.scalingValues[str(ispw)][str(iscan)]={}
                            if str(ifld) not in self.scalingValues[str(ispw)][str(iscan)].keys():
                                self.scalingValues[str(ispw)][str(iscan)][str(ifld)]=1.0 # default no scaling

                            # AutoCorr is divided by B (can be BANDPASS or PHASE cal AutoCorr) 
                            # make if statement as getACdata can now return None if data was not filled
                            ToB=self.getACdata(iscan,ispw,ifld,True)
                            if ToB is not None: 
                                ToB/=B

                                # Renorm function will be Nb0 divided by a fit
                                N=ToB.copy()

                                # get the ATM transmission here for target - above for BP and Phase already
                                # and fix the data - should we do per scan or bulk - bulk should be enough
                                # to get rid of the main defect so fitting will work close enough (one hopes)
                                if correctATM or checkFalsePositives:
                                    # we are in iscan, ispw  and ifld
                                    # in a mosaic we are safe to use one field as representative
                                    # because these differences are 'negligable' 
                                    # compared to possibly large ones we are trying to fix between the BP and target
                                    fldname=self.msmeta.namesforfields(ifld)[0]  

                                    # need per scan, per spw - if we are just doing a bulk correction we miss any scan variations ??? 
                                
                                    if str(fldname) not in self.atmtrans.keys():
                                        self.atmtrans[str(fldname)]={}
                                        if checkFalsePositives:
                                            self.atmMask[str(fldname)]={}
                                            self.atmWarning[str(fldname)]={}
                                            self.atmExcludeCmd[str(fldname)] = {}
                                    if str(ispw) not in self.atmtrans[str(fldname)].keys():
                                        self.atmtrans[str(fldname)][str(ispw)]={}
                                        if checkFalsePositives:
                                            self.atmMask[str(fldname)][str(ispw)]={}
                                            self.atmWarning[str(fldname)][str(ispw)] = None
                                            self.atmExcludeCmd[str(fldname)][str(ispw)] = None
                                    if str(iscan) not in self.atmtrans[str(fldname)][str(ispw)].keys():
                                        # now we know this field, spw and scan is not filled and we will calc it
                                        # otherwise we just use what's there - i.e for a mosaic it doesn't redo for each ifld
                                        # because the atm trans model reads scan level only
                                        self.atmtrans[str(fldname)][str(ispw)][str(iscan)]=self.ATMtrans(iscan,ispw,verbose=verbose)
                                        
                                        # If desired, keep track of where ATM lines are so we can know if they are causing issues
                                        if checkFalsePositives:
                                            atm_mask = np.ones(N.shape[1], bool)*False
                                            if self.Band in [9, 10]:
                                                atm_centers, atm_gammas = self.fitAtmLines(
                                                        self.atmtrans[str(fldname)][str(ispw)][str(iscan)][0], int(ispw)
                                                        )
                                                atm_centers_SB, atm_gammas_SB = self.fitAtmLines(
                                                        self.atmtrans[str(fldname)][str(ispw)][str(iscan)][1], int(ispw)
                                                        )
                                                atm_centers += atm_centers_SB
                                                atm_gammas += atm_gammas_SB
                                            else:
                                                atm_centers, atm_gammas = self.fitAtmLines(
                                                        self.atmtrans[str(fldname)][str(ispw)][str(iscan)], int(ispw)
                                                        )
                                            for cen, gam in zip(atm_centers, atm_gammas):
                                                atm_mask[max(0,floor(cen-1.3*gam)):min(N.shape[1],ceil(cen+1.3*gam))] = True
                                            self.atmMask[str(fldname)][str(ispw)][str(iscan)] = atm_mask
                                            if atm_mask.any():
                                                self.atmExcludeCmd[str(fldname)][str(ispw)] = self.suggestAtmExclude(
                                                        target, str(ispw), return_command=True
                                                        )                                                
                                                if atmAutoExclude:
                                                    # Get a dictionary returned of regions to exclude due to ATM lines
                                                    atm_exchan = self.suggestAtmExclude(target, str(ispw), return_dict=True)
                                                    # Merge ATM results with user input and update our accounting
                                                    excludechan = self.merge_dicts(excludechan, atm_exchan)
                                                    self.rnstats['inputs']['excludechan'].update(excludechan)

                                skipAtmCorr=True
                                if correctATM:
                                    # check if we want to do the fix, if the ATM line is not strong
                                    # it's a pointless calculation to work out the are differences
                                    # between the BandPass and Target pointings
                                    if np.min(self.atmtrans[str(fldname)][str(ispw)][str(iscan)])<limATM:
                                        # * check now the global as if the ATM code previously didn't
                                        # * find the correct PWV, only nominal values were input
                                        # * and we probably don't want to use those for ATM correction
                                        # * as it could make ATM residuals worse
                                        if self.corrATM is False:
                                            # statement that is won't do the correction
                                            if verbose:
                                                casalog.post('WARNING will not account for any ATM lines as requested as PWV not found')
                                        else:
                                            skipAtmCorr = False
                                            # Want to keep a "clean" copy of the data so we can plot
                                            # the original data and see the improvement.
                                            N_atm = N.copy()
                                            # now we pass to a function to do the correction
                                            if usePhaseAC:
                                                self.ATMcorrection(N,iscan,ispw,ifld,str(Phscan[scanIdx]),'PhaseCal', verbose=verbose) 
                                                # just edits the N in place - i.e. should flattens out the ATM region 
                                                # - could pass fldname also but re-gets this in ATM correction function
                                            else:
                                                # bscanatm already specified above 
                                                self.ATMcorrection(N,iscan,ispw,ifld,str(Bscanatm),'BandPass',verbose=True) 
                                                # just edits the N in place - i.e. should flattens out the ATM region 
                                    else:
                                        casalog.post('No ATM features found below set limATM limit of '+str(limATM)+'. Skipping computation of ATM correction.')
                                        skipAtmCorr = True


                                if excludechan:
                                    # First check to make sure we have defined the N_atm variable as it holds the 
                                    # "original" copy of the spectrum before any ATM fixing.
                                    if 'N_atm' not in locals():
                                        N_atm = N.copy()

                                    # Now, what if N_atm exists due to an earlier spw but none of the above settings
                                    # have updated it yet?
                                    #
                                    # If we are skipping explicit ATM correction but N_atm already exists, it needs 
                                    # to be updated.
                                    if not self.corrATM and skipAtmCorr:
                                        N_atm = N.copy()

                                    # Now if this spw is in the list of spws that include exclusion, find the ranges
                                    # and apply the exclusion (set data in channel ranges = 1.0)
                                    if str(ispw) in excludechan.keys():
                                        ranges = [rng.strip() for rng in excludechan[str(ispw)].split(';')]
                                        exloch, exhich = self.interpretExcludechan(ranges, ispw)
                                        for i in range(len(exloch)):
                                            N[:,exloch[i]:exhich[i],:].fill(1.0)

                                # ants and corrs to calculate:
                                (nCor,nCha,nAnt)=N.shape

                                # If we want renorm to resize segments on-the-fly due to possible issues with the 
                                # boundary choice, we need to loop over each fitting and check.
                                if useDynamicSegments and (str(ispw) not in bwthreshspw.keys()):
                                    # We may have to loop several times over our fitting process to ensure that we
                                    # don't have a line that needs to be corrected falling on a segment boundary.
                                    # Therefore, we set these to temporary values so we can adjust them freely.
                                    nseg_tmp = nseg
                                    dNchan_tmp = dNchan
                                    
                                    i = 0
                                    # We will only loop down to 1 segment.
                                    while i <= floor(nseg/2):
                                        if verbose:
                                            casalog.post('\tAttempting fitting with nseg='+str(nseg_tmp))
                                        # N is edited in place by self.calcReNorm1() so we make a clean copy to play 
                                        # with while figuring out if the segmenting is working well.
                                        N_tmp = N.copy()
                                        if (self.corrATM and not skipAtmCorr) or (str(ispw) in excludechan.keys()):
                                            N_atm_tmp = N_atm.copy()
                                        # Initialize a redo variable for redoing the while loop
                                        redo = False
                                        # Loop over antennas, segments, and correlations
                                        for iant in range(nAnt):
                                            for iseg in range(nseg_tmp):
                                                lochan=iseg*dNchan_tmp
                                                hichan=(iseg+1)*dNchan_tmp
                                                for icor in range(nCor):
                                                    # Edits N in place! just does the fit to get zero baseline. 
                                                    # This is calcuating the ReNorm scaling per ant !!!
                                                    if checkLineForest and nseg_tmp>1:
                                                        redo = self.calcReNorm1(
                                                                N_tmp[icor,lochan:hichan,iant],
                                                                checkFit=True,
                                                                doplot=False,
                                                                iden='Ant'+str(iant)+'Seg'+str(iseg)+'Corr'+str(icor),
                                                                verbose=True
                                                                )
                                                    else:
                                                        self.calcReNorm1(
                                                                N_tmp[icor,lochan:hichan,iant],
                                                                checkFit=False,
                                                                doplot=False,
                                                                iden='Ant'+str(iant)+'Seg'+str(iseg)+'Corr'+str(icor),
                                                                verbose=True
                                                                )

                                                    # If we applied an ATM correction to the data, want to
                                                    # also see the non-ATM corrected data.
                                                    if (self.corrATM and not skipAtmCorr) or (str(ispw) in excludechan.keys()):
                                                        self.calcReNorm1(
                                                                N_atm_tmp[icor,lochan:hichan,iant],
                                                                checkFit=False,
                                                                doplot=False,
                                                                verbose=False
                                                                )
                                                    if redo:
                                                        casalog.post('    Widening segment and retrying fit.')
                                                        break
                                                if redo:
                                                    break
                                            if redo:
                                                break
                                        if redo:
                                            (nseg_tmp,dNchan_tmp) = self.calcChanRanges(
                                                            ispw,
                                                            bwthresh,
                                                            max(nseg_tmp-2,1),
                                                            edge=mededge,
                                                            verbose=verbose
                                                            )
                                            i += 1
                                            continue


                                        # Now check to see if any of the +/- 10 channels near the upper or
                                        # lower segment edge are greater than 0.5% indicating an issue. If
                                        # there is an issue, then reset and do the above again with 2 fewer
                                        # segments
                                        #
                                        # Ignore the very edges, nothing we can do there anyway!
                                        Nmed_tmp = np.nanmedian(N_tmp,axis=2)
                                        for iseg in range(1,nseg_tmp):
                                            seg_border=iseg*dNchan_tmp
                                            # Want to check +/- 10 MHz on either side of the segment border
                                            # or at least 10 channels.
                                            limit = max(10, ceil(10.e6 / abs(self.msmeta.chanwidths(ispw)[0])))
                                            if nseg_tmp > 1:
                                                trigger_floor = 1.005
                                                if verbose:
                                                    casalog.post('\tChecking segment border at chan='+str(seg_border))
                                                if (Nmed_tmp[:,seg_border-limit:seg_border+limit] > trigger_floor).any():
                                                    casalog.post('\tLarge scaling spectrum value (>' \
                                                            + str(round((trigger_floor-1)*100,1)) \
                                                            + '%) found at border, resizing segments and trying again.')
                                                    (nseg_tmp,dNchan_tmp) = self.calcChanRanges(
                                                            ispw,
                                                            bwthresh,
                                                            max(nseg_tmp-2,1),
                                                            edge=mededge,
                                                            verbose=verbose
                                                            )
                                                    i += 1
                                                    redo=True
                                                    break
                                        # Redo the fitting and such if we need to, otherwise, we're finished.
                                        if redo:
                                            continue
                                        else:
                                            if verbose:
                                                casalog.post('\tCompleted fitting procedure with nseg='+str(nseg_tmp))
                                            break

                                    # Make sure to set N and N_atm correctly as well as capture the 
                                    # final nseg and dNchan that was used. Note that this only keeps
                                    # the very last nseg and dNchan that was used so if the final scan
                                    # was different, it will not reflect the aggreagate choice, only
                                    # the very last choice.
                                    self.rnstats['inputs'][target][str(ispw)]['num_segments'].append(nseg_tmp)
                                    self.rnstats['inputs'][target][str(ispw)]['dNchan'].append(dNchan_tmp)
                                    N = N_tmp.copy()
                                    if (self.corrATM and not skipAtmCorr) or (str(ispw) in excludechan.keys()):
                                        N_atm = N_atm_tmp.copy()

                                # If we don't want to dynamically change the segment sizes or have specified one to 
                                # use, just do it once and move on.
                                else:
                                    self.rnstats['inputs'][target][str(ispw)]['num_segments'].append(nseg)
                                    self.rnstats['inputs'][target][str(ispw)]['dNchan'].append(dNchan)

                                    #If we are not using dynamic segments, then calculate what we have.
                                    for iant in range(nAnt):
                                        for iseg in range(nseg):
                                            lochan=iseg*dNchan
                                            hichan=(iseg+1)*dNchan
                                            for icor in range(nCor):
                                                # edits N in place! just does the fit to get zero baseline - this is calcuating the ReNorm scaling per ant !!!
                                                self.calcReNorm1(
                                                        N[icor,lochan:hichan,iant],
                                                        doplot=False,
                                                        iden='Ant'+str(iant)+'Seg'+str(iseg)+'Corr'+str(icor)
                                                        )
                                                #N[icor,lochan:hichan,iant] = self.calcRenormLegendre(N[icor,lochan:hichan,iant])

                                                # If we applied an ATM correction to the data, want to
                                                # also see the non-ATM corrected data.
                                                if (self.corrATM and not skipAtmCorr) or (str(ispw) in excludechan.keys()):
                                                     self.calcReNorm1(N_atm[icor,lochan:hichan,iant],doplot=False)                                    

                                ## LM added 
                                if mededge:
                                    # will set the 0.01 (1% - default) of all edge channels to the median value of the scaling spectrum (circa 1)
                                    # stops high edge outliers
                                    self.calcSetEdge(N, edge=mededge)
                                    # If we applied an ATM correction to the data, want to
                                    # also see the non-ATM corrected data.
                                    if (self.corrATM and not skipAtmCorr) or (str(ispw) in excludechan.keys()):
                                        self.calcSetEdge(N_atm, edge=mededge)


                                # LM added - excflagged
                                # regardless of any manually input excludeants we still check the cross-corr
                                # data for those antennas and simply see if it is entirely flagged
                                # i.e. 100% flagged antenna we set to 1.0 - i.e. no scaling
                                # thus plots are not skewed and anyway these antennas are not in the IF data
                                if excflagged:
                                    # get the XC flags - if true returned its 100% flagged - deals with spw SPW, per scan basis as it is selected
                                    antflagged = self.getXCflags(iscan,ispw,ifld,verbose=verbose)
                                    # adds to excludeants list if its not already there
                                    for excant in antflagged:
                                        N[:,:,excant].fill(1.0)
                                        # If we applied an ATM correction to the data, want to
                                        # also see the non-ATM corrected data.
                                        if (self.corrATM and not skipAtmCorr) or (str(ispw) in excludechan.keys()):
                                            N_atm[:,:,excant].fill(1.0)
                                        if verbose:
                                            casalog.post('**** auto flagged antenna: '+self.AntName[excant]+' for SPW='+str(ispw)+', scan='+str(iscan)+', field='+str(ifld)+' ****')

                                if excludeants:
                                    # we are excluding antennas all by index - converted above from names if input
                                    # they should be set to 1.0 - this is a workaround to
                                    # avoid bad antennas messing up the plots - if an analyst really needed
                                    # to make a list of badantennas, and they were not flagged by pipeline
                                    # then it is worrying why data are bad ...
                                    for excant in excludeants:
                                        N[:,:,excant].fill(1.0)
                                        # If we applied an ATM correction to the data, want to
                                        # also see the non-ATM corrected data.
                                        if (self.corrATM and not skipAtmCorr) or (str(ispw) in excludechan.keys()):
                                            N_atm[:,:,excant].fill(1.0)


                                # Check and fix outlier antennas compared to a representative median spectrum
                                if fixOutliers: 
                                    AntChk = self.checkOutlierAnt(N)

                                    if len(AntChk) > 0:
                                        # pass badant to the fix code for channel by channel investigation and correction
                                        self.calcFixReNorm(N,AntChk,iscan,ispw,ifld,doplot=antHeuristicsSpectra,verbose=verbose) 
                                        # If we applied an ATM correction to the data, want to
                                        # also see the non-ATM corrected data.
                                        if (self.corrATM and not skipAtmCorr) or (str(ispw) in excludechan.keys()):
                                            self.calcFixReNorm(N_atm,AntChk,iscan,ispw,ifld,doplot=antHeuristicsSpectra,verbose=verbose) 

                                # No need to do any of this on the second round of data 
                                if not second_pass:            
                                    # Need to calculate the maximum "N" value found, averaging over all antennas.
                                    # We also need to exclude values of 1.0 if/where ants are flagged - otherwise 
                                    # the output will be skewed by those values and misrepresented. Here we use 
                                    # the found maximum when the max value != 1, otherwise we set it to a NaN and
                                    # then calculate the mean ignoring NaN's. 
                                    Nmax = np.nanmean(np.where(N.max(axis=1)!=1, N.max(axis=1), np.nan), axis=1) 

                                    # If we wish to check for "false positives" where application of renormalization is
                                    # falsely triggered by an ATM feature, then do the same calculation as above but
                                    # only in the areas of the spectrum where ATM features are located. If there is a
                                    # feature above the set "hardLim" (usually 2%) then renorm will get triggered for it
                                    # or at least wrongly applied in that region. 
                                    # 
                                    # Also we want to keep track of the false positives if they occur (or not) using 
                                    # an attribute of the renorm object - self.atmWarning[target][spw]
                                    if checkFalsePositives:
                                        # Check and make sure there actually is an ATM line, otherwise ignore.
                                        if atm_mask.any():
                                            # If we automatically excluded ATM lines then everything is set to 1.0
                                            # in the range, therefore we need to check the N_atm array rather than
                                            # the N array.
                                            if atmAutoExclude:
                                                Nmax_atm = np.nanmean(
                                                        np.where(
                                                            N_atm[:,atm_mask,:].max(axis=1) != 1,
                                                            N_atm[:,atm_mask,:].max(axis=1),
                                                            np.nan
                                                            ),
                                                        axis=1
                                                        )
                                            else:
                                                Nmax_atm = np.nanmean(
                                                        np.where(
                                                            N[:,atm_mask,:].max(axis=1) != 1,
                                                            N[:,atm_mask,:].max(axis=1),
                                                            np.nan
                                                            ),
                                                        axis=1
                                                        )
                                            # Check to see if the atm region was blanked by other means (excludechan)
                                            # such that the np.nanmean() of it will return NaN. 
                                            if sum(Nmax_atm) != sum(Nmax_atm):
                                                self.atmWarning[str(fldname)][str(ispw)] = False
                                            else:
                                                if (Nmax_atm > hardLim).any():
                                                    if atmAutoExclude:
                                                        if verbose:
                                                            casalog.post('   Significant atmospheric signal was removed by atmAutoExclude.')
                                                    else:
                                                        casalog.post('   WARNING! There may be significant artifical signal from an' \
                                                                    ' atmospheric feature that will trigger renorm application!!!')
                                                    self.atmWarning[str(fldname)][str(ispw)] = True
                                                elif self.atmWarning[str(fldname)][str(ispw)]:
                                                    pass
                                                else:
                                                    self.atmWarning[str(fldname)][str(ispw)] = False
                                        else:
                                            self.atmWarning[str(fldname)][str(ispw)] = False 

                                    if np.isnan(np.sum(Nmax)):
                                        Nmax = np.array([1.0,1.0])
                                    Nmads = np.nanmedian(np.where(N!=1.0,np.absolute(N-1.0),np.nan),[1,2]) 
                                    if np.isnan(np.sum(Nmads)):
                                        Nmads = np.array([0.0,0.0])
                                    scanNmax.append(np.mean(Nmax))
                                    alarm='   '
                                    if np.any(np.greater(Nmax,1.0+usefthresh)):
                                        alarm='***'
                                    if verbose:
                                        casalog.post('  Mean peak renormalization factor (power) per polarization = '+str(alarm)+str(Nmax))
                                        casalog.post('  Median renormalization deviation (power) per polarization = '+'   '+str(Nmads)) 

                                    # Diagnoastic plots - one level more detail vs. summary plots
                                    # this is really the ant level what will be applied as a scaling
                                    #
                                    # skip these if second pass...
                                    if diagSpectra:
                                        if (self.corrATM and not skipAtmCorr) or (str(ispw) in excludechan.keys()):
                                            self.plotdiagSpectra(N, iscan, ispw, ifld, plotATM=plotATM, N_atm=N_atm)
                                        else:
                                            self.plotdiagSpectra(N, iscan, ispw, ifld, plotATM=plotATM)

                                    # In the spectra, antennas that have some scans as 1.0 due to being 
                                    # interferometrically flagged (or we excluded) we don't want them
                                    # to skew the summary cumulative average plots using the rnstats
                                    # if some scans are flagged and some are unflagged
                                    if excflagged:  
                                        # regardless of flagged antennas or not we need to initiate the rnstats on the first scan
                                        if ngoodscan==0:
                                            self.rnstats['N'][target][str(ispw)]= N
                                            if (self.corrATM and not skipAtmCorr) or (str(ispw) in excludechan.keys()):
                                                self.rnstats['N_atm'][target][str(ispw)]= N_atm
                                            ngoodscan+=1
                                        elif antflagged and ngoodscan!=0:
                                            # enter this loop if there ARE flagged antennas  
                                            for lpAnt in range(nAnt):
                                                # if the antenna is not listed as flagged and the initiated first
                                                # entry to rnstats['N'] is not 1.0 (i.e. flagged) we do
                                                # the cumulative sum for the average spectra
                                                if lpAnt not in antflagged and np.sum(self.rnstats['N'][target][str(ispw)][:,:,lpAnt])/(2.*nCha)!=1.0:
                                                    self.rnstats['N'][target][str(ispw)][:,:,lpAnt]=self.rnstats['N'][target][str(ispw)][:,:,lpAnt]*ngoodscan/(ngoodscan+1)  + N[:,:,lpAnt]/(ngoodscan+1)
                                                # if the stored antenna scan value in rnstats is 1.0 (i.e. initiated with a flagged antenna
                                                # but the antenna scan value we want to add now is good
                                                # then just replace the rnstat antenna scaling values entirely
                                                elif lpAnt not in antflagged and np.sum(self.rnstats['N'][target][str(ispw)][:,:,lpAnt])/(2.*nCha)==1.0:
                                                    casalog.post('replacing scan with good for '+str(self.AntName[lpAnt]))
                                                    self.rnstats['N'][target][str(ispw)][:,:,lpAnt]= N[:,:,lpAnt] 
                                                if (self.corrATM and not skipAtmCorr) or (str(ispw) in excludechan.keys()):
                                                    try: 
                                                        if lpAnt not in antflagged and np.sum(self.rnstats['N_atm'][target][str(ispw)][:,:,lpAnt])/(2.*nCha)!=1.0:
                                                            self.rnstats['N_atm'][target][str(ispw)][:,:,lpAnt]=self.rnstats['N_atm'][target][str(ispw)][:,:,lpAnt]*ngoodscan/(ngoodscan+1)  + N_atm[:,:,lpAnt]/(ngoodscan+1)
                                                        elif lpAnt not in antflagged and np.sum(self.rnstats['N_atm'][target][str(ispw)][:,:,lpAnt])/(2.*nCha)==1.0:
                                                            self.rnstats['N_atm'][target][str(ispw)][:,:,lpAnt]= N_atm[:,:,lpAnt]
                                                    # If we end up here, then the atmAutoExclude algorithm has only just 
                                                    # started excluding scans. So we need to instantiate the N_atm dict
                                                    # with everything that is contained in N so far and then we can add
                                                    # the new data.
                                                    except KeyError:
                                                        self.rnstats['N_atm'][target][str(ispw)]=self.rnstats['N'][target][str(ispw)]
                                                        if lpAnt not in antflagged and np.sum(self.rnstats['N_atm'][target][str(ispw)][:,:,lpAnt])/(2.*nCha)!=1.0:
                                                            self.rnstats['N_atm'][target][str(ispw)][:,:,lpAnt]=self.rnstats['N_atm'][target][str(ispw)][:,:,lpAnt]*ngoodscan/(ngoodscan+1)  + N_atm[:,:,lpAnt]/(ngoodscan+1)
                                                        elif lpAnt not in antflagged and np.sum(self.rnstats['N_atm'][target][str(ispw)][:,:,lpAnt])/(2.*nCha)==1.0:
                                                            self.rnstats['N_atm'][target][str(ispw)][:,:,lpAnt]= N_atm[:,:,lpAnt]
                                            # remember to add to the scans assessed
                                            ngoodscan+=1
                                        else:
                                            # if no flagged antennas were passed we do the default cumulative average as normal
                                            self.rnstats['N'][target][str(ispw)]=self.rnstats['N'][target][str(ispw)]*ngoodscan/(ngoodscan+1)  + N/(ngoodscan+1)
                                            if (self.corrATM and not skipAtmCorr) or (str(ispw) in excludechan.keys()):
                                                try:
                                                    self.rnstats['N_atm'][target][str(ispw)]=self.rnstats['N_atm'][target][str(ispw)]*ngoodscan/(ngoodscan+1)  + N_atm/(ngoodscan+1)
                                                # If we end up here, then the atmAutoExclude algorithm has only just 
                                                # started excluding scans. So we need to instantiate the N_atm dict
                                                # with everything that is contained in N so far plus the new data.
                                                except KeyError:
                                                    self.rnstats['N_atm'][target][str(ispw)]=self.rnstats['N'][target][str(ispw)]*ngoodscan/(ngoodscan+1) + N_atm/(ngoodscan+1)
                                            ngoodscan+=1
                                    ## Non flagged antenna cases
                                    else:
                                        # incrementally accumulate scan-mean spectra - keeps adding even as we do per field
                                        self.rnstats['N'][target][str(ispw)]=self.rnstats['N'][target][str(ispw)]*ngoodscan/(ngoodscan+1)  + N/(ngoodscan+1)
                                        if (self.corrATM and not skipAtmCorr) or (str(ispw) in excludechan.keys()):
                                            self.rnstats['N_atm'][target][str(ispw)]=self.rnstats['N_atm'][target][str(ispw)]*ngoodscan/(ngoodscan+1)  + N_atm/(ngoodscan+1)     
                                        ngoodscan+=1

                                    # PIPE 1168 (1)
                                    # Repeat the same process but now we'll only be keeping normalized spectra that is above the threshold.
                                    # This helps us plot mosaic sources and multi-target MSs as the mixture of empty/problem fields can wash the peaks.
                                    if np.mean(Nmax) > hardLim:
                                        if excflagged:  
                                            # regardless of flagged antennas or not we need to initiate the rnstats on the first scan
                                            if ngoodscan_thresh==0:
                                                self.rnstats['N_thresh'][target][str(ispw)]= N
                                                ngoodscan_thresh+=1
                                            elif antflagged and ngoodscan_thresh!=0:
                                                # enter this loop if there ARE flagged antennas  
                                                for lpAnt in range(nAnt):
                                                    # if the antenna is not listed as flagged and the initiated first
                                                    # entry to rnstats['N_thresh'] is not 1.0 (i.e. flagged) we do
                                                    # the cumulative sum for the average spectra
                                                    if lpAnt not in antflagged and np.sum(self.rnstats['N_thresh'][target][str(ispw)][:,:,lpAnt])/(2.*nCha)!=1.0:
                                                        self.rnstats['N_thresh'][target][str(ispw)][:,:,lpAnt]=self.rnstats['N_thresh'][target][str(ispw)][:,:,lpAnt]*ngoodscan_thresh/(ngoodscan_thresh+1)  + N[:,:,lpAnt]/(ngoodscan_thresh+1)
                                                    # if the stored antenna scan value in rnstats is 1.0 (i.e. initiated with a flagged antenna
                                                    # but the antenna scan value we want to add now is good
                                                    # then just replace the rnstat antenna scaling values entirely
                                                    elif lpAnt not in antflagged and np.sum(self.rnstats['N_thresh'][target][str(ispw)][:,:,lpAnt])/(2.*nCha)==1.0:
                                                        casalog.post('replacing scan with good for '+str(self.AntName[lpAnt]))
                                                        self.rnstats['N_thresh'][target][str(ispw)][:,:,lpAnt]= N[:,:,lpAnt] 
                                                # remember to add to the scans assessed
                                                ngoodscan_thresh+=1
                                            else:
                                                # if no flagged antennas were passed we do the default cumulative average as normal
                                                self.rnstats['N_thresh'][target][str(ispw)]=self.rnstats['N_thresh'][target][str(ispw)]*ngoodscan_thresh/(ngoodscan_thresh+1)  + N/(ngoodscan_thresh+1)
                                                ngoodscan_thresh+=1

                                        ## Non flagged antenna cases
                                        else:
                                            # incrementally accumulate scan-mean spectra - keeps adding even as we do per field
                                            self.rnstats['N_thresh'][target][str(ispw)]=self.rnstats['N_thresh'][target][str(ispw)]*ngoodscan_thresh/(ngoodscan_thresh+1)  + N/(ngoodscan_thresh+1)
                                            ngoodscan_thresh+=1

                                    # below is George's original code and these are used in other RN.plot*  (not the spectral one)    
                                    # this does not add per field, it appears that only the last field will be appended currently
                                    # as it is not cumulative, it is a replacement that calls only ispw and iscan
                                    # the current median will possibly be skewed by antennas set to 1.0 if flagged
                                    # if the last field has no scaling, plots using these scan based stats/and latter plots
                                    # will not show anything useful
                                    self.rnstats['rNmax'][:,:,spws.index(ispw),targscans.index(iscan)]=N.max(1)
                                    self.rnstats['rNmdev'][:,:,spws.index(ispw),targscans.index(iscan)]=np.median(np.absolute(N-1.0),1)

                                    # write in the max value for this SPW, scan, field into the self.scalingValues dictionary
                                    self.scalingValues[str(ispw)][str(iscan)][str(ifld)]=np.mean(Nmax) # average the correlations

                                    # Need to move this part out of the docorr check so that we can add a check to above thresh, then add to 
                                    # to the dictionary. Then after each scan/field, we check for it to be above the limit and update it each
                                    # time to set to True/False unless it is already set to True. Should do this regardless of the number of
                                    # fields as it would catch single field targets that wobble around the limit. 
                                    #
                                    # Check if above limit but only on the first pass through the data
                                    if np.mean(Nmax) > hardLim: 
                                        # Nmax hold 2 values for dual corr (xx,yy) coming from mean of all maximal values of all ants  
                                        self.docorrApply[target][str(ispw)]=True # will correct this field
                                    # if this field isn't above the limit but it's already True, pass
                                    elif self.docorrApply[target][str(ispw)]:
                                        pass
                                    # if not above the limit and not already set to True, set to False
                                    else:
                                        self.docorrApply[target][str(ispw)]=False
                                
                                # If we want to apply the correction and it's the second time through the data
                                if docorr and second_pass: 
                                    # apply the correction
                                    # Antenna-based Correction factors are in voltage units (pair products are power)
                                    Nv=np.sqrt(N)
                                    self.applyReNorm(iscan,ispw,ifld,Nv,datacolumn) 
                            
                                    # do self.recordApply here and pass scan, spw, field too
                                    self.recordApply(iscan,ispw,ifld)

                                    # ****
                                    # In combination with the docorrApply dictionary, and the printed message
                                    # PLWG might want to include also Keyword dictionaries into the MS
                                    # here might be a good place for this
                                    # ****

                                    casalog.post('Application of the ReNormalization was written to the MS history for spw'+str(ispw)+' scan'+str(iscan)+' field'+str(ifld))

                            # closes the data check to see if the AC data is confirmed to be filled 
                            #    - only gets here if None was returned
                            else:
                                if verbose:
                                    casalog.post(' **** No data found - skipping field '+str(ifld)+' in scan '+str(iscan)+' ****')
                                continue

                            if not second_pass:
                            # print per scan max val
                                if ifld == max(Tarfld):
                                    casalog.post('  Max peak renormalization factor (power) over scan '+str(iscan)+' = '+str(max(scanNmax)))

                        # After doing the first pass, if docorr is True and docorrApply was set to True, 
                        # we now need to go through again and actually apply the renormalization 
                        if docorr and not second_pass and self.docorrApply[target][str(ispw)]:
                            second_pass_required = True


                    if checkFalsePositives:
                        exclude_cmd = self.suggestAtmExclude(target, str(ispw), return_command=True)
                        if self.atmWarning[str(fldname)][str(ispw)]:
                            if atmAutoExclude:
                                casalog.post('Atmospheric features above the threshold have been mitigated by atmAutoExclude.')
                                casalog.post('Equivalent manual call: '+exclude_cmd)
                            else:
                                casalog.post('ATM features may be falsely triggering renorm!')
                                casalog.post('Suggested channel exclusion: '+exclude_cmd)
                        elif exclude_cmd:
                            casalog.post('Atmospheric features mitigated.')
                            casalog.post('Equivalent manual call: '+exclude_cmd)

        # PIPE 1168 (3)
        # Loops through the scalingValue dict and populates the pipeline needed dictionary
        self.rnpipestats = {}
        target_field_ids = self.msmeta.fieldsforintent('*TARGET*')
        target_fields = np.unique(self.msmeta.namesforfields(target_field_ids))
        for trg in self.rnstats['N'].keys(): #target_fields:
            self.rnpipestats[trg] = {}
            for spw in self.rnstats['N'][trg].keys(): # spws:
                self.rnpipestats[trg][spw] = {}
                scans = np.intersect1d(self.msmeta.scansforintent('*TARGET*'), self.msmeta.scansforfield(trg)) # find scans related to this target
                scans = np.intersect1d(scans, self.msmeta.scansforspw(int(spw))) # find scans related to given spw (spectral scan)
                scans = np.intersect1d(scans, targscans) # if user input scans, limit to those
                pipe_target_sv, pipe_target_fld = [],[]
                for scan in scans:
                    fields = np.intersect1d(self.msmeta.fieldsforintent('*TARGET*'), self.msmeta.fieldsforname(trg)) # fields for target
                    fields = np.intersect1d(fields, self.msmeta.fieldsforscan(scan)) # fields for given scan
                    for field in fields:
                        pipe_target_sv.append(self.scalingValues[str(spw)][str(scan)][str(field)])
                        pipe_target_fld.append(field)
                self.rnpipestats[trg][spw]['max_rn'] = max(pipe_target_sv)
                self.rnpipestats[trg][spw]['max_rn_field'] = pipe_target_fld[np.where(np.array(pipe_target_sv) == self.rnpipestats[trg][spw]['max_rn'])[0][0]]
                self.rnpipestats[trg][spw]['threshold'] = hardLim
                if len(np.unique(self.rnstats['inputs'][trg][spw]['dNchan'])) > 1:
                    self.rnpipestats[trg][spw]['seg_change'] = True
                else:
                    self.rnpipestats[trg][spw]['seg_change'] = False

        
        # final log end of the renormalize function
        casalog.post('*** ALMA almarenorm.py ***', 'INFO', 'ReNormalize')   
        casalog.post('*** '+str(self.RNversion)+' ***', 'INFO', 'ReNormalize')   
        casalog.post('*** End of renormalization run ***', 'INFO', 'ReNormalize')   

    def calcChanRanges(self,spw,bwthresh=120e6,bwdiv='odd',onlyfdm=True, edge=0.01,verbose=False):
        
        Tbw=self.msmeta.bandwidths(spw)
        nchan=self.msmeta.nchan(spw)
        
        nseg=1
        dNchan=nchan

        # Only do non-trivial segments if bwthresh exceeded AND spw is FDM 
        # (lots of channels). First check the number of channels explicitly. 
        # If we have a small number of channels, we don't want to divide up 
        # the spectrum into a ridiculous number of divisions and overfit. Less
        # than 128, no need to divide the window up at all, from there scale 
        # up to a max of 7.
        if nchan <= 128:
            casalog.post(' **Small number of channels found, will not divide up the spw.')
            if type(bwdiv) is int:
                casalog.post(' Ignoring input bwdiv of '+str(bwdiv)+' since number of channels is already small.')
            return (nseg,dNchan)
        elif nchan <= 300:
            max_divs = 3.0
        elif nchan <= 550:
            max_divs = 5.0
        else:
            max_divs = 7.0
        # check first if we are forcing a divide
        if type(bwdiv) is int and spw in self.fdmspws:
            # make float so we can check dNchan
            nseg=float(bwdiv)
            dNchan=nchan/nseg
            # check if nseg gives an integer nchans or if the remaining chans 
            # when dNchan is not an int will be covered by the edge boundaries
            if dNchan.is_integer() or nchan%nseg < edge*nchan:
                dNchan = int(dNchan)
                nseg=int(nseg)
            else:
                casalog.post(" Input bwdiv (nseg) does not divide the SPW into an integer number of channels ")
                casalog.post(" and the remaining channels are not excluded in the edges ")
                casalog.post(" Will default to power of 2 division based on bwthresh ")
                nseg=max(0,2**int(ceil(log(Tbw/bwthresh)/log(2))))
                dNchan=int(nchan/nseg)
        # This is overkill given the above restrictions, limiting to 3, 5, or 7
        # segments ensures that there are no answers for which the remainder 
        # does not fall into the edge channel rejection...
        elif Tbw>bwthresh and spw in self.fdmspws:
            if bwdiv=='odd':
                # work out best odd divisibile when dNchan is an int need to
                # avoid splitting up wide spws into too many slices which can
                # cause major issues
                nseg = min(max_divs, Tbw/bwthresh)
                dNchan=nchan/nseg
                
                # while condition is met when float value is equivalent to an 
                # int value (i.e. 7.0.is_integer() = True)
                while not dNchan.is_integer():
                    nseg=int(ceil(nseg))
                    dNchan=nchan/nseg
                    # check if the new nseg is actually odd, otherwise make it odd
                    if nseg%2 != 1:
                        nseg=int(ceil(nseg))+1.0
                        dNchan=nchan/nseg
                    # check if the remainder for this odd nseg falls into the 
                    # median set edges i.e don't need to worry if nchan/nseg 
                    # is a true integer division 
                    # e.g. 2048 into 5, dNchan is 409.6, so 5x409 segs and 
                    #      remaining is 3 chans, but edge chans are 20, so 
                    #      they are anyway set to to the median value (~1)
                    #      and we can use nseg = 5 without worry 
                    if nchan%nseg < edge*nchan:
                        # round down to nearest int and exit the while statment                        
                        dNchan=float(int(dNchan)) 
                    else:
                        # if remainder not in edges, add to nseg and continue 
                        nseg=int(ceil(nseg))+1.0
                            
                    # case where odd numbers just will not go into the channel 
                    # number inc remainder accounting
                    if nseg > 50:
                        nseg=max(0,2**int(ceil(log(Tbw/bwthresh)/log(2))))
                        break

                nseg=int(ceil(nseg))
                dNchan=int(nchan/nseg) 

            else:
                # George's base function
                nseg=max(0,2**int(ceil(log(Tbw/bwthresh)/log(2))))
                dNchan=int(nchan/nseg)

        casalog.post('\tDividing spw into '+str(nseg)+' segments of '+str(dNchan)+' channels each.')
        
        return (nseg,dNchan)


    def stats(self):
        return self.rnstats

    def merge_dicts(self, dict1, dict2):
        """
        Purpose:
            Given two dictionaries, merge them together such that common
            keys do not get overwritten. The main purpose is for merging
            user input excludechan ranges with ATM excludechan ranges.

        Inputs:
            dict1, dict2 : dictionaries
                The two dictionaries to merge together.

        Outputs:
            The merged dictionary.
        """
        dict3 = copy.deepcopy(dict1)
        for key, value in dict2.items():
            if key not in dict3.keys():
                dict3[key] = value
            else:
                cval = dict3[key]
                dict3[key] = cval+';'+value
        return dict3

    def interpretExcludechan(self, ranges, spw):
        """
        Pupose: 
            As an option to the self.renormalize() method, a user may input 
            channel ranges to be excluded from the analysis via the excludechan 
            option. Additionally, if the atmAutoExclude option is enabled the 
            renormalize method will attempt to fix any atmospheric features 
            that it thinks to be problematic. Those channel ranges, either 
            input or generated, are added to a dictionary and then need to be 
            interpreted correctly for both the exclusion and later plotting. 

        Inputs: 
            ranges : list
                This is a list of the different ranges within a single spectral
                window that need to be excluded. The expected format is:
                    
                ["low_channel~high_channel"; "low_channel~high_channel"; ...]

            spw : integer
                This is the spectral window number. This is only needed if the
                inputs of ranges are in frequency but must always be given.

        Outputs: 
            starts : list
                A list containing the starting channels for each range to be 
                excluded.

            ends : list
                A list containing the ending channels for each range to be 
                excluded.
        """
        starts, ends = [], []
        for rng in ranges:
            # If the range is given in GHz rather than channels, need to find the 
            # correct indicies for that frequency range.
            if 'GHz' in rng:
                exlofq, exhifq = rng.split('~')
                if 'GHz' in exlofq:
                    exlofq = float(exlofq.split('GHz')[0])
                else:
                    exlofq = float(exlofq)
                exloch = self.findNearest(
                        self.msmeta.chanfreqs(spw, 'GHz'), 
                        exlofq, 
                        index=True
                        )
                if 'GHz' in exhifq:
                    exhifq = float(exhifq.split('GHz')[0])
                else:
                    exhifq = float(exhifq)
                exhich = self.findNearest(
                        self.msmeta.chanfreqs(spw, 'GHz'), 
                        exhifq, 
                        index=True
                        )
            else:
                rng = rng.split('~')
                exloch = int(rng[0])
                exhich = int(rng[1])
            # Protect from "backward" inputs.     
            if exloch > exhich:
                tmp = exloch
                exloch = exhich
                exhich = tmp
            starts.append(exloch)
            ends.append(exhich)
        return starts, ends

    def fitAtmLines(self, ATMprof, spw, verbose=False):
        """
        Purpose:
            Given an atmospheric profile, find the features caused by water/ozone/etc 
            and fit a Lorentizian profile to it, returning the center and gamma scale.

        Inputs:
            ATMprof : numpy.ndarray
                An atmospheric profile as returned by ATMtrans()

            verbose : boolean : OPTIONAL
                If True, then information about the fit will be printed to the terminal.

        Outputs:
            centers : list of floats
                The center channel of found features. 

            scales : list of floats
                The gamma scale factor of found features. 

        Note:
            This utilizes the scipy package, specifically scipy.signal.find_peaks and
            scipy.optimize.curve_fit. 
        """
        from scipy.optimize import curve_fit
        from scipy.signal import find_peaks

        def get_atm_peaks(ATMprof):
            """
            Purpose: Use scipy's peak finding algorithm to find ATM dips >1%.
            
            Inputs: 
                ATMprof : array
                    An atmospheric profile which is simply a 1 dimensional array.
            """
            normATM = -ATMprof+np.median(ATMprof)
            peaks, _ = find_peaks(normATM, height=0.01)
            return peaks

        def lorentzian(x, x0, a, gam, off):
            """
            Purpose: Standard definition of a Lorentizian to optimize.

            Inputs:
                x : array
                    The x data to fit to
                x0 : float
                    The line center.
                a : float
                    The amplitude of the feature.
                gam : float
                    The width of the feature (2*gamma = FWHM).
                off : float
                    The offset from 0.
            """
            return a * gam**2 / (gam**2 + ( x - x0 )**2) + off

        def get_gamma_bounds(center, width=50.):
            """
            Purpose: Assuming that atmospheric features are typically about 50km/s
                     wide, return the frequency width for a given center frequency
                     using the standard radio convention for velocity width.

                        delta_V = delta_nu / nu_0 * c
            Inputs:
                center : float
                    Freqency location of the line center, nu_0 in the above equation.

                width : float : OPTIONAL
                    The velocity width in km/s.
                    Default: 50.
            """
            ckms = 299792.4580
            return width*center/ckms
        
        # Find atmospheric features in the profile
        atm_feature_idxs = get_atm_peaks(ATMprof)
        
        # Define our x and y data.
        xData = np.arange(0,len(ATMprof))
        yData = ATMprof
        
        # Loop over each feature, fitting a Lorentizian and reporting the fits.
        centers, scales = [], []
        for i in range(len(atm_feature_idxs)):
            x0_guess = atm_feature_idxs[i] 
            a_guess = yData[x0_guess] - max(yData)
            gamma_guess = 1.0
            off_guess = np.median(yData)

            # center must be +/- 20 channels, 
            # amp must be between 0 and -1 (atm dips are always negative here)
            # gamma must be between 1 channel and full spw width.
            # offset is between 0 (no transmission at all) and 1 (no opacity issues).
            x0_bounds = [x0_guess-20, x0_guess+20]
            a_bounds = [-1, 0]
            gamma_bounds = [
                    1, 
                    get_gamma_bounds(self.msmeta.chanfreqs(spw)[x0_guess])/abs(self.msmeta.chanwidths(spw)[0])
                    ]
            off_bounds = [0,1]

            popt, cov = curve_fit(
                            f=lorentzian, 
                            xdata=xData, 
                            ydata=yData, 
                            p0=[x0_guess,a_guess,gamma_guess, off_guess],
                            bounds=(
                                [x0_bounds[0], a_bounds[0], gamma_bounds[0], off_bounds[0]],
                                [x0_bounds[1], a_bounds[1], gamma_bounds[1], off_bounds[1]]
                                )
                        )
            centers.append(popt[0])
            scales.append(popt[2])
            if verbose:
                casalog.post('Initial Guesses:')
                casalog.post('\tx0 = '+str(x0_guess))
                casalog.post('\ta = '+str(a_guess))
                casalog.post('\tgamma = '+str(gamma_guess))
                casalog.post('\toffset = '+str(off_guess))
                casalog.post('')
                casalog.post('Bounds:')
                casalog.post('\tx0 : ['+str(x0_bounds[0])+', '+ str(x0_bounds[1])+']')
                casalog.post('\ta : ['+str(a_bounds[0])+', '+str(a_bounds[1])+']')
                casalog.post('\tgamma : ['+str(gamma_bounds[0])+', '+str(gamma_bounds[1])+']')
                casalog.post('\toffset : ['+str(off_bounds[0])+', '+str(off_bounds[1])+']')
                casalog.post('')
                casalog.post('Best Fit from scipy.optimize.curve_fit:')
                casalog.post('\tx0 = '+str(popt[0]))
                casalog.post('\ta = '+str(popt[1]))
                casalog.post('\tgamma = '+str(popt[2]))
                casalog.post('\toffset = '+str(popt[3]))
                casalog.post('')
                casalog.post('\tcovariance matrix:')
                casalog.post(str(cov))
                casalog.post('')
                casalog.post('\t std_devs = '+str(np.sqrt(np.diag(cov))))
        return centers, scales 




    def plotSpectra(
            self, 
            plotATM=True, 
            titlein=None, 
            plotDivisions=True, 
            hardcopy=True,             
            createpdf=True, 
            includeSummary=True,
            plotOriginal=True,
            shadeAtm=True,
            showExcluded=True):
        """
        Purpose:
            This function makes a summary plot of the renormalization spectrum 
            for every spectral window that has been evaluated for a 
            renormalization correction. The plots are a cumulative average over 
            all scans and fields for each antenna and correlation. For sources 
            that have multiple fields per scan (mosaics), only those fields 
            that exceed the threshold are shown in the plot. This also means 
            that even single pointings that oscillate around the threshold will 
            have only those scans that exceed the threshold included in the 
            summary plot created. Additionally, any antenna that were fully 
            flagged during calibration will have their renormalization values 
            set to 1.0.
            
            Note that renormalization() must be run before this can be run.

        Inputs:
            plotATM : boolean : OPTIONAL
                This is a boolean switch to include the ATM transmission curves
                in the plots. The bandpass is used as the representative ATM 
                transmission curve. Note that in Bands 9 and 10, the image 
                sideband transmission curve is also shown as a black line. 
                Default: True

            titlein: string: OPTIONAL
                This allows one to introduce their own title text. 
                Default: None

            plotDivisions: boolean: OPTIONAL
                This is a boolean switch to include vertical lines at all 
                locations where the spectral window was broken up during the 
                calcReNorm() stage when the spectral window renormalization 
                spectrum was fit to flatten the spectrum. 
                Default: True

            hardcopy : boolean : OPTIONAL
                This is a boolean switch to create a hardcopy of the plot as a
                PNG file. If this is set to False, the plots will be shown 
                interactively but not saved.
                Default: True

            createpdf : boolean : OPTIONAL
                This is a boolean switch to create a PDF of the summary plot 
                using the convertPlotsToPDF() method. This will only trigger 
                if hardcopy is also set to True.
                Default: True

            includeSummary : boolean : OPTIONAL
                This is a boolean switch to include the summary plot in the 
                created PDF. As such, this is only evaluated if both hardcopy 
                and createpdf are set to True.
                Default: True

            plotOriginal : boolean : OPTIONAL
                This is a boolean switch to overplot the original data before 
                any ATM correction was performed. 
                Default: True

            shadeAtm : boolean : OPTIONAL
                If set to True, this will shade the region of the spectrum 
                influenced by atmospheric features. Features are found and 
                fitted if plotATM=True, otherwise this option has no effect.
                Default: True

            showExcluded : boolean : OPTIONAL
                If set to True, then areas of the spectrum that have been 
                excluded via the excludechan option in renormalize() will 
                be shown. 
                Default: True
        """
        # Check that renormalize() has been run
        if len(self.rnstats) == 0:
            casalog.post('Please run renormalize before plotting!')
            return

        # Grab all available targets. 
        target_list = self.rnstats['N'].keys()

        # Loop over all targets and make a separate summary plot for each
        for target in target_list:
            # Check to make sure the dictionary is filled (i.e. this target 
            # was evaluated).
            if not bool(self.rnstats['N'][target]):
                continue
            plt.ioff()

            # Loop over all spws being processed to make a summary for each 
            # target/spw. Grab the spws that exist after running renormalize() 
            # (i.e. if someone chose to only run a few spws rather than all 
            # available, only those that were processed are grabbed).
            doSpws = self.rnstats['N'][target].keys()
            for spw in doSpws:
                freqs = self.msmeta.chanfreqs(int(spw),'GHz')
                
                # Not all targets are in all scans, we need to iterate over  
                # only those scans containing the target
                target_scans = np.intersect1d(
                                        self.msmeta.scansforintent('*TARGET*'), 
                                        self.msmeta.scansforfield(target)
                                        )
                # Make an additional cut to catch only those scans which  
                # contain the current spw (usually only relevant for spectral 
                # scan datasets)
                target_scans = np.intersect1d(
                                            target_scans, 
                                            self.msmeta.scansforspw(int(spw))
                                            )
                # if the user specified scans during renormalize() then the  
                # full scan list might not be included
                target_scans = np.intersect1d(
                                                target_scans, 
                                                self.rnstats['scans']
                                                )
                nscans= len(target_scans)

                # renormalize() will populate the N_thresh dictionary for each 
                # target/spw only if a target/spw/scan/field exceeds the 
                # threshold. This allows us to plot a summary that only has the 
                # fields that exceed the threshold shown which prevents the 
                # renormalization factor being washed out by fields with no 
                # emission. If no field exceeded the threshold then the 
                # dictionary is simply filled with zeros and we fall back to 
                # the total cumulative sum. 
                if np.sum(self.rnstats['N_thresh'][target][str(spw)]) == 0.0:
                    N=self.rnstats['N'][target][str(spw)]
                else:
                    N=self.rnstats['N_thresh'][target][str(spw)]
                    # If this part is triggered then only some scans/fields 
                    # triggered meaning that not all scans may be in the final 
                    # plot. Therefore, properly display the number of averaged 
                    # scans in the title.
                    nscans=0
                    for tscan in target_scans:
                        for fld in self.scalingValues[str(spw)][str(tscan)].keys():
                            if self.scalingValues[str(spw)][str(tscan)][fld] > self.bandThresh[self.Band]:
                                nscans+=1
                                break

                if plotOriginal:
                    # If the dictionary is zero length, then no atm 
                    # corrections were performed and the "original"
                    # data is the data.
                    if len(self.rnstats['N_atm'][target]) == 0:
                        atmCorr=False
                    else:
                        # However, the first spw(s) may not necessarily be 
                        # defined so we need to catch those cases. 
                        try: 
                            if len(self.rnstats['N_atm'][target][str(spw)]) > 0:
                                N_atm = self.rnstats['N_atm'][target][str(spw)]
                                atmCorr=True
                            else:
                                atmCorr=False
                        except KeyError:
                            atmCorr = False

                (nCor,nCha,nAnt)=N.shape

                # Initialize the figure
                fig = plt.figure(figsize=(10,8))
                ax_rn = fig.add_subplot(111, frame_on=False)
                ax_rn.set_ylabel('Renorm Scaling')
                ax_rn.set_xlabel('Frequency (GHz) (TOPO)')
                ax_rn.minorticks_on()
                ax_rn.ticklabel_format(useOffset=False)

                # Setup secondary x-axis to display channels
                ax_rn1 = ax_rn.twiny()
                ax_rn1.set_xlabel('Channel')
                ax_rn1.minorticks_on()
                
                
                # If user input a title, set it up, otherwise use default
                if titlein:
                    plt.title(
                            str(titlein) + '\n'
                            + self.msname
                            + ' Nant='+str(self.nAnt)
                            + ' <Nscan='+str(nscans)+'> ',
                            {'fontsize': 'medium'}
                            )
                else:
                    ax_rn.set_title(
                            self.msname
                            + '\nTarget='+target
                            + ' Spw='+str(spw)
                            + ' Nant='+str(self.nAnt)
                            + ' <Nscan='+str(nscans)+'>',
                            {'fontsize': 'medium'}
                            )
                
                # If we want to plot the original spectrum before dealing  
                # with the ATM, find the mean of the spectrum with no atm 
                # corrections, ignoring any antennas that have had all their 
                # values set to 1.0 (due to flagging), then plot the mean.
                if atmCorr:
                    try:
                        Nm_atm = np.nanmean(np.where(N_atm!=1, N_atm, np.nan), 2)
                        Nm_atm[:][np.isnan(Nm_atm[:])] = 1.0                 
                        style = ['k--','k--']
                        for icor in range(nCor):
                            ax_rn.plot(
                                    freqs, 
                                    Nm_atm[icor,:], 
                                    style[icor], 
                                    alpha=0.25, 
                                    lw=2, 
                                    zorder=11
                                    )
                    except:
                        casalog.post('ATM corrections were not properly stored, ' \
                                +'cannot plot original spectrum!')
               
                # For each antenna/correlation, plot the cummulative sum, 
                #  making the correlations unique colors.
                style = ['r:','b:']
                for iant in range(nAnt):
                    for icor in range(nCor):
                        ax_rn.plot(freqs, N[icor,:,iant],style[icor])
                
                # Find the mean of the spectrum over all antennas, ignoring  
                # any antennas that have had all their values set to 1.0 
                # (due to flagging).
                Nm = np.nanmean(np.where(N!=1, N, np.nan), 2)
                Nm[:][np.isnan(Nm[:])] = 1.0

                # Plot the mean renormalization spectrum
                style = ['r-', 'b-']
                for icor in range(nCor):
                    ax_rn.plot(freqs, Nm[icor,:], style[icor])
                
                # Find max over all ants then the mean of that, ignoring any  
                # flagged antenna. This matches the values calculated by  
                # renormalize(). Because of discrete sampling and noise, the  
                # max of the mean spectrum does not necessarily equal the mean 
                # value of the maxes from each antenna because some antenna may  
                # peak in different channels for lines that spread over multiple 
                # channels.
                if nCor == 1:
                    Nxmax = np.nanmean(
                            np.where(N.max(1)!=1, N.max(1), np.nan),1
                            )[0] 
                    Nymax = Nxmax
                elif nCor == 2:
                    Nxmax, Nymax = np.nanmean(
                            np.where(N.max(1)!=1, N.max(1), np.nan),1
                            ) 
                
                # If the max in either correlation is above the alarm theshold  
                # (fthresh), then draw a line at that amplidude, centered in 
                # the plot. 
                if Nxmax >= (1.0+self.fthresh) or Nymax >= (1.0+self.fthresh):
                    fmin = 3./8.*max(freqs) + 5./8.*min(freqs)
                    fmax = 5./8.*max(freqs) + 3./8.*min(freqs)
                    ax_rn.plot([fmin,fmax],[Nxmax]*2,'r-')
                    ax_rn.text(fmin,Nxmax,'<X>='+str(round(Nxmax, 4)),
                            ha='right',va='center',color='r',size='x-small')
                    if nCor == 2:
                        ax_rn.plot([fmin,fmax],[Nymax]*2,'b-')
                        ax_rn.text(fmax,Nymax,'<Y>='+str(round(Nymax, 4)),
                                va='center',color='b',size='x-small')

                # Grab the plot limits and set them for making it "look pretty"
                lims = list(ax_rn.axis())
                lims[0]=min(freqs)*0.99999
                lims[1]=max(freqs)*1.00001
                lims[2]=min(0.999,lims[2])
                lims[3]=max(1.15*lims[3]-0.15*lims[2],1.02)
                ax_rn.axis(lims)
                
                # If True, then show a small yellow area indicating where the 
                # spectrum has been blanked.
                if showExcluded:
                    # Check to make sure it is not an empty input
                    if self.rnstats['inputs']['excludechan']:
                        # Check to make sure this spw has anything input, then grab the 
                        # range and plot it.
                        if str(spw) in self.rnstats['inputs']['excludechan'].keys():
                            ranges = [
                                    rng.strip() 
                                    for rng in self.rnstats['inputs']['excludechan'][str(spw)].split(';')
                                    ]
                            starts, ends = self.interpretExcludechan(ranges, int(spw))
                            for i in range(len(starts)):
                                ax_rn.axvspan(
                                        freqs[starts[i]], 
                                        freqs[ends[i]], 
                                        ymin=0.05, 
                                        ymax=0.06, 
                                        facecolor='yellow', 
                                        ec='black', 
                                        zorder=0
                                        )

                # If True, draw thin, dotted lines at the locations where the 
                # renormalization spectrum was broken up during the fitting 
                # process.
                #
                # Since the number of divisions can change through renormalization
                # the mode is chosen and an indicator is placed in the upper right
                # if the number of segments changed.
                if plotDivisions:
                    if self.rnpipestats[target][str(spw)]['seg_change']:
                        ax_rn.text(0.6, 0.93, "Segment sizes changed!", color='red', fontsize='18', transform=ax_rn.transAxes)
                        dNchan = max(
                                    set(
                                        self.rnstats['inputs'][target][str(spw)]['dNchan']
                                        ), 
                                    key=self.rnstats['inputs'][target][str(spw)]['dNchan'].count
                                    )
                        nseg = max(
                                    set(
                                        self.rnstats['inputs'][target][str(spw)]['num_segments']
                                        ),
                                    key = self.rnstats['inputs'][target][str(spw)]['num_segments'].count
                                    )
                    else:
                        dNchan = self.rnstats['inputs'][target][str(spw)]['dNchan'][0]
                        nseg = self.rnstats['inputs'][target][str(spw)]['num_segments'][0]
                    if nseg > 1: 
                        xlocs = [iseg*dNchan for iseg in range(1,nseg)]
                        ax_rn.vlines(freqs[xlocs], 0.5, 5, linestyles='dotted', 
                                colors='grey', alpha=0.5, zorder=10)
                
                # Set the channels labels in the correct direction since the
                #  LSB will have "backward" frequencies and here the 
                # frequencies are always shown low to high.
                if freqs[0] > freqs[-1]:
                    ax_rn1.set_xlim(len(freqs),0)
                else:
                    ax_rn1.set_xlim(0,len(freqs))
                
                # If option selected, add the atmospheric profile to the plots, 
                # using the bandpass as the profile. 
                if plotATM:
                    # Setup the axis to draw on, using the same frequency axis
                    ax_atm = ax_rn.twinx()
                    # Grab the bandpass scan and protect against multiple 
                    # existing. Also protect from a missing bandpass scan by 
                    # falling back to the phase calibrator if necessary.
                    Bscanatm = self.getBscan(int(spw), verbose=False)
                    if not Bscanatm:
                        Bscanatm = self.getPhscan(int(spw), verbose=False)
                    if type(Bscanatm) is list:
                        Bscanatm = Bscanatm[0]
                    # If renormalize(correctATM=True) was run, the ATM profile
                    # already exists in a dictionary so use it. Otherwise, 
                    # grab a new one and make sure to also grab the image 
                    # sideband ATM profile if needed.
                    if 'BandPass' not in self.atmtrans.keys():
                        if self.Band in [9, 10]:
                            ATMprof, ATMprof_imageSB = self.ATMtrans(
                                                                Bscanatm, 
                                                                int(spw), 
                                                                verbose=False
                                                                )
                        else:
                            ATMprof = self.ATMtrans(
                                                        Bscanatm, 
                                                        int(spw), 
                                                        verbose=False
                                                        )
                    else:
                        # Currently, correctATM will not properly handle Bands 
                        # 9 and 10 but eventually the image sideband will need 
                        # to be added here.
                        ATMprof = self.atmtrans['BandPass'][str(spw)][str(Bscanatm)]
                    
                    # Plot the ATM profile
                    ax_atm.plot(
                            freqs, 
                            100*ATMprof, 
                            c='m', 
                            linestyle='-', 
                            linewidth=2
                            )
                    if self.Band in [9, 10]:
                        ax_atm.plot(
                                freqs, 
                                100*ATMprof_imageSB, 
                                c='k', 
                                linestyle='-', 
                                linewidth=2
                                )
                    ax_atm.yaxis.tick_right()
                    if self.Band in [9, 10]:
                        peak = max(np.maximum(ATMprof, ATMprof_imageSB)*100.)+10
                        ax_atm.set_ylabel('ATM Transmission (%), Image Sideband')
                    else:
                        peak = max(ATMprof*100)+10
                        ax_atm.set_ylabel('ATM Transmission (%)')
                    ax_atm.yaxis.set_label_position('right')
                    
                    # Enforce a range of 100
                    atm_ymax = peak
                    atm_ymin = peak-100
                    ax_atm.set_ylim(atm_ymin, atm_ymax)

                    # Make sure that we don't label values that are less 
                    # than 0 since that has no physical meaning.
                    yvals = np.arange(round(atm_ymin), round(atm_ymax))
                    yvals_mod = yvals%20
                    ylabels = yvals[list(np.where(yvals_mod==0)[0])]
                    ylabels_mask = ylabels >= 0 
                    ax_atm.set_yticks(ylabels[ylabels_mask])
                    ax_atm.set_yticklabels(
                            [
                                str(int(ylbl)) 
                                for ylbl in ylabels[ylabels_mask]
                                ]
                            )

                    # Gather stats for pipeline development
                    if 'atmStats' not in self.rnstats.keys():
                        self.rnstats['atmStats'] = {}
                    if target not in self.rnstats['atmStats'].keys():
                        self.rnstats['atmStats'][target] = {}

                    # Find the index and frequency with the maximum renorm value
                    maxRnIdx = np.where(Nm[0] == max(Nm[0]))[0][0]
                    maxRnFreq = freqs[maxRnIdx]
                    
                    # Find where the ATM lines are by fitting Lorentzian profiles
                    atm_centers, atm_gammas = self.fitAtmLines(ATMprof, int(spw))
                    if self.Band in [9, 10]:
                        num_lines = len(atm_centers)
                        atm_centers_SB, atm_gammas_SB = self.fitAtmLines(
                                                                ATMprof_imageSB, 
                                                                int(spw)
                                                                )
                        atm_centers += atm_centers_SB
                        atm_gammas += atm_gammas_SB

                    # Report found lines, if any.
                    # PL requested that statistics of ATM lines be printed out 
                    # with this function.
                    #
                    # Output is:
                    #
                    # UID, SPW, Freq @ max renrorm value, Max renorm value, Freq @ Atm line, renorm value @ Atm line
                    #
                    # If there are multiple atm features, then multiple lines 
                    # are output
                    if len(atm_centers) == 0:
                        self.rnstats['atmStats'][target][str(spw)] = \
                                ', '.join(
                                        [
                                            self.msname, 
                                            str(spw), 
                                            str(maxRnFreq), 
                                            str(Nm[0][maxRnIdx])
                                            ]
                                        )
                        casalog.post('\n{0:^30}'.format('ASDM uid')  
                                + '{0:^5}'.format('SPW')  
                                + '{0:^12}'.format('Freq@R_max') 
                                + '{0:^9}'.format('R_max') 
                                )
                        casalog.post(''.join(
                            [
                                '{0:^30}'.format(self.msname),
                                '{0:^5}'.format(str(spw)), 
                                '{0:^12}'.format(str(round(maxRnFreq,6))),
                                '{0:^9}'.format(str(round(Nm[0][maxRnIdx],5))) 
                                ]
                            )
                            )
                        casalog.post('')
                    else:
                        casalog.post('\n{0:^30}'.format('ASDM uid')  
                                + '{0:^5}'.format('SPW')  
                                + '{0:^12}'.format('Freq@R_max') 
                                + '{0:^9}'.format('R_max')
                                + '{0:^12}'.format('Freq@atm') 
                                + '{0:^9}'.format('R_atm')
                                )
                        for i in range(len(atm_centers)):
                            # Set the ATM profile we want to report which might 
                            # vary for Bands 9 and 10
                            profile = ATMprof
                            if self.Band in [9,10]:
                                if i >= num_lines:
                                    profile = ATMprof_imageSB
                            # A Lorentizian has a width of gamma (which is != a 
                            # Gaussian sigma!) where 2*gamma is the FWHM. Here 
                            # we go a bit further to capture most of the ATM 
                            # feature that is above the noise. This is from my 
                            # empirical estimates from datasets I've collected 
                            # and seems to capture most of signal without 
                            # catching real signal for cases where an ATM line 
                            # is coincident (or nearly so) with a real line. 
                            #
                            # PIPEREQ-228 - Need to protect instances where the
                            # fitted feature starts or ends outside the spw!
                            atm_start = min(
                                            max(
                                                0, 
                                                int(atm_centers[i]-1.3*atm_gammas[i])
                                                ), 
                                            len(profile)-1
                                            )
                            atm_end = max(
                                            0, 
                                            min(
                                                int(atm_centers[i]+1.3*atm_gammas[i]), 
                                                len(profile)-1
                                                )
                                            )
                            if atm_start == atm_end:
                                continue

                            # Draw a shaded region where the line is
                            if shadeAtm:
                                ax_atm.axvspan(
                                        freqs[atm_start],
                                        freqs[atm_end],
                                        ymin=0, 
                                        ymax=10,
                                        alpha=0.25, 
                                        facecolor='grey'
                                        )
                            
                            # Report the stats
                            atm_min_idx = atm_start + np.where(
                                    profile[atm_start:atm_end] == min(profile[atm_start:atm_end])
                                    )[0][0]
                            atm_dip_freq = freqs[atm_min_idx]
                            if str(spw) not in self.rnstats['atmStats'][target].keys():
                                self.rnstats['atmStats'][target][str(spw)] = \
                                        ', '.join(
                                                [
                                                    self.msname, 
                                                    str(spw), 
                                                    str(maxRnFreq),
                                                    str(Nm[0][maxRnIdx]), 
                                                    str(atm_dip_freq),
                                                    str(Nm[0][atm_min_idx]),
                                                    '\n'
                                                    ]
                                                )
                            else:
                                self.rnstats['atmStats'][target][str(spw)] += \
                                        ', '.join(
                                                [
                                                    self.msname, 
                                                    str(spw), 
                                                    str(maxRnFreq),
                                                    str(Nm[0][maxRnIdx]), 
                                                    str(atm_dip_freq),
                                                    str(Nm[0][atm_min_idx]),
                                                    '\n'
                                                    ]
                                                )

                            casalog.post(''.join(
                                [
                                    '{0:^30}'.format(self.msname),
                                    '{0:^5}'.format(str(spw)), 
                                    '{0:^12}'.format(str(round(maxRnFreq,6))),
                                    '{0:^9}'.format(str(round(Nm[0][maxRnIdx],5))), 
                                    '{0:^12}'.format(str(round(atm_dip_freq,6))),
                                    '{0:^9}'.format(str(round(Nm[0][atm_min_idx],5)))
                                    ]
                                )
                                )
                        casalog.post('')

                # If option is selected, save a hardcopy of the plots. 
                # Otherwise, produce interactive plot and wait for user 
                # input to go on to the next plot.
                if hardcopy:
                    # Ensure the plots directory exists, if not, create it.
                    if not os.path.exists('RN_plots'):
                        os.mkdir('RN_plots')
                    fname = self.msname \
                            + '_'+target \
                            + '_spw'+str(spw) \
                            + '_ReNormSpectra.png'
                    casalog.post('Saving hardcopy plot: '+fname)
                    plt.savefig('./RN_plots/'+fname)
                    plt.close()
                    # Save the filename of the plot to the rnpipestats 
                    # dictionary so Pipeline can easily reference it.
                    self.rnpipestats[target][str(spw)]['spec_plot'] = fname
                    if createpdf:
                        self.convertPlotsToPDF(
                                target, 
                                int(spw), 
                                include_summary=includeSummary, 
                                verbose=False
                                )
                else:
                    plt.show()
                    # Python 2 vs. 3, raw_input() changed to input()
                    try:
                        raw_input(
                                'Please close plot and press ENTER to continue.'
                                )
                    except NameError:
                        input('Please close plot and press ENTER to continue.')



    # George's default code
    def plotScanStats(self,hardcopy=True):
        if len(self.rnstats)==0:
            casalog.post('Please run renormalize before plotting!')
            return
        plt.ioff()
        pfig=plt.figure(12,figsize=(14,9))
        plt.ioff()
        pfig.clf()

        sh=self.rnstats['rNmax'].shape

        nSpw=len(self.rnstats['spws'])
        nXspw,nYspw = self.xyplots(nSpw)
        
        plt.clf()
        k=1
        scans=np.array(self.rnstats['scans'])
        loscan=scans.min()-1
        hiscan=scans.max()+1
        for spw in self.rnstats['spws']:
            ispw=self.rnstats['spws'].index(spw)
            plt.subplot(nYspw,nXspw,k)
            plt.ioff()
            # CASA 6 units change unless specificed
            plt.ticklabel_format(style='plain', useOffset=False)
            if (k-1)%nXspw==0:
                plt.ylabel('Peak frac renorm scale')

            if k>(nSpw-nXspw):
                plt.xlabel('Scan')

            if k==1:
                plt.title(
                        self.msname
                        +' Nant='+str(self.nAnt)
                        +' Nscan='+str(len(self.rnstats['scans'])),
                        {
                            'horizontalalignment': 'left', 
                            'fontsize': 'medium',
                            'verticalalignment': 'bottom'
                            }
                        )

            k+=1
            F=self.rnstats['rNmax'][:,:,ispw,:]-1.0
            Ferr=self.rnstats['rNmdev'][:,:,ispw,:]
            Fmax=F.max()*1.15
            Fmax=max(0.01,Fmax)
            
            for i in range(sh[1]):
                for j in range(sh[0]):
                    plt.plot(scans,F[j,i,:],'-')
                    plt.plot(scans,F[j,i,:],'k.')
                    plt.plot(scans,Ferr[j,i,:],':')
            plt.axis([loscan,hiscan,0.0,Fmax])
            
            plt.text(loscan+0.25,0.9*Fmax,'Spw='+str(spw))

        if hardcopy:
            if not os.path.exists('RN_plots'):
                os.mkdir('RN_plots')
            fname=self.msname+'_ReNormAmpVsScan.png'
            casalog.post('Saving hardcopy plot: '+fname)
            plt.savefig('./RN_plots/'+fname)
            plt.close()
        else:
            plt.show()

    # George's default code 
    #
    # Won't work with 1 correlation
    def plotSpwStats(self,hardcopy=True):
        if len(self.rnstats)==0:
            casalog.post('Please run renormalize before plotting!')
            return
        plt.ioff()
        pfig=plt.figure(13,figsize=(14,9))
        plt.ioff()
        pfig.clf()

        sh=self.rnstats['rNmax'].shape

        spws=np.array(self.rnstats['spws'])
        lospw=spws.min()-1
        hispw=spws.max()+1

        F=np.mean(self.rnstats['rNmax'],3)-1.0

        plt.clf()
        for i in range(sh[1]):
            plt.plot(spws-0.05,F[0,i,:],'r.')
            plt.plot(spws+0.05,F[1,i,:],'b.')

        plt.axis([lospw,hispw]+list(plt.axis()[2:]))
        plt.xlabel('Spw Id')
        plt.ylabel('Scan-mean Peak frac renorm scale')
        plt.title(
                self.msname
                + ' Nant='+str(self.nAnt)
                + ' <Nscan='+str(len(self.rnstats['scans']))+'>',
                {
                    'horizontalalignment': 'center', 
                    'fontsize': 'medium',
                    'verticalalignment': 'bottom'
                    }
                )
       
        if hardcopy:
            if not os.path.exists('RN_plots'):
                os.mkdir('RN_plots')
            fname=self.msname+'_ReNormAmpVsSpw.png'
            casalog.post('Saving hardcopy plot: '+fname)
            plt.savefig('./RN_plots/'+fname)
            plt.close()
        else:
            plt.show()

    ## LM added field input
    # AL added XX only case
    def applyReNorm(self,scan,spw,field,rN,datacolumn='CORRECTED_DATA'):

        (a1,a2,X)=self.getXCdata(scan,spw,field,datacolumn)

        (nCor,nCha,nRow)=X.shape
        for irow in range(nRow):
            for icor in range(nCor):
                if nCor==1:
                    X[icor,:,irow]*=(rN[icor,:,a1[irow]]*rN[icor,:,a2[irow]])
                elif nCor==2:
                    X[icor,:,irow]*=(rN[icor,:,a1[irow]]*rN[icor,:,a2[irow]])
                elif nCor==4:
                    X[icor,:,irow]*=(rN[int(icor/2),:,a1[irow]]*rN[int(icor%2),:,a2[irow]])
                    
        self.putXCdata(scan,spw,field,X,datacolumn)

    def calcReNorm1(self, R, checkFit=False, doplot=False, iden='', verbose=True):   
        """
        Purpose:
            Perform a simple polynomial fit to the renormalization scaling 
            spectrum to remove any residual baseline ripples. Used within the
            renormalize method to fit a baseline to each segment of the full 
            spectrum. 

            Note that this operates directly on the input spectrum!

        Inputs:
            R : numpy.array
                Scaling spectrum for a specific scan, spw, field and pol.

            doplot : boolean : OPTIONAL
                If set to True, produce a plot showing the polynomial fits 
                that have been applied to the data. 

            iden : str : OPTIONAL
                An optional identifier to append to image names to make them
                unique, otherwise they will get written over. 

        Outputs:
            None directly, but note that the input scaling spectrum will be 
            adjusted directly.
        """
        # NB:  R will be adjust in place

        nCha=len(R)

        x=np.array(range(nCha))*1.0/nCha - 0.5

        mask=np.ones(len(R),bool)
        ylim=0.0

        if doplot:
            plt.ioff()
            plt.clf()
            fig = plt.figure(figsize=(20,10))
        
        # Find where ATM was masked
        atm_masked = np.where(R==1.0)[0]
        
        atm_mask = mask
        atm_mask[atm_masked] = False

        # initial "fit" - Ignoring atm regions
        if len(R[atm_mask]) == 0:
            casalog.post('    WARN: No data left in segment after removing atm features.')
            refit=True
            return refit
        else:
            f=np.array([np.median(R[atm_mask])])


        for ifit in range(1,self.nfit):

            R0=R.copy()

            # ~flattened, zeroed spectrum
            R0/=np.polyval(f,x)
            R0-=1.0
                            
            # thresh is half peak negative
            #thresh=abs(R0.min()/2.0)

            # thresh is 2x median deviation (~1.33 sigma) 
            # need to ignore regions of the spectrum with atm features
            thresh=np.median(np.absolute(R0[atm_mask]))*2.0
            
            # mask outside thresh
            mask[:]=True
            mask[R0<-thresh]=False
            mask[R0>thresh]=False
            
            # This was meant to check for line forest like data but ultimately
            # just fails when there is an atm feature and noisy data
            #if checkFit:
            #    if sum(mask)/len(mask) > 0.5:
            #        refit=False
            #    else:
            #        if verbose:
            #            casalog.post('    WARN: More than 50% of the selected data is masked in the fit.')
            #        refit=True
            #        # Exit here since we need to refit anyway, no sense in
            #        # wasting compute time. There's also a small chance that
            #        # the entire segment is masked from an atm feature.
            #        return refit

            # ignore ATM masked regions
            mask[atm_masked]=False

            if checkFit:
                if sum(mask)/len(mask) > 0.1:
                    refit=False
                else:
                    if verbose:
                        casalog.post('    WARN: Less than 10% of the selected data is available ' \
                                +'to fit likely due to atm and/or science line features.')
                        refit=True
                        # Exit here since we need to refit because there's 
                        # almost no data left.
                        return refit

            if doplot:
                med=np.median(R)
                plt.subplot(2,self.nfit,ifit)
                plt.plot(range(nCha),R,'b,', label='Data')
                plt.plot(
                        np.array(range(nCha))[np.logical_not(mask)],
                        R[np.logical_not(mask)],
                        'r.',
                        label='Masked'
                        )
                plt.plot(range(nCha),np.polyval(f,x),'r-', label='Initial/Last Fit')
                #plt.axis([-1,nCha,med-0.003,med+0.003])
                            
                if ylim==0.0:
                    ylim=5*thresh
                plt.subplot(2,self.nfit,ifit+self.nfit)
                plt.plot(range(nCha),R0,'b,', label='Data after last fit')
                plt.plot([-1,nCha],[-thresh,-thresh],'r:', label='Threshold')
                plt.plot([-1,nCha],[thresh,thresh],'r:')
                #plt.axis([-1,nCha,-ylim,ylim])
                #casalog.post(ifit-1, thresh, abs(R0.min()/2.0), np.sum(mask), f)
                plt.legend()

            # fit to _R_ in masked spectra
            f=np.polyfit(x[mask],R[mask],ifit)
            
            if doplot:
                plt.subplot(2,self.nfit,ifit)
                plt.plot(range(nCha),np.polyval(f,x),'g-', label='N='+str(ifit)+' Fit')
                #casalog.post(ifit, f)
                plt.legend()

        R/=np.polyval(f,x)

        # reset ATM masked region
        R[atm_masked] = 1.0

        if doplot:
            plt.subplot(2,self.nfit,ifit+1)
            plt.plot(range(nCha),R,'g-')
            plt.title('Final Data After Fitting')
            plt.savefig(
                    './RN_plots/seg_fitting_results_'+iden+'.png', 
                    format='png', 
                    bbox_inches='tight'
                    )
            plt.close()
        
        if checkFit:        
            return refit

    def calcRenormLegendre(self, R, nseg):
        """
        Purpose:
            Perform a fit to the renormalization spectrum to flatten the 
            profile and make it so that outside of spectral features the 
            data is unchanged (i.e. multiplied by 1.0). Here, Legendre 
            polynomials are used for the fitting process. 

        Inputs:
            R : numpy.array
                This is the segment of renormalization spectrum you wish to 
                perform a fit on.  

            nseg : integer
                This is the number of segments that calcChannelRanges() 
                suggested. This function will decide on the order of the fit 
                based on the number of segments and the self.nfit parameter 
                as self.nfit*nseg.
        """
        # First we make a quick copy of the array. We are going to need to 
        # ignore sections of the spectrum where actual spectral features exist 
        # so we quickly operate on a copy but perform the fit on the actual 
        # given array.
        R0 = R.copy()
        
        # Define x
        x = np.linspace(0,len(R0)-1, len(R0))

        # Legendre polynomials are bounded within [-1,1] so convert our x to 
        # that space.
        nx = 2*x/x[-1] - 1

        # An "initial fit" to our array copy just to somewhat flatten it and  
        # move everything to around 0.0.
        ifit = np.array([np.median(R0)])
        R0 = R0/np.polyval(ifit,x) - 1.0
        
        # Now find where there are spectral features and create a mask on them.
        thresh = np.median(np.absolute(R0)) * 3.0
        mask = np.ones(len(R), bool)
        mask[:] = True
        mask[R0 <- thresh] = False
        mask[R0 > thresh] = False

        # Perform the least-squares fit to everywhere else in the real 
        # given array
        coeffs = np.polynomial.legendre.legfit(
                                            nx[mask], 
                                            R[mask],
                                            self.nfit*nseg
                                            )

        # Evaluate the amplitudes from out fit.
        fit = np.polynomial.legendre.legval(
                                            nx,
                                            coeffs
                                            )

        # Apply the fit to our data.
        R = R/fit
        return R


    def checkOutlierAnt(self, R):
        """
        Purpose:
            Perform a check of each antenna in a renormalization spectrum
            dictionary and return a list of possibly problematic antenna
            that may need flagging due to outlier data (e.g. from birdies).

        Inputs:
            R : dictionary
               The renormalization spectrum as created by the renormalize()
                method.

        Outputs:
            AntChk : list of strings
                A list of antennas that may contain problematic data which 
                can be used as input for the calcFixReNorm method.
        """
        AntChk=[]

        # Median spectra for each pol. In principle all ants should be the 
        # same per field. Need to be careful to avoid an issue if there are 
        # too many 1.0's from flagged data - median ends up being 1.0! 
        # Exclude channels that equal 1.0 for taking the median and then 
        # replace them after the calculation so we don't have to deal with NaN.
        M = np.nanmedian(np.where(R!=1,R,np.nan),2)
        M[:][np.isnan(M[:])]=1.0 

        # Median in channel axis. Left with MAD per corr per ant, 
        # then median of all
        medMAD =np.median(np.median(np.absolute(R - 1.0),1))

        # Mean max value per pol - as above
        Rmax=np.mean(R.max(1),1)

        # Values <0.25% are trivial
        thresh = 1.0025  

        # TBD some bad ants have >1.025
        for jcor in range(R.shape[0]):
            # set thresh to avoid a line free spectrum defining 
            # 'noise' as differences
            RmaxT = np.maximum(Rmax[jcor]-1.0,0.0025)

            # first review the median for birdies (same as the calcFixReNorm),
            # pass over a range and check for huge spikes
            for nch in range(10,M.shape[1]-10): 
                if np.absolute(np.median(M[jcor,nch-2:nch+3])-M[jcor,nch]) > 0.5*RmaxT:
                    M[jcor,nch]=np.median(M[jcor,nch-2:nch+3])

            for jant in range(R.shape[2]):
                Rcomp = 1.0+np.absolute((R[jcor,:,jant]/M[jcor,:])-1.0)

                # now check if any channel triggers a real outlier - do not assess 
                # per channel here just store to the outlier ant list for detailed 
                # investigation later
                if Rcomp.max(0) > thresh and jant not in AntChk:
                    AntChk.append(jant)

        return AntChk

    def calcFixReNorm(
            self,
            R,
            AntUse,
            scanin, 
            spwin,
            fldin,
            doplot=True, 
            plotDivisions=True, 
            hardcopy=True,
            verbose=False
            ):
        """
        Purpose:
            This function takes in a renormalization spectrum and performs a 
            set of heuristics to check for outliers caused by issues with the
            autocorrelations (e.g. birdies). Outlier channels are replaced with
            the median spectrum. If many channels are found to be outliers 
            (greater than 10 consecutive channels or >10% of total spectrum), 
            then the whole median spectrum of that correlation is used for the 
            replacement instead. 

            Note that this operates on the input renormalization spectrum 
            directly! Therefore the input spectrum will be directly adjusted.

        Inputs:
            R : dictionary
                The renormalization spectrum as created by the renormalize()
                method.

            AntUse : list of strings
                This is a list of antennas to investigate for outliers. The
                checkOutlierAnt method will return an appropriate list to
                be used as input here.

            scanin : string
                The scan number to check.

            spwin : string
                The spw number to check.

            fldin : string
                The field number to check.

            doplot : boolean : OPTIONAL
                If set to True, produce plots that show original spectrum
                and replaced values. 
                Default: True

            plotDivisions : boolean : OPTIONAL
                If set to True, then include vertical lines of the plots that
                show where the spectrum was divided up during the fitting 
                process of the calcReNorm1 method.
                Default: True

            hardcopy : boolean : OPTIONAL
                If set to True, save a hardcopy of the produced plot to disk.
                Default: True

            verbose : boolean : OPTIONAL
                If set to True, produce additional output.
                Default: False

        Outputs:
            The input renormalization spectrum, R, is operated on directly.
            No additional output is generated.

        Notes for Improvement: 
            If AntUse is not defined, could default to running checkOutlierAnt
            method by default.
        """
        # Median spectra for each pol - in principle all ants should be the 
        # same per field. However, there can be a problem if there are too 
        # many 1.0's from flagged data because median ends up being 1.0. For
        # excluded chan ranges which are 'nan' need to set back to 1.0, 
        # otherwise median has nan values and rest of stats max, min, etc 
        # do not deal with it.
        M = np.nanmedian(np.where(R!=1,R,np.nan),2)  
        M[:][np.isnan(M[:])]=1.0 
        Mabs = 1.0+np.absolute(M-1.0)

        # mean max value per pol - as above
        Rmax = np.mean(R.max(1),1) 
        Rmin = np.mean(R.min(1),1)

        # Median in channel axis. Left with MAD per corr per ant, 
        # then median of all
        medMAD = np.median(np.median(np.absolute(R - 1.0),1))  

        # Scales of this magnitude are negligable 
        thresh = 1.0025 

        if doplot:
            # Make a copy for plotting
            Rorig = R.copy() 

        # Assuming 2 corr/pols but this doesn't hurt single pol or full pol
        lineOut=[[],[]]
        plttxt=[[],[]]
        corPrt=['XX','YY']
        
        # This repeats the median spectrum making and checking actively - fast 
        # so just copied as from cehckOutlierAnt code 
        for jcor in range(R.shape[0]):

            RmaxT = np.maximum(Rmax[jcor]-1.0,0.0025)

            for nch in range(10,M.shape[1]-10): 
                if np.absolute(np.median(M[jcor,nch-2:nch+3])-M[jcor,nch]) > 0.5*RmaxT:
                    M[jcor,nch]=np.median(M[jcor,nch-2:nch+3])
                    self.birdiechan[str(spwin)].append(
                            'chan'+str(nch)+'_scan'+str(scanin)+'_field'+str(fldin)
                            )

        # setup threshold, for the read in spw, scan, field
        thresharr=M.copy()

        # Now we set to a max value of either thresh or the median array max taken over 10 
        # channels assesses +/- 5 chans max values in median spec and then compares with 
        # thresh and attributes to the max over +/- 5 assessed over the same 5 channels if 
        # over thresh acts as the buffer the not miss -ve in absorbtion features (black-
        # dashed in plot) set to +/- 10 now due to abs/emm CO lines in ALMA-IMF data 
        for jcor in range(R.shape[0]): 
            thresharr[jcor,0:10][thresharr[jcor,0:10]<thresh]=thresh # set ends
            thresharr[jcor,-10:][thresharr[jcor,-10:]<thresh]=thresh # set ends
            thresharr[jcor,10:-10]=[
                    thresh 
                    if thresharr[jcor,nch-10:nch+10].max(0) < thresh 
                    else thresharr[jcor,nch-10:nch+10].max(0) 
                    for nch in range(10,M.shape[1]-10)
                    ]

           
        # loops ants that triggered in outlier list
        for jant in AntUse:

            # append to known dict if not there already
            if self.AntName[jant] not in self.AntOut[str(spwin)].keys(): 
                self.AntOut[str(spwin)][self.AntName[jant]]={}
                
            # for later logic of action if required
            replaceCorr = [False for iCor in range(self.num_corrs)]
            
            for jcor in range(R.shape[0]):  

                Rcomp = 1.0+np.absolute((R[jcor,:,jant]/M[jcor,:])-1.0)
                # make channel based assessment and reset to specific channel value in median 
                # spectrum (default operation) but as we replace we store how many channels 
                # are replaced for later logic
                R[jcor,:,jant] = [
                        M[jcor][nch] 
                        if Rcomp[nch]>thresharr[jcor][nch] 
                        else R[jcor,:,jant][nch] 
                        for nch in range(M.shape[1])
                        ]
                lineOut[jcor]=[
                        spl 
                        for spl in range(M.shape[1]) 
                        if Rcomp[spl]>thresharr[jcor][spl]
                        ]

                # store a print statement for plot - can be later overwritten
                plttxt[jcor]=' **** Replace flagged channels with that from median spectrum **** ' 
                if len(lineOut[jcor])>0:
                    if verbose:
                        casalog.post('\tOutlier antenna identified '+str(self.AntName[jant]) \
                                +' '+str(corPrt[jcor])+' will repair outlier channels')

                    # open the list for outlier channels if not already existing (fill below)
                    if corPrt[jcor] not in self.AntOut[str(spwin)][self.AntName[jant]].keys(): 
                        self.AntOut[str(spwin)][self.AntName[jant]][corPrt[jcor]]=[]
                    
                    # also want to know the maximum consecutive channels 
                    maxConseq = self.calcMaxConseq(lineOut[jcor])

                    # if there are more than 10 consecutive lines replace with median spectrum
                    if maxConseq > 10: 
                        replaceCorr[jcor]=True
                else:
                    # There were no triggered lines, maybe only one pol was 
                    # bad - don't assess any further. It triggered the 
                    # outlierAnt but this pol wasn't bad
                    continue 
        
                # If we find a lot of outlier channels, or the consecutive 
                # amount of bad channels is triggered, we work out what action 
                # to take - 10% of SPW must be bad in total - this is a hard 
                # coded choice 
                if len(lineOut[jcor])>0.1*M.shape[1] or replaceCorr[jcor]:
                    R[jcor,:,jant]=M[jcor]
                    plttxt[jcor]=' **** Replaced '+corPrt[jcor]+' spectrum with median ' \
                            +corPrt[jcor]+' spectrum **** '

                # Only plot those where some replacement of the spectrum was needed
                if len(lineOut[jcor]) < 10 \
                        and set(
                                lineOut[jcor]
                                ).issubset(
                                        self.AntOut[str(spwin)][self.AntName[jant]][corPrt[jcor]]
                                        ):
                    # usually plot the per spw, per scan, per field correction made, but if that 
                    # ant was shown already, don't repeat i.e. known as problematic don't really 
                    # need to show the same channel 'fix' for all scans/fields
                    if hardcopy and doplot:
                        if verbose:
                            casalog.post('\tNot saving hardcopy - ' \
                                    + 'channels already identified for this antenna')
                    continue                  
                elif maxConseq < 10:
                    if hardcopy and doplot:
                        if verbose:
                            casalog.post('\tNot saving hardcopy - ' \
                                    +'less than 10 conseq channels adjusted - ' \
                                    +'only birdies outliers')
                    continue
                else:
                    # extend the list to add known channels now and follow to the plots
                    self.AntOut[str(spwin)][self.AntName[jant]][corPrt[jcor]].extend(
                            [
                                lineO 
                                for lineO in lineOut[jcor] 
                                if lineO not in \
                                        self.AntOut[str(spwin)][self.AntName[jant]][corPrt[jcor]]
                                ]
                            )
                    
                # These are the heuristics diagnostic plots to be used for 
                # checking the heuristics logic to see what was occuring. This
                # can make 100+ plots, so if heursitics are trusted they don't 
                # really need investigation and are mostly for testing and 
                # deep dives later if needed.
                if doplot:
                    # Initialize figure and clear buffer
                    plt.ioff()
                    plt.clf()
                    fig = plt.figure(figsize=(10,8))
                    ax = fig.add_subplot(111,frame_on=False)

                    # Plot the original renorm spectrum, the comparison median spectrum, 
                    # the adjusted spectra, and the threshold.
                    ax.plot(Rorig[jcor,:,jant],c='r',linestyle='--', label='Orig. Spec.') 
                    ax.plot(Rcomp,c='b',label='Divided (comp) spec.',alpha=0.5)
                    ax.plot(M[jcor],c='g',linewidth='2', label='Median Spec.')
                    ax.plot(R[jcor,:,jant],c='k',label='New Spec.') 
                    ax.plot(
                            thresharr[jcor],
                            c='0.5',
                            alpha=0.5,
                            linestyle='--',
                            linewidth='3',
                            label='Threshold'
                            )
                    
                    # Plot any birdies/outliers that have been found
                    for lineP in lineOut[jcor]:
                        ax.plot(lineP,0.999,c='y',marker='s')
                    # to get the single label
                    ax.plot(lineOut[jcor][0],0.999,c='y',marker='s', label='Outlier Chns.')

                    # Find the data edges to set plot size
                    pltmin=np.array(
                            [R[jcor,:,jant].min(0),0.9977,Rorig[jcor,:,jant].min(0)]
                            ).min(0)
                    Pmax = np.array(
                            [R[jcor,:,jant].max(0),1.015,Rcomp.max(0),thresharr[jcor].max(0)]
                            ).max(0)
                    pltmax= 1.+(Pmax-1.)*1.10
                    ax.axis([0.0,M.shape[1],pltmin,pltmax])

                    # If a substitution was made above, inform on plot
                    if plttxt[jcor]:
                        ax.text(0,1.+(Pmax-1.)*1.04,plttxt[jcor])

                    # If selected, plot the locations where the spectrum was broken up 
                    # during fitting
                    if plotDivisions:
                        target = self.msmeta.namesforfields(fldin)[0]
                        dNchan = self.rnstats['inputs'][target][str(spwin)]['dNchan'][-1]
                        nseg = self.rnstats['inputs'][target][str(spwin)]['num_segments'][-1]
                        # if there is only 1 segment, then no lines to draw
                        if nseg==1:
                            plotDivisions=False
                        else:
                            xlocs = [iseg*dNchan for iseg in range(1,nseg)]                    
                            ax.vlines(
                                    xlocs, 
                                    0.5, 
                                    5, 
                                    linestyles='dotted', 
                                    colors='grey', 
                                    alpha=0.5, 
                                    zorder=10
                                    )
                    ax.set_xlabel('Channels')
                    ax.set_ylabel('ReNorm Scaling')
                    fname=self.msname+'_ReNormHeuristicOutlierAnt_' + self.AntName[jant] \
                            + '_spw' + str(spwin) \
                            + '_scan' + str(scanin) \
                            + '_field' + str(fldin) \
                            + '_'+corPrt[jcor]
                    ax.set_title(
                            self.msname
                                + '\nAntenna ' + self.AntName[jant]
                                + ' Spw: ' + str(spwin)
                                + ' Scan: ' + str(scanin)
                                + ' Field: ' + str(fldin)
                                + ' Corr: ' + corPrt[jcor], 
                            {'fontsize': 'medium'}
                            )

                    # legend lines
                    ax.legend(
                            loc='lower center',
                            bbox_to_anchor=(0.5,-0.28),
                            prop={'size':8},
                            ncol=3
                            )
                    fig.subplots_adjust(bottom=0.20)
                    
                    # Create secondary x-axis with frequency labels
                    freqs = self.msmeta.chanfreqs(spwin,'GHz')
                    ax1 = ax.twiny()
                    ax1.set_xlabel('Frequency (GHz) (TOPO)')
                    ax1.set_xlim(freqs[0],freqs[-1])
                    ax1.ticklabel_format(useOffset=False)

                    # Save a hardcopy of the plots if desired 
                    # or show plots interactively
                    if hardcopy:
                        if not os.path.exists('RN_plots'):
                            os.mkdir('RN_plots')
                        if verbose:
                            casalog.post('\tSaving hardcopy plot: '+fname)
                        plt.savefig('./RN_plots/'+fname+'.png')
                        plt.close('all')
                    else:
                        plt.show()

            

    # Main diagnostic spectra at lowest level - scaling that each spw, scan, field, ant, correlation will have
    # these plots should look good
    def plotdiagSpectra(
            self, 
            R, 
            scanin, 
            spwin, 
            fldin, 
            threshline=None, 
            plotATM=True, 
            plotDivisions=True, 
            N_atm=None, 
            shadeAtm=True, 
            showExcluded=True
            ):
        """
        Purpose: 
            This creates diagnotic spectra at the per field per spectral window level for each scan.
            Each antenna and correlation are plotted for each field.
        Inputs:
            R : numpy.array
                This is the renormalization spectrum that you wish to plot. Requires data in the form
                of a numpy array in the shape [correlation, channel, antenna] such that [0,:,0] would
                be the 0-th correlation (XX), all channels, for the 0-th antenna (e.g. DA45) and would
                be the found renormalization spectrum.

            scanin : int
                This is the scan number which corresponds to the renormalization spectrum supplied.

            spwin : int
                This is the spectral window number which corresponds to the renormalization spectrum
                supplied.

            fldin : int
                This is the field number which corresponds to the normalization spectrum supplied.

            plotATM : boolean : OPTIONAL
                This option will plot (or not) the atmospheric transmission profile for the input
                spectrum.
                Default: True

            plotDivisions : boolean : OPTIONAL
                This option will plot (or not) the division lines as vertical, grey, dotted lines
                at all the locations where the renormalization spectrum was broken up during the
                fitting stage.
                Default: True

            threshLine : float or None : OPTIONAL
                This option allows the user to draw a line of the plot to represent the threshold
                level.
                Default: None

            N_atm : numpy.array OR None : OPTIONAL
                Similar to R above, this is the renormalization spectrum that you want to plot except
                this should be the renormalization spectrum that has not had any atmospheric corrections
                applited to it. If provided, then this "original" data will be plotted showing the 
                differences between this and the renormalization spectrum that will be applied.
                Default: None

            shadeAtm : boolean : OPTIONAL
                If set to True, this will find atmospheric features, fit them with a Lorentzian 
                profile and shade the regions of the spectrum influenced by the feature.
                Default: True

            showExcluded : boolean : OPTIONAL
                If set to True, then areas of the spectrum that have been excluded via the 
                excludechan option in renormalize() will be shown. 
                Default: True
         
        """

        # If an original Renormalization spectrum was provided (i.e. one without any atmospheric
        # corrections applied to it) then find the median (avoiding zeros) so that we can overplot 
        # it to see the improvements directly.
        if N_atm is not None:
            plot_original = True
            M_atm = np.nanmedian(np.where(N_atm!=1,N_atm,np.nan),2)
            M_atm[:][np.isnan(M_atm[:])]=1.0
        else:
            plot_original = False

        # Grab the median renormalization spectrum, avoiding values set to exactly 1.0 since that
        # indicates that an antenna has been completely flagged and skews the median. Then, reset 
        # spectrum after found.
        M = np.nanmedian(np.where(R!=1,R,np.nan),2)
        M[:][np.isnan(M[:])]=1.0 

        # Grab the frequencies
        freqs = self.msmeta.chanfreqs(spwin, 'GHz')

        # Simple scaling to abide by for plots to try somewhat keep default axes
        # but as we do a diag for each spw, scan, fld as the scaling spectrum is 
        # found, then we don't store or compare between and get an 'overall' 
        # range for plotting 
        maxVal=[1.0,1.02,1.05,1.1,1.2,1.3,1.4,1.5,2.0,2.5]
        try:
            plMax = [plM for plM in maxVal if plM >= R.max()][0]
        except IndexError:
            plMax = 3.0
            casalog.post('\n\tWARNING!!!!')
            casalog.post('\tUNREALISTICALLY HIGH RENORM VALUE FOUND!! THERE ARE LIKELY CORRELATOR \
                    ISSUES WITH SCAN '+str(scanin)+' OR THE FITTING HAS DIVERGED.\n')
        plMax = max(plMax, 1.02)
        plMin = min(R.min(), 0.995)
        
        # Grab the target name
        target = self.msmeta.namesforfields(fldin)[0]

        # Initialize the plot
        plt.close('all')
        plt.ioff()
        plt.clf()
        fig = plt.figure(figsize=(10,8))
        ax_rn = fig.add_subplot(111, frame_on=False)
        ax_rn.set_ylabel('Renorm Amplitude')
        ax_rn.set_xlabel('Frequency (GHz) (TOPO)')
        ax_rn.minorticks_on()

        # Initialize secondary x-axis and label channels in the correct direction
        ax_rn1 = ax_rn.twiny()
        ax_rn1.set_xlabel('Channel')
        if freqs[0] > freqs[-1]:
            ax_rn1.set_xlim(len(freqs),0)
        else:
            ax_rn1.set_xlim(0,len(freqs))
        ax_rn1.minorticks_on()

        # Plot renormalization spectrum for each antenna and correlation using 
        # different styles. Also plot median spectrum.
        corColor=['r','b']
        medColor=['k','g']
        medLine=[':','--']
        for iCor in range(R.shape[0]):
            for iAnt in range(R.shape[2]):
                ax_rn.plot(freqs, R[iCor,:,iAnt],c=corColor[iCor],alpha=0.5)
            ax_rn.plot(
                    freqs, 
                    M[iCor],
                    c=medColor[iCor],
                    linewidth='4',
                    linestyle=medLine[iCor]
                    )
            # If provided the original data, plot it in the background.
            if plot_original:
                ax_rn.plot(
                        freqs, 
                        M_atm[iCor], 
                        c='k', 
                        linewidth='2', 
                        linestyle='--', 
                        alpha=0.25, 
                        zorder=11
                        )

        # If supplied, plot a threshold line
        if threshline and threshline < plMax:
            ax_rn.plot(
                    [min(freqs), max(freqs)],
                    [threshline, threshline],
                    linestyle='-',
                    c='c',
                    linewidth='2'
                    )

        # Set the labels and such
        ax_rn.set_title(
                        self.msname+'\nTarget: '+target+' Spw: '+str(spwin)+' Scan: ' \
                        +str(scanin)+' Field: '+str(fldin),
                        {'fontsize': 'medium'}
                        )
        ax_rn.ticklabel_format(useOffset=False)
        ax_rn.set_xlim(min(freqs)*0.99999, max(freqs)*1.00001)
        ax_rn.set_ylim(plMin,plMax)

        # If True, then show a small yellow area indicating where the spectrum
        # has been blanked.
        if showExcluded:
            # Check to make sure it is not an empty input
            if self.rnstats['inputs']['excludechan']:
                # Check to make sure this spw has anything input, then grab the 
                # range and plot it.
                if str(spwin) in self.rnstats['inputs']['excludechan'].keys():
                    ranges = [
                            rng.strip() 
                            for rng in self.rnstats['inputs']['excludechan'][str(spwin)].split(';')
                            ]
                    starts, ends = self.interpretExcludechan(ranges, int(spwin))
                    for i in range(len(starts)):
                        ax_rn.axvspan(
                                freqs[starts[i]], 
                                freqs[ends[i]], 
                                ymin=0.05, 
                                ymax=0.06, 
                                facecolor='yellow', 
                                ec='black', 
                                zorder=0
                                )

        # If selected, plot the locations where the spectrum was divided during the fitting process
        if plotDivisions:
            dNchan = self.rnstats['inputs'][target][str(spwin)]['dNchan'][-1]
            nseg = self.rnstats['inputs'][target][str(spwin)]['num_segments'][-1]
            if nseg == 1:
                plotDivisions=False
            else:
                xlocs = [iseg*dNchan for iseg in range(1,nseg)]                    
                ax_rn.vlines(
                        freqs[xlocs], 
                        0.5, 
                        5, 
                        linestyles='dotted', 
                        colors='grey', 
                        alpha=0.5, 
                        zorder=10
                        )

        # If selected, plot the atmospheric transmission profile(s). Note that in Bands 9 and 10,
        # the image sideband atmospheric transmission profile is also plotted.
        if plotATM:
            # If renormalize(correctATM=True) was used, then the ATM profile already exists in
            # a dictionary, use that. Otherwise, grab the profile and image sideband if necessary.
            if len(self.atmtrans.keys())==0:
                if self.Band in [9, 10]:
                    ATMprof, ATMprof_imageSB = self.ATMtrans(scanin, spwin, verbose=False)
                else:
                    ATMprof=self.ATMtrans(scanin,spwin,verbose=False)
            else:
                if self.Band in [9,10]:
                    ATMprof, ATMprof_imageSB = self.atmtrans[target][str(spwin)][str(scanin)]
                else:
                    ATMprof=self.atmtrans[target][str(spwin)][str(scanin)]

            # Setup secondary y-axis using the same frequency axis and plot the profile(s).
            ax_atm = ax_rn.twinx()
            ax_atm.plot(freqs, 100.*ATMprof,c='m',linestyle='-',linewidth=2)

            if shadeAtm:
                # Find where the ATM lines are by fitting Lorentzian profiles
                atm_centers, atm_gammas = self.fitAtmLines(ATMprof, spwin)
                if self.Band in [9, 10]:
                    num_lines = len(atm_centers)
                    atm_centers_SB, atm_gammas_SB = self.fitAtmLines(ATMprof_imageSB, spwin)
                    atm_centers += atm_centers_SB
                    atm_gammas += atm_gammas_SB

                # For every ATM feature, plot a shaded area so that it is easy to distiguish.
                for i in range(len(atm_centers)):
                    # Set the ATM profile we want to report which might vary for Bands 9 and 10
                    profile = ATMprof
                    if self.Band in [9,10]:
                        if i >= num_lines:
                            profile = ATMprof_imageSB
                    # A Lorentizian has a width of gamma (which is != a Gaussian sigma!) 
                    # where 2*gamma is the FWHM. Here we go a bit further to capture most
                    # of the ATM feature that is above the noise. This is from my empirical
                    # estimates from datasets I've collected and seems to capture most of 
                    # signal without catching real signal for cases where an ATM line is 
                    # coincident (or nearly so) with a real line. 
                    #
                    # PIPEREQ-228 - Need to protect instances where the
                    # fitted feature starts or ends outside the spw!
                    atm_start = min(
                                    max(
                                        0, 
                                        int(atm_centers[i]-1.3*atm_gammas[i])
                                        ), 
                                    len(profile)-1
                                    )
                    atm_end = max(
                                    0, 
                                    min(
                                        int(atm_centers[i]+1.3*atm_gammas[i]), 
                                        len(profile)-1
                                        )
                                    )
                    if atm_start == atm_end:
                        continue
                    # Draw a shaded region where the line is
                    ax_atm.axvspan(
                            freqs[atm_start],
                            freqs[atm_end],
                            ymin=0, 
                            ymax=10,
                            alpha=0.2, 
                            facecolor='grey'
                            )

            if self.Band in [9, 10]:
                ax_atm.plot(freqs, 100.*ATMprof_imageSB, c='k', linestyle='-', linewidth=2)
            if self.Band in [9,10]:
                peak = max(np.maximum(ATMprof,ATMprof_imageSB)*100.)+10
                ax_atm.set_ylabel('ATM Transmission (%), Image Sideband')                
            else:
                peak = max(ATMprof*100.)+10
                ax_atm.set_ylabel('ATM Transmission (%)')                
            ax_atm.yaxis.set_label_position('right')
            
            # Avoid labelling values less than 0% since they have no physical meaning.
            # Enforce a range of 100
            atm_ymax = peak
            atm_ymin = peak-100
            ax_atm.set_ylim(atm_ymin, atm_ymax)

            ## Make sure that we don't label values that are less than 0 since that
            ## has no physical meaning.
            yvals = np.arange(round(atm_ymin), round(atm_ymax))
            yvals_mod = yvals%20
            ylabels = yvals[list(np.where(yvals_mod==0)[0])]
            ylabels_mask = ylabels >= 0 
            ax_atm.set_yticks(ylabels[ylabels_mask])
            ax_atm.set_yticklabels([str(int(ylbl)) for ylbl in ylabels[ylabels_mask]])
            
        # Save the plotted figure, setting up the plot directory if it doesn't already exist.
        if not os.path.exists('RN_plots'):
            os.mkdir('RN_plots')
        fnameM=self.msname+'_ReNormDiagnosticCheck_'+target+'_spw'+str(spwin) \
                +'_scan'+str(scanin)+'_field'+str(fldin)        
        plt.savefig('./RN_plots/'+fnameM+'.png')
        plt.close('all')

            
    def calcSetEdge(self,R,edge=0.01):
        # this changes the edge channels to the median value
        (lpcor,lpcha,lpant)=R.shape
        chlo=int(ceil(edge*lpcha))
        chhi=lpcha-1-chlo
        for lcor in range(lpcor):
            for lant in range(lpant):
                Rmed = np.median(R[lcor,:,lant])
                R[lcor,0:chlo,lant]=Rmed
                R[lcor,chhi:,lant]=Rmed

    def calcMaxConseq(self, linelist):
        cntConsec,maxConsec=0,0
        # loops the differnce of the channels and counts the maximal 
        # consecuitve amount - used later as a trigger to replace channels
        # 2021 Feb 16 added that a gap of 2 can be consecutive
        # as sometimes data are just fluctuating above the threshold 
        # better to do a full replace vs. piece-meal every other channel
        for diffVal in np.diff(linelist):
            cntConsec = cntConsec + 1 if diffVal == 1 or diffVal == 2 else 0
            maxConsec = max(cntConsec,maxConsec)

        return maxConsec


    def ATMtrans(self, iscan, ispw, ifld=None, verbose=False):
        """
        Purpose:
            This function will return the atmospheric model for the input spectral window
            and scan. In the case of a Band 9 or 10 spectral window, the atmospheric model
            for the image sideband is also returned. 

        Inputs:
            iscan : int
                The user input scan from which to get the atmospheric model. This is used
                to find the observation time, elevation, and weather information.

            ispw : int
                The user input spectral window.

            ifld : int : OPTIONAL
                The user input field ID. This is optional but in the case of mosaics, can
                make the atmospheric model slightly more accurate. 
                Default: None

            verbose : boolean : OPTIONAL
                Setting to True will output additional information as the atmospheric profile 
                is calculated.
                Default: False
        
        Returns:
            transmission: numpy.array
            (transmission, transmission_SB): numpy.arrays : ONLY FOR BANDS 9 AND 10
                This is a 1-D array with the same length as the number of channels of the input 
                scan/spw. The values of the array consist of the atmospheric transmission by 
                channel as a fraction between 0.0 and 1.0.

        """
        if verbose:
            casalog.post('  Getting ATM transmission profile for spw='+str(ispw)+' and scan='+str(iscan))

        # Input iscan should be type(int) but if not, attempt to use the first index.
        if type(iscan) is list:
            iscan=iscan[0]

        # Find the field if none supplied
        if ifld is None:
            # Find all fields associated with this scan
            ifld = self.msmeta.fieldsforscan(iscan)
            # If there is more than 1 field, it is a mosaic. Take the central field value, hoping
            # that it is near the center...
            if len(ifld) > 1:
                if verbose:
                    casalog.post('\tInput scan is a mosaic but no field was supplied. Will attempt to use '+ \
                        'central field number.\n')
                ifld = int(floor(np.median(ifld)))
            else:
                ifld = int(ifld[0]) 

        # Fixed parameters adopted from Todd's aU and plotbandpass3 code
        # - could make options later but probably not required for what we need this function to do
        dP=5.0
        dPm=1.1
        maxAltitude=60.0
        h0=1.0
        atmType =  1
        nbands = 1
        telescopeName = 'ALMA'

        # Get x-axis information
        #
        # Hanning implementation: 
        # We want to get the full resolution ATM model and then hanning smooth it ourselves
        # so we need to specify the correct number of channels. However, we need to be careful
        # about paying attention to the cycle. Pre-cycle 3 data does not have any way of reliably
        # getting the binning factor. So for those datasets we will want to just use the same
        # number of channels and deal with it. For everything else, we will get the binning 
        # factor from the MS and apply that binning later. 
        #binningFactor = self.onlineBinningFactor()[ispw]
        #numchan = self.msmeta.nchan(ispw)*binningFactor
        #freqs = np.linspace(
        #                self.msmeta.chanfreqs(ispw,'GHz')[0],
        #                self.msmeta.chanfreqs(ispw,'GHz')[-1],
        #                numchan
        #            )
        #
        # No benefit was found from implementing Hanning smoothing of the profile, so just
        # using the profile as is.
        freqs = self.msmeta.chanfreqs(ispw, 'GHz')
        numchan = self.msmeta.nchan(ispw)
        reffreq=0.5*(freqs[int(numchan/2)-1]+freqs[int(numchan/2)])

        # Get some metadata information about the scan. We need the sky location in Azimuth 
        # and Elevation to know the airmass contribution. Here we use the median scan time to
        # calculate the Elevation, assuming that scan times aren't ridiculously long, that should
        # be good enough for the entire scan. The times will also help us read the weather
        # tables to get the right PWV values out. 
        mydirection=self.renormradec2rad(self.renormdirection2radec(self.msmeta.phasecenter(ifld))) 
        scanTimes = self.msmeta.timesforscan(iscan)
        myscantime = np.median(scanTimes) 
        scanLength = scanTimes[-1] - scanTimes[0]        
        casalog.filterMsg('Position:') # message filter as this prints ALMA's position each call
        azel=self.renormcomputeAzElFromRADecMJD(mydirection,myscantime/86400.)
        casalog.clearFilterMsgList()
        airmass = 1.0/np.cos((90.-azel[1])*np.pi/180.)

        # Get weather results from the MS tables and populate variables from results.
        weatherResult = self.renormWeather(iscan, verbose=False) 
        P= weatherResult[0]['pressure']
        H= weatherResult[0]['humidity']
        T= weatherResult[0]['temperature']+273.15
        # Sometimes weather values passed are zeros, even if going through the full weather code.
        # In those cases, set to the default values. 
        if P == 0:
            P = 563.0
        if H == 0:
            H = 20.0
        if T == 0:
            T = 273.15    

        # Get the median PWV. 
        # The PWV measurements are taken before or after scans, so we create a timerange over
        # which to look for those measurements and take the median of those found values. 
        # If none are found, then we fall back to using the PWV for the Band from the 
        # sensitivity calculator.
        timerange = [scanTimes[0] - scanLength / 2., scanTimes[0] + scanLength/2.]
        pwvmedian=self.renormMedianPWV(timerange, verbose=False) 
        if pwvmedian == 0:
            self.corrATM=False
            # get the band and then set these from quartiles
            pwvmedian = self.usePWV[self.Band] # so at least it will make resonable plots

        # Need the inputs to be in a specific form (dictionary) which the quanta tool does for us.
        myqa = qatool()

        # Gather frequency information into the form we'll need to make the model.
        # For Bands 9 and 10, the spectral window also has atmospheric contributions from the 
        # image sideband which we must also calculate. 
        if self.Band in [9,10]:
            nbands = 2
            freqs_SB, chansep_SB, center_SB, width_SB = self.getImageSBFreqs(ispw)
            fCenter = myqa.quantity([reffreq, center_SB],'GHz')
            chansep=(freqs[-1]-freqs[0])/(numchan-1)
            fResolution = myqa.quantity([chansep, -chansep],'GHz')
            fWidth = myqa.quantity([numchan*chansep, numchan*-chansep],'GHz')
        else:
            fCenter = myqa.quantity(reffreq,'GHz')
            chansep=(freqs[-1]-freqs[0])/(numchan-1)
            fResolution = myqa.quantity(chansep,'GHz')
            fWidth = myqa.quantity(numchan*chansep,'GHz')

        # Setup the CASA atmosphere tool and generate the model. 
        # Note this is more or less from inside Todd's aU of CalcAtmTransmission
        # from Plotbandpass3.py code
        myat=attool()
        ATMresult = myat.initAtmProfile(humidity=H, temperature=myqa.quantity(T,"K"),
                                 altitude=myqa.quantity(5059,"m"),
                                 pressure=myqa.quantity(P,'mbar'),
                                 atmType=atmType,
                                 h0=myqa.quantity(h0,"km"),
                                 maxAltitude=myqa.quantity(maxAltitude,"km"),
                                 dP=myqa.quantity(dP,"mbar"),
                                 dPm=dPm)
        # CASA 5 vs 6 check
        if type(ATMresult) == tuple:
            ATMresult = ATMresult[0]

        # This sets the frequency information and the PWV measurement
        myat.initSpectralWindow(nbands,fCenter,fWidth,fResolution)
        myat.setUserWH2O(myqa.quantity(pwvmedian,'mm'))

        # Now calculate the model based on inputs provided above and get the transmission out
        dry = np.array(myat.getDryOpacitySpec(0)[1]) # CO, O3, etc. 
        wet = np.array(myat.getWetOpacitySpec(0)[1]['value']) # water absorption
        transmission = np.exp(-airmass*(wet+dry)) # e^-tau; 
        if self.Band in [9,10]:
            dry_SB = np.array(myat.getDryOpacitySpec(1)[1])
            wet_SB = np.array(myat.getWetOpacitySpec(1)[1]['value'])
            transmission_SB = np.exp(-airmass*(wet_SB+dry_SB))

        # Close the tools
        myat.close()
        myqa.done()

        # caution, there is a netsideband affect - LSB and USB
        # if netsideband %2 == 0 then sense = 2, else 1
        # if 1, the trasnsmission flips over the loop in channels 

        mytb.open(self.msname+'/SPECTRAL_WINDOW')
        refFreq = mytb.getcol('REF_FREQUENCY')
        net_sideband = mytb.getcol('NET_SIDEBAND')
        measFreqRef = mytb.getcol('MEAS_FREQ_REF')
        spwname=mytb.getcol('NAME')
        mytb.close()

        sense=0
        # LSB are =1, USB are +2 - lower need reversing
        if refFreq[ispw]*1e-9>np.mean(freqs):
            if net_sideband[ispw] % 2 == 0:
                sense = 1
            else:
                sense = 2

        if sense == 1:
            if verbose:
                casalog.post('********* REVERSING THE ATM FOUND FOR LSB ***********')
            transmission = transmission[::-1] # reverse the order
            if self.Band in [9, 10]:
                transmission_SB = transmission_SB[::-1]
        
        # Hanning implementation:
        # Smooth the calculated transmission profile by a Hanning kernel
        # and then "decimate" the smoothed signal back down to the correct
        # number of channels. The Hanning smoothing sometimes drops the 
        # edge channels which doesn't actually affect the data (even if it's
        # applied) but looks bad. So we set the edge channels equal to the 
        # one next to it so it looks better.
        #transmission = np.convolve(transmission, [0, 0.25, 0.5, 0.25, 0], mode='same')
        #transmission = transmission[::binningFactor]
        #transmission[0] = transmission[1]
        #transmission[-1] = transmission[-2]
        #if self.Band in [9, 10]:
        #    transmission_SB = np.convolve(transmission_SB, [0, 0.25, 0.5, 0.25, 0], mode='same')
        #    transmission_SB = transmission_SB[::binningFactor]
        #    transmission_SB[0] = transmission_SB[1]
        #    transmission_SB[-1] = transmission_SB[-2]

        if self.Band in [9, 10]:
            return np.array(transmission), np.array(transmission_SB)
        else:
            return transmission

    def onlineBinningFactor(self):
        """
        Purpose:
            Return the online channel binning factor for the relevant spectral
            windows. Note that for early data (<Cycle 3) this will return the 
            wrong values (i.e. it always returns 1).

            This is only used in the Hanning smoothing application of ATM 
            corrections which is currently not implemented.
        """
        mytb.open(self.msname+'/SPECTRAL_WINDOW')
        bins = mytb.getcol('SDM_NUM_BIN')
        mytb.close()
        return bins


    # LM added 
    def ATMcorrection(self,R,inscan, inspw, infld, calscan, calname, verbose=False):
        # this is the code that will do the real difference between the 
        # bandpass and the input target and make a correction to the 
        # R, i.e the target AC / Bp AC
        
        # R is the AC input
        if verbose:
            casalog.post(' ***** Doing an ATM transmission correction for the ATM line in spw'+str(inspw)+' scan'+str(inscan)+' field'+str(infld)+'*****')

        # crude theory here - just correct back in by the BP (or phase) and Target model transmission curves 
        # R is Tar / bandpass   (or phase)
        # so we multiply back in the ATM ? 
        fldnam=self.msmeta.namesforfields(infld)[0]  

        # imporved way - get ratio spectrum to find actually differences and then median outside ant loop
        #
        # AL - Is this the right order??? Souldn't it be Bandpass/Target?? Also, does it make sense to subtract the medians 
        #      first rather than multiplying the ratio and then subtract the median?
        #       - The order is indeed correct. The ACs are basically Tsys measurements which are a 
        #         measure of the sky brightness which has an atmospheric contribution equal to e^tau.
        #         Therefore, to mitigate this effect, we must multiply by e^-tau which we estimate
        #         via the atmospheric model. 
        #       - I do think that using the median causes ill effects and is the wrong thing to do.
        #         It ends up changing the line peaks even away from the atm features. Therefore, I 
        #         removed it. 
        #    - There are additional effects that are still present because 1) the models aren't perfect
        #      but 2) there are additional terms for when a line is located in an atm line because that
        #      line is being attenuated by e^-tau. Currently, all we are doing is the simple first step.
        #ratioATM = self.atmtrans[fldnam][str(inspw)][str(inscan)] / self.atmtrans[calfld][str(inspw)][calscan]
        if self.Band in [9,10]:
            sidebands=2
        else:
            sidebands=1
        for i in range(sidebands):
            if sidebands == 2:
                trg_atm = self.atmtrans[fldnam][str(inspw)][str(inscan)][i]
                cal_atm = self.atmtrans[calname][str(inspw)][str(calscan)][i]
            else:
                trg_atm = self.atmtrans[fldnam][str(inspw)][str(inscan)]
                cal_atm = self.atmtrans[calname][str(inspw)][str(calscan)]                
            ratioATM = trg_atm/cal_atm 
            #ratioMed = np.array(np.median(ratioATM))
            # shift to baseline of average 1.0
            #ratioATM = ratioATM + (1.0 - ratioMed)

            for jcor in range(R.shape[0]):
                for lpant in range(R.shape[2]):
                    # TESTING casalog.post('ATM correcting corr '+str(jcor)+' antenna '+str(lpant))
                    
                    # simple correction, the target is attenuated by its own ATM, so we multiply back
                    # whereas "R" here is Tar_AC/BP_AC and so to correct the BP attenuated by its own
                    # ATM profile we have to divide by BP ATM
                    # essntially we are multiplying R by (Tar_ATM/BP_ATM) - NB first test was doing just that and result were actually good
                    ## OLD FIRST WAY R[jcor,:,lpant] = R[jcor,:,lpant] * (self.atmtrans[fldnam][str(inspw)][str(inscan)] / self.atmtrans[calfld][str(inspw)][calscan])
                    R[jcor,:,lpant] = R[jcor,:,lpant] * ratioATM

                    # - improved method - set already R to ~1.0 median and the ATM ratio spectrum (done above), then correct (more notes below)
                    #medR = np.array(np.median(R[jcor,:,lpant]))
                    #R[jcor,:,lpant] = (R[jcor,:,lpant]+(1.0-medR)) * ratioATM                

        # LM notes:

        # first crude tests:
        # applying the correction directly as the ratio only between Tar_ATM/BP_ARM as
        # a multiplication accross the entire SPW will act to change the scaling
        # from real astronomical lines. e.g.,
        # - imagine an scaling spectrum (Tar_AutoCorr/BP_AutoCorr) with a baseline at ~0.9, an astronomical line that peaks at 1.2 
        # in the middle of the SPW, and away from the ATM line that causes a dip down to 0.8 at the upper edge
        # if one were to fit this to obtain the scaling spectrum, the peak rescaling is 1.3, i.e. 1.0-0.9 = 0.1 + 1.2 peak.
        # - consider the ratio of ATM transmission (Tar_ATM/BP_ATM) is ~0.95 AWAY from the ATM line and 1.15 at the ATM feature
        # this means the baseline of the scaling spectrum * ratio ATM transmission becomes 0.9 * 0.95 -> 0.855
        # while the peak astronomical line will become 1.2 * 0.95 -> 1.14
        # the ATM line would be almost entirely corrected out, i.e 0.8 * 1.1 -> 0.88 - close to the new baseline
        # however now fitting this spectrum, the baseline to peak astronomical line is 1.0-0.855 + 1.14 = 1.285
        # thus the overall peak resacling as reduced by 1.3 - 1.285 = 0.015, 1.5%
        # -- the point is the ATM ratio, and the Tar_AC/BP_AC spectra are not baselined, the have 
        #    an aribtary baseline y scale.

        ## anyway, better to get average and apply
        # (i) shift all "R" scaling spectra to a baseline of ~1 (i.e. like first part of fitting code does anyway)
        # (ii) shift the ATM trans ratio correction to a baseline of 1
        # (iii) apply the multiplication, only regions close to the ATM line, i.e. not = 1 will
        #       but changed - i.e. more-or-less correcting out the ATM line residual between target and BP
        #  there is some low level 'shape' remainig in the ATM line channels -  but 
        #  this correction is made before the fitting, which goes 'bad/divergent' when ATM lines exist as
        # lorenzian like profiles and cannot be fitted out - this code doesn't need to fully correct 
        # the ATM profile, it should just remove most of the ATM residual differences so the rest of the 
        # renorm code can handle fitting the scaling spectrum
        
        # option, maybe set the ATM ratio spectrum to 1.0 i.e. no scaling to any channels
        # except where the ATM transmission is deeper than a given value ?
        
        # TESTING TO CODE - i.e. run with and without ATM on many data and see effect on % scaling

    # No return acts on the rescaling spectrum directly 

    def suggestAtmExclude(self, target, spw, return_command=False, return_dict=False):
        """
        Purpose:
            Given that renormalization() has been run with the checkFalsePositives=True
            flag, the self.atmMask attribute has been filled with information where
            there are atmospheric features in the spectrum. This function will merge
            the masks from all scans and suggest channel ranges that one may input back
            into renormalization() using the excludechan parameter. 

        Inputs:
            target : string
                The target source evaluated in renormalization()

            spw : string
                The spectral window you wish to evaluate a mask for assuming that it
                was also run in the renormalization() call. 

            return_command : boolean : OPTIONAL
                If set to True, then a string is returned with the correct syntax to
                put into the excludechan option of a self.renormalization() call.
                Default: False

            return_dict : boolean : OPTIONAL
                If set to True, then a dictionary is returned with the correct syntax
                to fill the excludechan parameter of the self.renormalize() method.
                Default: False
                NOTE: This takes priority over return_command if both are set to True.

        Outputs:
            A list of list pairs suggesting ranges for input into the excludechan 
            option of renormalization().
        """


        def subsets(chans):
            """
            Purpose: Find subset ranges within an array of consecutive values and
                     return the sub-ranges.
            """
            class groupby:
                # A subset task of the itertools package.
                # https://docs.python.org/3/library/itertools.html#itertools.groupby

                # [k for k, g in groupby('AAAABBBCCDAABBB')] --> A B C D A B
                # [list(g) for k, g in groupby('AAAABBBCCD')] --> AAAA BBB CC D
                def __init__(self, iterable, key=None):
                    if key is None:
                        key = lambda x: x
                    self.keyfunc = key
                    self.it = iter(iterable)
                    self.tgtkey = self.currkey = self.currvalue = object()

                def __iter__(self):
                    return self

                def __next__(self):
                    self.id = object()
                    while self.currkey == self.tgtkey:
                        self.currvalue = next(self.it)    # Exit on StopIteration
                        self.currkey = self.keyfunc(self.currvalue)
                    self.tgtkey = self.currkey
                    return (self.currkey, self._grouper(self.tgtkey, self.id))

                def _grouper(self, tgtkey, id):
                    while self.id is id and self.currkey == tgtkey:
                        yield self.currvalue
                        try:
                            self.currvalue = next(self.it)
                        except StopIteration:
                            return
                        self.currkey = self.keyfunc(self.currvalue)

            def itemgetter(*items):
                # A subset task of the operator package.
                # https://docs.python.org/3/library/operator.html#operator.itemgetter
                if len(items) == 1:
                    item = items[0]
                    def g(obj):
                        return obj[item]
                else:
                    def g(obj):
                        return tuple(obj[item] for item in items)
                return g

            def consecutive_groups(iterable, ordering=lambda x: x):
                """
                A subset of the more-itertools package.
                https://more-itertools.readthedocs.io/en/stable
                        /_modules/more_itertools/more.html#consecutive_groups

                Yield groups of consecutive items using :func:`itertools.groupby`.
                The *ordering* function determines whether two items are adjacent by
                returning their position.

                By default, the ordering function is the identity function. This is
                suitable for finding runs of numbers:

                    >>> iterable = [1, 10, 11, 12, 20, 30, 31, 32, 33, 40]
                    >>> for group in consecutive_groups(iterable):
                    ...     print(list(group))
                    [1]
                    [10, 11, 12]
                    [20]
                    [30, 31, 32, 33]
                    [40]

                For finding runs of adjacent letters, try using the :meth:`index` method
                of a string of letters:

                    >>> from string import ascii_lowercase
                    >>> iterable = 'abcdfgilmnop'
                    >>> ordering = ascii_lowercase.index
                    >>> for group in consecutive_groups(iterable, ordering):
                    ...     print(list(group))
                    ['a', 'b', 'c', 'd']
                    ['f', 'g']
                    ['i']
                    ['l', 'm', 'n', 'o', 'p']

                Each group of consecutive items is an iterator that shares it source with
                *iterable*. When an an output group is advanced, the previous group is
                no longer available unless its elements are copied (e.g., into a ``list``).

                    >>> iterable = [1, 2, 11, 12, 21, 22]
                    >>> saved_groups = []
                    >>> for group in consecutive_groups(iterable):
                    ...     saved_groups.append(list(group))  # Copy group elements
                    >>> saved_groups
                    [[1, 2], [11, 12], [21, 22]]

                Borrowed for PL use from https://pypi.org/project/more-itertools/
                """
                for k, g in groupby(
                    enumerate(iterable), key=lambda x: x[0] - ordering(x[1])
                ):
                    yield map(itemgetter(1), g)

            subsets = [list(group) for group in consecutive_groups(chans)]
            ranges = []
            for ss in subsets:
                ranges.append([min(ss), max(ss)])
            return ranges

        # Get list of scans that were evaluated
        scans = list(self.atmMask[target][spw].keys())

        # We need to compile a "complete" atmospheric mask. Since the objects may set throughout
        # the course of an observation, the atmospheric profile can get worse, leading to a 
        # slightly different profile with wider wings. This compiles all the atmospheric masks
        # together into a single, full mask. 
        #
        # Note that the self.atmMask arrays are set to True where atmospheric features were found
        # and False otherwise.
        full_mask = self.atmMask[target][spw][scans[0]]
        if len(scans) > 1:
            for scan in scans[1:]:
                full_mask = np.logical_or(full_mask, self.atmMask[target][spw][scan])

        # With the full mask, get the channel numbers where there are atmospheric features and 
        # calculate the channel ranges of the features.
        atm_channels = np.where(full_mask == True)[0]
        ranges = subsets(atm_channels)

        # Return either the ranges themselves or a flagging command compiling the ranges.
        if return_dict:
            if len(ranges) == 0:
                return {str(spw):''}
            elif len(ranges) > 1:
                full_range = ''
                for rng in ranges:
                    full_range+=str(rng[0])+'~'+str(rng[1])+';'
                return {str(spw): full_range[:-1]}
            else:
                return {str(spw): str(ranges[0][0])+'~'+str(ranges[0][1])}
        elif return_command:
            if len(ranges) == 0:
                return 'No flagging suggested.'
            elif len(ranges) > 1:
                cmd = 'excludechan={"'+str(spw)+'":"'
                for rng in ranges:
                    cmd += str(rng[0])+'~'+str(rng[1])+';'
                cmd = cmd[:-1] + '"}'
            else:
                cmd = 'excludechan={"'+str(spw)+'":"'+str(ranges[0][0])+'~'+str(ranges[0][1])+'"}'
            return cmd
        else:
            return ranges

    def recordApply(self, scanout=None, spwout=None, fldout=None):
        """
        Purpose:
            This will write to the history of the dataset upon application of
            renorm (i.e. self.renormalize(docorr=True)). Within the 
            renormalize() method, a check of the MS HISTORY table is performed
            to ensure that double application is avoided. 

        Inputs:
            scanout, spwout, fldout : int : OPTIONAL
                The scan, spw, and field numbers where renormalization was 
                applied. If set to None then a generic message is filled.
            Default: None
        """
        if (scanout is not None) and (spwout is not None) and (fldout is not None):
            messageIn = 'ReNormalization correction applied to spw'+str(spwout) \
                    +' scan'+str(scanout)+' field'+str(fldout)+' '+self.RNversion
        else:
            messageIn = 'ReNormalization correction applied '+self.RNversion
        myms.open(self.msname)
        myms.writehistory(messageIn)
        myms.close()

    def checkApply(self):
        # just need to get the history column
        # appears that list history is useless and only writes to the logger ?
        mytb.open(self.msname+'/HISTORY')
        messageOut = list(mytb.getcol('MESSAGE'))
        applyStatus = False # i.e. assume not applied
        for messLine in messageOut:
            # only need to check for this statement
            if 'ReNormalization' in messLine:
                applyStatus = True
        mytb.close()

        return applyStatus

    def getband(self,freq):
        """ 
        Identify the Band for specific frequency (in GHz)

        Note that the Band 2/3 identification is currently not
        guaranteed to be correct.
        """
        lo=np.array([35,67,84,125,157,211,275,385,602,787])*1e9
        hi=np.array([50,83.999999999,116,163,212,275,373,500,720,950])*1e9

        return np.arange(1,len(lo)+1)[(freq>lo)&(freq<hi)][0]

    def writeTsysTemps(self, dictIn=None, rettemplist=False):
        """
        Purpose:
            Uses the dictionary from the Tsys Plotting code and will return a
            list with each element being a line to be entered into the 
            tsystemplate in PL style

            Optionally, an input dictionary in correct syntax can be passed. 
          
            Using retflchan=True in the plotRelTsysSpectra() method will save
            the dictionary to the argument of the call if you want a manual 
            view of the dict or to see and edit and input manually into this 
            flag_lines code.

            NOTE: This is to be used with caution as it has not been validated
            yet and is mainly for helping with manual DR only!!!
        
        Inputs:
            dictIn : dictionary : OPTIONAL
                If desired, a user may input a dictionary of flags. If one is
                not input, then the output of self.plotRelTsysSpectra() is used
                instead.
                Default: None

            rettemplist : boolean : OPTIONAL
                If set to True, then the flags constructed during this script, 
                in addition to being written to a file via self.writeTsysFlags(),
                will be returned as a list of strings.

        Outputs:
            A file of Tsys flags is written via the self.writeTsysFlags() method.
            Additionally, if rettemplist=True, a list of strings is returned that
            contains all the flagging commands.
       
        Suggestions for Improvement:
            - Syntax is not currently explicity checked for the dictIn option. 
        """
        if dictIn:
            if type(dictIn) is not dict:
                casalog.post('Option dictIn must be a dictionary!')
                raise TypeError(
                        'Option dictIn of writeTsysTemps() must be a dictionary.'
                        )
        else:
            dictIn = self.TsysReturn
            
        listToKeep=[]
        TDM=True
        for keyuse in dictIn.keys():
            spwKeep = keyuse.split('=')[-1]
            # then sub layer is the scan key - not intent based its 
            # code based on Tsys TARGET intent scans 
            scanKeys = dictIn[keyuse].keys()
            # check if the Tsys are in same SPW setup, i.e. in the fdmspws, then ACA 
            # and uses a slightly different option in channel_ranges funct.
            if int(spwKeep) in self.fdmspws:
                TDM=False
            for scankeyuse in scanKeys:
                scanKeep= scankeyuse.split('=')[-1]
                # pass to channel_ranges which will do some consolidation and buffering (i.e. few extra channels)
                chanRans = self.channel_ranges(dictIn[keyuse][scankeyuse], TDM=TDM)
                for chanranlp in chanRans:
                    spwStrUse = str(spwKeep)+':'+str(chanranlp[0])+'~'+str(chanranlp[1])

                    listToKeep.append("mode=\'manual\' spw=\'{}\' scan=\'{}\' reason=\'QA2:tsysflag_tsys_channel\'".format(spwStrUse,scanKeep))   

        # already write the file here for the logs
        self.writeTsysFlags(listToKeep)

        # passes out template list to argument as option
        if rettemplist:
            return listToKeep  

    def channel_ranges(self, channels, TDM=True):
        """
        Purpose:
            Given a list of channels, this will return a list of ranges that 
            describe them accounting for a buffer and gap in input that can be assumed 
            as consecutive.
        """
        channels.sort()
        
        if TDM:
            addChan = 1
            gapChan = 3
        else:
            # To combat piece wise flags due to channels triggering just around
            # the threshold, set extra buffers.
            addChan = 5
            gapChan = 10 

        channel_range = [channels[0]-addChan, channels[0]] # gives a wider flag buffer 

        for i, chan in enumerate(channels):
            # checks if a gap of 3 and assumes continous
            if chan <= channel_range[1] + gapChan: 
                # gives a wider buffer to written flag
                channel_range[1] = chan+addChan 
            else:
                # for discountinuty will call this funct again and 'appends' 
                return [channel_range] + self.channel_ranges(channels[i:])

        # get here if last channel reached
        return [channel_range]

    def writeTsysFlags(self, tsys_flags):
        """
        Purpose:
            Given the list from the writeTsysTemps method that makes a list of 
            flag template compatible strings, this code simply writes a PL 
            style flag template file at the most detailed level, i.e. spw and 
            channel per scan triggered.
        """
        with open(self.msname+'_ReNormflagtsystemplate.txt', 'w') as file_tsys:
            # loop over list and write
            for tsys_flag in tsys_flags:
                file_tsys.write(tsys_flag+'\n')

    def renormcomputeAzElFromRADecMJD(self, raDec, mjd):
        """
        Purpose:
            Computes the azimuth and elevation for a specified RA, Dec, and MJD 
            for ALMA observations using the CASA measures tool.

            Adapted from Todd Hunter's AU tool computeAzElFromRADecMJD.            
        
        Inputs:
            raDec : list or tuple
                Must be in radians: [ra,dec]

            mjd : float
                Must be in days.

        Output:
            The azimuth and elevation are returned as a list in degrees.
        """
        # Open CASA tools and define relevant quantities
        myme = metool() 
        myqa = qatool() 
        frame='AZEL'
        raQuantity = myqa.quantity(raDec[0], 'rad')
        decQuantity = myqa.quantity(raDec[1], 'rad')
        mydir = myme.direction('J2000', raQuantity, decQuantity)
        myme.doframe(myme.epoch('mjd', myqa.quantity(mjd, 'd')))
        observatory='ALMA'
        myme.doframe(myme.observatory(observatory))  
        myqa.done()
        myazel = myme.measure(mydir,frame)
        myme.done()

        # Once the inputs and frames are specified, pull out solution.        
        myaz = myazel['m0']['value']
        myel = myazel['m1']['value']
        
        # Want output in Degrees 
        myaz *= 180/np.pi
        myel *= 180/np.pi

        return([myaz,myel])



    def renormradec2rad(self, radecstring):
        """
        Purpose:
            Convert a sky position from a single RA/Dec sexagesimal string 
            to RA and Dec in radians.

            Adapted from Todd Hunter's AU tool radec2rad.

        Inputs:
            radecstring: string
                Any leading 'J2000' string is removed before consideration. 
                
                The RA and Dec portions can be separated by a comma or a space.
                
                The RA portion of the string must be colon-delimited, space-
                delimited, or 'h/m/s' delimited. 
                
                The Dec portion of the string can be either ":", "." or space-
                delimited. If it is "." delimited, then it must have degrees, 
                minutes, *and* seconds.

        Returns:
            The RA and Dec are retuned as a tuple in radians.
        """
        # Strip off J2000 leading string if it exists.
        if radecstring.find('J2000')==0:
            radecstring = radecstring.replace('J2000','')
        
        # If in hms form, replace to ":" separated form.
        if (radecstring.find('h')>0 and radecstring.find('d')>0):
            radecstring = radecstring.replace(
                                            'h', ':'
                                            ).replace(
                                                    'm', ':'
                                                    ).replace(
                                                            'd', ':'
                                                            ).replace(
                                                                    's',''
                                                                    )
        
        # Create a separation between RA and Dec, then split on spaces to find
        # what type of input was used.
        radec1 = radecstring.replace(',',' ')
        tokens = radec1.split()
        
        # If input was not space separated, we have RA then Dec. If input was 
        # space separated, we have RA and Dec split over 6 items. Otherwise, 
        # bad input and reject it.         
        if (len(tokens) == 2):
            (ra,dec) = radec1.split()
        elif (len(tokens) == 6):
            h,m,s,d,dm,ds = radec1.split()
            ra = '%s:%s:%s' % (h,m,s)
            dec = '%+f:%s:%s' % (float(d), dm, ds)
        else:
            casalog.post("Invalid format for RA/Dec string: ", radec1)
            raise SyntaxError('Invalid format for RA/Dec string: '+str(radec1))
        
        # Evaluate found RA/Dec input which at this point we know is in ":" format. 
        # Strip any errant spaces and split on ":" to get each portion. To get to 
        # degrees, we need to follow: 
        #     ra_degrees = 15 * (ra_hours + ra_minutes/60 + ra_seconds/3600)
        #     dec_degress = dec_degrees + dec_minutes/60 + dec_seconds/3600
        tokens = ra.strip().split(':')
        hours = 0
        for i,t in enumerate(tokens):
            hours += float(t)/(60.**i)
        ra1 = hours*15
        # We don't yet know if Dec is in "." or ":" format so account for both.
        if (dec.find(':') > 0):
            tokens = dec.lstrip().split(':')
        elif (dec.find('.') > 0):
            try:
                (d,m,s) = dec.lstrip().split('.')
            except:
                (d,m,s,sfraction) = dec.lstrip().split('.')
                s = s + '.' + sfraction
            tokens = [d,m,s]
        else:  # just an integer
            tokens = [dec]
        dec1 = 0
        for i,t in enumerate(tokens):
            dec1 += abs(float(t)/(60.**i))
        # Account for possibility that Dec can be negative.
        if (dec.lstrip().find('-') == 0):
            dec1 = -dec1
        
        # Convert degrees to radians.
        decrad = dec1*np.pi/180.
        rarad = ra1*np.pi/180.
  
        return(rarad,decrad)


    def renormdirection2radec(self, direction):
        """
        Purpose:
            Convert a direction dictionary to a sexadecimal string of format:
                HH:MM:SS.SSSSS, +DDD:MM:SS.SSSSSS

            Adapted from Todd Hunter's AU tool direction2radec.

        Inputs:
            direction : dictionary
                This input is what is returned from the msmd tool of a pointing,
                i.e. the return of msmdtool().phasecenter.

        Outputs: 
            The RA, Dec pointing direction as a string in sexadecimal format.
        """
        ra  = direction['m0']['value']
        dec = direction['m1']['value']
        myqa = qatool()
        prec = 5
        mystring = '%s, %s' % (myqa.formxxx(
                                    '%.12frad'%ra,
                                    format='hms',
                                    prec=prec
                                    ),
                               myqa.formxxx(
                                   '%.12frad'%dec,
                                   format='dms',
                                   prec=prec
                                   ).replace('.',':',2))
        myqa.done()
   
        return(mystring)



    def renormWeather(self, scan, verbose=False): 
        ''' 
        Purpose:
            Returns the weather conditions and time stamps of a given scan. 
            This is heavily adapted from Todd Hunter's analysisUtils code,
            au.getWeather() function.

        Inputs:
            scan : integer
                The scan number.

            verbose : boolean : OPTIONAL
                If set to true, some additional output is printed to screen.

        Returns:
            [conditions, myTimes]
                conditions : dictionary
                    Includes the average temperature (Celcius), humidity (%), 
                    and Pressure (mB) for the given scan.

                myTimes : numpy.array
                    List of times during the given scan.
                    
        '''
        # Use a standard preferred weather station (as noted in AU task).
        preferredStation = 'TB2'
        conditions = {}
        conditions['pressure']=conditions['temperature']=conditions['humidity'] = 0
        myTimes = self.msmeta.timesforscan(scan)
        
        # Get the weather table.
        try:
            # mytb is global tool instance already
            mytb.open(self.msname+'/WEATHER')  
        except:
            casalog.post("Could not open the WEATHER table for this ms, default returned.")
            conditions['pressure']=563.0
            conditions['temperature']=0.0 # in deg C
            conditions['humidity'] = 20.0
            return([conditions,myTimes])
        
        # Get all weather information.
        mjdsec = mytb.getcol('TIME')
        indices = np.argsort(mjdsec) # sometimes slightly out of order, fix.
        pressure = mytb.getcol('PRESSURE')
        relativeHumidity = mytb.getcol('REL_HUMIDITY')
        temperature = mytb.getcol('TEMPERATURE')
        # If in units of Kelvin, convert to C
        if (np.mean(temperature) > 100):
            temperature = temperature-273.15 

        # Apply correct ordering
        mjdsec = np.array(mjdsec)[indices]
        pressure = np.array(pressure)[indices]
        relativeHumidity = np.array(relativeHumidity)[indices]
        temperature = np.array(temperature)[indices]
        # Grab weather station IDs.
        if 'NS_WX_STATION_ID' in mytb.colnames():
            stations = mytb.getcol('NS_WX_STATION_ID')
        else:
            stations = None
        mytb.close()
        
        # Get the weather station names.
        wsdict = self.renormWeatherStationNames()
        if wsdict is not None:
            preferredStationID = None
            # Loop over weather stations, searching for the preferred.
            for w in list(wsdict.keys()):
                if wsdict[w].find(preferredStation) >= 0:
                    preferredStationID = w
            # If preferred found, use only data from that one, otherwise use all.
            if preferredStationID is None:
                if verbose:
                    casalog.post("Preferred station (%s) not found in this dataset. Using all." % (preferredStation))
            else:
                indices = np.where(stations == preferredStationID)
                mjdsec = np.array(mjdsec)[indices]
                pressure = np.array(pressure)[indices]
                relativeHumidity = np.array(relativeHumidity)[indices]
                temperature = np.array(temperature)[indices]
                stations = np.array(stations)[indices]
    
        # Find the overlap of weather measurement times and scan times
        matches = np.where(mjdsec>=min(myTimes))[0] 
        matches2 = np.where(mjdsec<=max(myTimes))[0]
        noWeatherData = False
        if (len(matches)>0 and len(matches2) > 0):
            # Average the weather points enclosed by the scan time range.
            selectedValues = range(matches[0], matches2[-1]+1)
            # If there was a either gap in the weather data, or an incredibly short scan duration
            # find the closest in time index.
            if (len(selectedValues) == 0):
                selectedValues = self.renormfindClosestTime(mjdsec, myTimes[0])  
        # If all points are greater than myTime, take the first one.
        elif (len(matches)>0):
            selectedValues = matches[0]
        # If all points are less than myTime, take the last one.
        elif (len(matches2)>0):
            selectedValues = matches2[-1]
        # If here, then the table has no weather data at all (7M?).
        else:
            noWeatherData = True

        if (noWeatherData):
            conditions['pressure'] = 563.0
            conditions['temperature'] = 0  # Celsius is expected
            conditions['humidity'] = 20.0
            casalog.post("WARNING: No weather data found in the WEATHER table!")
        else:
            # Separate the relevant index values.
            selectPressure = pressure[selectedValues]
            selectTemperature = temperature[selectedValues]
            selectHumidity = relativeHumidity[selectedValues]
            # Check to make sure that there is at least one valid (non-zero) value.
            mask = (selectPressure > 0)
            if not mask.any():
                casalog.post('No valid weather data for timerange!')
                conditions['pressure'] = 563.0
                conditions['temperature'] = 0  # Celsius is expected
                conditions['humidity'] = 20.0
            # Find average value of each weather condition, using only valid entries.
            else:
                conditions['pressure'] = np.mean(selectPressure[mask])
                conditions['temperature'] = np.mean(selectTemperature[mask])
                conditions['humidity'] = np.mean(selectHumidity[mask])
                if verbose:
                    casalog.post("  Pressure = %.2f mb" % (conditions['pressure']))
                    casalog.post("  Temperature = %.2f C" % (conditions['temperature']))
                    casalog.post("  Relative Humidity = %.2f %%" % (conditions['humidity']))

        return([conditions,myTimes])


    def renormMedianPWV(self, myTimes=[0,99999999999], verbose=False):
        """
        Extracts the PWV measurements from the WVR on all antennas all times.  
        First, it tries to find the ASDM_CALWVR
        table in the ms.  If that fails, it then tries to find the 
        ASDM_CALATMOSPHERE table in the ms.  
        Returns:
        The median PWV
        For further help and examples, see https://safe.nrao.edu/wiki/bin/view/ALMA/GetMedianPWV
        -- Todd Hunter - aU version
        -- Luke Maud - copied and stripped down for almarenorm to make independent of 
        the analysis utilities
        """
        pwvmean = 0  ## actually is the median 
        if (verbose):
            casalog.post("in renormMedianPWV with myTimes = ", myTimes)
        try:
            mytb.open(self.msname+'/ASDM_CALWVR')
            pwvtime = mytb.getcol('startValidTime')  # mjdsec
            antenna = mytb.getcol('antennaName')
            pwv = mytb.getcol('water')
            mytb.close()
            # if read but somehow nothing comes back
            if (len(pwv) < 1):
                if verbose:
                    casalog.post("Found no data in ASDM_CALWVR table")
                pwv=0
        except:
            pwv = 0
        if type(pwv) is int:
            try:
                pwvtime, antenna, pwv = self.renormPWVFromASDM_CALATMOSPHERE()
                if (len(pwv) < 1):
                    if verbose:
                        casalog.post("Found no data in ASDM_CALATMOSPHERE table")
                    return pwvmean
            except:
                pwv = 0

        # i.e. didnt get anything from above tables at all
        if type(pwv) is int:
            if verbose:
                casalog.post("Found no data in ASDM_CALWVR nor ASDM_CALATMOSPHERE tables")
            return pwvmean
        
        # Data from before May 2016 may have all PWV values set to a default 1.0 rather 
        # than a real value. Reject these as there is no real data to use.
        if all(i==1.0 for i in pwv):
            casalog.post('All recorded entries of PWV are set equal to 1.0! No data available.')
            return pwvmean
 
        # my times is hardcoded so should find something
        try:
            matches = np.where(np.array(pwvtime)>myTimes[0])[0]
        except:
            if verbose:
                casalog.post("Found no times > %d" % (myTimes[0]))
            return pwvmean

        # for testing 
        #casalog.post("%d matches = " % (len(matches)), matches)
        #casalog.post("%d pwv = " % (len(pwv)), pwv)
        ptime = np.array(pwvtime)[matches]
        matchedpwv = np.array(pwv)[matches]
        matches2 = np.where(ptime<=myTimes[-1])[0]
        # for testing 
        #casalog.post("matchedpwv = %s" % (matchedpwv))
        #casalog.post("pwv = %s" % (pwv))
        if (len(matches2) < 1):
            # look for the value with the closest start time
            mindiff = 1e12
            for i in range(len(pwvtime)):
                if (abs(myTimes[0]-pwvtime[i]) < mindiff):
                    mindiff = abs(myTimes[0]-pwvtime[i])
                    #                pwvmean = pwv[i]*1000
            matchedpwv = []
            for i in range(len(pwvtime)):
                if (abs(abs(myTimes[0]-pwvtime[i]) - mindiff) < 1.0):
                    matchedpwv.append(pwv[i])
            pwvmean = 1000*np.median(matchedpwv)
            if (verbose):
                casalog.post("Taking the median of %d pwv measurements from all antennas = %.3f mm" % (len(matchedpwv),pwvmean))
        else:
            pwvmean = 1000*np.median(matchedpwv[matches2])
            if (verbose):
                casalog.post("Taking the median of %d pwv measurements from all antennas = %.3f mm" % (len(matches2),pwvmean))
        return pwvmean
        # end of getMedianPWV


    def renormPWVFromASDM_CALATMOSPHERE(self):
        """
        Reads the PWV via the water column of the ASDM_CALATMOSPHERE table in MS.
        - Todd Hunter - aU version
        - Luke Maud copied and edited for almarenorm so independent of analysisUtils
        """
        try:
            mytb.open(self.msname+'/ASDM_CALATMOSPHERE')
            pwvtime = mytb.getcol('startValidTime')  # mjdsec
            antenna = mytb.getcol('antennaName')
            pwv = mytb.getcol('water')[0]  # There seem to be 2 identical entries per row, so take first one.
            mytb.close()
        except:
            return(0,0,0)
        return(pwvtime, antenna, pwv)




    def renormWeatherStationNames(self):
        """
        Returns a dictionary keyed by ALMA weather station ID, with the value
        equal to the station name (e.g. 'WSTBn').
        vis: single measurement set
        -Todd Hunter - aU version
        -Luke Maud copied and modified/stripped down
        from analysisUtils to make almarenorm.py independent
        """
        prefix=['WSTB','Meteo','OSF']
        asdmStation = self.msname+'/ASDM_STATION'

        try:
            mytb.open(asdmStation)
            
            names = mytb.getcol('name')
            mydict = {}
            for i,name in enumerate(names):
                for p in prefix:
                    #            casalog.post "Checking if %s contains %s" % (name.lower(),p.lower())
                    if (name.lower().find(p.lower()) >= 0):
                        mydict[i] = name
            mytb.close()
        except:
            casalog.post("This measurement set does not have an ASDM_STATION table.")
            return
        return(mydict)
        
    def findNearest(self, array, value, index=False):
        """
        Purpose: 
            Given an array and a value that falls within that array, find
            the nearest value or index within the array to that value.

        Inputs:
            array : list of floats or ints
                An array for which to evaluate.
            value : float or int
                The value you wish to find the closest match to within 
                the array.
            index : boolean : OPTIONAL
                If True, return the index location within array that is 
                closest to the value. Otherwise, return the value of that
                index.
                Default: False
        """
        array = np.asarray(array)
        idx = (np.abs(array - value)).argmin()
        if index:
            return idx
        else:
            return array[idx]

    def renormfindClosestTime(self, mytimes, mytime):
        myindex = 0
        mysep = np.absolute(mytimes[0]-mytime)
        for m in range(1,len(mytimes)):
            if (np.absolute(mytimes[m] - mytime) < mysep):
                mysep = np.absolute(mytimes[m] - mytime)
                myindex = m
        return(myindex)

    # AL added - PIPE 1168 (2)
    def convertPlotsToPDF(self, target, spw, include_summary=True, include_heuristics=False, verbose=False):
        """
        Super hacky way to create PDFs of created plots so that we can display them in the weblog.
        Simply calls the bash commands "montage" (to create super plots of pngs), "convert" (to
        then convert those super plots into pdfs), and "pdfunite" (to combine all pdfs into one).

        Imports: 
            target : string
                Name of the target field that matches the filename target
            
            spw : str (or int, the type is forced)
                The spectral window of the files that need to be converted to a PDF.
            
            include_summary : boolean (OPTIONAL)
                If set to True, the summary plot averaging over all scans/fields will be included.
                Default: True

            include_heuristics : boolean (OPTIONAL)
                If set to True, the per antenna heuristics plots will be included in the PDFs.
                Default: False

        Output:
            The target/spw matched plots are montaged and transformed into a PDF and placed into the
            ./RN_plots directory. The name of the PDF is put into the self.rnpipestats dictionary. 
        """
        import glob

        def diagnostic_sort(fn):
            # All files names are deterministic, we want to sort on the scan number, then on field number.
            return int(fn.split('scan')[-1].split('_')[0]), int(fn.split('field')[-1].split('.')[0])

        # Defaults stolen from AU tools.
        tile = '2x4'
        geometry = '1000x800+2+2'
        
        pngs = [] 

        # Create a list of PNGs starting with the summary plot
        if include_summary:
            if os.path.exists('./RN_plots/'+self.msname+'_'+target+'_spw'+str(spw)+'_ReNormSpectra.png'):
                pngs.append('./RN_plots/'+self.msname+'_'+target+'_spw'+str(spw)+'_ReNormSpectra.png')
            else:
                casalog.post('No summary PNG found! Has plotSpectra() been run? Exiting without creating PDF.')
                raise OSError('No summary PNG found within '+os.path.join(os.getcwd(),'RN_plots')+'. Has plotSpectra() been run?')

        # Add the antenna diagnostic plots next
        diag_pngs = glob.glob('./RN_plots/'+self.msname+'_ReNormDiagnosticCheck_'+target+'_spw'+str(spw)+'_scan*_field*.png')
        if len(diag_pngs) == 0:
            casalog.post('No diagnostic PNGs found! Only the summary spectrum will be included.')
        else:
            diag_pngs.sort(key=diagnostic_sort) # sort file names by scan number, then by field to get the right order
            # Add the diagnostic PNGs to the list
            pngs += diag_pngs

        # Add the outlier antenna plots
        if include_heuristics:
            fields = np.intersect1d(self.msmeta.fieldsforintent('*TARGET*'),self.msmeta.fieldsforname(target))            
            ant_pngs = glob.glob('./RN_plots/'+self.msname+'_ReNormHeuristicOutlierAnt_*_spw'+str(spw)+'_scan*field'+str(fields)+'*.png')
            if len(ant_pngs) != 0:
                ant_pngs.sort()
                pngs += ant_pngs

        # Figure out how many pages are needed. We will create tiles of 2 columns x 4 rows
        pages = int(ceil(len(pngs)/8.)) 
        j = 0
        montaged_pngs = [] # Keep the list of newly created montages
        for i in range(pages):
            figs = pngs[j:j+8]
            figs = ' '.join([fig for fig in figs])
            j+=8
            montage = './RN_plots/'+self.msname+'_ReNormDiagnosticCheck_'+target+'_spw'+str(spw)+'_montage_'+str(i)+'.png'
            os.system('montage -geometry '+geometry+' -tile '+tile+' '+figs+' '+montage)
            montaged_pngs.append(montage)

        # Now convert all the PNGs into PDFs
        for mfile in montaged_pngs:
            if verbose:
                from subprocess import Popen
                from subprocess import PIPE
                proc = Popen(['which','convert'], stdout=PIPE, stderr=PIPE)
                which_out, err = proc.communicate()
                proc = Popen(['convert','-version'], stdout=PIPE, stderr=PIPE)
                version_out, err = proc.communicate()
                casalog.post('')
                casalog.post('Using ImageMagicks "convert" for png --> pdf conversion located here:')
                casalog.post(which_out.decode('utf-8'))
                casalog.post('')
                casalog.post(version_out.decode('utf-8'))
                casalog.post('')
            os.system('convert '+mfile+' '+mfile.split('.png')[0]+'.pdf')
        pdflist = ' '.join([fname.split('.png')[0]+'.pdf' for fname in montaged_pngs])

        # Finally, create the summary PDF containing all the plots
        outfile = self.msname+'_ReNormDiagnosticCheck_'+target+'_spw'+str(spw)+'_summary.pdf'
        os.system('pdfunite '+pdflist+' ./RN_plots/'+outfile)

        # Put the name of the new PDF into the self.rnpipestats dictionary.
        if self.rnpipestats[target][str(spw)]:
            self.rnpipestats[target][str(spw)]['pdf_summary'] = outfile

    # AL added - PIPE-1188
    def getImageSBFreqs(self,spwin):
        """
        Purpose:
            Given the spectral window, find the LO frequency and return the frequencies of
            the image sideband. Specifically, return the inputs needed for ATMtrans() in 
            order to properly show the atmospheric transmission for bands 9 and 10 where the
            image sideband atomospheric window also contributes to the spectral window. 

        Inputs:
            spwin : int
                The spectral window for which the image sideband frequencies are needed.

        Outputs:
            fSB : numpy.array
                The frequencies of the image sideband. These will be properly arranged 
                (i.e. they will be opposite in direction to the input spectral window 
                frequencies). 

            chansepSB : float
                The separation between channels (channel width).

            fCenterSB : float
                The mean frequency (center) of the image sideband.
                
            fWidthSB : float
                The total bandwidth of the image sideband (will be equal to the input
                spectral window). 
        
        Note: This has been taken from Todd Hunter's AU tools, specifically au.interpretLOs,
              au.getLOs, and plotbandpass3.py - CalcAtmTranmission.
        """
        # Get the information we need from the MS table ASDM_RECEIVER which will give us
        # the spw numbers, the LOs, and the "names" (more like an intent) of the spws.
        mytb.open(self.msname+'/ASDM_RECEIVER')
        numLO = mytb.getcol('numLO')
        freqLO = []
        spws = []
        names = []
        for i in range(len(numLO)):
            spw = int((mytb.getcell('spectralWindowId',i).split('_')[1]))
            if (spw not in spws):
                spws.append(spw)
                freqLO.append(mytb.getcell('freqLO',i))
                names.append(mytb.getcell('name',i))
        mytb.close()
        
        # We want to ignore the superfluous WVR windows and find the right index for our
        # input spectral window.
        sawWVR=False
        indices = []
        for i in range(len(spws)):
            if (names[i].find('WVR') >= 0):
                if (not sawWVR):
                    indices.append(i)
                    sawWVR = True
            else:
                indices.append(i)

        # This is quite clever (taken from Todd's bandpass3), the LO is the frequency that 
        # is exactly between the spw and the image sideband. Therefore, 
        #   2*(spw_freq - LO_freq) - spw_freq
        #   2 * spw_freq - 2 * LO_freq - spw_freq
        #   spw_freq - 2 * LO_freq
        # this results in each channel getting the correctly matched image sideband frequency
        # such that the array counts in the right direction which is opposite the input spectral
        # window. 
        fSB = np.array(2*freqLO[indices[spwin]][0] - self.msmeta.chanfreqs(spwin))*1e-9
        fCenterSB = np.mean(fSB)
        chansepSB = (fSB[-1]-fSB[0])/(len(fSB)-1)
        fWidthSB = chansepSB*len(fSB)

        return fSB, chansepSB, fCenterSB, fWidthSB
