#
# [SCRIPT INFORMATION]
# GIT REPO: https://bitbucket.alma.cl/scm/~harold.francke_ar/sd-atm-line-correction-prototype.git
# COMMIT: a75554f8bb0
#
#SDcalatmcorr: Casapipescript with wrapper for TS script "atmcorr.py" for the removal
#of atmospheric line residuals.
#2020 - T.Sawada, H. Francke

# 08/apr/2022: - (v2.6) Change logic of best model evaluation to be done inside the atmcor() method.
#              - Created a function makePlot() to clean up the plot generation par of the code.
#                This function added a label with Applied/Discarded to point out the used model.
#                Additionally, the atmospheric transmission was added to the plot, and the channels
#                selected for metric measurement.
# 31/mar/2022: - (v2.5) Added feature to automatically select representative science target for metric
#                measurements.
#              - Fixed issue with range of atmospheric line range selection.
# 25/feb/2022: - (v2.4) Added feature to automatically select least flagged antenna for doing the metric measurement.
#              - Changed default parameters to consider atmtype 1,2,3,4 only for models testing.
#              - Remove remaining dependencies on analysisUtils
#              - Cleaned up procedure to handle default model fallback case, including the return of 
#                a 'fitstatus' variable decribing whether a best fit or a fallback to default model happened.
# 27/jan/2022: - (v2.3) Fixed bug that made script crash with multiples EBs getting different
#                SPW selection with multiple ATM selected for model testing
# 25/jan/2022: - (v2.2) Added catch for case when skyline get fully confused with science line and script
#                ends with no data for model fit
#              - Changed Science target channel detection to previous version (selsource() task)
#              - Added EB uid to plots
# 24/jan/2021: - (v2.1) Fixed issue with spwstoprocess variable with smoothing
# 21/jan/2022: - (v2.0) Fixed several bugs, including handling crashes with fully flagged data pieces
#              - Handles cases where no result is possible when no significant skyline is present, to return default model
#              - Support for using multiple skylines for metric calculation
#              - Added plots for absolute value of derivative, to better assess "*diff" metrics
#              - Added ability to smooth derivative data to reduce noise
#              - Improved skyline range detection from model tau
# 15/dec/2021: - Converted to Python 3/CASA 6 (NOT COMPATIBLE WITH CASA<6 ANYMORE!)
#              - Removed atmospheric correction calculation routine inherited from Sawada san's script and replaced
#                it by one mstransform and several sdatmcor call, in order to calculate the corrected data
#              - Added metrics 'intabsdiff' (integral of absolute value of derivative) and
#                'intsqdiff' (integral of square value of derivative).
#              - Improved statistical estimation of data errors 
#              - Changed data normalization routine and science emission detection.
#              - Uploaded to ALMA Bitbucket
# 03/mar/2021: - Fixed bug handling datasets having TDM SPWs
#              - Fixed metric calculation and plotting problem that got model data stuck in non-corrected data
#              - Rearranged code to move atmcorr core routine to separate function and moved auxiliary spectral setup data
#                into the "spwsetup" dictionary.
# 08/jan/2021: - Added functionality to run atmcorr routine in "try" mode and extract three different
#                metrics for a family of model parameters.
# 24/jul/2020: - Aligned with script atmcorr_20200722.py

import os, sys
from sys import path

#This is in order to be able to use cx_Oracle. Please set path to the correct
#folder in your system. If the cx_Oracle library is not available,
#set the variable accessarchive to False
accessarchive = False
if accessarchive:
    os.environ['LD_LIBRARY_PATH'] = '/usr/lib/oracle/12.2/client64/lib:/opt/casa/packages/RHEL7/release/casa-6.2.1-7-pipeline-2021.2.0.128/lib'
    path.append('/usr/lib64/python36.zip')
    path.append('/usr/lib64/python3.6')
    path.append('/usr/lib64/python3.6/lib-dynload')
    path.append('/usr/local/lib64/python3.6/site-packages')
    path.append('/usr/lib64/python3.6/site-packages')
    path.append('/usr/lib/python3.6/site-packages')
    from cx_Oracle import connect

import glob
import numpy as np
from scipy import stats
import pylab as pl
from itertools import product
from matplotlib import pyplot as plt
import time as systime
import datetime
from scipy.interpolate import CubicSpline
from casatools import msmetadata as msmdtool
from casatools import table as tbtool
from casatools import quanta as qatool
from casatools import atmosphere as attool
from casatools import agentflagger as aftool
from casaplotms import plotms
from casatasks import sdatmcor
from casatasks import mstransform

import pipeline.infrastructure.callibrary as callibrary
pipelineloaded = True

# try:
#     from pipeline.h.cli import h_init
#     from pipeline.hsd.cli import hsd_importdata
#     from pipeline.hsd.cli import hsd_flagdata
#     from pipeline.h.cli import h_tsyscal
#     from pipeline.hsd.cli import hsd_tsysflag
#     from pipeline.hsd.cli import hsd_skycal
#     from pipeline.hsd.cli import hsd_k2jycal
#     from pipeline.hsd.cli import hsd_applycal
#     from pipeline.hsd.cli import hsd_atmcor
#     from pipeline.h.cli import h_resume
#     from pipeline.hsd.cli import hsd_baseline
#     from pipeline.hsd.cli import hsd_blflag
#     from pipeline.hsd.cli import hsd_imaging
#     from pipeline.hsd.cli import hsd_exportdata
#     from pipeline.h.cli import h_save
#     import pipeline.infrastructure.callibrary as callibrary
#     pipelineloaded = True
# except ImportError:
#     print('Could not load pipeline tasks! Please run CASA with --pipeline option.')
#     print('You will not be able to run the redPipeSDatmcorr() task.')
#     pipelineloaded = False

#Useful function for fully flattening array
flattenlist = lambda l: [item for sublist in l for item in sublist]

#ALMA Oracle DB authetication
almaoracleauth = 'almasu/alma4dba@ora.sco.alma.cl:1521/ALMA.SCO.CL'

def createCasaTool(mytool):
    """
    A wrapper to handle the changing ways in which casa tools are invoked.
    For CASA < 6, it relies on "from taskinit import *" in the preamble above.
    mytool: a tool name, like tbtool
    Todd Hunter
    """
    if 'casac' in locals():
        if (type(casac.Quantity) != type):  # casa 4.x and 5.x
            myt = mytool()
        else:  # casa 3.x
            myt = mytool.create()
    else:
        # this is CASA 6
        myt = mytool()
    return(myt)

def robuststats(A):
    '''Return median and estimate standard deviation
    of numpy array A using median statistics.
    '''
    #correction factor for obtaining sigma from MAD
    madfactor = 1.482602218505602
    n = len(A)
    #Fitting parameters for sample size correction factor b(n)
    if (n%2 == 0):
        alpha = 1.32
        beta = -1.5
    else:
        alpha = 1.32
        beta = -0.9
    bn = 1.0 - 1.0/(alpha*n + beta)
    mu = np.ma.median(A)
    sigma = (1.0/bn)*madfactor*np.ma.median(np.ma.abs(A - mu))
    return (mu, sigma)

def binnedstats(x, A, Aerr, nbins = None, bins = None):
    '''Return a tuple of arrays (of size nbins) of median and sigmas 
    calculated from robuststats() from chopping the array A in nbins pieces.
    Alternatively, bins can be explicitly be given with the bins parameter in the form:
    bins = [[x0, x1], [x2, x3], ...]
    '''
    n = len(A)
    if (type(nbins) == int) and (bins is None):
        binsize = int(np.ceil(1.0*n/nbins))
        p = [[i*binsize, min((i+1)*binsize,n-1)] for i in range(nbins)]
    elif (nbins is None) and (type(bins) == list):
        p = bins
    elif ((nbins is None) and (type(bins) == None)) or ((type(nbins) == int) and (type(bins) == list)):
        print('Either set nbins=(integer) or give bins as a partition of channels!')
        return (None, None, None)
    npart = len(p)
    xbin = np.zeros(npart)
    mu = np.zeros(npart)
    sigma = np.zeros(npart)
    mederr = np.zeros(npart)
    chi2 = np.zeros(npart)
    mskfrac = np.zeros(npart)
    for i in range(npart):
        sel = (A.mask[p[i][0]:p[i][1]] == False)
        nsel = np.sum(sel)
        xbin[i] = np.mean(x[p[i][0]:p[i][1]][sel])
        thisA = A[p[i][0]:p[i][1]][sel]
        thisAerr = Aerr[p[i][0]:p[i][1]][sel]
        (thismu, thissigma) = robuststats(thisA)
        mu[i] = thismu
        sigma[i] = thissigma
        (thismederr, thiserrsigma) = robuststats(thisAerr)
        mederr[i] = thismederr
        chi2[i] = np.sum(np.square((thisA - thismu)/thisAerr))/(nsel - 1.0)
        mskfrac[i] = 1.0*np.sum(A.mask[p[i][0]:p[i][1]])/(p[i][1]-p[i][0]+1.0)
    return (xbin, mu, sigma, mederr, chi2, mskfrac, p)

def fitbaseline(x, y, dy, initialbins = 5, detectionsigma = (-99.0, 5.0),
                minbinfracsize = 0.025, targetchi2 = 4.0, maxoutlierfrac = 0.9,
                outlierprobmu = 0.5, outlierprobsigma = 0.25, bordertokeep = 0.05):
    '''Fit baseline to function y(x) (having error dy(x)).
    Starts partitioning domain in 'initialbins' and the subdividing bins up to 
    a fractional size 'minbinfracsize' of the total number of channels.
    Returns PPoly instance with fit.
    '''
    #Initialize values for start
    n = len(x)
    spwrange = np.max(x) - np.min(x)
    outliermu = np.min(x) + outlierprobmu*spwrange
    outliersig = outlierprobsigma*spwrange
    outlierprob = np.exp(-0.5*np.square((x - outliermu)/outliersig))
    print('fitbaseline: len(x): '+str(n)+' y.mask:'+str(np.sum(y.mask))+' dy.mask:'+str(np.sum(dy.mask)))
    #If initialbins <2, that's too few, we need two points at least
    if (initialbins < 2) or (minbinfracsize > 0.5):
        print('initialbins cannot be less than 2!!! setting it to 2...')
        initialbins = 2
        minbinfracsize = 0.45
    #Setting minimum bin size
    minbinsize = int(np.round(1.0*n*minbinfracsize))
    #If initial number of bins is not consistent with minbinfracsize, augment
    #minbinsize to match it, but complain about it
    if (initialbins*minbinsize >= n) and (minbinsize >= 20):
        print('Minimal bin size and initial number of bins inconsistent!')
        print('setting minimal bin size to nchans/initialbins - 1...')
        minbinsize = int(np.round(1.0*n/initialbins)) - 1
    elif minbinsize < 20:
        print('Minimal bin size < 20 channels! setting it to 20 to avoid noisy statistics...')
        minbinsize = 20

    #Calculate border to keep
    bordermin = int(bordertokeep*n)
    bordermax = int(n - bordertokeep*n)

    #Put value just to start
    repchi2 = 10*targetchi2
    rerun = False
    #Initial stats per bin
    binsize = int(np.round(1.0*n/initialbins))
    p = [[i*binsize, min((i+1)*binsize,n-1)] for i in range(initialbins)]

    #Loop while representative chi2 has not reached goal while
    #the minimum bin size has not shrunk below minimum allowed size
    while (repchi2 > targetchi2):
        #Bin data according to partition p
        (xbin, ybin, dybin, meddybin, chi2binaux, mskfracbin, p) = binnedstats(x, y, dy, bins = p)
        #Check is there are bin with no valid results
        allvalid = np.all([np.isfinite(xbin),np.isfinite(ybin)],axis=0)
        print('nbins: '+str(len(xbin))+' allvalid: '+str(np.sum(allvalid))+' y.mask: '+str(np.sum(y.mask)))
        print('xbins: '+str(xbin))
        print('p: '+str(p))
        if (not (len(allvalid) == np.sum(allvalid))) and (np.sum(allvalid) > 1):
            #If we still have at least two datapoints, leave the nan's outside and continue
            xbin = xbin[allvalid]
            ybin = ybin[allvalid]
            dybin = dybin[allvalid]
        if np.sum(allvalid) <= 1:
            print('Could not converge to fit!!!')
            blfit = None 
            #Calculate the residuals of the baseline fit and bin them
            res = (y - np.ma.median(y))
            chi2bin = chi2binaux
            break
        #Calculate baseline fit
        orderbins = np.argsort(xbin)
        blfit = CubicSpline(xbin[orderbins], ybin[orderbins], bc_type='not-a-knot')
        #Calculate the residuals of the baseline fit and bin them
        res = (y - blfit(x))
        #Calculate the median residual per bin
        (xaux, resbin, dresbin, mederrbin, chi2bin, mskfracres, paux) = binnedstats(x, res, dy, bins = p)
        #Revise stats per bin and split them if residuals are too high
        binsplitidx = chi2bin*np.any([chi2bin > targetchi2, mskfracres > maxoutlierfrac])
        bin2split = np.argsort(binsplitidx)[-1]
        #Save chi2 of bin with highest chi2
        repchi2 = chi2bin[bin2split]
        print('p='+str(p))
        print('chi2bin='+str(chi2bin))
        #If chi^2 is too big, and bin is wide enough, split it.
        if (binsplitidx[bin2split] > 0.0) and (p[bin2split][1]-p[bin2split][0] > minbinsize):
            midval = int(np.floor(0.5*(p[bin2split][0]+p[bin2split][1])))
            p = [p[i] for i in range(0,bin2split)] + \
                [[p[bin2split][0], midval], [midval, p[bin2split][1]]] + \
                [p[i] for i in range(bin2split + 1, len(p))]
        #If chi^2 is too big, but bin size has become too small, remove segment (unless it's in the border we keep)
        elif (binsplitidx[bin2split] > 0.0) and (p[bin2split][1]-p[bin2split][0] <= minbinsize) and \
             (p[bin2split][0] > bordermin) and (p[bin2split][1] < bordermax):
            print('p[bin2split]='+str(p[bin2split]))
            p.remove(p[bin2split])
        #If there are no bins with chi^2 larger than target, stop here
        else:
            break

    #Detect outliers
    if not (res is None):
        outliers = np.any([res*outlierprob < detectionsigma[0]*dy, res*outlierprob > detectionsigma[1]*dy], axis=0)
    else:
        outliers = np.zeros(n, dtype=bool)

    output = {}
    output['blfit'] = blfit
    output['xbin'] = xbin
    output['ybin'] = ybin
    output['dybin'] = dybin
    output['chi2bin'] = chi2bin
    output['p'] = p
    output['outliers'] = outliers

    return output

def segmentEdges(seq, gap, label, sortdata = True):
    '''Return edges of sequence of values with gaps
    Assumes 1D array "seq" is ordered. Otherwise one should sort it first.
    seq: Sequence of values to search gaps in.
    gap: Gap size to consider.
    label: Label to put in the numpy array returned.
    sortdata: Whether to sort data in the seq vector.
    '''

    if sortdata:
        seq = np.sort(seq)
    n = len(seq)
    diff = seq[1:] - seq[0:n-1]
    if np.any(diff < 0):
        print('Array is not sorted, use sortdata = True')
        return None
    isgap = (diff > gap)
    if np.sum(isgap) > 0:
        startseg = seq[np.append([True],isgap)]
        endseg = seq[np.append(isgap,[True])]
    else:
        startseg = seq[0]
        enseg = seq[-1]

    return np.array([(startseg[i], endseg[i], label) for i in range(len(startseg))],np.dtype([('tstart',np.float64),('tend',np.float64),('intent',np.unicode_,40)]))

def selectRanges(timeseq, rangetable):
    '''Return selection boolean array for a time sequence, given a table of time ranges.
    '''
    nranges = len(rangetable)
    ndata = len(timeseq)
    sel = np.zeros(ndata, dtype=bool)
    for i in range(nranges):
        sel += np.all([timeseq >= rangetable['tstart'][i], timeseq <= rangetable['tend'][i]], axis=0)
    return sel



def sigclipfit(x, A, Asig, fitdeg, nclips, nsigma, bordertokeep = 0.05, progdegree = False, smoothselbox = 0.05):
    '''Return polynomial model parameters done to vector A and residuals from the fit.
    x: x coordinate
    A: Data set vector.
    Asig: Data sigma vector. Should include variance due to skylines and science target emission.
    fitdeg: Fitting polynomial degree.
    nclips: Number of sigma clipping iterations.
    nsigma: Number of sigmas in sigma clipping iterations. Positive float or two-value tuple.
    bordertokeep: Fraction of channels to keep in the borders
    progdegree: Whether to increase polynomial fit progressively in each iteration.
    smoothselbox: Enlarge each outlier detection with a box of this size (as fraction of SPW).
    '''
    if type(nsigma) == float:
        nsigmahigh = nsigma
        nsigmalow = nsigma
    elif ((type(nsigma) == tuple) or (type(nsigma) == list)) and (len(nsigma) == 2):
        nsigmahigh = nsigma[1]
        nsigmalow = nsigma[0]
    output = {}
    gooddata = np.ma.MaskedArray(np.ones(len(A)), mask=A.mask)
    #Start data selection with all good data
    sel = gooddata
    minx = np.ma.min(x)
    maxx = np.ma.max(x)
    bordermin = minx + bordertokeep*(maxx - minx)
    bordermax = maxx - bordertokeep*(maxx - minx)
    #Set border selection, avoiding any bad data
    border = np.ma.MaskedArray(np.ma.all([np.ma.any([x < bordermin, x > bordermax], axis=0), gooddata], axis=0), mask=A.mask)
    if progdegree:
        degree = 0
    else:
        degree = fitdeg
    if smoothselbox > 0.0:
        box = max(2, int(smoothselbox*len(A)))
    else:
        box = 1
    while (degree <= fitdeg):
        for i in range(nclips):
            #Include constant border, if demanded
            thissel = np.ma.any([sel, border], axis=0)
            #If everything is flagged, continue with a default selection of everything
            if np.sum(~A.mask[thissel]) <= fitdeg:
                thissel = np.ma.any([gooddata, border], axis=0)
            try:
                coefs = np.polyfit(x[thissel], A[thissel], fitdeg, w=1/Asig[thissel])
            except:
                coefs = [0.0 for i in range(degree+1)]
                print('No data!!!\nx='+str(x[thissel])+'\nA='+str(A[thissel])+'\nAsig='+str(Asig[thissel]))
            model = np.ma.sum([coefs[i]*(x**(degree-i)) for i in range(degree+1)], axis=0)
            diff = A - model
            (mu, sigma) = robuststats(diff)
            #Select pixels to use in the next iteration from pixels within nsigma of mean
            #including only non-masked data, but not in the border.
            sel = np.ma.all([((diff - mu)/sigma >= -nsigmalow), ((diff - mu)/sigma <= nsigmahigh), gooddata], axis=0)
            #If boxcar smoothing of selection is chosen (smoothselbox > 0), broadening non-selected pixels
            if box > 1:
                sel = np.ma.logical_not(enlargesel(np.ma.logical_not(sel), box))
        degree += 1
    output['coefs'] = coefs
    output['residuals'] = diff
    output['sel'] = sel
    output['border'] = border
    return output

def enlargesel(sel, box):
    '''Enlarge selection by "box" pixels around each selected pixel
    in "sel" vector. 
    '''
    n = len(sel)
    newsel = 0.0*sel
    for i in range(n):
        startrange = max(0, i - box)
        endrange = min(n - 1, i + box + 1)
        newsel[i] = np.ma.max(sel[startrange:endrange])
    return newsel

def smooth(y, box_pts):
    '''Smooth using boxcar convolution.
    '''
    box = np.ma.ones(box_pts)/box_pts
    y_smooth = np.ma.convolve(y, box, mode='same')
    return y_smooth

def noisenormdata(tmdataon, antson, dataon, tmatm, antatm, tsys, trec, tau):
    '''Compute a noise-normalized data for source selection. To be used by selsource() function.
    '''
    npol, nch, nint = np.shape(dataon)
    npolatm, nchatm, nlinesatm = np.shape(tsys)
    nant = len(np.unique(antson))
    natm =  int(nlinesatm/nant)
    #Get skylines, if any
    #skylines = getskylines(tau, fraclevel = 0.5)
    #skymask = skysel(skylines, avoidpeak = 0.0)
    #Smooth Trx
    smbox = int(0.01*nch)
    smtrec = 0.0*trec
    for pol in range(npolatm):
        for atmline in range(nlinesatm):
            smtrec[pol, :, atmline] = smooth(trec[pol, :, atmline], smbox)
    #Mask borders
    minmask = np.min(dataon.mask, axis=(0,2))
    badborder = int(0.05*nch)
    minmask[0:badborder] = True
    minmask[nch-badborder:nch] = True
    tmavelist = []
    tmsigmalist = []
    tavenorm = np.ma.MaskedArray(np.zeros(nch), mask=minmask)
    tnorm = np.ma.MaskedArray(np.zeros(nch), mask=minmask)
    normdata = 0.0*dataon
    cx = 0
    for pol in range(npol):
        for atmscan in range(natm):
            for ant in range(nant):
                #Select data for this pol, ATM scan and antenna
                tstart = tmatm[atmscan*nant+ant]
                if atmscan < natm - 1:
                    tend = tmatm[(atmscan+1)*nant+ant]
                else:
                    tend = tmdataon[-1]
                sel = np.ma.all([tmdataon >= tstart, tmdataon <= tend, antson == ant], axis=0)
                nsel = np.sum(sel)
                if nsel > 0:
                    #Extract piece of data
                    thisdata = dataon[pol,:,sel]
                    normdata[pol,:,sel] = thisdata/(tsys[pol, :, atmscan*nant+ant] - smtrec[pol, :, atmscan*nant+ant])
    #Renormalize fluctuations
    #Find median value for each integration and calculate data - median(data)
    medmedian = np.ma.median(np.ma.median(normdata, axis=1), axis=1)
    if np.any(np.isnan(medmedian)):
        return None
    fqmedian = np.ma.median(normdata, axis=1)
    fqmedianzeros = (fqmedian <= 0.0)
    fqmedian[fqmedianzeros] = 1.0
    renormdata = 0.0*normdata
    for pol in range(npol):
        renormdata[pol, :, :] = normdata[pol, :, :]*np.outer(np.ones(nch), medmedian[pol]/fqmedian[pol])

    return renormdata

def selsource2(spw, spwsetup, nu, tmdataon, antson, dataon, tmatm, antatm, tsys, trec, tau, smoothselbox = 0.05, maxmasked = 0.5,
               ndegree = 3, nclips = 3, nsigma = 5.0, verbose = False):
    '''Select science target channels and return boolean vector with selection. Task attempts polynomial
    fits to a baseline of normalized (to Tsys) noise and detect upwards outliers (variance higher than expected
    from Tsys level). Final best fit gets determined by chi^2 (including extpenalty factor)
    Positional params: From MS selection:
                       nu: (freq. vector of SPW), tmdataon (time vector of dataon data selection),
                       antson: (vector of antenna column of dataon data selection),
                       dataon: (masked array of selected data),
                       Data from CALATMOSPHERE table:
                       tmatm: (vector of times of sel. rows), antatm (vector of antenna IDs of sel. rows)
                       tsys: (Tsys table), trec (T Receiver table), tau (optical depth table)
    '''
    #Initialize variables
    npol, nch, nint = np.shape(dataon)
    if smoothselbox > 0.0:
        box = max(2, int(smoothselbox*len(nu)))
    else:
        box = 1

    #Get skylines, if any. Avoid peak of skylines in science target detection
    skylines = getskylines(tau, spw, spwsetup, fraclevel = 0.7)
    skymask = skysel(skylines, avoidpeak = 0.0)
    noskymask = np.ma.logical_not(skymask)
    #Normalize the standard deviation of the data by Tsys
    normdata = noisenormdata(tmdataon, antson, dataon, tmatm, antatm, tsys, trec, tau)
    normsigma = np.ma.sum(np.ma.median(normdata, axis=2), axis=0)
    #If normalized sigma data array is masked more than maxmasked fraction of data, return an empty selection
    if (normdata is None) or (np.sum(normsigma.mask) >= maxmasked*nch):
        return np.zeros(nch)
    (mu, sig) = robuststats(normsigma)
    normsigmasigma = np.ma.MaskedArray(np.ones(nch)*sig, mask=normsigma.mask)
    basenoisefit = sigclipfit(nu, normsigma, normsigmasigma, ndegree, nclips, (99.0, nsigma), bordertokeep = 0.0, smoothselbox=smoothselbox)
    blmodel = np.ma.sum([basenoisefit['coefs'][i]*(nu**(ndegree-i)) for i in range(ndegree+1)], axis=0)
    #Select the outliers as probable science target emission channels
    srcsel = np.ma.logical_not(basenoisefit['sel'])
    srcsel[skymask] = False
    finalsrcsel = (enlargesel(srcsel, box) > 0)
    #basenoisefit = fitbaseline(nu, normsigma, normsigmasigma)
    #srcsel = (enlargesel(basenoisefit['outliers'], box) > 0)

    if verbose:
        plt.plot(nu, normsigma, '.k')
        plt.plot(nu, blmodel, '-b')
        plt.plot(nu[finalsrcsel], normsigma[finalsrcsel], 'sr')
        plt.xlabel('Frequency [GHz]')
        plt.ylabel('Normalized Sigma Amp')
        plt.savefig('selsource2_normsigma.png')

    return finalsrcsel

def selsource(spw, spwsetup, nu, tmdataon, antson, dataon, tmatm, antatm, tsys, trec, tau,
              ndegree = 3, nclips = 3, nsigma = 3.0, smoothbox = 0.03, extpenalty = 0.1,
              verbose = False, maxmasked = 0.5):
    '''Select science target channels and return boolean vector with selection. Task attempts polynomial
    fits to a baseline of normalized (to Tsys) noise and detect upwards outliers (variance higher than expected
    from Tsys level). Final best fit gets determined by chi^2 (including extpenalty factor)
    Positional params: From MS selection:
                       nu: (freq. vector of SPW), tmdataon (time vector of dataon data selection),
                       antson: (vector of antenna column of dataon data selection),
                       dataon: (masked array of selected data),
                       Data from CALATMOSPHERE table:
                       tmatm: (vector of times of sel. rows), antatm (vector of antenna IDs of sel. rows)
                       tsys: (Tsys table), trec (T Receiver table), tau (optical depth table)
    Keyword params:
    ndegree: Maximum degree of polynomial used to fit noise baseline (default=7)
    nclips: Maximum number of sigma clipping iterations to try in noise baseline fitting (default=5)
    nsigma: number of sigmas for science source channel selection. (default=3.0)
    smoothbox: Number of pixels to enlarge science target selection around pixels initially selected through
               the nsigma excess. Given as fraction of SPW width. (default=0.03)
    extpenalty: Factor to add a penalty to chi^2 of the fits, equal to extpenalty*(channel_range/number_of_channels)
    where channel_range is the range of channels of detected science sources. This factor aims at favoring more
    compact detections. (default=1.0)
    verbose: Return all fits' selections, fit coefficient and chi^2's instead of only the best fit. Also produces
    plots for each fit. (default=False)
    maxmasked: Maximum fraction of data allowed to be masked. If higher than this, return a vector with no selection.
    '''
    npol, nch, nint = np.shape(dataon)
    #Get skylines, if any. Avoid peak of skylines in science target detection
    skylines = getskylines(tau, spw, spwsetup, fraclevel = 0.3)
    skymask = skysel(skylines, avoidpeak = 0.0)
    noskymask = np.ma.logical_not(skymask)
    #Normalize the standard deviation of the data by Tsys
    normdata = noisenormdata(tmdataon, antson, dataon, tmatm, antatm, tsys, trec, tau)
    normsigma = np.ma.sum(np.ma.median(normdata, axis=2), axis=0)
    #If normalized sigma data array is masked more than maxmasked fraction of data, return an empty selection
    if (normdata is None) or (np.sum(normsigma.mask) >= maxmasked*nch):
        return np.zeros(nch)
    (mu, sig) = robuststats(normsigma)
    (xbin, mubin, sigbin, mederrbin, chi2bin, mskfracbin, p) = binnedstats(nu, normsigma, np.ones(nch)*sig, nbins = 10)
    minsig = np.min(sigbin)
    normsigmasigma = np.ma.MaskedArray(np.ones(nch)*minsig, mask=normsigma.mask)
    allbasenoisefit = {}
    allsel = {}
    allchi2 = []
    chi2idx = {}
    invchi2idx = {}
    chi2cx = 0
    onlyskysel = {}
    allressigma = []
    for nc in range(1, nclips+1):
        for deg in range(1, ndegree+1):
            basenoisefit = sigclipfit(nu, normsigma, normsigmasigma, deg, nc, (999.0, nsigma), bordertokeep = 0.05, smoothselbox=smoothbox)
            sourcesel = np.ma.logical_not(basenoisefit['sel'])
            sourcesel[normsigma.mask] = False
            sourcesel[skymask] = False
            #Measure full extent of selection
            idxsourcesel = np.where(sourcesel)[0]
            if len(idxsourcesel) >= 2:
                extfactor = extpenalty*(np.max(idxsourcesel)-np.min(idxsourcesel))/nch
            else:
                extfactor = 0.0
            allbasenoisefit[(nc, deg)] = basenoisefit
            allsel[(nc, deg)] = sourcesel
            noskylinesel = np.ma.all([basenoisefit['sel'], noskymask], axis=0)
            onlyskysel[(nc,deg)] = np.any([noskylinesel, sourcesel], axis=0)
            (muaux, sigaux) = robuststats(basenoisefit['residuals'][onlyskysel[(nc,deg)]])
            allressigma.append(sigaux)
            chi2idx[chi2cx] = (nc, deg)
            invchi2idx[(nc, deg)] = chi2cx
            chi2cx += 1
    bestsigma = np.min(allressigma)
    for nc in range(1, nclips+1):
        for deg in range(1, ndegree+1):
            #Measure full extent of selection
            idxsourcesel = np.where(allsel[(nc, deg)])[0]
            if len(idxsourcesel) >= 2:
                extfactor = extpenalty*(np.max(idxsourcesel)-np.min(idxsourcesel))/nch
            else:
                extfactor = 0.0
            allchi2.append(np.ma.sum((allbasenoisefit[(nc, deg)]['residuals'][onlyskysel[(nc,deg)]]**2)/(bestsigma**2))/(1.0*np.sum(onlyskysel[(nc,deg)]) - deg - 1)+extfactor)
    allchi2 = np.array(allchi2)
    bestfit = np.argsort(allchi2)[0]
    (bestnc, bestdeg) = chi2idx[bestfit]
    sourcesel = allsel[(bestnc, bestdeg)]
    if verbose:
        for nc in range(1, nclips+1):
            for deg in range(1, ndegree+1):
                model = np.ma.sum([allbasenoisefit[(nc, deg)]['coefs'][i]*(nu**(deg-i)) for i in range(deg+1)], axis=0)
                plt.clf()
                plt.plot(nu, normsigma, '-k')
                plt.plot(nu[sourcesel], normsigma[sourcesel], 'sr')
                plt.plot(nu, model, ':g')
                plt.text(0.8*np.ma.max(nu)+0.2*np.ma.min(nu), 0.8*np.ma.max(normsigma)+0.2*np.ma.min(normsigma), 'chi2: '+str(allchi2[invchi2idx[(nc,deg)]]))
                plt.xlabel('Frequency [GHz]')
                plt.ylabel('Normalized Sigma(Amp)')
                if (nc == bestnc) and (deg == bestdeg):
                    plt.title('Normalized Noise plot (best fit)')
                else:
                    plt.title('Normalized Noise plot')
                plt.savefig('selsource_output1_nc'+str(nc)+'_deg'+str(deg)+'.png')

    return sourcesel

def getskylines(tauspec, spw, spwsetup, fraclevel = 0.5, minpeaklevel = 0.0, spwbordertoavoid = 0.025, taudeg = 2):
    '''Get position of sky lines and line widths from the optical depth.
    fraclevel: fraction of the maximum of the optical depth to look for.
    If fraclevel=0.5 (default), the width results will be the HWHM.
    minpeaklevel: Minimum relative opacity at a given peak (relative to median opacity)
    to consider a line. Lines smaller than this level are ignored. (default 0.05)
    spwbordertoavoid: Fractional border of the SPW to avoid for peak detection.
    Default is 0.025 (2.5% of SPW BW)
    '''
    nch = len(tauspec)
    borderpix = int(max(1.0,np.round(nch*spwbordertoavoid)))
    noborder = np.ones(nch, dtype=bool)
    noborder[0:borderpix] = False
    noborder[nch-borderpix:nch] = False
    mintau = np.min(tauspec)
    mediantau = np.median(tauspec)
    tauzeroD = np.array(np.ediff1d(1.0*(np.ediff1d(tauspec) > 0.0), to_begin=0, to_end=0), dtype=bool)
    taunegDD = np.array(np.ediff1d(np.ediff1d(tauspec), to_begin=0, to_end=0) < 0.0, dtype=bool)
    taugtminlevel = (tauspec/mediantau >= (1.0 + minpeaklevel))
    taupeak = np.all([tauzeroD, taunegDD, taugtminlevel, noborder], axis=0)
    tauvalley = np.all([tauzeroD, ~taunegDD], axis=0)
    idxpeak = np.arange(nch, dtype=int)[taupeak]
    idxvalley = np.arange(nch, dtype=int)[tauvalley]
    npeak = len(idxpeak)
    peaktau = tauspec[idxpeak]
    output = {'nch': nch, 'npeak': npeak, 'taumin': mintau, 'fraclevel': fraclevel, 'valley': idxvalley, 'peaksinfo': {}}
    hwhmleft = np.zeros(npeak)
    hwhmright = np.zeros(npeak)
    widthratio = np.zeros(npeak)
    #Fit "baseline" to tau
    nu = spwsetup[spw]['chanfreqs']/1.e09
    taufit = sigclipfit(nu, np.ma.MaskedArray(tauspec, mask=np.zeros(nch)), 
                        np.ma.MaskedArray(np.sqrt(tauspec), mask=np.zeros(nch)), 2, 3, 5.0)
    taubline = np.ma.sum([taufit['coefs'][deg]*(nu**(taudeg-deg)) for deg in range(taudeg + 1)], axis = 0)
    subtau = tauspec - taubline
    output['taufit'] = taufit
    for k, idx in enumerate(idxpeak):
        #calculate the decrease from the tau depth relative to the minimum tau in the SPW
        #peakratioleft = np.array([(tauspec[idx - i]-mintau)/(peaktau[k]-mintau) for i in range(idx)])
        #peakratioright = np.array([(tauspec[idx + i]-mintau)/(peaktau[k]-mintau) for i in range(nch-idx)])
        peakratioleft = np.array([subtau[idx - i]/subtau[idx] for i in range(idx)])
        peakratioright = np.array([subtau[idx + i]/subtau[idx] for i in range(nch-idx)])
        posrightneg = peakratioright < fraclevel
        if np.sum(posrightneg) > 0:
            hwhmright[k] = np.where(posrightneg)[0][0]
        else:
            #search of peak to the right of this peak, select the border if none
            if k < npeak-1:
                hwhmright[k] = 0.5*(idxpeak[k+1] - idx)
            else:
                hwhmright[k] = nch - idx - 1
        posleftneg = peakratioleft < fraclevel
        if np.sum(posleftneg) > 0:
            hwhmleft[k] = np.where(posleftneg)[0][0]
        else:
            #search of peak to the left of this peak, select the border if none
            if k > 0:
                hwhmleft[k] = 0.5*(idx - idxpeak[k-1])
            else:
                hwhmleft[k] = idx

    #If the range of the skylines goes beyond a valley, shrink range around line using those valleys
    for k in range(npeak):
        valleypointsleft = np.sort([point for point in idxvalley if (point < idxpeak[k])])
        if (len(valleypointsleft) > 0) and (idxpeak[k]-hwhmleft[k] < valleypointsleft[-1]):
            hwhmleft[k] = idxpeak[k] - valleypointsleft[-1]
        valleypointsright = np.sort([point for point in idxvalley if (point > idxpeak[k])])
        if (len(valleypointsright) > 0) and (idxpeak[k]+hwhmright[k] > valleypointsright[0]):
            hwhmright[k] = valleypointsright[0] - idxpeak[k]

    #Check if there is more than one peak and if so check if ranges overlap and correct in that case
    if npeak > 1:
        for k in range(npeak - 1):
            if (idxpeak[k]+hwhmright[k] > idxpeak[k+1]-hwhmleft[k+1]):
                #If there is an valley point detected between the peaks, choose that one.
                #If not, just pick the middle point between peaks
                valleypoints = [point for point in idxvalley if (point > idxpeak[k]) and (point < idxpeak[k+1])]
                if len(valleypoints) > 0:
                    hwhmright[k] = valleypoints[0]-idxpeak[k]
                    hwhmleft[k+1] = idxpeak[k+1]-valleypoints[0]
                else:
                    hwhmright[k] = 0.5*(idxpeak[k+1]-idxpeak[k])
                    hwhmleft[k+1] = 0.5*(idxpeak[k+1]-idxpeak[k])

    #Create output dictionary
    for k, idx in enumerate(idxpeak):
        output['peaksinfo'][k] = {}
        output['peaksinfo'][k]['peakpos'] = int(idx)
        output['peaksinfo'][k]['taupeak'] = tauspec[idx]
        output['peaksinfo'][k]['hwhmleft'] = int(hwhmleft[k])
        output['peaksinfo'][k]['hwhmright'] = int(hwhmright[k])
        output['peaksinfo'][k]['minrange'] = int(idx - hwhmleft[k])
        output['peaksinfo'][k]['maxrange'] = int(idx + hwhmright[k])
    return output

def gradeskylines(skylines, cntrweight = 1.0):
    spwlist = np.sort(list(skylines.keys()))
    idxspw = []
    idxpeak = []
    taupeak = []
    centerdist = []
    #Read values and calculate distance to SPW centers
    for spw in spwlist:
        for i in range(skylines[spw]['npeak']):
            taupeak.append(skylines[spw]['peaksinfo'][i]['taupeak'])
            centerdist.append(2.0*(skylines[spw]['peaksinfo'][i]['peakpos'] - 0.5*skylines[spw]['nch'])/skylines[spw]['nch'])
            idxspw.append(spw)
            idxpeak.append(i)
    taupeak = np.array(taupeak)
    centerdist = np.array(centerdist)
    #Calculate grade
    normtau = (taupeak - np.min(taupeak))/(np.max(taupeak) - np.min(taupeak))
    grade = normtau*(1.0 - cntrweight*np.abs(centerdist))
    bestgrade = np.argsort(grade)[-1]
    #Put grade back into skylines dictionary
    idx = 0
    for spw in spwlist:
        for i in range(skylines[spw]['npeak']):
            skylines[spw]['peaksinfo'][i]['grade'] = grade[idx]
            idx += 1
    #Return SPW and index of higher grade peak
    return (idxspw[bestgrade], idxpeak[bestgrade])

def skysel(skylines, linestouse = 'all', avoidpeak = 0.0):
    '''Create selection array from dictionary of skylines list.
    avoidpeak: Parameter to avoid a range around the line peak as fraction of FWHM
    '''
    skysel = np.zeros(skylines['nch'], dtype=bool)
    if linestouse == 'all':
        linestouse = range(skylines['npeak'])
    for i in linestouse:
        skysel[skylines['peaksinfo'][i]['minrange']:skylines['peaksinfo'][i]['maxrange']+1] = np.ones(skylines['peaksinfo'][i]['maxrange']-skylines['peaksinfo'][i]['minrange']+1, dtype=bool)
        if avoidpeak > 0.0:
            startrange = max(0, skylines['peaksinfo'][i]['peakpos']-int(avoidpeak*skylines['peaksinfo'][i]['hwhmleft']))
            endrange = min(skylines['peaksinfo'][i]['peakpos']+int(avoidpeak*skylines['peaksinfo'][i]['hwhmright']), skylines['nch'])
            skysel[startrange:endrange] = np.zeros(endrange-startrange, dtype=bool)

    return skysel

def calcmetric(rawsample, rawsigmasample, metrictype = 'intabsdiff', smoothbox = 1):
    (npols, nch) = np.shape(rawsample)
    #Smooth if requested
    if (smoothbox > 1) and (type(smoothbox) == int):
        sample = 0.0*rawsample
        for i in range(npols):
            sample[i,:] = smooth(rawsample[i,:], smoothbox)
        sigmasample = rawsigmasample/np.sqrt(smoothbox)
    else:
        sample = rawsample
        sigmasample = rawsigmasample
    #Transform arrays to ma if not like that already
    if not type(sample) == np.ma.core.MaskedArray:
        sample = np.ma.MaskedArray(sample)
        sigmasample = np.ma.MaskedArray(sigmasample)
    if metrictype == 'intabs':
        auxval = []
        auxerr = []
        for i in range(npols):
            goodsample = (sample.data[i])[~sample.mask[i]]
            sigmagoodsample = (sigmasample[i])[~sample.mask[i]]
            if len(goodsample) > 0:
                auxval.append(np.ma.sum(np.ma.abs(sample[i,:])))
                auxerr.append(np.ma.sqrt(np.ma.sum(sigmasample[i,:]*sigmasample[i,:])))
        if len(auxval) > 0:
            value = np.sum(auxval)
            error = np.sqrt(np.sum(np.array(auxerr)*np.array(auxerr)))
        else:
            value = np.nan
            error = np.nan
        return (value, error)
    elif metrictype == 'maxabs':
        auxval = []
        auxerr = []
        for i in range(npols):
            goodsample = (sample.data[i])[~sample.mask[i]]
            sigmagoodsample = (sigmasample[i])[~sample.mask[i]]
            if len(goodsample) > 0:
                idx = np.argsort(np.abs(goodsample))[-1]
                auxval.append(np.abs(goodsample[idx]))
                auxerr.append(sigmagoodsample[idx])
        if len(auxval) > 0:
            idx = np.argsort(np.abs(auxval))[-1]
            value = auxval[idx]
            error = auxerr[idx]
        else:
            value = np.nan
            error = np.nan
        return (value, error)
    elif metrictype == 'maxabsdiff':
        auxval = []
        auxerr = []
        for i in range(npols):
            goodsample = (sample.data[i])[~sample.mask[i]]
            sigmagoodsample = (sigmasample[i])[~sample.mask[i]]
            if len(goodsample) > 0:
                absdiff = np.abs(np.diff(goodsample))
                idx = np.argsort(absdiff)[-1]
                auxval.append(absdiff[idx])
                auxerr.append(np.sqrt(sigmagoodsample[idx]**2+sigmagoodsample[idx+1]**2))
        if len(auxval) > 0:
            idx = np.argsort(np.abs(auxval))[-1]
            value = auxval[idx]
            error = auxerr[idx]
        else:
            value = np.nan
            error = np.nan
        return (value, error)
    elif metrictype == 'intabsdiff':
        auxval = []
        auxerr = []
        for i in range(npols):
            goodsample = (sample.data[i])[~sample.mask[i]]
            sigmasqgoodsample = np.square((sigmasample[i])[~sample.mask[i]])
            if len(goodsample) > 0:
                absdiff = np.abs(np.diff(goodsample))
                absdiffsqerr = sigmasqgoodsample + np.roll(sigmasqgoodsample, 1)
                auxval.append(np.sum(absdiff))
                auxerr.append(np.sqrt(np.sum(absdiffsqerr)))
        if len(auxval) > 0:
            value = np.sum(auxval)
            error = np.sqrt(np.sum(np.array(auxerr)*np.array(auxerr)))
        else:
            value = np.nan
            error = np.nan
        return (value, error)
    elif metrictype == 'intsqdiff':
        auxval = []
        auxerr = []
        for i in range(npols):
            goodsample = (sample.data[i])[~sample.mask[i]]
            sigmasqgoodsample = np.square((sigmasample[i])[~sample.mask[i]])
            if len(goodsample) > 0:
                absdiff = np.abs(np.diff(goodsample))
                sqdiff = np.square(absdiff)
                absdiffsqerr = sigmasqgoodsample + np.roll(sigmasqgoodsample, 1)
                sqdifferr = 4*absdiff*absdiff*(absdiffsqerr[0:-1])
                auxval.append(np.sum(sqdiff))
                auxerr.append(np.sqrt(np.sum(sqdifferr)))
        if len(auxval) > 0:
            value = np.sum(auxval)
            error = np.sqrt(np.sum(np.array(auxerr)*np.array(auxerr)))
        else:
            value = np.nan
            error = np.nan
        return (value, error)
    else:
        return -1.0

def bootstrap(data, metrictype = 'intabsdiff', nsample = 100):
    metric = [calcmetric(data, np.sqrt(data), metrictype = metrictype)[0]]
    n = len(data)
    for i in range(nsample):
        idxsample = np.random.randint(0,n,n)
        sample = data[idxsample]
        metric.append(calcmetric(sample, np.sqrt(sample), metrictype = metrictype)[0])
    metric = np.array(metric)
    return metric

#Copied over from analysisUtils
def getSpwList(msmd,intent='OBSERVE_TARGET#ON_SOURCE',tdm=True,fdm=True, sqld=False):
    spws = msmd.spwsforintent(intent)
    almaspws = msmd.almaspws(tdm=tdm,fdm=fdm,sqld=sqld)
    scienceSpws = np.intersect1d(spws,almaspws)
    return(list(scienceSpws))

#Copied over from analysisUtils
def onlineChannelAveraging(msmd, spws=None):
    """
    For Cycle 3-onward data, determines the channel averaging factor from
    the ratio of the effective channel bandwidth to the channel width.
    spw: a single value, or a list; if Nonne, then uses science spws
    Returns: single value for a single spw, or a list for a list of spws
    -Todd Hunter
    """
    hanning_effBw = {1: 2.667, 2: 3.200, 4: 4.923, 8: 8.828, 16: 16.787}
    if spws is None:
        spws = getSpwList(msmd)
    if type(spws) != list:
        spws = [spws]
    Ns = list(hanning_effBw.keys())
    ratios = [hanning_effBw[i]/i for i in Ns]
    Nvalues = []
    for spw in spws:
        chanwidths = msmd.chanwidths(spw)
        nchan = len(chanwidths)
        if (nchan < 5):
            return 1
        chanwidth = abs(chanwidths[0])
        chaneffwidth = msmd.chaneffbws(spw)[0]
        ratio = chaneffwidth/chanwidth
        Nvalues.append(Ns[np.argmin(abs(ratios - ratio))])
    if (len(spws) == 1):
        return Nvalues[0]
    else:
        return Nvalues

#Obtain spectral setup
def getSpecSetup(myms, spwlist = []):
    '''Task to gather spectral setup information.
    myms: MS to be processed.
    spwlist: List of SPW to be included in queries, if left empty, all science SPWs
    will be considered.
    intentlist: List of intents to be included in the queries. By default is *OBSERVE_TARGET*.
    '''

    #Else read in the information from the MS
    #if spwlist is empty, get all science SPWs
    intentlist = ['*OBSERVE_TARGET#ON_SOURCE*', '*OBSERVE_TARGET#OFF_SOURCE*']
    msmd = createCasaTool(msmdtool)
    msmd.open(myms)
    if len(spwlist) == 0:
        spwlist = getSpwList(msmd)
    spwsetup = {}
    spwsetup['spwlist'] = spwlist
    spwsetup['intentlist'] = intentlist
    spwsetup['namesfor'] = msmd.fieldsforsources(asnames=True)
    spwsetup['scan'] = {}
    spwsetup['fieldname'] = {}
    spwsetup['fieldid'] = {}
    for intent in intentlist:
        spwsetup['scan'][intent] = list(msmd.scansforintent(intent))
        spwsetup['fieldname'][intent] = list(msmd.fieldsforintent(intent,asnames=True))
        spwsetup['fieldid'][intent] = list(msmd.fieldsforintent(intent,asnames=False))
    spwsetup['scifieldidoff'] = {}
    for fieldid in spwsetup['fieldid']['*OBSERVE_TARGET#ON_SOURCE*']:
        fieldname = spwsetup['namesfor'][str(fieldid)][0]
        listfieldnameoff = [item[0] for item in spwsetup['namesfor'].values() if (fieldname in item[0]) and ('OFF' in item[0])]
        if len(listfieldnameoff) == 1:
            fieldidoff = [i for i in spwsetup['namesfor'].keys() if listfieldnameoff[0] == spwsetup['namesfor'][i]][0]
            spwsetup['scifieldidoff'][str(fieldid)] = fieldidoff
        else:
            spwsetup['scifieldidoff'][str(fieldid)] = ''
    allscans = list(set(flattenlist([spwsetup['scan'][intent] for intent in intentlist])))
    spwsetup['allscans'] = allscans
    spwsetup['intent'] = {}
    for scan in allscans:
        spwsetup['intent'][scan] = [intent for intent in spwsetup['intentlist'] if scan in spwsetup['scan'][intent]]
    spwsetup['antids'] = msmd.antennasforscan(scan = spwsetup['scan'][intentlist[0]][0])
    spwsetup['antnames'] = np.array(msmd.antennanames(spwsetup['antids']))
    spwsetup['spwnames'] = np.array(msmd.namesforspws(spwlist))
    spwsetup['fdmspws'] = msmd.fdmspws()
    spwsetup['tdmspws'] = msmd.tdmspws()

    nchanperbb = [0, 0, 0, 0]

    for spwid in spwlist:
        spwsetup[spwid] = {}
        #Get SPW information: frequencies of each channel, etc.
        chanfreqs = msmd.chanfreqs(spw=spwid)
        nchan = len(chanfreqs)
        fcenter = (chanfreqs[int(nchan/2-1)]+chanfreqs[int(nchan/2)])/2.
        chansep = (chanfreqs[-1]-chanfreqs[0])/(nchan-1)
        spwsetup[spwid]['chanfreqs'] = chanfreqs
        spwsetup[spwid]['nchan'] = nchan
        spwsetup[spwid]['fcenter'] = fcenter
        spwsetup[spwid]['chansep'] = chansep
        spwsetup[spwid]['Nave'] = onlineChannelAveraging(msmd, spwid)
        #Get data descriptor for each SPW
        spwsetup[spwid]['ddi'] = msmd.datadescids(spw=spwid)[0]
        spwsetup[spwid]['npol'] = msmd.ncorrforpol(msmd.polidfordatadesc(spwsetup[spwid]['ddi']))
        #Get baseband for each SPW
        spwsetup[spwid]['BB'] = int(msmd.baseband(spwid))
        spwsetup[spwid]['BBname'] = 'BB_'+str(spwsetup[spwid]['BB'])
        spwsetup[spwid]['istdm'] = (spwid in spwsetup['tdmspws'])
        spwsetup[spwid]['nchan_high'] = nchan*5
        spwsetup[spwid]['chansep_high'] = chansep/5.
        spwsetup[spwid]['chanfreqs_high']  = chanfreqs[0] + (chansep/5.)*np.arange(nchan*5)
        spwsetup[spwid]['fcenter_high'] = (spwsetup[spwid]['chanfreqs_high'][int(nchan/2-1)]+spwsetup[spwid]['chanfreqs_high'][int(nchan/2)])/2.
        nchanperbb[spwsetup[spwid]['BB'] - 1] += spwsetup[spwid]['nchan']

    spwsetup['nchanperbb'] = nchanperbb
    for spwid in spwlist:
        spwsetup[spwid]['istdm2'] = (nchanperbb[spwsetup[spwid]['BB'] - 1] in [128, 256])
        spwsetup[spwid]['dosmooth'] = (nchanperbb[spwsetup[spwid]['BB'] - 1]*spwsetup[spwid]['npol'] in [256, 8192])
        if spwsetup[spwid]['dosmooth']:
            print('Spw %d in BB_%d (total Nchan within BB is %d, sp avg likely not applied).  dosmooth=True' % (spwid, spwsetup[spwid]['BB'], nchanperbb[spwsetup[spwid]['BB'] - 1]*spwsetup[spwid]['npol']))
        else:
            print('Spw %d in BB_%d (total Nchan within BB is %d, sp avg likely applied).  dosmooth=False' % (spwid, spwsetup[spwid]['BB'], nchanperbb[spwsetup[spwid]['BB'] - 1]*spwsetup[spwid]['npol']))

    msmd.close()

    return spwsetup

def getAntennaFlagFrac(ms, fieldid, spwid, spwsetup):
    '''Get flagging fraction for each antenna, in a vector listed ordered by order given in spwsetup dictionary.
    ms: Input MS
    fieldid: Field ID used to select data
    spwid: SPW used to select data
    spwsetup: Spectral setup dictionary, as obtained from getSpecSetup()
    '''
    af = createCasaTool(aftool)
    af.open(ms)
    af.selectdata(field = str(fieldid), spw = str(spwid))
    af.parsesummaryparameters()
    af.init()
    flag_stats_dict = af.run()
    af.done()
    antflag = flag_stats_dict['report0']['antenna']
    antflag_frac = [antflag[spwsetup['antnames'][antid]]['flagged']/antflag[spwsetup['antnames'][antid]]['total'] for antid in spwsetup['antids']]
    return antflag_frac

def getCalAtmData(ms, spws, spwsetup):
    tb = createCasaTool(tbtool)
    #Open CALATMOSPHERE table
    tb.open(os.path.join(ms, 'ASDM_CALATMOSPHERE'))
    #Get weather parameters
    tground_all = tb.getcol('groundTemperature')
    pground_all = tb.getcol('groundPressure')
    hground_all = tb.getcol('groundRelHumidity')
    #Get the spectra of the ATM measurements
    tmatm_all = {}
    tsys = {}
    trec = {}
    tau = {}
    antatm = {}
    for spwid in spws:
        minfreq = np.min(spwsetup[spwid]['chanfreqs'])
        maxfreq = np.max(spwsetup[spwid]['chanfreqs'])
        midfreq = 0.5*(minfreq+maxfreq)
        samebbspw = [s for s in spwsetup['spwlist'] if spwsetup[s]['BBname'] == spwsetup[spwid]['BBname']]
        subtb = tb.query('basebandName=="{0:s}" && syscalType=="TEMPERATURE_SCALE"'.format(str(spwsetup[spwid]['BBname'])))
        tmatm_all[spwid] = tb.getcol('startValidTime')
        #Get frequency vector and find section belonging to this SPW
        fullfreq = subtb.getcell('frequencySpectrum', 0)
        sectionmid = []
        freqsection = []
        chansec = []
        chanidx = 0
        for s in samebbspw:
            chansec.append([chanidx,chanidx+spwsetup[s]['nchan']])
            section = fullfreq[chanidx:chanidx+spwsetup[s]['nchan']]
            chanidx += spwsetup[s]['nchan']
            sectionmid.append(np.mean(section))
            freqsection.append(section)
        thissection = np.argsort(np.abs(np.array(sectionmid) - midfreq))[0]
        freq = freqsection[thissection]
        order = np.argsort(freq)
        startchan = chansec[thissection][0]
        endchan = chansec[thissection][1]
        #Load Tsys and Trx tables
        auxtsys = [subtb.getcell('tSysSpectrum', i) for i in range(subtb.nrows())]
        auxtrec = [subtb.getcell('tRecSpectrum', i) for i in range(subtb.nrows())]
        # (npols, nchantsys, nrowstsys) = np.shape(auxtsys)
        npols = auxtsys[0].shape[0]
        nrowstsys = len(auxtsys)
        tsys[spwid] = np.zeros((npols, spwsetup[spwid]['nchan'], nrowstsys))
        trec[spwid] = np.zeros((npols, spwsetup[spwid]['nchan'], nrowstsys))
        #Resample the curves to match the frequency of the data SPWs
        for pol in range(npols):
            for row in range(nrowstsys):
                tsysfit = CubicSpline(freq[order], auxtsys[row][pol][startchan:endchan][order], bc_type='not-a-knot')
                trecfit = CubicSpline(freq[order], auxtrec[row][pol][startchan:endchan][order], bc_type='not-a-knot')
                tsys[spwid][pol,:,row] = tsysfit(spwsetup[spwid]['chanfreqs'])
                trec[spwid][pol,:,row] = trecfit(spwsetup[spwid]['chanfreqs'])
        antatm[spwid] = subtb.getcol('antennaName')
        #To get the skyline list, pick index of first antenna, first entry of tau, for the first polarization
        #Resample according to the same procedure used for Tsys and Trx
        auxtau = subtb.getcell('tauSpectrum', 0)[0]
        taufit = CubicSpline(freq[order], auxtau[startchan:endchan][order], bc_type='not-a-knot')
        tau[spwid] = taufit(spwsetup[spwid]['chanfreqs'])
    tb.close()

    return (tground_all, pground_all, hground_all, tmatm_all, tsys, trec, tau, antatm)

def makeNANmetrics(fieldid, spwid, nmodels):
    metricdtypes = np.dtype([('maxabs', float), ('maxabserr', float), ('intabs', float), ('intabserr', float), ('maxabsdiff', float), ('maxabsdifferr', float), ('intabsdiff', float), ('intabsdifferr', float), ('intsqdiff', float), ('intsqdifferr', float)])
    metrics = {fieldid: {spwid: np.zeros(nmodels, dtype = metricdtypes)}}
    for k in range(nmodels):
        metrics[fieldid][spwid]['maxabs'][k] = np.nan
        metrics[fieldid][spwid]['maxabserr'][k] = np.nan
        metrics[fieldid][spwid]['intabs'][k] = np.nan
        metrics[fieldid][spwid]['intabserr'][k] = np.nan
        metrics[fieldid][spwid]['maxabsdiff'][k] = np.nan
        metrics[fieldid][spwid]['maxabsdifferr'][k] = np.nan
        metrics[fieldid][spwid]['intabsdiff'][k] = np.nan
        metrics[fieldid][spwid]['intabsdifferr'][k] = np.nan
        metrics[fieldid][spwid]['intsqdiff'][k] = np.nan
        metrics[fieldid][spwid]['intsqdifferr'][k] = np.nan
    return metrics

def makePlot(nu = None, tmavedata = None, skychansel = None, scisrcsel = None, bline = None,
             tau = None, title = None, output = None, isize = 300, psize = 2,
             highbuf = 0.5, lowbuf = 0.3, atmhighbuf = 0.05,
             diffsmoothbox = 1, ischosen = None, takediff = False, xlabel = None, ylabel = None):

    (npol, nchan) = np.shape(tmavedata)
    #Choose whether to plot data as-is or its derivative
    if takediff:
        ydata = np.ma.MaskedArray(np.zeros((npol, nchan-1)), mask=np.zeros((npol, nchan-1)))
        for ipol in range(npol):
            ydata[ipol] = np.ma.abs(np.ma.diff(tmavedata[ipol]))
            ydata.mask[ipol] = tmavedata.mask[ipol][1:]
        xdata = nu[1:]
        seldata = scisrcsel[1:]
        if ylabel is None:
            ylabel = 'Abs(Amp[i+1]-Amp[i])'
    else:
        ydata = tmavedata
        xdata = nu
        seldata = scisrcsel
        if ylabel is None:
            ylabel = 'Corr Amp [Jy]'
    #Also apply smoothing if requested
    if diffsmoothbox > 1:
        newnpol, newnchan = np.shape(ydata)
        newydata = np.ma.MaskedArray(np.zeros((newnpol, newnchan)), mask=np.zeros((newnpol, newnchan)))
        for ipol in range(npol):
            newydata[ipol] = smooth(ydata[ipol], diffsmoothbox)
            newydata.mask[ipol] = enlargesel(ydata.mask[ipol], diffsmoothbox)
        ydata = newydata
        if xlabel is None:
            xlabel = 'Freq [Ghz] (smooth:'+str(diffsmoothbox)+' ch)'
    elif xlabel is None:
        xlabel = 'Freq [Ghz]'

    #Calculate data limit for plot
    try:
        mindata = np.ma.min(ydata)
        maxdata = np.ma.max(ydata)
        datarange = np.abs(maxdata - mindata)
        ymin = mindata - lowbuf*datarange
        ymax = maxdata + highbuf*datarange
    except:
        print('Could not determine normalization of data! is all of it flagged??')
        datarange = 2.0
        ymin = -1.0
        ymax = 1.0
    xmin = np.min(xdata) - 0.02*(np.max(xdata)-np.min(xdata))
    xmax = np.max(xdata) + 0.02*(np.max(xdata)-np.min(xdata))

    #Plot data before correction
    plt.clf()
    #Data axis ax1
    fig, ax1 = plt.subplots()
    fig.set_size_inches(8, 6)
    ax1.set_title(title)
    for ipol in range(npol):
        thisstyle = ('blue' if (ipol == 0) else 'red')
        ax1.plot(xdata, ydata[ipol], '.', color=thisstyle, markersize=psize)
        if np.sum(scisrcsel) > 0:
            ax1.plot(xdata[seldata], ydata[ipol][seldata], '.', color='green', markersize=psize)
        if bline is not None:
            ax1.plot(nu, bline[ipol], '--', color=thisstyle)

    ax1.set_xlabel(xlabel)
    ax1.set_ylabel(ylabel)
    ax1.set_xlim((xmin,xmax))
    ax1.set_ylim((ymin,ymax))
    #Add Accepted/Discarded text
    if ischosen is not None:
        if ischosen:
            ax1.text(0.15*xmin+0.85*xmax, 0.1*ymin+0.9*ymax, 'Applied', fontsize = 14, color = 'black', fontweight = 'bold')
        else:
            ax1.text(0.15*xmin+0.85*xmax, 0.1*ymin+0.9*ymax, 'Discarded', fontsize = 12, color = 'gray', fontweight = 'normal')

    #Plot atmospheric transmission and skyline windows, if provided
    if tau is not None:
        transm = 100.0*np.exp(-1.0*tau)
        mintr = np.min(transm)
        maxtr = np.max(transm)
        trrange = maxtr - mintr
        lowbuf2 = (1 + atmhighbuf)*(1+lowbuf)/highbuf
        ymin2 = mintr - lowbuf2*trrange
        ymax2 = maxtr + atmhighbuf*trrange
        ywin = ymin2 + 0.1*lowbuf2*trrange
        ax2 = ax1.twinx() 
        if np.sum(skychansel) > 0:
            ax2.plot(nu[skychansel], ywin*np.ones(np.sum(skychansel)), 's', color='black', markersize=psize)
        ax2.plot(nu, transm, linestyle='solid', linewidth=1.0, color='magenta')
        ax2.set_ylim((ymin2, ymax2))
        ax2.set_yticks([mintr, 0.5*(maxtr+mintr), maxtr])
        ax2.set_ylabel('% ATM Transmission', color = 'magenta')

    plt.savefig(output, dpi=isize)
    plt.close(fig)

    return

def atmcorr(ms, datacolumn = 'CORRECTED_DATA', iant = 'auto', atmtype = 1,
            maxalt = 120.0, lapserate = -5.6, scaleht = 2.0,
            jyperkfactor = None, dobackup = False, forcespws = None, forcefield = None, forcemetricline = None,
            blinedeg = 3, blinenclip = 1, blinensigma = 5.0, maxonlyspw = False,
            minpeaklevel = 0.05, timestamp = None, plotsfolder = None, diffsmooth = 0.002,
            psize = 2, isize = 300, plotbline = False,
            defatmtype = 1, defmaxalt = 120, deflapserate = -5.6, defscaleht = 2.0,
            decisionmetric = 'intabsdiff'):
    ''' Task to apply CSV-3320 correction for atmospheric lines.
    Parameter list:
    ms: (first argument) MS file to be processed
    output: Compute the metrics over the skyline residuals and return them
    for best parameter evaluation. Output in this case is a tuple (models, metrics) where models is an array
    of the models used, and metrics is a dictionary containing arrays of metric results for each one
    of the models for each field and SPW, structured like: metrics[FIELDID][SPW]
    Keyword arguments:
    datacolumn: Column to apply correction to, options: 'DATA', 'CORRECTED_DATA'(default)
    iant: Antenna number to get pointing direction (antenna having mount issues should
    be avoided). Also used to test atmospheric residuals. Default is 0.
    jyperkfactor: Dictionary of Jy/K factors for each EB, antenna and SPW. Default will
    assume a factor of 1.0. To be obtained from getJyperKfromCSV() function
    dobackup: Whether to do a backup of the MS before applying the correction.
    (True/False) Default is True. 
    forcespws: Force the list of SPWs to process. If not specified, task will do all SPWs.
    forcefield: Force the FIELD ID to process. If not specified task will pick the first one.
    blinedeg: Degree of the baseline fitting polynomial.
    blinenclip: Number of sigma clipping iterations to use in the baseline fitting routine.
    blinensigma: Significance in number of sigmas for the sigma clipping routine that fit the
    baseline polynomial.
    maxonlyspw: Whether to select the SPW with largest skyline only. (Default: True)
    timestamp: Timestamp string to use for the model plots folder
    plotsfolder: Explicit folder name for model plots folder. If given, timestamp will be ignored.
    Following parameters define the atmospheric model. For runmode = 'try', the parameters below
    can be vectors, in that case the task will try all combinations and return a tuple of the list
    of models and the results of the metrics for each model on the residual of skylines.
    amttype: Atmosphere type paremeter for model in at module (1: tropical, 2: mid lat summer,
    3: mid lat winter, etc). Default is 2
    maxalt: maxAltitude parameter for model, in km. default is 120 (km)
    lapserate: Lapse Rate dTem_dh parameter for at (lapse rate; K/km). Default is -5.6
    scaleht: h0 parameter for at (water scale height; km). Default is 2.0
    '''

    qa = createCasaTool(qatool)
    ms = str(ms)
    #Do a backup of the MS if requested
    backupms = ms.replace('.ms','.ms.backup')
    if dobackup and not os.path.exists(backupms):
        os.system('cp -r '+ms+' '+backupms)
    #If there is no timestamp, generate new one
    if timestamp is None:
        timestamp = getTimeStamp()
    #If there is no name for the results file, create one
    if plotsfolder is None:
        plotsfolder = 'modelplots_'+timestamp
    #Make plot models folder, if needed
    if not os.path.exists(plotsfolder):
        os.system('mkdir '+plotsfolder)

    ################################################################
    ### Get metadata
    ################################################################
    print('Obtaining metadata for MS: '+ms)
    spwsetup = getSpecSetup(ms)
    #chanfreqs = {}
    msmd = createCasaTool(msmdtool)
    msmd.open(ms)
    #Get data times for OBSERVE_TARGET
    tmonsource = msmd.timesforintent('OBSERVE_TARGET#ON_SOURCE')
    tmoffsource = msmd.timesforintent('OBSERVE_TARGET#OFF_SOURCE')
    #Make table of subscans while on source
    onsourcetab = segmentEdges(tmonsource, 5.0, 'onsource')
    #Initialize list of all SPWs to work on
    spws = spwsetup['spwlist']
    metricskylineids = 'all'
    print('>>> forcespws='+str(forcespws)+' >>> metricskylineids='+str(forcemetricline))
    print('>>> spws='+str(spws)+' >>> metricskylineids='+str(metricskylineids))
    msmd.close()
    #Set fields to correct
    if (forcefield is not None) and (type(forcefield) == int):
        fieldid = forcefield
    if (forcefield is not None) and (type(forcefield) == str) and forcefield.isdigit():
        fieldid = int(forcefield)
    else:
        #We could not receive the fieldid from calling method, default to first field
        fieldid = spwsetup['fieldid']['*OBSERVE_TARGET#ON_SOURCE*'][0]
    bnd = (pl.diff(tmoffsource)>1)
    w1 = pl.append([True], bnd)
    w2 = pl.append(bnd, [True])
    tmoffsource = (tmoffsource[w1]+tmoffsource[w2])/2.  ### midpoint of OFF subscan

    ################################################################
    ### Get atmospheric parameters for ATM
    ################################################################
    print('Obtaining atmospheric parameters for MS: '+ms)
    tb = createCasaTool(tbtool)
    tb.open(os.path.join(ms, 'ASDM_CALWVR'))
    tmpwv_all = tb.getcol('startValidTime')
    pwv_all = tb.getcol('water')
    tb.close()

    #Open CALATMOSPHERE table
    (tground_all, pground_all, hground_all, tmatm_all, tsys, trec, tau, antatm) = getCalAtmData(ms, spws, spwsetup)
    tmatm = pl.unique(tmatm_all[spws[0]])

    #Search for sky lines
    skylines = {}
    skylinesbroad = {}
    for spwid in spws:
        skylines[spwid] = getskylines(tau[spwid], spwid, spwsetup, fraclevel = 0.3, minpeaklevel = minpeaklevel)
        skylinesbroad[spwid] = getskylines(tau[spwid], spwid, spwsetup, fraclevel = 0.2, minpeaklevel = minpeaklevel)

    #Select SPWs to process
    spwstoprocess = []
    if forcespws is None:
        for spwid in spws:
            #If there is at least one skyline in this SPW, add this spw to the ones to be processed
            if (skylines[spwid]['npeak'] > 0):
                spwstoprocess.append(spwid)
    elif (forcespws is not None) and (type(forcespws) == list):
        spwstoprocess = forcespws
    elif (forcespws is not None) and (type(forcespws) == int):
        spwstoprocess = [forcespws]
    else:
        spwstoprocess = []
    print('initial spwstoprocess='+str(spwstoprocess))

    #If only one SPW is to be processed, pick the best one
    if (len(spwstoprocess) > 0) and (forcemetricline is None) and maxonlyspw:
        (bestspw, bestpeak) = gradeskylines(skylines)
        spwstoprocess = [bestspw]
        metricskylineids = [bestpeak]
    elif (len(spwstoprocess) > 0) and (forcemetricline is None) and not maxonlyspw:
        (bestspw, bestpeak) = gradeskylines(skylines)
        spwstoprocess = [bestspw]
        metricskylineids = 'all'
    if (len(spwstoprocess) > 0) and (forcemetricline is not None) and (type(forcemetricline) == list):
        metricskylineids = forcemetricline
    if (len(spwstoprocess) > 0) and (forcemetricline is not None) and (type(forcespws) == int):
        metricskylineids = [forcemetricline]
    print('selecting spwstoprocess='+str(spwstoprocess)+' metricskylineids='+str(metricskylineids))
    #Smoothing box for diff metrics
    if (len(spwstoprocess) > 0):
        diffsmoothbox = max(1,int(np.round(diffsmooth*np.max([spwsetup[s]['nchan'] for s in spwstoprocess]))))
    else:
        diffsmoothbox = 1

    ################################################################
    ### Define list of models to try
    ################################################################
    defmodel = (defatmtype, defmaxalt, deflapserate, defscaleht)
    if 'int' in str(type(atmtype)):
        atmtype = [atmtype]
    if ('int' in str(type(maxalt))) or ('float' in str(type(maxalt))):
        maxalt = [maxalt]
    if ('int' in str(type(lapserate))) or ('float' in str(type(lapserate))):
        lapserate = [lapserate]
    if ('int' in str(type(scaleht))) or ('float' in str(type(scaleht))):
        scaleht = [scaleht]
    modtypes = np.dtype([('atmtype', int), ('maxalt', float), ('lapserate', float), ('scaleht', float)])
    models = np.array([model for model in product(atmtype, maxalt, lapserate, scaleht)], dtype = modtypes)
    nmodels = len(models)
    metricdtypes = np.dtype([('maxabs', float), ('maxabserr', float), ('intabs', float), ('intabserr', float), ('maxabsdiff', float), ('maxabsdifferr', float), ('intabsdiff', float), ('intabsdifferr', float), ('intsqdiff', float), ('intsqdifferr', float)])
    print('fieldid: '+str(fieldid)+' spwstoprocess: '+str(spwstoprocess))
    #Create metric output dictionary
    if (len(spwstoprocess) > 0) and (jyperkfactor is not None):
        #Case where we have at least one skyline available
        print('metricskylineids: '+str(metricskylineids))
        metrics = {fieldid: {spwid: np.zeros(nmodels, dtype = metricdtypes) for spwid in spwstoprocess}}
    #If no peak, abort process!!
    else:
        #We either have no skylines or no jy/K factor. Return the correct message
        if len(spwstoprocess) == 0:
            print('No skylines! Reverting to default model...')
        if jyperkfactor is None:
            print('No Jy/K factor!! Cannot perform calculation, reverting to default model...')
        spwid = spws[0]
        metrics = makeNANmetrics(fieldid, spwid, nmodels)
        bestmodels = defmodel
        fitstatus = 'defaultmodel'
        return (bestmodels, models, metrics, fitstatus, spwstoprocess, metricskylineids)

    pwv, tground, pground, hground = [], [], [], []
    for tt in tmatm:
        deltat = abs(tmpwv_all-tt)
        pwv.append(pl.median(pwv_all[deltat==deltat.min()]))
        tground.append(pl.median(tground_all[tmatm_all==tt]))
        pground.append(pl.median(pground_all[tmatm_all==tt]))
        hground.append(pl.median(hground_all[tmatm_all==tt]))
        print('PWV = %fm, T = %fK, P = %fPa, H = %f%% at %s' % (pwv[-1], tground[-1], pground[-1], hground[-1], qa.time('%fs' % tt, form='fits')[0]))

    ################################################################
    ### Looping over spws
    ################################################################
    print('start processing '+ms+' ...')
    print('will go over SPWs: '+str(spwstoprocess))

    ################################################################
    ## Apply correction using all the different models
    ################################################################

    tmpfolder = 'tmpapply_'+timestamp
    os.system('mkdir '+tmpfolder)
    tmpspwstr = ','.join([str(s) for s in spwstoprocess])
    #If for the test field we have a separate OFF position, create list with both
    if len(spwsetup['scifieldidoff'][str(fieldid)]) > 0:
        testfields = ','.join([str(s) for s in sorted([str(fieldid)]+[spwsetup['scifieldidoff'][str(fieldid)]])])
    else:
        testfields = str(fieldid)
    testspws = ','.join([str(s) for s in spwstoprocess])
    #Split out the relevant fields and SPWs to a smaller MS for testing models
    testms = ms + '.modeltest'
    #mstransform(vis = ms, outputvis = tmpfolder+'/'+testms, field = testfields, spw = testspws, antenna = str(iant)+'&&'+str(iant), datacolumn = 'all', reindex = False)

    #Select antenna to be used, if not selected
    if (type(iant) == int) or ((type(iant) == str) and iant.isnumeric()):
        iantsel = int(iant)
        print('Test data selected forced to use antenna {0:d} ({1:s})'.format(iantsel,spwsetup['antnames'][iantsel]))
    else:
        antflagfrac = getAntennaFlagFrac(ms, testfields, spwid, spwsetup)
        iantsel = np.argsort(antflagfrac)[0]
        print('Test data selected automatically with antenna {0:d} ({1:s})'.format(iantsel,spwsetup['antnames'][iantsel]))

    #String with data column to be used in sdatmcor command
    if datacolumn == 'CORRECTED_DATA':
        sddatacolumn = 'corrected'
    else:
        sddatacolumn = 'float_data'

    #cycle over all models, calling sdatmcor
    for k in range(nmodels):
        for spwid in spwstoprocess:
            strmodel = '{0:d}/{1:d}: (atmType,maxAlt,scaleht,lapserate)=({2:d},{3:.2f}km,{4:.2f}km,{5:.2f}K/km)'.format(k+1, nmodels, models['atmtype'][k], models['maxalt'][k], models['scaleht'][k], models['lapserate'][k])
            print('Correcting data with model '+strmodel)
            outfname = tmpfolder+'/'+ms.replace('.ms','.spw'+str(spwid)+'.model'+str(k)+'.ms')
            sdatmcor(infile=ms, datacolumn=sddatacolumn, outfile=outfname,
                     overwrite=False, spw=str(spwid), antenna = str(iantsel)+'&&'+str(iantsel), field = testfields,
                     gainfactor=jyperkfactor[ms][str(spwid)],
                     dtem_dh=str(models['lapserate'][k])+'K/km', h0=str(models['scaleht'][k])+'km',
                     atmtype=int(models['atmtype'][k]), atmdetail=False)

    #Open uncorrected data to measure skylines and presence of science target
    tb.open(ms, nomodify=False)
    for spwid in spwstoprocess:
        print('Processing spw '+str(spwid))
        nu = spwsetup[spwid]['chanfreqs']/(1.e+09)

        ################################################################
        ### Calculate and apply correction values
        ################################################################
        querystr = 'DATA_DESC_ID in {0:s} && FIELD_ID in {1:s}'.format(str(spwsetup[spwid]['ddi']), str(fieldid))
        print('Reading data for TaQL query: '+querystr)
        subtb = tb.query(querystr)
        tmdata = subtb.getcol('TIME')
        data = subtb.getcol(datacolumn)
        flag = subtb.getcol('FLAG')
        flagrow = subtb.getcol('FLAG_ROW')
        ant1 = subtb.getcol('ANTENNA1')
        npol = data.shape[0]
        #Add the flag ROW flag to the individual row flag arrays,
        #in order to use only one array.
        flaggedrowlist = np.where(flagrow)[0]
        for row in flaggedrowlist:
            flag[:,:,row] = np.logical_or(flag[:,:,row],~flag[:,:,row])
        #Create selection vector for on-source rows
        onsel = selectRanges(tmdata, onsourcetab)
        tmdataon = tmdata[onsel]
        antson = ant1[onsel]

        #Create masked data numpy array for ease of use, cdata to contain the data before correction
        #and diffbuffer after correction
        cdata = np.ma.masked_array(np.real(data.copy()), mask=flag, fill_value=0.0)

        #Search for science target channels
        dataon = np.transpose(np.transpose(cdata)[onsel])
        #Pre-correction average
        precorravedataon = np.ma.mean(dataon, axis = 2)
        maskedchans = np.any(precorravedataon.mask, axis = 0)

        #scisrcsel = selsource2(spwid, spwsetup, nu, tmdataon, antson, dataon, tmatm_all[spwid], antatm[spwid], tsys[spwid], trec[spwid], tau[spwid])
        scisrcsel = selsource(spwid, spwsetup, nu, tmdataon, antson, dataon, tmatm_all[spwid], antatm[spwid], tsys[spwid], trec[spwid], tau[spwid])
        if (scisrcsel is not None) and (len(scisrcsel) > 0):
            scisrcsel[maskedchans] = False
            nchsci = int(np.sum(scisrcsel))
        else:
            nchsci = 0
        isscisrc = (nchsci > 0)
        #Create a selection vector for baseline subtraction including the line but removing possible
        #overlap with science target channels
        #Narrow sky channels selection for measuring the metrics, broad for baseline fitting
        skychansel = skysel(skylines[spwid], linestouse = metricskylineids)
        skychansel[maskedchans] = False
        skychanselbroad = skysel(skylinesbroad[spwid])
        skychanselbroad[maskedchans] = False
        #Make baseline channel selection excluding both science target and sky lines
        baselinesel = np.ones(len(nu), dtype=bool)
        if isscisrc:
            skychansel[scisrcsel] = np.zeros(nchsci, dtype=bool)
            skychanselbroad[scisrcsel] = np.zeros(nchsci, dtype=bool)
            baselinesel[scisrcsel] = np.zeros(nchsci, dtype=bool)
        baselinesel[skychanselbroad] = np.zeros(int(np.sum(skychanselbroad)), dtype=bool)
        #Initialize variables for baseline subtraction
        blinefit = {}
        blinemodel = {}
        metricnorm = -99

        #If we are left with no channels with skylines, we are in trouble
        if np.sum(skychansel) == 0:
            print('Could not select skyline channels! Aborting...')
            metrics = makeNANmetrics(fieldid, spwid, nmodels)
            bestmodels = defmodel
            fitstatus = 'defaultmodel'
            return (bestmodels, models, metrics, fitstatus, spwstoprocess, metricskylineids)

        #Pre-correction average
        #precorrdataon = np.transpose(np.transpose(cdata)[onsel])
        #precorravedataon = np.ma.mean(precorrdataon, axis = 2)
        normsample = dataon[:,skychansel]
        #Try to calculate the normalizing value for the metrics
        #If is cannot calculate it, fill default value of 1
        #similar thing for plot ranges
        try:
            metricnorm = np.ma.max(np.ma.abs(normsample))
        except:
            print('Could not determine normalization of data! is all of it flagged??')
            metricnorm = 1.0

        # xmin = np.min(nu) - 0.02*(np.max(nu)-np.min(nu))
        # xmax = np.max(nu) + 0.02*(np.max(nu)-np.min(nu))
        # try:
        #     mindata0 = np.ma.min(precorravedataon)
        #     maxdata0 = np.ma.max(precorravedataon)
        #     ymin0 = mindata0 - 1.0*np.abs(maxdata0 - mindata0)
        #     ymax0 = maxdata0 + 1.0*np.abs(maxdata0 - mindata0)
        #     yatm0 = maxdata0 + 0.5*np.abs(maxdata0 - mindata0)
        # except:
        #     print('Could not determine normalization of data! is all of it flagged??')
        #     ymin0 = -1.0
        #     ymax0 = 1.0
        #     yatm = 0.5

        #Plot data before correction
        # plt.clf()
        # for ipol in range(npol):
        #     thisstyle = ('blue' if (ipol == 0) else 'red')
        #     plt.plot(nu, precorravedataon[ipol], '.', color=thisstyle, markersize=psize)
        #     if isscisrc:
        #         plt.plot(nu[scisrcsel], precorravedataon[ipol][scisrcsel], '.', color='green', markersize=psize)
        #     #plt.plot(nu[skychansel], precorravedataon[ipol][skychansel], 's', color='magenta', markersize=psize)
        #     plt.plot(nu[skychansel], yatm0*np.ones(np.sum(skychansel)), 's', color='magenta', markersize=psize)
        # plt.xlabel('Freq [Ghz]')
        # plt.ylabel('Corr Amp [Jy]')
        # strmodel = 'EB:{0:s}\nSPW:{1:s}, Field:{2:s}'.format(ms, str(spwid), spwsetup['namesfor'][str(fieldid)][0])
        # plt.title('Uncorrected data '+strmodel)
        # plt.xlim((xmin,xmax))
        # plt.ylim((ymin0,ymax0))
        # plt.savefig(plotsfolder+'/'+ms+'.field'+str(fieldid)+'.spw'+str(spwid)+'.nocorr.'+str(k)+'.old.png',dpi=isize)
        makePlot(nu=nu, tmavedata=precorravedataon, skychansel=skychansel, scisrcsel=scisrcsel, tau=tau[spwid],
                 title=strmodel, diffsmoothbox=1, takediff=False, ischosen=None, isize = isize, psize = psize,
                 output=plotsfolder+'/'+ms+'.field'+str(fieldid)+'.spw'+str(spwid)+'.nocorr.png')

    #End of processing uncorrected dataset, close it
    tb.close()

    #Lists of plots to do after looping over all models
    plotlist = []
    plotlistnobline = []
    plotlistdiff = []

    #Open uncorrected data to measure skylines and presence of science target
    for spwid in spwstoprocess:
        #cycle over all models
        for k in range(nmodels):
            strmodel = '{0:d}/{1:d}: ({2:d},{3:.2f}km,{4:.2f}km,{5:.2f}K/km)\nEB:{6:s}\nSPW:{7:s}, Field:{8:s}'.format(k+1, nmodels, models['atmtype'][k], models['maxalt'][k], models['scaleht'][k], models['lapserate'][k], ms, str(spwid), spwsetup['namesfor'][str(fieldid)][0])
            print('Going over model '+strmodel)
            msk = tmpfolder+'/'+ms.replace('.ms','.spw'+str(spwid)+'.model'+str(k)+'.ms')

            print('Processing spw '+str(spwid))
            nu = spwsetup[spwid]['chanfreqs']/(1.e+09)

            ################################################################
            ### Read corrected data for model k
            ################################################################
            tb.open(msk, nomodify=False)
            querystr = 'DATA_DESC_ID in {0:s} && FIELD_ID in {1:s}'.format(str(spwsetup[spwid]['ddi']), str(fieldid))
            print('Reading data for TaQL query: '+querystr)
            subtb = tb.query(querystr)
            tmdatak = subtb.getcol('TIME')
            datak = subtb.getcol('DATA')
            flagk = subtb.getcol('FLAG')
            flagrowk = subtb.getcol('FLAG_ROW')
            #Add the flag ROW flag to the individual row flag arrays,
            #in order to use only one array.
            flaggedrowlistk = np.where(flagrowk)[0]
            for row in flaggedrowlistk:
                flagk[:,:,row] = np.logical_or(flagk[:,:,row],~flag[:,:,row])

            #Create selection vector for on-source rows
            onselk = selectRanges(tmdatak, onsourcetab)

            #Create masked data numpy array for ease of use, data after correction
            cdatak = np.ma.masked_array(np.real(datak.copy()), mask=flagk, fill_value=0.0)

            #Calculate metrics for model k
            #First select channel range, and average over time
            diffdataonk = np.transpose(np.transpose(cdatak)[onselk])
            npolk, nchk, nrowk = np.shape(diffdataonk)
            tmavedataonk = np.ma.mean(diffdataonk, axis = 2)
            tmstddataonk = np.ma.std(diffdataonk, axis = 2)/np.sqrt(nrowk)

            #Fit baseline
            for ipol in range(npol):
                blinefit[ipol] = fitbaseline(nu[baselinesel], tmavedataonk[ipol][baselinesel], tmstddataonk[ipol][baselinesel])
                if not (blinefit[ipol]['blfit'] is None):
                    blinemodel[ipol] = blinefit[ipol]['blfit'](nu)
                else:
                    blinemodel[ipol] = 0.0*tmavedataonk[ipol]
                #blinefit[ipol] = sigclipfit(nu[baselinesel], tmavedataonk[ipol][baselinesel], tmstddataonk[ipol][baselinesel], blinedeg, blinenclip, blinensigma, bordertokeep = 0.05, smoothselbox = 0.0)
                #blinemodel[ipol] = np.ma.sum([blinefit[ipol]['coefs'][deg]*(nu**(blinedeg-deg)) for deg in range(blinedeg+1)], axis = 0)
            #Compute baseline subtracted data
            blinesubdataon = 0.0*tmavedataonk
            for ipol in range(npol):                    
                blinesubdataon[ipol] = tmavedataonk[ipol] - blinemodel[ipol]

            #Plot corrected data with baseline fit, etc.
            if plotbline:
                bline = blinemodel
            else:
                bline = None
            plotlist.append({'nu': nu, 'tmavedata': tmavedataonk, 'skychansel': skychansel, 'scisrcsel': scisrcsel,
                             'tau': tau[spwid], 'title': 'Model '+strmodel, 'diffsmoothbox': 1, 
                             'takediff': False, 'ischosen': False, 'bline': bline,
                             'isize': isize, 'psize': psize,
                             'output': plotsfolder+'/'+ms+'.field'+str(fieldid)+'.spw'+str(spwid)+'.model.'+str(k)+'.png'})


            #Plot corrected data without baseline
            plotlistnobline.append({'nu': nu, 'tmavedata': blinesubdataon, 'skychansel': skychansel,
                                    'scisrcsel': scisrcsel, 'tau': tau[spwid], 'title': 'Model '+strmodel,
                                    'diffsmoothbox': 1, 'takediff': False, 'ischosen': False,
                                    'isize': isize, 'psize': psize, 'ylabel': 'Corr Amp (-baseline) [Jy]',
                                    'output': plotsfolder+'/'+ms+'.field'+str(fieldid)+'.spw'+str(spwid)+'.model.'+str(k)+'.nobline.png'})

            #Plot corrected data absolute value of derivative
            plotlistdiff.append({'nu': nu, 'tmavedata': tmavedataonk, 'skychansel': skychansel,
                                    'scisrcsel': scisrcsel, 'tau': tau[spwid], 'title': 'Model '+strmodel,
                                    'diffsmoothbox': diffsmoothbox, 'takediff': True, 'ischosen': False,
                                    'isize': isize, 'psize': psize,
                                    'output': plotsfolder+'/'+ms+'.field'+str(fieldid)+'.spw'+str(spwid)+'.model.'+str(k)+'.absdiff.png'})

            #Select sample data for metrics
            skysamplenobline = blinesubdataon[:,skychansel]/metricnorm
            skysample = tmavedataonk[:,skychansel]/metricnorm
            skysamplesigma = tmstddataonk[:,skychansel]/(metricnorm*np.sqrt(nrowk))
            #Calculate metrics
            (maxabs, maxabserr) = calcmetric(skysamplenobline, skysamplesigma, metrictype='maxabs')
            metrics[fieldid][spwid]['maxabs'][k] = maxabs
            metrics[fieldid][spwid]['maxabserr'][k] = maxabserr
            (intabs, intabserr) = calcmetric(skysamplenobline, skysamplesigma, metrictype='intabs')
            metrics[fieldid][spwid]['intabs'][k] = intabs
            metrics[fieldid][spwid]['intabserr'][k] = intabserr
            (maxabsdiff, maxabsdifferr) = calcmetric(skysample, skysamplesigma, metrictype='maxabsdiff', smoothbox = diffsmoothbox)
            metrics[fieldid][spwid]['maxabsdiff'][k] = maxabsdiff
            metrics[fieldid][spwid]['maxabsdifferr'][k] = maxabsdifferr
            (intabsdiff, intabsdifferr) = calcmetric(skysample, skysamplesigma, metrictype='intabsdiff', smoothbox = diffsmoothbox)
            metrics[fieldid][spwid]['intabsdiff'][k] = intabsdiff
            metrics[fieldid][spwid]['intabsdifferr'][k] = intabsdifferr
            (intsqdiff, intsqdifferr) = calcmetric(skysample, skysamplesigma, metrictype='intsqdiff', smoothbox = diffsmoothbox)
            metrics[fieldid][spwid]['intsqdiff'][k] = intsqdiff
            metrics[fieldid][spwid]['intsqdifferr'][k] = intsqdifferr

            #End of processing corrected dataset k, close it
            tb.close()

    #Pick best model
    chosenspw = spwstoprocess[0]
    chosenmetric = metrics[fieldid][chosenspw][decisionmetric]
    print('for ms: {0:s}, chosenmetric = {1:s}'.format(ms, str(chosenmetric)))
    if not np.any(np.isnan(chosenmetric)):
        idxbestmodel = np.argsort(chosenmetric)[0]
        bestmodels = models[idxbestmodel]
        fitstatus = 'bestfitmodel'
    else:
        idxbestmodel = None
        bestmodels = defmodel
        fitstatus = 'defaultmodel'
    #Set best model plot parameter as "Accepted"
    if idxbestmodel is not None:
        plotlist[idxbestmodel]['ischosen'] = True
        plotlistnobline[idxbestmodel]['ischosen'] = True
        plotlistdiff[idxbestmodel]['ischosen'] = True

    #Create all model plots
    for idx in range(len(plotlist)):
        makePlot(**plotlist[idx])
        makePlot(**plotlistnobline[idx])
        makePlot(**plotlistdiff[idx])

    return (bestmodels, models, metrics, fitstatus, spwstoprocess, metricskylineids)

def getJyperKfromCSV(jyperkfile = 'jyperk.csv', mssuffix = ''):
    ''' Get Jy/K factor from the indicated CSV file.
    Assumes columns in the file are MS,Antenna,Spwid,Polarization,Factor
    and that the first line is the header.
    '''
    if not os.path.exists(jyperkfile):
        return None
    mslist = []
    spwlist = []
    jyperklist = []
    f = open(jyperkfile, 'r')
    for i, line in enumerate(f):
        if i > 0:
            (ms, ant, spw, pol, jyperk) = line.split(',')
            mslist.append(ms + mssuffix)
            spwlist.append(spw)
            jyperklist.append(float(jyperk.replace('\n','')))
    mslist = np.array(mslist)
    spwlist = np.array(spwlist)
    jyperklist = np.array(jyperklist)
    uniquems = np.unique(mslist)
    uniquespw = np.unique(spwlist)
    output = {ms:{spw:np.mean(jyperklist[np.all([mslist == ms, spwlist == spw], axis = 0)])
               for spw in uniquespw} for ms in uniquems}

    return output

def getJyperKfromCaltable(mslist, context):
    '''Get Jy/K factors from pipeline caltables. Will determine the MS list and tables from pipeline context.

    This method could raise RuntimeError when Jy/K caltable associated with any of given MS does not exist.
    '''
    tb = createCasaTool(tbtool)
    callib = context.callibrary

    def _extract_jyperk_table(vis: str) -> str:
        """Extract name of Jy/K caltable from callibrary.

        Args:
            vis: name of MS

        Raises:
            RuntimeError: No Jy/K caltable is available.
            RuntimeError: Found more than one Jy/K caltable.

        Returns:
            Name of Jy/K caltable
        """
        caltables = callib.get_calstate(callibrary.CalTo(vis)).get_caltable(caltypes='amp')
        # there should be only one Jy/K caltable in callibrary
        if len(caltables) == 0:
            raise RuntimeError(f'No Jy/K caltable for {os.path.basename(vis)}.')
        elif len(caltables) > 1:
            raise RuntimeError(f'Detected more than one Jy/K caltables for {os.path.basename(vis)}. Something went wrong.')
        return caltables.pop()

    jyperktables = [_extract_jyperk_table(vis) for vis in mslist]

    output = {}
    for i, ms in enumerate(mslist):
        #Get SPW info for this MS
        spwsetup = getSpecSetup(ms)
        #Open Jy/K Amp table
        tb.open(jyperktables[i])
        ant1 = tb.getcol('ANTENNA1')
        spw = tb.getcol('SPECTRAL_WINDOW_ID')
        cparam = tb.getcol('CPARAM')
        jyperk = 1./np.square(np.real(cparam[0][0]))
        tb.close()
        output[ms] = {str(thisspw): np.mean(jyperk[(spw == thisspw)]) for thisspw in spw if thisspw in spwsetup['spwlist']}
    return output

def selectModelParams(mslist, context = None, jyperkfactor = None, decisionmetric = 'intabsdiff',
                      iant = 'auto', atmtype = [1,2,3,4], maxalt = [120], 
                      lapserate = [-5.6], scaleht = [2.0], resultsfile = None, plotsfolder = None,
                      forcespws = None, forcefield = None, forcemetricline = None, blinedeg = 5, blinenclip = 1, 
                      blinensigma = 10.0, maxonlyspw = False, minpeaklevel = 0.05,
                      timestamp = None, diffsmooth = 0.002, psize = 2, isize = 300,
                      defatmtype = 1, defmaxalt = 120, deflapserate = -5.6, defscaleht = 2.0,
                      plotbline = False):
    '''
    Positional parameters: mslist, context
    mslist: List of MS filenames to be processed.
    context: Pipeline context associated to this reduction.
    decisionmetric: Metric to use to take the decision on best model selection,
    options:'maxabs' (Maximum of absolute residual value, calculated after baseline subtraction),
            'intabs' (integral of absolute residual value, calculated after baseline subtraction),
            'maxabsdiff' (Maximum absolute value of residual derivative)
            'intabsdiff' (integral of absolute value of residual derivative)
            'intsqdiff' (integral of square value of residual derivative)
    Quantities are calculate over a selection of channels around the skylines.
    iant, atmtype, maxalt, lapserate, scaleht: see parameters description in atmcorr()
    defatmtype, defmaxalt, deflapserate, defscaleht: Parameters for default model in case no best fit is possible.
    minpeaklevel: Comparative minimum depth of an atmospheric line (as measured in the optical depth tau)
                  to be considered while searching for sky lines. Will only consider lines above this threshold.
                  This parameters is given in units relative to the median value of tau over the SPW.
    diffsmooth: Size of smoothing box applied during the calculation of the metrics that use derivatives.
                Size if given as a fraction of SPW.
    psize, isize: Point size and image size used for making diagnostic plots.
    resultsfile: Text output file containing models, metrics calculated and chosen best model.
    '''

    #Obtain Jy/K factor
    if (jyperkfactor is None) and (context is not None):
        #from context
        jyperkfactor = getJyperKfromCaltable(mslist, context)
    elif (jyperkfactor is None) and (context is None):
        print('Neither jy/K factor dictionary nor pipeline context given as input!!')
        print('Could not measure atmospheric line residuals, exiting to default...')

    #Output dictionaries
    models = {}
    metrics = {}
    bestmodels = {}
    fitstatus = {}
    #Default model
    #Set default atmospheric model in case determination is not possible
    defmodel = (defatmtype, defmaxalt, deflapserate, defscaleht)

    #If there is no timestamp, generate new one
    if timestamp is None:
        timestamp = getTimeStamp()
    #If there is no name for the results file, create one
    if resultsfile is None:
        resultsfile = 'modelparam_'+timestamp+'.txt'
    #Get info from first MS
    print('Obtaining metadata for First MS: '+mslist[0])
    spwsetup1 = getSpecSetup(mslist[0])
    #Set fields to correct
    if (forcefield is None) and (context is not None):
        #Try to obtain representative Field ID, if not possible,
        #the task will just pick first Field ID
        msobj = context.observing_run.get_ms(mslist[0])
        repfield, repspw = msobj.get_representative_source_spw()
        forcefield = [f for f in spwsetup1['namesfor'].keys() if spwsetup1['namesfor'][f] == repfield][0]
    elif (forcefield is None) and (context is None):
        #We could not get the fieldid from the context, default to first field
        print('No context to obtain the representative field from! using first field...')
        forcefield = None
    elif (forcefield is not None) and (type(forcefield) == int):
        fieldid = forcefield
    else:
        print('Could not understand FIELDID='+str(forcefield)+'! Taking first instead...')
        forcefield = None

    #If the SPW and/or the metric line is force to a value, will keep using it for all MSs
    spwstoprocess = forcespws
    metricskylineids = forcemetricline
    #Execute correction for all MSes
    for ms in mslist:
        (bestmodels[ms], models[ms], metrics[ms], fitstatus[ms], spwstoprocess, metricskylineids) = \
            atmcorr(ms, datacolumn = 'CORRECTED_DATA', iant = iant, atmtype = atmtype, maxalt = maxalt, lapserate = lapserate,
                    scaleht = scaleht, jyperkfactor = jyperkfactor, dobackup = False,
                    forcespws = spwstoprocess, plotsfolder = plotsfolder,
                    forcefield = forcefield, forcemetricline = metricskylineids, blinedeg = blinedeg, blinenclip = blinenclip,
                    blinensigma = blinensigma, maxonlyspw = maxonlyspw, minpeaklevel = minpeaklevel, timestamp = timestamp,
                    diffsmooth = diffsmooth, psize = psize, isize = isize, plotbline = plotbline,
                    defatmtype=defatmtype, defmaxalt=defmaxalt, deflapserate=deflapserate, defscaleht=defscaleht,
                    decisionmetric = decisionmetric)

    print('metrics: '+str(metrics))
    print('bestmodels: '+str(bestmodels))
    print('fitstatus: '+str(fitstatus))

    ms1 = mslist[0]
    f1 = list(metrics[ms1].keys())[0]
    s1 = list(metrics[ms1][f1].keys())[0]
    paramnames = models[ms1].dtype.names
    metricnames = [item for item in metrics[ms1][f1][s1].dtype.names if 'err' not in item]
    nmodels = len(models[ms1])

    f = open(resultsfile, 'w')
    #Write summary of results
    f.write('#Summary of resulting best models\n')
    f.write('#MS,FieldID,SPW,'+','.join(paramnames)+',fitstatus'+'\n')
    for ms in mslist:
        f.write('{0:s},{1:d},{2:d},{3:d},{4:.6f},{5:.6f},{6:.6f},{7:s}\n'.format(ms,f1,s1,bestmodels[ms][0],bestmodels[ms][1],bestmodels[ms][2],bestmodels[ms][3],fitstatus[ms]))

    #All models should be the same for all MSs, so let's just print the first one (?)
    f.write('#List of models attempted\n')
    f.write('#Nmodel,'+','.join(paramnames)+'\n')
    for m in range(nmodels):
        f.write('{0:d},{1:d},{2:.6f},{3:.6f},{4:.6f}\n'.format(m,models[ms1][m][0],models[ms1][m][1],models[ms1][m][2],models[ms1][m][3]))

    #Write results for the metrics
    f.write('#Metrics results\n')
    f.write('#MS,Nmodel,'+(','.join(['{0:s},{1:s}'.format(met, met+'err') for met in metricnames]))+'\n')
    for ms in mslist:
        for m in range(nmodels):
            mdata = ','.join(['{0:.6f},{1:.8f}'.format(metrics[ms][f1][s1][m][met],metrics[ms][f1][s1][m][met+'err']) for met in metricnames])
            print('ms,m,mdata={0:s} {1:d} {2:s}\n'.format(ms,m,mdata))
            f.write('{0:s},{1:d},{2:s}\n'.format(ms,m,mdata))

    f.close()

    return (bestmodels, models, metrics, fitstatus)

def getSBData(asdm):
    f = open(asdm+'/SBSummary.xml', 'r')
    sbuid = ''
    for line in f:
        item = line.split()
        if (len(item) == 5) and ('EntityRef' in item[0]) and ('SchedBlock' in item[3]):
            sbuid = item[1].split('=')[1].replace('"','')
    sqlfmt = """SELECT PRJ_ARCHIVE_UID, PRJ_CODE, MOUS_STATUS_UID, ARCHIVE_UID AS SB_UID, SB_NAME FROM (SELECT PRJ_ARCHIVE_UID, PRJ_CODE FROM ALMA.BMMV_OBSPROJECT) JOIN (SELECT ARCHIVE_UID, PRJ_REF, MOUS_STATUS_UID, SB_STATUS_UID, SB_NAME FROM ALMA.BMMV_SCHEDBLOCK WHERE ARCHIVE_UID = '*SBUID*') ON PRJ_ARCHIVE_UID = PRJ_REF"""
    sql = sqlfmt.replace('*SBUID*', sbuid)
    conn = connect(almaoracleauth)
    cursor = conn.cursor()
    print('Querying the archive for : '+sql)
    cursor.execute(sql)
    result = cursor.fetchall()
    return result[0]

def getTimeStamp(timefmt = '{0:04d}{1:02d}{2:02d}{3:02d}{4:02d}{5:02d}'):
    #Get timestamp for output folders and files
    aux = systime.localtime()
    timestamp = timefmt.format(aux.tm_year, aux.tm_mon, aux.tm_mday, aux.tm_hour, aux.tm_min, aux.tm_sec)
    return timestamp

def redPipeSDatmcorr(iant = 'auto', atmtype = [1, 2, 3, 4],
                     maxalt = 120, lapserate = -5.6, scaleht = 2.0, decisionmetric = 'intabsdiff',
                     dobackup = True, pcode = '0000.1.00000.S', mousstatusuid = 'uid://A000/X000/X000',
                     metricspws = None, metricfield = None, metricline = None, minpeaklevel = 0.05, diffsmooth = 0.002):
    ''' Task to run SD pipeline, applying CSV-3320 correction for atmospheric lines
    between stage hsd_applycal and hsd_baseline.
    Keyword arguments:
    iant: Antenna number to get pointing direction (antenna having mount issues should
    be avoided). Default is 0.
    amttype: Atmosphere type paremeter for model in at module (1: tropical, 2: mid lat summer,
    3: mid lat winter, etc). Default is list [1, 2, 3, 4]
    maxalt: maxAltitude parameter for model, in km. default is 120 (km)
    lapserate: Lapse Rate dTem_dh parameter for at (lapse rate; K/km). Default is -5.6
    scaleht: h0 parameter for at (water scale height; km). Default is 2.0
    decisionmetric: Metric to use to take the decision on best model selection (see selectModelParams())
    metricspws: List of SPWs to use for model selection. Parameter gets passed to task selectModelParams() as forcespws.
    metricfield: List of Field to use for model selection. Currently only supported one. 
    Parameter gets passed to task selectModelParams() as forcefield.
    dobackup: Whether to do a backup of the MS before applying the correction.
    (True/False) Default is True
    pcode: Project Code to enter into the pipeline context.
    mousstatusuid: MOUS status uid to enter into the pipeline context.
    '''
    from pipeline.h.cli import h_init
    from pipeline.hsd.cli import hsd_importdata
    from pipeline.hsd.cli import hsd_flagdata
    from pipeline.h.cli import h_tsyscal
    from pipeline.hsd.cli import hsd_tsysflag
    from pipeline.hsd.cli import hsd_skycal
    from pipeline.hsd.cli import hsd_k2jycal
    from pipeline.hsd.cli import hsd_applycal
    from pipeline.hsd.cli import hsd_atmcor
    from pipeline.h.cli import h_resume
    from pipeline.hsd.cli import hsd_baseline
    from pipeline.hsd.cli import hsd_blflag
    from pipeline.hsd.cli import hsd_imaging
    from pipeline.hsd.cli import hsd_exportdata
    from pipeline.h.cli import h_save

    timestamp = getTimeStamp()
    if not pipelineloaded:
        print('Pipeline tasks not available!')
        return

    uidfilelist = glob.glob('uid___*')
    asdmlist = [item for item in uidfilelist if not '.' in item]
    print('Found the following ASDMs: '+str(asdmlist))
    #Create the working folder...
    os.system('mkdir working')
    for asdm in asdmlist:
        os.system('ln -s ../'+asdm+' working/'+asdm)
    #es = aU.stuffForScienceDataReduction()
    #Get Project Code and MOUS uid to enter dat into the context
    if accessarchive:
        (prjuid, pcode, mousstatusuid, sbuid, sbname) = getSBData(asdmlist[0])
    else:
        print('Could not access ALMA oracle database, set Project Code and MOUS uid manually.')

    os.chdir('working')
    context = h_init()
    context.set_state('ProjectSummary', 'proposal_code', pcode)
    #context.set_state('ProjectSummary', 'piname', 'unknown')
    #context.set_state('ProjectSummary', 'proposal_title', 'unknown')
    #context.set_state('ProjectStructure', 'ous_part_id', 'X0')
    #context.set_state('ProjectStructure', 'ous_title', 'Undefined')
    #context.set_state('ProjectStructure', 'ppr_file', '')
    #context.set_state('ProjectStructure', 'ps_entity_id', '')
    context.set_state('ProjectStructure', 'recipe_name', 'hsd_calimage')
    #context.set_state('ProjectStructure', 'ous_entity_id', '')
    context.set_state('ProjectStructure', 'ousstatus_entity_id', mousstatusuid)

    #Import ASDM and start pipeline
    hsd_importdata(vis=asdmlist)
    #Obtain the Jy/K factors
    #mslist = glob.glob('uid___*.ms')
    mslist = [x.name for x in context.observing_run.measurement_sets]

    #Deactivated Jy/K factor from es.
    #print('Obtaining Jy/K factors for MSs: '+str(mslist))
    #es.getJyPerK(mslist)
    #Continue pipeline
    hsd_flagdata()
    h_tsyscal()
    hsd_tsysflag()
    hsd_skycal()
    hsd_k2jycal()
    hsd_applycal()

    ###Correct atmospheric ozone lines###
    ### following recipe in CSV-3320  ###
    #Read in Jy/K factor from the CSV file (DEACTIVATED)
    #jyperkfactor = getJyperKfromCSV()

    ###Run correction routine in 'try' mode to compute best model to use among the list given###
    (bestmodels, models, metrics, fitstatus) = selectModelParams(mslist, context = context, decisionmetric = decisionmetric,
                                                                 iant = iant, atmtype = atmtype,
                                                                 maxalt = maxalt, lapserate = lapserate, scaleht = scaleht,
                                                                 forcespws = metricspws, forcefield = metricfield,
                                                                 forcemetricline = metricline, diffsmooth = diffsmooth,
                                                                 minpeaklevel = minpeaklevel, timestamp = timestamp)

    #NEW, using hsd_atmcor()
    atmtypelist = [int(bestmodels[ms][0]) for ms in mslist]
    dtem_dhlist = [str(bestmodels[ms][2])+'K/km' for ms in mslist]
    h0list = [str(bestmodels[ms][3])+'km' for ms in mslist]
    hsd_atmcor(infiles=mslist,atmtype=atmtypelist,dtem_dh=dtem_dhlist,h0=h0list)

    ### Finish ATM correction ###

    #Continue pipeline execution from sdbaseline stage
    hsd_baseline()
    hsd_blflag()
    hsd_baseline()
    hsd_blflag()
    hsd_imaging()
    hsd_exportdata()
    h_save()
    os.chdir('..')

    return
