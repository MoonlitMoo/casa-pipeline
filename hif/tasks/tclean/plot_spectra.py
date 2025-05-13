#!/usr/bin/env python

# This module has been derived from Todd Hunter's plotSpectrumFromMask module.
# It is used to create the diagnostic spectra plots for the cube imaging weblog.
import os
import re
from math import degrees

import matplotlib.pyplot as plt
import matplotlib.ticker
import numpy as np

from casatasks import imhead

import pipeline.infrastructure as infrastructure
from pipeline.infrastructure import casa_tools

LOG = infrastructure.get_logger(__name__)

ARCSEC_PER_RAD = 206264.80624709636
c_mks = 2.99792458e8


def addFrequencyAxisAbove(ax1, firstFreq, lastFreq, freqType='', spw=None,
                          fontsize=10, showlabel=True, twinx=False,
                          ylimits=None, xlimits=None):
    """
    Given the descriptor returned by plt.subplot, draws a frequency
    axis label at the top x-axis.
    firstFreq and lastFreq: in Hz
    twinx: if True, the create an independent y-axis on right edge
    freqType: 'LSRK', 'REST', etc. for axis label purposes only
    spw: integer or string (simply to add to the title)
    xlimits: tuple or list or array of 2 values (in Hz)
    """
    if showlabel:
        if (spw != '' and spw is not None):
            label = '(Spw %s) %s Frequency (GHz)' % (str(spw), freqType)
        else:
            label = '%s Frequency (GHz)' % freqType
    if twinx:
        ax2 = ax1.twiny().twinx()
        ax2.set_ylabel('Per-channel noise (mJy/beam)', color='k')
        ax2.set_ylim(ylimits)
        # ax2.set_xlabel does not work in this case, so use plt.text instead
        plt.text(0.5, 1.055, label, ha='center', va='center', transform=plt.gca().transAxes, size=11)
    else:
        ax2 = ax1.twiny()
        ax2.set_xlabel(label, size=fontsize)
    if xlimits is not None:
        ax2.set_xlim(np.array(xlimits)*1e-9)
    else:
        ax2.set_xlim(firstFreq*1e-9, lastFreq*1e-9)
    freqRange = np.abs(lastFreq-firstFreq)
    power = int(np.log10(freqRange))-9
    ax2.xaxis.set_major_locator(matplotlib.ticker.MultipleLocator(10**power))
    numberOfTicks = 0
    visibleTicks = []
    for mytick in ax2.get_xticks():
        if mytick*1e9 >= firstFreq and mytick*1e9 <= lastFreq:
            numberOfTicks += 1
            visibleTicks.append(mytick)
    if (numberOfTicks < 2):
        ax2.xaxis.set_major_locator(matplotlib.ticker.MultipleLocator(0.5*10**power))
        numberOfTicks = 0
        visibleTicks = []
        for mytick in ax2.get_xticks():
            if mytick*1e9 >= firstFreq and mytick*1e9 <= lastFreq:
                numberOfTicks += 1
                visibleTicks.append(mytick)
    ax2.xaxis.set_minor_locator(matplotlib.ticker.MultipleLocator(0.1*10**power))
    ax2.xaxis.set_major_formatter(matplotlib.ticker.ScalarFormatter(useOffset=False))


def numberOfChannelsInCube(img, returnFreqs=False, returnChannelWidth=False, verbose=False):
    """
    Finds the number of channels in a CASA image cube, using the ia tool.
    returnFreqs: if True, then also return the frequency of the
           first and last channel (in Hz)
    returnChannelWidth: if True, then also return the channel width (in Hz)
    verbose: if True, then print the frequencies of first and last channel
    Returns: an int
    -Todd Hunter
    """

    with casa_tools.ImageReader(img) as image:
        lcs = image.coordsys()

        try:
            axis = lcs.findaxisbyname('spectral')
        except:
            if image.shape().shape[0] > 3:
                LOG.warning("Can't find spectral axis. Assuming it is 3.")
                axis = 3
            elif image.shape().shape[0] > 2:
                LOG.warning("Can't find spectral axis. Assuming it is 2.")
                axis = 2
            elif image.shape().shape[0] == 2:
                LOG.error("No spectral axis found.")
                raise Exception('No spectral axis found/')

        naxes = image.shape().shape[0]
        nchan = image.shape()[axis]
        cdelt = lcs.increment()['numeric'][axis]
        pixel = [0]*naxes
        firstFreq = lcs.toworld(pixel, format='n')['numeric'][axis]
        pixel[axis] = nchan-1
        lastFreq = lcs.toworld(pixel, format='n')['numeric'][axis]
        lcs.done()

    nchan = int(nchan)
    if returnFreqs:
        if returnChannelWidth:
            return nchan, firstFreq, lastFreq, cdelt
        else:
            return nchan, firstFreq, lastFreq
    else:
        if returnChannelWidth:
            return nchan, cdelt
        else:
            return nchan


def cubeFrameToTopo(img, freqrange='', prec=4, verbose=False,
                    nchan=None, f0=None, f1=None, chanwidth=None,
                    msname='', spw='', fieldid=-1, header=''):
    """
    Reads the date of observation, central RA and Dec,
    and observatory from an image cube header and then calls lsrkToTopo or
    casaRestToTopo to return the specified frequency range in TOPO (in Hz).
    freqrange: desired range of frequencies (empty string or list = whole cube)
          floating point list of two frequencies, or a delimited string
          (delimiter = ',', '~' or space)
    prec: in fractions of Hz (only used to display the value when verbose=True)
    msname: MS name
    spw: spectral window ID (string)
    fieldid: field ID (integer)
    header: dictionary output by imheadlist
    -Todd Hunter
    """
    if header == '':
        header = imheadlist(img, omitBeam=True)
    if nchan is None or f0 is None or f1 is None or chanwidth is None:
        nchan, f0, f1, chanwidth = numberOfChannelsInCube(img, returnFreqs=True, returnChannelWidth=True)
    if 'reffreqtype' in header:
        if header['reffreqtype'].upper() == 'TOPO':
            return np.array([f0, f1])
    if len(freqrange) == 0:
        startFreq = f0
        stopFreq = f1
    elif isinstance(freqrange, str):
        if freqrange.find(',') > 0:
            freqrange = [parseFrequencyArgument(i) for i in freqrange.split(',')]
        elif freqrange.find('~') > 0:
            freqrange = [parseFrequencyArgument(i) for i in freqrange.split('~')]
        else:
            freqrange = [parseFrequencyArgument(i) for i in freqrange.split()]
        startFreq, stopFreq = freqrange
    else:
        startFreq, stopFreq = freqrange
    ra, dec = rad2radec(header['crval1'], header['crval2'], delimiter=' ', verbose=False).split()
    equinox = header['equinox']
    observatory = header['telescope']
    datestring = header['date-obs']

    with casa_tools.ImageReader(img) as image:
        lcs = image.coordsys()
        freqFrame = lcs.referencecode('spectral')[0]
        lcs.done()

    if freqFrame == 'LSRK':
        f0 = lsrkToTopo(startFreq, datestring, ra, dec, equinox, observatory, prec, verbose)
        f1 = lsrkToTopo(stopFreq, datestring, ra, dec, equinox, observatory, prec, verbose)
    elif freqFrame == 'REST':
        # REST (or rather SOURCE) frame for ephemeris objects
        if msname == '':
            LOG.warning('No MS provided for SOURCE/REST to TOPO conversion. Skipping frame conversion.')
            f0 = startFreq
            f1 = stopFreq
        else:
            c0, c1 = casaRestToTopo(startFreq, stopFreq, msname, spw, fieldid)

            # convert TOPO channel to TOPO frequency
            with casa_tools.MSMDReader(msname) as msmd:
                chanfreqs = msmd.chanfreqs(int(spw))
                if chanfreqs[1] > chanfreqs[0]:  # USB
                    f0 = chanfreqs[c0]
                    f1 = chanfreqs[c1]
                else:
                    f0 = chanfreqs[c1]
                    f1 = chanfreqs[c0]
    elif freqFrame == 'TOPO':
        f0 = startFreq
        f1 = stopFreq
    else:
        LOG.warning('Unrecognized frequency frame type: %s. Skipping frame conversion.' % (freqFrame))
        f0 = startFreq
        f1 = stopFreq

    return np.array([f0, f1])


def lsrkToTopo(lsrkFrequency, datestring, ra, dec, equinox='J2000', observatory='ALMA', prec=4, verbose=False):
    """
    Converts an LSRKfrequency and observing date/direction
    to the corresponding frequency in the TOPO frame.
    Inputs:
    lsrkFrequency: floating point value in Hz or GHz, or a string with units
    datestring:  "YYYY/MM/DD/HH:MM:SS" (format = image header keyword 'date-obs')
    ra: string "HH:MM:SS.SSSS"
    dec: string "DD.MM.SS.SSSS" or "DD:MM:SS.SSSS" (colons will be replaced with .)
    prec: only used to display the value when verbose=True
    Returns: the TOPO frequency in Hz
    -Todd Hunter
    """
    velocityLSRK = 0  # does not matter what it is, just needs to be same in both calls
    restFreqHz = lsrkToRest(lsrkFrequency, velocityLSRK, datestring, ra, dec, equinox,
                            observatory, prec, verbose)
    topoFrequencyHz = restToTopo(restFreqHz, velocityLSRK, datestring, ra, dec, equinox, observatory, verbose=verbose)
    return topoFrequencyHz


def restToTopo(restFrequency, velocityLSRK, datestring, ra, dec, equinox='J2000',
               observatory='ALMA', veltype='radio', verbose=False):
    """
    Converts a rest frequency, LSRK velocity, and observing date/direction
    to the corresponding frequency in the TOPO frame.
    Inputs:
    restFrequency: floating point value in Hz or GHz, or a string with units
    velocityLSRK: floating point value in km/s
    datestring:  "YYYY/MM/DD/HH:MM:SS"
    ra: string "HH:MM:SS.SSSS"
    dec: string "DD.MM.SS.SSSS" or "DD:MM:SS.SSSS" (colons will be replaced with .)
    prec: only used to display the value when verbose=True
    Returns: the TOPO frequency in Hz
    -Todd Hunter
    """
    topoFreqHz, diff1, diff2 = frames(velocityLSRK, datestring, ra, dec, equinox,
                                      observatory, verbose=verbose,
                                      restFreq=restFrequency, veltype=veltype)
    return topoFreqHz


def casaRestToTopo(freqstart, freqend, msname, spw, fieldid):
    """
    Converts a range of SOURCE frame frequencies to TOPO channels in a 
    specified measurement set.  The input frequencies should be the 
    center frequencies of the edge channels in the cube.
    freqstart: range start frequency in Hz
    freqend: range end frequency in Hz
    msname: MS name
    spw: spectral window ID (string)
    fieldid: field ID (integer)
    """

    lsu = casa_tools.synthesisutils

    # Figure out which channels in the ms were used to make the SOURCE frame cube
    # this function expects the center frequency of each edge channel and returns channel number in ms/spw
    result = lsu.advisechansel(msname=msname, freqframe='SOURCE',
                               ephemtable='TRACKFIELD', fieldid=fieldid,
                               freqstart='%sHz'%(str(freqstart)),
                               freqend='%sHz'%(str(freqend)))
    idx = np.where(result['spw'] == int(spw))[0]
    startChan = result['start'][idx]
    nchan = result['nchan'][idx]
    stopChan = startChan + nchan - 1
    return startChan, stopChan


def frames(velocity=286.7, datestring="2005/11/01/00:00:00",
           ra="05:35:28.105", dec="-069.16.10.99", equinox="J2000",
           observatory="ALMA", prec=4, verbose=True,
           restFreq=345.79599, veltype='optical'):
    """
    Converts an optical velocity into barycentric, LSRK and TOPO frames.
    Converts a radio LSRK velocity into TOPO frame.
    Inputs:
    velocity: in km/s
    datestring:  "YYYY/MM/DD/HH:MM:SS"
    ra: "05:35:28.105"
    dec: "-069.16.10.99"
    equinox: "J2000"
    observatory: "ALMA"
    prec: precision to display (digits to the right of the decimal point)
    veltype: 'radio' or 'optical'
    restFreq: in Hz, GHz or a string with units
    Returns:
    * TOPO frequency in Hz
    * difference between LSRK-TOPO in km/sec
    * difference between LSRK-TOPO in Hz
    - Todd Hunter
    """
    lme = casa_tools.measures
    lqa = casa_tools.quanta

    if dec.find(':') >= 0:
        dec = dec.replace(':', '.')

    position = lme.direction(equinox, ra, dec)
    obstime = lme.epoch('TAI', datestring)

    if veltype.lower().find('opt') == 0:
        velOpt = lqa.quantity(velocity, "km/s")
        dopp = lme.doppler("OPTICAL", velOpt)
        # CASA doesn't do Helio, but difference to Bary is hopefully small
        rvelOpt = lme.toradialvelocity("BARY", dopp)
    elif veltype.lower().find('rad') == 0:
        rvelOpt = lme.radialvelocity('LSRK', str(velocity)+'km/s')
    else:
        print("veltype must be 'rad'io or 'opt'ical")
        return

    lme.doframe(position)
    lme.doframe(lme.observatory(observatory))
    lme.doframe(obstime)
    lme.showframe()

    rvelRad = lme.measure(rvelOpt, 'LSRK')
    doppRad = lme.todoppler("RADIO", rvelRad)
    restFreq = parseFrequencyArgumentToGHz(restFreq)
    freqRad = lme.tofrequency('LSRK', doppRad, lme.frequency('rest', str(restFreq)+'GHz'))

    lsrk = lqa.tos(rvelRad['m0'], prec=prec)
    rvelTop = lme.measure(rvelOpt, 'TOPO')
    doppTop = lme.todoppler("RADIO", rvelTop)
    freqTop = lme.tofrequency('TOPO', doppTop, lme.frequency('rest', str(restFreq)+'GHz'))

    topo = lqa.tos(rvelTop['m0'], prec=prec)
    velocityDifference = 0.001*(rvelRad['m0']['value']-rvelTop['m0']['value'])
    frequencyDifference = freqRad['m0']['value'] - freqTop['m0']['value']

    lme.done()
    lqa.done()

    return freqTop['m0']['value'], velocityDifference, frequencyDifference


def RescaleTrans(trans, lim):
    # Input: the array of transmission values and current y-axis limits
    # Returns: arrays of the rescaled transmission values and the zero point
    #          values in units of the frame, and in amplitude.
    debug = False
    yrange = lim[1]-lim[0]
    labelgap = 0.5 # Use this fraction of the margin to separate the top
                       # curve from the upper y-axis
    TOP_MARGIN = 0.25
    y2 = lim[1] - labelgap*yrange*TOP_MARGIN/(1.0+TOP_MARGIN)
    y1 = lim[1] - yrange*TOP_MARGIN/(1.0+TOP_MARGIN)
    transmissionRange = np.max(trans)-np.min(trans)
    if (transmissionRange < 0.05):
        # force there to be a minimum range of transmission display
        # overemphasize tiny ozone lines
        transmissionRange = 0.05
    # convert transmission to amplitude
    newtrans = y2 - (y2-y1)*(np.max(trans)-trans)/transmissionRange

    # Use edge values
    edgeValueTransmission = trans[-1]
    otherEdgeValueTransmission = trans[0]

    # Now convert the edge channels' transmission values into amplitude
    edgeValueAmplitude = y2 - (y2-y1)*(np.max(trans)-trans[-1])/transmissionRange
    otherEdgeValueAmplitude = y2 - (y2-y1)*(np.max(trans)-trans[0])/transmissionRange

    # Now convert amplitude to frame units, offsetting downward by half
    # the font size
    fontoffset = 0.01
    edgeValueFrame = (edgeValueAmplitude - lim[0])/yrange  - fontoffset
    otherEdgeValueFrame = (otherEdgeValueAmplitude - lim[0])/yrange  - fontoffset

    # scaleFactor is how large the plot is from the bottom x-axis
    # up to the labelgap, in units of the transmissionRange
    scaleFactor = (1+TOP_MARGIN*(1-labelgap)) / (TOP_MARGIN*(1-labelgap))

    # compute the transmission at the bottom of the plot, and label it
    y0transmission = np.max(trans) - transmissionRange*scaleFactor
    y0transmissionFrame = 0
    y0transmissionAmplitude = lim[0]

    if (y0transmission <= 0):
        # If the bottom of the plot is below zero transmission, then label
        # the location of zero transmission instead.
        y0transmissionAmplitude = y1-(y2-y1)*(np.min(trans)/transmissionRange)
        y0transmissionFrame = (y0transmissionAmplitude-lim[0]) / (lim[1]-lim[0])
        y0transmission = 0
    return(newtrans, edgeValueFrame, y0transmission, y0transmissionFrame,
           otherEdgeValueFrame, edgeValueTransmission,
           otherEdgeValueTransmission, edgeValueAmplitude,
           otherEdgeValueAmplitude, y0transmissionAmplitude)


def lsrkToRest(lsrkFrequency, velocityLSRK, datestring, ra, dec,
               equinox='J2000', observatory='ALMA', prec=4, verbose=True):
    """
    Converts an LSRK frequency, LSRK velocity, and observing date/direction
    to the corresponding frequency in the rest frame.
    Inputs:
    lsrkFrequency: floating point value in Hz or GHz, or a string with units
    velocityLSRK: floating point value in km/s
    datestring:  "YYYY/MM/DD/HH:MM:SS" (format = image header keyword 'date-obs')
    ra: string "HH:MM:SS.SSSS"
    dec: string "DD.MM.SS.SSSS" or "DD:MM:SS.SSSS" (colons will be replaced with .)
    prec: only used to display the value when verbose=True
    Returns: the Rest frequency in Hz
    -Todd Hunter
    """
    if dec.find(':') >= 0:
        dec = dec.replace(':', '.')
        if verbose:
            print("Warning: replacing colons with decimals in the dec field.")
    freqGHz = parseFrequencyArgumentToGHz(lsrkFrequency)
    lqa = casa_tools.quanta
    lme = casa_tools.measures
    velocityRadio = lqa.quantity(velocityLSRK, "km/s")
    position = lme.direction(equinox, ra, dec)
    obstime = lme.epoch('TAI', datestring)
    dopp = lme.doppler("RADIO", velocityRadio)
    radialVelocityLSRK = lme.toradialvelocity("LSRK", dopp)
    lme.doframe(position)
    lme.doframe(lme.observatory(observatory))
    lme.doframe(obstime)
    rvelRad = lme.measure(radialVelocityLSRK, 'LSRK')
    doppRad = lme.todoppler('RADIO', rvelRad)
    freqRad = lme.torestfrequency(lme.frequency('LSRK', str(freqGHz)+'GHz'), dopp)
    lqa.done()
    lme.done()
    return freqRad['m0']['value']


def parseFrequencyArgumentToGHz(bandwidth):
    """
    Converts a frequency string into floating point in GHz, based on the units.
    If the units are not present, then the value is assumed to be GHz if less
    than 10000, otherwise it assumes Hz.
    -Todd Hunter
    """
    value = parseFrequencyArgument(bandwidth)
    if (value > 10000 or str(bandwidth).lower().find('hz') >= 0):
        value *= 1e-9
    return(value)


def parseFrequencyArgument(bandwidth):
    """
    Converts a string frequency (or dictionary) into floating point in Hz, based on
    the units.  If the units are not present, then the value is simply converted to float.
    -Todd Hunter
    """
    if (isinstance(bandwidth, dict)):
        bandwidth = str(bandwidth['value']) + bandwidth['unit']
    else:
        bandwidth = str(bandwidth)
    ghz = bandwidth.lower().find('ghz')
    mhz = bandwidth.lower().find('mhz')
    khz = bandwidth.lower().find('khz')
    hz = bandwidth.lower().find('hz')
    if (ghz>0):
        bandwidth = 1e9*float(bandwidth[:ghz])
    elif (mhz>0):
        bandwidth = 1e6*float(bandwidth[:mhz])
    elif (khz>0):
        bandwidth = 1e3*float(bandwidth[:khz])
    elif (hz>0):
        bandwidth = float(bandwidth[:hz])
    else:
        bandwidth = float(bandwidth)
    return(bandwidth)


# FIXME: function contains unresolved references, clean-up or remove if not needed by Pipeline.
def rad2radec(ra=0,dec=0,imfitdict=None, prec=5, verbose=True, component=0,
              replaceDecDotsWithColons=True, hmsdms=False, delimiter=', ',
              prependEquinox=False, hmdm=False):
    """
    Convert a position in RA/Dec from radians to sexagesimal string which
    is comma-delimited, e.g. '20:10:49.01, +057:17:44.806'.
    The position can either be entered as scalars via the 'ra' and 'dec'
    parameters, as a tuple via the 'ra' parameter, as an array of shape (2,1)
    via the 'ra' parameter, or
    as an imfit dictionary can be passed via the 'imfitdict' argument, and the
    position of component 0 will be displayed in RA/Dec sexagesimal.
    replaceDecDotsWithColons: replace dots with colons as the Declination d/m/s delimiter
    hmsdms: produce output of format: '20h10m49.01s, +057d17m44.806s'
    hmdm: produce output of format: '20h10m49.01, +057d17m44.806' (for simobserve)
    delimiter: the character to use to delimit the RA and Dec strings output
    prependEquinox: if True, insert "J2000" before coordinates (i.e. for clean or simobserve)
    Todd Hunter
    """
    if (isinstance(imfitdict, dict)):
        comp = 'component%d' % (component)
        ra  = imfitdict['results'][comp]['shape']['direction']['m0']['value']
        dec = imfitdict['results'][comp]['shape']['direction']['m1']['value']
    if (isinstance(ra, tuple) or isinstance(ra, list) or isinstance(ra, np.ndarray)):
        if (len(ra) == 2):
            dec = ra[1] # must come first before ra is redefined
            ra = ra[0]
        else:
            ra = ra[0]
            dec = dec[0]
    if np.shape(ra) == (2, 1):
        dec = ra[1][0]
        ra = ra[0][0]
    lqa = casa_tools.quanta
    myra = lqa.formxxx('%.12frad' % ra, format='hms', prec=prec+1)
    mydec = lqa.formxxx('%.12frad' % dec, format='dms', prec=prec-1)
    if replaceDecDotsWithColons:
        mydec = mydec.replace('.', ':', 2)
    if len(mydec.split(':')[0]) > 3:
        mydec = mydec[0] + mydec[2:]
    mystring = '%s, %s' % (myra, mydec)
    lqa.done()
    if (hmsdms):
        # FIXME: fix unresolved reference and/or clean-up/remove function if not needed by Pipeline.
        mystring = convertColonDelimitersToHMSDMS(mystring)
        if (prependEquinox):
            mystring = "J2000 " + mystring
    elif (hmdm):
        # FIXME: fix unresolved reference and/or clean-up/remove function if not needed by Pipeline.
        mystring = convertColonDelimitersToHMSDMS(mystring, s=False)
        if (prependEquinox):
            mystring = "J2000 " + mystring
    if (delimiter != ', '):
        mystring = mystring.replace(', ', delimiter)
    return(mystring)


def imheadlist(vis, omitBeam=False):
    """
    Emulates imhead(mode='list') but leaves off the min/max/minpos/maxpos
    keywords, the filling of which makes it take so long to run on large cubes.
    -Todd Hunter
    """
    if (not os.path.exists(vis)):
        print("Could not find image.")
        return
    header = {}
    keys = ['bunit', 'date-obs', 'equinox', 'imtype', 'masks',
            'object', 'observer', 'projection', 'reffreqtype',
            'restfreq', 'shape', 'telescope']
    if not omitBeam:
        singleBeam = imhead(vis=vis, mode='get', hdkey='beammajor')
        if (singleBeam == False):
            header = imhead(vis, mode='list')
            if (header is None):
                print("No beam found.  Re-run with omitBeam=True.")
                return -1
            if 'perplanebeams' not in header:
                print("No beam found.  Re-run with omitBeam=True.")
                return -1
            beammajor = []
            beamminor = []
            beampa = []
            for beamchan in range(header['perplanebeams']['nChannels']):
                beamdict = header['perplanebeams']['*'+str(beamchan)]
                beammajor.append(beamdict['major']['value'])
                beamminor.append(beamdict['minor']['value'])
                beampa.append(beamdict['positionangle']['value'])
            bmaj = np.median(beammajor)
            bmin = np.median(beamminor)
            sinbpa = np.sin(np.radians(np.array(beampa)))
            cosbpa = np.cos(np.radians(np.array(beampa)))
            bpa = degrees(np.median(np.arctan2(np.median(sinbpa), np.median(cosbpa))))
            header['beammajor'] = bmaj
            header['beamminor'] = bmin
            header['beampa'] = bpa
        else:
            keys += ['beammajor', 'beamminor', 'beampa']
    for key in keys:
        try:
            header[key] = imhead(vis, mode='get', hdkey=key)
        except:
            pass
    for axis in range(len(header['shape'])):
        for key in ['cdelt', 'crval']:
            mykey = key+str(axis+1)
            try:
                result = imhead(vis, mode='get', hdkey=mykey)
                if (isinstance(result, dict)):
                    header[mykey] = result['value']
                else:
                    # crval3 (pol axis) will be an array (e.g. ['I']) not a dict
                    header[mykey] = result
            except:
                print("Failed to set header key: ", mykey)
                pass
        for key in ['crpix', 'ctype', 'cunit']:
            mykey = key+str(axis+1)
            try:
                header[mykey] = imhead(vis, mode='get', hdkey=mykey)
            except:
                pass
    return(header)


def CalcAtmTransmissionForImage(img, chanInfo='', airmass=1.5, pwv=-1,
                                spectralaxis=-1, value='transmission', P=-1, H=-1,
                                T=-1, altitude=-1, msname='', spw='', fieldid=-1):
    """
    This function is called by atmosphereVariation.
    Supported telescopes are VLA and ALMA (needed for default weather and PWV)
    img: name of CASA image
    value: 'transmission' or 'tsky'
    chanInfo: a list containing nchan, firstFreqHz, lastFreqHz, channelWidthHz
    pwv: in mm
    P: in mbar
    H: in percent
    T: in Kelvin
    msname: MS name
    spw: spectral window ID (string)
    fieldid: field ID (integer)
    Returns:
    2 arrays: frequencies (in GHz) and values (Kelvin, or transmission: 0..1)
    """

    with casa_tools.ImageReader(img) as image:
        lcs = image.coordsys()
        telescopeName = lcs.telescope()
        lcs.done()

    if chanInfo == '':
        chanInfo = numberOfChannelsInCube(img, returnChannelWidth=True, returnFreqs=True)

    freqs = np.linspace(chanInfo[1]*1e-9, chanInfo[2]*1e-9, chanInfo[0])
    numchan = len(freqs)
    # Make sure to not call conversion twice
    if chanInfo[-1] != 'TOPO':
        result = cubeFrameToTopo(img, chanInfo[1:3], msname=msname, spw=spw, fieldid=fieldid)
    else:
        result = None
    if (result is None):
        topofreqs = freqs
    else:
        topoWidth = (result[1]-result[0])/(numchan-1)
        topofreqs = np.linspace(result[0], result[1], chanInfo[0]) * 1e-9
    P0 = 1000.0 # mbar
    H0 = 20.0   # percent
    T0 = 273.0  # Kelvin
    if (telescopeName.find('ALMA') >= 0 or telescopeName.find('ACA') >= 0):
        pwv0 = 1.0
        P0 = 563.0
        H0 = 20.0
        T0 = 273.0
        altitude0 = 5059
    elif (telescopeName.find('VLA') >= 0):
        P0 = 786.0
        pwv0 = 5.0
        altitude0 = 2124
    else:
        pwv0 = 10.0
        altitude0 = 0
    if (pwv < 0):
        pwv = pwv0
    if (T < 0):
        T = T0
    if (H < 0):
        H = H0
    if (P < 0):
        P = P0
    if (altitude < 0):
        altitude = altitude0
    tropical = 1
    midLatitudeSummer = 2
    midLatitudeWinter = 3
    reffreq = 0.5*(topofreqs[numchan//2-1]+topofreqs[numchan//2])
#    reffreq = np.mean(topofreqs)
    numchanModel = numchan*1
    chansepModel = (topofreqs[-1]-topofreqs[0])/(numchanModel-1)
    nbands = 1
    lqa = casa_tools.quanta
    fCenter = lqa.quantity(reffreq, 'GHz')
    fResolution = lqa.quantity(chansepModel, 'GHz')
    fWidth = lqa.quantity(numchanModel*chansepModel, 'GHz')
    myat = casa_tools.atmosphere
    myat.initAtmProfile(humidity=H, temperature=lqa.quantity(T, "K"),
                        altitude=lqa.quantity(altitude, "m"),
                        pressure=lqa.quantity(P, 'mbar'), atmType=midLatitudeWinter)
    myat.initSpectralWindow(nbands, fCenter, fWidth, fResolution)
    myat.setUserWH2O(lqa.quantity(pwv, 'mm'))
#    myat.setAirMass()  # This does not affect the opacity, but it does effect TebbSky, so do it manually.
    lqa.done()

    dry = np.array(myat.getDryOpacitySpec(0)[1])
    wet = np.array(myat.getWetOpacitySpec(0)[1]['value'])
    TebbSky = myat.getTebbSkySpec(spwid=0)[1]['value']
    # readback the values to be sure they got set

    if (myat.getRefFreq()['unit'] != 'GHz'):
        LOG.warning("There is a unit mismatch for refFreq in the atm code.")
    if (myat.getChanSep()['unit'] != 'MHz'):
        LOG.warning("There is a unit mismatch for chanSep in the atm code.")
    numchanModel = myat.getNumChan()
    freq0 = myat.getChanFreq(0)['value']
    freq1 = myat.getChanFreq(numchanModel-1)['value']
    # We keep the original LSRK freqs for overlay on the LSRK spectrum, but associate
    # the transmission values from the equivalent TOPO freqs
    newfreqs = np.linspace(freqs[0], freqs[-1], numchanModel)  # fix for SCOPS-4815
    transmission = np.exp(-airmass*(wet+dry))
    TebbSky *= (1-np.exp(-airmass*(wet+dry)))/(1-np.exp(-wet-dry))
    if value=='transmission':
        values = transmission
    else:
        values = TebbSky
    myat.done()
    del myat
    return(newfreqs, values)


def plot_spectra(image_robust_rms_and_spectra, rec_info, plotfile, msname, spw, fieldid):
    """
    Takes a pipeline-produced cube and plots the spectrum within the clean
    mask (pixels with value=1 in the mask), and the noise spectrum from outside
    the mask and within the 0.2-0.3 level (auto adjusted upward if image size
    has been mitigated).
    image_robust_rms_and_spectra: dictionary of spectra and metadata
    rec_info: dictionary of receiver information (type (DSB/TSB), LO1 frequency)
    msname: name of representative MS
    spw: spectral window (string)
    fieldid: field ID (integer)
    """

    qaTool = casa_tools.quanta

    cube = os.path.basename(image_robust_rms_and_spectra['nonpbcor_imagename'])
    # Get spectral frame
    with casa_tools.ImageReader(cube) as image:
        lcs = image.coordsys()
        frame = lcs.referencecode('spectral')[0]
        lcs.done()

    unmaskedPixels = image_robust_rms_and_spectra['nonpbcor_image_cleanmask_npoints']
    if unmaskedPixels is None:
        unmaskedPixels = 0

    plt.clf()
    desc = plt.subplot(111)
    units = 'mJy'
    fontsize = 9

    # x axes
    nchan = len(image_robust_rms_and_spectra['nonpbcor_image_cleanmask_spectrum'])
    channels = np.arange(1, nchan + 1)
    freq_ch1 = qaTool.getvalue(qaTool.convert(image_robust_rms_and_spectra['nonpbcor_image_non_cleanmask_freq_ch1'], 'Hz'))
    freq_chN = qaTool.getvalue(qaTool.convert(image_robust_rms_and_spectra['nonpbcor_image_non_cleanmask_freq_chN'], 'Hz'))
    freqs = np.linspace(freq_ch1, freq_chN, nchan)

    # Flux density spectrum
    intensity = image_robust_rms_and_spectra['nonpbcor_image_cleanmask_spectrum'] * 1000.
    if unmaskedPixels > 0:
        mycolor = 'r'
        message = 'Red spectrum from %d pixels in flattened clean mask' % (unmaskedPixels)
        plt.text(0.025, 0.96, message, transform=desc.transAxes, ha='left', fontsize=fontsize + 1, color='r')
    else:
        mycolor = 'b'
        pblimit = image_robust_rms_and_spectra['nonpbcor_image_cleanmask_spectrum_pblimit']
        plt.text(0.025, 0.96,
                'Blue spectrum is mean from pixels above the pb=%.2f level (no clean mask was found)' % pblimit,
                 transform=desc.transAxes, ha='left', fontsize=fontsize+1, color='b')

    # Noise spectrum
    noise = image_robust_rms_and_spectra['nonpbcor_image_non_cleanmask_robust_rms'] * 1000.
    plt.text(0.025, 0.93, 'Black spectrum is per-channel scaled MAD from imstat annulus and outside clean mask',
             transform=desc.transAxes, ha='left', fontsize=fontsize+1)
    # Turn off rightside y-axis ticks to make way for second y-axis
    desc.yaxis.set_ticks_position('left')
    plt.plot(channels, intensity, '-', color=mycolor)

    plt.xlabel('%d Channels' % len(channels))
    plt.ylabel('Flux density (%s)' % units, color=mycolor)
    plt.tick_params(axis='y', labelcolor=mycolor, color=mycolor)
    # Give a buffer between final data point and y-axes in order
    # to be able to see high edge channel values
    plt.xlim([channels[0] - len(channels) // 100, channels[-1] + len(channels) // 100])
    freqDelta = (freqs[1]-freqs[0])*(len(channels)//100) # in Hz
    freqLimits = np.array([freqs[0]-freqDelta, freqs[-1]+freqDelta])  # Hz

    # Ignore edge channels (otherwise the first channel in ephemeris
    # cubes may go way off scale
    ylimits = np.array([np.min(intensity[1:-1]), np.max(intensity[1:-1])])
    ylimits[1] += ylimits[1]-ylimits[0]
    # Be sure the mean intensity spectrum is separated from lower y-axis by
    # reducing the y lower limit by 5 percent.
    yrange = ylimits[1]-ylimits[0]
    ylimits[0] -= 0.05*yrange
    ylimits[1] += 0.05*yrange
    noiseLimits = [2*np.min(noise[1:-1])-np.max(noise[1:-1]),
                   np.max(noise[1:-1])]
    yrange = noiseLimits[1]-noiseLimits[0]
    noiseLimits[0] -= 0.1*yrange
    noiseLimits[1] += 0.1*yrange
    plt.ylim(ylimits)

    # Plot horizontal dotted line at zero intensity
    plt.plot(plt.xlim(), [0, 0], 'k:')
    plt.text(0.5, 1.085, cube, transform=desc.transAxes, fontsize=fontsize, ha='center')
    addFrequencyAxisAbove(plt.gca(), freqs[0], freqs[-1], frame,
                          twinx=True, ylimits=noiseLimits,
                          xlimits=freqLimits)
    plt.plot(freqs * 1e-9, noise, 'k-')

    # Plot continuum frequency ranges
    fpattern = re.compile(r'([\d.]*)(~)([\d.]*)(\D*)')
    cont_freq_ranges = fpattern.findall(image_robust_rms_and_spectra['cont_freq_ranges'].replace(';', ''))
    for cont_freq_range in cont_freq_ranges:
        fLowGHz = qaTool.getvalue(qaTool.convert(qaTool.quantity(float(cont_freq_range[0]), cont_freq_range[3]), 'GHz'))
        fHighGHz = qaTool.getvalue(qaTool.convert(qaTool.quantity(float(cont_freq_range[2]), cont_freq_range[3]), 'GHz'))
        fcLevel = plt.ylim()[0] + yrange * 0.025
        plt.plot([fLowGHz, fHighGHz], [fcLevel] * 2, 'c-', lw=2)

    # Overlay atmosphere transmission
    freq, transmission = CalcAtmTransmissionForImage(cube, msname=msname, spw=spw, fieldid=fieldid)
    rescaledY, edgeYvalue, zeroValue, zeroYValue, otherEdgeYvalue, edgeT, otherEdgeT, edgeValueAmplitude, otherEdgeValueAmplitude, zeroValueAmplitude = RescaleTrans(transmission, plt.ylim())
    plt.plot(freq, rescaledY, 'm-')

    if rec_info['type'] == 'DSB':
        LO1 = float(qaTool.getvalue(qaTool.convert(rec_info['LO1'], 'GHz')))
        # Calculate image frequencies using TOPO frequencies from signal sideband.
        imageFreq0 = (2.0 * LO1 - freq[0]) * 1e9
        imageFreq1 = (2.0 * LO1 - freq[-1]) * 1e9
        chanInfo = [len(freq), imageFreq0, imageFreq1, float(-1e9 * (freq[1]-freq[0])), 'TOPO']
        imageFreq, imageTransmission = CalcAtmTransmissionForImage(cube, chanInfo, msname=msname, spw=spw, fieldid=fieldid)
        results = RescaleTrans(imageTransmission, plt.ylim())
        rescaledImage = results[0]
        # You need to keep the signal sideband frequency range so that the overlay works!
        plt.plot(freq, rescaledImage, 'm--')

    plt.draw()
    fig = plt.gcf()
    fig.set_size_inches(8,6)
    fig.canvas.flush_events()
    plt.savefig(plotfile, bbox_inches='tight')
    plt.clf()
    plt.close()
