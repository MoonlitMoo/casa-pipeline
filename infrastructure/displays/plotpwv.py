import math
import os

import matplotlib
import matplotlib.pyplot as plt
import numpy as np
from scipy.interpolate import splev, splrep

import pipeline.infrastructure as infrastructure
from pipeline.infrastructure import casa_tools
from pipeline.infrastructure.utils.conversion import mjd_seconds_to_datetime

from .plotstyle import RescaleXAxisTimeTicks

LOG = infrastructure.get_logger(__name__)


# A useful sequence of 18 unique matplotlib colors to cycle through
overlayColors = [
      [0.00,  0.00,  1.00],
      [0.00,  0.50,  0.00],
      [1.00,  0.00,  0.00],
      [0.00,  0.75,  0.75],
      [0.75,  0.00,  0.75],
      [0.25,  0.25,  0.25],
      [0.75,  0.25,  0.25],
      [0.25,  0.25,  0.75],
      [1.00,  0.75,  0.75],  # [0.75, 0.75, 0.75] is invisible on gray border
      [0.00,  1.00,  0.00],
      [0.76,  0.57,  0.17],
      [0.54,  0.63,  0.22],
      [0.34,  0.57,  0.92],
      [1.00,  0.10,  0.60],
      [0.70,  1.00,  0.70],  # [0.88,  0.75,  0.73], hard to see on gray
      [0.10,  0.49,  0.47],
      [0.66,  0.34,  0.65],
      [0.99,  0.41,  0.23]]


def plotPWV(ms, figfile='', plotrange=[0, 0, 0, 0], clip=True):
    """
    Read and plot the PWV values from the ms via the ASDM_CALWVR table.
    If that table is not found, read them from the ASDM_CALATMOSPHERE table.
    Different antennas are shown in different colored points.

    Arguments:
           ms: The measurement set
      figfile: True, False, or a string
    plotrange: The ranges for the X and Y axes (default=[0,0,0,0] which is autorange)
         clip: True = do not plot outliers beyond 5 * MAD from the median.

    If figfile is not a string, the file created will be <ms>.pwv.png.
    """
    if not os.path.exists(ms):
        LOG.warning("Could not find  ms: %s" % ms)
        return

    if not os.path.exists(ms+'/ASDM_CALWVR') and not os.path.exists(ms+'/ASDM_CALATMOSPHERE'):
        # Confirm that it is ALMA data
        observatory = getObservatoryName(ms)
        if observatory.find('ALMA') < 0 and observatory.find('ACA') < 0:
            LOG.warning("This is not ALMA data.  No PWV plot made.")
        else:
            LOG.warning("Could not find either %s/ASDM_CALWVR or ASDM_CALATMOSPHERE" % ms)
        return

    try:
        [watertime, water, antennaName] = readPWVFromMS(ms)
    except:
        observatory = getObservatoryName(ms)
        if observatory.find('ALMA') < 0 and observatory.find('ACA') < 0:
            LOG.info("This is not ALMA data.  No ASDM_CALWVR or ASDM_CALATMOSPHERE")
        else:
            LOG.warning("Could not open %s/ASDM_CALWVR nor ASDM_CALATMOSPHERE" % ms)
        return

    # Initialize plotting
    plt.clf()
    adesc = plt.subplot(111)
    ms = ms.split('/')[-1]

    # Clip the PWV values
    water = np.array(water) * 1000
    if clip:
        mad = MAD(water)
        median = np.median(water)
        if mad <= 0:
            matches = list(range(len(water)))
        else:
            matches = np.where(abs(water - median) < 5 * mad)[0]
            nonmatches = np.where(abs(water - median) >= 5 * mad)[0]
            if len(nonmatches) > 0:
                mymedian = np.median(water[nonmatches])
        water = water[matches]
        watertime = watertime[matches]
        antennaName = antennaName[matches]

    unique_antennas = np.unique(antennaName)
    list_of_date_times = mjd_seconds_to_datetime(watertime)
    timeplot = matplotlib.dates.date2num(list_of_date_times)
    for a in range(len(unique_antennas)):
        matches = np.where(unique_antennas[a] == np.array(antennaName))[0]
        plt.plot_date(timeplot[matches], water[matches], '.', color=overlayColors[a % len(overlayColors)])

    # Now sort to average duplicate timestamps to one value, then fit spline
    indices = np.argsort(watertime)
    watertime = watertime[indices]
    water = water[indices]
    newwater = []
    newtime = []
    for w in range(len(water)):
        if watertime[w] not in newtime:
            matches = np.where(watertime[w] == watertime)[0]
            newwater.append(np.median(water[matches]))
            newtime.append(watertime[w])
    watertime = newtime
    water = newwater
    regular_time = np.linspace(watertime[0], watertime[-1], len(watertime))
    order = 3
    if len(water) <= 3:
        order = 1
    if len(water) > 1:
        ius = splrep(watertime, water, s=len(watertime)-math.sqrt(2*len(watertime)), k=order)
        water = splev(regular_time, ius, der=0)
    list_of_date_times = mjd_seconds_to_datetime(regular_time)
    timeplot = matplotlib.dates.date2num(list_of_date_times)
    plt.plot_date(timeplot, water, 'k-')

    # Plot limits and ranges
    if plotrange[0] != 0 or plotrange[1] != 0:
        plt.xlim([plotrange[0], plotrange[1]])
    if plotrange[2] != 0 or plotrange[3] != 0:
        plt.ylim([plotrange[2], plotrange[3]])
    xlim = plt.xlim()
    ylim = plt.ylim()
    xrange = xlim[1]-xlim[0]
    yrange = ylim[1]-ylim[0]

    for a in range(len(unique_antennas)):
        plt.text(xlim[1]+0.01*xrange+0.055*xrange*(a // 48), ylim[1]-0.024*yrange*(a % 48 - 2),
                 unique_antennas[a], color=overlayColors[a % len(overlayColors)], size=8)
    date_string = mjd_seconds_to_datetime(watertime[0:])[0].strftime('%Y-%m-%d')
    plt.xlabel('Universal Time (%s)' % date_string)
    plt.ylabel('PWV (mm)')
    adesc.xaxis.grid(True, which='major')
    adesc.yaxis.grid(True, which='major')

    plt.title(ms)
    if len(water) > 1:
        adesc.xaxis.set_major_locator(matplotlib.dates.MinuteLocator(byminute=list(range(0, 60, 30))))
        adesc.xaxis.set_minor_locator(matplotlib.dates.MinuteLocator(byminute=list(range(0, 60, 10))))
        adesc.xaxis.set_major_formatter(matplotlib.dates.DateFormatter('%H:%M'))
        adesc.fmt_xdata = matplotlib.dates.DateFormatter('%H:%M')
        RescaleXAxisTimeTicks(plt.xlim(), adesc)
    auto_figure_name = "%s.pwv.png" % ms
    plt.draw()

    # Save plot
    if figfile==True:
        plt.savefig(auto_figure_name)
    elif len(figfile) > 0:
        plt.savefig(figfile)
    else:
        LOG.warning("Failed to create PWV plot")
    plt.clf()
    plt.close()


def readPWVFromMS(vis):
    """
    Reads all the PWV values from a measurement set, returning a list
    of lists:   [[mjdsec], [pwv], [antennaName]]
    """
    if os.path.exists("%s/ASDM_CALWVR" % vis):
        with casa_tools.TableReader(vis+"/ASDM_CALWVR") as table:
            time = table.getcol('startValidTime')  # mjdsec
            antenna = table.getcol('antennaName')
            pwv = table.getcol('water')

        if len(pwv) < 1:
            LOG.info("The ASDM_CALWVR table is empty, switching to ASDM_CALATMOSPHERE")
            time, antenna, pwv = readPWVFromASDM_CALATMOSPHERE(vis)
    elif os.path.exists("%s/ASDM_CALATMOSPHERE" % vis):
        time, antenna, pwv = readPWVFromASDM_CALATMOSPHERE(vis)
    else:
        LOG.warning("Did not find ASDM_CALWVR nor ASDM_CALATMOSPHERE")
        return[[0], [1], [0]]

    return [time, pwv, antenna]


def readPWVFromASDM_CALATMOSPHERE(vis):
    """
    Reads the PWV via the water column of the ASDM_CALATMOSPHERE table.
    """
    if not os.path.exists(vis+'/ASDM_CALATMOSPHERE'):
        if vis.find('.ms') < 0:
            vis += '.ms'
            if not os.path.exists(vis):
                LOG.warning("Could not find measurement set")
                return
            elif not os.path.exists(vis+'/ASDM_CALATMOSPHERE'):
                LOG.warning("Could not find ASDM_CALATMOSPHERE in the measurement set")
                return
        else:
            LOG.warning("Could not find measurement set")
            return

    with casa_tools.TableReader(vis + "/ASDM_CALATMOSPHERE") as table:
        pwvtime = table.getcol('startValidTime')  # mjdsec
        antenna = table.getcol('antennaName')
        pwv = table.getcol('water')[0]  # There seem to be 2 identical entries per row, so take first one.

    return pwvtime, antenna, pwv


# The following routines are general purpose and may eventually
# be useful elsewhere.

def getObservatoryName(ms):
    """
    Returns the observatory name in the specified ms.
    """
    obsTable = ms + '/OBSERVATION'
    try:
        with casa_tools.TableReader(obsTable) as table:
            myName = table.getcell('TELESCOPE_NAME')
    except:
        LOG.warning("Could not open OBSERVATION table to get the telescope name: %s" % obsTable)
        myName = ''
    return myName


def MAD(a, c=0.6745, axis=0):
    """
    Median Absolute Deviation along given axis of an array:

    median(abs(a - median(a))) / c

    c = 0.6745 is the constant to convert from MAD to std; it is used by
    default

    """
    a = np.array(a)
    good = (a == a)
    a = np.asarray(a, np.float64)
    if a.ndim == 1:
        d = np.median(a[good])
        m = np.median(np.fabs(a[good] - d) / c)
    else:
        d = np.median(a[good], axis=axis)
        # I don't want the array to change so I have to copy it?
        if axis > 0:
            aswp = np.swapaxes(a[good], 0, axis)
        else:
            aswp = a[good]
        m = np.median(np.fabs(aswp - d) / c, axis=0)

    return m
