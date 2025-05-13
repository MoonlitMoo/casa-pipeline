import matplotlib
import matplotlib.pyplot as plt
import numpy as np

import pipeline.infrastructure as infrastructure
from pipeline.infrastructure import casa_tools
from pipeline.infrastructure.utils.conversion import mjd_seconds_to_datetime

from .plotstyle import RescaleXAxisTimeTicks

LOG = infrastructure.get_logger(__name__)


def plot_suntrack(vis='', figfile='', elvstime=True):
    """
    Plots the solar az/el or time/el during a dataset, with one point per scan.
    Adopted from au.plotSunDuringTrack()
    """

    azimuth = []
    elevation = []
    pointingScans = []
    startmjd = []
    endmjd = []
    mjd = []

    vis = vis.split('/')[-1]

    with casa_tools.MSMDReader(vis) as msmd:
        scannumbers = msmd.scannumbers()
        t = []
        for snumber in scannumbers:
            mytimes = msmd.timesforscan(snumber)
            t.append([np.min(mytimes), np.max(mytimes)])
        firstScan = np.min(scannumbers)
        lastScan = np.max(scannumbers)
        if ('CALIBRATE_POINTING#ON_SOURCE' in msmd.intents()):
            pointingScans = msmd.scansforintent('CALIBRATE_POINTING#ON_SOURCE')
        else:
            pointingScans = []

    obsTable = vis + '/OBSERVATION'
    try:
        with casa_tools.TableReader(obsTable) as table:
            observatory = table.getcell('TELESCOPE_NAME')
    except:
        LOG.warning("Could not open OBSERVATION table to get the telescope name: %s" % obsTable)
        return

    for scantime in t:
        az, el = get_az_el_from_body('sun', observatory=observatory, mjdsec=np.min(scantime))
        azimuth.append(az)
        elevation.append(el)
        mjd.append(np.min(scantime))
        startmjd.append(np.min(scantime))
        az, el = get_az_el_from_body('sun', observatory=observatory, mjdsec=np.max(scantime))
        azimuth.append(az)
        elevation.append(el)
        endmjd.append(np.max(scantime))
        mjd.append(np.max(scantime))

    plt.clf()
    adesc = plt.subplot(111)
    twilight = False
    if (elevation[0]*elevation[-1] < 0):
        twilight = True
    if (twilight):
        color = 'r'
    elif (elevation[0] < 0):  # nighttime
        color = 'k'
    else:  # daytime
        color = 'b'
    azimuthStart = np.array(azimuth)[range(0, len(azimuth), 2)]
    elevationStart = np.array(elevation)[range(0, len(elevation), 2)]
    azimuthEnd = np.array(azimuth)[range(1, len(azimuth), 2)]
    elevationEnd = np.array(elevation)[range(1, len(elevation), 2)]
    if (elvstime):
        list_of_date_times = mjd_seconds_to_datetime(mjd)
        timeplot = matplotlib.dates.date2num(list_of_date_times)
        plt.plot_date(timeplot, elevation, '%s-' % color)
        timeplot = matplotlib.dates.date2num(mjd_seconds_to_datetime(startmjd))
        plt.plot_date(timeplot, elevationStart, '%so' % color)
        timeplot = matplotlib.dates.date2num(mjd_seconds_to_datetime(endmjd))
        plt.plot_date(timeplot, elevationEnd, 'wo', markeredgecolor=color)
        timeplot = matplotlib.dates.date2num(list_of_date_times)
        plt.plot_date(timeplot, elevation, '%s.-' % color)
        adesc.xaxis.set_major_locator(matplotlib.dates.MinuteLocator(byminute=range(0, 60, 30)))
        adesc.xaxis.set_minor_locator(matplotlib.dates.MinuteLocator(byminute=range(0, 60, 10)))
        adesc.xaxis.set_major_formatter(matplotlib.dates.DateFormatter('%H:%M'))
        adesc.fmt_xdata = matplotlib.dates.DateFormatter('%H:%M')
        xlims = plt.xlim()
        myxrange = xlims[1]-xlims[0]
        plt.xlim([xlims[0]-0.06*myxrange, xlims[1]+0.06*myxrange])
        RescaleXAxisTimeTicks(plt.xlim(), adesc)
        adesc.xaxis.grid(True, which='major')
    else:
        plt.plot(azimuth, elevation, '%s-' % color)
        plt.plot(azimuthStart, elevationStart, '%so' % color)
        plt.plot(azimuthEnd, elevationEnd, 'wo', markeredgecolor=color)
        plt.plot(azimuth, elevation, '%s.-' % color)

    xlims = plt.xlim()
    plt.ylim([-92, 92])
    ylims = plt.ylim()
    azoff = (xlims[1]-xlims[0])*0.05
    for p in pointingScans:
        if (elvstime == False):
            plt.text(azimuth[p-1]-azoff*0.5, elevation[p-1]-8, 'Point')
        else:
            plt.text(timeplot[p-1]-azoff*0.5, elevation[p-1]-8, 'Point')
    if (elvstime == False):
        plt.text(azimuth[0]-azoff, elevation[0]+3, 'Scan %d' % (firstScan))
        plt.text(azimuth[-1]-azoff, elevation[-1]+3, 'Scan %d' % (lastScan))
    else:
        plt.text(timeplot[0]-azoff, elevation[0]+3, 'Scan %d' % (firstScan))
        plt.text(timeplot[-1]-azoff, elevation[-1]+3, 'Scan %d' % (lastScan))
    plt.axhline(0, ls='--', color='k')
    plt.ylabel('Elevation (deg)')
    if (elvstime):
        plt.xlabel('Time (UT on %s)' % (mjd_seconds_to_datetime([mjd[0]])[0].strftime('%Y-%m-%d')))
    else:
        plt.xlabel('Azimuth (deg)')
    plt.yticks(range(-90, 92, 15))
    adesc.xaxis.grid(True, which='major')
    adesc.yaxis.grid(True, which='major')
    plt.title('{}'.format(vis))

    if len(figfile) < 1:
        if (elvstime):
            figfile = vis + '.sun.el_vs_time.png'
        else:
            figfile = vis + '.sun.el_vs_az.png'
    else:
        suntrack_file = figfile
    plt.savefig(figfile)
    plt.draw()

    LOG.debug("Mean elevation = %f deg" % (np.mean(elevation)))


def get_az_el_from_body(body, observatory='ALMA', mjdsec=0.0):

    me = casa_tools.measures
    qa = casa_tools.quanta

    me.doframe(me.epoch('mjd', qa.quantity(mjdsec, 's')))
    me.doframe(me.observatory(observatory))
    azel = me.measure(me.direction(body), 'AZELGEO')
    az = qa.convert(azel['m0'], 'deg')['value']
    el = qa.convert(azel['m1'], 'deg')['value']

    return (az, el)
