import math

import matplotlib
import matplotlib.pyplot as plt
import numpy as np

import pipeline.infrastructure as infrastructure
from pipeline.infrastructure import casa_tools
from pipeline.infrastructure.utils.conversion import mjd_seconds_to_datetime

from .plotstyle import RescaleXAxisTimeTicks

LOG = infrastructure.get_logger(__name__)


def plot_weather(vis='', figfile='', station=[], help=False):
    """
    Compiles and plots the major weather parameters for the specified ms.
    Station can be a single integer or integer string, or a list of two integers.
    The default empty list means to plot all data from up to 2 of the stations
    present in the data.  The default plot file name will be 'vis'.weather.png.
    """
    if help:
        LOG.info("plot_weather(vis='', figfile='', station=[])")
        LOG.info("  Plots pressure, temperature, relative humidity, wind speed and direction.")
        LOG.info("Station can be a single integer or integer string, or a list of two integers.")
        LOG.info("The default empty list means to plot the data form up to 2 of the stations")
        LOG.info("present in the data.  The default plot file name will be 'vis'.weather.png.")
        return

    myfontsize = 8

    try:
        # Fetch data from weather table in MS.
        with casa_tools.TableReader(vis+"/WEATHER") as table:
            available_cols = table.colnames()
            mjdsec = table.getcol('TIME')
            pressure = table.getcol('PRESSURE')
            relative_humidity = table.getcol('REL_HUMIDITY')
            temperature = table.getcol('TEMPERATURE')
            # Nobeyama does not have DEW_POINT and NS_WX_STATION_ID
            dew_point = table.getcol('DEW_POINT') if 'DEW_POINT' in available_cols else None
            wind_direction = (180 / math.pi) * table.getcol('WIND_DIRECTION')
            wind_speed = table.getcol('WIND_SPEED')
            stations = table.getcol('NS_WX_STATION_ID') if 'NS_WX_STATION_ID' in available_cols else []
    except:
        LOG.info("Could not open WEATHER table.  Did you importasdm with asis='*'?")
        return

    mjdsec1 = mjdsec
    vis = vis.split('/')[-1]
    unique_stations = list(np.unique(stations))

    try:
        with casa_tools.TableReader(vis + '/ASDM_STATION') as table:
            station_names = table.getcol('name')
    except:
        LOG.info("Could not open ASDM_STATION table. The Station IDs (instead of Names) will be used.")
        station_names = None

    unique_station_names = []
    for station_id in unique_stations:
        station_name = str(station_id)
        if station_names is not None:
            if any([wx_prefix.lower() in station_names[station_id].lower() for wx_prefix in ['WSTB', 'Meteo', 'OSF']]):
                station_name = station_names[station_id]
        unique_station_names.append(station_name)

    # PIPE-31: deprioritize the station with "Itinerant" in name (typically: "MeteoItinerant"), 
    # since it only has a wind sensor.
    try:
        meteoitinerant_idx = unique_station_names.index('Itinerant')
        unique_station_names.append(unique_station_names.pop(meteoitinerant_idx))
        unique_stations.append(unique_stations.pop(meteoitinerant_idx))
    except ValueError:
        pass

    if station:
        if isinstance(station, int):
            if station not in unique_stations:
                LOG.info("Station %d is not in the data.  Present are: " % station, unique_stations)
                return
            unique_stations = [station]
        elif isinstance(station, list):
            if len(station) > 2:
                LOG.info("Only 2 stations can be overlaid.")
                return
            if station[0] not in unique_stations:
                LOG.info("Station %d is not in the data.  Present are: " % station[0], unique_stations)
                return
            if station[1] not in unique_stations:
                LOG.info("Station %d is not in the data.  Present are: " % station[1], unique_stations)
                return
            unique_stations = station
        elif isinstance(station, str):
            if station.isdigit():
                if int(station) not in unique_stations:
                    LOG.info("Station %s is not in the data.  Present are: " % station, unique_stations)
                    return
                unique_stations = [int(station)]
            else:
                LOG.info("Invalid station ID, it must be an integer, or list of integers.")
                return

    if len(unique_stations) > 1:
        first_station_rows = np.where(stations == unique_stations[0])[0]
        second_station_rows = np.where(stations == unique_stations[1])[0]

        pressure2 = pressure[second_station_rows]
        relative_humidity2 = relative_humidity[second_station_rows]
        temperature2 = temperature[second_station_rows]
        dew_point2 = dew_point[second_station_rows] if dew_point is not None else None
        wind_direction2 = wind_direction[second_station_rows]
        wind_speed2 = wind_speed[second_station_rows]
        mjdsec2 = mjdsec[second_station_rows]

        pressure = pressure[first_station_rows]
        relative_humidity = relative_humidity[first_station_rows]
        temperature = temperature[first_station_rows]
        dew_point = dew_point[first_station_rows] if dew_point is not None else None
        wind_direction = wind_direction[first_station_rows]
        wind_speed = wind_speed[first_station_rows]
        mjdsec1 = mjdsec[first_station_rows]
        if np.mean(temperature2) > 100:
            # convert to Celsius
            temperature2 -= 273.15
        if dew_point2 is not None and np.mean(dew_point2) > 100:
            dew_point2 -= 273.15

    if np.mean(temperature) > 100:
        # convert to Celsius
        temperature -= 273.15
    if dew_point is not None and np.mean(dew_point) > 100:
        dew_point -= 273.15
    if dew_point is not None and np.mean(dew_point) == 0:
        # assume it is not measured and use NOAA formula to compute from humidity:
        dew_point = ComputeDewPointCFromRHAndTempC(relative_humidity, temperature)
    if np.mean(relative_humidity) < 0.001:
        if dew_point is None or np.count_nonzero(dew_point) == 0:
            # dew point is all zero so it was not measured, so cap the rH at small non-zero value
            relative_humidity = 0.001 * np.ones(len(relative_humidity))
        else:
            LOG.info("Replacing zeros in relative humidity with value computed from dew point and temperature.")
            dew_point_wvp = computeWVP(dew_point)
            ambient_wvp = computeWVP(temperature)
            LOG.info("dWVP=%f, aWVP=%f" % (dew_point_wvp[0], ambient_wvp[0]))
            relative_humidity = 100*(dew_point_wvp/ambient_wvp)

    if len(unique_stations) > 1:
        if np.mean(temperature2) > 100:
            # convert to Celsius
            temperature2 -= 273.15
        if dew_point2 is not None and np.mean(dew_point2) > 100:
            dew_point2 -= 273.15
        if dew_point2 is not None and np.mean(dew_point2) == 0:
            # assume it is not measured and use NOAA formula to compute from humidity:
            dew_point2 = ComputeDewPointCFromRHAndTempC(relative_humidity2, temperature2)
        if np.mean(relative_humidity2) < 0.001:
            if dew_point2 is None or np.count_nonzero(dew_point2) == 0:
                # dew point is all zero so it was not measured, so cap the rH at small non-zero value
                relative_humidity2 = 0.001 * np.ones(len(relative_humidity2))
            else:
                LOG.info("Replacing zeros in relative humidity with value computed from dew point and temperature.")
                dew_point_wvp2 = computeWVP(dew_point2)
                ambient_wvp2 = computeWVP(temperature2)
                LOG.info("dWVP2=%f, aWVP2=%f" % (dew_point_wvp2[0], ambient_wvp2[0]))
                relative_humidity2 = 100*(dew_point_wvp2/ambient_wvp2)


    # take timerange from OBSERVATION table if there is only one unique timestamp
    if len(np.unique(mjdsec)) == 1:
        with casa_tools.TableReader(vis+"/OBSERVATION") as table:
            timerange = table.getcol('TIME_RANGE')
        obs_timerange = [np.min(timerange), np.max(timerange)]
        manual_xlim = matplotlib.dates.date2num(mjd_seconds_to_datetime(obs_timerange))
        do_manual_xlim = True
    else:
        manual_xlim = None
        do_manual_xlim = False

    mysize = 'small'
    plt.clf()
    adesc = plt.subplot(321)
    myhspace = 0.25
    mywspace = 0.25
    markersize = 3
    plt.subplots_adjust(hspace=myhspace, wspace=mywspace)
    plt.title(vis)
    list_of_date_times = mjd_seconds_to_datetime(mjdsec1)
    timeplot = matplotlib.dates.date2num(list_of_date_times)
    plt.plot_date(timeplot, pressure, markersize=markersize)
    if len(unique_stations) > 1:
        list_of_date_times = mjd_seconds_to_datetime(mjdsec2)
        timeplot2 = matplotlib.dates.date2num(list_of_date_times)
        plt.plot_date(timeplot2, pressure2, markersize=markersize, color='r')

    if do_manual_xlim is True:
        plt.xlim(manual_xlim)

    resizeFonts(adesc, myfontsize)
    plt.ylabel('Pressure (mb)', size=mysize)
    adesc.xaxis.set_major_locator(matplotlib.dates.MinuteLocator(byminute=list(range(0, 60, 30))))
    adesc.xaxis.set_minor_locator(matplotlib.dates.MinuteLocator(byminute=list(range(0, 60, 10))))
    adesc.xaxis.set_major_formatter(matplotlib.dates.DateFormatter('%H:%M'))
    adesc.fmt_xdata = matplotlib.dates.DateFormatter('%H:%M')
    RescaleXAxisTimeTicks(plt.xlim(), adesc)
    adesc.xaxis.grid(True, which='major')
    adesc.yaxis.grid(True, which='major')

    adesc = plt.subplot(322)
    plt.plot_date(timeplot, temperature, markersize=markersize)
    if len(unique_stations) > 1:
        list_of_date_times = mjd_seconds_to_datetime(mjdsec2)
        timeplot2 = matplotlib.dates.date2num(list_of_date_times)
        plt.plot_date(timeplot2, temperature2, markersize=markersize, color='r')

    if do_manual_xlim is True:
        plt.xlim(manual_xlim)

    resizeFonts(adesc, myfontsize)
    plt.ylabel('Temperature (C)', size=mysize)
    adesc.xaxis.set_major_locator(matplotlib.dates.MinuteLocator(byminute=list(range(0, 60, 30))))
    adesc.xaxis.set_minor_locator(matplotlib.dates.MinuteLocator(byminute=list(range(0, 60, 10))))
    adesc.xaxis.set_major_formatter(matplotlib.dates.DateFormatter('%H:%M'))
    adesc.fmt_xdata = matplotlib.dates.DateFormatter('%H:%M')
    RescaleXAxisTimeTicks(plt.xlim(), adesc)
    adesc.xaxis.grid(True, which='major')
    adesc.yaxis.grid(True, which='major')
    
    colors = ['b', 'r']
    xinc = 0.4
    labxstart = 0.5-(min(len(unique_station_names), 2)-1.0)*xinc/2.0
    for idx, station_name in enumerate(unique_station_names):
        if idx > 1:
            continue
        y0 = 1.05
        plt.text(labxstart+idx*xinc, y0, station_name,
                 color=colors[idx], transform=adesc.transAxes, ha='center')

    adesc = plt.subplot(323)
    plt.plot_date(timeplot, relative_humidity, markersize=markersize)
    if len(unique_stations) > 1:
        list_of_date_times = mjd_seconds_to_datetime(mjdsec2)
        timeplot2 = matplotlib.dates.date2num(list_of_date_times)
        plt.plot_date(timeplot2, relative_humidity2, markersize=markersize, color='r')

    if do_manual_xlim is True:
        plt.xlim(manual_xlim)

    resizeFonts(adesc, myfontsize)
    plt.ylabel('Relative Humidity (%)', size=mysize)
    adesc.xaxis.set_major_locator(matplotlib.dates.MinuteLocator(byminute=list(range(0, 60, 30))))
    adesc.xaxis.set_minor_locator(matplotlib.dates.MinuteLocator(byminute=list(range(0, 60, 10))))
    adesc.xaxis.set_major_formatter(matplotlib.dates.DateFormatter('%H:%M'))
    adesc.fmt_xdata = matplotlib.dates.DateFormatter('%H:%M')
    RescaleXAxisTimeTicks(plt.xlim(), adesc)
    adesc.xaxis.grid(True, which='major')
    adesc.yaxis.grid(True, which='major')

    pid = 4
    if dew_point is not None:
        adesc = plt.subplot(3, 2, pid)
        plt.plot_date(timeplot, dew_point, markersize=markersize)
        if len(unique_stations) > 1:
            list_of_date_times = mjd_seconds_to_datetime(mjdsec2)
            timeplot2 = matplotlib.dates.date2num(list_of_date_times)
            plt.plot_date(timeplot2, dew_point2, markersize=markersize, color='r')

        if do_manual_xlim is True:
            plt.xlim(manual_xlim)

        resizeFonts(adesc, myfontsize)
        plt.ylabel('Dew point (C)', size=mysize)
        adesc.xaxis.set_major_locator(matplotlib.dates.MinuteLocator(byminute=list(range(0, 60, 30))))
        adesc.xaxis.set_minor_locator(matplotlib.dates.MinuteLocator(byminute=list(range(0, 60, 10))))
        adesc.xaxis.set_major_formatter(matplotlib.dates.DateFormatter('%H:%M'))
        adesc.fmt_xdata = matplotlib.dates.DateFormatter('%H:%M')
        RescaleXAxisTimeTicks(plt.xlim(), adesc)
        adesc.xaxis.grid(True, which='major')
        adesc.yaxis.grid(True, which='major')
        pid += 1

    adesc = plt.subplot(3, 2, pid)
    plt.plot_date(timeplot, wind_speed, markersize=markersize)
    if len(unique_stations) > 1:
        list_of_date_times = mjd_seconds_to_datetime(mjdsec2)
        timeplot2 = matplotlib.dates.date2num(list_of_date_times)
        plt.plot_date(timeplot2, wind_speed2, markersize=markersize, color='r')

    if do_manual_xlim is True:
        plt.xlim(manual_xlim)

    resizeFonts(adesc, myfontsize)
    # Get date of observation.
    date_string = mjd_seconds_to_datetime(mjdsec[0:])[0].strftime('%Y-%m-%d')
    xlabel = 'Universal Time (%s)' % date_string
    plt.xlabel(xlabel, size=mysize)
    plt.ylabel('Wind speed (m/s)', size=mysize)
    adesc.xaxis.set_major_locator(matplotlib.dates.MinuteLocator(byminute=list(range(0, 60, 30))))
    adesc.xaxis.set_minor_locator(matplotlib.dates.MinuteLocator(byminute=list(range(0, 60, 10))))
    adesc.xaxis.set_major_formatter(matplotlib.dates.DateFormatter('%H:%M'))
    adesc.fmt_xdata = matplotlib.dates.DateFormatter('%H:%M')
    RescaleXAxisTimeTicks(plt.xlim(), adesc)
    adesc.xaxis.grid(True, which='major')
    adesc.yaxis.grid(True, which='major')
    pid += 1

    adesc = plt.subplot(3, 2, pid)
    plt.xlabel(xlabel, size=mysize)
    plt.ylabel('Wind direction (deg)', size=mysize)
    plt.plot_date(timeplot, wind_direction, markersize=markersize)
    if len(unique_stations) > 1:
        list_of_date_times = mjd_seconds_to_datetime(mjdsec2)
        timeplot2 = matplotlib.dates.date2num(list_of_date_times)
        plt.plot_date(timeplot2, wind_direction2, markersize=markersize, color='r')

    if do_manual_xlim is True:
        plt.xlim(manual_xlim)

    resizeFonts(adesc, myfontsize)
    adesc.xaxis.set_major_locator(matplotlib.dates.MinuteLocator(byminute=list(range(0, 60, 30))))
    adesc.xaxis.set_minor_locator(matplotlib.dates.MinuteLocator(byminute=list(range(0, 60, 10))))
    adesc.xaxis.set_major_formatter(matplotlib.dates.DateFormatter('%H:%M'))
    adesc.fmt_xdata = matplotlib.dates.DateFormatter('%H:%M')
    RescaleXAxisTimeTicks(plt.xlim(), adesc)
    adesc.xaxis.grid(True, which='major')
    adesc.yaxis.grid(True, which='major')
    if len(figfile) < 1:
        weather_file = vis+'.weather.png'
    else:
        weather_file = figfile
    plt.savefig(weather_file)
    plt.draw()
    LOG.info("Wrote file = %s" % weather_file)


def ComputeDewPointCFromRHAndTempC(relativeHumidity, temperature):
    """
    inputs:  relativeHumidity in percentage, temperature in C
    output: in degrees C
    Uses formula from http://en.wikipedia.org/wiki/Dew_point#Calculating_the_dew_point
    """
    es = 6.112*np.exp(17.67*temperature/(temperature+243.5))
    e = relativeHumidity*0.01*es
    dewPoint = 243.5*np.log(e/6.112)/(17.67-np.log(e/6.112))
    return dewPoint


def computeWVP(d):
    """
    This simply converts the specified temperature (in Celsius) to water vapor
    pressure, which can be used to estimate the relative humidity from the
    measured dew point.
    """
    # d is in Celsius
    t = d+273.15
    w = np.exp(-6096.9385/t + 21.2409642 - 2.711193e-2*t + 1.673952e-5*t**2 + 2.433502*np.log(t))
    return w


def resizeFonts(adesc, fontsize):
    """
    Plotting utility routine
    """
    y_format = matplotlib.ticker.ScalarFormatter(useOffset=False)
    adesc.yaxis.set_major_formatter(y_format)
    adesc.xaxis.set_major_formatter(y_format)
    plt.setp(adesc.get_xticklabels(), fontsize=fontsize)
    plt.setp(adesc.get_yticklabels(), fontsize=fontsize)




