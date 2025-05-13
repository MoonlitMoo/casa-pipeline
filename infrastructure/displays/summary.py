import datetime
import math
import operator
import os
from typing import TYPE_CHECKING, Generator, List, Tuple

import matplotlib
import matplotlib.dates as dates
import matplotlib.figure as figure
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import numpy as np

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.renderer.logger as logger
import pipeline.infrastructure.utils as utils
import pipeline.infrastructure.vdp as vdp
from pipeline.domain.measures import FrequencyUnits, DistanceUnits
from pipeline.h.tasks.common import atmutil
from pipeline.infrastructure import casa_tasks
from pipeline.infrastructure import casa_tools
from pipeline.infrastructure.displays.plotstyle import casa5style_plot
from . import plotmosaic
from . import plotpwv
from . import plotweather
from . import plotsuntrack

if TYPE_CHECKING:
    from pipeline.infrastructure.launcher import Context

LOG = infrastructure.get_logger(__name__)
DISABLE_PLOTMS = False

ticker.TickHelper.MAXTICKS = 10000


class AzElChart(object):
    def __init__(self, context, ms):
        self.context = context
        self.ms = ms
        self.figfile = self._get_figfile()

        # Plot the first channel for all the science spws.
        self.spwlist = ''
        for spw in ms.get_spectral_windows(science_windows_only=True):
            if self.spwlist == '':
                self.spwlist += '%d:0~0' % spw.id
            else:
                self.spwlist += ',%d:0~0' % spw.id

    def plot(self):
        if DISABLE_PLOTMS:
            LOG.debug('Disabling AzEl plot due to problems with plotms')
            return None

        # inputs based on analysisUtils.plotElevationSummary
        task_args = {
            'vis': self.ms.name,
            'xaxis': 'azimuth',
            'yaxis': 'elevation',
            'title': 'Elevation vs Azimuth for %s' % self.ms.basename,
            'coloraxis': 'field',
            'avgchannel': '9000',
            'avgtime': '10',
            'antenna': '0&&*',
            'spw': self.spwlist,
            'plotfile': self.figfile,
            'clearplots': True,
            'showgui': False,
            'customflaggedsymbol': True,
            'flaggedsymbolshape': 'autoscaling'}

        task = casa_tasks.plotms(**task_args)

        if not os.path.exists(self.figfile):
            task.execute()

        return self._get_plot_object(task)

    def _get_figfile(self):
        session_part = self.ms.session
        ms_part = self.ms.basename

        return os.path.join(self.context.report_dir,
                            'session%s' % session_part,
                            ms_part, 'azel.png')

    def _get_plot_object(self, task):
        return logger.Plot(self.figfile,
                           x_axis='Azimuth',
                           y_axis='Elevation',
                           parameters={'vis': self.ms.basename},
                           command=str(task))


class SunTrackChart(object):
    def __init__(self, context, ms):
        self.context = context
        self.ms = ms
        self.figfile = self._get_figfile()

    def plot(self):
        if os.path.exists(self.figfile):
            LOG.debug('Returning existing SunTrack plot')
            return self._get_plot_object()

        LOG.debug('Creating new SunTrack plot')
        try:
            # Based on the analysisUtils method
            plotsuntrack.plot_suntrack(vis=self.ms.name, figfile=self.figfile, elvstime=True)
        except:
            return None
        finally:
            # plot suntrack does not close the plot! work around that here rather
            # than editing the code as we might lose the fix (again..)
            try:
                plt.close()
            except:
                pass

        return self._get_plot_object()

    def _get_figfile(self):
        session_part = self.ms.session
        ms_part = self.ms.basename
        return os.path.join(self.context.report_dir,
                            'session%s' % session_part,
                            ms_part, 'solar_el_vs_time.png')

    def _get_plot_object(self):
        return logger.Plot(self.figfile,
                           x_axis='Azimuth',
                           y_axis='Elevation',
                           parameters={'vis': self.ms.basename})


class WeatherChart(object):
    def __init__(self, context, ms):
        self.context = context
        self.ms = ms
        self.figfile = self._get_figfile()

    def plot(self):
        if os.path.exists(self.figfile):
            LOG.debug('Returning existing Weather plot')
            return self._get_plot_object()

        LOG.debug('Creating new Weather plot')
        try:
            # Based on the analysisUtils method
            plotweather.plot_weather(vis=self.ms.name, figfile=self.figfile)
        except:
            return None
        finally:
            # plot weather does not close the plot! work around that here rather
            # than editing the code as we might lose the fix (again..)
            try:
                plt.close()
            except:
                pass

        return self._get_plot_object()

    def _get_figfile(self):
        session_part = self.ms.session
        ms_part = self.ms.basename
        return os.path.join(self.context.report_dir,
                            'session%s' % session_part,
                            ms_part, 'weather.png')

    def _get_plot_object(self):
        return logger.Plot(self.figfile,
                           x_axis='Time',
                           y_axis='Weather',
                           parameters={'vis': self.ms.basename})


class ElVsTimeChart(object):
    def __init__(self, context, ms):
        self.context = context
        self.ms = ms
        self.figfile = self._get_figfile()

        # Plot the first channel for all the science spws.
        self.spwlist = ''
        for spw in ms.get_spectral_windows(science_windows_only=True):
            if self.spwlist == '':
                self.spwlist = self.spwlist + '%d:0~0' % spw.id
            else:
                self.spwlist = self.spwlist + ',%d:0~0' % spw.id

    def plot(self):
        if DISABLE_PLOTMS:
            LOG.debug('Disabling ElVsTime plot due to problems with plotms')
            return None

        # Inputs based on analysisUtils.plotElevationSummary
        task_args = {'vis': self.ms.name,
                     'xaxis': 'time',
                     'yaxis': 'elevation',
                     'title': 'Elevation vs Time for %s' % self.ms.basename,
                     'coloraxis': 'field',
                     'avgchannel': '9000',
                     'avgtime': '10',
                     'antenna': '0&&*',
                     'spw': self.spwlist,
                     'plotfile': self.figfile,
                     'clearplots': True,
                     'showgui': False,
                     'customflaggedsymbol': True,
                     'flaggedsymbolshape': 'autoscaling'}

        task = casa_tasks.plotms(**task_args)
        if not os.path.exists(self.figfile):
            task.execute()

        return self._get_plot_object(task)

    def _get_figfile(self):
        session_part = self.ms.session
        ms_part = self.ms.basename
        return os.path.join(self.context.report_dir,
                            'session%s' % session_part,
                            ms_part, 'el_vs_time.png')

    def _get_plot_object(self, task):
        return logger.Plot(self.figfile,
                           x_axis='Time',
                           y_axis='Elevation',
                           parameters={'vis': self.ms.basename},
                           command=str(task))


class ParameterVsTimeChart(object):
    """
    Base class for FieldVsTimeChart and IntentVsTimeChart, sharing common logic such as the colour scheme for intents
    """

    # list of intents that shares a same scan but segregated by subscan
    # (To distinguish ON and OFF source subscans in ALMA-TP)
    _subscan_intents = ('TARGET', 'REFERENCE')

    # the order of items here corresponds to the order they are shown in IntentVsTime diagram (from bottom to top).
    _intent_colours = dict([
        ('TARGET', 'blue'),
        ('REFERENCE', 'deepskyblue'),
        ('PHASE', 'cyan'),
        ('CHECK', '#700070'),  # slightly darker than 'purple'
        ('BANDPASS', 'red'),
        ('AMPLITUDE', 'green'),
        ('ATMOSPHERE', 'magenta'),
        ('POINTING', 'yellow'),
        ('SIDEBAND', 'orange'),
        ('WVR', 'lime'),
        ('DIFFGAINREF', 'maroon'),
        ('DIFFGAINSRC', 'deeppink'),
        ('POLARIZATION', 'navy'),
        ('POLANGLE', 'mediumslateblue'),
        ('POLLEAKAGE', 'plum'),
        ('UNKNOWN', 'grey'),
    ])

    @staticmethod
    def _set_time_axis(figure, ax, datemin, datemax):
        border = datetime.timedelta(minutes=5)
        ax.set_xlim(datemin - border, datemax + border)

        if datemax - datemin < datetime.timedelta(seconds=7200):
            # scales if observation spans less than 2 hours
            quarterhours = dates.MinuteLocator(interval=15)
            minutes = dates.MinuteLocator(interval=5)
            ax.xaxis.set_major_locator(quarterhours)
            ax.xaxis.set_major_formatter(dates.DateFormatter('%Hh%Mm'))
            ax.xaxis.set_minor_locator(minutes)
        elif datemax - datemin < datetime.timedelta(seconds=21600):
            # scales if observation spans less than 6 hours
            halfhours = dates.MinuteLocator(interval=30)
            minutes = dates.MinuteLocator(interval=10)
            ax.xaxis.set_major_locator(halfhours)
            ax.xaxis.set_major_formatter(dates.DateFormatter('%Hh%Mm'))
            ax.xaxis.set_minor_locator(minutes)
        elif datemax - datemin < datetime.timedelta(days=1):
            # scales if observation spans less than a day
            hours = dates.HourLocator(interval=1)
            minutes = dates.MinuteLocator(interval=10)
            ax.xaxis.set_major_locator(hours)
            ax.xaxis.set_major_formatter(dates.DateFormatter('%Hh%Mm'))
            ax.xaxis.set_minor_locator(minutes)
        elif datemax - datemin < datetime.timedelta(days=7):
            # spans more than a day, less than a week
            days = dates.DayLocator()
            hours = dates.HourLocator(np.arange(0, 25, 6))
            ax.xaxis.set_major_locator(days)
            ax.xaxis.set_major_formatter(dates.DateFormatter('%Y-%m-%d:%Hh'))
            ax.xaxis.set_minor_locator(hours)
        else:
            # spans more than a week
            months = dates.MonthLocator(bymonthday=1, interval=3)
            mondays = dates.WeekdayLocator(dates.MONDAY)
            ax.xaxis.set_major_locator(months)
            ax.xaxis.set_major_formatter(dates.DateFormatter('%Y-%m'))
            ax.xaxis.set_minor_locator(mondays)

        ax.set_xlabel('Time')
        figure.autofmt_xdate()


class FieldVsTimeChartInputs(vdp.StandardInputs):

    @vdp.VisDependentProperty
    def output(self):
        session_part = self.ms.session
        ms_part = self.ms.basename
        output = os.path.join(self.context.report_dir,
                              'session%s' % session_part,
                              ms_part, 'field_vs_time.png')
        return output

    def __init__(self, context, vis=None, output=None):
        super(FieldVsTimeChartInputs, self).__init__()

        self.context = context
        self.vis = vis

        self.output = output


class FieldVsTimeChart(ParameterVsTimeChart):
    Inputs = FieldVsTimeChartInputs

    def __init__(self, inputs):
        self.inputs = inputs

    @casa5style_plot
    def plot(self):
        ms = self.inputs.ms

        obs_start = utils.get_epoch_as_datetime(ms.start_time)
        obs_end = utils.get_epoch_as_datetime(ms.end_time)

        filename = self.inputs.output
        if os.path.exists(filename):
            plot = logger.Plot(filename,
                               x_axis='Time',
                               y_axis='Field',
                               parameters={'vis': ms.basename})
            return plot

        f = plt.figure()
        plt.clf()
        plt.axes([0.1, 0.15, 0.8, 0.7])
        ax = plt.gca()

        nfield = len(ms.fields)
        for field in ms.fields:
            ifield = field.id
            for scan in [scan for scan in ms.scans
                         if field in scan.fields]:
                intents_to_plot = self._get_intents_to_plot(field.intents.intersection(scan.intents))
                num_intents = len(intents_to_plot)
                assert num_intents > 0, "number of intents to plot is not larger than 0"

                # vertical position to plot
                y0 = ifield-0.5
                y1 = ifield+0.5

                height = (y1 - y0) / float(num_intents)
                ys = y0
                ye = y0 + height
                for intent in intents_to_plot:
                    colour = self._intent_colours[intent]
                    if intent in self._subscan_intents and len(scan.intents.intersection(self._subscan_intents)) > 1:
                        time_ranges = [tuple(map(utils.get_epoch_as_datetime, o)) \
                                       for o in get_intent_subscan_time_ranges(ms.name, utils.to_CASA_intent(ms, intent), scan.id)]
                    else:
                        # all 'datetime' objects are in UTC.
                        start = utils.get_epoch_as_datetime(scan.start_time)
                        end = utils.get_epoch_as_datetime(scan.end_time)
                        time_ranges = ((start, end), )

                    for (x0, x1) in time_ranges:
                        ax.fill([x0, x1, x1, x0],
                                [ys, ys, ye, ye],
                                facecolor=colour,
                                edgecolor=colour)
                    ys += height
                    ye += height

        # set the labelling of the time axis
        self._set_time_axis(figure=f, ax=ax, datemin=obs_start, datemax=obs_end)

        # set FIELD_ID axis ticks etc.
        if nfield < 11:
            major_locator = ticker.FixedLocator(np.arange(0, nfield+1))
            minor_locator = ticker.MultipleLocator(1)
            ax.yaxis.set_minor_locator(minor_locator)
        else:
            step = np.ceil(nfield / 10.)  # show at most 10 tick labels
            major_locator = ticker.IndexLocator(step, 0)
        ax.yaxis.set_major_locator(major_locator)
        ax.grid(True)

        plt.ylabel('Field ID')
        major_formatter = ticker.FormatStrFormatter('%d')
        ax.yaxis.set_major_formatter(major_formatter)

        # plot key
        self._plot_key()

        plt.savefig(filename)
        plt.clf()
        plt.close()

        plot = logger.Plot(filename,
                           x_axis='Time',
                           y_axis='Field',
                           parameters={'vis': ms.basename})

        return plot

    def _plot_key(self):
        plt.axes([0.1, 0.8, 0.8, 0.2])
        lims = plt.axis()
        plt.axis('off')

        x = 0.00
        size = [0.4, 0.4, 0.6, 0.6]
        # show a sorted list of intents occurring in this plot, but move UNKNOWN to the end of the list
        intents = sorted(self._intent_colours.keys())
        del intents[intents.index('UNKNOWN')]
        intents.append('UNKNOWN')
        for intent in intents:
            if (intent in self.inputs.ms.intents) or intent == 'UNKNOWN':
                plt.gca().fill([x, x+0.035, x+0.035, x], size, facecolor=self._intent_colours[intent], edgecolor=None)
                plt.text(x+0.04, 0.4, intent, size=9, va='bottom', rotation=45)
                x += 0.10

        plt.axis(lims)

    def _get_intents_to_plot(self, user_intents):
        intents = [intent for intent in sorted(self._intent_colours.keys(), key=operator.itemgetter(0))
                   if intent in user_intents]
        if not intents:
            intents.append('UNKNOWN')
        return intents


class IntentVsTimeChartInputs(vdp.StandardInputs):

    @vdp.VisDependentProperty
    def output(self):
        session_part = self.ms.session
        ms_part = self.ms.basename
        output = os.path.join(self.context.report_dir,
                              'session%s' % session_part,
                              ms_part, 'intent_vs_time.png')
        return output

    def __init__(self, context, vis=None, output=None):
        super(IntentVsTimeChartInputs, self).__init__()

        self.context = context
        self.vis = vis

        self.output = output


class IntentVsTimeChart(ParameterVsTimeChart):
    Inputs = IntentVsTimeChartInputs

    # http://matplotlib.org/examples/color/named_colors.html

    # list of intents that shares a same scan but segregated by subscan
    # (To dustinguish ON and OFF source subscans in ALMA-TP)
    _subscan_intents = ('TARGET', 'REFERENCE')

    def __init__(self, inputs):
        self.inputs = inputs

    def plot(self):
        if os.path.exists(self.inputs.output):
            return self._get_plot_object()

        fig = plt.figure(figsize=(14, 9))
        ax = fig.add_subplot(111)

        ms = self.inputs.ms
        obs_start = utils.get_epoch_as_datetime(ms.start_time)
        obs_end = utils.get_epoch_as_datetime(ms.end_time)

        for scan in ms.scans:
            scan_start = utils.get_epoch_as_datetime(scan.start_time)
            scan_end = utils.get_epoch_as_datetime(scan.end_time)
            for scan_y, (intent, colour) in enumerate(self._intent_colours.items()):
                if intent not in scan.intents:
                    continue
                if intent in self._subscan_intents and \
                        len(scan.intents.intersection(self._subscan_intents)) > 1:
                    time_ranges = [tuple(map(utils.get_epoch_as_datetime, o))
                                   for o in get_intent_subscan_time_ranges(ms.name, utils.to_CASA_intent(ms, intent), scan.id) ]
                else:
                    time_ranges = ((scan_start, scan_end),)
                for (time_start, time_end) in time_ranges:
                    ax.fill([time_start, time_end, time_end, time_start],
                            [scan_y, scan_y, scan_y+1, scan_y+1],
                            facecolor=colour)

                ax.annotate('%s' % scan.id, (scan_start, scan_y+1.2))

        # put intent names on the vertical axis, replacing 'TARGET' with 'SCIENCE', removing 'UNKNOWN', and keeping other names intact
        intent_colours = self._intent_colours.copy()    # make a copy and then delete one element
        del intent_colours['UNKNOWN']
        num_intents = len(intent_colours)
        ax.set_ylim(0, num_intents+0.5)  # extra space on top for the label
        ax.set_yticks(np.linspace(0.5, num_intents-0.5, num_intents))
        ax.set_yticklabels([name.replace('TARGET', 'SCIENCE') for name in intent_colours.keys()])

        # set the labelling of the time axis
        self._set_time_axis(
            figure=fig, ax=ax, datemin=obs_start, datemax=obs_end)
        ax.grid(True)

        plt.title(
            'Measurement set: ' + ms.basename + ' - Start time:' +
            obs_start.strftime('%Y-%m-%dT%H:%M:%S') + ' End time:' +
            obs_end.strftime('%Y-%m-%dT%H:%M:%S'), fontsize=12)

        fig.savefig(self.inputs.output)
        plt.clf()
        plt.close()

        return self._get_plot_object()

    @staticmethod
    def _in_minutes(dt):
        return (dt.days * 86400 + dt.seconds + dt.microseconds * 1e-6) / 60.0

    def _get_plot_object(self):
        filename = self.inputs.output
        return logger.Plot(filename,
                           x_axis='Time',
                           y_axis='Intent',
                           parameters={'vis': self.inputs.ms.basename})


class PWVChart(object):
    def __init__(self, context, ms):
        self.context = context
        self.ms = ms
        self.figfile = self._get_figfile()

    def plot(self):
        if os.path.exists(self.figfile):
            return self._get_plot_object()

        try:
            plotpwv.plotPWV(self.ms.name, figfile=self.figfile)
        except:
            LOG.debug('Could not create PWV plot')
            return None

        return self._get_plot_object()

    def _get_figfile(self):
        session_part = self.ms.session
        ms_part = self.ms.basename
        return os.path.join(self.context.report_dir,
                            'session%s' % session_part,
                            ms_part, 'pwv.png')

    def _get_plot_object(self):
        return logger.Plot(self.figfile,
                           x_axis='Time',
                           y_axis='PWV',
                           parameters={'vis': self.ms.basename})


class MosaicChart(object):
    def __init__(self, context, ms, source):
        self.context = context
        self.ms = ms
        self.source = source
        self.figfile = self._get_figfile()

    def plot(self):
        if os.path.exists(self.figfile):
            return self._get_plot_object()

        try:
            plotmosaic.plot_mosaic(self.ms, self.source, self.figfile)
        except Exception as e:
            LOG.warn('Could not create mosaic plot: {}'.format(e))
            return None

        return self._get_plot_object()

    def _get_figfile(self):
        session_part = self.ms.session
        ms_part = self.ms.basename
        return os.path.join(self.context.report_dir,
                            'session%s' % session_part,
                            ms_part, 'mosaic_source%s.png' % self.source.id)

    def _get_plot_object(self):
        return logger.Plot(self.figfile,
                           x_axis='RA Offset',
                           y_axis='Dec Offset',
                           parameters={'vis': self.ms.basename})


class PlotAntsChart(object):
    def __init__(self, context, ms, polarlog=False):
        self.context = context
        self.ms = ms
        self.polarlog = polarlog
        self.figfile = self._get_figfile()
        self.site = casa_tools.measures.observatory(ms.antenna_array.name)

    def plot(self):
        if os.path.exists(self.figfile):
            return self._get_plot_object()

        # map: with pad names
        plf1 = plt.figure(1)
        plt.clf()
        if self.polarlog:
            self.draw_polarlog_ant_map_in_subplot(plf1)
        else:
            self.draw_pad_map_in_subplot(plf1, self.ms.antennas)
        plt.title('Antenna Positions for %s' % self.ms.basename)
        plt.savefig(self.figfile, format='png', dpi=108)
        plt.clf()
        plt.close()

        return self._get_plot_object()

    def _get_figfile(self):
        session_part = self.ms.session
        ms_part = self.ms.basename
        if self.polarlog:
            figfilename = 'plotants_polarlog.png'
        else:
            figfilename = 'plotants.png'
        return os.path.join(self.context.report_dir,
                            'session%s' % session_part,
                            ms_part, figfilename)

    def _get_plot_object(self):
        return logger.Plot(self.figfile,
                           x_axis='Antenna Longitude',
                           y_axis='Antenna Latitude',
                           parameters={'vis': self.ms.basename})

    def draw_pad_map_in_subplot(self, plf, antennas, xlimit=None,
                                ylimit=None, showemptypads=True):
        """
        Draw a map of pads and antennas on them.

        plf: a matplotlib.pyplot.figure instance
        pads: a dictionary of antennas {"Name": (X, Y, Z), ...}
        antennas: a dictionary of antennas {"AntennaName": "PadName", ...}
        xlimit, ylimit: lists (or tuples, arrays) for the x and y axis limits.
                        if not given, automatically adjusted.
        showemptypads: set False not to draw pads and their names
        """
        subpl = plf.add_subplot(1, 1, 1, aspect='equal')

        if showemptypads:
            for antenna in antennas:
                padpos = self.get_position(antenna)
                circ = plt.Circle(padpos[:2], antenna.diameter/2.0)
                subpl.add_artist(circ)
                circ.set_alpha(0.5)
                circ.set_facecolor([1.0, 1.0, 1.0])
                tt = subpl.text(padpos[0]+antenna.diameter/2.0*1.3, padpos[1]-4., antenna.station)
                plt.setp(tt, size='small', alpha=0.5)

        (xmin, xmax, ymin, ymax) = (9e9, -9e9, 9e9, -9e9)
        for antenna in antennas:
            padpos = self.get_position(antenna)
            circ = plt.Circle(padpos[:2], radius=antenna.diameter/2.0)
            subpl.add_artist(circ)
            circ.set_alpha(1.0)
            circ.set_facecolor([0.8, 0.8, 0.8])
            subpl.text(padpos[0]+antenna.diameter/2.0*1.3, padpos[1]+1, antenna.name)
            if padpos[0] < xmin:
                xmin = padpos[0]
            if padpos[0] > xmax:
                xmax = padpos[0]
            if padpos[1] < ymin:
                ymin = padpos[1]
            if padpos[1] > ymax:
                ymax = padpos[1]

        subpl.set_xlabel('X [m]')
        subpl.set_ylabel('Y [m]')
        plotwidth = max(xmax-xmin, ymax-ymin) * 6./10.  # extra 1/10 is the margin
        (xcenter, ycenter) = ((xmin+xmax)/2., (ymin+ymax)/2.)
        if xlimit is None:
            # subpl.set_xlim(xcenter-plotwidth, xcenter+plotwidth)
            subpl.set_xlim(xcenter[0]-plotwidth[0], xcenter[0]+plotwidth[0])
        else:
            subpl.set_xlim(xlimit[0], xlimit[1])
        if ylimit is None:
            # subpl.set_ylim(ycenter-plotwidth, ycenter+plotwidth)
            subpl.set_ylim(ycenter[0]-plotwidth[0], ycenter[0]+plotwidth[0])
        else:
            subpl.set_ylim(ylimit[0], ylimit[1])

    @staticmethod
    def get_position(antenna):
        # if self.ms.antenna_array.name == 'ALMA':
        #    # Arbitrarily shift ALMA coord so that central cluster comes
        #    # around (0, 0).
        #    pos = (pos[0]+480., pos[1]-14380., pos[2])

        pos = [[antenna.offset['longitude offset']['value']],
               [antenna.offset['latitude offset']['value']],
               [antenna.offset['elevation offset']['value']]]

        return np.array(pos)

    # This plot is adapted from the "plotPositionsLogarithmic" function
    # in the Analysis Utils written by Todd Hunter.
    def draw_polarlog_ant_map_in_subplot(self, plf):
        """
        Draw a polar-log map of antennas.

        plf: a matplotlib.pyplot.figure instance
        """

        # Get longitude and latitude offsets in meters for antennas.
        xoffsets = np.array([ant.offset['longitude offset']['value']
                             for ant in self.ms.antennas])
        yoffsets = np.array([ant.offset['latitude offset']['value']
                             for ant in self.ms.antennas])

        # Set center of plot and min/max rmin as appropriate for observatory.
        if self.context.project_summary.telescope in ('VLA', 'EVLA'):
            # For (E)VLA, set a fixed local center position that has been
            # tuned to work well for its array configurations (CAS-7479).
            xcenter, ycenter = -32, 0
            rmin_min, rmin_max = 12.5, 350
        else:
            # For non-(E)VLA, take the median of antenna offsets as the
            # center for the plot.
            xcenter = np.median(xoffsets)
            ycenter = np.median(yoffsets)
            rmin_min, rmin_max = 3, 350

        # Derive radial offset w.r.t. center position.
        r = ((xoffsets-xcenter)**2 + (yoffsets-ycenter)**2)**0.5

        # Set rmin, clamp between a min and max value, ignore station
        # at r=0 if one is there.
        rmin = min(rmin_max, max(rmin_min, 0.8*np.min(r[r > 0])))

        # Update r to move any points below rmin to r=rmin.
        r[r <= rmin] = rmin
        rmin = np.log(rmin)

        # Set rmax.
        rmax = np.log(1.5*np.max(r))

        # Derive angle of offset w.r.t. center position.
        theta = np.arctan2(xoffsets-xcenter, yoffsets-ycenter)

        # Set up subplot.
        subpl = plf.add_subplot(1, 1, 1, polar=True, projection='polar')

        # Set zero point and direction for theta angle.
        subpl.set_theta_zero_location('N')
        subpl.set_theta_direction(-1)

        # Do not show azimuth labels.
        subpl.set_xticklabels([])
        subpl.set_yticklabels([])

        # Do not show grid.
        subpl.grid(False)

        # Draw circles at specific distances from the center.
        angles = np.arange(0, 2.01*np.pi, 0.01*np.pi)
        show_circle = True
        for cr in [30, 100, 300, 1000, 3000, 10000]:

            # Only draw circles outside rmin.
            if cr > np.min(r) and show_circle:

                # Draw the circle.
                radius = np.ones(len(angles))*np.log(cr)
                subpl.plot(angles, radius, 'k:')

                # Draw tick marks on the circle at 1 km intervals.
                inc = 0.1*10000/cr
                if cr > 100:
                    for angle in np.arange(inc/2., 2*np.pi+0.05, inc):
                        subpl.plot([angle, angle],
                                   [np.log(0.95*cr), np.log(1.05*cr)], 'k-')

                # Add text label to circle to denote distance from center.
                va = 'top'
                circle_label_angle = -20.0 * np.pi / 180.
                if cr >= 1000:
                    if np.log(cr) < rmax:
                        subpl.text(circle_label_angle, np.log(cr),
                                   '%d km' % (cr//1000), size=8, va=va)
                        subpl.text(circle_label_angle + np.pi, np.log(cr),
                                   '%d km' % (cr // 1000), size=8, va=va)
                else:
                    subpl.text(circle_label_angle, np.log(cr), '%dm' % (cr),
                               size=8, va=va)
                    subpl.text(circle_label_angle + np.pi, np.log(cr), '%dm' % (cr),
                               size=8, va=va)

            # Find out if most recently drawn circle was outside all antennas,
            # if so, no more circles will be drawn.
            if np.log(cr) > rmax:
                show_circle = False

        # For each antenna:
        for i, antenna in enumerate(self.ms.antennas):

            # Draw the antenna position.
            subpl.plot(theta[i], np.log(r[i]), 'ko', ms=5, mfc='k')

            # Draw label for the antenna.
            subpl.text(theta[i], np.log(r[i]), '   '+antenna.name, size=8,
                       color='k', ha='left', va='bottom', weight='bold')

            # Create label for legend
            if max(r) < 100:
                label = r'{}: {:2.0f} m, {:4.0f}$^\circ$'.format(
                    antenna.name, r[i], np.degrees(theta[i]))
            elif max(r) < 1000:
                label = r'{}: {:3.0f} m, {:4.0f}$^\circ$'.format(
                    antenna.name, r[i], np.degrees(theta[i]))
            elif max(r) < 3000:
                label = r'{}: {:3.2f} km, {:4.0f}$^\circ$'.format(
                    antenna.name, 0.001*r[i], np.degrees(theta[i]))
            else:
                label = r'{}: {:3.1f} km, {:4.0f}$^\circ$'.format(
                    antenna.name, 0.001*r[i], np.degrees(theta[i]))

            # Draw a key in the legend for finding the antenna.
            subpl.annotate(label, xy=(0.5, 0.5),
                           xytext=(0.02, 0.925-0.90*i/len(self.ms.antennas)),
                           xycoords='figure fraction',
                           textcoords='figure fraction', weight='bold',
                           arrowprops=None, color='black', ha='left',
                           va='center', size=8)

        # Set minimum and maximum radius.
        subpl.set_rmax(rmax)
        subpl.set_rmin(rmin)


class UVChart(object):
    # CAS-11793: calsurveys do not have TARGET sources, so we must also
    # search for sources with other intents
    preferred_intent_order = ['TARGET', 'AMPLITUDE', 'BANDPASS', 'PHASE']

    def __init__(self, context, ms, customflagged=False, output_dir=None, title_prefix=None):
        self.context = context
        self.ms = ms
        self.customflagged = customflagged
        self.figfile = self._get_figfile(output_dir=output_dir)

        # Get spw_id, field, field_name, and intent to plot.
        self.spw_id, self.field, self.field_name, self.intent = self._get_spwid_and_field()

        if self.spw_id is not None:
            # Determine number of channels in spw.
            self.nchan = self._get_nchan_for_spw(self.spw_id)

            # Set title of plot, modified by prefix if provided.
            self.title = 'UV coverage for {}'.format(self.ms.basename)
            if title_prefix:
                self.title = title_prefix + self.title

            # get max UV via unprojected baseline
            spw = ms.get_spectral_window(self.spw_id)
            wavelength_m = 299792458 / float(spw.max_frequency.to_units(FrequencyUnits.HERTZ))
            bl_max = float(ms.antenna_array.baseline_max.length.to_units(DistanceUnits.METRE))
            self.uv_max = math.ceil(1.05 * bl_max / wavelength_m)

    def plot(self):
        if DISABLE_PLOTMS:
            LOG.debug('Disabling UV coverage plot due to problems with plotms')
            return None

        # Don't plot if no spw was found for the field/source/intent or if the set of plotting parameters doesn't
        # exist in the MS. See PIPE-1225.
        if (self.spw_id is None) or (not self._is_valid()):
            LOG.debug('Disabling UV coverage plot due to being unable to find a set of parameters to plot.')
            return None

        # inputs based on analysisUtils.plotElevationSummary
        task_args = {
            'vis': self.ms.name,
            'xaxis': 'uwave',
            'yaxis': 'vwave',
            'title': self.title,
            'avgchannel': self.nchan,
            'antenna': '*&*',
            'spw': self.spw_id,
            'field': self.field,
            'intent': utils.to_CASA_intent(self.ms, self.intent),
            'plotfile': self.figfile,
            'clearplots': True,
            'showgui': False,
            'customflaggedsymbol': self.customflagged,
            'plotrange': [-self.uv_max, self.uv_max, -self.uv_max, self.uv_max],
            'height': 1000,
            'width': 1000
        }

        task = casa_tasks.plotms(**task_args)

        if not os.path.exists(self.figfile):
            task.execute()

        return self._get_plot_object(task)

    def _get_figfile(self, output_dir=None):
        # If output dir is specified, then store as <msname>-uv_coverage.png in output dir.
        if output_dir:
            figfile = os.path.join(output_dir, "{}-uv_coverage.png".format(self.ms.basename))
        # Otherwise, store under <sessionname>/<msname>/ directory as uv_coverage.png.
        else:
            session_part = self.ms.session
            ms_part = self.ms.basename
            figfile = os.path.join(self.context.report_dir, 'session%s' % session_part, ms_part, 'uv_coverage.png')
        return figfile

    def _get_plot_object(self, task):
        return logger.Plot(self.figfile,
                           parameters={'vis': self.ms.basename,
                                       'field': self.field,
                                       'field_name': self.field_name,
                                       'intent': self.intent,
                                       'spw': self.spw_id},
                           command=str(task))

    def _get_spwid_and_field(self) -> Tuple[str, str, str, str]:
        # Attempt to get representative source and spwid.
        repr_src, repr_spw = self._get_representative_source_and_spwid()

        # Check that representative source was covered with TARGET intent,
        # otherwise reject.
        target_sources = [source for source in self.ms.sources
                          if source.name == repr_src
                          and 'TARGET' in source.intents]
        if not target_sources:
            repr_src = None

        if repr_src:
            field, field_name, intent = self._get_field_for_source(repr_src)
            if repr_spw:
                # If both are defined, return representative src and spw.
                return str(repr_spw), field, field_name, intent
            else:
                # If only the repr_src is defined, get the field, then find the first valid spw
                spw = self._get_first_available_science_spw(field, intent)
                return spw, field, field_name, intent

        # If no representative source was identified, then get the preferred source and science spw
        return self._get_preferred_science_spw_and_field()

    def _get_representative_source_and_spwid(self) -> Tuple[str, int]:
        # Is the representative source in the context or not
        if not self.context.project_performance_parameters.representative_source:
            source_name = None
        else:
            source_name = self.context.project_performance_parameters.representative_source

        # Is the representative spw in the context or not
        if not self.context.project_performance_parameters.representative_spwid:
            source_spwid = None
        else:
            source_spwid = self.context.project_performance_parameters.representative_spwid

        # Determine first target and first science spw for that target (even in multi-band data) for VLA
        if self.context.project_summary.telescope in ('VLA', 'EVLA'):
            fieldobjs = self.ms.get_fields(intent='TARGET')
            first_field = fieldobjs[0]
            source_name = first_field.name
            source_spwobjlist = list(first_field.valid_spws)
            source_spwidlist = [spw.id for spw in source_spwobjlist]
            source_spwidlist.sort()
            source_spwid = source_spwidlist[0]

        # Determine the representative source name and spwid for the ms
        repsource_name, repsource_spwid = self.ms.get_representative_source_spw(source_name=source_name,
                                                                                source_spwid=source_spwid)

        return repsource_name, repsource_spwid

    def _get_first_available_science_spw(self, field: str, intent: str) -> str:
        science_spws = self.ms.get_spectral_windows(science_windows_only=True)
        selected_field = self.ms.get_fields(field_id=int(field))[0]
        possible_spws = selected_field.valid_spws.intersection(set(science_spws))
        possible_spws_intents = [spw for spw in possible_spws if intent in spw.intents]

        # Do not set spw_id or plot if it wasn't possible to find a usable spw for the selected source, field, and intent.
        if not possible_spws_intents:
            LOG.debug("{}: Could not find a spw to plot with the field: {} and intent: {}".format(self.ms.basename, self.field, self.intent))
            spw = None
            return spw

        # Get and return first spw by id
        final_spw = sorted(possible_spws_intents, key=operator.attrgetter('id'))[0]
        spw = str(final_spw.id)
        return spw

    def _get_preferred_science_spw_and_field(self) -> Tuple[str, str, str, str]:
        # take first TARGET sources, otherwise first AMPLITUDE sources, etc.
        for intent in self.preferred_intent_order:
            sources_with_intent = [s for s in self.ms.sources if intent in s.intents]
            if sources_with_intent:
                src = sources_with_intent[0]
                break
        else:
            LOG.warning('No source found with an intent in {}. Using first source for UV chart.'
                        ''.format(self.preferred_intent_order))
            src = list(self.ms.sources).pop()

        field, field_name, intent = self._get_field_for_source(src.name)
        spw = self._get_first_available_science_spw(field, intent)

        return spw, field, field_name, intent

    def _get_field_for_source(self, src_name: str) -> Tuple[str, str, str]:
        sources_with_name = [s for s in self.ms.sources if s.name == src_name]
        if not sources_with_name:
            LOG.error("Source {} not found in MS.".format(src_name))
            return ''
        if len(sources_with_name) > 1:
            LOG.warning('More than one source called {} in {}. Taking first source'.format(src_name, self.ms.basename))
        src = sources_with_name[0]

        # Identify fields covered by an intent in preferred_intent_order, in order
        for intent in self.preferred_intent_order:
            fields_with_intent = [f for f in src.fields if intent in f.intents]
            if fields_with_intent:
                centre_field = self._get_center_field(fields_with_intent)
                break
        else:
            LOG.error("Source {} has no field with an intent in {}".format(src_name, self.preferred_intent_order))
            return '', '', ''

        return str(centre_field.id), centre_field.name, intent

    @staticmethod
    def _get_center_field(fields):
        # TODO: need algorithm to determine centermost field among
        # series of pointings for a mosaic.
        # For now, assume the first field is the centermost pointing.
        return fields[0]

    def _get_nchan_for_spw(self, spwid):
        if spwid is None:
            return None

        spw = self.ms.get_spectral_window(int(spwid))
        nchan = str(len(spw.channels))
        return nchan

    def _is_valid(self) -> bool:
        with casa_tools.MSReader(self.ms.name) as msfile:
            casa_intent = utils.to_CASA_intent(self.ms, self.intent)
            staql = {'field': self.field, 'spw': self.spw_id, 'scanintent': casa_intent}
            select_valid = msfile.msselect(staql, onlyparse=False)
            return select_valid


class SpwIdVsFreqChartInputs(vdp.StandardInputs):
    """Inputs class for SpwIdVsFreqChart."""

    @vdp.VisDependentProperty
    def output(self) -> str:
        """Return file path of output PNG file.

        Returns:
            output: File path of output PNG file
        """
        session_part = self.ms.session
        ms_part = self.ms.basename
        output = os.path.join(self.context.report_dir,
                              'session%s' % session_part,
                              ms_part, 'spwid_vs_freq.png')
        return output

    def __init__(self, context: 'Context', vis: str) -> None:
        """Construct SpwIdVsFreqChartInputs instance.

        Args:
            context: Pipeline context
            vis: Name of MS
        """
        super().__init__()

        self.context = context
        self.vis = vis


class SpwIdVsFreqChart(object):
    """Generate a plot of SPW ID Versus Frequency coverage."""

    Inputs = SpwIdVsFreqChartInputs

    def __init__(self, inputs: SpwIdVsFreqChartInputs, context: 'Context') -> None:
        """Construct SpwIdVsFreqChart instance.

        Args:
            inputs: SpwIdVsFreqChartInputs instance
            context: Pipeline context
        """
        self.inputs = inputs
        self.context = context

    def _extract_spwdata_vla(self) -> Generator[List[int], None, None]:
        """Extract list of SPW IDs of VLA from measurement set.

        Yields:
            List of SPW IDs for a baseband, for science_windows_only=True.
        """
        ms = self.inputs.ms
        banddict = ms.get_vla_baseband_spws(science_windows_only=True, return_select_list=False, warning=False)
        for band in banddict:
            for baseband in banddict[band]:
                spw_list = [list(spwitem.keys())[0] for spwitem in banddict[band][baseband]]
                yield spw_list

    def _extract_spwdata_alma_nro(self) -> Generator[List[int], None, None]:
        """Extract list of SPW IDs of ALMA or NRO from measurement set.

        Yields:
            List of SPW IDs for a tuning, for scan_intent='TARGET'.
        """
        ms = self.inputs.ms
        request_spws = ms.get_spectral_windows()
        targeted_scans = ms.get_scans(scan_intent='TARGET')
        scan_spws = {spw for scan in targeted_scans for spw in scan.spws if spw in request_spws}
        for spwid_list in utils.get_spectralspec_to_spwid_map(scan_spws).values():
            yield spwid_list

    def plot(self) -> logger.Plot:
        """Create the plot.

        Returns:
            Plot object
        """
        filename = self.inputs.output
        if os.path.exists(filename):
            return self._get_plot_object()
        ms = self.inputs.ms
        request_spws = ms.get_spectral_windows()
        targeted_scans = ms.get_scans(scan_intent='TARGET')
        antid = 0
        if hasattr(ms, 'reference_antenna') and isinstance(ms.reference_antenna, str):
            antid = ms.get_antenna(search_term=ms.reference_antenna.split(',')[0])[0].id
        # prepare axes
        fig = figure.Figure(figsize=(9.6, 7.2))
        ax_spw = fig.add_axes([0.1, 0.1, 0.8, 0.8])
        bar_height = 0.4
        max_spws_to_annotate_VLA = 16  # request for VLA, PIPE-1415.
        max_spws_to_annotate_ALMA_NRO = np.inf  # annotate all spws for ALMA/NRO
        colorcycle = matplotlib.rcParams['axes.prop_cycle']()
        atm_color = [0.17, 0.17, 0.17]  # very dark gray
        ax_atm = ax_spw.twinx()

        # plot spws
        if self.context.project_summary.telescope in ('VLA', 'EVLA'):  # For VLA
            spw_list_generator = self._extract_spwdata_vla()
            scan_spws = request_spws
            max_spws_to_annotate = max_spws_to_annotate_VLA
        else:  # for ALMA or NRO
            spw_list_generator = self._extract_spwdata_alma_nro()
            scan_spws = {spw for scan in targeted_scans for spw in scan.spws if spw in request_spws}
            max_spws_to_annotate = max_spws_to_annotate_ALMA_NRO
        xmin, xmax = np.inf, -np.inf
        totalnum_spws = len(scan_spws)
        idx = 0
        rmin = np.inf
        for spwid_list in spw_list_generator:
            color = next(colorcycle)['color']
            for spwid in spwid_list:

                # 1. draw bars
                spwdata = [spw for spw in scan_spws if spw.id == spwid][0]
                bw = float(spwdata.bandwidth.to_units(FrequencyUnits.GIGAHERTZ))
                fmin = float(spwdata.min_frequency.to_units(FrequencyUnits.GIGAHERTZ))
                xmin, xmax = min(xmin, fmin), max(xmax, fmin+bw)
                ax_spw.barh(idx, bw, height=bar_height, left=fmin, color=color)

                # 2. annotate each bars
                if totalnum_spws <= max_spws_to_annotate or spwid in [spwid_list[0], spwid_list[-1]]:
                    ax_spw.annotate(str(spwid), (fmin + bw/2, idx - bar_height/2), fontsize=14, ha='center', va='bottom')
                idx += 1
                rmin = min(rmin, abs(atmutil.get_spw_spec(vis=ms.name, spw_id=spwid)[2]))

        # 3. Frequency vs. ATM transmission
        center_freq = (xmin + xmax) / 2.0
        # Determining the resolution value so that generates fine ATM transmission curve: it is set
        # to smaller than 500 kHz but is set to larger than that corresponding to 48001 data points.
        default_resolution = 5e-4  # To have 5 data points within the ozone feature of 2 MHz FWHM:
                                   # 2 MHz/(5-1) = 500 kHz.
        max_nchan = 48001  # 24 GHz/500 kHz, where 24 GHz covers both sidebands of a 4-12 GHz IF
                           # for a single LO tuning.
        fspan = xmax - xmin
        resolution = min(default_resolution, rmin)
        nchan = min(max_nchan, round(fspan / resolution) + 1)
        resolution = fspan / (nchan - 1)
        LOG.info("'Spectral Window ID vs. Frequency' plots the atmospheric transmission with %d data points at %.3f kHz intervals." % (nchan, resolution*1e6))
        atm_freq, atm_transmission = atmutil.get_transmission_for_range(vis=ms.name, center_freq=center_freq, nchan=nchan, resolution=resolution, antenna_id=antid, doplot=False)

        ax_atm.plot(atm_freq, atm_transmission, color=atm_color, alpha=0.6, linestyle='-', linewidth=2.0)

        # set axes limits, title, label, grid, tick, and save the plot to file
        ax_spw.set_xlim(xmin-(xmax-xmin)/15.0, xmax+(xmax-xmin)/15.0)
        ax_spw.invert_yaxis()
        ax_spw.set_ylim(totalnum_spws + totalnum_spws/20.0, -1.0 - totalnum_spws/20.0)  # The spw indices are from 0 to totalnum_spws. y-axis is inverted. totalnum_spws/20.0 is a mergin. -1.0 is upper edge.
        ax_spw.set_title('Spectral Window ID vs. Frequency', loc='center')
        ax_spw.set_xlabel("Frequency (GHz)", fontsize=14)
        ax_spw.grid(axis='x')
        ax_spw.tick_params(labelsize=13)
        ax_spw.set_yticks([])
        ax_atm.set_ylabel('ATM Transmission', color=atm_color, labelpad=2, fontsize=14)
        ax_atm.set_ylim(0, 1.05)
        ax_atm.tick_params(direction='out', colors=atm_color, labelsize=13)
        ax_atm.yaxis.set_major_formatter(ticker.FuncFormatter(lambda t, pos: '{}%'.format(int(t * 100))))
        ax_atm.yaxis.tick_right()
        fig.savefig(filename)
        return self._get_plot_object()

    def _get_plot_object(self) -> logger.Plot:
        """Get plot object.

        Returns:
            Plot object
        """
        filename = self.inputs.output
        return logger.Plot(filename,
                           x_axis='Frequency',
                           y_axis='spw ID',
                           parameters={'vis': self.inputs.ms.basename})


def get_intent_subscan_time_ranges(msname, casa_intent, scanid):
    """
    This function returns a list of start/end epoch pair of
    consequtive integrations (a subscan) with a selected intent
    in a selected scan. It can be used to filter subscans with
    an intent in a mixed intents scans, e.g., an ALMA TP
    scan that has both 'TARGET' and 'REFERENCE' subscans.

    Parameters
        msname: (string) the name of MeasurementSet
        casa_intent: (string) CASA intent to filter
        scanid: (int) a Scan ID to search. Must be
    Returns
        a list of start/end epoch tuple, e.g.,
        [(start_epoch, end_epoch), (start_epoch, end_epoch), ....]
    """
    if not os.path.exists(msname):
        raise ValueError('Could not find: {}'.format(msname))

    qt = casa_tools.quanta
    mt = casa_tools.measures

    with casa_tools.MSMDReader(msname) as msmd:
        LOG.info('obtaining subscan start/end time of {} in scan {}'.format(casa_intent, scanid))
        # Define a reference SpW ID that matches a selected scan and intent
        # the first spw tend to be WVR in ALMA. Pick the last one instead.
        intent_scan_spw = np.intersect1d(msmd.spwsforintent(intent=casa_intent),
                                         msmd.spwsforscan(scan=scanid))
        if len(intent_scan_spw) == 0:
            raise ValueError('No Spw match for a selected scan and intent')
        ref_spw = intent_scan_spw[-1]
        scan_times = msmd.timesforscan(scan=scanid, perspw=True)[str(ref_spw)]
        qhalf_exposure = qt.div(msmd.exposuretime(scan=scanid, spwid=ref_spw), 2.0)
        intent_times = msmd.timesforintent(casa_intent)

    # obtain time unit and ref from MS.
    # msmd returns raw TIME values but not unit and reference.
    with casa_tools.TableReader(msname) as tb:
        time_colkeywords = tb.getcolkeywords('TIME')

    time_unit = time_colkeywords['QuantumUnits'][0]
    time_ref = time_colkeywords['MEASINFO']['Ref']
    # sort scan_times to make sure it is in time order
    scan_times.sort()
    # obtain indices in scan_times array that has the selected intent
    scan_intent_idx = np.intersect1d(scan_times, intent_times, return_indices=True)[1]
    if len(scan_intent_idx) == 0:  # No integration with the intent
        LOG.info('No match found for scan {} and intent {}'.format(scanid, casa_intent))
        return ()

    # obtain subscan start/end indices
    if len(scan_intent_idx) == 1:  # only one integration matches
        split_scan_intent_idx = np.array([scan_intent_idx])
    else:  # split an array by consecutive idx
        split_scan_intent_idx = np.split(scan_intent_idx, np.where(np.diff(scan_intent_idx) != 1)[0]+1)

    LOG.info('Identified {} subscans'.format(len(split_scan_intent_idx)))

    subscan_time_ranges = []
    LOG.trace('subscan time ranges:')
    for subscan_idx in split_scan_intent_idx:
        start_time = qt.sub(qt.quantity(scan_times[subscan_idx[0]], time_unit),
                            qhalf_exposure)
        end_time = qt.add(qt.quantity(scan_times[subscan_idx[-1]],  time_unit),
                          qhalf_exposure)
        epoch_range = (mt.epoch(time_ref, start_time),
                       mt.epoch(time_ref, end_time))
        subscan_time_ranges.append(epoch_range)
        LOG.trace('* {} (id={}) - {} (id={})'.format(utils.get_epoch_as_datetime(epoch_range[0]), subscan_idx[0], utils.get_epoch_as_datetime(epoch_range[1]), subscan_idx[-1]))
    return subscan_time_ranges
