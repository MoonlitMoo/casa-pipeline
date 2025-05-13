from typing import Union, List, Dict

import pipeline.h.tasks.common.displays.common as common
import pipeline.infrastructure.callibrary as callibrary
import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.utils as utils

LOG = infrastructure.get_logger(__name__)


class GaincalSummaryChart(object):
    """
    Base class for executing plotms per spw
    """
    def __init__(self, context, result, calapps, intent, xaxis, yaxis, plotrange=None, coloraxis='', ant : str=''):
        if plotrange is None:
            plotrange = []
        if yaxis == 'amp':
            calmode = 'a'
        elif yaxis == 'phase':
            calmode = 'p'
        else:
            raise ValueError('Unmapped calmode for y-axis: ' % yaxis)    

        # Identify the phase-only or amp-only solution for the target and filter by intent.
        selected = [c for c in calapps
                    # Check to see if any of the intents passed in are in the list of intents in the calapp
                    if (any([input_intent in c.intent for input_intent in intent.split(",")]) or c.intent == '') and
                    calmode == utils.get_origin_input_arg(c, 'calmode')]

        # request plots per spw, overlaying all antennas
        #
        # The PIPE-390 case of needing to handle plotting multiple caltables is now handled by the 
        # ability to support lists of calapps in the plotting infrastructure added in PIPE-1409 and
        # PIPE-1377.
        self.plotters = common.PlotmsCalSpwComposite(context, result, selected,
                                                xaxis=xaxis, yaxis=yaxis, ant=ant,
                                                plotrange=plotrange, coloraxis=coloraxis)

    def plot(self):
        plot_wrappers = []
        plot_wrappers.extend(self.plotters.plot())
        return plot_wrappers


class GaincalDetailChart(object):
    """
    Base class for executing plotms per spw and antenna
    """
    def __init__(self, context, result, calapps: List[callibrary.CalApplication], intent, xaxis, yaxis, plotrange=None, coloraxis=''):
        if plotrange is None:
            plotrange = []
        if yaxis == 'amp':
            calmode = 'a'
        elif yaxis == 'phase':
            calmode = 'p'
        else:
            raise ValueError('Unmapped calmode for y-axis: ' % yaxis)    

        # Identify the phase-only or amp-only solution for the target and filter by intent.
        selected = [c for c in calapps
                    # Check to see if any of the intents passed in are in the list of intents in the calapp
                    if (any([input_intent in c.intent for input_intent in intent.split(",")]) or c.intent == '') and
                    calmode == utils.get_origin_input_arg(c, 'calmode')]

        # Request plots per spw for the list of selected calapps, setting the same y-range for each spw.
        #
        # The PIPE-390 case of needing to handle plotting multiple caltables is now handled by the 
        # ability to support lists of calapps in the plotting infrastructure added in PIPE-1409 and
        # PIPE-1377. 
        self.plotters = common.PlotmsCalSpwAntComposite(context, result, selected,
                                                    xaxis=xaxis, yaxis=yaxis, 
                                                    plotrange=plotrange, coloraxis=coloraxis,
                                                    ysamescale=True)

    def plot(self):
        plot_wrappers = []
        plot_wrappers.extend(self.plotters.plot())
        return plot_wrappers


class GaincalAmpVsTimeSummaryChart(GaincalSummaryChart):
    """
    Create an amplitude vs time plot for each spw, overplotting by antenna as selected by ant,
    and setting the colors of the points by antenna. 
    """
    def __init__(self, context, result, calapps, intent, ant : str=''):
        super(GaincalAmpVsTimeSummaryChart, self).__init__(
            context, result, calapps, intent, xaxis='time', yaxis='amp', coloraxis='antenna1', ant=ant)


class GaincalAmpVsTimeDetailChart(GaincalDetailChart):
    """
    Create a phase vs time plot for each spw/antenna combination.
    """
    def __init__(self, context, result, calapps, intent):
        super(GaincalAmpVsTimeDetailChart, self).__init__(
            context, result, calapps, intent, xaxis='time', yaxis='amp', coloraxis='corr')


class GaincalPhaseVsTimeSummaryChart(GaincalSummaryChart):
    """
    Create a phase vs time plot for each spw, overplotting by antenna, and
    setting the colors of the points by antenna.
    """
    def __init__(self, context, result, calapps, intent):
        # request plots per spw, overlaying all antennas
        super().__init__(
            context, result, calapps, intent, xaxis='time', yaxis='phase', plotrange=[0, 0, -180, 180],
            coloraxis='antenna1')


class GaincalPhaseVsTimeDetailChart(GaincalDetailChart):
    """
    Create a phase vs time plot for each spw/antenna combination.
    """
    def __init__(self, context, result, calapps, intent):
        super(GaincalPhaseVsTimeDetailChart, self).__init__(
            context, result, calapps, intent, xaxis='time', yaxis='phase', plotrange=[0, 0, -180, 180],
            coloraxis='corr')
