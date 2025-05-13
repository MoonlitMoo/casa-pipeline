import os
from typing import List

import matplotlib.pyplot as plt
from matplotlib.ticker import MultipleLocator

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.callibrary as callibrary
import pipeline.infrastructure.renderer.logger as logger
import pipeline.infrastructure.utils as utils
from pipeline.hifa.tasks.polcal import polcal
from pipeline.infrastructure.launcher import Context
from . import applycal
from . import common

LOG = infrastructure.get_logger(__name__)


class AmpVsAntennaChart(object):
    """
    Plotting class that creates a gain amplitude (ratio) vs. antenna plot
    per SpW for a caltable.
    """
    def __init__(self, context: Context, result: polcal.PolcalResults, calapps: List[callibrary.CalApplication],
                 correlation: str = ''):
        plot_args = {
            'xaxis': 'antenna1',
            'yaxis': 'amp',
            'correlation': correlation,
            'coloraxis': 'antenna1',
        }
        self.plotters = common.PlotmsCalSpwComposite(context, result, calapps, **plot_args)

    def plot(self) -> List[logger.Plot]:
        plot_wrappers = []
        plot_wrappers.extend(self.plotters.plot())
        return plot_wrappers


# TODO: move parent chart to common module?
class AmpVsParangSummaryChart(applycal.SpwSummaryChart):
    """
    Plotting class that creates an amplitude vs. parallactic angle plot per SpW
    for a measurement set.
    """
    def __init__(self, context: Context, output_dir: str, calto: callibrary.CalTo, **overrides):
        plot_args = {
            'ydatacolumn': 'data',
            'correlation': 'XX,YY',
            'averagedata': True,
            'avgchannel': '9999',
            'avgbaseline': True,
            'plotrange': [0, 0, 0, 0],
            'coloraxis': 'corr',
            'overwrite': True
        }
        plot_args.update(**overrides)

        super().__init__(context, output_dir, calto, xaxis='parang', yaxis='amp',
                         intent='POLARIZATION,POLANGLE,POLLEAKAGE', **plot_args)


class AmpVsScanChart(object):
    """
    Plotting class that creates a polarisation ratio amplitude vs. scan plot
    for a caltable.
    """
    def __init__(self, context: Context, result: polcal.PolcalResults, calapps: List[callibrary.CalApplication]):
        plot_args = {
            'xaxis': 'scan',
            'yaxis': 'amp',
            'correlation': '/',
            'coloraxis': 'spw',
        }
        self.plotters = common.PlotmsCalLeaf(context, result, calapps, **plot_args)

    def plot(self) -> List[logger.Plot]:
        plot_wrappers = []
        plot_wrappers.extend(self.plotters.plot())
        return plot_wrappers


class GainRatioRMSVsScanChart(object):
    """
    Plotting class that creates a gain ratio RMS vs. scan plot for a
    measurement set, using Matplotlib.
    """
    def __init__(self, context: Context, output_dir: str, result: polcal.PolcalSessionResults):
        self.context = context
        self.output_dir = output_dir
        self.result = result

    def plot(self) -> List[logger.Plot]:
        # Set plot file name.
        figfile = self._get_figfile()

        # Create plot wrapper.
        wrapper = logger.Plot(figfile, x_axis='Scan', y_axis='Gain Ratio RMS',
                              parameters={'vis': os.path.basename(self.result.vis)})

        # Create plot if still missing.
        if not os.path.exists(figfile):
            LOG.trace(f"Gain ratio rms vs scan plot for {self.result.vis} not found. Creating new plot:"
                      f" {figfile}.")

            try:
                self._create_plot(figfile)
            except Exception as e:
                LOG.error(f"Could not create Gain ratio rms vs scan plot for {self.result.vis}.")
                LOG.exception(e)
                # Ensure nothing is kept for next figure.
                plt.clf()
                return []

        return [wrapper]

    def _get_figfile(self) -> str:
        png = f"{self.result.vis}.gain_ratio_rms_vs_scan.png"
        return os.path.join(self.output_dir, png)

    def _create_plot(self, figfile: str):
        # Retrieve the gain ratio RMS per scan before and after polarization
        # calibration.
        scans_before, rrms_before = self.result.gain_ratio_rms_prior
        scans_after, rrms_after = self.result.gain_ratio_rms_after

        # Create plot with Matplotlib.
        fig, ax = plt.subplots()
        ax.plot(scans_before, rrms_before, 'or', label='Before polcal')
        ax.plot(scans_after, rrms_after, 'ob', label='After polcal')
        ax.set_xlabel('Scan Number')
        ax.set_ylabel('Gain Ratio RMS')
        ax.xaxis.set_minor_locator(MultipleLocator(1))
        ax.xaxis.set_major_locator(MultipleLocator(10))
        ax.legend(numpoints=1)
        plt.savefig(figfile)
        plt.close()


class PhaseVsChannelChart(object):
    """
    Plotting class that creates a polarisation ratio phase vs. channel plot
    for a caltable.
    """
    def __init__(self, context: Context, result: polcal.PolcalResults, calapps: List[callibrary.CalApplication]):
        plot_args = {
            'xaxis': 'chan',
            'yaxis': 'phase',
            'coloraxis': 'spw',
        }
        self.plotters = common.PlotmsCalLeaf(context, result, calapps, **plot_args)

    def plot(self) -> List[logger.Plot]:
        plot_wrappers = []
        plot_wrappers.extend(self.plotters.plot())
        return plot_wrappers


class RealVsImagChart(applycal.PlotmsLeaf):
    """
    Plotting class that creates a real vs. imag plot.
    """
    def __init__(self, context: Context, output_dir: str, calto: callibrary.CalTo, **overrides):
        plot_args = {
            'xaxis': 'real',
            'xdatacolumn': 'corrected',
            'yaxis': 'imag',
            'ydatacolumn': 'corrected',
            'intent': 'POLARIZATION,POLANGLE,POLLEAKAGE',
            'correlation': 'XX,YY',
            'averagedata': True,
            'avgchannel': '4000',
            'avgtime': '1000',
            'plotrange': [0, 0, 0, 0],
            'coloraxis': 'corr',
            'overwrite': True
        }
        plot_args.update(**overrides)

        super().__init__(context, output_dir, calto, **plot_args)

    def plot(self) -> List[logger.Plot]:
        jobs_and_wrappers = super().plot()
        successful_wrappers = utils.plotms_iterate(jobs_and_wrappers)
        return successful_wrappers


class XVsChannelSummaryChart(object):
    """
    Plotting class that creates an "X" vs. channel summary chart per SpW for
    all antennas, for a caltable.
    """
    def __init__(self, context: Context, result: polcal.PolcalResults, calapps: List[callibrary.CalApplication],
                 yaxis: str):
        plot_args = {
            'xaxis': 'chan',
            'yaxis': yaxis,
            'coloraxis': 'antenna1',
        }
        self.plotters = common.PlotmsCalSpwComposite(context, result, calapps, **plot_args)

    def plot(self) -> List[logger.Plot]:
        plot_wrappers = []
        plot_wrappers.extend(self.plotters.plot())
        return plot_wrappers


class XVsChannelDetailChart(object):
    """
    Plotting class that creates an "X" vs. channel detail chart per SpW and per
    antenna for a caltable.
    """
    def __init__(self, context: Context, result: polcal.PolcalResults, calapps: List[callibrary.CalApplication],
                 yaxis: str):
        plot_args = {
            'xaxis': 'chan',
            'yaxis': yaxis,
        }
        self.plotters = common.PlotmsCalSpwAntComposite(context, result, calapps, **plot_args)

    def plot(self) -> List[logger.Plot]:
        plot_wrappers = []
        plot_wrappers.extend(self.plotters.plot())
        return plot_wrappers
