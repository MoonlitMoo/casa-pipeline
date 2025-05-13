import os

import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import numpy as np

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.renderer.logger as logger

LOG = infrastructure.get_logger(__name__)


class SpatialStructureFunctionChart(object):
    """ 
    Creates a spatial structure function plot from the results 
    from PhaseStabilityHeuristics. Adapted from PIPE692.py as part of PIPE-1624. 
    """

    def __init__(self, context, result):
        self.result = result
        self.context = context
        self.vis = os.path.basename(self.result.inputs['vis'])
        self.spw = self.result.phaserms_results['spw']
        self.scan = self.result.phaserms_results['scan']
        self.field = self.result.phaserms_results['field']

    def create_plot(self):
        phaserms_results = self.result.phaserms_results

        # Set up plot
        figsize = np.array([11.0, 8.5])  # Appears mostly to affect the savefig not the interactive one
        plt.close(1)
        fig = plt.figure(1)
        fig.set_size_inches(figsize[0], figsize[1], forward=True)
        fig.clf()

        # positons,  width,  height - main plot 
        ax1 = fig.add_axes([0.10, 0.17, 0.82, 0.75])  

        # Plot the main result 
        ax1.plot(phaserms_results['bllen'], phaserms_results['blphaserms'], linestyle='', marker='o', c='0.6', zorder=0, label='Total-Time')
        # "after" is red like WVR plots
        ax1.plot(phaserms_results['bllen'], phaserms_results['blphasermscycle'], linestyle='', marker='o', c='r', zorder=1, label='Cycle-Time')

        ax1.set_xscale('log')
        ax1.set_yscale('log')

        # Make wide lines for 'limits' at 30, 50, 70 deg 
        ax1.plot([1.0, 20000.0], [29.0, 29.0], linestyle='-', c='g', linewidth=10.0, zorder=2, alpha=0.5) # green limit
        ax1.plot([1.0, 20000.0], [48.0, 48.0], linestyle='-', c='b', linewidth=10.0, zorder=2, alpha=0.5) # blue limit
        ax1.plot([1.0, 20000.0], [67.0, 67.0], linestyle='-', c='yellow', linewidth=10.0, zorder=2, alpha=0.5) # yellow limit

        # PIPE-1662 related, now plot the P80 bar out only to the MAX baseline that is UNFLAGGED
        ax1.plot([phaserms_results['blP80'], np.max(np.array(phaserms_results['bllen'])[np.isfinite(
            np.array(phaserms_results['blphaserms']))])], [phaserms_results['phasermscycleP80'], 
                     phaserms_results['phasermscycleP80']], c='0.2', linewidth=10, zorder=5, label='Median (bl > P80)', alpha=0.75)

        # Set y-axis ticks using phasermscycleP80
        if phaserms_results['phasermscycleP80'] < 50.0 :
            ax1.set_yticks([10.0, 20.0, 30.0, 50.0])
        else:
            ax1.set_yticks([10.0, 20.0, 30.0, 50.0, 70.0, 100.0, 300.0])

        # Set x-axis ticks using the max baseline
        if np.max(phaserms_results['bllen']) > 5000.0:
            ax1.set_xticks((50.0, 100.0, 500.0, 1000.0, 5000.0, 10000.0))
        elif np.max(phaserms_results['bllen']) > 1000.0:
            ax1.set_xticks((10.0, 50.0, 100.0, 500.0, 1000.0, 3000.0))
        elif np.max(phaserms_results['bllen']) > 500.0:
            ax1.set_xticks((10.0, 50.0, 100.0, 300.0, 500.0, 700.0))
        elif np.max(phaserms_results['bllen']) > 100.0:
            ax1.set_xticks((10.0, 30.0, 50.0, 70.0, 100.0, 300.0))
        else: # ACA should default to this 
            ax1.set_xticks((10.0, 20.0, 30.0, 50.0, 70.0, 90.0, 100.0))

        # Line annotate for the 30, 50, and 70 degree 'limits'
        ax1.annotate('30deg RMS limit', xy=(np.min(phaserms_results['bllen'])/2.0, 27), xycoords='data')
        ax1.annotate('50deg RMS limit', xy=(np.min(phaserms_results['bllen'])/2.0, 44.5), xycoords='data')
        ax1.annotate('70deg RMS limit', xy=(np.min(phaserms_results['bllen'])/2.0, 61.5), xycoords='data')

        if phaserms_results['bllenbad'] is not None:
            # Plot Outliers:
            # should over plot on the plot, i.e cover the full plot already - white them out
            ax1.plot(phaserms_results['bllenbad'],phaserms_results['blphasermsbad'],linestyle='',marker='o',c='w',zorder=3)
            ax1.plot(phaserms_results['bllenbad'],phaserms_results['blphasermscyclebad'],linestyle='',marker='o',c='w',zorder=4)
            # over plot outliers as shade
            ax1.plot(phaserms_results['bllenbad'],phaserms_results['blphasermsbad'],linestyle='',marker='o',c='0.6',zorder=5, 
                     alpha=0.1, label='Total-time (outlier)')
            ax1.plot(phaserms_results['bllenbad'],phaserms_results['blphasermscyclebad'],linestyle='',marker='o',c='r',zorder=6,
                     alpha=0.1, label='Cycle-time (outlier)') 

        # Calc max and min
        phaseRMSmax = np.max([np.max(phaserms_results['blphasermscycle'][np.isfinite(phaserms_results['blphasermscycle'])]),
                              np.max(phaserms_results['blphaserms'][np.isfinite(phaserms_results['blphaserms'])])])
        phaseRMSmin = np.min([np.min(phaserms_results['blphasermscycle'][np.isfinite(phaserms_results['blphasermscycle'])]),
                              np.min(phaserms_results['blphaserms'][np.isfinite(phaserms_results['blphaserms'])])])

        # Make limit at least 35 on plot for green data
        if phaseRMSmax < 35:
            phaseRMSmax = 35.0
            if phaseRMSmin > 3:
                phaseRMSmin = 2.0
        ax1.grid(True)

        # crude logic here as log-log plots have some issue when there are < 9 tick markers
        # if this happens, matplotlib is adding its own extra (minor) tick markers with wrong formatting
        # e.g. if the plot range is 20 to 100, there are only 9 markers, so the set_ytick is not obeyed
        # se requested ax1.set_yticks([10.0,20.0,30.0,50.0,70.0, 100.0, 300.0])
        # but we get sci format ticks also at 40 and 60
        # hence below we find the range of total ticks and if this is < 10 we
        # Extend the plot max range
        plotrange = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 200, 300, 400, 500]
        idplot = [id for id, val in enumerate(plotrange) if phaseRMSmin < val and phaseRMSmax > val]
        indexplt = 10 - len(idplot) + np.max(idplot)
        
        if len(idplot) < 10 and indexplt < len(plotrange) - 1:  # i.e. if we cannot adjust further than plotrange, we don't
            phaseRMSmax = plotrange[indexplt]
        
        title = "Spatial structure function: Execution Block {} \n SPW {} Correlation X        All Unflagged Antennas     Bandpass: {}      Scan {}".format(self.vis, self.spw, self.field, self.scan)
        ax1.set_title(title, fontsize=10)

        ax1.set_ylabel('Phase RMS (deg)')
        ax1.set_xlabel('Baseline Length (m)')

        ax1.set_xlim(np.min(phaserms_results['bllen'])/2.0, np.max(phaserms_results['bllen']) * 1.1)
        ax1.set_ylim(phaseRMSmin * 0.9, phaseRMSmax * 1.1) 

        ax1.xaxis.set_major_formatter(ticker.ScalarFormatter())
        ax1.yaxis.set_major_formatter(ticker.ScalarFormatter())
        # In some TBD cases, extra axis ticks are added

        # Upper center is where the box of the legend locator is
        ax1.legend(loc='upper left', bbox_to_anchor=(0.01, -0.085), prop={'size':8}, frameon=False, ncol=3)

        figfile = self.get_figfile()
        plt.savefig(figfile, format='png', dpi=100.0)

    def plot(self):
        plots = []
        plot = self.get_plot_wrapper()
        if plot is not None: 
            plots.append(plot)
        return plots

    def get_figfile(self):
        vis = os.path.basename(self.result.inputs['vis'])
        return os.path.join(self.context.report_dir,
                            'stage%s' % self.result.stage_number,
                            '%s.phase_rms_structure_plot.png' % vis)

    def get_plot_wrapper(self):
        figfile = self.get_figfile()
        vis = os.path.basename(self.result.inputs['vis'])
        wrapper = logger.Plot(figfile,
                              x_axis='Baseline length (m)',
                              y_axis='Phase RMS (deg)',
                              parameters={'vis': vis,
                                          'spw': self.spw,
                                          'scan': self.scan,
                                          'field': self.field,
                                          'desc': 'Baseline length vs. Phase RMS'})

        if not os.path.exists(figfile):
            LOG.trace('Phase RMS structure plot for vis %s not found. Creating new '
                      'plot: %s' % (vis, figfile))
            try:
                self.create_plot()
            except Exception as ex:
                LOG.error('Could not create Phase RMS structure plot for'
                          ' vis %s' % vis)
                LOG.exception(ex)
                # Close figure just in case state is transferred between calls
                plt.clf()
                return None

        return wrapper
