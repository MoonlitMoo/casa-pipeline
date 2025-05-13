import collections
import math
import os
from typing import List, Tuple

import matplotlib.cbook as cbook
import matplotlib.pyplot as plt
import numpy as np
import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.casa_tools as casa_tools
import pipeline.infrastructure.renderer.logger as logger

LOG = infrastructure.get_logger(__name__)


class weightboxChart(object):
    whis = 3.944

    def __init__(self, context, result):
        self.context = context
        self.result = result
        self.ms = context.observing_run.get_ms(result.inputs['vis'])
        self.result.weight_stats = {}
        self.band2spw = None

    @staticmethod
    def _get_weight_from_wtable(tbl, this_ant='', this_spw='', this_scan='', spw_list=[]):
        """Get the weights from the weights table with the specificed ant, spw, and scan. 
        The spw_list parameter only works with only the this_ant specified, and restricts
        the selection of entries in the weights table to those with spws in the spw_list."""

        if (this_ant != '') and (this_spw != '') and (this_scan!=''):
            query_str = 'SPECTRAL_WINDOW_ID=={0} && ANTENNA1=={1} && SCAN_NUMBER=={2} ntrue(FLAG)==0'.format(this_spw, this_ant, this_scan)
        elif (this_ant != '') and (this_spw != ''):
            query_str = 'SPECTRAL_WINDOW_ID=={0} && ANTENNA1=={1} && ntrue(FLAG)==0'.format(this_spw, this_ant)
        elif (this_ant != '') and (this_scan != ''):
            query_str = 'SCAN_NUMBER=={0} && ANTENNA1=={1} && ntrue(FLAG)==0'.format(this_scan, this_ant)
        elif (this_scan != '') and (this_spw != ''):
            query_str = 'SPECTRAL_WINDOW_ID=={0} && SCAN_NUMBER=={1} && ntrue(FLAG)==0'.format(this_spw, this_scan)
        elif (this_ant != '') and spw_list: 
            query_str = 'ANTENNA1=={0} && ntrue(FLAG)==0 && SPECTRAL_WINDOW_ID IN {1}'.format(this_ant, list(map(int, spw_list)))
        elif (this_ant != ''):
            query_str = 'ANTENNA1=={0} && ntrue(FLAG)==0'.format(this_ant)
        elif (this_spw != ''):
            query_str = 'SPECTRAL_WINDOW_ID=={0} && ntrue(FLAG)==0'.format(this_spw)
        elif (this_scan !=''):
            query_str = 'SCAN_NUMBER=={0} && ntrue(FLAG)==0'.format(this_scan)
        else:
            query_str = ''

        with casa_tools.TableReader(tbl) as tb:
            stb = tb.query(query_str)
            weights = stb.getcol('CPARAM').ravel()
            stb.done()

        return weights.real

    # This is used to get the scans for a band by passing in the appropriate list of spws
    # and is only used for the VLA-PI code 
    def _get_scans_with_spws(self, tbl: str, spws: List=[]):
        """Get the scans from the weights table with the spws in the input list"""

        query = 'ntrue(FLAG)==0 && SPECTRAL_WINDOW_ID IN {0}'.format(list(map(int, spws)))
        scans = None        
        with casa_tools.TableReader(tbl) as tb:
            stb = tb.query(query)
            scans = np.sort(np.unique(stb.getcol('SCAN_NUMBER')))
            stb.done()
        return scans

    def plot(self):
        plots = []
        for k, _ in self.result.wtables.items():
            plots.extend(self._get_plot_wrapper(suffix=k))
        return [p for p in plots if p is not None]

    def _split(self, to_split: List, n: int) -> Tuple[List]: 
        """Utility function to divide the input list to_split into sublists of size n and return 
        a tuple of these lists"""
        quotient, remainder = divmod(len(to_split), n)
        return (to_split[i*quotient+min(i, remainder):(i+1)*quotient+min(i+1, remainder)] for i in range(n))


class vlaWeightboxChart(weightboxChart):
    def __init__(self, context, results):
        # A dict where the keys are the band names and the values are lists of the spws for that band
        # ex: {'C':[1,2,3], 'K':[4,5,6]}
        self.band2spw = None
        super().__init__(context, results)

    def _get_figfile(self, suffix, band=''):
        stage_dir = os.path.join(self.context.report_dir, 'stage{}'.format(self.result.stage_number))
        if not os.path.exists(stage_dir):
            os.mkdir(stage_dir)
        fig_basename = '-'.join(list(filter(None, ['statwt',
                                            self.ms.basename, 'summary', band, suffix])))+'.png'
        return os.path.join(stage_dir, fig_basename)

    def _get_plot_wrapper(self, suffix:str='', band:str='') -> List[logger.Plot]:
        figfile = self._get_figfile(suffix, band)
        wrappers = []

        if not os.path.exists(figfile):
            LOG.trace('Statwt summary plot not found. Creating new plot.')
            try:
                bands = self._create_plot_from_wtable(suffix)
                for band in bands:
                    figfile = self._get_figfile(suffix, band)
                    wrapper = logger.Plot(figfile, x_axis='antenna or spectral window', y_axis='antenna-based weight',
                              parameters={'vis': self.ms.basename,
                                          'x_axis': 'ant/spw/scan',
                                          'y_axis': 'weight',
                                          'type': suffix, 
                                          'band': band})
                    wrappers.append(wrapper)
                
            except Exception as ex:
                LOG.error('Could not create ' + suffix + ' plot.')
                LOG.exception(ex)
                return None
        return wrappers

    def _create_plot_from_wtable(self, suffix):
        """Create plots for each band"""
        tbl = self.result.wtables[suffix]

        with casa_tools.TableReader(tbl) as tb:
            spws = np.sort(np.unique(tb.getcol('SPECTRAL_WINDOW_ID')))
            scans = np.sort(np.unique(tb.getcol('SCAN_NUMBER')))

        with casa_tools.TableReader(tbl+'/ANTENNA') as tb:
            ant_names = tb.getcol('NAME')
            ant_idxs = range(len(ant_names))

        # VLA PI plots are separated out by band, so determine the spws 
        # for each band.  
        spw2band = self.ms.get_vla_spw2band() # Format: {'1':'C', '2':'K' }
        band2spw = collections.defaultdict(list)
        listspws = [spw for spw in spws]
        for spw, band in spw2band.items():
            if spw in listspws:
                band2spw[band].append(str(spw))
        self.band2spw = band2spw  # Format: {'C':[1,2,3], 'K':[4,5,6]}

        bands_return = []
        for band in self.band2spw: 
            figfile = self._get_figfile(suffix, band)
            LOG.info('Making antenna-based weight plot: {0} for band: {1}'.format(figfile, band))
            bxpstats_per_ant = list()
            for this_ant in ant_idxs:
                dat = self._get_weight_from_wtable(tbl, this_ant=this_ant, spw_list=self.band2spw[band])
                if dat.size > 0:
                    dat = dat[dat > 0]
                    bxpstats = cbook.boxplot_stats(dat, whis=self.whis)
                    bxpstats[0]['quartiles'] = np.percentile(dat, [0, 25, 50, 75, 100])
                    bxpstats[0]['stdev'] = dat.std()
                    bxpstats[0]['min'] = np.min(dat)
                    bxpstats[0]['max'] = np.max(dat)
                    bxpstats_per_ant.extend(bxpstats)
                else:
                    bxpstats = cbook.boxplot_stats([0], whis=self.whis)
                    bxpstats[0]['quartiles'] = None
                    bxpstats[0]['stdev'] = None
                    bxpstats[0]['min'] = None
                    bxpstats[0]['max'] = None
                    bxpstats_per_ant.extend(bxpstats)
                bxpstats_per_ant[-1]['ant'] = ant_names[this_ant]

            bxpstats_per_spw = list()
            for this_spw in self.band2spw[band]:
                dat = self._get_weight_from_wtable(tbl, this_spw=this_spw)
                if dat.size > 0:
                    dat = dat[dat > 0]
                    bxpstats = cbook.boxplot_stats(dat, whis=self.whis)
                    bxpstats[0]['quartiles'] = np.percentile(dat, [0, 25, 50, 75, 100])
                    bxpstats[0]['stdev'] = dat.std()
                    bxpstats[0]['min'] = np.min(dat)
                    bxpstats[0]['max'] = np.max(dat)
                    bxpstats_per_spw.extend(bxpstats)
                else:
                    bxpstats = cbook.boxplot_stats([0], whis=self.whis)
                    bxpstats[0]['quartiles'] = None
                    bxpstats[0]['stdev'] = None
                    bxpstats[0]['min'] = None
                    bxpstats[0]['max'] = None
                    bxpstats_per_spw.extend(bxpstats)
                bxpstats_per_spw[-1]['spw'] = this_spw
           
            scans = self._get_scans_with_spws(tbl, self.band2spw[band])

            bxpstats_per_scan = list()
            for this_scan in scans: 
                dat = self._get_weight_from_wtable(tbl, this_scan=this_scan)
                if dat.size > 0:
                    dat = dat[dat > 0]
                    bxpstats = cbook.boxplot_stats(dat, whis=self.whis)
                    bxpstats[0]['quartiles'] = np.percentile(dat, [0, 25, 50, 75, 100])
                    bxpstats[0]['stdev'] = dat.std()
                    bxpstats[0]['min'] = np.min(dat)
                    bxpstats[0]['max'] = np.max(dat)
                    bxpstats_per_scan.extend(bxpstats)
                else:
                    bxpstats = cbook.boxplot_stats([0], whis=self.whis)
                    bxpstats[0]['quartiles'] = None
                    bxpstats[0]['stdev'] = None
                    bxpstats[0]['min'] = None
                    bxpstats[0]['max'] = None
                    bxpstats_per_scan.extend(bxpstats)
                bxpstats_per_scan[-1]['scan'] = this_scan

            # Setup plot sizes and number of plots

            # Scan plots are split into subplots every 75 scans for readability
            max_scans_per_plot = 75 
            number_of_scan_plots = math.ceil(len(scans)/max_scans_per_plot)
            number_of_plots = number_of_scan_plots + 2 # There is always one spw plot and one antenna plot.
            plot_len = 15
            plot_height = number_of_plots * 2
            fig, subplots = plt.subplots(number_of_plots, 1, figsize=(plot_len, plot_height))

            if number_of_scan_plots == 1:
                ax1, ax2, ax3 = subplots
            else: 
                ax1 = subplots[0]
                ax2 = subplots[1]
                ax_scans = subplots[2:]

            flierprops = dict(marker='+', markerfacecolor='royalblue', markeredgecolor='royalblue')

            # Create per-antenna plots
            ax1.bxp(bxpstats_per_ant, flierprops=flierprops)
            ax1.axes.set_xticklabels(ant_names, rotation=45, ha='center')
            ax1.set_ylabel('$Wt_{i}$')
            ax1.set_title('Antenna-based weights, {}-band'.format(band))
            ax1.get_yaxis().get_major_formatter().set_useOffset(False)

            # Create per-spw plots
            ax2.bxp(bxpstats_per_spw, flierprops=flierprops)
            ax2.axes.set_xticklabels(self.band2spw[band], rotation=45, ha='center')
            ax2.set_xlabel('SPW ID')
            ax2.set_ylabel('$Wt_{i}$')
            ax2.get_yaxis().get_major_formatter().set_useOffset(False)
            
            # Save-off y-axis limits so they can be re-used for the scans plots. 
            # Since the scan plost can be split up if there are more than max_scans_per_plot scans, 
            # this is used to keep the y-axes consistent.
            y_min, y_max = ax2.get_ylim()

            # Create per-scan sub-plots
            if number_of_scan_plots == 1: 
                ax3.bxp(bxpstats_per_scan, flierprops=flierprops)
                ax3.axes.set_xticklabels(scans, rotation=45, ha='center')
                ax3.set_xlabel('Scan Number')
                ax3.set_ylabel('$Wt_{i}$')
                ax3.get_yaxis().get_major_formatter().set_useOffset(False)
            else: 
                bxpstats_per_scan_split = list(self._split(bxpstats_per_scan, number_of_scan_plots))
                scans_split = list(self._split(scans, number_of_scan_plots))
                for i, axis in enumerate(ax_scans): 
                    axis.bxp(bxpstats_per_scan_split[i], flierprops=flierprops)
                    axis.axes.set_xticklabels(scans_split[i], rotation=45, ha='center')
                    axis.set_xlabel('Scan Number')
                    axis.set_ylabel('$Wt_{i}$')
                    axis.set_ylim([y_min, y_max])
                    axis.get_yaxis().get_major_formatter().set_useOffset(False) 

            fig.tight_layout()
            fig.savefig(figfile)
            plt.close(fig)

            if not self.result.weight_stats:
                self.result.weight_stats[suffix] = {}
            self.result.weight_stats[suffix][band] =  {'per_spw': bxpstats_per_spw,
                                                       'per_ant': bxpstats_per_ant,
                                                       'per_scan': bxpstats_per_scan}
            bands_return.append(band)
                                                    
        return bands_return


class vlassWeightboxChart(weightboxChart):
    def __init__(self, context, results):
        super().__init__(context, results)

    def _get_figfile(self, suffix):
        stage_dir = os.path.join(self.context.report_dir, 'stage{}'.format(self.result.stage_number))
        if not os.path.exists(stage_dir):
            os.mkdir(stage_dir)
        fig_basename = '-'.join(list(filter(None, ['statwt',
                                           self.ms.basename, 'summary', suffix])))+'.png'
        
        return os.path.join(stage_dir, fig_basename)

    def _get_plot_wrapper(self, suffix:str='') -> List[logger.Plot]:
        figfile = self._get_figfile(suffix)
        wrappers = []

        if self.result.inputs['statwtmode'] == "VLASS-SE": 
            wrapper = logger.Plot(figfile, x_axis='antenna or spectral window', y_axis='antenna-based weight',
                    parameters={'vis': self.ms.basename,
                                'x_axis': 'ant/spw',
                                'y_axis': 'weight',
                                'type': suffix})
            wrappers.append(wrapper)

        if not os.path.exists(figfile):
            LOG.trace('Statwt summary plot not found. Creating new plot.')
            try:
                self._create_plot_from_wtable(suffix)
            except Exception as ex:
                LOG.error('Could not create ' + suffix + ' plot.')
                LOG.exception(ex)
                return None
        return wrappers

    def  _create_plot_from_wtable(self, suffix):
        tbl = self.result.wtables[suffix]

        with casa_tools.TableReader(tbl) as tb:
            spws = np.sort(np.unique(tb.getcol('SPECTRAL_WINDOW_ID')))

        with casa_tools.TableReader(tbl+'/ANTENNA') as tb:
            ant_names = tb.getcol('NAME')
            ant_idxs = range(len(ant_names))

        figfile = self._get_figfile(suffix)

        bxpstats_per_ant = list()
        for this_ant in ant_idxs:
            dat = self._get_weight_from_wtable(tbl, this_ant=this_ant)
            if dat.size > 0:
                dat = dat[dat > 0]
                bxpstats = cbook.boxplot_stats(dat, whis=self.whis)
                bxpstats[0]['quartiles'] = np.percentile(dat, [0, 25, 50, 75, 100])
                bxpstats[0]['stdev'] = dat.std()
                bxpstats_per_ant.extend(bxpstats)
            else:
                bxpstats = cbook.boxplot_stats([0], whis=self.whis)
                bxpstats[0]['quartiles'] = None
                bxpstats[0]['stdev'] = None
                bxpstats_per_ant.extend(bxpstats)
            bxpstats_per_ant[-1]['ant'] = ant_names[this_ant]

        bxpstats_per_spw = list()
        for this_spw in spws:
            dat = self._get_weight_from_wtable(tbl, this_spw=this_spw)
            if dat.size > 0:
                dat = dat[dat > 0]
                bxpstats = cbook.boxplot_stats(dat, whis=self.whis)
                bxpstats[0]['quartiles'] = np.percentile(dat, [0, 25, 50, 75, 100])
                bxpstats[0]['stdev'] = dat.std()
                bxpstats_per_spw.extend(bxpstats)
            else:
                bxpstats = cbook.boxplot_stats([0], whis=self.whis)
                bxpstats[0]['quartiles'] = None
                bxpstats[0]['stdev'] = None
                bxpstats_per_spw.extend(bxpstats)
            bxpstats_per_spw[-1]['spw'] = this_spw

        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(10, 6))
        flierprops = dict(marker='+', markerfacecolor='royalblue', markeredgecolor='royalblue')

        ax1.bxp(bxpstats_per_ant, flierprops=flierprops)
        ax1.axes.set_xticklabels(ant_names, rotation=45, ha='center')
        ax1.set_ylabel('$Wt_{i}$')
        ax1.set_title('Antenna-based weights')
        ax1.get_yaxis().get_major_formatter().set_useOffset(False)

        ax2.bxp(bxpstats_per_spw, flierprops=flierprops)
        ax2.axes.set_xticklabels(spws)
        ax2.set_xlabel('SPW ID')
        ax2.set_ylabel('$Wt_{i}$')
        ax2.get_yaxis().get_major_formatter().set_useOffset(False)

        fig.tight_layout()
        fig.savefig(figfile)
        plt.close(fig)

        self.result.weight_stats[suffix] = {'per_spw': bxpstats_per_spw,
                                            'per_ant': bxpstats_per_ant}
        return

