import collections
import os
import numpy as np

from matplotlib.pyplot import cm
import matplotlib.colors as colors
import pipeline.infrastructure.exceptions as exceptions
import pipeline.infrastructure.renderer.basetemplates as basetemplates

from . import display as statwtdisplay

class T2_4MDetailsstatwtRenderer(basetemplates.T2_4MDetailsDefaultRenderer):
    def __init__(self, uri='statwt.mako', description='Statwt summary',
                 always_rerender=False):
        super(T2_4MDetailsstatwtRenderer, self).__init__(uri=uri, description=description,
                                                         always_rerender=always_rerender)

    def get_display_context(self, context, results):
        super_cls = super(T2_4MDetailsstatwtRenderer, self)
        ctx = super_cls.get_display_context(context, results)

        weblog_dir = os.path.join(context.report_dir, 'stage%s' % results.stage_number)
        summary_plots = {}
        plotter = None
        ant_table_rows = None
        spw_table_rows = None
        scan_table_rows = None

        for result in results:

            if result.inputs['statwtmode'] == 'VLASS-SE':
                weightboxChart = statwtdisplay.vlassWeightboxChart
            else:
                weightboxChart = statwtdisplay.vlaWeightboxChart

            plotter = weightboxChart(context, result)
            plots = plotter.plot()
            ms = os.path.basename(result.inputs['vis'])

            # Only VLASS-SE has 'before' and 'after' available
            if result.inputs['statwtmode'] == 'VLASS-SE':
                summary_plots[ms] = plots
                is_same = True
                try:
                    weight_stats = plotter.result.weight_stats
                    before_by_ant = weight_stats['before']['per_ant']
                    after_by_ant = weight_stats['after']['per_ant']

                    for idx in range(before_by_ant):
                        is_same &= before_by_ant[idx]['mean'] == after_by_ant[idx]['mean']
                        is_same &= before_by_ant[idx]['med'] == after_by_ant[idx]['med']
                        is_same &= before_by_ant[idx]['stdev'] == after_by_ant[idx]['stdev']
                    
                except:
                    is_same = False

                if is_same:
                    raise exceptions.PipelineException("Statwt failed to recalculate the weights, cannot continue.")
            else:
                # VLA PI has per-band plots and tables for only "after"
                bands = plotter.band2spw.keys()

                scan_table_rows = collections.defaultdict(list)
                ant_table_rows = collections.defaultdict(list)
                spw_table_rows = collections.defaultdict(list)

                for band in bands: 
                    summary_plots[band] = collections.defaultdict(list)

                    after_by_scan=plotter.result.weight_stats['after'][band]['per_scan']
                    scan_table_rows[band] = self.make_stats_table(after_by_scan, table_type='scan')

                    after_by_ant=plotter.result.weight_stats['after'][band]['per_ant']
                    ant_table_rows[band] = self.make_stats_table(after_by_ant, table_type='ant')
                    
                    after_by_spw=plotter.result.weight_stats['after'][band]['per_spw']
                    spw_table_rows[band] = self.make_stats_table(after_by_spw, table_type='spw')

                for plot in plots: 
                    band = plot.parameters['band']
                    summary_plots[band][ms].append(plot)

        ctx.update({'summary_plots': summary_plots,
                    'plotter': plotter,
                    'dirname': weblog_dir,
                    'ant_table_rows': ant_table_rows, # only populated for VLA-PI
                    'spw_table_rows': spw_table_rows, # only populated for VLA-PI
                    'scan_table_rows': scan_table_rows, # only populated for VLA-PI
                    'band2spw': plotter.band2spw}) # only populated for VLA-PI

        return ctx


    def summarize_stats(self, input_stats):
        summary = collections.defaultdict(list)
        for i, row in enumerate(input_stats):
            for stat in row:
                val = input_stats[i][stat]
                summary[stat].append(val)
        return summary


    def format_cell(self, whole, value, stat):
        if (value is None) or (whole is None) or (stat is None) or (value == 'N/A'):
            return ''
        else:
            summary = np.array(whole[stat], dtype=float)

            # When the table column has only one or zero entires, it doesn't make sense to compare
            # it to "the other values in the table"
            if len(summary) <= 1:
                return ''

            median = np.nanmedian(summary)
            sigma = 1.4826 * np.nanmedian(np.abs(summary - median))
            dev = abs(float(value)) - median
            # PIPE-1737: returning background color for non zero values.
            if abs(dev) > sigma*3.0 and float(value) > 0:
                bgcolor = dev2shade(dev/sigma, float(value) > median)
                return f'style="background-color: {bgcolor}"'
            else:
                return ''

    StatsTR = collections.namedtuple('StatsTR', 'index median q1 q2 mean stdev min max')

    def make_stats_table(self, weight_stats, table_type='scan'):
        summary_stats = self.summarize_stats(weight_stats)
        rows = []
        for i in range(len(weight_stats)):
            median = weight_stats[i]['med']
            q1 = weight_stats[i]['q1']
            q3 = weight_stats[i]['q3']
            mean = weight_stats[i]['mean']
            std = weight_stats[i]['stdev']
            min = weight_stats[i]['min']
            max = weight_stats[i]['max']
            tr = self.StatsTR(weight_stats[i][table_type], median, q1, q3, mean, std, min, max)
            tds = self.make_shaded_tds(tr, summary_stats)
            rows.append(tds)
        return rows
    
    def make_shaded_tds(self, tr, summary_stats): 
        """Takes a StatsTR and shades and formats it"""        
        to_return = []
        for i, elt in enumerate(tr):
            if i == 0: 
                to_return.append(elt)
            else: 
                val = format_wt(elt)
                format = self.format_cell(summary_stats, val, tr._fields[i])
                formatted = "<td {0}>{1}</td>".format(format, val)
                to_return.append(formatted)
        return to_return


# Note: this is copied and slightly modified from Rui's verision in hif_makecutoutimages 
# This is a potential candidate for refactoring out into a common location
# in the future.
def dev2shade(x, above_median=True):
    absx=abs(x)
    if above_median: 
        cmap=cm.get_cmap(name='Reds')
    else: 
        cmap=cm.get_cmap(name='Blues')
    if absx<4 and absx>=3:
        rgb_hex=colors.to_hex(cmap(0.2))
    elif absx<5 and absx>=4:
        rgb_hex=colors.to_hex(cmap(0.3))
    elif absx<6 and absx>=5:
        rgb_hex=colors.to_hex(cmap(0.4))
    elif absx>=6:
        rgb_hex=colors.to_hex(cmap(0.5))
    else: 
        rgb_hex=colors.to_hex(cmap(0.1))
    return rgb_hex 


def format_wt(wt):
    if wt is None:
        return 'N/A'
    else:
        return np.format_float_positional(wt, precision=4, fractional=False, trim='-')