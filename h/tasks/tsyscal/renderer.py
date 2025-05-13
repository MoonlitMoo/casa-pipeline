"""
Created on 9 Sep 2014

@author: sjw
"""
import collections
import os

import numpy

import pipeline.infrastructure.filenamer as filenamer
import pipeline.infrastructure.logging as logging
import pipeline.infrastructure.renderer.basetemplates as basetemplates
import pipeline.infrastructure.utils as utils
from pipeline.h.tasks.common.displays import tsys as displays
from pipeline.infrastructure import casa_tools

LOG = logging.get_logger(__name__)

TsysStat = collections.namedtuple('TsysScore', 'median rms median_max')
TsysMapTR = collections.namedtuple('TsysMapTR', 'vis tsys science')


class T2_4MDetailsTsyscalRenderer(basetemplates.T2_4MDetailsDefaultRenderer):
    def __init__(self, uri='tsyscal.mako', 
                 description='Calculate Tsys calibration',
                 always_rerender=False):
        super(T2_4MDetailsTsyscalRenderer, self).__init__(
            uri=uri, description=description, always_rerender=always_rerender)

    def update_mako_context(self, mako_context, pipeline_context, results):
        weblog_dir = os.path.join(pipeline_context.report_dir,
                                  'stage%s' % results.stage_number)

        summary_plots = {}
        subpages = {}
        eb_plots = []
        for result in results:
            if not result.final:
                continue

            calapp = result.final[0]
            plotter = displays.TsysSummaryChart(pipeline_context, result, calapp)
            plots = plotter.plot()
            vis = os.path.basename(result.inputs['vis'])
            summary_plots[vis] = plots

            # generate per-antenna plots
            plotter = displays.TsysPerAntennaChart(pipeline_context, result)
            plots = plotter.plot()

            # render per-EB plot detail pages
            renderer = TsyscalPlotRenderer(pipeline_context, result, plots)
            with renderer.get_file() as fileobj:
                fileobj.write(renderer.render())
                # the filename is sanitised - the MS name is not. We need to
                # map MS to sanitised filename for link construction.
                subpages[vis] = renderer.path

            eb_plots.extend(plots)

        # additionally render plots for all EBs in one page
        renderer = TsyscalPlotRenderer(pipeline_context, results, eb_plots)
        with renderer.get_file() as fileobj:
            fileobj.write(renderer.render())
            # .. and we want the subpage links to go to this master page
            for vis in subpages:
                subpages[vis] = renderer.path

        tsysmap = self._get_tsysmap_table_rows(pipeline_context, results)

        mako_context.update({'summary_plots': summary_plots,
                             'summary_subpage': subpages,
                             'tsysmap': tsysmap,
                             'dirname': weblog_dir})

    def _get_tsysmap_table_rows(self, pipeline_context, results):
        rows = []

        for result in results:
            vis = os.path.basename(result.inputs['vis'])
            calto = result.final[0]

            ms = pipeline_context.observing_run.get_ms(vis)
            science_spws = ms.get_spectral_windows(science_windows_only=True)
            science_spw_ids = [spw.id for spw in science_spws]

            sci2tsys = dict((spw, tsys) for (spw, tsys) in enumerate(calto.spwmap)
                            if spw in science_spw_ids 
                            and spw not in result.unmappedspws)

            tsys2sci = collections.defaultdict(list)
            for sci, tsys in sci2tsys.items():
                tsys2sci[tsys].append(sci)

            tsysmap = dict((k, sorted(v)) for k, v in tsys2sci.items())

            for tsys, sci in tsysmap.items():
                tr = TsysMapTR(vis, tsys, ', '.join([str(w) for w in sci]))
                rows.append(tr)

            if result.unmappedspws:
                tr = TsysMapTR(vis, 'Unmapped', ', '.join([str(w) for w in result.unmappedspws]))
                rows.append(tr)

        return utils.merge_td_columns(rows)


class TsyscalPlotRenderer(basetemplates.JsonPlotRenderer):
    def __init__(self, context, result, plots):
        vis = utils.get_vis_from_plots(plots)

        title = 'T<sub>sys</sub> plots for %s' % vis
        outfile = filenamer.sanitize('tsys-%s.html' % vis)

        # need to wrap result in a list to give common implementation for the
        # following code that extracts spwmap and gaintable
        if not isinstance(result, collections.abc.Iterable):
            result = [result]
        self._caltable = {os.path.basename(r.inputs['vis']): r.final[0].gaintable
                          for r in result}
        self._spwmap = {os.path.basename(r.inputs['vis']): r.final[0].spwmap
                        for r in result}

        super(TsyscalPlotRenderer, self).__init__(
                'tsyscal_plots.mako', context, result, plots, title, outfile)

    def update_json_dict(self, d, plot):
        antenna_name = plot.parameters['ant']
        tsys_spw_id = plot.parameters['tsys_spw']
        vis = plot.parameters['vis']
        stat = self.get_stat(vis, tsys_spw_id, antenna_name)

        d.update({'tsys_spw': str(tsys_spw_id),
                  'median': stat.median,
                  'median_max': stat.median_max,
                  'rms': stat.rms})

    def get_stat(self, vis, spw, antenna):
        tsys_spw = self._spwmap[vis][spw]
        with casa_tools.CalAnalysis(self._caltable[vis]) as ca:
            args = {'spw': tsys_spw,
                    'antenna': antenna,
                    'axis': 'TIME',
                    'ap': 'AMPLITUDE'}

            LOG.trace('Retrieving caltable data for %s %s spw %s', vis,
                      antenna, spw)
            ca_result = ca.get(**args)
            return self.get_stat_from_calanalysis(ca_result)

    def get_stat_from_calanalysis(self, ca_result):
        """
        Calculate the median and RMS for a calanalysis result. The argument
        supplied to this function should be a calanalysis result for ONE
        spectral window and ONE antenna only!
        """
        # get the unique timestamps from the calanalysis result
        timestamps = {v['time'] for v in ca_result.values()}
        representative_tsys_per_timestamp = []
        for timestamp in sorted(timestamps):
            # get the dictionary for each timestamp, giving one dictionary per feed
            stats_for_timestamp = [v for v in ca_result.values() if v['time'] == timestamp]
            # get the median Tsys for each feed at this timestamp
            median_per_feed = [numpy.median(v['value']) for v in stats_for_timestamp]
            # use the average of the medians per antenna feed as the typical
            # tsys for this antenna at this timestamp
            representative_tsys_per_timestamp.append(numpy.mean(median_per_feed))

        median = numpy.median(representative_tsys_per_timestamp)
        rms = numpy.std(representative_tsys_per_timestamp)
        median_max = numpy.max(representative_tsys_per_timestamp)

        return TsysStat(median, rms, median_max)


def create_url_fn(root, plots):
    vis_set = {p.parameters['vis'] for p in plots}

    if len(vis_set) == 1:
        return lambda x: filenamer.sanitize('%s-%s.html' % (root, x))
    else:
        return lambda x: filenamer.sanitize('%s-all_data.html' % root)
