"""
T2_4MDetailsSDApplycalRenderer class.

Created on 24 Oct 2014

@author: sjw
"""
import collections
import os
from typing import TYPE_CHECKING, Dict, List, Tuple, Union

import pipeline.domain.measures as measures
import pipeline.h.tasks.applycal.renderer as super_renderer
import pipeline.infrastructure
import pipeline.infrastructure.logging as logging
import pipeline.infrastructure.utils as utils
from pipeline.h.tasks.common import flagging_renderer_utils as flagutils
from pipeline.h.tasks.common.displays import applycal as applycal
from pipeline.infrastructure import casa_tools
from pipeline.infrastructure.launcher import Context
from pipeline.infrastructure.basetask import ResultsList

if TYPE_CHECKING:
    from pipeline.domain.source import Source
    from pipeline.domain.measurementset import MeasurementSet
    from pipeline.h.tasks.applycal.applycal import ApplycalResults
    from pipeline.infrastructure.renderer.logger import Plot

LOG = logging.get_logger(__name__)


class T2_4MDetailsSDApplycalRenderer(super_renderer.T2_4MDetailsApplycalRenderer):
    """SDApplyCal Renderer class for t2_4m."""

    def __init__(self, uri: str='hsd_applycal.mako',
                 description: str='Apply calibrations from context',
                 always_rerender: bool=False):
        """Initialise the class.

        Args:
            uri: template file name. default:'hsd_applycal.mako'
            description : description of the class, default:'Apply calibrations from context'
            always_rerender : rerendering execution flag, default: False
        """
        super(T2_4MDetailsSDApplycalRenderer, self).__init__(
            uri=uri, description=description, always_rerender=always_rerender)

    def update_mako_context(self, ctx: Dict, context: Context, result: ResultsList):
        """Update mako context dict to render.

        Args:
            ctx: mako context dict
            context: pipeline context
            result: list of ApplycalResults
        """
        weblog_dir = os.path.join(context.report_dir,
                                  'stage%s' % result.stage_number)

        # calculate which intents to display in the flagging statistics table
        intents_to_summarise = ['TARGET']  # flagutils.intents_to_summarise(context)
        flag_table_intents = ['TOTAL', 'SCIENCE SPWS']
        flag_table_intents.extend(intents_to_summarise)

        flag_totals = {}
        for r in result:
            if r.inputs['flagsum'] is True:
                flag_totals = utils.dict_merge(flag_totals,
                                               flagutils.flags_for_result(r, context, intents_to_summarise=intents_to_summarise))

        calapps = {}
        for r in result:
            calapps = utils.dict_merge(calapps,
                                       self.calapps_for_result(r))

        caltypes = {}
        for r in result:
            caltypes = utils.dict_merge(caltypes,
                                        self.caltypes_for_result(r))

        filesizes = {}
        for r in result:
            vis = r.inputs['vis']
            ms = context.observing_run.get_ms(vis)
            filesizes[ms.basename] = ms._calc_filesize()

        # return all agents so we get ticks and crosses against each one
        agents = ['before', 'applycal']

        # PIPE-615: Add links to the hif_applycal weblog for viewing each
        # callibrary table (and store all callibrary tables in the weblog
        # directory)
        callib_map = super_renderer.copy_callibrary(result, context.report_dir)

        ctx.update({
            'flags': flag_totals,
            'calapps': calapps,
            'caltypes': caltypes,
            'agents': agents,
            'dirname': weblog_dir,
            'filesizes': filesizes,
            'callib_map': callib_map,
            'flag_table_intents': flag_table_intents
        })

        # CAS-5970: add science target plots to the applycal page
        (science_amp_vs_freq_summary_plots, science_amp_vs_freq_subpages, uv_max) = self.create_single_dish_science_plots(context, result)

        ctx.update({
            'science_amp_vs_freq_plots': science_amp_vs_freq_summary_plots,
            'science_amp_vs_freq_subpages': science_amp_vs_freq_subpages,
            'uv_max': uv_max,
        })

        # members for parent template applycal.mako
        ctx.update({
            'amp_vs_freq_plots': [],
            'phase_vs_freq_plots': [],
            'amp_vs_time_plots': [],
            'amp_vs_uv_plots': [],
            'phase_vs_time_plots': [],
            'corrected_to_antenna1_plots': [],
            'corrected_to_model_vs_uvdist_plots': [],
            'science_amp_vs_uv_plots': [],
            'uv_plots': [],
            'amp_vs_freq_subpages': [],
            'phase_vs_freq_subpages': [],
            'amp_vs_time_subpages': [],
            'amp_vs_uv_subpages': [],
            'phase_vs_time_subpages': [],
            'outliers_path_link': ''
        })

    def create_single_dish_science_plots(self, context: Context, results: ResultsList) \
            -> Tuple[Dict[str, List[List[Union[str, List['Plot']]]]], Dict[str, str], Dict[str, measures.Distance]]:
        """
        Create plots for the science targets.

        MODIFIED for single dish

        Args:
            context: pipeline context
            results: ResultsList instance containing Applycal Results

        Returns:
            Three dictionaries of plot objects, subpage paths, and
            max UV distances for each vis.
        """
        amp_vs_freq_summary_plots = collections.defaultdict(dict)
        max_uvs = collections.defaultdict(dict)

        amp_vs_freq_detail_plots = {}

        # set to determine that this dict is for hsd_applycal.
        # it should be removed in template before rendering
        amp_vs_freq_summary_plots['__hsd_applycal__'] = []

        for result in results:
            vis = os.path.basename(result.inputs['vis'])
            ms = context.observing_run.get_ms(vis)
            max_uvs[vis] = measures.Distance(value=0.0, units=measures.DistanceUnits.METRE)

            amp_vs_freq_summary_plots[vis] = []

            for source in filter(lambda source: 'TARGET' in source.intents, ms.sources):
                if len(source.fields) > 0:
                    source_name = source.fields[0].name
                    plots = self._plot_source(context, result, ms, source)
                    amp_vs_freq_summary_plots[vis].append([source_name, plots])

            if pipeline.infrastructure.generate_detail_plots(result):
                fields = set()
                # scans = ms.get_scans(scan_intent='TARGET')
                # for scan in scans:
                #     fields.update([field.id for field in scan.fields])
                with casa_tools.MSMDReader(result.inputs['vis']) as msmd:
                    fields.update(list(msmd.fieldsforintent("OBSERVE_TARGET#ON_SOURCE")))

                # Science target detail plots. Note that summary plots go onto the
                # detail pages
                plots = self.science_plots_for_result(context,
                                                      result,
                                                      applycal.RealVsFrequencyDetailChart,
                                                      fields, None,
                                                      preserve_coloraxis=True)
                amp_vs_freq_detail_plots[vis] = plots

        # create detail pages
        amp_vs_freq_subpage = None
        for d, plotter_cls in (
                (amp_vs_freq_detail_plots, super_renderer.ApplycalAmpVsFreqPerAntSciencePlotRenderer),):
            if d:
                all_plots = list(utils.flatten([v for v in d.values()]))
                renderer = plotter_cls(context, results, all_plots)
                with renderer.get_file() as fileobj:
                    fileobj.write(renderer.render())
                amp_vs_freq_subpage = renderer.path
        amp_vs_freq_subpages = dict((vis, amp_vs_freq_subpage) for vis in amp_vs_freq_detail_plots.keys())

        return amp_vs_freq_summary_plots, amp_vs_freq_subpages, max_uvs

    def _plot_source(self, context: Context, result: 'ApplycalResults', ms: 'MeasurementSet', source: 'Source') \
            -> List['Plot']:
        """Plot science plots for result.

        Args:
            context : pipeline context
            result : applycal result object
            ms : Measurement Set
            source : target source

        Returns:
            List of Plot object
        """
        brightest_field = super_renderer.get_brightest_field(ms, source)
        plots = self.science_plots_for_result(context,
                                              result,
                                              applycal.RealVsFrequencySummaryChart,
                                              [brightest_field.id], None,
                                              preserve_coloraxis=True)
        for plot in plots:
            plot.parameters['source'] = source
        return plots
