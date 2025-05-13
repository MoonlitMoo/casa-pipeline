import collections
import os
from typing import List, Union

import pipeline.infrastructure.logging as logging
import pipeline.infrastructure.renderer.basetemplates as basetemplates
import pipeline.infrastructure.utils as utils
from pipeline.hifa.tasks.gaincal import display as gaincal_displays
from pipeline.hifa.tasks.gaincal import renderer as gaincal_renderer
from pipeline.infrastructure import generate_detail_plots
from pipeline.infrastructure.launcher import Context
from pipeline.infrastructure.basetask import ResultsList

LOG = logging.get_logger(__name__)

DiffGainApplication = collections.namedtuple('DiffGainApplication', 'ms gaintable calmode solint intent spw')


class T2_4MDetailsDiffgaincalRenderer(basetemplates.T2_4MDetailsDefaultRenderer):
    """
    Renders detailed HTML output for the Diffgaincal task.
    """
    def __init__(self, uri='diffgaincal.mako',
                 description='Differential Gain Calibration',
                 always_rerender=False):
        super().__init__(uri=uri, description=description, always_rerender=always_rerender)

    def update_mako_context(self, mako_context, pipeline_context, results):
        # Get info on phase diffgain caltable.
        applications = get_diffgain_applications(pipeline_context, results)

        # Get plots.
        phase_vs_time_summaries, phase_vs_time_subpages = get_plots(
            pipeline_context, results, gaincal_displays.GaincalPhaseVsTimeSummaryChart,
            gaincal_displays.GaincalPhaseVsTimeDetailChart, gaincal_renderer.GaincalPhaseVsTimePlotRenderer)

        offset_results = [result.phaseoffsetresult for result in results]
        offset_vs_time_summaries, offset_vs_time_subpages = get_plots(
            pipeline_context, offset_results, gaincal_displays.GaincalPhaseVsTimeSummaryChart,
            gaincal_displays.GaincalPhaseVsTimeDetailChart,
            gaincal_renderer.GaincalPhaseOffsetVsTimeDiagnosticPlotRenderer)

        # Update mako context.
        mako_context.update({
            'applications': applications,
            'offset_vs_time_plots': offset_vs_time_summaries,
            'offset_vs_time_subpages': offset_vs_time_subpages,
            'phase_vs_time_plots': phase_vs_time_summaries,
            'phase_vs_time_subpages': phase_vs_time_subpages,
        })


def get_diffgain_applications(context: Context, results: ResultsList) -> List[DiffGainApplication]:
    calmode_map = {
        'p': 'Phase only',
        'a': 'Amplitude only',
        'ap': 'Phase and amplitude'
    }

    applications = []

    for result in results:
        ms = context.observing_run.get_ms(result.vis)

        for calapp in result.final:
            gaintable = os.path.basename(calapp.gaintable)

            calmode = utils.get_origin_input_arg(calapp, 'calmode')
            calmode = calmode_map.get(calmode, calmode)

            solint = utils.get_origin_input_arg(calapp, 'solint')
            if solint == 'inf':
                solint = 'Infinite'

            to_intent = ', '.join(calapp.intent.split(','))
            if to_intent == '':
                to_intent = 'ALL'

            to_spw = ', '.join(calapp.spw.split(','))

            applications.append(DiffGainApplication(ms.basename, gaintable, calmode, solint, to_intent, to_spw))

    return applications


def get_plots(context: Context, results: Union[List, ResultsList], summary_plot_cls, detail_plot_cls, renderer_cls):
    summaries = {}
    subpages = {}
    details = {}

    for result in results:
        vis = os.path.basename(result.inputs['vis'])

        # Create summary plots.
        plotter = summary_plot_cls(context, result, result.final, '')
        summaries[vis] = plotter.plot()

        # Generate detailed plots and corresponding subpage renderers.
        if generate_detail_plots(result):
            # Create detailed plots.
            plotter = detail_plot_cls(context, result, result.final, '')
            details[vis] = plotter.plot()

            # Render subpage for detailed plots.
            renderer = renderer_cls(context, result, details[vis])
            with renderer.get_file() as fileobj:
                fileobj.write(renderer.render())
                subpages[vis] = renderer.path

    # Render the subpages for all results in a single page.
    if details:
        all_plots = list(utils.flatten([v for v in details.values()]))
        renderer = renderer_cls(context, results, all_plots)
        with renderer.get_file() as fileobj:
            fileobj.write(renderer.render())
        # Redirect subpage links to the new single page.
        for vis in subpages:
            subpages[vis] = renderer.path

    return summaries, subpages
