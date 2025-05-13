"""Renderer for hsd_atmcor stage."""
import collections
import glob
import os
import re
from typing import TYPE_CHECKING, Generator, List, Tuple

import pipeline.infrastructure.casa_tools as casa_tools
import pipeline.infrastructure.filenamer as filenamer
import pipeline.infrastructure.logging as logging
import pipeline.infrastructure.renderer.basetemplates as basetemplates
import pipeline.infrastructure.renderer.logger as logger
import pipeline.infrastructure.utils as utils
from pipeline.infrastructure.basetask import ResultsList
from pipeline.infrastructure.launcher import Context

from .display import PlotmsRealVsFreqPlotter

if TYPE_CHECKING:
    from .atmcor import SDATMCorrectionResults

LOG = logging.get_logger(__name__)


ATMHeuristicsTR = collections.namedtuple(
    'ATMHeuristicsTR',
    'msname apply plot atmtype h0 dtem_dh'
)


def construct_heuristics_table_row(results: 'SDATMCorrectionResults', detail_page: str) -> ATMHeuristicsTR:
    """Construct table row for ATM heuristics summary table.

    Args:
        results: SDAtmCorrectionResults instance
        detail_page: relative path to ATM heuristics detail page

    Raises:
        RuntimeError: results doesn't hold model parameters

    Returns:
        table row as ATMHeuristicsTR instance
    """
    vis = os.path.basename(results.inputs['vis'])
    if results.atm_heuristics == 'Y':
        plot = ' '.join([
            f'<a href="{detail_page}"',
            'class="replace"',
            f'data-vis="{vis}">View</a>',
        ])
        atm_model = results.model_list[results.best_model_index]
    else:
        # results.atm_heuristics should be either 'N' or 'Default'
        #
        # If it is 'N', ATM heuristics was skipped, and
        # results.model_list should store a parameters taken from
        # results.task_args.
        #
        # If it is 'Default', ATM heuristics was attempted but failed, and
        # results.model_list should store a set of default parameters.
        #
        # In either case, length of results.model_list should be 1.
        plot = 'N/A'
        if len(results.model_list) == 1:
            atm_model = results.model_list[0]
        else:
            # something went wrong
            raise RuntimeError('model_list should store default model.')

    row = ATMHeuristicsTR(msname=vis,
                          apply=results.atm_heuristics,
                          plot=plot,
                          atmtype=atm_model.atmtype,
                          h0=atm_model.h0,
                          dtem_dh=atm_model.dtem_dh)
    return row


def identify_heuristics_plots(stage_dir: str, results: 'SDATMCorrectionResults') -> List[logger.Plot]:
    """Identify ATM heuristics plots created by SDcalatmcor module.

    Args:
        stage_dir: Weblog directory for hsd_atmcor stage
        results: SDATMCorrectionResults instance

    Returns:
        List of plots. Each plot element is Plot instance.
    """
    if results.atm_heuristics != 'Y' or results.best_model_index == -1:
        # no useful heuristics plots exist, return empty list
        return []

    basename = os.path.basename(results.inputs['vis'])
    heuristics_plots = []
    best_model_index = results.best_model_index
    model_list = results.model_list
    p = fr'{basename}\.field([0-9]+)\.spw([0-9]+)\.model\.([0-9]+)\.png$'
    LOG.info('Collecting ATM heuristics plots')
    LOG.debug(f'figure name pattern: "{p}"')
    pattern = re.compile(p)
    # examine png file names using regex pattern search
    matching_results = map(
        lambda x: pattern.search(x),
        glob.iglob(os.path.join(stage_dir, '*.png'))
    )
    # process only matched png file names
    for m in filter(lambda x: x is not None, matching_results):
        png_file = m.string
        field_id = m.group(1)
        spw_id = m.group(2)
        model_id = int(m.group(3))
        LOG.info(f'Match: field {field_id} spw {spw_id} model {model_id}')
        status = 'Applied' if model_id == best_model_index else 'Discarded'
        model = str(model_list[model_id])
        heuristics_plots.append(
            logger.Plot(
                png_file,
                x_axis='Frequency',
                y_axis='Amplitude',
                field=field_id,
                parameters={
                    'vis': basename,
                    'spw': spw_id,
                    'ant': 'all',
                    'pol': 'XXYY',
                    'model': model,
                    'status': status,
                }
            )
        )
    LOG.debug(f'{heuristics_plots}')
    return heuristics_plots


def iterate_field_spw(vis: str, field_id_list: List[int], spw_id_list: List[int]) -> Generator[Tuple[int, int], None, None]:
    """Yields valid pair of field_id and spw_id.

    Args:
        vis: Name of MS
        field_id_list: List of field ids
        spw_id_list: List of spw ids

    Yields:
        valid (field_id, spw_id) pair
    """
    with casa_tools.MSMDReader(vis) as msmd:
        for spw_id in spw_id_list:
            fields = set(msmd.fieldsforspw(spw_id))
            for field_id in fields.intersection(field_id_list):
                yield field_id, spw_id


class SDATMCorrHeuristicsDetailPlotRenderer(basetemplates.JsonPlotRenderer):
    """Renderer class for ATM heuristics detail plots."""

    def __init__(self, context: Context, result: 'SDATMCorrectionResults', plots: List[logger.Plot]) -> None:
        """
        Construct SDATMCorrHeuristicsDetailPlotRenderer instance.

        Args:
            context: pipeline Context
            result: SDATMCorrectionResults instance
            plots: list of plot objects
        """
        uri = 'hsd_atmcor_heuristics_detail_plots.mako'
        title = 'ATM Heuristics Plots'
        outfile = filenamer.sanitize(f'{title.lower()}.html')
        super().__init__(uri, context, result, plots, title, outfile)

    def update_json_dict(self, d: dict, plot: logger.Plot) -> None:
        """
        Update json dict to add new filters.

        Args:
            d: Json dict for plot
            plot: plot object
        """
        d['status'] = plot.parameters['status']
        d['model'] = plot.parameters['model']


class T2_4MDetailsSingleDishATMCorRenderer(basetemplates.T2_4MDetailsDefaultRenderer):
    """Renderer class for hsd_atmcor stage."""

    def __init__(self, always_rerender=False):
        """Initialize renderer.

        Args:
            always_rerender: Set True to always render the page. Defaults to False.
        """
        uri = 'hsd_atmcor.mako'
        description = 'Apply correction for atmospheric effects'
        super().__init__(uri=uri, description=description, always_rerender=always_rerender)

    def update_mako_context(self,
                            mako_context: dict,
                            pipeline_context: Context,
                            result: ResultsList):
        """Update Mako context.

        Args:
            mako_context (): original Mako context
            pipeline_context (): pipeline context
            result (): ResultsList containing SDATMCorrectionResults

        Raises:
            RuntimeError: given results object is not valid
        """
        super().update_mako_context(mako_context, pipeline_context, result)
        stage_dir = os.path.join(
            pipeline_context.report_dir,
            'stage{}'.format(result.stage_number)
        )
        if not os.path.exists(stage_dir):
            os.mkdir(stage_dir)

        summary_plots = {}
        detail_plots = []
        heuristics_plots = []
        for r in result:
            LOG.info('Rendering result for "%s"', r.inputs['vis'])
            if not hasattr(r, 'atmcor_ms_name'):
                raise RuntimeError('Wrong result object is given.')

            vis = r.inputs['vis']
            atmvis = r.atmcor_ms_name
            ms = pipeline_context.observing_run.get_ms(os.path.basename(vis))
            antenna_ids = sorted([int(a.id) for a in ms.get_antenna()])
            field_ids = sorted([int(f.id) for f in ms.get_fields(intent='TARGET')])
            science_spws = sorted([int(s.id) for s in ms.get_spectral_windows(science_windows_only=True)])
            spw_selection = r.inputs['spw']
            if len(spw_selection) > 0:
                selected_spws = set(map(int, spw_selection.split(','))).intersection(science_spws)
            else:
                selected_spws = science_spws
            selected_spws = sorted(selected_spws)

            plotter = PlotmsRealVsFreqPlotter(
                ms=ms, atmvis=atmvis,
                atmtype=r.task_args['atmtype'], output_dir=stage_dir
            )
            summaries = collections.OrderedDict()
            for field_id, spw_id in iterate_field_spw(vis, field_ids, selected_spws):
                LOG.info(f'field {field_id} spw {spw_id}')
                spw = str(spw_id)
                plotter.set_field(field_id)
                field_name = plotter.original_field_name
                summaries.setdefault(field_name, collections.OrderedDict())
                plotter.set_spw(spw)
                # reset antenna selection
                plotter.set_antenna()
                p = plotter.plot()
                summaries[field_name][spw] = p
                for antenna_id in antenna_ids:
                    plotter.set_antenna(antenna_id)
                    p = plotter.plot()
                    detail_plots.append(p)
            summary_plots[os.path.basename(vis)] = summaries

            # PIPE-1443 ATM heuristics plots
            heuristics_plots.extend(identify_heuristics_plots(stage_dir, r))

        detail_page_title = 'ATM corrected amplitude vs frequency'
        detail_renderer = basetemplates.JsonPlotRenderer(
            'generic_x_vs_y_field_spw_ant_detail_plots.mako',
            pipeline_context,
            result,
            detail_plots,
            detail_page_title,
            filenamer.sanitize(f'{detail_page_title.lower()}.html')
        )

        with detail_renderer.get_file() as fileobj:
            fileobj.write(detail_renderer.render())

        heuristics_renderer = SDATMCorrHeuristicsDetailPlotRenderer(
            pipeline_context,
            result,
            heuristics_plots,
        )

        with heuristics_renderer.get_file() as fileobj:
            fileobj.write(heuristics_renderer.render())

        # PIPE-1443 ATM heuristics table
        heuristics_path = os.path.relpath(heuristics_renderer.path, pipeline_context.report_dir)
        LOG.info(heuristics_path)
        heuristics_summary = [
            construct_heuristics_table_row(r, heuristics_path) for r in result
        ]
        heuristics_table = utils.merge_td_columns(heuristics_summary, num_to_merge=0)

        mako_context.update({
            'summary_plots': summary_plots,
            'detail_page': detail_renderer.path,
            'heuristics_table': heuristics_table,
        })
