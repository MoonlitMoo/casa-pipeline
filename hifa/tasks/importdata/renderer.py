"""
Created on 5 Sep 2014

@author: sjw
"""
import os

import pipeline.infrastructure.logging as logging
import pipeline.infrastructure.renderer.logger as logger
from pipeline.h.tasks.importdata.renderer import T2_4MDetailsImportDataRenderer
from pipeline.infrastructure import casa_tasks
from pipeline.infrastructure.basetask import Executor
from pipeline.infrastructure.filenamer import sanitize
from pipeline.infrastructure.renderer.rendererutils import get_relative_url

LOG = logging.get_logger(__name__)


class T2_4MDetailsALMAImportDataRenderer(T2_4MDetailsImportDataRenderer):
    def __init__(self, uri='almaimportdata.mako',
                 description='Register measurement sets with the pipeline',
                 always_rerender=False):
        super().__init__(uri=uri, description=description, always_rerender=always_rerender)

    def update_mako_context(self, mako_context, pipeline_context, result):
        super().update_mako_context(mako_context, pipeline_context, result)

        minparang = result.inputs['minparang']
        parang_ranges = result.parang_ranges
        if parang_ranges['pol_intents_found']:
            parang_plots = make_parang_plots(pipeline_context, result)
        else:
            parang_plots = {}

        mako_context.update({
            'minparang': minparang,
            'parang_ranges': parang_ranges,
            'parang_plots': parang_plots,
        })


def make_parang_plots(context, result):
    """
    Create parallactic angle plots for each session.
    """
    plot_colors = ['0000ff', '007f00', 'ff0000', '00bfbf', 'bf00bf', '3f3f3f',
                   'bf3f3f', '3f3fbf', 'ffbfbf', '00ff00', 'c1912b', '89a038',
                   '5691ea', 'ff1999', 'b2ffb2', '197c77', 'a856a5', 'fc683a']

    intent_to_plot = 'CALIBRATE_POLARIZATION#ON_SOURCE'
    parang_plots = {}
    stage_id = 'stage{}'.format(result.stage_number)
    ous_id = context.project_structure.ousstatus_entity_id
    sessions = result.parang_ranges['sessions']
    for session_name in sessions:
        sanitised_filename_component = sanitize(f'{ous_id}_{session_name}')
        plot_name = os.path.join(context.report_dir, stage_id, f'{sanitised_filename_component}_parallactic_angle.png')
        # translate uid://A123/X12... to uid___A123_X12
        plot_title = 'MOUS {}, session {}'.format(ous_id, session_name)
        num_ms = len(sessions[session_name]['vis'])
        clearplots = True
        for i, msname in enumerate(sessions[session_name]['vis']):
            symbolcolor = plot_colors[i % len(plot_colors)]
            science_spws = context.observing_run.get_ms(msname).get_spectral_windows()
            # Specify center channels of science spws
            spwspec = ','.join('{}:{}'.format(s.id, s.num_channels//2) for s in science_spws)
            task_args = {
                'vis': msname,
                'plotfile': '',
                'xaxis': 'time',
                'yaxis': 'parang',
                'customsymbol': True,
                'symbolcolor': symbolcolor,
                'title': plot_title,
                'spw': spwspec,
                'plotrange': [0, 0, 0, 360],
                'plotindex': i,
                'clearplots': clearplots,
                'intent': intent_to_plot,
                'showgui': False
                }

            if i == num_ms-1:
                task_args['plotfile'] = plot_name

            task = casa_tasks.plotms(**task_args)
            Executor(context).execute(task)

            clearplots = False

        parang_plots[session_name] = {}
        parang_plots[session_name]['name'] = plot_name

        # create a plot object so we can access (thus generate) the thumbnail
        plot_obj = logger.Plot(plot_name)

        fullsize_relpath = get_relative_url(context.report_dir, stage_id, plot_name)
        thumbnail_relpath = os.path.relpath(plot_obj.thumbnail, os.path.abspath(context.report_dir))
        title = 'Parallactic angle coverage for session {}'.format(session_name)

        html_args = {
            'fullsize': fullsize_relpath,
            'thumbnail': thumbnail_relpath,
            'title': title,
            'alt': title,
            'rel': 'parallactic-angle-plots'
        }

        html = ('<a href="{fullsize}"'
                '   title="{title}"'
                '   data-fancybox="{rel}"'
                '   data-caption="{title}">'
                '    <img data-src="{thumbnail}"'
                '         title="{title}"'
                '         alt="{alt}"'
                '         class="lazyload img-responsive">'
                '</a>'.format(**html_args))

        parang_plots[session_name]['html'] = html

    return parang_plots
