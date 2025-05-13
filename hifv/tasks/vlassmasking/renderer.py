import os

import pipeline.infrastructure.logging as logging
import pipeline.infrastructure.renderer.basetemplates as basetemplates

from . import display

LOG = logging.get_logger(__name__)


class T2_4MDetailsVlassmaskingRenderer(basetemplates.T2_4MDetailsDefaultRenderer):
    def __init__(self, uri='vlassmasking.mako',
                 description='Produce a VLASS Mask',
                 always_rerender=False):
        super(T2_4MDetailsVlassmaskingRenderer, self).__init__(uri=uri,
                                                               description=description, always_rerender=always_rerender)

    def get_display_context(self, context, results):
        ctx = super().get_display_context(context, results)

        weblog_dir = os.path.join(context.report_dir, 'stage%s' % results.stage_number)

        summary_plots = {}
        # PIPE-1945: after hifv_vlassmasking got converted into a multi-vis task, the task returns a Result object
        # instead of a ResultList object. However, htmlrenderer.py/T2_4MDetailsRender.render() always tries to
        # wrap the task result into a list. Therefore, we still need to pick the first element from the list here.
        result = results[0]

        plotter = display.MaskSummary(context, result)
        plots = plotter.plot()
        mslist_str = '<br>'.join([os.path.basename(vis) for vis in result.inputs['vis']])
        summary_plots[mslist_str] = plots

        ctx.update({'summary_plots': summary_plots,
                    'dirname': weblog_dir})

        return ctx
