import pipeline.infrastructure.renderer.basetemplates as basetemplates
from pipeline.infrastructure.utils.weblog import plots_to_html

from . import display


class T2_4MDetailsEditimlistRenderer(basetemplates.T2_4MDetailsDefaultRenderer):
    def __init__(self, uri='editimlist.mako', description='Editimlist',
                 always_rerender=False):
        super().__init__(
            uri=uri, description=description, always_rerender=always_rerender)

    def update_mako_context(self, ctx, context, results):

        result = results[0]

        vlass_flagsummary_plots_html = None
        if hasattr(result, 'vlass_flag_stats') and isinstance(result.vlass_flag_stats, dict):
            vlass_flagsummary_plots = display.VlassFlagSummary(context, result).plot()
            if vlass_flagsummary_plots:
                vlass_flagsummary_plots_html = plots_to_html(vlass_flagsummary_plots, report_dir=context.report_dir)[0]

        ctx.update({'vlass_flagsummary_plots_html': vlass_flagsummary_plots_html})

        return ctx
