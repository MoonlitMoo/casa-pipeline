import contextlib
import os

import pipeline.infrastructure.filenamer as filenamer
import pipeline.infrastructure.logging as logging
import pipeline.infrastructure.renderer.basetemplates as basetemplates
import pipeline.infrastructure.renderer.weblog as weblog

from . import display as selfcaldisplay

LOG = logging.get_logger(__name__)


class VLASubPlotRenderer(object):
    #template = 'testdelays_plots.html'

    def __init__(self, context, result, plots, json_path, template, filename_prefix):
        self.context = context
        self.result = result
        self.plots = plots
        self.ms = os.path.basename(self.result.inputs['vis'])
        self.template = template
        self.filename_prefix = filename_prefix

        self.summary_plots = {}
        self.selfcalphasegaincal_subpages = {}

        self.selfcalphasegaincal_subpages[self.ms] = filenamer.sanitize('selfcalphasegaincal' + '-%s.html' % self.ms)

        if os.path.exists(json_path):
            with open(json_path, 'r') as json_file:
                self.json = json_file.readlines()[0]
        else:
            self.json = '{}'

    def _get_display_context(self):
        return {'pcontext': self.context,
                'result': self.result,
                'plots': self.plots,
                'dirname': self.dirname,
                'json': self.json,
                'selfcalphasegaincal_subpages': self.selfcalphasegaincal_subpages}

    @property
    def dirname(self):
        stage = 'stage%s' % self.result.stage_number
        return os.path.join(self.context.report_dir, stage)

    @property
    def filename(self):
        filename = filenamer.sanitize(self.filename_prefix + '-%s.html' % self.ms)
        return filename

    @property
    def path(self):
        return os.path.join(self.dirname, self.filename)

    def get_file(self):
        if not os.path.exists(self.dirname):
            os.makedirs(self.dirname)

        file_obj = open(self.path, 'w')
        return contextlib.closing(file_obj)

    def render(self):
        display_context = self._get_display_context()
        t = weblog.TEMPLATE_LOOKUP.get_template(self.template)
        return t.render(**display_context)


class T2_4MDetailsselfcalRenderer(basetemplates.T2_4MDetailsDefaultRenderer):
    def __init__(self, uri='vlass_selfcal.mako', description='Selfcal tables',
                 always_rerender=False):
        super(T2_4MDetailsselfcalRenderer, self).__init__(
            uri=uri, description=description, always_rerender=always_rerender)

    def get_display_context(self, context, results):
        super_cls = super(T2_4MDetailsselfcalRenderer, self)
        ctx = super_cls.get_display_context(context, results)

        weblog_dir = os.path.join(context.report_dir,
                                  'stage%s' % results.stage_number)

        summary_plots = {}
        selfcalphasegaincal_subpages = {}

        for result in results:

            # plotter = selfcaldisplay.selfcalSummaryChart(context, result)
            # plots = plotter.plot()
            ms = os.path.basename(result.inputs['vis'])
            if 'VLASS-SE' in result.inputs['selfcalmode']:
                plots = [selfcaldisplay.selfcalSolutionNumPerFieldChart(context, result).plot()]
            else:
                plots = []
            summary_plots[ms] = [p for p in plots if p is not None]

            # generate selfcal phase gain cal solution plots and JSON file
            plotter = selfcaldisplay.selfcalphaseGainPerAntennaChart(context, result)
            plots = plotter.plot()
            json_path = plotter.json_filename

            # write the html for each MS to disk
            renderer = VLASubPlotRenderer(context, result, plots, json_path,
                                          'vlass_selfcal_plots.mako', 'selfcalphasegaincal')
            with renderer.get_file() as fileobj:
                fileobj.write(renderer.render())
                selfcalphasegaincal_subpages[ms] = renderer.filename

        ctx.update({'summary_plots': summary_plots,
                    'selfcalphasegaincal_subpages': selfcalphasegaincal_subpages,
                    'dirname': weblog_dir})

        return ctx
