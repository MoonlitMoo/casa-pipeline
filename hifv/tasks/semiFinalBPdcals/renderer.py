import contextlib
import os
import collections

import pipeline.infrastructure.filenamer as filenamer
import pipeline.infrastructure.logging as logging
import pipeline.infrastructure.renderer.basetemplates as basetemplates
import pipeline.infrastructure.renderer.weblog as weblog
from . import display as semifinalBPdcalsdisplay

LOG = logging.get_logger(__name__)


class VLASubPlotRenderer(object):
    #template = 'testdelays_plots.html'

    def __init__(self, context, result, plots, json_path, template, filename_prefix, bandlist, spwlist=None, spw_plots=None):
        self.context = context
        self.result = result
        self.plots = plots
        self.ms = os.path.basename(self.result.inputs['vis'])
        self.template = template
        self.filename_prefix = filename_prefix
        self.bandlist = bandlist
        self.spwlist = spwlist
        self.spw_plots = spw_plots

        if self.spwlist is None:
            self.spwlist = []
        if self.spw_plots is None:
            self.spw_plots = []

        self.summary_plots = {}
        self.delay_subpages = {}
        self.phasegain_subpages = {}
        self.bpsolamp_subpages = {}
        self.bpsolphase_subpages = {}

        self.delay_subpages[self.ms] = filenamer.sanitize('delays' + '-%s.html' % self.ms)
        self.phasegain_subpages[self.ms] = filenamer.sanitize('phasegain' + '-%s.html' % self.ms)
        self.bpsolamp_subpages[self.ms] = filenamer.sanitize('bpsolamp' + '-%s.html' % self.ms)
        self.bpsolphase_subpages[self.ms] = filenamer.sanitize('bpsolphase' + '-%s.html' % self.ms)

        if os.path.exists(json_path):
            with open(json_path, 'r') as json_file:
                self.json = json_file.readlines()[0]
        else:
            self.json = '{}'

    def _get_display_context(self):
        return {'pcontext': self.context,
                'result': self.result,
                'plots': self.plots,
                'spw_plots': self.spw_plots,
                'dirname': self.dirname,
                'json': self.json,
                'delay_subpages': self.delay_subpages,
                'phasegain_subpages': self.phasegain_subpages,
                'bpsolamp_subpages': self.bpsolamp_subpages,
                'bpsolphase_subpages': self.bpsolphase_subpages,
                'spwlist': self.spwlist,
                'bandlist': self.bandlist}

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


class T2_4MDetailssemifinalBPdcalsRenderer(basetemplates.T2_4MDetailsDefaultRenderer):
    def __init__(self, uri='semifinalbpdcals.mako', description='Semi-final delay and bandpass calibrations',
                 always_rerender=False):
        super(T2_4MDetailssemifinalBPdcalsRenderer, self).__init__(
            uri=uri, description=description, always_rerender=always_rerender)

    def get_display_context(self, context, results):
        super_cls = super(T2_4MDetailssemifinalBPdcalsRenderer, self)
        ctx = super_cls.get_display_context(context, results)

        weblog_dir = os.path.join(context.report_dir,
                                  'stage%s' % results.stage_number)

        summary_plots = {}
        summary_plots_per_spw = {}
        delay_subpages = {}
        ampgain_subpages = {}
        phasegain_subpages = {}
        bpsolamp_subpages = {}
        bpsolphase_subpages = {}

        band2spw = collections.defaultdict(list)

        suffix = ''

        for result in results:

            m = context.observing_run.get_ms(result.inputs['vis'])
            spw2band = m.get_vla_spw2band()
            spwobjlist = m.get_spectral_windows(science_windows_only=True)
            listspws = [spw.id for spw in spwobjlist]
            for spw, band in spw2band.items():
                if spw in listspws:  # Science intents only
                    band2spw[band].append(str(spw))

            bandlist = [band for band in band2spw.keys()]
            # LOG.info("BAND LIST: " + ','.join(bandlist))

            plotter = semifinalBPdcalsdisplay.semifinalBPdcalsSummaryChart(context, result, suffix=suffix)
            plots = plotter.plot()
            ms = os.path.basename(result.inputs['vis'])
            summary_plots[ms] = plots

            # generate per-SPW semifinalBPdcals plots
            spws = m.get_spectral_windows(science_windows_only=True)
            spwlist = []
            per_spw_plots = []
            for spw in spws:
                if spw.specline_window:
                    plotter = semifinalBPdcalsdisplay.semifinalBPdcalsSpwSummaryChart(context, result, suffix=suffix, spw=spw.id)
                    plots = plotter.plot()
                    per_spw_plots.extend(plots)
                    spwlist.append(str(spw.id))

            if per_spw_plots:
                summary_plots_per_spw[ms] = []
                summary_plots_per_spw[ms].extend(per_spw_plots)

            # generate testdelay plots and JSON file
            plotter = semifinalBPdcalsdisplay.DelaysPerAntennaChart(context, result, suffix=suffix)
            plots = plotter.plot()
            json_path = plotter.json_filename

            # write the html for each MS to disk
            renderer = VLASubPlotRenderer(context, result, plots, json_path, 'semifinalcals_plots.mako', 'delays', band2spw)
            with renderer.get_file() as fileobj:
                fileobj.write(renderer.render())
                delay_subpages[ms] = renderer.filename

            # generate phase Gain plots and JSON file
            plotter = semifinalBPdcalsdisplay.semifinalphaseGainPerAntennaChart(context, result, suffix=suffix)
            plots = plotter.plot()
            json_path = plotter.json_filename

            # write the html for each MS to disk
            renderer = VLASubPlotRenderer(context, result, plots, json_path, 'semifinalcals_plots.mako', 'phasegain', band2spw)
            with renderer.get_file() as fileobj:
                fileobj.write(renderer.render())
                phasegain_subpages[ms] = renderer.filename

            # generate amp bandpass solution plots and JSON file
            plotter = semifinalBPdcalsdisplay.semifinalbpSolAmpPerAntennaChart(context, result, suffix=suffix)
            plots = plotter.plot()
            json_path = plotter.json_filename

            plotter = semifinalBPdcalsdisplay.semifinalbpSolAmpPerAntennaPerSpwChart(context, result, suffix=suffix)
            spw_plots = plotter.plot()

            # write the html for each MS to disk
            renderer = VLASubPlotRenderer(context, result, plots, json_path, 'semifinalcals_plots.mako', 'bpsolamp', band2spw, spwlist, spw_plots=spw_plots)
            with renderer.get_file() as fileobj:
                fileobj.write(renderer.render())
                bpsolamp_subpages[ms] = renderer.filename

            # generate phase bandpass solution plots and JSON file
            plotter = semifinalBPdcalsdisplay.semifinalbpSolPhasePerAntennaChart(context, result, suffix=suffix)
            plots = plotter.plot()
            json_path = plotter.json_filename

            plotter = semifinalBPdcalsdisplay.semifinalbpSolPhasePerAntennaPerSpwChart(context, result, suffix=suffix)
            spw_plots = plotter.plot()

            # write the html for each MS to disk
            renderer = VLASubPlotRenderer(context, result, plots, json_path, 'semifinalcals_plots.mako', 'bpsolphase', band2spw, spwlist, spw_plots=spw_plots)
            with renderer.get_file() as fileobj:
                fileobj.write(renderer.render())
                bpsolphase_subpages[ms] = renderer.filename

        ctx.update({'summary_plots': summary_plots,
                    'summary_plots_per_spw': summary_plots_per_spw,
                    'delay_subpages': delay_subpages,
                    'phasegain_subpages': phasegain_subpages,
                    'bpsolamp_subpages': bpsolamp_subpages,
                    'bpsolphase_subpages': bpsolphase_subpages,
                    'dirname': weblog_dir})

        return ctx
