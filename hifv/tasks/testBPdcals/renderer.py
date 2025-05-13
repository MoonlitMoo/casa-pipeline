import contextlib
import os
import collections

import pipeline.infrastructure.filenamer as filenamer
import pipeline.infrastructure.logging as logging
import pipeline.infrastructure.renderer.basetemplates as basetemplates
import pipeline.infrastructure.renderer.weblog as weblog
from . import display as testBPdcalsdisplay

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
        self.testdelay_subpages = {}
        self.ampgain_subpages = {}
        self.phasegain_subpages = {}
        self.bpsolamp_subpages = {}
        self.bpsolphase_subpages = {}

        self.testdelay_subpages[self.ms] = filenamer.sanitize('testdelays' + '-%s.html' % self.ms)
        self.ampgain_subpages[self.ms] = filenamer.sanitize('ampgain' + '-%s.html' % self.ms)
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
                'testdelay_subpages': self.testdelay_subpages,
                'ampgain_subpages': self.ampgain_subpages,
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


class T2_4MDetailstestBPdcalsRenderer(basetemplates.T2_4MDetailsDefaultRenderer):
    def __init__(self, uri='testbpdcals.mako', description='Initial test calibrations',
                 always_rerender=False):
        super(T2_4MDetailstestBPdcalsRenderer, self).__init__(
            uri=uri, description=description, always_rerender=always_rerender)

    def get_display_context(self, context, results):
        super_cls = super(T2_4MDetailstestBPdcalsRenderer, self)
        ctx = super_cls.get_display_context(context, results)

        weblog_dir = os.path.join(context.report_dir,
                                  'stage%s' % results.stage_number)

        summary_plots = {}
        summary_plots_per_spw = {}
        testdelay_subpages = {}
        ampgain_subpages = {}
        phasegain_subpages = {}
        bpsolamp_subpages = {}
        bpsolphase_subpages = {}

        band2spw = collections.defaultdict(list)

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

            plotter = testBPdcalsdisplay.testBPdcalsSummaryChart(context, result)
            plots = plotter.plot()
            ms = os.path.basename(result.inputs['vis'])
            summary_plots[ms] = plots

            # generate per-SPW testBPdcals plots for specline windows
            spws = m.get_spectral_windows(science_windows_only=True)
            spwlist = []
            per_spw_plots = []
            for spw in spws:
                if spw.specline_window:
                    plotter = testBPdcalsdisplay.testBPdcalsPerSpwSummaryChart(context, result, spw=spw.id)
                    plots = plotter.plot()
                    per_spw_plots.extend(plots)
                    spwlist.append(str(spw.id))

            if per_spw_plots:
                summary_plots_per_spw[ms] = []
                summary_plots_per_spw[ms].extend(per_spw_plots)

            # generate testdelay plots and JSON file
            plotter = testBPdcalsdisplay.testDelaysPerAntennaChart(context, result)
            plots = plotter.plot()
            json_path = plotter.json_filename

            # write the html for each MS to disk
            renderer = VLASubPlotRenderer(context, result, plots, json_path, 'testcals_plots.mako', 'testdelays', band2spw)
            with renderer.get_file() as fileobj:
                fileobj.write(renderer.render())
                testdelay_subpages[ms] = renderer.filename

            # generate amp Gain plots and JSON file
            plotter = testBPdcalsdisplay.ampGainPerAntennaChart(context, result)
            plots = plotter.plot()
            json_path = plotter.json_filename

            # write the html for each MS to disk
            renderer = VLASubPlotRenderer(context, result, plots, json_path, 'testcals_plots.mako', 'ampgain', band2spw)
            with renderer.get_file() as fileobj:
                fileobj.write(renderer.render())
                ampgain_subpages[ms] = renderer.filename

            # generate phase Gain plots and JSON file
            plotter = testBPdcalsdisplay.phaseGainPerAntennaChart(context, result)
            plots = plotter.plot()
            json_path = plotter.json_filename

            # write the html for each MS to disk
            renderer = VLASubPlotRenderer(context, result, plots, json_path, 'testcals_plots.mako', 'phasegain', band2spw)
            with renderer.get_file() as fileobj:
                fileobj.write(renderer.render())
                phasegain_subpages[ms] = renderer.filename

            # generate amp bandpass solution plots and JSON file
            plotter = testBPdcalsdisplay.bpSolAmpPerAntennaChart(context, result)
            plots = plotter.plot()
            json_path = plotter.json_filename

            # generate amp bandpass solution per-spw plots
            plotter = testBPdcalsdisplay.bpSolAmpPerAntennaPerSpwChart(context, result)
            spw_plots = plotter.plot()

            # write the html for each MS to disk
            renderer = VLASubPlotRenderer(context, result, plots, json_path, 'testcals_plots.mako', 'bpsolamp', band2spw, spwlist, spw_plots=spw_plots)
            with renderer.get_file() as fileobj:
                fileobj.write(renderer.render())
                bpsolamp_subpages[ms] = renderer.filename

            # generate phase bandpass solution plots and JSON file
            plotter = testBPdcalsdisplay.bpSolPhasePerAntennaChart(context, result)
            plots = plotter.plot()
            json_path = plotter.json_filename

            # generate phase bandpass per spw solution plots
            plotter = testBPdcalsdisplay.bpSolPhasePerAntennaPerSpwChart(context, result)
            spw_plots = plotter.plot()

            # write the html for each MS to disk
            renderer = VLASubPlotRenderer(context, result, plots, json_path, 'testcals_plots.mako', 'bpsolphase', band2spw, spwlist, spw_plots=spw_plots)
            with renderer.get_file() as fileobj:
                fileobj.write(renderer.render())
                bpsolphase_subpages[ms] = renderer.filename

        ctx.update({'summary_plots': summary_plots,
                    'summary_plots_per_spw': summary_plots_per_spw,
                    'testdelay_subpages': testdelay_subpages,
                    'ampgain_subpages': ampgain_subpages,
                    'phasegain_subpages': phasegain_subpages,
                    'bpsolamp_subpages': bpsolamp_subpages,
                    'bpsolphase_subpages': bpsolphase_subpages,
                    'dirname': weblog_dir})

        return ctx
