import collections
import contextlib
import os

import pipeline.infrastructure.filenamer as filenamer
import pipeline.infrastructure.logging as logging
import pipeline.infrastructure.renderer.basetemplates as basetemplates
import pipeline.infrastructure.renderer.weblog as weblog
import pipeline.infrastructure.utils as utils
from . import fluxbootdisplay
from . import testgainsdisplay

LOG = logging.get_logger(__name__)


class VLASubPlotRenderer(object):

    def __init__(self, context, result, plots, json_path, template, filename_prefix, bandlist):
        self.context = context
        self.result = result
        self.plots = plots
        self.ms = os.path.basename(self.result.inputs['vis'])
        self.template = template
        self.filename_prefix=filename_prefix
        self.bandlist = bandlist

        self.summary_plots = {}
        self.testgainsamp_subpages = {}
        self.testgainsphase_subpages = {}

        self.testgainsamp_subpages[self.ms] = filenamer.sanitize('amp' + '-%s.html' % self.ms)
        self.testgainsphase_subpages[self.ms] = filenamer.sanitize('phase' + '-%s.html' % self.ms)

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
                'testgainsamp_subpages': self.testgainsamp_subpages,
                'testgainsphase_subpages': self.testgainsphase_subpages,
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


class T2_4MDetailsSolintRenderer(basetemplates.T2_4MDetailsDefaultRenderer):
    def __init__(self, uri='solint.mako', description='Determine solint and Test gain calibrations',
                 always_rerender=False):
        super(T2_4MDetailsSolintRenderer, self).__init__(uri=uri,
                                                         description=description, always_rerender=always_rerender)

    def get_display_context(self, context, results):
        super_cls = super(T2_4MDetailsSolintRenderer, self)
        ctx = super_cls.get_display_context(context, results)

        weblog_dir = os.path.join(context.report_dir, 'stage%s' % results.stage_number)

        summary_plots = {}
        testgainsamp_subpages = {}
        testgainsphase_subpages = {}

        longsolint = {}
        gain_solint2 = {}

        shortsol2 = {}
        short_solint = {}
        new_gain_solint1 = {}

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

            plotter = testgainsdisplay.testgainsSummaryChart(context, result)
            # plots = plotter.plot()
            plots = []
            ms = os.path.basename(result.inputs['vis'])
            summary_plots[ms] = plots

            # generate testdelay plots and JSON file
            plotter = testgainsdisplay.testgainsPerAntennaChart(context, result, 'amp')
            plots = plotter.plot() 
            json_path = plotter.json_filename

            # write the html for each MS to disk
            renderer = VLASubPlotRenderer(context, result, plots, json_path, 'testgains_plots.mako', 'amp', bandlist)
            with renderer.get_file() as fileobj:
                fileobj.write(renderer.render())
                testgainsamp_subpages[ms] = renderer.filename

            # generate amp Gain plots and JSON file
            plotter = testgainsdisplay.testgainsPerAntennaChart(context, result, 'phase')
            plots = plotter.plot() 
            json_path = plotter.json_filename

            # write the html for each MS to disk
            renderer = VLASubPlotRenderer(context, result, plots, json_path, 'testgains_plots.mako', 'phase', bandlist)
            with renderer.get_file() as fileobj:
                fileobj.write(renderer.render())
                testgainsphase_subpages[ms] = renderer.filename

            # String type formatting of solution intervals
            new_gain_solint1_string = ''
            for band, value in result.new_gain_solint1.items():
                new_gain_solint1_string += '{!s} band: {!s}    '.format(band, value)
            longsolint_string = ''
            for band, value in result.longsolint.items():
                longsolint_string += '{!s} band: {:6.2f}s    '.format(band, float(value))

            # String type
            new_gain_solint1[ms] = new_gain_solint1_string
            longsolint[ms] = longsolint_string

        ctx.update({'summary_plots': summary_plots,
                    'testgainsamp_subpages': testgainsamp_subpages,
                    'testgainsphase_subpages': testgainsphase_subpages,
                    'new_gain_solint1': new_gain_solint1,
                    'longsolint': longsolint,
                    'dirname': weblog_dir})

        return ctx


class T2_4MDetailsfluxbootRenderer(basetemplates.T2_4MDetailsDefaultRenderer):
    def __init__(self, uri='fluxboot.mako', description='Gain table for flux density bootstrapping',
                 always_rerender=False):
        super(T2_4MDetailsfluxbootRenderer, self).__init__(uri=uri,
                                                           description=description, always_rerender=always_rerender)

    def get_display_context(self, context, results):
        super_cls = super(T2_4MDetailsfluxbootRenderer, self)
        ctx = super_cls.get_display_context(context, results)

        weblog_dir = os.path.join(context.report_dir, 'stage%s' % results.stage_number)

        summary_plots = {}
        weblog_results = {}
        spindex_results = {}

        webdicts = {}

        for result in results:

            plotter = fluxbootdisplay.fluxgaincalSummaryChart(context, result, result.caltable)
            plots = plotter.plot()

            plotter = fluxbootdisplay.fluxbootSummaryChart(context, result)
            plots.extend(plotter.plot())

            ms = os.path.basename(result.inputs['vis'])

            weblog_results[ms] = result.weblog_results
            spindex_results[ms] = result.spindex_results

            # Sort into dictionary collections and combine bands for a single source
            FluxTR = collections.namedtuple('FluxTR', 'source fitorder band bandcenterfreq fitflx spix curvature gamma delta')
            rows = []

            precision = 5
            # print("{:.{}f}".format(pi, precision))

            for row in sorted(result.spindex_results, key=lambda p: (p['source'], float(p['sortingfreq']))):
                spix = "{:.{}f}".format(float(row['spix']), precision)
                spixerr = "{:.{}f}".format(float(row['spixerr']), precision)
                curvature = "{:.{}f}".format(float(row['curvature']), precision)
                curvatureerr = "{:.{}f}".format(float(row['curvatureerr']), precision)
                gamma = "{:.{}f}".format(float(row['gamma']), precision)
                gammaerr = "{:.{}f}".format(float(row['gammaerr']), precision)
                delta = "{:.{}f}".format(float(row['delta']), precision)
                deltaerr = "{:.{}f}".format(float(row['deltaerr']), precision)
                fitflx = "{:.{}f}".format(float(row['fitflx']), precision)
                fitflxerr = "{:.{}f}".format(float(row['fitflxerr']), precision)
                reffreq = "{:.{}f}".format(float(row['reffreq']), precision)
                bandcenterfreq = "{:.{}f}".format(float(row['bandcenterfreq'])/1.e9, precision)
                # fitflxAtRefFreq = "{:.{}f}".format(float(row['fitflxAtRefFreq']), precision)
                # fitflxAtRefFreqErr = "{:.{}f}".format(float(row['fitflxAtRefFreqErr']), precision)

                curvval = curvature + ' +/- ' + curvatureerr
                gammaval = gamma + ' +/- ' + gammaerr
                deltaval = delta + ' +/- ' + deltaerr

                if float(row['curvature']) == 0.0:
                    curvval = '----'
                if float(row['gamma']) == 0.0:
                    gammaval = '----'
                if float(row['delta']) == 0.0:
                    deltaval = '----'

                tr = FluxTR(row['source'], row['fitorder'], row['band'], bandcenterfreq, fitflx + ' +/- ' + fitflxerr,
                            spix + ' +/- ' + spixerr,
                            curvval,
                            gammaval,
                            deltaval)
                            # reffreq, fitflxAtRefFreq + ' +/- ' + fitflxAtRefFreqErr)
                rows.append(tr)

            spixtable = utils.merge_td_columns(rows)

            # Sort into dictionary collections to prep for weblog table
            webdicts[ms] = collections.defaultdict(list)
            for row in sorted(weblog_results[ms], key=lambda p: (p['source'], float(p['freq']))):
                webdicts[ms][row['source']].append({'freq': row['freq'], 'data': row['data'], 'error': row['error'],
                                                    'fitteddata': row['fitteddata']})

            plotter = fluxbootdisplay.residualsSummaryChart(context, result, webdicts[ms])
            plots.extend(plotter.plot())

            plotter = fluxbootdisplay.modelfitSummaryChart(context, result, webdicts[ms])
            plots.extend(plotter.plot())

            summary_plots[ms] = plots

            weblog_results[ms] = webdicts[ms]

        ctx.update({'summary_plots': summary_plots,
                    'weblog_results': weblog_results,
                    'spindex_results': spindex_results,
                    'spixtable': spixtable,
                    'dirname': weblog_dir})

        return ctx

