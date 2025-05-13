"""
Created on 24 Oct 2014

@author: brk
"""

import contextlib
import os

import pipeline.infrastructure.filenamer as filenamer
import pipeline.infrastructure.logging as logging
import pipeline.infrastructure.renderer.basetemplates as basetemplates
import pipeline.infrastructure.renderer.weblog as weblog
from . import display as syspowerdisplay

LOG = logging.get_logger(__name__)


class VLASubPlotRenderer(object):

    def __init__(self, context, result, plots, json_path, template, filename_prefix, band, spw, allbands):
        self.context = context
        self.result = result
        self.plots = plots
        self.ms = os.path.basename(self.result.inputs['vis'])
        self.template = template
        self.filename_prefix = filename_prefix
        self.band = band
        self.spw = spw
        self.allbands = allbands

        self.summary_plots = {}
        self.syspowerspgain_subpages = {}
        self.pdiffspgain_subpages = {}

        # Links for the subpages at the top of each sub-rendering page
        for bandname in allbands:
            subpage = dict()
            subpage[self.ms] = filenamer.sanitize('spgainrq-{!s}-band'.format(bandname) + '-%s.html' % self.ms)
            self.syspowerspgain_subpages[bandname] = subpage

            subpage = dict()
            subpage[self.ms] = filenamer.sanitize('spgainpdiff-{!s}-band'.format(bandname) + '-%s.html' % self.ms)
            self.pdiffspgain_subpages[bandname] = subpage

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
                'syspowerspgain_subpages': self.syspowerspgain_subpages,
                'pdiffspgain_subpages': self.pdiffspgain_subpages}

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


class T2_4MDetailssyspowerRenderer(basetemplates.T2_4MDetailsDefaultRenderer):
    def __init__(self, uri='syspower.mako',
                 description='Syspower (modified rq gains)',
                 always_rerender=False):
        super(T2_4MDetailssyspowerRenderer, self).__init__(uri=uri, description=description,
                                                           always_rerender=always_rerender)

    def get_display_context(self, context, results):
        super_cls = super(T2_4MDetailssyspowerRenderer, self)
        ctx = super_cls.get_display_context(context, results)

        weblog_dir = os.path.join(context.report_dir,
                                  'stage%s' % results.stage_number)

        m = context.observing_run.measurement_sets[0]

        swpowspgain_subpages = {}
        pdiffspgain_subpages = {}
        # box_plots = {}
        # bar_plots = {}
        # compression_plots = {}
        # median_plots = {}
        all_plots = {}
        band_display = {}

        for result in results:

            ms = os.path.basename(result.inputs['vis'])

            if result.template_table:
                # PIPE-2184: plot data points only for science scans
                m = context.observing_run.get_ms(ms)
                science_scan_ids = ','.join(str(scan.id) for scan in m.scans
                                            if not {'SYSTEM_CONFIGURATION', 'UNSPECIFIED#UNSPECIFIED', 'FOCUS', 'POINTING'} & scan.intents)

                for band in result.template_table:
                    plotter = syspowerdisplay.syspowerBoxChart(context, result, result.dat_common[band], band)
                    box_plots = plotter.plot()

                    plotter = syspowerdisplay.syspowerBarChart(context, result, result.dat_common[band], band)
                    bar_plots = plotter.plot()

                    plotter = syspowerdisplay.compressionSummary(context, result, result.spowerdict[band], band)
                    compression_plots = plotter.plot()

                    plotter = syspowerdisplay.medianSummary(context, result, result.spowerdict[band], band)
                    median_plots = plotter.plot()

                    # Collect all plots in a given band
                    all_plots_band = dict()
                    all_plots_band[ms] = [box_plots[0], bar_plots[0], compression_plots[0], median_plots[0]]
                    all_plots[band] = all_plots_band

                # generate switched power plots and JSON file
                allbands = list(result.band_baseband_spw.keys())
                spw = ''

                for band in result.band_baseband_spw:
                    selectspw = []
                    selectbasebands = []
                    for baseband in result.band_baseband_spw[band]:

                        # Pick one from each baseband if available
                        if result.band_baseband_spw[band][baseband]:
                            ispw = int(len(result.band_baseband_spw[band][baseband]) / 2)
                            selectspw.append(str(result.band_baseband_spw[band][baseband][ispw]))
                            selectbasebands.append(baseband)
                            spw = ','.join(selectspw)

                    plotter = syspowerdisplay.syspowerPerAntennaChart(context, result, 'spgain',
                                                                      result.plotrq, 'syspower', 'rq',
                                                                      band, spw, selectbasebands, science_scan_ids)
                    plots = plotter.plot()
                    json_path = plotter.json_filename

                    # write the html for each MS to disk
                    renderer = VLASubPlotRenderer(context, result, plots, json_path,
                                                  'syspower_plots.mako', 'spgainrq-{!s}-band'.format(band),
                                                  band, spw, allbands)
                    with renderer.get_file() as fileobj:
                        fileobj.write(renderer.render())
                        swpowspgain_subpages_band = dict()
                        swpowspgain_subpages_band[ms] = renderer.filename
                        swpowspgain_subpages[band] = swpowspgain_subpages_band

                    # plot template pdiff table
                    plotter = syspowerdisplay.syspowerPerAntennaChart(context, result, 'spgain',
                                                                      result.template_table[band], 'syspower', 'pdiff',
                                                                      band, spw, selectbasebands, science_scan_ids)
                    plots = plotter.plot()
                    json_path = plotter.json_filename

                    # write the html for each MS to disk
                    renderer = VLASubPlotRenderer(context, result, plots, json_path,
                                                  'syspower_plots.mako', 'spgainpdiff-{!s}-band'.format(band),
                                                  band, spw, allbands)
                    with renderer.get_file() as fileobj:
                        fileobj.write(renderer.render())
                        pdiffspgain_subpages_band = dict()
                        pdiffspgain_subpages_band[ms] = renderer.filename
                        pdiffspgain_subpages[band] = pdiffspgain_subpages_band

                banddict = m.get_vla_baseband_spws(science_windows_only=True, return_select_list=False, warning=False)
                if len(banddict) == 0:
                    LOG.debug("Baseband name cannot be parsed and will not appear in the weblog.")

                for band in result.band_baseband_spw:
                    baseband_display = {}
                    for baseband in result.band_baseband_spw[band]:
                        spws = []
                        minfreqs = []
                        maxfreqs = []
                        for spwitem in banddict[band][baseband]:
                            spws.append(str([*spwitem][0]))
                            minfreqs.append(spwitem[list(spwitem.keys())[0]][0])
                            maxfreqs.append(spwitem[list(spwitem.keys())[0]][1])
                        bbandminfreq = min(minfreqs)
                        bbandmaxfreq = max(maxfreqs)
                        baseband_display[baseband] = str(bbandminfreq) + ' to ' + str(bbandmaxfreq) + '    spw:' + ','.join(spws)
                    band_display[band] = baseband_display

        ctx.update({'dirname': weblog_dir,
                    'all_plots': all_plots,
                    'syspowerspgain_subpages': swpowspgain_subpages,
                    'pdiffspgain_subpages': pdiffspgain_subpages,
                    'band_display': band_display})

        return ctx
