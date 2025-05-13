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
from . import opacitiesdisplay
from . import swpowdisplay

LOG = logging.get_logger(__name__)


class VLASubPlotRenderer(object):
    # template = 'testdelays_plots.html'

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
        self.swpowspgain_subpages = {}
        self.swpowtsys_subpages = {}

        # PIPE-1755: Links for the subpages at the top of each sub-rendering page
        for bandname in allbands:
            subpage = dict()
            subpage[self.ms] = filenamer.sanitize('spgain-{!s}-band'.format(bandname) + '-%s.html' % self.ms)
            self.swpowspgain_subpages[bandname] = subpage

            subpage = dict()
            subpage[self.ms] = filenamer.sanitize('tsys-{!s}-band'.format(bandname) + '-%s.html' % self.ms)
            self.swpowtsys_subpages[bandname] = subpage

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
                'swpowspgain_subpages': self.swpowspgain_subpages,
                'swpowtsys_subpages': self.swpowtsys_subpages}

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


class T2_4MDetailspriorcalsRenderer(basetemplates.T2_4MDetailsDefaultRenderer):
    def __init__(self, uri='priorcals.mako', description='Priorcals (gaincurves, opacities, antenna positions corrections, rq gains, and switched power)',
                 always_rerender=False):
        super(T2_4MDetailspriorcalsRenderer, self).__init__(uri=uri,
                                                            description=description,
                                                            always_rerender=always_rerender)

    def get_display_context(self, context, results):
        super_cls = super(T2_4MDetailspriorcalsRenderer, self)
        ctx = super_cls.get_display_context(context, results)

        weblog_dir = os.path.join(context.report_dir,
                                  'stage%s' % results.stage_number)

        opacity_plots = {}
        spw = {}
        center_frequencies = {}
        opacities = {}
        swpowspgain_subpages = {}
        swpowtsys_subpages = {}
        summary_plots = {}
        tec_plotfile = ''

        for result in results:

            ms = os.path.basename(result.inputs['vis'])
            spw[ms] = result.oc_result.spw.split(',')
            center_frequencies[ms] = result.oc_result.center_frequencies
            opacities[ms] = result.oc_result.opacities
            # PIPE-2184: plot data points only for science scans
            m = context.observing_run.get_ms(ms)
            science_scan_ids = ','.join(str(scan.id) for scan in m.scans
                                        if not {'SYSTEM_CONFIGURATION', 'UNSPECIFIED#UNSPECIFIED', 'FOCUS', 'POINTING'} & scan.intents)

            plotter = opacitiesdisplay.opacitiesSummaryChart(context, result)
            plots = plotter.plot()
            opacity_plots[ms] = plots

            # plotter = swpowdisplay.swpowSummaryChart(context, result)
            # plots = plotter.plot()
            ms = os.path.basename(result.inputs['vis'])
            summary_plots[ms] = None

            # PIPE-1755: Plot one SPW per baseband to speedup hifv_priorcals
            m = context.observing_run.get_measurement_sets(ms)[0]
            banddict = m.get_vla_baseband_spws(science_windows_only=True, return_select_list=False, warning=False)
            spwstouse = ''
            allbands = list(banddict.keys())
            for band in banddict:
                selectspw = []
                selectbasebands = []
                for baseband in banddict[band]:
                    # select a SPW from a baseband
                    if banddict[band][baseband]:
                        ispw = int(len(banddict[band][baseband]) / 2)
                        for key, value in banddict[band][baseband][ispw].items():
                            selectspw.append(str(key))
                        selectbasebands.append(baseband)
                        spwstouse = ','.join(selectspw)

                # generate switched power plots and JSON file
                plotter = swpowdisplay.swpowPerAntennaChart(context, result, 'spgain', science_scan_ids, band, selectbasebands, spwstouse)
                if result.sw_result.final:
                    plots = plotter.plot()
                else:
                    plots = None
                json_path = plotter.json_filename

                # write the html for each MS to disk
                renderer = VLASubPlotRenderer(context, result, plots, json_path, 'swpow_plots.mako', 'spgain-{!s}-band'.format(band), band, spw, allbands)
                with renderer.get_file() as fileobj:
                    fileobj.write(renderer.render())
                    swpowspgain_subpages_band = dict()
                    swpowspgain_subpages_band[ms] = renderer.filename
                    swpowspgain_subpages[band] = swpowspgain_subpages_band
                # generate switched power plots and JSON file
                plotter = swpowdisplay.swpowPerAntennaChart(context, result, 'tsys', science_scan_ids, band, selectbasebands, spwstouse)
                if result.sw_result.final:
                    plots = plotter.plot()
                else:
                    plots = None
                json_path = plotter.json_filename

                # write the html for each MS to disk
                renderer = VLASubPlotRenderer(context, result, plots, json_path, 'swpow_plots.mako', 'tsys-{!s}-band'.format(band), band, spw, allbands)
                with renderer.get_file() as fileobj:
                    fileobj.write(renderer.render())
                    swpowtsys_subpages_band = dict()
                    swpowtsys_subpages_band[ms] = renderer.filename
                    swpowtsys_subpages[band] = swpowtsys_subpages_band

            # Plot already generated by tec_maps CASA recipe
            # Use plot name saved in the tec_maps results and move to appropriate pipeline context weblog
            # stage directory
            if result.tecmaps_result:
                if result.tecmaps_result.tec_plotfile:
                    original_tec_plotfile = result.tecmaps_result.tec_plotfile
                    move_tec_plotfile = os.path.join(context.report_dir,
                                                     'stage%s' % result.stage_number, original_tec_plotfile)
                    tec_plotfile = os.path.join('stage%s' % result.stage_number, original_tec_plotfile)
                    os.rename(original_tec_plotfile, move_tec_plotfile)

        ctx.update({'opacity_plots': opacity_plots,
                    'spw': spw,
                    'center_frequencies': center_frequencies,
                    'opacities': opacities,
                    'dirname': weblog_dir,
                    'summary_plots': summary_plots,
                    'swpowspgain_subpages': swpowspgain_subpages,
                    'swpowtsys_subpages': swpowtsys_subpages,
                    'tec_plotfile': tec_plotfile})

        return ctx
