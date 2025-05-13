import collections
import itertools
import operator
import os

import numpy as np

import pipeline.domain.measures as measures
import pipeline.h.tasks.applycal.renderer as applycal_renderer
import pipeline.infrastructure
import pipeline.infrastructure.filenamer as filenamer
import pipeline.infrastructure.logging as logging
import pipeline.infrastructure.renderer.basetemplates as basetemplates
import pipeline.infrastructure.utils as utils
from pipeline.h.tasks.common.displays import applycal as applycal
from . import display as plotsummarydisplay

LOG = logging.get_logger(__name__)


class T2_4MDetailsplotsummaryRenderer(basetemplates.T2_4MDetailsDefaultRenderer):
    def __init__(self, uri='plotsummary.mako',
                 description='VLA Plot Summary', always_rerender=False):
        super(T2_4MDetailsplotsummaryRenderer, self).__init__(
            uri=uri, description=description, always_rerender=always_rerender)

    def update_mako_context(self, ctx, context, results_list):
        weblog_dir = os.path.join(context.report_dir,
                                  'stage%s' % results_list.stage_number)

        flag_totals = {}

        calapps = {}
        for r in results_list:
            calapps = utils.dict_merge(calapps,
                                       self.calapps_for_result(r))

        caltypes = {}
        for r in results_list:
            caltypes = utils.dict_merge(caltypes,
                                        self.caltypes_for_result(r))

        filesizes = {}
        baseband_spws = {}  # VLA baseband lookup dictionary
        for r in results_list:
            vis = r.inputs['vis']
            ms = context.observing_run.get_ms(vis)
            filesizes[os.path.basename(vis)] = ms._calc_filesize()
            baseband_spws[os.path.basename(vis)] = ms.get_vla_baseband_spws(
                science_windows_only=True, return_select_list=False, warning=False)

        # original plot summary plots
        summary_plots = {}

        for r in results_list:
            plotter = plotsummarydisplay.plotsummarySummaryChart(context, r)
            plots = plotter.plot()
            ms = os.path.basename(r.inputs['vis'])
            summary_plots[ms] = plots

        # return all agents so we get ticks and crosses against each one
        agents = ['before', 'applycal']

        m = context.observing_run.measurement_sets[0]
        corrstring = m.get_vla_corrstring()

        ctx.update({'summary_plots': summary_plots,
                    'flags': flag_totals,
                    'calapps': calapps,
                    'caltypes': caltypes,
                    'agents': agents,
                    'dirname': weblog_dir,
                    'filesizes': filesizes})

        intent_sort_order = {
            'PHASE': 1,
            'BANDPASS': 2
        }

        amp_vs_freq_summary_plots = utils.OrderedDefaultdict(list)
        for intents in [['PHASE'], ['BANDPASS']]:
            plots = self.create_plots(context,
                                      results_list,
                                      applycal.AmpVsFrequencyPerFieldBasebandSummaryChart,
                                      intents, correlation=corrstring)

            for vis, vis_plots in plots.items():
                vis_plots_mod = []
                for p in vis_plots:
                    baseband_desc, baseband_centfreq = self.get_baseband_desc(
                        baseband_spws[vis], spws_select=p.parameters['spw'].split(','))
                    p.parameters['baseband_desc'] = baseband_desc
                    p.parameters['baseband_centfreq'] = np.mean(baseband_centfreq)
                    p.parameters['intent_idx'] = intent_sort_order[','.join(p.parameters['intent'])]
                    field = m.get_fields(p.parameters['field'])[0]
                    p.parameters['fieldid'] = field.id
                    vis_plots_mod.append(p)
                amp_vs_freq_summary_plots[vis].extend(vis_plots_mod)

        phase_vs_freq_summary_plots = utils.OrderedDefaultdict(list)
        for intents in [['PHASE'], ['BANDPASS']]:
            plots = self.create_plots(context,
                                      results_list,
                                      applycal.PhaseVsFrequencyPerFieldBasebandSummaryChart,
                                      intents, correlation=corrstring)

            for vis, vis_plots in plots.items():
                vis_plots_mod = []
                for p in vis_plots:
                    baseband_desc, baseband_centfreq = self.get_baseband_desc(
                        baseband_spws[vis], spws_select=p.parameters['spw'].split(','))
                    p.parameters['baseband_desc'] = baseband_desc
                    p.parameters['baseband_centfreq'] = np.mean(baseband_centfreq)
                    p.parameters['intent_idx'] = intent_sort_order[','.join(p.parameters['intent'])]
                    field = m.get_fields(p.parameters['field'])[0]
                    p.parameters['fieldid'] = field.id
                    vis_plots_mod.append(p)
                phase_vs_freq_summary_plots[vis].extend(vis_plots_mod)

        # Polarization plots
        pol_intent_sort_order = {
            'POLANGLE': 1,
            'POLLEAKAGE': 2,
            'PHASE': 3,
            'BANDPASS': 4
        }

        phase_vs_freq_polarization_plots = utils.OrderedDefaultdict(list)
        amp_vs_freq_polarization_plots = utils.OrderedDefaultdict(list)
        allintents = list(m.intents)

        if [intent for intent in allintents if 'POL' in intent]:
            for intents, correlation in [(['POLANGLE'], 'RL,LR'), (['POLLEAKAGE'], 'RL,LR'),
                                         (['PHASE'], 'RL,LR'), (['BANDPASS'], 'RL,LR')]:
                plots = self.create_plots(context,
                                          results_list,
                                          applycal.PhaseVsFrequencyPerFieldBasebandSummaryChart,
                                          intents, correlation=correlation, coloraxis='corr', avgtime='1e8',
                                          avgbaseline=True, avgantenna=False, plotrange=[0, 0, -180, 180])

                use_pol_plots = False
                for vis, vis_plots in plots.items():
                    vis_plots_mod = []
                    for p in vis_plots:
                        baseband_desc, baseband_centfreq = self.get_baseband_desc(
                            baseband_spws[vis], spws_select=p.parameters['spw'].split(','))
                        p.parameters['baseband_desc'] = baseband_desc
                        p.parameters['baseband_centfreq'] = np.mean(baseband_centfreq)
                        p.parameters['intent_idx'] = pol_intent_sort_order[','.join(p.parameters['intent'])]
                        field = m.get_fields(p.parameters['field'])[0]
                        p.parameters['fieldid'] = field.id
                        vis_plots_mod.append(p)

                    phase_vs_freq_polarization_plots[vis].extend(vis_plots_mod)
                    if vis_plots and (('POLANGLE' in m.intents) or ('POLLEAKAGE' in m.intents)):
                        use_pol_plots = True

            for intents, correlation in [(['POLANGLE'], 'RL,LR'), (['POLLEAKAGE'], 'RL,LR'),
                                         (['PHASE'], 'RL,LR'), (['BANDPASS'], 'RL,LR')]:
                plots = self.create_plots(context,
                                          results_list,
                                          applycal.AmpVsFrequencyPerFieldBasebandSummaryChart,
                                          intents, correlation=correlation, coloraxis='corr', avgtime='1e8',
                                          avgbaseline=True, avgantenna=False, plotrange=[])

                use_pol_plots = False
                for vis, vis_plots in plots.items():
                    vis_plots_mod = []
                    for p in vis_plots:
                        baseband_desc, baseband_centfreq = self.get_baseband_desc(
                            baseband_spws[vis], spws_select=p.parameters['spw'].split(','))
                        p.parameters['baseband_desc'] = baseband_desc
                        p.parameters['baseband_centfreq'] = np.mean(baseband_centfreq)
                        p.parameters['intent_idx'] = pol_intent_sort_order[','.join(p.parameters['intent'])]
                        field = m.get_fields(p.parameters['field'])[0]
                        p.parameters['fieldid'] = field.id
                        vis_plots_mod.append(p)

                    amp_vs_freq_polarization_plots[vis].extend(vis_plots_mod)
                    if vis_plots and (('POLANGLE' in m.intents) or ('POLLEAKAGE' in m.intents)):
                        use_pol_plots = True
        else:
            use_pol_plots = False

        science_amp_vs_freq_summary_plots = utils.OrderedDefaultdict(list)
        (plots, uv_max) = self.create_science_plots(context, results_list, correlation=corrstring)
        for vis, vis_plots in plots.items():
            vis_plots_mod = []
            for p in vis_plots:
                baseband_desc, baseband_centfreq = self.get_baseband_desc(
                    baseband_spws[vis], spws_select=p.parameters['spw'].split(','))
                p.parameters['baseband_desc'] = baseband_desc
                p.parameters['baseband_centfreq'] = np.mean(baseband_centfreq)
                vis_plots_mod.append(p)
            science_amp_vs_freq_summary_plots[vis].extend(vis_plots_mod)

        if pipeline.infrastructure.generate_detail_plots(results_list):
            for result in results_list:
                # detail plots. Don't need the return dictionary, but make sure a
                # renderer is passed so the detail page is written to disk
                self.create_plots(context,
                                  result,
                                  applycal.AmpVsFrequencyDetailChart,
                                  ['BANDPASS', 'PHASE'],
                                  ApplycalAmpVsFreqPlotRenderer, correlation=corrstring)

                self.create_plots(context,
                                  result,
                                  applycal.PhaseVsFrequencyDetailChart,
                                  ['BANDPASS', 'PHASE'],
                                  ApplycalPhaseVsFreqPlotRenderer, correlation=corrstring)

                self.create_plots(context,
                                  result,
                                  applycal.AmpVsUVDetailChart,
                                  ['AMPLITUDE'],
                                  ApplycalAmpVsUVPlotRenderer, correlation=corrstring)

                self.create_plots(context,
                                  result,
                                  applycal.PhaseVsUVDetailChart,
                                  ['AMPLITUDE'],
                                  ApplycalPhaseVsUVPlotRenderer, correlation=corrstring)

                self.create_plots(context,
                                  result,
                                  applycal.AmpVsTimeDetailChart,
                                  ['AMPLITUDE', 'PHASE', 'BANDPASS', 'TARGET'],
                                  ApplycalAmpVsTimePlotRenderer, correlation=corrstring)

                self.create_plots(context,
                                  result,
                                  applycal.PhaseVsTimeDetailChart,
                                  ['AMPLITUDE', 'PHASE', 'BANDPASS', 'TARGET'],
                                  ApplycalPhaseVsTimePlotRenderer, correlation=corrstring)

        ctx.update({'amp_vs_freq_plots': amp_vs_freq_summary_plots,
                    'phase_vs_freq_plots': phase_vs_freq_summary_plots,
                    'science_amp_vs_freq_plots': science_amp_vs_freq_summary_plots,
                    'phase_vs_freq_polarization_plots': phase_vs_freq_polarization_plots,
                    'amp_vs_freq_polarization_plots': amp_vs_freq_polarization_plots,
                    'use_pol_plots' : use_pol_plots,
                    'uv_max': uv_max})

    def create_science_plots(self, context, results, correlation):
        """
        Create plots for the science targets, returning two dictionaries of
        vis:[Plots].
        """
        amp_vs_freq_summary_plots = collections.defaultdict(list)
        phase_vs_freq_summary_plots = collections.defaultdict(list)
        amp_vs_uv_summary_plots = collections.defaultdict(list)
        max_uvs = {}

        for result in results:
            # Plot for 1 science field (either 1 science target or for a mosaic 1
            # pointing). The science field that should be chosen is the one with
            # the brightest average amplitude over all spws
            vis = os.path.basename(result.inputs['vis'])
            ms = context.observing_run.get_ms(vis)

            # Ideally, the uvmax of the spectrum (plots 1 and 2)
            # would be set by the appearance of plot 3; that is, if there is
            # no obvious drop in amplitude with uvdist, then use all the data.
            # A simpler compromise would be to use a uvrange that captures the
            # inner half the data.
            max_uvs[vis], uv_range = utils.scale_uv_range(ms)
            LOG.debug('Setting UV range to %s for %s', uv_range, vis)

            brightest_fields = T2_4MDetailsplotsummaryRenderer.get_brightest_fields(ms)

            # Limit to 30 sources via CAS-8737
            # MAX_PLOTS = 30
            # Nplots = (len(brightest_fields.items())/30)+1

            m = context.observing_run.measurement_sets[0]
            alltargetfields = m.get_fields(intent='TARGET')
            Nplots = (len(alltargetfields) // 30) + 1

            targetfields = [field for field in alltargetfields[0:len(alltargetfields):Nplots]]

            plotfields = targetfields

            for field in plotfields:
                plots = self.science_plots_for_result(context,
                                                      result,
                                                      applycal.AmpVsFrequencyPerBasebandSummaryChart,
                                                      [field.id],
                                                      uv_range, correlation=correlation)

                plots_mod = []
                for p in plots:
                    p.parameters['fieldid'] = field.id
                    plots_mod.append(p)

                amp_vs_freq_summary_plots[vis].extend(plots_mod)

            if pipeline.infrastructure.generate_detail_plots(results):
                LOG.info("RENDERER_DETAIL_INFORMATION")
                scans = ms.get_scans(scan_intent='TARGET')
                fields = set()
                for scan in scans:
                    fields.update([field.id for field in scan.fields])

                # Science target detail plots. Note that summary plots go onto the
                # detail pages; we don't create plots per spw or antenna
                self.science_plots_for_result(context,
                                              result,
                                              applycal.AmpVsFrequencyPerBasebandSummaryChart,
                                              fields,
                                              uv_range,
                                              ApplycalAmpVsFreqSciencePlotRenderer, correlation=correlation)

                self.science_plots_for_result(context,
                                              result,
                                              applycal.PhaseVsFrequencySummaryChart,
                                              fields,
                                              uv_range,
                                              ApplycalPhaseVsFreqSciencePlotRenderer, correlation=correlation)

                self.science_plots_for_result(context,
                                              result,
                                              applycal.AmpVsUVBasebandSummaryChart,
                                              fields,
                                              renderer_cls=ApplycalAmpVsUVSciencePlotRenderer, correlation=correlation)

        return (amp_vs_freq_summary_plots, max_uvs)

    def science_plots_for_result(self, context, result, plotter_cls, fields,
                                 uvrange=None, renderer_cls=None, correlation=''):
        # override field when plotting amp/phase vs frequency, as otherwise
        # the field is resolved to a list of all field IDs
        overrides = {'coloraxis': 'spw',
                     'correlation': correlation}
        if uvrange is not None:
            overrides['uvrange'] = uvrange

        m = context.observing_run.measurement_sets[0]
        intentselection = 'TARGET'

        plots = []
        plot_output_dir = os.path.join(context.report_dir, 'stage%s' % result.stage_number)
        calto, _ = applycal_renderer._get_data_selection_for_plot(context, result, [intentselection])

        for field in fields:
            # override field when plotting amp/phase vs frequency, as otherwise
            # the field is resolved to a list of all field IDs
            overrides['field'] = field

            if plotter_cls.__name__ == 'AmpVsFrequencyPerBasebandSummaryChart':
                fieldobjs = m.get_fields(intent=intentselection, field_id=field)
                first_field = fieldobjs[0]
                source_spwobjlist = list(first_field.valid_spws)
                source_spwidlist = [spw.id for spw in source_spwobjlist]
                source_spwidlist.sort()
                # PIPE-1259 - filter out scans that are not TARGET intent
                spwlist_forscan = []
                for spw_id in source_spwidlist:
                    scan = m.get_scans(scan_intent=intentselection, field=field, spw=spw_id)
                    if scan:
                        spwlist_forscan.append(spw_id)
                overrides['spws'] = ','.join([str(spwid) for spwid in spwlist_forscan])

            LOG.info("PlotSummary Plotting:" + 'Field ' + str(field) + ', ' + str(m.get_fields(field_id=field)[0].name))

            plotter = plotter_cls(context, plot_output_dir, calto, intentselection, **overrides)
            plots.extend(plotter.plot())

        for plot in plots:
            plot.parameters['intent'] = [intentselection]

        if renderer_cls is not None:
            renderer = renderer_cls(context, result, plots, **overrides)
            with renderer.get_file() as fileobj:
                fileobj.write(renderer.render())

        return plots

    @staticmethod
    def get_baseband_desc(baseband_spws, spws_select=[]):
        """Get the baseband descriptions from a specifield spw list.

        note: derived from a similar implementation in infrastructure.renderer.htmlrenderer.T2_1DetailsRenderer
        """
        vla_baseband_desc = []
        vla_baseband_centfreq = []

        if not baseband_spws:
            LOG.debug("Baseband name cannot be parsed and will not appear in the weblog.")

        for band in baseband_spws:
            for baseband in baseband_spws[band]:
                spws = []
                minfreqs = []
                maxfreqs = []
                for spwitem in baseband_spws[band][baseband]:
                    if (str([*spwitem][0]) in spws_select) or spws_select == []:
                        spws.append(str([*spwitem][0]))
                        minfreqs.append(spwitem[list(spwitem.keys())[0]][0])
                        maxfreqs.append(spwitem[list(spwitem.keys())[0]][1])
                if len(spws) > 0:
                    bbandminfreq = min(minfreqs)
                    bbandmaxfreq = max(maxfreqs)
                    vla_baseband_desc.append(band.capitalize()+':'+baseband+':  ' + str(bbandminfreq) + ' to ' +
                                             str(bbandmaxfreq))
                    vla_baseband_centfreq.append((bbandminfreq+bbandmaxfreq)/2)
        return vla_baseband_desc, vla_baseband_centfreq

    @staticmethod
    def get_brightest_fields(ms, intent='TARGET'):
        """

        """
        # get IDs for all science spectral windows
        spw_ids = set()
        for scan in ms.get_scans(scan_intent=intent):
            scan_spw_ids = {dd.spw.id for dd in scan.data_descriptions}
            spw_ids.update(scan_spw_ids)

        if intent == 'TARGET':
            science_ids = {spw.id for spw in ms.get_spectral_windows()}
            spw_ids = spw_ids.intersection(science_ids)

        result = collections.OrderedDict()

        by_source_id = lambda field: field.source.id
        fields_by_source_id = sorted(ms.get_fields(intent=intent),
                                     key=by_source_id)
        for source_id, source_fields in itertools.groupby(fields_by_source_id,
                                                          by_source_id):
            fields = list(source_fields)

            # give the sole science target name if there's only one science target
            # in this ms.
            if len(fields) == 1:
                LOG.info('Only one %s target for Source #%s. Bypassing '
                         'brightest target selection.', intent, source_id)
                result[source_id] = fields[0]
                continue

            field = fields[0]
            LOG.warning('Bypassing brightest field selection due to problem '
                        'with visstat. Using Field #%s (%s) for Source #%s'
                        '', field.id, field.name, source_id)
            result[source_id] = field
            continue
            # FIXME: code below here in remainder of for-loop is unreachable

            field_ids = {(f.id, f.name) for f in fields}

            # holds the mapping of field name to mean flux
            average_flux = {}

            # defines the parameters for the visstat job
            job_params = {
                'vis': ms.name,
                'axis': 'amp',
                'datacolumn': 'corrected',
                'spw': ','.join(map(str, spw_ids)),
            }

            # solve circular import problem by importing at run-time
            from pipeline.infrastructure import casa_tasks

            LOG.info('Calculating which %s field has the highest mean flux '
                     'for Source #%s', intent, source_id)
            # run visstat for each scan selection for the target
            for field_id, field_name in field_ids:
                job_params['field'] = str(field_id)
                job = casa_tasks.visstat(**job_params)
                LOG.debug('Calculating statistics for %r (#%s)', field_name, field_id)
                result = job.execute()

                average_flux[(field_id, field_name)] = float(result['CORRECTED']['mean'])

            LOG.debug('Mean flux for %s targets:', intent)
            for (field_id, field_name), v in average_flux.items():
                LOG.debug('\t%r (%s): %s', field_name, field_id, v)

            # find the ID of the field with the highest average flux
            sorted_by_flux = sorted(average_flux.items(), key=operator.itemgetter(1), reverse=True)
            (brightest_id, brightest_name), highest_flux = sorted_by_flux[0]

            LOG.info('%s field %r (%s) has highest mean flux (%s)', intent,
                     brightest_name, brightest_id, highest_flux)
            result[source_id] = brightest_id

        return result

    def create_plots(self, context, results, plotter_cls, intents, renderer_cls=None, **kwargs):
        """
        Create plots and return a dictionary of vis:[Plots].
        """
        d = {}
        for result in results:
            plots = self.plots_for_result(context, result, plotter_cls, intents, renderer_cls, **kwargs)
            d = utils.dict_merge(d, plots)
        return d

    def plots_for_result(self, context, result, plotter_cls, intents, renderer_cls=None, **kwargs):
        vis = os.path.basename(result.inputs['vis'])
        output_dir = os.path.join(context.report_dir, 'stage%s' % result.stage_number)
        calto, str_intents = applycal_renderer._get_data_selection_for_plot(context, result, intents)

        plotter = plotter_cls(context, output_dir, calto, str_intents, **kwargs)
        plots = plotter.plot()
        for plot in plots:
            plot.parameters['intent'] = intents

        d = collections.defaultdict(dict)
        d[vis] = plots

        if renderer_cls is not None:
            renderer = renderer_cls(context, result, plots, **kwargs)
            with renderer.get_file() as fileobj:
                fileobj.write(renderer.render())

        return d

    def calapps_for_result(self, result):
        calapps = collections.defaultdict(list)
        for calapp in result.applied:
            vis = os.path.basename(calapp.vis)
            calapps[vis].append(calapp)
        return calapps

    def caltypes_for_result(self, result):
        type_map = {
            'bandpass': 'Bandpass',
            'gaincal': 'Gain',
            'tsys': 'T<sub>sys</sub>',
            'wvr': 'WVR',
        }

        d = {}
        for calapp in result.applied:
            for calfrom in calapp.calfrom:
                caltype = type_map.get(calfrom.caltype, calfrom.caltype)

                if calfrom.caltype == 'gaincal':
                    # try heuristics to detect phase-only and amp-only
                    # solutions
                    caltype += self.get_gain_solution_type(calfrom.gaintable)

                d[calfrom.gaintable] = caltype

        return d

    def get_gain_solution_type(self, gaintable):
        # solve circular import problem by importing at run-time
        from pipeline.infrastructure import casa_tasks

        # get stats on amp solution of gaintable
        calstat_job = casa_tasks.calstat(caltable=gaintable, axis='amp',
                                         datacolumn='CPARAM', useflags=True)
        calstat_result = calstat_job.execute()
        stats = calstat_result['CPARAM']

        # amp solutions of unity imply phase-only was requested
        tol = 1e-3
        no_amp_soln = all([utils.approx_equal(stats['sum'], stats['npts'], tol),
                           utils.approx_equal(stats['min'], 1, tol),
                           utils.approx_equal(stats['max'], 1, tol)])

        # same again for phase solution
        calstat_job = casa_tasks.calstat(caltable=gaintable, axis='phase',
                                         datacolumn='CPARAM', useflags=True)
        calstat_result = calstat_job.execute()
        stats = calstat_result['CPARAM']

        # phase solutions ~ 0 implies amp-only solution
        tol = 1e-5
        no_phase_soln = all([utils.approx_equal(stats['sum'], 0, tol),
                             utils.approx_equal(stats['min'], 0, tol),
                             utils.approx_equal(stats['max'], 0, tol)])

        if no_phase_soln and not no_amp_soln:
            return ' (amplitude only)'
        if no_amp_soln and not no_phase_soln:
            return ' (phase only)'
        return ''


class ApplycalAmpVsFreqPlotRenderer(basetemplates.JsonPlotRenderer):
    def __init__(self, context, result, plots, **overrides):
        vis = os.path.basename(result.inputs['vis'])
        title = 'Calibrated amplitude vs frequency for %s' % vis
        outfile = filenamer.sanitize('amp_vs_freq-%s.html' % vis)

        super(ApplycalAmpVsFreqPlotRenderer, self).__init__(
            'generic_x_vs_y_field_spw_ant_detail_plots.mako', context,
            result, plots, title, outfile, **overrides)


class ApplycalPhaseVsFreqPlotRenderer(basetemplates.JsonPlotRenderer):
    def __init__(self, context, result, plots, **overrides):
        vis = os.path.basename(result.inputs['vis'])
        title = 'Calibrated phase vs frequency for %s' % vis
        outfile = filenamer.sanitize('phase_vs_freq-%s.html' % vis)

        super(ApplycalPhaseVsFreqPlotRenderer, self).__init__(
            'generic_x_vs_y_field_spw_ant_detail_plots.mako', context,
            result, plots, title, outfile, **overrides)


class ApplycalAmpVsFreqSciencePlotRenderer(basetemplates.JsonPlotRenderer):
    def __init__(self, context, result, plots, **overrides):
        vis = os.path.basename(result.inputs['vis'])
        title = 'Calibrated amplitude vs frequency for %s' % vis
        outfile = filenamer.sanitize('science_amp_vs_freq-%s.html' % vis)

        super(ApplycalAmpVsFreqSciencePlotRenderer, self).__init__(
            'generic_x_vs_y_field_baseband_detail_plots.mako', context,
            result, plots, title, outfile, **overrides)


class ApplycalPhaseVsFreqSciencePlotRenderer(basetemplates.JsonPlotRenderer):
    def __init__(self, context, result, plots, **overrides):
        vis = os.path.basename(result.inputs['vis'])
        title = 'Calibrated phase vs frequency for %s' % vis
        outfile = filenamer.sanitize('science_phase_vs_freq-%s.mako' % vis)

        super(ApplycalPhaseVsFreqSciencePlotRenderer, self).__init__(
            'generic_x_vs_y_field_baseband_detail_plots.html', context,
            result, plots, title, outfile, **overrides)


class ApplycalAmpVsUVSciencePlotRenderer(basetemplates.JsonPlotRenderer):
    def __init__(self, context, result, plots, **overrides):
        vis = os.path.basename(result.inputs['vis'])
        title = 'Calibrated amplitude vs UV distance for %s' % vis
        outfile = filenamer.sanitize('science_amp_vs_uv-%s.html' % vis)

        super(ApplycalAmpVsUVSciencePlotRenderer, self).__init__(
            'generic_x_vs_y_field_baseband_detail_plots.mako', context,
            result, plots, title, outfile, **overrides)


class ApplycalAmpVsUVPlotRenderer(basetemplates.JsonPlotRenderer):
    def __init__(self, context, result, plots, **overrides):
        vis = os.path.basename(result.inputs['vis'])
        title = 'Calibrated amplitude vs UV distance for %s' % vis
        outfile = filenamer.sanitize('amp_vs_uv-%s.html' % vis)

        super(ApplycalAmpVsUVPlotRenderer, self).__init__(
            'generic_x_vs_y_spw_ant_plots.mako', context,
            result, plots, title, outfile, **overrides)


class ApplycalPhaseVsUVPlotRenderer(basetemplates.JsonPlotRenderer):
    def __init__(self, context, result, plots, **overrides):
        vis = os.path.basename(result.inputs['vis'])
        title = 'Calibrated phase vs UV distance for %s' % vis
        outfile = filenamer.sanitize('phase_vs_uv-%s.html' % vis)

        super(ApplycalPhaseVsUVPlotRenderer, self).__init__(
            'generic_x_vs_y_spw_ant_plots.mako', context,
            result, plots, title, outfile, **overrides)


class ApplycalAmpVsTimePlotRenderer(basetemplates.JsonPlotRenderer):
    def __init__(self, context, result, plots, **overrides):
        vis = os.path.basename(result.inputs['vis'])
        title = 'Calibrated amplitude vs times for %s' % vis
        outfile = filenamer.sanitize('amp_vs_time-%s.html' % vis)

        super(ApplycalAmpVsTimePlotRenderer, self).__init__(
            'generic_x_vs_y_field_spw_ant_detail_plots.mako', context,
            result, plots, title, outfile, **overrides)


class ApplycalPhaseVsTimePlotRenderer(basetemplates.JsonPlotRenderer):
    def __init__(self, context, result, plots, **overrides):
        vis = os.path.basename(result.inputs['vis'])
        title = 'Calibrated phase vs times for %s' % vis
        outfile = filenamer.sanitize('phase_vs_time-%s.html' % vis)

        super(ApplycalPhaseVsTimePlotRenderer, self).__init__(
            'generic_x_vs_y_field_spw_ant_detail_plots.mako', context,
            result, plots, title, outfile, **overrides)
