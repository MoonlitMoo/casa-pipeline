"""
Created on 29 Oct 2014

@author: sjw
"""
import collections
import os

import pipeline.infrastructure
import pipeline.infrastructure.filenamer as filenamer
import pipeline.infrastructure.logging as logging
import pipeline.infrastructure.renderer.basetemplates as basetemplates
import pipeline.infrastructure.utils as utils
from . import display as gaincal_displays

LOG = logging.get_logger(__name__)

GaincalApplication = collections.namedtuple('GaincalApplication',
                                            'ms gaintable calmode solint intent field spw gainfield')


class T2_4MDetailsGaincalRenderer(basetemplates.T2_4MDetailsDefaultRenderer):
    def __init__(self, uri='timegaincal.mako', description='Gain calibration', always_rerender=False):
        super().__init__(uri=uri, description=description, always_rerender=always_rerender)

    def update_mako_context(self, ctx, context, results):
        applications = []
        spw_mapping = {}
        spw_mapping_without_check = {}

        amp_vs_time_summaries = collections.defaultdict(list)
        phase_vs_time_summaries = {}
        amp_vs_time_details = {}
        phase_vs_time_details = {}

        diagnostic_amp_vs_time_summaries = collections.defaultdict(list)
        diagnostic_phase_vs_time_summaries = {}
        diagnostic_phaseoffset_vs_time_summaries = {}
        diagnostic_amp_vs_time_details = {}
        diagnostic_phase_vs_time_details = {}
        diagnostic_phaseoffset_vs_time_details = {}

        amp_vs_time_subpages = {}
        phase_vs_time_subpages = {}
        diagnostic_amp_vs_time_subpages = {}
        diagnostic_phase_vs_time_subpages = {}
        diagnostic_phaseoffset_vs_time_subpages = {}

        diagnostic_solints = collections.defaultdict(dict)

        for result in results:
            vis = os.path.basename(result.inputs['vis'])
            ms = context.observing_run.get_ms(vis)

            # Get a string summarizing the SpW mapping info for current MS.
            spw_mapping[vis] = self.get_spw_mappings(ms)

            # Get a string summarizing the Spw mapping info for the current MS, omitting any CHECK sources.
            spw_mapping_without_check[vis] = self.get_spw_mappings(ms, omit_intent='CHECK')

            # Get gain cal applications for current MS.
            ms_applications = self.get_gaincal_applications(context, result, ms)
            applications.extend(ms_applications)

            try:
                # diagnostic phase vs time plots are made from a caltable for BANDPASS
                diag_phase = [a for a in ms_applications
                              if a.calmode == 'Phase only'
                              and 'BANDPASS' in a.intent][0]
                solint = 'int' if 'Per integration' in diag_phase.solint else diag_phase.solint
                diagnostic_solints[vis]['phase'] = solint
            except IndexError:
                diagnostic_solints[vis]['phase'] = 'N/A'

            try:
                diag_calapp = result.calampresult.final[0]
                diag_solint = utils.get_origin_input_arg(diag_calapp, 'solint')
                diagnostic_solints[vis]['amp'] = diag_solint
            except IndexError:
                diagnostic_solints[vis]['amp'] = 'N/A'

            # Identify set of antennas of same antenna diameter.
            ant_diameters = {antenna.diameter for antenna in ms.antennas}

            # result.final calapps contains p solution for solint=int,inf and a
            # solution for solint=inf.

            # PIPE-125: for amp-vs-time plots, if there are multiple antenna
            # diameters present in this dataset, then create separate plots
            # for each diameter; otherwise create a single summary plot without
            # explicitly restricting antennas (to avoid long plot title).
            if len(ant_diameters) > 1:
                for antdiam in ant_diameters:
                    ants = ','.join([str(antenna.id) for antenna in ms.antennas if antenna.diameter == antdiam])

                    # Generate the amp-vs-time plots.
                    plotter = gaincal_displays.GaincalAmpVsTimeSummaryChart(context, result, sorted(result.final, key=lambda cal: cal.gaintable), 'TARGET', ant=ants)
                    plot_wrappers = plotter.plot()
                    # Add diameter info to plot wrappers and store wrappers.
                    for wrapper in plot_wrappers:
                        wrapper.parameters['antdiam'] = antdiam
                    amp_vs_time_summaries[vis].extend(plot_wrappers)

                    # Generate diagnostic amp vs time plots for bandpass solution.
                    # Create copy of CalApplication for subset of antennas with
                    # current antenna diameter.
                    plotter = gaincal_displays.GaincalAmpVsTimeSummaryChart(context, result, sorted(result.calampresult.final, key=lambda cal: cal.gaintable), '', ant=ants)
                    plot_wrappers = plotter.plot()
                    # Add diameter info to plot wrappers and store wrappers.
                    for wrapper in plot_wrappers:
                        wrapper.parameters['antdiam'] = antdiam
                    diagnostic_amp_vs_time_summaries[vis].extend(plot_wrappers)
            else:
                # Generate the amp-vs-time plots.
                plotter = gaincal_displays.GaincalAmpVsTimeSummaryChart(context, result, sorted(result.final, key=lambda cal: cal.gaintable), 'TARGET')
                plot_wrappers = plotter.plot()
                amp_vs_time_summaries[vis].extend(plot_wrappers)

                # Generate diagnostic amp vs time plots for bandpass solution.
                plotter = gaincal_displays.GaincalAmpVsTimeSummaryChart(context, result, result.calampresult.final, '')
                plot_wrappers = plotter.plot()
                diagnostic_amp_vs_time_summaries[vis].extend(plot_wrappers)

            # generate phase vs time plots
            plotter = gaincal_displays.GaincalPhaseVsTimeSummaryChart(context, result, sorted(result.final, key=lambda cal: cal.gaintable), 'TARGET')
            phase_vs_time_summaries[vis] = plotter.plot()

            # generate diagnostic phase vs time plots for bandpass solution, i.e. 
            # with solint=int
            #
            # Retrieve the phase solutions from hifa_timegaincal that were saved off specifically to make this plot
            # (See: PIPE-1377 for more information)             
            diagnostic_phase_calapps = result.phasecal_for_phase_plot
            # Add the CHECK sources from hifa_gfluxscale to this plot (see: PIPE-1377)
            if ms.phase_calapps_for_check_sources:
                diagnostic_phase_calapps.extend(ms.phase_calapps_for_check_sources)
            else: 
                LOG.info('Could not find check source solutions from hifa_gfluxscale, omitting from diagnostic phase vs. time plot for {}.'.format(ms.name))

            diagnostic_phase_calapps.sort(key=lambda cal: cal.gaintable)

            # There's no need to pass specific intents for this, because the list is already limited to the appropriate plots.
            plotter = gaincal_displays.GaincalPhaseVsTimeSummaryChart(context, result, diagnostic_phase_calapps, '') 
            diagnostic_phase_vs_time_summaries[vis] = plotter.plot()

            # generate diagnostic phase offset vs time plots
            if result.phaseoffsetresult is not None:
                plotter = gaincal_displays.GaincalPhaseVsTimeSummaryChart(context, result,
                                                                          result.phaseoffsetresult.final, '')
                diagnostic_phaseoffset_vs_time_summaries[vis] = plotter.plot()

                # PIPE-1762: add information about spw mapping (if any)
                for plot in diagnostic_phaseoffset_vs_time_summaries[vis]:
                    plotspw = plot.parameters['spw']
                    spw_combined = []  # list of fields in which the spws are combined
                    spw_mapped = []  # list of target spws and fields in which plotspw is mapped to a target spw
                    num_fields_with_phase_intent = 0
                    for ifld, spwmap in ms.spwmaps.items():
                        if ifld.intent != 'PHASE':
                            continue
                        num_fields_with_phase_intent += 1
                        if spwmap.combine:
                            spw_combined.append(ifld.field)
                        elif spwmap.spwmap and spwmap.spwmap[plotspw] != plotspw:
                            # only add the message if the spw is not mapped onto itself
                            spw_mapped.append((spwmap.spwmap[plotspw], ifld.field))
                    # add a message to the caption of diagnostic phase offset plot in case that the spw is mapped.
                    # in the case of more than one phase calibrator, append the field name in brackets to the spw name.
                    captionmessage = []
                    for targetspw, field in spw_mapped:
                        captionmessage.append('spw {}{}'.format(
                            targetspw, ' ({})'.format(field) if num_fields_with_phase_intent > 1 else ''))
                    if spw_combined:
                        captionmessage.append('all spws combined' + (' ({})'.format(', '.join(spw_combined))
                                                                     if num_fields_with_phase_intent > 1 else ''))
                    if captionmessage:
                        plot.captionmessage = ('This spw is calibrated using the phase solution for {}.'.format(
                            ', '.join(captionmessage)))

            # Generate detailed plots and render corresponding sub-pages.
            if pipeline.infrastructure.generate_detail_plots(result):
                # phase vs time plots
                plotter = gaincal_displays.GaincalPhaseVsTimeDetailChart(context, result, result.final, 'TARGET')
                phase_vs_time_details[vis] = plotter.plot()
                renderer = GaincalPhaseVsTimePlotRenderer(context, result, phase_vs_time_details[vis])
                with renderer.get_file() as fileobj:
                    fileobj.write(renderer.render())
                    phase_vs_time_subpages[vis] = renderer.path

                # amp vs time plots
                plotter = gaincal_displays.GaincalAmpVsTimeDetailChart(context, result, result.final, 'TARGET')
                amp_vs_time_details[vis] = plotter.plot()
                renderer = GaincalAmpVsTimePlotRenderer(context, result, amp_vs_time_details[vis])
                with renderer.get_file() as fileobj:
                    fileobj.write(renderer.render())        
                    amp_vs_time_subpages[vis] = renderer.path

                # phase vs time for solint=int
                # There's no need to pass specific intents, because the list is already limited to the appropriate plots.
                plotter = gaincal_displays.GaincalPhaseVsTimeDetailChart(context, result, diagnostic_phase_calapps, '')
                diagnostic_phase_vs_time_details[vis] = plotter.plot()
                renderer = GaincalPhaseVsTimeDiagnosticPlotRenderer(context, result,
                                                                    diagnostic_phase_vs_time_details[vis])
                with renderer.get_file() as fileobj:
                    fileobj.write(renderer.render())        
                    diagnostic_phase_vs_time_subpages[vis] = renderer.path

                # amp vs time plots for solint=int
                plotter = gaincal_displays.GaincalAmpVsTimeDetailChart(context, result, result.calampresult.final, '')
                diagnostic_amp_vs_time_details[vis] = plotter.plot()
                renderer = GaincalAmpVsTimeDiagnosticPlotRenderer(context, result, diagnostic_amp_vs_time_details[vis])
                with renderer.get_file() as fileobj:
                    fileobj.write(renderer.render())        
                    diagnostic_amp_vs_time_subpages[vis] = renderer.path

                # diagnostic phaseoffset vs time plots for solint=inf
                if result.phaseoffsetresult is not None:
                    plotter = gaincal_displays.GaincalPhaseVsTimeDetailChart(context, result,
                                                                             result.phaseoffsetresult.final, '')
                    diagnostic_phaseoffset_vs_time_details[vis] = plotter.plot()
                    renderer = GaincalPhaseOffsetVsTimeDiagnosticPlotRenderer(
                        context, result, diagnostic_phaseoffset_vs_time_details[vis])
                    with renderer.get_file() as fileobj:
                        fileobj.write(renderer.render())        
                        diagnostic_phaseoffset_vs_time_subpages[vis] = renderer.path

        # render plots for all EBs in one page
        for d, plotter_cls, subpages in (
                (amp_vs_time_details, GaincalAmpVsTimePlotRenderer, amp_vs_time_subpages),
                (phase_vs_time_details, GaincalPhaseVsTimePlotRenderer, phase_vs_time_subpages),
                (diagnostic_amp_vs_time_details, GaincalAmpVsTimeDiagnosticPlotRenderer, diagnostic_amp_vs_time_subpages),
                (diagnostic_phase_vs_time_details, GaincalPhaseVsTimeDiagnosticPlotRenderer, diagnostic_phase_vs_time_subpages),
                (diagnostic_phaseoffset_vs_time_details, GaincalPhaseOffsetVsTimeDiagnosticPlotRenderer, diagnostic_phaseoffset_vs_time_subpages)):
            if d:
                all_plots = list(utils.flatten([v for v in d.values()]))
                renderer = plotter_cls(context, results, all_plots)
                with renderer.get_file() as fileobj:
                    fileobj.write(renderer.render())
                    # redirect subpage links to master page
                    for vis in subpages:
                        subpages[vis] = renderer.path

        # add the PlotGroups to the Mako context. The Mako template will parse
        # these objects in order to create links to the thumbnail pages we
        # just created
        ctx.update({
            'applications': applications,
            'spw_mapping': spw_mapping,
            'spw_mapping_without_check': spw_mapping_without_check,
            'amp_vs_time_plots': amp_vs_time_summaries,
            'phase_vs_time_plots': phase_vs_time_summaries,
            'diagnostic_amp_vs_time_plots': diagnostic_amp_vs_time_summaries,
            'diagnostic_phase_vs_time_plots': diagnostic_phase_vs_time_summaries,
            'diagnostic_phaseoffset_vs_time_plots': diagnostic_phaseoffset_vs_time_summaries,
            'amp_vs_time_subpages': amp_vs_time_subpages,
            'phase_vs_time_subpages': phase_vs_time_subpages,
            'diagnostic_amp_vs_time_subpages': diagnostic_amp_vs_time_subpages,
            'diagnostic_phase_vs_time_subpages': diagnostic_phase_vs_time_subpages,
            'diagnostic_phaseoffset_vs_time_subpages': diagnostic_phaseoffset_vs_time_subpages,
            'diagnostic_solints': diagnostic_solints
        })

    @staticmethod
    def get_gaincal_applications(context, result, ms):
        applications = []

        calmode_map = {
            'p': 'Phase only',
            'a': 'Amplitude only',
            'ap': 'Phase and amplitude'
        }

        for calapp in result.final:
            solint = utils.get_origin_input_arg(calapp, 'solint')

            if solint == 'inf':
                solint = 'Infinite'

            # Convert solint=int to a real integration time. 
            # solint is spw dependent; science windows usually have the same
            # integration time, though that's not guaranteed.
            if solint == 'int':
                in_secs = ['%0.2fs' % (dt.seconds + dt.microseconds * 1e-6) 
                           for dt in utils.get_intervals(context, calapp)]
                solint = 'Per integration (%s)' % utils.commafy(in_secs, quotes=False, conjunction='or')

            gaintable = os.path.basename(calapp.gaintable)
            spw = ', '.join(calapp.spw.split(','))

            to_intent = ', '.join(sorted(calapp.calto.intent.split(',')))
            if to_intent == '':
                to_intent = 'ALL'

            to_field = ', '.join(sorted(calapp.calto.field.split(',')))

            calmode = utils.get_origin_input_arg(calapp, 'calmode')
            calmode = calmode_map.get(calmode, calmode)

            assert(len(calapp.calfrom) == 1)
            gainfield = calapp.calfrom[0].gainfield

            a = GaincalApplication(ms.basename, gaintable, calmode, solint, to_intent, to_field, spw, gainfield)
            applications.append(a)

        return applications

    @staticmethod
    def get_spw_mappings(ms, omit_intent: str='') -> str:
        combined = []
        mapped = []
        default_mapped = []
        for ifld, spwmap in ms.spwmaps.items():
            # Skip intent in omit_intent
            if omit_intent and (ifld.intent == omit_intent):
                continue

            if spwmap.combine:
                combined.append(f"{ifld.field} ({ifld.intent})")
            else:
                if spwmap.spwmap:
                    mapped.append(f"{ifld.field} ({ifld.intent})")
                else:
                    # If the spwmap is an empty list, use the phrase "default mapped"
                    default_mapped.append(f"{ifld.field} ({ifld.intent})")

        # Construct string summarizing SpW mapping for MS.
        combined_str = f"Spectral windows combined for {', '.join(combined)}." if combined else ""
        mapped_str = f"Spectral windows mapped for {', '.join(mapped)}." if mapped else ""
        default_mapped_str = f"Spectral windows default mapped for {', '.join(default_mapped)}." if default_mapped else ""
        summary_str = ' '.join([combined_str, default_mapped_str, mapped_str]).strip()

        return summary_str


class GaincalPhaseVsTimePlotRenderer(basetemplates.JsonPlotRenderer):
    def __init__(self, context, result, plots):
        vis = utils.get_vis_from_plots(plots)

        title = 'Phase vs time for %s' % vis
        outfile = filenamer.sanitize('phase_vs_time-%s.html' % vis)

        super(GaincalPhaseVsTimePlotRenderer, self).__init__(
            'generic_x_vs_y_spw_ant_plots.mako', context, result, plots, title, outfile)


class GaincalPhaseVsTimeDiagnosticPlotRenderer(basetemplates.JsonPlotRenderer):
    def __init__(self, context, results, plots):
        vis = utils.get_vis_from_plots(plots)

        title = 'Phase vs time for %s' % vis
        outfile = filenamer.sanitize('diagnostic_phase_vs_time-%s.html' % vis)

        if not isinstance(results, collections.abc.Iterable):
            results = [results]

        # collect QA results generated for this vis
        self._qa_data = {}
        for result in results:
            b = os.path.basename(result.inputs['vis'])
            self._qa_data[b] = [v for k, v in result.qa.phase_qa_results_dict.items() if b in k]

        self._score_types = frozenset(['PHASE_SCORE_XY', 'PHASE_SCORE_X2X1'])

        super(GaincalPhaseVsTimeDiagnosticPlotRenderer, self).__init__(
            'diagnostic_phase_vs_time_plots.mako', context, results, plots, title, outfile)

    def update_json_dict(self, json_dict, plot):
        ant_name = plot.parameters['ant']
        spw_id = plot.parameters['spw']

        scores_dict = {}
        try:
            for qa_data in self._qa_data[plot.parameters['vis']]:
                antenna_ids = dict((v, k) for (k, v) in qa_data['QASCORES']['ANTENNAS'].items())
                ant_id = antenna_ids[ant_name]

                for score_type in self._score_types:            
                    average_score = 0.0
                    num_scores = 0

                    phase_field_ids = set(qa_data['PHASE_FIELDS'])
                    if phase_field_ids:
                        # not all PHASE fields have scores, eg. uid://A002/X6a533e/X834.
                        # Avoid KeyErrors by only retrieving scores for those
                        # with scores.
                        fields_with_scores = set(qa_data['QASCORES']['SCORES'].keys())
                        for field_id in phase_field_ids.intersection(fields_with_scores):
                            score = qa_data['QASCORES']['SCORES'][field_id][spw_id][ant_id][score_type]
                            if score == 'C/C':
                                average_score += -0.1
                            else:
                                average_score += score
                            num_scores += 1
                    else:
                        average_score += 1.0
                        num_scores += 1

                    if num_scores != 0:
                        average_score /= num_scores
                    scores_dict[score_type] = average_score

        except:
            scores_dict = dict((score_type, 0.0) for score_type in self._score_types)

        json_dict.update(scores_dict)
        plot.scores = scores_dict


class GaincalAmpVsTimePlotRenderer(basetemplates.JsonPlotRenderer):
    def __init__(self, context, result, plots):
        vis = utils.get_vis_from_plots(plots)

        title = 'Amplitude vs time for %s' % vis
        outfile = filenamer.sanitize('amp_vs_time-%s.html' % vis)

        super(GaincalAmpVsTimePlotRenderer, self).__init__(
            'generic_x_vs_y_spw_ant_plots.mako', context, result, plots, title, outfile)


class GaincalAmpVsTimeDiagnosticPlotRenderer(basetemplates.JsonPlotRenderer):
    def __init__(self, context, result, plots):
        vis = utils.get_vis_from_plots(plots)

        title = 'Amplitude vs time for %s' % vis
        outfile = filenamer.sanitize('diagnostic_amp_vs_time-%s.html' % vis)

        super(GaincalAmpVsTimeDiagnosticPlotRenderer, self).__init__(
            'generic_x_vs_y_spw_ant_plots.mako', context, result, plots, title, outfile)


class GaincalPhaseOffsetVsTimeDiagnosticPlotRenderer(basetemplates.JsonPlotRenderer):
    def __init__(self, context, result, plots):
        vis = utils.get_vis_from_plots(plots)

        title = 'Phase offset vs time for %s' % vis
        outfile = filenamer.sanitize('diagnostic_phaseoffset_vs_time-%s.html' % vis)

        super(GaincalPhaseOffsetVsTimeDiagnosticPlotRenderer, self).__init__(
            'generic_x_vs_y_spw_ant_plots.mako', context, result, plots, title, outfile)
