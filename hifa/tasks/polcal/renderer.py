import collections
import os

import pipeline.infrastructure.callibrary as callibrary
import pipeline.infrastructure.filenamer as filenamer
import pipeline.infrastructure.logging as logging
import pipeline.infrastructure.renderer.basetemplates as basetemplates
import pipeline.infrastructure.tablereader as tablereader
import pipeline.infrastructure.utils as utils
from pipeline.h.tasks.common.displays import polcal

LOG = logging.get_logger(__name__)

ResidPolTR = collections.namedtuple('ResidPolTR', 'session field spw I Q U V')


class T2_4MDetailsPolcalRenderer(basetemplates.T2_4MDetailsDefaultRenderer):
    """
    Renders detailed HTML output for the Polcal task.
    """
    def __init__(self, uri='polcal.mako',
                 description='polarization Calibration',
                 always_rerender=False):
        super().__init__(uri=uri, description=description, always_rerender=always_rerender)

    def update_mako_context(self, mako_context, pipeline_context, results):
        # As a multi-vis task, there is only 1 Result for Polcal.
        result = results[0]
        output_dir = os.path.join(pipeline_context.report_dir, 'stage%s' % result.stage_number)

        # Create local copy of pipeline context and register the polarization
        # session MSes, to enable creation of session related plots that rely
        # on the MS being registered.
        pipeline_context = self.create_copy_pcontext_with_session_mses(pipeline_context, result)

        # Initialize required output for weblog.
        session_names = []
        vislists = {}
        refants = {}
        polfields = {}
        scanid_highest_xy = {}

        # Retrieve info for each session.
        for session_name, session_results in result.session.items():
            # Store session name and corresponding vislist.
            session_names.append(session_name)
            vislists[session_name] = session_results.vislist

            # Store pol cal field name and refant.
            refants[session_name] = session_results.refant
            polfields[session_name] = session_results.polcal_field_name

            # Store ID of scan with highest XY signal.
            scanid_highest_xy[session_name] = session_results.best_scan_id

            # Add stage number to session result, needed by steps that render
            # based on session result.
            session_results.stage_number = result.stage_number

        # Create residual polarization table.
        residual_pol_table_rows = self.create_pol_table_rows(result, 'residual')

        # Create polarization calibrator table.
        polcal_table_rows = self.create_pol_table_rows(result, 'polcal')

        # Create amp vs. parallactic angle plots.
        amp_vs_parang = self.create_amp_parang_plots(pipeline_context, output_dir, result)

        # Create gain amp polarization ratio vs. scan plots.
        amp_vs_scan = self.create_amp_scan_plots(pipeline_context, result)

        # Create cross-hand phase vs. channel plots.
        phase_vs_channel = self.create_phase_channel_plots(pipeline_context, result)

        # Create gain ratio RMS plots.
        gain_ratio_rms_vs_scan = self.create_gain_ratio_rms_plots(pipeline_context, output_dir, result)

        # Create leakage solution real/imag gain vs. channel plots, per ant.
        leak_summary, leak_subpages = self.create_leakage_vs_channel_plots(pipeline_context, result)

        # Create XY gain amplitude vs. antenna plots.
        amp_vs_ant, ampratio_vs_ant = self.create_xy_amp_ant_plots(pipeline_context, result)

        # Render real vs. imaginary corrected XX,XY and XY,YX plots for the session.
        real_vs_imag = self.create_real_imag_plots(pipeline_context, output_dir, result)

        # Update the mako context.
        mako_context.update({
            'session_names': session_names,
            'vislists': vislists,
            'refants': refants,
            'polfields': polfields,
            'scanid_highest_xy': scanid_highest_xy,
            'residual_pol_table_rows': residual_pol_table_rows,
            'polcal_table_rows': polcal_table_rows,
            'amp_vs_parang': amp_vs_parang,
            'amp_vs_scan': amp_vs_scan,
            'phase_vs_channel': phase_vs_channel,
            'gain_ratio_rms_vs_scan': gain_ratio_rms_vs_scan,
            'leak_summary': leak_summary,
            'leak_subpages': leak_subpages,
            'amp_vs_ant': amp_vs_ant,
            'ampratio_vs_ant': ampratio_vs_ant,
            'real_vs_imag': real_vs_imag,
        })

    @staticmethod
    def create_pol_table_rows(result, tabletype):
        rows = []

        for session_name, session_results in result.session.items():
            # Retrieve the correct dictionary with polarization results based
            # on type of table.
            if tabletype == 'residual':
                polcal_dict = session_results.cal_pfg_result
            elif tabletype == 'polcal' and session_results.polcal_phase_result is not None:
                polcal_dict = session_results.polcal_phase_result.polcal_returns[0]
            else:
                polcal_dict = {}

            # Create rows for each field and each SpW.
            for field, fres in polcal_dict.items():
                for spw, iquv in fres.items():
                    iquv = [f"{i:.6f}" for i in iquv]
                    rows.append(ResidPolTR(session_name, field, spw[3:].replace('Ave', 'Average'), *iquv))

        return utils.merge_td_columns(rows)

    @staticmethod
    def create_copy_pcontext_with_session_mses(context, result):
        # Create a copy of the pipeline context.
        LOG.debug("Creating local copy of pipeline context for weblog rendering.")
        context = utils.pickle_copy(context)

        # Register each session MS in this new copy of the pipeline context.
        for session_results in result.session.values():
            if session_results.vis:
                LOG.debug(f"Registering session MS {session_results.vis} to local copy of pipeline context for"
                          f" weblog rendering.")
                session_ms = tablereader.MeasurementSetReader.get_measurement_set(session_results.vis)
                context.observing_run.add_measurement_set(session_ms)
            else:
                LOG.debug(f"No session MS found for session {session_results.session}, unable to render.")

        return context

    @staticmethod
    def create_amp_parang_plots(context, output_dir, result):
        plots = {}
        for session_name, session_results in result.session.items():
            if session_results.vis:
                calto = callibrary.CalTo(vis=session_results.vis)
                plots[session_name] = polcal.AmpVsParangSummaryChart(context, output_dir, calto).plot()

        return plots

    @staticmethod
    def create_amp_scan_plots(context, result):
        plots = collections.defaultdict(list)
        for session_name, session_results in result.session.items():
            if session_results.init_gcal_result is None:
                continue

            # Create amp vs scan plots for 'before' calibration.
            plots_before = polcal.AmpVsScanChart(context, result, session_results.init_gcal_result.final).plot()
            # Add before/after calibration to plot parameters for display on
            # weblog page.
            for plot in plots_before:
                plot.parameters['calib'] = "before"
            plots[session_name].extend(plots_before)

            # Create amp vs scan plots for 'after' calibration.
            plots_after = polcal.AmpVsScanChart(context, result, session_results.final_gcal_result.final).plot()
            # Add before/after calibration to plot parameters for display on
            # weblog page.
            for plot in plots_after:
                plot.parameters['calib'] = "after"
            plots[session_name].extend(plots_after)

        return plots

    @staticmethod
    def create_phase_channel_plots(context, result):
        plots = {}
        for session_name, session_results in result.session.items():
            if session_results.polcal_phase_result is not None:
                plots[session_name] = polcal.PhaseVsChannelChart(
                    context, result, session_results.polcal_phase_result.final).plot()

        return plots

    @staticmethod
    def create_gain_ratio_rms_plots(context, output_dir, result):
        plots = {}
        for session_name, sresults in result.session.items():
            if sresults.vis:
                plots[session_name] = polcal.GainRatioRMSVsScanChart(context, output_dir, sresults).plot()

        return plots

    @staticmethod
    def create_leakage_vs_channel_plots(context, result):
        summary_plots = collections.defaultdict(list)
        detail_plots = []
        subpages = {}

        for session_name, session_results in result.session.items():
            if session_results.leak_polcal_result is None:
                continue
            # Create the summary plots.
            summary_plots[session_results.vis].extend(polcal.XVsChannelSummaryChart(
                 context, result, session_results.leak_polcal_result.final, yaxis='real').plot())
            summary_plots[session_results.vis].extend(polcal.XVsChannelSummaryChart(
                 context, result, session_results.leak_polcal_result.final, yaxis='imag').plot())
            # Add y-axis to plot.parameters for display on weblog page.
            for plot in summary_plots[session_results.vis]:
                plot.parameters['yaxis'] = plot.y_axis.capitalize()

            # Create the detailed plots.
            detail_plots.extend(polcal.XVsChannelDetailChart(
                context, result, session_results.leak_polcal_result.final, yaxis='real').plot())
            detail_plots.extend(polcal.XVsChannelDetailChart(
                context, result, session_results.leak_polcal_result.final, yaxis='imag').plot())

        # Add y-axis to plot.parameters for display on weblog page.
        for plot in detail_plots:
            plot.parameters['yaxis'] = plot.y_axis.capitalize()

        # Render a single subpage with the detailed plots for all sessions, and
        # register this joint plot subpage for each session MS.
        renderer = PolcalLeakagePlotRenderer(context, result, detail_plots)
        with renderer.get_file() as fileobj:
            fileobj.write(renderer.render())
            outfile = os.path.basename(renderer.path)
        for session_results in result.session.values():
            subpages[session_results.vis] = outfile

        return summary_plots, subpages

    @staticmethod
    def create_xy_amp_ant_plots(context, result):
        plots_amp, plots_ampratio = {}, {}
        for session_name, session_results in result.session.items():
            if session_results.xyratio_gcal_result is None:
                continue
            plots_amp[session_name] = polcal.AmpVsAntennaChart(
                context, result, session_results.xyratio_gcal_result.final).plot()
            plots_ampratio[session_name] = polcal.AmpVsAntennaChart(
                context, result, session_results.xyratio_gcal_result.final, correlation='/').plot()

        return plots_amp, plots_ampratio

    @staticmethod
    def create_real_imag_plots(context, output_dir, result):
        plots = {}
        for session_name, session_results in result.session.items():
            if not session_results.vis:
                continue
            calto = callibrary.CalTo(vis=session_results.vis)
            plots[session_name] = polcal.RealVsImagChart(context, output_dir, calto, correlation='XX,YY').plot()
            plots[session_name].extend(polcal.RealVsImagChart(context, output_dir, calto, correlation='XY,YX').plot())

        return plots


class PolcalLeakagePlotRenderer(basetemplates.JsonPlotRenderer):
    """
    Renders the page with the detailed plots for the leakage solutions gain vs
    channel.
    """
    def __init__(self, context, result, plots):
        vis = utils.get_vis_from_plots(plots)

        title = f"Leakage gain vs channels for {vis}"
        outfile = filenamer.sanitize(f"leakage_gain_vs_channels-{vis}.html")

        super().__init__('polcal_leakage_plots.mako', context, result, plots, title, outfile)
