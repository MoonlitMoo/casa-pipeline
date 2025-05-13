import collections
import os
import shutil
from typing import Dict, List

import pipeline.infrastructure.logging as logging
import pipeline.infrastructure.renderer.basetemplates as basetemplates
import pipeline.infrastructure.renderer.logger as logger
import pipeline.infrastructure.utils as utils
from pipeline.infrastructure.launcher import Context
from pipeline.infrastructure.basetask import ResultsList
from pipeline.hifa.tasks.spwphaseup import display

LOG = logging.get_logger(__name__)

PhaseTR = collections.namedtuple('PhaseTR', 'ms phase_field field_names')
SnrTR = collections.namedtuple('SnrTR', 'ms threshold field intent spw snr')
SpwMapInfo = collections.namedtuple('SpwMapInfo', 'ms intent field fieldid combine spwmap scanids scispws')
SpwPhaseupApplication = collections.namedtuple('SpwPhaseupApplication', 'ms gaintable calmode solint intent spw')
PhaseRmsTR = collections.namedtuple('PhaseRmsTR', 'ms type time median_phase_rms noisy_ant')


class T2_4MDetailsSpwPhaseupRenderer(basetemplates.T2_4MDetailsDefaultRenderer):
    def __init__(self, uri='spwphaseup.mako',
                 description='Spw phase offsets calibration',
                 always_rerender=False):
        super().__init__(uri=uri, description=description, always_rerender=always_rerender)

    def update_mako_context(self, ctx, context, results):
        # Get info on spectral window mappings.
        spwmaps = get_spwmaps(context, results)

        # Generate rows for phase SNR table.
        snr_table_rows = get_snr_table_rows(context, results)

        # Generate rows for phase calibrator mapping table.
        pcal_table_rows = get_pcal_table_rows(context, results)

        # Get info on phase caltable.
        applications = get_gaincal_applications(context, results)

        # Get info on the phase RMS spatial structure function plots and tables
        if results[0].phaserms_results:
            rmsplots = make_rms_plots(context, results)
            phaserms_table_rows = get_phaserms_table_rows(context, results)
        else:
            rmsplots = None
            phaserms_table_rows = None

        # Update mako context.
        ctx.update({
            'applications': applications,
            'pcal_table_rows': pcal_table_rows,
            'snr_table_rows': snr_table_rows,
            'phaserms_table_rows': phaserms_table_rows,
            'spwmaps': spwmaps,
            'rmsplots': rmsplots
        })


def get_gaincal_applications(context: Context, results: ResultsList) -> List[SpwPhaseupApplication]:
    """
    Return list of SpwPhaseupApplication entries that contain all the necessary
    information to show in a Phase-up caltable application table in the task
    weblog page.

    Args:
        context: the pipeline context.
        results: list of task results.

    Returns:
        List of SpwPhaseupApplication instances.
    """
    applications = []

    calmode_map = {
        'p': 'Phase only',
        'a': 'Amplitude only',
        'ap': 'Phase and amplitude'
    }

    for result in results:
        ms = context.observing_run.get_ms(result.vis)

        for calapp in result.phaseup_result.final:
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

            to_intent = ', '.join(calapp.intent.split(','))
            if to_intent == '':
                to_intent = 'ALL'

            calmode = utils.get_origin_input_arg(calapp, 'calmode')
            calmode = calmode_map.get(calmode, calmode)

            applications.append(SpwPhaseupApplication(ms.basename, gaintable, solint, calmode, to_intent, spw))

    return applications


def get_spwmaps(context: Context, results: ResultsList) -> List[SpwMapInfo]:
    """
    Return list of SpwMapInfo entries that contain all the necessary
    information to be shown in a Spectral Window Mapping table in the task
    weblog page.

    Args:
        context: the pipeline context.
        results: list of task results.

    Returns:
        List of SpwMapInfo instances.
    """
    spwmaps = []

    for result in results:
        ms = context.observing_run.get_ms(result.vis)

        # Get science spws
        science_spw_ids = [spw.id for spw in ms.get_spectral_windows(science_windows_only=True)]

        if result.spwmaps:
            for (intent, field), spwmapping in result.spwmaps.items():
                # Get ID of field and scans.
                fieldid = ms.get_fields(name=[field])[0].id
                scanids = ", ".join(str(scan.id) for scan in ms.get_scans(scan_intent=intent, field=field))

                # Append info on spwmap to list.
                spwmaps.append(SpwMapInfo(ms.basename, intent, field, fieldid, spwmapping.combine, spwmapping.spwmap,
                                          scanids, science_spw_ids))
        else:
            spwmaps.append(SpwMapInfo(ms.basename, '', '', '', '', '', '', ''))

    return spwmaps


def get_pcal_table_rows(context: Context, results: ResultsList) -> List[str]:
    """
    Return list of strings containing HTML TD columns, representing rows for
    the phase calibrator mapping table.

    Args:
        context: the pipeline context.
        results: list of task results.

    Returns:
        List of strings containing rows for phase calibrator mapping table.
    """
    rows = []
    for result in results:
        if result.phasecal_mapping:
            ms = context.observing_run.get_ms(result.vis)
            for pfield, tcfields in result.phasecal_mapping.items():
                # Compose phase field string.
                pfieldid = ms.get_fields(name=[pfield])[0].id
                field_str = f"{pfield} (#{pfieldid})"

                rows.append(PhaseTR(ms.basename, field_str, ", ".join(sorted(tcfields))))

    return utils.merge_td_columns(rows)


def get_snr_table_rows(context: Context, results: ResultsList) -> List[str]:
    """
    Return list of strings containing HTML TD columns, representing rows for
    the phase SNR table.

    Args:
        context: the pipeline context.
        results: list of task results.

    Returns:
        List of strings containing rows for phase SNR table.
    """
    rows = []
    for result in results:
        ms = context.observing_run.get_ms(result.vis)
        if result.spwmaps:
            # Get phase SNR threshold, and present this in the table if the phase
            # SNR test was run during task.
            threshold = result.inputs['phasesnr']
            spwmapmode = result.inputs['hm_spwmapmode']
            if spwmapmode == 'auto':
                thr_str = str(threshold)
            else:
                thr_str = f"N/A <p>(hm_spwmapmode='{spwmapmode}')"

            for (intent, field), spwmapping in result.spwmaps.items():
                # Compose field string.
                fieldid = ms.get_fields(name=[field])[0].id
                field_str = f"{field} (#{fieldid})"

                # For each SpW in SNR info, create a row, and highlight when
                # the SNR was missing or below the phase SNR threshold.
                for row in spwmapping.snr_info:
                    spwid = row[0]
                    if row[1] is None:
                        snr = '<strong class="alert-danger">N/A</strong>'
                    elif row[1] < threshold:
                        snr = f'<strong class="alert-danger">{row[1]:.1f}</strong>'
                    else:
                        snr = f'{row[1]:.1f}'

                    rows.append(SnrTR(ms.basename, thr_str, field_str, intent, spwid, snr))
        else:
            rows.append(SnrTR(ms.basename, '', '', '', '', ''))

    return utils.merge_td_columns(rows)


def get_phaserms_table_rows(context: Context, results: ResultsList) -> List[str]:
    """
    Return list of strings containing HTML TD columns, representing rows for
    the decoherence assessment phase rms results table. (SEE PIPE-692)

    Args:
        context: the pipeline context.
        results: list of task results.

    Returns:
        List of strings containing rows for phase rms table.
    """
    rows = []
    for result in sorted(results, key=lambda result: result.vis):
        if result.phaserms_results:
            ms = context.observing_run.get_ms(result.vis)
            if result.phaserms_antout == '':
                result.phaserms_antout = "None"
            total_time = f'{result.phaserms_totaltime:.1f}'
            cycle_time = f'{result.phaserms_cycletime:.1f}'
            phasermsp80 = result.phaserms_results['phasermsP80']
            phasermscyclep80 = result.phaserms_results['phasermscycleP80']
            phaserms_totaltime = f'{phasermsp80:.2f}'
            phaserms_cycletime = f'{phasermscyclep80:.2f}'
            
            rows.append(PhaseRmsTR(ms.basename, 'Total Time', total_time,
                        phaserms_totaltime, result.phaserms_antout))
            rows.append(PhaseRmsTR(ms.basename, 'Cycle Time', cycle_time,
                        phaserms_cycletime, result.phaserms_antout))
    return utils.merge_td_columns(rows)


def make_rms_plots(context, results) -> Dict[str, List[logger.Plot]]:
    """
    Create and return a list of the Spatial Structure Functions (SSF) plots. 
    (See PIPE-692)

    Args:
        results: the spwphaseup results. 
    Returns:
        summary_plots: dictionary with MS
                    as the keys and lists of plot objects as the values
    """
    rmsplots = collections.defaultdict(list)

    for result in results: 
        if result.phaserms_results:
            rmsplotter = display.SpatialStructureFunctionChart(context, result)
            vis = os.path.basename(result.inputs['vis'])
            rmsplots[vis] = rmsplotter.plot()

    return rmsplots