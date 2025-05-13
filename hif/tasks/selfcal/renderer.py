
import collections
import os
import string

import pipeline.infrastructure.filenamer as filenamer
import pipeline.infrastructure.logging as logging
import pipeline.infrastructure.renderer.basetemplates as basetemplates
import pipeline.infrastructure.utils as utils

from . import display

LOG = logging.get_logger(__name__)


class SelfCalQARenderer(basetemplates.CommonRenderer):
    def __init__(self, context, results, cleantarget, solint):
        super().__init__('selfcalqa.mako', context, results)

        slib = cleantarget['sc_lib']
        target, band = cleantarget['field_name'], cleantarget['sc_band']

        stage_dir = os.path.join(context.report_dir, 'stage%s' % results.stage_number)
        r = results[0]
        outfile = f'{target}_{band}_{solint}.html'
        valid_chars = "_.-%s%s" % (string.ascii_letters, string.digits)
        self.path = os.path.join(stage_dir, filenamer.sanitize(outfile, valid_chars))
        self.rel_path = os.path.relpath(self.path, context.report_dir)

        image_plots, antpos_plots, antpos_predrop_plots, phasefreq_plots, fracflag_plots = display.SelfcalSummary(
            context, r, cleantarget).plot_qa(solint)
        summary_tab, nsol_tab = self.make_summary_table(
            context, r, cleantarget, solint, image_plots, antpos_plots, antpos_predrop_plots, fracflag_plots)

        self.extra_data = {
            'summary_tab': summary_tab,
            'nsol_tab': nsol_tab,
            'target': target,
            'band': band,
            'solint': solint,
            'antpos_plots': antpos_plots,
            'antpos_predrop_plots': antpos_predrop_plots,
            'phasefreq_plots': phasefreq_plots,
            'fracflag_plots': fracflag_plots,
            'slib': slib}

    def update_mako_context(self, mako_context):
        mako_context.update(self.extra_data)

    def make_summary_table(self, context, r, cleantarget, solint, image_plots, antpos_plots, antpos_predrop_plots, fracflag_plots):

        slib = cleantarget['sc_lib']

        # the prior vs. post image comparison table

        rows = []
        row_names = ['Data Type', 'Image', 'SNR', 'RMS', 'Beam']
        vislist = slib['vislist']
        slib_solint = slib[vislist[0]][solint]
        for row_name in row_names:
            if row_name == 'Data Type':
                row_values = ['Prior', 'Post']
            if row_name == 'Image':
                row_values = utils.plots_to_html(image_plots, report_dir=context.report_dir,
                                                 title='Prior/Post Image Comparison')
            if row_name == 'SNR':
                row_values = ['{:0.3f}'.format(slib_solint['SNR_pre']),
                              '{:0.3f}'.format(slib_solint['SNR_post'])]
            if row_name == 'RMS':
                row_values = ['{:0.3f} mJy/bm'.format(slib_solint['RMS_pre']*1e3),
                              '{:0.3f} mJy/bm'.format(slib_solint['RMS_post']*1e3)]
            if row_name == 'Beam':
                row_values = ['{:0.3f}"x{:0.3f}" {:0.3f} deg'.format(
                    slib_solint['Beam_major_pre'],
                    slib_solint['Beam_minor_pre'],
                    slib_solint['Beam_PA_pre']),
                    '{:0.3f}"x{:0.3f}" {:0.3f} deg'.format(
                    slib_solint['Beam_major_post'],
                    slib_solint['Beam_minor_post'],
                    slib_solint['Beam_PA_post'])]
            rows.append([row_name]+row_values)

        # per-vis solution summary table

        nsol_rows = []
        vislist = slib['vislist']
        for vis in vislist:
            nsol_stats = antpos_plots[vis].parameters
            antpos_html = utils.plots_to_html([antpos_plots[vis]], report_dir=context.report_dir)
            if slib['obstype'] == 'mosaic':
                nsol_stats_predrop = antpos_predrop_plots[vis].parameters
                antpos_predrop_html = utils.plots_to_html([antpos_predrop_plots[vis]], report_dir=context.report_dir)

            if fracflag_plots[vis] is not None:
                fracflag_html = utils.plots_to_html([fracflag_plots[vis]], report_dir=context.report_dir)
            else:
                fracflag_html = []

            vis_desc = ('<a class="anchor" id="{0}_summary"></a>'
                        '<a href="#{0}_byant">'
                        '   <b>{0}</b>'
                        '</a>'.format(vis))

            vis_row_names = [vis_desc, 'N Sol.', 'N Flagged Sol.', 'Frac. Flagged Sol.',
                             'Fallback Mode', 'Spwmap', 'Frac. Flagged <br> (per Ant)']
            if fracflag_html:
                vis_row_names += ['Frac. Flagged vs. Baseline']

            for row_name in vis_row_names:
                if row_name == '':
                    row_values = ['Initial', 'Final']
                if row_name == vis_desc:
                    if slib['obstype'] == 'mosaic':
                        row_values = ['<b>Initial</b>', '<b>Final</b>']
                    else:
                        row_values = [row_name]*2
                if row_name == 'N Sol.':
                    if slib['obstype'] == 'mosaic':
                        row_values = [nsol_stats_predrop['nsols'], nsol_stats['nsols']]
                    else:
                        row_values = [nsol_stats['nsols']]
                if row_name == 'N Flagged Sol.':
                    if slib['obstype'] == 'mosaic':
                        row_values = [nsol_stats_predrop['nflagged_sols'], nsol_stats['nflagged_sols']]
                    else:
                        row_values = [nsol_stats['nflagged_sols']]
                if row_name == 'Frac. Flagged Sol.':
                    row_values = []
                    if slib['obstype'] == 'mosaic':
                        if nsol_stats_predrop['nsols'] > 0:
                            row_values += ['{:0.3f} &#37;'.format(nsol_stats_predrop['nflagged_sols'] /
                                                                  nsol_stats_predrop['nsols']*100.)]
                        else:
                            row_values += ['-']
                    if nsol_stats['nsols'] > 0:
                        row_values += ['{:0.3f} &#37;'.format(nsol_stats['nflagged_sols']/nsol_stats['nsols']*100.)]
                    else:
                        row_values += ['-']
                if row_name == 'Fallback Mode':
                    row_value = '-'
                    if solint == 'inf_EB' and 'fallback' in slib[vis][solint]:
                        fallback_mode = slib[vis][solint]['fallback']
                        if fallback_mode == '':
                            row_value = 'None'
                        if fallback_mode == 'combinespw':
                            row_value = 'Combine SPW'
                        if fallback_mode == 'spwmap':
                            row_value = 'SPWMAP'
                        if fallback_mode == 'combinespwpol':
                            row_value = 'Combine SPW & Pol'
                    if slib['obstype'] == 'mosaic':
                        row_values = [row_value]*2
                    else:
                        row_values = [row_value]
                if row_name == 'Spwmap':
                    if slib['obstype'] == 'mosaic':
                        row_values = [slib[vis][solint]['spwmap']]*2
                    else:
                        row_values = [slib[vis][solint]['spwmap']]
                if row_name == 'Frac. Flagged <br> (per Ant)':
                    if slib['obstype'] == 'mosaic':
                        row_values = [' '.join(antpos_predrop_html), ' '.join(antpos_html)]
                    else:
                        row_values = [' '.join(antpos_html)]
                if row_name == 'Frac. Flagged vs. Baseline':
                    if slib['obstype'] == 'mosaic':
                        row_values = [''.join(fracflag_html)]*2
                    else:
                        row_values = [''.join(fracflag_html)]

                nsol_rows.append([row_name]+row_values)

        return utils.merge_td_columns(rows, vertical_align=True), utils.merge_td_rows(utils.merge_td_columns(nsol_rows, vertical_align=True))


class T2_4MDetailsSelfcalRenderer(basetemplates.T2_4MDetailsDefaultRenderer):
    def __init__(self, uri='selfcal.mako',
                 description='Produce rms images',
                 always_rerender=False):
        super().__init__(uri=uri,
                         description=description, always_rerender=always_rerender)

    def make_targets_summary_table(self, r, targets):
        """Make the selfcal targets summary list table."""
        rows = []

        def bool2icon(value):
            if value:
                return '<span class="glyphicon glyphicon-ok"></span>'
            else:
                return '<span class="glyphicon glyphicon-remove"></span>'

        def format_solints(target):
            """
            Formats the solution intervals (solints) for a given target by adding HTML tags to highlight
            the final and attempted solints.

            Args:
                target (dict): A dictionary containing the following keys:
                    - 'sc_lib' (dict): A dictionary representing the solution library.
                        - 'vislist' (list): A list of visibility keys.
                        - 'final_solint' (str, optional): The final solution interval, if successful.
                        - 'final_phase_solint' (str, optional): The final phase solution interval, if successful.
                        - 'SC_success' (bool): Indicates if the solution was successful.
                    - 'sc_solints' (list): A list of solution intervals to be formatted.

            Returns:
                list: A list of formatted solution intervals as strings with HTML tags applied to
                the final and attempted solints.
            """
            slib = target['sc_lib']
            solints = target['sc_solints']
            vislist = slib['vislist']
            vis_keys = set(slib[vislist[-1]])

            # A final solint here presents one successful/applied solint gain solution
            final_solints = []
            if slib.get('SC_success'):
                if 'inf_EB' in solints:
                    final_solints.append('inf_EB')
                if slib.get('final_solint', None) not in (None, 'None'):
                    final_solints.append(slib.get('final_solint'))
                if slib.get('final_phase_solint', None) not in (None, 'None'):
                    final_solints.append(slib.get('final_phase_solint'))
            final_solints = set(final_solints)

            # Aggregate the actually attempted solints
            attempted_solints = set(solint for solint in solints if solint in vis_keys)

            formated_solints = []
            for solint in solints:
                formated_solint = solint
                if solint in final_solints:
                    formated_solint = f"<a style='color:blue'>{formated_solint}</a>"
                if solint in attempted_solints:
                    formated_solint = f"<strong>{formated_solint}</strong>"
                formated_solints.append(formated_solint)

            return formated_solints

        def format_band(band_str):
            """
            Format the band string from selfcal libraries.

            The band strings from selfcal libraries are in the following formats:
                * VLA: EVLA_KU, etc.
                * ALMA: Band_6, etc.
            
            This function converts the band string into a more readable form by:
                1. Stripping any leading/trailing whitespace.
                2. Removing the 'EVLA_' prefix.
                3. Replacing underscores with spaces.
                4. Capitalizing the resulting string.

            Args:
                band_str (str): The band string to format.

            Returns:
                str: The formatted band string.
            """
            return band_str.strip().replace('EVLA_', '').replace('_', ' ').capitalize()

        for target in targets:
            row = []
            valid_chars = "%s%s" % (string.ascii_letters, string.digits)
            id_name = filenamer.sanitize(target['field_name']+'_'+target['sc_band'], valid_chars)
            row.append(f' <a href="#{id_name}">{fm_target(target)}</a> ')
            row.append(format_band(target['sc_band']))
            row.append(utils.find_ranges(target['spw']))
            row.append(target['phasecenter'])
            row.append(target['cell'])
            row.append(target['imsize'])
            row.append(', '.join(format_solints(target)))
            row.append(bool2icon(target['sc_lib']['SC_success']))
            row.append(bool2icon(target['sc_lib']['SC_success'] and r.applycal_result_contline is not None))
            row.append(bool2icon(target['sc_lib']['SC_success'] and r.applycal_result_line is not None))
            rows.append(row)

        return utils.merge_td_columns(rows, vertical_align=True)

    def update_mako_context(self, ctx, context, results):

        stage_dir = os.path.join(context.report_dir, 'stage%s' % results.stage_number)
        r = results[0]
        cleantargets = [cleantarget for cleantarget in r.targets if not cleantarget['sc_exception']]
        is_restore = r.is_restore

        if cleantargets:
            targets_summary_table = self.make_targets_summary_table(r, cleantargets)
        else:
            targets_summary_table = None

        summary_tabs = collections.OrderedDict()
        solint_tabs = collections.OrderedDict()
        spw_tabs = collections.OrderedDict()
        spw_tabs_msg = collections.OrderedDict()

        for target in cleantargets:

            if not is_restore:
                # only do this when not restoring from a previous run (selfcal_resourcs avaibility is not warranted)
                key = (target['field_name'], target['sc_band'])
                slib = target['sc_lib']
                summary_tabs[key] = self.make_summary_table(context, r, target)
                solint_tabs[key] = self.make_solint_summary_table(target, context, results)
                spw_tabs[key], spw_tabs_msg[key] = self.make_spw_summary_table(slib)

        ctx.update({'targets_summary_table': targets_summary_table,
                    'is_restore': is_restore,
                    'solint_tabs': solint_tabs,
                    'summary_tabs': summary_tabs,
                    'spw_tabs': spw_tabs,
                    'cleantargets': cleantargets, })

    def make_solint_summary_table(self, cleantarget, context, results):

        solints, target, band = cleantarget['sc_solints'], cleantarget['field_name'], cleantarget['sc_band']
        slib = cleantarget['sc_lib']
        check_solint = False

        rows = []
        vislist = slib['vislist']
        vis_keys = list(slib[vislist[-1]].keys())
        row_names = [
            'Pass', 'intflux_final', 'intflux_improvement', 'SNR_final', 'SNR_Improvement', 'SNR_NF_final',
            'SNR_NF_Improvement', 'RMS_final', 'RMS_Improvement', 'RMS_NF_final', 'RMS_NF_Improvement',
            'Beam_pre', 'Beam_post', 'Beam_Ratio',
            'clean_threshold']
        qa_extra_data = {}

        for row_name in row_names:

            row = []

            if row_name == 'Pass':
                row.append('Result')
            if row_name == 'intflux_final':
                row.append('Integrated Flux')
            if row_name == 'intflux_improvement':
                row.append('Integrated Flux Change')
            if row_name == 'SNR_final':
                row.append('Dynamic Range')
            if row_name == 'SNR_Improvement':
                row.append('DR Improvement')
            if row_name == 'SNR_NF_final':
                row.append('Dynamic Range (N.F.)')
            if row_name == 'SNR_NF_Improvement':
                row.append('DR Improvement (N.F.)')
            if row_name == 'RMS_final':
                row.append('RMS')
            if row_name == 'RMS_Improvement':
                row.append('RMS Improvement')
            if row_name == 'RMS_NF_final':
                row.append('RMS (N.F.)')
            if row_name == 'RMS_NF_Improvement':
                row.append('RMS Improvement (N.F.)')
            if row_name == 'Beam_pre':
                row.append('Beam Pre')
            if row_name == 'Beam_post':
                row.append('Beam post')
            if row_name == 'Beam_Ratio':
                row.append(text_with_tooltip('Ratio of Beam Area',
                           'The ratio of the beam area before and after self-calibration.'))
            if row_name == 'clean_threshold':
                row.append('Clean Threshold')

            for solint in solints:
                if 'Pass' in slib[vislist[-1]].get(solint, {}):
                    check_solint = True
                    slib_solint = slib[vislist[-1]][solint]
                    vis_solint_keys = slib_solint.keys()
                    if row_name == 'Pass':
                        result_desc = '-'
                        if not slib_solint['Pass']:
                            result_desc = '<font color="red">{}</font> {}'.format('Fail', slib_solint['Fail_Reason'])
                        else:
                            result_desc = '<font color="blue">{}</font>'.format('Pass')

                        qa_renderer = SelfCalQARenderer(context, results, cleantarget, solint)
                        qa_extra_data[solint] = qa_renderer.extra_data
                        with qa_renderer.get_file() as fileobj:
                            fileobj.write(qa_renderer.render())
                        result_desc = f'{result_desc}<br><a class="replace" href="{qa_renderer.rel_path}">QA Plots</a>'
                        row.append(result_desc)
                    if row_name == 'intflux_final':
                        row.append('{:0.3f} &#177 {:0.3f} mJy'.format(
                            slib_solint['intflux_post'] * 1e3,
                            slib_solint['e_intflux_post'] * 1e3))
                    if row_name == 'intflux_improvement':
                        row.append('{:0.3f}'.format(
                            slib_solint['intflux_post'] /
                            slib_solint['intflux_pre']))
                    if row_name == 'SNR_final':
                        row.append('{:0.3f}'.format(slib_solint['SNR_post']))
                    if row_name == 'SNR_Improvement':
                        row.append('{:0.3f}'.format(
                            slib_solint['SNR_post'] /
                            slib_solint['SNR_pre']))
                    if row_name == 'SNR_NF_final':
                        row.append('{:0.3f}'.format(slib_solint['SNR_NF_post']))
                    if row_name == 'SNR_NF_Improvement':
                        row.append('{:0.3f}'.format(
                            slib_solint['SNR_NF_post'] /
                            slib_solint['SNR_NF_pre']))

                    if row_name == 'RMS_final':
                        row.append('{:0.3f} mJy/bm'.format(slib_solint['RMS_post']*1e3))
                    if row_name == 'RMS_Improvement':
                        row.append('{:0.3f}'.format(
                            slib_solint['RMS_pre'] /
                            slib_solint['RMS_post']))
                    if row_name == 'RMS_NF_final':
                        row.append('{:0.3f} mJy/bm'.format(slib_solint['RMS_NF_post']*1e3))
                    if row_name == 'RMS_NF_Improvement':
                        row.append('{:0.3f}'.format(
                            slib_solint['RMS_NF_pre'] /
                            slib_solint['RMS_NF_post']))
                    if row_name == 'Beam_pre':
                        row.append('{:0.3f}"x{:0.3f}" {:0.3f} deg'.format(
                            slib_solint['Beam_major_pre'],
                            slib_solint['Beam_minor_pre'],
                            slib_solint['Beam_PA_pre']))
                    if row_name == 'Beam_post':
                        row.append('{:0.3f}"x{:0.3f}" {:0.3f} deg'.format(
                            slib_solint['Beam_major_post'],
                            slib_solint['Beam_minor_post'],
                            slib_solint['Beam_PA_post']))
                    if row_name == 'Beam_Ratio':
                        row.append('{:0.3f}'.format(
                            (slib_solint['Beam_major_post'] *
                             slib_solint['Beam_minor_post']) /
                            (slib['Beam_major_orig'] *
                             slib['Beam_minor_orig'])))
                    if row_name == 'clean_threshold':
                        if row_name in vis_solint_keys:
                            row.append('{:0.3f} mJy/bm'.format(
                                slib_solint['clean_threshold']*1e3))
                        else:
                            row.append('Not Available')
                else:
                    row.append('-')

            rows.append(row)

        for vis in vislist:

            rows.append(['<b>Measurement Set<b>']+['<b>'+vis+'<b>']*len(solints))
            if slib['obstype'] == 'mosaic':
                row_names = ['Flagged Frac.<br>by antenna',
                             'N.Sols (initial)', 'N.Sols Flagged (initial)', 'Flagged Frac.']
            else:
                row_names = ['Flagged Frac.<br>by antenna', 'N.Sols', 'N.Sols Flagged', 'Flagged Frac.']

            for row_name in row_names:
                row = [row_name]
                for solint in solints:
                    if 'Pass' in slib[vislist[-1]].get(solint, {}):
                        nsol_stats = qa_extra_data[solint]['antpos_plots'][vis].parameters
                        if slib['obstype'] == 'mosaic':
                            nsol_stats_predrop = qa_extra_data[solint]['antpos_predrop_plots'][vis].parameters
                        if row_name == 'N.Sols':
                            row.append(nsol_stats['nsols'])
                        if row_name == 'N.Sols (initial)':
                            row.append('{} ({})'.format(nsol_stats['nsols'], nsol_stats_predrop['nsols']))
                        if row_name == 'N.Sols Flagged':
                            row.append(nsol_stats['nflagged_sols'])
                        if row_name == 'N.Sols Flagged (initial)':
                            row.append('{} ({})'.format(nsol_stats['nflagged_sols'],
                                       nsol_stats_predrop['nflagged_sols']))
                        if row_name == 'Flagged Frac.':
                            if nsol_stats['nsols'] > 0:
                                row.append('{:0.3f} &#37;'.format(nsol_stats['nflagged_sols']/nsol_stats['nsols']*100.))
                            else:
                                row.append('-')
                        if row_name == 'Flagged Frac.<br>by antenna':
                            antpos_html = utils.plots_to_html(
                                [qa_extra_data[solint]['antpos_plots'][vis]],
                                report_dir=context.report_dir, title='Frac. Flagged Sol. Per Antenna')[0]
                            row.append(antpos_html)
                    else:
                        row.append('-')
                rows.append(row)

        if check_solint:
            new_rows = utils.merge_td_rows(utils.merge_td_columns(rows, vertical_align=True))
        else:
            new_rows = None

        return new_rows

    def make_spw_summary_table(self, slib):

        spwlist = list(slib['per_spw_stats'].keys())
        check_all_spws = False

        rows = []
        rows.append(['']+spwlist)
        quantities = ['bandwidth', 'effective_bandwidth', 'SNR_orig', 'SNR_final', 'RMS_orig', 'RMS_final']
        for key in quantities:
            row = [key]
            for spw in spwlist:
                spwkeys = slib['per_spw_stats'][spw].keys()
                if 'SNR' in key and key in spwkeys:
                    row.append('{:0.3f}'.format(slib['per_spw_stats'][spw][key]))
                    check_all_spws = True
                    continue
                if 'RMS' in key and key in spwkeys:
                    row.append('{:0.3f} mJy/bm'.format(slib['per_spw_stats'][spw][key]*1e3))
                    check_all_spws = True
                    continue
                if 'bandwidth' in key and key in spwkeys:
                    row.append('{:0.4f} GHz'.format(slib['per_spw_stats'][spw][key]))
                    continue
                row.append('-')
            rows.append(row)
        warning_msg = []
        # for spw in spwlist:
        #     spwkeys = slib['per_spw_stats'][spw].keys()
        #     if 'delta_SNR' in spwkeys or 'delta_RMS' in spwkeys or 'delta_beamarea' in spwkeys:
        #         if slib['per_spw_stats'][spw]['delta_SNR'] < 0.0:
        #             htmlOut.writelines('WARNING SPW '+spw+' HAS LOWER SNR POST SELFCAL')
        #         if slib['per_spw_stats'][spw]['delta_RMS'] > 0.0:
        #             htmlOut.writelines('WARNING SPW '+spw+' HAS HIGHER RMS POST SELFCAL')
        #         if slib['per_spw_stats'][spw]['delta_beamarea'] > 0.05:
        #             htmlOut.writelines('WARNING SPW '+spw+' HAS A >0.05 CHANGE IN BEAM AREA POST SELFCAL')
        if check_all_spws:
            return utils.merge_td_columns(rows, vertical_align=True), warning_msg
        else:
            return None, warning_msg

    def make_summary_table(self, context, r, cleantarget):
        """Make a per-target summary table."""

        slib = cleantarget['sc_lib']
        rows = []
        row_names = ['Data Type', 'Image', 'Integrated Flux', 'SNR', 'SNR (N.F.)', 'RMS', 'RMS (N.F.)', 'Beam']

        def fm_sc_success(success):
            if success:
                return '<a style="color:blue">Yes</a>'
            else:
                return '<a style="color:red">No</a>'

        def fm_reason(slib):
            rkey = 'Stop_Reason'
            if rkey not in slib:
                return 'Estimated Selfcal S/N too low for solint'
            else:
                return slib[rkey]

        def fm_solint(slib):
            if slib['final_solint'] in [None, 'None']:
                return '-'
            else:
                return slib['final_solint']

        def fm_values(row_values, v1, v2):
            if v1 == -99.:
                row_values[0] = '-'
                row_values[2] = '-'
            if v2 == -99.:
                row_values[1] = '-'
                row_values[2] = '-'
            return row_values

        desc_args = {'success': fm_sc_success(slib['SC_success']),
                     'reason': fm_reason(slib),
                     'finalsolint': fm_solint(slib)}
        summary_desc = ('<ul style="list-style-type:none;">'
                        '<li>Selfcal Success: {success}</li>'
                        '<li>Stop Reason: {reason}</li>'
                        '<li>Final Successful solint: {finalsolint}</li>'
                        '</ul>'.format(**desc_args))
        summary_desc = f'<div style="text-align:left">{summary_desc}</div>'

        for row_name in row_names:
            if row_name == 'Data Type':
                row_values = ['Initial', 'Final', 'Brightness Dist. / Ratio']
                if not slib['SC_success']:
                    row_values = ['Initial/Final', 'Initial/Final', 'Brightness Dist.']
            if row_name == 'Image':
                summary_plots, noisehist_plot = display.SelfcalSummary(context, r, cleantarget).plot()
                row_values = utils.plots_to_html(
                    summary_plots[0:2] + [noisehist_plot],
                    report_dir=context.report_dir, title='Initial/Final Image Comparison')
                if not slib['SC_success']:
                    row_values = utils.plots_to_html(
                        [summary_plots[0]]*2 + [noisehist_plot],
                        report_dir=context.report_dir, title='Initial/Final Image Comparison')
            if row_name == 'Integrated Flux':
                row_values = [
                    '{:0.3f} &#177 {:0.3f} mJy'.format(slib['intflux_orig'] * 1e3, slib['e_intflux_orig'] * 1e3),
                    '{:0.3f} &#177 {:0.3f} mJy'.format(slib['intflux_final'] * 1e3, slib['e_intflux_final'] * 1e3)] + \
                    ['{:0.3f}'.format(slib['intflux_final']/slib['intflux_orig'])]
                if not slib['SC_success']:
                    row_values[2] = row_values[1]
                row_values = fm_values(row_values, slib['intflux_orig'], slib['intflux_final'])

            if row_name == 'SNR':
                row_values = ['{:0.3f}'.format(slib['SNR_orig']),
                              '{:0.3f}'.format(slib['SNR_final'])] +\
                    ['{:0.3f}'.format(slib['SNR_final']/slib['SNR_orig'])]
                if not slib['SC_success']:
                    row_values[2] = row_values[1]
                row_values = fm_values(row_values, slib['SNR_orig'], slib['SNR_final'])

            if row_name == 'SNR (N.F.)':
                row_values = ['{:0.3f}'.format(slib['SNR_NF_orig']),
                              '{:0.3f}'.format(slib['SNR_NF_final'])] +\
                    ['{:0.3f}'.format(slib['SNR_NF_final']/slib['SNR_NF_orig'])]
                if not slib['SC_success']:
                    row_values[2] = row_values[1]
                row_values = fm_values(row_values, slib['SNR_NF_orig'], slib['SNR_NF_final'])

            if row_name == 'RMS':
                row_values = ['{:0.3f} mJy/bm'.format(slib['RMS_orig']*1e3),
                              '{:0.3f} mJy/bm'.format(slib['RMS_final']*1e3)] +\
                    ['{:0.3f}'.format(slib['RMS_final']/slib['RMS_orig'])]
                if not slib['SC_success']:
                    row_values[2] = row_values[1]
                row_values = fm_values(row_values, slib['RMS_orig'], slib['RMS_final'])

            if row_name == 'RMS (N.F.)':
                row_values = ['{:0.3f} mJy/bm'.format(slib['RMS_NF_orig']*1e3),
                              '{:0.3f} mJy/bm'.format(slib['RMS_NF_final']*1e3)] +\
                    ['{:0.3f}'.format(slib['RMS_NF_final']/slib['RMS_NF_orig'])]
                if not slib['SC_success']:
                    row_values[2] = row_values[1]
                row_values = fm_values(row_values, slib['RMS_NF_orig'], slib['RMS_NF_final'])

            if row_name == 'Beam':
                row_values = [
                    '{:0.3f}"x{:0.3f}" {:0.3f} deg'.format(
                        slib['Beam_major_orig'],
                        slib['Beam_minor_orig'],
                        slib['Beam_PA_orig']),
                    '{:0.3f}"x{:0.3f}" {:0.3f} deg'.format(
                        slib['Beam_major_final'],
                        slib['Beam_minor_final'],
                        slib['Beam_PA_final'])] + [
                    '{:0.3f}'.format(
                        slib['Beam_major_final'] * slib['Beam_minor_final'] / slib['Beam_major_orig'] /
                        slib['Beam_minor_orig'])]
                if not slib['SC_success']:
                    row_values[2] = row_values[1]

            rows.append([row_name]+row_values)

        rows.append(['Success / Final Solint']+[desc_args['success']+' / '+desc_args['finalsolint']]*3)
        rows.append(['Stop Reason']+[desc_args['reason']]*3)

        return utils.merge_td_rows(utils.merge_td_columns(rows, vertical_align=True))


def text_with_tooltip(text, tooltip):
    return f'<div data-toggle="tooltip" data-placement="bottom" title="{tooltip}">{text}</div>'


def fm_target(target):
    target_str = target['field']
    if target['is_repr_target']:
        target_str += '<br> (rep.source)'
    return target_str
