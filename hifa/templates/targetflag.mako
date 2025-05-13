<%!
rsc_path = ""
import os
import pipeline.infrastructure.renderer.htmlrenderer as hr
import pipeline.infrastructure.utils as utils
import pipeline.infrastructure.renderer.rendererutils as rendererutils
%>

<%inherit file="t2-4m_details-base.mako"/>

<%block name="title">Target Flagging</%block>

<p>
    This task computes the ultra high baseline flagging heuristics for TARGET
    intents by calling hif_correctedampflag which looks for outlier visibility
    points by statistically examining the scalar difference of the corrected
    amplitude minus model amplitudes and flags those outliers.
</p>
<p>
    In further detail, the workflow is as follows: an a priori calibration is
    applied using pre-existing caltables in the calibration state, the flagging
    heuristics are run and any outliers are flagged. The score for this stage
    is the standard data flagging score (depending on the fraction of data
    flagged).
</p>

<h2>Contents</h2>
<ul>
% if htmlreports:
    <li><a href="#flagging_commands">Flagging commands</a></li>
% endif
    <li><a href="#flagged_data_summary">Flagged data summary table</a></li>
% if any(v != [] for v in time_plots.values()):
    <li><a href="#amp_vs_time">Amplitude vs time plots for flagging</a></li>
% endif
% if any(v != [] for v in uvdist_plots.values()):
    <li><a href="#amp_vs_uvdist">Amplitude vs uvdist plots for flagging</a></li>
% endif
</ul>

% if htmlreports:
    <h2 id="flagging_commands" class="jumptarget">Flagging</h2>
    <table class="table table-bordered table-striped">
        <caption>Report Files</caption>
        <thead>
            <tr>
                <th>Measurement Set</th>
                <th>Flagging Commands</th>
                <th>Number of Statements</th>
            </tr>
        </thead>
        <tbody>
        % for msname, relpath in htmlreports.items():
            <tr>
                <td>${msname}</td>
                <td><a class="replace-pre" href="${relpath}">${os.path.basename(relpath)}</a></td>
                <td>${rendererutils.num_lines(os.path.join(pcontext.report_dir, relpath))}</td>
            </tr>
        % endfor
        </tbody>
    </table>
% endif

<h2 id="flagged_data_summary" class="jumptarget">Flagged data summary</h2>

% for ms in flags.keys():
<h4>Measurement Set: ${os.path.basename(ms)}</h4>
<table class="table table-bordered table-striped ">
    <caption>Summary of flagged data. Each cell states the amount of data
        flagged as a fraction of the specified data selection.
    </caption>
    <thead>
        <tr>
            <th rowspan="2">Data Selection</th>
            <!-- flags before task is always first agent -->
            <th rowspan="2">flagged before</th>
            <th rowspan="2">flagged after</th>
        </tr>
    </thead>
    <tbody>
    % for k in ['TOTAL', 'BANDPASS', 'AMPLITUDE', 'PHASE', 'TARGET']:
        <tr>
            <th>${k}</th>
            % for step in ['before','after']:
                % if flags[ms].get(step) is not None:
                    <td>${rendererutils.percent_flagged(flags[ms][step]['Summary'][k])}</td>
                % else:
                    <td>0.0%</td>
                % endif
            % endfor
        </tr>
    % endfor
    </tbody>
</table>

% endfor

% if any(v != [] for v in time_plots.values()):

<h2 id="per_ms_plots" class="jumptarget">Plots</h2>

<%self:plot_group plot_dict="${time_plots}"
                  url_fn="${lambda x: 'junk'}"
                  rel_fn="${lambda plot: 'amp_vs_time_%s_%s' % (plot.parameters['vis'], plot.parameters['spw'])}"
                  title_id="amp_vs_time"
                  break_rows_by="intent,field,type_idx"
                  sort_row_by="spw">

    <%def name="title()">
        Amplitude vs time
    </%def>

    <%def name="preamble()">
        <p>These plots show amplitude vs time for two cases: 1, the calibrated data before application of any flags;
        and 2, where flagging was applied, the calibrated data after application of flags.</p>

        <p>Data are plotted for all antennas and correlations, with different
        fields shown in different colours.</p>
    </%def>

    <%def name="mouseover(plot)">Click to show amplitude vs time for spw ${plot.parameters['spw']}</%def>

    <%def name="fancybox_caption(plot)">
        ${rendererutils.plot_type(plot)}<br>
        ${plot.parameters['vis']}<br>
        Spw ${plot.parameters['spw']}<br>
        Intents: ${utils.commafy([plot.parameters['intent']], False)}
    </%def>

    <%def name="caption_title(plot)">
        Spectral Window ${plot.parameters['spw']}<br>
    </%def>

    <%def name="caption_subtitle(plot)">
        Intents: ${utils.commafy([plot.parameters['intent']], False)}
    </%def>

    <%def name="caption_text(plot, ptype)">
        ${rendererutils.plot_type(plot)}.
    </%def>

</%self:plot_group>

% endif

% if any(v != [] for v in uvdist_plots.values()):

<%self:plot_group plot_dict="${uvdist_plots}"
                  url_fn="${lambda x: 'junk'}"
                  rel_fn="${lambda plot: 'amp_vs_uvdist_%s_%s' % (plot.parameters['vis'], plot.parameters['spw'])}"
                  title_id="amp_vs_uvdist"
                  break_rows_by="type_idx"
                  sort_row_by="spw">

    <%def name="title()">
        Amplitude vs UV distance
    </%def>

    <%def name="preamble()">
        <p>These plots show amplitude vs UV distance for two cases: 1, the calibrated data before application of any
        flags; and 2, where flagging was applied, the calibrated data after application of flags.</p>

        <p>Data are plotted for all antennas and correlations, with different
        correlations shown in different colours.</p>

        <p>The plots of amplitude vs UV distance show only the target fields for which new flags were found, and are
        only produced for spws with new flags.</p>
    </%def>

    <%def name="mouseover(plot)">Click to show amplitude vs UV distance for spw ${plot.parameters['spw']}</%def>

    <%def name="fancybox_caption(plot)">
        ${rendererutils.plot_type(plot)}<br>
        ${plot.parameters['vis']}<br>
        Spw ${plot.parameters['spw']}<br>
        Intents: ${utils.commafy([plot.parameters['intent']], False)}<br>
        Fields: ${rendererutils.summarise_fields(plot.parameters['field'])}
    </%def>

    <%def name="caption_title(plot)">
        Spectral Window ${plot.parameters['spw']}<br>
    </%def>

    <%def name="caption_subtitle(plot)">
        Intents: ${utils.commafy([plot.parameters['intent']], False)}<br>
        Fields: ${rendererutils.summarise_fields(plot.parameters['field'])}
    </%def>

    <%def name="caption_text(plot, ptype)">
        ${rendererutils.plot_type(plot)}.
    </%def>

</%self:plot_group>

% endif
