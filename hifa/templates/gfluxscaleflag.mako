<%!
rsc_path = ""
import html
import os
import pipeline.infrastructure.utils as utils
import pipeline.infrastructure.renderer.rendererutils as rendererutils

# method to output flagging percentages neatly
def percent_flagged(flagsummary):
    flagged = flagsummary.flagged
    total = flagsummary.total

    if total == 0:
        return 'N/A'
    else:
        return '%0.3f%%' % (100.0 * flagged / total)

_types = {
    'before': 'Calibrated data before flagging',
    'after': 'Calibrated data after flagging'
}

def plot_type(plot):
    return _types[plot.parameters['type']]

%>

<%inherit file="t2-4m_details-base.mako"/>

<%block name="header" />

<%block name="title">Flagging of flux, diffgain, phase calibrators and check source</%block>

<p>
    This task computes the flagging heuristics on the flux, diffgain, and phase
    calibrators and the check source, by calling hif_correctedampflag which
    looks for outlier visibility points by statistically examining the scalar
    difference of corrected amplitudes minus model amplitudes, and flags those
    outliers. The philosophy is that only outlier data points that have remained
    outliers after calibration will be flagged. The heuristic works equally well
    on resolved calibrators and point sources because it is not performing a
    vector difference, and thus is not sensitive to nulls in the flux density
    vs. uvdistance domain. Note that the phase of the data is not assessed.
</p>
<p>
    In further detail, the workflow is as follows: an a priori calibration is
    applied using pre-existing caltables in the calibration state, a
    preliminary phase and amplitude gaincal solution is solved and applied, the
    flagging heuristics are run, and any outliers are flagged. Plots are
    generated at two points in this workflow: after preliminary phase and
    amplitude calibration but before flagging heuristics are run, and after
    flagging heuristics have been run and applied. If no points were flagged,
    the "after" plots are not generated or displayed. The score for this stage
    is the standard data flagging score, which depends on the fraction of data
    flagged.
</p>

<h2>Contents</h2>
<ul>
% if htmlreports:
    <li><a href="#flagging_commands">Flagging commands</a></li>
%endif
    <li><a href="#flagged_data_summary">Flagged data summary table</a></li>
    <li><a href="#amp_vs_time">Amplitude vs time plots for flagging</a></li>
    <li><a href="#amp_vs_uvdist">Amplitude vs UV distance plots for flagging</a></li>
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

<p> The values in the following table(s) are computed from the temporarily-calibrated visibility data after applying 
the integration-based phase 'G' solutions and the scan-based amplitude 'A' solutions computed in this stage. 
The "flagged before" column is the percentage of flagged visibilities before also applying the newly-generated 
flags computed from the statistical analysis of the surviving calibrated data, while the "flagged after" column 
also includes those flags. High values on this page are indicative of low SNR achieved on the corresponding 
object on the per-integration timescale. In all cases, both columns will naturally be higher than the 
corresponding values seen on the later hif_applycal stage because in that stage the phase solutions are 
scan-based, thus have higher SNR. The spw mapping/combination heuristics determined in hifa_spwphaseup are used 
in computing the solutions in this stage. Note that with 50 antennas, a loss of 10% of the antenna solutions (5) 
will result in a loss of 19% of the baselines (235).
</p>

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
		% for k in ['TOTAL', 'BANDPASS', 'AMPLITUDE', 'PHASE', 'CHECK', 'DIFFGAINREF', 'DIFFGAINSRC', 'TARGET']:
		<tr>
			<th>${k}</th>
			% for step in ['before','after']:
			% if flags[ms].get(step) is not None and flags[ms][step]['Summary'].get(k) is not None:
				<td>${percent_flagged(flags[ms][step]['Summary'][k])}</td>
			% else:
				<td>0.0%</td>
			% endif
			% endfor
		</tr>
		% endfor
	</tbody>
</table>

% endfor

<%self:plot_group plot_dict="${time_plots}"
				  url_fn="${lambda x: 'junk'}"
                  rel_fn="${lambda plot: 'amp_vs_time_%s_%s_%s_%s' % (plot.parameters['vis'], plot.parameters['intent'], plot.parameters['field'], plot.parameters['spw'])}"
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
		correlations shown in different colours.</p>
	</%def>

	<%def name="mouseover(plot)">Click to show amplitude vs time for spw ${plot.parameters['spw']}</%def>

	<%def name="fancybox_caption(plot)">
		${plot_type(plot)}<br>
		${plot.parameters['vis']}<br>
		Spw ${plot.parameters['spw']}<br>
		Intents: ${utils.commafy([plot.parameters['intent']], False)}<br>
        Fields: ${html.escape(plot.parameters['field'], True)}
	</%def>

    <%def name="caption_title(plot)">
		Spectral Window ${plot.parameters['spw']}<br>
	</%def>

	<%def name="caption_subtitle(plot)">
		Intents: ${utils.commafy([plot.parameters['intent']], False)}<br>
        Fields: ${utils.commafy(utils.safe_split(plot.parameters['field']), quotes=False)}
	</%def>

    <%def name="caption_text(plot, ptype)">
		${plot_type(plot)}.
	</%def>

</%self:plot_group>


<%self:plot_group plot_dict="${uvdist_plots}"
				  url_fn="${lambda x: 'junk'}"
                  rel_fn="${lambda plot: 'amp_vs_uvdist_%s_%s_%s_%s' % (plot.parameters['vis'], plot.parameters['intent'], plot.parameters['field'], plot.parameters['spw'])}"
				  title_id="amp_vs_uvdist"
                  break_rows_by="intent,field,type_idx"
                  sort_row_by="spw">

	<%def name="title()">
		Amplitude vs UV distance
	</%def>

	<%def name="preamble()">
		<p>These plots show amplitude vs UV distance for two cases: 1, the calibrated data before application of any
        flags; and 2, where flagging was applied, the calibrated data after application of flags.</p>

		<p>Data are plotted for all antennas and correlations, with different
		correlations shown in different colours.</p>
	</%def>

	<%def name="mouseover(plot)">Click to show amplitude vs UV distance for ${plot.parameters['field']} (${plot.parameters['intent']}) spw ${plot.parameters['spw']}</%def>

	<%def name="fancybox_caption(plot)">
		${plot_type(plot)}<br>
		${plot.parameters['vis']}<br>
		Spw ${plot.parameters['spw']}<br>
		Intents: ${utils.commafy([plot.parameters['intent']], False)}<br>
        Fields: ${html.escape(plot.parameters['field'], True)}
	</%def>

    <%def name="caption_title(plot)">
		Spectral Window ${plot.parameters['spw']}<br>
	</%def>

	<%def name="caption_subtitle(plot)">
		Intents: ${utils.commafy([plot.parameters['intent']], False)}<br>
        Field: ${utils.commafy(utils.safe_split(plot.parameters['field']), quotes=False)}
	</%def>

    <%def name="caption_text(plot, ptype)">
		${plot_type(plot)}.
	</%def>

</%self:plot_group>
