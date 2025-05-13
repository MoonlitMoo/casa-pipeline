<%!
rsc_path = ""
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

<%block name="title">Bandpass Calibration Flagging</%block>

<p>
    This task performs a preliminary phased-up bandpass solution and
    temporarily applies it, then computes the flagging heuristics by calling
    hif_correctedampflag which looks for outlier visibility points by
    statistically examining the scalar difference of the corrected amplitude
    minus model amplitudes, and then flags those outliers. The philosophy is
    that only outlier data points that have remained outliers after calibration
    will be flagged. Note that the phase of the data is not assessed.
</p>
<p>
    Plots are generated at two points in this workflow: after bandpass
    calibration but before flagging heuristics are run, and after flagging
    heuristics have been run and applied. If no points were flagged, the
    "after" plots are not generated or displayed.
</p>

<h2>Contents</h2>
<ul>
% if updated_refants:
    <li><a href="#flagging_table">Reference Antenna update table</a></li>
%endif
% if htmlreports:
    <li><a href="#flagging_commands">Flagging commands</a></li>
%endif
    <li><a href="#flagged_data_summary">Flagged data summary table</a></li>
    <li><a href="#amp_vs_time">Amplitude vs time plots for flagging</a></li>
    <li><a href="#amp_vs_uvdist">Amplitude vs UV distance plots for flagging</a></li>    
</ul>

% if updated_refants:
<h2 id="refants" class="jumptarget">Reference Antenna update</h2>

<p>For the measurement set(s) listed below, the reference antenna
    list was updated due to significant flagging (antennas moved to
    end and/or removed). See QA messages for details.
    Shown below are the updated reference antenna lists,
    only for those measurement sets where it was modified.</p>

<table class="table table-bordered table-striped"
	   summary="Reference Antennas">
	<caption>Updated reference antenna selection per measurement set. Antennas are
	listed in order of highest to lowest priority.</caption>
	<thead>
		<tr>
			<th>Measurement Set</th>
			<th>Reference Antennas (Highest to Lowest)</th>
		</tr>
	</thead>
	<tbody>
%for vis in updated_refants:
		<tr>
			<td>${os.path.basename(vis)}</td>
			## insert spaces in refant list to allow browser to break string
			## if it wants
			<td>${updated_refants[vis].replace(',', ', ')}</td>
		</tr>
%endfor
	</tbody>
</table>
% endif

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

<p>
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
		correlations shown in different colours.</p>
	</%def>

	<%def name="mouseover(plot)">Click to show amplitude vs time for spw ${plot.parameters['spw']}</%def>

	<%def name="fancybox_caption(plot)">
		${plot_type(plot)}<br>
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
		${plot_type(plot)}.
	</%def>

</%self:plot_group>


<%self:plot_group plot_dict="${uvdist_plots}"
				  url_fn="${lambda x: 'junk'}"
                  rel_fn="${lambda plot: 'amp_vs_uvdist_%s_%s' % (plot.parameters['vis'], plot.parameters['spw'])}"
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

	<%def name="mouseover(plot)">Click to show amplitude vs UV distance for spw ${plot.parameters['spw']}</%def>

	<%def name="fancybox_caption(plot)">
		${plot_type(plot)}<br>
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
		${plot_type(plot)}.
	</%def>

</%self:plot_group>
