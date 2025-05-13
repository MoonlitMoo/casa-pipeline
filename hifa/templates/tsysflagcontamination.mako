<%!
rsc_path = ""
import html
import os

import pipeline.infrastructure.utils as utils
import pipeline.infrastructure.renderer.rendererutils as rendererutils

# method to output a cell for percentage flagged in flagging summary table
def get_td_for_percent_flagged(flagsummary, step):
    flagged = flagsummary.flagged
    total = flagsummary.total

    if total == 0:
        return '<td>N/A</td>'

    pflagged = (100.0 * flagged / total)
    if step == 'before' and pflagged > 1.0:
        return '<td class="warning">{:.3f}%</td>'.format(pflagged)

    return '<td>{:.3f}%</td>'.format(pflagged)
%>

<%inherit file="t2-4m_details-base.mako"/>

<%
# these functions are defined in template scope so we have access to the weblog
# context objects.

def flagcmd_file_data(caltable, flagcmd_file):
    if flagcmd_file is not None:
        row = ('<td>{}</td>'
               '<td><a class="replace-pre" href="{}" data-title="Flagging Commands">{}</a></td>'
               '<td>{}</td>'
               ''.format(caltable, flagcmd_file, os.path.basename(flagcmd_file),
                         rendererutils.num_lines(os.path.join(pcontext.report_dir, flagcmd_file))))
    else:
        row = '<td>{}</td><td>N/A</td><td>N/A</td>'.format(caltable)
    return row
%>

<%block name="title">Flag T<sub>sys</sub> astrophysical line contamination</%block>

<p>This task detects and flags the frequency ranges in the T<sub>sys</sub> calibration table
affected by astronomical emission.</p>

<p>The QA score of this task is 1.0 (green) if the task runs correctly and there
are no flags to be applied and 0.9 (blue) if the task runs correctly and flags
are applied. For some data types for which the task is not designed, the QA
score will be 0.6 (yellow). The QA score will be 0.6 as well if the task
detects issues that deserve special attention, such as:<p>

<ol style="list-style-type: lower-roman;">

    <li>If the QA score message says "Large difference between the bandpass telluric
    line and..." it means that there is a possible atmospheric line
    contamination affecting the autocorrelation renormalization corrections which needs
    to be excluded manually.</li>

    <li>If the QA score message says "Astronomical contamination covering a wide frequency
    range..." it means that the contaminated line range detected by the algorithm is too wide
    and needs to be examined. In that case, the flagging associated with the wide range is
    not applied.</li>

    <li>Finally, if the message says "Large residuals..." it means that the general T<sub>sys</sub>
    solution needs to be examined due to instrumental instabilities or very
    widespread T<sub>sys</sub> contamination.</li>

</ol>

% if any([msg for msg in task_incomplete_msg.values()]):
  <p>For the following measurement sets, the task ended prematurely with the following message:</p>
  <ul>
  % for vis, msg in task_incomplete_msg.items():
  	<li>${msg}</li>
  % endfor
  </ul>
% endif

% if any([summary_plots, updated_refants, summary_plots, flags, flagcmd_files]):
<h2>Contents</h2>
<ul>
% if updated_refants:
    <li><a href="#refants">Reference antenna update</a></li>
% endif
% if summary_plots:
    <li><a href="#plots">T<sub>sys</sub> after flagging</a></li>
% endif
% if flags:
    <li><a href="#summarytable">Flagged data summary</a></li>
% endif
% if flagcmd_files:
    <li><a href="#contamination">Contamination flagging details</a></li>
% endif
</ul>
% endif

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
        % for vis in updated_refants:
		<tr>
			<td>${os.path.basename(vis)}</td>
			## insert spaces in refant list to allow browser to break string
			## if it wants
			<td>${updated_refants[vis].replace(',', ', ')}</td>
		</tr>
        % endfor
	</tbody>
</table>
% endif

<%self:plot_group plot_dict="${summary_plots}"
				  url_fn="${lambda x: summary_subpage[x]}"
				  data_tsysspw="${True}"
                  data_vis="${True}"
                  title_id="plots">

	<%def name="title()">
		T<sub>sys</sub> vs frequency after flagging
	</%def>

	<%def name="preamble()">
		<p>Plots of time-averaged T<sub>sys</sub> vs frequency, colored by antenna.</p>
	</%def>

	<%def name="mouseover(plot)">Click to show Tsys vs frequency for Tsys spw ${plot.parameters['tsys_spw']}</%def>

	<%def name="fancybox_caption(plot)">
		T<sub>sys</sub> spw: ${plot.parameters['tsys_spw']}<br/>
		Science spws: ${', '.join([str(i) for i in plot.parameters['spw']])}
	</%def>

	<%def name="caption_title(plot)">
		T<sub>sys</sub> spw ${plot.parameters['tsys_spw']}
	</%def>

	<%def name="caption_text(plot, _)">
		Science spw${utils.commafy(plot.parameters['spw'], quotes=False, multi_prefix='s')}
	</%def>

</%self:plot_group>

<%self:plot_group plot_dict="${contamination_plots}"
				  url_fn="${lambda x: contamination_subpages[x]}"
				  data_tsysspw="${True}"
                  data_vis="${True}"
                  sort_row_by="tsys_spw"
                  title_id="contamination plots">

	<%def name="title()">
		T<sub>sys</sub> line contamination heuristic: diagnostic plots
	</%def>

	<%def name="preamble()">
		<p>The diagnostic plots below show antenna and scan averaged (stacked) T<sub>sys</sub>
           contamination tables for each spectral window.

           For each spectral windows, there are two graphs. The graph on the left shows
           the stacked T<sub>sys</sub> spectrum taken toward the bandpass calibrator in orange color.
           The stacked T<sub>sys</sub> spectrum taken toward other T<sub>sys</sub> field (with
           CALIBRATE_ATMOSPHERE intent) is shown in blue. The difference between these two
           (plus a constant to ease the display) is shown in green. The T<sub>sys</sub> contaminated
           ranges are shown in red. The graph on the right shows this same difference T<sub>sys</sub>
           spectrum but rectified in order to scatter around zero. It has also been
           re-scaled by a noise estimation in order to display signal-to-noise ratio.</p>
	</%def>

	<%def name="mouseover(plot)">Click to show Tsys vs frequency for Tsys spw ${plot.parameters['tsys_spw']}</%def>

	<%def name="fancybox_caption(plot)">
		T<sub>sys</sub> spw: ${plot.parameters['tsys_spw']}<br/>
		Science spws: ${', '.join([str(i) for i in plot.parameters['spw']])}<br/>
		Intent: ${plot.parameters['intent']}<br/>
		Fields: ${html.escape(plot.parameters['field'], True)}
	</%def>

	<%def name="caption_title(plot)">
		T<sub>sys</sub> spw ${plot.parameters['tsys_spw']}
	</%def>

	<%def name="caption_text(plot, _)">
		Science spw${utils.commafy(plot.parameters['spw'], quotes=False, multi_prefix='s')}<br/>
		Intent: ${plot.parameters['intent']}<br/>
		Fields: ${html.escape(plot.parameters['field'], True)}
	</%def>

</%self:plot_group>

% if flags:
<h2>Flagged data summary</h2>

% for ms in flags:
<h4>Table: ${ms}</h4>
<table id="summarytable" class="table table-bordered table-striped ">
	<caption>Summary of flagged solutions. Each cell states the amount of
        solutions flagged as a fraction of the specified data selection, with
        the <em>Flagging Step</em> columns giving this information per flagging
		step. Note: for each data selection intent, the flagging statistics
        are calculated for the T<sub>sys</sub> scans (with intent=ATMOSPHERE)
        that cover those fields that also match the data selection intent. A
        value of "N/A" in a row means that no T<sub>sys</sub> scan was acquired
        on the object(s) observed for the corresponding intent.
	</caption>
	<thead>
		<tr>
			<th rowspan="2">Data Selection</th>
			<!-- flags before task is always first agent -->
			<th rowspan="2">flagged before</th>
			<th colspan="${len(components)}">Flagging Step</th>
			<th rowspan="2">flagged after</th>
		</tr>
		<tr>
			<th>contamination</th>
		</tr>
	</thead>
	<tbody>
		% for intent in flag_table_intents:
		<tr>
			<th>${intent}</th>
			% for step in ['before'] + components + ['after']:
			  % if flags[ms].get(step):
				${get_td_for_percent_flagged(flags[ms][step]['Summary'][intent], step)}
			  % else:
				<td>N/A</td>
			  % endif
			% endfor
		</tr>
		% endfor
	</tbody>
</table>

% endfor
% endif

% if flagcmd_files:
<h2>Flag Step Details</h2>
<p>
    The following section provides details of the flagging commands used to
    flag astrophysical line contamination detected in the T<sub>sys</sub>
    measurements.
</p>

	<h3 id="contamination" class="jumptarget">Astrophysical line contamination</h3>
	Flag astrophysical line contamination in T<sub>sys</sub> spectra. If contamination is detected, the regions to mask
	are written to a template file and the caltable is flagged, unless the data is Band 9 or 10.

    <h4>Template files</h4>
    <table class="table table-bordered table-striped">
	<thead>
	    <tr>
	    	<th>Table</th>
	        <th>File</th>
	        <th>Number of Statements</th>
	    </tr>
	</thead>
	<tbody>
    % for caltable, flagcmd_file in flagcmd_files.items():
        <tr>
            ${flagcmd_file_data(caltable, flagcmd_file)}
        </tr>
    % endfor
	</tbody>
    </table>
% endif
