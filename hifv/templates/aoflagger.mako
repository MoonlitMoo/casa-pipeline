<%!
rsc_path = ""
import os
import pipeline.infrastructure.renderer.htmlrenderer as hr
import pipeline.infrastructure.utils as utils
%>
<%inherit file="t2-4m_details-base.mako"/>

<%block name="title">RFI Flagging</%block>

<p>Flag possible RFI using aoflagger; flag_target=${repr(result[0].inputs['flag_target'])}, use_corrected=${repr(result[0].inputs['use_corrected'])}.</p>


<%
is_summary_plots = False
for ms in summary_plots:
    is_summary_plots = bool(summary_plots[ms])
%>

% if is_summary_plots:

<%self:plot_group plot_dict="${summary_plots}"
                                  url_fn="${lambda ms:  'noop'}">
        
        <%def name="title()">
            Amplitude vs Frequency
        </%def>

        <%def name="preamble()">
        </%def>

        <%def name="mouseover(plot)">Summary window</%def>

        <%def name="fancybox_caption(plot)">
            ${plot.parameters['plotms_args']['title']}
        </%def>

        <%def name="caption_title(plot)">
            ${plot.parameters['plotms_args']['title']}
        </%def>

</%self:plot_group>

%endif


% if True:

<%

# these functions are defined in template scope so we have access to the flags
# and agents context objects

def percent_flagged(flagsummary):
    flagged = flagsummary.flagged
    total = flagsummary.total

    if total == 0:
        return 'N/A'
    else:
        return '%0.3f%%' % (100.0 * flagged / total)

def percent_flagged_diff(flagsummary1, flagsummary2):
    flagged1 = flagsummary1.flagged
    flagged2 = flagsummary2.flagged
    total = flagsummary1.total

    if total == 0:
        return 'N/A'
    else:
        return '%0.3f%%' % (100.0 * (flagged2-flagged1) / total)

%>

% for ms in flags.keys():
<% ms = ms.strip('"') %>

<h3 id="flagged_data_summary" class="jumptarget">Aoflagger Summary</h3>

<h4>${os.path.basename(ms)}</h4>

<p>Summary Data Selection Parameter(s)</p>
<ul>
    % for key, value in dataselect[ms].items():
        % if value!='':
            <li> ${key}: ${repr(utils.find_ranges(value))}
        %endif
    % endfor
</ul>

<table style="float: left; margin:0 10px; width: auto; text-align:center" class="table table-bordered table-striped table-hover">
	<caption></caption>
	<thead>
		<tr>
			<th rowspan="2">Antenna</th>
			<!-- flags before task is always first agent -->
			<th rowspan="2">flagged before</th>
			<th rowspan="2">flagged after</th>
            <th rowspan="2">flagged additional</th>
		</tr>
	</thead>
	<tbody>
		% for k in sorted(flags[ms]['by_antenna']['before'].keys()):
		<tr>
			<th style="text-align:center">${k}</th>
            <td>${percent_flagged(flags[ms]['by_antenna']['before'][k])}</td>
            <td>${percent_flagged(flags[ms]['by_antenna']['after'][k])}</td>
            <td>${percent_flagged_diff(flags[ms]['by_antenna']['before'][k],flags[ms]['by_antenna']['after'][k])}</td>
		</tr>
		% endfor
	</tbody>
</table>

<table style="float: left; margin:0 10px; width: auto; text-align:center" class="table table-bordered table-striped table-hover">
	<caption>Summary of flagged data. Each cell states the amount of data
		flagged as a fraction of the specified data selection.
	</caption>
	<thead>
		<tr>
			<th rowspan="2">Spw</th>
			<!-- flags before task is always first agent -->
			<th rowspan="2">flagged before</th>
			<th rowspan="2">flagged after</th>
            <th rowspan="2">flagged additional</th>
		</tr>
	</thead>
	<tbody>
		% for k in sorted(flags[ms]['by_spw']['before'].keys(),key=int):
		<tr>
			<th style="text-align:center">${k}</th>
            <td>${percent_flagged(flags[ms]['by_spw']['before'][k])}</td>
            <td>${percent_flagged(flags[ms]['by_spw']['after'][k])}</td>
            <td>${percent_flagged_diff(flags[ms]['by_spw']['before'][k],flags[ms]['by_spw']['after'][k])}</td>
		</tr>
		% endfor
	</tbody>
</table>

% endfor

%endif