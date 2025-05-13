<%!
rsc_path = ""
import os
import pipeline.infrastructure.filenamer as filenamer
import pipeline.infrastructure.renderer.htmlrenderer as hr

def get_ant_str_for_caption_text(parameters):
    ants = parameters.get('ant', "")
    if ants:
        return ", and antennas: {}".format(', '.join(ants.split(',')))
    else:
        return ", all antennas"

def get_ant_str_for_caption_title(parameters):
    antdiam = parameters.get('antdiam', '')
    if antdiam:
        return ", {:.1f} m antennas".format(antdiam)
    else:
        return ""
%>
<%inherit file="t2-4m_details-base.mako"/>

<%block name="header" />

<%block name="title">Gain Calibration</%block>

<p>This task creates gain solutions for each measurement set.</p>

<ul>
    <li>Plots</li>
    <ul>
    % if phase_vs_time_plots:
        <li><a href="#phase_vs_time_plots">Phase vs time</a></li>
    % endif
    % if amp_vs_time_plots:
        <li><a href="#amp_vs_time_plots">Amplitude vs time</a></li>
    % endif
    </ul>
    <li>Diagnostic plots</li>
    <ul>
    % if diagnostic_phase_vs_time_plots:
        <li><a href="#diagnostic_phase_vs_time_plots">Phase vs time</a></li>
    % endif
    % if diagnostic_phaseoffset_vs_time_plots:
        <li><a href="#diagnostic_phaseoffset_vs_time_plots">Phase offsets vs time</a></li>
    % endif
    % if diagnostic_amp_vs_time_plots:
        <li><a href="#diagnostic_amp_vs_time_plots">Amplitude vs time</a></li>
    % endif
    </ul>
</ul>

<h2>Results</h2>
<table class="table table-bordered" summary="Application Results">
	<caption>Applied calibrations and parameters used for caltable generation</caption>
    <thead>
        <tr>
            <th scope="col" rowspan="2">Measurement Set</th>
			<th scope="col" colspan="2">Solution Parameters</th>
			<th scope="col" colspan="4">Applied To</th>
            <th scope="col" rowspan="2">Calibration Table</th>
		</tr>
		<tr>
			<th>Type</th>
            <th>Interval</th>
			<th>Scan Intent</th>
            <th>Field</th>
			<th>Spectral Windows</th>
            <th>Gainfield</th>
        </tr>
    </thead>
	<tbody>
% for application in applications:
		<tr>
			<td>${application.ms}</td>
		  	<td>${application.calmode}</td>
		  	<td>${application.solint}</td>
		  	<td>${application.intent}</td>
		  	<td>${application.field}</td>
		  	<td>${application.spw}</td>
            <td>${application.gainfield}</td>
		  	<td>${application.gaintable}</td>
		</tr>
% endfor		
	</tbody>
</table>

% if phase_vs_time_plots or amp_vs_time_plots:
<h2>Plots</h2>

<%self:plot_group plot_dict="${phase_vs_time_plots}"
				  url_fn="${lambda x: phase_vs_time_subpages[x]}"
				  data_spw="${True}"
                  data_vis="${True}"
                  sort_row_by="spw"
                  title_id="phase_vs_time_plots">

	<%def name="title()">
		Phase vs time
	</%def>

	<%def name="preamble()">
		<p>Plots show the phase correction to be applied to the target source. 
		A plot is shown for each spectral window, with phase correction data points
		plotted per antenna and correlation as a function of time.</p>

		<p>Click the summary plots to enlarge them, or the spectral window heading to
		see detailed plots per spectral window and antenna.</p> 
	</%def>

    <%def name="ms_preamble(ms)">
        % if ms in spw_mapping_without_check:
            <p>${spw_mapping_without_check[ms]}</p>
        %endif
    </%def>

	<%def name="mouseover(plot)">Click to show phase vs time for spectral window ${plot.parameters['spw']}</%def>

	<%def name="fancybox_caption(plot)">
        ${plot.parameters['vis']}<br>
        Spectral window ${plot.parameters['spw']}
    </%def>

	<%def name="caption_title(plot)">
		Spectral window ${plot.parameters['spw']}
	</%def>

	<%def name="caption_text(plot, intent)"> 
		Phase vs time, all antennas and correlations.</%def>

</%self:plot_group>


<%self:plot_group plot_dict="${amp_vs_time_plots}"
				  url_fn="${lambda x: amp_vs_time_subpages[x]}"
                  data_ant="${True}"
				  data_spw="${True}"
                  data_vis="${True}"
                  sort_row_by="spw"
                  break_rows_by="ant"
                  title_id="amp_vs_time_plots">

	<%def name="title()">
		Amplitude vs time
	</%def>

	<%def name="preamble()">
		<p>Plots show the amplitude calibration to be applied to the target source. 
		A plot is shown for each spectral window and each set of antennas with the
        same antenna diameter, with amplitude correction data points per antenna and
        correlation as a function of time.</p>
	
		<p>Click the summary plots to enlarge them, or the spectral window heading to
		see detailed plots per spectral window and antenna.</p> 
	</%def>

	<%def name="mouseover(plot)">Click to show amplitude vs time for spectral window ${plot.parameters['spw']}</%def>

	<%def name="fancybox_caption(plot)">
        Spectral window ${plot.parameters['spw']}${get_ant_str_for_caption_title(plot.parameters)}
    </%def>

	<%def name="caption_title(plot)">
        Spectral window ${plot.parameters['spw']}${get_ant_str_for_caption_title(plot.parameters)}
	</%def>

	<%def name="caption_text(plot, intent)"> 
		Amplitude vs time for spectral window ${plot.parameters['spw']}, 
        all correlations${get_ant_str_for_caption_text(plot.parameters)}.
	</%def>

</%self:plot_group>

%endif

% if diagnostic_phase_vs_time_plots or diagnostic_phaseoffset_vs_time_plots or diagnostic_amp_vs_time_plots:
<h2>Diagnostic plots</h2>

<%self:plot_group plot_dict="${diagnostic_phase_vs_time_plots}"
				  url_fn="${lambda x: diagnostic_phase_vs_time_subpages[x]}"
				  data_spw="${True}"
                  data_vis="${True}"
                  sort_row_by="spw"
                  title_id="diagnostic_phase_vs_time_plots">

	<%def name="title()">
		Phase vs time
	</%def>

	<%def name="preamble()">
		<p>These diagnostic plots show the phase solution for a calibration
            generated using a short solution interval (in the case of the checksource(s), 
            they are taken from the tables generated by hifa_gfluxscale). In case of very 
            low SNR on a particular phase calibrator or check source (if present), solutions 
            averaged in time with a solint = 1/4 the scan time are shown for that field. 
            This calibration is not applied to the target. One plot is shown for each 
            non-combined spectral window, with phase correction plotted per antenna and
            correlation as a function of time.</p>

		<p>Click the summary plots to enlarge them, or the spectral window
            heading to see detailed plots per spectral window and antenna.</p>
	</%def>

    <%def name="ms_preamble(ms)">
        % if ms in spw_mapping:
        <p>${spw_mapping[ms]}</p>
        %endif
        <p>Plots show the diagnostic phase calibration for ${ms}.
            <!-- calculated using solint='${diagnostic_solints[ms]['phase']}'. -->
        </p>
    </%def>

	<%def name="mouseover(plot)">Click to show phase vs time for spectral window ${plot.parameters['spw']}</%def>

	<%def name="fancybox_caption(plot)">
		${plot.parameters['vis']}<br>
		Spectral window ${plot.parameters['spw']}<br>
	</%def>

	<%def name="caption_title(plot)">
		Spectral window ${plot.parameters['spw']}
	</%def>

	<%def name="caption_text(plot, intent)">
		Phase vs time, all antennas and correlations.
	</%def>

</%self:plot_group>

<%self:plot_group plot_dict="${diagnostic_phaseoffset_vs_time_plots}"
				  url_fn="${lambda x: diagnostic_phaseoffset_vs_time_subpages[x]}"
				  data_spw="${True}"
                  data_vis="${True}"
                  sort_row_by="spw"
                  title_id="diagnostic_phaseoffset_vs_time_plots">

	<%def name="title()">
		Phase offsets vs time
	</%def>

	<%def name="preamble()">
		<p>These diagnostic plots show the phase offsets as a function of time. The phase offsets 
            are computed by preapplying an spw-combined phase-only solution to the phase calibrator 
            data and computing a new phase solution for each spw. The new phase solutions should scatter 
            about zero with no drift. The points shown for the other calibrators will be zero. The new 
            solutions are not applied to the target. One plot is shown for each spectral window, with 
            phase offset plotted per antenna and correlation as a function of time.</p>
                
		<p>Click the summary plots to enlarge them, or the spectral window
            heading to see detailed plots per spectral window and antenna.</p>
	</%def>

    <%def name="ms_preamble(ms)">
        <p>Plots show the diagnostic phase offsets for ${ms} calculated using solint='inf'.</p>
    </%def>

	<%def name="mouseover(plot)">Click to show phase offset vs time for spectral window ${plot.parameters['spw']}</%def>

	<%def name="fancybox_caption(plot)">Spectral window ${plot.parameters['spw']}</%def>

	<%def name="caption_title(plot)">
		Spectral window ${plot.parameters['spw']}
	</%def>

	<%def name="caption_text(plot, intent)"> 
		Phase offset vs time for spectral window <strong>${plot.parameters['spw']}</strong>,
        all antennas and correlations.
		${plot.captionmessage}
	</%def>

</%self:plot_group>


<%self:plot_group plot_dict="${diagnostic_amp_vs_time_plots}"
				  url_fn="${lambda x: diagnostic_amp_vs_time_subpages[x]}"
                  data_ant="${True}"
				  data_spw="${True}"
                  data_vis="${True}"
                  sort_row_by="spw"
                  break_rows_by="ant"
                  title_id="diagnostic_amp_vs_time_plots">

	<%def name="title()">
		Amplitude vs time
	</%def>

	<%def name="preamble()">
		<p>These diagnostic plots show the amplitude solution for a calibration
            generated using a short solution interval. This calibration is not applied
            to the target. One plot is shown for each non-combined spectral
            window and each set of antennas with the same antenna diameter, with
            amplitude correction plotted per antenna and correlation as a function of time.</p>

		<p>Click the summary plots to enlarge them, or the spectral window
            heading to see detailed plots per spectral window and antenna.</p>
	</%def>

    <%def name="ms_preamble(ms)">
        <p>Plots show the diagnostic amplitude calibration for ${ms} calculated
            using solint='${diagnostic_solints[ms]['amp']}'.</p>
    </%def>

	<%def name="mouseover(plot)">Click to show amplitude vs time for spectral window ${plot.parameters['spw']}</%def>

	<%def name="fancybox_caption(plot)">
        Spectral window ${plot.parameters['spw']}${get_ant_str_for_caption_title(plot.parameters)}
    </%def>

	<%def name="caption_title(plot)">
		Spectral window ${plot.parameters['spw']}${get_ant_str_for_caption_title(plot.parameters)}
	</%def>

	<%def name="caption_text(plot, intent)">
		Amplitude vs time for spectral window ${plot.parameters['spw']},
        all correlations${get_ant_str_for_caption_text(plot.parameters)}.
	</%def>

</%self:plot_group>

% endif
