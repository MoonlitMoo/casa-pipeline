<%inherit file="t2-4m_details-base.mako"/>

<%block name="title">Differential Gain Calibration</%block>

<p>This task creates the spectral window phase gain offset table used to allow
calibrating the "science" spectral setup with phase gains from a "reference"
spectral setup. A bright point source Quasar, called the Differential Gain
Calibrator (DIFFGAIN) source, is used for this purpose. This DIFFGAIN source is
typically observed in groups of interleaved "reference" and "on-source" scans,
once at the start and once at the end of the observations. In very long
observations, there may be a group of scans occurring during the middle. Scan
groups are combined while solving for SpW offsets between "reference" and
"on-source" spectral setups.

<h2 id="results">Results</h2>

<h3>Differential gain calibration</h3>
<table class="table table-bordered" summary="Application Results">

    <caption>Applied calibrations and parameters used for caltable generation</caption>
    <thead>
        <tr>
            <th scope="col" rowspan="2">Measurement Set</th>
            <th scope="col" colspan="2">Solution Parameters</th>
            <th scope="col" colspan="2">Applied To</th>
            <th scope="col" rowspan="2">Calibration Table</th>
        </tr>
        <tr>
            <th>Type</th>
            <th>Interval</th>
            <th>Scan Intent</th>
            <th>Spectral Windows</th>
        </tr>
    </thead>
    <tbody>
    % for application in applications:
        <tr>
            <td>${application.ms}</td>
            <td>${application.calmode}</td>
            <td>${application.solint}</td>
            <td>${application.intent}</td>
            <td>${application.spw}</td>
            <td>${application.gaintable}</td>
        </tr>
    % endfor
    </tbody>
</table>

% if phase_vs_time_plots:
<h2>Plots</h2>

<%self:plot_group plot_dict="${phase_vs_time_plots}"
				  url_fn="${lambda x: phase_vs_time_subpages[x]}"
				  data_spw="${True}"
                  data_vis="${True}"
                  sort_row_by="spw"
                  title_id="phase_vs_time_plots">

	<%def name="title()">
		Band offsets vs time
	</%def>

	<%def name="preamble()">
		<p>Plots show the diffgain phase correction to be applied to the target
		source. A plot is shown for each spectral window, with phase correction
		data points plotted per antenna and correlation as a function of time.
		</p>

		<p>Click the summary plots to enlarge them, or the spectral window
		heading to see detailed plots per spectral window and antenna.</p>
	</%def>

	<%def name="mouseover(plot)">
	    Click to show phase vs time for spectral window ${plot.parameters['spw']}
	</%def>

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

% endif

% if offset_vs_time_plots:
<h2>Diagnostic plots</h2>

<%self:plot_group plot_dict="${offset_vs_time_plots}"
				  url_fn="${lambda x: offset_vs_time_subpages[x]}"
				  data_spw="${True}"
                  data_vis="${True}"
                  sort_row_by="spw"
                  title_id="offset_vs_time_plots">

	<%def name="title()">
		Residual phase offsets vs time
	</%def>

	<%def name="preamble()">
		<p>These diagnostic plots show the diffgain phase offsets as a function
		of time. The phase offsets are computed by preapplying a scan-combined
		phase-only solution to the diffgain calibrator data and computing a new
		phase solution for each spw. The new phase solutions should scatter
		about zero with no drift. The new solutions are not applied to the
		target. One plot is shown for each spectral window, with phase offset
		plotted per antenna and correlation as a function of time.</p>

		<p>Click the summary plots to enlarge them, or the spectral window
        heading to see detailed plots per spectral window and antenna.</p>
	</%def>

    <%def name="ms_preamble(ms)">
        <p>Plots show the diagnostic phase offsets for ${ms} calculated using solint='inf'.</p>
    </%def>

	<%def name="mouseover(plot)">
	    Click to show phase offset vs time for spectral window ${plot.parameters['spw']}
	</%def>

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

% endif
