<%!
rsc_path = ""
from pipeline.infrastructure.renderer import rendererutils
%>
<%inherit file="t2-4m_details-base.mako"/>

<%block name="title">Polarization Calibration</%block>

<p>This task creates polarization solutions for each polarization session of measurement sets.</p>

<h2>Contents</h2>
<ul>
  <li><a href="#sessions">Sessions</a></li>
  <li><a href="#sessions">Polarization</a></li>
  <ul>
    <li><a href="#residpol">Residual polarization after calibration</a></li>
    <li><a href="#polcalpol">Derived polarization of the polarization calibrator</a></li>
  </ul>
  <li><a href="#plots">Plots</a></li>
  <ul>
    <li><a href="#amp_vs_parang_plots">Amplitude vs. Parallactic Angle</a></li>
    <li><a href="#amp_pol_ratio_vs_scan_plots">Gain Amplitude Polarization Ratio vs. Scan</a></li>
    <li><a href="#xy_phase_vs_channel_plots">Cross-hand Phase vs. Channel</a></li>
    <li><a href="#dterms_gain_vs_channel_plots">D-terms Solutions Gain vs. Channel</a></li>
    <li><a href="#gain_ratio_rms_vs_scan_plots">Gain Ratio RMS vs. Scan</a></li>
    <li><a href="#amp_vs_ant_plots">X,Y amplitude vs. antenna</a></li>
    <li><a href="#ampratio_vs_ant_plots">X/Y amplitude gain ratio vs. antenna</a></li>
    <li><a href="#real_vs_imag_plots">Real vs. Imaginary</a></li>
  </ul>
</ul>


<h2 id="sessions" class="jumptarget">Sessions</h2>
<table class="table table-bordered table-striped">
    <caption>Summary of polarization calibrator per session.</caption>
    <thead>
        <tr>
            <th scope="col">Session</th>
            <th scope="col">Measurement Sets</th>
            <th scope="col">Polarization Calibrator</th>
            <th scope="col">Reference Antenna</th>
        </tr>
    </thead>
    <tbody>
    % for session_name in session_names:
        <%
        nvis = len(vislists[session_name])
        %>
        <tr>
        % if nvis == 0:
            <td rowspan="1">${session_name}</td>
        % else:
            <td rowspan="${nvis}">${session_name}</td>
            <td>${vislists[session_name][0]}</td>
            <td rowspan="${len(vislists[session_name])}">${polfields[session_name]}</td>
            <td rowspan="${len(vislists[session_name])}">${refants[session_name]}</td>
        % endif
        </tr>
        % if len(vislists[session_name]) > 1:
            % for vis in vislists[session_name][1:]:
            <tr>
                <td>${vis}</td>
            </tr>
            % endfor
        % endif
    % endfor
    </tbody>
</table>

<h2 id="polarization" class="jumptarget">Polarization</h2>

<h3 id="residpol" class="jumptarget">Residual polarization after calibration</h3>
<table class="table table-bordered table-striped">
    <caption>Residual polarization after calibration.</caption>
    <thead>
        <tr>
            <th scope="col" rowspan="2">Session</th>
            <th scope="col" rowspan="2">Polarization Calibrator</th>
            <th scope="col" rowspan="2">Spectral Window</th>
            <th  scope="col" colspan="4">Fractional Stokes</th>
        </tr>
	    <tr>
	        <th scope="col">I</th>
	        <th scope="col">Q</th>
	        <th scope="col">U</th>
	        <th scope="col">V</th>
	    </tr>
	</thead>
    <tbody>
    % for tr in residual_pol_table_rows:
		<tr>
		% for td in tr:
			${td}
		% endfor
		</tr>
    % endfor
    </tbody>
</table>

<h3 id="polcalpol" class="jumptarget">Derived polarization of the polarization calibrator</h3>
<table class="table table-bordered table-striped">
    <caption>Derived polarization of the polarization calibrator.</caption>
    <thead>
        <tr>
            <th scope="col" rowspan="2">Session</th>
            <th scope="col" rowspan="2">Polarization Calibrator</th>
            <th scope="col" rowspan="2">Spectral Window</th>
            <th  scope="col" colspan="4">Fractional Stokes</th>
        </tr>
	    <tr>
	        <th scope="col">I</th>
	        <th scope="col">Q</th>
	        <th scope="col">U</th>
	        <th scope="col">V</th>
	    </tr>
	</thead>
    <tbody>
    % for tr in polcal_table_rows:
		<tr>
		% for td in tr:
			${td}
		% endfor
		</tr>
    % endfor
    </tbody>
</table>

<h2 id="plots" class="jumptarget">Plots</h2>

<%self:plot_group plot_dict="${amp_vs_parang}"
                  url_fn="${lambda x: 'noop'}"
                  data_vis="${True}"
                  data_spw="${True}"
                  title_id="amp_vs_parang_plots">

    <%def name="title()">
        Amplitude vs. Parallactic Angle
    </%def>

    <%def name="preamble()">
        <p>These plots show the amplitude vs. parallactic angle for the polarization calibrator before polarization
        calibration.</p>

        <p>Data are plotted per spectral window for all antennas and correlations XX and YY, colorized by
        correlation.</p>

        <p>Click the plots to enlarge them.</p>
    </%def>

    <%def name="mouseover(plot)">Click to show Amplitude vs. Parallactic Angle</%def>

    <%def name="fancybox_caption(plot)">
        ${plot.parameters['vis']}<br>
        SpW: ${plot.parameters['spw']}<br>
    </%def>

    <%def name="caption_title(plot)">
        ${plot.parameters['vis']}<br>
        SpW: ${plot.parameters['spw']}<br>
    </%def>

    <%def name="caption_text(plot, _)">
        Amplitude vs. Parallactic Angle
    </%def>

</%self:plot_group>

<%self:plot_group plot_dict="${amp_vs_scan}"
                  url_fn="${lambda x: 'noop'}"
                  data_vis="${True}"
                  title_id="amp_pol_ratio_vs_scan_plots"
                  sort_row_by="spw">

    <%def name="title()">
        Gain amplitude polarization ratio vs. Scan
    </%def>

    <%def name="preamble()">
        <p>These plots show the gain amplitude polarization ratio vs. scan for the polarization calibrator prior to and
        after polarization calibration.</p>

        <p>Data are plotted for all antennas and spectral windows, colorized by spectral window.</p>

        <p>Click the plots to enlarge them.</p>
    </%def>

    <%def name="mouseover(plot)">
        Click to show Gain amplitude polarization ratio vs. Scan ${plot.parameters['calib']} calibration
    </%def>

    <%def name="fancybox_caption(plot)">
        ${plot.parameters['vis']}<br>
        ${plot.parameters['calib'].capitalize()} calibration
    </%def>

    <%def name="caption_title(plot)">
        ${plot.parameters['vis']}<br>
        ${plot.parameters['calib'].capitalize()} calibration
    </%def>

    <%def name="caption_text(plot, _)">
        Gain amplitude polarization ratio vs. Scan ${plot.parameters['calib']} calibration.
    </%def>

</%self:plot_group>

<%self:plot_group plot_dict="${phase_vs_channel}"
                  url_fn="${lambda x: 'noop'}"
                  data_vis="${True}"
                  title_id="xy_phase_vs_channel_plots">

    <%def name="title()">
        Cross-hand phase vs. Channel
    </%def>

    <%def name="preamble()">
        <p>These plots show the cross-hand (XY) phase vs. channel for the polarization calibrator.</p>

        <p>Data are plotted for all antennas and spectral windows, colorized by spectral window.</p>

        <p>Click the plots to enlarge them.</p>
    </%def>

    <%def name="mouseover(plot)">Click to show Cross-hand phase vs. Channel</%def>

    <%def name="fancybox_caption(plot)">
        ${plot.parameters['vis']}
    </%def>

    <%def name="caption_title(plot)">
        ${plot.parameters['vis']}
    </%def>

    <%def name="caption_text(plot, _)">
        Cross-hand phase vs. Channel.
    </%def>

</%self:plot_group>

<%self:plot_group plot_dict="${leak_summary}"
                  url_fn="${lambda x: leak_subpages[x]}"
                  data_vis="${True}"
                  data_spw="${True}"
                  title_id="dterms_gain_vs_channel_plots"
                  sort_row_by="spw">

    <%def name="title()">
        D-terms solutions gain vs. channel
    </%def>

    <%def name="preamble()">
        <p>These plots show the real and imaginary component of the D-terms solutions gain vs. channel for the
        polarization calibrator.</p>

        <p>Data are plotted per SpW for all antennas, colorized by antenna. Click on the summary plots to enlarge
        them.</p>

        <p>Click on the session MS heading to show detailed plots for that session, or on the links in the summary
        plot captions to show detailed plots (per antenna) for that session and SpW.
    </%def>

    <%def name="mouseover(plot)">
        Click to show D-terms Solutions Gain ${plot.parameters['yaxis']} vs. Channel
    </%def>

    <%def name="fancybox_caption(plot)">
        ${plot.parameters['vis']}<br>
        Gain: ${plot.parameters['yaxis']}<br>
        SpW: ${plot.parameters['spw']}<br>
    </%def>

    <%def name="caption_title(plot)">
        ${plot.parameters['vis']}<br>
        Gain: ${plot.parameters['yaxis']}<br>
        SpW: ${plot.parameters['spw']}<br>
    </%def>

    <%def name="caption_text(plot, _)">
        D-terms Solutions Gain ${plot.parameters['yaxis']} vs. Channel.
    </%def>

</%self:plot_group>

<%self:plot_group plot_dict="${gain_ratio_rms_vs_scan}"
                  url_fn="${lambda x: 'noop'}"
                  data_vis="${True}"
                  title_id="gain_ratio_rms_vs_scan_plots">

    <%def name="title()">
        Gain Ratio RMS vs. Scan
    </%def>

    <%def name="preamble()">
        <p>These plots show the gain ratio RMS vs. scan for the polarization calibrator, before and after
        polarization calibration.</p>

        <p>Data are plotted for all antennas and spectral windows.</p>

        <p>Click the plots to enlarge them.</p>
    </%def>

    <%def name="ms_preamble(ms)">
        <p>ID of scan with highest XY signal: ${scanid_highest_xy[ms]}</p>
    </%def>

    <%def name="mouseover(plot)">Click to show Gain Ratio RMS vs. Scan</%def>

    <%def name="fancybox_caption(plot)">
        ${plot.parameters['vis']}
    </%def>

    <%def name="caption_title(plot)">
        ${plot.parameters['vis']}
    </%def>

    <%def name="caption_text(plot, _)">
        Gain Ratio RMS vs. Scan before and after polarization calibration.
    </%def>

</%self:plot_group>

<%self:plot_group plot_dict="${amp_vs_ant}"
                  url_fn="${lambda x: 'noop'}"
                  data_vis="${True}"
                  data_spw="${True}"
                  title_id="amp_vs_ant_plots">

    <%def name="title()">
        X,Y amplitude vs. antenna
    </%def>

    <%def name="preamble()">
        <p>These plots show the X,Y amplitude vs. antenna for the polarization calibrator.</p>

        <p>Data are plotted per spectral window for all antennas, colorized by antenna.</p>

        <p>Click the plots to enlarge them.</p>
    </%def>

    <%def name="mouseover(plot)">Click to show X,Y Amplitude vs. Antenna</%def>

    <%def name="fancybox_caption(plot)">
        ${plot.parameters['vis']}<br>
        SpW: ${plot.parameters['spw']}<br>
    </%def>

    <%def name="caption_title(plot)">
        ${plot.parameters['vis']}<br>
        SpW: ${plot.parameters['spw']}<br>
    </%def>

    <%def name="caption_text(plot, _)">
        X,Y Amplitude vs. Antenna.
    </%def>

</%self:plot_group>

<%self:plot_group plot_dict="${ampratio_vs_ant}"
                  url_fn="${lambda x: 'noop'}"
                  data_vis="${True}"
                  data_spw="${True}"
                  title_id="ampratio_vs_ant_plots">

    <%def name="title()">
        X/Y amplitude gain ratio vs. antenna
    </%def>

    <%def name="preamble()">
        <p>These plots show the X/Y amplitude gain ratio vs. antenna for the polarization calibrator.</p>

        <p>Data are plotted per spectral window for all antennas, colorized by antenna.</p>

        <p>Click the plots to enlarge them.</p>
    </%def>

    <%def name="mouseover(plot)">Click to show X/Y Amplitude Gain Ratio vs. Antenna</%def>

    <%def name="fancybox_caption(plot)">
        ${plot.parameters['vis']}<br>
        SpW: ${plot.parameters['spw']}<br>
    </%def>

    <%def name="caption_title(plot)">
        ${plot.parameters['vis']}<br>
        SpW: ${plot.parameters['spw']}<br>
    </%def>

    <%def name="caption_text(plot, _)">
        X/Y Amplitude Gain Ratio vs. Antenna.
    </%def>

</%self:plot_group>

<%self:plot_group plot_dict="${real_vs_imag}"
                  url_fn="${lambda x: 'noop'}"
                  data_vis="${True}"
                  title_id="real_vs_imag_plots">

    <%def name="title()">
        Real vs. Imaginary
    </%def>

    <%def name="preamble()">
        <p>These plots show the real vs. imaginary component for the polarization calibrator after polarization
        calibration, for correlations XX,YY and XY,YX.</p>

        <p>Data are plotted for all antennas and spectral windows, colorized by correlation.</p>

        <p>Click the plots to enlarge them.</p>
    </%def>

    <%def name="mouseover(plot)">Click to show Real vs. Imag ${plot.parameters['correlation']}</%def>

    <%def name="fancybox_caption(plot)">
        ${plot.parameters['vis']}<br>
        Corr: ${plot.parameters['correlation']}
    </%def>

    <%def name="caption_title(plot)">
        ${plot.parameters['vis']}<br>
        Corr: ${plot.parameters['correlation']}
    </%def>

    <%def name="caption_text(plot, _)">
        Real vs. Imaginary ${plot.parameters['correlation']}
    </%def>

</%self:plot_group>
