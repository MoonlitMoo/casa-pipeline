<%!
rsc_path = ""
import os
import pipeline.infrastructure.renderer.htmlrenderer as hr
from pipeline.infrastructure.renderer import rendererutils
%>
<%inherit file="t2-4m_details-base.mako"/>

<%block name="title">Initial test calibrations</%block>

<p>Initial test calibrations using bandpass and delay calibrators</p>

% for ms in summary_plots:
    <h4>Plots:  <a class="replace"
           href="${rendererutils.get_relative_url(pcontext.report_dir, dirname, testdelay_subpages[ms])}">Test delay plots </a>|
        <a class="replace"
           href="${rendererutils.get_relative_url(pcontext.report_dir, dirname, ampgain_subpages[ms])}">Gain Amplitude </a>|
        <a class="replace"
           href="${rendererutils.get_relative_url(pcontext.report_dir, dirname, phasegain_subpages[ms])}">Gain Phase </a>|
        <a class="replace"
           href="${rendererutils.get_relative_url(pcontext.report_dir, dirname, bpsolamp_subpages[ms])}">BP Amp solution </a>|
        <a class="replace"
           href="${rendererutils.get_relative_url(pcontext.report_dir, dirname, bpsolphase_subpages[ms])}">BP Phase solution </a>
    </h4>
%endfor


<%self:plot_group plot_dict="${summary_plots}"
                  url_fn="${lambda ms: 'noop'}">

        <%def name="title()">
            testBPdcals summary plot
        </%def>

        <%def name="preamble()">
        </%def>

        <%def name="mouseover(plot)">Summary window </%def>

        <%def name="fancybox_caption(plot)">
          Initial calibrated bandpass
        </%def>

        <%def name="caption_title(plot)">
           Initial calibrated bandpass
        </%def>
</%self:plot_group>
<%self:plot_group plot_dict="${summary_plots_per_spw}"
                  url_fn="${lambda ms: 'noop'}">

        <%def name="title()">
            testBPdcals per spectral line spw summary plot
        </%def>

        <%def name="preamble()">
        </%def>

        <%def name="mouseover(plot)">Per-spw summary window </%def>

        <%def name="fancybox_caption(plot)">
            Initial calibrated bandpass,
            Spw: ${plot.parameters['spw']}<br>
        </%def>

        <%def name="caption_title(plot)">
            Initial calibrated bandpass,
            Spw: ${plot.parameters['spw']}<br>
        </%def>
</%self:plot_group>

<h3>Flag bad deformatters</h3>

<p>Identify and flag basebands with bad deformatters or RFI based on bandpass (BP) table amps and phases.</p>

        % for single_result in result:

            <h3>BP Table Amps</h3>
            <table class="table table-bordered table-striped table-condensed" summary="Deformatter Flagging Amp">
	        <caption></caption>
	        <thead>
		    <tr>
			    <th>Antenna</th>
			    <th>SPWs</th>
			    <th>Band / Basebands</th>
		    </tr>
	        </thead>

	        <tbody>

            % for bandname, result_amp_perband in single_result.result_amp.items():
	            % if result_amp_perband == []:
	            <tr>
	            <td>None</td>
	            <td>None</td>
	            <td>${bandname}</td>
	            </tr>
	            % else:
	                % for key, valueDict in single_result.amp_collection[bandname].items():
	                <tr>
	                <td>${key}</td>
	                <td>${','.join(valueDict['spws'])}</td>
	                <td>${','.join(valueDict['basebands'])}</td>
	                </tr>
	                % endfor
	            % endif
	        % endfor

	        </tbody>
            </table>

            <br>

            <h3>BP Table Phases</h3>
            <table class="table table-bordered table-striped table-condensed" summary="Deformatter Flagging Phase">
	        <caption></caption>
	        <thead>
		    <tr>
			    <th>Antenna</th>
			    <th>SPWs</th>
			    <th>Band / Basebands</th>
		    </tr>
	        </thead>

	        <tbody>

            % for bandname, result_phase_perband in single_result.result_phase.items():
	            % if result_phase_perband == []:
	                <tr>
	                <td>None</td>
	                <td>None</td>
	                <td>${bandname}</td>
	                </tr>
	            % else:
	                % for key, valueDict in single_result.phase_collection[bandname].items():
	                    <tr>
	                    <td>${key}</td>
	                    <td>${','.join(valueDict['spws'])}</td>
	                    <td>${','.join(valueDict['basebands'])}</td>
	                    </tr>
	                % endfor
	            % endif
	        % endfor

	        </tbody>
            </table>

        % endfor
