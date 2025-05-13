<%!
rsc_path = ""
import os
import pipeline.infrastructure.renderer.htmlrenderer as hr
from pipeline.infrastructure.renderer import rendererutils
%>
<%inherit file="t2-4m_details-base.mako"/>

<%block name="title">Semi-final delay and bandpass calibrations</%block>

<p>Perform semi-final delay and bandpass calibrations, as the spectral index
of the bandpass calibrator has not yet been determined.</p>

% for ms in summary_plots:
    <h4>Plots:  <a class="replace"
           href="${rendererutils.get_relative_url(pcontext.report_dir, dirname, delay_subpages[ms])}">Delay plots </a>|
        <a class="replace"
           href="${rendererutils.get_relative_url(pcontext.report_dir, dirname, phasegain_subpages[ms])}">Gain phase </a>|
        <a class="replace"
           href="${rendererutils.get_relative_url(pcontext.report_dir, dirname, bpsolamp_subpages[ms])}">BP Amp solution </a>|
        <a class="replace"
           href="${rendererutils.get_relative_url(pcontext.report_dir, dirname, bpsolphase_subpages[ms])}">BP Phase solution </a>
    </h4>
%endfor

<br>

<%self:plot_group plot_dict="${summary_plots}"
                  url_fn="${lambda ms: 'noop'}">

        <%def name="title()">
            semiFinalBPdcals summary plot
        </%def>

        <%def name="preamble()">
        </%def>

        <%def name="mouseover(plot)">Summary window </%def>

        <%def name="fancybox_caption(plot)">
          Semi-final calibrated bandpass
        </%def>

        <%def name="caption_title(plot)">
           Semi-final calibrated bandpass
        </%def>
</%self:plot_group>
<%self:plot_group plot_dict="${summary_plots_per_spw}"
                  url_fn="${lambda ms: 'noop'}">

        <%def name="title()">
            semiFinalBPdcals per spectral line spw summary plot
        </%def>

        <%def name="preamble()">
        </%def>

        <%def name="mouseover(plot)">Per-spw summary window </%def>

        <%def name="fancybox_caption(plot)">
            Semi-final calibrated bandpass,
            Spw: ${plot.parameters['spw']}<br>
        </%def>

        <%def name="caption_title(plot)">
            Semi-final calibrated bandpass,
            Spw: ${plot.parameters['spw']}<br>
        </%def>
</%self:plot_group>