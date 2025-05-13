<%!
rsc_path = ""
import os
import pipeline.infrastructure.renderer.htmlrenderer as hr
from pipeline.infrastructure.renderer import rendererutils
%>
<%inherit file="t2-4m_details-base.mako"/>

<%block name="title">Final calibration tables</%block>

<p>Make the final calibration tables.</p>

% for ms in summary_plots:
    <h4>Plots: <br> <a class="replace"
           href="${rendererutils.get_relative_url(pcontext.report_dir, dirname, finaldelay_subpages[ms])}">Final delay plots </a> |
        <a class="replace"
           href="${rendererutils.get_relative_url(pcontext.report_dir, dirname, phasegain_subpages[ms])}">BP initial gain phase </a> |
        <a class="replace"
           href="${rendererutils.get_relative_url(pcontext.report_dir, dirname, bpsolamp_subpages[ms])}">BP Amp solution </a> |
        <a class="replace"
           href="${rendererutils.get_relative_url(pcontext.report_dir, dirname, bpsolphase_subpages[ms])}">BP Phase solution </a> |
        <a class="replace"
           href="${rendererutils.get_relative_url(pcontext.report_dir, dirname, bpsolphaseshort_subpages[ms])}">Phase (short) gain solution</a> |
        <a class="replace"
           href="${rendererutils.get_relative_url(pcontext.report_dir, dirname, finalamptimecal_subpages[ms])}">Final amp time cal </a> |
        <a class="replace"
           href="${rendererutils.get_relative_url(pcontext.report_dir, dirname, finalampfreqcal_subpages[ms])}">Final amp freq cal </a> |
        <a class="replace"
           href="${rendererutils.get_relative_url(pcontext.report_dir, dirname, finalphasegaincal_subpages[ms])}">Final phase gain cal </a>
    </h4>
%endfor
