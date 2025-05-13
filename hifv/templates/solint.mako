<%!
rsc_path = ""
import os
import pipeline.infrastructure.renderer.htmlrenderer as hr
from pipeline.infrastructure.renderer import rendererutils
%>
<%inherit file="t2-4m_details-base.mako"/>

<%block name="title">Solution Interval and test gain calibrations</%block>

<p>Determine the solution interval for a scan-average equivalent and do test gain calibrations to establish a short solution interval.</p>

% for ms in summary_plots:
    <ul>
        <li>The long solution intervals per band are: <b> ${longsolint[ms]}</b>.</li>
        <li>The short solution intervals per band that are used: <b>${new_gain_solint1[ms]}</b>.</li>
    </ul>

    <h4>Plots:  <a class="replace"
           href="${rendererutils.get_relative_url(pcontext.report_dir, dirname, testgainsamp_subpages[ms])}">Testgains amp plots </a> |
        <a class="replace"
           href="${rendererutils.get_relative_url(pcontext.report_dir, dirname, testgainsphase_subpages[ms])}">Testgains phase plots</a>
    </h4>
%endfor
