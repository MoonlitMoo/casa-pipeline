<%!
rsc_path = ""
import os
import pipeline.infrastructure.renderer.htmlrenderer as hr
from pipeline.infrastructure.renderer import rendererutils
%>
<%inherit file="t2-4m_details-base.mako"/>

<%block name="title">Selfcal</%block>

<p>Pipeline task running gaincal and applycal: selfcalmode=${result[0].inputs['selfcalmode']}</p>


<%self:plot_group plot_dict="${summary_plots}"
                                  url_fn="${lambda ms:  'noop'}">

        <%def name="title()">
            Selfcal Summary Plot
        </%def>

        <%def name="preamble()">
        </%def>

        <%def name="mouseover(plot)">Summary window</%def>

        <%def name="fancybox_caption(plot)">
            Plot of ${plot.y_axis} vs. ${plot.x_axis}
        </%def>

        <%def name="caption_title(plot)">
            Plot of ${plot.y_axis} vs. ${plot.x_axis}
        </%def>
</%self:plot_group>



% for ms in summary_plots:


    <h4>Plots:  <a class="replace"
           href="${rendererutils.get_relative_url(pcontext.report_dir, dirname, selfcalphasegaincal_subpages[ms])}">Phase vs. time plots </a>
    </h4>

%endfor

