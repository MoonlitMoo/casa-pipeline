<%!
rsc_path = ""
import os
import pipeline.infrastructure.renderer.htmlrenderer as hr

def is_rejected(keep):
    desc=''
    if not keep:
        desc = '<a style="color:red">rejected</a>'
    return desc
%>
<%inherit file="t2-4m_details-base.mako"/>

<%
termlist = [s.replace('.','') for s in info_dict['multitermlist']]
if termlist[0]=='':
    termlabel = ''
else:
    termlabel = '{} when multi-term'.format('/'.join(termlist))
%>

<%block name="title">Primary beam corrected images</%block>

<p>${termlabel}</p>

% for ms,ms_pbcorplots in pbcorplots.items():

    <%self:plot_group plot_dict="${ms_pbcorplots}"
                                    url_fn="${lambda basename:  'noop'}">

            <%def name="title()">
            ${ms}
            </%def>

            <%def name="preamble()">
            </%def>

            <%def name="ms_preamble(basename)">
            ${is_rejected(info_dict[basename])}
            <%
                # restored stats
                pbcor_min = plotter.result.pbcor_stats[basename].get('min')[0]
                pbcor_max = plotter.result.pbcor_stats[basename].get('max')[0]
                pbcor_sigma = plotter.result.pbcor_stats[basename].get('sigma')[0]
                pbcor_madRMS = plotter.result.pbcor_stats[basename].get('medabsdevmed')[0] * 1.4826  # see CAS-9631 
                pbcor_unit = 'Jy/beam'

                # residual of pb corrected image stats
                residual_min = plotter.result.residual_stats[basename].get('min')[0]
                residual_max = plotter.result.residual_stats[basename].get('max')[0]
                residual_sigma = plotter.result.residual_stats[basename].get('sigma')[0]
                residual_madRMS = plotter.result.residual_stats[basename].get('medabsdevmed')[0] * 1.4826  # see CAS-9631 
                residual_unit = 'Jy/beam'
            %>       

            <table class="table">
                <tr>
                    <th></th>
                    <th>restored</th>
                    <th>residual</th>
                </tr>
                <tr>
                    <td><strong>maximum</strong></td>
                    <td>${'{:.4e}'.format(pbcor_max)} ${pbcor_unit}</td>
                    <td>${'{:.4e}'.format(residual_max)} ${residual_unit}</td>
                </tr>
                <tr>
                    <td><strong>minimum</strong></td>
                    <td>${'{:.4e}'.format(pbcor_min)} ${pbcor_unit}</td>
                    <td>${'{:.4e}'.format(residual_min)} ${residual_unit}</td>
                </tr>
                <tr>
                    <td><strong>sigma</strong></td>
                    <td>${'{:.4e}'.format(pbcor_sigma)}  ${pbcor_unit}</td>
                    <td>${'{:.4e}'.format(residual_sigma)} ${residual_unit}</td>
                </tr>
                <tr>
                    <td><strong>MAD rms</strong></td>
                    <td>${'{:.4e}'.format(pbcor_madRMS)} ${pbcor_unit}</td>
                    <td>${'{:.4e}'.format(residual_madRMS)} ${residual_unit}</td>
                </tr>
            </table>                   


            </%def>



            <%def name="mouseover(plot)">${plot.basename}</%def>



            <%def name="fancybox_caption(plot)">
            primary beam corrected plot (tt0/tt1 when multi-term)
            </%def>


            <%def name="caption_title(plot)">
            ${plot.basename}
            </%def>
    </%self:plot_group>

% endfor
