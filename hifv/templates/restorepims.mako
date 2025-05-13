<%!
rsc_path = ""
import os
import pipeline.infrastructure.renderer.htmlrenderer as hr
%>
<%inherit file="t2-4m_details-base.mako"/>

<%

def format_cell(value):
    if isinstance(value, bool):
        if value:
            return '<span class="glyphicon glyphicon-ok"></span>'
        else:
            return '<span class="glyphicon glyphicon-remove"></span>'    
    elif isinstance(value, list):
            return '<br>'.join(map(str,value))
    else:
        return str(value)

%>

<%block name="title">Restore PIMS</%block>

<p>Restore RFI-flagged and self-calibrated visibility for a per-image measurement set (PIMS), using reimaging resources from the single-epoch continuum imaging products.</p>



% for r in result:

    <%
    rr =r.restore_resources
    item_list=[('Reimaging Resources Tarball','reimaging_resources'),
               ('Flags Directory','flag_dir'),
               ('Flag Restore Version','flag_version'),
               ('Selfcal Table','selfcal_table'),
               ('Tier 1 Mask','tier1_mask'),
               ('Combined Final Mask','tier2_mask'),
               ('Selfcal Model Image(s)','model_images')]
    %>
    <table class="table table-bordered table-striped table-condensed">
        <tbody>
            % for name, key in item_list:
            <tr>
                <th style="vertical-align:middle">${name}</th>
                <td style="vertical-align:middle">${format_cell(rr[key][0])}</td>
                <td style="vertical-align:middle">${format_cell(rr[key][1])}</td>
            </tr>
            % endfor                    
        </tbody>
    </table>

% endfor