<%!
rsc_path = ""
import os
import pipeline.infrastructure.renderer.htmlrenderer as hr
%>
<%inherit file="t2-4m_details-base.mako"/>

<%block name="title">Hanning Smoothing</%block>

<p>Hanning Smoothing</p>

<table class="table table-bordered table-striped table-condensed" summary="Hanning Smoothing Spectral Window Information">
    <caption>Hanning smoothing information by spw</caption>
        <thead>
            <tr>
                <th scope="col" rowspan="2">MS Name</th>
                <th scope="col" rowspan="2">SPW ID</th>
                <th scope="col" rowspan="2">SPW Name</th>
                <th scope="col" rowspan="2">Central Frequency</th>
                <th scope="col" rowspan="2">SPW Bandwidth</th>
                <th scope="col" rowspan="2">Channel Width</th>
			    <th scope="col" rowspan="2">Smoothed</th>
                <th scope="col" rowspan="2">Reason</th>
			</tr>
        </thead>
        <body>
        % for tr in table_rows:
        <tr>
            % for td in tr:
                ${td}
            % endfor
        </tr>
        % endfor
        </body>
</table>
