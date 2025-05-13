<%!
rsc_path = ""
import os

%>
<%inherit file="t2-4m_details-base.mako"/>

<%block name="title">Fit and Subtract UV Continuum Model</%block>

<p>This task computes the UV continuum model and subtracts it from the
(science target) data. The result is stored in the DATA column of a new set of MSes called "&lt;UID&gt;_targets_line.ms"</p>

<h2>Results</h2>

<table class="table table-bordered" summary="Application Results">
        <caption>Applied calibrations and parameters used for caltable generation</caption>
    <thead>
        <tr>
            <th scope="col" rowspan="2">Measurement Set</th>
                        <th scope="col" colspan="2">Solution Parameters</th>
                        <th scope="col" colspan="2">Applied To</th>
        </tr>
        <tr>
                        <th>Frequency Ranges (TOPO)</th>
                        <th>Fit Order</th>
                        <th>Source Intent</th>
                        <th>Spectral Window</th>
        </tr>
    </thead>
        <tbody>
                % for tr in table_rows:
                <tr>
                    % for td in tr:
                        ${td}
                    % endfor
                </tr>
                % endfor
        </tbody>
</table>
