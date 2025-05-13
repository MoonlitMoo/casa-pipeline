<%!
import os
%>

<%inherit file="t2-4m_details-base.mako"/>

<%block name="title">Select Reference Antenna for Session(s)</%block>

<p>
    This task re-evaluates the reference antenna lists from all measurement
    sets within a session and combines these to select a single common
    reference antenna (per session) that is to be used by any subsequent
    pipeline stages.
</p>

<h2>Reference antenna</h2>
<table class="table table-bordered table-striped">
    <caption>Final choice of reference antenna for each session.</caption>
    <thead>
        <tr>
            <th>Session</th>
            <th>Measurement Sets</th>
            <th>Reference Antenna</th>
        </tr>
    </thead>
    <tbody>
    % for session_name, session_info in sessions.items():
        <tr>
            <td rowspan="${len(session_info['vislist'])}">${session_name}</td>
            <td>${os.path.basename(session_info['vislist'][0])}</td>
            <td rowspan="${len(session_info['vislist'])}">${session_info['refant']}</td>
        </tr>
        % if len(session_info['vislist']) > 1:
            % for vis in session_info['vislist'][1:]:
            <tr>
                <td>${os.path.basename(vis)}</td>
            </tr>
            % endfor
        % endif
    % endfor
    </tbody>
</table>
