<%inherit file="importdata.mako"/>

<%block name="title">ALMA Import Data</%block>

<%block name="addendum">
<h3>Parallactic Angle Ranges</h3>
% if parang_ranges['pol_intents_found']:
<p>The following table and plots show the ranges of parallactic angles of the polarization calibrator(s) per session.</p>
<table class="table table-bordered table-striped table-condensed"
       summary="Parallactic angle information">
    <thead>
        <tr>
            <th>Session</th>
            <th>Parallactic angle range</th>
            <th>Parallactic angle plot</th>
        </tr>
    </thead>
    <tbody>
        % for session_name in parang_ranges['sessions']:
            <tr>
                <td>${session_name}</td>
                <td>
                    ${'%.1f' % (parang_ranges['sessions'][session_name]['min_parang_range'])}&deg;
                    % if parang_ranges['sessions'][session_name]['min_parang_range'] >= minparang:
                        &ge;
                    % else:
                        &lt;
                    % endif
                    min. parallactic angle (${'%.1f' % (minparang)}&deg;)
                </td>
                <td>${parang_plots[session_name]['html']}</td>
            </tr>
        % endfor
    </tbody>
</table>
% else:
<p>No polarization intents found.</p>
% endif
</%block>
