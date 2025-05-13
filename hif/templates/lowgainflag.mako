<%!
import os.path
import pipeline.infrastructure.renderer.rendererutils as rendererutils
%>

<%inherit file="t2-4m_details-base.mako"/>

<%block name="title">Flag antennas with low gain</%block>
 
## % if plots:
## <h2>Plots</h2>
## <ul>
##     % for vis, relpath in plots.items():
##         <li>
##                <a class="replace" href="${relpath}">${vis}</a>
##                shows the images used for flagging.
##     % endfor
## </ul>
## % endif

% if updated_refants:
<h2 id="refants" class="jumptarget">Reference Antenna update</h2>

<p>For the measurement set(s) listed below, the reference antenna
    list was updated due to significant flagging (antennas moved to
    end and/or removed). See QA messages for details.
    Shown below are the updated reference antenna lists,
    only for those measurement sets where it was modified.</p>

<table class="table table-bordered table-striped"
	   summary="Reference Antennas">
	<caption>Updated reference antenna selection per measurement set. Antennas are
	listed in order of highest to lowest priority.</caption>
	<thead>
		<tr>
			<th>Measurement Set</th>
			<th>Reference Antennas (Highest to Lowest)</th>
		</tr>
	</thead>
	<tbody>
%for vis in updated_refants:
		<tr>
			<td>${os.path.basename(vis)}</td>
			## insert spaces in refant list to allow browser to break string
			## if it wants
			<td>${updated_refants[vis].replace(',', ', ')}</td>
		</tr>
%endfor
	</tbody>
</table>
% endif

% if htmlreports:
<h2>Flags</h2>
<table class="table table-bordered table-striped">
	<caption>Report Files</caption>
	<thead>
		<tr>
			<th>Measurement Set</th>
			<th>Flagging Commands</th>
			<th>Number of Statements</th>
            <th>Flagging Views</th>
		</tr>
	</thead>
	<tbody>
		% for msname, relpath in htmlreports.items():
		<tr>
			<td>${msname}</td>
			<td><a class="replace-pre" href="${relpath}">${os.path.basename(relpath)}</a></td>
			<td>${rendererutils.num_lines(os.path.join(pcontext.report_dir, relpath))}</td>
            % if plots_path:
                <td><a class="replace" data-vis="${msname}" href="${plots_path}">Display</a></td>
            % else:
                <td>N/A</td>
            % endif
		</tr>
		% endfor
	</tbody>
</table>
% endif
