<%!
rsc_path = ""
import os
%>

<script>
    pipeline.pages.tclean_plots.ready();
</script>

<div class="page-header">
    <h2>Major cycle table for ${field} SpW ${spw}
        <div class="btn-toolbar pull-right" role="toolbar">
            % if qa_previous or qa_next:
            <div class="btn-group" role="group">
                % if qa_previous:
                    <button type="button" class="btn btn-default replace" data-href="${os.path.relpath(qa_previous, pcontext.report_dir)}"><span class="glyphicon glyphicon-step-backward"></span></button>
                % else:
                    <button type="button" class="btn btn-default disabled"><span class="glyphicon glyphicon-step-backward"></button>
                % endif
                % if qa_next:
                    <button type="button" class="btn btn-default replace" data-href="${os.path.relpath(qa_next, pcontext.report_dir)}"><span class="glyphicon glyphicon-step-forward"></span></button>
                % else:
                    <button type="button" class="btn btn-default disabled"><span class="glyphicon glyphicon-step-forward"></span></button>
                % endif
            </div>
            % endif
            <div class="btn-group" role="group">
                <button class="btn btn-default replace" data-href="${os.path.relpath(base_url, pcontext.report_dir)}">Back</button>
            </div>
        </div>
    </h2>
</div>

<div class="row">
% if table_dict is not None:

	% for index, table in table_dict.items():

		<table class="table table-striped">

		<thead>
			<tr>
				% for column in table['cols']:
				<th>${column}</th>
				% endfor
			</tr>
		</thead>

		<tbody>
		% for ir in range(table['nrow']):
		<tr>
			% for column in table['cols']:
				<th>${table[column][ir] if ir <= len(table[column]) else ''}</th>
			% endfor
		</tr>
        % endfor
		</tbody>
		</table>
	% endfor

% else:
	<br>No table to render.</br>
% endif
</div>

