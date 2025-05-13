<%!
import os
import pipeline.infrastructure.renderer.rendererutils as rendererutils

agent_description = {
	'before'   : 'Before Task',
	'after'    : 'Flagged by Task'
}

total_keys = {
	'TOTAL'        : 'All Data',
	'SCIENCE SPWS' : 'Science Spectral Windows',
	'BANDPASS'     : 'Bandpass',
	'AMPLITUDE'    : 'Flux',
	'PHASE'        : 'Phase',
	'TARGET'       : 'Target (science spws)',
    'POLARIZATION' : 'Polarization',
    'POLANGLE'     : 'Polarization angle',
    'POLLEAKAGE'   : 'Polarization leakage',
	'CHECK'		   : 'Check',
	'DIFFGAINREF'  : 'Diffgain reference',
	'DIFFGAINSRC'  : 'Diffgain on-source',
}
%>


<%
# these functions are defined in template scope so we have access to the flags 
# and agents context objects

def total_for_mses(mses, row):
	flagged = 0
	total = 0
	for ms in mses:
		total += flags[ms]['before'][row].total
		for agent in flags[ms].keys():
			fs = flags[ms][agent][row]
			flagged += fs.flagged
	if total == 0:
		return 'N/A'
	else:
		return '%0.3f%%' % (100.0 * flagged / total)

def total_for_agent(agent, row, mses=flags.keys()):
	flagged = 0
	total = 0
	for ms in mses:
		if agent in flags[ms]:
			fs = flags[ms][agent][row]
			flagged += fs.flagged
			total += fs.total
		else:
			# agent was not activated for this MS. 
			total += flags[ms]['before'][row].total
	if total == 0:
		return 'N/A'
	else:
		return '%0.3f%%' % (100.0 * flagged / total)

def agent_data(agent, ms):
	if agent not in flags[ms]:
		if agent in ('online', 'template'):
			return '<td></td><td></td>'
		else:
			return '<td></td>'

	if agent in ('online', 'template'):
		if isinstance(result.inputs['vis'], str):
			flagfile = os.path.basename(result.inputs['file%s' % agent])
		elif isinstance(result.inputs['vis'], list):
			for v in result.inputs['vis']:
				if os.path.basename(v) == ms:
					ms_idx = result.inputs['vis'].index(v)
			flagfile = os.path.basename(result.inputs['file%s' % agent][ms_idx])

		relpath = os.path.join('stage%s' % result.stage_number, flagfile)
		abspath = os.path.join(pcontext.report_dir, relpath)
		if os.path.exists(abspath):
			num_lines = rendererutils.num_lines(abspath)
			return ('<td><a class="replace-pre" href="%s">%s</a></td>'
					'<td>%s</td>' % (relpath, flagfile, num_lines))
		else:
			return '<td>%s</td><td>N/A</td>' % flagfile
	else:
		return '<td><span class="glyphicon glyphicon-ok"></span></td>'
%>

<script>
    $(document).ready(function(){
        $("th.rotate").each(function(){ $(this).height($(this).find('span').width() + 8) });
    });
</script>

<%inherit file="t2-4m_details-base.mako"/>
<%block name="header" />

<%block name="title">Flag raw channels</%block>

% if htmlreports:
<h2>Flags</h2>
<table class="table table-bordered table-striped">
	<caption>Report Files</caption>
	<thead>
		<tr>
			<th>Measurement Set</th>
			<th>Flagging Commands</th>
			<th>Number of Statements</th>
	        <th>Flagging View</th>
		</tr>
	</thead>
	<tbody>
	% for msname, relpath in htmlreports.items():
		<tr>
			<td>${msname}</td>
			<td><a class="replace-pre" href="${relpath}">${os.path.basename(relpath)}</a></td>
			<td>${rendererutils.num_lines(os.path.join(pcontext.report_dir, relpath))}</td>
            <td><a class="replace" data-vis="${msname}" href="${plots_path}">Display</a></td>
        </tr>
	% endfor
	</tbody>
</table>

<h2>Flagged data summary</h2>
<table class="table table-bordered table-striped "
	   summary="Flagged Data">
	<caption>Summary of flagged data.</caption>
	<thead>
		<tr>
			<th rowspan="2">Data Selection</th>
			<!-- flags before task is always first agent -->
			<th rowspan="2">${agent_description[agents[0]]}</th>
			<th rowspan="2">${agent_description[agents[1]]}</th>
			<th rowspan="2">Total</th>
			<th colspan="${len(flags)}">Measurement Set</th>
		</tr>
        <tr>
        % for ms in flags.keys():
			<th class="rotate"><div><span>${ms}</span></div></th>
        % endfor
        </tr>
	</thead>
	<tbody>
        % for k in flag_table_intents:
		<tr>
			<th>${total_keys[k]}</th>		
	        % for agent in agents:
			    <td>${total_for_agent(agent, k)}</td>
	        % endfor
			<td>${total_for_mses(flags.keys(), k)}</td>
	        % for ms in flags.keys():
    			<td>${total_for_mses([ms], k)}</td>
	        % endfor
		</tr>
        % endfor
        % for ms in flags.keys():
		<tr>
			<th>${ms}</th>
	        % for agent in agents:
    			<td>${total_for_agent(agent, 'TOTAL', [ms])}</td>
	        % endfor
			<td>${total_for_mses([ms], 'TOTAL')}</td>
	        % for ms in flags.keys():
    			<td></td>
	        % endfor
		</tr>
        % endfor
	</tbody>
</table>

% endif

