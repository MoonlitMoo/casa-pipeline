<%!
import pipeline.infrastructure.renderer.htmlrenderer as hr
import pipeline.domain.measures as measures
from pipeline.infrastructure import utils
%>
<html>
<body>

<div class="page-header">
	<h1>Spectral Setup Details<button class="btn btn-default pull-right" onclick="javascript:window.history.back();">Back</button></h1>
</div>

<div id="tabbable">
	<ul class="nav nav-tabs">
		<li class="active"><a href="#tabs-science" data-toggle="tab">Science Windows</a></li>
		<li><a href="#tabs-all" data-toggle="tab">All Windows</a></li>
	</ul>
	<div class="tab-content">
		<div class="tab-pane active" id="tabs-science">		
			<h2>Science Windows</h2>
			<table class="table table-bordered table-striped table-condensed" summary="Science Spectral Windows in ${ms.basename}">
				<caption>Spectral Windows with Science Intent in ${ms.basename}</caption>
			    <thead>
			        <tr>
			            <th scope="col" rowspan="2">Real ID</th>
			            <th scope="col" rowspan="2">Virtual ID</th>
			            <th scope="col" rowspan="2">Name</th>
						<th scope="col" rowspan="2">Type</th> <!-- note: Type is used for different things for VLA vs ALMA -->
			            <th scope="col" colspan="3">Frequency ${'(%s)' % (ms.get_spectral_windows()[0].frame)}</th>
			            <th scope="col" rowspan="2">Bandwidth ${'(%s)' % (ms.get_spectral_windows()[0].frame)}</th>
			            <th scope="col" rowspan="2">Transitions</th>
						<%
						# Check to see whether to display the "Online Spec. Avg." column or not to determine the colspan
						channels_colspan = '3'
						if show_online_spec_avg_col.science_windows:
							channels_colspan = '4'
						%>
			            <th scope="col" colspan=${channels_colspan}>Channels ${'(%s)' % (ms.get_spectral_windows()[0].frame)}</th>
			            <th scope="col" rowspan="2">Correlator Axis</th>
						<!-- Omit Correlation Bits column for the ACA Spectrometer. See PIPE-1993/4 -->
						% if ms.correlator_name != 'ALMA_ACASPEC':
							<th scope="col" rowspan="2">Correlation Bits</th>
						% endif
			            <th scope="col" rowspan="2">Band</th>
			            <th scope="col" rowspan="2">Band Type</th>
			            <th scope="col" rowspan="2">Median Feed Receptor Angle (degrees)</th>
						<%
						basebandlabel=''
						if pcontext.project_summary.telescope != 'ALMA':
                                        basebandlabel = '<th scope="col" rowspan="2">Baseband</th>'
						endif
						%>
						${basebandlabel}
			        </tr>
			        <tr>
			        	<th>Start</th>
			        	<th>Centre</th>
			        	<th>End</th>
			        	<th>Number</th>
						% if show_online_spec_avg_col.science_windows:
							<th>Online Spec. Avg.</th>
					    % endif
			        	<th>Frequency Width</th>
			        	<th>Velocity Width</th>
			        </tr>
			    </thead>
				<tbody>
					% for spw in ms.get_spectral_windows(science_windows_only=True):
					<tr>
					  <td>${spw.id}</td>
					  <td>${pcontext.observing_run.real2virtual_spw_id(int(spw.id), ms)}</td>
					  <td>${pcontext.observing_run.virtual_science_spw_shortnames.get(pcontext.observing_run.virtual_science_spw_ids.get(pcontext.observing_run.real2virtual_spw_id(int(spw.id), ms), 'N/A'), 'N/A')}</td>
			            <%
						spwtypeentry='<td>'+str(spw.type)+'</td>'
						if 'VLA' in pcontext.project_summary.telescope:
							if spw.specline_window:
								window_type = "Line"
							else: 
								window_type = "Continuum"
							spwtypeentry='<td>'+window_type+'</td>'
						%>
						${spwtypeentry}
					  <td>${str(spw.min_frequency)}</td>
					  <td>${str(spw.centre_frequency)}</td>
					  <td>${str(spw.max_frequency)}</td>
					  <td>${str(spw.bandwidth)}</td>
					  <td>${', '.join(spw.transitions)}</td>
					  <td>${spw.num_channels}</td>
                      % if show_online_spec_avg_col.science_windows:
						% if pcontext.project_summary.telescope == 'ALMA':
							% if (ms.get_alma_cycle_number() == 2 and spw.sdm_num_bin == 1) or spw.sdm_num_bin is None:
								<td> ? </td> <!-- A value of sdm_num_bin of 1 for CYCLE 2 datasets may indicate that a default value of 1 was used so use '?' to indicate that the value is unclear. See PIPE: 584-->
							% else:
							<td>${spw.sdm_num_bin}</td>
							% endif
						% elif 'VLA' in ms.antenna_array.name:
							% if spw.sdm_num_bin is None: 
							<td> ? </td>
							% else:
							<td>${spw.sdm_num_bin}</td>
							% endif
						% endif
					  % endif
					  <td>${spw.channels[0].getWidth()}</td>
					  <td>${str(measures.LinearVelocity(299792458 * spw.channels[0].getWidth().to_units(measures.FrequencyUnits.HERTZ) / spw.centre_frequency.to_units(measures.FrequencyUnits.HERTZ), measures.LinearVelocityUnits.METRES_PER_SECOND))}</td>
					  <td>${', '.join(sorted(ms.get_data_description(spw=spw).corr_axis))}</td>
					  <!-- Omit Correlation Bits column for the ACA Spectrometer and set to BITS_4x4 for the ACA correlator. See PIPE-1993/4 -->
						% if ms.correlator_name != 'ALMA_ACASPEC':
							% if spw.correlation_bits and spw.correlation_bits != "UNKNOWN":
								<td>${spw.correlation_bits}</td>
							% elif ms.correlator_name == 'ALMA_ACA':
								<td>BITS_4x4</td>
							% else:
								<td>Unknown</td>
							% endif
						% endif
					  <td>${spw.band}</td>
                      <%
                      if spw.receiver:
                          bandtype = '<td>{}</td>'.format(spw.receiver)
                      else:
                          bandtype = '<td>Unknown</td>'
                      %>
                      ${bandtype}
                      <%
                      if spw.median_receptor_angle is not None:
                          recep_angle = '<td>' + ', '.join(map('{:.1f}'.format, spw.median_receptor_angle)) + '</td>'
                      else:
                          recep_angle = '<td>N/A</td>'
                      %>
                      ${recep_angle}
                      <%
						basebanditem=''
						if pcontext.project_summary.telescope != 'ALMA':
						    try:
						        basebandstring = spw.name.split('#')[1]
						        basebanditem = '<td>'+basebandstring+'</td>'
						    except:
						        basebanditem = '<td></td>'
						endif
					  %>
					  ${basebanditem}
					</tr>
					% endfor
				</tbody>
			</table>
		</div>
	
		<div class="tab-pane" id="tabs-all">
			<h2>All Windows</h2>
			<table class="table table-bordered table-striped table-condensed" summary="Spectral Windows in ${ms.basename}">
				<caption>All Spectral Windows in ${ms.basename}</caption>
			    <thead>
			        <tr>
			            <th scope="col" rowspan="2">ID</th>
			            <%
						spwtypelabel='<th scope="col" rowspan="2">Type</th>'
						if 'VLA' in pcontext.project_summary.telescope:
						            spwtypelabel=''
						endif
						%>
						${spwtypelabel}
			            <th scope="col" colspan="3">Frequency ${'(%s)' % (ms.get_spectral_windows()[0].frame)}</th>
			            <th scope="col" rowspan="2">Bandwidth ${'(%s)' % (ms.get_spectral_windows()[0].frame)}</th>
			            <th scope="col" rowspan="2">Transitions</th>
						<%
						# Check to see whether to display the "Online Spec. Avg." column or not to determine the colspan
						channels_colspan = '3'
						if show_online_spec_avg_col.all_windows:
							channels_colspan = '4'
						%>
			            <th scope="col" colspan=${channels_colspan}>Channels ${'(%s)' % (ms.get_spectral_windows()[0].frame)}</th>
						<th scope="col" rowspan="2">Correlator Axis</th>
						<!-- Omit Correlation Bits column for the ACA Spectrometer. See PIPE-1993/4 -->
						% if ms.correlator_name != 'ALMA_ACASPEC':
							<th scope="col" rowspan="2">Correlation Bits</th>
						% endif
			            <th scope="col" rowspan="2">Band</th>
			            <th scope="col" rowspan="2">Band Type</th>
			            <th scope="col" rowspan="2">Median Feed Receptor Angle (degrees)</th>
						<%
						     basebandlabel=''
						     if pcontext.project_summary.telescope != 'ALMA':
                                 basebandlabel = '<th scope="col" rowspan="2">Baseband</th>'
						     endif
						%>
						${basebandlabel}
			            <th scope="col" rowspan="2">Intents</th>
			        </tr>
			        <tr>
			        	<th>Start</th>
			        	<th>Centre</th>
			        	<th>End</th>
			        	<th>Number</th>
						% if show_online_spec_avg_col.all_windows:
							<th>Online Spec. Avg.</th>
					    % endif
			        	<th>Frequency Width</th>
			        	<th>Velocity Width</th>
			        </tr>
			    </thead>
				<tbody>
					% for spw in ms.get_spectral_windows(science_windows_only=False):
					<tr>
						<td>${spw.id}</td>
			            <%
						spwtypeentry='<td>'+str(spw.type)+'</td>'
						if 'VLA' in pcontext.project_summary.telescope:
					                spwtypeentry=''
						endif
						%>
						${spwtypeentry}
						<td>${str(spw.min_frequency)}</td>
						<td>${str(spw.centre_frequency)}</td>
						<td>${str(spw.max_frequency)}</td>
						<td>${str(spw.bandwidth)}</td>
					    <td>${','.join(spw.transitions)}</td>
						<td>${spw.num_channels}</td>
						% if show_online_spec_avg_col.all_windows: 
							% if pcontext.project_summary.telescope == 'ALMA':
								% if (ms.get_alma_cycle_number() == 2 and spw.sdm_num_bin == 1) or spw.sdm_num_bin is None: 
								<td> ? </td> <!-- A value of sdm_num_bin of 1 for CYCLE 2 datasets may indicate that a default value of 1 was used so use '?' to indicate that the value is unclear, See PIPE: 584-->
								% else:
								<td>${spw.sdm_num_bin}</td>
								% endif
							% elif 'VLA' in ms.antenna_array.name:
								% if spw.sdm_num_bin is None: 
								<td> ? </td>
								% else:
								<td>${spw.sdm_num_bin}</td>
								% endif
							% endif
						%endif

						<td>${spw.channels[0].getWidth()}</td>
						<td>${str(measures.LinearVelocity(299792458 * spw.channels[0].getWidth().to_units(measures.FrequencyUnits.HERTZ) / spw.centre_frequency.to_units(measures.FrequencyUnits.HERTZ), measures.LinearVelocityUnits.METRES_PER_SECOND))}</td>
						<%
							dd = ms.get_data_description(spw=spw)
							if dd is None:
								polarizations = ""
							else:
								polarizations = ', '.join(sorted(dd.corr_axis))
						%>
						<td>${polarizations}</td>
						<!-- Omit Correlation Bits column for the ACA Spectrometer. See PIPE-1993/4 -->
						% if ms.correlator_name != 'ALMA_ACASPEC':
							% if spw.correlation_bits and spw.correlation_bits != "UNKNOWN":
								<td>${spw.correlation_bits}</td>
							% elif ms.correlator_name == 'ALMA_ACA':
								<td>BITS_4x4</td>
							% else:
								<td>Unknown</td>
							% endif
						% endif
						<td>${spw.band}</td>
                        <%
                        if spw.receiver:
                            bandtype = '<td>{}</td>'.format(spw.receiver)
                        else:
                            bandtype = '<td>Unknown</td>'
                        %>
                        ${bandtype}
                        <%
                        if spw.median_receptor_angle is not None:
                            recep_angle = '<td>' + ', '.join(map('{:.1f}'.format, spw.median_receptor_angle)) + '</td>'
                        else:
                            recep_angle = '<td>N/A</td>'
                        %>
                        ${recep_angle}
						<%
						    basebanditem=''
						    if pcontext.project_summary.telescope != 'ALMA':
						        try:
						            basebandstring = spw.name.split('#')[1]
						            basebanditem = '<td>'+basebandstring+'</td>'
						        except:
						            basebanditem = '<td></td>'
						    endif
					     %>
					     ${basebanditem}
				  		<td>${', '.join(sorted([i for i in spw.intents]))}</td>
					</tr>
					% endfor
				</tbody>
			</table>
		</div>
	</div>
</div>

</body>
</html>
