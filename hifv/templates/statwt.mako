<%!
rsc_path = ""
import os
import pipeline.infrastructure.renderer.htmlrenderer as hr
import pipeline.hifv.tasks.statwt.renderer as render
%>
<%inherit file="t2-4m_details-base.mako"/>

<%block name="title">Reweight visibilities</%block>

<p>Calculate data weights based on st. dev. within each spw.</p>

<%
import numpy as np

def format_wt_overall(wt):
    if wt is None:
        return 'N/A'
    if wt >= 10**5:
        return np.format_float_scientific(wt, precision=6, trim='-')
    else:
        return np.format_float_positional(wt, precision=6, fractional=False, trim='-')

weight_stats=plotter.result.weight_stats

if result[0].inputs['statwtmode'] == 'VLA':
    description = "after"
    table_header = "Weight Properties"
else: 
    before_by_spw=weight_stats['before']['per_spw']
    before_by_ant=weight_stats['before']['per_ant']

    after_by_spw=weight_stats['after']['per_spw']
    after_by_ant=weight_stats['after']['per_ant']

    description = "before/after"
    table_header = "statwt after"
%>

<!-- VLA PI: show the overall mean and variance at the top -->
% if result[0].inputs['statwtmode'] == 'VLA':
    <% 
    mean =  result[0].jobs[0]['mean']
    variance = result[0].jobs[0]['variance'] 
    %>

    <h3>Overall results:</h3>
    <b>Mean:</b> ${format_wt_overall(mean)} 
    <br>
    <b>Variance:</b> ${format_wt_overall(variance)}

    <h2 id="flagged_data_summary" class="jumptarget">Statwt Summary</h2>

    <!-- VLA PI has table cell color highlighting, described below. -->
    <%
    bgcolor_list=[render.dev2shade(3., True), render.dev2shade(4., True), render.dev2shade(5., True), render.dev2shade(6., True)]
    bgcolor_list_blue=[render.dev2shade(3., False), render.dev2shade(4., False), render.dev2shade(5., False), render.dev2shade(6., False)]
    %>

    <p>
    In the tables and plots below, the weight properties are shown in the "antenna-based" scale: we first derive the optimized solutions 
    to Wt_bl_ij=Wt_i*Wt_j for individual integrations, where Wt_bl_ij is the "baseline" weight (i.e., the MS WEIGHT column) and Wt_i/Wt_j is 
    the "antenna-based" weight; then we compute and present the statistical properties of the latter values after slicing them along different 
    data axes (antenna, spw, scan). Note that there is a square scale difference between the baseline weights and the antenna-based weights.
    </p>

    <p>The color background present in some table cells highlights spectral windows with a statistical property signficantly deviated from its median over all of the relevant group (spw, scan, antenna): 
    <p> For values above the median, shades of red are used: </p>
    <p style="background-color:${bgcolor_list[0]}; display:inline;">3&#963&le;dev&lt;4&#963</p>; <p style="background-color:${bgcolor_list[1]}; display:inline;">4&#963&le;dev&lt;5&#963</p>; 
    <p style="background-color:${bgcolor_list[2]}; display:inline;">5&#963&le;dev&lt;6&#963</p>; <p style="background-color:${bgcolor_list[3]}; display:inline;">6&#963&le;dev,</p>
    <p>where &#963 is defined as 1.4826*MAD.</p>
    <p>For values below the median, shades of blue are used: </p> 
    <p style="background-color:${bgcolor_list_blue[0]}; display:inline;">3&#963&le;dev&lt;4&#963</p>; <p style="background-color:${bgcolor_list_blue[1]}; display:inline;">4&#963&le;dev&lt;5&#963</p>; 
    <p style="background-color:${bgcolor_list_blue[2]}; display:inline;">5&#963&le;dev&lt;6&#963</p>; <p style="background-color:${bgcolor_list_blue[3]}; display:inline;">
        6&#963&le;dev,</p> 
    <p>where &#963 is defined as 1.4826*MAD.</p>
% endif 

<!-- The table and plot display below is different for VLA PI and VLASS -->
<!-- VLA PI has per-band plots and tables for only after statwt was run, and includes per-scan plots and tables and also highlights table cells--> 
<!-- vs. VLASS has before/after plots and tables without being separated by band, doesn't include per-scan plots or tables, and doesn't include highlighting of table cells-->
% if result[0].inputs['statwtmode'] == 'VLA':
    <% 
    bandsort = {'4':0, 'P':1, 'L':2, 'S':3, 'C':4, 'X':5, 'U':6, 'K':7, 'A':8, 'Q':9}
    %>

    % for band in bandsort.keys():
        <!-- Create per-band headers and navigation-->
        % if band in band2spw.keys():
        <a id="${band}"></a><br>
        <div class="row">
        <hr>
        <h4>
        % for bb in bandsort.keys():
            % if bb in band2spw.keys():
                <a href="#${bb}">${bb}-band</a>&nbsp;|&nbsp;
            % endif
        % endfor
        <a href="#flagged_data_summary">Top of page </a> | (Click to Jump)<br><br>
                ${band}-band
        </h4> 
        
        <!--Plots for band -->
        <%self:plot_group plot_dict="${summary_plots[band]}"
                                        url_fn="${lambda ms:  'noop'}">

                <%def name="title()">
                    Statwt Summary Plot
                </%def>

                <%def name="preamble()">
                </%def>

                <%def name="mouseover(plot)">Summary window for band ${band}</%def>

                <%def name="fancybox_caption(plot)">
                    Plot of ${plot.y_axis} vs. ${plot.x_axis} (${plot.parameters['type']} re-weight, ${band}-band)
                </%def>

                <%def name="caption_title(plot)">
                    Plot of ${plot.y_axis} vs. ${plot.x_axis} (${plot.parameters['type']} re-weight, ${band}-band)
                </%def>
        </%self:plot_group>

        <!--Antenna, spw, scans tables for band--> 
        <table style="float: left; margin:0 10px; width: auto; text-align:center" class="table table-bordered table-striped ">
            <caption>Summary of ${description}-statwt antenna-based weights (<i>W</i><sub>i</sub>) for each antenna, Band ${band}. The antenna-based weights are derived from the visibility WEIGHT column: <i>W</i><sub>ij</sub>&asymp;<i>W</i><sub>i</sub><i>W</i><sub>j</sub>. 
            </caption>
            <thead>
                <tr>
                    <th scope="col" rowspan="2">Antenna Selection</th>
                    <!-- flags before task is always first agent -->
                    <th scope="col" colspan="7" style="text-align:center">${table_header}</th>
                </tr>
                <tr>
                    <th scope="col">Median</th>
                    <th scope="col">1st Quartile</th>
                    <th scope="col">3rd Quartile</th>
                    <th scope="col">Mean</th>
                    <th scope="col">S.Dev.</th>
                    <th scope="col">Minimum</th>
                    <th scope="col">Maximum</th>
                </tr>        
            </thead>
            <tbody>
                % for row in ant_table_rows[band]: 
                <tr>
                    % for i, td in enumerate(row):
                        % if i == 0:
                            <th style="text-align:center">${td}</th>  
                        % else: 
                            ${td}
                        % endif
                    %endfor
                </tr>
                % endfor
            </tbody>
        </table>

        <table style="float: left; margin:0 10px; width: auto; text-align:center" class="table table-bordered table-striped ">
            <caption>Summary of ${description}-statwt antenna-based weights (<i>W</i><sub>i</sub>) for each spectral window. The antenna-based weights are derived from the visibility WEIGHT column: <i>W</i><sub>ij</sub>&asymp;<i>W</i><sub>i</sub><i>W</i><sub>j</sub>.
            </caption>
            <thead>
                <tr>
                    <th scope="col" rowspan="2">Spw Selection</th>
                    <!-- flags before task is always first agent -->
                    <th scope="col" colspan="7" style="text-align:center">${table_header}</th>
                </tr>
                <tr>
                    <th scope="col">Median</th>
                    <th scope="col">1st Quartile</th>
                    <th scope="col">3rd Quartile</th>
                    <th scope="col">Mean</th>
                    <th scope="col">S.Dev.</th>
                    <th scope="col">Minimum</th>
                    <th scope="col">Maximum</th>
                </tr>        
            </thead>
            <tbody>
                 % for row in spw_table_rows[band]: 
                <tr>
                    % for i, td in enumerate(row):
                        % if i == 0:
                            <th style="text-align:center">${td}</th>  
                        % else: 
                            ${td}
                        % endif
                    %endfor
                </tr>
                % endfor
            </tbody>
        </table>

        <table style="float: left; margin:0 10px; width: auto; text-align:center" class="table table-bordered table-striped ">
            <caption>Summary of ${description}-statwt antenna-based weights (<i>W</i><sub>i</sub>) for each scan. The antenna-based weights are derived from the visibility WEIGHT column: <i>W</i><sub>ij</sub>&asymp;<i>W</i><sub>i</sub><i>W</i><sub>j</sub>.
            </caption>
            <thead>
                <tr>
                    <th scope="col" rowspan="2">Scan Selection</th>
                    <th scope="col" colspan="8" style="text-align:center">${table_header}</th>
                </tr>
                <tr>
                    <th scope="col">Median</th>
                    <th scope="col">1st Quartile</th>
                    <th scope="col">3rd Quartile</th>
                    <th scope="col">Mean</th>
                    <th scope="col">S.Dev.</th>
                    <th scope="col">Minimum</th>
                    <th scope="col">Maximum</th>
                </tr>        
            </thead>
            <tbody>
                % for row in scan_table_rows[band]: 
                <tr>
                    % for i, td in enumerate(row):
                        % if i == 0:
                            <th style="text-align:center">${td}</th>  
                        % else: 
                            ${td}
                        % endif
                    %endfor
                </tr>
                % endfor
            </tbody>
        </table>
        </div>
    %endif
    %endfor
%else: 
    <!-- Include plots and tables for VLASS --> 
    <!-- Plots for VLASS before/after, not separated by band, doesn't include per-scan plots or tables or shaded cells-->
    <%self:plot_group plot_dict="${summary_plots}"
                                    url_fn="${lambda ms:  'noop'}">

            <%def name="title()">
                Statwt Summary Plot
            </%def>

            <%def name="preamble()">
            </%def>

            <%def name="mouseover(plot)">Summary window</%def>

            <%def name="fancybox_caption(plot)">
                Plot of ${plot.y_axis} vs. ${plot.x_axis} (${plot.parameters['type']} re-weight)
            </%def>

            <%def name="caption_title(plot)">
                Plot of ${plot.y_axis} vs. ${plot.x_axis} (${plot.parameters['type']} re-weight)
            </%def>
    </%self:plot_group>

    <!-- Tables for VLASS before/after, not separated by band-->
    <h2 id="flagged_data_summary" class="jumptarget">Statwt Summary</h2>
    <table style="float: left; margin:0 10px; width: auto; text-align:center" class="table table-bordered table-striped ">
	<caption>Summary of before/after-statwt antenna-based weights (<i>W</i><sub>i</sub>) for each antenna. The antenna-based weights are derived from the visibility WEIGHT column: <i>W</i><sub>ij</sub>&asymp;<i>W</i><sub>i</sub><i>W</i><sub>j</sub>. 
    </caption>
	<thead>
		<tr>
			<th scope="col" rowspan="2">Antenna Selection</th>
			<!-- flags before task is always first agent -->
			<th scope="col" colspan="3" style="text-align:center">statwt before</th>
			<th scope="col" colspan="3" style="text-align:center">statwt after</th>
		</tr>
		<tr>
            <th scope="col">Median</th>
            <th scope="col">1st/3rd Quartile</th>
            <th scope="col">Mean &#177 S.Dev.</th>
            <th scope="col">Median</th>
            <th scope="col">1st/3rd Quartile</th>
            <th scope="col">Mean &#177 S.Dev.</th>
		</tr>        
	</thead>
	<tbody>
		% for i in range(len(after_by_ant)):
		<tr>  
			<th style="text-align:center">${after_by_ant[i]['ant']}</th>  
            <td>${render.format_wt(before_by_ant[i]['med'])}</td>
            % if before_by_ant[i]['quartiles'] is not None:
                <td>${render.format_wt(before_by_ant[i]['q1'])}/${render.format_wt(before_by_ant[i]['q3'])}</td>
            % else:
                <td>N/A</td>
            % endif 
            <td>${render.format_wt(before_by_ant[i]['mean'])} &#177 ${render.format_wt(before_by_ant[i]['stdev'])}</td>    
            <td>${render.format_wt(after_by_ant[i]['med'])}</td>
            % if after_by_ant[i]['quartiles'] is not None:
                <td>${render.format_wt(after_by_ant[i]['q1'])}/${render.format_wt(after_by_ant[i]['q3'])}</td>
            % else:
                <td>N/A</td>
            % endif
            <td>${render.format_wt(after_by_ant[i]['mean'])} &#177 ${render.format_wt(after_by_ant[i]['stdev'])}</td>      
		</tr>
		% endfor
	</tbody>
</table>

<table style="float: left; margin:0 10px; width: auto; text-align:center" class="table table-bordered table-striped ">
	<caption>Summary of before/after-statwt antenna-based weights (<i>W</i><sub>i</sub>) for each spectral window. The antenna-based weights are derived from the visibility WEIGHT column: <i>W</i><sub>ij</sub>&asymp;<i>W</i><sub>i</sub><i>W</i><sub>j</sub>.
    </caption>
	<thead>
		<tr>
			<th scope="col" rowspan="2">Spw Selection</th>
			<!-- flags before task is always first agent -->
			<th scope="col" colspan="3" style="text-align:center">statwt before</th>
			<th scope="col" colspan="3" style="text-align:center">statwt after</th>
		</tr>
		<tr>
            <th scope="col">Median</th>
            <th scope="col">1st/3rd Quartile</th>
            <th scope="col">Mean &#177 S.Dev.</th>
            <th scope="col">Median</th>
            <th scope="col">1st/3rd Quartile</th>
            <th scope="col">Mean &#177 S.Dev.</th>
		</tr>        
	</thead>
	<tbody>
		% for i in range(len(after_by_spw)):
		<tr>
			<th style="text-align:center">${after_by_spw[i]['spw']}</th>  
            <td>${render.format_wt(before_by_spw[i]['med'])}</td>
            % if before_by_spw[i]['quartiles'] is not None:
                <td>${render.format_wt(before_by_spw[i]['q1'])}/${render.format_wt(before_by_spw[i]['q3'])}</td>
            % else:
                <td>N/A</td>
            % endif            
            <td>${render.format_wt(before_by_spw[i]['mean'])} &#177 ${render.format_wt(before_by_spw[i]['stdev'])}</td>  
            <td>${render.format_wt(after_by_spw[i]['med'])}</td>
            % if after_by_spw[i]['quartiles'] is not None:
                <td>${render.format_wt(after_by_spw[i]['q1'])}/${render.format_wt(after_by_spw[i]['q3'])}</td>
            % else:
                <td>N/A</td>
            % endif
            <td>${render.format_wt(after_by_spw[i]['mean'])} &#177 ${render.format_wt(after_by_spw[i]['stdev'])}</td>
		</tr>
		% endfor
	</tbody>
</table>

%endif 