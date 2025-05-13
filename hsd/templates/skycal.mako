<%!
rsc_path = ""
import os
%>
<%inherit file="t2-4m_details-base.mako"/>

<%block name="header" />

<%block name="title">Single-Dish Sky Calibration</%block>

<%
def get_mses():
	"""Return a list of MS domain objects"""
	return [pcontext.observing_run.get_ms(name=r.inputs['vis']) for r in result]


def get_original_field_name(field_name, vis):
    ms = pcontext.observing_run.get_ms(vis)
    field_generator = (f for f in ms.fields if f.clean_name == field_name)
    field = next(field_generator)

    # strip leading and trailing double quotes that could be
    # added by the filed domain object when field name contains
    # some special characters
    original_name = field.name.strip('"')

    return original_name
%>

<p>This task generates a sky calibration table, a collection of OFF spectra for single dish data calibration.</p>

<h2>Contents</h2>
<ul>
<li><a href="#resulttable">Results</a></li>
% if len(reference_coords) > 0:
	<li><a href="#refcoordtable">Reference Coordinates</a></li>
% endif
<li><a href="#ampfreqplots">Amp vs. Frequency Plots</a></li>
<li><a href="#amptimeplots">Amp vs. Time Plots</a></li>
<li><a href="#intervaltimeplots">Interval vs. Time Plots</a></li>
<li><a href="#eldiffplots">Elevation Difference vs. Time Plots</a></li>
</ul>

<h2 id="resulttable" class="jumptarget">Results</h2>
<table class="table table-bordered" summary="Application Results">
	<caption>Applied calibrations and parameters used for caltable generation</caption>
    <thead>
        <tr>
            <th scope="col" rowspan="2">Measurement Set</th>
			<th scope="col" colspan="1">Solution Parameters</th>
			<th scope="col" colspan="4">Applied To</th>
            <th scope="col" rowspan="2">Calibration Table</th>
		</tr>
		<tr>
			<th>Type</th>
			<th>Field</th>
			<th>Scan Intent</th>
			<th>Antennas</th>
			<th>Spectral Windows</th>
        </tr>
    </thead>
	<tbody>
% for application in applications:
		<tr>
			<td>${application['ms']}</td>
		  	<td>${application['caltype']}</td>
		  	<td>${application['field']}</td>
		  	<td>${application['intent']}</td>
		  	<td>${application['antenna']}</td>
		  	<td>${application['spw']}</td>
		  	<td>${application['gaintable']}</td>
		</tr>
% endfor
	</tbody>
</table>

% if len(reference_coords) > 0:
    <h2 id="refcoordtable" class="jumptarget">Reference Coordinates</h2>
    <table class="table table-bordered" summary="Reference Coordinates">
	    <caption>Reference coordinates for position-switch calibration</caption>
        <thead>
            <tr>
                <th scope="col">Measurement Set</th>
			    <th scope="col">Field</th>
			    <th scope="col">Reference Coordinate</th>
		    </tr>
        </thead>
	    <tbody>
	    % for vis, coords in reference_coords.items():
	        <% isfirst = True %>
	        % for field, coord in coords.items():
	            <tr>
	                % if isfirst:
	                    <% isfirst = False %>
                        <td rowspan=${len(coords)}>${vis}</td>
                    % endif
	                <td>${field}</td>
	                <td>${coord}</td>
	            </tr>
	        % endfor
	    % endfor
	    </tbody>
    </table>
% endif

<h2 id="ampfreqplots" class="jumptarget">Amp vs. Frequency Plots</h2>
% for ms in get_mses():
    <%
        vis = ms.basename
        subpage = os.path.join(dirname, amp_vs_freq_subpages[vis])
    %>
    <h4><a class="replace" href="${subpage}" data-vis="${vis}">${vis}</a></h4>
    % for plot in summary_amp_vs_freq[vis]:
        % if os.path.exists(plot.thumbnail):
            <%
                img_path = os.path.relpath(plot.abspath, pcontext.report_dir)
                thumbnail_path = os.path.relpath(plot.thumbnail, pcontext.report_dir)
                spw = plot.parameters['spw']
                clean_field_name = plot.parameters['field']
                original_field_name = get_original_field_name(clean_field_name, vis)
            %>
 	        <div class="col-md-3">
	            <div class="thumbnail">
	                <a href="${img_path}" data-fancybox="thumbs">
	                    <img class="lazyload"
                             data-src="${thumbnail_path}"
	                         title="Sky level summary for Field ${original_field_name}, Spw ${spw}">
	                </a>
	                <div class="caption">
	                    <h4>
	                        <a href="${subpage}" class="replace"
	                           data-vis="${vis}" data-field="${clean_field_name}" data-spw="${spw}">
	                           Field ${original_field_name} Spw ${spw}
	                        </a>
	                    </h4>

	                    <p>Plot of sky level vs frequency for field ${original_field_name}, spw ${spw}.</p>
	                </div>
	            </div>
	        </div>
        % endif
    % endfor
	<div class="clearfix"></div><!--  flush plots, break to next row -->
% endfor

<h2 id="amptimeplots" class="jumptarget">Amp vs. Time Plots</h2>
% for ms in get_mses():
    <%
        vis = ms.basename
        subpage = os.path.join(dirname, amp_vs_time_subpages[vis])
    %>
    <h4><a class="replace" href="${subpage}" data-vis="${vis}">${vis}</a></h4>
    % for plot in summary_amp_vs_time[vis]:
        % if os.path.exists(plot.thumbnail):
            <%
                img_path = os.path.relpath(plot.abspath, pcontext.report_dir)
                thumbnail_path = os.path.relpath(plot.thumbnail, pcontext.report_dir)
                spw = plot.parameters['spw']
                clean_field_name = plot.parameters['field']
                original_field_name = get_original_field_name(clean_field_name, vis)
            %>
 	        <div class="col-md-3">
	            <div class="thumbnail">
	                <a href="${img_path}" data-fancybox="thumbs">
	                    <img class="lazyload"
                             data-src="${thumbnail_path}"
	                         title="Sky level summary for Field ${original_field_name}, Spw ${spw}">
	                </a>
	                <div class="caption">
	                    <h4>
	                        <a href="${subpage}" class="replace"
	                           data-vis="${vis}" data-field="${clean_field_name}" data-spw="${spw}">
	                           Field ${original_field_name} Spw ${spw}
	                        </a>
	                    </h4>

	                    <p>Plot of sky level vs time for field ${original_field_name}, spw ${spw}.</p>
	                </div>
	            </div>
	        </div>
        % endif
    % endfor
	<div class="clearfix"></div><!--  flush plots, break to next row -->
% endfor

<h2 id="intervaltimeplots" class="jumptarget">Interval vs. Time Plots</h2>
% for ms in get_mses():
    <%
        vis = ms.basename
        subpage = os.path.join(dirname, interval_vs_time_subpages[vis])
    %>
    <h4><a class="replace" href="${subpage}" data-vis="${vis}">${vis}</a></h4>
    % for plot in summary_interval_vs_time[vis]:
        % if os.path.exists(plot.thumbnail):
            <%
                img_path = os.path.relpath(plot.abspath, pcontext.report_dir)
                thumbnail_path = os.path.relpath(plot.thumbnail, pcontext.report_dir)
                spw = plot.parameters['spw']
                ant = plot.parameters['ant']
                clean_field_name = plot.parameters['field']
                original_field_name = get_original_field_name(clean_field_name, vis)
            %>
 	        <div class="col-md-3">
	            <div class="thumbnail">
	                <a href="${img_path}" data-fancybox="thumbs">
	                    <img class="lazyload"
                             data-src="${thumbnail_path}"
	                         title="Interval Ratio (off-source/on-source) summary for Field ${original_field_name}, Ant ${ant}, SPW ${spw}">
	                </a>
	                <div class="caption">
	                    <h4>
	                        <a href="${subpage}" class="replace"
	                           data-vis="${vis}" data-field="${clean_field_name}" data-ant="${ant}" data-spw="${spw}">
	                           Field ${original_field_name} Antenna ${ant} SPW ${spw}
	                        </a>
	                    </h4>

	                    <p>Plot of interval ratio (off-source/on-source) vs time for field ${original_field_name}, antenna ${ant}, spw ${spw}.</p>
	                </div>
	            </div>
	        </div>
        % endif
    % endfor
	<div class="clearfix"></div><!--  flush plots, break to next row -->
% endfor

<h2 id="eldiffplots" class="jumptarget">Elevation Difference vs. Time Plots</h2>
% for ms in get_mses():
    <%
        vis = ms.basename
        subpage = os.path.join(dirname, elev_diff_subpages[vis])
    %>
    <h4><a class="replace" href="${subpage}" data-vis="${vis}">${vis}</h4>
    % for plot in summary_elev_diff[vis]:
        % if os.path.exists(plot.thumbnail):
            <%
                img_path = os.path.relpath(plot.abspath, pcontext.report_dir)
                thumbnail_path = os.path.relpath(plot.thumbnail, pcontext.report_dir)
                clean_field_name = plot.parameters['field']
                original_field_name = get_original_field_name(clean_field_name, vis)
            %>
            <div class="col-md-3">
                <div class="thumbnail">
                    <a href="${img_path}" data-fancybox="thumbs">
                        <img class="lazyload"
                             data-src="${thumbnail_path}"
                             title="Elevation difference for Field ${original_field_name}">
                    </a>
                    <div class="caption">
                        <h4>
                            <a href="${subpage}" class="replace"
                               data-vis="${vis}" data-field="${clean_field_name}">
                               Field ${original_field_name}
                            </a>
                        </h4>

                        <p>Plot of elevation difference vs time for field ${original_field_name}.</p>
                    </div>
                </div>
            </div>
        % endif
    % endfor
    <div class="clearfix"></div><!--  flush plots, break to next row -->
% endfor
