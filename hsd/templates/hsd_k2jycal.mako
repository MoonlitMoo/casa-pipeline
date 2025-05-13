<%!
rsc_path = "../"
import os

import pipeline.infrastructure.utils as utils
%>
<%inherit file="t2-4m_details-base.mako"/>

<%block name="header" />

<%block name="title">Generate Kelvin to Jansky conversion caltables</%block>

<%
stage_dir = os.path.join(pcontext.report_dir, 'stage%s'%(result.stage_number))
observing_run = pcontext.observing_run
def id2name(spwid):
    return observing_run.virtual_science_spw_shortnames[observing_run.virtual_science_spw_ids[spwid]]
%>

<p>This task generates calibtation tables to convert the unit of single dish spectra from Kelvin to Jansky.</p>


<h3>Summary of Jy/K Conversion Factor</h3>
<p>
Numbers in histograms show that of MS, antenna, spectral window, and polarization
combination whose conversion factor is in each bin.
</p>
% for plot in jyperk_hist:
	% if plot is not None:
    <div class="col-md-3 col-sm-4">
    	% if os.path.exists(plot.thumbnail):
    		<%
            	fullsize_relpath = os.path.relpath(plot.abspath, pcontext.report_dir)
                thumbnail_relpath = os.path.relpath(plot.thumbnail, pcontext.report_dir)
            %>

            <div class="thumbnail">
            	<a href="${fullsize_relpath}"
                   data-fancybox
                   title='<div>Receiver: ${plot.parameters['receiver']}<br>Spw: ${plot.parameters['spw']}<br></div>'>
                	<img class="lazyload"
                         data-src="${thumbnail_relpath}"
                         title="Click to show histrogram of Jy/K factors of spw ${plot.parameters['spw']}">
                </a>

                <div class="caption">
                    <!-- title -->
                    %if dovirtual:
                    <h4>Virtual Spectral Window ${plot.parameters['spw']}</h4>
                    % else:
                    <h4>Spectral Window ${plot.parameters['spw']}</h4>
                    % endif
                    <!-- sub-title -->
	                <h6>${plot.parameters['receiver']}</h6>
                    <!-- description -->
                    % if dovirtual:
                    <p>Variation of Jy/K factors in virtual spw ${plot.parameters['spw']}<br>name: ${id2name(plot.parameters['spw'])}</p>
                    % else:
                    <p>Variation of Jy/K factors in spw ${plot.parameters['spw']}</p>
                    % endif
                </div>
            </div>
        % endif
    </div>
    % endif
% endfor
<div class="clearfix"></div><!--  flush plots, break to next row -->

<h3>Jy/K Conversion Factors</h3>
The following table lists the Jy/K factors.
% if reffile_list is not None:
    Parameters can be found in:
    % for idx, reffile in enumerate(reffile_list):
        % if reffile is not None and len(reffile) > 0 and os.path.exists(os.path.join(stage_dir, os.path.basename(reffile))):
            <a class="replace-pre" href="${os.path.relpath(reffile, pcontext.report_dir)}">${os.path.basename(reffile)}</a>
            % if idx < len(reffile_list)-1:
                ,
            % else:
                .
            % endif
        % endif
    % endfor
% else:
    No Jy/K factors file is specified. 
% endif
<table class="table table-bordered table-striped" summary="Jy/K factors">
    <thead>
    % if dovirtual:
	<tr><th>Virtual Spw</th><th>MS</th><th>Real Spw</th><th>Antenna</th><th>Pol</th><th>Factor</th></tr>
	% else:
	<tr><th>Spw</th><th>MS</th><th>Antenna</th><th>Pol</th><th>Factor</th></tr>
	% endif
    </thead>
	<tbody>
	% for tr in jyperk_rows:
		<tr>
		% for td in tr:
			${td}
		% endfor
		</tr>
	%endfor
	</tbody>
</table>
<p/>
