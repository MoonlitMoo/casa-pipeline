<%!
rsc_path = ""
import os
from pipeline.infrastructure.renderer import rendererutils
%>
<%inherit file="t2-4m_details-base.mako"/>

<%block name="header" />

<%block name="title">Generate Baseline tables and subtract spectral baseline</%block>

<script>
$(document).ready(function() {
    // return a function that sets the SPW text field to the given spw
    var createSpwSetter = function(spw) {
        return function() {
        	if (typeof spw !== "undefined") {
	            // trigger a change event, otherwise the filters are not changed
	            $("#select-spw").select2("val", [spw]).trigger("change");
        	}
        };
    };

    // create a callback function for each overview plot that will select the
    // appropriate spw once the page has loaded
    $(".thumbnail a").each(function (i, v) {
        var o = $(v);
        var spw = o.data("spw");
        o.data("callback", createSpwSetter(spw));
    });
});
</script>

<%
def get_spw_exp(spw):
    spw_exp = 'Spectral Window {}'.format(spw)
    if dovirtual:
        spw_exp = 'Virtual ' + spw_exp 
    return spw_exp 
    
def get_spw_desc(spw):
    spw_exp = get_spw_exp(spw).replace('Window', 'Window:')
    if dovirtual:
        spw_name = pcontext.observing_run.virtual_science_spw_ids[spw]
        spw_short_name = pcontext.observing_run.virtual_science_spw_shortnames[spw_name]
        spw_exp += '<br>({})'.format(spw_short_name)
    return spw_exp
        
def get_spw_inline_desc(spw):
    spw_exp = get_spw_exp(spw).lower()
    if dovirtual:
        spw_name = pcontext.observing_run.virtual_science_spw_ids[spw]
        spw_short_name = pcontext.observing_run.virtual_science_spw_shortnames[spw_name]
        spw_exp += ' ({})'.format(spw_short_name)
    return spw_exp
    
def spmap_plot_title(spw, apply_bl):
    spw_exp = get_spw_exp(spw)
    
    if apply_bl:
        apply_exp = 'after'
    else:
        apply_exp = 'before'
    
    return "Sparse Profile Map for {} {} Baseline Subtraction".format(spw_exp, apply_exp)

def clustering_plot_title(title_exp, spw, field=None):
    spw_exp = get_spw_exp(spw)
    
    title = "{} for {}".format(title_exp, spw_exp)

    if field is not None:
        title += " Field {}".format(field)
        
    return title
%>

<!-- short description of what the task does -->
<p>This task generates baseline fitting tables and subtracts baseline from spectra. 
Spectral lines are detected by clustering analysis for each spectral window, 
and line free channels are used for baseline fitting. </p>

<p>Spectral data before and after baseline subtraction are shown below. 
The topmost panel in each plot is an integrated spectrum while a set of bottom panels shows a profile map. 
Each panel in the profile map shows an individual spectrum that is located around panel's position. 
Cyan regions shows detected line regions. The line regions in the top panel indicates a property of detected 
cluster while the ones in the profile map are the result of line detection for the corresponding spectra.
Horizontal red bars in the top panel shows the additional masks that corresponds to channels having large 
deviation, which effectively are any spectral features including the ones not detected or validated in the 
line detection stage.</p>

<h2>Contents</h2>
<ul>
<li><a href="#rawbeforebaseline">Raw Spectral Data Before Baseline Subtraction</a></li>
<li><a href="#avgbeforebaseline">Averaged Spectral Data Before Baseline Subtraction</a></li>
<li><a href="#rawafterbaseline">Raw Spectral Data After Baseline Subtraction</a></li>
<li><a href="#flatnessafterbaseline">Baseline Flatness After baseline Subtraction</a></li>
% if detail is not None:
<li><a href="#clusteranalysis">Line Detection by Clustering Analysis</a></li>
% endif
</ul>

<h2 id="rawbeforebaseline" class="jumptarget">Spectral Data Before Baseline Subtraction</h2>

<p>Red lines indicate the result of baseline fit that is subtracted from the calibrated spectra.</p>

% for field in sparsemap_subpage_before_raw:
    <h3><a class="replace"
           href="${os.path.join(dirname, sparsemap_subpage_before_raw[field])}"
           data-field="${field}">${field}</a>
    </h3>
    % for plot in sparsemap_before_raw[field]:
        % if os.path.exists(plot.thumbnail):
	        <div class="col-md-3">
	            <div class="thumbnail">
	                <a href="${os.path.relpath(plot.abspath, pcontext.report_dir)}"
	                   data-fancybox="thumbs">
	                    <img class="lazyload"
                             data-src="${os.path.relpath(plot.thumbnail, pcontext.report_dir)}"
	                         title="${spmap_plot_title(plot.parameters['spw'], apply_bl=False)}">
	                </a>
	
	                <div class="caption">
	                    <h4>
	                        <a href="${os.path.join(dirname, sparsemap_subpage_before_raw[field])}"
	                           class="replace"
	                           data-spw="${plot.parameters['spw']}"
	                           data-field="${field}">
	                           ${get_spw_exp(plot.parameters['spw'])}
	                        </a>
	                    </h4>
	                    <p>Antenna: ${plot.parameters['ant']}<br>
	                        Field: ${plot.parameters['field']}<br>
	                        ${get_spw_desc(plot.parameters['spw'])}<br>
	                        Polarisation: ${plot.parameters['pol']}
	                    </p>
	                </div>
	            </div>
	        </div>
        % endif
    % endfor
	<div class="clearfix"></div><!--  flush plots, break to next row -->
%endfor

<h2 id="avgbeforebaseline" class="jumptarget">Averaged Spectral Data Before Baseline Subtraction</h2>

<p>Plotted data are obtained by averaging all the spectral associating with each grid. Averaging the data improves 
S/N ratio so that spectral line feature becomes more prominent and it can be easily compared with the line mask 
for baseline subtraction.</p>

% for field in sparsemap_subpage_before_avg:
    <h3><a class="replace"
           href="${os.path.join(dirname, sparsemap_subpage_before_avg[field])}"
           data-field="${field}">${field}</a>
    </h3>
    % for plot in sparsemap_before_avg[field]:
        % if os.path.exists(plot.thumbnail):
	        <div class="col-md-3">
	            <div class="thumbnail">
	                <a href="${os.path.relpath(plot.abspath, pcontext.report_dir)}"
	                   data-fancybox="thumbs">
	                    <img class="lazyload"
                             data-src="${os.path.relpath(plot.thumbnail, pcontext.report_dir)}"
	                         title="${spmap_plot_title(plot.parameters['spw'], apply_bl=False)}">
	                </a>
	
	                <div class="caption">
	                    <h4>
	                        <a href="${os.path.join(dirname, sparsemap_subpage_before_avg[field])}"
	                           class="replace"
	                           data-spw="${plot.parameters['spw']}"
	                           data-field="${field}">
	                           ${get_spw_exp(plot.parameters['spw'])}
	                        </a>
	                    </h4>
	                    <p>Antenna: ${plot.parameters['ant']}<br>
	                        Field: ${plot.parameters['field']}<br>
	                        ${get_spw_desc(plot.parameters['spw'])}<br>
	                        Polarisation: ${plot.parameters['pol']}
	                    </p>
	                </div>
	            </div>
	        </div>
        % endif
    % endfor
	<div class="clearfix"></div><!--  flush plots, break to next row -->
%endfor


<h2 id="rawafterbaseline" class="jumptarget">Spectral Data After Baseline Subtraction</h2>

<p>Red lines show zero-level. Spectra that are properly subtracted should be located around red lines.</p>

% for field in sparsemap_subpage_after_raw:
    <h3><a class="replace"
           href="${os.path.join(dirname, sparsemap_subpage_after_raw[field])}"
           data-field="${field}">${field}</a>
    </h3>
    % for plot in sparsemap_after_raw[field]:
        % if os.path.exists(plot.thumbnail):
	        <div class="col-md-3">
	            <div class="thumbnail">
	                <a href="${os.path.relpath(plot.abspath, pcontext.report_dir)}"
	                   data-fancybox="thumbs">
	                    <img class="lazyload"
                             data-src="${os.path.relpath(plot.thumbnail, pcontext.report_dir)}"
	                         title="${spmap_plot_title(plot.parameters['spw'], apply_bl=True)}">
	                </a>
	
	                <div class="caption">
	                    <h4>
	                        <a href="${os.path.join(dirname, sparsemap_subpage_after_raw[field])}"
	                           class="replace"
	                           data-spw="${plot.parameters['spw']}"
	                           data-field="${field}">
	                           ${get_spw_exp(plot.parameters['spw'])}
	                        </a>
	                    </h4>
	                    <p>Antenna: ${plot.parameters['ant']}<br>
	                        Field: ${plot.parameters['field']}<br>
	                        ${get_spw_desc(plot.parameters['spw'])}<br>
	                        Polarisation: ${plot.parameters['pol']}
	                    </p>
	                </div>
	            </div>
	        </div>
        % endif
    % endfor
	<div class="clearfix"></div><!--  flush plots, break to next row -->
%endfor

<h2 id="flatnessafterbaseline" class="jumptarget">Baseline Flatness After baseline Subtraction</h2>

<p>Red dots show binned spectrum of baseline channels. Dashed lines show 1 sigma of raw spectrum and zero level. </p>

% for field in sparsemap_subpage_after_flatness:
    <h3><a class="replace"
           href="${os.path.join(dirname, sparsemap_subpage_after_flatness[field])}"
           data-field="${field}">${field}</a>
    </h3>
    % for plot in sparsemap_after_flatness[field]:
        % if os.path.exists(plot.thumbnail):
	        <div class="col-md-3">
	            <div class="thumbnail">
	                <a href="${os.path.relpath(plot.abspath, pcontext.report_dir)}"
	                   data-fancybox="thumbs">
	                    <img class="lazyload"
                             data-src="${os.path.relpath(plot.thumbnail, pcontext.report_dir)}"
	                         title="${spmap_plot_title(plot.parameters['spw'], apply_bl=True)}">
	                </a>
	
	                <div class="caption">
	                    <h4>
	                        <a href="${os.path.join(dirname, sparsemap_subpage_after_flatness[field])}"
	                           class="replace"
	                           data-spw="${plot.parameters['spw']}"
	                           data-field="${field}">
	                           ${get_spw_exp(plot.parameters['spw'])}
	                        </a>
	                    </h4>
	                    <p>Antenna: ${plot.parameters['ant']}<br>
	                        Field: ${plot.parameters['field']}<br>
	                        ${get_spw_desc(plot.parameters['spw'])}<br>
	                        Polarisation: ${plot.parameters['pol']}
	                    </p>
	                </div>
	            </div>
	        </div>
        % endif
    % endfor
	<div class="clearfix"></div><!--  flush plots, break to next row -->
%endfor

% if detail is not None:

<h2 id="clusteranalysis" class="jumptarget">Line Detection by Clustering Analysis</h2>

% if len(detail) == 0:
  <p>No Lines are detected.</p>
% else:
% for field in detail.keys():

  <h3>${field}</h3>

  % if len(detail[field]) > 0 or len(cover_only[field]) > 0:
  
    <!-- Link to details page -->
    % for plots in detail[field]:
      <h4><a class="replace"
      href="${os.path.join(dirname, plots['html'])}" data-field="${field}">${plots['title']}</h4>
    
<!--		href="${rendererutils.get_relative_url(pcontext.report_dir, dirname, plots['html'])}">${plots['title']}-->
      % for plot in plots['cover_plots']:
        % if os.path.exists(plot.thumbnail):
			<div class="col-md-3">
			  	<div class="thumbnail">
                    <a href="${os.path.relpath(plot.abspath, pcontext.report_dir)}"
                       data-fancybox="thumbs">
                       <img class="lazyload"
                            data-src="${os.path.relpath(plot.thumbnail, pcontext.report_dir)}"
                            title="${clustering_plot_title(plots['title'], plot.parameters['spw'], plot.field)}">
                    </a>
                    <div class="caption">
                        <h4>
                            <a href="${os.path.join(dirname, plots['html'])}"
                               class="replace"
                               data-spw="${plot.parameters['spw']}"
                               data-field=${plot.field}>
                               ${get_spw_exp(plot.parameters['spw'])}
                            </a>
                        </h4>
                        <p>Clustering plot of ${get_spw_inline_desc(plot.parameters['spw'])}.
                        </p>
                    </div>
                </div>
           	</div>
          % endif
      % endfor
	  <div class="clearfix"></div><!--  flush plots, break to next row -->
    % endfor

    <!-- No details -->
    % for plots in cover_only[field]:
      <h4>${plots['title']}</h4>
      % for plot in plots['cover_plots']:
		% if os.path.exists(plot.thumbnail):
			<div class="col-md-3">
			  	<div class="thumbnail">
                    <a href="${os.path.relpath(plot.abspath, pcontext.report_dir)}"
                       data-fancybox="thumbs">
                       <img class="lazyload"
                            data-src="${os.path.relpath(plot.thumbnail, pcontext.report_dir)}"
                            title="${clustering_plot_title(plots['title'], plot.parameters['spw'])}">
                    </a>
					<div class="caption">
						<h4>${get_spw_exp(plot.parameters['spw'])}</h4>
						<p>Clustering plot of ${get_spw_inline_desc(plot.parameters['spw'])}.</p>
					</div>
				</div>
			</div>
        % endif
      % endfor
	  <div class="clearfix"></div><!--  flush plots, break to next row -->
    % endfor

  % else:
  <p>No Lines are detected for ${field}.</p>
  % endif
% endfor
% endif

% endif
