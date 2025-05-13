<%!
rsc_path = "../"
import os
import collections
%>
<%inherit file="t2-4m_details-base.mako"/>

<%block name="header" />

<%block name="title">Image single dish data</%block>

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

    // return a function that sets the Antenna text field to the given spw
    var createAntennaSetter = function(ant) {
        return function() {
        	if (typeof ant !== "undefined") {
	            // trigger a change event, otherwise the filters are not changed
	            $("#select-ant").select2("val", [ant]).trigger("change");
        	}
        };
    };

    // return a function that sets the Field text field to the given spw
    var createFieldSetter = function(field) {
        return function() {
        	if (typeof field !== "undefined") {
	            // trigger a change event, otherwise the filters are not changed
	            $("#select-field").select2("val", [field]).trigger("change");
        	}
        };
    };

    // return a function that sets the Polarization text field to the given spw
    var createPolarizationSetter = function(pol) {
        return function() {
        	if (typeof pol !== "undefined") {
	            // trigger a change event, otherwise the filters are not changed
	            $("#select-pol").select2("val", [pol]).trigger("change");
        	}
        };
    };

    //
    var createMixedSetter = function(spw, ant, field, pol) {
        return function() {
            // trigger a change event, otherwise the filters are not changed
        	if (typeof spw !== "undefined") {
	            $("#select-spw").select2("val", [spw]).trigger("change");
        	}
        	if (typeof ant !== "undefined") {
            	$("#select-ant").select2("val", [ant]).trigger("change");
        	}
        	if (typeof field !== "undefined") {
            	$("#select-field").select2("val", [field]).trigger("change");
        	}
        	if (typeof pol !== "undefined") {
	            $("#select-pol").select2("val", [pol]).trigger("change");
        	}
        };
    };

    // create a callback function for each overview plot that will select the
    // appropriate spw once the page has loaded
    $(".thumbnail a").each(function (i, v) {
        var o = $(v);
        var spw = o.data("spw");
        var ant = o.data("ant");
        var field = o.data("field");
        var pol = o.data("pol");
        o.data("callback", createMixedSetter(spw, ant, field, pol));
    });
});
</script>

<%
def get_spw_short_exp(spw):
    spw_exp = 'SPW {}'.format(spw)
    if dovirtual:
        spw_exp = 'V' + spw_exp
    return spw_exp

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

stage_dir = os.path.join(pcontext.report_dir, 'stage%s'%(result.stage_number))
plots_list = [{'title': 'Channel Map',
               'subpage': channelmap_subpage,
               'plot': channelmap_plots},
              {'title': 'Baseline Rms Map',
               'subpage': rmsmap_subpage,
               'plot': rmsmap_plots},
              {'title': 'Moment Map',
               'subpage': momentmap_subpage,
               'plot': momentmap_plots},
              {'title': 'Integrated Intensity Map',
               'subpage': integratedmap_subpage,
               'plot': integratedmap_plots}]
%>

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
%>

<p>This task generates single dish images per source per spectral window.
It generates an image combined spectral data from whole antenna as well as images per antenna.</p>

<h3>Contents</h3>
<ul>
%if rms_table is not None and len(rms_table) > 0:
    <li><a href="#sensitivity">Image Sensitivity Table</a></li>
%endif
%if sparsemap_subpage is not None and sparsemap_subpage != {}:
    <li><a href="#profilemap">Profile Map</a></li>
%endif
% for plots in plots_list:
    % if plots['subpage'] is not None and plots['subpage'] != {}:
        <li><a href="#${plots['title'].replace(" ", "")}">${plots['title']}</a></li>
    %endif
% endfor
%if contaminationmap_plots is not None:
    <li><a href="#contaminationplot">Contamination Plots</a></li>
%endif
</ul>

%if rms_table is not None and len(rms_table) > 0:
	<h3 id="sensitivity" class="jumptarget">Image Sensitivity</h3>
	<p>
	RMS of line-free channels. Estimated RMS is listed for representative images.
	</p>
	<table class="table table-bordered table-striped" summary="Image Sentivitity">
		<caption>RMS of line-free channels</caption>
    	<thead>
	    	<tr>
	        	<th>Name</th><th>Frequency Ranges</th><th>Channel width</th><th>Theoretical RMS</th><th>Observed RMS</th>
	    	</tr>

  		</thead>
		<tbody>
		% for tr in rms_table:
			<tr>
			% for td in tr:
				${td}
			% endfor
			</tr>
		%endfor
		</tbody>
	</table>
%endif


%if sparsemap_subpage is not None and sparsemap_subpage != {}:
<h3 id="profilemap" class="jumptarget">Profile Map</h3>
  % for field, subpage in sparsemap_subpage.items():
    <h4><a class="replace"
           href="${os.path.join(dirname, subpage)}"
	   data-field="${field}">
           ${field}
        </a>
    </h4>
    % for plot in sparsemap_plots[field]:
        % if os.path.exists(plot.thumbnail):
	        <div class="col-md-3">
	            <div class="thumbnail">
	                <a href="${os.path.relpath(plot.abspath, pcontext.report_dir)}"
	                   title='<div class="pull-left">Profile Map<br>${get_spw_short_exp(plot.parameters['spw'])}<br>Source ${field}</div>'
	                   data-fancybox="thumbs">
	                    <img class="lazyload"
                             data-src="${os.path.relpath(plot.thumbnail, pcontext.report_dir)}"
	                         title="Profile map summary for ${get_spw_exp(plot.parameters['spw'])}">
	                </a>

	                <div class="caption">
	                    <h4>
	                        <a href="${os.path.join(dirname, subpage)}"
	                           class="replace"
	                           data-spw="${plot.parameters['spw']}"
	                           data-field="${field}">
	                           ${get_spw_exp(plot.parameters['spw'])}
	                        </a>
	                    </h4>

	                    <p>Profile map for ${get_spw_inline_desc(plot.parameters['spw'])}.</p>

	                    % if profilemap_subpage is not None:
	                      <h4>Detailed profile map</h4>
	                      <table border width="100%">
		                      <tr><th>ANTENNA</th><th colspan="${len(list(profilemap_entries[field].values())[0])}">POL</th></tr>
		                      % for ant, pols in profilemap_entries[field].items():
		                        <tr><td>${ant}</td>
		                        <td align="center">
		                        % for pol in pols:
		                            <a href="${os.path.join(dirname, profilemap_subpage[field])}"
		                               class="btn replace"
		                               data-spw="${plot.parameters['spw']}"
		                               data-ant="${ant}"
		                               data-field="${field}"
		                               data-pol="${pol}">
		                            ${pol}
		                            </a>
		                        % endfor
		                        </td>
		                        </tr>
		                      % endfor
	                      </table>
	                    % endif
	                </div>
	            </div>
	        </div>
        % endif
    % endfor
	<div class="clearfix"></div><!--  flush plots, break to next row -->
  %endfor
%endif

% for plots in plots_list:
    % if plots['subpage'] == {} or plots['subpage'] is None:
        <% continue %>
    % endif
    <h3 id="${plots['title'].replace(" ", "")}" class="jumptarget">${plots['title']}</h3>
    % if plots['title'] == 'Channel Map':
    Click in the Field Name or Spectral Window ID to get different spectral selections.
    % endif
    % for field, subpage in plots['subpage'].items():
        <h4><a class="replace"
               href="${os.path.join(dirname, subpage)}"
               data-field="${field}"
               data-ant="COMBINED">
               ${field}
            </a>
        </h4>
        % for plot in plots['plot'][field]:
            % if os.path.exists(plot.thumbnail):
	            <div class="col-md-3">
	                <div class="thumbnail">
	                    <a href="${os.path.relpath(plot.abspath, pcontext.report_dir)}"
	                       title='<div class="pull-left">${plots['title']}<br>${get_spw_short_exp(plot.parameters['spw'])}<br>Source ${field}</div>'
	                       data-fancybox="thumbs">
	                        <img class="lazyload"
                                 data-src="${os.path.relpath(plot.thumbnail, pcontext.report_dir)}"
	                             title="${plots['title']} for ${get_spw_exp(plot.parameters['spw'])}">
	                    </a>

	                    <div class="caption">
	                        <h4>
	                            <a href="${os.path.join(dirname, subpage)}"
	                               class="replace"
	                               data-spw="${plot.parameters['spw']}"
	                               data-field="${field}"
                                   data-ant="COMBINED">
	                               ${get_spw_exp(plot.parameters['spw'])}
	                            </a>
	                        </h4>

	                        <p>${plots['title']} for ${get_spw_inline_desc(plot.parameters['spw'])}.</p>
	                    </div>
	                </div>
	            </div>
            % endif
        % endfor
 	    <div class="clearfix"></div><!--  flush plots, break to next row -->
    % endfor
	<div class="clearfix"></div><!--  flush plots, break to next row -->
% endfor

%if contaminationmap_plots is not None:
<h3 id="contaminationplot" class="jumptarget">Contamination Plots</h3>
    % for field, plot_list in contaminationmap_plots.items():
      <h4>${field}</h4>
	  % for plot in plot_list:
		% if os.path.exists(plot.thumbnail):
			<div class="col-md-3">
			  	<div class="thumbnail">
                    <a href="${os.path.relpath(plot.abspath, pcontext.report_dir)}"
                       data-fancybox="thumbs">
                       <img class="lazyload"
                            data-src="${os.path.relpath(plot.thumbnail, pcontext.report_dir)}"
                            title="Contamination Plot for Field ${field} ${get_spw_inline_desc(plot.parameters['spw'])}">
                    </a>
					<div class="caption">
						<h4>${get_spw_exp(plot.parameters['spw'])}</h4>
						<p>Contamination Plot for Field ${field} ${get_spw_inline_desc(plot.parameters['spw'])}.</p>
					</div>
				</div>
			</div>
        % endif
	  %endfor
	  <div class="clearfix"></div><!--  flush plots, break to next row -->
    %endfor
%endif
