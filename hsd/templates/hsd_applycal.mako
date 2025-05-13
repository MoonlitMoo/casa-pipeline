<%!
rsc_path = ""
import html
import os

import pipeline.infrastructure.filenamer as filenamer
import pipeline.infrastructure.logging as logging
import pipeline.infrastructure.renderer.htmlrenderer as hr
import pipeline.infrastructure.renderer.rendererutils as rendererutils
import pipeline.infrastructure.utils as utils

%>
<%inherit file="applycal.mako"/>

## override plot_group()
<%def name="plot_group(plot_dict, url_fn, data_spw=False, data_field=False, data_baseband=False, data_tsysspw=False,
                       data_vis=False, data_ant=False, title_id=None, rel_fn=None, break_rows_by='', sort_row_by='')">

<%
	## determine that plot_dict is for hsd_applycal
	is_hsd_plot_dict = False
	if isinstance(plot_dict, dict):
		is_hsd_plot_dict = '__hsd_applycal__' in plot_dict.keys()
		if is_hsd_plot_dict:
			del plot_dict['__hsd_applycal__']
%>

% if is_hsd_plot_dict:
	% if title_id:
		<h3 id="${title_id}" class="jumptarget">${caller.title()}</h3>
	% else:
		<h3>${caller.title()}</h3>
	% endif

	% if hasattr(caller, 'preamble'):
		${caller.preamble()}
	% endif

	% for ms, lst in plot_dict.items():
		<%
			relurl = url_fn(ms)
			if relurl:
				subpage_path = rendererutils.get_relative_url(pcontext.report_dir, dirname, relurl,
															allow_nonexistent=False)
				subpage_exists = subpage_path is not None
			else:
				subpage_exists = false
		%>

		<h4>
			% if subpage_exists:
			<a class="replace"
			% if data_vis:
			data-vis="${ms}"
			% endif
			href="${subpage_path}">
			% endif
				${ms}
			% if subpage_exists:
			</a>
			% endif
		</h4>

		% if hasattr(caller, 'ms_preamble'):
			${caller.ms_preamble(ms)}
		% endif

		% for field, ms_plots in lst:

			<h4>
				% if subpage_exists:
				<a class="replace"
				% if data_vis:
				data-vis="${ms}"
				% endif
				% if data_field:
				data-field="${field | h}"
				% endif
				href="${subpage_path}">
				% endif
					${field | h}
				% if subpage_exists:
				</a>
				% endif
			</h4>

			% for plots_in_row, _ in rendererutils.group_plots(ms_plots, break_rows_by):
			<div class="row">
				% if plots_in_row is not None:
				% for plot in rendererutils.sort_row_by(plots_in_row, sort_row_by):
				<%
					intent = plot.parameters.get('intent', 'No intent')
					if isinstance(intent, list):
						intent = utils.commafy(intent, quotes=False)
					intent = intent.upper()
				%>
				<div class="col-md-3 col-sm-4">
					% if os.path.exists(plot.thumbnail):
					<%
						fullsize_relpath = os.path.relpath(plot.abspath, pcontext.report_dir)
						thumbnail_relpath = os.path.relpath(plot.thumbnail, pcontext.report_dir)
					%>

					<div class="thumbnail">
						<a href="${fullsize_relpath}"
						% if rel_fn:
							data-fancybox="${rel_fn(plot)}"
						% elif relurl:
							data-fancybox="${relurl}"
						% else:
							data-fancybox="${caller.title()}"
						% endif
						% if hasattr(caller, 'fancybox_caption'):
							data-caption="${caller.fancybox_caption(plot).strip()}"
						% endif
						% if plot.command:
							data-plotCommandTarget="#plotcmd-${hash(plot.abspath)}"
						% endif
						>
							<img class="lazyload"
								data-src="${thumbnail_relpath}"
							% if hasattr(caller, 'mouseover'):
								title="${caller.mouseover(plot)}"
							% endif
							>
						</a>

						% if plot.command:
						<div id="plotcmd-${hash(plot.abspath)}" class="modal-content pipeline-plotcommand" style="display:none;">
							<div class="modal-header">
								<button type="button" class="close" data-fancybox-close aria-label="Close">
									<span aria-hidden="true">&times;</span>
								</button>
								<h4 class="modal-title">Plot Command</h4>
							</div>
							<div class="modal-body" data-selectable="true">
								<p>${rendererutils.get_command_markup(pcontext, plot.command)}</p>
							</div>
							<div class="modal-footer">
								<button type="button" class="btn btn-default" data-fancybox-close>Close</button>
							</div>
						</div>
						% endif

						<div class="caption">
							<h4>
							% if subpage_exists:
								<a href="${subpage_path}"
								% if data_field:
								data-field="${html.escape(plot.parameters['field'], True)}"
								% endif
								% if data_spw:
								data-spw="${plot.parameters['spw']}"
								% endif
								% if data_tsysspw:
								data-tsys_spw="${plot.parameters['tsys_spw']}"
								% endif
								% if data_baseband:
								data-baseband="${plot.parameters['baseband']}"
								% endif
								% if data_vis:
								data-vis="${plot.parameters['vis']}"
								% endif
								% if data_ant:
								data-ant="${rendererutils.sanitize_data_selection_string(plot.parameters.get('ant', ""))}"
								% endif
								class="replace">
							% endif
							${caller.caption_title(plot)}
							% if subpage_exists:
								</a>
							% endif
							</h4>
							% if hasattr(caller, 'caption_subtitle'):
								<h6>${caller.caption_subtitle(plot)}</h6>
							% endif

							% if hasattr(caller, 'caption_text'):
							<p>${caller.caption_text(plot, intent)}</p>
							% endif
						</div>
					</div>
					% endif
				</div>
				% endfor
				% endif
			</div><!-- end row -->
			% endfor
		% endfor

	% endfor

% else:
	<%
		parent.plot_group(plot_dict, url_fn, data_spw, data_field, data_baseband, data_tsysspw,
                          data_vis, data_ant, title_id, rel_fn, break_rows_by, sort_row_by)
	%>
%endif
</%def>
