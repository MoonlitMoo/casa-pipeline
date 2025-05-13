<%!
rsc_path = ""
import collections
import html

import pipeline.infrastructure.utils as utils

SELECTORS = ['vis', 'tsys_spw', 'intent']
HISTOGRAM_LABELS = collections.OrderedDict([])
HISTOGRAM_AXES = collections.OrderedDict([])
%>
<%inherit file="detail_plots_basetemplate.mako"/>

<%
    multi_vis = len({p.parameters['vis'] for p in plots}) > 1
%>

<%self:render_plots plots="${sorted(plots, key=lambda p: p.parameters['tsys_spw'])}">
	<%def name="mouseover(plot)">Click to magnify plot for Tsys spw ${plot.parameters['tsys_spw']}</%def>

	<%def name="fancybox_caption(plot)">
        % if multi_vis:
        ${plot.parameters['vis']}
        % endif
		T<sub>sys</sub> spw: ${plot.parameters['tsys_spw']}<br/>
		Science spws: ${', '.join([str(i) for i in plot.parameters['spw']])}<br/>
		Intent: ${plot.parameters['intent']}<br/>
		Fields: ${html.escape(plot.parameters['field'], True)}
	</%def>

	<%def name="caption_text(plot)">
        % if multi_vis:
		<span class="text-center">${plot.parameters['vis']}</span><br>
        % endif
		<span class="text-center">T<sub>sys</sub> spw ${plot.parameters['tsys_spw']}</span><br>
		<span class="text-center">Science spw${utils.commafy(plot.parameters['spw'], quotes=False, multi_prefix='s')}</span><br/>
		<span class="text-center">Intent: ${plot.parameters['intent']}</span><br/>
		<span class="text-center">Fields: ${html.escape(plot.parameters['field'], True)}</span>
	</%def>
</%self:render_plots>
