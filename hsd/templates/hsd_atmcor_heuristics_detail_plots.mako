<%!
rsc_path = ""
SELECTORS = ['vis', 'model', 'status']
%>
<%inherit file="detail_plots_basetemplate.mako"/>

<%
    if len(pcontext.observing_run.measurement_sets)>1:
        multi_vis = True
    else:
        multi_vis = False
%>

<%self:render_plots plots="${sorted(plots, key=lambda p: p.parameters['spw'])}">
        <%def name="mouseover(plot)">Click to magnify plot for ${plot.parameters['vis']} model ${plot.parameters['model']} (${plot.parameters['status']})</%def>

	<%def name="fancybox_caption(plot)">
        % if multi_vis:
        ${plot.parameters['vis']}<br>
        % endif
        ${plot.parameters['status']}<br>
        ${plot.parameters['model']}
	</%def>

	<%def name="caption_text(plot)">
        % if multi_vis:
		${plot.parameters['vis']}<br>
        % endif
        <b>${plot.parameters['status']}</b><br>
        ${plot.parameters['model']}
    </%def>
</%self:render_plots>
