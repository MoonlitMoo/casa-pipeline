<%!
rsc_path = ""
SELECTORS = ['vis', 'ant', 'spw']
%>
<%inherit file="detail_plots_basetemplate.mako"/>

<%
    multi_vis = len({p.parameters['vis'] for p in plots}) > 1
%>

<%self:render_plots plots="${sorted(plots, key=lambda p: (p.parameters['spw'], p.parameters['ant']))}">
	<%def name="mouseover(plot)">
	    Click to magnify plot for gain ${plot.parameters['yaxis']}, SpW ${plot.parameters['spw']},
	    antenna ${plot.parameters['ant']}
	</%def>

	<%def name="fancybox_caption(plot)">
        % if multi_vis:
        ${plot.parameters['vis']}
        % endif
        Gain ${plot.parameters['yaxis']}<br>
		SpW ${plot.parameters['spw']}<br>
		Antenna ${plot.parameters['ant']}
	</%def>

	<%def name="caption_text(plot)">
        % if multi_vis:
		<span class="text-center">${plot.parameters['vis']}</span><br>
        % endif
        <span class="text-center">Gain: ${plot.parameters['yaxis']}</span><br>
		<span class="text-center">SpW ${plot.parameters['spw']}</span><br>
		<span class="text-center">Antenna ${plot.parameters['ant']}</span>
	</%def>
</%self:render_plots>
