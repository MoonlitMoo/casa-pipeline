<%!
import collections
rsc_path = ""
SELECTORS = ['vis', 'type', 'spw', 'ant', 'field', 'pol']
HISTOGRAM_LABELS = collections.OrderedDict([
    ( 'myflag',  'Flagged fraction' )
])

# set all X-axis labels to 'Flagged %' (see pipeline.js for defs)
HISTOGRAM_AXES = collections.OrderedDict([
    ( key, 'PLOTS.xAxisLabels["Flagged fraction"]' ) for key in HISTOGRAM_LABELS
])
%>
<%inherit file="detail_plots_basetemplate.mako"/>

<%
    if len(pcontext.observing_run.measurement_sets)>1:
        multi_vis = True    
    else:
        multi_vis = False
%>

<%self:render_plots plots="${sorted(plots, key=lambda p: p.parameters['spw'])}">
        <%def name="mouseover(plot)">Click to magnify plot for ${plot.parameters['vis']} field ${plot.parameters['field']} ${plot.parameters['ant']} Spw ${plot.parameters['spw']} pol ${plot.parameters['pol']} ${plot.parameters['type']}</%def>

    <%def name="fancybox_caption(plot)">
        % if multi_vis:
        ${plot.parameters['vis']}<br>
        % endif
        Field: ${plot.parameters['field']}<br>
        Antenna: ${plot.parameters['ant']}<br>
        Spectral Window: ${plot.parameters['spw']}<br>
        Polarization: ${plot.parameters['pol']}<br>
        ${plot.parameters['type']}
    </%def>

    <%def name="caption_text(plot)">
        % if multi_vis:
        ${plot.parameters['vis']}<br>
        % endif
        ${plot.parameters['field']}<br>
        ${plot.parameters['ant']}<br>
        Spw ${plot.parameters['spw']}<br>
        Pol ${plot.parameters['pol']}<br>
        ${plot.parameters['type']}<br>
    </%def>
</%self:render_plots>
