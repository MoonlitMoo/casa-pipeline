<%!
rsc_path = ""
import os
import pipeline.infrastructure.renderer.htmlrenderer as hr
from pipeline.infrastructure.renderer import rendererutils
%>
<%inherit file="t2-4m_details-base.mako"/>

<%block name="title">Syspower</%block>

<script src="${self.attr.rsc_path}resources/js/pipeline.js"></script>

<script>
$(document).ready(function() {
    // return a function that sets the SPW text field to the given spw
    var createSpwSetter = function(spw) {
        return function() {
            // trigger a change event, otherwise the filters are not changed
            $("#select-spw").select2("val", [spw]).trigger("change");
        };
    };

    // create a callback function for each overview plot that will select the
    // appropriate spw once the page has loaded
    $(".thumbnail a").each(function (i, v) {
        var o = $(v);
        var spw = o.data("spw");
        o.data("callback", createSpwSetter(spw));
    });

    $(".fancybox").fancybox({
        type: 'image',
        prevEffect: 'none',
        nextEffect: 'none',
        loop: false,
        helpers: {
            title: {
                type: 'outside'
            },
            thumbs: {
                width: 50,
                height: 50,
            }
        }
    });
});
</script>

<p>Syspower task for compression fix.</p>

%if syspowerspgain_subpages:

        %for single_result in result:
            %if result.inputs['apply']:
                <p>Task results written to: <b>${os.path.basename(single_result.gaintable)}</b>.  This table has been modified.</p>
                <p>An unaltered copy of the original caltable exists in ${os.path.basename(single_result.gaintable)+'.backup'}.</p>
            %else:
                <p>Summary and per-antenna plots are shown below,
                reflecting pdiff and modified rq tables.  However, task results were <b>not</b> written to any caltable in the callibrary.</p>
            %endif

	        <ul>
	        %for band in band_display:
	            <li>${band}-band</li>
	            <ul>
	            %for baseband in band_display[band]:
	                <li>Baseband ${baseband}: ${band_display[band][baseband]}</li>
	            %endfor
	            </ul>
	        %endfor
	        </ul>

        %endfor

        <h4>Per antenna plots:<br>
        <ul>
        %for band in syspowerspgain_subpages.keys():
            %for ms in syspowerspgain_subpages[band].keys():
                   <li><b>${band}-band</b>:
                    <a class="replace" href="${rendererutils.get_relative_url(pcontext.report_dir, dirname, syspowerspgain_subpages[band][ms])}">Syspower RQ SPgain plots</a> |
                    <a class="replace" href="${rendererutils.get_relative_url(pcontext.report_dir, dirname, pdiffspgain_subpages[band][ms])}">Syspower Pdiff Template SPgain plots</a></li>
            %endfor
        %endfor
        </ul>
        </h4>

    % for band in all_plots:
        <%self:plot_group plot_dict="${all_plots[band]}"
                          url_fn="${lambda ms: 'noop'}">

                <%def name="title()">
                    Summary plots ${band}-band
                </%def>

                <%def name="preamble()">
                </%def>

                <%def name="mouseover(plot)">${plot.parameters['largecaption']} ${band}-band</%def>

                <%def name="fancybox_caption(plot)">
                  ${plot.parameters['smallcaption']}  ${band}-band
                </%def>

                <%def name="caption_title(plot)">
                   ${plot.parameters['largecaption']}  ${band}-band
                </%def>
        </%self:plot_group>

    % endfor


%else:

No bands/basebands in these data will be processed for the hifv_syspower task.

%endif