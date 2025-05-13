<%!
rsc_path = ""
import os
import pipeline.infrastructure.renderer.htmlrenderer as hr
from pipeline.infrastructure.renderer import rendererutils
%>

% if plots:

<div class="page-header">
	<h1>${plots[0].parameters['band']}-band Switched Power ${plots[0].parameters['type'].title()} Plots<button class="btn btn-default pull-right" onClick="javascript:window.history.back();">Back</button></h1>
</div>

% for ms in swpowspgain_subpages[plots[0].parameters['band']].keys():
    <h4>Plots:
        <a class="replace" href="${rendererutils.get_relative_url(pcontext.report_dir, dirname, swpowspgain_subpages[plots[0].parameters['band']][ms])}">SwPow SPgain plots </a> |
        <a class="replace" href="${rendererutils.get_relative_url(pcontext.report_dir, dirname, swpowtsys_subpages[plots[0].parameters['band']][ms])}">SwPow Tsys plots</a>
    </h4>
%endfor

<br>

<h4>
%if plots[0].parameters['type'] == 'spgain':
    %for band in swpowspgain_subpages.keys():
        %for ms in swpowspgain_subpages[band].keys():
            <a class="replace" href="${rendererutils.get_relative_url(pcontext.report_dir, dirname, swpowspgain_subpages[band][ms])}">${band}-band</a> &nbsp;|&nbsp;
        %endfor
    %endfor
%elif plots[0].parameters['type'] == 'tsys':
    %for band in swpowtsys_subpages.keys():
        %for ms in swpowtsys_subpages[band].keys():
            <a class="replace" href="${rendererutils.get_relative_url(pcontext.report_dir, dirname, swpowtsys_subpages[band][ms])}">${band}-band</a> &nbsp;|&nbsp;
        %endfor
    %endfor
%else:
 &nbsp;
%endif
(Click to Jump)
</h4>

% for plot in sorted(plots, key=lambda p: p.parameters['ant']):
<div class="col-md-2 col-sm-3">
	<div class="thumbnail">
		<a data-fancybox="allplots"
		   href="${os.path.relpath(plot.abspath, pcontext.report_dir)}"
		   title="Antenna ${plot.parameters['ant']} Band:  ${plot.parameters['band']}">
			<img   src="${os.path.relpath(plot.thumbnail, pcontext.report_dir)}"
				 title="Antenna ${plot.parameters['ant']} Band:  ${plot.parameters['band']}"
				   alt="">
		</a>
		<div class="caption">
			<span class="text-center">Antenna ${plot.parameters['ant']}   &nbsp; &nbsp; &nbsp; &nbsp; Band:  ${plot.parameters['band']}</span>
		</div>
	</div>
</div>
% endfor

%endif
