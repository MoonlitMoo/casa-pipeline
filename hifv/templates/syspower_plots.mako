<%!
rsc_path = ""
import os
import pipeline.infrastructure.renderer.htmlrenderer as hr
from pipeline.infrastructure.renderer import rendererutils
%>

<div class="page-header">
	<h1>${plots[0].parameters['band']}-band SysPower ${plots[0].parameters['type'].title()} Plots<button class="btn btn-default pull-right" onClick="javascript:window.history.back();">Back</button></h1>
</div>

<h4>
%for ms in syspowerspgain_subpages[plots[0].parameters['band']].keys():
Plots: <a class="replace" href="${rendererutils.get_relative_url(pcontext.report_dir, dirname, syspowerspgain_subpages[plots[0].parameters['band']][ms])}">Syspower RQ SPgain plots</a> |
       <a class="replace" href="${rendererutils.get_relative_url(pcontext.report_dir, dirname, pdiffspgain_subpages[plots[0].parameters['band']][ms])}">Syspower Pdiff Template SPgain plots</a>
%endfor
</h4>

<br>

<h4>
%if plots[0].parameters['type'] == 'rq':
    %for band in syspowerspgain_subpages.keys():
        %for ms in syspowerspgain_subpages[band].keys():
            <a class="replace" href="${rendererutils.get_relative_url(pcontext.report_dir, dirname, syspowerspgain_subpages[band][ms])}">${band}-band</a> &nbsp;|&nbsp;
        %endfor
    %endfor
%elif plots[0].parameters['type'] == 'pdiff':
    %for band in pdiffspgain_subpages.keys():
        %for ms in pdiffspgain_subpages[band].keys():
            <a class="replace" href="${rendererutils.get_relative_url(pcontext.report_dir, dirname, pdiffspgain_subpages[band][ms])}">${band}-band</a> &nbsp;|&nbsp;
        %endfor
    %endfor
%else:
 &nbsp;
%endif
(Click to Jump)
</h4>
<br><br>


% for plot in sorted(plots, key=lambda p: p.parameters['ant']):
<div class="col-md-2 col-sm-3">
	<div class="thumbnail">
		<a data-fancybox="allplots"
		   href="${os.path.relpath(plot.abspath, pcontext.report_dir)}"
		   title="Antenna ${plot.parameters['ant']}  Band:  ${plot.parameters['band']}">
			<img   src="${os.path.relpath(plot.thumbnail, pcontext.report_dir)}"
				 title="Antenna ${plot.parameters['ant']}  Band:  ${plot.parameters['band']}"
				   alt="">
		</a>
		<div class="caption">
			<span class="text-center">Antenna ${plot.parameters['ant']}   &nbsp; &nbsp; &nbsp; &nbsp; Band:  ${plot.parameters['band']}</span>
		</div>
	</div>
</div>
% endfor
