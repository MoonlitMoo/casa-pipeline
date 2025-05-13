<%!
rsc_path = ""
import os
import pipeline.infrastructure.renderer.htmlrenderer as hr
from pipeline.infrastructure.renderer import rendererutils
bandsort = {'4':0, 'P':1, 'L':2, 'S':3, 'C':4, 'X':5, 'U':6, 'K':7, 'A':8, 'Q':9}
%>

<a id="topofpage"></a>
<div class="page-header">
	<h1>${plots[0].parameters['type'].title()} Plots<button class="btn btn-default pull-right" onClick="javascript:window.history.back();">Back</button></h1>
</div>

<br>

% for ms in testdelay_subpages.keys():
    <h4>Plots:  <a class="replace"
           href="${rendererutils.get_relative_url(pcontext.report_dir, dirname, testdelay_subpages[ms])}">Test delay plots </a>|
        <a class="replace"
           href="${rendererutils.get_relative_url(pcontext.report_dir, dirname, ampgain_subpages[ms])}">Gain Amplitude </a>|
        <a class="replace"
           href="${rendererutils.get_relative_url(pcontext.report_dir, dirname, phasegain_subpages[ms])}">Gain Phase </a>|
        <a class="replace"
           href="${rendererutils.get_relative_url(pcontext.report_dir, dirname, bpsolamp_subpages[ms])}">BP Amp solution </a>|
        <a class="replace"
           href="${rendererutils.get_relative_url(pcontext.report_dir, dirname, bpsolphase_subpages[ms])}">BP Phase solution </a>
    </h4>
% endfor


% for band in bandsort.keys():
    % if band in bandlist:
    <a id="${band}"></a><br>
    <hr>
    <div class="row">
    <h4>
    % for bb in bandsort.keys():
        % if bb in bandlist:
            <a href="#${bb}">${bb}-band</a>&nbsp;|&nbsp;
        % endif
    % endfor
    <%
    spwlist_band = list(set(bandlist[band]) & set(spwlist))
    if len(spwlist_band) > 0:
        spwlist_band.sort(key=lambda x: int(x))
        first_spw = spwlist_band[0]
    else: 
        first_spw = None
    %>
    <a href="#topofpage">Top of page </a> | (Click to Jump)<br><br>
    % if first_spw is not None: 
        <a href="#${band}-${first_spw}">${band}-band per-spw</a><br><br>
            ${band}-band
     % else:
            ${band}-band
    % endif 
    </h4> <br>
    % for plot in sorted(plots, key=lambda p: bandsort[p.parameters['bandname']]):
        % if band == plot.parameters['bandname']:
            <div class="col-md-2 col-sm-3">
                    <div class="thumbnail">
                        <a data-fancybox="allplots"
                           href="${os.path.relpath(plot.abspath, pcontext.report_dir)}"
                           title="Antenna ${plot.parameters['ant']}  Band ${plot.parameters['bandname']}">
                                <img   src="${os.path.relpath(plot.thumbnail, pcontext.report_dir)}"
                                     title="Antenna ${plot.parameters['ant']}  Band ${plot.parameters['bandname']}"
                                       alt="">
                        </a>
                        <div class="caption">
                            <span class="text-center">Antenna ${plot.parameters['ant']} &nbsp;&nbsp; Band: ${plot.parameters['bandname']}</span>
                        </div>
                    </div>
            </div>
        % endif
    % endfor
    </div>
    %if len(spwlist_band) > 0 and len(spw_plots) > 0:
        % for spw in spwlist_band: 
            <a id="${band}-${spw}"></a><br>
                <hr>
                <div class="row">
                <h4>
                Spw:
                % for spwk in spwlist_band:
                    <a href="#${band}-${spwk}">${spwk}</a>&nbsp;|&nbsp;
                % endfor
                <a href="#${band}">Top of ${band}-band</a> | (Click to Jump)<br><br>
                       ${band}-band Spw: ${spw}
                </h4> <br>
            <%
            sorted_spw_plots = sorted(spw_plots, key=lambda x: x.parameters['spw'])
            %>
            % for plot in sorted(sorted_spw_plots, key=lambda p: bandsort[p.parameters['bandname']]):
                % if band == plot.parameters['bandname'] and spw == plot.parameters['spw']:
                <div class="col-md-2 col-sm-3">
                    <div class="thumbnail">
                        <a data-fancybox="allplots"
                           href="${os.path.relpath(plot.abspath, pcontext.report_dir)}"
                           title="Antenna ${plot.parameters['ant']}  Band ${plot.parameters['bandname']} Spw ${plot.parameters['spw']}">
                                <img   src="${os.path.relpath(plot.thumbnail, pcontext.report_dir)}"
                                     title="Antenna ${plot.parameters['ant']}  Band ${plot.parameters['bandname']} Spw ${plot.parameters['spw']}"
                                       alt="">
                        </a>
                        <div class="caption">
                            <span class="text-center">Antenna ${plot.parameters['ant']} &nbsp;&nbsp; Band: ${plot.parameters['bandname']} &nbsp;&nbsp; Spw: ${plot.parameters['spw']}</span>
                        </div>
                    </div>
                </div>
                %endif
            % endfor
            </div>
        %endfor
    % endif
    % endif
% endfor
