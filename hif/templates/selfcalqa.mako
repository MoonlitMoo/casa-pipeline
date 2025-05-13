<%!
rsc_path = ""
import os
import pipeline.infrastructure.renderer.htmlrenderer as hr
%>

<%namespace name="base" file="t2-4m_details-base.mako"/>

<script>
    lazyload();
</script>

<style type="text/css">

.table {
  font-family: Arial, Helvetica, sans-serif;
  border-collapse: collapse;
  vertical-align: middle;
  text-align: center;
  float: left;
  margin: 0px 10px;
  width: auto;
}

.table td {
  vertical-align: middle;
  text-align: center;
  background-clip: padding-box;    
  min-width: 60px;     
}

.table th {
  font-weight: bold;
  vertical-align: middle;
  text-align: center;  
}

.table td:first-child {
  vertical-align: middle;
  text-align: center;       
}

.anchor { padding-top: 90px; }

img {
    height: auto;
    display: block;
    margin-left: auto;
    margin-right: auto;
}

</style>


<%
import numpy as np

def fm_band(band):
    return '('+band.strip().replace('EVLA_', '').replace('_', ' ').capitalize()+')'    

def fm_sc_success(success):
    if success:
        return '<a style="color:blue">Yes</a>'
    else:
        return '<a style="color:red">No</a>'

def fm_reason(slib):
    rkey='Stop_Reason'
    if rkey not in slib:
        return 'Estimated Selfcal S/N too low for solint'
    else:
        return slib[rkey]

vislist=slib['vislist']
%>

<h3>${target}&nbsp;${fm_band(band)}</h3>

<h4>
  Passed: ${fm_sc_success(slib[vislist[-1]][solint]['Pass'])}
</h4>


<div class="table-responsive">
<table class="table table-bordered">
  <caption>Prior/Post Image Comparisons</caption>
  <tbody>
  % for tr in summary_tab:
    <tr>
    % for td in tr:
      ${td}
    % endfor
    </tr>
  %endfor
  </tbody>
</table>      
</div>    

<div class="table-responsive">
<table class="table table-bordered">
  <%
  caption='Sol. Summary'
  caption_items=[]
  if 'mosaic' == slib['obstype'] :
    caption_items.append('<li>Initial vs. Final: Gaintables for mosaics include solutions for all self-calibratable sub-fields of that mosaic. As such, flagged gain-calibration solutions of a given field can lead to significant, and undesirable, additional flagging in adjacent fields, even if said adjacent field had a successful gain-calibration solution of its own. To mitigate this, all flagged solutions are dropped from the gaintable. Additionally any field with >25% flagging in total is removed from the gaintable and will not have this gaintable applied to avoid excessive interpolation. "Excluded" indicates the antenna is not present in this gaintable due to all of its solutions being dropped. In the event that this gaintable is applied, i.e. there is at least one field with solutions remaining in the gaintable, any excluded antennas are treated as if they are 100% flagged.</li>')
  if any(v is not None for v in fracflag_plots.values()):
    caption_items.append('<li>Frac. Flagged vs. Baseline: Each (per-EB) plot shows the fraction of solutions flagged for each antenna (blue circle) as a function of baseline length, along with a smoothed, continuous version of this distribution (black), and the first (green) and second (red) derivatives of this distribution. Long baselines are defined as the place where the flagging fraction begins to significantly increase (horizontal dashed line), as measured by the locations of significant peaks in the acceleration, and the threshold for passing through antennas with no calibration is defined by the intersection of the long baseline threshold with the smoothed, flagged fraction curve (horizontal dashed line). Antennas with flagged fractions above this threshold will have no calibrations applied but also will not be flagged, i.e. their solutions are set to 1.0+0j.</li>')
  if caption_items:
    caption+= '<ul>' + ''.join(caption_items) + '</ul>'
  %>
  <caption>${caption}</caption>
  <tbody>
  % for tr in nsol_tab:
    <tr>
    % for td in tr:
      ${td}
    % endfor
    </tr>
  %endfor
  </tbody>
</table>      
</div>    


<%base:plot_group plot_dict="${phasefreq_plots}"
                                  url_fn="${lambda ms: 'noop'}">

        <%def name="title()">
        </%def>

        <%def name="preamble()">
        </%def>

        <%def name="mouseover(plot)">${plot.basename}</%def>

        <%def name="fancybox_caption(plot)">
          ${plot.y_axis} vs. ${plot.x_axis}
        </%def>

        <%def name="caption_title(plot)">
           ${plot.y_axis} vs. ${plot.x_axis}
        </%def>

</%base:plot_group>

<div class="row"></div>