<%!
rsc_path = ""
import os
import pipeline.infrastructure.renderer.htmlrenderer as hr
import pipeline.infrastructure.filenamer as filenamer
import string
%>
<%inherit file="t2-4m_details-base.mako"/>

<%block name="title">
Self-Calibration<br>
<small>Self-calibration using the science target visibilities</small>
</%block>

<script type="text/javascript">

  $("td:contains('not implemented')").addClass('desc-summary');

  $(function () {
    $('[data-toggle="tooltip"]').tooltip()
  })

</script>


<style type="text/css">

.desc-summary {
  font-weight:bold;
  background-color:#F9F9F9;
}

.table {
  border: 2px solid #CCC;
  font-family: Arial, Helvetica, sans-serif;
  border-collapse: collapse;
  vertical-align: middle;
  text-align: center;
  float: left;
  margin: 0px 10px;
  width: auto;
}

.table td {
  border: 1px solid #CCC;
  vertical-align: middle;
  text-align: center;
  background-clip: padding-box;
  min-width: 80px;     
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

.rotate {
    /* FF3.5+ */
    -moz-transform: rotate(-90.0deg);
    /* Opera 10.5 */
    -o-transform: rotate(-90.0deg);
    /* Saf3.1+, Chrome */
    -webkit-transform: rotate(-90.0deg);
    /* IE6,IE7 */
    filter: progid: DXImageTransform.Microsoft.BasicImage(rotation=0.083);
    /* IE8 */
    -ms-filter: "progid:DXImageTransform.Microsoft.BasicImage(rotation=0.083)";
    /* Standard */
    transform: rotate(-90.0deg);
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
%>


<!-- Brief Summary -->

<a class="anchor" id="targetlist"></a>
<h3>List of Self-cal Targets</h3>

% if not cleantargets:
    <p>No valid self-calibration result was returned.</p>
    <% return STOP_RENDERING %>
% endif


<div class="table-responsive">
<table class="table table-bordered">
  <thead>
        <tr>
            <th>Field</th>
            <th>Band</th>
            <th>SpW</th>
            <th>Phasecenter</th>
            <th>Cell</th>
            <th>Imsize</th>
            <th>Solints to attempt</th>
            <th>Success</th>
            <th>Cont.<br>applied</th>
            <th>Line<br>applied</th>
        <tr>
  </thead>
  <caption>
    Self-Calibration Targets Summary: All attempted solution intervals (solints) are shown in <b>bold</b>. If a solint is highlighted in <b><a style="color:blue">blue</a></b>, it represents a final applicable solint.
  </caption>
  <tbody>
  % for tr in targets_summary_table:
    <tr>
    % for td in tr:
      ${td}
    % endfor
    </tr>
  %endfor
  </tbody>
</table> 
</div>

% if is_restore:
    <p>
    The task has skipped the self-calibration solver and is executed in the applycal-only mode based on the existing selfcal/restore resources.
    Please visit the original weblog for details on the self-calibration process.
    </p>
    <% return STOP_RENDERING %>
% endif

<h3>Self-cal Target Details</h3>

% for target in cleantargets:

    <%
    slib=target['sc_lib']
    key=(target['field_name'],target['sc_band'])
    show_spw_summary= slib['SC_success'] and spw_tabs[key] is not None
    show_sol_summary= solint_tabs[key] is not None
    valid_chars = "%s%s" % (string.ascii_letters, string.digits)
    id_name=filenamer.sanitize(target['field_name']+'_'+target['sc_band'],valid_chars)
    %>

    <a class="anchor" id="${id_name}"></a>
    <h4>
      ${target['field']}&nbsp;${fm_band(target['sc_band'])}&nbsp;
      <a href="#targetlist"><sup>back to top</sup></a>&nbsp;&nbsp;
      <a class="btn btn-sm btn-light" data-toggle="collapse" 
          href="#${id_name}_summary" 
          role="button" aria-expanded="false" aria-controls="${id_name}_summary">
          Summary
      </a>
      % if show_sol_summary:
      <a class="btn btn-sm btn-light" data-toggle="collapse"
          href="#${id_name}_persol"
          role="button" aria-expanded="false" aria-controls="${id_name}_persol">
          Per-Solint Details
      </a>
      % endif
      % if show_spw_summary:
      <a class="btn btn-sm btn-light" data-toggle="collapse"
          href="#${id_name}_perspw"
          role="button" aria-expanded="false" aria-controls="${id_name}_perspw">
          Per-Spw Details
      </a>
      % endif      
    </h4>
    
    <div class="table-responsive collapse multi-collapse in" id="${id_name}_summary">
    <table class="table table-bordered">
      <!-- <caption style="caption-side:top">Initial/Final Image Comparisons</caption> -->
      <caption>Initial/Final Image Comparisons</caption>
      <tbody>
      % for tr in summary_tabs[key]:
        <tr>
        % for td in tr:
          ${td}
        % endfor
        </tr>
      %endfor
      </tbody>
    </table>      
    </div>      
    
    % if show_sol_summary :
    <div class="table-responsive collapse multi-collapse in" id="${id_name}_persol">
    <table class="table table-bordered">
      <!-- <caption style="caption-side:top">Per solint stats</caption> -->
      <%
      caption='Per solint stats'
      if 'mosaic' == target['sc_lib']['obstype'] :
        caption+=': Gaintables for mosaics include solutions for all self-calibratable sub-fields of that mosaic. As such, flagged gain-calibration solutions of a given field can lead to significant, and undesirable, additional flagging in adjacent fields, even if said adjacent field had a successful gain-calibration solution of its own. To mitigate this, all flagged solutions are dropped from the gaintable. Additionally any field with >25% flagging in total is removed from the gaintable and will not have this gaintable applied to avoid excessive interpolation. "Excluded" indicates the antenna is not present in this gaintable due to all of its solutions being dropped. In the event that this gaintable is applied, i.e. there is at least one field with solutions remaining in the gaintable, any excluded antennas are treated as if they are 100% flagged.'
      %>
      <caption>${caption}</caption>
      <thead>
          <tr>
              <th>Solint</th>
              % for solint in target['sc_solints']:
                <th>${solint}</th>
              % endfor
          <tr>
      </thead>      
      <tbody>
      % for tr in solint_tabs[key]:
          <tr>
          % for td in tr:
            ${td}
          % endfor
          </tr>
      %endfor
      </tbody>
    </table>      
    </div>
    % endif

    % if show_spw_summary :
    <div class="table-responsive row" id="${id_name}_perspw">
    <table class="table table-bordered">
      <caption>Per Spectral-window Summary</caption>
      <tbody>
      % for tr in spw_tabs[key]:
        <tr>
        % for td in tr:
          ${td}
        % endfor
        </tr>
      %endfor
      </tbody>
    </table>      
    </div>        
    % endif
        
% endfor



