<%!
rsc_path = ""
import os
import pipeline.infrastructure.renderer.htmlrenderer as hr

def is_rejected(keep):
    desc=''
    if not keep:
        desc = ' <a style="color:red">rejected</a>'
    return desc

%>
<%inherit file="t2-4m_details-base.mako"/>


<%

import numpy as np
import matplotlib.pyplot as plt
from matplotlib.pyplot import cm
import matplotlib.colors as colors

def fmt_val(rms,scale=1.):
    if rms is None:
        return 'N/A'
    else:
        #return np.format_float_positional(rms*scale, precision=3, fractional=False, trim='-')
        return np.format_float_positional(rms*scale, precision=3, fractional=False)

def colorcode(x, color='Greys',vmin=None,vmax=None):
    """Generate hex colorcode from a value.

    cmap_name options: 'Greys', 'Purples', 'Blues', 'Greens', 'Oranges', 'Reds'
    colors
    """
    norm=colors.Normalize(vmin, vmax)
    x_norm=0.05+0.5*(x-vmin) / (vmax-vmin)

    cmap=cm.get_cmap(name=cmap_name)
    rgb=cmap(x_norm)
    rgb_hex=colors.to_hex(rgb)
    return rgb_hex

def val2color(x, cmap_name='Greys',vmin=None,vmax=None):
    """Generate hex colorcode from a value.

    some cmap_name options: 'Greys', 'Purples', 'Blues', 'Greens', 'Oranges', 'Reds'
    """
    norm=colors.Normalize(vmin, vmax)
    x_norm=0.05+0.5*(x-vmin) / (vmax-vmin)
    
    cmap=cm.get_cmap(name=cmap_name)
    rgb=cmap(x_norm)
    rgb_hex=colors.to_hex(rgb)
    return rgb_hex

def dev2color(x):
    color_list=['gainsboro','lightgreen','yellow','red']
    if x<=4 and x>3:
      rgb_hex=colors.cnames[color_list[0]]
    if x<=5 and x>4:
      rgb_hex=colors.cnames[color_list[1]]
    if x<=6 and x>5:
      rgb_hex=colors.cnames[color_list[2]]
    if x>6:
      rgb_hex=colors.cnames[color_list[3]]
    return rgb_hex            

def dev2shade(x):
    cmap=cm.get_cmap(name='Reds')
    absx=abs(x)
    if absx<4 and absx>=3:
      rgb_hex=colors.to_hex(cmap(0.2))
    if absx<5 and absx>=4:
      rgb_hex=colors.to_hex(cmap(0.3))
    if absx<6 and absx>=5:
      rgb_hex=colors.to_hex(cmap(0.4))
    if absx>=6:
      rgb_hex=colors.to_hex(cmap(0.5))
    return rgb_hex  

def pct2shade(x):
    cmap=cm.get_cmap(name='Reds')
    if x>=0.5 and x<0.8:
      rgb_hex=colors.to_hex(cmap(0.2))
    if x>=0.2 and x<0.5:
      rgb_hex=colors.to_hex(cmap(0.3))
    if x>0.0 and x<0.2:
      rgb_hex=colors.to_hex(cmap(0.4))
    if x<=0.0:
      rgb_hex=colors.to_hex(cmap(0.5))
    return rgb_hex        

border_line="2px solid #AAAAAA"
cell_line="1px solid #DDDDDD"
bgcolor_list=[dev2shade(3.),dev2shade(4.),dev2shade(5.),dev2shade(6.)]
bgcolor_pct_list=[pct2shade(0.7),pct2shade(0.3),pct2shade(.1),pct2shade(.0)]
%>


<style type="text/css">

.table {
    border-collapse: collapse;
    vertical-align: middle;
    text-align: center;      
}

.table td {
    border-collapse: collapse;
    vertical-align: middle;
    text-align: center;
    border-bottom: ${cell_line};
    border-right: ${cell_line};  
    font-size: 12px;
    background-clip: padding-box;
}

.table th {
    border-collapse: collapse;
    vertical-align: middle;
    text-align: center;
    font-size: 12px;
    border-bottom: ${border_line} !important;
    border-right: ${border_line} !important;
    border-top: ${border_line} !important;
    border-left: ${border_line}  !important;
    background-color: #F9F9F9;
}

.table caption {
    color: black;
}

</style>

<%block name="title">Make Cutout Images</%block>

% if not use_minified_js:
<link href="${self.attr.rsc_path}resources/css/select2.css" rel="stylesheet"/>
<link href="${self.attr.rsc_path}resources/css/select2-bootstrap.css" rel="stylesheet"/>
<script src="${self.attr.rsc_path}resources/js/select2.js"></script>
% endif

<script>
$(document).ready(function() {
    // return a function that sets the SPW text field to the given spw
    var createSpwSetter = function(spw) {
        return function() {
            // trigger a change event, otherwise the filters are not changed
            $("#select-spw").val([spw]).trigger("change");
        };
    };

    // create a callback function for each overview plot that will select the
    // appropriate spw once the page has loaded
    $(".thumbnail a").each(function (i, v) {
        var o = $(v);
        var spw = o.data("spw");
        o.data("callback", createSpwSetter(spw));
    });
});

$(function () {
    $("body").tooltip({
        selector: '[data-toggle="tooltip"]',
        container: 'body'
    });
})
</script>

<p>Make cutouts of requested imaging products.</p>


<h4>Cutout Image Statistical Properties</h4>

<div class="table-responsive">
<table style="float: left; margin:0 10px; width: auto; text-align:center" class="table table-hover table-condensed">

<caption>
  <li>
      Peak: the pixel value with the furthest deviation from zero. This value is either maximum or minimum of the image. The peak of a residual image is used by CASA/tclean as a basis for identifying model components, triggering major cycles, and setting clean stopping thresholds.
  </li>
  <li>
      MADrms: the median absolute deviation from the median (i.e., 'medabsdevmed' from CASA/imstat), multiplied by 1.4826, which is the scaling factor between rms and MAD for a normal distribution.
  </li>  
  <li>
      Px<sub><800&mu;Jy</sub>: the percentage of pixels below 800&mu;Jy in the unmasked region of Rms image. The color background is determined by the percentage level: <p style="background-color:${bgcolor_pct_list[0]}; display:inline;">50&#37&le;pct&lt;80&#37</p>; <p style="background-color:${bgcolor_pct_list[1]}; display:inline;">20&#37&le;pct&lt;50&#37</p>; <p style="background-color:${bgcolor_pct_list[2]}; display:inline;">0&#37&le;pct&lt;20&#37</p>; <p style="background-color:${bgcolor_pct_list[3]}; display:inline;">pct=0&#37</p>.
  </li>
  
  <li>
      For the columns present for all Stokes planes (except Px<sub><800&mu;Jy</sub>), the color background highlights spectral windows with a statistical property signficantly deviated from its median over all spw groups: <p style="background-color:${bgcolor_list[0]}; display:inline;">3&#963&le;dev&lt;4&#963</p>; <p style="background-color:${bgcolor_list[1]}; display:inline;">4&#963&le;dev&lt;5&#963</p>; <p style="background-color:${bgcolor_list[2]}; display:inline;">5&#963&le;dev&lt;6&#963</p>; <p style="background-color:${bgcolor_list[3]}; display:inline;">6&#963&le;dev</p>. The deviation, in units of &#963 (defined as 1.4826*MAD), is viewable in a tooltip box. For the 'Peak' column in which the value can be either positive or negative, the median and deviation are calculated using the peak amplitude. For the 'MADRms' or 'Median' columns, only the outliers above the median are highlighted.
  </li>        
</caption>

<thead>
</thead>

<tbody>

% for idx_pol, name_pol in enumerate(['I','Q','U','V']):

  <tr>
    <th>Stokes: <i>${name_pol}</i></th>
    <th colspan="3">Non-Pbcor Restored</th>
    <th colspan="3">Non-Pbcor Residual</th>
    
    % if name_pol=='I':
      <th colspan="4">Rms Image</th>
      <th colspan="3">Beam</th>
      <th colspan="3">PB</th>
    % else:
      <th colspan="3">Rms Image</th>
    % endif
  </tr>

  <tr>
    <th rowspan="2" style="vertical-align : middle;text-align:center;">Spw</th>
    <th colspan="1">Peak</th>
    <th colspan="1">MADrms</th>
    <th colspan="1" style="border-right: ${border_line}"><sup>Peak</sup>/<sub>MADrms</sub></th>
    <th colspan="1">Peak</th>
    <th colspan="1">MADrms</th>
    <th colspan="1" style="border-right: ${border_line}"><sup>Peak</sup>/<sub>MADrms</sub></th>
    <th colspan="1">Max</th>
    <th colspan="1">Median</th>
    % if name_pol=='I':
       <th colspan="1">Px<sub><800&mu;Jy</th>
      <th colspan="1"  style="border-right: ${border_line}">Masked</th>
      <th colspan="1">Major</th>
      <th colspan="1">Minor</th>
      <th colspan="1"  style="border-right: ${border_line}">P.A.</th>      
      <th colspan="1">Max</th>
      <th colspan="1">Min</th>
      <th colspan="1"  style="border-right: ${border_line}">Median</th>
    % else:
      <th colspan="1"  style="border-right: ${border_line}">Px<sub><800&mu;Jy</sub></th>
    % endif

  </tr>

  <tr>
    <th colspan="1" >mJy/bm</th>
    <th colspan="1" >mJy/bm</th>
    <th colspan="1"   style="border-right: ${border_line}" >N/A</th>
    <th colspan="1" >mJy/bm</th>
    <th colspan="1" >mJy/bm</th>
    <th colspan="1"   style="border-right: ${border_line}">N/A</th>
    <th colspan="1" >mJy/bm</th>
    <th colspan="1" >mJy/bm</th>
    % if name_pol=='I':
      <th colspan="1" >Pct.</th>
      <th colspan="1"   style="border-right: ${border_line}">Pct.</th>
      <th colspan="1" >arcsec</th>
      <th colspan="1" >arcsec</th>
      <th colspan="1"   style="border-right: ${border_line}">deg.</th>
      <th colspan="1" >N/A</th>
      <th colspan="1" >N/A</th>
      <th colspan="1"   style="border-right: ${border_line}">N/A</th>      
    % else:
      <th colspan="1"  style="border-right: ${border_line}">Pct.</th>
    % endif
  </tr>  

  % for idx_spw, (spw,stats_spw) in enumerate(stats.items()):
    <tr>

      <%
      cell_style=[f'border-left: {border_line}',f'border-right: {border_line}']
      if idx_spw==len(stats)-1:
          cell_style.append('border-bottom: '+border_line)          
      cell_style='style="{}"'.format(('; ').join(cell_style))                    
      reject_desc=is_rejected(info_dict.get(str(spw),True))
      %> 
      <td ${cell_style}><b>${str(spw)} ${reject_desc}</b></td>
      
      <%
      type_item_scale=[('image','peak',1e3),
                  ('image','madrms',1e3),
                  ('image','max/madrms',1),
                  ('residual','peak',1e3),
                  ('residual','madrms',1e3),
                  ('residual','max/madrms',1),
                  ('rms','max',1e3),
                  ('rms','median',1e3),
                  ('rms','pct<800e-6',1e2),
                  ('rms','pct_masked',1e2),
                  ('beam','bmaj',1),
                  ('beam','bmin',1),
                  ('beam','bpa',1),                                    
                  ('pb','max',1.),
                  ('pb','min',1.),
                  ('pb','median',1.)]
      %>

      % for idx_item,(t,i,s) in enumerate(type_item_scale):
        % if name_pol!='I' and (t,i) in [('rms','pct_masked'),('pb','max'),('pb','min'),('pb','median'),('beam','bmaj'),('beam','bmin'),('beam','bpa')]:
          <% continue %> 
        % endif

        <%
        cell_style=[]
        cell_title=''
        if 'pct' in i:
          suffix='&#37'
        else:
          suffix=''
        if not (t=='pb' or t=='beam' or i=='pct_masked'):
          dev_in_madrms=abs(stats_spw[t][i][idx_pol])-stats_summary[t][i]['spwwise_median'][idx_pol]
          madrms=stats_summary[t][i]['spwwise_madrms'][idx_pol]
          
          if i=='pct<800e-6':
            pct_800=stats_spw[t][i][idx_pol]
            if pct_800<0.8:
              bgcolor=pct2shade(pct_800)
              cell_style.append(f'background-color: {bgcolor}')
          elif (i=='median' or i =='madrms'):
            if dev_in_madrms>madrms*3.0:
              bgcolor=dev2shade(dev_in_madrms/madrms)
              cell_style.append(f'background-color: {bgcolor}')  
            cell_title='{:.2f}'.format(dev_in_madrms/madrms)           
          else:
            if abs(dev_in_madrms)>madrms*3.0:
              bgcolor=dev2shade(dev_in_madrms/madrms)
              cell_style.append(f'background-color: {bgcolor}')  
            cell_title='{:.2f}'.format(dev_in_madrms/madrms)     
                          

        if idx_item in (2,5,9,12,15) or (name_pol!='I' and idx_item in ((2,5,8))):
          cell_style.append('border-right: '+border_line)
        if idx_spw==len(stats)-1:
          cell_style.append('border-bottom: '+border_line)          
        cell_style='style="{}"'.format(('; ').join(cell_style))
        cell_style+=' title="{}" data-toggle="tooltip"'.format(cell_title)
        %>
      
        % if t!='beam':
          <td ${cell_style}>${fmt_val(stats_spw[t][i][idx_pol],scale=s)}${suffix}</td>
        % else:
          <td ${cell_style}>${fmt_val(stats_spw[t][i],scale=s)}${suffix}</td>
        % endif
      % endfor

    </tr>

  % endfor
  <tr><td style="border-bottom: none; border-top: none; border-left: none; border-right: none;"></td></tr>

% endfor

</tbody>

</table>
</div>

<div style="clear:both;"></div>


<%self:plot_group plot_dict="${rms_plots}" 
                  url_fn="${lambda ms:  'noop'}"
                  break_rows_by="band,order_idx"
                  sort_row_by="band,order_idx">

        <%def name="title()">
        </%def>

        <%def name="preamble()"></%def>

        <%def name="mouseover(plot)">${plot.basename}</%def>

        <%def name="fancybox_caption(plot)">
           ${plot.y_axis} vs. ${plot.x_axis}
        </%def>

        <%def name="caption_title(plot)">
           ${plot.y_axis} vs. ${plot.x_axis}
        </%def>
</%self:plot_group>

<%self:plot_group plot_dict="${img_plots}" 
                  url_fn="${lambda ms:  'noop'}"
                  break_rows_by="band",
                  separate_rows_by="thick-line",
                  sort_row_by="band,order_idx">

        <%def name="title()">
        </%def>

        <%def name="preamble()"></%def>

        <%def name="mouseover(plot)">${plot.basename}</%def>

        <%def name="fancybox_caption(plot)">
            Spw: ${plot.parameters['virtspw']} Stokes: ${plot.parameters['stokes']} Type: ${plot.parameters['type']}
        </%def>

        <%def name="caption_title(plot)">
            Spw: ${plot.parameters['virtspw']} Stokes: ${plot.parameters['stokes']} Type: ${plot.parameters['type']}
            <br>
            ${is_rejected(info_dict.get(plot.parameters['virtspw'],True))}             
        </%def>
</%self:plot_group>
