<%!
rsc_path = ""
import os
import pipeline.infrastructure.renderer.htmlrenderer as hr
%>

<%
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.pyplot import cm
import matplotlib.colors as colors

def fmt_cell(rms,scale=1.e3):
    if rms is None:
        return 'N/A'
    else:
        return np.format_float_positional(rms*scale, precision=3, fractional=False)

def fmt_model(amplitude,alpha):
    label='<i>I</i><sub>3GHz</sub>='+'{:.3f}'.format(amplitude*1e3)+'mJy/bm'
    label+=', '
    label+='alpha='+'{:.3f}'.format(alpha)
    return label

def fmt_spw(roi_stats,idx):
    reffreq=roi_stats['reffreq'][idx]
    spwstr=roi_stats['spw'][idx]
    keep=roi_stats['keep'][idx]
    label=spwstr+' / ' +f'{reffreq/1e9:.3f}'
    if not keep:
        label+=' <a style="color:red">rejected</a>'
    return label

def diff2shade(pct):
    cmap=cm.get_cmap(name='Reds')
    apct=abs(pct)
    if 5<=apct<10:
      rgb_hex=colors.to_hex(cmap(0.2))
    if 10<=apct<20:
      rgb_hex=colors.to_hex(cmap(0.3))
    if 20<=apct<30:
      rgb_hex=colors.to_hex(cmap(0.4))
    if apct>=30:
      rgb_hex=colors.to_hex(cmap(0.5))
    return rgb_hex   

def snr2shade(snr):
    cmap=cm.get_cmap(name='Reds')
    if 7.5<=snr<10:
      rgb_hex=colors.to_hex(cmap(0.2))
    if 5.0<=snr<7.5:
      rgb_hex=colors.to_hex(cmap(0.3))
    if 3.0<=snr<5.0:
      rgb_hex=colors.to_hex(cmap(0.4))
    if snr<3.0:
      rgb_hex=colors.to_hex(cmap(0.5))
    return rgb_hex       

border_line="2px solid #AAAAAA"
cell_line="1px solid #DDDDDD"
bgcolor_pct_list=[diff2shade(7.),diff2shade(15.),diff2shade(25.),diff2shade(35.)]
bgcolor_snr_list=[snr2shade(8.),snr2shade(6.),snr2shade(4.),snr2shade(2.)]
%>

<%inherit file="t2-4m_details-base.mako"/>

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
    font-size: 12px;
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

<%block name="title">Analysis of Stokes Cubes</%block>

<p>This task performs analyses of Stokes cubes.</p>


<%self:plot_group plot_dict="${stokesplots}"
                                  url_fn="${lambda ms:  'noop'}">

        <%def name="title()">
        </%def>

        <%def name="preamble()">
        </%def>

        <%def name="mouseover(plot)">${plot.basename}</%def>

        <%def name="fancybox_caption(plot)">
            ${plot.parameters['desc']}: ${plot.y_axis} vs. ${plot.x_axis}
        </%def>

        <%def name="caption_title(plot)">
            ${plot.parameters['desc']}
        </%def>
</%self:plot_group>


<%self:plot_group plot_dict="${rmsplots}"
                                  url_fn="${lambda ms:  'noop'}">

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

</%self:plot_group>

<%self:plot_group plot_dict="${fluxplots}"
                                  url_fn="${lambda ms:  'noop'}">

        <%def name="title()">
        </%def>

        <%def name="preamble()">
        </%def>

        <%def name="mouseover(plot)">${plot.basename}</%def>

        <%def name="fancybox_caption(plot)">
            ${plot.parameters['desc']}: ${plot.y_axis} vs. ${plot.x_axis}
        </%def>

        <%def name="caption_title(plot)">
            ${plot.parameters['desc']}
        </%def>
</%self:plot_group>


<h4>Flux Properties</h4>

<div class="table-responsive">
<table style="float: left; margin:0 10px; width: auto; text-align:center" class="table table-bordered table-hover table-condensed">
<caption>
    <li>
        <i>I</i>, <i>Q</i>, <i>U</i>, <i>V</i> : Brightness at peak positions, averaged over a 3x3 pixel box.
    </li>     
    <li>
        <i>I</i><sub>SNR</sub>: Peak signal-to-noise ratio (SNR) at Stokes I.
        The color background is determined by SNR: 
        <p style="background-color:${bgcolor_snr_list[0]}; display:inline;">7.5&le;snr&lt;10</p>; 
        <p style="background-color:${bgcolor_snr_list[1]}; display:inline;">5&le;snr&lt;7.5</p>; 
        <p style="background-color:${bgcolor_snr_list[2]}; display:inline;">3&le;snr&lt;5</p>; 
        <p style="background-color:${bgcolor_snr_list[3]}; display:inline;">snr&lt;3.0</p>.
    </li>       
    <li>
        <i>I</i><sub>model</sub> : Brightness from the best-fit power-law model.
    </li>   
    <li>
        <i>I</i><sub>res</sub> : <i>I</i>-<i>I</i><sub>model</sub>
    </li>          
    <li>
        <i>I</i><sub>res,pct</sub>: percentage difference between the data and model: (<i>I</i>-<i>I</i><sub>model</sub>)/<i>I</i>.
        The color background is determined by the absolute percentage difference level: <p style="background-color:${bgcolor_pct_list[0]}; display:inline;">5&#37&le;pct&lt;10&#37</p>; <p style="background-color:${bgcolor_pct_list[1]}; display:inline;">10&#37&le;pct&lt;20&#37</p>; <p style="background-color:${bgcolor_pct_list[2]}; display:inline;">20&#37&le;pct&lt;30&#37</p>; <p style="background-color:${bgcolor_pct_list[3]}; display:inline;">30&#37&le;pct</p>.
    </li>  
    <li>
        Any rejected plane is excluded for spectral model fitting.
    </li>        
</caption>    

<thead>
</thead>

<tbody>

    <tr>
        <th colspan="2"><b>Region</b></td>
        <th colspan="8"><b>Stokes I Peak</b></td>
        <th colspan="8"><b>LinPol Peak</b></td>
    </tr>
    <tr>
        <th rowspan="2" style="vertical-align : middle;text-align:center;"><b>Spw / Freq (GHz)</b></td>
        <th rowspan="1" style="vertical-align : middle;text-align:center;"><b>RMS<sub>median<sub></b></td>
        % for idx in [0,1]:
            <th colspan="1"><b><i>I</i></b></th>
            <th colspan="1"><b><i>I</i><sub>SNR</sub></b></th>
            <th colspan="1"><b><i>I</i><sub>model</sub></b></td>
            <th colspan="1"><b><i>I</i><sub>res</sub></b></td>
            <th colspan="1"><b><i>I</i><sub>res,pct</sub></b></td>
            <th colspan="1"><b><i>Q</i></b></td>
            <th colspan="1"><b><i>U</i></b></td>
            <th colspan="1"><b><i>V</i></b></td>
        % endfor     
    </tr>   
    <tr>
        <th colspan="1">mJy/bm</th>
        % for idx in [0,1]:
            <th colspan="1">mJy/bm</th>
            <th colspan="1">N/A</td>
            <th colspan="1">mJy/bm</td>
            <th colspan="1">mJy/bm</td>
            <th colspan="1">pct.</td>
            <th colspan="1">mJy/bm</td>
            <th colspan="1">mJy/bm</td>
            <th colspan="1">mJy/bm</td>
        % endfor     
    </tr>          

    % for idx in range(len(stats['peak_stokesi']['stokesi'])):
    <tr>
        <%
        cell_style=[f'border-left: {border_line}',f'border-right: {border_line}']
        if idx==len(stats['peak_stokesi']['stokesi'])-1:
            cell_style.append('border-bottom: '+border_line)          
        cell_style='style="{}"'.format(('; ').join(cell_style))                   
        rms_median_stokesi=stats['peak_stokesi']['rms'][idx][0]
        %> 

        <td ${cell_style}><b>${fmt_spw(stats['peak_stokesi'],idx)}</b></td>
        <td ${cell_style}>${fmt_cell(rms_median_stokesi)}</td>

        % for roi_name in ['peak_stokesi','peak_linpolint']:

            <%
            cell_style=[]
            if idx==len(stats['peak_stokesi']['stokesi'])-1:
                cell_style.append('border-bottom: '+border_line) 
                        
            cell_style_default='style="{}"'.format(('; ').join(cell_style))
            cell_style_withborder='style="{}"'.format(('; ').join(cell_style+['border-right: '+border_line]))
                

            diff_pct=(stats[roi_name]['stokesi'][idx]-stats[roi_name]['model_flux'][idx])/stats[roi_name]['stokesi'][idx]*100.
            if abs(diff_pct)>=5:
                bgcolor='background-color: '+diff2shade(diff_pct)
                cell_style_pct='style="{}"'.format(('; ').join(cell_style+[bgcolor]))
            else:
                cell_style_pct='style="{}"'.format(('; ').join(cell_style))                  
            
            snr=stats[roi_name]['stokesi'][idx]/rms_median_stokesi
            if snr<10:
                bgcolor='background-color: '+snr2shade(snr)
                cell_style_rms='style="{}"'.format(('; ').join(cell_style+[bgcolor]))
            else:
                cell_style_rms='style="{}"'.format(('; ').join(cell_style))            

            %>          
            
            <td colspan="1" ${cell_style_default}>${fmt_cell(stats[roi_name]['stokesi'][idx])}</td>
            <td colspan="1" ${cell_style_rms}>${fmt_cell(snr,scale=1.)}</td>
            <td colspan="1" ${cell_style_default}>${fmt_cell(stats[roi_name]['model_flux'][idx])}</td>
            <td colspan="1" ${cell_style_default}>${fmt_cell(stats[roi_name]['stokesi'][idx]-stats[roi_name]['model_flux'][idx])}</td>
            <td colspan="1" ${cell_style_pct}>${fmt_cell(diff_pct,scale=1.)}&#37</td>
            <td colspan="1" ${cell_style_default}>${fmt_cell(stats[roi_name]['stokesq'][idx])}</td>
            <td colspan="1" ${cell_style_default}>${fmt_cell(stats[roi_name]['stokesu'][idx])}</td>
            <td colspan="1" ${cell_style_withborder}>${fmt_cell(stats[roi_name]['stokesv'][idx])}</td>
        % endfor
    </tr>
    %endfor 

    <tr>
      <td colspan="2" style="border-left: ${border_line}; border-right: ${border_line}; border-bottom: ${border_line}""><b>Model</b></td>
      <td colspan="8" style="border-right: ${border_line}; border-bottom: ${border_line}">${fmt_model(stats['peak_stokesi']['model_amplitude'],stats['peak_stokesi']['model_alpha'])}</td>
      <td colspan="8" style="border-right: ${border_line}; border-bottom: ${border_line}">${fmt_model(stats['peak_linpolint']['model_amplitude'],stats['peak_linpolint']['model_alpha'])}</td>
    </tr>
                
</tbody>
</table>
</div>




