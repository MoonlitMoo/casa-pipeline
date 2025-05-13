<%!
rsc_path = ""
import os
import pipeline.infrastructure.renderer.htmlrenderer as hr
from pipeline.infrastructure.renderer import rendererutils
%>
<%inherit file="t2-4m_details-base.mako"/>

<%block name="title">Prior calibrations</%block>

<p>Gain curves, opacities, antenna position corrections, requantizer gains, TEC maps, and switched power plots
using the CASA task <b>gencal</b>.</p>

	<h2>Gain Curves</h2>
	
	%for single_result in result:
	    %if single_result.gc_result:
	        Gain curve table written to:
	        <p><b>${os.path.basename(single_result.gc_result.inputs['caltable'])}</b></p>
	    %else:
	        No gain curve table written
	    %endif
    %endfor


<%self:plot_group plot_dict="${opacity_plots}"
                  url_fn="${lambda ms: 'noop'}">

        <%def name="title()">
            Opacities
        </%def>

        <%def name="preamble()">
                Opacities written to:
                
                %for single_result in result:
                        <p><b>${os.path.basename(single_result.oc_result.inputs['caltable'])}</b></p>
                %endfor
                
                <p>
                </p>
        </%def>
        
        <%def name="mouseover(plot)">Summary window        </%def>

        <%def name="fancybox_caption(plot)">
          Opacities
        </%def>

        <%def name="caption_title(plot)">
           Opacities
        </%def>
</%self:plot_group>


% for ms in center_frequencies:
    <table class="table table-bordered table-striped table-condensed"
	   summary="Summary of gencal opacities">
	<caption>Summary of gencal opacities</caption>
        <thead>
	    <tr>
	        <th scope="col" rowspan="2">SPW</th>
	        <th scope="col" rowspan="2">Frequency [GHz]</th>
		<th scope="col" rowspan="2">Opacity [Nepers]</th>
	    </tr>
	</thead>
	<tbody>
	% for i in range(len(center_frequencies[ms])):
		<tr>
		        <td>${spw[ms][i]}</td>
			<td>${center_frequencies[ms][i]/1.e9}</td>
			<td>${opacities[ms][i]}</td>
		</tr>
	% endfor
	</tbody>
    </table>
% endfor

        
<h2>Antenna positions</h2>
        
        %for single_result in result:
            % if single_result.antcorrect == {}:
                <b>No antenna position corrections to apply.</b>
            % else:
                Antenna position corrections written to:
                <p><b>${os.path.basename(single_result.antpos_result.final[0].gaintable)}</b></p>
                <table class="table table-bordered table-striped table-condensed"
	                   summary="Summary of gencal opacities">
	               <caption>Antenna Position Corrections</caption>
                       <thead>
	               <tr>
	                    <th scope="col" rowspan="2">Antenna</th>
	                    <th scope="col" rowspan="2">x</th>
		            <th scope="col" rowspan="2">y</th>
		            <th scope="col" rowspan="2">z</th>
	               </tr>
	               </thead>
	               <tbody>
	               % for key, value in sorted(single_result.antcorrect.items()):
		           <tr>
		               <td>${key}</td>
			       <td>${value[0]}</td>
			       <td>${value[1]}</td>
			       <td>${value[2]}</td>
		           </tr>
	               % endfor
	              </tbody>
                      </table>
            % endif
        %endfor

    <h2>Requantizer gains</h2>
    %if single_result.rq_result:
	    Requantizer gains written to:
	
	    %for single_result in result:
	        <p><b>${os.path.basename(single_result.rq_result.inputs['caltable'])}</b></p>
        %endfor
    %else:
        No requantizer gain table written
    %endif


    %if single_result.tecmaps_result:

        %if single_result.tecmaps_result.tec_image and single_result.tecmaps_result.tec_rms_image and tec_plotfile:

            <h2>TEC Maps</h2>

            %for single_result in result:
                %if single_result.tecmaps_result.inputs['apply_tec_correction']:
                    TEC Caltable written to:
                    <p><b>${os.path.basename(single_result.tecmaps_result.inputs['caltable'])}</b></p>
                %endif
            %endfor
            <br>
            TEC Images written to:
            %for single_result in result:
                <p><b>${single_result.tecmaps_result.tec_image}</b></p>
                <p><b>${single_result.tecmaps_result.tec_rms_image}</b></p>
            %endfor

            %if single_result.tecmaps_result.inputs['show_tec_maps']:
                <img src="${tec_plotfile}">
            %endif
        %endif

    %endif

    %if swpowspgain_subpages:

        <h2>Switched Power plots</h2>
        Switched Power table written to:
        %for single_result in result:
	        <p><b>${os.path.basename(single_result.sw_result.inputs['caltable'])}</b></p>
        %endfor
        This table is NOT applied or added to the pipeline context callibrary.

        <h4>Per antenna plots:<br>
        <ul>
        %for band in swpowspgain_subpages.keys():
            %for ms in swpowspgain_subpages[band].keys():
                <li><b>${band}-band</b>:
                <a class="replace" href="${rendererutils.get_relative_url(pcontext.report_dir, dirname, swpowspgain_subpages[band][ms])}">SwPower SPgain plots</a> |
                <a class="replace" href="${rendererutils.get_relative_url(pcontext.report_dir, dirname, swpowtsys_subpages[band][ms])}">SwPower Tsys plots</a>
                </li>
            %endfor
        %endfor
        </ul>
        </h4>
    %endif