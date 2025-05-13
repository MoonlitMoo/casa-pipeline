<%!
rsc_path = ""
import os
import pipeline.infrastructure.renderer.htmlrenderer as hr
%>
<%inherit file="t2-4m_details-base.mako"/>

<%block name="title">Flux density bootstrapping and spectral index fitting</%block>

<p>Make a gain table that includes gain and opacity corrections for final amp cal and for flux density bootstrapping.</p>
<p>Fit the spectral index of calibrators with a power-law and put the fit in the model column.</p>

<%self:plot_group plot_dict="${summary_plots}"
                                  url_fn="${lambda ms:  'noop'}">

        <%def name="title()">
            Fluxboot summary plots
        </%def>

        <%def name="preamble()">


        </%def>
        
        
        <%def name="mouseover(plot)">Summary window </%def>
        
        
        
        <%def name="fancybox_caption(plot)">
          ${plot.parameters['figurecaption']}
        </%def>
        
        
        <%def name="caption_title(plot)">
           ${plot.parameters['figurecaption']}
        </%def>
</%self:plot_group>
    



% for ms in summary_plots:
    
<table class="table table-bordered table-striped table-condensed" summary="Spectral Indices">
	<caption>Table showing the flux density and spectral properties computed at each band center, based
on the global coefficients of the fit across all bands.
	 </caption>
        <thead>
	    <tr>
	        <th scope="col" rowspan="2">Source</th>
	        <th scope="col" rowspan="2">Fit Order</th>
	        <th scope="col" rowspan="2">Band</th>
	        <th scope="col" rowspan="2">Band Center [GHz]</th>
	        <th scope="col" rowspan="2">Flux density [Jy] (at Band Center)</th>
		    <th scope="col" rowspan="2">Spectral Index</th>
		    <th scope="col" rowspan="2">2nd order coeff</th>
		    <th scope="col" rowspan="2">3rd order coeff</th>
		    <th scope="col" rowspan="2">4th order coeff</th>


	    </tr>

	</thead>
	<tbody>
	% for tr in spixtable:
		<tr>
		% for td in tr:
			${td}
		% endfor
		</tr>
	%endfor
	</tbody>
    </table>

       <table class="table table-bordered table-striped table-condensed"
	   summary="Data, error, fit, and residuals">
	<caption>Data, error, fit, and residuals</caption>
        <thead>
	    <tr>
	        <th scope="col" rowspan="2">Source</th>
	        <th scope="col" rowspan="2">Frequency [GHz]</th>
	        <th scope="col" rowspan="2">Data</th>
		    <th scope="col" rowspan="2">Error</th>
		    <th scope="col" rowspan="2">Fitted Data</th>
		    <th scope="col" rowspan="2">Residual: Data-Fitted Data</th>
	    </tr>

	</thead>
	<tbody>   
  
     % for sourcekey in sorted(weblog_results[ms].keys()):
        <tr>
		    <td rowspan="${len(weblog_results[ms][sourcekey])}">${sourcekey}</td>
                % for row in sorted(weblog_results[ms][sourcekey], key=lambda p: float(p['freq'])):

		        <td>${row['freq']}</td>
			    <td>${'{0:.4f}'.format(float(row['data']))}</td>
			    <td>${'{0:.6f}'.format(float(row['error']))}</td>
			    <td>${'{0:.4f}'.format(float(row['fitteddata']))}</td>
			    <td>${'{0:.6f}'.format(float(row['data']) - float(row['fitteddata']))}</td>
		</tr>
                % endfor
    % endfor
	</tbody>
    </table>
    

%endfor