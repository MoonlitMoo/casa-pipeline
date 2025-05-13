<%!
rsc_path = ""
import os
import html
import pipeline.infrastructure.renderer.htmlrenderer as hr
%>
<%inherit file="t2-4m_details-base.mako"/>

<%block name="title">Renormalization</%block>

<h2>Contents</h2>
<ul>
<li><a href="#table">Table</a></li>
<li><a href="#plots">Plots</a></li>
</ul>

<p>ALMA cross-correlations are divided by the auto-correlation as a function of frequency, in the correlator. This has a variety of advantages for operations and calibration, but if there is strong line emission detected in the autocorrelation (i.e. as would be detected in a single dish spectrum), that emission can anomalously decrease the cross-correlation amplitude at those frequencies.</p>
<p>This effect can be mitigated by comparing the autocorrelation spectrum (AC) of the target with the AC of the bandpass, which is generally located away from any such bright contaminating line emission. The ratio of the bandpass AC to the target AC provides a scaling factor as a function of frequency that can be used as a first order correction spectrum. However, atmospheric and instrumental variation (e.g. baseline ripple) need to be fitted and removed, so the spectrum is divided into several segments (marked on the plots as thin dotted vertical lines) for that fitting. The fitted AC ratio is presented here as the 'renorm scale factor' or 'renorm amplitude'.</p>
<p>All targets, spws, and measurement sets with maximum scaling above the observatory determined threshold are colored in the table.
<p>Informative plots are included on this page and collected in a pdf for each spw and source, linked from the table below.</p>
<p>The plot shown on this page is a ReNormSpectra summary plot showing the average scaling spectrum over all scans, and for mosaics, all fields in the mosaic with peak scaling above the threshold. All antennas are plotted as dashed red and blue (for X and Y), and the mean is plotted solid. Vertical grey shaded regions indicate areas of the spectrum that may be affected by atmospheric features.
<p>The pdf next contains RenormDiagnosicCheck plots corresponding to each field and scan. The scaling spectrum is plotted as solid lines for each antenna (again red and blue for XX and YY), and the median as a dashed line (green and black for XX and YY).</p>
<p>The renormalization script has heuristics to detect and correct spikes, dips, and jumps near the segment boundaries (marked with thin vertical dotted lines). Less significant (below the threshold for applying the correction) features may remain.</p>

<p>Features in the scaling spectrum associated with atmospheric features require additional care - ALMA data reduction staff will have evaluated these and minimized them insofar as possible with current heuristics, but PIs should take note of the shape and magnitude of any applied correction when performing line science at frequencies overlapping atmospheric lines.</p>

<hr/>

<h2 id="table" class="jumptarget">Table</h2>

<p><b>MS/Source/SPW that trigger the need for renormalization above a threshold of ${result[0].threshold} highlighted in ${table_color_text}</b></p>
<p><em>Please refer to the Pipeline User's Guide (linked to this weblog's Home page) for more details on renormalization and interpretation of the plots.</em></p>
<table class="table table-bordered" summary="Renormalization results">
    <thead>
        <tr>
            <th>MS Name</th>
            <th>Source Name</th>
            <th>SPW</th>
            <th>Max Renorm Scale Factor (field id)</th>
            <th>PDF Link to Diagnostic Plots</th>
	    </tr>
	</thead>
	<tbody>
    % if not table_rows:
      <tr>
          <td colspan="5">No Corrections</td>
      </tr>
    % else:
        % for tr in table_rows:
        <tr>
            % for td in tr:
                ${td}
            % endfor
        </tr>
        %endfor
    % endif
	</tbody>
</table>

<h2 id="plots" class="jumptarget">Plots</h2>

<%self:plot_group plot_dict="${summary_plots}" 
                  url_fn="${lambda ms: 'noop'}"
                  data_spw="${True}"
                  data_vis="${True}"
                  data_field="${True}"
                  break_rows_by="field"
                  sort_row_by="field,spw"
                  separate_rows_by='thick-line'
                  show_row_break_value="${True}">


        <%def name="title()">
        </%def>

        <%def name="preamble()">
        </%def>

        <%def name="mouseover(plot)">${plot.basename}</%def>

       	<%def name="fancybox_caption(plot)">
    		Spw: ${plot.parameters['spw']}<br>
	    	Target: ${html.escape(plot.parameters['field'], True)}
    	</%def>

    	<%def name="caption_title(plot)">
	    	Spectral Window ${plot.parameters['spw']}<br>
	    </%def>
	
        <%def name="caption_text(plot, _)">
		    ${plot.parameters['caption']}<br>
	    </%def>

</%self:plot_group>