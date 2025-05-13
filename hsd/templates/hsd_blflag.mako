<%!
rsc_path = "../"
import collections
import os
import pipeline.infrastructure.utils as utils

def get_fraction(flagged, total):
   if total == 0 or flagged < 0.0:
       return 'N/A' 
   else:
       return '%0.1f%%' % (100.0 * float(flagged) / float(total))

FlagDetailTR = collections.namedtuple("FlagDetailTR", "name spw ant pol nrow totnrow totfrac tsys before postbl prebl postrmean prermean postrms prerms link")
FlagDetailTRV = collections.namedtuple("FlagDetailTR", "name vspw spw ant pol nrow totnrow totfrac tsys before postbl prebl postrmean prermean postrms prerms link")
%>
<%inherit file="t2-4m_details-base.mako"/>

<%block name="header" />

<%block name="title">Flag data by Tsys and statistics of spectra</%block>

<!-- short description of what the task does -->
<p>This task flags spectra by several criteria:
<ol>
	<li> eliminate spectra with outlier RMS (Baseline RMS)</li>
	<li> eliminate rapid variation of spectra using deviation from the running mean (Running mean)</li>
	<li> eliminate spectra with remarkably large RMS than expected (Expected RMS)</li>
	<li> eliminate spectra with outlier Tsys value</li>
</ol>
For 1.-3., the RMSes of spectra before and after baseline fit are obtained using line free channels.
</p>

<h2>Contents</h2>
<ul>
<li><a href="#summarytablepereb">Flag Summary per EB</a></li>
<li><a href="#summarytableperfield">Flag Summary per Field and SpW</a></li>
</ul>


<H2 id="summarytablepereb" class="jumptarget">Flag Summary per EB</H2>
<table class="table table-bordered table-striped" summary="Flag Summary per EB">
	<caption>Summary of flagged solutions.<br>
            "Flags by Reason" states the amount of solutions flagged (in number of data rows) as a fraction of the specified data. 
            Pre-fit metrics are performed on calibrated spectra before the baseline-fit, 
            while the post-fit ones are performed on data after the baseline-fit.<br>
            "Flagged Fraction" indicates the amount of data flagged (in number of spectral channels). 
    </caption>
    <thead>
	    <tr>
	        <th scope="col" rowspan="3">Measurement Set</th>
	        <th scope="col" colspan="7">Flags by Reason</th>
	        <th scope="col" rowspan="2" colspan=3>Flagged Fraction</th>
		</tr>
		<tr>
	        <th scope="col" colspan="2">Baseline RMS</th>
	        <th scope="col" colspan="2">Running mean</th>
	        <th scope="col" colspan="2">Expected RMS</th>
	        <th scope="col" rowspan="2">Outlier Tsys</th>
	    </tr>
        <tr>
            <th scope="col">Post-fit</th>
            <th scope="col">Pre-fit</th>
            <th scope="col">Post-fit</th>
            <th scope="col">Pre-fit</th>
            <th scope="col">Post-fit</th>
            <th scope="col">Pre-fit</th>
            <th scope="col">Before</th>
            <th scope="col">Additional</th>
            <th scope="col">Total</th>
	</thead>
	<tbody>
	% for tr in per_eb_summary_table_rows:
        <%
            baseline_post  = os.path.join( dirname, subpages_per_type['Baseline RMS post-fit'] )
            baseline_pre   = os.path.join( dirname, subpages_per_type['Baseline RMS pre-fit'] )
            runmean_post   = os.path.join( dirname, subpages_per_type['Running mean post-fit'] )
            runmean_pre    = os.path.join( dirname, subpages_per_type['Running mean pre-fit'] )
            expectrms_post = os.path.join( dirname, subpages_per_type['Expected RMS post-fit'] )
            expectrms_pre  = os.path.join( dirname, subpages_per_type['Expected RMS pre-fit'] )
            outlyer_tsys   = os.path.join( dirname, subpages_per_type['Outlier Tsys'] )
        %>
		<tr>
		% for td in tr:
			${td} 
		% endfor
		</tr>
	% endfor
    <TR>
    <th scope="row">Flagging Details</th>
    <TD> <a href="${baseline_post}"  class="replace">Plots</a> </TD>
    <TD> <a href="${baseline_pre}"   class="replace">Plots</a> </TD>
    <TD> <a href="${runmean_post}"   class="replace">Plots</a> </TD>
    <TD> <a href="${runmean_pre}"    class="replace">Plots</a> </TD>
    <TD> <a href="${expectrms_post}" class="replace">Plots</a> </TD>
    <TD> <a href="${expectrms_pre}"  class="replace">Plots</a> </TD>
    <TD> <a href="${outlyer_tsys}"   class="replace">Plots</a> </TD>
    <TD colspan="3"> </TD>
    </TR>
	</tbody>
</table>



<H2 id="summarytableperfield" class="jumptarget">Flag Summary per Field and SpW</H2>
<table class="table table-bordered table-striped" summary="Flag Summary per Field and SpW">
	<caption>Flag summary of ON-source target scans per source and SpW <(in number of spectral channels).</caption>
    <thead>
	    <tr>
	        <th scope="col" rowspan="2">Field</th>
	        %if dovirtual:
	        <th scope="col" rowspan="2">Virtual SpW</th>
	        %else:
	        <th scope="col" rowspan="2">SpW</th>
	        %endif
	        <th scope="col" colspan="3">Flagged Fraction</th>
		</tr>
		<tr>
	        <th scope="col">Before</th>
	        <th scope="col">Additional</th>
	        <th scope="col">Total</th>
	    </tr>
	</thead>
	<tbody>
	% for tr in per_field_summary_table_rows:
		<tr>
		% for td in tr:
			${td}
		% endfor
		</tr>
	%endfor
	</tbody>
</table>


