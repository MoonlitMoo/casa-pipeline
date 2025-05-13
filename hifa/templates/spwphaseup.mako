<%!
rsc_path = ""
import os

def format_spwmap(spwmap, scispws):
    if not spwmap:
        return ''
    else:
        spwmap_strings=[]
        for ind, spwid in enumerate(spwmap):
        	if ind in scispws:
        		spwmap_strings.append("<strong>{0}</strong>".format(spwid))
        	else:
        		spwmap_strings.append(str(spwid))
        
        return ', '.join(spwmap_strings)

def format_field(field_name, field_id):
    if not field_name:
        return ''
    else:
        return '{} (#{})'.format(field_name, field_id)
%>
<%inherit file="t2-4m_details-base.mako"/>

<%block name="title">Compute Spw Phaseup Map and Offsets</%block>

<h2>Contents</h2>

<ul>
    <li><a href="#results">Results</a></li>
    <li><a href="#structure">Phase RMS structure plots</a></li>
</ul>

<p>This task computes the spectral window map that will be used to apply the time gaincal phase solutions
and the caltable containing per spw phase offsets.</p>

<h2 id="results">Results</h2>

<table class="table table-bordered table-striped" summary="Narrow to wide spw mapping results">
	<caption>Phase solution spw map per measurement set. If a measurement set
        is listed with no further information, this indicates that there were
        no valid PHASE or CHECK fields for which to derive a SpW mapping (e.g.
        because those fields also covered other calibrator intents).</caption>
    <thead>
	    <tr>
	        <th>Measurement Set</th>
            <th>Field</th>
            <th>Intent</th>
            <th>Scan IDs</th>
            <th>Combine</th>
	        <th>Spectral Window Map</th>
	    </tr>
	</thead>
	<tbody>
    % for spwmap in spwmaps:
		<tr>
			<td>${spwmap.ms}</td>
            <td>${format_field(spwmap.field, spwmap.fieldid)}</td>
            <td>${spwmap.intent}</td>
            <td>${spwmap.scanids}</td>
            <td>${spwmap.combine}</td>
            <td>${format_spwmap(spwmap.spwmap, spwmap.scispws)}</td>
		</tr>
    % endfor
	</tbody>
</table>

% if snr_table_rows:
<table class="table table-bordered table-striped" summary="Estimated phase signal to noise ratios">
	<caption>Estimated phase calibrator signal to noise ratios per measurement
        set. For spectral windows where the estimated SNR is below the
        specified threshold ('phasesnr' parameter), the SNR value is indicated
        in <strong>bold</strong>. If a measurement set is listed with no
        further information, this indicates that there were no valid PHASE or
        CHECK fields for which to derive a SpW mapping (e.g. because those
        fields also covered other calibrator intents).</caption>
    <thead>
	    <tr>
	        <th>Measurement Set</th>
            <th>Phase SNR threshold</th>
            <th>Field</th>
            <th>Intent</th>
            <th>Spectral Window</th>
            <th>Estimated SNR</th>
        </tr>
	</thead>
	<tbody>
    % for tr in snr_table_rows:
        <tr>
        % for td in tr:
            ${td}
        % endfor
        </tr>
    %endfor
	</tbody>
</table>
% else:
    <p>No information available on estimated phase signal to noise ratios.
% endif

% if pcal_table_rows:
<table class="table table-bordered table-striped" summary="Phase calibrator mapping to target/check">
    <caption>Mapping of phase calibrator fields to TARGET / CHECK fields.</caption>
    <thead>
        <tr>
            <th>Measurement Set</th>
            <th>Phase Field</th>
            <th>TARGET/CHECK Fields</th>
        </tr>
    </thead>
    <tbody>
    % for tr in pcal_table_rows:
        <tr>
        % for td in tr:
            ${td}
        % endfor
        </tr>
    %endfor
    </tbody>
</table>
% else:
<p>No information available on mapping of phase calibrators to TARGET / CHECK fields.
% endif

<table class="table table-bordered" summary="Application Results">
        <caption>Applied calibrations and parameters used for caltable generation</caption>
    <thead>
        <tr>
            <th scope="col" rowspan="2">Measurement Set</th>
            <th scope="col" colspan="2">Solution Parameters</th>
            <th scope="col" colspan="2">Applied To</th>
            <th scope="col" rowspan="2">Calibration Table</th>
        </tr>
        <tr>
            <th>Type</th>
            <th>Interval</th>
            <th>Scan Intent</th>
            <th>Spectral Windows</th>
        </tr>
    </thead>
    <tbody>
    % for application in applications:
        <tr>
            <td>${application.ms}</td>
            <td>${application.solint}</td>
            <td>${application.calmode}</td>
            <td>${application.intent}</td>
            <td>${application.spw}</td>
            <td>${application.gaintable}</td>
        </tr>
    % endfor
    </tbody>
</table>

<h2 id="structure">Phase RMS structure plots</h2>
% if rmsplots: 
    <p>
    The pipeline uses the bandpass phase solutions to create structure functions plots, baseline length versus phase RMS. 
    The measure of the phase RMS over a time interval equal to the phase referencing cycle-time is useful as a proxy for the 
    expected residual phase RMS of a target source after phase referencing. The action of phase referencing itself is to correct 
    phase fluctuations, caused by the atmosphere, on timescales longer than the cycle-time. 
    
    The median phase RMS reported in the table is for the longest baselines of the array above the &gt;80th percentile projected baseline (labeled "P80"). 
    Previously flagged baseline(s)/antenna(s) are excluded and listed as 'flagged' in the log, while any antenna with noticeably elevated values as identified 
    in this stage will be listed in the table as "Noisier antennas" and marked as 'outlier' in the log.
    
    For excellent stability conditions, phase RMS (	&lt; 30 deg), 
    the target images will have minimal decoherence. For stable conditions, phase RMS (30-50 deg), the target image can have slight 
    decoherence which could be improved by self-calibration. When exceeding the phase RMS considered as stable conditions (50-70 deg), 
    target images can suffer from significant decoherence up to 50%. Self-calibration can help improve the final products. In conditions 
    exceeding the poor stability threshold, phase RMS (&gt; 70 deg), target images are expected to be poor, suffer from extreme levels of 
    decoherence and possibly have structure defects. Only self-calibration of known strong targets could recover these data. 
    Self-calibration on bright enough targets may be able to mitigate the degradation caused by phase instability.
    </p>
    <p>
    If the cycle time is longer than the total length of the bandpass scan, the phase RMS is only reported for the total time.
    </p>

    <table class="table table-bordered table-striped" summary="Phase RMS structure results">
        <thead>
            <tr>
                <th scope="col">Measurement Set</th>
                <th scope="col">Type</th>
                <th scope="col">Time</th>
                <th scope="col">Median Phase RMS (deg)</th>
                <th scope="col">Noisier antennas</th>
            </tr>
        </thead>
        <tbody>
        % for tr in phaserms_table_rows:
            <tr>
            % for td in tr:
                ${td}
            % endfor
            </tr>
        %endfor
        </tbody>
    </table>

    <%self:plot_group plot_dict="${rmsplots}"
                    url_fn="${lambda ms: 'noop'}">
            <%def name="title()">
            </%def>

            <%def name="preamble()">
            </%def>

            <%def name="mouseover(plot)">${plot.basename}</%def>

            <%def name="fancybox_caption(plot)">
                ${plot.parameters['desc']}
            </%def>

            <%def name="caption_title(plot)">
                ${plot.parameters['desc']}
            </%def>
    </%self:plot_group>
%else: 
    <p>Decoherence Phase RMS Structure function assessment could not be made.</p>
%endif 