<%!
rsc_path = ""
import os
import pipeline.infrastructure.renderer.htmlrenderer as hr
from pipeline.infrastructure import casa_tools
%>
<%inherit file="t2-4m_details-base.mako"/>

<%block name="title">Image Pre-Check</%block>

%if result[0].error:
This task had an error!
%else:
<%
cqa = casa_tools.quanta
real_repr_target = result[0].real_repr_target
repr_source = result[0].repr_source
repr_spw = '%s' % (result[0].repr_spw)
reprBW_mode = '%s' % (result[0].reprBW_mode)
reprBW_nbin = '%d' % (result[0].reprBW_nbin)
repr_freq = '%.4f GHz' % (cqa.getvalue(cqa.convert(result[0].repr_target[1], 'GHz')))
repr_bw = '%.4g MHz' % (cqa.getvalue(cqa.convert(result[0].repr_target[2], 'MHz')))
sens_bw = '%.4g MHz' % (cqa.getvalue(cqa.convert(result[0].sensitivity_bandwidth, 'MHz')))
minAR_v = cqa.getvalue(cqa.convert(result[0].minAcceptableAngResolution, 'arcsec'))
maxAR_v = cqa.getvalue(cqa.convert(result[0].maxAcceptableAngResolution, 'arcsec'))
minAR = '%#.3g arcsec' % (minAR_v)
maxAR = '%#.3g arcsec' % (maxAR_v)
maxBR_v = cqa.getvalue(result[0].maxAllowedBeamAxialRatio)[0]

# SRDP user-specified min/max angular resolution
user_minAR, userMaxAR = None, None
if result[0].user_minAcceptableAngResolution:
    user_minAR_v = cqa.getvalue(cqa.convert(result[0].user_minAcceptableAngResolution, 'arcsec'))
    user_minAR = '%#.3g arcsec' % (user_minAR_v)

if result[0].user_maxAcceptableAngResolution:
    user_maxAR_v = cqa.getvalue(cqa.convert(result[0].user_maxAcceptableAngResolution, 'arcsec'))
    user_maxAR = '%#.3g arcsec' % (user_maxAR_v)

user_maxBR_v = cqa.getvalue(result[0].user_maxAllowedBeamAxialRatio)[0]
sensitivityGoal_v = cqa.getvalue(cqa.convert(result[0].sensitivityGoal, 'mJy'))
sensitivityGoal = '%#.3g mJy' % (sensitivityGoal_v)
robust = '%.1f' % (result[0].hm_robust)
uvtaper = '%s' % (result[0].hm_uvtaper)
single_continuum = result[0].single_continuum
%>
%endif
<p>
%if not result[0].error:
%if real_repr_target:
<h4>Goals From OT:</h4>
%else:
<h4>Assumed Values (Goal information not available):</h4>
%endif
Representative Target: ${repr_source}
%if not real_repr_target:
(First science target)
%endif
<br>
Representative Frequency: ${repr_freq} (SPW ${repr_spw})
%if not real_repr_target:
(Center of first science spw)
%endif
<br>
Bandwidth for Sensitivity: ${repr_bw}
%if real_repr_target:
    %if reprBW_mode=='nbin' or reprBW_mode=='repr_spw':
(rounded to nearest integer #channels (${reprBW_nbin}), repBW = ${sens_bw})
    %endif
%else:
(repBW=${repr_bw}, channel width of first science spw)
%endif
<br>
Min / Max Acceptable Resolution:
%if minAR_v==0.0 and maxAR_v==0.0:
    Not available
%else:
    ${minAR} / ${maxAR}
%endif

<br>
Maximum expected beam axial ratio (from OT):
%if maxBR_v==0.0:
    Not available
%else:
    ${maxBR_v}
%endif
<br>
Goal PI sensitivity:
%if real_repr_target:
    %if sensitivityGoal_v==0.0:
        Not available
    %else:
        ${sensitivityGoal}
    %endif
%else:
    Not available
%endif
<br>
Single Continuum:
%if real_repr_target:
    ${single_continuum}
%else:
    Not available
%endif

<!-- Only include the user-entered min and max angular resolution for SRDP -->
%if user_minAR and user_maxAR: 
 %if not(user_minAR_v==0.0 and user_maxAR_v==0.0):
    <p>
    <h4>Goals From User:</h4>
    Min / Max Acceptable Resolution:
        ${user_minAR} / ${user_maxAR}
    <p>
 %endif
%endif

<h4>Estimated Synthesized Beam and Sensitivities for the Representative
Target/Frequency</h4>

Estimates are given for four possible values of the tclean robust
weighting parameter: robust = 0.0, +0.5 (default), +1.0, and +2.0.
<b>If the "Min / Max Acceptable Resolution" is available
(&gt;=Cycle 5 12-m Array data)</b>,
the robust value closest to the default (+0.5) that predicts a beam
area (defined as simply major x minor) that is in the range of the
PI requested beam areas according to the table row for repBW
(Bandwidth for Sensitivity) is chosen. If none of these robust
values predict a beam area that is in range, robust=+2.0 is chosen
if the predicted beam area is too small, and robust=0.0 is chosen
if the predicted beam area is too large. The chosen robust value is
highlighted in green and used for all science target imaging. In
addition to an estimate for the repBW, an estimate for the aggregate
continuum bandwidth (aggBW) is also given assuming NO line contamination
but accounting for spw frequency overlap. If the Bandwidth for Sensitivity
(repBW) is &gt; the bandwidth of the spw containing the representative
frequency (repSPW), then the beam is predicted using all spws, otherwise
the beam is predicted for the repSPW alone. A message appears on the
"By Task" view if a non-default value of robust (i.e., not +0.5) is chosen.
Additionally, if the predicted beam is not within the PI requested range
using one of the four robust values, Warning messages appear on this page.
<br></br>
<b>These estimates should always be considered as the BEST CASE SCENARIO.</b>
These estimates account for Tsys, the observed uv-coverage, and prior
flagging. The estimates DO NOT account for (1) subsequent science target
flagging; (2) loss of continuum bandwidth due to the hif_findcont process
(i.e. removal of lines and other spectral features from the data used
to image the continuum); (3) Issues that affect the image quality like
(a) poor match of uv-coverage to image complexity; (b) dynamic range
effects; (c) calibration deficiencies (poor phase transfer, residual
baseline based effects, residual antenna position errors, etc.).
<em style="color:blue">
It is also important to note that both the repBW and aggBW beam
calculations are intrinsically multi-frequency synthesis continuum
calculations, using the relevant spws as described above. The synthesized
beam for a single channel in a cube will typically be larger and can be
significantly larger depending on the details of uv-coverage and channel
width.
</em>
<table class="table">
    <thead>
        <tr>
            <th>robust</th>
            <th>uvtaper</th>
            <th>Synthesized Beam</th>
            <th>Cell</th>
            <th>Beam Ratio</th>
            <th>Bandwidth</th>
            <th>BW Mode</th>
            <th>Effective Sensitivity</th>
        </tr>
    </thead>
    <tbody>
        %for tr in table_rows:
            %if tr.robust==result[0].hm_robust and tr.uvtaper==result[0].hm_uvtaper:
            <tr bgcolor="lightgreen">
            %else:
            <tr>
            %endif
            %for td in tr:
                <td>${td}</td>
            %endfor
            </tr>
        %endfor
    </tbody>
</table>
%endif
