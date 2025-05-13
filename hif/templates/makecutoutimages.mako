<%!
rsc_path = ""
import os
import pipeline.infrastructure.renderer.htmlrenderer as hr
%>
<%inherit file="t2-4m_details-base.mako"/>

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
</script>

<p>Make cutouts of requested imaging products.</p>

<%
    image_min = plotter.result.image_stats.get('min')[0]
    image_max = plotter.result.image_stats.get('max')[0]
    image_sigma = plotter.result.image_stats.get('sigma')[0]
    image_madRMS = plotter.result.image_stats.get('medabsdevmed')[0] * 1.4826  # see CAS-9631
    image_unit = 'Jy/beam'

    pbcor_min = plotter.result.pbcor_stats.get('min')[0]
    pbcor_max = plotter.result.pbcor_stats.get('max')[0]
    pbcor_sigma = plotter.result.pbcor_stats.get('sigma')[0]
    pbcor_madRMS = plotter.result.pbcor_stats.get('medabsdevmed')[0] * 1.4826  # see CAS-9631
    pbcor_unit = 'Jy/beam'

    pbcor_residual_min = plotter.result.pbcor_residual_stats.get('min')[0]
    pbcor_residual_max = plotter.result.pbcor_residual_stats.get('max')[0]
    pbcor_residual_sigma = plotter.result.pbcor_residual_stats.get('sigma')[0]
    pbcor_residual_madRMS = plotter.result.pbcor_residual_stats.get('medabsdevmed')[0] * 1.4826  # see CAS-9631
    pbcor_residual_unit = 'Jy/beam'

    image_residual_min = plotter.result.residual_stats.get('min')[0]
    image_residual_max = plotter.result.residual_stats.get('max')[0]
    image_residual_sigma = plotter.result.residual_stats.get('sigma')[0]
    image_residual_madRMS = plotter.result.residual_stats.get('medabsdevmed')[0] * 1.4826  # see CAS-9631
    image_residual_unit = 'Jy/beam'

    rms_min = plotter.result.rms_stats.get('min')[0]
    rms_max = plotter.result.rms_stats.get('max')[0]
    rms_mean = plotter.result.rms_stats.get('mean')[0]
    rms_median = plotter.result.rms_stats.get('median')[0]
    rms_sigma = plotter.result.rms_stats.get('sigma')[0]
    rms_madRMS = plotter.result.rms_stats.get('medabsdevmed')[0] * 1.4826  # see CAS-9631 
    rms_unit = 'Jy/beam'

    pb_min = plotter.result.pb_stats.get('min')[0]
    pb_max = plotter.result.pb_stats.get('max')[0]
    pb_mean = plotter.result.pb_stats.get('mean')[0]
    pb_median = plotter.result.pb_stats.get('median')[0]

    x_px = image_size.get('pixels_x')
    y_px = image_size.get('pixels_y')
    x_arcsec = image_size.get('arcsec_x')
    y_arcsec = image_size.get('arcsec_y')
%>

<table style="float: left; margin:0 10px; width: auto;" class="table table-condensed table-bordered table-striped">
  <tr style="font-weight:bold; background-color:#ccffff">
    <td></td>
    <td>pbcor restored</td>
    <td>pbcor residual</td>
  </tr>
  <tr>
    <td style="font-weight:bold; background-color:#ccffff">max</td>
    <td>${'{:.4e}'.format(pbcor_max)} ${pbcor_unit}</td>
    <td>${'{:.4e}'.format(pbcor_residual_max)} ${pbcor_residual_unit}</td>
  </tr>
  <tr>
    <td style="font-weight:bold; background-color:#ccffff">min</td>
    <td>${'{:.4e}'.format(pbcor_min)} ${pbcor_unit}</td>
    <td>${'{:.4e}'.format(pbcor_residual_min)} ${pbcor_residual_unit}</td>
  </tr>
  <tr>
    <td style="font-weight:bold; background-color:#ccffff">sigma</td>
    <td>${'{:.4e}'.format(pbcor_sigma)} ${pbcor_unit}</td>
    <td>${'{:.4e}'.format(pbcor_residual_sigma)} ${pbcor_residual_unit}</td>
  </tr>
  <tr>
    <td style="font-weight:bold; background-color:#ccffff">MADrms</td>
    <td>${'{:.4e}'.format(pbcor_madRMS)} ${pbcor_unit}</td>
    <td>${'{:.4e}'.format(pbcor_residual_madRMS)} ${pbcor_residual_unit}</td>
  </tr>
  <tr>
    <td style="font-weight:bold; background-color:#ccffff">max/MADrms</td>
    <td>${'{:.4f}'.format(pbcor_max / pbcor_madRMS)}</td>
    <td>${'{:.4f}'.format(pbcor_residual_max / pbcor_residual_madRMS)}</td>
  </tr>
  <tr>
    <td style="font-weight:bold; background-color:#ccffff">max/sigma</td>
    <td>${'{:.4f}'.format(pbcor_max / pbcor_sigma)}</td>
    <td>${'{:.4f}'.format(pbcor_residual_max / pbcor_residual_sigma)}</td>
  </tr>
</table>

<table style="float: left; margin:0 10px; width: auto;" class="table table-condensed table-bordered table-striped">
  <tr style="font-weight:bold;background-color:#ffd9b3">
    <td></td>
    <td>non-pbcor restored</td>
    <td>non-pbcor residual</td>
  </tr>
  <tr>
    <td style="font-weight:bold; background-color:#ffd9b3">max</td>
    <td>${'{:.4e}'.format(image_max)} ${image_unit}</td>
    <td>${'{:.4e}'.format(image_residual_max)} ${image_residual_unit}</td>
  </tr>
  <tr>
    <td style="font-weight:bold; background-color:#ffd9b3">min</td>
    <td>${'{:.4e}'.format(image_min)} ${image_unit}</td>
    <td>${'{:.4e}'.format(image_residual_min)} ${image_residual_unit}</td>
  </tr>
  <tr>
    <td style="font-weight:bold; background-color:#ffd9b3">sigma</td>
    <td>${'{:.4e}'.format(image_sigma)} ${image_unit}</td>
    <td>${'{:.4e}'.format(image_residual_sigma)} ${image_residual_unit}</td>
  </tr>
  <tr>
    <td style="font-weight:bold; background-color:#ffd9b3">MADrms</td>
    <td>${'{:.4e}'.format(image_madRMS)} ${image_unit}</td>
    <td>${'{:.4e}'.format(image_residual_madRMS)} ${image_residual_unit}</td>
  </tr>
  <tr>
    <td style="font-weight:bold; background-color:#ffd9b3">max/MADrms</td>
    <td>${'{:.4f}'.format(image_max / image_madRMS)}</td>
    <td>${'{:.4f}'.format(image_residual_max / image_residual_madRMS)}</td>
  </tr>
  <tr>
    <td style="font-weight:bold; background-color:#ffd9b3">max/sigma</td>
    <td>${'{:.4f}'.format(image_max / image_sigma)}</td>
    <td>${'{:.4f}'.format(image_residual_max / image_residual_sigma)}</td>
  </tr>
</table>

<table style="float: left; margin:0 10px; width: auto;" class="table table-condensed table-bordered table-striped">
  <tr style="font-weight:bold; background-color:#ffff99">
    <td></td>
    <td>RMS</td>
  </tr>
  <tr>
    <td style="font-weight:bold; background-color:#ffff99">max</td>
    <td>${'{:.4e}'.format(rms_max)} ${rms_unit}</td>
  </tr>
  <tr>
    <td style="font-weight:bold; background-color:#ffff99">min</td>
    <td>${'{:.4e}'.format(rms_min)} ${rms_unit}</td>
  </tr>
  <tr>
    <td style="font-weight:bold; background-color:#ffff99">mean</td>
    <td>${'{:.4e}'.format(rms_mean)} ${rms_unit}</td>
  </tr>
  <tr>
    <td style="font-weight:bold; background-color:#ffff99">median</td>
    <td>${'{:.4e}'.format(rms_median)} ${rms_unit}</td>
  </tr>
  <tr>
    <td style="font-weight:bold; background-color:#ffff99">sigma</td>
    <td>${'{:.4e}'.format(rms_sigma)} ${rms_unit}</td>
  </tr>
  <tr>
    <td style="font-weight:bold; background-color:#ffff99">MADrms</td>
    <td>${'{:.4e}'.format(rms_madRMS)} ${rms_unit}</td>
  </tr>
</table>

<table style="float: left; margin:0 10px; width: auto;" class="table table-condensed table-bordered table-striped">
  <tr style="font-weight:bold; background-color:#ffcccc">
    <td></td>
    <td>primary beam</td>
  </tr>
  <tr>
    <td style="font-weight:bold; background-color:#ffcccc">max</td>
    <td>${'{:.4e}'.format(pb_max)}</td>
  </tr>
  <tr>
    <td style="font-weight:bold; background-color:#ffcccc">min</td>
    <td>${'{:.4e}'.format(pb_min)}</td>
  </tr>
  <tr>
    <td style="font-weight:bold; background-color:#ffcccc">mean</td>
    <td>${'{:.4e}'.format(pb_mean)}</td>
  </tr>
  <tr>
    <td style="font-weight:bold; background-color:#ffcccc">median</td>
    <td>${'{:.4e}'.format(pb_median)}</td>
  </tr>
</table>

<table style="margin:0 10px; width: auto;" class="table table-condensed table-bordered table-striped">
    <tr>
        <td style="font-weight:bold; background-color:#ccffcc">Fraction of pixels with <= 120 &mu;Jy RMS</td>
        <td>${'%4.2f &#37;' % (plotter.result.RMSfraction120)}</td>
    </tr>
    <tr>
        <td style="font-weight:bold; background-color:#ccffcc">Fraction of pixels with <= 168 &mu;Jy RMS</td>
        <td>${'%4.2f &#37;' % (plotter.result.RMSfraction168)}</td>
    </tr>
    <tr>
        <td style="font-weight:bold; background-color:#ccffcc">Fraction of pixels with <= 200 &mu;Jy RMS</td>
        <td>${'%4.2f &#37;' % (plotter.result.RMSfraction200)}</td>
    </tr>
    <tr>
        <td style="font-weight:bold; background-color:#ccffcc">Image size (x, y)</td>
        <td>${'{:.0f}px, {:.0f}px'.format(x_px, y_px)}</td>
    </tr>
    <tr>
        <td style="font-weight:bold; background-color:#ccffcc">Image size (RA, DEC)</td>
        <td>${'{:.2f}", {:.2f}"'.format(x_arcsec, y_arcsec)}</td>
    </tr>
    <tr>
        <td style="font-weight:bold; background-color:#ccffcc">Masked pixel count</td>
        <td>${'{}'.format(plotter.result.n_masked)}</td>
    </tr>
    <tr>
        <td style="font-weight:bold; background-color:#ccffcc">Fraction masked</td>
        <td>${'{:.2f} %'.format(plotter.result.pct_masked)}</td>
    </tr>
</table>

<div style="clear:both;"></div>

<%self:plot_group plot_dict="${subplots}" url_fn="${lambda ms:  'noop'}" sort_row_by="isalpha">

        <%def name="title()">
            Cutout images
        </%def>

        <%def name="preamble()"></%def>

        <%def name="mouseover(plot)">${plot.basename}</%def>

        <%def name="fancybox_caption(plot)">
          ${plot.basename}
        </%def>

        <%def name="caption_title(plot)">
           ${plot.basename}
        </%def>
</%self:plot_group>
