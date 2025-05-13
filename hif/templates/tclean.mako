<%!
rsc_path = ""
import html
import os.path
import numpy as np
import pipeline.hif.tasks.tclean.renderer as clean_renderer
import pipeline.infrastructure.utils as utils
import pipeline.infrastructure.renderer.rendererutils as rendererutils
from pipeline.domain import DataType

columns = {
    'cleanmask' : 'Clean Mask',
    'flux' : 'Primary Beam',
    'image' : 'Image',
    'residual' : 'Residual',
    'model' : 'Final Model',
    'psf' : 'PSF'
}
%>
<%inherit file="t2-4m_details-base.mako"/>

<%block name="header" />

<%block name="title">
<%
try:
    long_description = '<br><small>{!s}'.format(result.metadata['long description'])
except:
    long_description = ''
%>MakeImages${long_description}</%block>

%if imaging_mode == 'ALMA':
    <p>In this stage, images with significant emission are cleaned to a threshold of
    2 x (predicted rms noise) x (dynamic range correction factor) using automasking.
    If a clean mask is not found automatically, then this threshold is doubled and
    the bulk of the whole image is used (PB&gt;0.3). The dynamic range correction factor
    (abbreviated as "DR correction") accounts for the fact that sources with a high
    dynamic range will typically exhibit larger imaging artifacts, resulting in a
    noise level greater than an equivalent blank field. The artifacts are worse for
    poorer UV coverage, so different dynamic range (DR) correction factors are
    adopted for different antenna configurations (12m Array vs. 7m Array multi-EB vs.
    7m Array single-EB), and for different targets (science targets vs. calibrators).
    See the Pipeline User's Guide for details.<br>
    The DR correction adopted is a function of the dirty dynamic range (abbreviated
    as "Dirty DR"), which is defined as the peak intensity divided by the theoretical
    rms sensitivity delivered by the visibilities.</p>
%elif imaging_mode == 'VLA':
    <p>In this stage, images with significant emission are cleaned to a threshold
    of 4 x (the RMS noise measured by tclean) using automasking.
    If a clean mask is not found automatically, then no further cleaning is performed.</p>
%endif

%if len(result[0].targets) != 0:
    %if len(image_info) != 0:
        %if image_info[0].intent == 'CHECK':
            <h2>Check Source Fit Results</h2>
                <table class="table table-striped">
                    <thead>
                        <tr>
                            <th>EB</th>
                            <th>Field</th>
                            <th>Virtual SPW</th>
                            <th>Bandwidth (GHz)</th>
                            <th>Position offset (mas)</th>
                            <th>Position offset (synth beam)</th>
                            <th>Fitted Flux Density (mJy)</th>
                            <th>Image S/N</th>
                            <th>Fitted [Peak Intensity / Flux Density] Ratio</th>
                            <th>gfluxscale mean visibility</th>
                            <th>gfluxscale S/N</th>
                            <th>[Fitted / gfluxscale] Flux Density Ratio</th>
                        </tr>
                    </thead>
                    <tbody>
                        %for row in chk_fit_info:
                            <tr>
                            %for td in row:
                                ${td}
                            %endfor
                            </tr>
                        %endfor
                    </tbody>
                </table>
                <h4>
                NOTE: The Position offset uncertainties only include the error in
                the fitted position; the uncertainty in the source catalog
                positions are not available. Additionally, the Peak Fitted
                Intensity, Fitted Flux Density, and gfluxscale Derived Flux may be
                low due to a number of factors other than decorrelation, including
                low S/N, and spatially resolved (non point-like) emission.
                </h4>
                <br>
        %endif
        %if have_polcal_fit:
            <h2>Polarization Calibrator Fit Results</h2>
                <table class="table table-striped">
                    <thead>
                        <tr>
                            <th>Session</th>
                            <th>EBs</th>
                            <th>Field</th>
                            <th>Virtual SPW</th>
                            <th>Polarization Fraction</th>
                            <th>Polarization Angle</th>
                            <th>Polarization Intensity Plot</th>
                            <th>Polarization Angle Plot</th>
                        </tr>
                    </thead>
                    <tbody>
                        %for row, plots in zip(pol_fit_info, pol_fit_plots):
                            <tr>
                            %for td in row:
                                ${td}
                            %endfor
                                <td>
                                    <%
                                    fullsize_relpath = os.path.relpath(plots.poli_abspath, pcontext.report_dir)
                                    thumbnail_relpath = os.path.relpath(plots.poli_thumbnail, pcontext.report_dir)
                                    %>
                                    <a href="${fullsize_relpath}"
                                       data-fancybox="clean-summary-images"
                                       data-tcleanCommandTarget="N/A"
                                       data-caption="POLI image"
                                       title='<div class="pull-left">POLI image'>
                                      <img class="lazyload"
                                           data-src="${thumbnail_relpath}"
                                           title="POLI image"
                                           alt="POLI image"
                                           class="img-thumbnail img-responsive">
                                    </a>
                                </td>
                                <td>
                                    <%
                                    fullsize_relpath = os.path.relpath(plots.pola_abspath, pcontext.report_dir)
                                    thumbnail_relpath = os.path.relpath(plots.pola_thumbnail, pcontext.report_dir)
                                    %>
                                    <a href="${fullsize_relpath}"
                                       data-fancybox="clean-summary-images"
                                       data-tcleanCommandTarget="N/A"
                                       data-caption="POLA image"
                                       title='<div class="pull-left">POLA image'>
                                      <img class="lazyload"
                                           data-src="${thumbnail_relpath}"
                                           title="POLA image"
                                           alt="POLA image"
                                           class="img-thumbnail img-responsive">
                                    </a>
                                </td>
                            </tr>
                        %endfor
                    </tbody>
                </table>
        %endif
    %endif
%endif

<h2>Image Details</h2>

%if len(result[0].targets) == 0:
    %if result[0].clean_list_info == {}:
        <p>There are no clean targets.
    %else:
        <p>${result[0].clean_list_info.get('msg', '')}
    %endif
%elif len(image_info) == 0:
    <h4 style="color:#990000">No image details found despite existing imaging targets. Please check for cleaning errors.</h4>
%else:
    <!--
    image_info contains the details of all imaging targets. It is a list sorted by
    field and spw. In PIPE-612 a restructuring of the weblog rendering was requested.
    This requires generating some index vectors before entering the main loop to
    create the table.
    -->

    <%
    field_block_indices = []
    field_vis = None
    for i, row in enumerate(image_info):
        if (row.field, row.vis) != field_vis:
            field_block_indices.append(i)
            field_vis = (row.field, row.vis)
    field_block_indices.append(len(image_info))
    max_num_columns = min(max(np.array(field_block_indices[1:])-np.array(field_block_indices[:-1])) + 1, 5)
    %>

    %if len(field_block_indices) > 2:
        <h3>
        Fields
        </h3>
        <ul>
            %for i in field_block_indices[:-1]:
                <li>
                <a href="#field_block_${i}">
                    ${image_info[i].field}
                    (${DataType.get_short_datatype_desc(image_info[i].datatype)})
                    %if image_info[i].result.is_per_eb:
                        (${image_info[i].vis})
                    %endif
                </a>
                </li>
            %endfor
        </ul>
    %endif

    <table class="table table-striped">
        <thead>
        <tr>
            <th>Field</th>
            <th>Spw</th>
            <th></th>
            <th></th>
            <th></th>
        </tr>
        </thead>
        <tbody>
        %for i in range(len(field_block_indices)-1):
            %if len(field_block_indices) > 2:
                <tr id="field_block_${field_block_indices[i]}" class="jumptarget" style="border-bottom:2px solid black"><td colspan="${max_num_columns}"></td></tr>
            %endif
            %for j in range(field_block_indices[i], field_block_indices[i+1], 4):
                <tr>
                    <td rowspan="2" style="width:150px;">
                        ${image_info[j].field}
                        (${DataType.get_short_datatype_desc(image_info[j].datatype)})
                    </td>
                    %for k in range(j, min(j+4, field_block_indices[i+1])):
                        <td style="width:250px;height:50px;">
                            <div style="word-wrap:break-word;overflow-y:scroll;width:250px;height:50px;">
                                ${'%s / %s' % (image_info[k].spw.replace(',',', '), image_info[k].spwnames.replace(',',', '))}
                            </div>
                        </td>
                    %endfor
                </tr>
                <tr>
                    %for k in range(j, min(j+4, field_block_indices[i+1])):
                        <td style="width:250px;">
                            %if image_info[k].plot is not None:
                                <%
                                fullsize_relpath = os.path.relpath(image_info[k].plot.abspath, pcontext.report_dir)
                                thumbnail_relpath = os.path.relpath(image_info[k].plot.thumbnail, pcontext.report_dir)
                                %>
                                <a href="${fullsize_relpath}"
                                   data-fancybox="clean-summary-images"
                                   data-tcleanCommandTarget="#tcleancmd-${hash(image_info[k].plot.abspath)}"
                                   data-caption="Iteration: ${image_info[k].plot.parameters['iter']}<br>Spw: ${image_info[k].plot.parameters['virtspw']}<br>Field: ${html.escape(image_info[k].field, True)}"
                                   title='<div class="pull-left">Iteration: ${image_info[k].plot.parameters['iter']}<br>
                                          Spw: ${image_info[k].plot.parameters["virtspw"]}<br>
                                          Field: ${html.escape(image_info[k].field, True)}</div><div class="pull-right"><a href="${fullsize_relpath}">Full Size</a></div>'>
                                  <img class="lazyload"
                                       data-src="${thumbnail_relpath}"
                                       title="Iteration ${image_info[k].plot.parameters['iter']}: image"
                                       alt="Iteration ${image_info[k].plot.parameters['iter']}: image"
                                       class="img-thumbnail img-responsive">
                                </a>
                                <div class="caption">
                                    <p>
                                        <a class="replace"
                                           href="${os.path.relpath(image_info[k].qa_url, pcontext.report_dir)}"
                                           role="button">
                                            View other QA images...
                                        </a>
                                    </p>
                                </div>
                                <div id="tcleancmd-${hash(image_info[k].plot.abspath)}" class="modal-content pipeline-tcleancommand" style="display:none;">
                                    <div class="modal-header">
                                        <button type="button" class="close" data-fancybox-close aria-label="Close">
                                            <span aria-hidden="true">&times;</span>
                                        </button>
                                        <h4 class="modal-title">Tclean Command</h4>
                                    </div>
                                    <div class="modal-body" data-selectable="true">
                                        <p>${rendererutils.get_command_markup(pcontext, image_info[k].tclean_command)}</p>
                                    </div>
                                    <div class="modal-footer">
                                        <button type="button" class="btn btn-default" data-fancybox-close>Close</button>
                                    </div>
                                </div>
                            %else:
                                No image available
                            %endif
                        </td>
                    %endfor
                </tr>
                <tr>
                    <th>data type</th>
                    %for k in range(j, min(j+4, field_block_indices[i+1])):
                        <td style="width:250px;">
                            <div style="word-wrap:break-word;width:250px;">
                                ${image_info[k].datatype_info}
                            </div>
                        </td>
                    %endfor
                </tr>
                <tr>
                    <th>${image_info[j].stokes_label}</th>
                    %for k in range(j, min(j+4, field_block_indices[i+1])):
                        <td style="width:250px;">${image_info[k].pol}</td>
                    %endfor
                </tr>
                <tr>
                    <th>${image_info[j].frequency_label}</th>
                    %for k in range(j, min(j+4, field_block_indices[i+1])):
                        <td style="width:250px;">${image_info[k].frequency}</td>
                    %endfor
                </tr>
                <tr>
                    <th>beam</th>
                    %for k in range(j, min(j+4, field_block_indices[i+1])):
                        <td style="width:250px;">${image_info[k].beam}</td>
                    %endfor
                </tr>
                <tr>
                    <th>beam p.a.</th>
                    %for k in range(j, min(j+4, field_block_indices[i+1])):
                        <td style="width:250px;">${image_info[k].beam_pa}</td>
                    %endfor
                </tr>
                <tr>
                    <th>final theoretical sensitivity</th>
                    %for k in range(j, min(j+4, field_block_indices[i+1])):
                        <td style="width:250px;">${image_info[k].sensitivity}</td>
                    %endfor
                </tr>
                %if image_info[k].cleaning_threshold_label is not None:
                <tr>
                    <th>
                        ${image_info[k].cleaning_threshold_label}
                    </th>
                    %for k in range(j, min(j+4, field_block_indices[i+1])):
                        <td style="width:250px;">
                            %if image_info[k].cube_all_cont:
                                <b>findCont=AllCont, no cleaning</b><br>
                            %endif
                            ${image_info[k].cleaning_threshold}
                        </td>
                    %endfor
                </tr>
                %endif
                % if row.nmajordone_total is not None:
                <tr>
                    <th>total number of major cycles done</th>
                    %for k in range(j, min(j+4, field_block_indices[i+1])):
                        <td>${image_info[k].nmajordone_total}</td>
                    %endfor
                </tr>
                % endif
                <tr>
                    <th>clean residual peak / scaled MAD</th>
                    %for k in range(j, min(j+4, field_block_indices[i+1])):
                        <td style="width:250px;">${image_info[k].residual_ratio}</td>
                    %endfor
                </tr>
                <tr>
                    <th>${image_info[k].non_pbcor_label}</th>
                    %for k in range(j, min(j+4, field_block_indices[i+1])):
                        <td style="width:250px;">${image_info[k].non_pbcor}</td>
                    %endfor
                </tr>
                <tr>
                    <th>pbcor image max / min</th>
                    %for k in range(j, min(j+4, field_block_indices[i+1])):
                        <td style="width:250px;">${image_info[k].pbcor}</td>
                    %endfor
                </tr>
                <tr>
                    <th>${image_info[k].fractional_bw_label}</th>
                    %for k in range(j, min(j+4, field_block_indices[i+1])):
                        <td style="width:250px;">${image_info[k].fractional_bw}</td>
                    %endfor
                </tr>
                %if image_info[k].aggregate_bw_label is not None:
                    <tr>
                        <th>${image_info[k].aggregate_bw_label}</th>
                        %for k in range(j, min(j+4, field_block_indices[i+1])):
                            <td style="width:250px;">${image_info[k].aggregate_bw}</td>
                        %endfor
                    </tr>
                %endif
                %if image_info[k].nsigma_label is not None:
                    <tr>
                        <th>${image_info[k].nsigma_label}</th>
                        %for k in range(j, min(j+4, field_block_indices[i+1])):
                            <td style="width:250px;">${image_info[k].nsigma}</td>
                        %endfor
                    </tr>
                %endif
                %if image_info[k].initial_nsigma_mad_label is not None:
                    <tr>
                        <th>${image_info[k].initial_nsigma_mad_label}</th>
                        %for k in range(j, min(j+4, field_block_indices[i+1])):
                            <td style="width:250px;">${image_info[k].initial_nsigma_mad}</td>
                        %endfor
                    </tr>
                %endif
                %if image_info[k].final_nsigma_mad_label is not None:
                    <tr>
                        <th>${image_info[k].final_nsigma_mad_label}</th>
                        %for k in range(j, min(j+4, field_block_indices[i+1])):
                            <td style="width:250px;">${image_info[k].final_nsigma_mad}</td>
                        %endfor
                    </tr>
                %endif
                %if image_info[k].vis_amp_ratio_label is not None:
                    <tr>
                        <th>${image_info[k].vis_amp_ratio_label}</th>
                        %for k in range(j, min(j+4, field_block_indices[i+1])):
                            <td style="width:250px;">${'{:.4}'.format(image_info[k].vis_amp_ratio)}</td>
                        %endfor
                    </tr>
                %endif                <tr>
                    <th>score</th>
                    %for k in range(j, min(j+4, field_block_indices[i+1])):
                        <td style="width:250px;">${image_info[k].score}</td>
                    %endfor
                </tr>
                <tr>
                    <th>image file</th>
                    %for k in range(j, min(j+4, field_block_indices[i+1])):
                        <td style="width:250px;">
                            <div style="word-wrap:break-word;width:250px;">
                                ${image_info[k].image_file}
                            </div>
                        </td>
                    %endfor
                </tr>
            %endfor
        %endfor
        </tbody>
    </table>
%endif
