<%!
rsc_path = ""
import os
from pipeline.domain import DataType

columns = {'cleanmask' : ('Clean Mask', 'Clean Mask'),
           'flux' : ('Primary Beam', 'Primary Beam'),
           'pbcorimage' : ('Pb-corrected Image', 'Pb-corrected Image'),
           'image' : ('Image', 'Image'),
           'residual' : ('Residual', 'Residual'),
           'model' : ('Final Model', 'Final Model'),
           'psf' : ('PSF', 'PSF'),
           'mom0_fc': ('Line-free Moment', 'Integ. int. (mom0) of line-free channels'),
           'mom8_fc': ('Line-free Moment', 'Maximum int. (mom8) of line-free channels'),
           'spectra': ('Spectra / Mask', 'Spectra from flattened clean mask'),
           'psf_per_channel': ('Beam per channel', 'Beam per channel')}

def get_plot(plots, prefix, datatype, field, spw, stokes, i, colname, moment=None):
    try:
        if moment is not None:
            return plots[prefix][datatype][field][spw][stokes][i][colname][moment]
        else:
            for m in ('mean', 'mom8', 'mom0', 'N/A'):
                if m in plots[prefix][datatype][field][spw][stokes][i][colname]:
                    return plots[prefix][datatype][field][spw][stokes][i][colname][m]
            return None
    except KeyError:
        return None
%>
<script>
    pipeline.pages.tclean_plots.ready();
</script>


<div class="page-header">
    <h2>Clean results for ${field} (${DataType.get_short_datatype_desc(datatype)}) SpW ${spw}
        <div class="btn-toolbar pull-right" role="toolbar">
            % if qa_previous or qa_next:
            <div class="btn-group" role="group">
                % if qa_previous:
                    <button type="button" class="btn btn-default replace" data-href="${os.path.relpath(qa_previous, pcontext.report_dir)}"><span class="glyphicon glyphicon-step-backward"></span></button>
                % else:
                    <button type="button" class="btn btn-default disabled"><span class="glyphicon glyphicon-step-backward"></button>
                % endif
                % if qa_next:
                    <button type="button" class="btn btn-default replace" data-href="${os.path.relpath(qa_next, pcontext.report_dir)}"><span class="glyphicon glyphicon-step-forward"></span></button>
                % else:
                    <button type="button" class="btn btn-default disabled"><span class="glyphicon glyphicon-step-forward"></span></button>
                % endif
            </div>
            % endif
            <div class="btn-group" role="group">
                <button class="btn btn-default replace" data-href="${os.path.relpath(base_url, pcontext.report_dir)}">Back</button>
            </div>
        </div>
    </h2>
</div>

<div class="row">
<table class="table table-striped">
    <thead>
        <tr>
            <th>Iterations</th>
            % for colname, moment in colorders[0]:
                <th>${columns[colname][0]}s</th>
            % endfor
        </tr>
    </thead>
    <tbody>

        % for i in sorted(plots_dict[prefix][datatype][field][spw][pol].keys())[::-1]:
            % for colorder in colorders:
                <tr>
                    <!-- iteration row heading -->
                    <td class="vertical-align"><p class="text-center">${i}
                    %if i==0 and cube_all_cont:
                        <br>findCont=AllCont<br>no cleaning
                    %endif
                    </p></td>
                    <!-- plots for this iteration, in column order -->
                % for colname, moment in colorder:
                <td>
                    <% plot = get_plot(plots_dict, prefix, datatype, field, spw, pol, i, colname, moment) %>
                    <!-- use bootstrap markup for thumbnails -->
                    % if plot is not None:
                    <div class="thumbnail">
                        <a href="${os.path.relpath(plot.abspath, pcontext.report_dir)}"
                           title="Iteration ${i}: ${columns[colname][1]}"
                           data-caption="${columns[colname][1]}<br>Iteration ${i}"
                           data-fancybox="iteration-${colname}">
                            <img data-src="${os.path.relpath(plot.thumbnail, pcontext.report_dir)}"
                                 title="Iteration ${i}: ${columns[colname][1]}"
                                 alt="Iteration ${i}: ${columns[colname][1]}"
                                 class="lazyload img-responsive">
                        </a>
                        % if cube_mode:
                        <div class="caption">
                            <% moment_label = '' if moment is None else f' ({moment})' %>
                            <p class="text-center">${columns[colname][1]}${moment_label}</p>
                        </div>
                        % endif
                    </div>
                    % endif
                </td>
                % endfor <!-- /colname,moment loop-->
            % endfor <!-- /colorders loop-->
            </tr>
        % endfor <!-- /iteration loop -->

        <tr>
            <td></td>
            <%
            if cube_mode:
                colnames = ['flux', 'psf', 'model', 'psf_per_channel']
            else:
                colnames = ['flux', 'psf', 'model']
            %>

            % for colname in colnames:
                <td>
                    <!-- model/psf/flux plots are associated with the final iteration -->
                    <% 
                    lastiter = sorted(plots_dict[prefix][datatype][field][spw][pol].keys())[-1]
                    plot = get_plot(plots_dict, prefix, datatype, field, spw, pol, lastiter, colname)
                    %>
                    % if plot is not None:
                        <div class="thumbnail">
                            <a data-fancybox
                               href="${os.path.relpath(plot.abspath, pcontext.report_dir)}"
                               data-caption="${columns[colname][1]}"
                               title="${columns[colname][1]}">
                                <img data-src="${os.path.relpath(plot.thumbnail, pcontext.report_dir)}"
                                     title="${columns[colname][1]}"
                                     alt="${columns[colname][1]}"]
                                     class="lazyload img-responsive">
                            </a>
                            <div class="caption">
                                <p class="text-center">${columns[colname][1]}</p>
                            </div>
                        </div>
                    % endif
                </td>
            % endfor <!-- /colname loop -->
        </tr>
    </tbody>
</table>
</div>

<ul>
<li>
The inset in the PSF image (when present) corresponds to the central 41 pixels of the PSF. 
When the beam shape is significantly non-Gaussian, the dotted contour of the 50% level 
of the PSF image will become distinctly visible apart from the fitted synthesized beam, 
which is shown as the solid contour.
</li>
%if cube_mode:
<li>
The Line-free Moment 0 and Moment 8 images are created from the line-free
(continuum) channels identified in the hif_findcont stage. In the absence of
line contamination these moment images (known as the mom0fc and mom8fc images)
will be noise like. A QA score is generated (in part) from the "mom8fc Peak SNR"
defined as [ mom8fc['max'] - median ] / noise from the continuum channels of
the cube, its value is printed on the mom8fc thumbnail.
</li>
<li>
The Spectral plot shows the integrated spectrum of the cube, if there is a
clean mask the spectrum is red and is taken from inside the clean mask, if not
the spectrum is blue and is taken from the whole image. The black spectrum
shows the noise level per channel. Additionally, the atmospheric transmission
is shown by a magenta line and the hif_findcont channel ranges by cyan lines.
<br>
More details about all these plots can be found in the ALMA Pipeline User Guide.
</li> 
%endif
</ul>
