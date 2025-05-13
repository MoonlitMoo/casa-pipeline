<%!
rsc_path = ""
import html
import os.path
import numpy as np
import pipeline.hif.tasks.tclean.renderer as clean_renderer
import pipeline.infrastructure.utils as utils
import pipeline.infrastructure.renderer.rendererutils as rendererutils

columns = {
    'cleanmask' : 'Clean Mask',
    'flux' : 'Primary Beam',
    'image' : 'Image',
    'residual' : 'Residual',
    'model' : 'Final Model',
    'psf' : 'PSF'}

def is_rejected(spw,plane_keep_dict):
    plane_keep_dict[spw]
    desc=''
    if not plane_keep_dict[spw]:
        desc = ' <a style="color:red">(rejected)</a>'
    return desc
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
        if (row.field+str(row.spw), row.vis) != field_vis:
            field_block_indices.append(i)
            field_vis = (row.field+str(row.spw), row.vis)
    field_block_indices.append(len(image_info))
    max_num_columns = min(max(np.array(field_block_indices[1:])-np.array(field_block_indices[:-1])) + 1, 5)
    %>

    %if len(field_block_indices) > 2:
        <h3>
        Spw Selections
        </h3>
        <ul>
            %for i in field_block_indices[:-1]:
                <li>
                <a href="#field_block_${i}">${image_info[i].spw.replace(',',', ')}
                %if image_info[i].result.is_per_eb:
                    (${image_info[i].vis})
                %endif
                ${is_rejected(image_info[i].spw,plane_keep_dict)}
                </a>
                </li>
            %endfor
        </ul>
    %endif


    %if isinstance(vlass_cubesummary_plots_html,str):
        <h3>VLASS Coarse Cube Plane Summary</h3>
        ${vlass_cubesummary_plots_html}
    %endif


    <div class="table-responsive">
    <table class="table table-striped table-hover table-condensed">
        
     
        <thead>
            <th style="width:200px;">Spw Selection</th>
            <th colspan="4">Details</th>
        </thead>

        <tbody>

        %for i in range(len(field_block_indices)-1):

            %if len(field_block_indices) > 2:
                <tr id="field_block_${field_block_indices[i]}" class="jumptarget" style="border-bottom:2px solid black"><td colspan="${max_num_columns}"></td></tr>
            %endif            
            <%
            row=image_info[field_block_indices[i]]
            %>
            <tr>
                
                <td rowspan="6">${row.spw.replace(',',', ')}${is_rejected(row.spw,plane_keep_dict)}</td>
                
                <th>${row.frequency_label}</th>
                <td>${row.frequency}</td> 
                <th></th>
                <td></td>
            </tr>

            <tr>
                <th>beam</th>
                <td>${row.beam}</td>
                <th>beam p.a.</th>
                <td>${row.beam_pa}</td>                
            </tr>

            <tr>
                <th>${row.fractional_bw_label}</th>
                <td>${row.fractional_bw}</td>
                % if row.aggregate_bw_label is not None:
                    <th>${row.aggregate_bw_label}</th>
                    <td>${row.aggregate_bw}</td>
                % endif
            </tr>

            <tr>
                <th>total number of major cycles done</th>
                <td>${row.nmajordone_total}</td>
                <th>clean iterations</th>
                <td>${row.iterdone}</td>
            </tr>            

            <tr>
                <th>stop reason</th>
                <td>[${row.stopcode}] ${row.stopreason}</td>                
                <th>score</th>
                <td>${row.score}</td>  
            </tr>
            <tr>
                <th>image file</th>
                <td colspan="3">${row.image_file}</td>
            </tr>            

        
            %for j in range(field_block_indices[i], field_block_indices[i+1], 4):
                <tr style="border-top:1px solid gray">
                    <th style="width:20%">Stokes</th>
                    %for k in range(j, min(j+4, field_block_indices[i+1])):
                    <th style="width:20%">${'<i>%s<i>' % (image_info[k].pol)}</th>
                    %endfor
                </tr>            
                <tr>
                    
                    <td style="vertical-align:middle">
                    
                    % if row.majorcycle_stat_plot is not None:                    
                    <%
                    fullsize_relpath = os.path.relpath(row.majorcycle_stat_plot.abspath, pcontext.report_dir)
                    thumbnail_relpath = os.path.relpath(row.majorcycle_stat_plot.thumbnail, pcontext.report_dir)
                    %>
                    
                    <a href="${fullsize_relpath}"
                    data-fancybox="clean-summary-images"
                    title='<div class="pull-left">
                            Test figure<br>
                            Spw: .test<br>
                            Field: something
                            </div>
                            <div class="pull-right"><a href="${fullsize_relpath}">Full Size</a></div>'>
                    <img class="lazyload img-responsive" 
                        data-src="${thumbnail_relpath}"
                        title="Major cycle statistics"
                        alt="Major cycle statistics">
                    </a>
                    <div class="caption">
                        <p>
                            <a class="replace"
                            href="${os.path.relpath(row.tab_url, pcontext.report_dir)}"
                            role="button">
                                View major cycle table...
                            </a>
                        </p>
                    </div>
                    % endif                      
                    
                    </td>

                    %for k in range(j, min(j+4, field_block_indices[i+1])):
                    <td style="vertical-align:middle">
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
                            <img class="lazyload img-responsive"
                                data-src="${thumbnail_relpath}"
                                title="Iteration ${image_info[k].plot.parameters['iter']}: image"
                                alt="Iteration ${image_info[k].plot.parameters['iter']}: image">
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
                    <th>clean residual peak / scaled MAD</th>
                    %for k in range(j, min(j+4, field_block_indices[i+1])):
                        <td>${image_info[k].residual_ratio}</td>
                    %endfor
                </tr>
                <tr>
                    <th>${image_info[k].non_pbcor_label}</th>
                    %for k in range(j, min(j+4, field_block_indices[i+1])):
                        <td>${image_info[k].non_pbcor}</td>
                    %endfor
                </tr>
                %if image_info[k].nsigma_label is not None:
                    <tr>
                        <th>${image_info[k].nsigma_label}</th>
                        %for k in range(j, min(j+4, field_block_indices[i+1])):
                            <td>${image_info[k].nsigma}</td>
                        %endfor
                    </tr>
                %endif
                %if image_info[k].initial_nsigma_mad_label is not None:
                    <tr>
                        <th>${image_info[k].initial_nsigma_mad_label}</th>
                        %for k in range(j, min(j+4, field_block_indices[i+1])):
                            <td>${image_info[k].initial_nsigma_mad}</td>
                        %endfor
                    </tr>
                %endif
                %if image_info[k].final_nsigma_mad_label is not None:
                    <tr>
                        <th>${image_info[k].final_nsigma_mad_label}</th>
                        %for k in range(j, min(j+4, field_block_indices[i+1])):
                            <td>${image_info[k].final_nsigma_mad}</td>
                        %endfor
                    </tr>
                %endif

                %if image_info[k].model_pos_flux is not None:
                <tr>
                    <th>flux in positive model image components</th>
                    %for k in range(j, min(j+4, field_block_indices[i+1])):
                        <td>${image_info[k].model_pos_flux}</td>
                    %endfor
                </tr>
                %endif
                
                %if image_info[k].model_neg_flux is not None:
                <tr>
                    <th>flux in negative model image components</th>
                    %for k in range(j, min(j+4, field_block_indices[i+1])):
                        <td>${image_info[k].model_neg_flux}</td>
                    %endfor
                </tr>
                %endif

                %if image_info[k].model_flux_inner_deg is not None:
                <tr>
                    <th>flux in model image (inner square deg.)</th>
                    %for k in range(j, min(j+4, field_block_indices[i+1])):
                        <td>${image_info[k].model_flux_inner_deg}</td>
                    %endfor
                </tr>
                %endif


                %if image_info[k].pbcor is not None:
                <tr>
                    <th>flatnoise image max / min</th>
                    %for k in range(j, min(j+4, field_block_indices[i+1])):
                    <td>${image_info[k].pbcor}</td>
                    %endfor
                </tr>
                %endif  

                %if image_info[k].vis_amp_ratio_label is not None:
                    <tr>
                        <th>${image_info[k].vis_amp_ratio_label}</th>
                        %for k in range(j, min(j+4, field_block_indices[i+1])):
                            <td>${'{:.4}'.format(image_info[k].vis_amp_ratio)}</td>
                        %endfor
                    </tr>
                %endif                

            %endfor
         %endfor
        </tbody>
    </table>
    </div>
       
%endif
