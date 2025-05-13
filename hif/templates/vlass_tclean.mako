<%!
rsc_path = ""
import html
import os.path
import pipeline.hif.tasks.tclean.renderer as clean_renderer
import pipeline.infrastructure.utils as utils
import pipeline.infrastructure.renderer.rendererutils as rendererutils

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


<h2>Image Details</h2>

%if not len(result[0].targets):
    %if result[0].clean_list_info == {}:
        <p>There are no clean targets.
    %else:
        <p>${result[0].clean_list_info.get('msg', '')}
    %endif
%else:
    <table class="table table-striped">
        <thead>
        <tr>
            <th>Field</th>
            <th>Spw</th>
            <th>Pol</th>
            <th colspan="2">Image details</th>
            <th>Image result</th>
        </tr>
        </thead>
        <tbody>
            
            % for row in image_info:

                <%
                rowspan = 15
                if row.model_pos_flux is not None: rowspan += 1
                if row.model_neg_flux is not None: rowspan += 1
                if row.model_flux_inner_deg is not None: rowspan += 1
                if row.nmajordone_total is not None: rowspan += 1
                if row.aggregate_bw_label is not None: rowspan += 1
                if row.nmajordone_per_iter is not None: rowspan += len(row.nmajordone_per_iter.keys())
                %>

                %if row.frequency is not None:
                    <tr>
                        <td rowspan="${rowspan}">${row.field}</td>
                        <td rowspan="${rowspan}">${row.spw}</td>
                        <td rowspan="${rowspan}">${row.pol}</td>
                        <th>${row.frequency_label}</th>
                        <td>${row.frequency}</td>
                        <td rowspan="${rowspan}">
                            % if row.plot is not None:
                                <%
                                fullsize_relpath = os.path.relpath(row.plot.abspath, pcontext.report_dir)
                                thumbnail_relpath = os.path.relpath(row.plot.thumbnail, pcontext.report_dir)
                                %>
                                <a href="${fullsize_relpath}"
                                data-fancybox="clean-summary-images"
                                title='<div class="pull-left">Iteration: ${row.plot.parameters['iter']}<br>
                                        Spw: ${row.plot.parameters['virtspw']}<br>
                                        Field: ${html.escape(row.field, True)}</div><div class="pull-right"><a href="${fullsize_relpath}">Full Size</a></div>'>
                                <img src="${thumbnail_relpath}"
                                    title="Iteration ${row.plot.parameters['iter']}: image"
                                    alt="Iteration ${row.plot.parameters['iter']}: image"
                                    class="img-thumbnail img-responsive">
                                </a>
                                <div class="caption">
                                    <p>
                                        <a class="replace"
                                        href="${os.path.relpath(row.qa_url, pcontext.report_dir)}"
                                        role="button">
                                            View other QA images...
                                        </a>
                                    </p>
                                </div>
                            % else:
                                <p>No QA image available</p>
                            % endif
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
                                <img src="${thumbnail_relpath}"
                                    title="Major cycle statistics"
                                    alt="Major cycle statistics"
                                    class="img-thumbnail img-responsive">
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
                    </tr>

                    <tr>
                        <th>beam</th>
                        <td>${row.beam}</td>
                    </tr>

                    <tr>
                        <th>beam p.a.</th>
                        <td>${row.beam_pa}</td>
                    </tr>

                    <tr>
                        <th>${row.cleaning_threshold_label}</th>
                        <td>${row.cleaning_threshold}</td>
                    </tr>

                    ## added for PIPE-488
                    <tr>
                        <th>${row.initial_nsigma_mad_label}</th>
                        <td>${row.initial_nsigma_mad}</td>
                    </tr>

                    ## added for PIPE-488
                    <tr>
                        <th>${row.final_nsigma_mad_label}</th>
                        <td>${row.final_nsigma_mad}</td>
                    </tr>

                    ## added for PIPE-1081
                    % if row.outmaskratio is not None:
                    <tr>
                        <th>${row.outmaskratio_label}</th>
                        <td>${row.outmaskratio}</td>
                    </tr>
                    % endif

                    ## added for PIPE-991
                    % if row.model_pos_flux is not None:
                    <tr>
                        <th>flux in positive model image components</th>
                        <td>${row.model_pos_flux}</td>
                    </tr>
                    % endif

                    % if row.model_neg_flux is not None:
                    <tr>
                        <th>flux in negative model image components</th>
                        <td>${row.model_neg_flux}</td>
                    </tr>
                    % endif

                    % if row.model_flux_inner_deg is not None:
                    <tr>
                        <th>flux in model image (inner square deg.)</th>
                        <td>${row.model_flux_inner_deg}</td>
                    </tr>
                    % endif

                    % if row.nmajordone_total is not None:
                    <tr>
                        <th>total number of major cycles done</th>
                        <td>${row.nmajordone_total}</td>
                    </tr>
                    % endif

                    <tr>
                        <th>clean residual peak / scaled MAD</th>
                        <td>${row.residual_ratio}</td>
                    </tr>

                    <tr>
                        <th>${row.non_pbcor_label}</th>
                        <td>${row.non_pbcor}</td>
                    </tr>

                    <tr>
                        <th>flatnoise image max / min</th>
                        <td>${row.pbcor}</td>
                    </tr>

                    <tr>
                        <th>${row.fractional_bw_label}</th>
                        <td>${row.fractional_bw}</td>
                    </tr>

                    % if row.aggregate_bw_label is not None:
                    <tr>
                        <th>${row.aggregate_bw_label}</th>
                        <td>${row.aggregate_bw}</td>
                    </tr>
                    % endif

                    <tr>
                        <th>clean iterations</th>
                        <td>${row.iterdone}</td>
                    </tr>

                    <tr>
                        <th>stop reason</th>
                        <td>[${row.stopcode}] ${row.stopreason}</td>
                    </tr>

                    <tr>
                        <th>score</th>
                        <td>${row.score}</td>
                    </tr>

                    <tr>
                        <th>image file</th>
                        <td colspan="2">${row.image_file}</td>
                    </tr>

                    <tr>
                        <th>data type</th>
                        <td colspan="2">${row.datatype_info}</td>
                    </tr>
                %endif
            
            %endfor
        
        </tbody>
    </table>
%endif
