<%!
import html
import os
import xml.sax.saxutils as saxutils

from pipeline.infrastructure.utils import get_obj_size

import pipeline.domain.measures as measures
import pipeline.infrastructure.filenamer as filenamer
import pipeline.infrastructure.logging as logging
import pipeline.infrastructure.renderer.htmlrenderer as hr
import pipeline.infrastructure.renderer.rendererutils as rendererutils
import pipeline.infrastructure.utils as utils
from pipeline.infrastructure.pipelineqa import WebLogLocation, scores_with_location
%>
<html>
<head>
    <script>
        lazyload();
    </script>
</head>

<%def name="plot_group(plot_dict, url_fn, data_spw=False, data_field=False, data_baseband=False, data_tsysspw=False,
                       data_vis=False, data_ant=False, title_id=None, rel_fn=None, break_rows_by='', sort_row_by='', separate_rows_by='', show_row_break_value=False)">
% if plot_dict:
    % if title_id:
        <h3 id="${title_id}" class="jumptarget">${caller.title()}</h3>
    % else:
        <h3>${caller.title()}</h3>
    % endif

    % if hasattr(caller, 'preamble'):
         ${caller.preamble()}
    % endif

    % for ms, ms_plots in plot_dict.items():
        <%
            relurl = url_fn(ms)
            if relurl:
                subpage_path = rendererutils.get_relative_url(pcontext.report_dir, dirname, relurl,
                                                              allow_nonexistent=False)
                subpage_exists = subpage_path is not None
            else:
                subpage_exists = false
        %>

        <h4>
            % if subpage_exists:
            <a class="replace"
               % if data_vis:
               data-vis="${ms}"
               % endif
               href="${subpage_path}">
            % endif
                ${ms}
            % if subpage_exists:
            </a>
            % endif
        </h4>

        % if hasattr(caller, 'ms_preamble'):
            ${caller.ms_preamble(ms)}
        % endif

        % for idx_row, (plots_in_row, group_name) in enumerate(rendererutils.group_plots(ms_plots, break_rows_by)):

        % if idx_row!=0:
            % if separate_rows_by=='thick-line':
                <hr style="height:2px;border-width:0;color:gray;background-color:gray">
            % endif
        % endif

        % if show_row_break_value: 
            <h5>${group_name}</h5>
        % endif

        % if plots_in_row is not None:
            % for plot_counter, plot in enumerate(rendererutils.sort_row_by(plots_in_row, sort_row_by)):
            % if plot_counter // 4 > 0 and plot_counter % 4 == 0:
            </div>
            % endif
            % if plot_counter % 4 == 0:
            <div class="row">
            % endif

            <%
                intent = plot.parameters.get('intent', 'No intent')
                if isinstance(intent, list):
                    intent = utils.commafy(intent, quotes=False)
                intent = intent.upper()
            %>
            <div class="col-md-3">
                % if os.path.exists(plot.thumbnail):
                <%
                    fullsize_relpath = os.path.relpath(plot.abspath, pcontext.report_dir)
                    thumbnail_relpath = os.path.relpath(plot.thumbnail, pcontext.report_dir)
                    link = plot.parameters.get('link', '')
                    if link != '':
                        id_link = 'id="{}"'.format(link)
                    else: 
                        id_link = ''
                %>

                <div class="thumbnail">
                    <a ${id_link} href="${fullsize_relpath}"
                       % if rel_fn:
                           data-fancybox="${rel_fn(plot)}"
                       % elif relurl:
                           data-fancybox="${relurl}"
                       % else:
                           data-fancybox="${caller.title()}"
                       % endif
                       % if hasattr(caller, 'fancybox_caption'):
                           data-caption="${caller.fancybox_caption(plot).strip()}"
                       % endif
                       % if plot.command:
                           data-plotCommandTarget="#plotcmd-${hash(plot.abspath)}"
                       % endif
                    >
                        <img class="lazyload"
                             data-src="${thumbnail_relpath}"
                           % if hasattr(caller, 'mouseover'):
                             title="${caller.mouseover(plot)}"
                           % endif
                        >
                    </a>

                    % if plot.command:
                    <div id="plotcmd-${hash(plot.abspath)}" class="modal-content pipeline-plotcommand" style="display:none;">
                        <div class="modal-header">
                            <button type="button" class="close" data-fancybox-close aria-label="Close">
                                <span aria-hidden="true">&times;</span>
                            </button>
                            <h4 class="modal-title">Plot Command</h4>
                        </div>
                        <div class="modal-body" data-selectable="true">
                            <p>${rendererutils.get_command_markup(pcontext, plot.command)}</p>
                        </div>
                        <div class="modal-footer">
                            <button type="button" class="btn btn-default" data-fancybox-close>Close</button>
                        </div>
                    </div>
                    % endif

                    <div class="caption">
                        <h4>
                        % if subpage_exists:
                            <a href="${subpage_path}"
                            % if data_field:
                               data-field="${html.escape(plot.parameters['field'], True)}"
                            % endif
                            % if data_spw:
                               data-spw="${plot.parameters['spw']}"
                            % endif
                            % if data_tsysspw:
                               data-tsys_spw="${plot.parameters['tsys_spw']}"
                            % endif
                            % if data_baseband:
                               data-baseband="${plot.parameters['baseband']}"
                            % endif
                            % if data_vis:
                               data-vis="${plot.parameters['vis']}"
                            % endif
                            % if data_ant:
                               data-ant="${rendererutils.sanitize_data_selection_string(plot.parameters.get('ant', ""))}"
                            % endif
                               class="replace">
                        % endif
                        ${caller.caption_title(plot)}
                        % if subpage_exists:
                            </a>
                        % endif
                        </h4>
                        % if hasattr(caller, 'caption_subtitle'):
                            <h6>${caller.caption_subtitle(plot)}</h6>
                        % endif

                        % if hasattr(caller, 'caption_text'):
                        <p>${caller.caption_text(plot, intent)}</p>
                        % endif
                    </div>
                </div>
                % endif
            </div>
            % endfor
        </div><!-- end row -->
        % endif
        % endfor

    % endfor

% endif
</%def>

<div class="page-header">
    <h1>${hr.get_stage_number(result)}. <%block name="title">Untitled Task</%block><%block name="backbutton"><button class="btn btn-default pull-right" onClick="javascript:window.history.back();">Back</button></%block></h1>
</div>

<%
weblog_scores = scores_with_location(result.qa.pool, [WebLogLocation.ACCORDION, WebLogLocation.BANNER, WebLogLocation.UNSET])
num_scores = 0
score_color_counts = []
representative_score = result.qa.representative
if representative_score is not None and representative_score.score is not None:
    if -0.1 <= representative_score.score <= rendererutils.SCORE_THRESHOLD_ERROR:
        representative_score_render_class = 'danger alert-danger'
    elif rendererutils.SCORE_THRESHOLD_ERROR < representative_score.score <= rendererutils.SCORE_THRESHOLD_WARNING:
        representative_score_render_class = 'warning alert-warning'
    elif rendererutils.SCORE_THRESHOLD_WARNING < representative_score.score <= rendererutils.SCORE_THRESHOLD_SUBOPTIMAL:
        representative_score_render_class = 'info alert-info'
    elif rendererutils.SCORE_THRESHOLD_SUBOPTIMAL < representative_score.score <= 1.0:
        representative_score_render_class = 'success alert-success'
    else:
        representative_score_render_class = 'panel panel-default'
else:
    representative_score_render_class = 'panel panel-default'

error_scores = rendererutils.scores_in_range(weblog_scores, -0.1, rendererutils.SCORE_THRESHOLD_ERROR)
if len(error_scores) > 0:
    num_scores += len(error_scores)
    score_color_counts.append('%d red' % (len(error_scores)))
    if representative_score is None:
        representative_score = min(error_scores, key=lambda s: s.score)
        representative_score_render_class = 'danger alert-danger'

warning_scores = rendererutils.scores_in_range(weblog_scores, rendererutils.SCORE_THRESHOLD_ERROR, rendererutils.SCORE_THRESHOLD_WARNING)
if len(warning_scores) > 0:
    num_scores += len(warning_scores)
    score_color_counts.append('%d yellow' % (len(warning_scores)))
    if representative_score is None:
        representative_score = min(warning_scores, key=lambda s: s.score)
        representative_score_render_class = 'warning alert-warning'

suboptimal_scores = rendererutils.scores_in_range(weblog_scores, rendererutils.SCORE_THRESHOLD_WARNING, rendererutils.SCORE_THRESHOLD_SUBOPTIMAL)
if len(suboptimal_scores) > 0:
    num_scores += len(suboptimal_scores)
    score_color_counts.append('%d blue' % (len(suboptimal_scores)))
    if representative_score is None:
        representative_score = min(suboptimal_scores, key=lambda s: s.score)
        representative_score_render_class = 'info alert-info'

optimal_scores = rendererutils.scores_in_range(weblog_scores, rendererutils.SCORE_THRESHOLD_SUBOPTIMAL, 1.0)
if len(optimal_scores) > 0:
    num_scores += len(optimal_scores)
    score_color_counts.append('%d green' % (len(optimal_scores)))
    if representative_score is None:
        representative_score = min(optimal_scores, key=lambda s: s.score)
        representative_score_render_class = 'success alert-success'
%>

<div class="panel-group" id="qa-details-accordion" role="tablist" aria-multiselectable="true">

    <div class="panel panel-default">
        <div class="panel-heading-compact" role="tab" id="headingTwo">
            <h5 class="panel-title-compact ${representative_score_render_class}">
                % if num_scores > 1:
                    % if representative_score.score is not None:
                        QA Score: &nbsp; ${'%0.2f' % representative_score.score} &nbsp; ${representative_score.longmsg} &nbsp;
                        <a data-toggle="collapse" data-parent="#qa-details-accordion" href="#collapseTwo" aria-expanded="false" aria-controls="collapseTwo">
                        <u><i><b>All QA Scores (${', '.join(score_color_counts)})</b></i></u>
                        </a>
                    % else:
                        QA Score: &nbsp; N/A &nbsp; ${representative_score.longmsg if representative_score.shortmsg!='No QA' else ''}
                        <a data-toggle="collapse" data-parent="#qa-details-accordion" href="#collapseTwo" aria-expanded="false" aria-controls="collapseTwo">
                        <u><i><b>All QA Scores (${', '.join(score_color_counts)})</b></i></u>
                        </a>
                    % endif
                % else:
                    % if representative_score.score is not None:
                        <tr class="${representative_score_render_class}">
                        QA Score: &nbsp; ${'%0.2f' % representative_score.score} &nbsp; ${representative_score.longmsg}
                        </tr>
                    % else:
                        <tr class="${representative_score_render_class}">
                        QA Score: &nbsp; N/A &nbsp; ${representative_score.longmsg if representative_score.shortmsg!='No QA' else ''}
                        </tr>
                    % endif
                % endif
            </h5>
        </div>
        % if num_scores > 1:
        <div id="collapseTwo" class="panel-collapse collapse" role="tabpanel" aria-labelledby="headingTwo">
            <div class="panel-body">
                % if result.qa.pool:
                <table class="table table-bordered table-condensed" summary="Pipeline QA summary">
                    <caption>Pipeline QA summary for this task.</caption>
                    <thead>
                        <tr>
                            <th>Score</th>
                            <th>Reason</th>
                        </tr>
                    </thead>
                    <tbody>
                    % for qascore in error_scores:
                    <tr class="danger alert-danger">
                        <td>${'%0.2f' % qascore.score}</td>
                        <td>${qascore.longmsg}</td>
                    </tr>
                    % endfor
                    % for qascore in warning_scores:
                    <tr class="warning alert-warning">
                        <td>${'%0.2f' % qascore.score}</td>
                        <td>${qascore.longmsg}</td>
                    </tr>
                    % endfor
                    % for qascore in suboptimal_scores:
                    <tr class="info alert-info">
                        <td>${'%0.2f' % qascore.score}</td>
                        <td>${qascore.longmsg}</td>
                    </tr>
                    % endfor
                    % for qascore in optimal_scores:
                    <tr class="success alert-success">
                        <td>${'%0.2f' % qascore.score}</td>
                        <td>${qascore.longmsg}</td>
                    </tr>
                    % endfor
                    </tbody>
                </table>
                % else:
                    No pipeline QA for this task.
                % endif
            </div>
        </div>
        % endif
    </div>

</div>

<%
    notification_trs, most_severe_render_class = rendererutils.get_notification_trs(result, alerts_info, alerts_success)

    # PIPE-2022 asked for low scores if PNG files are missing. This can only be checked in the
    # weblog generation step after QA scoring is already done. Also any warning messages logged
    # during the weblog rendering will not be caught by the automatic collection of logrecords
    # in the result object. Thus there are PL tasks that send an "extra_logrecords" list into
    # the Mako system to have these shown on weblog pages. The following block tries to get them.
    # To make sure that the errors and warnings get to the top of the list, the two kinds of
    # messages are processed in separate loops, inserting any new message at the first list position.
    try:
        for extra_logrecord in extra_logrecords:
            if extra_logrecord.levelno == logging.logging.WARNING:
                notification_trs.insert(0, rendererutils.format_notification('warning alert-warning', 'Warning!', extra_logrecord.msg))
                if most_severe_render_class not in ('danger alert-danger', 'warning alert-warning'):
                    most_severe_render_class = 'warning alert-warning'
        for extra_logrecord in extra_logrecords:
            if extra_logrecord.levelno == logging.logging.ERROR:
                notification_trs.insert(0, rendererutils.format_notification('danger alert-danger', 'Error!', extra_logrecord.msg))
                if most_severe_render_class != 'danger alert-danger':
                    most_severe_render_class = 'danger alert-danger'
    except Exception as e:
        pass
%>
% if notification_trs:
<div class="panel-group" id="notification-details-accordion" role="tablist" aria-multiselectable="true">

    <div class="panel panel-default">
        <div class="panel-heading-compact" role="tab" id="headingThree">
            <h5 class="panel-title-compact ${most_severe_render_class}">
                % if len(notification_trs) > 1:
                    Most Severe Notification: &nbsp; ${notification_trs[0]} &nbsp;
                    <a data-toggle="collapse" data-parent="#notification-details-accordion" href="#collapseThree" aria-expanded="false" aria-controls="collapseThree">
                    <u><i><b>All Notifications (${len(notification_trs)})</b></i></u>
                    </a>
                % else:
                    <tr class="${most_severe_render_class}">
                    Notification: &nbsp; ${notification_trs[0]}
                    </tr>
                % endif
            </h5>
        </div>
        % if len(notification_trs) > 1:
        <div id="collapseThree" class="panel-collapse collapse" role="tabpanel" aria-labelledby="headingThree">
            <div class="panel-body">
                <table class="table table-bordered table-condensed" summary="Task notifications">
                    <caption>Notifications for this task.</caption>
                    <thead>
                    </thead>
                    <tbody>
                    % for tr in notification_trs:
                        ${tr}
                    % endfor
                    </tbody>
                </table>
            </div>
        </div>
        % endif
    </div>

</div>
% endif

${next.body()}

<div class="clearfix"></div>
<div class="panel-group" id="details-accordion" role="tablist" aria-multiselectable="true">

    <%doc>
    Help disabled until the task descriptions from John's presentation are added

    %if taskhelp:

        <div class="accordion-group">
            <div class="accordion-heading">
                <a class="accordion-toggle" data-toggle="collapse" data-parent="#details-accordion" href="#collapseOne">
                    Task Help
                </a>
            </div>
            <div id="collapseOne" class="accordion-body collapse">
                <div class="accordion-inner">
                    ${taskhelp}
                </div>
            </div>
        </div>
    %endif
    </%doc>

    <div class="panel panel-default">
        <div class="panel-heading" role="tab" id="headingFour">
            <h4 class="panel-title">
                <a data-toggle="collapse" data-parent="#details-accordion" href="#collapseFour" aria-expanded="false" aria-controls="collapseFour">
                Input Parameters
                </a>
            </h4>
        </div>
        <div id="collapseFour" class="panel-collapse collapse" role="tabpanel" aria-labelledby="headingFour">
            <div class="panel-body">
                <dl class="dl-horizontal">
                % for k, v in result.inputs.items():
                    <dt>${str(k)}</dt>
                    <dd>${str(v) if str(v) != '' else '&nbsp;'}</dd>
                % endfor
                </dl>
            </div>
        </div>
    </div>

    <div class="panel panel-default">
        <div class="panel-heading" role="tab" id="headingFive">
            <h4 class="panel-title">
                <a data-toggle="collapse" data-parent="#details-accordion" href="#collapseFive" aria-expanded="false" aria-controls="collapseFive">
                Tasks Execution Statistics
                </a>
            </h4>
        </div>
        <div id="collapseFive" class="panel-collapse collapse" role="tabpanel" aria-labelledby="headingFive">
            <div class="panel-body">
                <dl class="dl-horizontal">
                    <dt>Start time</dt>
                    <dd>${utils.format_datetime(result.timestamps.start, dp=3)}</dd>
                    <dt>End</dt>
                    <dd>${utils.format_datetime(result.timestamps.end, dp=3)}</dd>
                    <dt>Duration</dt>
                    <dd>${utils.format_timedelta(result.timestamps.end - result.timestamps.start, dp=3)}</dd>
                    % if logging.logging_level <= logging.DEBUG:
                        <dt>Context size</dt>
                        <dd>${str(measures.FileSize(get_obj_size(pcontext), measures.FileSizeUnits.BYTES))}</dd>
                    % endif
                </dl>
            Note, WebLog generation is not included in the time.
            </div>
        </div>
    </div>
</div>

<%def name="li_anchor_to_file(relpath)">
    <%
    abspath = os.path.join(pcontext.report_dir, relpath)
    file_exists = os.path.exists(abspath)
    if file_exists:
        total_bytes = os.path.getsize(abspath)
        filesize = measures.FileSize(total_bytes, measures.FileSizeUnits.BYTES)
    %>
    % if file_exists:
        <li><a href="${relpath}" class="replace-pre" data-title="CASA log for stage ${result.stage_number}">View</a> or <a href="${relpath}" download="${relpath}">download</a> ${relpath} (${str(filesize)})</li>
    % endif
</%def>

%if casalog_url:
<div class="panel panel-default">
    <div class="panel-heading">
        <h3 class="panel-title">CASA logs for stage ${result.stage_number}</h3>
    </div>
    <div class="panel-body">
        <ul>${li_anchor_to_file(casalog_url)}</ul>
    </div>
</div>
% endif

</html>
