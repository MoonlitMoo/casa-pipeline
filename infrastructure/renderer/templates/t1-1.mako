<%!
navbar_active='Home'

import collections
import datetime
import functools
import itertools
import operator
import os

import pipeline.domain.measures as measures
import pipeline.infrastructure.utils as utils
from pipeline.environment import iers_info

def tablerow_cmp(tr1, tr2):
    # sort rows by:
    # 1. OUS ID
    # 2. session time
    # 3. session name

    # OUS ID is single valued for any context so no point sorting on it
    # See context.project_structure.ousstatus_entity_id
##     if tr1.ousstatus_entity_id != tr2.ousstatus_entity_id:
##         # simple string compare
##         return cmp(tr1.ousstatus_entity_id, tr2.ousstatus_entity_id)
    if tr1.time_start != tr2.time_start:
        # use MS time as a proxy for session time
        return (tr1.time_start > tr2.time_start) - (tr1.time_start < tr2.time_start)
    if tr1.session != tr2.session:
        # natural sort so that session9 comes before session10
        name_sorted = utils.natural_sort((tr1.session, tr2.session))
        return -1 if name_sorted[0] == tr1.session else 1
    return 0

%>
<%inherit file="base.mako"/>

<script>
$(document).ready(function() {
    pipeline.pages.t1_1.ready();
});
</script>


<%block name="title">Home</%block>

<div class="row">
    <div class="col-md-6">
        <h2>Observation Overview</h2>
        <table class="table table-condensed" summary="Data Details">
            <thead>
                <tr>
                    <th class="col-md-5"></th>
                    <th class="col-md-7"></th>
                </tr>
            </thead>
            <tbody>
        % if project_id is not None:
                <tr>
                    <th>Project</th>
                    <td>${project_uids}</td>
                </tr>
        % endif
        % if observer is not None:
                <tr>
                    <th>Principal Investigator</th>
                    <td>${observers}</td>
                </tr>
        % endif

        % if ous_uid != 'unknown':
                <tr>
                    <th>OUS Status Entity id</th>
                    <td>${ousstatus_entity_id}</td>
                </tr>
        % endif
                <tr>
                    <th>Observation Start</th>
                    <td>${obs_start}&nbsp;UTC</td>
                </tr>
                <tr>
                    <th>Observation End</th>
                    <td>${obs_end}&nbsp;UTC</td>
                </tr>
                <tr>
                    <th>Number of Execution Blocks</th>
                    <td>${number_of_execblocks}</td>
                </tr>
            </tbody>
        </table>
    </div>

    <div class="col-md-6">
        <h2>Pipeline Summary</h2>

        <table class="table table-condensed">
            <thead>
                <tr>
                    <th class="col-md-3"></th>
                    <th class="col-md-6"></th>
                    <th class="col-md-2"></th>
                    <th class="col-md-1"></th>
                </tr>
            </thead>
            <tbody>
                % if ppr_uid is not None:
                <tr>
                    <th>PPR ID</th>
                    <td></td>
                    <td></td>
                    <td></td>
                </tr>
                % endif
                <tr>
                    <th>Pipeline Version</th>
                    <td>${pipeline_revision}
                    % if pipeline_doclink is not None:
                        (<a href=${pipeline_doclink}>documentation</a>)
                    % endif
                    </td>
                    <td></td>
                    <td></td>
                </tr>
                <tr>
                    <th>CASA Version</th>
                    <td>${casa_version} (<a href="javascript:"
                                            data-fancybox
                                            data-selectable="true"
                                            data-options='{"touch" : false}'
                                            data-src="#hidden-environment">environment</a>)</td>
                    <td></td>
                    <td></td>
                </tr>
                <tr>
                    <th>IERSeop2000 Version</th>
                    % if iers_eop_2000_version != 'NOT FOUND':
                        <td>${iers_eop_2000_version} (last date: ${utils.format_datetime(iers_eop_2000_last_date)})</td>
                    % else:
                        <td>
                            <p class="danger alert-danger">
                                <span class="glyphicon glyphicon-remove-sign"></span> IERSeop2000 Table not found.
                            </p>
                        </td>
                    % endif
                    <td></td>
                    <td></td>
                </tr>
                <tr>
                    <th>IERSpredict Version</th>
                    % if iers_predict_version != 'NOT FOUND':
                        <td>${iers_predict_version} (last date: ${utils.format_datetime(iers_predict_last_date)}) </td>
                    % else:
                        <td>
                            <p class="danger alert-danger">
                                <span class="glyphicon glyphicon-remove-sign"></span> IERSpredict Table not found.
                            </p>
                        </td>
                    % endif
                    <td></td>
                    <td></td>
                </tr>
                <tr>
                    <th>Pipeline Start</th>
                    <td>${exec_start}&nbsp;UTC</td>
                    <td></td>
                    <td></td>
                </tr>
                <tr>
                    <th>Execution Duration</th>
                    <td>${exec_duration}</td>
                    <td></td>
                    <td></td>
                </tr>
<%doc>
                <tr>
                    <th>QA Total</th>
                    <td></td>
                    <td><div class="progress" style="margin-bottom:0px;"><div class="bar" style="width:0%;"><span class="text-center"></span></div></div></td>
                    <td><span class="badge">N/A</span></td>
                </tr>
                % for section in topicregistry.topics.values():
                <tr>
                    <td>&nbsp;&nbsp;&nbsp;&nbsp;<a href="${section.url}">${section.description}</a></td>
                                        <td></td>
                    <td><div class="progress" style="margin-bottom:0px;">
                          <div class="bar" style="width:0%;">
                            <span class="text-center"></span>
                          </div>
                        </div>
                    </td>
                    <td><span class="badge">N/A</span></td>

                </tr>
                % endfor
</%doc>
            </tbody>
        </table>
<!-- 
        % if pcontext.logtype == 'GOUS':
        <li>GOUS ID</li>
        <ul>
            <li>MOUS1 ID</li>
            <li>MOUS2 ID</li>
            <li>MOUS3 ID</li>
        </ul>
        % endif
 -->
    </div>

</div>


        <div id="qa_notes" data-href="qa_notes.html"></div>

        <h2>Observation Summary</h2>

        <table class="table table-bordered table-condensed"
            summary="Measurement Set Summaries">

        <thead>
            <tr>

            <th scope="col" rowspan="2">Measurement Set</th>
            <th scope="col" rowspan="2">Receivers</th>
            <th scope="col" rowspan="2">Num Antennas</th>
            <th scope="col" colspan="3">Time (UTC)</th>
                    <!-- break heading divider for subcolumns -->
            <th scope="col" colspan="3">Baseline Length</th>
            <th scope="col" rowspan="2">Size</th>
            % if pcontext.project_summary.telescope.lower() == 'nro':
                <th scope="col" rowspan="2">Merge2 Version</th>
            % endif
            </tr>
            <tr>
            <th scope="col">Start</th>
            <th scope="col">End</th>
            <th scope="col">On Target</th>
            <th scope="col">Min</th>
            <th scope="col">Max</th>
            <th scope="col">RMS</th>
            </tr>
        </thead>
            <tbody>
                <%
                    ms_sorted_rows = sorted(ms_summary_rows, key=functools.cmp_to_key(tablerow_cmp))
                %>
                % for ouskey, ousgroup in itertools.groupby(ms_sorted_rows, key=operator.attrgetter('ousstatus_entity_id')):
                    <%
                        ouslabel = ''
                        ousid = ''
                        if pcontext.project_summary.telescope == 'ALMA':
                            ouslabel = '<b> Observing Unit Set Status: </b>'
                            ousid = ouskey
                    %>
                    % for sb_id, sbgroup in itertools.groupby(ousgroup, key=operator.attrgetter('schedblock_id')):
                        <%
                            sb_group = list(sbgroup)
                            sb_name = sb_group[0].schedblock_name
                            if sb_name != None:
                                sb_name_markup = '<b>Scheduling Block Name:</b> {}'.format(sb_name)
                            else:
                                sb_name_markup = ''
                            if pcontext.project_summary.telescope.lower() == 'nro':
                                numcol = 11
                            else:
                                numcol = 10
                        %>
                        % for sessionkey, sessiongroup in itertools.groupby(sb_group, key=operator.attrgetter('session')):
                            <tr bgcolor="#D1E0FF">
                                <td colspan="${numcol}">${ouslabel} ${ousid} <b>Scheduling Block ID:</b> ${sb_id} ${sb_name_markup}</td>
                            </tr>
                            <tr bgcolor="#E8F0FF">
                            <% 
                                session_group = list(sessiongroup)
                                if pcontext.project_summary.telescope == 'ALMA':
                                    acs_version = session_group[0].acs_software_version
                                    software_build_version = session_group[0].acs_software_build_version
                                    observing_modes = ', '.join(om if om != "none" else "Not Populated" for om in session_group[0].observing_modes)
                                    if not observing_modes:
                                        observing_modes = "Not Populated"
                            %>
                            % if pcontext.project_summary.telescope == 'ALMA':                        
                                <td colspan="${numcol}"><b>Session:</b> ${sessionkey} <b>ACS Version:</b> ${acs_version}, <b>Build Version:</b> ${software_build_version}, <b>Observing Mode(s):</b> ${observing_modes} </td>
                            % else: 
                                <td colspan="${numcol}"><b>Session:</b> ${sessionkey} </td>
                            % endif
                            </tr>
                            % for row in session_group:
                                % if pcontext.project_summary.telescope == 'ALMA':
                                <!-- If either the ACS software version or build version is different from the previous value, display the new software and build versions. -->
                                    % if row.acs_software_version != acs_version or row.acs_software_build_version != software_build_version:
                                        <td colspan="${numcol}"><b>ACS Version:</b> ${row.acs_software_version}, <b>Build Version:</b> ${row.acs_software_build_version} </td>
                                        <% 
                                        acs_version = row.acs_software_version
                                        software_build_version = row.acs_software_build_version
                                        %>
                                    % endif
                                % endif
                                <tr>
                                    <td><a href="${row.href}">${row.ms}</a></td>
                                    <td>${utils.commafy(row.receivers, quotes=False)}</td>
                                    <td>${row.num_antennas}</td>
                                    <td>${utils.format_datetime(row.time_start)}</td>
                                    <td>${utils.format_datetime(row.time_end)}
                                    % if iers_info.validate_date(row.time_end): 
                                         </td>
                                    % elif iers_info.date_message_type(row.time_end) == "INFO":
                                            <p>MS dates not fully covered by IERSeop2000. CASA will use IERSpredict.</p>
                                        </td>
                                    % elif iers_info.date_message_type(row.time_end) == "WARN":
                                            <p class="warning alert-warning"> 
                                                <span class="glyphicon glyphicon-exclamation-sign"></span> MS dates not fully covered by IERSeop2000. CASA will use IERSpredict.
                                            </p>
                                        </td>
                                    % else:
                                            <p class="danger alert-danger">
                                                <span class="glyphicon glyphicon-remove-sign"></span> MS dates not fully covered by IERSpredict. Please update your data repository.
                                            </p>
                                        </td>
                                    % endif
                                    <td>${row.time_on_source}</td>
                                    <td>${str(row.baseline_min)}</td>
                                    <td>${str(row.baseline_max)}</td>
                                    <td>${str(row.baseline_rms)}</td>
                                    <td>${str(row.filesize)}</td>
                                    % if pcontext.project_summary.telescope.lower() == 'nro':
                                        <td>${getattr(row, 'merge2_version', 'N/A')}</td>
                                    % endif
                                </tr>
                            % endfor
                        <%
                            ouslabel = ''
                            ousid = ''
                            sblabel = ''
                            sbid = ''
                        %>
                    % endfor
                %endfor
                % endfor
            </tbody>
        </table>

<div style="display: none;" id="hidden-environment">
    <p><strong>Execution Mode:</strong> ${execution_mode}</p>

    % for title, table in environment_tables.items():
    <table class="table table-bordered" summary="${title}">
        <thead>
          <tr>
            <th class="info" scope="col" colspan="${table.num_columns}">${title}</th>
          </tr>
        </thead>
        <tbody>
            % for tr in table.table_rows:
            <tr>
                % for td in tr:
                    ${td}
                % endfor
            </tr>
            % endfor
        </tbody>
    </table>
    % endfor

</div>
