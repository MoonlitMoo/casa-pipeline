import html
import itertools
import os
from typing import List, Optional, Union

import numpy as np

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.logging as logging
import pipeline.infrastructure.utils as utils
from pipeline.infrastructure import casa_tools
from pipeline.infrastructure.pipelineqa import QAScore, WebLogLocation, scores_with_location

from typing import Any

LOG = infrastructure.get_logger(__name__)

SCORE_THRESHOLD_ERROR = 0.33
SCORE_THRESHOLD_WARNING = 0.66
SCORE_THRESHOLD_SUBOPTIMAL = 0.9


def printTsysFlags(tsystable, htmlreport):
    """Method that implements a version of printTsysFlags by Todd Hunter.
    """
    with casa_tools.TableReader(tsystable) as mytb:
        spws = mytb.getcol("SPECTRAL_WINDOW_ID")

    with casa_tools.TableReader(tsystable+"/ANTENNA") as mytb:
        ant_names = mytb.getcol("NAME")

    with open(htmlreport, 'w') as stream:
        stream.write('<html>')

        with casa_tools.TableReader(tsystable) as mytb:
            for iant in range(len(ant_names)):
                for spw in np.unique(spws):

                    # select rows from table for specified antenna and spw
                    zseltb = mytb.query("SPECTRAL_WINDOW_ID=={0} && ANTENNA1=={1}".format(spw, iant))
                    try:
                        flags = zseltb.getcol("FLAG")
                        times = zseltb.getcol("TIME")
                        fields = zseltb.getcol("FIELD_ID")
                    finally:
                        zseltb.close()

                    npol = len(flags)
                    nchan = len(flags[0])
                    uniqueTimes = np.unique(times)

                    for pol in range(npol):
                        for t in range(len(times)):
                            zflag = np.where(flags[pol, :, t])[0]

                            if len(zflag) > 0:
                                if len(zflag) == nchan:
                                    chans = 'all channels'
                                else:
                                    chans = '%d channels: ' % (
                                      len(zflag)) + str(zflag)

                                time_index = list(uniqueTimes).index(times[t])
                                myline = ant_names[iant] + \
                                  " (#%02d), field %d, time %d, pol %d, spw %d, "%(
                                  iant, fields[t], time_index, pol, spw) + \
                                  chans + "<br>\n"

                                stream.write(myline)

                # format break between antennas
                stream.write('<br>\n')


def renderflagcmds(flagcmds, htmlflagcmds):
    """Method to render a list of flagcmds into html format.
    """
    lines = []
    for flagcmd in flagcmds:
        lines.append(flagcmd.flagcmd)

    with open(htmlflagcmds, 'w') as stream:
        stream.write('<html>')
        stream.write('<head/>')
        stream.write('<body>')
        stream.write('''This is the list of flagcmds created by this stage.
          <br>''')

        for line in lines:
            stream.write('%s<br>' % line)
        stream.write('</body>')
        stream.write('</html>')


def get_bar_class(pqascore):
    score = pqascore.score
    if score in (None, '', 'N/A'):
        return ''
    elif score <= SCORE_THRESHOLD_ERROR:
        return ' progress-bar-danger'
    elif score <= SCORE_THRESHOLD_WARNING:
        return ' progress-bar-warning'
    elif score <= SCORE_THRESHOLD_SUBOPTIMAL:
        return ' progress-bar-info'
    else:
        return ' progress-bar-success'


def get_badge_class(pqascore):
    score = pqascore.score
    if score in (None, '', 'N/A'):
        return ''
    elif score <= SCORE_THRESHOLD_ERROR:
        return ' alert-danger'
    elif score <= SCORE_THRESHOLD_WARNING:
        return ' alert-warning'
    elif score <= SCORE_THRESHOLD_SUBOPTIMAL:
        return ' alert-info'
    else:
        return ' alert-success'


def get_bar_width(pqascore):
    if pqascore.score in (None, '', 'N/A'):
        return 0
    else:
        return 5.0 + 95.0 * pqascore.score


def format_score(pqascore):
    if pqascore.score in (None, '', 'N/A'):
        return 'N/A'
    return '%0.2f' % pqascore.score


def get_sidebar_style_for_task(result):
    if all(qa_score.score in (None, '', 'N/A') for qa_score in result.qa.pool):
        return 'text-muted'
    return 'text-dark'


def get_symbol_badge(result):
    if get_failures_badge(result):
        symbol = '<span class="glyphicon glyphicon-minus-sign alert-danger transparent-bg" aria-hidden="true"></span>'
    elif get_errors_badge(result):
        symbol = '<span class="glyphicon glyphicon-remove-sign alert-danger transparent-bg" aria-hidden="true"></span>'
    elif get_warnings_badge(result):
        symbol = '<span class="glyphicon glyphicon-exclamation-sign alert-warning transparent-bg" aria-hidden="true"></span>'
    elif get_attentions_badge(result):
        symbol = '<span class="glyphicon glyphicon-exclamation-sign alert-attention transparent-bg" aria-hidden="true"></span>'
    elif get_suboptimal_badge(result):
        symbol = '<span class="glyphicon glyphicon-question-sign alert-info transparent-bg" aria-hidden="true"></span>'
    else:
        return '<span class="glyphicon glyphicon-none" aria-hidden="true"></span>'
    return symbol


def get_failures_badge(result):
    failure_tracebacks = utils.get_tracebacks(result)
    n = len(failure_tracebacks)
    if n > 0:
        return '<span class="badge alert-important pull-right">%s</span>' % n
    else:
        return ''


def get_attentions_badge(result):
    attention_logrecords = utils.get_logrecords(result, logging.ATTENTION)
    l = len(attention_logrecords)
    if l > 0:
        return '<span class="badge alert-attention pull-right">%s</span>' % l
    else:
        return ''


def get_warnings_badge(result):
    warning_logrecords = utils.get_logrecords(result, logging.WARNING)
    warning_qascores = utils.get_qascores(result, SCORE_THRESHOLD_ERROR, SCORE_THRESHOLD_WARNING)
    l = len(warning_logrecords) + len(warning_qascores)
    if l > 0:
        return '<span class="badge alert-warning pull-right">%s</span>' % l
    else:
        return ''


def get_errors_badge(result):
    error_logrecords = utils.get_logrecords(result, logging.ERROR)
    error_qascores = utils.get_qascores(result, -0.1, SCORE_THRESHOLD_ERROR)
    l = len(error_logrecords) + len(error_qascores)
    if l > 0:
        return '<span class="badge alert-important pull-right">%s</span>' % l
    else:
        return ''


def get_suboptimal_badge(result):
    suboptimal_qascores = utils.get_qascores(result, SCORE_THRESHOLD_WARNING, SCORE_THRESHOLD_SUBOPTIMAL)
    l = len(suboptimal_qascores)
    if l > 0:
        return '<span class="badge alert-info pull-right">%s</span>' % l
    else:
        return ''


def get_command_markup(ctx, command):
    if not command:
        return ''
    # PIPE-1839: avoid removing '/' symbols if not part of a non-empty directory path
    if ctx.report_dir:
        command = command.replace('%s/' % ctx.report_dir, '')
    if ctx.output_dir:
        command = command.replace('%s/' % ctx.output_dir, '')
    return html.escape(command, True).replace('\'', '&#39;')


def format_shortmsg(pqascore):
    # First check against None. Comparisons of None and float are no longer
    # allowed in Python 3.
    if pqascore.score is None:
        return pqascore.shortmsg
    if pqascore.score > SCORE_THRESHOLD_SUBOPTIMAL:
        return ''
    else:
        return pqascore.shortmsg


def sort_row_by(row, axes):
    # build primary, secondary, tertiary, etc. axis sorting functions
    def f(axis):
        def g(plot):
            return plot.parameters.get(axis, '')
        return g

    # create a parameter getter for each axis
    accessors = [f(axis.strip()) for axis in axes.split(',')]

    # sort plots in row, using a generated tuple (p1, p2, p3, ...) for
    # secondary sort
    return sorted(row, key=lambda plot: tuple([fn(plot) for fn in accessors]))


def group_plots(data, axes):
    if data is None:
        return []

    # build primary, secondary, tertiary, etc. axis sorting functions
    def f(axis):
        def g(plot):
            return plot.parameters.get(axis, '')
        return g

    keyfuncs = [f(axis) for axis in axes.split(',')]
    return _build_rows([], data, keyfuncs)


def _build_rows(rows, data, keyfuncs, axis: str=''):
    # if this is a leaf, i.e., we are in the lowest level grouping and there's
    # nothing further to group by, add a new row
    if not keyfuncs:
        rows.append((data, axis))
        return

    # otherwise, this is not the final sorting axis and so proceed to group
    # the results starting with the first (or next) axis...
    keyfunc = keyfuncs[0]
    data = sorted(data, key=keyfunc)
    for group_value, items_with_value_generator in itertools.groupby(data, keyfunc):
        # convert to list so we don't exhaust the generator
        items_with_value = list(items_with_value_generator)
        # ... , creating sub-groups for each group as we go
        _build_rows(rows, items_with_value, keyfuncs[1:], axis=group_value)

    return rows


def sanitize_data_selection_string(text):
    split_text = utils.safe_split(text)
    sanitized_text = "[{}]".format(", ".join(["&quot;{}&quot;".format(field) for field in split_text]))
    return sanitized_text


def num_lines(path):
    """
    Report number of non-empty non-comment lines in a file specified by the
    path argument. If the file does not exist, report N/A.
    """
    if os.path.exists(path):
        return sum(1 for line in open(path) if line.strip() and not line.startswith('#'))
    else:
        return 'N/A'


def scores_in_range(pool: List[QAScore], lo: float, hi: float) -> List[QAScore]:
    """
    Filter QA scores by range.
    """
    return [score for score in pool
            if score.score not in ('', 'N/A', None)
            and lo < score.score <= hi]


def get_notification_trs(result, alerts_info, alerts_success):
    # suppress scores not intended for the banner, taking care not to suppress
    # legacy scores with a default message destination (=UNSET) so that old
    # tasks continue to render as before
    all_scores: List[QAScore] = result.qa.pool
    # PIPE-1481 potentially asks for the removal of banner QA notification.
    # Thus disabling these for now.
    #banner_scores = scores_with_location(all_scores, [WebLogLocation.BANNER, WebLogLocation.UNSET])
    banner_scores = []

    notifications = []
    most_severe_render_class = None

    if banner_scores:
        for qa_score in scores_in_range(banner_scores, -0.1, SCORE_THRESHOLD_ERROR):
            n = format_notification('danger alert-danger', 'QA', qa_score.longmsg, 'glyphicon glyphicon-remove-sign')
            notifications.append(n)
            if most_severe_render_class is None:
                most_severe_render_class = 'danger alert-danger'
        for qa_score in scores_in_range(banner_scores, SCORE_THRESHOLD_ERROR, SCORE_THRESHOLD_WARNING):
            n = format_notification('warning alert-warning', 'QA', qa_score.longmsg, 'glyphicon glyphicon-exclamation-sign')
            notifications.append(n)
            if most_severe_render_class is None:
                most_severe_render_class = 'warning alert-warning'

    for logrecord in utils.get_logrecords(result, logging.ERROR):
        n = format_notification('danger alert-danger', 'Error!', logrecord.msg)
        notifications.append(n)
        if most_severe_render_class is None:
            most_severe_render_class = 'danger alert-danger'
    for logrecord in utils.get_logrecords(result, logging.WARNING):
        n = format_notification('warning alert-warning', 'Warning!', logrecord.msg)
        notifications.append(n)
        if most_severe_render_class is None:
            most_severe_render_class = 'warning alert-warning'
    for logrecord in utils.get_logrecords(result, logging.ATTENTION):
        n = format_notification('attention alert-attention', 'Attention!', logrecord.msg)
        notifications.append(n)
        if most_severe_render_class is None:
            most_severe_render_class = 'attention alert-attention'

    if alerts_info:
        for msg in alerts_info:
            n = format_notification('info alert-info', '', msg)
            notifications.append(n)
            if most_severe_render_class is None:
                most_severe_render_class = 'info alert-info'
    if alerts_success:
        for msg in alerts_success:
            n = format_notification('success alert-success', '', msg)
            notifications.append(n)
            if most_severe_render_class is None:
                most_severe_render_class = 'success alert-success'

    return notifications, most_severe_render_class


def format_notification(tr_class, alert, msg, icon_class=None):
    if icon_class:
        icon = '<span class="%s"></span> ' % icon_class
    else:
        icon = ''
    return '<tr class="%s"><td>%s<strong>%s</strong> %s</td></tr>' % (tr_class, icon, alert, msg)


def get_relative_url(report_dir: str, stage_dir: str, subpage_dir: str,
                     allow_nonexistent: bool = True) -> Union[str, None]:
    """
    Return url to weblog subpage relative to the weblog root path, based on
    provided report dir, stage dir, and subpage dir. Check for and remove
    common path elements and handle either all relative paths, or all absolute
    paths.

    If allow_nonexistent (default: True) is set to False, return None when the
    constructed path does not exist.
    """
    # Check whether weblog stage path contains common path
    # with report dir, and if so, determine actual relative path.
    stage_cpath = os.path.commonpath([report_dir, stage_dir])
    if stage_cpath:
        stage_relpath = os.path.relpath(stage_dir, stage_cpath)
    else:
        stage_relpath = stage_dir

    # Check whether subpage path contains common path with the
    # report + weblog stage path, and if so, determine actual
    # relative path.
    subpage_cpath = os.path.commonpath([os.path.join(report_dir, stage_relpath), subpage_dir])
    if subpage_cpath:
        subpage_relpath = os.path.relpath(subpage_dir, subpage_cpath)
    else:
        subpage_relpath = subpage_dir

    # Combine paths.
    report_abspath = os.path.abspath(report_dir)
    subpage_abspath = os.path.join(report_abspath, stage_relpath, subpage_relpath)

    # Return relative url if path exists.
    if os.path.exists(subpage_abspath) or allow_nonexistent:
        return os.path.relpath(subpage_abspath, report_abspath)
    else:
        return None


def percent_flagged(flagsummary: Any) -> str:
    """
    Method to output flagging percentages neatly.
    """

    flagged = flagsummary.flagged
    total = flagsummary.total

    if total == 0:
        return 'N/A'
    else:
        return '%0.3f%%' % (100.0 * flagged / total)


_types = {
    'before': 'Calibrated data before flagging',
    'after': 'Calibrated data after flagging'
}

def plot_type(plot: Any) -> str:
    """
    Output plot type.
    """

    return _types[plot.parameters['type']]


def summarise_fields(fields: str) -> str:
    """
    Output field summary string. List all fields if up to 10,
    else first 3 fields and last field.

    Args:
        fields: comma separated list of field names

    Returns:
        Summary string
    """

    field_list = utils.numeric_sort(fields.split(','))

    max_fields = 10
    num_fields = len(field_list)
    if num_fields <= max_fields:
        return ', '.join([str(f) for f in field_list])

    field_str = f'{field_list[0]}, {field_list[1]}, {field_list[2]}, ..., {field_list[-1]}'
    return field_str
