"""
QA plugins for the ALMA applycal task.

This module demonstrates how to target QAScore messages at particular sections
of the web log.
"""
import collections
import copy
import itertools
import operator
import os
from typing import Dict, Iterable, List, Reversible

import pipeline.h.tasks.applycal.applycal as h_applycal
import pipeline.hif.tasks.applycal.ifapplycal as hif_applycal
import pipeline.infrastructure.logging as logging
import pipeline.infrastructure.pipelineqa as pqa
import pipeline.infrastructure.utils as utils
from pipeline.domain.measurementset import MeasurementSet
from . import ampphase_vs_freq_qa

LOG = logging.get_logger(__name__)


# Maps outlier reasons to a text snippet that can be used in a QAScore message
REASONS_TO_TEXT = {
    'amp_vs_freq.intercept,amp.slope': ('Amp vs frequency', 'zero point and slope outliers', ''),
    'amp_vs_freq.intercept': ('Amp vs frequency', 'zero point outliers', ''),
    'amp_vs_freq.slope': ('Amp vs frequency', 'slope outliers', ''),
    'amp_vs_freq': ('Amp vs frequency', 'outliers', ''),
    'phase_vs_freq.intercept,phase_vs_freq.slope': ('Phase vs frequency', 'zero point and slope outliers', ''),
    'phase_vs_freq.intercept': ('Phase vs frequency', 'zero point outliers', ''),
    'phase_vs_freq.slope': ('Phase vs frequency', 'slope outliers', ''),
    'phase_vs_freq': ('Phase vs frequency', 'outliers', ''),
    'gt90deg_offset_phase_vs_freq.intercept,phase_vs_freq.slope': ('Phase vs frequency', 'zero point and slope outliers', '; phase offset > 90deg detected'),
    'gt90deg_offset_phase_vs_freq.intercept': ('Phase vs frequency', 'zero point outliers', '; phase offset > 90deg detected'),
    'gt90deg_offset_phase_vs_freq.slope': ('Phase vs frequency', 'slope outliers', '; phase offset > 90deg detected'),
    'gt90deg_offset_phase_vs_freq': ('Phase vs frequency', 'outliers', '; phase offset > 90deg detected'),
}

# PIPE356Switches is a struct used to hold various options for outlier
# detection and reporting
PIPE356Switches = collections.namedtuple(
    'PIPE356Switches', 'calculate_metrics export_outliers export_messages include_scores outlier_score flag_all'
)

# PIPE356_MODES defines some preset modes for outlier detection and reporting
PIPE356_MODES = {
    'TEST_REAL_OUTLIERS': PIPE356Switches(calculate_metrics=True, export_outliers=True, export_messages=True,
                                          include_scores=True, outlier_score=0.5, flag_all=False),
    'TEST_FAKE_OUTLIERS': PIPE356Switches(calculate_metrics=True, export_outliers=True, export_messages=True,
                                          include_scores=True, outlier_score=0.5, flag_all=True),
    'ON': PIPE356Switches(calculate_metrics=True, export_outliers=True, export_messages=False, include_scores=True,
                          outlier_score=0.9, flag_all=False),
    'DEBUG': PIPE356Switches(calculate_metrics=True, export_outliers=True, export_messages=True, include_scores=False,
                             outlier_score=0.5, flag_all=False),
    'OFF': PIPE356Switches(calculate_metrics=False, export_outliers=False, export_messages=False, include_scores=False,
                           outlier_score=0.5, flag_all=False)
}


# Tuple to hold data selection parameters. The field order is important as it
# sets in which order the dimensions are rolled up. With the order below,
# scores are merged first by pol, then ant, then spw, etc.
DataSelection = collections.namedtuple('DataSelection', 'vis intent scan spw ant pol')


# The key data structure used to consolidate and merge QA scores: a dict
# mapping data selections to the QA scores that cover that data selection. The
# DataSelection keys are simple tuples, with index relating to a data
# selection parameter (e.g., vis=[0], intent=[1], scan=[2], etc.).
DataSelectionToScores = Dict[DataSelection, List[pqa.QAScore]]


class ALMAApplycalListQAHandler(pqa.QAPlugin):
    """
    QA plugin to process lists of ALMA applycal results.

    This plugin is required to overwrite how representative is set. We want
    the representative score to be a headline banner score pulled from the
    per-EB representative scores, not a lower priority score pulled from the
    general score pool. The latter would be done by default, hence we
    overwrite the handle() implementation.
    """
    result_cls = collections.abc.Iterable
    child_cls = h_applycal.ApplycalResults
    generating_task = hif_applycal.SerialIFApplycal

    def handle(self, context, result):
        super().handle(context, result)

        # The default representative implementation filters on all scores in
        # the pool. Overwrite that selection by selecting a representative
        # from all representative scores
        result.qa.representative = min([r.qa.representative for r in result],
                                       key=operator.attrgetter('score'))


class ALMAApplycalQAHandler(pqa.QAPlugin):
    """
    QA plugin to handle an applycal result for a single ALMA EB.

    This plugin does outlier detection and QA score reprocessing to generate
    messages targeted at different sections of the web log.
    """

    # Register this QAPlugin as handling singular ApplycalResults object
    # returned by IFApplycal only
    result_cls = h_applycal.ApplycalResults
    child_cls = None
    generating_task = hif_applycal.SerialIFApplycal

    def handle(self, context, result: h_applycal.ApplycalResults):
        vis = result.inputs['vis']
        ms = context.observing_run.get_ms(vis)

        # skip if this is a VLA data set
        if 'ALMA' not in ms.antenna_array.name:
            return

        pipe356_mode = os.environ.get('PIPE356_QA_MODE', 'ON').upper()
        mode_switches = PIPE356_MODES[pipe356_mode]

        # dict to hold all QA scores, keyed by intended web log destination
        qa_scores: Dict[pqa.WebLogLocation, List[pqa.QAScore]] = {}

        # calculate the outliers and convert to scores
        if mode_switches.calculate_metrics:
            # calculate the raw QA scores
            raw_qa_scores = get_qa_scores(
                ms, mode_switches.export_outliers, mode_switches.outlier_score, mode_switches.flag_all
            )
            # group and summarise as required by PIPE-477
            qa_scores = summarise_scores(raw_qa_scores, ms)

        # dump weblog messages to a separate file if requested
        if qa_scores and mode_switches.export_messages:
            targets_to_log = [pqa.WebLogLocation.ACCORDION, pqa.WebLogLocation.BANNER]
            with open('PIPE356_messages.txt', 'a') as export_file:
                for weblog_target in targets_to_log:
                    for qa_score in qa_scores[weblog_target]:
                        export_file.write('{}\n'.format(qa_score.longmsg))

        if qa_scores and mode_switches.include_scores:
            for score_list in qa_scores.values():
                result.qa.pool.extend(score_list)

        # pick the minimum score of all (flagging and outliers) as
        # representative, i.e. task score
        result.qa.representative = min(result.qa.pool, key=operator.attrgetter('score'))


def get_qa_scores(ms: MeasurementSet, export_outliers: bool, outlier_score: float, flag_all: bool):
    """
    Calculate amp/phase vs freq outliers for an EB and convert to QA scores.

    This is the key entry point for applycal QA metric calculation. It
    delegates to the detailed metric implementation in ampphase_vs_freq_qa.py
    to detect outliers, converting the outlier descriptions to normalised QA
    scores.
    """
    intents = ['AMPLITUDE', 'BANDPASS', 'PHASE', 'CHECK', 'DIFFGAINREF', 'DIFFGAINSRC', 'POLARIZATION', 'POLANGLE',
               'POLLEAKAGE']

    all_scores = []

    # if requested, write outlier file header
    if export_outliers:
        debug_path = 'applycalQA_outliers.txt'
        with open(debug_path, 'a') as debug_file:
            debug_file.write(f'AMPLITUDE_SLOPE_THRESHOLD: {ampphase_vs_freq_qa.AMPLITUDE_SLOPE_THRESHOLD}\n')
            debug_file.write(f'AMPLITUDE_INTERCEPT_THRESHOLD: {ampphase_vs_freq_qa.AMPLITUDE_INTERCEPT_THRESHOLD}\n')
            debug_file.write(f'PHASE_SLOPE_THRESHOLD: {ampphase_vs_freq_qa.PHASE_SLOPE_THRESHOLD}\n')
            debug_file.write(f'PHASE_INTERCEPT_THRESHOLD: {ampphase_vs_freq_qa.PHASE_INTERCEPT_THRESHOLD}\n')

    for intent in intents:
        # delegate to dedicated module for outlier detection
        outliers = ampphase_vs_freq_qa.score_all_scans(ms, intent, flag_all=flag_all)

        # if requested, export outlier descriptions to a file
        if export_outliers:
            with open(debug_path, 'a') as debug_file:
                for i,o in enumerate(outliers):
                    # Filter doubles from sources with multiple intents
                    duplicate_entry = False
                    for j in range(i-1):
                        if o.vis == outliers[j].vis and \
                           o.scan == outliers[j].scan and \
                           o.spw == outliers[j].spw and \
                           o.ant == outliers[j].ant and \
                           o.pol == outliers[j].pol and \
                           o.reason == outliers[j].reason and \
                           o.num_sigma == outliers[j].num_sigma:
                            duplicate_entry = True
                            break
                    if not duplicate_entry:
                        if o.scan == {-1, }:
                            msg = (f'{o.vis} {o.intent} scan={{all}} spw={o.spw} ant={o.ant} '
                                   f'pol={o.pol} reason={o.reason} sigma_deviation={o.num_sigma}')
                        else:
                            msg = (f'{o.vis} {o.intent} scan={o.scan} spw={o.spw} ant={o.ant} '
                                   f'pol={o.pol} reason={o.reason} sigma_deviation={o.num_sigma}')
                        debug_file.write('{}\n'.format(msg))

        # convert outliers to QA scores
        scores_for_intent = outliers_to_qa_scores(ms, outliers, outlier_score)
        all_scores.extend(scores_for_intent)

    return all_scores


class QAMessage:
    """
    QAMessage constructs a user-friendly QA message for an Outlier.

    The QAMessage instance has two attributes, full_message and short_message,
    that are of interest. full_message holds the text to be used when the
    message is the first to be printed. short_message holds the text to be
    used when this message is to be appended to the text of other QAMessages.
    Naturally, this assumes the the calling code only concatenates messages
    that originate from the same reason.
    """

    def __init__(self, ms, outlier, reason):
        metric_axes, outlier_description, extra_description = REASONS_TO_TEXT[reason]

        # convert pol=0,1 to pol=XX,YY
        # corr axis should be the same for all windows so just pick the first
        if outlier.pol:
            spw_id = list(outlier.spw)[0]
            corr_axes = dict(enumerate(ms.get_data_description(spw=spw_id).corr_axis))
            corrs = sorted([corr_axes[c] for c in outlier.pol])
            corr_msg = ','.join(corrs)
        else:
            corr_msg = ''

        vis = utils.commafy(sorted(outlier.vis), quotes=False)
        intent_msg = f' {utils.commafy(sorted(outlier.intent), quotes=False)} calibrator' if outlier.intent else ''
        spw_msg = f' spw {utils.find_ranges(outlier.spw)}' if outlier.spw else ''
        if -1 in outlier.scan:
            # outlier.scan can be a set or a tuple
            tmp_scan = set(copy.deepcopy(outlier.scan))
            tmp_scan.remove(-1)
            scan_msg = f' scan {",".join(list(filter(None, ["all", utils.find_ranges(tmp_scan)])))}'
        else:
            scan_msg = f' scan {utils.find_ranges(outlier.scan)}' if outlier.scan else ''

        # convert ant=1,3,5 to ant=DV03,CM05,CM08 etc.
        ant_names = sorted([ant.name
                            for ant_id in outlier.ant
                            for ant in ms.antennas
                            if ant.id == ant_id])

        if len(ant_names) == len(ms.antennas):
            ant_names = ['all antennas']
        ant_msg = f' {",".join(ant_names)}' if ant_names else ''
        corr_msg = f' {corr_msg}' if corr_msg else ''

        short_msg = f'{metric_axes} {outlier_description}'
        full_msg = f'{short_msg} for {vis}{intent_msg}{spw_msg}{ant_msg}{corr_msg}{scan_msg}{extra_description}'

        self.short_message = short_msg
        self.full_message = full_msg


def outliers_to_qa_scores(ms: MeasurementSet,
                          outliers: List[ampphase_vs_freq_qa.Outlier],
                          outlier_score: float) -> List[pqa.QAScore]:
    """
    Convert a list of consolidated Outliers into a list of equivalent
    QAScores.

    The MeasurementSet argument is required to convert antenna IDs to antenna
    names.

    All generated QAScores will be assigned the numeric score given in
    outlier_score.

    :param ms: MeasurementSet domain object for the DataSelections
    :param outliers: list of Outliers
    :param outlier_score: score to assign to generated QAScores
    :return:
    """
    hashable = []
    for outlier in outliers:
        # convert ['amp.slope','amp.intercept'] into 'amp.slope,amp.intercept', etc.
        hashable.append(ampphase_vs_freq_qa.Outlier(vis=outlier.vis,
                                                    intent=outlier.intent,
                                                    scan=outlier.scan,
                                                    spw=outlier.spw,
                                                    ant=outlier.ant,
                                                    pol=outlier.pol,
                                                    num_sigma=outlier.num_sigma,
                                                    phase_offset_gt90deg=outlier.phase_offset_gt90deg,
                                                    reason=','.join(sorted(outlier.reason))))
    reasons = {outlier.reason for outlier in hashable}

    qa_scores = []
    for reason in reasons:
        outliers_for_reason = [outlier for outlier in hashable if outlier.reason == reason]
        if not outliers_for_reason:
            continue

        for outlier in outliers_for_reason:
            msgs = QAMessage(ms, outlier, reason=outlier.reason)

            applies_to = pqa.TargetDataSelection(vis=outlier.vis, scan=outlier.scan,
                                                 intent=outlier.intent, spw=outlier.spw,
                                                 ant=outlier.ant, pol=outlier.pol)

            score = pqa.QAScore(outlier_score, longmsg=msgs.full_message, shortmsg=msgs.short_message,
                                applies_to=applies_to, hierarchy=reason,
                                weblog_location=pqa.WebLogLocation.ACCORDION)
            score.origin = pqa.QAOrigin(metric_name=reason,
                                        metric_score=outlier.num_sigma,
                                        metric_units='sigma deviation from reference fit')
            qa_scores.append(score)

    return qa_scores


def to_data_selection(tds: pqa.TargetDataSelection) -> DataSelection:
    """
    Convert a pipeline QA TargetDataSelection object to a DataSelection tuple.
    """
    hashable_vals = {attr: tuple(sorted(getattr(tds, attr))) for attr in DataSelection._fields}
    return DataSelection(**hashable_vals)


def in_casa_format(data_selections: DataSelectionToScores) -> DataSelectionToScores:
    """
    Restate data selections in concise CASA format.
    """
    # we need a neat way to format each message from the indices. This dict
    # states how each field should be formatted
    formatters = {
        'vis': lambda l: ','.join(l),
        'intent': lambda l: ','.join(l),
        # scan, spw, and ant can be expressed in compressed CASA syntax
        # e.g., 1,3,4,5,7 = 1,3~5,7
        'scan': lambda l: utils.find_ranges(l),
        'spw': lambda l: utils.find_ranges(l),
        'ant': lambda l: utils.find_ranges(l),
        'pol': lambda l: ','.join((str(i) for i in l))
    }

    formatted = {}
    for ds, scores in data_selections.items():
        formatted_args = {attr: formatters[attr](val) for attr, val in ds._asdict().items()}
        new_ds = DataSelection(**formatted_args)
        formatted[new_ds] = scores
    return formatted


def summarise_scores(all_scores: List[pqa.QAScore], ms: MeasurementSet) -> Dict[pqa.WebLogLocation, List[pqa.QAScore]]:
    """
    Process a list of QAscores, replacing the detailed and highly specific
    input scores with compressed representations intended for display in the
    web log accordion, and even more generalised summaries intended for
    display as warning banners.
    """
    # list to hold the final QA scores: non-combined hidden scores, plus the
    # summarised (and less specific) accordion scores and banner scores
    final_scores: Dict[pqa.WebLogLocation, List[pqa.QAScore]] = {}

    # we don't want the non-combined scores reported in the web log. They're
    # useful for the QA report written to disk, but for the web log the
    # individual scores will be aggregated into general, less specific QA
    # scores.
    hidden_scores = copy.deepcopy(all_scores)
    for score in hidden_scores:
        score.weblog_location = pqa.WebLogLocation.HIDDEN
    final_scores[pqa.WebLogLocation.HIDDEN] = hidden_scores

    # JH update to spec for PIPE-477:
    #
    # After looking at the current messages, I have come to the conclusion
    # that its really not necessary for the message to relate whether the
    # outlier is slope or offset or both, or that the pol is XX or YY or both,
    # since these are easily discerned once the antenna and scan are known. So
    # that level of detail can be suppressed to keep the number of accordion
    # messages down. I have changed the example in the description accordingly.

    accordion_scores = []
    # Collect scores. The phase scores (normal and > 90 deg offset) should
    # get just one single 1.0 score in case of no outliers.
    for hierarchy_roots in [['amp_vs_freq'], ['phase_vs_freq', 'gt90deg_offset_phase_vs_freq']]:
        # erase just the polarisation dimension for accordion messages,
        # leaving the messages specific enough to identify the plot that
        # caused the problem
        discard = ['pol']
        num_scores = 0
        for hierarchy_root in hierarchy_roots:
            msgs = combine_scores(all_scores, hierarchy_root, discard, ms, pqa.WebLogLocation.ACCORDION)
            num_scores += len(msgs)
            accordion_scores.extend(msgs)

        # add a single 1.0 accordion score for metrics that generated no outlier
        if num_scores == 0:
            metric_axes, outlier_description, _ = REASONS_TO_TEXT[hierarchy_roots[0]]
            # Correct capitalisation as we'll prefix the metric with 'No '
            metric_axes = metric_axes.lower()
            short_msg = 'No {} outliers'.format(metric_axes)
            long_msg = 'No {} {} detected for {}'.format(metric_axes, outlier_description, ms.basename)
            score = pqa.QAScore(1.0,
                                longmsg=long_msg,
                                shortmsg=short_msg,
                                hierarchy=hierarchy_roots[0],
                                weblog_location=pqa.WebLogLocation.ACCORDION,
                                applies_to=pqa.TargetDataSelection(vis={ms.basename}))
            score.origin = pqa.QAOrigin(metric_name=hierarchy_roots[0],
                                        metric_score=0,
                                        metric_units='number of outliers')
            accordion_scores.append(score)

    final_scores[pqa.WebLogLocation.ACCORDION] = accordion_scores

    banner_scores = []
    for hierarchy_root in ['amp_vs_freq', 'phase_vs_freq', 'gt90deg_offset_phase_vs_freq']:
        # erase several dimensions for banner messages. These messages outline
        # just the vis, spw, and intent. For specific info, people should look
        # at the accordion messages.
        msgs = combine_scores(all_scores, hierarchy_root, ['pol', 'ant', 'scan'], ms, pqa.WebLogLocation.BANNER)
        banner_scores.extend(msgs)
    final_scores[pqa.WebLogLocation.BANNER] = banner_scores

    # JH request from 8/4/20:
    #
    # The one thing I'd like to ask is to list the accordion messages ordered
    # by {ms; intent; spw} so that they appear in "figure order" (currently
    # they seem to be ordered by {ms; intent; scan}
    #
    for destination, unsorted_scores in final_scores.items():
        sorted_scores = sorted(unsorted_scores, key=lambda score: (sorted(score.applies_to.vis),
                                                                   sorted(score.applies_to.intent),
                                                                   sorted(score.applies_to.spw),
                                                                   sorted(score.applies_to.scan)))
        final_scores[destination] = sorted_scores

    return final_scores


def combine_scores(all_scores: List[pqa.QAScore],
                   hierarchy_base: str,
                   discard: List[str],
                   ms: MeasurementSet,
                   location: pqa.WebLogLocation) -> List[pqa.QAScore]:
    """
    Combine and summarise a list of QA scores.

    QA scores that share a base metric type and/or differ only in the data
    selection dimensions given in discard are aggregated and summarised
    together.
    """
    all_scores = copy.deepcopy(all_scores)

    # We're going to merge slope and offset outliers together into a single
    # record. We do not need to group or partition the input data by QA score
    # as every outlier gets the same score. If that condition changes, the
    # algorithm below will need to change to group and process by score value
    # too.

    # create a filter function to leave the scores we want to process
    filter_fn = lambda qa_score: qa_score.hierarchy.startswith(f'{hierarchy_base}.')

    # get all QA scores generated by a '<metric> vs X' algorithm
    scores_for_metric = [o for o in all_scores if filter_fn(o)]

    # create a data structure that map data selections to the scores that
    # apply to them
    ds = map_data_selection_to_scores(scores_for_metric)

    # Combine data selections that differ only by polarisation. This erases
    # the distinction between pol=0 and pol=1, placing QA scores in a single
    # data selection that spans all polarisations (other data selection
    # dimensions aside).
    no_pols = discard_dimension(ds, discard)

    # hierarchically merge adjacent data selections
    merged_scores = compress_data_selections(no_pols, DataSelection._fields)

    # filter out to leave one QA score, the score with the highest-valued
    # metric, in the list. This also resets the QAScore.applies_to to match
    # the data selection.
    as_min = take_min_as_representative(merged_scores)

    # we can now discard the DataSelection keys, as the QAScore has now been
    # updated with the correct data selection
    qa_scores = [score for score_list in as_min.values() for score in score_list]

    # rewrite the score messages
    for qa_score in qa_scores:
        # QAMessage takes an Outlier namedtuple, but TargetDataSelection
        # shares enough of the Outlier interface (vis, spw, scan, intent,
        # etc.) that we can pass QAMessage directly without converting to
        # an Outlier
        msgs = QAMessage(ms, qa_score.applies_to, reason=hierarchy_base)
        qa_score.shortmsg = msgs.short_message
        qa_score.longmsg = msgs.full_message
        qa_score.hierarchy = hierarchy_base
        qa_score.weblog_location = location

    return qa_scores


def take_min_as_representative(to_merge: DataSelectionToScores) -> DataSelectionToScores:
    """
    Filter out all but the worst score per data selection.

    Note that this function also rewrites QAScore.applies_to to match the data
    selection the QA score applies to.

    This function operates on a dict that maps DataSelections to list of QA
    scores. For each list, it discards all but the worst score as determined
    by the metric. The selection will be biased towards metrics whose value
    distribution tends higher than other metrics, but it seem the best we can
    do.
    """
    result: DataSelectionToScores = {}
    for ds, all_scores_for_ds in to_merge.items():
        # get score with worst metric
        scores_and_metrics = [(1-o.score, o.origin.metric_score, o) for o in all_scores_for_ds]

        # PIPE-634: hif_applycal crashes with TypeError: '>' not supported
        # between instances of 'QAScore' and 'QAScore'
        #
        # When the score and metric score are equal, max starts comparing the
        # QAScores themselves, which fails as the comparison operators are
        # not implemented. From the perspective of a 'worst score' calculation
        # the scores are equal so it doesn't matter which one we take. Hence,
        # we can supply an ordering function which simply excludes the QAScore
        # object from the calculation.
        def omit_qascore_instance(t):
            return t[0], t[1]

        worst_score = max(scores_and_metrics, key=omit_qascore_instance)[2]

        c = copy.deepcopy(worst_score)
        c.applies_to = pqa.TargetDataSelection(**ds._asdict())

        result[ds] = [c]

    return result


def discard_dimension(to_merge: DataSelectionToScores, attrs: Iterable[str]) -> DataSelectionToScores:
    """
    Aggregate QA scores held in one or more DataSelection dimensions,
    discarding data selection indices for those dimensions.

    This function discards DataSelection dimensions. Say four QA scores were
    registered, one each to spws 16, 18, 20, and 22. Calling this function
    with attrs=['spw'] would combine those scores into a single DataSelection
    with spw='', i.e., spw data selection is left unspecified.
    """
    new_dsts = {}
    for data_selection, qa_scores in to_merge.items():
        new_attrs = {attr: tuple() for attr in attrs}
        new_ds = data_selection._replace(**new_attrs)

        # note that the QA scores themselves still have the data selection
        # specifiers in their .applies_to. For example, when asked to discard
        # pol, the DataSelection keys in the the result object would have
        # pol='' but the list of QA scores held as a value for that key would
        # still have pol=0 or pol=1, etc.
        if new_ds in new_dsts:
            new_dsts[new_ds].extend(qa_scores)
        else:
            new_dsts[new_ds] = copy.deepcopy(qa_scores)
    return new_dsts


def map_data_selection_to_scores(scores: Iterable[pqa.QAScore]) -> DataSelectionToScores:
    """
    Expand QAScores to a dict-based data structure that maps data selections
    to the QA scores applicable to that selection.

    :param scores: scores to decompose
    """
    return {to_data_selection(score.applies_to): [score] for score in scores}


def compress_data_selections(to_merge: DataSelectionToScores,
                             attrs_to_merge: Reversible[str]) -> DataSelectionToScores:
    """
    Combine adjacent data selections to give a new data structure that
    expresses the same data selection but in a more compressed form.

    A data selection applies over various dimensions: spw, field, pol, etc..
    This function merges data selections hierarchically, identifying adjacent
    data selections per data selection dimension given in attrs_to_merge, and
    concatenating those adjacent dimension indices together.
    """
    to_merge = copy.deepcopy(to_merge)

    # We can identify data selections that apply to the same data except for a
    # particular field by creating a new tuple that omits that field and
    # sorting/grouping on the reduced tuple. This function is used to create
    # the reduced tuple that 'ignores' the specified field(s)
    def get_keyfunc(cols_to_ignore: List[str]):
        def keyfunc(ds: DataSelection):
            return tuple(getattr(ds, field) for field in ds._fields if field not in cols_to_ignore)
        return keyfunc

    # hierarchical merging of tuple fields, grouping/merging in reverse order of
    # DataSelection fields. This has the effect of merging DataSelections from
    # the bottom up, e.g., first merge data selections that differ only in pol,
    # then merge data selections that differ only in ant, etc.
    keys_to_merge = to_merge.keys()
    for attr in reversed(attrs_to_merge):
        key_func = get_keyfunc([attr])
        keys_to_merge = sorted(keys_to_merge, key=key_func)
        to_add = {}
        to_del = []
        for k, g in itertools.groupby(keys_to_merge, key_func):
            # k is the data selection minus the ignored field, while g
            # iterates over the data selections matching k that differ only in
            # the ignored field. These selections in g can be merged.
            group = list(g)
            # convert from ((1, ), (2, ), (5, )) to (1, 2, 5)
            merged_vals = tuple(itertools.chain(*(getattr(g, attr) for g in group)))

            # we now need to reconstruct the full data selection tuple from the
            # reduced tuple in k plus the values we've just merged. We do this
            # by created a dict of DataSelection named arguments for everything
            # except the field we've merged...
            d = {o: p for p, o in zip(k, (f for f in DataSelection._fields if f != attr))}
            # ... and then adding the merged field with the merged value
            d[attr] = merged_vals

            merged_ds = DataSelection(**d)
            to_add[merged_ds] = list(itertools.chain(*[to_merge[o] for o in group]))
            to_del.extend(group)

        for k in to_del:
            del to_merge[k]
        to_merge.update(to_add)

        # replace the original unmerged data with our merged selections and
        # we're ready to go round again for the next field
        keys_to_merge = list(to_merge.keys())

    return to_merge
