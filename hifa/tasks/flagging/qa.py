import collections

import pipeline.infrastructure.logging as logging
import pipeline.infrastructure.pipelineqa as pqa
import pipeline.infrastructure.utils as utils
import pipeline.qa.scorecalculator as qacalc
from pipeline.h.tasks.exportdata import aqua
from . import flagdeteralma
from . import flagtargetsalma

LOG = logging.get_logger(__name__)

_lowtrans_qa_metric_name = 'LowTransmissionFlags'


class FlagDeterALMAQAHandler(pqa.QAPlugin):
    result_cls = flagdeteralma.FlagDeterALMAResults
    child_cls = None
    generating_task = flagdeteralma.FlagDeterALMA

    def handle(self, context, result):
        vis = result.inputs['vis']
        ms = context.observing_run.get_ms(vis)

        # Create QA score for flagging done by 'lowtrans' agent.
        score = qacalc.score_lowtrans_flagcmds(ms, result)

        # Modify origin with locally defined metric name to ensure
        new_origin = pqa.QAOrigin(metric_name=_lowtrans_qa_metric_name,
                                  metric_score=score.origin.metric_score,
                                  metric_units=score.origin.metric_units)
        score.origin = new_origin

        # Add score to the existing pool of QA scores. The latter may already
        # have been populated by other QA handlers that work on instances of
        # self.result_cls or its parent class(es).
        result.qa.pool.append(score)

        # PIPE-1759: aggregate former warning messages into a new QA score
        if result.missing_baseband_spws:
            result.qa.pool.append(pqa.QAScore(score=0.8,
                longmsg='Unable to determine baseband range for {}, spw{}, skipping ACA FDM edge flagging'.format(
                    ms.basename, utils.commafy(result.missing_baseband_spws, quotes=False, multi_prefix='s')),
                shortmsg='Unable to determine baseband range'))



class FlagDeterALMAListQAHandler(pqa.QAPlugin):
    result_cls = collections.abc.Iterable
    child_cls = flagdeteralma.FlagDeterALMAResults
    generating_task = flagdeteralma.FlagDeterALMA

    def handle(self, context, result):
        # collate the QAScores from each child result, pulling them into our
        # own QAscore list
        collated = utils.flatten([r.qa.pool for r in result])
        result.qa.pool[:] = collated


class FlagTargetsALMAQAHandler(pqa.QAPlugin):
    result_cls = flagtargetsalma.FlagTargetsALMAResults
    child_cls = None

    def handle(self, context, result):
        vis = result.inputs['vis']
        ms = context.observing_run.get_ms(vis)

        # calculate QA scores from agentflagger summary dictionary, adopting
        # the minimum score as the representative score for this task
        # Leave in the flag summary off option
        try:
            scores = [qacalc.score_almatargets_agents(ms, result.summaries)]
        except:
            scores = [pqa.QAScore(1.0, longmsg='Flag Summary off', shortmsg='Flag Summary off')]

        result.qa.pool[:] = scores


class FlagTargetsALMAListQAHandler(pqa.QAPlugin):
    result_cls = collections.abc.Iterable
    child_cls = flagtargetsalma.FlagTargetsALMAResults

    def handle(self, context, result):
        # collate the QAScores from each child result, pulling them into our
        # own QAscore list
        collated = utils.flatten([r.qa.pool for r in result])
        result.qa.pool[:] = collated


aqua_exporter = aqua.xml_generator_for_metric(_lowtrans_qa_metric_name, '{:0.3f}')
aqua.register_aqua_metric(aqua_exporter)
