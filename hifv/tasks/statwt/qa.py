import collections

import pipeline.infrastructure.logging as logging
import pipeline.infrastructure.pipelineqa as pqa
import pipeline.infrastructure.utils as utils
import pipeline.qa.scorecalculator as qacalc
from . import statwt

LOG = logging.get_logger(__name__)


class StatwtQAHandler(pqa.QAPlugin):
    result_cls = statwt.StatwtResults
    child_cls = None
    generating_task = statwt.Statwt

    def handle(self, context, result):
        vis = result.inputs['vis']
        ms = context.observing_run.get_ms(vis)

        # Score based on incremental flag fraction
        score0 = qacalc.score_data_flagged_by_agents(ms, result.summaries, 0.05, 0.6, agents=['statwt'])
        new_origin = pqa.QAOrigin(metric_name='%StatwtFlagging',
                                  metric_score=score0.origin.metric_score,
                                  metric_units=score0.origin.metric_units)
        score0.origin = new_origin
        scores = [score0]
        result.qa.pool.extend(scores)

        # Score based on overall mean and variance for VLA PI Pipeline
        if result.inputs['statwtmode'] == 'VLA' :
            mean =  result.jobs[0]['mean']
            variance = result.jobs[0]['variance'] 
            stats_origin = pqa.QAOrigin(metric_name='%StatwtStats',
                                    metric_score=(mean, variance),
                                    metric_units='')

            if mean > 1000000 and variance > 5000000:
                score = 0.0
                shortmsg = 'Very high mean and variance of weights'
                longmsg = 'Very High mean and variance of weights; bad weights are very likely to be present and require flagging'
                result.qa.pool.append(pqa.QAScore(score, longmsg=longmsg, shortmsg=shortmsg, vis=vis, origin=stats_origin))
            elif mean > 10000 and variance > 500000:
                score = 0.1
                shortmsg = 'High mean and variance of weights'
                longmsg = 'High mean and variance of weights; possibly erroneous weights present that may require flagging'
                result.qa.pool.append(pqa.QAScore(score, longmsg=longmsg, shortmsg=shortmsg, vis=vis, origin=stats_origin))
            elif mean > 1000 and variance > 50000:
                score=0.75
                shortmsg = 'Moderately high mean and variance for weights.'
                longmsg = 'Moderately high mean and variance for weight; possibly erroneous weights present that may require flagging.'
                result.qa.pool.append(pqa.QAScore(score, longmsg=longmsg, shortmsg=shortmsg, vis=vis, origin=stats_origin))


class StatwtListQAHandler(pqa.QAPlugin):
    """
    QA handler for a list containing StatwtResults.
    """
    result_cls = collections.abc.Iterable
    child_cls = statwt.StatwtResults
    generating_task = statwt.Statwt

    def handle(self, context, result):
        # collate the QAScores from each child result, pulling them into our
        # own QAscore list
        collated = utils.flatten([r.qa.pool for r in result])
        result.qa.pool[:] = collated
