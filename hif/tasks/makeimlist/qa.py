import collections

import pipeline.infrastructure.logging as logging
import pipeline.infrastructure.pipelineqa as pqa
import pipeline.infrastructure.utils as utils
from . import resultobjects

LOG = logging.get_logger(__name__)


class MakeImListQAHandler(pqa.QAPlugin):
    result_cls = resultobjects.MakeImListResult
    child_cls = None

    def handle(self, context, result):
        # There are two types of scores:
        # S1: comparing number of targets against expected number
        # S2: comparing number of good spws against number of all spws (in
        #     cont targets)

        # Score 1
        if result.mitigation_error:
            score = 0.0
            longmsg = 'Size mitigation error. No targets were created.'
            shortmsg = 'Size mitigation error.'
        elif result.error:
            score = 0.0
            longmsg = shortmsg = result.error_msg
        elif result.expected_num_targets == 0:
            score = None
            longmsg = 'No clean targets expected.'
            shortmsg = 'No clean targets expected'
        else:
            score = float(result.num_targets)/float(result.expected_num_targets)
            longmsg, shortmsg = ('All clean targets defined', '') if score == 1.0 else \
                ('Expected %d clean targets but got only %d.' % \
                 (result.expected_num_targets, result.num_targets), \
                 'Expected %d clean targets' % (result.expected_num_targets))
        result.qa.pool[:] = [pqa.QAScore(score, longmsg=longmsg, shortmsg=shortmsg)]

        # Score 2
        for target in result.targets:
            if target['specmode'] == 'cont':
                num_bad_spws = target['num_all_spws'] - target['num_good_spws']
                num_bad_spw_ratio = float(num_bad_spws) / float(target['num_all_spws'])
                if num_bad_spws == 0:
                    score = 1.0
                    longmsg = shortmsg = 'Continuum range found'
                elif target['num_good_spws'] == 0:
                    score = 0.3
                    longmsg = 'No continuum ranges found for any spw for cont image of {!s}.'.format(target['field'])
                    shortmsg = 'No continuum ranges'
                else:
                    if num_bad_spws == 1 and num_bad_spw_ratio < 0.5:
                        score = 0.9
                    elif num_bad_spws == 2 or num_bad_spw_ratio == 0.5:
                        score = 0.6
                    else:
                        score = 0.5
                    longmsg = 'Missing continuum ranges for cont image of {!s}.'.format(target['field'])
                    shortmsg = 'Missing continuum ranges'
            else:
                score = 1.0
                longmsg = shortmsg = 'Continuum range found'
            result.qa.pool.append(pqa.QAScore(score, longmsg=longmsg, shortmsg=shortmsg))


class MakeImListListQAHandler(pqa.QAPlugin):
    result_cls = collections.abc.Iterable
    child_cls = resultobjects.MakeImListResult

    def handle(self, context, result):
        # collate the QAScores from each child result, pulling them into our
        # own QAscore list
        collated = utils.flatten([r.qa.pool for r in result])
        result.qa.pool[:] = collated
