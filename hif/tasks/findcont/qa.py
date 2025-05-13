import collections

import pipeline.infrastructure.logging as logging
import pipeline.infrastructure.pipelineqa as pqa
import pipeline.infrastructure.utils as utils

from . import resultobjects

LOG = logging.get_logger(__name__)


class FindContQAHandler(pqa.QAPlugin):
    result_cls = resultobjects.FindContResult
    child_cls = None

    def handle(self, context, result):

        scores = []

        if context.vla_skip_mfs_and_cube_imaging:
            scores.append(
                pqa.QAScore(
                    None, longmsg='Skip the VLA continuum finding due to absence of required datatype: CONTLINE_SCIECNE',
                    shortmsg='Stage skipped'))
            result.qa.pool.extend(scores)
            return

        score1 = self._found_ranges(result)
        scores.append(score1)

        for xx in result.single_range_channel_fractions:
            newscore = self._single_range_channel_fraction_score(xx)
            scores.append(newscore)

        result.qa.pool.extend(scores)

    def _found_ranges(self, result):
        if result.mitigation_error:
            score = 0.0
            longmsg = 'Size mitigation error. No targets were processed.'
            shortmsg = 'Size mitigation error.'
        elif result.num_total != 0:
            score = float(result.num_found) / float(result.num_total)
            longmsg, shortmsg = ('Found continuum ranges', '') if score == 1.0 else \
                ('Found only %d of %d continuum ranges' % (result.num_found, result.num_total), 'Missing continuum ranges')
        else:
            score = 0.0
            longmsg = 'No clean targets were defined. Can not run continuum finding.'
            shortmsg = 'No clean targets defined'
        return pqa.QAScore(score, longmsg=longmsg, shortmsg=shortmsg)

    def _single_range_channel_fraction_score(self, entry):
        if entry.get('fraction') < 0.05:
            if entry.get('is_repsource'):
                score = 0.4
            else:
                score = 0.6
            longmsg = ('Only a single narrow range of channels was found for continuum in '
                       '{field} in spw {spw}, so the continuum subtraction '
                       'may be poor for that spw.'.format(field=entry.get('field'), spw=entry.get('spw')))
            shortmsg = 'Single narrow range found.'
        else:
            score = 1.0
            longmsg = ('Found more than a single narrow range of channels for continuum in '
                       '{field} in spw {spw}.'.format(field=entry.get('field'), spw=entry.get('spw')))
            shortmsg = 'Found more than a single narrow range.'
        return pqa.QAScore(score, longmsg=longmsg, shortmsg=shortmsg)


class FindContListQAHandler(pqa.QAPlugin):
    result_cls = collections.abc.Iterable
    child_cls = resultobjects.FindContResult

    def handle(self, context, result):
        # collate the QAScores from each child result, pulling them into our
        # own QAscore list
        collated = utils.flatten([r.qa.pool for r in result]) 
        result.qa.pool[:] = collated
