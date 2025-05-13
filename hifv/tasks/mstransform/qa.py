import collections
import os

import pipeline.infrastructure.logging as logging
import pipeline.infrastructure.pipelineqa as pqa
import pipeline.infrastructure.utils as utils
import pipeline.qa.scorecalculator as qacalc
from . import mstransform

LOG = logging.get_logger(__name__)


class VlaMstransformQAHandler(pqa.QAPlugin):
    result_cls = mstransform.VlaMstransformResults
    child_cls = None
    generating_task = mstransform.VlaMstransform

    def handle(self, context, result):

        # Check for existence of the science targets cont+line MS.
        score1 = self._targets_ms_exists(os.path.dirname(result.outputvis), os.path.basename(result.outputvis))
        scores = [score1]

        result.qa.pool.extend(scores)

    def _targets_ms_exists(self, output_dir, target_ms):
        """
        Check for the existence of the science targets cont+line MS
        """
        return qacalc.score_path_exists(output_dir, target_ms, 'science targets cont+line ms')


class VlaMstransformListQAHandler(pqa.QAPlugin):
    """
    QA handler for a list containing MstransformResults.
    """
    result_cls = collections.abc.Iterable
    child_cls = mstransform.VlaMstransformResults
    generating_task = mstransform.VlaMstransform

    def handle(self, context, result):
        # collate the QAScores from each child result, pulling them into our
        # own QAscore list
        collated = utils.flatten([r.qa.pool for r in result])
        result.qa.pool[:] = collated
        mses = [r.inputs['vis'] for r in result]
        longmsg = 'No missing science targets cont+line MS(s) for %s' % utils.commafy(mses, quotes=False, conjunction='or')
        result.qa.all_unity_longmsg = longmsg
