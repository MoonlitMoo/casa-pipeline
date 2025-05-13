import pipeline.infrastructure.basetask as basetask
import pipeline.infrastructure.logging as logging
import pipeline.infrastructure.pipelineqa as pqa
import pipeline.qa.scorecalculator as qacalc
import pipeline.infrastructure.utils as utils

from . import restoredata

LOG = logging.get_logger(__name__)

class RestoreDataQAHandler(pqa.QAPlugin):
    result_cls = restoredata.RestoreDataResults
    child_cls = None

    def handle(self, context, result):
        # Check to see if renorm was applied
        score1 = qacalc.score_renorm(result)
        scores = [score1]

        result.qa.pool.extend(scores)

class RestoreDataListQAHandler(pqa.QAPlugin):
    """
    QA handler for a list containing RestoreResults.
    """
    result_cls = basetask.ResultsList
    child_cls = restoredata.RestoreDataResults

    def handle(self, context, result):
        # collate the QAScores from each child result, pulling them into our
        # own QAscore list
        collated = utils.flatten([r.qa.pool for r in result])
        result.qa.pool[:] = collated
