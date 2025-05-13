import pipeline.infrastructure.basetask as basetask
import pipeline.infrastructure.logging as logging
import pipeline.infrastructure.pipelineqa as pqa
import pipeline.infrastructure.utils as utils
from . import resultobjects
from pipeline.infrastructure.refantflag import format_fully_flagged_antenna_notification

LOG = logging.get_logger(__name__)


class BandpassflagQAHandler(pqa.QAPlugin):
    """
    QA handler for an uncontained BandpassflagResults.
    """
    result_cls = resultobjects.BandpassflagResults
    child_cls = None

    def handle(self, context, result):
        # Run correctedampflag QA on correctedampflag result.
        pqa.qa_registry.do_qa(context, result.cafresult)

        # Store flagging score into result.
        result.qa.pool[:] = result.cafresult.qa.pool


class BandpassflagListQAHandler(pqa.QAPlugin):
    """
    QA handler for a list containing BandpassflagResults.
    """
    result_cls = basetask.ResultsList
    child_cls = resultobjects.BandpassflagResults

    def handle(self, context, results):
        # collate the QAScores from each child result, pulling them into our
        # own QAscore list
        scores = []
        for result in results:
            scores.append(result.qa.pool)
            try:
                for notification in result.fully_flagged_antenna_notifications:
                    score = pqa.QAScore(
                        0.8,
                        longmsg=format_fully_flagged_antenna_notification(result.inputs['vis'], notification),
                        shortmsg='Fully flagged antennas',
                        vis=result.inputs['vis'])
                    scores.append(score)
            except AttributeError:
                LOG.error('Unable to find the list of fully flagged antennas')

        results.qa.pool[:] = utils.flatten(scores)
