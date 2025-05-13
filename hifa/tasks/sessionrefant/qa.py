import collections

import pipeline.infrastructure.logging as logging
import pipeline.infrastructure.pipelineqa as pqa
import pipeline.infrastructure.utils as utils
from . import resultobjects
from . import sessionrefant

LOG = logging.get_logger(__name__)


class SessionRefAntListQAHandler(pqa.QAPlugin):
    """
    QA plugin to process lists of SessionRefAntResults.
    """
    result_cls = collections.abc.Iterable
    child_cls = resultobjects.SessionRefAntResults
    generating_task = sessionrefant.SessionRefAnt

    def handle(self, context, result):
        # collate the QAScores from each child result, pulling them into our
        # own QAscore list
        collated = utils.flatten([r.qa.pool for r in result])
        result.qa.pool[:] = collated


class SessionRefAntQAHandler(pqa.QAPlugin):
    """
    QA plugin to handle a singular SessionRefAntResults.

    This plugin creates a score for each session in the results, based on
    whether a final best reference antenna was found for the session.
    """
    # Register this QAPlugin as handling singular SessionRefAntResults object
    # returned by SessionRefAnt only.
    result_cls = resultobjects.SessionRefAntResults
    child_cls = None
    generating_task = sessionrefant.SessionRefAnt

    def handle(self, context, result: resultobjects.SessionRefAntResults):

        scores = []
        # If sessions were identified, check for presence of final refant.
        if result.refant:
            for session_name, session_info in result.refant.items():
                if session_info['refant']:
                    qa_score = 1.0
                    longmsg = "Reference antenna for session {} was selected successfully".format(session_name)
                    shortmsg = "Refant OK"
                else:
                    qa_score = 0.0
                    longmsg = "Could not select reference antenna for session {}".format(session_name)
                    shortmsg = "No refant"

                origin = pqa.QAOrigin(metric_name='SessionRefAntQAHandler',
                                      metric_score=bool(qa_score),
                                      metric_units='Reference antenna was identified for session')

                scores.append(pqa.QAScore(qa_score, longmsg=longmsg, shortmsg=shortmsg, origin=origin))
        # Otherwise, the lack of identifiable sessions results in a score of 0.
        else:
            origin = pqa.QAOrigin(metric_name='SessionRefAntQAHandler',
                                  metric_score=False,
                                  metric_units='Sessions were identified')

            scores.append(pqa.QAScore(0.0, longmsg="No sessions found", shortmsg="No sessions found", origin=origin))

        result.qa.pool[:] = scores
