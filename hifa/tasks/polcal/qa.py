import collections

import pipeline.infrastructure.logging as logging
import pipeline.infrastructure.pipelineqa as pqa
import pipeline.infrastructure.utils as utils
import pipeline.qa.scorecalculator as qacalc
from pipeline.h.tasks.common import commonhelpermethods
from . import polcal

LOG = logging.get_logger(__name__)


class PolcalQAHandler(pqa.QAPlugin):
    """
    QA handler for an uncontained PolcalResults.
    """
    result_cls = polcal.PolcalResults
    child_cls = None
    generating_task = polcal.Polcal

    def handle(self, context, result):
        scores = []

        for session_name, session_result in result.session.items():
            # Create QA score for whether the session was calibrated.
            scores.append(qacalc.score_polcal_results(session_name, session_result.final))

            # Skip remaining QA scores for this session if no valid results were
            # returned at all.
            if not session_result.final:
                continue

            # Get first MS from session for antenna ID > name translation.
            ms = context.observing_run.get_ms(name=session_result.vislist[0])
            ant_names, _ = commonhelpermethods.get_antenna_names(ms)

            # Create QA score for residual polarization (Q, U) after
            # polarization has been applied.
            scores.extend(qacalc.score_polcal_residual_pol(session_name, session_result.cal_pfg_result))

            # Create QA score for gain ratio RMS after polarization correction.
            scores.append(qacalc.score_polcal_gain_ratio_rms(session_name, session_result.gain_ratio_rms_after))

            # Create QA score for D-term solutions.
            scores.extend(qacalc.score_polcal_leakage(session_name, ant_names, session_result.leak_polcal_result))

            # Create QA score for gain ratios.
            scores.extend(qacalc.score_polcal_gain_ratio(session_name, ant_names, session_result.xyratio_gcal_result))

        # Add all scores to the QA pool
        result.qa.pool.extend(scores)


class PolcalListQAHandler(pqa.QAPlugin):
    """
    QA handler for a list containing PolcalResults.
    """
    result_cls = collections.abc.Iterable
    child_cls = polcal.PolcalResults
    generating_task = polcal.Polcal

    def handle(self, context, result):
        # collate the QAScores from each child result, pulling them into our
        # own QAscore list
        collated = utils.flatten([r.qa.pool for r in result])
        result.qa.pool[:] = collated
