import collections

import pipeline.infrastructure.logging as logging
import pipeline.infrastructure.pipelineqa as pqa
import pipeline.infrastructure.utils as utils
import pipeline.qa.scorecalculator as qacalc
from . import diffgaincal

LOG = logging.get_logger(__name__)


class DiffgaincalQAHandler(pqa.QAPlugin):
    """
    QA handler for an uncontained DiffgaincalResults.
    """
    result_cls = diffgaincal.DiffGaincalResults
    child_cls = None
    generating_task = diffgaincal.DiffGaincal

    def handle(self, context, result):
        vis = result.inputs['vis']
        ms = context.observing_run.get_ms(vis)

        scores = []

        # Only create QA scores for BandToBand measurement sets:
        if ms.is_band_to_band:
            # Create QA score for whether the diffgain phase caltable was created
            # successfully.
            if result.final:
                gaintable = result.final[0].gaintable
            elif result.error:
                gaintable = list(result.error)[0].gaintable
            else:
                gaintable = None

            scores.append(qacalc.score_path_exists(ms.name, gaintable, 'caltable'))

        # Add all scores to the QA pool
        result.qa.pool.extend(scores)


class DiffgaincalListQAHandler(pqa.QAPlugin):
    """
    QA handler for a list containing DiffgaincalResults.
    """
    result_cls = collections.Iterable
    child_cls = diffgaincal.DiffGaincalResults
    generating_task = diffgaincal.DiffGaincal

    def handle(self, context, result):
        # collate the QAScores from each child result, pulling them into our
        # own QAscore list
        collated = utils.flatten([r.qa.pool for r in result])
        result.qa.pool[:] = collated

        mses = [r.inputs['vis'] for r in result]
        longmsg = f"Diffgain phase caltables created for {utils.commafy(mses, quotes=False, conjunction='and')}"
        result.qa.all_unity_longmsg = longmsg
