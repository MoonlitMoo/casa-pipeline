"""QA score module for skycal task."""
import pipeline.infrastructure.basetask as basetask
import pipeline.infrastructure.logging as logging
import pipeline.infrastructure.pipelineqa as pqa
import pipeline.infrastructure.utils as utils
import pipeline.qa.scorecalculator as qacalc
import pipeline.h.tasks.exportdata.aqua as aqua
from . import skycal
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from pipeline.infrastructure.launcher import Context

LOG = logging.get_logger(__name__)


class SDSkyCalQAHandler(pqa.QAPlugin):
    """Class to handle QA score for skycal result."""

    result_cls = skycal.SDSkyCalResults
    child_cls = None

    def handle(self, context: 'Context', result: skycal.SDSkyCalResults) -> None:
        """Evaluate QA score for skycal result.

        Args:
            context: Pipeline context.
            result: SDSkyCalResults instance.
        """
        calapps = result.outcome
        resultdict = skycal.compute_elevation_difference(context, result)
        vis = calapps[0].calto.vis
        ms = context.observing_run.get_ms(vis)
        threshold = skycal.SerialSDSkyCal.ElevationDifferenceThreshold
        scores = qacalc.score_sd_skycal_elevation_difference(ms, resultdict, threshold=threshold)
        result.qa.pool.append(scores)


class SDSkyCalListQAHandler(pqa.QAPlugin):
    """Class to handle QA score for a list of skycal results."""

    result_cls = basetask.ResultsList
    child_cls = skycal.SDSkyCalResults

    def handle(self, context: 'Context', result: skycal.SDSkyCalResults) -> None:
        """Evaluate QA score for a list of skycal results.

        Args:
            context: Pipeline context (not used).
            result: List of SDSkyCalResults instances.
        """
        # collate the QAScores from each child result, pulling them into our
        # own QAscore list
        collated = utils.flatten([r.qa.pool for r in result]) 
        result.qa.pool[:] = collated


aqua_exporter = aqua.xml_generator_for_metric('OnOffElevationDifference', '{:0.3f}deg')
aqua.register_aqua_metric(aqua_exporter)
