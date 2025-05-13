"""QA handling for hsd_atmcor stage."""
import collections
import os

import pipeline.infrastructure.logging as logging
import pipeline.infrastructure.pipelineqa as pqa
import pipeline.infrastructure.utils as utils
from . import atmcor
from pipeline.infrastructure.launcher import Context

LOG = logging.get_logger(__name__)


class SDATMCorrectionQAHandler(pqa.QAPlugin):
    """QA handler for hsd_atmcor stage."""

    result_cls = atmcor.SDATMCorrectionResults
    child_cls = None

    def handle(self, context: Context, result: result_cls):
        """Generate QA score for hsd_atmcor.

        Generate QA score for hsd_atmcor and register it to the result.
        Handle single results instance.

        Args:
            context: pipeline context
            result: results instance
        """
        atmcor_ms_name = result.atmcor_ms_name
        is_outfile_exists = os.path.exists(atmcor_ms_name)
        task_exec_status = result.success
        is_successful = (task_exec_status is True) and (is_outfile_exists is True)

        vis = os.path.basename(result.inputs['vis'])

        if is_successful:
            shortmsg = 'Execution of sdatmcor was successful'
            longmsg = f'Execution of sdatmcor for {vis} was successful'
            score = 1.0
        else:
            shortmsg = 'Execution of sdatmcor failed'
            longmsg = f'Execution of sdatmcor for {vis} failed. output MS may be created but will be corrupted.'
            score = 0.0
        scores = [pqa.QAScore(score, longmsg=longmsg, shortmsg=shortmsg)]

        result.qa.pool.extend(scores)


class SDATMCorrectionListQAHandler(pqa.QAPlugin):
    """QA handler for hsd_atmcor stage."""

    result_cls = collections.abc.Iterable
    child_cls = atmcor.SDATMCorrectionResults

    def handle(self, context: Context, result: result_cls):
        """Generate QA score for hsd_atmcor.

        Generate QA score for hsd_atmcor and register it to the result.
        Handles list of results using handler specified by child_cls.

        Args:
            context: pipeline context
            result: list of results instance
        """
        # collate the QAScores from each child result, pulling them into our
        # own QAscore list
        collated = utils.flatten([r.qa.pool for r in result])
        result.qa.pool[:] = collated
