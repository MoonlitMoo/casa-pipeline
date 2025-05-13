"""QA score handlers for k2jycal task."""
import collections

from typing import TYPE_CHECKING

import pipeline.infrastructure.logging as logging
import pipeline.infrastructure.pipelineqa as pqa
import pipeline.infrastructure.utils as utils
from . import k2jycal

if TYPE_CHECKING:
    from pipeline.infrastructure.launcher import Context
    from pipeline.infrastructure.basetask import ResultsList

LOG = logging.get_logger(__name__)


class SDK2JyCalQAHandler(pqa.QAPlugin):
    """Class to handle QA score for k2jycal result."""

    result_cls = k2jycal.SDK2JyCalResults
    child_cls = None

    def handle(self, context: 'Context', result: k2jycal.SDK2JyCalResults) -> None:
        """Evaluate QA score for k2jycal result.

        Score is 0.0 if

            - there is missing Jy/K factor, or,
            - accessed Jy/K DB and failed.

        Args:
            context: Pipeline context (not used)
            result: SDK2JyCalResults instance
        """
        is_missing_factor = (not result.all_ok)

        shortmsg = "Missing Jy/K factors for some data" if is_missing_factor else "Jy/K factors are found for all data"
        longmsg = shortmsg + (" in "+result.vis if result.vis is not None else "") + (". Those data will remain in the unit of Kelvin after applying the calibration tables." if is_missing_factor else "")
        score = 0.0 if is_missing_factor else 1.0
        scores = [pqa.QAScore(score, longmsg=longmsg, shortmsg=shortmsg)]

        # PIPE-384 lower the score to 0.8 if DB access was failed
        if result.dbstatus is not None:
            # the task attempted to access the DB
            if result.dbstatus is True:
                statusstr = 'successful'
                score = 1.0
            else:
                statusstr = 'failed'
                score = 0.0
                #score = 0.8
            shortmsg = "Accessing Jy/K DB was {}".format(statusstr)
            longmsg = shortmsg + " for " + (result.vis if result.vis is not None else "input vis")
            scores.append(pqa.QAScore(score, longmsg=longmsg, shortmsg=shortmsg))
        result.qa.pool.extend(scores)


class SDK2JyCalListQAHandler(pqa.QAPlugin):
    """Class to handle QA score for a list of k2jycal results."""

    result_cls = collections.abc.Iterable
    child_cls = k2jycal.SDK2JyCalResults

    def handle(self, context: 'Context', result: 'ResultsList') -> None:
        """Evaluate QA score for a list of k2jycal results.

        Args:
            context: Pipeline context (not used)
            result: List of SDK2JyCalResults instances
        """
        # collate the QAScores from each child result, pulling them into our
        # own QAscore list
        collated = utils.flatten([r.qa.pool for r in result])
        result.qa.pool[:] = collated
