import collections

import pipeline.infrastructure.logging as logging
import pipeline.infrastructure.pipelineqa as pqa
import pipeline.infrastructure.utils as utils
import pipeline.qa.scorecalculator as qacalc
from . import Finalcals
from . import FinalcalsResults


LOG = logging.get_logger(__name__)


class FinalcalsQAHandler(pqa.QAPlugin):
    result_cls = FinalcalsResults
    child_cls = None
    generating_task = Finalcals

    def handle(self, context, result):
        # get a QA score for fraction of failed (flagged) bandpass solutions in the bandpass table
        # < 5%   of data flagged  --> 1
        # 5%-60% of data flagged  --> 1 to 0
        # > 60%  of data flagged  --> 0

        m = context.observing_run.get_ms(result.inputs['vis'])

        if result.flaggedSolnApplycalbandpass and result.flaggedSolnApplycaldelay:
            self._checkKandBsolution(result.flaggedSolnApplycaldelay, m)
            self._checkKandBsolution(result.flaggedSolnApplycalbandpass, m)

            score1 = qacalc.score_total_data_flagged_vla_bandpass(
                result.bpdgain_touse, result.flaggedSolnApplycalbandpass['antmedian']['fraction'])
            score2 = qacalc.score_total_data_vla_delay(result.ktypecaltable, m)
            scores = [score1, score2]
        else:
            LOG.error('Error with bandpass and/or delay table.')
            scores = [pqa.QAScore(0.0, longmsg='No flagging stats about the bandpass table or info in delay table.',
                                  shortmsg='Bandpass or delay table problem.')]

        result.qa.pool.extend(scores)

    @staticmethod
    def _checkKandBsolution(table, m):

        antenna_names = [a.name for a in m.antennas]

        for antenna in table['antspw']:
            spwcollect = []
            for spw in table['antspw'][antenna]:
                for pol in table['antspw'][antenna][spw]:
                    frac = table['antspw'][antenna][spw][pol]['fraction']
                    if frac == 1.0:
                        spwcollect.append(str(spw))
            if len(spwcollect) > 1:
                spwcollect = sorted(set(spwcollect))
                spwcollect = [int(spw) for spw in spwcollect]
                spwcollect.sort()
                spwcollect = [str(spw) for spw in spwcollect]
                LOG.warning('Antenna {!s}, spws: {!s} have a flagging fraction of 1.0.'
                            ''.format(antenna_names[antenna], ','.join(spwcollect)))

        return


class FinalcalsListQAHandler(pqa.QAPlugin):
    """
    QA handler for a list containing FinalcalsResults.
    """
    result_cls = collections.abc.Iterable
    child_cls = FinalcalsResults

    def handle(self, context, result):
        # collate the QAScores from each child result, pulling them into our
        # own QAscore list
        collated = utils.flatten([r.qa.pool for r in result])
        result.qa.pool.extend(collated)
