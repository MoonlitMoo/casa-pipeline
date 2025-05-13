import collections

import pipeline.infrastructure.logging as logging
import pipeline.infrastructure.pipelineqa as pqa
import pipeline.infrastructure.utils as utils
import pipeline.qa.scorecalculator as qacalc
from . import testBPdcals
from . import testBPdcalsResults


LOG = logging.get_logger(__name__)


class testBPdcalsQAHandler(pqa.QAPlugin):
    result_cls = testBPdcalsResults
    child_cls = None
    generating_task = testBPdcals

    def handle(self, context, result):
        # get a QA score for fraction of failed (flagged) bandpass solutions in the bandpass table
        # < 5%   of data flagged  --> 1
        # 5%-60% of data flagged  --> 1 to 0
        # > 60%  of data flagged  --> 0

        m = context.observing_run.get_ms(result.inputs['vis'])

        scores = []

        self.antspw = collections.defaultdict(list)

        for bandname, bpdgain_touse in result.bpdgain_touse.items():
            if result.flaggedSolnApplycalbandpass[bandname] and result.flaggedSolnApplycaldelay[bandname]:
                self._checkKandBsolution(result.flaggedSolnApplycaldelay[bandname], m)
                self._checkKandBsolution(result.flaggedSolnApplycalbandpass[bandname], m)

                score1 = qacalc.score_total_data_flagged_vla_bandpass(result.bpdgain_touse[bandname],
                                                                      result.flaggedSolnApplycalbandpass[bandname]['antmedian']['fraction'])
                score2 = qacalc.score_total_data_vla_delay(result.ktypecaltable[bandname], m)
                scores.append(score1)
                scores.append(score2)
            else:
                LOG.error('Error with bandpass and/or delay table for band {!s}.'.format(bandname))
                scores.append(pqa.QAScore(0.0,
                                          longmsg='No flagging stats about the bandpass table or info in delay table.',
                                          shortmsg='Bandpass or delay table problem.'))

            # get a QA score for flagging
            # 0%   of data flagged  --> 1
            # 0%-30% of data flagged  --> 1 to 0
            # > 30%  of data flagged  --> 0
            score3 = qacalc.score_flagged_vla_baddef(result.amp_collection[bandname],
                                                     result.phase_collection[bandname],
                                                     result.num_antennas[bandname])

            scores.append(score3)

        for antenna, spwlist in self.antspw.items():
            uniquespw = list(set(spwlist))
            uniquespwlist = [int(spw) for spw in uniquespw]
            uniquespwlist.sort()
            uniquespwlist = [str(spw) for spw in uniquespwlist]
            LOG.warning('Antenna {!s}, spws: {!s} have a flagging fraction of 1.0.'
                        ''.format(antenna, ','.join(uniquespwlist)))

        result.qa.pool.extend(scores)

    def _checkKandBsolution(self, table, m):

        antenna_names = [a.name for a in m.antennas]

        for antenna in table['antspw']:
            spwcollect = []
            for spw in table['antspw'][antenna]:
                for pol in table['antspw'][antenna][spw]:
                    frac = table['antspw'][antenna][spw][pol]['fraction']
                    if frac == 1.0:
                        spwcollect.append(int(spw))
            if len(spwcollect) > 1:
                spwcollect = sorted(set(spwcollect))
                spwcollect = [str(spw) for spw in spwcollect]
                self.antspw[antenna_names[antenna]].extend(spwcollect)
                # LOG.warning('Antenna {!s}, spws: {!s} have a flagging fraction of 1.0.'
                #         ''.format(antenna_names[antenna], ','.join(spwcollect)))

        return


class testBPdcalsListQAHandler(pqa.QAPlugin):
    """
    QA handler for a list containing testBPdcalsResults.
    """
    result_cls = collections.abc.Iterable
    child_cls = testBPdcalsResults

    def handle(self, context, result):
        # collate the QAScores from each child result, pulling them into our
        # own QAscore list
        collated = utils.flatten([r.qa.pool for r in result])
        result.qa.pool.extend(collated)
