import collections

import pipeline.infrastructure.logging as logging
import pipeline.infrastructure.pipelineqa as pqa
import pipeline.infrastructure.utils as utils
import pipeline.qa.scorecalculator as qacalc
from . import spwphaseup

LOG = logging.get_logger(__name__)


class SpwPhaseupQAHandler(pqa.QAPlugin):
    result_cls = spwphaseup.SpwPhaseupResults
    child_cls = None
    generating_task = spwphaseup.SpwPhaseup

    def handle(self, context, result):
        vis = result.inputs['vis']
        ms = context.observing_run.get_ms(vis)

        scores = []

        # Create QA score for whether the phaseup caltable was created successfully.
        if not result.phaseup_result.final:
            gaintable = list(result.phaseup_result.error)[0].gaintable
        else:
            gaintable = result.phaseup_result.final[0].gaintable
        scores.append(qacalc.score_path_exists(ms.name, gaintable, 'caltable'))

        # Create QA scores for median SNR per field and per SpW, but skip this
        # for SpWs that have been re-mapped.
        for (intent, field, spw), median_snr in result.snr_info.items():
            # Skip if encountering unhandled intent.
            if intent not in ['CHECK', 'PHASE']:
                LOG.warning(f"{ms.basename}: unexpected intent '{intent}' encountered in SNR info result, cannot"
                            f" assign a QA score.")
                continue

            # Get SpW mapping info for current intent and field.
            spwmapping = result.spwmaps.get((intent, field), None)

            # If SpW mapping info exists for the current intent and field and
            # the current SpW is not mapped to itself, then skip the QA score
            # calculation. Note: if the SpW map is empty, that means by default
            # that each SpW is mapped to itself, i.e. QA scoring gets run for
            # current SpW.
            if spwmapping and spwmapping.spwmap and spwmapping.spwmap[spw] != spw:
                continue

            # Check which QA score heuristic to use, based on intent.
            if intent == 'CHECK':
                score = qacalc.score_phaseup_spw_median_snr_for_check(ms, field, spw, median_snr,
                                                                      result.inputs['phasesnr'])
            elif intent == 'PHASE':
                score = qacalc.score_phaseup_spw_median_snr_for_phase(ms, field, spw, median_snr,
                                                                      result.inputs['phasesnr'])

            # If SpW mapping info exists for the current intent and field, and
            # there is a non-empty SpW map in which other SpWs are mapped to
            # current SpW, then mention this in the QA score message.
            if spwmapping and spwmapping.spwmap and spwmapping.spwmap.count(spw) > 1:
                score.longmsg += f' This SpW has one or more other SpWs mapped to it.'

            # Add score to list of scores.
            scores.append(score)

        # QA scores for decoherence assessment (See: PIPE-692 and PIPE-1624)
        if result.phaserms_results:
            decoherence_score = qacalc.score_decoherence_assessment(ms, result.phaserms_results, result.phaserms_antout)
        else:
            # "missing results" decoherence assessment QA score
            base_score = 0.9
            shortmsg = "Cannot assess Phase RMS."
            longmsg = 'Unable to assess the Phase RMS decoherence, for {}.'.format(ms.basename)

            phase_stability_origin = pqa.QAOrigin(metric_name='Phase stability',
                                        metric_score=None,
                                        metric_units='Degrees')
            decoherence_score = pqa.QAScore(base_score, longmsg=longmsg, shortmsg=shortmsg, vis=ms.basename,
                                            origin=phase_stability_origin, weblog_location=pqa.WebLogLocation.ACCORDION)

        scores.append(decoherence_score)

        # Add all scores to the QA pool
        result.qa.pool.extend(scores)


class SpwPhaseupListQAHandler(pqa.QAPlugin):
    """
    QA handler for a list containing SpwPhaseupResults.
    """
    result_cls = collections.abc.Iterable
    child_cls = spwphaseup.SpwPhaseupResults
    generating_task = spwphaseup.SpwPhaseup

    def handle(self, context, result):
        # collate the QAScores from each child result, pulling them into our
        # own QAscore list
        collated = utils.flatten([r.qa.pool for r in result])
        result.qa.pool[:] = collated

        mses = [r.inputs['vis'] for r in result]
        longmsg = 'No mapped narrow spws in %s' % utils.commafy(mses, quotes=False, conjunction='or')
        result.qa.all_unity_longmsg = longmsg
