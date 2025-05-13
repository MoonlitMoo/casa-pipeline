import collections

import pipeline.infrastructure.logging as logging
import pipeline.infrastructure.pipelineqa as pqa
import pipeline.infrastructure.utils as utils
from pipeline.h.tasks.common import commonfluxresults
from . import vlasetjy

LOG = logging.get_logger(__name__)


class VLASetjyQAHandler(pqa.QAPlugin):
    result_cls = commonfluxresults.FluxCalibrationResults
    child_cls = None
    generating_task = vlasetjy.VLASetjy

    def handle(self, context, result):
        standard_source_names, standard_source_fields = vlasetjy.standard_sources(result.inputs['vis'])

        m = context.observing_run.get_ms(result.inputs['vis'])

        if sum(standard_source_fields, []):
            scorevalue = 0.5  # Default if a standard source position is found, but no intents.
            msg = 'QA No VLA standard calibrator present, continuing with the FLUX calibrator assuming a flux of 1 Jy.'
            for i, fields in enumerate(standard_source_fields):
                for myfield in fields:
                    domainfield = m.get_fields(myfield)[0]
                    if 'AMPLITUDE' in domainfield.intents:
                        scorevalue = 1.0
                        msg = 'Standard calibrator present.'
            score = pqa.QAScore(scorevalue, longmsg=msg, shortmsg=msg)
        else:
            score = pqa.QAScore(0.5,
                                longmsg='QA No VLA standard calibrator present, continuing with the FLUX calibrator'
                                        ' assuming a flux of 1 Jy.',
                                shortmsg='No standard calibrator present.')
            LOG.warning('QA No VLA standard calibrator present, continuing with the FLUX calibrator assuming a flux of 1'
                        ' Jy.')

        scores = [score]

        result.qa.pool.extend(scores)

        """
        vis = result.inputs['vis']
        ms = context.observing_run.get_ms(vis)
        if 'spw' in result.inputs:
            spw = result.inputs['spw']
        else:
            spw = ''

        # Check for the existence of the expected flux measurements
        # and assign a score based on the fraction of actual to
        # expected ones.
        scores = [qacalc.score_setjy_measurements(ms, result.inputs['field'],
                  result.inputs['intent'], spw, result.measurements)]
        result.qa.pool[:] = scores
        result.qa.all_unity_longmsg = 'No missing flux measurements in %s' % ms.basename
        """


class VLASetjyListQAHandler(pqa.QAPlugin):
    """
    QA handler for a list containing FluxCalibrationResults.
    """
    result_cls = collections.abc.Iterable
    child_cls = commonfluxresults.FluxCalibrationResults

    def handle(self, context, result):
        # collate the QAScores from each child result, pulling them into our
        # own QAscore list
        collated = utils.flatten([r.qa.pool for r in result])
        result.qa.pool[:] = collated

        mses = [r.inputs['vis'] for r in result]
        longmsg = 'No missing flux measurements in %s' % utils.commafy(mses, quotes=False, conjunction='or')
        result.qa.all_unity_longmsg = longmsg
