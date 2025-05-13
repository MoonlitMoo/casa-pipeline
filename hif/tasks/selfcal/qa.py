"""QA score module for selfcal task."""
import collections

import pipeline.infrastructure.logging as logging
import pipeline.infrastructure.pipelineqa as pqa
import pipeline.infrastructure.utils as utils

from . import selfcal

LOG = logging.get_logger(__name__)


class SelfcalQAHandler(pqa.QAPlugin):
    result_cls = selfcal.SelfcalResults
    child_cls = None

    def handle(self, context, result):

        scores = []

        targets = []
        targets_exception = []          # An exception is triggered during the selfcal-solver execution
        targets_attempt = []            # At least one solint is attempted in the selfcal-solver.
        targets_success = []            # Selfcal is successful and a solution is applied
        targets_improved = []           # Selfcal is applied and the RMS is improved
        targets_unimproved = []         # Selfcal is applied but the RMS is not improved
        targets_mosaic = []             # Targets observed as mosaic

        for target in result.targets:

            if target['sc_exception']:
                # If a self-calibration worker encounters an exception, none of the "sc_*"" keys will exist in "target".
                # In this case, we collect the basic information and exit early.
                band = 'spw='+target['spw']
                targets.append((target['field_name'], band))
                if target.get('is_mosaic', None):
                    targets_mosaic.append((target['field_name'], band))
                targets_exception.append((target['field_name'], band))
                continue

            # if not exception, we will use the "sc_*"" keys.
            band = target['sc_band'].strip().replace('EVLA_', '').replace('_', ' ').capitalize()
            targets.append((target['field_name'], band))
            if target.get('is_mosaic', None):
                targets_mosaic.append((target['field_name'], band))

            slib = target['sc_lib']
            vislist = slib['vislist']
            solints = target['sc_solints']
            if not set(solints).isdisjoint(slib[vislist[-1]].keys()):
                targets_attempt.append((target['field_name'], band))
            else:
                continue
            if slib['SC_success']:
                targets_success.append((target['field_name'], band))
            else:
                continue
            if slib['RMS_final'] < 1.02*slib['RMS_orig'] and slib['RMS_final'] != -99.0 and slib['RMS_orig'] != -99.0:
                targets_improved.append((target['field_name'], band))
            else:
                targets_unimproved.append((target['field_name'], band))

        LOG.debug('QA targets: %s', targets)
        LOG.debug('QA targets_attempt: %s', targets_attempt)
        LOG.debug('QA targets_success: %s', targets_success)
        LOG.debug('QA targets_improved: %s', targets_improved)
        LOG.debug('QA targets_unimproved: %s', targets_unimproved)
        LOG.debug('QA targets_mosaic: %s', targets_mosaic)

        if not targets:
            score = None
            longmsg = 'No self-calibration attempted: unsupported modes (e.g., ephemeris targets), or no suitable datatype found.'
            shortmsg = 'No self-calibration attempted'
            scores.append(pqa.QAScore(score, longmsg=longmsg, shortmsg=shortmsg))

        if targets_mosaic:
            score = 0.90
            targets_desc = utils.commafy([name+f' ({band})' for name, band in targets_mosaic], quotes=False)
            longmsg = f'A new mode is used during self-calibration for {targets_desc}.'
            n_field = len(targets_mosaic)
            s_field = 'target field' if n_field == 1 else 'target fields'
            shortmsg = f'A new mode is used during self-calibration for {n_field} {s_field}.'
            scores.append(pqa.QAScore(score, longmsg=longmsg, shortmsg=shortmsg))

        if targets and not targets_exception and not targets_attempt:
            score = 1.0
            longmsg = f'No field has sufficient SNR to attempt self-calibration.'
            shortmsg = longmsg
            scores.append(pqa.QAScore(score, longmsg=longmsg, shortmsg=shortmsg))

        if targets_exception:
            score = 0.8
            targets_desc = utils.commafy([name+f' ({band})' for name, band in targets_exception], quotes=False)
            n_field = len(targets_exception)
            s_field = 'target field' if n_field == 1 else 'target fields'
            it_them = 'it' if n_field == 1 else 'them'
            longmsg = f'The self-calibration worker failed for {targets_desc} and no calibration solutions could be applied for {it_them}.'
            shortmsg = f'The self-calibration worker failed for {n_field} {s_field} and no calibration solutions could be applied for {it_them}.'
            scores.append(pqa.QAScore(score, longmsg=longmsg, shortmsg=shortmsg))

        if targets and targets_attempt and not targets_success:
            # pass
            score = 0.99
            longmsg = 'Self-calibration attempted but not applied for any fields.'
            shortmsg = longmsg
            scores.append(pqa.QAScore(score, longmsg=longmsg, shortmsg=shortmsg))

        if targets_success and not targets_unimproved:
            score = 0.98
            targets_desc = utils.commafy([name+f' ({band})' for name, band in targets_improved], quotes=False)
            longmsg = f'Self-calibrations applied for {targets_desc}.'
            n_field = len(targets_improved)
            s_field = 'target field' if n_field == 1 else 'target fields'
            shortmsg = f'Self-calibrations applied for {n_field} {s_field}.'
            scores.append(pqa.QAScore(score, longmsg=longmsg, shortmsg=shortmsg))

        if targets_success and targets_unimproved:
            score = 0.85
            targets_desc1 = utils.commafy([name+f' ({band})' for name, band in targets_success], quotes=False)
            targets_desc2 = utils.commafy([name+f' ({band})' for name, band in targets_improved], quotes=False)
            targets_desc3 = utils.commafy([name+f' ({band})' for name, band in targets_unimproved], quotes=False)
            n_field1 = len(targets_success)
            s_field1 = 'target field' if n_field1 == 1 else 'target fields'
            n_field2 = len(targets_improved)
            s_field2 = 'target field' if n_field2 == 1 else 'target fields'
            n_field3 = len(targets_unimproved)
            s_field3 = 'target field' if n_field3 == 1 else 'target fields'
            longmsg = []
            shortmsg = []
            if n_field1 > 0:
                longmsg.append(f'Self-calibrations applied for {targets_desc1}.')
                shortmsg.append(f'Self-calibrations applied for {n_field1} {s_field1}.')
            if n_field2 > 0:
                longmsg.append(f'SNR and RMS improved for {targets_desc2}.')
                shortmsg.append(f'SNR and RMS improved for {n_field2} {s_field2}.')
            if n_field3 > 0:
                longmsg.append(f'SNR improved but RMS increased by more than 2% for {targets_desc3}.')
                shortmsg.append(f'SNR improved but RMS increased by more than 2% for {n_field3} {s_field3}.')
            longmsg = ' '.join(longmsg)
            shortmsg = ' '.join(shortmsg)
            scores.append(pqa.QAScore(score, longmsg=longmsg, shortmsg=shortmsg))

        result.qa.pool[:] = scores


class SelfcalListQAHandler(pqa.QAPlugin):
    result_cls = collections.abc.Iterable
    child_cls = selfcal.SelfcalResults

    def handle(self, context, result):

        collated = utils.flatten([r.qa.pool for r in result])
        result.qa.pool[:] = collated
