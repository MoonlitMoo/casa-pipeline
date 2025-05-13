import collections
import os
import numpy as np

import pipeline.infrastructure.logging as logging
import pipeline.infrastructure.pipelineqa as pqa
import pipeline.infrastructure.utils as utils
import pipeline.qa.scorecalculator as qacalc
from . import Fluxboot
from . import FluxbootResults
from . import solint

LOG = logging.get_logger(__name__)


class SolintQAHandler(pqa.QAPlugin):
    result_cls = solint.SolintResults
    child_cls = None
    generating_task = solint.Solint

    def handle(self, context, result):

        # Check for existence of the the target MS.
        score1 = self._ms_exists(os.path.dirname(result.inputs['vis']), os.path.basename(result.inputs['vis']))
        scores = [score1]

        result.qa.pool.extend(scores)

    def _ms_exists(self, output_dir, ms):
        """
        Check for the existence of the target MS
        """
        return qacalc.score_path_exists(output_dir, ms, 'Solint')


class SolintListQAHandler(pqa.QAPlugin):
    """
    QA handler for a list containing SolintResults.
    """
    result_cls = collections.abc.Iterable
    child_cls = solint.SolintResults
    generating_task = solint.Solint

    def handle(self, context, result):
        # collate the QAScores from each child result, pulling them into our
        # own QAscore list
        collated = utils.flatten([r.qa.pool for r in result])
        result.qa.pool[:] = collated
        mses = [r.inputs['vis'] for r in result]
        longmsg = 'No missing target MS(s) for %s' % utils.commafy(mses, quotes=False, conjunction='or')
        result.qa.all_unity_longmsg = longmsg


class FluxbootQAHandler(pqa.QAPlugin):
    result_cls = FluxbootResults
    child_cls = None
    generating_task = Fluxboot

    def handle(self, context, result):
        # Get a QA score based on RMS of the residuals per receiver band and source

        m = context.observing_run.get_ms(result.inputs['vis'])
        weblog_results = {}
        webdicts = {}

        ms = os.path.basename(result.inputs['vis'])

        weblog_results[ms] = result.weblog_results

        # Sort into dictionary collections to prep for table
        webdicts[ms] = collections.defaultdict(list)
        for row in sorted(weblog_results[ms], key=lambda p: (p['source'], float(p['freq']))):
            webdicts[ms][row['source']].append({'freq': row['freq'], 'data': row['data'], 'error': row['error'],
                                                'fitteddata': row['fitteddata']})

        spix_list = []
        for spi_result in result.spindex_results:
            spix_list.append(spi_result["spix"])

        fractional_residuals = self.getFractionalResiduals(webdicts[ms])
        score1 = qacalc.score_vla_flux_residual_rms(fractional_residuals, len(result.spws), spix_list)
        scores = [score1]
        if scores == []:
            LOG.error('Error with computing flux density bootstrapping residuals')
            scores = [pqa.QAScore(0.0, longmsg='Unable to compute flux density bootstrapping residuals.',
                                  shortmsg='Fluxboot issue.')]

        result.qa.pool.extend(scores)

    def getFractionalResiduals(self, webdicts):
        """
        Compute the fractional residuals between observed and predicted data.

        Parameters:
            webdicts (dictionary) : A dictionary containing data and predicted data per source

        Returns:
            list : The fractional residuals, calculated as (observed - predicted) / observed.

        Raises:
            ZeroDivisionError: If any observed value is zero.

        Example:
            >>> webdicts = {'J1407+2827': [{'freq': '7.257',
                            'data': '1.44, ',
                            'error': '0.0017146284762749138',
                            'fitteddata': '1.45'}],
                            'J1820-2528': [{'freq': '7.257',
                            'data': '0.73',
                            'error': '0.0028120763231611317',
                            'fitteddata': '0.70'}]
                            }
            >>> getFractionalResiduals(webdicts)
            [[-0.006], [0.04]]
        """
        fractional_residuals = []
        for source, datadicts in webdicts.items():
            try:
                residuals = []
                for datadict in datadicts:
                    residuals.append((float(datadict['data']) - float(datadict['fitteddata'])) / float(datadict['data']))
                fractional_residuals.append(residuals)
            except Exception:
                continue
        return fractional_residuals


class FluxbootListQAHandler(pqa.QAPlugin):
    """
    QA handler for a list containing FluxbootResults.
    """
    result_cls = collections.abc.Iterable
    child_cls = FluxbootResults

    def handle(self, context, result):
        # collate the QAScores from each child result, pulling them into our
        # own QAscore list
        collated = utils.flatten([r.qa.pool for r in result])
        result.qa.pool[:] = collated
