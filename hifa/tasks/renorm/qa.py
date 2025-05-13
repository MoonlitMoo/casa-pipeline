import copy
import os
import traceback

import numpy as np

import pipeline.infrastructure.basetask as basetask
import pipeline.infrastructure.logging as logging
import pipeline.infrastructure.pipelineqa as pqa
import pipeline.infrastructure.utils as utils
from pipeline.qa.utility import scorers

from . import renorm

LOG = logging.get_logger(__name__)


class RenormQAHandler(pqa.QAPlugin):
    """
    QA handler for an uncontained RenormResults.
    """
    result_cls = renorm.RenormResults
    child_cls = None

    def handle(self, context, result):
        # Get task threshold value. In the future one might rely on
        # the automatic defaults in almarenorm. In that case the threshold
        # per spw needs to be fetched from the stats dictionary in the spw
        # loop below (result.stats[source][spw]['threshold']).
        threshold = result.threshold

        if result.exception is not None:
            score = 0.0
            shortmsg = 'Failure in renormalization'
            longmsg = 'Failure in running renormalization heuristic: {}'.format(result.exception)
            result.qa.pool.append(pqa.QAScore(score, longmsg=longmsg, shortmsg=shortmsg, vis=result.vis))
            return

        if result.createcaltable and result.caltable is not None and not result.alltdm and not result.calTableCreated:
            # Cal table not created
            score = 0.0
            shortmsg = 'No cal table created'
            longmsg = 'EB {}: No cal table was created.'.format(os.path.basename(result.vis))
            result.qa.pool.append(pqa.QAScore(score, longmsg=longmsg, shortmsg=shortmsg, vis=result.vis))

        for source in result.stats:
            for spw in result.stats[source]:
                try:
                    # Max factor score
                    max_factor = result.stats[source][spw]['max_rn']

                    origin = pqa.QAOrigin(metric_name='MaxRenormFactor',
                                          metric_score=max_factor,
                                          metric_units='')

                    if result.rnstats['invalid'][source][spw]:
                        # Any NaNs apart from max_factor?
                        score = 0.0
                        shortmsg = 'Unexpected values.'
                        longmsg = 'EB {} source {} spw {}: Error calculating corrections. NaNs encountered.'.format(
                                  os.path.basename(result.vis), source, spw)
                        result.qa.pool.append(pqa.QAScore(score, longmsg=longmsg, shortmsg=shortmsg, vis=result.vis, origin=origin))

                    if (max_factor < 1.0) or (max_factor > 2.5) or np.isnan(max_factor):
                        # These values should never occur
                        score = 0.0
                        shortmsg = 'Unexpected values.'
                        longmsg = 'EB {} source {} spw {}: Erroneous or unrealistic scaling values were found. Error calculating corrections. Maximum factor is {}.'.format(
                                  os.path.basename(result.vis), source, spw, max_factor)
                        result.qa.pool.append(pqa.QAScore(score, longmsg=longmsg, shortmsg=shortmsg, vis=result.vis, origin=origin))

                    if 1.0 <= max_factor <= threshold:
                        scorer = scorers.linScorer(1.0, threshold, 1.0, 0.91)  # score stays between 1.0 and 0.91 - green
                        score = scorer(max_factor)
                        shortmsg = 'Renormalization factor within threshold'
                        longmsg = 'EB {} source {} spw {}: maximum renormalization factor of {:.3f} ' \
                                  'is within threshold of {:.1%} and so was not applied'.format(
                                      os.path.basename(result.vis), source, spw, max_factor, threshold-1.0)
                        result.qa.pool.append(pqa.QAScore(score, longmsg=longmsg, shortmsg=shortmsg, vis=result.vis, origin=origin))
                    elif max_factor > threshold:
                        if result.createcaltable:
                            if max_factor < 2.5:
                                scorer = scorers.linScorer(threshold, 2.5, 0.9, 0.67)  # score stays between 0.9 and 0.67 - blue
                                score = scorer(max_factor)
                            else:
                                score = 0.66  # yellow score
                            shortmsg = 'Renormalization computed'
                            longmsg = 'EB {} source {} spw {}: maximum renormalization factor of {:.3f} ' \
                                      'is outside threshold of {:.1%} so corrections were applied.'.format(
                                          os.path.basename(result.vis), source, spw, max_factor, threshold-1.0)
                        else:
                            if max_factor < 2.5:
                                scorer = scorers.linScorer(threshold, 2.5, 0.66, 0.34)  # score stays between 0.66 and 0.34 - yellow
                                score = scorer(max_factor)
                            else:
                                score = 0.33  # red score
                            shortmsg = 'Renormalization factor outside threshold'
                            longmsg = 'EB {} source {} spw {}: maximum renormalization factor of {:.3f} ' \
                                      'is outside threshold of {:.1%} but no corrections were applied.'.format(
                                          os.path.basename(result.vis), source, spw, max_factor, threshold-1.0)

                        result.qa.pool.append(pqa.QAScore(score, longmsg=longmsg, shortmsg=shortmsg, vis=result.vis, origin=origin))

                    # Segment change score
                    if result.stats[source][spw]['seg_change'] and (max_factor > threshold) and result.createcaltable:
                        origin = pqa.QAOrigin(metric_name='SegChange',
                                              metric_score=True,
                                              metric_units='')

                        score = 0.9
                        shortmsg = 'Number of segments changed.'
                        longmsg = 'EB {} source {} spw {}: The number of segments was changed throughout renormalization analysis.' \
                                  ' Summary plot may show discontinuities due to averaging over fields with different segment boundaries.'.format(
                                      os.path.basename(result.vis), source, spw)

                        result.qa.pool.append(pqa.QAScore(score, longmsg=longmsg, shortmsg=shortmsg, vis=result.vis, origin=origin))

                except Exception as ex:
                    # No factors for this spw. Just skip it.
                    LOG.warning("Renorm QA Handler Exception: {!s}".format(str(ex)))
                    LOG.debug(traceback.format_exc())

        # Make a copy of this dict to keep track of whether a QA message was issued for
        # each excludechan SPW yet.
        excludechan = copy.deepcopy(result.excludechan)

        # Only warn about spws in excludechan not matching the spws for the automated suggestions if there are whole suggested spws
        # not covered by the input excludechan dict. (Without this, there is a QA warning when the input excludechan values are
        # so close to the ones that would be recommeded by renorm, that no additional automated suggestions are made.)
        warn_excludechan_spw = False

        for target in result.atmWarning:
            for spw in result.atmWarning[target]:
                if result.atmWarning[target][spw] and result.stats[target][spw]['max_rn'] > threshold:
                    if not result.createcaltable:
                        atm_score = 0.89
                        shortmsg = "Renormalization correction may be incorrect due to an atmospheric feature"
                        longmsg = "Renormalization correction may be incorrect in SPW {} due to an atmospheric feature. Suggested "\
                                  "channel exclusion: {}".format(spw, result.atmExcludeCmd[target][spw])
                    else:
                        if excludechan:
                            excluded_spws = excludechan.keys()
                            if spw in excluded_spws:
                                longmsg = "Channels {} are being excluded from renormalization correction to SPW {}. Auto-calculated channel " \
                                    "exclusion: {}".format(excludechan[spw], spw, result.atmExcludeCmd[target][spw])
                                # Since we printed a message about the excluded channels for this SPW, remove it from the list that haven't yet been covered.
                                del excludechan[spw]
                            else:
                                longmsg = "No channels are being excluded from renormalization correction to SPW {}. Auto-calculated channel " \
                                    "exclusion: {}".format(spw, result.atmExcludeCmd[target][spw])
                                warn_excludechan_spw = True
                            atm_score = 0.85
                            shortmsg = "Channels are being excluded from renormalization correction"
                        elif result.atmAutoExclude:
                            atm_score = 0.89
                            shortmsg = "Channels are being excluded from renormalization correction due to an atmospheric feature"
                            longmsg = "Channels {} are being excluded from renormalization correction to SPW {} due to an atmospheric " \
                                      "feature.".format(result.atmExcludeCmd[target][spw], spw)
                        else:
                            atm_score = 0.66
                            shortmsg = "Renormalization correction may be incorrectly applied"
                            longmsg = "A renormalization correction may be incorrectly applied to SPW {} due to an atmospheric feature. " \
                                      "Suggested channel exclusion: {}".format(spw, result.atmExcludeCmd[target][spw])
                    result.qa.pool.append(pqa.QAScore(atm_score, longmsg=longmsg, shortmsg=shortmsg, vis=result.vis))

        # If there were any entries in excludechan for spws that weren't identified as having an atmospheric feature by renorm,
        # when createcaltable=True, and there are also provide a QA message.
        if result.createcaltable and warn_excludechan_spw:
            for exclude_spw in excludechan:
                longmsg = "Channels {} are being excluded from renormalization correction to SPW {}. " \
                          "Auto-calculated channel exclusion indicated that no channels need to be excluded for SPW {}." \
                          .format(excludechan[exclude_spw], exclude_spw, exclude_spw)
                shortmsg = "Channels are being excluded from renormalization correction"
                atm_score = 0.85
                result.qa.pool.append(pqa.QAScore(atm_score, longmsg=longmsg, shortmsg=shortmsg, vis=result.vis))


class RenormListQAHandler(pqa.QAPlugin):
    """
    QA handler for a list containing RenormResults.
    """
    result_cls = basetask.ResultsList
    child_cls = renorm.RenormResults

    def handle(self, context, result):
        # collate the QAScores from each child result, pulling them into our
        # own QAscore list
        collated = utils.flatten([r.qa.pool for r in result])
        result.qa.pool[:] = collated

        if all([r.alltdm for r in result]):
            # PIPE-2283: no QA score for TDM data
            shortmsg = "No FDM spectral windows are present."
            longmsg = "No FDM spectral windows are present, so the amplitude scale does not need to be assessed for renormalization."
            result.qa.pool.append(pqa.QAScore(None, longmsg=longmsg, shortmsg=shortmsg))
            return
        # add MOUS level score for band 9/10 data
        band9_10_in_ms = []
        for ms in context.observing_run.get_measurement_sets():
            band9_10_in_ms.append(
                any([spw for spw in ms.get_spectral_windows(science_windows_only=True) 
                     if spw.band in ('ALMA Band 9', 'ALMA Band 10')])
                     )
        if any(band9_10_in_ms):
            # PIPE-2283: blue score for band 9/10 FDM data
            score = 0.9
            shortmsg = "Double Sideband Receivers using FDM mode."
            longmsg = "Double Sideband Receivers using FDM mode; check results carefully."
            result.qa.pool.append(pqa.QAScore(score, longmsg=longmsg, shortmsg=shortmsg))
