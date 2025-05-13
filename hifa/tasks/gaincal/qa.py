import collections
import os

import pipeline.infrastructure.logging as logging
import pipeline.infrastructure.pipelineqa as pqa
import pipeline.infrastructure.utils as utils
from pipeline.infrastructure import casa_tools
import pipeline.qa.gpcal as gpcal
from pipeline.h.tasks.common import commonhelpermethods
from pipeline.hif.tasks.gaincal import common

import numpy as np

LOG = logging.get_logger(__name__)


class TimegaincalQAPool(pqa.QAScorePool):
    xy_x2x1_score_types = {
        'PHASE_SCORE_XY': ('XY_TOTAL', 'X-Y phase deviation'),
        'PHASE_SCORE_X2X1': ('X2X1_TOTAL', 'X2-X1 phase deviation')
    }

    xy_x2x1_short_msg = {
        'PHASE_SCORE_XY': 'X-Y deviation',
        'PHASE_SCORE_X2X1': 'X2-X1 deviation'
    }

    def __init__(self, phase_qa_results_dict, phase_offsets_qa_results_list):
        super(TimegaincalQAPool, self).__init__()
        self.phase_qa_results_dict = phase_qa_results_dict
        self.phase_offsets_qa_results_list = phase_offsets_qa_results_list

    def update_scores(self, ms, refant):
        # Phase offsets scores
        hidden_phase_offsets_scores, public_phase_offsets_scores = self._get_phase_offset_scores(ms, refant.split(',')[0])
        self.pool.extend(public_phase_offsets_scores)
        self.pool.extend(hidden_phase_offsets_scores)

        # X-Y / X2-X1 scores
        #
        # PIPE-365: Revise hifa_timegaincal QA scores for Cy7
        #
        # Remy: Remove consideration of X-Y, X2-X1 phase vs. time metrics
        # from QA scoring (keep evaluation for now - I will open a CASR to
        # revise these evaluations)
        #
        # phase_field_ids = [field.id for field in ms.get_fields(intent='PHASE')]
        # self.pool.extend([self._get_xy_x2x1_qascore(ms, phase_field_ids, t) for t in self.xy_x2x1_score_types])

    def _get_phase_offset_scores(self, ms, refant):

        """
        Calculate phase offsets scores.
        """

        try:
            tbTool = casa_tools.table

            # Define thresholds (in degrees) a factors for QA tests
            NoiseThresh = 15.0

            QA1_Thresh1 = 5.0
            QA1_Thresh2 = 30.0
            QA1_Factor = 6.0

            QA2_Thresh1 = 15.0
            QA2_Thresh2 = 30.0
            QA2_Factor1 = 2.0
            QA2_Factor2 = 4.0

            QA3_Thresh1 = 15.0
            QA3_Thresh2 = 30.0
            QA3_Factor = 6.0

            # define scores
            GOOD = 1.0
            NOISY = 0.82
            OUTLIER1 = 0.8
            OUTLIER2 = 0.5
            SPW_MAPPING_PENALTY = 0.1  # decrease the score for outliers by this much if the spw(s) are mapped

            phase_scan_ids = np.array([s.id for s in ms.get_scans(scan_intent='PHASE')])

            # Create antenna ID to name translation dictionary, reject empty antenna name strings.
            antenna_id_to_name = {ant.id: ant.name for ant in ms.antennas if ant.name.strip()}

            # Get refant ID
            refant_id = [ant.id for ant in ms.antennas if ant.name.strip() == refant][0]

            hidden_phase_offsets_scores = []
            public_phase_offsets_scores = []
            subscores = {}
            for gaintable in self.phase_offsets_qa_results_list:
                tbTool.open(gaintable)
                # Get phase offsets in degrees, obeying any flags.
                phase_offsets = np.rad2deg(np.ma.angle(np.ma.array(tbTool.getcol('CPARAM'), mask=tbTool.getcol('FLAG'))))
                scan_numbers = tbTool.getcol('SCAN_NUMBER')
                spw_ids = tbTool.getcol('SPECTRAL_WINDOW_ID')
                ant_ids = tbTool.getcol('ANTENNA1')
                tbTool.done()
                subscores[gaintable] = {}
                good_spw_ids = set()
                noisy_spw_ids = set()
                outlier_spws = {}
                # PIPE-1762: add information about spw mapping (if any):
                # the set contains all spws that are not mapped to itself in any field with intent=="PHASE"
                mapped_spws = set()
                for ifld, spwmap in ms.spwmaps.items():
                    if ifld.intent != 'PHASE':
                        continue
                    if spwmap.combine:
                        mapped_spws.update(set(spw_ids))  # all spws are mapped
                    elif spwmap.spwmap:
                        mapped_spws.update(set(spw for spw in spw_ids if spwmap.spwmap[spw] != spw))

                for spw_id in sorted(set(spw_ids)):
                    subscores[gaintable][spw_id] = {}
                    # Get names of polarization(s), and create polarization index
                    corr_type = commonhelpermethods.get_corr_axis(ms, spw_id)
                    # Estimate of the overall noise (all ant, pol, solutions) for a given EB and SPW
                    selection = phase_offsets[:, 0,
                        np.ma.where((np.isin(scan_numbers, phase_scan_ids)) & (spw_ids == spw_id) & (ant_ids != refant_id))]
                    # MAD multiplied by a fudge factor that makes it equivalent to standard deviation
                    # if the input values are normally distributed
                    Stot = 1.4826 * np.ma.median(np.ma.abs(selection - np.ma.median(selection)))
                    if Stot > NoiseThresh:
                        # Noise too high for further evaluation
                        noisy_spw_ids.add(spw_id)
                        for ant_id in sorted(set(ant_ids)-{refant_id}):
                            subscores[gaintable][spw_id][ant_id] = {}
                            # PIPE-1628: in case when the given spw has only a single polarization
                            # (this always means 'X' in the current workflow), the phase_offsets table
                            # still has two columns, but one of them is empty and should be ignored.
                            for pol_id, pol_name in enumerate(corr_type):
                                subscores[gaintable][spw_id][ant_id][pol_id] = {}
                                for scorekey in ['QA1', 'QA2', 'QA3']:
                                    score = NOISY
                                    longmsg = (f'Phase offsets too noisy to usefully evaluate {ms.basename} SPW {spw_id} '
                                               f'Antenna {antenna_id_to_name[ant_id]} Polarization {pol_name}')
                                    shortmsg = 'Phase offsets too noisy'
                                    origin = pqa.QAOrigin(metric_name='Stot(EB, spw)',
                                                          metric_score=Stot,
                                                          metric_units='degrees')
                                    data_selection = pqa.TargetDataSelection(
                                        vis={ms.basename}, spw={spw_id}, intent={'PHASE'}, ant={ant_id}, pol={pol_name})
                                    weblog_location = pqa.WebLogLocation.HIDDEN
                                    subscores[gaintable][spw_id][ant_id][pol_id][scorekey] = pqa.QAScore(score=score,
                                        longmsg=longmsg, shortmsg=shortmsg, vis=ms.basename, origin=origin,
                                        applies_to=data_selection, weblog_location=weblog_location)
                                    hidden_phase_offsets_scores.append(subscores[gaintable][spw_id][ant_id][pol_id][scorekey])
                    else:
                        for ant_id in sorted(set(ant_ids)-{refant_id}):
                            subscores[gaintable][spw_id][ant_id] = {}
                            # PIPE-1628: loop over available polarizations, which could be fewer than the number of columns in the table
                            for pol_id, pol_name in enumerate(corr_type):
                                subscores[gaintable][spw_id][ant_id][pol_id] = {}
                                selection = phase_offsets[pol_id, 0,
                                    np.ma.where((np.isin(scan_numbers, phase_scan_ids)) & (spw_ids == spw_id) & (ant_ids == ant_id))]
                                N = np.ma.count(selection)  # number of unflagged phase solutions for given spw, ant, pol selection
                                if N > 0:
                                    M = np.ma.mean(selection)
                                    S = np.ma.std(selection, ddof=1)
                                    MaxOff = np.ma.max(np.ma.abs(selection))
                                else:
                                    # PIPE-1707: no data for the given pol/ant/spw combination exists - skip further evaluation
                                    LOG.info(f'No data to evaluate phase offsets for {ms.basename} SPW {spw_id} '
                                             f'Antenna {antenna_id_to_name[ant_id]} Polarization {pol_name}')
                                    for key in ['QA1', 'QA2', 'QA3']:
                                        subscores[gaintable][spw_id][ant_id][pol_id][key] = pqa.QAScore(score=GOOD,
                                            longmsg='No data', shortmsg='No data', vis=ms.basename, origin=pqa.QAOrigin(
                                            metric_name='Number of solutions', metric_score=N, metric_units='N/A'))
                                    continue

                                # Mean test
                                QA1_score = GOOD
                                QA1_longmsg = (f'Phase offsets mean for {ms.basename} SPW {spw_id} '
                                               f'Antenna {antenna_id_to_name[ant_id]} Polarization {pol_name} within range')
                                QA1_shortmsg = 'Phase offsets mean within range'
                                if np.abs(M) > max(QA1_Thresh1, QA1_Factor * Stot / np.sqrt(N)):
                                    QA1_score = OUTLIER1
                                    QA1_longmsg = (f'Phase offsets mean for {ms.basename} SPW {spw_id} '
                                                   f'Antenna {antenna_id_to_name[ant_id]} Polarization {pol_name} exceeding first threshold')
                                    QA1_shortmsg = 'Phase offsets mean too large'
                                if np.abs(M) > max(QA1_Thresh2, QA1_Factor * Stot / np.sqrt(N)):
                                    QA1_score = OUTLIER2
                                    QA1_longmsg = (f'Phase offsets mean for {ms.basename} SPW {spw_id} '
                                                   f'Antenna {antenna_id_to_name[ant_id]} Polarization {pol_name} exceeding second threshold')
                                    QA1_shortmsg = 'Phase offsets mean too large'
                                QA1_origin = pqa.QAOrigin(metric_name='phase offsets mean',
                                                          metric_score=M,
                                                          metric_units='degrees')
                                QA1_data_selection = pqa.TargetDataSelection(
                                    vis={ms.basename}, spw={spw_id}, intent={'PHASE'}, ant={ant_id}, pol={pol_name})
                                QA1_weblog_location = pqa.WebLogLocation.HIDDEN
                                subscores[gaintable][spw_id][ant_id][pol_id]['QA1'] = pqa.QAScore(score=QA1_score,
                                    longmsg=QA1_longmsg, shortmsg=QA1_shortmsg, vis=ms.basename, origin=QA1_origin,
                                    applies_to=QA1_data_selection, weblog_location=QA1_weblog_location)
                                hidden_phase_offsets_scores.append(subscores[gaintable][spw_id][ant_id][pol_id]['QA1'])

                                if N < 4:
                                    # skip this test due to poor statistics when have too few solutions,
                                    # and add a dummy GOOD score to the QA score pool as per PIPE-1762
                                    # (need to add _something_ because the subsequent code still checks these scores)
                                    QA2_score = GOOD
                                    QA2_longmsg = (f'Phase offsets with poor statistics for {ms.basename} SPW {spw_id} '
                                                   f'Antenna {antenna_id_to_name[ant_id]} Polarization {pol_name}')
                                    QA2_shortmsg = 'Poor phase offsets statistics'
                                    QA2_origin = pqa.QAOrigin(metric_name='Number of solutions',
                                                          metric_score=N,
                                                          metric_units='N/A')
                                else:
                                    # Standard deviation test
                                    QA2_score = GOOD
                                    QA2_longmsg = (f'Phase offsets standard deviation for {ms.basename} SPW {spw_id} '
                                                   f'Antenna {antenna_id_to_name[ant_id]} Polarization {pol_name} within range')
                                    QA2_shortmsg = 'Phase offsets standard deviation within range'
                                    if S > max(QA2_Thresh1, QA2_Factor1 * Stot):
                                        QA2_score = OUTLIER1
                                        QA2_longmsg = (f'Phase offsets standard deviation for {ms.basename} SPW {spw_id} '
                                                       f'Antenna {antenna_id_to_name[ant_id]} Polarization {pol_name} exceeding first threshold')
                                        QA2_shortmsg = 'Phase offsets standard deviation too large'
                                    if S > max(QA2_Thresh2, QA2_Factor2 * Stot):
                                        QA2_score = OUTLIER2
                                        QA2_longmsg = (f'Phase offsets standard deviation for {ms.basename} SPW {spw_id} '
                                                       f'Antenna {antenna_id_to_name[ant_id]} Polarization {pol_name} exceeding second threshold')
                                        QA2_shortmsg = 'Phase offsets standard deviation too large'
                                    QA2_origin = pqa.QAOrigin(metric_name='phase offsets standard deviation',
                                                              metric_score=S,
                                                              metric_units='degrees')
                                QA2_data_selection = pqa.TargetDataSelection(
                                    vis={ms.basename}, spw={spw_id}, intent={'PHASE'}, ant={ant_id}, pol={pol_name})
                                QA2_weblog_location = pqa.WebLogLocation.HIDDEN
                                subscores[gaintable][spw_id][ant_id][pol_id]['QA2'] = pqa.QAScore(score=QA2_score,
                                    longmsg=QA2_longmsg, shortmsg=QA2_shortmsg, vis=ms.basename, origin=QA2_origin,
                                    applies_to=QA2_data_selection, weblog_location=QA2_weblog_location)
                                hidden_phase_offsets_scores.append(subscores[gaintable][spw_id][ant_id][pol_id]['QA2'])

                                # Maximum test
                                QA3_score = GOOD
                                QA3_longmsg = (f'Phase offsets maximum for {ms.basename} SPW {spw_id} '
                                               f'Antenna {antenna_id_to_name[ant_id]} Polarization {pol_name} within range')
                                QA3_shortmsg = 'Phase offsets maximum within range'
                                if np.abs(MaxOff) > max(QA3_Thresh1, QA3_Factor * Stot):
                                    QA3_score = OUTLIER1
                                    QA3_longmsg = (f'Phase offsets maximum for {ms.basename} SPW {spw_id} '
                                                   f'Antenna {antenna_id_to_name[ant_id]} Polarization {pol_name} exceeding first threshold')
                                    QA3_shortmsg = 'Phase offsets maximum too large'
                                if np.abs(MaxOff) > max(QA3_Thresh2, QA3_Factor * Stot):
                                    QA3_score = OUTLIER2
                                    QA3_longmsg = (f'Phase offsets maximum for {ms.basename} SPW {spw_id} '
                                                   f'Antenna {antenna_id_to_name[ant_id]} Polarization {pol_name} exceeding second threshold')
                                    QA3_shortmsg = 'Phase offsets maximum too large'
                                QA3_origin = pqa.QAOrigin(metric_name='phase offsets maximum',
                                                          metric_score=MaxOff,
                                                          metric_units='degrees')
                                QA3_data_selection = pqa.TargetDataSelection(
                                    vis={ms.basename}, spw={spw_id}, intent={'PHASE'}, ant={ant_id}, pol={pol_name})
                                QA3_weblog_location = pqa.WebLogLocation.HIDDEN
                                subscores[gaintable][spw_id][ant_id][pol_id]['QA3'] = pqa.QAScore(score=QA3_score,
                                    longmsg=QA3_longmsg, shortmsg=QA3_shortmsg, vis=ms.basename, origin=QA3_origin,
                                    applies_to=QA3_data_selection, weblog_location=QA3_weblog_location)
                                hidden_phase_offsets_scores.append(subscores[gaintable][spw_id][ant_id][pol_id]['QA3'])

                # Create aggregated scores
                for spw_id in sorted(set(spw_ids)):
                    EB_spw_QA_scores = []
                    # PIPE-1628: loop over available polarizations, which could be fewer than the number of columns in the table
                    corr_type = commonhelpermethods.get_corr_axis(ms, spw_id)
                    for ant_id in sorted(set(ant_ids)-{refant_id}):
                        for pol_id, pol_name in enumerate(corr_type):
                            # Minimum of QA1, QA2, QA3 scores
                            QA_scores = [subscores[gaintable][spw_id][ant_id][pol_id]['QA1'],
                                         subscores[gaintable][spw_id][ant_id][pol_id]['QA2'],
                                         subscores[gaintable][spw_id][ant_id][pol_id]['QA3']]
                            min_index = np.argmin([qas.score for qas in QA_scores])
                            QA_score = QA_scores[min_index].score
                            QA_longmsg = QA_scores[min_index].longmsg
                            QA_shortmsg = QA_scores[min_index].shortmsg
                            QA_origin = QA_scores[min_index].origin
                            QA_data_selection = QA_scores[min_index].applies_to
                            QA_weblog_location = pqa.WebLogLocation.HIDDEN
                            subscores[gaintable][spw_id][ant_id][pol_id]['QA'] = pqa.QAScore(score=QA_score,
                                longmsg=QA_longmsg, shortmsg=QA_shortmsg, vis=ms.basename, origin=QA_origin,
                                applies_to=QA_data_selection, weblog_location=QA_weblog_location)
                            # Just collect the scores for further aggregation. The minimum is already
                            # in the hidden_phase_offsets_scores list.
                            EB_spw_QA_scores.append(subscores[gaintable][spw_id][ant_id][pol_id]['QA'])
                            if QA_score <= OUTLIER1:
                                LOG.info(f'Poor phase offsets score for {ms.basename} SPW {spw_id} '
                                         f'Antenna {antenna_id_to_name[ant_id]} Polarization {pol_name}: '
                                         f'QA score = {QA_score:.1f}, '
                                         f'mean phase offset = {subscores[gaintable][spw_id][ant_id][pol_id]["QA1"].origin.metric_score:.1f} deg, '
                                         f'standard deviation = {subscores[gaintable][spw_id][ant_id][pol_id]["QA2"].origin.metric_score:.1f} deg, '
                                         f'max offset = {subscores[gaintable][spw_id][ant_id][pol_id]["QA3"].origin.metric_score:.1f} deg')
                    # Minimum of per aggregated ant/pol scores as per EB/spw score
                    EB_spw_min_index = np.argmin([qas.score for qas in EB_spw_QA_scores])
                    EB_spw_QA_score = EB_spw_QA_scores[EB_spw_min_index].score

                    # The aggregation of NOISY and GOOD scores is handled outside this loop,
                    # and for outliers we collect the information about antennas and spws and aggregate it later.
                    if EB_spw_QA_score == GOOD:
                        good_spw_ids.add(spw_id)
                        continue
                    elif EB_spw_QA_score == NOISY:
                        # it was already added to the set of noisy_spw_ids
                        continue
                    elif EB_spw_QA_score in (OUTLIER1, OUTLIER2):
                        antenna_names_list = []
                        spw_score = GOOD
                        for ant_id in sorted(set(ant_ids)-{refant_id}):
                            # Get only the non-noisy antennas
                            per_antenna_score = min(subscores[gaintable][spw_id][ant_id][pol_id]['QA'].score
                                                    for pol_id in range(len(corr_type)))
                            spw_score = min(spw_score, per_antenna_score)
                            if per_antenna_score in (GOOD, NOISY):
                                pass
                            elif per_antenna_score == OUTLIER2:
                                # Highlight bad antennas with an asterisk
                                antenna_names_list.append('*'+antenna_id_to_name[ant_id])
                            elif per_antenna_score == OUTLIER1:
                                antenna_names_list.append(antenna_id_to_name[ant_id])
                            else:
                                # should not have gotten here - debugging trap
                                LOG.error(f'Unexpected QA score {per_antenna_score} for {ms.basename} '
                                    f'SPW {spw_id} Antenna {antenna_id_to_name[ant_id]}')
                        # PIPE-1762: aggregate the QA scores and messages about potential phase outliers.
                        # One or more spws sharing the same unique set of antennas generates one message.
                        # outlier_spws is a dict with keys being the unique sets of antennas (rather, tuples),
                        # and values themselves are dicts with two elements: the set of spws and the lowest score.
                        antenna_names_list = tuple(antenna_names_list)  # convert list to tuple, to be used as dict key
                        if antenna_names_list in outlier_spws:
                            # update the dict item associated with the same unique list of antennas
                            outlier_spws[antenna_names_list] = {
                                'spws': set.union(outlier_spws[antenna_names_list]['spws'], {spw_id}),
                                'min_score': min(outlier_spws[antenna_names_list]['min_score'], spw_score),
                            }
                        else:
                            # create a new dict item for this list of antennas
                            outlier_spws[antenna_names_list] = {
                                'spws': {spw_id},
                                'min_score': spw_score,
                            }
                    else:
                        # should not have gotten here - the previous conditions check all possible cases
                        LOG.error(f'Unexpected QA score {EB_spw_QA_score} for {ms.basename} SPW {spw_id}')

                if good_spw_ids:
                    # Add aggregated score for good spws
                    public_phase_offsets_scores.append(pqa.QAScore(
                        score=GOOD,
                        longmsg='Phase offsets for {0}, spw{1} within range'.format(
                            ms.basename,
                            utils.commafy(sorted(good_spw_ids), quotes=False, multi_prefix='s')),
                        shortmsg='Phase offsets within range',
                        vis=ms.basename,
                        origin=pqa.QAOrigin(
                            metric_name='Stot / Mean / SD / MaxOff',
                            metric_score=-999,
                            metric_units='degrees'),
                        applies_to=pqa.TargetDataSelection(
                            vis={ms.basename},
                            spw=good_spw_ids,
                            intent={'PHASE'}),
                        weblog_location=pqa.WebLogLocation.ACCORDION))

                if noisy_spw_ids:
                    # Add aggregated score for noisy spws
                    public_phase_offsets_scores.append(pqa.QAScore(
                        score=NOISY,
                        longmsg='Phase offsets too noisy to usefully evaluate for {0}, spw{1}'.format(
                            ms.basename,
                            utils.commafy(sorted(noisy_spw_ids), quotes=False, multi_prefix='s')),
                        shortmsg='Phase offsets too noisy',
                        vis=ms.basename,
                        origin=pqa.QAOrigin(
                            metric_name='Stot(EB, spw)',
                            metric_score=-999,
                            metric_units='degrees'),
                        applies_to=pqa.TargetDataSelection(
                            vis={ms.basename},
                            spw=noisy_spw_ids,
                            intent={'PHASE'}),
                        weblog_location=pqa.WebLogLocation.ACCORDION))

                for outlier_antennas, outlier_spws_and_score in outlier_spws.items():
                    # PIPE-1762: Add a message for each set of spws sharing the same list of antennas
                    # marked as phase offset outliers
                    score = outlier_spws_and_score['min_score']
                    mapped_spws_for_antennas = outlier_spws_and_score['spws'].intersection(mapped_spws)
                    if mapped_spws_for_antennas:
                        score -= SPW_MAPPING_PENALTY
                        mapping_msg = 'SPW mapping used for spw{}, so any offsets could affect calibration.'.format(
                            utils.commafy(sorted(mapped_spws_for_antennas), quotes=False, multi_prefix='s'))
                    else:
                        mapping_msg = 'No SPW mapping used, so any offsets should calibrate out.'
                    public_phase_offsets_scores.append(pqa.QAScore(
                        score=score,
                        longmsg='Potential phase offset outliers for {0}, spw{1}, antenna{2}. {3}'.format(
                            ms.basename,
                            utils.commafy(sorted(outlier_spws_and_score['spws']), quotes=False, multi_prefix='s'),
                            utils.commafy(outlier_antennas, quotes=False, multi_prefix='s'),
                            mapping_msg),
                        shortmsg='Potential phase offset outliers',
                        vis=ms.basename,
                        applies_to=pqa.TargetDataSelection(
                            vis={ms.basename},
                            spw=outlier_spws_and_score['spws'],
                            intent={'PHASE'}),
                        weblog_location=pqa.WebLogLocation.ACCORDION))

            return hidden_phase_offsets_scores, public_phase_offsets_scores

        except Exception as e:
            import traceback
            LOG.error('Phase offsets score calculation failed: %s\n%s' % (e, traceback.format_exc(limit=10)))
            return [], [pqa.QAScore(-0.1, longmsg='Phase offsets score calculation failed',
                                    shortmsg='Phase offsets score calculation failed', vis=ms.basename)]


    def _get_xy_x2x1_qascore(self, ms, phase_field_ids, score_type):
        try:
            (total_score, table_name, field_name, ant_name, spw_name) = \
                self._get_xy_x2x1_total(phase_field_ids, self.xy_x2x1_score_types[score_type][0])
            longmsg = 'Total score for %s is %0.2f (%s field %s %s spw %s)' % (
                self.xy_x2x1_score_types[score_type][1], total_score, ms.basename, field_name, ant_name, spw_name)
            shortmsg = self.xy_x2x1_short_msg[score_type]

            origin = pqa.QAOrigin(metric_name='TimegaincalQAPool',
                                  metric_score=total_score,
                                  metric_units='Total gpcal QA score')

            return pqa.QAScore(total_score, longmsg=longmsg, shortmsg=shortmsg, vis=ms.basename, origin=origin)

        except Exception as e:
            LOG.error('X-Y / X2-X1 score calculation failed: %s' % (e))
            return pqa.QAScore(-0.1, longmsg='X-Y / X2-X1 score calculation failed', shortmsg='X-Y / X2-X1 score calculation failed', vis=ms.basename)

    def _get_xy_x2x1_total(self, phase_field_ids, score_key):
        # attrs to hold score and QA identifiers
        total_score = 1.0
        total_table_name = None
        total_field_name = 'N/A'
        total_ant_name = 'N/A'
        total_spw_name = 'N/A'

        for table_name, qa_result in self.phase_qa_results_dict.items():
            for field_id in phase_field_ids:
                qa_context_score = qa_result['QASCORES']['SCORES'][field_id][score_key]
                if qa_context_score['SCORE'] != 'C/C':
                    if qa_context_score['SCORE'] < total_score:
                        total_score = qa_context_score['SCORE']
                        total_field_name = qa_result['QASCORES']['FIELDS'][qa_context_score['FIELD']]
                        total_ant_name = qa_result['QASCORES']['ANTENNAS'][qa_context_score['ANTENNA']]
                        total_spw_name = qa_result['QASCORES']['SPWS'][qa_context_score['SPW']]
                        total_table_name = table_name

        return total_score, total_table_name, total_field_name, total_ant_name, total_spw_name


class TimegaincalQAHandler(pqa.QAPlugin):
    """
    QA handler for an uncontained TimegaincalResult.
    """
    result_cls = common.GaincalResults
    child_cls = None

    def handle(self, context, result):
        vis = result.inputs['vis']
        ms = context.observing_run.get_ms(vis)

        qa_dir = os.path.join(context.report_dir, 'stage%s' % result.stage_number, 'qa')

        if not os.path.exists(qa_dir):
            os.makedirs(qa_dir)

        phase_qa_results_dict = {}
        phase_offsets_qa_results_list = []

        try:
            # Get phase cal tables
            for calapp in result.final:
                solint = utils.get_origin_input_arg(calapp, 'solint')
                calmode = utils.get_origin_input_arg(calapp, 'calmode')
                if solint == 'int' and calmode == 'p':
                    phase_qa_results_dict[calapp.gaintable] = gpcal.gpcal(calapp.gaintable)

            # Get phase offsets tables
            for calapp in result.phaseoffsetresult.final:
                phase_offsets_qa_results_list.append(calapp.gaintable)

            result.qa = TimegaincalQAPool(phase_qa_results_dict, phase_offsets_qa_results_list)
            result.qa.update_scores(ms, result.inputs['refant'])
        except Exception as e:
            LOG.error('Problem occurred running QA analysis. QA results will not be available for this task')
            LOG.exception(e)


class TimegaincalListQAHandler(pqa.QAPlugin):
    """
    QA handler for a list containing TimegaincalResults.
    """
    result_cls = collections.abc.Iterable
    child_cls = common.GaincalResults

    def handle(self, context, result):
        # collate the QAScores from each child result, pulling them into our
        # own QAscore list
        collated = utils.flatten([r.qa.pool for r in result])
        result.qa.pool[:] = collated
