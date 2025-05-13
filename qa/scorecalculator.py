"""
Created on 9 Jan 2014

@author: sjw
"""
# Do not evaluate type annotations at definition time.
from __future__ import annotations

import collections
import datetime
import functools
import math
import operator
import os
import re
import shutil
import traceback
from typing import TYPE_CHECKING, Dict, List, Tuple, Union

import numpy as np
from scipy import interpolate
from scipy.special import erf

import pipeline.domain as domain
import pipeline.domain.measures as measures
import pipeline.infrastructure.basetask
import pipeline.infrastructure.logging as logging
import pipeline.infrastructure.pipelineqa as pqa
import pipeline.infrastructure.renderer.rendererutils as rutils
import pipeline.infrastructure.utils as utils
import pipeline.qa.checksource as checksource
from pipeline.domain.datatable import OnlineFlagIndex
from pipeline.domain.measurementset import MeasurementSet
from pipeline.hsd.heuristics.rasterscan import RasterScanHeuristicsResult
from pipeline.hsd.tasks.imaging.resultobjects import SDImagingResultItem
from pipeline.hsd.tasks.importdata.importdata import SDImportDataResults
from pipeline.infrastructure import casa_tools

if TYPE_CHECKING:
    from pipeline.domain.singledish import MSReductionGroupMember
    from pipeline.hif.tasks.gaincal.common import GaincalResults
    from pipeline.hif.tasks.polcal.polcalworker import PolcalWorkerResults
    from pipeline.hsd.tasks.baseline.baseline import SDBaselineResults
    from pipeline.hsd.tasks.imaging.resultobjects import SDImagingResultItem
    from pipeline.infrastructure.launcher import Context

__all__ = ['score_polintents',                                # ALMA specific
           'score_bands',                                     # ALMA specific
           'score_science_spw_names',                         # ALMA specific
           'score_tsysspwmap',                                # ALMA specific
           'score_number_antenna_offsets',                    # ALMA specific
           'score_missing_derived_fluxes',                    # ALMA specific
           'score_derived_fluxes_snr',                        # ALMA specific
           'score_phaseup_spw_median_snr_for_phase',          # ALMA specific
           'score_phaseup_spw_median_snr_for_check',          # ALMA specific
           'score_decoherence_assessment',                    # ALMA specific
           'score_refspw_mapping_fraction',                   # ALMA specific
           'score_missing_phaseup_snrs',                      # ALMA specific
           'score_missing_bandpass_snrs',                     # ALMA specific
           'score_poor_phaseup_solutions',                    # ALMA specific
           'score_poor_bandpass_solutions',                   # ALMA specific
           'score_missing_phase_snrs',                        # ALMA specific
           'score_poor_phase_snrs',                           # ALMA specific
           'score_flagging_view_exists',                      # ALMA specific
           'score_checksources',                              # ALMA specific
           'score_gfluxscale_k_spw',                          # ALMA specific
           'score_fluxservice',                               # ALMA specific
           'score_observing_modes',                           # ALMA specific
           'score_renorm',                                    # ALMA IF specific
           'score_polcal_gain_ratio',                         # ALMA IF specific
           'score_polcal_gain_ratio_rms',                     # ALMA IF specific
           'score_polcal_leakage',                            # ALMA IF specific
           'score_polcal_residual_pol',                       # ALMA IF specific
           'score_polcal_results',                            # ALMA IF specific
           'score_file_exists',
           'score_path_exists',
           'score_flags_exist',
           'score_mses_exist',
           'score_applycmds_exist',
           'score_caltables_exist',
           'score_setjy_measurements',
           'score_missing_intents',
           'score_ephemeris_coordinates',
           'score_online_shadow_template_agents',
           'score_lowtrans_flagcmds',                         # ALMA IF specific
           'score_applycal_agents',
           'score_vla_agents',
           'score_total_data_flagged',
           'score_total_data_flagged_vla',
           'score_total_data_flagged_vla_bandpass',
           'score_flagged_vla_baddef',
           'score_total_data_vla_delay',
           'score_vla_flux_residual_rms',
           'score_ms_model_data_column_present',
           'score_ms_history_entries_present',
           'score_contiguous_session',
           'score_multiply',
           'score_mom8_fc_image',
           'score_iersstate',
           'score_tsysflagcontamination_contamination_flagged',
           'score_tsysflagcontamination_external_heuristic']

LOG = logging.get_logger(__name__)

# - utility functions --------------------------------------------------------------------------------------------------

def log_qa(method):
    """
    Decorator that logs QA evaluations as they return with a log level of
    INFO for scores between perfect and 'slightly suboptimal' scores and
    WARNING for any other level. These messages are meant for pipeline runs
    without a weblog output.
    """
    def f(self, *args, **kw):
        # get the size of the CASA log before task execution
        qascore = method(self, *args, **kw)
        if pipeline.infrastructure.basetask.DISABLE_WEBLOG:
            if isinstance(qascore, tuple):
                _qascore = qascore[0]
            else:
                _qascore = qascore
            if _qascore.score >= rutils.SCORE_THRESHOLD_SUBOPTIMAL:
                LOG.info(_qascore.longmsg)
            else:
                LOG.warning(_qascore.longmsg)
        return qascore

    return f


# struct to hold flagging statistics
AgentStats = collections.namedtuple("AgentStats", "name flagged total")


def calc_flags_per_agent(summaries, scanids=None):
    """
    Calculate flagging statistics per agents. If scanids are provided,
    restrict statistics to just those scans.
    """
    stats = []
    flagsum = 0

    # Go through summary for each agent.
    for idx, summary in enumerate(summaries):
        if scanids:
            # Add up flagged and total for specified scans.
            flagcount = 0
            totalcount = 0
            for scanid in scanids:
                if scanid in summary['scan']:
                    flagcount += int(summary['scan'][scanid]['flagged'])
                    totalcount += int(summary['scan'][scanid]['total'])
        else:
            # Add up flagged and total for all data.
            flagcount = int(summary['flagged'])
            totalcount = int(summary['total'])

        # From the second summary onwards, subtract counts from the previous
        # one.
        if idx > 0:
            flagcount -= flagsum

        # Create agent stats object, append to output.
        stat = AgentStats(name=summary['name'],
                          flagged=flagcount,
                          total=totalcount)
        stats.append(stat)

        # Keep count of total number of flags found in summaries, for
        # subsequent summaries.
        flagsum += flagcount

    return stats


def calc_frac_total_flagged(summaries, agents=None, scanids=None):
    """
    Calculate total fraction of data that is flagged. If agents are provided,
    then restrict to statistics for those agents. If scanids are provided,
    then restrict to statistics for those scans.
    """

    agent_stats = calc_flags_per_agent(summaries, scanids=scanids)

    # sum the number of flagged rows for the selected agents
    frac_flagged = functools.reduce(
        operator.add, [float(s.flagged)/s.total for s in agent_stats if not agents or s.name in agents], 0)

    return frac_flagged


def calc_vla_science_frac_total_flagged(summaries, agents=None, scanids=None):
    """
    Calculate total fraction of vla science data that is flagged. If agents are provided,
    then restrict to statistics for those agents. If scanids are provided,
    then restrict to statistics for those scans.
    """

    agent_stats = calc_flags_per_agent(summaries, scanids=scanids)

    # remove the non-sciece vis flagged from the science total
    if agent_stats:
        science_total = functools.reduce(operator.sub, [s.flagged for s in agent_stats
                                                        if s.name in ('before', 'anos', 'intents',
                                                                      'shadow')], agent_stats[0].total)

    # once we have the new science total, replace it for those agents
    # and create a new list that only includes 'science' agents
    science_agents = []
    for stat in agent_stats:
        if stat.name not in ('before', 'anos', 'intents', 'shadow'):
            science_agents.append(AgentStats(name=stat.name,
                                             flagged=stat.flagged,
                                             total=science_total))

    # sum the number of flagged rows for the selected agents
    frac_flagged = functools.reduce(operator.add, [float(s.flagged)/s.total for s in science_agents
                                                   if not agents or s.name in agents], 0)

    return frac_flagged


def calc_frac_newly_flagged(summaries, agents=None, scanids=None):
    """
    Calculate fraction of data that is newly flagged, i.e. exclude pre-existing
    flags (assumed to be represented in first summary). If agents are provided,
    then restrict to statistics for those agents. If scanids are provided,
    then restrict to statistics for those scans.
    """
    agent_stats = calc_flags_per_agent(summaries, scanids=scanids)

    # sum the number of flagged rows for the selected agents
    frac_flagged = functools.reduce(
        operator.add, [float(s.flagged)/s.total for s in agent_stats[1:] if not agents or s.name in agents], 0)

    return frac_flagged


def linear_score(x, x1, x2, y1=0.0, y2=1.0):
    """
    Calculate the score for the given data value, assuming the
    score follows a linear gradient between the low and high values.

    x values will be clipped to lie within the range x1->x2
    """
    x1 = float(x1)
    x2 = float(x2)
    y1 = float(y1)
    y2 = float(y2)

    clipped_x = sorted([x1, x, x2])[1]
    m = (y2-y1) / (x2-x1)
    c = y1 - m*x1
    return m*clipped_x + c


def score_data_flagged_by_agents(ms, summaries, min_frac, max_frac, agents=None, intents=None):
    """
    Calculate a score for the agentflagger summaries based on the fraction of
    data flagged by certain flagging agents. If intents are provided, then
    restrict scoring to the scans that match one or more of these intents.

    min_frac < flagged < max_frac maps to score of 1-0
    """
    # If intents are provided, identify which scans to calculate flagging
    # fraction for.
    if intents:
        scanids = {str(scan.id) for intent in intents for scan in ms.get_scans(scan_intent=intent)}
        if not scanids:
            LOG.warning("Cannot restrict QA score to intent(s) {}, since no matching scans were found."
                        " Score will be based on scans for all intents.".format(utils.commafy(intents, quotes=False)))
    else:
        scanids = None

    # Calculate fraction of flagged data.
    frac_flagged = calc_frac_total_flagged(summaries, agents=agents, scanids=scanids)

    # Convert fraction of flagged data into a score.
    score = linear_score(frac_flagged, min_frac, max_frac, 1.0, 0.0)

    # Set score messages and origin.
    percent = 100.0 * frac_flagged
    longmsg = ("{:.2f}% data in {} flagged".format(percent, ms.basename))
    if agents:
        longmsg += " by {} flagging agents".format(utils.commafy(agents, quotes=False))
    if intents:
        longmsg += ' for intent(s): {}.'.format(utils.commafy(intents, quotes=False))
    shortmsg = '%0.2f%% data flagged' % percent

    origin = pqa.QAOrigin(metric_name='score_data_flagged_by_agents',
                          metric_score=frac_flagged,
                          metric_units='Fraction of data newly flagged')

    return pqa.QAScore(score, longmsg=longmsg, shortmsg=shortmsg, vis=ms.basename, origin=origin)


def score_vla_science_data_flagged_by_agents(ms, summaries, min_frac, max_frac, agents, intents=None):
    """
    Calculate a score for the agentflagger summaries based on the fraction of
    VLA science data flagged by certain flagging agents. If intents are provided, then
    restrict scoring to the scans that match one or more of these intents.

    min_frac < flagged < max_frac maps to score of 1-0
    """
    # If intents are provided, identify which scans to calculate flagging
    # fraction for.
    if intents:
        scanids = {str(scan.id) for intent in intents for scan in ms.get_scans(scan_intent=intent)}
        if not scanids:
            LOG.warning("Cannot restrict QA score to intent(s) {}, since no matching scans were found."
                        " Score will be based on scans for all intents.".format(utils.commafy(intents, quotes=False)))
    else:
        scanids = None

    # Calculate fraction of flagged data.
    frac_flagged = calc_vla_science_frac_total_flagged(summaries, agents=agents, scanids=scanids)

    # Convert fraction of flagged data into a score.
    score = linear_score(frac_flagged, min_frac, max_frac, 1.0, 0.0)

    # Set score messages and origin.
    percent = 100.0 * frac_flagged
    longmsg = ('%0.2f%% data in %s flagged by %s flagging agents'
               '' % (percent, ms.basename, utils.commafy(agents, quotes=False)))
    if intents:
        longmsg += ' for intent(s): {}'.format(utils.commafy(intents, quotes=False))
    shortmsg = '%0.2f%% data flagged' % percent

    origin = pqa.QAOrigin(metric_name='score_vla_science_data_flagged_by_agents',
                          metric_score=frac_flagged,
                          metric_units='Fraction of data newly flagged')

    return pqa.QAScore(score, longmsg=longmsg, shortmsg=shortmsg, vis=ms.basename, origin=origin)

# - exported scoring functions -----------------------------------------------------------------------------------------


def score_ms_model_data_column_present(all_mses, mses_with_column):
    """
    Give a score for a group of mses based on the number with modeldata
    columns present.
    None with modeldata - 100% with modeldata = 1.0 -> 0.5
    """
    num_with = len(mses_with_column)
    num_all = len(all_mses)

    if num_all == 0:
        return pqa.QAScore(0.0, 'No MSes were imported', 'No MSes imported')

    f = float(num_with) / num_all

    if mses_with_column:
        # log a message like 'No model columns found in a.ms, b.ms or c.ms'
        basenames = [ms.basename for ms in mses_with_column]
        s = utils.commafy(basenames, quotes=False)
        longmsg = 'Model data column found in %s' % s
        shortmsg = '%s/%s have MODELDATA' % (num_with, num_all)
    else:
        # log a message like 'Model data column was found in a.ms and b.ms'
        basenames = [ms.basename for ms in all_mses]
        s = utils.commafy(basenames, quotes=False, conjunction='or')
        longmsg = ('No model data column found in %s' % s)
        shortmsg = 'MODELDATA empty'

    score = linear_score(f, 0.0, 1.0, 1.0, 0.9)

    origin = pqa.QAOrigin(metric_name='score_ms_model_data_column_present',
                          metric_score=f,
                          metric_units='Fraction of MSes with modeldata columns present')

    return pqa.QAScore(score, longmsg, shortmsg, origin=origin)


@log_qa
def score_ms_history_entries_present(all_mses, mses_with_history):
    """
    Give a score for a group of mses based on the number with history
    entries present.
    None with history - 100% with history = 1.0 -> 0.5
    """
    num_with = len(mses_with_history)
    num_all = len(all_mses)

    if num_all == 0:
        return pqa.QAScore(0.0, 'No MSes were imported', 'No MSes imported')

    if mses_with_history:
        # log a message like 'Entries were found in the HISTORY table for
        # a.ms and b.ms'
        basenames = utils.commafy([ms.basename for ms in mses_with_history], quotes=False)
        if len(mses_with_history) == 1:
            longmsg = ('Unexpected entries were found in the HISTORY table of %s. '
                       'This measurement set may already be processed.' % basenames)
        else:
            longmsg = ('Unexpected entries were found in the HISTORY tables of %s. '
                       'These measurement sets may already be processed.' % basenames)
        shortmsg = '%s/%s have HISTORY' % (num_with, num_all)

    else:
        # log a message like 'No history entries were found in a.ms or b.ms'
        basenames = [ms.basename for ms in all_mses]
        s = utils.commafy(basenames, quotes=False, conjunction='or')
        longmsg = 'No HISTORY entries found in %s' % s
        shortmsg = 'No HISTORY entries'

    f = float(num_with) / num_all
    score = linear_score(f, 0.0, 1.0, 1.0, 0.5)

    origin = pqa.QAOrigin(metric_name='score_ms_history_entries_present',
                          metric_score=f,
                          metric_units='Fraction of MSes with HISTORY')

    return pqa.QAScore(score, longmsg, shortmsg, origin=origin)


@log_qa
def score_observing_modes(mses: List[MeasurementSet]) -> List[pqa.QAScore]:
    """
    This QA heuristic evaluates a list of measurement sets, creating a QA score
    for each MS, and returning the aggregate list of QA scores for all MSes.
    Each MS is scored based on consistency checks between their registered
    Observing Mode(s) and e.g. the presence of differential gain SpWs / fields.

    Args:
        mses: list of measurement sets to score.

    Returns:
        List of QA scores.
    """
    # Create separate score for each MS.
    scores = []
    for ms in mses:
        # If the Observing Modes include "band to band", perform a few validity
        # checks w.r.t. the presence of DIFFGAIN* fields and diffgain SpW setup:
        if 'BandToBand Interferometry' in ms.observing_modes:
            if ms.get_diffgain_mode() != 'B2B':
                score = 0.0
                shortmsg = 'Incorrect Observing Mode'
                longmsg = f'Incorrect BandToBand Observing Mode, {ms.basename} does not contain a DIFFGAIN* intent' \
                          f' and/or SpW setup consistent with band-to-band.'
            elif len(ms.get_fields(intent="DIFFGAINREF,DIFFGAINSRC")) > 1:
                score = 0.0
                shortmsg = 'Too many DIFFGAIN* fields'
                longmsg = f'Unable to process BandToBand dataset {ms.basename}, found more than 1 DIFFGAIN* field'
            else:
                score = 0.9
                shortmsg = 'BandToBand mode used'
                longmsg = f'BandToBand mode used in {ms.basename}'

        # If the Observing Modes do not include "band to band", but the MS
        # contains a DIFFGAIN* intent and a SpW setup consistent with
        # band-to-band, then lower the score.
        elif 'BandToBand Interferometry' not in ms.observing_modes and ms.get_diffgain_mode() == 'B2B':
            score = 0.0
            shortmsg = 'Incorrect Observing Mode'
            longmsg = f'Incorrect Observing Mode, unexpectedly found a BandToBand DIFFGAIN* intent in {ms.basename}'

        # If the Observing Modes include "bandwidth switching", then lower the
        # score, since processing these data has not yet been validated.
        elif 'BandwidthSwitching Interferometry' in ms.observing_modes:
            score = 0.0
            shortmsg = 'BandwidthSwitching mode used'
            longmsg = f'BandwidthSwitching mode used in {ms.basename}'

        # If all validity checks are passed, score the MS as ok.
        else:
            score = 1.0
            shortmsg = 'Observing mode(s) ok.'
            longmsg = f'Observing mode(s) ok for {ms.basename}'

        # Append score for current MS.
        origin = pqa.QAOrigin(metric_name='score_observing_modes',
                              metric_score=score,
                              metric_units='MS score based on the observing modes')
        scores.append(pqa.QAScore(score, longmsg=longmsg, shortmsg=shortmsg, origin=origin))

    return scores


@log_qa
def score_bands(mses):
    """
    Score a MeasurementSet object based on the presence of
    ALMA bands with calibration issues.
    """

    # ALMA receiver bands. Warnings will be raised for any
    # measurement sets containing the following bands.
    score = 1.0
    score_map = {'9': -1.0,
                 '10': -1.0}

    unsupported = set(score_map.keys())

    num_mses = len(mses)
    all_ok = True
    complaints = []

    # analyse each MS
    for ms in mses:
        msbands = []
        for spw in ms.get_spectral_windows(science_windows_only=True):
            bandnum = spw.band.split(' ')[2]
            msbands.append(bandnum)
        msbands = set(msbands)
        overlap = unsupported.intersection(msbands)
        if not overlap:
            continue
        all_ok = False
        for m in overlap:
            score += (score_map[m] / num_mses)
        longmsg = ('%s contains band %s data'
                   '' % (ms.basename, utils.commafy(overlap, False)))
        complaints.append(longmsg)

    if all_ok:
        longmsg = ('No high frequency %s band data were found in %s.' % (list(unsupported),
                   utils.commafy([ms.basename for ms in mses], False)))
        shortmsg = 'No high frequency band data found'
    else:
        longmsg = '%s.' % utils.commafy(complaints, False)
        shortmsg = 'High frequency band data found'

    origin = pqa.QAOrigin(metric_name='score_bands',
                          metric_score=score,
                          metric_units='MS score based on presence of high-frequency data')

    # Make score linear
    return pqa.QAScore(max(rutils.SCORE_THRESHOLD_SUBOPTIMAL, score), longmsg=longmsg, shortmsg=shortmsg, origin=origin)


@log_qa
def score_parallactic_range(
        pol_intents_present: bool, session_name: str, field_name: str, coverage: float, threshold: float
        ) -> List[pqa.QAScore]:
    """
    Score a session based on parallactic angle coverage.

    Issues a warning if the parallactic coverage < threshold. See PIPE-597
    for full spec.
    """
    # holds the final list of QA scores
    scores: List[pqa.QAScore] = []

    # are polarisation intents expected? true if pol recipe, false if not
    # Polcal detected in session (this function was called!) but this is not a
    # polcal recipe, hence nothing to check
    if not pol_intents_present:
        longmsg = (f'No polarisation intents detected. No parallactic angle coverage check required for session '
                   f'{session_name}.')
        shortmsg = 'Parallactic angle'
        origin = pqa.QAOrigin(
            metric_name='ScoreParallacticAngle',
            metric_score=0.0,
            metric_units='degrees'
        )
        score = pqa.QAScore(1.0, longmsg=longmsg, shortmsg=shortmsg, origin=origin,
                            weblog_location=pqa.WebLogLocation.ACCORDION,
                            applies_to=pqa.TargetDataSelection(session={session_name}))
        return [score]

    # accordion message if coverage is adequate
    if coverage >= threshold:
        longmsg = (f'Sufficient parallactic angle coverage ({coverage:.2f}\u00B0 > {threshold:.2f}\u00B0) for '
                   f'polarisation calibrator {field_name} in session {session_name}')
        shortmsg = 'Parallactic angle'
        origin = pqa.QAOrigin(
            metric_name='ScoreParallacticAngle',
            metric_score=coverage,
            metric_units='degrees'
        )
        score = pqa.QAScore(1.0, longmsg=longmsg, shortmsg=shortmsg, origin=origin,
                            weblog_location=pqa.WebLogLocation.ACCORDION,
                            applies_to=pqa.TargetDataSelection(session={session_name}))
        scores.append(score)

    # complain with a banner message if coverage is insufficient
    else:
        longmsg = (f'Insufficient parallactic angle coverage ({coverage:.2f}\u00B0 < {threshold:.2f}\u00B0) for '
                   f'polarisation calibrator {field_name} in session {session_name}')
        shortmsg = 'Parallactic angle'
        origin = pqa.QAOrigin(
            metric_name='ScoreParallacticAngle',
            metric_score=coverage,
            metric_units='degrees'
        )
        score = pqa.QAScore(0.6, longmsg=longmsg, shortmsg=shortmsg, origin=origin,
                            weblog_location=pqa.WebLogLocation.BANNER,
                            applies_to=pqa.TargetDataSelection(session={session_name}))
        scores.append(score)

    return scores


@log_qa
def score_polintents(recipe_name: str, mses: List[domain.MeasurementSet]) -> List[pqa.QAScore]:
    """
    Score a MeasurementSet object based on the presence of
    polarization intents.
    """
    pol_intents = {'POLARIZATION', 'POLANGLE', 'POLLEAKAGE'}
    # these recipes are allowed to process polarisation data
    pol_recipes = {'hifa_polcal', 'hifa_polcalimage', 'hifa_polcal_renorm', 'hifa_polcalimage_renorm'}

    # Sort to ensure presentation consistency
    mses = sorted(mses, key=lambda ms: ms.basename)

    # are polarisation intents expected? true if pol recipe, false if not
    pol_intents_expected = recipe_name in pol_recipes

    # holds the final list of QA scores
    scores: List[pqa.QAScore] = []

    # Spec from PIPE-606:
    #
    # Currently, hifa_importdata sets the score of that stage to 0 if a
    # polarization calibrator is found (in qa/scorecalculator.py). This score
    # should now become 1 if one of these recipes is being run, or 0.5 if any
    # other recipe is being run.

    # analyse each MS, recording an accordion warning if there's an unexpected
    # pol calibrator

    if pol_intents_expected:
        pol_intents_present = any([pol_intents.intersection(ms.intents) for ms in mses])

        ms_names = {ms.basename for ms in mses}
        mses_for_msg = utils.commafy(sorted(ms_names), False)

        # and an 'all OK' accordion message if pol scans were expected and found
        if pol_intents_present:
            longmsg = f'Polarization calibrations expected and found: {mses_for_msg}'
            shortmsg = 'Polarization calibrators'
            origin = pqa.QAOrigin(metric_name='score_polintents',
                                  metric_score=1.0,
                                  metric_units='MS score based on presence of polarisation data')
            score = pqa.QAScore(1.0, longmsg=longmsg, shortmsg=shortmsg, origin=origin,
                                weblog_location=pqa.WebLogLocation.ACCORDION,
                                applies_to=pqa.TargetDataSelection(vis=ms_names))
            scores.append(score)

        # and complain with a banner message if they weren't. Perhaps this
        # should be part of score_missing_intents?
        else:
            longmsg = f'Expected polarization calibrations not found in {mses_for_msg}'
            shortmsg = 'Polarization calibrators'
            origin = pqa.QAOrigin(metric_name='score_polintents',
                                  metric_score=0.5,
                                  metric_units='MS score based on presence of polarisation data')
            score = pqa.QAScore(0.5, longmsg=longmsg, shortmsg=shortmsg, origin=origin,
                                weblog_location=pqa.WebLogLocation.BANNER,
                                applies_to=pqa.TargetDataSelection(vis=ms_names))
            scores.append(score)

        return scores

    # so, pol data not expected. Find any problem scans and record an accordion warning
    for ms in mses:
        pol_intents_in_ms = sorted(pol_intents.intersection(ms.intents))

        if pol_intents_in_ms:
            longmsg = f'{ms.basename} contains polarization calibrations: {utils.commafy(pol_intents_in_ms, False)}'
            shortmsg = 'Polarization intents'
            origin = pqa.QAOrigin(metric_name='score_polintents',
                                  metric_score=0.5,
                                  metric_units='MS score based on presence of polarisation data')
            score = pqa.QAScore(0.5, longmsg=longmsg, shortmsg=shortmsg, origin=origin,
                                weblog_location=pqa.WebLogLocation.ACCORDION,
                                applies_to=pqa.TargetDataSelection(vis=ms.basename))
            scores.append(score)

    # if there are accordion warnings, summarise them in a banner warning too
    if scores:
        affected_mses = {score.applies_to.vis for score in scores}
        longmsg = f'Unexpected polarization calibrations in {utils.commafy(affected_mses, False)}'
        shortmsg = 'Polarization intents'
        origin = pqa.QAOrigin(metric_name='score_polintents',
                              metric_score=0.5,
                              metric_units='MS score based on presence of polarisation data')
        score = pqa.QAScore(0.5, longmsg=longmsg, shortmsg=shortmsg, origin=origin,
                            weblog_location=pqa.WebLogLocation.BANNER,
                            applies_to=pqa.TargetDataSelection(vis=affected_mses))
        scores.append(score)

    # Add an 'all OK' accordion message if no unexpected intents detected
    if not scores and not pol_intents_expected:
        ms_names = {ms.basename for ms in mses}
        longmsg = f'No polarization calibrations found: {utils.commafy(sorted(ms_names), False)}'
        shortmsg = 'No polarization calibrators'
        origin = pqa.QAOrigin(metric_name='score_polintents',
                              metric_score=1.0,
                              metric_units='MS score based on presence of polarisation data')
        score = pqa.QAScore(1.0, longmsg=longmsg, shortmsg=shortmsg, origin=origin,
                            weblog_location=pqa.WebLogLocation.ACCORDION,
                            applies_to=pqa.TargetDataSelection(vis=ms_names))
        scores.append(score)

    return scores


@log_qa
def score_samecalobjects(recipe_name: str, mses: List[domain.MeasurementSet]) -> List[pqa.QAScore]:
    """
        Check if BP/Phcal/Ampcal are all the same object and score appropriately
    """
    alma_recipes = {'hifa_cal', 'hifa_calimage', 'hifa_calsurvey', 'hifa"image', 'hifa_polcal', 'hifa_polcalimage'}

    # Sort to ensure presentation consistency
    mses = sorted(mses, key=lambda ms: ms.basename)

    # We are just considering ALMA recipes.   True if an ALMA recipe, false if not
    alma_recipes_expected = recipe_name in alma_recipes

    # holds the final list of QA scores
    scores: List[pqa.QAScore] = []

    if alma_recipes_expected:
        for ms in mses:
            # Get list of calibrator names
            calfields = ms.get_fields(intent='AMPLITUDE,PHASE,BANDPASS')
            calfieldnames = [field.name for field in calfields]

            # Check for any duplicate names in the list of calibrator field names
            # samefieldnames = all(element == calfieldnames[0] for element in calfieldnames)
            samefieldnames = any(calfieldnames.count(x) > 1 for x in calfieldnames)
            if samefieldnames:
                scorevalue = 0.3
                msg = "Some calibrators are the same object."
            else:
                scorevalue = 1.0
                msg = "Calibrators are different objects."

            origin = pqa.QAOrigin(metric_name='score_samecalobjects',
                                  metric_score=scorevalue,
                                  metric_units='samecalobjects')

            score = pqa.QAScore(scorevalue, longmsg=msg, shortmsg=msg, origin=origin)

            scores.append(score)

    return scores


@log_qa
def score_missing_intents(mses, array_type='ALMA_12m'):
    """
    Score a MeasurementSet object based on the presence of certain
    observing intents.
    """
    # Required calibration intents. Warnings will be raised for any
    # measurement sets missing these intents
    score = 1.0
    if array_type == 'ALMA_TP':
        score_map = {'ATMOSPHERE': -1.0}
    elif array_type == 'VLA':
        score_map = {
            'PHASE': -1.0,
            # 'FLUX': -1.0,
            # CALIBRATE_FLUX and CALIBRATE_AMPLITUDE are both associated to 'AMPLITUDE'
            'AMPLITUDE': -1.0
        }
    else:
        score_map = {
            'PHASE': -0.7,
            'BANDPASS': -0.7,
            'AMPLITUDE': -0.7
        }

    required = set(score_map.keys())

    num_mses = len(mses)
    all_ok = True
    complaints = []

    # hold names of MSes this QA will be applicable to
    applies_to = set()

    # analyse each MS
    for ms in mses:
        # do we have the necessary calibrators?
        if not required.issubset(ms.intents):
            all_ok = False
            missing = required.difference(ms.intents)
            for m in missing:
                score += (score_map[m] / num_mses)

            longmsg = ('%s is missing %s calibration intents'
                       '' % (ms.basename, utils.commafy(missing, False)))
            complaints.append(longmsg)

            applies_to.add(ms.basename)

    if all_ok:
        longmsg = ('All required calibration intents were found in '
                   '%s.' % utils.commafy([ms.basename for ms in mses], False))
        shortmsg = 'All calibrators found'
        applies_to = {ms.basename for ms in mses}
    else:
        longmsg = '%s.' % utils.commafy(complaints, False)
        shortmsg = 'Calibrators missing'

    origin = pqa.QAOrigin(metric_name='score_missing_intents',
                          metric_score=score,
                          metric_units='Score based on missing calibration intents')

    return pqa.QAScore(
        max(0.0, score), longmsg=longmsg, shortmsg=shortmsg, origin=origin,
        applies_to=pqa.TargetDataSelection(vis=applies_to)
    )


@log_qa
def score_ephemeris_coordinates(mses):

    """
    Score a MeasurementSet object based on the presence of possible
    ephemeris coordinates.
    """

    score = 1.0

    num_mses = len(mses)
    all_ok = True
    complaints = []
    zero_direction = casa_tools.measures.direction('j2000', '0.0deg', '0.0deg')
    zero_ra = casa_tools.quanta.formxxx(zero_direction['m0'], format='hms', prec=3)
    zero_dec = casa_tools.quanta.formxxx(zero_direction['m1'], format='dms', prec=2)

    applies_to = set()

    # analyse each MS
    for ms in mses:
        # Examine each source
        for source in ms.sources:
            if source.ra == zero_ra and source.dec == zero_dec:
                all_ok = False
                score += (-1.0 / num_mses)
                longmsg = ('Suspicious source coordinates for  %s in %s. Check whether position of '
                           '00:00:00.0+00:00:00.0 is valid.' % (source.name, ms.basename))
                complaints.append(longmsg)
                applies_to.add(ms.basename)

    if all_ok:
        longmsg = ('All source coordinates OK in '
                   '%s.' % utils.commafy([ms.basename for ms in mses], False))
        shortmsg = 'All source coordinates OK'
        applies_to = {ms.basename for ms in mses}
    else:
        longmsg = '%s.' % utils.commafy(complaints, False)
        shortmsg = 'Suspicious source coordinates'

    origin = pqa.QAOrigin(metric_name='score_ephemeris_coordinates',
                          metric_score=score,
                          metric_units='Score based on presence of ephemeris coordinates')

    return pqa.QAScore(
        max(0.0, score), longmsg=longmsg, shortmsg=shortmsg, origin=origin,
        applies_to=pqa.TargetDataSelection(vis=applies_to)
    )


@log_qa
def score_online_shadow_template_agents(ms, summaries):
    """
    Get a score for the fraction of data flagged by online, shadow, and template agents.

    0 < score < 1 === 60% < frac_flagged < 5%
    """
    score = score_data_flagged_by_agents(ms, summaries, 0.05, 0.6,
                                         agents=['online', 'shadow', 'qa0', 'qa2', 'before', 'template'])

    new_origin = pqa.QAOrigin(metric_name='score_online_shadow_template_agents',
                              metric_score=score.origin.metric_score,
                              metric_units='Fraction of data newly flagged by online, shadow, and template agents')
    score.origin = new_origin

    return score


@log_qa
def score_lowtrans_flagcmds(ms, result):
    """
    Get a score for data flagged by the low transmission agent.

    Heuristic from PIPE-624: search for SpW(s) flagged for low transmission,
    and set score to:
    * 1 if no SpWs are flagged.
    * red warning threshold if the representative SpW is flagged, or otherwise,
    * blue suboptimal threshold if any non-representative SpW is flagged.
    """
    # Search in flag commands for SpW(s) flagged for low transmission.
    spws = []
    flagcmds = [f for f in result.flagcmds() if "low_transmission" in f]
    for flagcmd in flagcmds:
        match = re.search(r"spw='(\d*)'", flagcmd)
        if match:
            spws.append(int(match.group(1)))

    # If successful in finding SpWs that were flagged for low transmission,
    # then create a score that depends on whether the representative SpW was
    # among those flagged.
    if spws:
        # Get representative SpW for MS.
        _, rspw = ms.get_representative_source_spw()
        if rspw in spws:
            score = rutils.SCORE_THRESHOLD_ERROR
            longmsg = f"Representative SpW {rspw} flagged for low transmission"
            shortmsg = f"Representative SpW flagged for low transmission"
        else:
            score = rutils.SCORE_THRESHOLD_SUBOPTIMAL
            longmsg = f"Non-representative SpW(s) {', '.join(str(s) for s in spws)} flagged for low transmission"
            shortmsg = f"Non-representative SpW(s) flagged for low transmission"
    else:
        score = 1.0
        longmsg = "No SpW(s) flagged for low transmission"
        shortmsg = "No SpW(s) flagged for low transmission"

    origin = pqa.QAOrigin(metric_name='score_lowtrans',
                          metric_score=score,
                          metric_units='SpW(s) flagged by low transmission agent.')

    return pqa.QAScore(score, longmsg=longmsg, shortmsg=shortmsg, vis=ms.basename, origin=origin)


@log_qa
def score_vla_agents(ms, summaries):
    """
    Get a score for the fraction of data flagged by online, shadow, and template agents.

    0 < score < 1 === 60% < frac_flagged < 5%
    """
    score = score_vla_science_data_flagged_by_agents(ms, summaries, 0.05, 0.6,
                                                     ['online', 'template', 'autocorr', 'edgespw',
                                                      'clip', 'quack', 'baseband'])

    new_origin = pqa.QAOrigin(metric_name='score_vla_agents',
                              metric_score=score.origin.metric_score,
                              metric_units='Fraction of data newly flagged by online, shadow, and template agents')
    score.origin = new_origin

    return score


@log_qa
def score_applycal_agents(ms, summaries):
    """
    Get a score for the fraction of data flagged by applycal agents.

    0 < score < 1 === 60% < frac_flagged < 5%
    """
    agents = ['applycal']
    intents = ['TARGET']

    # Get score for 'applycal' agent and 'TARGET' intent.
    score = score_data_flagged_by_agents(ms, summaries, 0.05, 0.6, agents=agents, intents=intents)
    perc_flagged = 100. * score.origin.metric_score

    # Get score for all agents (total) for 'TARGET' intent.
    dummy_score = score_data_flagged_by_agents(ms, summaries, 0, 1, intents=['TARGET'])
    total_perc_flagged = 100. * (1. - dummy_score.score)

    # Recreate the long message from the applycal score to also mention the total
    # percentage flagged.
    longmsg = ("For {}, intent(s): {}, {:.2f}% of the data was newly flagged by {} flagging agents, for a total of"
               " {:.2f}% flagged.".format(ms.basename, utils.commafy(intents, quotes=False), perc_flagged,
                                          utils.commafy(agents, quotes=False), total_perc_flagged))
    score.longmsg = longmsg

    # Update origin.
    new_origin = pqa.QAOrigin(metric_name='score_applycal_agents',
                              metric_score=score.origin.metric_score,
                              metric_units=score.origin.metric_units)
    score.origin = new_origin

    return score


@log_qa
def score_total_data_flagged(filename, summaries):
    """
    Calculate a score for the flagging task based on the total fraction of
    data flagged.

    0%-5% flagged   -> 1
    5%-50% flagged  -> 0.5
    50-100% flagged -> 0
    """
    # Calculate fraction of flagged data.
    frac_flagged = calc_frac_total_flagged(summaries)

    # Convert fraction of flagged data into a score.
    if frac_flagged > 0.5:
        score = 0
    else:
        score = linear_score(frac_flagged, 0.05, 0.5, 1.0, 0.5)

    # Set score messages and origin.
    percent = 100.0 * frac_flagged
    longmsg = '%0.2f%% of data in %s was flagged' % (percent, filename)
    shortmsg = '%0.2f%% data flagged' % percent

    origin = pqa.QAOrigin(metric_name='score_total_data_flagged',
                          metric_score=frac_flagged,
                          metric_units='Total fraction of data that is flagged')

    return pqa.QAScore(score, longmsg=longmsg, shortmsg=shortmsg, vis=os.path.basename(filename), origin=origin)


@log_qa
def score_total_data_flagged_vla(filename, summaries):
    """
    Calculate a score for the flagging task based on the total fraction of
    data flagged.

    0%-5% flagged   -> 1
    5%-60% flagged  -> 1 to 0
    60-100% flagged -> 0
    """
    # Calculate fraction of flagged data.
    frac_flagged = calc_frac_newly_flagged(summaries)

    # Convert fraction of flagged data into a score.
    if frac_flagged > 0.6:
        score = 0
    else:
        score = linear_score(frac_flagged, 0.05, 0.6, 1.0, 0.0)

    # Set score messages and origin.
    percent = 100.0 * frac_flagged
    longmsg = '%0.2f%% of data in %s was flagged' % (percent, filename)
    shortmsg = '%0.2f%% data flagged' % percent

    origin = pqa.QAOrigin(metric_name='score_total_data_flagged_vla',
                          metric_score=frac_flagged,
                          metric_units='Total fraction of VLA data that is flagged')

    return pqa.QAScore(score, longmsg=longmsg, shortmsg=shortmsg, vis=os.path.basename(filename), origin=origin)


@log_qa
def score_total_data_flagged_vla_bandpass(filename, frac_flagged):
    """
    Calculate a score for the flagging task based on the data flagged in the bandpass table.

    0%-5% flagged   -> 1
    5%-60% flagged  -> 1 to 0
    60-100% flagged -> 0
    """

    # Convert fraction of flagged data into a score.
    if frac_flagged > 0.6:
        score = 0
    else:
        score = linear_score(frac_flagged, 0.05, 0.6, 1.0, 0.0)

    # Set score messages and origin.
    percent = 100.0 * frac_flagged
    longmsg = '%0.2f%% of data in %s was flagged' % (percent, filename)
    shortmsg = '%0.2f%% data flagged' % percent

    origin = pqa.QAOrigin(metric_name='score_total_data_flagged_vla_bandpass',
                          metric_score=frac_flagged,
                          metric_units='Total fraction of VLA data that is flagged in the caltable')

    return pqa.QAScore(score, longmsg=longmsg, shortmsg=shortmsg, vis=os.path.basename(filename), origin=origin)


@log_qa
def score_flagged_vla_baddef(amp_collection, phase_collection, num_antennas):
    """
    Calculate a score for the flagging task based on the number of antennas flagged.

    0% flagged   -> 1
    0%-30% flagged  -> 1 to 0
    30%-100% flagged -> 0

    frac_flagged -- fraction of antennas flagged
    """

    amp_antennas = set(amp_collection.keys())
    phase_antennas = set(phase_collection.keys())
    affected_antennas = amp_antennas.union(phase_antennas)
    num_affected_antennas = len(affected_antennas)
    frac_flagged = num_affected_antennas / float(num_antennas)
    origin = pqa.QAOrigin(metric_name='score_flagged_vla_baddef',
                          metric_score=frac_flagged,
                          metric_units='Fraction of VLA antennas flagged by hifv_flagbaddef')
    if 0 == frac_flagged:
        return pqa.QAScore(1, longmsg='No antennas flagged', shortmsg='No antennas flagged', origin=origin)
    else:
        # Convert fraction of flagged data into a score.
        score = linear_score(frac_flagged, 0.0, 0.3, 1.0, 0.0)
        # Set score messages and origin.
        percent = 100.0 * frac_flagged
        longmsg = "{:d} of {:d} ({:0.2f}%) antennas affected and some of their spws are flagged" \
                  "".format(num_affected_antennas, num_antennas, percent)
        shortmsg = "{:0.2f}% antennas affected".format(percent)
        return pqa.QAScore(score, longmsg=longmsg, shortmsg=shortmsg, origin=origin)


def countbaddelays(m, delaytable, delaymax):
    """
    Args:
        m: measurement set object
        delaytable: Delay caltable
        delaymax: units of ns.  If delay is over this value, it could be added to the dictionary

    Returns:
        Dictionary with antenna name as key
    """
    delaydict = collections.defaultdict(list)
    with casa_tools.TableReader(delaytable) as tb:
        spws = np.unique(tb.getcol('SPECTRAL_WINDOW_ID'))
        for ispw in spws:
            # byspw table must be written to disk in outer layer to avoid 'Table does not exist' error
            tbspw = tb.query(query='SPECTRAL_WINDOW_ID==' + str(ispw), name='byspw')
            ants = np.unique(tbspw.getcol('ANTENNA1'))
            for iant in ants:
                tbant = tbspw.query(query='ANTENNA1==' + str(iant))
                absdel = np.absolute(tbant.getcol('FPARAM'))
                if np.max(absdel) > delaymax:
                    antname = m.get_antenna(iant)[0].name
                    delaydict[antname].append((absdel > delaymax).sum())
                    LOG.info('Spw=' + str(ispw) + ' Ant=' + antname
                             + '  Delays greater than 200 ns ='
                             + str((absdel > delaymax).sum()))
                tbant.close()
            tbspw.close()
            # clean up byspw table after each iteration
            byspw = os.getcwd() + '/byspw'
            if os.path.exists(byspw):
                shutil.rmtree(byspw)

    return delaydict


@log_qa
def score_total_data_vla_delay(filename, m):
    """
    Use a filename of a delay (K-type) calibration table
    Calculate a score for antennas with a delay > 200 ns
    For each antenna with delays > 200 ns, reduce score by 0.1
    """

    with casa_tools.TableReader(filename) as tb:
        fpar = tb.getcol('FPARAM')
        delays = np.abs(fpar)  # Units of nanoseconds
        maxdelay = np.max(delays)

    if maxdelay < 200.0:
        score = 1.0
    else:
        # For each antenna with a delay > 200.0 ns, deduct 0.1 from the score
        delaydict = countbaddelays(m, filename, 200.0)
        count = len(delaydict)
        score = 1.0 - (0.1 * count)
    if score < 0.0:
        score = 0.0

    # Set score message and origin
    longmsg = 'Max delay is {!s} ns'.format(str(maxdelay))
    shortmsg = longmsg

    origin = pqa.QAOrigin(metric_name='score_total_data_vla_delay',
                          metric_score=score,
                          metric_units='Delays that exceed 200 ns')

    return pqa.QAScore(score, longmsg=longmsg, shortmsg=shortmsg, vis=os.path.basename(filename), origin=origin)


@log_qa
def score_vla_flux_residual_rms(fractional_residuals, num_spws, spixl):
    """
    score_vla_flux_residual_rms: calculates the score for pipeline task hifv_fluxboot

    output: returns QAscore
    --------- parameter descriptions ---------------------------------------------
    fractional_residuals: Take the RMS values of the residuals.
    nums_spws: number of spws
    spixl: list of a spectral index
    """

    # PIPE-119, part a
    max_res = max(max(res) for res in fractional_residuals)
    if max_res < 0.01:
        score = 1.0
    else:
        score = 1.0 - max_res

    # PIPE-119 part b
    for res in fractional_residuals:
        if max(np.abs(res))> 0.3:
            LOG.warning("Fractional residuals are > 0.3")
            break

    # PIPE-119 part c
    bool_spix = [True if eval(spix) < -3 or eval(spix) > 2 else False for spix in spixl]
    if num_spws > 1 and all(bool_spix):
        score = score - 0.5
        LOG.warning("spix for a band/s is <-3 or >2, reducing score by 0.5")

    if score < 0.0:
        score = 0.0

    # Set score message and origin
    try:
        longmsg = 'Max rms of the residuals is {!s}'.format(max_res)
    except Exception as e:
        longmsg = 'No max rms.'
    shortmsg = longmsg

    origin = pqa.QAOrigin(metric_name='score_vla_flux_residual_rms',
                          metric_score=score,
                          metric_units='rms values that exceed 0.01')

    return pqa.QAScore(score, longmsg=longmsg, shortmsg=shortmsg, origin=origin)


@log_qa
def score_fraction_newly_flagged(filename, summaries, vis):
    """
    Calculate a score for the flagging task based on the fraction of
    data newly flagged.

    0%-5% flagged   -> 1
    5%-50% flagged  -> 0.5
    50-100% flagged -> 0
    """
    # Calculate fraction of flagged data.
    frac_flagged = calc_frac_newly_flagged(summaries)

    # Convert fraction of flagged data into a score.
    if frac_flagged > 0.5:
        score = 0
    else:
        score = linear_score(frac_flagged, 0.05, 0.5, 1.0, 0.5)

    # Set score messages and origin.
    percent = 100.0 * frac_flagged
    longmsg = '%0.2f%% of data in %s was newly flagged' % (percent, filename)
    shortmsg = '%0.2f%% data flagged' % percent

    origin = pqa.QAOrigin(metric_name='score_fraction_newly_flagged',
                          metric_score=frac_flagged,
                          metric_units='Fraction of data that is newly flagged')

    return pqa.QAScore(score, longmsg=longmsg, shortmsg=shortmsg, vis=os.path.basename(vis), origin=origin)


@log_qa
def linear_score_fraction_newly_flagged(filename, summaries, vis):
    """
    Calculate a score for the flagging task based on the fraction of
    data newly flagged.

    fraction flagged   -> score
    """
    # Calculate fraction of flagged data.
    frac_flagged = calc_frac_newly_flagged(summaries)

    # Convert fraction of flagged data into a score.
    score = 1.0 - frac_flagged

    # Set score messages and origin.
    percent = 100.0 * frac_flagged
    longmsg = '%0.2f%% of data in %s was newly flagged' % (percent, filename)
    shortmsg = '%0.2f%% data flagged' % percent

    origin = pqa.QAOrigin(metric_name='linear_score_fraction_newly_flagged',
                          metric_score=frac_flagged,
                          metric_units='Fraction of data that is newly flagged')

    return pqa.QAScore(score, longmsg=longmsg, shortmsg=shortmsg, vis=os.path.basename(vis), origin=origin)


@log_qa
def linear_score_fraction_unflagged_newly_flagged_for_intent(ms, summaries, intent):
    """
    Calculate a score for the flagging task based on the fraction of unflagged
    data for scans belonging to specified intent that got newly flagged.

    If no unflagged data was found in the before summary, then return a score
    of 0.0.
    """

    # Identify scan IDs belonging to intent.
    scanids = [str(scan.id) for scan in ms.get_scans(scan_intent=intent)]

    # Calculate flags for scans belonging to intent.
    agent_stats = calc_flags_per_agent(summaries, scanids=scanids)

    # Calculate counts of unflagged data.
    unflaggedcount = agent_stats[0].total - agent_stats[0].flagged

    # If the "before" summary had unflagged data, then proceed to compute
    # the fraction fo unflagged data that got newly flagged.
    if unflaggedcount > 0:
        frac_flagged = functools.reduce(operator.add,
                                        [float(s.flagged)/unflaggedcount for s in agent_stats[1:]], 0)

        score = 1.0 - frac_flagged
        percent = 100.0 * frac_flagged
        longmsg = '{:0.2f}% of unflagged data with intent {} in {} was newly ' \
                  'flagged.'.format(percent, intent, ms.basename)
        shortmsg = '{:0.2f}% unflagged data flagged.'.format(percent)

        origin = pqa.QAOrigin(metric_name='linear_score_fraction_unflagged_newly_flagged_for_intent',
                              metric_score=frac_flagged,
                              metric_units='Fraction of unflagged data for intent '
                                           '{} that is newly flagged'.format(intent))
    # If no unflagged data was found at the start, return score of 0.
    else:
        score = 0.0
        longmsg = 'No unflagged data with intent {} found in {}.'.format(intent, ms.basename)
        shortmsg = 'No unflagged data.'
        origin = pqa.QAOrigin(metric_name='linear_score_fraction_unflagged_newly_flagged_for_intent',
                              metric_score=False,
                              metric_units='Presence of unflagged data.')

    # Append extra warning to QA message if score falls at-or-below the "warning" threshold.
    if score <= rutils.SCORE_THRESHOLD_WARNING:
        longmsg += ' Please investigate!'
        shortmsg += ' Please investigate!'

    return pqa.QAScore(score, longmsg=longmsg, shortmsg=shortmsg, vis=ms.basename, origin=origin)


@log_qa
def score_contiguous_session(mses, tolerance=datetime.timedelta(hours=1)):
    """
    Check whether measurement sets are contiguous in time.
    """
    # only need to check when given multiple measurement sets
    if len(mses) < 2:
        origin = pqa.QAOrigin(metric_name='score_contiguous_session',
                              metric_score=0,
                              metric_units='Non-contiguous measurement sets present')
        return pqa.QAScore(1.0,
                           longmsg='%s forms one continuous observing session.' % mses[0].basename,
                           shortmsg='Unbroken observing session',
                           vis=mses[0].basename,
                           origin=origin)

    # reorder MSes by start time
    by_start = sorted(mses,
                      key=lambda m: utils.get_epoch_as_datetime(m.start_time))

    # create an interval for each one, including our tolerance
    intervals = []
    for ms in by_start:
        start = utils.get_epoch_as_datetime(ms.start_time)
        end = utils.get_epoch_as_datetime(ms.end_time)
        interval = measures.TimeInterval(start - tolerance, end + tolerance)
        intervals.append(interval)

    # check whether the intervals overlap
    bad_mses = []
    for i, (interval1, interval2) in enumerate(zip(intervals[0:-1],
                                                   intervals[1:])):
        if not interval1.overlaps(interval2):
            bad_mses.append(utils.commafy([by_start[i].basename,
                                           by_start[i+1].basename]))

    if bad_mses:
        basenames = utils.commafy(bad_mses, False)
        longmsg = ('Measurement sets %s are not contiguous. They may be '
                   'miscalibrated as a result.' % basenames)
        shortmsg = 'Gaps between observations'
        score = 0.5
    else:
        basenames = utils.commafy([ms.basename for ms in mses])
        longmsg = ('Measurement sets %s are contiguous.' % basenames)
        shortmsg = 'Unbroken observing session'
        score = 1.0

    origin = pqa.QAOrigin(metric_name='score_contiguous_session',
                          metric_score=not bool(bad_mses),
                          metric_units='Non-contiguous measurement sets present')

    return pqa.QAScore(score, longmsg=longmsg, shortmsg=shortmsg, origin=origin)


@log_qa
def score_wvrgcal(ms_name, dataresult):

    wvr_score = dataresult.qa_wvr.overall_score
    score = wvr_score.copy()

    # create lists for disc, rms, and flagged antennas checks
    disc_list=[]
    rms_list=[]
    flagant_list=[]
    for WVRinfo in dataresult.wvr_infos:
        disc_list.append(WVRinfo.disc.value)
        rms_list.append(WVRinfo.rms.value)
        if WVRinfo.flag:
            flagant_list.append(WVRinfo.antenna)

    # limits for disc and rms triggers -
    # same as hard coded in wvrg_qa to make the remcloud
    # trigger result object boolean
    disc_max = 500 # in um
    rms_max = 500 # in um
    # subset lists for ants exceeding the limits
    disc_limit=[dval for dval in disc_list if dval > disc_max]
    rms_limit=[rval for rval in rms_list if rval > rms_max]

    qa_messages = []

    # check the booleans that pass important information
    if dataresult.PHnoisy:
        qa_messages.append('Only Bandpass used for WVR improvement assessment')
    if dataresult.suggest_remcloud:
        qa_messages.append('Remcloud suggested')

    if score > 1.0:
        # if nothing else score passes will be >1.0
        # truncate to 1.0 - ratio_score now holding improvement
        score = 1.0
        if len(flagant_list) > 0 or len(disc_limit) > 0 or len(rms_limit) > 0 or dataresult.PHnoisy is True:
            score = 0.9  # i.e. to blue as a maximum value
            # now adjust 0.1 per bad entry
            reduceBy =  len(flagant_list)*0.1
            reduceBy += len(disc_limit)*0.1
            reduceBy += len(rms_limit)*0.1
            score = score - reduceBy
            # Crude check for the message - check if flag or disc/rms
            if len(flagant_list) > 0:
                qa_messages.append('Flagged antenna(s)')
            if len(disc_limit) > 0:
                qa_messages.append('Elevated disc value(s)')
            if len(rms_limit) > 0:
                qa_messages.append('Elevated rms value(s)')
            # before making the score check if noisy BP was triggered
            if dataresult.BPnoisy:
                score = 0.66  # downgrade to yellow to trigger a warning
                qa_messages.append('Atmospheric phases appear unstable')
                if len(flagant_list) > 0 or len(disc_limit) > 0 or len(rms_limit) > 0 :
                    # inherit previous reduceBy values
                    score = score - reduceBy
                # new linear score for yellow truncation
                score = linear_score(score, 0.0, 0.66, 0.34, 0.66)
            else:
                score = linear_score(score, 0.0, 0.9, 0.67, 0.9)
                # i.e. inputs will be truncated to between 0.0 and 0.9, linfited to be then between 0.67 and 0.9 - blue

    # now for scores < 1.0
    elif score < 1.0:
        qa_messages.append('No WVR improvement')  # PIPE-1837 message changed, now below

        # presuming disc list and rms list are all filled
        if np.median(disc_list) > disc_max or np.median(rms_list) > rms_max:
            score = 0.33
            qa_messages.append('Elevated disc/rms value(s) - Check atmospheric phase stability')
            if len(flagant_list) > 0:
                reduceBy = len(disc_limit)*0.1
                qa_messages.append('Flagged antenna(s)')
                score = score - reduceBy
            score = linear_score(score, 0.0, 0.33, 0.0, 0.33)
            # i.e. inputs will be truncated to between 0.0 and 0.33, linfited to be then between 0.0 and 0.33 RED

        else:
            score = 0.66
            reduceBy = 0.0  # initiate due to PIPE-1837 if/else loops
            if len(flagant_list) > 0 or len(disc_limit) > 0 or len(rms_limit) > 0 :
                # now adjust 0.1 per bad entry
                reduceBy += len(flagant_list)*0.1
                reduceBy += len(disc_limit)*0.1
                reduceBy += len(rms_limit)*0.1
                score = score - reduceBy
                # Crude check for the message - check if flag or disc/rms
                if len(flagant_list) > 0:
                    qa_messages.append('Flagged antenna(s)')
                if len(disc_limit) > 0:
                    qa_messages.append('Elevated disc value(s)')
                if len(rms_limit) > 0:
                    qa_messages.append('Elevated rms value(s)')

            # PIPE-1837 before final yellow scoring we assess if the
            # phase rms from wvrg_qa was 'good' i.e. <1 radian
            # but only when there are no other WVR soln issues, i.e.
            # disc or rms are below the fixed limits - note
            # message changes explicitly if only BP is 'good' or both BP and Phase
            # technically the phase can be noisy due to SNR, not atmospheric variations
            if len(disc_limit) == 0 and len(rms_limit) == 0:
                # here we would check if initscore > 0.X: "limit'
                if dataresult.BPgood:
                    qa_messages.append('Bandpass ' + ('and Phase ' if dataresult.PHgood else '') +
                                       'calibrator atmospheric phase stability appears to be good')
                    score = 0.9 - reduceBy  # still account for flagged antennas
                    score = linear_score(score, 0.0, 0.9, 0.67, 0.9)
                else:
                    # we don't modify from the previous assessment - i.e. data seem ok, no poor rms or disc,
                    # but the phase RMS is not explicitly reported as good - I suspect some LB and HF might come here
                    qa_messages.append('Check atmospheric phase stability')
                    # if disc and rms didn't trigger but phase stability not reported as good - still yellow
                    score = linear_score(score, 0.0, 0.66, 0.34, 0.66)

            # Otherwise now we are back to yellow when disc or rms also triggered on any ant and append message now
            else:
                qa_messages.append('Check atmospheric phase stability')
                score = linear_score(score, 0.0, 0.66, 0.34, 0.66)
                # i.e. inputs will be truncated to between 0.0 and 0.66, linfited to be then between 0.34 and 0.66

    # join the short messages for the QA score (are these stored?? )
    qa_mesg = ' - '.join(qa_messages)

    if qa_mesg:
        longmsg = 'phase RMS improvement was %0.2f for %s - %s' % (wvr_score, ms_name, qa_mesg)
    else:
        longmsg = 'phase RMS improvement was %0.2f for %s' % (wvr_score, ms_name)

    # should be made always
    shortmsg = '%0.2fx improvement' % wvr_score

    origin = pqa.QAOrigin(metric_name='score_wvrgcal',
                          metric_score=wvr_score,
                          metric_units='Phase RMS improvement after applying WVR correction')

    return pqa.QAScore(score, longmsg=longmsg, shortmsg=shortmsg, vis=os.path.basename(ms_name), origin=origin)


@log_qa
def score_sdtotal_data_flagged(label, frac_flagged):
    """
    Calculate a score for the flagging task based on the total fraction of
    data flagged.

    0%-5% flagged   -> 1
    5%-50% flagged  -> 0.5
    50-100% flagged -> 0
    """
    if frac_flagged > 0.5:
        score = 0
    else:
        score = linear_score(frac_flagged, 0.05, 0.5, 1.0, 0.5)

    percent = 100.0 * frac_flagged
    longmsg = '%0.2f%% of data in %s was newly flagged' % (percent, label)
    shortmsg = '%0.2f%% data flagged' % percent

    origin = pqa.QAOrigin(metric_name='score_sdtotal_data_flagged',
                          metric_score=frac_flagged,
                          metric_units='Fraction of data newly flagged')

    return pqa.QAScore(score, longmsg=longmsg, shortmsg=shortmsg, vis=None, origin=origin)


@log_qa
def score_sdtotal_data_flagged_old(name, ant, spw, pol, frac_flagged, field=None):
    """
    Calculate a score for the flagging task based on the total fraction of
    data flagged.

    0%-5% flagged   -> 1
    5%-50% flagged  -> 0.5
    50-100% flagged -> 0
    """
    if frac_flagged > 0.5:
        score = 0
    else:
        score = linear_score(frac_flagged, 0.05, 0.5, 1.0, 0.5)

    percent = 100.0 * frac_flagged
    if field is None:
        longmsg = '%0.2f%% of data in %s (Ant=%s, SPW=%d, Pol=%d) was flagged' % (percent, name, ant, spw, pol)
    else:
        longmsg = ('%0.2f%% of data in %s (Ant=%s, Field=%s, SPW=%d, Pol=%s) was '
                   'flagged' % (percent, name, ant, field, spw, pol))
    shortmsg = '%0.2f%% data flagged' % percent

    origin = pqa.QAOrigin(metric_name='score_sdtotal_data_flagged_old',
                          metric_score=frac_flagged,
                          metric_units='Fraction of data newly flagged')

    return pqa.QAScore(score, longmsg=longmsg, shortmsg=shortmsg, vis=os.path.basename(name), origin=origin)


@log_qa
def score_tsysspwmap(ms, unmappedspws):
    """
    Score is equal to the fraction of unmapped windows
    """

    if len(unmappedspws) <= 0:
        score = 1.0
        longmsg = 'Tsys spw map is complete for %s ' % ms.basename
        shortmsg = 'Tsys spw map is complete'
    else:
        nscispws = len([spw.id for spw in ms.get_spectral_windows(science_windows_only=True)])
        if nscispws <= 0:
            score = 0.0
        else:
            score = float(nscispws - len(unmappedspws)) / float(nscispws)
        longmsg = 'Tsys spw map is incomplete for %s science window%s ' % (ms.basename,
                                                                           utils.commafy(unmappedspws, False, 's'))
        shortmsg = 'Tsys spw map is incomplete'

    origin = pqa.QAOrigin(metric_name='score_tsysspwmap',
                          metric_score=score,
                          metric_units='Fraction of unmapped Tsys windows')

    return pqa.QAScore(score, longmsg=longmsg, shortmsg=shortmsg, vis=ms.basename, origin=origin)


@log_qa
def score_setjy_measurements(ms, reqfields, reqintents, reqspws, measurements):
    """
    Score is equal to the ratio of the number of actual flux
    measurements to expected number of flux measurements
    """

    # Expected fields
    scifields = {field for field in ms.get_fields(reqfields, intent=reqintents)}

    # Expected science windows
    scispws = {spw.id for spw in ms.get_spectral_windows(reqspws, science_windows_only=True)}

    # Loop over the expected fields
    nexpected = 0
    for scifield in scifields:
        validspws = {spw.id for spw in scifield.valid_spws}
        nexpected += len(validspws.intersection(scispws))

    # Loop over the measurements
    nmeasured = 0
    for value in measurements.values():
        # Loop over the flux measurements
        nmeasured += len(value)

    # Compute score
    if nexpected == 0:
        score = 0.0
        longmsg = 'No flux calibrators for %s ' % ms.basename
        shortmsg = 'No flux calibrators'
    elif nmeasured == 0:
        score = 0.0
        longmsg = 'No flux measurements for %s ' % ms.basename
        shortmsg = 'No flux measurements'
    elif nexpected == nmeasured:
        score = 1.0
        longmsg = 'All expected flux calibrator measurements present for %s ' % ms.basename
        shortmsg = 'All expected flux calibrator measurements present'
    elif nmeasured < nexpected:
        score = float(nmeasured) / float(nexpected)
        longmsg = 'Missing flux calibrator measurements for %s %d/%d ' % (ms.basename, nmeasured, nexpected)
        shortmsg = 'Missing flux calibrator measurements'
    else:
        score = 0.0
        longmsg = 'Too many flux calibrator measurements for %s %d/%d' % (ms.basename, nmeasured, nexpected)
        shortmsg = 'Too many flux measurements'

    origin = pqa.QAOrigin(metric_name='score_setjy_measurements',
                          metric_score=score,
                          metric_units='Ratio of number of flux measurements to number expected')

    return pqa.QAScore(score, longmsg=longmsg, shortmsg=shortmsg, vis=ms.basename, origin=origin)


@log_qa
def score_number_antenna_offsets(ms, offsets):
    """
    Score is 1.0 if no antenna needed a position offset correction, and
    set to the "suboptimal" threshold if at least one antenna needed a
    correction.
    """
    nant_with_offsets = len(offsets) // 3

    if nant_with_offsets == 0:
        score = 1.0
        longmsg = 'No antenna position offsets for %s ' % ms.basename
        shortmsg = 'No antenna position offsets'
    else:
        # CAS-8877: if at least 1 antenna needed correction, then set the score
        # to the "suboptimal" threshold.
        score = rutils.SCORE_THRESHOLD_SUBOPTIMAL
        longmsg = '%d nonzero antenna position offsets for %s ' % (nant_with_offsets, ms.basename)
        shortmsg = 'Nonzero antenna position offsets'

    origin = pqa.QAOrigin(metric_name='score_number_antenna_offsets',
                          metric_score=nant_with_offsets,
                          metric_units='Number of antennas requiring position offset correction')

    return pqa.QAScore(score, longmsg=longmsg, shortmsg=shortmsg, vis=ms.basename, origin=origin)


@log_qa
def score_missing_derived_fluxes(ms, reqfields, reqintents, measurements):
    """
    Score is equal to the ratio of actual flux
    measurement to expected flux measurements
    """
    # Expected fields
    scifields = {field for field in ms.get_fields(reqfields, intent=reqintents)}

    # Expected science windows
    scispws = {spw.id for spw in ms.get_spectral_windows(science_windows_only=True)}

    # Loop over the expected fields
    nexpected = 0
    for scifield in scifields:
        validspws = {spw.id for spw in scifield.valid_spws}
        nexpected += len(validspws.intersection(scispws))

    # Loop over measurements
    nmeasured = 0
    for key, value in measurements.items():
        # Loop over the flux measurements
        for flux in value:
            fluxjy = getattr(flux, 'I').to_units(measures.FluxDensityUnits.JANSKY)
            uncjy = getattr(flux.uncertainty, 'I').to_units(measures.FluxDensityUnits.JANSKY)
            if fluxjy <= 0.0 or uncjy <= 0.0:
                continue
            nmeasured += 1

    # Compute score
    if nexpected == 0:
        score = 0.0
        longmsg = 'No secondary calibrators for %s ' % ms.basename
        shortmsg = 'No secondary calibrators'
    elif nmeasured == 0:
        score = 0.0
        longmsg = 'No derived fluxes for %s ' % ms.basename
        shortmsg = 'No derived fluxes'
    elif nexpected == nmeasured:
        score = 1.0
        longmsg = 'All expected derived fluxes present for %s ' % ms.basename
        shortmsg = 'All expected derived fluxes present'
    elif nmeasured < nexpected:
        score = float(nmeasured) / float(nexpected)
        longmsg = 'Missing derived fluxes for %s %d/%d' % (ms.basename, nmeasured, nexpected)
        shortmsg = 'Missing derived fluxes'
    else:
        score = 0.0
        longmsg = 'Extra derived fluxes for %s %d/%d' % (ms.basename, nmeasured, nexpected)
        shortmsg = 'Extra derived fluxes'

    origin = pqa.QAOrigin(metric_name='score_missing_derived_fluxes',
                          metric_score=score,
                          metric_units='Ratio of number of flux measurements to number expected')

    return pqa.QAScore(score, longmsg=longmsg, shortmsg=shortmsg, vis=ms.basename, origin=origin)


@log_qa
def score_refspw_mapping_fraction(ms, ref_spwmap):
    """
    Compute the fraction of science spws that have not been
    mapped to other windows.
    """
    if ref_spwmap == [-1]:
        score = 1.0
        longmsg = 'No mapped science spws for %s ' % ms.basename
        shortmsg = 'No mapped science spws'

        origin = pqa.QAOrigin(metric_name='score_refspw_mapping_fraction',
                              metric_score=0,
                              metric_units='Number of unmapped science spws')
    else:
        # Expected science windows
        scispws = {spw.id for spw in ms.get_spectral_windows(science_windows_only=True)}
        nexpected = len(scispws)

        nunmapped = 0
        for spwid in scispws:
            if spwid == ref_spwmap[spwid]:
                nunmapped += 1

        if nunmapped >= nexpected:
            score = 1.0
            longmsg = 'No mapped science spws for %s ' % ms.basename
            shortmsg = 'No mapped science spws'
        else:
            # Replace the previous score with a warning
            score = rutils.SCORE_THRESHOLD_WARNING
            longmsg = 'There are %d mapped science spws for %s ' % (nexpected - nunmapped, ms.basename)
            shortmsg = 'There are mapped science spws'

        origin = pqa.QAOrigin(metric_name='score_refspw_mapping_fraction',
                              metric_score=nunmapped,
                              metric_units='Number of unmapped science spws')

    return pqa.QAScore(score, longmsg=longmsg, shortmsg=shortmsg, vis=ms.basename, origin=origin)


@log_qa
def score_combine_spwmapping(ms, intent, field, spwmapping):
    """
    Evaluate whether or not a spw mapping is using combine.
    If not, then set score to 1. If so, then set score to the sub-optimal
    threshold (for blue info message).
    """
    if spwmapping.combine:
        score = rutils.SCORE_THRESHOLD_SUBOPTIMAL
        longmsg = f'Using combined spw mapping for {ms.basename}, intent={intent}, field={field}'
        shortmsg = 'Using combined spw mapping'
    else:
        score = 1.0
        longmsg = f'No combined spw mapping for {ms.basename}, intent={intent}, field={field}'
        shortmsg = 'No combined spw mapping'

    origin = pqa.QAOrigin(metric_name='score_check_phaseup_combine_mapping',
                          metric_score=spwmapping.combine,
                          metric_units='Using combined spw mapping')

    applies_to = pqa.TargetDataSelection(vis={ms.basename}, intent={intent}, field={field})

    return pqa.QAScore(score, longmsg=longmsg, shortmsg=shortmsg, vis=ms.basename, origin=origin, applies_to=applies_to)


@log_qa
def score_phaseup_mapping_fraction(ms, intent, field, spwmapping):
    """
    Compute the fraction of science spws that have not been
    mapped to other probably wider windows.
    """
    if not spwmapping.spwmap:
        nunmapped = len([spw for spw in ms.get_spectral_windows(science_windows_only=True)])
        score = 1.0
        longmsg = f'No spw mapping for {ms.basename}, intent={intent}, field={field}'
        shortmsg = 'No spw mapping'
    elif spwmapping.combine:
        nunmapped = 0
        score = rutils.SCORE_THRESHOLD_WARNING
        longmsg = f'Combined spw mapping for {ms.basename}, intent={intent}, field={field}'
        shortmsg = 'Combined spw mapping'
    else:
        # Expected science windows
        scispws = [spw for spw in ms.get_spectral_windows(science_windows_only=True)]
        scispwids = [spw.id for spw in ms.get_spectral_windows(science_windows_only=True)]
        nexpected = len(scispwids)

        nunmapped = 0
        samesideband = True
        for spwid, scispw in zip(scispwids, scispws):
            if spwid == spwmapping.spwmap[spwid]:
                nunmapped += 1
            else:
                if scispw.sideband != ms.get_spectral_window(spwmapping.spwmap[spwid]).sideband:
                    samesideband = False

        if nunmapped >= nexpected:
            score = 1.0
            longmsg = f'No spw mapping for {ms.basename}, intent={intent}, field={field}'
            shortmsg = 'No spw mapping'
        else:
            # Replace the previous score with a warning
            if samesideband is True:
                score = rutils.SCORE_THRESHOLD_SUBOPTIMAL
                longmsg = f'Spw mapping within sidebands for {ms.basename}, intent={intent}, field={field}'
                shortmsg = 'Spw mapping within sidebands'
            else:
                score = rutils.SCORE_THRESHOLD_WARNING
                longmsg = f'Spw mapping across sidebands required for {ms.basename}, intent={intent}, field={field}'
                shortmsg = 'Spw mapping across sidebands'

    origin = pqa.QAOrigin(metric_name='score_phaseup_mapping_fraction',
                          metric_score=nunmapped,
                          metric_units='Number of unmapped science spws')

    applies_to = pqa.TargetDataSelection(vis={ms.basename}, intent={intent}, field={field})

    return pqa.QAScore(score, longmsg=longmsg, shortmsg=shortmsg, vis=ms.basename, origin=origin, applies_to=applies_to)


@log_qa
def score_phaseup_spw_median_snr_for_phase(ms, field, spw, median_snr, snr_threshold):
    """
    Score the median achieved SNR for a given phase calibrator field and SpW.
    Introduced for hifa_spwphaseup (PIPE-665).
    """
    if median_snr <= 0.3 * snr_threshold:
        score = rutils.SCORE_THRESHOLD_ERROR
        shortmsg = 'Low median SNR'
        longmsg = f'For {ms.basename}, field={field} (intent=PHASE), SpW={spw}, the median achieved SNR' \
                  f' ({median_snr:.1f}) is <= 30% of the phase SNR threshold ({snr_threshold:.1f}).'
    elif median_snr <= 0.5 * snr_threshold:
        score = rutils.SCORE_THRESHOLD_WARNING
        shortmsg = 'Low median SNR'
        longmsg = f'For {ms.basename}, field={field} (intent=PHASE), SpW={spw}, the median achieved SNR' \
                  f' ({median_snr:.1f}) is <= 50% of the phase SNR threshold ({snr_threshold:.1f}).'
    elif median_snr <= 0.75 * snr_threshold:
        score = rutils.SCORE_THRESHOLD_SUBOPTIMAL
        shortmsg = 'Low median SNR'
        longmsg = f'For {ms.basename}, field={field} (intent=PHASE), SpW={spw}, the median achieved SNR' \
                  f' ({median_snr:.1f}) is <= 75% of the phase SNR threshold ({snr_threshold:.1f}).'
    else:
        score = 1.0
        shortmsg = 'Median SNR is ok'
        longmsg = f'For {ms.basename}, field={field} (intent=PHASE), SpW={spw}, the median achieved SNR' \
                  f' ({median_snr:.1f}) is > 75% of the phase SNR threshold ({snr_threshold:.1f}).'

    origin = pqa.QAOrigin(metric_name='score_phaseup_spw_median_snr',
                          metric_score=median_snr,
                          metric_units='Median SNR')

    applies_to = pqa.TargetDataSelection(vis={ms.basename}, field={field}, spw={spw})

    return pqa.QAScore(score, longmsg=longmsg, shortmsg=shortmsg, vis=ms.basename, origin=origin, applies_to=applies_to)


@log_qa
def score_decoherence_assessment(ms: MeasurementSet, phaserms_results, outlier_antennas: str):
    """
    Assess the cycle time phase RMS value, which is important as everything longer than a cycle time
    is corrected by phase referencing (in terms of atmospheric phase variations).

    Also checks the outlier antennas and the 80th percentile baseline with and without flagged antennas.
    """
    try:
        phasermscycle_p80: float = phaserms_results['phasermscycleP80']
        bl_p80: float = phaserms_results['blP80']
        bl_p80_orig: float = phaserms_results['blP80orig']

        initial_score = 1.0 - phasermscycle_p80/100.0
        RMSstring = str(round(phasermscycle_p80, 2))

        LOG.info("For {0}, the Phase RMS calculated over the cycle time for the unflagged baselines longer than 80th percentile is {1} \
                    deg".format(ms.basename, RMSstring))

        # Stable Phases, < 30 deg phaseRMS
        if initial_score > 0.7:
            base_score = 1.0
            shortmsg = "Excellent stability Phase RMS (<30deg)."
            longmsg = "For {0}, excellent stability: The baseline-based median phase RMS for baselines longer than P80 is {1} \
                        deg over the cycle time.".format(ms.basename, RMSstring)

            # Check for problem antennas and update the score if needed.
            # these are outliers >100 deg, or those beyond "outlier_limit" in SSFherusitics (6 MAD)
            if len(outlier_antennas) > 0:
                base_score = 0.9

        elif initial_score > 0.5 and initial_score <= 0.7:
            # 30 to 50 deg phase RMS: not really a problem just informative
            # that the phase noise is elevated - no 'need' to look
            # as 50 deg phase RMS can still cause ~30% decoherence
            base_score = 0.9
            shortmsg = "Stable conditions phase RMS (30-50deg)."
            longmsg = "For {0}, good stability: The baseline-based median phase RMS for baselines longer than P80 is {1} \
                            deg over the cycle time.".format(ms.basename, RMSstring)

        # These are high phase noise -
        # outliers have already been clipped past 100 degrees
        # and those >4 MAD above the P80 phase RMS value
        # so, if we still get here, the phases were poor/v.bad - or there
        # were too many antennas classed as bad in the analysis function
        elif initial_score <= 0.5 and initial_score > 0.3:
            # 50 - 70 deg phase RMS, i.e. 30-50% lost due to decoherence
            # The initial score is representative
            base_score = initial_score
            shortmsg = "Elevated Phase RMS (50-70deg) exceeds stable parameters."
            longmsg = "For {0}, elevated phase instability: The baseline-based median phase RMS for baselines longer than P80 is {1} \
                            deg over the cycle time. Some image artifacts/defects may occur.".format(ms.basename, RMSstring)

        elif initial_score <= 0.3:
            if initial_score <= 0.0:
                base_score = 0.0
            else:
                base_score = initial_score

            shortmsg = "High Phase RMS (>70deg) exceeds limit for poor stability"
            longmsg = "For {0}, very poor phase stability: The baseline-based median phase RMS for baselines longer than P80 is {1} \
                        deg over the cycle time. Significant image artifacts/defects may be present.".format(ms.basename, RMSstring)

        else:  # This should never happen
            base_score = 0.0
            shortmsg = "The phase RMS could not be assessed."
            longmsg = "For {}, the spatial structure function could not be assessed".format(ms)

        # Append antenna outlier information to longmsg if present
        if len(outlier_antennas) > 0:
            if len(outlier_antennas.split(",")) == 1:
                longmsg = "{0} {1} has higher phase RMS.".format(longmsg, outlier_antennas)
            else:
                longmsg = "{0} {1} have higher phase RMS".format(longmsg, outlier_antennas)

        # The P80 is shorter than the P80 of all data due to notable baseline flagging
        #       tbd but if the P80 is 10-15% lower than expected - i.e. can later impact QA2
        #       as the longer baselines have maybe been flagged out
        if bl_p80 < bl_p80_orig * 0.85:
            LOG.info("P80 of unflagged data is more than 15% shorter than P80 of all baselines due to baseline and antennas flags")
            if base_score == 1.0:
                base_score = 0.9
            longmsg = "{} P80 of unflagged data is more than 15% shorter than P80 of all baselines".format(longmsg)

    except:
        # For any error in the above:
        base_score = 0.0
        phasermscycle_p80 = 0.0
        shortmsg = "The phase RMS could not be assessed."
        longmsg = "For {}, the spatial structure function could not be assessed.".format(ms.basename)
        LOG.error(traceback.format_exc())

    # Create metric origin
    phase_stability_origin = pqa.QAOrigin(metric_name='Phase stability',
                                          metric_score=phasermscycle_p80,
                                          metric_units='Degrees')

    return pqa.QAScore(base_score, longmsg=longmsg, shortmsg=shortmsg, vis=ms.basename, origin=phase_stability_origin,
                       weblog_location=pqa.WebLogLocation.ACCORDION)


@log_qa
def score_phaseup_spw_median_snr_for_check(ms, field, spw, median_snr, snr_threshold):
    """
    Score the median achieved SNR for a given check source field and SpW.
    Introduced for hifa_spwphaseup (PIPE-665).
    """
    if median_snr <= 0.3 * snr_threshold:
        score = 0.7
        shortmsg = 'Low median SNR'
        longmsg = f'For {ms.basename}, field={field} (intent=CHECK), SpW={spw}, the median achieved SNR' \
                  f' ({median_snr:.1f}) is <= 30% of the phase SNR threshold ({snr_threshold:.1f}).'
    elif median_snr <= 0.5 * snr_threshold:
        score = 0.8
        shortmsg = 'Low median SNR'
        longmsg = f'For {ms.basename}, field={field} (intent=CHECK), SpW={spw}, the median achieved SNR' \
                  f' ({median_snr:.1f}) is <= 50% of the phase SNR threshold ({snr_threshold:.1f}).'
    elif median_snr <= 0.75 * snr_threshold:
        score = 0.9
        shortmsg = 'Low median SNR'
        longmsg = f'For {ms.basename}, field={field} (intent=CHECK), SpW={spw}, the median achieved SNR' \
                  f' ({median_snr:.1f}) is <= 75% of the phase SNR threshold ({snr_threshold:.1f}).'
    else:
        score = 1.0
        shortmsg = 'Median SNR is ok'
        longmsg = f'For {ms.basename}, field={field} (intent=CHECK), SpW={spw}, the median achieved SNR' \
                  f' ({median_snr:.1f}) is > 75% of the phase SNR threshold ({snr_threshold:.1f}).'

    origin = pqa.QAOrigin(metric_name='score_phaseup_spw_median_snr',
                          metric_score=median_snr,
                          metric_units='Median SNR')

    applies_to = pqa.TargetDataSelection(vis={ms.basename}, field={field}, spw={spw})

    return pqa.QAScore(score, longmsg=longmsg, shortmsg=shortmsg, vis=ms.basename, origin=origin, applies_to=applies_to)


@log_qa
def score_missing_phaseup_snrs(ms, spwids, phsolints):
    """
    Score is the fraction of spws with phaseup SNR estimates
    """
    # Compute the number of expected and missing SNR measurements
    nexpected = len(spwids)
    missing_spws = []
    for i in range(len(spwids)):
        if not phsolints[i]:
            missing_spws.append(spwids[i])
    nmissing = len(missing_spws)

    if nexpected <= 0:
        score = 0.0
        longmsg = 'No phaseup SNR estimates for %s ' % ms.basename
        shortmsg = 'No phaseup SNR estimates'
    elif nmissing <= 0:
        score = 1.0
        longmsg = 'No missing phaseup SNR estimates for %s ' % ms.basename
        shortmsg = 'No missing phaseup SNR estimates'
    else:
        score = float(nexpected - nmissing) / nexpected
        longmsg = 'Missing phaseup SNR estimates for spws %s in %s ' % \
            (missing_spws, ms.basename)
        shortmsg = 'Missing phaseup SNR estimates'

    origin = pqa.QAOrigin(metric_name='score_missing_phaseup_snrs',
                          metric_score=nmissing,
                          metric_units='Number of spws with missing SNR measurements')

    applies_to = pqa.TargetDataSelection(vis={ms.basename}, spw={spwid for spwid in spwids})

    return pqa.QAScore(score, longmsg=longmsg, shortmsg=shortmsg, vis=ms.basename, origin=origin, applies_to=applies_to)


@log_qa
def score_poor_phaseup_solutions(ms, spwids, nphsolutions, min_nsolutions):
    """
    Score is the fraction of spws with poor phaseup solutions
    """
    # Compute the number of expected and poor SNR measurements
    nexpected = len(spwids)
    poor_spws = []
    for i in range(len(spwids)):
        if not nphsolutions[i]:
            poor_spws.append(spwids[i])
        elif nphsolutions[i] < min_nsolutions:
            poor_spws.append(spwids[i])
    npoor = len(poor_spws)

    if nexpected <= 0:
        score = 0.0
        longmsg = 'No phaseup solutions for %s ' % ms.basename
        shortmsg = 'No phaseup solutions'
    elif npoor <= 0:
        score = 1.0
        longmsg = 'No poorly determined phaseup solutions for %s ' % ms.basename
        shortmsg = 'No poorly determined phaseup solutions'
    else:
        score = float(nexpected - npoor) / nexpected
        longmsg = 'Poorly determined phaseup solutions for spws %s in %s ' % \
            (poor_spws, ms.basename)
        shortmsg = 'Poorly determined phaseup solutions'

    origin = pqa.QAOrigin(metric_name='score_poor_phaseup_solutions',
                          metric_score=npoor,
                          metric_units='Number of poor phaseup solutions')

    applies_to = pqa.TargetDataSelection(vis={ms.basename}, spw={spwid for spwid in spwids})

    return pqa.QAScore(score, longmsg=longmsg, shortmsg=shortmsg, vis=ms.basename, origin=origin, applies_to=applies_to)


@log_qa
def score_missing_bandpass_snrs(ms, spwids, bpsolints):
    """
    Score is the fraction of spws with bandpass SNR estimates
    """

    # Compute the number of expected and missing SNR measurements
    nexpected = len(spwids)
    missing_spws = []
    for i in range(len(spwids)):
        if not bpsolints[i]:
            missing_spws.append(spwids[i])
    nmissing = len(missing_spws)

    if nexpected <= 0:
        score = 0.0
        longmsg = 'No bandpass SNR estimates for %s ' % ms.basename
        shortmsg = 'No bandpass SNR estimates'
    elif nmissing <= 0:
        score = 1.0
        longmsg = 'No missing bandpass SNR estimates for %s ' % ms.basename
        shortmsg = 'No missing bandpass SNR estimates'
    else:
        score = float(nexpected - nmissing) / nexpected
        longmsg = 'Missing bandpass SNR estimates for spws %s in%s ' % \
            (missing_spws, ms.basename)
        shortmsg = 'Missing bandpass SNR estimates'

    origin = pqa.QAOrigin(metric_name='score_missing_bandpass_snrs',
                          metric_score=nmissing,
                          metric_units='Number of missing bandpass SNR estimates')

    return pqa.QAScore(score, longmsg=longmsg, shortmsg=shortmsg, vis=ms.basename, origin=origin)


@log_qa
def score_poor_bandpass_solutions(ms, spwids, nbpsolutions, min_nsolutions):
    """
    Score is the fraction of spws with poor bandpass solutions
    """
    # Compute the number of expected and poor solutions
    nexpected = len(spwids)
    poor_spws = []
    for i in range(len(spwids)):
        if not nbpsolutions[i]:
            poor_spws.append(spwids[i])
        elif nbpsolutions[i] < min_nsolutions:
            poor_spws.append(spwids[i])
    npoor = len(poor_spws)

    if nexpected <= 0:
        score = 0.0
        longmsg = 'No bandpass solutions for %s ' % ms.basename
        shortmsg = 'No bandpass solutions'
    elif npoor <= 0:
        score = 1.0
        longmsg = 'No poorly determined bandpass solutions for %s ' % ms.basename
        shortmsg = 'No poorly determined bandpass solutions'
    else:
        score = float(nexpected - npoor) / nexpected
        longmsg = 'Poorly determined bandpass solutions for spws %s in %s ' % (poor_spws, ms.basename)
        shortmsg = 'Poorly determined bandpass solutions'

    origin = pqa.QAOrigin(metric_name='score_missing_bandpass_snrs',
                          metric_score=npoor,
                          metric_units='Number of poor bandpass solutions')

    return pqa.QAScore(score, longmsg=longmsg, shortmsg=shortmsg, vis=ms.basename, origin=origin)


@log_qa
def score_missing_phase_snrs(ms, spwids, snrs):
    """
    Score is the fraction of spws with SNR estimates
    """
    # Compute the number of expected and missing SNR measurements
    nexpected = len(spwids)
    missing_spws = []
    for i in range(len(spwids)):
        if not snrs[i]:
            missing_spws.append(spwids[i])
    nmissing = len(missing_spws)

    if nexpected <= 0:
        score = 0.0
        longmsg = 'No gaincal SNR estimates for %s ' % ms.basename
        shortmsg = 'No gaincal SNR estimates'
    elif nmissing <= 0:
        score = 1.0
        longmsg = 'No missing gaincal SNR estimates for %s ' % ms.basename
        shortmsg = 'No missing gaincal SNR estimates'
    else:
        score = float(nexpected - nmissing) / nexpected
        longmsg = 'Missing gaincal SNR estimates for spws %s in %s ' % (missing_spws, ms.basename)
        shortmsg = 'Missing gaincal SNR estimates'

    origin = pqa.QAOrigin(metric_name='score_missing_phase_snrs',
                          metric_score=nmissing,
                          metric_units='Number of missing phase SNR estimates')

    return pqa.QAScore(score, longmsg=longmsg, shortmsg=shortmsg, vis=ms.basename, origin=origin)


@log_qa
def score_poor_phase_snrs(ms, spwids, minsnr, snrs):
    """
    Score is the fraction of spws with poor snr estimates
    """
    # Compute the number of expected and poor solutions
    nexpected = len(spwids)
    poor_spws = []
    for i in range(len(spwids)):
        if not snrs[i]:
            poor_spws.append(spwids[i])
        elif snrs[i] < minsnr:
            poor_spws.append(spwids[i])
    npoor = len(poor_spws)

    if nexpected <= 0:
        score = 0.0
        longmsg = 'No gaincal SNR estimates for %s ' % \
            ms.basename
        shortmsg = 'No gaincal SNR estimates'
    elif npoor <= 0:
        score = 1.0
        longmsg = 'No low gaincal SNR estimates for %s ' % \
            ms.basename
        shortmsg = 'No low gaincal SNR estimates'
    else:
        score = float(nexpected - npoor) / nexpected
        longmsg = 'Low gaincal SNR estimates for spws %s in %s ' % \
            (poor_spws, ms.basename)
        shortmsg = 'Low gaincal SNR estimates'

    origin = pqa.QAOrigin(metric_name='score_poor_phase_snrs',
                          metric_score=npoor,
                          metric_units='Number of poor phase SNR estimates')

    return pqa.QAScore(score, longmsg=longmsg, shortmsg=shortmsg, vis=ms.basename, origin=origin)


@log_qa
def score_derived_fluxes_snr(ms, measurements):
    """
    Score the SNR of the derived flux measurements.

    See PIPE-644 for latest description.

    Linearly scale QA score based on SNR value, where
      QA = 1.0 if SNR >= 26.25
      QA = 0.66 if SNR < 5.0
    and QA linearly scales from 0.66 to 1.0 between SNR 5 to 26.25.

    These QA and SNR threshold were chosen such that SNR values in the range of
    5-20 should map to QA scores in the range that will show as a blue
    "suboptimal" level QA message.
    """
    # Loop over measurements
    nmeasured = 0
    score = 0.0
    minscore = 1.0
    minsnr = None
    snr_thresh = 26.25
    low_snr_flux = collections.defaultdict(list)

    for fieldid, field_measurements in measurements.items():
        # Loop over the flux measurements
        for measurement in field_measurements:
            fluxjy = measurement.I.to_units(measures.FluxDensityUnits.JANSKY)
            uncjy = measurement.uncertainty.I.to_units(measures.FluxDensityUnits.JANSKY)
            if fluxjy <= 0.0 or uncjy <= 0.0:
                continue
            snr = fluxjy / uncjy
            minsnr = snr if minsnr is None else min(minsnr, snr)
            nmeasured += 1
            score1 = linear_score(float(snr), 5.0, snr_thresh, 0.66, 1.0)
            minscore = min(minscore, score1)
            score += score1
            if score1 < 1.0:
                low_snr_flux[fieldid].append(measurement.spw_id)

    if nmeasured > 0:
        score /= nmeasured

    if nmeasured == 0:
        score = 0.0
        longmsg = 'No derived fluxes for %s ' % ms.basename
        shortmsg = 'No derived fluxes'
    elif minscore >= 1.0:
        score = 1.0
        longmsg = 'No low SNR derived fluxes for %s ' % ms.basename
        shortmsg = 'No low SNR derived fluxes'
    else:
        # Report which field(s) and SpW(s) had low SNR.
        fld_summaries = [f'field {fid}, SpW(s) {", ".join(str(s) for s in sorted(spwids))}'
                         for fid, spwids in sorted(low_snr_flux.items())]
        longmsg = f'For {ms.basename}, the fractional uncertainty in the derived scaling factor is large' \
                  f' (> {100/snr_thresh:.1f}%) for {"; ".join(fld_summaries)}. The calibrator may be too faint.'
        shortmsg = 'Uncertainty in some of the derived fluxes'

    origin = pqa.QAOrigin(metric_name='score_derived_fluxes_snr',
                          metric_score=minsnr,
                          metric_units='Minimum SNR of derived flux measurement')

    return pqa.QAScore(score, longmsg=longmsg, shortmsg=shortmsg, vis=ms.basename, origin=origin)


@log_qa
def score_path_exists(mspath, path, pathtype):
    """
    Score the existence of the path
        1.0 if it exist
        0.0 if it does not
    """
    if os.path.exists(path):
        score = 1.0
        longmsg = 'The %s file %s for %s was created' % (pathtype, os.path.basename(path), os.path.basename(mspath))
        shortmsg = 'The %s file was created' % pathtype
    else:
        score = 0.0
        longmsg = 'The %s file %s for %s was not created' % (pathtype, os.path.basename(path), os.path.basename(mspath))
        shortmsg = 'The %s file was not created' % pathtype

    origin = pqa.QAOrigin(metric_name='score_path_exists',
                          metric_score=bool(score),
                          metric_units='Path exists on disk')

    return pqa.QAScore(score, longmsg=longmsg, shortmsg=shortmsg, origin=origin)


@log_qa
def score_file_exists(filedir, filename, filetype):
    """
    Score the existence of a products file
        1.0 if it exists
        0.0 if it does not
    """
    if filename is None:
        score = 1.0
        longmsg = 'The %s file is undefined' % filetype
        shortmsg = 'The %s file is undefined' % filetype

        origin = pqa.QAOrigin(metric_name='score_file_exists',
                              metric_score=None,
                              metric_units='No %s file to check' % filetype)

        return pqa.QAScore(score, longmsg=longmsg, shortmsg=shortmsg, origin=origin)

    file_path = os.path.join(filedir, os.path.basename(filename))
    if os.path.exists(file_path):
        score = 1.0
        longmsg = 'The %s file has been exported' % filetype
        shortmsg = 'The %s file has been exported' % filetype
    else:
        score = 0.0
        longmsg = 'The %s file %s does not exist' % (filetype, os.path.basename(filename))
        shortmsg = 'The %s file does not exist' % filetype

    origin = pqa.QAOrigin(metric_name='score_file_exists',
                          metric_score=bool(score),
                          metric_units='File exists on disk')

    return pqa.QAScore(score, longmsg=longmsg, shortmsg=shortmsg, origin=origin)


@log_qa
def score_mses_exist(filedir, visdict):
    """
    Score the existence of the flagging products files
        1.0 if they all exist
        n / nexpected if some of them exist
        0.0 if none exist
    """

    nexpected = len(visdict)
    nfiles = 0
    missing = []

    for visname in visdict:
        file_path = os.path.join(filedir, os.path.basename(visdict[visname]))
        if os.path.exists(file_path):
            nfiles += 1
        else:
            missing.append(os.path.basename(visdict[visname]))

    if nfiles <= 0:
        score = 0.0
        longmsg = 'Final ms files %s are missing' % (','.join(missing))
        shortmsg = 'Missing final ms files'
    elif nfiles < nexpected:
        score = float(nfiles) / float(nexpected)
        longmsg = 'Final ms files %s are missing' % (','.join(missing))
        shortmsg = 'Missing final ms files'
    else:
        score = 1.0
        longmsg = 'No missing final ms  files'
        shortmsg = 'No missing final ms files'

    origin = pqa.QAOrigin(metric_name='score_mses_exist',
                          metric_score=len(missing),
                          metric_units='Number of missing ms product files')

    return pqa.QAScore(score, longmsg=longmsg, shortmsg=shortmsg, origin=origin)


@log_qa
def score_flags_exist(filedir, visdict):
    """
    Score the existence of the flagging products files
        1.0 if they all exist
        n / nexpected if some of them exist
        0.0 if none exist
    """
    nexpected = len(visdict)
    nfiles = 0
    missing = []

    for visname in visdict:
        file_path = os.path.join(filedir, os.path.basename(visdict[visname][0]))
        if os.path.exists(file_path):
            nfiles += 1
        else:
            missing.append(os.path.basename(visdict[visname][0]))

    if nfiles <= 0:
        score = 0.0
        longmsg = 'Final flag version files %s are missing' % (','.join(missing))
        shortmsg = 'Missing final flags version files'
    elif nfiles < nexpected:
        score = float(nfiles) / float(nexpected)
        longmsg = 'Final flag version files %s are missing' % (','.join(missing))
        shortmsg = 'Missing final flags version files'
    else:
        score = 1.0
        longmsg = 'No missing final flag version files'
        shortmsg = 'No missing final flags version files'

    origin = pqa.QAOrigin(metric_name='score_flags_exist',
                          metric_score=len(missing),
                          metric_units='Number of missing flagging product files')

    return pqa.QAScore(score, longmsg=longmsg, shortmsg=shortmsg, origin=origin)


@log_qa
def score_flagging_view_exists(filename, result):
    """
    Assign a score of zero if the flagging view cannot be computed
    """

    # By default, assume no flagging views were found.
    score = 0.0
    longmsg = 'No flagging views for %s' % filename
    shortmsg = 'No flagging views'

    # Check if this is a flagging result for a single metric, where
    # the flagging view is stored directly in the result.
    try:
        view = result.view
        if view:
            score = 1.0
            longmsg = 'Flagging views exist for %s' % filename
            shortmsg = 'Flagging views exist'
    except AttributeError:
        pass

    # Check if this flagging results contains multiple metrics,
    # and look for flagging views among components.
    try:
        # Set score to 1 as soon as a single metric contains a
        # valid flagging view.
        for metricresult in result.components.values():
            view = metricresult.view
            if view:
                score = 1.0
                longmsg = 'Flagging views exist for %s' % filename
                shortmsg = 'Flagging views exist'
    except AttributeError:
        pass

    origin = pqa.QAOrigin(metric_name='score_flagging_view_exists',
                          metric_score=bool(score),
                          metric_units='Presence of flagging view')

    return pqa.QAScore(score, longmsg=longmsg, shortmsg=shortmsg, vis=filename, origin=origin)


@log_qa
def score_applycmds_exist(filedir, visdict):
    """
    Score the existence of the apply commands products files
        1.0 if they all exist
        n / nexpected if some of them exist
        0.0 if none exist
    """
    nexpected = len(visdict)
    nfiles = 0
    missing = []

    for visname in visdict:
        file_path = os.path.join(filedir, os.path.basename(visdict[visname][1]))
        if os.path.exists(file_path):
            nfiles += 1
        else:
            missing.append(os.path.basename(visdict[visname][1]))

    if nfiles <= 0:
        score = 0.0
        longmsg = 'Final apply commands files %s are missing' % (','.join(missing))
        shortmsg = 'Missing final apply commands files'
    elif nfiles < nexpected:
        score = float(nfiles) / float(nexpected)
        longmsg = 'Final apply commands files %s are missing' % (','.join(missing))
        shortmsg = 'Missing final apply commands files'
    else:
        score = 1.0
        longmsg = 'No missing final apply commands files'
        shortmsg = 'No missing final apply commands files'

    origin = pqa.QAOrigin(metric_name='score_applycmds_exist',
                          metric_score=len(missing),
                          metric_units='Number of missing apply command files')

    return pqa.QAScore(score, longmsg=longmsg, shortmsg=shortmsg, origin=origin)


@log_qa
def score_caltables_exist(filedir, sessiondict):
    """
    Score the existence of the caltables products files
        1.0 if theu all exist
        n / nexpected if some of them exist
        0.0 if none exist
    """
    nexpected = len(sessiondict)
    nfiles = 0
    missing = []

    for sessionname in sessiondict:
        file_path = os.path.join(filedir, os.path.basename(sessiondict[sessionname][1]))
        if os.path.exists(file_path):
            nfiles += 1
        else:
            missing.append(os.path.basename(sessiondict[sessionname][1]))

    if nfiles <= 0:
        score = 0.0
        longmsg = 'Caltables files %s are missing' % (','.join(missing))
        shortmsg = 'Missing caltables files'
    elif nfiles < nexpected:
        score = float(nfiles) / float(nexpected)
        longmsg = 'Caltables files %s are missing' % (','.join(missing))
        shortmsg = 'Missing caltables files'
    else:
        score = 1.0
        longmsg = 'No missing caltables files'
        shortmsg = 'No missing caltables files'

    origin = pqa.QAOrigin(metric_name='score_caltables_exist',
                          metric_score=len(missing),
                          metric_units='Number of missing caltables')

    return pqa.QAScore(score, longmsg=longmsg, shortmsg=shortmsg, origin=origin)


@log_qa
def score_images_exist(filesdir, imaging_products_only, calimages, targetimages):
    if imaging_products_only:
        if len(targetimages) <= 0:
            score = 0.0
            metric = 0
            longmsg = 'No target images were exported'
            shortmsg = 'No target images exported'
        else:
            score = 1.0
            metric = len(targetimages)
            longmsg = '%d target images were exported' % (len(targetimages))
            shortmsg = 'Target images exported'
    else:
        if len(targetimages) <= 0 and len(calimages) <= 0:
            score = 0.0
            metric = 0
            longmsg = 'No images were exported'
            shortmsg = 'No images exported'
        else:
            score = 1.0
            metric = len(calimages) + len(targetimages)
            longmsg = '%d images were exported' % (len(calimages) + len(targetimages))
            shortmsg = 'Images exported'

    origin = pqa.QAOrigin(metric_name='score_images_exist',
                          metric_score=metric,
                          metric_units='Number of exported images')

    return pqa.QAScore(score, longmsg=longmsg, shortmsg=shortmsg, origin=origin)


def get_line_ranges(lines: List[List[Union[float, bool]]]) -> List[Tuple[int, int]]:
    """Get valid line ranges from list of line properties.

    Args:
        lines: List of lines. Each line is expressed as at least three
               values, [center, width, validity_flag], where the line
               is regarded as valid if validity_flag is True.

    Returns:
        List of (start, end) channels for valid lines
    """
    line_ranges = []
    for line in lines:
        center, width, is_valid = line[:3]
        if is_valid:
            chan_left = int(np.floor(center - width / 2 + 0.5))
            chan_right = int(np.floor(center + width / 2 + 0.5))
            line_ranges.append((chan_left, chan_right))

    return line_ranges


@log_qa
def examine_sd_edge_lines(line_ranges: List[Tuple[int, int]], nchan: int, edge: Tuple[int, int] = (0, 0)) -> float:
    """Examine the existence of lines at edge channels.

    This function checks if there are lines that spans edge channels.
    Excluded channels via edge parameter are taken into account.

    Args:
        line_ranges: List of line ranges
        nchan: Number of channels
        edge: Number of edge channels excluded from both sides

    Returns:
        True if there are lines that satisfy the condition above.
        Otherwise, False.
    """
    chan_leftmost = min(line[0] for line in line_ranges)
    chan_rightmost = max(line[1] for line in line_ranges)
    LOG.debug(
        'leftmost %s, rightmost %s, nchan %s, edge (%s, %s)',
        chan_leftmost, chan_rightmost, nchan, edge[0], edge[1]
    )
    return chan_leftmost <= edge[0] or nchan - 1 - edge[1] <= chan_rightmost


@log_qa
def examine_sd_wide_lines(line_ranges: List[Tuple[int, int]], nchan: int, edge: Tuple[int, int] = (0, 0)) -> float:
    """Examine the existence of wide lines.

    This function returns True if there is a line with the width
    exceeding 1/3 of spw bandwidth. If there are multiple lines
    and their coverage exceeds the criterion *in total*, the
    function also returns True. Otherwise, it returns False.
    Excluded channels via edge parameter are taken into account.

    Args:
        line_ranges: List of line ranges
        nchan: Number of channels
        edge: Number of edge channels excluded from both sides

    Returns:
        True if wide line exists. Otherwise, Flase. Please see
        the above description on more detailed explanation of
        return value.
    """
    # see PIPEREQ-304 for the origin of the value
    fraction = 1 / 3

    mask = np.zeros(nchan, dtype=np.uint8)
    for line in line_ranges:
        ch_start = max(line[0], 0)
        ch_end = min(line[1], nchan - 1) + 1
        mask[ch_start:ch_end] = 1

    start = edge[0]
    end = nchan - edge[1]
    effective_nchan = nchan - sum(edge)
    line_coverage = sum(mask[start:end])

    return line_coverage > effective_nchan * fraction


def select_deviation_masks(deviation_masks: dict, reduction_group_member: 'MSReductionGroupMember') -> List[Tuple[int, int]]:
    """Select deviation masks that belongs to given reduction group member.

    Args:
        deviation_masks: List of all deviation masks
        reduction_group_member: Reduction group member instance

    Returns:
        List of (start, end) channels for selected deviation masks
    """
    ms_name = reduction_group_member.ms.basename
    field_id = reduction_group_member.field_id
    spw_id = reduction_group_member.spw_id
    antenna_id = reduction_group_member.antenna_id
    return deviation_masks[ms_name].get((field_id, antenna_id, spw_id), [])


def channel_ranges_for_image(edge: Tuple[int, int], nchan: int, sideband: int, ranges: List[Tuple[int, int]]) -> List[Tuple[int, int]]:
    """Convert channel ranges in MS coordinate to those in image coordinate.

    Frequency coordinates are different between MS and image product.
    This method converts channel ranges in MS coordinate into the ranges
    in image coordinate. The conversion includes the following operations.

        - to reverse channels if spw is LSB
        - to shift channels by the amount specified by edge parameter

    Args:
        edge: Number of edge channels excluded
        nchan: Number of channels
        sideband: 1 for USB, -1 for LSB
        ranges: List of two tuples representing channel ranges

    Returns:
        Converted channel ranges
    """
    edge_left, edge_right = edge

    if sideband == -1:
        # LSB
        chan_offset = edge_right

        def _reverse_range(x):
            return nchan - 1 - x

        _ranges_image = (map(_reverse_range, x[::-1]) for x in ranges)

    else:
        # USB
        chan_offset = edge_left

        _ranges_image = ranges

    # offset channel ranges
    def _offset_range(x):
        return x - chan_offset

    _ranges_image = (map(_offset_range, x) for x in _ranges_image)

    return sorted(tuple(x) for x in _ranges_image)


@log_qa
def score_sd_line_detection(reduction_group: dict, result: 'SDBaselineResults') -> List[pqa.QAScore]:
    """Compute QA score based on the line detection result.

    QA scores are evaluated based on the line detection result for
    each combination of spw and field individually. Scoring scheme of
    individual results are as follows:

        - skip scoring if no lines were detected (no QAScore object)
        - 0.55 if there are lines extends to edge channels
        - 0.6 if there are lines that covers more than 1/3 of spw
          bandwidth in total
        - 1.0 otherwise

    If no lines were detected in any spws/fields so that no QAScore
    objects were created, this function creates QAScore object with
    the score of 0.8 and the message indicating that no lines were
    detected. Therefore, returned list should contain at least one
    QAScore object.

    In addition to the detected lines, deviation masks are also
    examined. Score will be 0.65 if deviation masks with any widths
    exist. Returned QAScore objects are the collection of scores from
    both spectral lines and deviation masks but they can be
    differentiated by the associated messages and/or metric units.

    Relevant JIRA tickets:
        PIPE-2136
        PIPEREQ-304

    Args:
        reduction_group: Reduction group
        result: Result object generated by hsd_baseline stage

    Returns:
        List of QAScore objects derived from line detection results
        and/or deviation masks
    """
    line_detection_scores = []
    # deviation_mask_qa_data collects set of (spw_id, antenna_id, masked_ranges)
    # data per field per EB
    deviation_mask_qa_data = {}
    deviation_mask_all = result.outcome['deviation_mask']

    # edge parameter
    # channels specified by edge will be excluded from resulting images
    _edge = result.outcome['edge']
    edge = (_edge, _edge) if isinstance(_edge, int) else tuple(_edge[:2])

    for bl in result.outcome['baselined']:
        reduction_group_id = bl['group_id']
        reduction_group_desc = reduction_group[reduction_group_id]
        member_list = bl['members']
        field_name = reduction_group_desc.field_name
        spw = reduction_group_desc[member_list[0]].spw
        # sideband is 1 for USB, -1 for LSB
        sideband = int(spw.sideband)
        nchan = reduction_group_desc.nchan

        # deviation mask
        for member_id in member_list:
            reduction_group_member = reduction_group_desc[member_id]
            deviation_masks = select_deviation_masks(
                deviation_mask_all, reduction_group_member
            )

            if deviation_masks:
                data_per_field = deviation_mask_qa_data.setdefault(field_name, collections.defaultdict(list))
                spw_id = reduction_group_member.spw_id
                ms_name = reduction_group_member.ms.origin_ms
                antenna_name = reduction_group_member.antenna_name
                data_per_field[ms_name].append((spw_id, antenna_name, deviation_masks))

        # spectral lines
        lines = get_line_ranges(bl['lines'])
        if len(lines) > 0:
            # sort lines by left channel
            lines.sort(key=operator.itemgetter(0))

            is_edge_line = examine_sd_edge_lines(lines, nchan, edge)
            is_wide_line = examine_sd_wide_lines(lines, nchan, edge)

            if is_edge_line and is_wide_line:
                score = 0.55  # min(0.55, 0.6)
                msg = 'Edge and wide lines were detected'
            elif is_edge_line:
                score = 0.55
                msg = 'Edge line was detected'
            elif is_wide_line:
                score = 0.6
                msg = 'Wide line was detected'
            else:
                # detected lines do not harm baseline subtraction
                score = 1.0
                msg = 'Line ranges were detected'

            spw_ids = set(reduction_group_desc[m].spw_id for m in member_list)
            spw_desc = ', '.join(map(str, spw_ids))
            shortmsg = f'{msg}.'
            longmsg = f'{msg} in Field {field_name}, Spw {spw_desc}.'
            lines_image = channel_ranges_for_image(edge, nchan, sideband, lines)
            metric_value = ';'.join([f'{left}~{right}' for left, right in lines_image])
            origin = pqa.QAOrigin(metric_name='score_sd_line_detection',
                                  metric_score=metric_value,
                                  metric_units='Channel range(s) of detected lines')
            selection = pqa.TargetDataSelection(spw=set(spw_desc.split(', ')),
                                                field=set([field_name]),
                                                intent={'TARGET'})
            line_detection_scores.append(
                pqa.QAScore(score, longmsg=longmsg, shortmsg=shortmsg, origin=origin, applies_to=selection)
            )

    if len(line_detection_scores) == 0:
        # add new entry with score of 0.8 if no spectral lines
        # were detected in any spws/fields
        score = 0.8
        longmsg = 'No line ranges were detected in all SPWs.'
        shortmsg = 'No line ranges were detected in all SPWs.'

        origin = pqa.QAOrigin(metric_name='score_sd_line_detection',
                              metric_score='N/A',
                              metric_units='Channel range(s) of detected lines')
        selection = pqa.TargetDataSelection(intent={'TARGET'})
        line_detection_scores.append(
            pqa.QAScore(
                score, longmsg=longmsg, shortmsg=shortmsg,
                origin=origin, applies_to=selection
            )
        )

    # dictionary for spw setup
    spw_config_dict = {}
    for reduction_group_desc in reduction_group.values():
        for reduction_group_member in reduction_group_desc:
            ms_name = reduction_group_member.ms.origin_ms
            spw = reduction_group_member.spw
            spw_id = spw.id
            nchan = spw.num_channels
            sideband = int(spw.sideband)
            dict_for_ms = spw_config_dict.setdefault(ms_name, {})
            dict_for_spw = dict_for_ms.setdefault(spw_id, {})
            dict_for_spw['nchan'] = nchan
            dict_for_spw['sideband'] = sideband

    # generate deviation mask QA scores with merged spws and antennas
    deviation_mask_scores = []
    for field_name, data_per_ms in deviation_mask_qa_data.items():
        for ms_name, data in data_per_ms.items():
            spw_ids = {x[0] for x in data}
            antenna_names = {x[1] for x in data}

            # score is fixed to 0.65. See PIPE-2136 and PIPEREQ-304.
            score = 0.65
            msg = 'Deviation mask was triggered'
            shortmsg = f'{msg}.'
            spw_string = ', '.join(map(str, sorted(spw_ids)))
            antenna_string = ', '.join(sorted(antenna_names))
            longmsg = f'{msg} in EB {ms_name}, Field {field_name}, Spw {spw_string}, Antenna {antenna_string}.'

            # get list of unique mask ranges per spw
            deviation_masks_per_spw = collections.defaultdict(set)
            for spw_id, _, deviation_mask in data:
                deviation_masks_per_spw[spw_id].update(map(tuple, deviation_mask))

            # metric value format: spw0:c0~c1:c2~c3,spw1:c4~c5
            deviation_masks_for_image = []
            for spw_id, deviation_masks in deviation_masks_per_spw.items():
                spw_config = spw_config_dict[ms_name][spw_id]
                nchan = spw_config['nchan']
                sideband = spw_config['sideband']
                devmasks_image = channel_ranges_for_image(edge, nchan, sideband, sorted(deviation_masks))
                deviation_masks_for_image.append((spw_id, devmasks_image))

            metric_value = ','.join([
                f'{spw_id}:' + ';'.join([f'{left}~{right}' for left, right in sorted(deviation_masks)])
                for spw_id, deviation_masks in deviation_masks_for_image
            ])
            origin = pqa.QAOrigin(metric_name='score_sd_line_detection',
                                  metric_score=metric_value,
                                  metric_units='Channel range(s) possibly affected by instrumental instabilities')
            selection = pqa.TargetDataSelection(vis=set([ms_name]),
                                                spw=spw_ids,
                                                field=set([field_name]),
                                                ant=antenna_names,
                                                intent={'TARGET'})
            deviation_mask_scores.append(
                pqa.QAScore(score, longmsg=longmsg, shortmsg=shortmsg, origin=origin, applies_to=selection)
            )

    return line_detection_scores + deviation_mask_scores



@log_qa
def score_sd_baseline_quality(vis: str, source: str, ant: str, vspw: str,
                              pol: str, stat: List[tuple]) -> pqa.QAScore:
    """
    Return Pipeline QA score of baseline quality.

    Args:
        vis: MS name
        source: source name
        ant: antenna name
        vspw: virtual spw ID
        pol: polarization
        stat: a list of binned statistics

    Returns:
        Pipeline QA score of baseline quality.
    """
    scores = []
    LOG.info(f'Statistics of {vis}: {source}, {ant}, {vspw}, {pol}')
    # See PIPEREQ-168 for details of QA metrics.
    # The values of bin_diff_ratio at the edges of ramp
    ramp_range = (1.8, 3.6)
    # The scores at the corresponding edges of ramp. These values are also
    # adopted in extrapolation beyond ramp_range.
    score_range = (1.0, 0.33)
    metric_func = interpolate.interp1d(ramp_range, score_range,
                                       kind='linear', bounds_error=False,
                                       fill_value=score_range)
    for s in stat:
        diff_score =  metric_func(s.bin_diff_ratio)
        scores.append(diff_score)
        LOG.info(f'rdiff = {s.bin_diff_ratio} -> score = {diff_score}')
    final_score = np.nanmin(scores)
    quality = 'Good'
    if final_score <= 0.66:
        quality='Poor'
    elif final_score <= 0.9:
        quality='Moderate'
    shortmsg = f'{quality} baseline flatness'
    longmsg = f'{quality} baseline flatness in {vis}, {source}, {ant}, virtual spw {vspw}, {pol}'
    origin = pqa.QAOrigin(metric_name='score_sd_baseline_quality',
                          metric_score=len(stat),
                          metric_units='Statistics of binned spectra')

    return pqa.QAScore(final_score, longmsg=longmsg, shortmsg=shortmsg, origin=origin)


@log_qa
def score_checksources(mses, fieldname, spwid, imagename, rms, gfluxscale, gfluxscale_err):
    """
    Score a single field image of a point source by comparing the source
    reference position to the fitted position and the source reference flux
    to the fitted flux.

    The source is assumed to be near the center of the image.
    The fit is performed using pixels in a circular regions
    around the center of the image
    """
    qa = casa_tools.quanta
    me = casa_tools.measures

    # Get the reference direction of the check source field
    #    There is at least one field with check source intent
    #    Protect against the same source having multiple fields
    #    with different intent.
    #    Assume that the same field as defined by its field name
    #    has the same direction in all the mses that contributed
    #    to the input image. Loop through the ms(s) and find the
    #    first occurrence of the specified field. Convert the
    #    direction to ICRS to match the default image coordinate
    #    system

    refdirection = None
    for ms in mses:
        field = ms.get_fields(name=fieldname)
        # No fields with the check source name. Should be
        # impossible at this point but check just in case
        if not field:
            continue
        # Find check field for that ms
        chkfield = None
        for fielditem in field:
            if 'CHECK' not in fielditem.intents:
                continue
            chkfield = fielditem
            break
        # No matching check field for that ms, next ms
        if chkfield is None:
            continue
        # Found field, get reference direction in ICRS coordinates
        LOG.info('Using field name %s id %s to determine check source reference direction' %
                 (chkfield.name, str(chkfield.id)))
        refdirection = me.measure(chkfield.mdirection, 'ICRS')
        break

    # Get the reference flux of the check source field
    #    Loop over all the ms(s) extracting the derived flux
    #    values for the specified field and spw. Set the reference
    #    flux to the maximum of these values.
    reffluxes = []
    for ms in mses:
        if not ms.derived_fluxes:
            continue
        for field_arg, measurements in ms.derived_fluxes.items():
            mfield = ms.get_fields(field_arg)
            chkfield = None
            for mfielditem in mfield:
                if mfielditem.name != fieldname:
                    continue
                if 'CHECK' not in mfielditem.intents:
                    continue
                chkfield = mfielditem
                break
            # No matching check field for this ms
            if chkfield is None:
                continue
            LOG.info('Using field name %s id %s to identify check source flux densities' %
                     (chkfield.name, str(chkfield.id)))
            for measurement in sorted(measurements, key=lambda m: int(m.spw_id)):
                if int(measurement.spw_id) != spwid:
                    continue
                for stokes in ['I']:
                    try:
                        flux = getattr(measurement, stokes)
                        flux_jy = float(flux.to_units(measures.FluxDensityUnits.JANSKY))
                        reffluxes.append(flux_jy)
                    except:
                        pass

    # Use the maximum reference flux
    if not reffluxes:
        refflux = None
    else:
        median_flux = np.median(np.array(reffluxes))
        refflux = qa.quantity(median_flux, 'Jy')

    # Do the fit and compute positions offsets and flux ratios
    fitdict = checksource.checkimage(imagename, rms, refdirection, refflux)

    msnames = ','.join(utils.remove_trailing_string(os.path.basename(ms.name), '.ms') for ms in mses)

    # Compute the scores the default score is the geometric mean of
    # the position and flux scores if both are available.
    if not fitdict:
        offset = None
        offset_err = None
        beams = None
        beams_err = None
        fitflux = None
        fitflux_err = None
        fitpeak = None
        score = 0.34
        longmsg = 'Check source fit failed for %s field %s spwid %d' % (msnames, fieldname, spwid)
        shortmsg = 'Check source fit failed'
        metric_score = 'N/A'
        metric_units = 'Check source fit failed'

    else:
        offset = fitdict['positionoffset']['value'] * 1000.0
        offset_err = fitdict['positionoffset_err']['value'] * 1000.0
        beams = fitdict['beamoffset']['value']
        beams_err = fitdict['beamoffset_err']['value']
        fitflux = fitdict['fitflux']['value']
        fitflux_err = fitdict['fitflux_err']['value']
        fitpeak = fitdict['fitpeak']['value']
        shortmsg = 'Check source fit successful'

        warnings = []

        offset_score = 0.0
        offset_metric = 'N/A'
        offset_unit = 'beams'
        if beams is None:
            warnings.append('unfitted offset')
        else:
            offset_score = max(0.33, 1.0 - min(1.0, beams))
            offset_metric = beams
            if beams > 0.30:
                warnings.append('large fitted offset of %.2f marcsec and %.2f synth beam' % (offset, beams))

        fitflux_score = 0.0
        fitflux_metric = 'N/A'
        fitflux_unit = 'fitflux/refflux'
        if gfluxscale is None:
            warnings.append('undefined gfluxscale result')
        elif gfluxscale == 0.0:
            warnings.append('gfluxscale value of 0.0 mJy')
        else:
            chk_fitflux_gfluxscale_ratio = fitflux * 1000. / gfluxscale
            fitflux_score = max(0.33, 1.0 - abs(1.0 - chk_fitflux_gfluxscale_ratio))
            fitflux_metric = chk_fitflux_gfluxscale_ratio
            if chk_fitflux_gfluxscale_ratio < 0.8:
                warnings.append('low [Fitted / gfluxscale] Flux Density Ratio of %.2f' % (chk_fitflux_gfluxscale_ratio))

        fitpeak_score = 0.0
        fitpeak_metric = 'N/A'
        fitpeak_unit = 'fitpeak/fitflux'
        if fitflux is None:
            warnings.append('undefined check fit result')
        elif fitflux == 0.0:
            warnings.append('Fitted Flux Density value of 0.0 mJy')
        else:
            chk_fitpeak_fitflux_ratio = fitpeak / fitflux
            fitpeak_score = max(0.33, 1.0 - abs(1.0 - (chk_fitpeak_fitflux_ratio)))
            fitpeak_metric = chk_fitpeak_fitflux_ratio
            if chk_fitpeak_fitflux_ratio < 0.7:
                warnings.append('low Fitted [Peak Intensity / Flux Density] Ratio of %.2f' % (chk_fitpeak_fitflux_ratio))

        snr_msg = ''
        if gfluxscale is not None and gfluxscale_err is not None:
            if gfluxscale_err != 0.0:
                chk_gfluxscale_snr = gfluxscale / gfluxscale_err
                if chk_gfluxscale_snr < 20.:
                    snr_msg = ', however, the S/N of the gfluxscale measurement is low'

        if any(np.array([offset_score, fitflux_score, fitpeak_score]) < 1.0):
            score = math.sqrt(offset_score * fitflux_score * fitpeak_score)
        else:
            score = offset_score * fitflux_score * fitpeak_score
        metric_score = [offset_metric, fitflux_metric, fitpeak_metric]
        metric_units = '%s, %s, %s' % (offset_unit, fitflux_unit, fitpeak_unit)

        if warnings != []:
            longmsg = 'EB %s field %s spwid %d: has a %s%s' % (msnames, fieldname, spwid, ' and a '.join(warnings), snr_msg)
            # Log warnings only if they would not be logged by the QA system (score <= 0.66)
            if score > 0.66:
                LOG.warning(longmsg)
        else:
            if score <= 0.9:
                longmsg = 'EB %s field %s spwid %d: Check source fit not optimal' % (msnames, fieldname, spwid)
            else:
                longmsg = 'EB %s field %s spwid %d: Check source fit successful' % (msnames, fieldname, spwid)

        if score <= 0.9:
            shortmsg = 'Check source fit not optimal'

    origin = pqa.QAOrigin(metric_name='ScoreChecksources',
                          metric_score=metric_score,
                          metric_units=metric_units)

    return pqa.QAScore(score, longmsg=longmsg, shortmsg=shortmsg, origin=origin), offset, offset_err, beams, beams_err, fitflux, fitflux_err, fitpeak


@log_qa
def score_multiply(scores_list):
    score = functools.reduce(operator.mul, scores_list, 1.0)
    longmsg = 'Multiplication of scores.'
    shortmsg = 'Multiplication of scores.'
    origin = pqa.QAOrigin(metric_name='score_multiply',
                          metric_score=len(scores_list),
                          metric_units='Number of multiplied scores.')

    return pqa.QAScore(score, longmsg=longmsg, shortmsg=shortmsg, origin=origin)


@log_qa
def score_sd_skycal_elevation_difference(ms, resultdict, threshold=3.0):
    """
    """
    field_ids = list(resultdict.keys())
    metric_score = []
    el_threshold = threshold
    lmsg_list = []
    for field_id in field_ids:
        field = ms.fields[field_id]
        if field_id not in resultdict:
            continue

        eldiffant = resultdict[field_id]
        warned_antennas = set()
        for antenna_id, eldiff in eldiffant.items():
            for spw_id, eld in eldiff.items():
                preceding = eld.eldiff0
                subsequent = eld.eldiff1
                # LOG.info('field {} antenna {} spw {} preceding={}'.format(field_id, antenna_id, spw_id, preceding))
                # LOG.info('field {} antenna {} spw {} subsequent={}'.format(field_id, antenna_id, spw_id, subsequent))
                max_pred = None
                max_subq = None
                if len(preceding) > 0:
                    max_pred = np.abs(preceding).max()
                    metric_score.append(max_pred)
                    if max_pred >= el_threshold:
                        warned_antennas.add(antenna_id)
                if len(subsequent) > 0:
                    max_subq = np.abs(subsequent).max()
                    metric_score.append(max_subq)
                    if max_subq >= el_threshold:
                        warned_antennas.add(antenna_id)
                LOG.debug('field {} antenna {} spw {} metric_score {}'.format(field_id, antenna_id, spw_id, metric_score))

        if len(warned_antennas) > 0:
            antenna_names = ', '.join([ms.antennas[a].name for a in warned_antennas])
            lmsg_list.append(
                'field {} (antennas {})'.format(field.name, antenna_names)
            )

    if len(lmsg_list) > 0:
        longmsg = 'Elevation difference between ON and OFF exceeds threshold ({}deg) for {}: {}'.format(
            el_threshold,
            ms.basename,
            ', '.join(lmsg_list)
        )
    else:
        longmsg = 'Elevation difference between ON and OFF is below threshold ({}deg) for {}'.format(
            el_threshold,
            ms.basename
        )

    # CAS-11054: it is decided that we do not calculate QA score based on elevation difference for Cycle 6
    # PIPE-246: we implement QA score based on elevation difference for Cycle 7.
    #           requirement is that score is 0.8 if elevation difference is larger than 3deg.
    # make sure threshold is 3deg
    assert el_threshold == 3.0
    max_metric_score = np.max(metric_score)
    # lower the score if elevation difference exceeds 3deg
    score = 1.0 if max_metric_score < el_threshold else 0.8
    origin = pqa.QAOrigin(metric_name='OnOffElevationDifference',
                          metric_score=max_metric_score,
                          metric_units='deg')

    if score < 1.0:
        shortmsg = 'Elevation difference between ON and OFF exceeds {}deg'.format(el_threshold)
    else:
        shortmsg = 'Elevation difference between ON and OFF is below {}deg'.format(el_threshold)

    return pqa.QAScore(score, longmsg=longmsg, shortmsg=shortmsg, origin=origin, vis=ms.basename)


def generate_metric_mask(context, result, cs, mask):
    """
    Generate boolean mask array for metric calculation in
    score_sdimage_masked_pixels. If image pixel contains
    observed points, mask will be True. Otherwise, mask is
    False.

    Arguments:
        context {Context} -- Pipeline context
        result {SDImagingResultItem} -- result item created by
                                        hsd_imaging
        cs {coordsys} -- CASA coordsys tool
        mask {bool array} -- image mask

    Returns:
        bool array -- metric mask (True: valid, False: invalid)
    """
    outcome = result.outcome
    org_direction = outcome['image'].org_direction
    imshape = mask.shape

    file_index = np.asarray(outcome['file_index'])
    antenna_list = np.asarray(outcome['assoc_antennas'])
    field_list = np.asarray(outcome['assoc_fields'])
    vspw_list = np.asarray(outcome['assoc_spws'])

    mses = context.observing_run.measurement_sets
    ms_list = [mses[i] for i in file_index]
    spw_list = np.asarray([context.observing_run.virtual2real_spw_id(i, m) for i, m in zip(vspw_list, ms_list)])

    ra = []
    dec = []
    ofs_ra = []
    ofs_dec = []
    online_flag = []

    for i in range(len(ms_list)):
        origin_basename = os.path.basename(ms_list[i].origin_ms)
        datatable_name = os.path.join(context.observing_run.ms_datatable_name, origin_basename)
        rotable_name = os.path.join(datatable_name, 'RO')
        rwtable_name = os.path.join(datatable_name, 'RW')
        _index = np.where(file_index == file_index[i])
        if len(_index[0]) == 0:
            continue

        _antlist = antenna_list[_index]
        _fieldlist = field_list[_index]
        _spwlist = spw_list[_index]

        with casa_tools.TableReader(rotable_name) as tb:
            unit_ra = tb.getcolkeyword('OFS_RA', 'UNIT')
            unit_dec = tb.getcolkeyword('OFS_DEC', 'UNIT')
            tsel = tb.query('SRCTYPE==0&&ANTENNA IN {}&&FIELD_ID IN {}&&IF IN {}'.format(list(_antlist), list(_fieldlist), list(_spwlist)))
            ofs_ra.extend(tsel.getcol('OFS_RA'))
            ofs_dec.extend(tsel.getcol('OFS_DEC'))
            rows = tsel.rownumbers()
            tsel.close()

        with casa_tools.TableReader(rwtable_name) as tb:
            permanent_flag = tb.getcol('FLAG_PERMANENT').take(rows, axis=2)
            online_flag.extend(permanent_flag[0, OnlineFlagIndex])

    if org_direction is None:
        ra = np.asarray(ofs_ra)
        dec = np.asarray(ofs_dec)
    else:
        for rr, dd in zip(ofs_ra, ofs_dec):
            shift_ra, shift_dec = direction_recover( rr, dd, org_direction )
            ra.append(shift_ra)
            dec.append(shift_dec)
        ra = np.asarray(ra)
        dec = np.asarray(dec)
    online_flag = np.asarray(online_flag)

    del ofs_ra, ofs_dec

    metric_mask = np.empty(imshape, dtype=bool)
    metric_mask[:] = False

    qa = casa_tools.quanta

    # template measure for world-pixel conversion
    # 2019/06/03 TN
    # Workaround for memory consumption issue (PIPE-362)
    # cs.topixel consumes some amount of memory and it accumulates,
    # too many call of cs.topixel results in unexpectedly large amount of
    # memory usage. To avoid cs.topixel, approximate mapping to pixel
    # coordinate is done manually.
    blc = cs.toworld([-0.5, -0.5, 0, 0], format='q')
    brc = cs.toworld([imshape[0] - 0.5, -0.5, 0, 0], format='q')
    tlc = cs.toworld([-0.5, imshape[1] - 0.5, 0, 0], format='q')
    trc = cs.toworld([imshape[0] - 0.5, imshape[1] - 0.5, 0, 0], format='q')
    #print('blc {} {}'.format(blc['quantity']['*1'], blc['quantity']['*2']))
    #print('brc {} {}'.format(brc['quantity']['*1'], brc['quantity']['*2']))
    #print('tlc {} {}'.format(tlc['quantity']['*1'], tlc['quantity']['*2']))
    #print('trc {} {}'.format(trc['quantity']['*1'], trc['quantity']['*2']))
    #print('cen {} {}'.format(cen['quantity']['*1'], cen['quantity']['*2']))
    cpi = qa.convert(qa.quantity(180, 'deg'), unit_ra)['value']
    s0 = (qa.convert(tlc['quantity']['*1'], unit_ra)['value'] - qa.convert(blc['quantity']['*1'], unit_ra)['value']) \
        / (qa.convert(tlc['quantity']['*2'], unit_dec)['value'] - qa.convert(blc['quantity']['*2'], unit_dec)['value'])
    t0 = ((qa.convert(blc['quantity']['*1'], unit_ra)['value']) + cpi) % (cpi * 2) - cpi
    s1 = (qa.convert(trc['quantity']['*1'], unit_ra)['value'] - qa.convert(brc['quantity']['*1'], unit_ra)['value']) \
        / (qa.convert(trc['quantity']['*2'], unit_dec)['value'] - qa.convert(brc['quantity']['*2'], unit_dec)['value'])
    t1 = ((qa.convert(brc['quantity']['*1'], unit_ra)['value']) + cpi) % (cpi * 2) - cpi
    ymax = (qa.convert(tlc['quantity']['*2'], unit_dec)['value'] + qa.convert(trc['quantity']['*2'], unit_dec)['value']) / 2
    ymin = (qa.convert(blc['quantity']['*2'], unit_dec)['value'] + qa.convert(brc['quantity']['*2'], unit_dec)['value']) / 2
    dy = (ymax - ymin) / imshape[1]
    #print('s0 {} t0 {} s1 {} t1 {}'.format(s0, t0, s1, t1))
    #print('ymax {} ymin {} dy {}'.format(ymax, ymin, dy))
    #world = cs.toworld([0, 0, 0, 0], format='m')
    px = np.empty_like(ra)
    py = np.empty_like(dec)
    px[:] = -1
    py[:] = -1
    for i, (x, y, f) in enumerate(zip(ra, dec, online_flag)):
        if f != 1:
            # PIPE-439 flagged pointing data are not taken into account
            continue

        #world['measure']['direction']['m0']['value'] = qa.quantity(x, unit_ra)
        #world['measure']['direction']['m1']['value'] = qa.quantity(y, unit_dec)
        #p = cs.topixel(world)
        #px[i] = p['numeric'][0]
        #py[i] = p['numeric'][1]
        y0 = y
        xmin = s0 * (y - y0) + t0
        xmax = s1 * (y - y0) + t1
        dx = (xmax - xmin) / imshape[0]
        #print('xmin {} xmax {} dx {}'.format(xmin, xmax, dx))
        #print('x {} y {}'.format(x, y))
        py[i] = (y - ymin) / dy - 0.5
        px[i] = (x - xmin) / dx - 0.5
        #print('WORLD {} {} <-> PIXEL {} {}'.format(x, y, px[i], py[i]))

    for x, y in zip(map(int, np.round(px)), map(int, np.round(py))):
        #print(x, y)
        if 0 <= x and x <= imshape[0] - 1 and 0 <= y and y <= imshape[1] - 1:
            metric_mask[x, y, :, :] = True

    # exclude edge channels
    edge_channels = [i for i in range(imshape[3]) if np.all(mask[:, :, :, i] == False)]
    LOG.debug('edge channels: {}'.format(edge_channels))
    metric_mask[:, :, :, edge_channels] = False

    return metric_mask


def direction_recover( ra, dec, org_direction ):
    me = casa_tools.measures
    qa = casa_tools.quanta

    direction = me.direction( org_direction['refer'],
                              str(ra)+'deg', str(dec)+'deg' )
    zero_direction  = me.direction( org_direction['refer'], '0deg', '0deg' )
    offset = me.separation( zero_direction, direction )
    posang = me.posangle( zero_direction, direction )
    new_direction = me.shift( org_direction, offset=offset, pa=posang )
    new_ra  = qa.convert( new_direction['m0'], 'deg' )['value']
    new_dec = qa.convert( new_direction['m1'], 'deg' )['value']

    return new_ra, new_dec


@log_qa
def score_sdimage_masked_pixels(context, result):
    """
    Evaluate QA score based on the fraction of masked pixels in image.


    Requirements (PIPE-249):
        - calculate the number of masked pixels in image
        - search area should be the extent of pointing direction
        - QA score should be
            1.0 (if the nuber of masked pixel == 0)
            0.5 (if any of the pixel in pointing area is masked)
            0.0 (if 10% of the pixels in pointing area are masked)
            *linearly interpolate between 0.5 and 0.0

    Arguments:
        context {Context} -- Pipeline context
        result {SDImagingResultItem} -- Imaging result instance

    Returns:
        QAScore -- QAScore instance holding the score based on number of
                   masked pixels in image
    """
    # metric score is a fraction of masked pixels
    result_item = result.outcome
    image_item = result_item['image']
    imagename = image_item.imagename

    LOG.debug('imagename = {}'.format(imagename))
    with casa_tools.ImageReader(imagename) as ia:
        # get mask
        # Mask Definition: True is valid, False is invalid.
        mask = ia.getchunk(getmask=True)

        imageshape = ia.shape()

        # cs represents coordinate system of the image
        cs = ia.coordsys()

    # image shape: (lon, lat, stokes, freq)
    LOG.debug('image shape: {}'.format(list(imageshape)))

    try:
        # metric_mask is boolean array that defines the region to be excluded
        #    True: included in the metric calculation
        #   False: excluded from the metric calculation
        # TODO: decide if any margin is necessary
        metric_mask = generate_metric_mask(context, result, cs, mask)
    finally:
        # done using coordsys tool
        cs.done()

    # calculate metric_score
    total_pixels = mask[metric_mask]
    LOG.debug('Total number of pixels to be included in: {}'.format(len(total_pixels)))
    masked_pixels = total_pixels[total_pixels == False]
    if len(total_pixels) == 0:
        LOG.warning('No pixels associated with pointing data exist. QA score will be zero.')
        metric_score = -1.0
    else:
        masked_fraction = float(len(masked_pixels)) / float(len(total_pixels))
        LOG.debug('Number of masked pixels: {} fraction {}'.format(len(masked_pixels), masked_fraction))
        metric_score = masked_fraction

    # score should be evaluated from metric score
    lmsg = ''
    smsg = ''
    score = 0.0
    metric_score_threshold = 0.1
    metric_score_max = 1.0
    metric_score_min = 0.0

    # convert score and threshold for logging purpose
    frac2percentage = lambda x: '{:.4g}%'.format(x * 100)
    imbasename = os.path.basename(imagename.rstrip('/'))

    if metric_score > metric_score_max:
        # metric_score should not exceed 1.0. something wrong.
        _x = frac2percentage(metric_score_max)
        lmsg = '{}: fraction of number of masked pixels should not exceed {}. something went wrong.'.format(imbasename, _x)
        smsg = 'metric value out of range.'
        score = 0.0
    elif metric_score < metric_score_min:
        lmsg = '{}: No pixels associated with pointing data exist. something went wrong.'.format(imbasename)
        smsg = 'metric value out of range.'
        score = 0.0
    elif metric_score == metric_score_min:
        lmsg = 'All examined pixels in image {} are valid.'.format(imbasename)
        smsg = 'All examined pixels are valid.'
        score = 1.0
    elif metric_score > metric_score_threshold:
        _x = frac2percentage(metric_score_threshold)
        _y = frac2percentage(metric_score)
        lmsg = 'Fraction of masked pixels in image {} is {}, exceeding threshold value ({}).'.format(imbasename, _y, _x)
        smsg = 'More than {} of image pixels are masked.'.format(_x)
        score = 0.0
    else:
        # interpolate between 0.5 and 0.0
        _x = frac2percentage(metric_score)
        lmsg = 'Fraction of masked pixels in image {} is {}.'.format(imbasename, _x)
        smsg = '{} of image pixels are masked.'.format(_x)
        smax = 0.5
        mmax = 0.0
        smin = 0.0
        mmin = 0.1
        score = (smax - smin) / (mmax - mmin) * (metric_score - mmin) + smin

    origin = pqa.QAOrigin(metric_name='SingleDishImageMaskedPixels',
                          metric_score=metric_score,
                          metric_units='Fraction of masked pixels in image')

    return pqa.QAScore(score,
                       longmsg=lmsg,
                       shortmsg=smsg,
                       origin=origin)


@log_qa
def score_sdimage_contamination(context: 'Context', result: 'SDImagingResultItem') -> pqa.QAScore:
    """Evaluate QA score based on the absorption feature in the image.

    If there is an emission at OFF_SOURCE position (contamination),
    it is emerged as an absorption feature in the calibrated spectra.
    Therefore, this QA score utilizes any significant absorption
    features as an indicator of potential contamination.

    Requirements (PIPE-2066):
        - QA score should be
          - 0.65 if absorption feature exists
          - 1.0 if absorption feature does not exist

    Args:
        context: Pipeline context
        result: Imaging result instance

    Returns:
        QAScore -- QAScore instance holding the score based on the
                   existence of the absorption feature in the image
    """
    contaminated = result.outcome.get('contaminated', False)
    imageitem = result.outcome['image']
    field = imageitem.sourcename
    spw = ','.join(map(str, np.unique(imageitem.spwlist)))
    if contaminated:
        lmsg = (f'Field {field} Spw {spw}: '
                'Possible astronomical line contamination was detected. '
                'Please check the contamination plots.')
        smsg = 'Possible astronomical line contamination was detected.'
        score = 0.65
    else:
        lmsg = (f'Field {field} Spw {spw}: '
                'No astronomical line contamintaion was detected.')
        smsg = 'No astronomical line contamination was detected.'
        score = 1.0

    origin = pqa.QAOrigin(metric_name='SingleDishImageContamination',
                          metric_score=contaminated,
                          metric_units='Sign of possible line contamination')
    selection = pqa.TargetDataSelection(spw=set(result.outcome['assoc_spws']),
                                        field=set(result.outcome['assoc_fields']),
                                        intent={'TARGET'},
                                        pol={'I'})
    return pqa.QAScore(score,
                       longmsg=lmsg,
                       shortmsg=smsg,
                       origin=origin,
                       applies_to=selection)


@log_qa
def score_gfluxscale_k_spw(vis, field, spw_id, k_spw, ref_spw):
    """ Convert internal spw_id-spw_id consistency ratio to a QA score.

    See CAS-10792 for full specification.
    See PIPE-644 for update to restrict range of scores.

    k_spw is equal to the ratio:

                       calibrated visibility flux / catalogue flux
    k_spw = --------------------------------------------------------------------
            (calibrated visibility flux / catalogue flux) for highest SNR window

    Q_spw = abs(1 - k_spw)

    If        Q_spw < 0.1 then QA score = 1.0  (green)
    If 0.1 <= Q_spw < 0.2 then QA score = 0.75 (Blue/below standard)
    If 0.2 <= Q_spw       then QA score = 0.5  (Yellow/warning)

    :param k_spw: numeric k_spw ratio, as per spec
    :param vis: name of measurement set to which k_spw applies
    :param field: field domain object to which k_spw applies
    :param spw_id: name of spectral window to which k_spw applies
    :return: QA score
    """
    q_spw = abs(1-k_spw)
    if q_spw < 0.1:
        score = 1.0
    elif q_spw < 0.2:
        score = 0.75
    else:
        score = 0.5

    longmsg = ('Ratio of <i>S</i><sub>calibrated</sub>/<i>S</i><sub>catalogue</sub> for {} ({}) spw {} in {} differs by'
               ' {:.0%} from the ratio for the highest SNR spw ({})'
               ''.format(utils.dequote(field.name), ','.join(field.intents), spw_id, vis, q_spw, ref_spw))
    shortmsg = 'Internal spw-spw consistency'

    origin = pqa.QAOrigin(metric_name='score_gfluxscale_k_spw',
                          metric_score=float(k_spw),
                          metric_units='Number of spws with missing SNR measurements')

    return pqa.QAScore(score, longmsg=longmsg, shortmsg=shortmsg, vis=vis, origin=origin)


@log_qa
def score_science_spw_names(mses, virtual_science_spw_names):
    """
    Check that all MSs have the same set of spw names. If this is
    not the case, the virtual spw mapping will not work.
    """

    score = 1.0
    msgs = []
    for ms in mses:
        spw_msgs = []
        for s in ms.get_spectral_windows(science_windows_only=True):
            if s.name not in virtual_science_spw_names:
                score = 0.0
                spw_msgs.append('{0} (ID {1})'.format(s.name, s.id))
        if spw_msgs != []:
            msgs.append('Science spw names {0} of EB {1} do not match spw names of first EB.'
                        ''.format(','.join(spw_msgs), os.path.basename(ms.name).replace('.ms', '')))

    if msgs == []:
        longmsg = 'Science spw names match virtual spw lookup table'
        shortmsg = 'Science spw names match'
    else:
        longmsg = '{0} Virtual spw ID mapping will not work.'.format(' '.join(msgs))
        shortmsg = 'Science spw names do not match'

    origin = pqa.QAOrigin(metric_name='score_spwnames',
                          metric_score=score,
                          metric_units='spw names match virtual spw name lookup table')

    return pqa.QAScore(score, longmsg=longmsg, shortmsg=shortmsg, origin=origin)


def score_renorm(result):
    if result.renorm_applied:
        msg = 'Restore successful with renormalization applied'
        score = rutils.SCORE_THRESHOLD_SUBOPTIMAL
    else:
        msg = 'Restore successful'
        score = 1.0

    origin = pqa.QAOrigin(metric_name='score_renormalize',
                          metric_score=score,
                          metric_units='')
    return pqa.QAScore(score, longmsg=msg, shortmsg=msg, origin=origin)


@log_qa
def score_polcal_gain_ratio(session_name: str, ant_names: dict, xyratio_result: GaincalResults,
                            threshold: float = 0.1) -> List[pqa.QAScore]:
    """
    This QA heuristic inspects the gain ratios in an X/Y gain ratio caltable
    and creates a score based on how large the deviation from one is.

    Args:
        session_name: name of session being evaluated.
        ant_names: dictionary mapping antenna IDs to names.
        xyratio_result: Gaincal task result object containing the
            CalApplication for the caltable to analyze.
        threshold: threshold used to determine whether the gain ratio deviates
            too much from 1 (resulting in a lowered score).
    Returns:
        List of QAScore objects.
    """
    scores = []
    # Score each caltable in result.
    for calapp in xyratio_result.final:
        # Retrieve data from caltable.
        with casa_tools.TableReader(calapp.gaintable) as table:
            gains = np.squeeze(table.getcol('CPARAM'))
            spws = table.getcol('SPECTRAL_WINDOW_ID')
            ants = table.getcol('ANTENNA1')

        # Score each SpW separately.
        for spwid in sorted(set(spws)):
            ind_spw = np.where(spws == spwid)[0]
            ratios = np.abs(gains[0, ind_spw]) / np.abs(gains[1, ind_spw])
            ind_bad = np.where(np.abs(1 - ratios) > threshold)[0]

            if len(ind_bad) > 0:
                score = 0.65
                ant_str = utils.commafy([f"{ant_names[i]} (#{i})" for i in ants[ind_bad]], quotes=False)
                longmsg = f"Session '{session_name}' has gain ratios deviate from 1 by more than {threshold} for SpW" \
                          f" {spwid}, antenna(s) {ant_str}"
                shortmsg = "Large gain ratio deviation"
                origin = pqa.QAOrigin(metric_name='score_polcal_gain_ratio',
                                      metric_score=score,
                                      metric_units='gain ratio')
                scores.append(pqa.QAScore(score, longmsg=longmsg, shortmsg=shortmsg, origin=origin))

    # If no large deviations in gain ratio are found, then create a good score.
    if not scores:
        score = 1.0
        longmsg = f"Session '{session_name}' has gain ratios with deviations from 1 <= the threshold of {threshold}" \
                  f" for all SpWs and all antennas."
        shortmsg = "Gain ratio"
        origin = pqa.QAOrigin(metric_name='score_polcal_gain_ratio',
                              metric_score=score,
                              metric_units='gain ratio')
        scores.append(pqa.QAScore(score, longmsg=longmsg, shortmsg=shortmsg, origin=origin))

    return scores


@log_qa
def score_polcal_gain_ratio_rms(session_name: str, gain_ratio_rms: Tuple[List, List], threshold: float = 0.02) \
        -> pqa.QAScore:
    """
    This QA heuristic receives gain ratio RMS corresponding to scan IDs, and
    scores outliers beyond the threshold.

    Args:
        session_name: name of session being evaluated.
        gain_ratio_rms: tuple containing a list of scan IDs and a list of
            corresponding gain ratio RMS.
        threshold: threshold used to determine whether the gain ratio RMS is
            high enough to return a lowered score.

    Returns:
        QAScore object
    """
    # Retrieve the gain ratio RMS and scan IDs.
    scanids, ratio_rms = gain_ratio_rms

    # Identify outlier gain ratio RMS
    ind_bad = np.where(np.asarray(ratio_rms) > threshold)[0]

    if len(ind_bad) > 0:
        score = 0.6
        longmsg = f"Session '{session_name}' has gain ratio RMS greater than {threshold} in scan(s)" \
                  f" {utils.commafy([str(s) for s in np.asarray(scanids)[ind_bad]], quotes=False)}."
        shortmsg = "High gain ratio RMS"
    else:
        score = 1.0
        longmsg = f"Session '{session_name}' has gain ratio RMS <= the threshold of {threshold} for all scans."
        shortmsg = "Gain ratio RMS"

    origin = pqa.QAOrigin(metric_name='score_polcal_gain_ratio_rms',
                          metric_score=score,
                          metric_units='gain ratio rms')

    return pqa.QAScore(score, longmsg=longmsg, shortmsg=shortmsg, origin=origin)


@log_qa
def score_polcal_leakage(session_name: str, ant_names: dict, leakage_result: PolcalWorkerResults, th_poor: float = 0.10,
                         th_bad: float = 0.15) -> List[pqa.QAScore]:
    """
    This heuristic inspects the polarization calibrator leakage (D-terms)
    solutions caltable and create a score based on how large the deviation from
    zero is.

    Args:
        session_name: name of session being evaluated.
        ant_names: dictionary mapping antenna IDs to names.
        leakage_result: PolcalWorker task result object containing the
            CalApplication for the leakage caltable to analyze.
        th_poor: threshold used to declare that leakage solutions are poor.
        th_bad: threshold used to declare that leakage solutions are bad.
    Returns:
        List of QAScore objects.
    """
    scores = []
    # Score each caltable in result.
    for calapp in leakage_result.final:
        # Retrieve data from leakage solutions caltable.
        with casa_tools.TableReader(calapp.gaintable) as table:
            # Retrieve unique SpWs and antennas.
            uniq_spws = sorted(set(table.getcol('SPECTRAL_WINDOW_ID')))
            uniq_ants = sorted(set(table.getcol('ANTENNA1')))

            # Score each SpW separately.
            for spwid in uniq_spws:
                # Retrieve D-terms for current SpW.
                data = table.query(f"SPECTRAL_WINDOW_ID=={spwid}", columns='CPARAM')
                dterms = data.getcol('CPARAM')

                # For each antenna, check if the D-terms solutions exceed the
                # threshold.
                bad_antids = []
                poor_antids = []
                for antid in uniq_ants:
                    pol1 = dterms[0, :, antid]
                    pol2 = dterms[1, :, antid]

                    rpol1 = abs(np.real(pol1)) > th_bad
                    ipol1 = abs(np.imag(pol1)) > th_bad
                    rpol2 = abs(np.real(pol2)) > th_bad
                    ipol2 = abs(np.imag(pol2)) > th_bad

                    if rpol1.any() or ipol1.any() or rpol2.any() or ipol2.any():
                        bad_antids.append(antid)
                        continue

                    rpol1 = abs(np.real(pol1)) > th_poor
                    ipol1 = abs(np.imag(pol1)) > th_poor
                    rpol2 = abs(np.real(pol2)) > th_poor
                    ipol2 = abs(np.imag(pol2)) > th_poor

                    if rpol1.any() or ipol1.any() or rpol2.any() or ipol2.any():
                        poor_antids.append(antid)

                if poor_antids:
                    score = 0.75
                    longmsg = f"Session '{session_name}' has D-terms solutions that deviate by {th_poor}-{th_bad} for" \
                              f" SpW {spwid}, antenna(s)" \
                              f" {utils.commafy([ant_names[i] for i in poor_antids], quotes=False)}."
                    shortmsg = "Large deviation D-terms solutions"
                    origin = pqa.QAOrigin(metric_name='score_polcal_leakage',
                                          metric_score=score,
                                          metric_units='D-terms solutions deviation')
                    scores.append(pqa.QAScore(score, longmsg=longmsg, shortmsg=shortmsg, origin=origin))

                if bad_antids:
                    score = 0.55
                    longmsg = f"Session '{session_name}' has D-terms solutions that deviate by more than {th_bad} for" \
                              f" SpW {spwid}, antenna(s)" \
                              f" {utils.commafy([ant_names[i] for i in bad_antids], quotes=False)}."
                    shortmsg = "Very large deviation D-terms solutions"
                    origin = pqa.QAOrigin(metric_name='score_polcal_leakage',
                                          metric_score=score,
                                          metric_units='D-terms solutions deviation')
                    scores.append(pqa.QAScore(score, longmsg=longmsg, shortmsg=shortmsg, origin=origin))

    # If no poor D-terms solutions are found, then create a good score.
    if not scores:
        score = 1.0
        longmsg = f"Session '{session_name}' has D-terms solutions <= the threshold of {th_poor} for all SpWs and" \
                  f" antennas."
        shortmsg = "D-terms solutions"
        origin = pqa.QAOrigin(metric_name='score_polcal_leakage',
                              metric_score=score,
                              metric_units='D-terms solutions deviation')
        scores.append(pqa.QAScore(score, longmsg=longmsg, shortmsg=shortmsg, origin=origin))

    return scores


@log_qa
def score_polcal_residual_pol(session_name: str, pfg_result: dict, threshold: float = 0.001) -> List[pqa.QAScore]:
    """
    This heuristic inspects the dictionary returned by CASA's polfromgain and
    scores the residual polarization in Q and U, compared to a threshold.

    Args:
        session_name: name of session being evaluated.
        pfg_result: dictionary of results produced by polfromgain
        threshold: threshold for residual polarization, above which the score
            should be lowered.
    Returns:
        List of QAScore objects.
    """
    scores = []

    # Create score for each field.
    for field_name, field_result in pfg_result.items():
        # Check residual polarization for each SpW.
        bad_spwids = []
        for spwid, spw_result in field_result.items():
            # Skip the result for "Average SpW".
            if 'Ave' in spwid:
                continue

            # The SpW result is a list of [I, Q, U, V], check whether Q or U
            # exceed threshold.
            if abs(spw_result[1]) > threshold or abs(spw_result[2]) > threshold:
                bad_spwids.append(spwid[3:])

        # Create a score depending on whether any SpW had too high residual
        # polarization.
        if bad_spwids:
            score = 0.5
            longmsg = f"Session '{session_name}' has residual polarization greater than {threshold} in SpW(s)" \
                      f" {utils.commafy(bad_spwids, quotes=False)}."
            shortmsg = "High residual polarization"
        else:
            score = 1.0
            longmsg = f"Session '{session_name}' residual polarization <= the threshold of {threshold}."
            shortmsg = "Residual polarization"
        origin = pqa.QAOrigin(metric_name='score_polcal_residual_pol',
                              metric_score=score,
                              metric_units='residual polarization')
        scores.append(pqa.QAScore(score, longmsg=longmsg, shortmsg=shortmsg, origin=origin))

    return scores


@log_qa
def score_polcal_results(session_name: str, caltables: list) -> pqa.QAScore:
    """
    This heuristic tests whether calibrations were derived for a polcal session.

    Args:
        session_name: name of session being evaluated.
        caltables: list of calibration tables derived for session.

    Returns:
        QAScore object
    """
    if not caltables:
        score = 0.0
        longmsg = f"No polarisation calibration derived for session '{session_name}'."
        shortmsg = "No polarisation calibration for session"
    else:
        score = 1.0
        longmsg = f"Polarisation calibration derived for session '{session_name}'."
        shortmsg = "Polarisation calibration derived for session"

    origin = pqa.QAOrigin(metric_name='score_polcal_results',
                          metric_score=score,
                          metric_units='polarisation caltables')

    return pqa.QAScore(score, longmsg=longmsg, shortmsg=shortmsg, origin=origin)


@log_qa
def score_fluxservice(result):
    """
    If the primary FS query fails and the backup is invoked,
    the severity level should be BLUE (below standard; numerically, on its own, 0.9).
    If the backup FS query also fails, the warning should be YELLOW (WARNING; numerically, on its own, 0.6).
    But it should keep running as it currently does.
    """

    if result.inputs['dbservice'] is False:
        score = 1.0
        msg = "Flux catalog service not used."
        for setjy_result in result.setjy_results:
            measurements = setjy_result.measurements
            for measurement in measurements.items():
                try:
                    fluxorigin = measurement[1][0].origin
                    if fluxorigin == 'Source.xml':
                        score = 0.3
                        msg = "Flux catalog service not used.  Source.xml is the origin."
                except Exception as e:
                    LOG.debug("Skip since there is not a flux measurement")

        origin = pqa.QAOrigin(metric_name='score_fluxservice',
                              metric_score=score,
                              metric_units='flux service')
        return pqa.QAScore(score, longmsg=msg, shortmsg=msg, origin=origin)
    elif result.inputs['dbservice'] is True:
        msg = ""
        if result.fluxservice == 'FIRSTURL':
            msg += "Flux catalog service used.  "
            score = 1.0
        elif result.fluxservice == 'BACKUPURL':
            msg += "Backup flux catalog service used.  "
            score = 0.9
        elif result.fluxservice == 'FAIL':
            msg += "Neither primary or backup flux service could be queried.  ASDM values used."
            score = 0.3

        agecounter = 0
        if result.fluxservice in ['FIRSTURL', 'BACKUPURL']:
            for setjy_result in result.setjy_results:
                measurements = setjy_result.measurements
                for measurement in measurements.items():
                    try:
                        fieldid = measurement[0]
                        mm = result.mses[0]
                        fieldobjs = mm.get_fields(field_id=fieldid)
                        intentlist = []
                        for fieldobj in fieldobjs:
                            intentlist.append(fieldobj.intents)

                        # PIPE-1124.  Only determine QA age scoring if 'AMPLITUDE' intent is present for a source.
                        if 'AMPLTIUDE' in intentlist:
                            age = measurement[1][0].age  # second element of a tuple, first element of list of flux objects
                            if int(abs(age)) > 14:
                                agecounter = agecounter + 1
                    except IndexError:
                        LOG.debug("Skip since there is no age present")

            # Any sources with age of nearest monitoring point greater than 14 days?
            if agecounter > 0:
                score = 0.5
                msg += "Age of nearest monitor point is greater than 14 days."

        origin = pqa.QAOrigin(metric_name='score_fluxservice',
                              metric_score=score,
                              metric_units='flux service')
        return pqa.QAScore(score, longmsg=msg, shortmsg=msg, origin=origin)


@log_qa
def score_fluxservicemessages(result):
    """
    Report any flux service messaging and status codes
    """
    if result.inputs['dbservice'] is False:
        score = 1.0
        longmsg = "Flux catalog service not used.  No warning messages reported."
        shortmsg = longmsg
    elif result.inputs['dbservice'] is True:
        score = 1.0
        longmsg = "No warning messages from the flux catalog service."
        shortmsg = longmsg
        if result.qastatus:
            longmsg = ""
            for qacode in result.qastatus:
                if qacode['clarification']:
                    score = 0.5
                    # Queries a per source, so there may be more than one message returned.
                    longmsg += "Source: {!s},  Status code: {!s},     Message: {!s}\n".format(qacode['source'],
                                                                                            qacode['status_code'],
                                                                                            qacode['clarification'])
                    shortmsg = "Flux service returned warning messages."

    origin = pqa.QAOrigin(metric_name='score_fluxservice_messaging',
                          metric_score=score,
                          metric_units='flux service messaging')
    return pqa.QAScore(score, longmsg=longmsg, shortmsg=shortmsg, origin=origin)


@log_qa
def score_fluxservicestatuscodes(result):
    """
    Report any flux service codes for status_code > 1
    """
    if result.inputs['dbservice'] is False:
        score = 1.0
        msg = "Flux catalog service not used.  No status codes to report."
    elif result.inputs['dbservice'] is True:
        score = 1.0
        msg = "Flux catalog service status queries returned code 0 or 1."
        if result.qastatus:
            scores = []
            for qacode in result.qastatus:
                # Status code can be 0,1,2,3
                if int(qacode['status_code']) > 1:
                    score = 0.3
                    msg = "A query of the flux catalog service returned a status code: {!s}".format(str(qacode['status_code']))

    origin = pqa.QAOrigin(metric_name='score_fluxservice_statuscode',
                          metric_score=score,
                          metric_units='flux service status code')
    return pqa.QAScore(score, longmsg=msg, shortmsg=msg, origin=origin)


@log_qa
def score_fluxcsv(result):
    """
    Check for existence of flux.csv file on import
    """

    if os.path.exists('flux.csv'):
        score = 1.0
        msg = 'flux.csv exists on disk'
    else:
        score = 0.3
        msg = "flux.csv does not exist"

    origin = pqa.QAOrigin(metric_name='score_fluxcsv',
                          metric_score=score,
                          metric_units='flux csv')
    return pqa.QAScore(score, longmsg=msg, shortmsg=msg, origin=origin)


@log_qa
def score_mom8_fc_image(mom8_fc_name, mom8_fc_peak_snr, mom8_10_fc_histogram_asymmetry, mom8_fc_max_segment_beams, mom8_fc_frac_max_segment):
    """
    Check the MOM8 FC image for outliers above a given SNR threshold. The score
    can vary between 0.33 and 1.0 depending on the fraction of outlier pixels.
    """

    mom8_fc_outlier_threshold1 = 5.0
    mom8_fc_outlier_threshold2 = 3.5
    mom8_fc_histogram_asymmetry_threshold1 = 0.20
    mom8_fc_histogram_asymmetry_threshold2 = 0.05
    mom8_fc_max_segment_beams_threshold = 1.0
    mom8_fc_score_min = 0.33
    mom8_fc_score_max = 1.00
    mom8_fc_metric_scale = 100.0
    if mom8_fc_frac_max_segment != 0.0:
        mom8_fc_score = mom8_fc_score_min + 0.5 * (mom8_fc_score_max - mom8_fc_score_min) * (1.0 + erf(-np.log10(mom8_fc_metric_scale * mom8_fc_frac_max_segment)))
    else:
        mom8_fc_score = mom8_fc_score_max

    with casa_tools.ImageReader(mom8_fc_name) as image:
        info = image.miscinfo()
        field = info.get('field')
        spw = info.get('virtspw')

    if (mom8_fc_peak_snr > mom8_fc_outlier_threshold1 and mom8_10_fc_histogram_asymmetry > mom8_fc_histogram_asymmetry_threshold1) or \
       (mom8_fc_peak_snr > mom8_fc_outlier_threshold2 and mom8_10_fc_histogram_asymmetry > mom8_fc_histogram_asymmetry_threshold2 and mom8_fc_max_segment_beams > mom8_fc_max_segment_beams_threshold):
        mom8_fc_final_score = min(mom8_fc_score, 0.65)
    else:
        mom8_fc_final_score = max(mom8_fc_score, 0.67)

    if 0.33 <= mom8_fc_final_score < 0.66:
        longmsg = 'MOM8 FC image for field {:s} virtspw {:s} with a peak SNR of {:#.5g} and a flux histogram asymmetry which indicate that there may be residual line emission in the findcont channels.'.format(field, spw, mom8_fc_peak_snr)
        shortmsg = 'MOM8 FC image indicates residual line emission'
        weblog_location = pqa.WebLogLocation.UNSET
    else:
        longmsg = 'MOM8 FC image for field {:s} virtspw {:s} has a peak SNR of {:#.5g} and no significant flux histogram asymmetry.'.format(field, spw, mom8_fc_peak_snr)
        shortmsg = 'MOM8 FC peak SNR and flux histogram'
        weblog_location = pqa.WebLogLocation.ACCORDION

    origin = pqa.QAOrigin(metric_name='score_mom8_fc_image',
                          metric_score=(mom8_fc_peak_snr, mom8_10_fc_histogram_asymmetry, mom8_fc_max_segment_beams, mom8_fc_frac_max_segment),
                          metric_units='Peak SNR / Histogram asymmetry, Max. segment size in beams, Max. segment fraction')

    return pqa.QAScore(mom8_fc_final_score, longmsg=longmsg, shortmsg=shortmsg, origin=origin, weblog_location=weblog_location)


@log_qa
def score_rasterscan_correctness_direction_domain_rasterscan_fail(result: SDImportDataResults) -> List[pqa.QAScore]:
    """Calculate QAScore of direction-domain raster scan heuristics analysis failure in importdata.

    Args:
        result (SDImportDataResults): instance of SDImportDataResults

    Returns:
        List[pqa.QAScore]: list of QAScores
    """
    msg = 'Direction-domain raster scan analysis failed, fallback to time-domain analysis'
    return _score_rasterscan_correctness(result.rasterscan_heuristics_results_direction, msg)


@log_qa
def score_rasterscan_correctness_time_domain_rasterscan_fail(result: SDImportDataResults) -> List[pqa.QAScore]:
    """Calculate QAScore of time-domain raster scan heuristics analysis failure in importdata.

    Args:
        result (SDImportDataResults): instance of SDImportDataResults

    Returns:
        List[pqa.QAScore]: list of QAScores
    """
    msg = 'Time-domain raster scan analysis issue detected. Failed to identify gap between raster map iteration'
    return _score_rasterscan_correctness(result.rasterscan_heuristics_results_time, msg)


@log_qa
def score_rasterscan_correctness_imaging_raster_gap(result: SDImagingResultItem) -> List[pqa.QAScore]:
    """Calculate QAScore of gap existence in raster pattern of imaging.

    Args:
        result (SDImagingResultItem): instance of SDImagingResultItem

    Returns:
        List[pqa.QAScore]: list of QAScores
    """
    msg = 'Unable to identify gap between raster map iteration'
    return _score_rasterscan_correctness(result.rasterscan_heuristics_results_rgap, msg)


@log_qa
def score_rasterscan_correctness_imaging_raster_analysis_incomplete(result: SDImagingResultItem) -> List[pqa.QAScore]:
    """Calculate QAScore when raster scan analysis was incomplete in imaging.

    Args:
        result (SDImagingResultItem): instance of SDImagingResultItem

    Returns:
        List[pqa.QAScore]: list of QAScores
    """
    msg = 'Raster scan analysis incomplete. Skipping calculation of theoretical image RMS'
    return _score_rasterscan_correctness(result.rasterscan_heuristics_results_incomp, msg)


def _score_rasterscan_correctness(rasterscan_heuristics_results: Dict[str, RasterScanHeuristicsResult], msg: str) -> List[pqa.QAScore]:
    """Generate score of raster scan correctness of importdata or imaging.

    Args:
        rasterscan_heuristics_results (Dict[str, RasterScanHeuristicsResult]): Dictionary of raster heuristics result objects
            treats QAScore of raster scan analysis.
        msg (str): short message for QA

    Returns:
        List[pqa.QAScore]: lists contains QAScore objects.
    """

    qa_scores = []  # [pqa.QAScore]

    # converting rasterscan_heuristics_results to QA score
    for _execblock_id, _rasterscan_heuristics_results_list in rasterscan_heuristics_results.items():
        for _results_list in _rasterscan_heuristics_results_list:
            _failed_ants = np.unique(_results_list.get_antennas_rasterscan_failed())
            if len(_failed_ants) > 0:
                qa_scores.append(_rasterscan_failed_per_eb(_execblock_id, _failed_ants, msg))

    return qa_scores


def _rasterscan_failed_per_eb(execblock_id:str, failed_ants: list[str], msg: str) -> 'pqa.QAScore':
    """Return an object which has FAILED information in raster scan analysis.

    Args:
        execblock_id (str): Execute Block ID
        failed_ants (list[str]): List of antenna names
        msg: short message for QA

    Returns:
        pqa.QAScore: QA score object
    """
    SCORE_FAIL = 0.8
    longmsg = msg + f' : EB:{execblock_id}:{",".join(failed_ants)}'
    origin = pqa.QAOrigin(metric_name='score_rasterscan_correctness',
                        metric_score=SCORE_FAIL,
                        metric_units='raster scan correctness')
    return pqa.QAScore(SCORE_FAIL, longmsg=longmsg, shortmsg=msg, origin=origin)


@log_qa
def score_tsysflagcontamination_contamination_flagged(vis, summaries) -> pqa.QAScore:
    """
    Calculate a score for the hifa_tsysflagcontamination task based on whether
    any line contamination was flagged.

    0% flagged  -> 1
    >0% flagged -> 0.9
    """
    frac_flagged = calc_frac_newly_flagged(summaries)

    if frac_flagged:
        score = 0.9
        longmsg = f'Tsys line contamination detected in {vis}.'
        shortmsg = 'Tsys contamination flagged'
    else:
        score = 1.0
        longmsg = f'No Tsys line contamination detected in {vis}.'
        shortmsg = 'No Tsys line contamination'

    origin = pqa.QAOrigin(metric_name='score_tsys_line_contamination_flagged',
                          metric_score=frac_flagged,
                          metric_units='Fraction of Tsys data that was identified as line contamination and flagged')
    return pqa.QAScore(
        score,
        longmsg=longmsg,
        shortmsg=shortmsg,
        vis=os.path.basename(vis),
        origin=origin,
        applies_to=pqa.TargetDataSelection(vis={vis})
    )


@log_qa
def score_tsysflagcontamination_external_heuristic(foreign_qascores: List[pqa.QAScore]) -> List[pqa.QAScore]:
    """
    Adopt QA scores that originate from the Tsysflag line contamination
    heuristic.

    The QA scores are generated in an external heuristic. This metric
    is here mainly to log the QA scores to console for non-weblog runs
    via the @log_qa decorator.
    """
    origin = pqa.QAOrigin(
        metric_name="score_tsysflagcontamination_external_heuristic",
        metric_score="N/A",
        metric_units="",
    )
    qascores = []
    for fq in foreign_qascores:
        adopted_qascore = pqa.QAScore(
            score=fq.score,
            shortmsg=fq.shortmsg,
            longmsg=fq.longmsg,
            applies_to=fq.applies_to,
            origin=origin
        )
        qascores.append(adopted_qascore)
    return qascores


@log_qa
def score_iersstate(mses: List[domain.MeasurementSet]) -> List[pqa.QAScore]:
    """
    Check state of IERS tables relative to observation date
    """

    iers_info = utils.IERSInfo()
    # reorder MSes by start time
    by_start = sorted(mses,
                      key=lambda m: utils.get_epoch_as_datetime(m.start_time))

    # create an interval for each one, including our tolerance
    scores = []
    for ms in by_start:
        longmsg = (f"Dates for {ms.basename} fully covered by IERSeop2000.")
        shortmsg = "MS dates fully covered by IERSeop2000."
        time_end = utils.get_epoch_as_datetime(ms.end_time)

        if iers_info.validate_date(time_end):
            score = 1.0
        elif iers_info.date_message_type(time_end) == "INFO":
            score = 0.9
            longmsg = (f"Dates for {ms.basename} not fully covered by IERSeop2000. CASA will use IERSpredict.")
            shortmsg = "MS dates not fully covered by IERSeop2000. CASA will use IERSpredict."
        elif iers_info.date_message_type(time_end) == "WARN":
            score = 0.5
            longmsg = (f"Dates for {ms.basename} not fully covered by IERSeop2000. CASA will use IERSpredict.")
            shortmsg = "MS dates not fully covered by IERSeop2000. CASA will use IERSpredict."
        else:
            score = 0.3
            longmsg = (f"Dates for {ms.basename} not fully covered by IERSeop2000. Please update your data repository.")
            shortmsg = "MS dates not fully covered by IERSpredict. Please update your data repository."

        origin = pqa.QAOrigin(metric_name='score_iersstate',
                              metric_score=score,
                              metric_units='state of IERS tables relative to observation date')

        scores.append(pqa.QAScore(score, longmsg=longmsg, shortmsg=shortmsg, vis=ms.basename, origin=origin))

    return scores
