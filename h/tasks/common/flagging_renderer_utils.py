"""
Created on 25 Nov 2014

@author: sjw
"""
import collections
import functools
from typing import Optional, List, Sequence, Dict

import pipeline.infrastructure.logging as logging
import pipeline.infrastructure.utils as utils
from pipeline.infrastructure.launcher import Context
from pipeline.domain.measurementset import MeasurementSet

LOG = logging.get_logger(__name__)

FlagTotal = collections.namedtuple('FlagSummary', 'flagged total')


def flags_for_result(result,
                     context: Context,
                     intents_to_summarise: Optional[List[str]] = None,
                     non_science_agents: Optional[List[str]] = None):
    if intents_to_summarise is None:
        intents_to_summarise = ['BANDPASS', 'PHASE', 'AMPLITUDE', 'TARGET']

    if non_science_agents is None:
        non_science_agents = []

    ms = context.observing_run.get_ms(result.inputs['vis'])
    summaries = result.summaries

    by_intent = flags_by_intent(ms, summaries, intents_to_summarise)
    by_spw = flags_by_science_spws(ms, summaries)
    merged = utils.dict_merge(by_intent, by_spw)

    adjusted = adjust_non_science_totals(merged, non_science_agents)

    return {ms.basename: adjusted}


def flags_by_intent(ms: MeasurementSet,
                    summaries: List[dict],
                    intents: Sequence[str] = None) -> Dict[str, Dict[str, FlagTotal]]:
    """
    Arguments:
        ms: an instance of MeasurementSet.
        summaries: a list of summaries created by flagging tasks.
        intents: list of intents for which the total flagging percentage should be reported.
    Returns:
        a dictionary indexed by summary name (e.g. 'before', '...'),
        where each element is itself a dictionary containing FlagTotal tuples for each intent.
    """
    # create a dictionary of scans per observing intent, eg. 'PHASE':['1','2','7']
    intent_scans = {
        # convert IDs to strings as they're used as summary dictionary keys
        intent: [str(scan.id) for scan in ms.scans if intent in scan.intents]
        for intent in intents
    }

    # while we're looping, get the total flagged by looking in all scans
    intent_scans['TOTAL'] = [str(s.id) for s in ms.scans]

    total = collections.defaultdict(dict)

    previous_summary = None
    for summary in summaries:

        for intent, scan_ids in intent_scans.items():
            flagcount = 0
            totalcount = 0

            for i in scan_ids:
                # workaround for KeyError exception when summary
                # dictionary doesn't contain the scan
                if i not in summary['scan']:
                    continue

                flagcount += int(summary['scan'][i]['flagged'])
                totalcount += int(summary['scan'][i]['total'])

                if previous_summary:
                    flagcount -= int(previous_summary['scan'][i]['flagged'])

            ft = FlagTotal(flagcount, totalcount)
            total[summary['name']][intent] = ft

        previous_summary = summary

    return total


def flags_by_science_spws(ms: MeasurementSet, summaries: List[dict]) -> Dict[str, Dict[str, FlagTotal]]:
    """
    Returns:
        a dictionary indexed by summary name, where each element is a dict
        with a single key 'SCIENCE_SPWS' and a FlagTotal value
    """
    science_spws = ms.get_spectral_windows(science_windows_only=True)

    total = collections.defaultdict(dict)

    previous_summary = None
    for summary in summaries:

        flagcount = 0
        totalcount = 0

        for spw in science_spws:
            spw_id = str(spw.id)
            # workaround for KeyError exception when summary
            # dictionary doesn't contain the spw
            if spw_id not in summary['spw']:
                continue
            flagcount += int(summary['spw'][spw_id]['flagged'])
            totalcount += int(summary['spw'][spw_id]['total'])

            if previous_summary:
                flagcount -= int(previous_summary['spw'][spw_id]['flagged'])

        ft = FlagTotal(flagcount, totalcount)
        total[summary['name']]['SCIENCE SPWS'] = ft

        previous_summary = summary

    return total


def adjust_non_science_totals(flagtotals, non_science_agents=None):
    """
    Return a copy of the FlagSummary dictionaries, with totals reduced to
    account for flagging performed by non-science flagging agents.

    The incoming flagtotals report how much data was flagged per agent per
    data selection. These flagtotals are divided into two groups: those whose 
    agent should be considered 'non-science' (and are indicated as such in the 
    non_science_agents argument) and the remainder. The total number of rows
    flagged due to non-science agents is calculated and subtracted from the 
    total for each of the remainder agents.     
    """
    if not non_science_agents:
        return flagtotals

    agents_to_copy = set(non_science_agents)
    agents_to_adjust = set(flagtotals.keys()) - agents_to_copy
    data_selections = set()
    for result in flagtotals.values():
        data_selections.update(set(result.keys()))

    # copy agents that use the total number of visibilities across to new 
    # results
    adjusted_results = dict((agent, flagtotals[agent])
                            for agent in agents_to_copy
                            if agent in flagtotals)

    # tot up how much data was flagged by each agent per data selection
    flagged_non_science = {}
    for data_selection in data_selections:
        flagged_non_science[data_selection] = sum([v[data_selection].flagged
                                                   for v in adjusted_results.values()])

    # subtract this 'number of rows flagged per data selection' from the total
    # for the remaining agents
    for agent in agents_to_adjust:
        for data_selection in flagtotals[agent]:
            unadjusted = flagtotals[agent][data_selection]
            adjusted = FlagTotal(unadjusted.flagged,
                                 unadjusted.total - flagged_non_science[data_selection])
            if agent not in adjusted_results:
                adjusted_results[agent] = {}
            adjusted_results[agent][data_selection] = adjusted

    return adjusted_results


def intents_to_summarise(context: Context, all_flag_summary_intents: Optional[Sequence[str]] = None) -> List[str]:
    """
    Find out which intents to list in the flagging table.
    Arguments:
        context: Pipeline context.
        all_flag_summary_intents: a list of intents to summarise;
        if None, the default list of all relevant intents (calibration and target) is used.
    Returns:
        the subset of intents from the input list all_flag_summary_intents that are actually present in at least one MS
        (a list of strings in the same order as input, but omitting missing ones).
    """

    # First get all intents across all MSes in context
    context_intents = functools.reduce(lambda x, m: x.union(m.intents),
                                       context.observing_run.measurement_sets,
                                       set())
    # then match intents against those we want in the table, removing those not present.
    # List order is preserved in the table.
    if all_flag_summary_intents is None:
        all_flag_summary_intents = [
            'AMPLITUDE', 'BANDPASS', 'CHECK', 'DIFFGAINREF', 'DIFFGAINSRC', 'PHASE', 'POLANGLE', 'POLARIZATION',
            'POLLEAKAGE', 'TARGET']
    intents_to_summarise = [i for i in all_flag_summary_intents
                            if i in context_intents.intersection(set(all_flag_summary_intents))]
    return intents_to_summarise
