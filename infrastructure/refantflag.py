import collections
import numpy
from typing import Any, DefaultDict, Dict, Iterable, List, NamedTuple, Optional, Set, Tuple, Union

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.utils as utils
from pipeline.domain import MeasurementSet
from pipeline.h.tasks.common.arrayflaggerbase import FlagCmd
from pipeline.h.tasks.common.flaggableviewresults import FlaggableViewResults
from pipeline.infrastructure.basetask import Results

LOG = infrastructure.get_logger(__name__)

# Tuple of field(string) and intent(string) used as a dict key
FieldIntent = NamedTuple(
    'FieldIntent', (('field', str), ('intent', str))
)

# Tuple of field(string), intent(string) and antenna name(string) used as a dict key
FieldIntentAntenna = NamedTuple(
    'FieldIntentAntenna', (('field', str), ('intent', str), ('antenna', str))
)

# Tuple of field(string), intent(string) and spw id(int) used as a key
# in the dict of fully flagged antennas in several flagging tasks.
FieldIntentSpw = NamedTuple(
    'FieldIntentSpw', (('field', str), ('intent', str), ('spw', int))
)

# Element in the list of notifications about fully flagged antennas:
# for each field(string), there may be one or more intents(tuple of strings),
# one or more spws(tuple of ints or a single string "all spws"),
# and one or more antennas (tuple of strings).
FullyFlaggedAntennasNotification = NamedTuple(
    'FullyFlaggedAntennasNotification',
    (('field', str), ('intents', Tuple[str]), ('spws', Union[str, Tuple[int]]), ('antennas', Union[str, Tuple[str]]))
)


def identify_fully_flagged_antennas_from_flagcmds(ms: MeasurementSet, flags: List[FlagCmd]) \
        -> Dict[str, Set[FieldIntentSpw]]:
    """
    Identify the set of antennas that were fully flagged in at least one spw
    considered in the given flagging task, for any of the fields or intents,
    by analyzing the flagging commands.
    Args:
        ms: measurement set.
        flags: list of flagging commands.

    Returns:
        a dict of antennas that are fully flagged in one or more spws:
        each element (antenna name) is a set containing all combinations of
        field(str), intent(str) and spw(int) in which this antenna is fully flagged.
    """

    # Create antenna ID to name translation dictionary.
    antenna_id_to_name: Dict[int, str] = {ant.id: (ant.name if ant.name.strip() else str(ant.id))
                                          for ant in ms.antennas}

    fully_flagged_antennas: Dict[str, Set[FieldIntentSpw]] = collections.defaultdict(set)

    # Create a summary of the flagging state by going through each flagging command.
    for flag in flags:
        # Only consider flagging commands with a specified antenna and
        # without a specified timestamp.
        if flag.antenna is not None and flag.time is None:
            # Skip flagging commands for baselines.
            if '&' in str(flag.antenna):
                continue
            # Convert CASA intent from flagging command to pipeline intent;
            # field is already a string, and spw is an int.
            key = FieldIntentSpw(flag.field,
                                 utils.to_pipeline_intent(ms, flag.intent) if flag.intent else None,
                                 flag.spw)
            ant_name = antenna_id_to_name[flag.antenna]
            fully_flagged_antennas[ant_name].add(key)

    return fully_flagged_antennas


def identify_fully_flagged_antennas_from_flagview(
        ms: MeasurementSet,
        results: FlaggableViewResults,
        scan_to_field_names: Dict[Any, Union[str, Tuple[str]]],
        intents_of_interest: Optional[Iterable[str]] = None) \
        -> Dict[str, Set[FieldIntentSpw]]:
    """
    Identify the set of antennas that were fully flagged in at least one spw
    considered in the given flagging task, for any of the fields or intents,
    by analyzing the flagging view.
    Args:
        ms: measurement set.
        results: a FlaggableViewResults object containing one or more flagging views,
            which are two-dimensional arrays with 0th axis enumerating the scans
            and 1st axis enumerating the antennas.
        scan_to_field_names: a dictionary providing the translation between scans and fields.
            the scans (0th axis) may be enumerated not only by scan index, but by any other
            parameter that uniquely determines the scan (such as the timestamp), which serves
            as the dict key, while its value contains the field(s) corresponding to each scan.
        intents_of_interest: if provided, lists the intents to be considered in this analysis.

    Returns:
        a dict of antennas that are fully flagged in one or more spws:
        each element (antenna name) is a set containing all combinations of
        field(str), intent(str) and spw(int) in which this antenna is fully flagged.
    """
    # Perform test separately for each of these intents.

    # Create translation of field -> intent for intents of interest.
    intents_for_field: Dict[str, Set[str]] = {field.name: field.intents for field in ms.fields}

    # Create antenna ID to name translation dictionary.
    antenna_id_to_name: Dict[int, str] = {ant.id: (ant.name if ant.name.strip() else str(ant.id))
                                          for ant in ms.antennas}

    # Create an empty dictionary for aggregating information of fully flagged antenna.
    fully_flagged_antennas: Dict[str, Set[FieldIntentSpw]] = collections.defaultdict(set)

    # Going through each view product for the specified metric, search for fully flagged antenna
    # for each combination of field, intent, and spw, and collect their information for the return.
    for description in results.descriptions():

        # Get final view.
        view = results.last(description)

        # Initialize the flagging state dict:
        # keys are combinations of field, intent, and spw, and values are
        # 2d arrays (assembled as lists of arrays), with rows corresponding
        # to scans, columns to antennas, and values indicating whether
        # the antenna is fully flagged in the given scan.
        flagging_state: Dict[FieldIntentSpw, list] = collections.defaultdict(list)

        # Go through each scan within view.
        for scan_index, scan_id in enumerate(view.axes[1].data):

            # Get flags per antenna for current scan
            flag_per_scan = view.flag[:, scan_index]

            # Obtain the list of field names corresponding to the given scan
            # (either a single value or a list)
            field_names = scan_to_field_names[scan_id]
            if isinstance(field_names, str):
                field_names = (field_names,)

            for field_name in field_names:
                # Get field ID and intent(s) for current scan
                intents_scan = intents_for_field[field_name]
                # For each intent that this scan belongs to:
                for intent in intents_scan:
                    # If this scan is for an intent of interest, then append
                    # this scan (i.e. flags for all antennas) to the list.
                    if intents_of_interest is None or intent in intents_of_interest:
                        flagging_state[FieldIntentSpw(field_name, intent, view.spw)].append(flag_per_scan)

        # Consider the 2d array with rows corresponding to scans and columns to antennas,
        # and collect the information about antennas that are fully flagged in all rows.
        for key, value in flagging_state.items():
            for idx_ant in numpy.where(numpy.all(value, axis=0))[0]:
                # PIPE-2147: mapping view column indices (corresponding to antennas) to antenna ids using
                # the flagging view x-axis data.
                antenna_id = view.axes[0].data[idx_ant]
                fully_flagged_antennas[antenna_id_to_name[antenna_id]].add(key)

    return fully_flagged_antennas


def mark_antennas_for_refant_update(
        ms: MeasurementSet,
        result: Results,
        fully_flagged_antennas: Dict[str, Set[FieldIntentSpw]],
        all_spwids: Set[int]):
    """
    Modify result to set antennas to be demoted/removed if/when result
    gets accepted into the pipeline context.
    Antennas are demoted if they are fully flagged in at least one spw
    in any field, and removed if they are flagged in all spws.
    If list of antennas to demote and/or remove comprises all antennas,
    then skip demotion/removal and raise a warning.

    Args:
        ms: measurement set.
        result: a subclass of basetask.Results, such as TsysflagResults or
            BandpassflagResults, containing the sets of ants_to_demote,
            ants_to_remove, and a list of fully_flagged_antenna_notifications.
        all_spwids: set of all spws considered in the given task.
        fully_flagged_antennas: dict containing the sets of fully flagged
            combinations of field/intent/spw for each antenna.
    Returns:
        result object with updated list of refants.
    """

    if not fully_flagged_antennas:  # nothing to do
        return result

    if not(hasattr(ms, 'reference_antenna')) or not isinstance(ms.reference_antenna, str):
        LOG.warning(
            '{0} - no reference antennas found in MS, cannot update '
            'the reference antenna list.'.format(ms.basename))
        return result

    # Assemble the dict of antennas that are fully flagged in all spws:
    # each element (antenna name) is a set containing all intents in which this antenna
    # is fully flagged in all spws.
    fully_flagged_antennas_in_all_spws = collections.defaultdict(set)

    # Iterate over antennas...
    for antenna, field_intent_spw_combinations in fully_flagged_antennas.items():
        # For each combination of field+intent, assemble the set of spws
        # in which the current antenna is fully flagged.
        spws_for_field_intent = collections.defaultdict(set)
        for item in field_intent_spw_combinations:
            spws_for_field_intent[(item.field, item.intent)].add(item.spw)

        # If any of these sets of spws comprise all spws for any field+intent combination,
        # add the current antenna and the corresponding intent to the dict of antennas
        # that are fully flagged in all spws.
        for (field, intent), spws in spws_for_field_intent.items():
            if spws == all_spwids:
                fully_flagged_antennas_in_all_spws[antenna].add(intent)

    # Obtain the current list of reference antennas from the MS.
    refant = ms.reference_antenna.split(',')

    # Sets of candidate antennas for demotion and removal from the refant list.
    ants_to_demote = set(fully_flagged_antennas.keys())
    ants_to_remove = set(fully_flagged_antennas_in_all_spws.keys())

    # Identify intersection between refants and fully flagged antennas and store in result.
    result.refants_to_remove = set()
    intents_for_refants_to_remove = set()
    for ant in refant:
        if ant in ants_to_remove:
            result.refants_to_remove.add(ant)
            intents_for_refants_to_remove.update(set(fully_flagged_antennas_in_all_spws[ant]))

    # Check if removal of refants would result in an empty refant list,
    # in which case the refant update is skipped and a warning is emitted.
    if result.refants_to_remove == set(refant):
        ant_list = utils.commafy(result.refants_to_remove, quotes=False)
        intent_list = utils.commafy(intents_for_refants_to_remove, quotes=False, multi_prefix='s')
        many_ants = len(result.refants_to_remove) > 1
        LOG.warning(
            '{0} - the following reference antenna{1} fully flagged '
            'in all spws for one or more fields with intent{2}, '
            'but {3} *NOT* removed from the refant list because doing so '
            'would result in an empty refant list: {4}'.
            format(ms.basename, 's are' if many_ants else ' is',
                   intent_list, 'are' if many_ants else 'is', ant_list))

        # Reset the refant removal list in the result to be empty.
        result.refants_to_remove = set()

    # Identify intersection between refants and candidate antennas to demote,
    # skipping those that are to be removed entirely, and store this list in the result.
    # These antennas should be moved to the end of the refant list (demoted)
    # upon merging the result into the context.
    result.refants_to_demote = set()
    intents_for_refants_to_demote = set()
    for ant in refant:
        if ant in ants_to_demote and ant not in result.refants_to_remove:
            result.refants_to_demote.add(ant)
            intents_for_refants_to_demote.update(
                item.intent for item in fully_flagged_antennas[ant])

    # Check if the list of refants-to-demote comprises all refants,
    # in which case the re-ordering of refants is skipped and a warning is emitted.
    if result.refants_to_demote == set(refant):
        ant_list = utils.commafy(result.refants_to_demote, quotes=False)
        intent_list = utils.commafy(intents_for_refants_to_demote, quotes=False, multi_prefix='s')
        many_ants = len(result.refants_to_demote) > 1
        LOG.warning(
            '{0} - the following antenna{1} fully flagged '
            'for one or more spws in one or more fields with intent{2}, '
            'but since {3}, the refant list is *NOT* reordered: {4}'.
            format(ms.basename, 's are' if many_ants else ' is', intent_list,
                   'these comprise all refants' if many_ants else 'it is the only refant', ant_list))

        # Reset the refant demotion list in the result to be empty.
        result.refants_to_demote = set()

    return result


def aggregate_fully_flagged_antenna_notifications(
        fully_flagged_antennas: Dict[str, Set[FieldIntentSpw]],
        all_spwids: Set[int]) \
        -> List[FullyFlaggedAntennasNotification]:
    """
    Aggregate the list of notifications about fully flagged antennas for the subsequent QA scoring.
    Args:
        fully_flagged_antennas:  dict with tuples(field, intent, spwid) as keys and sets of antenna ids as values.
        all_spwids:              set of all spw ids considered in this flagging task, used to replace
                                 the tuple of flagged spws by a string "all spws" when this is the case.
    Returns:
        list of notifications for QA, each item is a named tuple with the following items:
            field:    the name of the field;
            intents:  a tuple of intents;
            spws:     a tuple of spws, or 'all spws' if all relevant spws are flagged;
            antennas: a tuple of antenna names.
    """

    # Step 1: Swap the aggregation order of antennas and spws.
    # Input dict:        sets of one or more antennas for each combination of field, intent and spw.
    # Intermediate dict: sets of one or more spws for each combination of field, intent and antenna.
    # The resulting list is aggregated by spws only.
    spws_for_field_intent_antenna: \
        DefaultDict[FieldIntentAntenna, Set[int]] = \
        collections.defaultdict(set)
    for antenna, field_intent_spw_combinations in fully_flagged_antennas.items():
        for item in field_intent_spw_combinations:
            spws_for_field_intent_antenna[FieldIntentAntenna(item.field, item.intent, antenna)].add(item.spw)

    # Step 2: Collect all groups of spws assembled at the previous stage,
    # and for each unique group of one or more spws, create a dictionary indexed by
    # the field+intent pair and containing sets of all affected antennas.
    # The result is aggregated by _both_ spws and antennas, in that order.
    antennas_for_spws_and_field_intent_pairs: \
        DefaultDict[Union[str, Set[int]], Dict[FieldIntent, Set[str]]] = \
        collections.defaultdict(dict)
    for item, set_of_spws in spws_for_field_intent_antenna.items():
        # convert set of spws to tuple or string, to be used as a dict key
        spws = 'all spws' if set_of_spws == all_spwids else tuple(sorted(set_of_spws))
        field_intent_pair = FieldIntent(item.field, item.intent)
        if field_intent_pair not in antennas_for_spws_and_field_intent_pairs[spws]:
            antennas_for_spws_and_field_intent_pairs[spws][field_intent_pair] = {item.antenna}
        else:
            antennas_for_spws_and_field_intent_pairs[spws][field_intent_pair].add(item.antenna)

    # Step 3: Aggregate by intents: for each unique group of spws, loop over
    # all field+intent pairs and examine the corresponding sets of antennas.
    result = []
    for spws, antennas_for_field_intent_pair in antennas_for_spws_and_field_intent_pairs.items():
        intents_for_antennas_and_field: \
            Dict[Tuple[str], Dict[str, Set[str]]] = \
            collections.defaultdict(dict)

        # For each unique set of antennas, create a dictionary
        # indexed by the field name and containing all affected intents.
        for item, antenna_set in antennas_for_field_intent_pair.items():
            antennas = tuple(sorted(antenna_set))
            if item.field not in intents_for_antennas_and_field[antennas]:
                intents_for_antennas_and_field[antennas][item.field] = {item.intent}
            else:
                intents_for_antennas_and_field[antennas][item.field].add(item.intent)

        # Loop over the sets of antennas and create one notification for the given group of spws
        # sharing the same field, one or more intents, and one or more antennas.
        for antennas, intents_for_field in intents_for_antennas_and_field.items():
            for field, intents in intents_for_field.items():
                result.append(FullyFlaggedAntennasNotification(field, tuple(sorted(intents)), spws, antennas))

    return result


def format_fully_flagged_antenna_notification(vis: str, notification: FullyFlaggedAntennasNotification) -> str:
    """
    Format the notification message for fully flagged antennas.
    Args:
        vis:  the name of the ms.
        notification:  the tuple containing the information about fully flagged antennas.
    Returns:
        the text for the notification message.
    """
    # build the specification of intent, field and spw, omitting missing items
    spec_str = ''
    if len(notification.intents) >= 1 and notification.intents[0] is not None:
        spec_str += 'intent{}, '.format(utils.commafy(notification.intents, quotes=False, multi_prefix='s'))
    if notification.field:
        spec_str += 'field {}, '.format(notification.field)
    if notification.spws == 'all spws':
        spec_str += 'all spws, '
    else:
        spec_str += 'spw{}, '.format(utils.commafy(notification.spws, quotes=False, multi_prefix='s'))
    return '{}: For {} the following antenna{} fully flagged: {}'.format(
        vis, spec_str,
        's are' if len(notification.antennas) > 1 else ' is',
        utils.commafy(notification.antennas, quotes=False)
    )
