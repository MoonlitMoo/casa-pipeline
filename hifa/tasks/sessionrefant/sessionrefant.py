import collections
import contextlib
import operator
import os
from functools import reduce

import numpy as np

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.basetask as basetask
import pipeline.infrastructure.vdp as vdp
from pipeline.hif.tasks import gaincal
from pipeline.infrastructure import casa_tools
from pipeline.infrastructure import sessionutils
from pipeline.infrastructure import task_registry
from .resultobjects import SessionRefAntResults

LOG = infrastructure.get_logger(__name__)

__all__ = [
    'SessionRefAntInputs',
    'SessionRefAntResults',
    'SessionRefAnt'
]


class SessionRefAntInputs(vdp.StandardInputs):
    """
    SessionRefAntInputs defines the inputs for the SessionRefAnt pipeline task.
    """
    # Threshold for detecting "non-zero" phase outliers that imply that
    # during a CASA gaincal the specified reference antenna was overridden.
    phase_threshold = vdp.VisDependentProperty(default=0.005)

    def __init__(self, context, output_dir=None, vis=None, phase_threshold=None):
        self.context = context
        self.output_dir = output_dir
        self.vis = vis

        # Task specific input parameters.
        self.phase_threshold = phase_threshold


@task_registry.set_equivalent_casa_task('hifa_session_refant')
@task_registry.set_casa_commands_comment(
    'Reference antenna lists from all measurement sets within current session\n'
    'are evaluated and combined into a single common ranked reference antenna\n'
    'list for the session, that is to be used in any subsequent pipeline\n'
    'stages.'
)
class SessionRefAnt(basetask.StandardTaskTemplate):
    Inputs = SessionRefAntInputs

    # This is a multi-vis task that handles all MSes in a session at once.
    is_multi_vis_task = True

    def __init__(self, inputs):
        super(SessionRefAnt, self).__init__(inputs)

    def prepare(self, **parameters):
        # Initialize results.
        result = SessionRefAntResults()

        # Define maximum number of antennas to evaluate phases for.
        nant = 3

        # Inspect the vis list to identify sessions and corresponding MSes.
        vislist_for_session = sessionutils.group_vislist_into_sessions(self.inputs.context, self.inputs.vis)

        # Run reference antenna identification for each session.
        for session_name, vislist in vislist_for_session.items():
            LOG.info("Evaluating reference antennas for session \"{}\" with measurement set(s): {}."
                     "".format(session_name, ', '.join([os.path.basename(vis) for vis in vislist])))
            refant = self._identify_best_refant(session_name, vislist, nant=nant)
            LOG.info("Final choice of reference antenna for session \"{}\": {}".format(session_name, refant))
            result.refant[session_name] = {'vislist': vislist, 'refant': refant}

        return result

    def analyse(self, result):
        return result

    def _identify_best_refant(self, session_name, vislist, nant):
        """
        Identify best reference antenna for specified list of measurement sets.

        First, the reference antenna lists of all measurement sets are combined
        into a single ranked list.

        Secondly, the 'nant' highest ranked antennas are tested to see whether
        CASA gaincal refant heuristics would accept the antenna or instead
        would pick a different refant for one or more solutions. For this test,
        a phase caltable is created with refant set to a specific ant, and this
        phase caltable is evaluated to count how many rows have "non-zero"
        phases (absolute phase above a threshold) for the specified antenna.
        These "non-zero" phases are taken as an indirect indication that CASA
        gaincal overrode the specified antenna for one or more rows (e.g. spw).

        If an antenna is found to cause no non-zero phase deviations for
        any MS, then it is immediately returned as the highest ranking
        "perfect" reference antenna.

        Otherwise, once the 'nant' highest ranking antennas (or all available
        ants, whichever is lowest nr.) are evaluated for suitability, then
        the antenna with the lowest total number of non-zero phases is chosen
        as the final "best" reference antenna for this session.

        If the measurement sets have only a single reference antenna in
        common, then this antenna is immediately returned as the "best"
        reference antenna, without running the "non-zero" phase evaluation.

        If the measurement sets have no reference antennas in common, then an
        empty string is returned for "best" reference antenna.

        :param session_name: name of session
        :type session_name: str
        :param vislist: list of measurement sets
        :type vislist: list [vis, vis, ...]
        :param nant: maximum number of antennas to evaluate phases for
        :type nant: int
        :return: name of best reference antenna
        :rtype: str
        """
        # Create ranked refant list.
        refants = self._create_combined_refant_list(session_name, vislist)

        # Handle the edge cases of MSes having only 0 or 1 refant in common.
        nrefants = len(refants)
        if nrefants == 0:
            LOG.warning("Measurement sets for session \"{}\" have no reference antennas in common, cannot determine a"
                        " final best reference antenna.".format(session_name))
            return ''
        elif nrefants == 1:
            LOG.warning("Measurement sets for session \"{}\" have only one reference antennas in common ({}), which"
                        " will be set as the final best reference antenna.".format(session_name, refants[0]))
            return refants[0]

        # If there are multiple candidate refants in common, then continue with
        # evaluating the ranked list of refants based on non-zero phases.
        n_nonzero = collections.defaultdict(int)

        # If the maximum number of antennas to evaluate phases for is less than
        # one, then no best refant can be found.
        if nant < 1:
            LOG.warning("Unable to find best reference antenna, maximum number of antennas to evaluate phases for is"
                        " set too low: {}".format(nant))
            return ''

        # Run phase evaluation for specified nr. of antennas or nr. of
        # antennas in common, whichever is lowest.
        for iant in range(min(nant, nrefants)):
            ant = refants[iant]
            LOG.info("Session \"{}\": running phase evaluation heuristics for candidate antenna {}"
                     "".format(session_name, ant))

            # Run a gaincal for each vis with refant set to the current ant.
            gcal_result = self._create_phase_caltables(vislist, ant)

            # Evaluate caltables to detect if refant changed.
            n_nonzero_phase = self._evaluate_phase_caltables(gcal_result, ant, self.inputs.phase_threshold)

            # In the (unlikely) case that no refant changes (i.e. no non-zero
            # phases) were detected for any MS, then this ant is the highest
            # ranking "perfect" reference antenna, and there is no need to
            # evaluate any further down the list.
            if not any(n_nonzero_phase.values()):
                LOG.info("Session \"{}\": no phase outlier rows found for candidate antenna {}, so will select"
                         " this as the highest ranking best reference antenna.".format(session_name, ant))
                return ant

            # Store total nr. of non-zero phases for this antenna.
            n_nonzero[ant] = sum(n_nonzero_phase.values())
            LOG.info("Session \"{}\": number of phase outlier rows found for candidate antenna {}: {}"
                     "".format(session_name, ant, n_nonzero[ant]))

        # From evaluated antennas, pick the one with the lowest total number of non-zero phase values;
        best_refant = sorted(n_nonzero, key=n_nonzero.get)[0]

        # If the best choice of reference antenna still resulted in non-zero
        # phases, then log a warning.
        if n_nonzero[best_refant] != 0:
            LOG.warning("Session \"{}\": final choice of best reference antenna ({}) resulted in {} phase outliers."
                        "".format(session_name, best_refant, n_nonzero[best_refant]))

        return best_refant

    def _create_combined_refant_list(self, session_name, vislist):
        """
        Combines the ranked reference antenna lists for all measurement sets
        specified in 'vislist' to return as a single ranked reference antenna
        list.

        :param session_name: name of session
        :type session_name: str
        :param vislist: list of measurement sets
        :type vislist: list [vis, vis, ...]
        :return: list of reference antennas
        :rtype: list[str]
        """
        # If there is just one MS, then return its reference antenna list.
        nvis = len(vislist)
        if nvis == 1:
            LOG.info("Session \"{}\" has only one measurement set ({}), continuing with its reference antenna list for"
                     " evaluation of best reference antenna.".format(session_name, os.path.basename(vislist[0])))
            return self.inputs.context.observing_run.get_ms(vislist[0]).reference_antenna.split(',')

        # Otherwise, continue with combining the multiple refant lists.

        # Sort vislist by start time of observation; this will allow for
        # ordering by highest ranking in first observed MS, in cases where
        # other sorting (further below) has resulted in equal ranking.
        vislist = sorted(vislist, key=lambda v: self.inputs.context.observing_run.get_ms(v).start_time['m0']['value'])

        # Collect refant list for all MSes.
        refant_lists = [self.inputs.context.observing_run.get_ms(vis).reference_antenna.split(',') for vis in vislist]

        # Create dictionary of antennas, mapping each antenna to a list of
        # their per-MS ranking as refant. If an ant was 1st in the refant list,
        # it is assigned rank 1; 2nd in list is rank 2, # etc...
        refant_ranks = collections.defaultdict(list)
        for refant_list in refant_lists:
            for idx, ant in enumerate(refant_list):
                refant_ranks[ant].append(idx+1)

        # For each ant in common among all MSes, retrieve/compute the following
        # information:
        #  1. name of ant
        #  2. the cross-product of their per-MS ranks
        #  3. the standard deviation of their per-MS ranks
        #  4. their refant rank for the first observed MS
        #     (assumed to be first element in list, due to earlier sorting)
        antinfo = [(ant, reduce(lambda x, y: x * y, refant_ranks[ant]), np.std(refant_ranks[ant]), refant_ranks[ant][0])
                   for ant in refant_ranks if len(refant_ranks[ant]) == nvis]

        # Create the final list of ranked reference antennas, after sorting
        # consecutively by cross-product of per-MS ranks, standard deviation
        # of per-MS ranks, and refant ranking for first-observed MS.
        refants_ranked = sorted(antinfo, key=operator.itemgetter(1, 2, 3))

        # Report the combined ranked antenna list.
        refant_str = ["{} ({}, {}, {})".format(*ant) for ant in refants_ranked]
        LOG.info("Ranked antenna list for session \"{}\", listed as 'antenna (cross-product of per-MS ranking, stdev"
                 " of per-MS ranks, ranking for first observed MS)': {}".format(session_name, ', '.join(refant_str)))

        # Return a list of just the antenna names.
        return [ant[0] for ant in refants_ranked]

    def _create_phase_caltables(self, vislist, refant):
        """
        Runs CASA gaincal to create a phase caltable for every MS in specified
        'vislist' with reference antenna set to specified 'refant'.

        :param vislist: list of measurement sets
        :type vislist: list [vis, vis, ...]
        :param refant: name of reference antenna to use in gaincal
        :type refant: str
        :return: the Results returned by the gaincal.GTypeGaincal task
        """
        container = vdp.InputsContainer(gaincal.GTypeGaincal, self.inputs.context, vis=vislist, intent="PHASE",
                                        calmode='p', solint='int', refant=refant, minsnr=3, gaintype='G')
        gaincal_task = gaincal.GTypeGaincal(container)
        return self._executor.execute(gaincal_task)

    def _evaluate_phase_caltables(self, results, antenna, threshold):
        """
        Evaluate each phase caltable (one per measurement set) in specified
        'results' to determine for specified 'antenna' how many rows have
        absolute phases above the specified 'threshold'.

        :param results: gaincal.GTypeGaincal Results
        :param antenna: name of antenna to check for non-zero phase outliers
        :type antenna: str
        :param threshold: threshold for detecting "non-zero" phase outliers
        :type threshold: int or float
        :return: dictionary with total number of non-zero phase outliers for
            each vis that has a caltable in results.
        """
        nonzero_phase_per_vis = collections.defaultdict()
        for result in results:
            if result.final:
                calapp = result.final[0]

                # Convert antenna name to ID.
                ms = self.inputs.context.observing_run.get_ms(calapp.vis)
                antenna_id = ms.get_antenna(antenna)[0].id

                # Test whether antenna appears in caltable.
                with casa_tools.TableReader(calapp.gaintable) as tb:
                    ants = tb.getcol('ANTENNA1')
                    if antenna_id not in ants:
                        LOG.warning("Antenna {} ({}) not found in caltable {}"
                                    "".format(antenna, antenna_id, os.path.basename(calapp.gaintable)))
                        return

                # Determine non-zero phases for each spw, and add to total.
                nonzero = 0
                for spw in calapp.spw.split(','):
                    # Retrieve phase data from caltable for specified antenna and current spw.
                    with casa_tools.TableReader(calapp.gaintable) as tb:
                        taql = "ANTENNA1 == {} && SPECTRAL_WINDOW_ID == {}".format(antenna_id, spw)
                        with contextlib.closing(tb.query(taql)) as subtb:
                            phases = subtb.getcol('CPARAM')

                    # Add number of outlier phases to running total of "non-zero" phases.
                    nonzero += len(np.where(np.abs(np.angle(phases, deg=True)) > threshold)[0])

                # Store total number of non-zero phase outliers for current vis.
                nonzero_phase_per_vis[calapp.vis] = nonzero

        return nonzero_phase_per_vis
