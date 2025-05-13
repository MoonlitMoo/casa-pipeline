import collections
import copy
import os
from typing import Dict, List, Optional, Set, Tuple

import numpy
import traceback

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.basetask as basetask
import pipeline.infrastructure.callibrary as callibrary
import pipeline.infrastructure.utils as utils
import pipeline.infrastructure.vdp as vdp
from pipeline.domain.measurementset import MeasurementSet
from pipeline.h.tasks.common import commonhelpermethods
from pipeline.hif.tasks.gaincal import gtypegaincal
from pipeline.hif.tasks.gaincal.common import GaincalResults
from pipeline.hifa.heuristics.phasemetrics import PhaseStabilityHeuristics
from pipeline.hifa.heuristics.phasespwmap import combine_spwmap
from pipeline.hifa.heuristics.phasespwmap import simple_n2wspwmap
from pipeline.hifa.heuristics.phasespwmap import snr_n2wspwmap
from pipeline.hifa.heuristics.phasespwmap import update_spwmap_for_band_to_band
from pipeline.hifa.tasks.gaincalsnr import gaincalsnr
from pipeline.infrastructure import casa_tools
from pipeline.infrastructure import task_registry

LOG = infrastructure.get_logger(__name__)

__all__ = [
    'SpwPhaseupInputs',
    'SpwPhaseup',
    'SpwPhaseupResults'
]

IntentField = collections.namedtuple('IntentField', 'intent field')
SpwMapping = collections.namedtuple('SpwMapping', 'combine spwmap low_combinedsnr_spws snr_info')


class SpwPhaseupInputs(gtypegaincal.GTypeGaincalInputs):
    # Spw mapping mode heuristics, options are:
    #  'auto': apply SNR-based heuristics to determine which type of spw
    #          mapping / combination is appropriate.
    #  'combine': force use of combined spw mapping
    #  'simple': force use of simple narrow-to-wide spw mapping.
    #  'default': use the standard mapping, mapping each spw to itself.
    hm_spwmapmode = vdp.VisDependentProperty(default='auto')

    @hm_spwmapmode.convert
    def hm_spwmapmode(self, value):
        allowed = ('auto', 'combine', 'simple', 'default')
        if value not in allowed:
            m = ', '.join(('{!r}'.format(i) for i in allowed))
            raise ValueError('Value not in allowed value set ({!s}): {!r}'.format(m, value))
        return value

    # Fraction of bandwidth to ignore on each edge of a spectral window
    # for SNR assessment.
    bwedgefrac = vdp.VisDependentProperty(default=0.03125)

    # Maximum fraction of data that can be flagged for an antenna before that
    # antenna is considered "flagged" and no longer considered in SNR
    # estimation.
    maxfracflagged = vdp.VisDependentProperty(default=0.90)

    # Maximum narrow bandwidth.
    maxnarrowbw = vdp.VisDependentProperty(default='300MHz')

    # Width of spw must be larger than minfracmaxbw * maximum bandwith for
    # a spw to be a match.
    minfracmaxbw = vdp.VisDependentProperty(default=0.8)

    # Phase SNR threshold to use in spw mapping assessment to identify low
    # SNR spws.
    phasesnr = vdp.VisDependentProperty(default=32.0)

    # Toggle to select whether to restrict spw matching to the same baseband.
    samebb = vdp.VisDependentProperty(default=True)

    # Antenna flagging heuristics parameter
    hm_nantennas = vdp.VisDependentProperty(default='all')

    @hm_nantennas.convert
    def hm_nantennas(self, value):
        allowed = ('all', 'unflagged')
        if value not in allowed:
            m = ', '.join(('{!r}'.format(i) for i in allowed))
            raise ValueError('Value not in allowed value set ({!s}): {!r}'.format(m, value))
        return value

    # Allow user to set custom filename for the phase offsets caltable.
    caltable = vdp.VisDependentProperty(default=None)

    # Intent to use in data selection for computing phase offsets caltable.
    intent = vdp.VisDependentProperty(default='BANDPASS')

    # PIPE-629: parameter to unregister existing phaseup tables before appending to callibrary
    unregister_existing = vdp.VisDependentProperty(default=False)

    def __init__(self, context, vis=None, output_dir=None, caltable=None, intent=None, hm_spwmapmode=None,
                 phasesnr=None, bwedgefrac=None, hm_nantennas=None, maxfracflagged=None,
                 maxnarrowbw=None, minfracmaxbw=None, samebb=None, unregister_existing=None, **parameters):
        super().__init__(context, vis=vis, output_dir=output_dir, **parameters)
        self.caltable = caltable
        self.intent = intent
        self.hm_spwmapmode = hm_spwmapmode
        self.phasesnr = phasesnr
        self.bwedgefrac = bwedgefrac
        self.hm_nantennas = hm_nantennas
        self.maxfracflagged = maxfracflagged
        self.maxnarrowbw = maxnarrowbw
        self.minfracmaxbw = minfracmaxbw
        self.samebb = samebb
        self.unregister_existing = unregister_existing


@task_registry.set_equivalent_casa_task('hifa_spwphaseup')
class SpwPhaseup(gtypegaincal.GTypeGaincal):
    Inputs = SpwPhaseupInputs

    def prepare(self, **parameters):
        # Simplify the inputs
        inputs: SpwPhaseupInputs = self.inputs

        # Intents to derive separate SpW mappings for:
        spwmap_intents = 'CHECK,PHASE'

        # Do not derive separate SpW mappings for fields that also cover any of
        # these calibrator intents:
        exclude_intents = 'AMPLITUDE,BANDPASS,POLARIZATION,POLANGLE,POLLEAKAGE,DIFFGAINREF,DIFFGAINSRC'

        # PIPE-629: if requested, unregister old spwphaseup calibrations from
        # local copy of context, to stop these from being pre-applied during
        # this stage.
        if inputs.unregister_existing:
            self._unregister_spwphaseup()

        # Derive the mapping from phase fields to target/check fields.
        phasecal_mapping = self._derive_phase_to_target_check_mapping(inputs.ms)

        # Derive the optimal spectral window maps.
        spwmaps = self._derive_spwmaps(spwmap_intents, exclude_intents)

        # Compute the spw-to-spw phase offsets ("phaseup") cal table and accept
        # into local context.
        phaseupresult = self._do_phaseup()

        # Compute diagnostic phase caltables for both phase calibrator fields
        # and check source fields, with the spw-to-spw phase offset corrections
        # included in pre-apply.
        diag_phase_results = self._do_diagnostic_phasecal(spwmaps)

        # Compute what SNR is achieved for PHASE fields after the SpW phase-up
        # correction.
        snr_info = self._compute_median_snr(diag_phase_results)

        # Do the decoherence assessment
        phaserms_results, phaserms_cycletime, phaserms_totaltime, phaserms_antout \
            = self._do_decoherence_assessment()

        # Create the results object.
        result = SpwPhaseupResults(vis=inputs.vis, phasecal_mapping=phasecal_mapping, phaseup_result=phaseupresult,
                                   snr_info=snr_info, spwmaps=spwmaps, unregister_existing=inputs.unregister_existing,
                                   phaserms_totaltime=phaserms_totaltime, phaserms_cycletime=phaserms_cycletime,
                                   phaserms_results=phaserms_results, phaserms_antout=phaserms_antout)

        return result

    def analyse(self, result):
        # The caltable portion of the result is treated as if it were any other
        # calibration result. With no best caltable to find, our task is simply
        # to set the one caltable as the best result.

        # double-check that the caltable was actually generated
        on_disk = [ca for ca in result.phaseup_result.pool if ca.exists()]
        result.phaseup_result.final[:] = on_disk

        missing = [ca for ca in result.phaseup_result.pool if ca not in on_disk]
        result.phaseup_result.error.clear()
        result.phaseup_result.error.update(missing)

        return result

    @staticmethod
    def _derive_phase_to_target_check_mapping(ms: MeasurementSet) -> Dict[str, Set]:
        """
        Derive mapping between PHASE calibrator fields (by name) and
        corresponding fields (by name) with TARGET / CHECK intent that these
        PHASE calibrators should calibrate.

        PIPE-1154: This heuristic is intended for ALMA observing, and assumes
        that the first scan of a TARGET / CHECK field is always preceded by a
        scan of the corresponding PHASE calibrator. This method further assumes
        that scan IDs increase sequentially with observing time.

        Args:
            ms: MeasurementSet to derive mapping for.

        Returns:
            Dictionary of PHASE field names (key) and set of names of
            corresponding TARGET/CHECK fields (value).
        """
        # Get the PHASE field names.
        phase_fields = [f.name for f in ms.get_fields(intent='PHASE')]

        # Initialize the mapping for each PHASE calibrator field.
        mapping = {f: set() for f in phase_fields}

        # Get IDs of PHASE intent scans.
        phase_scan_ids = [s.id for s in ms.get_scans(scan_intent='PHASE')]

        for intent in ['CHECK', 'TARGET']:
            # Get field names for current intent.
            fields = [f.name for f in ms.get_fields(intent=intent)]

            for field in fields:
                # Get ID of first scan for current field with current intent.
                first_scan_id = ms.get_scans(field=field, scan_intent=intent)[0].id

                # PIPE-1154: in standard ALMA observing, each first scan of a
                # field with TARGET or CHECK intent should be preceded by a
                # scan of its corresponding PHASE calibrator.
                # Identify PHASE intent scans that preceded the first scan.
                preceding_phase_scan_ids = [i for i in phase_scan_ids if i < first_scan_id]
                if preceding_phase_scan_ids:
                    # Pick nearest in time PHASE intent scan as the match, and
                    # identify name of corresponding field.
                    matching_phase_scan_id = max(preceding_phase_scan_ids)
                else:
                    # Identify PHASE intent scans that followed the first scan.
                    following_phase_scan_ids = [i for i in phase_scan_ids if i > first_scan_id]
                    if following_phase_scan_ids:
                        # As a fall-back, pick nearest in time PHASE intent
                        # scan after first field scan, but raise warning.
                        matching_phase_scan_id = min(following_phase_scan_ids)
                        LOG.warning(f"{ms.basename}: no PHASE scans found prior to the first scan for field {field}"
                                    f" ({intent}), will match nearest PHASE scan that was taken after.")
                    else:
                        matching_phase_scan_id = None
                        LOG.warning(f"{ms.basename}: no PHASE scans found prior or after first scan for field {field}"
                                    f" ({intent}).")

                # If a matching PHASE scan was found, then update mapping to
                # link the corresponding PHASE field to current field.
                if matching_phase_scan_id:
                    matching_phase_field = [f.name for f in ms.get_scans(scan_id=matching_phase_scan_id)[0].fields][0]
                    mapping[matching_phase_field].add(field)

        return mapping

    def _derive_spwmaps(self, spwmap_intents: str, exclude_intents: str) -> Dict[IntentField, SpwMapping]:
        """
        Compute separate optimal spectral window mapping for each field
        covering one of the intents specified by "spwmap_intents", unless the
        field is already covered by a calibrator intent specified in
        "exclude_intents".

        Args:
            spwmap_intents: intents to derive separate SpW mappings for.
            exclude_intents: do not derive separate SpW mappings for fields
                that also cover any of these calibrator intents.

        Returns:
            Dictionary with (Intent, Field) combinations as keys and
            corresponding spectral window mapping as values.
        """
        # Simplify the inputs
        inputs: SpwPhaseupInputs = self.inputs

        # Report which spw mapping heuristics mode is being used.
        LOG.info(f"The spw mapping mode for {inputs.ms.basename} is \"{inputs.hm_spwmapmode}\"")

        # Initialize collection of spectral window maps and corresponding
        # SNR info.
        spwmaps = {}

        # Identify the combinations of intent and fields for which to derive a
        # separate spwmap.
        intent_field_to_assess = self._get_intent_field(inputs.ms, intents=spwmap_intents,
                                                        exclude_intents=exclude_intents)

        # Run derivation of spwmap for each intent, field combination.
        for intent, field in intent_field_to_assess:
            LOG.info(f'Deriving optimal spw mapping for {inputs.ms.basename}, intent={intent}, field={field}')
            spwmaps[IntentField(intent, field)] = self._derive_spwmap_for_intent_field(intent, field)

        return spwmaps

    def _derive_spwmap_for_intent_field(self, intent: str, field: str) -> SpwMapping:
        """
        Derive optimal spectral window mapping for specified intent and field.

        Args:
            intent: intent for which to derive SpW mapping.
            field: field for which to derive SpW mapping.

        Returns:
            SpwMapping object, representing the spectral window mapping.
        """
        # Simplify the inputs
        inputs: SpwPhaseupInputs = self.inputs

        # PIPE-2059: select what SpWs to analyse.
        if self.inputs.ms.is_band_to_band:
            # For a BandToBand MS, restrict the SpWs to diffgain reference SpWs
            # for PHASE intent, or to diffgain on-source SpWs for CHECK intent.
            if intent == 'PHASE':
                spws = inputs.ms.get_spectral_windows(task_arg=inputs.spw, intent='DIFFGAINREF')
            elif intent == 'CHECK':
                spws = inputs.ms.get_spectral_windows(task_arg=inputs.spw, intent='DIFFGAINSRC')
            else:
                LOG.warning(f"{inputs.ms.basename}: unexpected intent ({intent}) encountered while deriving SpW mapping"
                            f" for BandToBand; cannot create SpW mapping.")
                spws = []
        else:
            # For all other measurement sets, select all science SpWs.
            spws = inputs.ms.get_spectral_windows(task_arg=inputs.spw, science_windows_only=True)

        # By default, combine is False (i.e., no combining of spws), the
        # spwmap is empty (i.e. no mapping of spws), and there are no spws
        # whose combined phase SNR does not meet the threshold set by the
        # inputs.phasesnr parameter.
        combine = False
        spwmap = []
        low_combinedsnr_spws = []

        # Initialize lists needed to assess SNR info that is to be shown in
        # task weblog; these will get populated if an SNR test is run
        # (depending on the spw mapping mode).
        snrs = []
        combined_snrs = []
        spwids = []

        # PIPE-1436: if there is only one SpW, then no SpW re-mapping can be
        # done. In this case, just run the SNR test.
        if len(spws) == 1:
            LOG.info(f'Only 1 science SpW found for {inputs.ms.basename}, intent={intent}, field={field}, so using'
                     f' standard SpW map for this data selection.')
            # Run a task to estimate the gaincal SNR for given intent, field,
            # and spectral windows.
            nosnrs, spwids, snrs, goodsnrs = self._do_snrtest(intent, field, ','.join(str(spw.id) for spw in spws))

        # If there are multiple SpWs, then continue with computing the optimal
        # SpW map according to the rules defined by each mapping mode.
        elif inputs.hm_spwmapmode == 'auto':
            # Run a task to estimate the gaincal SNR for given intent, field,
            # and spectral windows.
            nosnrs, spwids, snrs, goodsnrs = self._do_snrtest(intent, field, ','.join(str(spw.id) for spw in spws))

            # No SNR estimates available, default to simple narrow-to-wide spw
            # mapping.
            if nosnrs:
                LOG.warning(f'No SNR estimates for any spws - Forcing simple spw mapping for {inputs.ms.basename},'
                            f' intent={intent}, field={field}')
                spwmap = simple_n2wspwmap(spws, inputs.maxnarrowbw, inputs.minfracmaxbw, inputs.samebb)
                LOG.info(f'Using spw map {spwmap} for {inputs.ms.basename}, intent={intent}, field={field}')

            # All spws have good SNR values, no spw mapping required.
            elif len([goodsnr for goodsnr in goodsnrs if goodsnr is True]) == len(goodsnrs):
                LOG.info(f'High SNR - Default spw mapping used for all spws {inputs.ms.basename}')
                LOG.info(f'Using spw map {spwmap} for {inputs.ms.basename}, intent={intent}, field={field}')

            # No spws have good SNR values, use combined spw mapping, and test
            # which spws have too low combined phase SNR.
            elif len([goodsnr for goodsnr in goodsnrs if goodsnr is True]) == 0:
                # Report spws for which no SNR estimate was available.
                if None in goodsnrs:
                    LOG.warning('Spws without SNR measurements {}'
                                ''.format([spwid for spwid, goodsnr in zip(spwids, goodsnrs) if goodsnr is None]))

                # Create a spw mapping for combining spws.
                spwmap = combine_spwmap(spws)
                combine = True
                LOG.info(f'Using combined spw map {spwmap} for {inputs.ms.basename}, intent={intent}, field={field}')

                # Run a test on the combined spws to identify spws whose
                # combined SNR would not meet the threshold set by
                # inputs.phasesnr.
                low_combinedsnr_spws, combined_snrs = self._do_combined_snr_test(spwids, snrs, spwmap)

            # If some, but not all, spws have good SNR values, then try to use
            # an SNR-based approach for, but fall back to combined spw mapping
            # if necessary.
            else:
                # Report spws for which no SNR estimate was available.
                if None in goodsnrs:
                    LOG.warning('Spws without SNR measurements {}'
                                ''.format([spwid for spwid, goodsnr in zip(spwids, goodsnrs) if goodsnr is None]))

                # Create an SNR-based spw mapping.
                goodmap, spwmap, _ = snr_n2wspwmap(spws, snrs, goodsnrs)

                # If the SNR-based mapping gave a good match for all spws, then
                # report the final phase up spw map.
                if goodmap:
                    LOG.info(f'Using spw map {spwmap} for {inputs.ms.basename}, intent={intent}, field={field}')

                # Otherwise, replace the spwmap with a combined spw map.
                else:
                    LOG.warning(f'Still unable to match all spws - Forcing combined spw mapping for'
                                f' {inputs.ms.basename}')

                    # Create a spw mapping for combining spws.
                    spwmap = combine_spwmap(spws)
                    combine = True
                    LOG.info(f'Using combined spw map {spwmap} for'
                             f' {inputs.ms.basename}, intent={intent}, field={field}')

                    # Run a test on the combined spws to identify spws whose
                    # combined SNR would not meet the threshold set by
                    # inputs.phasesnr.
                    low_combinedsnr_spws, combined_snrs = self._do_combined_snr_test(spwids, snrs, spwmap)

        elif inputs.hm_spwmapmode == 'combine':
            spwmap = combine_spwmap(spws)
            combine = True
            low_combinedsnr_spws = [spw.id for spw in spws]
            LOG.info(f'Using combined spw map {spwmap} for {inputs.ms.basename}, intent={intent}, field={field}')

        elif inputs.hm_spwmapmode == 'simple':
            spwmap = simple_n2wspwmap(spws, inputs.maxnarrowbw, inputs.minfracmaxbw, inputs.samebb)
            LOG.info(f'Using simple spw map {spwmap} for {inputs.ms.basename}, intent={intent}, field={field}')

        else:
            LOG.info(f'Using standard spw map {spwmap} for {inputs.ms.basename}, intent={intent}, field={field}')

        # PIPE-2059: for the PHASE calibrator in a BandToBand MS, adjust the
        # newly derived optimal SpW mapping to ensure that diffgain on-source
        # SpWs are remapped to an associated diffgain reference SpW.
        if self.inputs.ms.is_band_to_band and intent == 'PHASE':
            dg_refspws = inputs.ms.get_spectral_windows(task_arg=inputs.spw, intent='DIFFGAINREF')
            dg_srcspws = inputs.ms.get_spectral_windows(task_arg=inputs.spw, intent='DIFFGAINSRC')
            spwmap = update_spwmap_for_band_to_band(spwmap, dg_refspws, dg_srcspws)

        # Collect SNR info.
        snr_info = self._get_snr_info(spwids, snrs, combined_snrs)

        return SpwMapping(combine, spwmap, low_combinedsnr_spws, snr_info)

    def _do_snrtest(self, intent: str, field: str, spw: str) -> Tuple[bool, List, List, List]:
        """
        Run gaincal SNR task to perform SNR test for specified intent and
        field.

        Args:
            intent: intent for which to perform SNR test.
            field: field for which to perform SNR test.
            spw: SpW IDs for which to perform SNR test.
        Returns:
            Tuple containing
              * Boolean to denote whether no SNRs were derived for any SpW.
              * list of SpW IDs for which SNR was derived
              * list of derived SNRs
              * list of booleans denoting whether derived SNR
                was good (>= SNR threshold)
        """
        # Simplify inputs.
        inputs = self.inputs

        task_args = {
            'output_dir': inputs.output_dir,
            'vis': inputs.vis,
            'field': field,
            'intent': intent,
            'spw': spw,
            'phasesnr': inputs.phasesnr,
            'bwedgefrac': inputs.bwedgefrac,
            'hm_nantennas': inputs.hm_nantennas,
            'maxfracflagged': inputs.maxfracflagged,
        }
        task_inputs = gaincalsnr.GaincalSnrInputs(inputs.context, **task_args)
        gaincalsnr_task = gaincalsnr.GaincalSnr(task_inputs)
        result = self._executor.execute(gaincalsnr_task)

        nosnr = True
        spwids = []
        snrs = []
        goodsnrs = []
        for i in range(len(result.spwids)):
            if result.snrs[i] is None:
                spwids.append(result.spwids[i])
                snrs.append(None)
                goodsnrs.append(None)
            elif result.snrs[i] < inputs.phasesnr:
                spwids.append(result.spwids[i])
                snrs.append(result.snrs[i])
                goodsnrs.append(False)
                nosnr = False
            else:
                spwids.append(result.spwids[i])
                goodsnrs.append(True)
                snrs.append(result.snrs[i])
                nosnr = False

        return nosnr, spwids, snrs, goodsnrs

    def _do_combined_snr_test(self, spwlist: List, perspwsnr: List, spwmap: List) -> Tuple[List, Dict]:
        """
        Calculate combined SNRs from the "per-SpW SNR" and return a list of SpW
        IDs that does not meet phase SNR threshold. Grouping of SpWs is
        specified by input parameter spwmap.

        For each grouped SpWs, combined SNR is calculated by:
            combined SNR = numpy.linalg.norm(list of per SpW SNR in a group)

        Args:
            spwlist: List of spw IDs to calculate combined SNR
            perspwsnr: List of SNRs of each SpW
            spwmap: List representing a spectral window map that specifies
                which SpW IDs should be combined together.

        Returns:
            List of spectral window IDs whose combined phase SNR is below the
            threshold specified by inputs.phasesnr, and dictionary containing
            for a given reference SpW the corresponding mapped SpWs and
            combined SNR.
        """
        LOG.info("Start combined SpW SNR test")
        LOG.debug('- spwlist to analyze: {}'.format(spwlist))
        LOG.debug('- per SpW SNR: {}'.format(perspwsnr))
        LOG.debug('- spwmap = {}'.format(spwmap))

        # Initialize return objects.
        low_snr_spwids = []
        combined_snrs = {}

        # Filter reference SpW IDs of each group.
        unique_mappedspw = {spwmap[spwid] for spwid in spwlist}
        for mappedspwid in unique_mappedspw:
            snrlist = []
            combined_idx = []

            # only consider SpW IDs in spwlist for combination
            for i in range(len(spwlist)):
                spwid = spwlist[i]
                if spwmap[spwid] == mappedspwid:
                    snr = perspwsnr[i]
                    if snr is None:
                        continue
                    snrlist.append(snr)
                    combined_idx.append(i)

            if not snrlist:
                LOG.error('No SpW with valid SNR values; cannot calculate the combined SNR')
                return [], {}

            # calculate combined SNR from per spw SNR
            combined_snr = numpy.linalg.norm(snrlist)
            combined_spws = [spwlist[j] for j in combined_idx]
            LOG.info('Reference SpW ID = {} (Combined SpWs = {}) : Combined SNR = {}'
                     ''.format(mappedspwid, str(combined_spws), combined_snr))
            # For current reference SpW, store list of combined SpWs and
            # the combined SNR.
            combined_snrs[str(mappedspwid)] = (combined_spws, combined_snr)

            # If the combined SNR does not meet the phase SNR threshold, then
            # add these to the list of low combined SNR spws.
            if combined_snr < self.inputs.phasesnr:
                low_snr_spwids.extend([spwlist[i] for i in combined_idx])

        # Log results from SNR test.
        LOG.info('SpW IDs that has low combined SNR (threshold: {}) = {}'.format(self.inputs.phasesnr, low_snr_spwids))

        return low_snr_spwids, combined_snrs

    def _do_gaincal(self, caltable: Optional[str] = None, field: Optional[str] = None, intent: Optional[str] = None,
                    gaintype: Optional[str] = None, combine: Optional[str] = None, minblperant: Optional[int] = None,
                    minsnr: Optional[int] = None) -> GaincalResults:
        """
        Runs gaincal worker task separately for each SpectralSpec present
        among the requested SpWs, each appending to the same caltable.

        The CalApplications in the result are modified to set calwt to False.

        Args:
            caltable: name of output caltable
            field: field selection string
            intent: intent selection string
            gaintype: gain type to use
            combine: selects whether to combine SpWs
            minblperant: minimum baselines per antenna
            minsnr: minimum SNR

        Returns:
            Results object from gaincal worker task.
        """
        inputs = self.inputs
        ms = inputs.ms

        # Identify which science spws were selected by inputs parameter.
        request_spws = ms.get_spectral_windows(task_arg=inputs.spw)

        # Identify which scans covered the requested intent, field, and any of
        # the requested spws.
        targeted_scans = ms.get_scans(scan_intent=intent, spw=inputs.spw, field=field)

        # Among the requested spws, identify which have a scan among the
        # targeted scans.
        scan_spws = {spw for scan in targeted_scans for spw in scan.spws if spw in request_spws}

        # Create a separate phase solution caltable for each SpectralSpec
        # grouping of SpWs and collect the corresponding CalApplications from
        # the task results. Each caltable should have a unique filename since
        # the filename includes the SpW selection.
        original_calapps = []
        for spectral_spec, tuning_spw_ids in utils.get_spectralspec_to_spwid_map(scan_spws).items():
            tuning_spw_str = ','.join([str(i) for i in sorted(tuning_spw_ids)])
            LOG.info('Processing spectral spec {}, spws {}'.format(spectral_spec, tuning_spw_str))

            scans_with_data = ms.get_scans(spw=tuning_spw_str, scan_intent=inputs.intent)
            if not scans_with_data:
                LOG.info('No data to process for spectral spec {}. Continuing...'.format(spectral_spec))
                continue

            # Initialize gaincal inputs.
            task_args = {
                'output_dir': inputs.output_dir,
                'vis': inputs.vis,
                'caltable': caltable,
                'field': field,
                'intent': intent,
                'spw': tuning_spw_str,
                'solint': 'inf',
                'gaintype': gaintype,
                'calmode': 'p',
                'minsnr': minsnr,
                'combine': combine,
                'refant': inputs.refant,
                'minblperant': minblperant,
            }
            task_inputs = gtypegaincal.GTypeGaincalInputs(inputs.context, **task_args)

            # Initialize and execute gaincal task.
            phasecal_task = gtypegaincal.GTypeGaincal(task_inputs)
            phasecal_result = self._executor.execute(phasecal_task)

            # Collect CalApplications.
            original_calapps.extend(phasecal_result.pool)

        # Phase solution caltables should always be registered to be applied
        # with calwt=False (PIPE-1154). Create an updated version of each
        # CalApplication with the override to set calwt to False. Replace any
        # existing CalApplications in latest tuning result with complete list
        # of all updated CalApplications, and return this as the final result.
        processed_calapps = [callibrary.copy_calapplication(c, calwt=False) for c in original_calapps]
        phasecal_result.pool = processed_calapps
        phasecal_result.final = processed_calapps

        return phasecal_result

    def _do_phaseup(self) -> GaincalResults:
        """
        Creates the SpW-to-SpW phase-up caltable, and merges the resulting
        table into the local task context.

        Returns:
            Results object from gaincal worker task.
        """
        inputs = self.inputs

        # Create spw-to-spw phaseup caltable.
        LOG.info(f'Computing spw phase-up table for {inputs.ms.basename}')
        tuning_result = self._do_gaincal(caltable=inputs.caltable, field=inputs.field, intent=inputs.intent,
                                         gaintype='G', combine=inputs.combine, minsnr=inputs.minsnr,
                                         minblperant=inputs.minblperant)

        # Accept this spw-to-spw phase offsets result into the local context,
        # to ensure the caltable is included in pre-apply for subsequent steps
        # in this task.
        tuning_result.accept(inputs.context)

        return tuning_result

    def _do_diagnostic_phasecal(self, spwmaps: Dict[IntentField, SpwMapping]) -> List[GaincalResults]:
        """
        Creates diagnostic phase caltables for each phase calibrator field and
        each check source field, where the SpW-to-SpW phase-up caltable should
        be included in pre-apply (since it was merged into local context).

        These tables are used later in this stage to assess the median SNR
        achieved for each phase calibrator / check source in each SpW after the
        SpW-to-SpW phase-up is applied (PIPE-665).

        Use phase gaincal parameters appropriate for the SpW mapping derived
        earlier for each phase calibrator / check source field. Similar to
        hifa_timegaincal, set minblperant to 4 and minsnr to 3.

        Args:
            spwmaps: dictionary with (Intent, Field) combinations as keys and
                corresponding spectral window mapping as values.

        Returns:
            List of result objects from gaincal worker task(s) that produced
            the diagnostic phase caltable(s).
        """
        inputs = self.inputs

        # Derive separate phase solutions for each PHASE and each CHECK field.
        intents = ['PHASE', 'CHECK']
        gaincal_results = []
        for intent in intents:
            for field in inputs.ms.get_fields(intent=intent):
                # Get optimal value for phase solution parameters 'combine' and
                # 'gaintype' for current field from SpW mappings derived
                # earlier in this task.
                combine, gaintype = self._get_phasecal_params(spwmaps, intent, field.name)

                # Create diagnostic phase caltables.
                # PIPE-665: for the diagnostic phase caltables, always use
                # minsnr=3, minblperant=4.
                LOG.info(f'Computing diagnostic phase caltable for {inputs.ms.basename}, intent={intent},'
                         f' field={field.name}.')
                gaincal_results.append(self._do_gaincal(field=field.name, intent=intent, gaintype=gaintype,
                                                        combine=combine, minblperant=4, minsnr=3))

        return gaincal_results

    def _compute_median_snr(self, gaincal_results: List[GaincalResults]) -> Dict[Tuple[str, str, str], float]:
        """
        This method evaluates the diagnostic phase caltable(s) produced in an
        earlier step to compute the median achieved SNR for each phase calibrator /
        check source field, and for each SpW.

        Args:
            gaincal_results: List of gaincal worker task results representing
                the diagnostic phase caltable(s).

        Returns:
            Dictionary with intent, field name, and SpW as keys, and
            corresponding median achieved SNR as values.
        """
        inputs = self.inputs

        LOG.info(f'Computing median achieved phase SNR information for {inputs.ms.basename}.')

        snr_info = collections.defaultdict(dict)
        for result in gaincal_results:
            # Retrieve intent, field, and caltable.
            caltable = result.final[0].gaintable
            field = result.inputs['field']
            intent = result.inputs['intent']

            # Get SpWs and SNR info from caltable.
            with casa_tools.TableReader(caltable) as table:
                spws = table.getcol("SPECTRAL_WINDOW_ID")
                snrs = table.getcol("SNR")

            # Evaluate each unique SpW separately.
            for spw in sorted(set(spws)):
                # Get indices in caltable data corresponding to current SpW.
                ind_spw = numpy.where(spws == spw)[0]

                # Get number of correlations for this SpW.
                corr_type = commonhelpermethods.get_corr_products(inputs.ms, spw)
                ncorrs = len(corr_type)

                # Compute median achieved SNR and store in snr_info. If this
                # SpW covers a single polarisation, then compute the median SNR
                # using only the one corresponding column in the caltable.
                if ncorrs == 1:
                    # Identify which column in caltable to use for computing
                    # the median SNR.
                    ind_col = commonhelpermethods.get_pol_id(inputs.ms, spw, corr_type[0])
                    snr_info[(intent, field, spw)] = numpy.median(snrs[ind_col, 0, ind_spw])
                # Otherwise, i.e. SpW is multi-pol, use all columns.
                else:
                    snr_info[(intent, field, spw)] = numpy.median(snrs[:, 0, ind_spw])

        return snr_info

    def _do_decoherence_assessment(self) -> Tuple[Optional[Dict], Optional[str], Optional[str], List]:
        try:
            LOG.info("Starting phase RMS structure function decoherence assessment.")

            # Initialize the phase RMS structure function assessment
            inputs = copy.deepcopy(self.inputs)
            phase_rms = PhaseStabilityHeuristics(inputs, outlier_limit=180.0, flag_tolerance=0.3, max_poor_ant=11)

            # Do the analysis
            phaserms_results, phaserms_cycletime, phaserms_totaltime, phaserms_antout = phase_rms.analysis()

        except Exception:
            phaserms_results, phaserms_cycletime, phaserms_totaltime = None, None, None
            phaserms_antout = []
            LOG.error("For {}, phase RMS structure function analysis failed".format(self.inputs.ms.basename))
            LOG.error(traceback.format_exc())

        return phaserms_results, phaserms_cycletime, phaserms_totaltime, phaserms_antout
    
    @staticmethod
    def _get_intent_field(ms: MeasurementSet, intents: str, exclude_intents: str = None) -> List[Tuple[str, str]]:
        # If provided, convert "intents to exclude" into set of strings.
        if exclude_intents is None:
            exclude_intents = set()
        else:
            exclude_intents = set(exclude_intents.split(','))

        intent_field = []
        for intent in intents.split(','):
            for field in ms.get_fields(intent=intent):
                # Check whether found field also covers any of the intents to
                # skip.
                excluded_intents_found = field.intents.intersection(exclude_intents)
                if not excluded_intents_found:
                    intent_field.append((intent, field.name))
                else:
                    # Log a message to explain why no spwmap will be derived
                    # for this particular combination of field and intent.
                    excluded_intents_str = ", ".join(sorted(excluded_intents_found))
                    LOG.info(f'{ms.basename}: will not derive spwmap for field {field.name} (#{field.id}) and intent'
                             f' {intent} because this field also covers calibrator intent(s) {excluded_intents_str}')

        return intent_field

    def _unregister_spwphaseup(self):
        inputs = self.inputs
        ms = inputs.ms

        # predicate function that triggers when the spwphaseup caltable is detected
        def spwphaseup_matcher(_: callibrary.CalToArgs, calfrom: callibrary.CalFrom) -> bool:
            return 'hifa_spwphaseup' in calfrom.gaintable

        LOG.info('Temporarily unregistering previous spwphaseup tables while task executes')
        inputs.context.callibrary.unregister_calibrations(spwphaseup_matcher)

        # Reset the spwmaps registered in the MS. This will be restored if the
        # result is not accepted.
        LOG.info('Temporarily resetting spwmaps for %s', ms.basename)
        ms.spwmaps = {}

    def _get_snr_info(self, spwids: List[int], snrs: List[float], combined_snrs: Dict) -> List[Tuple[str, float]]:
        """
        Helper method that takes phase SNR info from the SNR test, and returns
        phase SNR info for all SpWs specified in inputs.spw.

        Args:
            spwids: list of SpW IDs for which phase SNRs were determined.
            snrs: list of phase SNRs.
            combined_snrs: dictionary of reference SpWs with list of
                corresponding combined SpW and combined phase SNR.

        Returns:
            List of tuples, specifying string representing SpW(s) and
            corresponding phase SNR.
        """
        spw_snr = {str(k): v for k, v in zip(spwids, snrs)}
        snr_info = []

        # Create entry for each SpW specified by inputs.
        for spwid in self.inputs.spw.split(','):
            # If this SpW is the reference SpW for a group of combined SpWs
            # then add an entry to list the combined SNR.
            if spwid in combined_snrs:
                combined_spws = ', '.join(str(s) for s in combined_snrs[spwid][0])
                combined_snr = combined_snrs[spwid][1]
                snr_info.append((f'Combined ({combined_spws})', combined_snr))

            # Retrieve SNR info for individual SpW if available.
            snr = spw_snr.get(spwid, None)
            snr_info.append((str(spwid), snr))

        return snr_info

    @staticmethod
    def _get_phasecal_params(spwmaps: Dict[IntentField, SpwMapping], intent: str, field: str) -> Tuple[str, str]:
        # By default, no spw mapping or combining, and gaintype='G'.
        combine = ''
        gaintype = 'G'

        # Try to retrieve SpW mapping info for requested intent and field.
        spwmapping = spwmaps.get((intent, field), None)

        # If a mapping was found, and it requires SpW combination, then update
        # combine and gaintype accordingly.
        if spwmapping and spwmapping.combine:
            combine = 'spw'
            gaintype = 'T'

        return combine, gaintype


class SpwPhaseupResults(basetask.Results):
    def __init__(self, vis: str = None, phasecal_mapping: Dict = None, phaseup_result: GaincalResults = None,
                 snr_info: Dict = None, spwmaps: Dict = None, unregister_existing: Optional[bool] = False,
                 phaserms_totaltime: str = None, phaserms_cycletime: str = None,
                 phaserms_results: Optional[Dict] = None, phaserms_antout: Optional[List] = None):
        """
        Initialise the phaseup spw mapping results object.
        """
        super().__init__()

        if spwmaps is None:
            spwmaps = {}
        if phaserms_antout is None:
            phaserms_antout = []

        self.vis = vis
        self.phasecal_mapping = phasecal_mapping
        self.phaseup_result = phaseup_result
        self.snr_info = snr_info
        self.spwmaps = spwmaps
        self.unregister_existing = unregister_existing
        self.phaserms_totaltime = phaserms_totaltime
        self.phaserms_cycletime = phaserms_cycletime
        self.phaserms_results = phaserms_results
        self.phaserms_antout = ",".join(phaserms_antout)

    def merge_with_context(self, context):
        if self.vis is None:
            LOG.error(' No results to merge ')
            return

        if not self.phaseup_result.final:
            LOG.error(' No results to merge ')
            return

        # PIPE-629: if requested, unregister previous spwphaseup caltables from
        # the context before merging in the newly derived caltable.
        if self.unregister_existing:
            # Identify the MS to process
            vis: str = os.path.basename(self.vis)

            # predicate function that triggers when the spwphaseup caltable is
            # detected for this MS
            def spwphaseup_matcher(calto: callibrary.CalToArgs, calfrom: callibrary.CalFrom) -> bool:
                calto_vis = {os.path.basename(v) for v in calto.vis}
                do_delete = 'hifa_spwphaseup' in calfrom.gaintable and vis in calto_vis
                if do_delete:
                    LOG.info(f'Unregistering previous spwphaseup tables for {vis}')
                return do_delete

            context.callibrary.unregister_calibrations(spwphaseup_matcher)

        # Merge the spw phaseup offset table
        self.phaseup_result.merge_with_context(context)

        ms = context.observing_run.get_ms(name=self.vis)
        if ms:
            # Merge the spectral window mappings and the list of spws whose
            # combined SNR does not meet the threshold.
            ms.spwmaps = self.spwmaps

            # Merge the phase calibrator mapping.
            ms.phasecal_mapping = self.phasecal_mapping

    def __repr__(self):
        if self.vis is None or not self.phaseup_result:
            return ('SpwPhaseupResults:\n'
                    '\tNo spw phaseup table computed')
        else:
            s = f'SpwPhaseupResults:\nvis={self.vis}\n'
            for (intent, field), spwmapping in self.spwmaps.items():
                s += f'\t{intent}, {field}:\n'
                s += f'\t\tCombine = {spwmapping.combine}\n'
                s += f'\t\tSpwmap = {spwmapping.spwmap}\n'
                if spwmapping.combine:
                    s += f'\t\tLow combined phase SNR spws = {spwmapping.low_combinedsnr_spws}'
            return s
