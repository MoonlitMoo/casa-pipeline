import collections
import contextlib
import operator
import os
import uuid
from functools import reduce
from typing import List, Set, Tuple, Union

import numpy as np
import scipy.stats as stats

import pipeline.domain as domain
import pipeline.domain.measures as measures
import pipeline.extern.adopted as adopted
import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.basetask as basetask
import pipeline.infrastructure.callibrary as callibrary
import pipeline.infrastructure.sessionutils as sessionutils
import pipeline.infrastructure.vdp as vdp
from pipeline.domain import FluxMeasurement
from pipeline.domain import MeasurementSet
from pipeline.h.tasks.common import commonfluxresults, mstools
from pipeline.h.tasks.flagging.flagdatasetter import FlagdataSetter
from pipeline.hif.tasks import applycal
from pipeline.hif.tasks import gaincal
from pipeline.hif.tasks.fluxscale import fluxscale
from pipeline.hif.tasks.gaincal.common import GaincalResults
from pipeline.hif.tasks.setmodel import setjy
from pipeline.infrastructure import casa_tasks
from pipeline.infrastructure import casa_tools
from pipeline.infrastructure import exceptions
from pipeline.infrastructure import task_registry
from . import fluxes
from ... import heuristics

__all__ = [
    'GcorFluxscale',
    'GcorFluxscaleInputs',
    'GcorFluxscaleResults',
    'SessionGcorFluxscale',
    'SessionGcorFluxscaleInputs'
]

LOG = infrastructure.get_logger(__name__)

ORIGIN = 'gcorfluxscale'


class GcorFluxscaleResults(commonfluxresults.FluxCalibrationResults):
    def __init__(self, vis, resantenna=None, uvrange=None, measurements=None, fluxscale_measurements=None,
                 applies_adopted=False, ampcal_flagcmds=None):
        super(GcorFluxscaleResults, self).__init__(vis, resantenna=resantenna, uvrange=uvrange,
                                                   measurements=measurements)
        self.applies_adopted = applies_adopted

        # PIPE-2155: to flagging commands for amplitude caltable.
        if ampcal_flagcmds is None:
            ampcal_flagcmds = {}
        self.ampcal_flagcmds = ampcal_flagcmds

        # To store the fluxscale derived flux measurements:
        if fluxscale_measurements is None:
            fluxscale_measurements = collections.defaultdict(list)
        self.fluxscale_measurements = fluxscale_measurements

        self.calapps_for_check_sources = [] 

    def merge_with_context(self, context):
        # Update the measurement set with the calibrated visibility based flux
        # measurements for later use in imaging (PIPE-644, PIPE-660).
        ms = context.observing_run.get_ms(self.vis)
        ms.derived_fluxes = self.measurements

        # Update the measurement set with the fluxscale derived flux
        # measurements for later use in polarisation calibration (PIPE-1776).
        ms.fluxscale_fluxes = self.fluxscale_measurements

        # Store these calapps in the context so that they can be plotted in hifa_timegaincal's
        # diagnostic phase vs. time plots. See PIPE-1377 for more information.
        ms.phase_calapps_for_check_sources = self.calapps_for_check_sources


class GcorFluxscaleInputs(fluxscale.FluxscaleInputs):
    amp_outlier_sigma = vdp.VisDependentProperty(default=50.0)
    antenna = vdp.VisDependentProperty(default='')
    hm_resolvedcals = vdp.VisDependentProperty(default='automatic')
    minsnr = vdp.VisDependentProperty(default=2.0)
    peak_fraction = vdp.VisDependentProperty(default=0.2)
    phaseupsolint = vdp.VisDependentProperty(default='int')
    refant = vdp.VisDependentProperty(default='')

    @vdp.VisDependentProperty
    def reffile(self):
        return os.path.join(self.context.output_dir, 'flux.csv')

    @vdp.VisDependentProperty
    def refspwmap(self):
        return []

    solint = vdp.VisDependentProperty(default='inf')
    # Include polarisation (PIPE-599) and diffgain (PIPE-2083) intent in the
    # transfer intent.
    transintent = vdp.VisDependentProperty(default='PHASE,BANDPASS,CHECK,DIFFGAINREF,DIFFGAINSRC,POLARIZATION,POLANGLE,'
                                                   'POLLEAKAGE')
    uvrange = vdp.VisDependentProperty(default='')

    def __init__(self, context, output_dir=None, vis=None, caltable=None, fluxtable=None, reffile=None, reference=None,
                 transfer=None, refspwmap=None, refintent=None, transintent=None, solint=None, phaseupsolint=None,
                 minsnr=None, refant=None, hm_resolvedcals=None, antenna=None, uvrange=None, peak_fraction=None,
                 amp_outlier_sigma=None):
        super(GcorFluxscaleInputs, self).__init__(context, output_dir=output_dir, vis=vis, caltable=caltable,
                                                  fluxtable=fluxtable, reference=reference, transfer=transfer,
                                                  refspwmap=refspwmap, refintent=refintent, transintent=transintent)
        self.reffile = reffile
        self.solint = solint
        self.phaseupsolint = phaseupsolint
        self.minsnr = minsnr
        self.refant = refant
        self.hm_resolvedcals = hm_resolvedcals
        self.antenna = antenna
        self.uvrange = uvrange
        self.peak_fraction = peak_fraction
        self.amp_outlier_sigma = amp_outlier_sigma


@task_registry.set_equivalent_casa_task('hifa_gfluxscale')
@task_registry.set_casa_commands_comment(
    'The absolute flux calibration is transferred to secondary calibrator sources.'
)
class GcorFluxscale(basetask.StandardTaskTemplate):
    Inputs = GcorFluxscaleInputs

    def __init__(self, inputs):
        super(GcorFluxscale, self).__init__(inputs)

    def prepare(self, **parameters):
        inputs = self.inputs

        # Initialize results.
        result = GcorFluxscaleResults(inputs.vis, resantenna='', uvrange='')

        # If this measurement set does not have an amplitude calibrator, then
        # log an error and return early with empty result.
        if inputs.reference == '':
            LOG.error('%s has no data with reference intent %s' % (inputs.ms.basename, inputs.refintent))
            return result

        # Run setjy for sources in the reference list which have transfer intents.
        if inputs.ms.get_fields(inputs.reference, intent=inputs.transintent):
            self._do_setjy(reffile=inputs.reffile, field=inputs.reference)
        else:
            LOG.info('Flux calibrator field(s) {!r} in {!s} have no data with '
                     'intent {!s}'.format(inputs.reference, inputs.ms.basename, inputs.transintent))

        # Get reference antenna.
        refant = self._get_refant()

        # Get reference spectral window map for flux scaling.
        refspwmap = self._get_refspwmap()

        # Evaluate heuristics for resolved sources to determine which antennae
        # should be used in subsequent gaincals.
        allantenna, filtered_refant, minblperant, resantenna, uvrange = self._derive_ants_to_use(refant)
        result.resantenna = resantenna
        result.uvrange = uvrange

        # Create the phase caltables and merge into the local context.
        phase_results = self._do_phasecals(allantenna, resantenna, filtered_refant, minblperant, uvrange)

        # PIPE-1377: get list of CHECK source CalApps and store in final
        # result. These are currently used in hifa_timegaincal's diagnostic
        # phase vs. time plots.
        result.calapps_for_check_sources = self._extract_calapps_for_check(phase_results)

        # Now do the amplitude-only gaincal. This will produce the caltable
        # that fluxscale will analyse.
        ampcal_result, caltable, check_ok = self._do_ampcal(allantenna, filtered_refant, minblperant)

        # If no valid amplitude caltable is available to analyse, then log an
        # error and return early.
        if not check_ok:
            LOG.error('Unable to complete flux scaling operation for MS %s' % (os.path.basename(inputs.vis)))
            return result

        # PIPE-2155: flag outliers in the amplitude caltable and store the
        # resulting flagging commands in the result.
        result.ampcal_flagcmds[caltable] = self._flag_ampcal(caltable)

        # PIPE-644: derive both fluxscale-based scaling factors, as well as
        # calibrated visibility fluxes.
        try:
            # Derive fluxscale-based flux measurements, and store in the result
            # for reporting in weblog.
            fluxscale_result = self._derive_fluxscale_flux(caltable, refspwmap)
            result.fluxscale_measurements.update(fluxscale_result.measurements)

            # Computing calibrated visibility fluxes will require a temporary
            # applycal, which is performed as part of "_derive_calvis_flux()"
            # below. To prepare for this temporary applycal, first update the
            # callibrary in the local context to replace the amplitude caltable
            # produced earlier (which used a default flux density of 1.0 Jy)
            # with the caltable produced by fluxscale, which contains
            # amplitudes set according to the derived flux values.
            self._replace_amplitude_caltable(ampcal_result, fluxscale_result)

            # Derive calibrated visibility based flux measurements
            # and store in result for reporting in weblog and merging into
            # context (into measurement set).
            calvis_fluxes = self._derive_calvis_flux()
            result.measurements.update(calvis_fluxes.measurements)
        except Exception as e:
            # Something has gone wrong, return an empty result
            LOG.error('Unable to complete flux scaling operation for MS {}'.format(inputs.ms.basename))
            LOG.exception('Flux scaling error', exc_info=e)

        return result

    def analyse(self, result):
        return result

    @staticmethod
    def _check_caltable(caltable: str, ms: MeasurementSet, reference: str, transfer: str):
        """
        Check that the given caltable is well-formed so that a 'fluxscale' will
        run successfully on it, by checking that the caltable contains results
        for the reference and transfer field(s). Log a warning if fields are
        missing.

        Args:
            caltable: path to caltable to evaluate
            ms: MeasurementSet domain object
            reference: string with name(s) of reference field(s)
            transfer: string with names of transfer fields
        """
        # Get the ids of the reference source and transfer calibrator source(s).
        ref_fieldid = {field.id for field in ms.fields if field.name in reference.split(',')}
        transfer_fieldids = {field.id for field in ms.fields if field.name in transfer.split(',')}

        # Get field IDs in caltable.
        with casa_tools.TableReader(caltable) as table:
            fieldids = table.getcol('FIELD_ID')

        # Warn if field IDs in caltable do not include the reference and transfer sources.
        fieldids = set(fieldids)
        if fieldids.isdisjoint(ref_fieldid):
            LOG.warning('%s contains ambiguous reference calibrator field names' % os.path.basename(caltable))
        if not fieldids.issuperset(transfer_fieldids):
            LOG.warning('%s does not contain results for all transfer calibrators' % os.path.basename(caltable))

    def _derive_ants_to_use(self, refant):
        inputs = self.inputs

        # Resolved source heuristics.
        #    Needs improvement if users start specifying the input antennas.
        #    For the time being force minblperant to be 2 instead of None to
        #    avoid ACA and Tsys flagging issues.
        allantenna = inputs.antenna
        minblperant = 2

        if inputs.hm_resolvedcals == 'automatic':

            # Get the antennas to be used in the gaincals, limiting
            # the range if the reference calibrator is resolved.
            refant0 = refant.split(',')[0]  # use the first refant
            resantenna, uvrange = heuristics.fluxscale.antenna(ms=inputs.ms, refsource=inputs.reference, refant=refant0,
                                                               peak_frac=inputs.peak_fraction)

            # Do nothing if the source is unresolved.
            # If the source is resolved but the number of
            # antennas equals the total number of antennas
            # use all the antennas but pass along the uvrange
            # limit.
            if resantenna == '' and uvrange == '':
                pass
            else:
                nant = len(inputs.ms.antennas)
                nresant = len(resantenna.split(','))
                if nresant >= nant:
                    resantenna = allantenna
        else:
            resantenna = allantenna
            uvrange = inputs.uvrange

        # Do a phase-only gaincal on the flux calibrator using a
        # restricted set of antennas
        if resantenna == '':
            filtered_refant = refant
        else:  # filter refant if resolved calibrator or antenna selection
            resant_list = resantenna.rstrip('&').split(',')
            filtered_refant = str(',').join([ant for ant in refant.split(',') if ant in resant_list])

        return allantenna, filtered_refant, minblperant, resantenna, uvrange

    def _derive_calvis_flux(self):
        """
        Derive calibrated visibility fluxes.

        To compute the "calibrated" fluxes, this method will "temporarily"
        apply the existing calibration tables, including the new phase and
        amplitude caltables created earlier during the hifa_gfluxscale task.

        First, create a back-up of the MS flagging state, then run an applycal
        for the necessary intents and fields. Next, compute the calibrated
        visibility fluxes. Finally, always restore the back-up of the MS
        flagging state, to undo any flags that were propagated from the applied
        caltables.

        :return: commonfluxresults.FluxCalibrationResults containing the
        calibrated visibility fluxes and uncertainties.
        """
        inputs = self.inputs

        # Identify fields and spws to derive calibrated vis for.
        transfer_fields = inputs.ms.get_fields(task_arg=inputs.transfer)
        sci_spws = set(inputs.ms.get_spectral_windows(science_windows_only=True))
        transfer_fieldids = {str(field.id) for field in transfer_fields}
        spw_ids = {str(spw.id) for field in transfer_fields for spw in field.valid_spws.intersection(sci_spws)}

        # Create back-up of MS flagging state.
        LOG.info('Creating back-up of flagging state')
        flag_backup_name = 'before_gfluxscale_calvis'
        task = casa_tasks.flagmanager(vis=inputs.vis, mode='save', versionname=flag_backup_name)
        self._executor.execute(task)

        # Run computation of calibrated visibility fluxes in try/finally to
        # ensure that the MS are always restored, even in case of an exception.
        try:
            # Apply all caltables registered in the callibrary in the local
            # context to the MS.
            LOG.info('Applying pre-existing caltables and preliminary phase-up and amplitude caltables.')
            acinputs = applycal.IFApplycalInputs(context=inputs.context, vis=inputs.vis, field=inputs.transfer,
                                                 intent=inputs.transintent, flagsum=False, flagbackup=False)
            actask = applycal.SerialIFApplycal(acinputs)
            self._executor.execute(actask)

            # Initialize result.
            result = commonfluxresults.FluxCalibrationResults(inputs.vis)

            # Compute the mean calibrated visibility flux for each field and
            # spw and add as flux measurement to the final result.
            for fieldid in transfer_fieldids:
                for spwid in spw_ids:
                    mean_flux, std_flux = mstools.compute_mean_flux(self.inputs.ms, fieldid, spwid, self.inputs.transintent)
                    if mean_flux:
                        flux = domain.FluxMeasurement(spwid, mean_flux, origin=ORIGIN)
                        flux.uncertainty = domain.FluxMeasurement(spwid, std_flux, origin=ORIGIN)
                        result.measurements[fieldid].append(flux)
        finally:
            # Restore the MS flagging state.
            LOG.info('Restoring back-up of flagging state.')
            task = casa_tasks.flagmanager(vis=inputs.vis, mode='restore', versionname=flag_backup_name)
            self._executor.execute(task)

        return result

    def _derive_fluxscale_flux(self, caltable, refspwmap):
        inputs = self.inputs

        # Schedule a fluxscale job using this caltable. This is the result
        # that contains the flux measurements for the context.
        # We need to write the fluxscale-derived flux densities to a file,
        # which can then be used as input for the subsequent setjy task.
        # This is the name of that file.
        # use UUID so that parallel MPI processes do not unlink the same file
        reffile = os.path.join(inputs.context.output_dir, 'fluxscale_{!s}.csv'.format(uuid.uuid4()))
        try:
            fluxscale_result = self._do_fluxscale(caltable, refspwmap=refspwmap)

            # Determine fields ids for which a model spix should be
            # set along with the derived flux. For now this is
            # restricted to BANDPASS fields
            fieldids_with_spix = [str(f.id) for f in inputs.ms.get_fields(task_arg=inputs.transfer, intent='BANDPASS')]

            # Store the results in a temporary file.
            fluxes.export_flux_from_fit_result(fluxscale_result, inputs.context, reffile,
                                               fieldids_with_spix=fieldids_with_spix)

            # Finally, do a setjy, add its setjy_settings
            # to the main result
            self._do_setjy(reffile=reffile, field=inputs.transfer)

            # Use the fluxscale measurements to get the uncertainties too.
            # This makes the (big) assumption that setjy executed exactly
            # what we passed in as arguments.
        finally:
            # clean up temporary file
            if os.path.exists(reffile):
                os.remove(reffile)

        return fluxscale_result

    def _do_ampcal(self, antenna: str, refant: str, minblperant: int) -> Tuple[Union[None, GaincalResults], str, bool]:
        inputs = self.inputs

        ampcal_result = None
        check_ok = False
        try:
            # Create amplitude gain solutions and merge into the local context,
            # so that these amplitude solutions will be used in a temporary
            # applycal when deriving calibrated visibility fluxes.
            ampcal_result = self._do_gaincal(
                field=f'{inputs.transfer},{inputs.reference}', intent=f'{inputs.transintent},{inputs.refintent}',
                gaintype='T', calmode='a', combine='', solint=inputs.solint, antenna=antenna, uvrange='',
                minsnr=inputs.minsnr, refant=refant, minblperant=minblperant, spwmap=None, interp=None,
                append=False)
            ampcal_result.accept(inputs.context)

            # Get the gaincal caltable from the results
            try:
                caltable = ampcal_result.final.pop().gaintable
            except:
                caltable = ' %s' % ampcal_result.error.pop().gaintable if ampcal_result.error else ''
                LOG.warning(f'Cannot compute compute the flux scaling table{os.path.basename(caltable)}')

            # Check that the caltable exists and contains data for the
            # reference and transfer fields.
            if os.path.exists(caltable):
                self._check_caltable(caltable=caltable, ms=inputs.ms, reference=inputs.reference,
                                     transfer=inputs.transfer)
                check_ok = True
        except:
            # Try to fetch caltable name from ampcal result.
            caltable = ' %s' % ampcal_result.error.pop().gaintable if (ampcal_result and ampcal_result.error) else ''
            LOG.warning(f'Cannot compute phase solution table{os.path.basename(caltable)} for the phase and bandpass'
                        f' calibrator')

        return ampcal_result, caltable, check_ok

    def _do_gaincal(self, caltable=None, field=None, intent=None, gaintype='G', calmode=None, combine=None, solint=None,
                    antenna=None, uvrange='', minsnr=None, refant=None, minblperant=None, spwmap=None, interp=None,
                    append=False):
        inputs = self.inputs

        # Use only valid science spws
        fieldlist = inputs.ms.get_fields(task_arg=field)
        sci_spws = set(inputs.ms.get_spectral_windows(science_windows_only=True))
        spw_ids = {spw.id for fld in fieldlist for spw in fld.valid_spws.intersection(sci_spws)}
        spw_ids = ','.join(map(str, spw_ids))

        # Initialize gaincal task inputs.
        task_args = {
            'output_dir': inputs.output_dir,
            'vis': inputs.vis,
            'caltable': caltable,
            'field': field,
            'intent': intent,
            'spw': spw_ids,
            'solint': solint,
            'gaintype': gaintype,
            'calmode': calmode,
            'minsnr': minsnr,
            'combine': combine,
            'refant': refant,
            'antenna': antenna,
            'uvrange': uvrange,
            'minblperant': minblperant,
            'solnorm': False,
            'append': append
        }
        task_inputs = gaincal.GTypeGaincal.Inputs(inputs.context, **task_args)

        # Initialize and execute gaincal task.
        task = gaincal.GTypeGaincal(task_inputs)
        result = self._executor.execute(task)

        # Define what overrides should be included in the cal application.
        # Add overrides for field, interpolation, and SpW mapping if
        # provided.
        calapp_overrides = {}

        # Phase solution caltables should be registered with
        # calwt=False (PIPE-1154).
        if calmode == 'p':
            calapp_overrides['calwt'] = False

        # Adjust the field if provided.
        if field:
            calapp_overrides['field'] = field

        # Adjust the intent if provided.
        if intent:
            calapp_overrides['intent'] = intent

        # Adjust the interp if provided.
        if interp:
            calapp_overrides['interp'] = interp

        # Adjust the spw map if provided.
        if spwmap:
            calapp_overrides['spwmap'] = spwmap

        # If a caltable was created and any overrides are necessary, then
        # create a modified CalApplication and replace CalApp in result with
        # this new one.
        if result.pool and calapp_overrides:
            original_calapp = result.pool[0]
            modified_calapp = callibrary.copy_calapplication(original_calapp, **calapp_overrides)
            result.pool[0] = modified_calapp
            result.final[0] = modified_calapp

        return result

    def _do_phasecals(self, all_ants: str, restr_ants: str, refant: str, minblperant: int,
                      uvrange: str) -> List[GaincalResults]:
        # Collect phase cal results for merging into context.
        phase_results = []

        # Identify unique transfer intents, whether PHASE/CHECK are present
        # among the transfer intents, and what non-PHASE/CHECK intents are
        # present.
        trans_intents = set(self.inputs.transintent.split(','))
        pc_intents = {'CHECK', 'PHASE'} & trans_intents
        non_pc_intents = trans_intents - pc_intents

        # PIPE-1154: identify set of all calibrator intents that are not PHASE
        # / CHECK; these impact which fields are in subsequent phase
        # calibrations.
        amp_intent = set(self.inputs.refintent.split(','))
        exclude_intents = amp_intent | non_pc_intents

        # Compute phase caltable for the flux calibrator, using restricted set
        # of antennas.
        phase_results.append(self._do_phasecal_for_amp_calibrator(restr_ants, refant, minblperant, uvrange,
                                                                  non_pc_intents))

        # PIPE-1154: compute phase caltable(s) with optimal parameters for
        # PHASE and/or CHECK fields that do not cover any of the other
        # calibrator intents.
        phase_results.extend(self._do_phase_for_phase_check_no_overlap(pc_intents, exclude_intents, all_ants, refant))

        # PIPE-1490: for PHASE fields that do cover other calibrator intents,
        # create a separate solve.
        phase_results.extend(self._do_phase_for_phase_with_overlap(exclude_intents, all_ants, refant))

        # PIPE-1154: for the remaining calibrator intents, compute phase
        # solutions using full set of antennas.
        if non_pc_intents:
            phase_results.extend(self._do_phasecal_for_other_calibrators(non_pc_intents, all_ants, refant))

        # Accept all phase cal results into the local context to register the
        # newly created phase caltables in the callibrary of the local context,
        # so that these phase solutions will be used in pre-apply during
        # upcoming amplitude solves.
        for result in phase_results:
            result.accept(self.inputs.context)

        return phase_results

    @staticmethod
    def _extract_calapps_for_check(gaincal_results: List[GaincalResults]) -> List:
        # Extract list of CalApps for any gaincal result where intent was
        # CHECK.
        calapps = []
        for result in gaincal_results:
            if result.inputs['intent'] == 'CHECK':
                calapps.extend(result.final)
        return calapps

    @staticmethod
    def _get_intent_field(ms: MeasurementSet, intents: Set, exclude_intents: Set = None) -> List[Tuple[str, str]]:
        if exclude_intents is None:
            exclude_intents = set()

        # PIPE-1493: only collect unique combinations of field name and intent.
        # This means that if multiple fields with different IDs have the same
        # name, then these will appear only once in the list of intents-fields
        # to process. This assumes that there is no scenario where there are
        # legitimately multiple different field IDs that have the same name,
        # and that should be processed separately.
        intent_field = set()
        for intent in intents:
            for field in ms.get_fields(intent=intent):
                # Check whether found field also covers any of the intents to
                # skip.
                excluded_intents_found = field.intents.intersection(exclude_intents)
                if not excluded_intents_found:
                    intent_field.add((intent, field.name))
                else:
                    # Log a message to explain why no phase caltable will be
                    # derived for this particular combination of field and
                    # intent.
                    excluded_intents_str = ", ".join(sorted(excluded_intents_found))
                    LOG.debug(f'{ms.basename}: will not derive phase calibration for field {field.name} (#{field.id})'
                              f' and intent {intent} because this field also covers calibrator intent(s)'
                              f' {excluded_intents_str}')

        return sorted(intent_field)

    def _do_phasecal_for_amp_calibrator(self, antenna: str, refant: str, minblperant: int,
                                        uvrange: str, non_pc_intents: Set) -> GaincalResults:
        inputs = self.inputs

        # Compute phase caltable for the amplitude calibrator (set by
        # "refintent"). PIPE-1154: for amplitude calibrator, always use
        # combine='' and gaintype="G", no spwmap or interp.
        LOG.info(f'Compute phase gaincal table for flux calibrator (intent={inputs.refintent},'
                 f' field={inputs.reference}).')
        phase_result = self._do_gaincal(field=inputs.reference, intent=inputs.refintent, gaintype="G", calmode='p',
                                        combine='', solint=inputs.phaseupsolint, antenna=antenna, uvrange=uvrange,
                                        minsnr=inputs.minsnr, refant=refant, minblperant=minblperant, spwmap=None,
                                        interp=None)
        # PIPE-1831: update the CalApplication to add the other non-phase/check
        # calibrator intents as valid intents that this phase caltable can be
        # applied to. This is necessary for datasets where a field that covers
        # both the amplitude calibrator and other (non-phase/check)
        # calibrators. Without this, any subsequent gaincal on this field would
        # split the call for this field into separate ones for AMP and other
        # intents, leading to undesired duplicate solutions.
        if phase_result.pool:
            original_calapp = phase_result.pool[0]
            intents_str = ",".join({original_calapp.intent} | non_pc_intents)
            modified_calapp = callibrary.copy_calapplication(original_calapp, intent=intents_str)
            phase_result.pool[0] = modified_calapp
            phase_result.final[0] = modified_calapp

        return phase_result

    def _do_phase_for_phase_check_no_overlap(self, pc_intents: Set, exclude_intents: Set, antenna: str,
                                             refant: str) -> List[GaincalResults]:
        # Collect phase cal results.
        phase_results = []

        # PIPE-1154: identify which fields covered the PHASE calibrator and/or
        # CHECK source intent while not also covering one of the other
        # calibrator intents (typically AMPLITUDE, BANDPASS, DIFFGAIN*, POL*).
        # For these fields, derive separate phase solutions for each combination
        # of intent, field, and use optimal gaincal parameters based on
        # spwmapping registered in the measurement set.
        intent_field_to_assess = self._get_intent_field(self.inputs.ms, intents=pc_intents,
                                                        exclude_intents=exclude_intents)

        for intent, field in intent_field_to_assess:
            phase_results.append(self._do_phasecal_for_intent_field(intent, field, antenna, refant))

        return phase_results

    def _do_phase_for_phase_with_overlap(self, exclude_intents: Set, antenna: str, refant: str) -> List[GaincalResults]:
        # Collect phase cal results.
        phase_results = []

        # PIPE-1154, PIPE-1490: identify which fields cover the PHASE
        # calibrator while also covering one of the other calibrator intents.
        # For these fields, derive phase solutions in a separate caltable.
        intent_field = set()
        for field in self.inputs.ms.get_fields(intent="PHASE"):
            if field.intents.intersection(exclude_intents):
                intent_field.add(("PHASE", field.name))

        # Run phase gaincal for selected PHASE fields.
        for intent, field in intent_field:
            phase_results.append(self._do_phasecal_for_intent_field(intent, field, antenna, refant))

        return phase_results

    def _do_phasecal_for_intent_field(self, intent: str, field: str, antenna: str, refant: str) -> GaincalResults:
        # Get optimal phase solution parameters for current intent and
        # field, based on spw mapping info in MS.
        combine, gaintype, interp, solint, spwmap = self._get_phasecal_params(intent, field)

        # Create phase caltable and merge it into the local context so that the
        # caltable is included in pre-apply for subsequent gaincal.
        LOG.info(f'Compute phase gaincal table for intent={intent}, field={field}.')
        result = self._do_gaincal(field=field, intent=intent, gaintype=gaintype, calmode='p', combine=combine,
                                  solint=solint, antenna=antenna, uvrange='', minsnr=self.inputs.minsnr, refant=refant,
                                  spwmap=spwmap, interp=interp)

        return result

    def _do_phasecal_for_other_calibrators(self, intent: Set, antenna: str, refant: str) -> List[GaincalResults]:
        inputs = self.inputs
        phase_results = []

        # Identify fields that cover the provided intents. Note that
        # inputs.transfer will skip any field that already covers the intent
        # inputs.refintent (the amplitude calibrator), as that is covered by a
        # separate gaincal.
        fields = ",".join(f.name for f in inputs.ms.get_fields(inputs.transfer)
                          if set(intent).intersection(set(f.intents)))

        # Only proceed if valid fields were found.
        if fields:
            # Intents as string for CASA input.
            intents_str = ",".join(intent)

            # PIPE-1154: the phase solves for the remaining calibrators should
            # always use combine='', gaintype="G", no spwmap or interp.
            LOG.info(f'Compute phase gaincal table for other calibrators (intent={intents_str}, field={fields}).')
            phase_results.append(self._do_gaincal(field=fields, intent=intents_str, gaintype='G', calmode='p',
                                                  combine='', solint=inputs.phaseupsolint, antenna=antenna, uvrange='',
                                                  minsnr=inputs.minsnr, refant=refant, minblperant=None, spwmap=None,
                                                  interp=None))
        return phase_results

    def _get_phasecal_params(self, intent: str, field: str):
        inputs = self.inputs
        ms = inputs.ms

        # By default, no spw mapping or combining, no interp, gaintype='G', and
        # using input solint.
        combine = ''
        gaintype = 'G'
        interp = None
        solint = inputs.phaseupsolint
        spwmap = []

        # Try to fetch spwmapping info from MS for requested intent and field.
        spwmapping = ms.spwmaps.get((intent, field), None)

        # If a mapping was found, use the spwmap, and update combine and interp
        # depending on whether it is a combine spw mapping.
        if spwmapping:
            spwmap = spwmapping.spwmap

            # If the spwmap is for combining spws, then override combine,
            # interp, and gaintype accordingly, and compute an optimal solint.
            if spwmapping.combine:
                combine = 'spw'
                gaintype = 'T'
                interp = 'linearPD,linear'

                # Compute optimal solint.
                spwidlist = [spw.id for spw in ms.get_spectral_windows(science_windows_only=True)]
                exptimes = heuristics.exptimes.get_scan_exptimes(ms, [field], intent, spwidlist)
                solint = '%0.3fs' % (min([exptime[1] for exptime in exptimes]) / 4.0)
            else:
                # PIPE-1154: when using a phase up spw mapping, ensure that
                # interp = 'linear,linear'; though this may need to be changed
                # in the future, see PIPEREQ-85.
                interp = 'linear,linear'

        return combine, gaintype, interp, solint, spwmap

    def _do_fluxscale(self, caltable=None, refspwmap=None):
        inputs = self.inputs

        task_args = {
            'output_dir': inputs.output_dir,
            'vis': inputs.vis,
            'caltable': caltable,
            'reference': inputs.reference,
            'transfer': inputs.transfer,
            'refspwmap': refspwmap
        }

        task_inputs = fluxscale.Fluxscale.Inputs(inputs.context, **task_args)
        task = fluxscale.Fluxscale(task_inputs)

        return self._executor.execute(task, merge=True)

    def _get_refant(self):
        inputs = self.inputs

        # By default, use reference antenna specified by task inputs.
        refant = inputs.refant

        # If no refant is provided by task inputs, get the reference antenna
        # for this measurement set from the context.then fetch refant
        # for this measurement set from the context.
        if refant == '':
            # Get refant from ms in inputs. This comes back as a string
            # containing a ranked list of antenna names.
            refant = inputs.ms.reference_antenna

            # If no reference antenna was found in the context for this measurement
            # (refant equals either None or an empty string), then raise an exception.
            if not (refant and refant.strip()):
                msg = ('No reference antenna specified and none found in context for %s' % inputs.ms.basename)
                LOG.error(msg)
                raise exceptions.PipelineException(msg)

        LOG.trace('refant: %s' % refant)

        return refant

    def _get_refspwmap(self):
        inputs = self.inputs

        # By default, use reference antenna specified by task inputs.
        refspwmap = inputs.refspwmap

        # If no ref spwmap is provided by task inputs, then try to get it from
        # the context for this measurement set.
        if not refspwmap:
            refspwmap = inputs.ms.reference_spwmap

            # If not valid reference spwmap was defined, then return a map
            # that signifies no mapping.
            if not refspwmap:
                refspwmap = [-1]

        return refspwmap

    def _do_setjy(self, reffile=None, field=None):
        inputs = self.inputs

        task_args = {
            'output_dir': inputs.output_dir,
            'vis': inputs.vis,
            'field': field,
            'intent': inputs.transintent,
            'reffile': reffile
        }

        task_inputs = setjy.Setjy.Inputs(inputs.context, **task_args)
        task = setjy.Setjy(task_inputs)

        return self._executor.execute(task, merge=True)

    def _replace_amplitude_caltable(self, ampresult, fsresult):
        inputs = self.inputs

        # Identify the MS to process.
        vis = os.path.basename(inputs.vis)

        # predicate function to match hifa_gfluxscale amplitude caltable for this MS
        def gfluxscale_amp_matcher(calto: callibrary.CalToArgs, calfrom: callibrary.CalFrom) -> bool:
            calto_vis = {os.path.basename(v) for v in calto.vis}

            # Standard caltable filenames contain task identifiers,
            # caltable type identifiers, etc. We can use this to identify
            # caltables created by this task. As an extra check we also
            # check the caltable type.
            do_delete = 'hifa_gfluxscale' in calfrom.gaintable and 'gaincal' in calfrom.caltype and vis in calto_vis \
                and 'gacal' in calfrom.gaintable

            if do_delete:
                LOG.debug(f'Unregistering previous amplitude calibrations for {vis}')
            return do_delete

        inputs.context.callibrary.unregister_calibrations(gfluxscale_amp_matcher)

        # Add caltable from fluxscale result to local context callibrary.
        orig_calapp = ampresult.pool[0]
        new_calapp = callibrary.copy_calapplication(orig_calapp, gaintable=fsresult.inputs['fluxtable'])
        LOG.debug(f'Adding calibration to callibrary:\n{new_calapp.calto}\n{new_calapp.calfrom}')
        inputs.context.callibrary.add(new_calapp.calto, new_calapp.calfrom)

    def _flag_ampcal(self, caltable: str) -> List[str]:
        # Get fields and SpWs to evaluate.
        fields = self.inputs.ms.get_fields(name=','.join([self.inputs.transfer, self.inputs.reference]))
        scispws = self.inputs.ms.get_spectral_windows()

        # Create an antenna id-to-name translation dictionary.
        antenna_id_to_name = {ant.id: ant.name for ant in self.inputs.ms.antennas if ant.name.strip()}

        # Evaluate each field separately.
        flagcmds = []
        for field in fields:
            # Retrieve unflagged amplitudes for current field from amplitude
            # caltable.
            with casa_tools.TableReader(caltable) as table:
                with contextlib.closing(table.query(f"FIELD_ID == {field.id}")) as subtable:
                    idx_unflagged = np.where(subtable.getcol('FLAG') == 0)
                    amplitudes = np.abs(subtable.getcol('CPARAM')[idx_unflagged])

            # Compute median and MAD for current field.
            median = np.median(amplitudes)
            mad = adopted.MAD(amplitudes)

            # Evaluate each SpW separately.
            for spw in scispws:
                # Skip evaluation if no data are available for current field and
                # SpW; this occurs for example with BandToBand datasets.
                if spw not in field.valid_spws:
                    continue

                # Retrieve unflagged amplitudes, timestamps, and antennas for
                # current field and SpW from amplitude caltable.
                with casa_tools.TableReader(caltable) as table:
                    taql = f"SPECTRAL_WINDOW_ID == {spw.id} && FIELD_ID == {field.id}"
                    with contextlib.closing(table.query(taql)) as subtable:
                        # Identify unflagged data. Note: CPARAM and FLAG are of
                        # shape [pol, channel, row], while TIME and ANTENNA1
                        # are of shape [row], hence using 3rd index for latter.
                        flags = subtable.getcol('FLAG')
                        # If this SpW is already fully flagged, skip it.
                        if np.all(flags):
                            continue
                        idx_unflagged = np.where(flags == 0)
                        amplitudes = np.abs(subtable.getcol('CPARAM')[idx_unflagged])
                        times = subtable.getcol('TIME')[idx_unflagged[2]]
                        antennas = subtable.getcol('ANTENNA1')[idx_unflagged[2]]

                # Compute sigma and identify outliers.
                sigma = np.abs(amplitudes - median) / mad
                idx_to_flag = np.where(sigma > self.inputs.amp_outlier_sigma)[0]

                # Generate flagging commands for outlier timestamps.
                # Note: '&&&' is appended to the antenna name to restrict to
                # flagging auto-correlation baselines for that antenna, and to
                # ensure that the antenna gets flagged even if it is the refant
                # (PIPE-2155).
                for idx in idx_to_flag:
                    start = casa_tools.quanta.time(casa_tools.quanta.quantity(times[idx] - 0.5, 's'), form=['ymd'])
                    end = casa_tools.quanta.time(casa_tools.quanta.quantity(times[idx] + 0.5, 's'), form=['ymd'])
                    flagcmds.append(f"mode='manual' antenna='{antenna_id_to_name[antennas[idx]]}&&&' spw='{spw.id}'"
                                    f" field='{field.id}' timerange='{start[0]}~{end[0]}'"
                                    f" reason='QA2:gfluxscale_amp_time_sigma={sigma[idx]:.6f}'")

        # Apply flags.
        if flagcmds:
            flagsetterinputs = FlagdataSetter.Inputs(context=self.inputs.context, table=caltable, inpfile=[])
            flagsettertask = FlagdataSetter(flagsetterinputs)
            flagsettertask.flags_to_set(flagcmds)
            self._executor.execute(flagsettertask)

        # Return flag commands for rendering in weblog.
        return flagcmds


class SessionGcorFluxscaleInputs(GcorFluxscaleInputs):
    # use common implementation for parallel inputs argument
    parallel = sessionutils.parallel_inputs_impl()

    def __init__(self, context, output_dir=None, vis=None, caltable=None, fluxtable=None, reffile=None, reference=None,
                 transfer=None, refspwmap=None, refintent=None, transintent=None, solint=None, phaseupsolint=None,
                 minsnr=None, refant=None, hm_resolvedcals=None, antenna=None, uvrange=None, peak_fraction=None,
                 parallel=None):
        super(SessionGcorFluxscaleInputs, self).__init__(context, output_dir=output_dir, vis=vis, caltable=caltable,
                                                         fluxtable=fluxtable, reffile=reffile, reference=reference,
                                                         transfer=transfer, refspwmap=refspwmap, refintent=refintent,
                                                         transintent=transintent, solint=solint,
                                                         phaseupsolint=phaseupsolint, minsnr=minsnr, refant=refant,
                                                         hm_resolvedcals=hm_resolvedcals, antenna=antenna,
                                                         uvrange=uvrange, peak_fraction=peak_fraction)
        self.parallel = parallel


AMPLITUDE_MISSING = '__AMPLITUDE_MISSING__'


@task_registry.set_equivalent_casa_task('session_gfluxscale')
class SessionGcorFluxscale(basetask.StandardTaskTemplate):
    Inputs = SessionGcorFluxscaleInputs

    def __init__(self, inputs):
        super(SessionGcorFluxscale, self).__init__(inputs)

    is_multi_vis_task = True

    def prepare(self):
        inputs = self.inputs

        vis_list = sessionutils.as_list(inputs.vis)

        assessed = []
        with sessionutils.VDPTaskFactory(inputs, self._executor, GcorFluxscale) as factory:
            task_queue = [(vis, factory.get_task(vis)) for vis in vis_list]

            for (vis, (task_args, task)) in task_queue:
                # only launch jobs for MSes with amplitude calibrators.
                # The analyse() method will subsequently adopt the
                # appropriate flux calibration measurements from one of
                # the completed jobs.
                ms = inputs.context.observing_run.get_ms(vis)
                if 'AMPLITUDE' not in ms.intents:
                    assessed.append(sessionutils.VisResultTuple(vis, task_args, AMPLITUDE_MISSING))
                    continue
                try:
                    worker_result = task.get_result()
                except exceptions.PipelineException as e:
                    assessed.append(sessionutils.VisResultTuple(vis, task_args, e))
                else:
                    assessed.append(sessionutils.VisResultTuple(vis, task_args, worker_result))

        return assessed

    def analyse(self, assessed):
        # all results will be added to this object
        final_result = basetask.ResultsList()

        context = self.inputs.context

        session_groups = sessionutils.group_into_sessions(context, assessed)
        for session_id, session_results in session_groups.items():
            # we need to convert the Field ID to field name in the
            # measurements
            measurements_per_field = collect_flux_measurements(context, session_results)

            averaged = calc_averages_per_field(measurements_per_field)

            for vis, task_args, vis_result in session_results:
                if vis_result == AMPLITUDE_MISSING:
                    no_amplitude_ms = context.observing_run.get_ms(vis)

                    # find other flux calibrations for any of our fields
                    no_amplitude_field_names = {f.name for f in no_amplitude_ms.fields}
                    fields_to_adopt = no_amplitude_field_names.intersection(set(averaged.keys()))

                    if len(fields_to_adopt) == 0:
                        LOG.error('Could not find a flux calibration to adopt for '
                                  '{!s}.'.format(no_amplitude_ms.basename))
                        continue

                    LOG.info('Adopting flux calibrations for {!s}; fields: {!s}'
                             ''.format(no_amplitude_ms.basename, ', '.join(fields_to_adopt)))

                    # these are the measurements to adopt, but the spw
                    # names still need to be remapped to spw IDs for
                    # this MS
                    unmapped_adopted = {k: v for k, v in averaged.items() if k in no_amplitude_field_names}

                    mapped_adopted = map_spw_names_to_id(context, vis, unmapped_adopted)

                    fake_result = GcorFluxscaleResults(vis=vis, measurements=mapped_adopted, applies_adopted=True)
                    fake_result.inputs = task_args
                    fake_result.task = SessionGcorFluxscale

                    final_result.append(fake_result)

                elif isinstance(vis_result, Exception):
                    LOG.error('No flux calibration created for {!s}'.format(os.path.basename(vis)))

                    fake_result = GcorFluxscaleResults(vis=vis)
                    fake_result.inputs = task_args

                    final_result.append(fake_result)

                else:
                    final_result.append(vis_result)

        return final_result


def get_field_name(context, vis, identifier):
    ms = context.observing_run.get_ms(vis)
    fields = set(ms.get_fields(task_arg=identifier))
    if len(fields) != 1:
        raise KeyError('{!r} does not uniquely identify a field: ({!s} matches found)'
                       ''.format(identifier, len(fields)))
    fields = fields.pop()
    return fields.name


def collect_flux_measurements(context, vis_result_tuples):
    """
    Compile the flux measurements from a set of results into a new
    dict data structure.

    :param context: the pipeline context
    :param vis_result_tuples: the VisResultTuples to inspect
    :type vis_result_tuples: list of VisResultTuples
    :return: dict of tuples
    :rtype: dict of {str field name: (vis, spw name, flux measurement)}
    """
    d = collections.defaultdict(list)

    for vis, _, result in vis_result_tuples:
        if result == AMPLITUDE_MISSING:
            continue

        ms = context.observing_run.get_ms(vis)

        for field_id, measurements in result.measurements.items():
            field_name = get_field_name(context, vis, field_id)

            for m in measurements:
                spws = ms.get_spectral_windows(task_arg=m.spw_id)
                assert(len(spws) == 1)
                spw = spws.pop()
                d[field_name].append((vis, spw.name, m))

    return d


def calc_averages_per_field(results):
    """
    Return a compiled and averaged flux calibrations per field.

    :param results:
    :return:
    """
    averages = collections.defaultdict(list)
    for field_name, measurement_structs in results.items():
        spw_names = {spw_name for _, spw_name, _ in measurement_structs}
        for spw_name in spw_names:
            measurements_for_spw = [measurement for _, name, measurement in measurement_structs
                                    if name == spw_name]
            if len(measurements_for_spw) == 0:
                continue

            mean = reduce(operator.add, measurements_for_spw) / len(measurements_for_spw)

            # copy the uncertainty if there's only one measurement,
            # otherwise calculate the standard error of the mean.
            if len(measurements_for_spw) == 1:
                m = measurements_for_spw[0]
                unc_I = m.uncertainty.I
                unc_Q = m.uncertainty.Q
                unc_U = m.uncertainty.U
                unc_V = m.uncertainty.V
            else:
                JY = measures.FluxDensityUnits.JANSKY
                unc_I = stats.sem([float(m.I.to_units(JY)) for m in measurements_for_spw])
                unc_Q = stats.sem([float(m.Q.to_units(JY)) for m in measurements_for_spw])
                unc_U = stats.sem([float(m.U.to_units(JY)) for m in measurements_for_spw])
                unc_V = stats.sem([float(m.V.to_units(JY)) for m in measurements_for_spw])

            # floats are interpreted as Jy, so we don't need to convert
            # SEM values
            mean.uncertainty = FluxMeasurement(spw_name, unc_I, Q=unc_Q, U=unc_U, V=unc_V, origin=ORIGIN)

            averages[field_name].append((spw_name, mean))

    return averages


def map_spw_names_to_id(context, vis, field_measurements):
    """
    Copy a flux result dict, remapping the target spectral window in
    the original result to a new measurement set.

    This function makes a copy of a dict of flux calibration results
    (keys=field names, values=FluxMeasurements), remapping the spectral
    window target in the results to the corresponding spectral window
    in the target measurement set.

    :param context: pipeline context
    :param vis: name of the measurement set to remap spws to
    :param field_measurements: flux calibrations to adopt
    :type field_measurements: dict with format {str: [FluxMeasurements]}
    :return: flux results remapped to measurement set
    :rtype: dict with format {str: [FluxMeasurements]}
    """
    ms = context.observing_run.get_ms(vis)
    science_spws = ms.get_spectral_windows(science_windows_only=True)
    spw_names_to_id = {spw.name: spw.id for spw in science_spws}
    # spw names must uniquely identify a science spw, otherwise we
    # can't create a correct spw ID mapping
    assert (len(spw_names_to_id) == len(science_spws))

    d = {field_name: [copy_flux_measurement(m, spw_id=spw_names_to_id[spw_name])
                      for spw_name, m in measurements if spw_name in spw_names_to_id]
         for field_name, measurements in field_measurements.items()}

    return d


def copy_flux_measurement(source, spw_id=None, I=None, Q=None, U=None, V=None, spix=None, uI=None, uQ=None, uU=None,
                          uV=None):
    if spw_id is None:
        spw_id = source.spw_id
    if I is None:
        I = source.I
    if Q is None:
        Q = source.Q
    if U is None:
        U = source.U
    if V is None:
        V = source.V
    if spix is None:
        spix = source.spix

    new_fm = FluxMeasurement(spw_id, I, Q=Q, U=U, V=V, spix=spix, origin=ORIGIN)

    if uI is None:
        uI = source.uncertainty.I
    if uQ is None:
        uQ = source.uncertainty.Q
    if uU is None:
        uU = source.uncertainty.U
    if uV is None:
        uV = source.uncertainty.V
    new_fm.uncertainty = FluxMeasurement(spw_id, uI, Q=uQ, U=uU, V=uV, origin=ORIGIN)

    return new_fm
