import os
from typing import Dict, List, Optional, Tuple

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.callibrary as callibrary
import pipeline.infrastructure.utils as utils
import pipeline.infrastructure.vdp as vdp
from pipeline.domain.measurementset import MeasurementSet
from pipeline.hif.tasks.gaincal import common
from pipeline.hif.tasks.gaincal import gtypegaincal
from pipeline.hifa.heuristics import exptimes as gexptimes
from pipeline.hifa.heuristics.phasespwmap import combine_spwmap
from pipeline.infrastructure import task_registry

LOG = infrastructure.get_logger(__name__)

__all__ = [
    'TimeGaincalInputs',
    'TimeGaincal',
]


class TimeGaincalInputs(gtypegaincal.GTypeGaincalInputs):

    # Amplitude caltable that is to be registered in callibrary as
    # applicable to TARGET/CHECK, and to AMPLITUDE,POL*,BANDPASS intents.
    amptable = vdp.VisDependentProperty(default=None)

    # Amplitude caltable used for diagnostic amplitude plots in weblog.
    calamptable = vdp.VisDependentProperty(default=None)

    calminsnr = vdp.VisDependentProperty(default=2.0)

    # Phase caltable to be registered for the non-PHASE calibrators.
    # This is also used for diagnostic phase vs. time plots in weblog.
    calphasetable = vdp.VisDependentProperty(default=None)

    # Default solint used for calibrators.
    calsolint = vdp.VisDependentProperty(default='int')

    # Override default base class intents for ALMA.
    @vdp.VisDependentProperty
    def intent(self):
        return 'PHASE,AMPLITUDE,BANDPASS,POLARIZATION,POLANGLE,POLLEAKAGE,DIFFGAINREF,DIFFGAINSRC'

    # Used for diagnostic phase offsets plots in weblog.
    offsetstable = vdp.VisDependentProperty(default=None)

    targetminsnr = vdp.VisDependentProperty(default=3.0)

    # Used for phase caltable that is to be registered in callibrary as
    # applicable to TARGET/CHECK, and to PHASE calibrator.
    targetphasetable = vdp.VisDependentProperty(default=None)

    targetsolint = vdp.VisDependentProperty(default='inf')

    def __init__(self, context, vis=None, output_dir=None, calamptable=None, calphasetable=None, offsetstable=None,
                 amptable=None, targetphasetable=None, calsolint=None, targetsolint=None, calminsnr=None,
                 targetminsnr=None, **parameters):
        super().__init__(context, vis=vis, output_dir=output_dir,  **parameters)

        self.amptable = amptable
        self.calamptable = calamptable
        self.calminsnr = calminsnr
        self.calphasetable = calphasetable
        self.calsolint = calsolint
        self.offsetstable = offsetstable
        self.targetminsnr = targetminsnr
        self.targetphasetable = targetphasetable
        self.targetsolint = targetsolint


@task_registry.set_equivalent_casa_task('hifa_timegaincal')
@task_registry.set_casa_commands_comment('Time dependent gain calibrations are computed.')
class TimeGaincal(gtypegaincal.GTypeGaincal):
    Inputs = TimeGaincalInputs

    def prepare(self, **parameters) -> common.GaincalResults:
        inputs = self.inputs

        # Create a results object.
        result = common.GaincalResults()
        result.phasecal_for_phase_plot = []

        # Compute the phase solutions for the science target, check source,
        # and phase calibrator. This caltable will be registered as applicable
        # to the target, check source, and phase calibrator when the result for
        # this task is accepted.
        LOG.info('Computing phase gain table(s) for target(s), check source(s), and phase calibrator(s).')
        target_phasecal_calapps = self._do_phasecal_for_target()
        # Add the solutions to final task result, for adopting into context
        # callibrary, but do not merge them into local task context, so they
        # are not used in pre-apply of subsequent gaincal calls.
        result.pool.extend(target_phasecal_calapps)
        result.final.extend(target_phasecal_calapps)

        # Compute the phase solutions for all calibrators in inputs.intents.
        # These phase cal results include solutions for the PHASE calibrator
        # field(s), and will be temporarily accepted into the local context to
        # have these available as pre-apply in subsequent gaincals (both for
        # amplitude solves and for computing residual phase offsets). But for
        # the final task result, these phase solutions will only be registered
        # as applicable to the bandpass, flux, differential gain, and
        # polarization calibrators.
        LOG.info('Computing phase gain table(s) for bandpass, flux, diffgain, and polarization calibrator(s).')
        cal_phase_results, max_phase_solint = self._do_phasecal_for_calibrators()

        # Merge the phase solutions for the calibrators into the local task
        # context so that it is marked for inclusion in pre-apply (gaintable)
        # in subsequent gaincal calls during this task.
        for cpres in cal_phase_results:
            cpres.accept(inputs.context)

        # Look through calibrator phasecal results for any CalApplications for
        # caltables that are applicable to non-PHASE calibrators (i.e.
        # AMPLITUDE, BANDPASS, POL*, and DIFFGAIN*). Add these CalApps to the
        # final task result, to be merged into the final context / callibrary.
        for cpres in cal_phase_results:
            cp_calapp = cpres.final[0]
            if cp_calapp.intent != 'PHASE':
                result.final.append(cp_calapp)
                result.pool.append(cp_calapp)

            # PIPE-1377: add all results to the list to be plotted in the
            # phase vs. time diagnostic plots in the renderer.
            result.phasecal_for_phase_plot.append(cp_calapp)

        # Compute the amplitude calibration.
        LOG.info('Computing the final amplitude gain table.')
        amplitude_calapps = self._do_target_ampcal()

        # Accept the amplitude calibration into the final results.
        result.pool.extend(amplitude_calapps)
        result.final.extend(amplitude_calapps)

        # Produce the diagnostic table for displaying amplitude vs time plots.
        # For the solint, use the maximum SpW-mapping-mode-based solint that
        # was used earlier for phase solutions for the PHASE calibrator(s).
        # This table is not applied to the data, and no special mapping is
        # required here.
        LOG.info('Computing diagnostic amplitude gain table for displaying amplitude vs time plots.')
        amp_diagnostic_result = self._do_caltarget_ampcal(solint=max_phase_solint)
        result.calampresult = amp_diagnostic_result

        # To ensure that a diagnostic phase offsets caltable (to be created in
        # an upcoming step) will be sensitive to residual phase offsets, it is
        # necessary to enforce that the phase caltable for PHASE calibrator
        # fields used in pre-apply was solved with combine='spw'. But the
        # caltables for PHASE calibrator fields that earlier got solved and
        # merged into local context (for pre-apply in amplitude solves) may not
        # always have used combine='spw' (since it used optimal parameters
        # based on SpW mapping). Hence, first unregister any phase caltable for
        # PHASE calibrator fields where the SpW mapping recommended combine=''.
        LOG.info("Prior to computing diagnostic residual phase offsets, unregistering any phase gain table(s) for"
                 " phase calibrator(s) that were derived with combine='', earlier in this stage.")
        self._unregister_phasecal_with_no_combine()

        # Next, compute a new phase solutions solve for those PHASE calibrator
        # fields where the recommended combine was '', while this time
        # enforcing combine='spw'.
        LOG.info("Prior to computing diagnostic residual phase offsets, where necessary (caltable was unregistered)"
                 " re-computing phase gain table(s) for phase calibrator(s) while enforcing combine='spw'.")
        phasecal_phase_results = self._do_phasecal_for_phase_calibrators_forcing_combine()

        # Merge the new phase solutions for PHASE calibrator fields into the
        # local task context, so they are marked for inclusion in pre-apply in
        # subsequent diagnostic phase offsets solve.
        for res in phasecal_phase_results:
            res.accept(inputs.context)

        # Now compute a new phase solutions caltable while using previous phase
        # caltables in pre-apply. Assuming that inputs.intent included 'PHASE'
        # (true by default, but can be overridden by user), then the merger of
        # the previous phase solutions result into the local task context will
        # mean that the initial phase corrections will be included in pre-apply
        # during this gaincal, and thus this new caltable will represent the
        # residual phase offsets.
        LOG.info('Computing diagnostic residual phase offsets gain table.')
        phase_offsets_result = self._do_offsets_phasecal()
        result.phaseoffsetresult = phase_offsets_result

        return result

    def analyse(self, result: common.GaincalResults) -> common.GaincalResults:
        # Double-check that the caltables were actually generated.
        on_disk = [table for table in result.pool if table.exists()]
        result.final[:] = on_disk

        missing = [table for table in result.pool if table not in on_disk]
        result.error.clear()
        result.error.update(missing)

        return result

    @staticmethod
    def _get_spw_groupings(ms: MeasurementSet, spw: str, spwmap: List[int]) -> List[Tuple[int, str, str]]:
        """
        Group selected SpWs by SpectralSpec.

        Args:
            ms: MeasurementSet to query for spectral specs.
            spw: A comma separated string of SpW IDs to group together
            spwmap: List representing spectral window mapping

        Returns:
            List of tuples representing SpW groupings, containing:
              * Reference Spw ID
              * Spectral Spec ID
              * SpW IDs associated to that SpW grouping (value)
        """
        grouped_spw = []

        if len(spwmap) == 0:  # No SpW combination
            return grouped_spw

        request_spws = set(ms.get_spectral_windows(task_arg=spw))
        for sspec, spws in utils.get_spectralspec_to_spwid_map(request_spws).items():
            ref_spw = {spwmap[i] for i in spws}
            assert len(ref_spw) == 1, 'A SpectralSpec is mapped to more than one SpWs'
            grouped_spw.append((ref_spw.pop(), sspec, str(',').join(str(s) for s in sorted(spws))))

        LOG.debug(f'Spectral window grouping: {grouped_spw}')

        return grouped_spw

    def _do_phasecal_for_target(self) -> List[callibrary.CalApplication]:
        """
        This method is responsible for creating phase gain caltable(s) that
        will be applicable to the TARGET, the CHECK source, and the PHASE
        calibrator.

        Separate phase solutions are created for each PHASE field, and for each
        SpectralSpec where not using "combine" SpW mapping.

        The resulting caltable(s) will be part of the final task result, with
        separate CalApplications to register the caltable(s) to be applicable
        to PHASE, as well as to TARGET/CHECK.
        """
        inputs = self.inputs
        p_intent = 'PHASE'

        # Initialize output list of CalApplications.
        calapp_list = []

        # Determine non-phase calibrator fields, to be added to gaincal solve
        # for plotting purposes.
        np_intents = ','.join(set(inputs.intent.split(',')) - {p_intent})
        np_fields = ','.join([f.name for f in inputs.ms.get_fields(intent=np_intents)])

        # Determine which SpWs to solve for, which SpWs the solutions should
        # apply to, and whether to override refantmode. By default, use all
        # input SpW, do not restrict what SpWs the solutions apply to, and do
        # not override the refantmode.
        spw_to_solve = inputs.spw
        refantmode = None
        apply_to_spw = None
        if inputs.ms.is_band_to_band:
            # PIPE-2087: for BandToBand, restrict the solve to the diffgain
            # reference SpWs, use refantmode strict for the solve, and register
            # the solutions to be applied to the diffgain on-source SpWs.
            dg_refspws = inputs.ms.get_spectral_windows(task_arg=inputs.spw, intent='DIFFGAINREF')
            dg_srcspws = inputs.ms.get_spectral_windows(task_arg=inputs.spw, intent='DIFFGAINSRC')
            spw_to_solve = ','.join(str(s.id) for s in dg_refspws)
            refantmode = 'strict'
            apply_to_spw = ','.join(str(s.id) for s in dg_srcspws)

        # Create separate phase solutions for each PHASE field.
        for field in inputs.ms.get_fields(intent=p_intent):
            # Retrieve from MS which TARGET/CHECK fields the gain solutions for
            # the current PHASE field should be applied to.
            tc_fields = ','.join(inputs.ms.phasecal_mapping.get(field.name, {}))

            # If the user specified a filename, then add the field name, to
            # ensure the filenames remain unique in case of multiple fields.
            caltable = None
            if inputs.targetphasetable:
                root, ext = os.path.splitext(inputs.targetphasetable)
                caltable = f'{root}.{field.name}{ext}'

            # Get optimal phase solution parameters for current PHASE field,
            # based on spw mapping info in MS.
            # No need to catch the values for optimal solint or "low SNR SpWs",
            # as the solint will be fixed to inputs.targetsolint.
            combine, gaintype, interp, _, _, spwmap = self._get_phasecal_params(p_intent, field.name)

            # PIPE-2087: for BandToBand override interp, for these phase
            # solutions that will apply to the science target.
            if inputs.ms.is_band_to_band:
                interp = 'linearPD,linear'

            # PIPE-390: if not combining across spw, then no need to deal with
            # SpectralSpec, so create a gaincal solution for all SpWs, using
            # provided gaintype, spwmap, and interp.
            if not combine:
                calapp_list.extend(self._do_target_phasecal(caltable=caltable, field=field.name, spw=spw_to_solve,
                                                            gaintype=gaintype, combine=combine, spwmap=spwmap,
                                                            interp=interp, apply_to_field=tc_fields,
                                                            apply_to_spw=apply_to_spw, include_field=np_fields,
                                                            refantmode=refantmode))

            # Otherwise, a combined SpW solution is expected, and we need to
            # create separate solutions for each SpectralSpec grouping of Spws.
            else:
                # Group the input SpWs by SpectralSpec.
                spw_groups = self._get_spw_groupings(inputs.ms, spw_to_solve, spwmap)
                if not spw_groups:
                    raise ValueError('Invalid SpW grouping input.')

                # Loop through each grouping of spws.
                for _, sspec, spw_sel in spw_groups:
                    LOG.info(f'Processing spectral spec {sspec} with spws {spw_sel}')

                    # Check if there are scans for current intent and SpWs.
                    selected_scans = inputs.ms.get_scans(scan_intent=p_intent, spw=spw_sel)
                    if len(selected_scans) == 0:
                        LOG.info(f'Skipping table generation for empty selection: spw={spw_sel}, intent={p_intent}')
                        continue

                    # If an explicit output filename is defined, then add the
                    # spw selection to ensure the filenames remain unique.
                    if caltable:
                        root, ext = os.path.splitext(caltable)
                        caltable = f'{root}.{spw_sel}{ext}'

                    # Run phase calibration.
                    calapp_list.extend(self._do_target_phasecal(caltable=caltable, field=field.name, spw=spw_sel,
                                                                gaintype=gaintype, combine=combine, spwmap=spwmap,
                                                                interp=interp, apply_to_field=tc_fields,
                                                                apply_to_spw=apply_to_spw, include_field=np_fields,
                                                                refantmode=refantmode))

        return calapp_list

    def _do_target_phasecal(self, caltable: str = None, field: str = None, spw: str = None, gaintype: str = None,
                            combine: str = None, interp: str = None, spwmap: List[int] = None,
                            apply_to_field: str = None, apply_to_spw: str = None, include_field: str = None,
                            refantmode: Optional[str] = None)\
            -> List[callibrary.CalApplication]:
        """
        This runs the gaincal for creating phase solutions intended for TARGET,
        CHECK, and PHASE. The result contains two CalApplications, one for
        how to apply the caltable to the PHASE calibrator, and a second one for
        how to apply the caltable to the TARGET and CHECK source(s).
        """
        inputs = self.inputs

        # If provided, add additional fields to gaincal.
        gc_fields = field
        if include_field:
            gc_fields = f'{field},{include_field}'

        # PIPE-1154: for phase solutions of target, check, phase, always use
        # solint=inputs.targetsolint.
        task_args = {
            'output_dir': inputs.output_dir,
            'vis': inputs.vis,
            'caltable': caltable,
            'field': gc_fields,
            'intent': inputs.intent,
            'spw': spw,
            'solint': inputs.targetsolint,
            'gaintype': gaintype,
            'calmode': 'p',
            'minsnr': inputs.targetminsnr,
            'combine': combine,
            'refant': inputs.refant,
            'refantmode': refantmode,
            'minblperant': inputs.minblperant,
            'solnorm': inputs.solnorm
        }
        result = do_gtype_gaincal(inputs.context, self._executor, task_args)

        # Create new CalApplications to correctly register caltable.
        new_calapps = []

        # Define what overrides should be included in the cal application.
        # Phase solution caltables should always be registered with
        # calwt=False (PIPE-1154). Adjust the interpolation and SpW mapping if
        # provided.
        calapp_overrides = {'calwt': False}
        if interp:
            calapp_overrides['interp'] = interp
        if spwmap:
            calapp_overrides['spwmap'] = spwmap

        # Create a modified CalApplication to register this caltable against
        # the PHASE calibrator field itself.
        new_calapps.append(callibrary.copy_calapplication(result.final[0], intent='PHASE', field=field,
                                                          gainfield=field, **calapp_overrides))

        # If the current PHASE field was mapped to TARGET/CHECK field(s), then
        # create a modified CalApplication to register this caltable against
        # those TARGET/CHECK fields.
        if apply_to_field:
            # Adjust what SpWs to apply to, if provided.
            if apply_to_spw:
                calapp_overrides['spw'] = apply_to_spw
            new_calapps.append(callibrary.copy_calapplication(
                result.final[0], intent='TARGET,CHECK', field=apply_to_field, gainfield=field, **calapp_overrides))

        return new_calapps

    def _do_phasecal_for_calibrators(self) -> Tuple[List[common.GaincalResults], Optional[float]]:
        """
        This method is responsible for creating phase gain caltable(s) that
        are applicable to all calibrators.
        """
        inputs = self.inputs

        # Initialize output list of phase gaincal results.
        phasecal_results = []

        # Split intents by PHASE and non-PHASE calibrators.
        p_intent = 'PHASE'
        np_intents = ','.join(set(inputs.intent.split(',')) - {p_intent})

        # PIPE-1154: first create a phase caltable for the non-PHASE
        # calibrators.
        if np_intents:
            np_phasecal_result = self._do_phasecal_for_non_phase_calibrators(np_intents)
            phasecal_results.append(np_phasecal_result)

        # PIPE-1154: next, compute the phase gain solutions for the PHASE
        # calibrator fields. These solutions for the PHASE fields are not
        # intended for the final result, but will be merged into the local task
        # context, so that they are used in pre-apply when computing the
        # residual phase offsets. Track the maximum phase solint that gets
        # used, so the same value can be when computing the diagnostic
        # amplitude caltable.
        max_phase_solint = None
        if p_intent in inputs.intent:
            pcal_results, max_phase_solint = self._do_phasecal_for_phase_calibrators(p_intent)
            phasecal_results.extend(pcal_results)

        return phasecal_results, max_phase_solint

    def _do_phasecal_for_non_phase_calibrators(self, intent: str):
        """
        This method is responsible for creating phase gain caltable(s) for the
        non-PHASE calibrators.
        """
        inputs = self.inputs

        # Identify fields covered by non-phase calibrators.
        fields = ','.join([f.name for f in inputs.ms.get_fields(intent=intent)])

        # PIPE-645: for bandpass, amplitude, diffgain, and polarisation intents,
        # always use minsnr set to 3.
        # PIPE-1154: for bandpass, amplitude, diffgain, and polarisation
        # intents, always use combine='', solint=inputs.calsolint, no spwmap,
        # and no interp.
        phasecal_result = self._do_calibrator_phasecal(field=fields, intent=intent, spw=inputs.spw, gaintype='G',
                                                       combine='', solint=inputs.calsolint, minsnr=3.0, interp=None,
                                                       spwmap=None)

        return phasecal_result

    def _do_phasecal_for_phase_calibrators(self, intent: str) -> Tuple[List[common.GaincalResults], float]:
        """
        This method is responsible for creating phase gain caltable(s) for the
        each field that covers a PHASE calibrator, using optimal gaincal
        parameters based on the SpW mapping registered in the measurement set.
        """
        inputs = self.inputs

        # Initialize list of phase gaincal results and solints used.
        phasecal_results = []
        solints = []

        # Determine which SpWs to solve for. By default, use all input SpWs.
        spw_to_solve = inputs.spw
        if inputs.ms.is_band_to_band:
            # PIPE-2087: for BandToBand, restrict the solve to the diffgain
            # reference SpWs.
            dg_refspws = inputs.ms.get_spectral_windows(task_arg=inputs.spw, intent='DIFFGAINREF')
            spw_to_solve = ','.join(str(spw.id) for spw in dg_refspws)

        # Create separate phase solutions for each PHASE field. These solutions
        # are intended to be used as a temporary pre-apply when generating the
        # final amplitude caltable and the phase offsets caltable.
        for field in inputs.ms.get_fields(intent=intent):
            # Get optimal phase solution parameters for current PHASE field,
            # based on spw mapping info in MS.
            combine, gaintype, interp, lowsnr_spws, solint, spwmap = self._get_phasecal_params(intent, field.name)

            # PIPE-390: if not combining across spw, then no need to deal with
            # SpectralSpec for solint considerations, so create a gaincal
            # solution for all SpWs, using provided solint, gaintype, and
            # interp.
            if not combine:
                phasecal_results.append(self._do_calibrator_phasecal(field=field.name, intent=intent, spw=spw_to_solve,
                                                                     gaintype=gaintype, combine=combine, solint=solint,
                                                                     minsnr=inputs.calminsnr, interp=interp,
                                                                     spwmap=spwmap))
                solints.append(solint)

            # Otherwise, a combined SpW solution is expected, and we need to
            # create separate solutions for each SpectralSpec grouping of SpWs.
            else:
                # Group the input SpWs by SpectralSpec.
                spw_groups = self._get_spw_groupings(inputs.ms, spw_to_solve, spwmap)
                if not spw_groups:
                    raise ValueError('Invalid SpW grouping input.')

                # Loop through each grouping of SpWs.
                for ref_spw, sspec, spw_sel in spw_groups:
                    LOG.info(f'Processing spectral spec {sspec} with SpWs {spw_sel}')

                    # PIPE-163: low/high SNR heuristic choice for the other
                    # calibrators, typically PHASE.
                    # Check if the reference SpW appears on the list of low SNR SpWs
                    # registered in the MS for current field and intent.
                    #  * if so, then as per low-SNR heuristics request, keep using the
                    #  provided solint (which is normally based on SpW mapping mode).
                    #  * if not, then override the provided solint to instead use
                    #  inputs.calsolint, just like for the other calibrator intents.
                    if ref_spw not in lowsnr_spws:
                        solint = inputs.calsolint

                    phasecal_results.append(self._do_calibrator_phasecal(field=field.name, intent=intent, spw=spw_sel,
                                                                         gaintype=gaintype, combine=combine,
                                                                         solint=solint, minsnr=inputs.calminsnr,
                                                                         interp=interp, spwmap=spwmap))
                    solints.append(solint)

        # PIPE-1154: determine which was the longest solint used for any of the
        # PHASE fields; this will be re-used for the diagnostic amplitude
        # caltable.
        max_solint = max(solints)

        return phasecal_results, max_solint

    def _do_phasecal_for_phase_calibrators_forcing_combine(self) -> List[common.GaincalResults]:
        """
        This method will create phase gain caltable(s) for each field that
        both a.) covers a PHASE calibrator, and b.) for which the SpW mapping
        registered in the measurement set recommended to use combine='' in the
        gaincal solve. For these fields, an appropriate "combine" SpW map is
        generated locally, then a new gaincal solve is performed using this SpW
        map and enforcing combine='spw'.

        The resulting caltable is used later during this task for computing the
        residual phase offsets.
        """
        inputs = self.inputs
        intent = "PHASE"

        # Initialize list of phase gaincal results.
        phasecal_results = []

        # Determine which SpWs to solve for. By default, use all input SpWs.
        spw_to_solve = inputs.spw
        if inputs.ms.is_band_to_band:
            # PIPE-2087: for BandToBand, restrict the solve to the diffgain
            # reference SpWs.
            dg_refspws = inputs.ms.get_spectral_windows(task_arg=inputs.spw, intent='DIFFGAINREF')
            spw_to_solve = ','.join(str(spw.id) for spw in dg_refspws)

        # Create separate phase solutions for each PHASE field.
        for field in inputs.ms.get_fields(intent="PHASE"):
            # Get optimal phase solution parameters for current PHASE field,
            # based on spw mapping info in MS.
            combine, gaintype, interp, lowsnr_spws, solint, spwmap = self._get_phasecal_params(intent, field.name)

            # Skip any field where the recommended combine was already 'spw'.
            if combine == 'spw':
                continue

            # For PHASE calibrator fields where the SpW mapping recommended to
            # use combine='', continue with creating a new caltable while
            # forcing the combination of SpWs, and generate a corresponding
            # combine spwmap with which this caltable should be registered.
            # This is done to ensure that the subsequent phase offsets
            # caltable is always sensitive to residual offsets on the PHASE
            # calibrator and the corresponding plots would display
            # meaningful information.

            # Create a "combine" spwmap for input SpWs.
            spws = inputs.ms.get_spectral_windows(inputs.spw)
            spwmap = combine_spwmap(spws)

            # Run the phase calibration, forcing combination of SpWs.
            phasecal_results.append(self._do_calibrator_phasecal(field=field.name, intent=intent, spw=spw_to_solve,
                                                                 gaintype=gaintype, combine='spw', solint=solint,
                                                                 minsnr=inputs.calminsnr, interp=interp,
                                                                 spwmap=spwmap))

        return phasecal_results

    # Used to calibrate "selfcaled" targets
    def _do_calibrator_phasecal(self, field: str = None, intent: str = None, spw: str = None, gaintype: str = 'G',
                                combine: str = None, solint: str = None, minsnr: float = None,
                                interp: str = None, spwmap: List[int] = None) -> common.GaincalResults:
        """
        This runs the gaincal for creating phase solutions intended for the
        calibrators (amplitude, bandpass, polarization, phase).
        """
        inputs = self.inputs

        # Construct filename of output caltable:
        # If provided, use the "calphasetable" parameter from top-level task
        # inputs as a basename, but modify to ensure the filename is unique.
        caltable = None
        if inputs.calphasetable:
            root, ext = os.path.splitext(inputs.calphasetable)
            # Always add intent, and if the intent is PHASE then also add field
            # name.
            field_str = f'.{field}' if intent == 'PHASE' else ''
            caltable = f'{root}.{intent}{field_str}{ext}'

        task_args = {
            'output_dir': inputs.output_dir,
            'vis': inputs.vis,
            'caltable': caltable,
            'field': field,
            'intent': intent,
            'spw': spw,
            'solint': solint,
            'gaintype': gaintype,
            'calmode': 'p',
            'minsnr': minsnr,
            'combine': combine,
            'refant': inputs.refant,
            'minblperant': inputs.minblperant,
            'solnorm': inputs.solnorm,
        }
        result = do_gtype_gaincal(inputs.context, self._executor, task_args)

        # Modify the cal application for this caltable based on overrides.
        # Phase solution caltables should always be registered to be applied
        # with calwt=False (PIPE-1154). Register the caltable to be applied
        # only to the calibrator intent(s). Adjust the interpolation and SpW
        # mapping if provided.
        calapp_overrides = {
            'calwt': False,
            'intent': intent,
        }
        if interp:
            calapp_overrides['interp'] = interp
        if spwmap:
            calapp_overrides['spwmap'] = spwmap

        # PIPE-1154: if adding solutions for a field with PHASE intent, then
        # modify the CalApplication to ensure that solutions from this PHASE
        # field are only applied to itself.
        if intent == 'PHASE':
            calapp_overrides['field'] = field
            calapp_overrides['gainfield'] = field

        # Create a modified CalApplication and replace CalApp in result with
        # this new one.
        modified_calapp = callibrary.copy_calapplication(result.final[0], **calapp_overrides)
        result.final = [modified_calapp]
        result.pool = [modified_calapp]

        return result

    def _do_offsets_phasecal(self) -> common.GaincalResults:
        """
        This method computes a diagnostic phase caltable where the previously
        derived phase caltable is pre-applied, to be used for diagnostic plots
        of the residual phase offsets. Resulting caltable will not be
        registered in the context callibrary, i.e. will not be applied to data.
        """
        inputs = self.inputs

        # PIPE-1154: for the residual phase offset table, always use
        # gaintype = 'G'.
        task_args = {
            'output_dir': inputs.output_dir,
            'vis': inputs.vis,
            'caltable': inputs.offsetstable,
            'field': inputs.field,
            'intent': inputs.intent,
            'spw': inputs.spw,
            'solint': 'inf',
            'gaintype': 'G',
            'calmode': 'p',
            'minsnr': inputs.calminsnr,
            'combine': '',
            'refant': inputs.refant,
            'minblperant': inputs.minblperant,
            'solnorm': inputs.solnorm
        }

        # Create a unique table name. The component filenames depend on task arguments (solint, calmode, gaintype,
        # etc.), hence we create a task to calculate the arguments, then modify the resulting filename
        task_inputs = gtypegaincal.GTypeGaincalInputs(inputs.context, **task_args)
        root, ext = os.path.splitext(task_inputs.caltable)
        task_args['caltable'] = '{}.{}{}'.format(root, 'offsets', ext)

        result = do_gtype_gaincal(inputs.context, self._executor, task_args)

        return result

    def _do_caltarget_ampcal(self, solint: Optional[float] = None) -> common.GaincalResults:
        """
        Create amplitude caltable used for diagnostic plots. Resulting
        caltable will not be registered in the context callibrary, i.e.
        will not be applied to data.
        """
        inputs = self.inputs

        task_args = {
            'output_dir': inputs.output_dir,
            'vis': inputs.vis,
            'caltable': inputs.calamptable,
            'field': inputs.field,
            'intent': inputs.intent,
            'spw': inputs.spw,
            'solint': solint,
            'gaintype': 'T',
            'calmode': 'a',
            'minsnr': inputs.calminsnr,
            'combine': inputs.combine,
            'refant': inputs.refant,
            'minblperant': inputs.minblperant,
            'solnorm': inputs.solnorm
        }
        result = do_gtype_gaincal(inputs.context, self._executor, task_args)

        return result

    def _do_target_ampcal(self) -> List[callibrary.CalApplication]:
        """
        This method computes the amplitude caltable intended for TARGET,
        CHECK, and all calibrators. It returns a list of two CalApplications,
        one for how to apply the caltable to all the calibrators, and a second
        one for how to apply the caltable to the TARGET and CHECK source(s).
        """
        inputs = self.inputs

        task_args = {
            'output_dir': inputs.output_dir,
            'vis': inputs.vis,
            'caltable': inputs.amptable,
            'field': inputs.field,
            'intent': inputs.intent,
            'spw': inputs.spw,
            'solint': 'inf',
            'gaintype': 'T',
            'calmode': 'a',
            'minsnr': inputs.targetminsnr,
            'combine': inputs.combine,
            'refant': inputs.refant,
            'minblperant': inputs.minblperant,
            'solnorm': inputs.solnorm
        }
        result = do_gtype_gaincal(inputs.context, self._executor, task_args)

        # Create modified CalApplications for registering this amplitude
        # caltable in the callibrary/context, once for TARGET,CHECK, and once
        # for the calibrators.
        #
        # Spec for gainfield application from CAS-12214:
        #
        # 1) When applying phase and/or amp gain tables to calibrators,
        #   continue to use gainfield='nearest'. This will ensure that time
        #   interpolations from one group's complex gain calibrator's
        #   solutions to the next group's complex gain calibrator solutions
        #   will never influence how either calibrator gets calibrated.
        #
        # 2) When applying phase and/or amp gain tables to science targets,
        #    leave gainfield blank (instead of 'nearest'), so that time
        #    interpolation across all gain calibrators will be used to
        #    calibrate science targets. This will automagically take care of
        #    the multiple science groups case. Specifically, since it is
        #    assumed that all science target groups are bracketed on both
        #    sides by "their" complex gain calibrator, there will never be a
        #    case where the interpolated solution between two complex gain
        #    calibrators needs to be used.
        result_calapp = result.final[0]

        # Create CalApplication for the calibrators.
        cal_calapp = callibrary.copy_calapplication(
            result_calapp, intent='AMPLITUDE,BANDPASS,PHASE,DIFFGAINREF,DIFFGAINSRC,POLARIZATION,POLANGLE,POLLEAKAGE',
            gainfield='nearest', interp='nearest,linear')

        # Create CalApplication for the TARGET/CHECK sources.
        calapp_overrides = {'intent': 'TARGET,CHECK',
                            'gainfield': ''}

        # PIPE-2087: for BandToBand, register the solutions to be applied to the
        # diffgain on-source SpWs, and use the amplitude solutions from the
        # BANDPASS intent.
        if inputs.ms.is_band_to_band:
            dg_srcspws = inputs.ms.get_spectral_windows(task_arg=inputs.spw, intent='DIFFGAINSRC')
            calapp_overrides['spw'] = ','.join(str(s.id) for s in dg_srcspws)
            calapp_overrides['gainfield'] = ','.join(f.name for f in inputs.ms.get_fields(intent='BANDPASS'))

        target_calapp = callibrary.copy_calapplication(result_calapp, **calapp_overrides)

        return [cal_calapp, target_calapp]

    def _get_phasecal_params(self, intent: str, field: str)\
            -> Tuple[str, str, Optional[str], List[int], str, List[int]]:
        inputs = self.inputs

        # By default, no spw mapping or combining, no interp, gaintype='G',
        # and use solint set by "calsolint" input parameter.
        combine = ''
        gaintype = 'G'
        interp = None
        solint = inputs.calsolint
        spwmap = []

        # Define a placeholder list of low snr SpWs, that needs to be returned
        # if a spwmap needs to be used.
        lowsnr_spws = []

        # Try to fetch spwmapping info from MS for requested intent and field.
        spwmapping = inputs.ms.spwmaps.get((intent, field), None)

        # If a mapping was found, use the spwmap, and update further parameters
        # depending on whether it is a combine spw mapping.
        if spwmapping:
            spwmap = spwmapping.spwmap

            # If the spwmap is for combining spws, then override combine,
            # interp, and gaintype accordingly, and compute an optimal solint.
            if spwmapping.combine:
                combine = 'spw'
                gaintype = 'T'
                interp = 'linearPD,linear'
                lowsnr_spws = spwmapping.low_combinedsnr_spws

                # Compute optimal solint.
                spwidlist = [spw.id for spw in inputs.ms.get_spectral_windows(science_windows_only=True)]
                exptimes = gexptimes.get_scan_exptimes(inputs.ms, [field], intent, spwidlist)
                solint = '%0.3fs' % (min([exptime[1] for exptime in exptimes]) / 4.0)
            else:
                # PIPE-1154: when using a phase up spw mapping, ensure that
                # interp = 'linear,linear'; though this may need to be changed
                # in the future, see PIPEREQ-85.
                interp = 'linear,linear'

        return combine, gaintype, interp, lowsnr_spws, solint, spwmap

    def _unregister_phasecal_with_no_combine(self):
        """
        This method will unregister from the callibrary in the local context
        (stored in inputs) any CalApplication that is registered for a PHASE
        calibrator field for which the SpW mapping recommends solving with
        combine='', or for which no SpW mapping exists (assumed the PHASE field
        will in that case have been solved with default combine='').
        """
        inputs = self.inputs

        # Identify the MS to process.
        vis: str = inputs.ms.basename

        # Identify which PHASE calibrator fields to process.
        # First identify all PHASE fields.
        fields = {f.name for f in inputs.ms.get_fields(intent="PHASE")}
        # Next, if a SpW mapping was registered for this PHASE field, check if
        # it already recommended solving with SpW combination, in which case
        # there is no reason to unregister. If no SpW mapping was registered,
        # then the PHASE field is expected to have been solved with the default
        # combine='', and so it's kept for unregistering.
        for (intent, field), spwmapping in inputs.ms.spwmaps.items():
            if intent == "PHASE" and spwmapping.combine:
                fields.remove(field)

        # If there were no PHASE calibrator fields with combine='', then there
        # are no CalApplications to unregister.
        if not fields:
            return

        # Define predicate function that matches the kind of caltable that
        # needs to be removed from the CalLibrary.
        def phase_no_combine_matcher(calto: callibrary.CalToArgs, calfrom: callibrary.CalFrom) -> bool:
            calto_vis = {os.path.basename(v) for v in calto.vis}
            # Find caltables generated by hifa_timegaincal, for current vis,
            # that are to be applied to PHASE calibrator intent, and that were
            # derived from a field that is on the list of fields to re-compute.
            # Use calfrom.gainfield rather than calto.field since the latter
            # may have been translated to field ID rather than name.
            do_delete = 'hifa_timegaincal' in calfrom.gaintable and vis in calto_vis and "PHASE" in calto.intent \
                        and calfrom.gainfield in fields
            if do_delete:
                # This message may appear multiple times for same caltable if
                # that table was registered through multiple CalApplications,
                # e.g. for different SpWs.
                LOG.debug(f'Unregistering phase caltable {calfrom.gaintable} from task-specific context.')
            return do_delete

        inputs.context.callibrary.unregister_calibrations(phase_no_combine_matcher)


def do_gtype_gaincal(context, executor, task_args) -> common.GaincalResults:
    task_inputs = gtypegaincal.GTypeGaincalInputs(context, **task_args)
    task = gtypegaincal.GTypeGaincal(task_inputs)
    result = executor.execute(task)

    # sanity checks in case gaincal starts returning additional caltable applications
    assert len(result.final) == 1, '>1 caltable application registered by gaincal'

    return result
