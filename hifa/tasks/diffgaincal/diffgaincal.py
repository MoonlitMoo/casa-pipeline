from typing import List, Optional

import numpy as np

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.basetask as basetask
import pipeline.infrastructure.callibrary as callibrary
import pipeline.infrastructure.vdp as vdp
from pipeline.hif.tasks.gaincal import common
from pipeline.hif.tasks.gaincal import gtypegaincal
from pipeline.hifa.heuristics.phasespwmap import update_spwmap_for_band_to_band
from pipeline.infrastructure import task_registry

LOG = infrastructure.get_logger(__name__)

__all__ = [
    'DiffGaincal',
    'DiffGaincalInputs',
    'DiffGaincalResults',
]


class DiffGaincalResults(basetask.Results):
    def __init__(self, vis=None, final=None, pool=None, phaseoffsetresult=None):
        if final is None:
            final = []
        if pool is None:
            pool = []

        super().__init__()
        self.error = set()
        self.final = final[:]
        self.phaseoffsetresult = phaseoffsetresult
        self.pool = pool[:]
        self.vis = vis

    def merge_with_context(self, context):
        # Register all CalApplications from each session.
        for calapp in self.final:
            LOG.debug(f'Adding calibration to callibrary:\n{calapp.calto}\n'
                      f'{calapp.calfrom}')
            context.callibrary.add(calapp.calto, calapp.calfrom)

    def __repr__(self):
        return 'DiffGaincalResults'


class DiffGaincalInputs(vdp.StandardInputs):

    def __init__(self, context, output_dir=None, vis=None):
        super().__init__()
        # Standard Pipeline inputs.
        self.context = context
        self.vis = vis
        self.output_dir = output_dir


@task_registry.set_equivalent_casa_task('hifa_diffgaincal')
@task_registry.set_casa_commands_comment('Compute the differential gain calibration.')
class DiffGaincal(basetask.StandardTaskTemplate):
    Inputs = DiffGaincalInputs

    def prepare(self) -> DiffGaincalResults:
        """
        Execute differential gain calibration heuristics and return Results
        object that includes final caltables.

        Returns:
            DiffGaincalResults instance.
        """
        # Initialize results.
        result = DiffGaincalResults(vis=self.inputs.vis)

        # Compute phase solutions for the diffgain reference intent.
        LOG.info('Computing phase gain table for the diffgain reference intent.')
        self._do_phasecal_for_diffgain_reference()

        # Compute phase solutions for the diffgain on-source spectral windows.
        LOG.info('Computing phase gain table for the diffgain on-source intent.')
        dg_ref_phase_results = self._do_phasecal_for_diffgain_onsource()
        # Adopt resulting CalApplication(s) into final result.
        result.pool = dg_ref_phase_results.pool

        # Compute residual phase offsets for the diffgain on-source intent, for
        # diagnostic plots in the weblog.
        LOG.info('Computing residual phase offsets for the diffgain on-source intent.')
        result.phaseoffsetresult = self._do_phasecal_for_diffgain_residual_offsets()

        return result

    def analyse(self, result: DiffGaincalResults) -> DiffGaincalResults:
        """
        Analyze the DiffGaincalResults: check that all caltables from
        CalApplications exist on disk.

        Args:
            result: DiffGaincalResults instance.

        Returns:
            DiffGaincalResults instance.
        """
        # Check that the caltables were all generated.
        on_disk = [ca for ca in result.pool if ca.exists()]
        result.final[:] = on_disk

        missing = [ca for ca in result.pool if ca not in on_disk]
        result.error.clear()
        result.error.update(missing)

        return result

    def _do_gaincal(self, append: bool = False, caltable: Optional[str] = None, combine: Optional[str] = None,
                    intent: Optional[str] = None, scan: Optional[str] = None) -> common.GaincalResults:
        """
        Local method to call the G-type gaincal worker task, with pre-defined
        values for parameters that are shared among all gaincal steps in this
        task.

        Args:
            append: boolean whether to append to existing caltable
            caltable: name of caltable to use
            combine: axis to combine solutions over
            intent: intent selection to use
            scan: scan selection to use

        Returns:
            GaincalResults
        """
        task_args = {
            'output_dir': self.inputs.output_dir,
            'vis': self.inputs.vis,
            'caltable': caltable,
            'intent': intent,
            'spw': ','.join(str(spw.id) for spw in self.inputs.ms.get_spectral_windows(intent=intent)),
            'scan': scan,
            'combine': combine,
            'solint': 'inf',
            'calmode': 'p',
            'minsnr': 3.0,
            'refantmode': 'strict',
            'append': append,
        }
        task_inputs = gtypegaincal.GTypeGaincalInputs(self.inputs.context, **task_args)
        task = gtypegaincal.GTypeGaincal(task_inputs)
        result = self._executor.execute(task)

        return result

    def _do_phasecal_for_diffgain_reference(self):
        """
        Compute phase gain solutions for the diffgain reference intent and
        register the resulting caltable in the local task context to be applied
        to the diffgain on-source intent.
        """
        # Run gaincal for the diffgain reference spws.
        phasecal_result = self._do_gaincal(intent='DIFFGAINREF')

        # If a caltable was created, then create a modified CalApplication to
        # correctly register the caltable.
        if phasecal_result.pool:
            # Retrieve the diffgain spectral windows, and create SpW mapping to
            # re-map diffgain on-source SpWs to diffgain reference SpWs.
            dg_refspws = self.inputs.ms.get_spectral_windows(intent='DIFFGAINREF')
            dg_srcspws = self.inputs.ms.get_spectral_windows(intent='DIFFGAINSRC')
            dg_spwmap = update_spwmap_for_band_to_band([], dg_refspws, dg_srcspws)

            # Define CalApplication overrides to specify how the diffgain
            # reference phase solutions should be registered in the callibrary.
            # Note: solutions derived for the diffgain reference intent are
            # registered here to be applied to the diffgain on-source intent and
            # SpWs with a corresponding SpW mapping.
            calapp_overrides = {
                'calwt': False,
                'intent': 'DIFFGAINSRC',
                'interp': 'linearPD,linear',
                'spw': ','.join(str(spw.id) for spw in dg_srcspws),
                'spwmap': dg_spwmap,
            }

            # There should be only 1 caltable, so replace the CalApp for that
            # one.
            modified_calapp = callibrary.copy_calapplication(phasecal_result.pool[0], **calapp_overrides)
            phasecal_result.pool[0] = modified_calapp
            phasecal_result.final[0] = modified_calapp

            # Register these diffgain reference phase solutions into local
            # context to ensure that these are used in pre-apply when computing
            # phase solutions for the diffgain on-source intent.
            phasecal_result.accept(self.inputs.context)

    def _do_phasecal_for_diffgain_onsource(self) -> Optional[common.GaincalResults]:
        """
        Compute phase gain solutions for the diffgain on-source intent. Gain
        solutions are created separately for groups of scans, using
        combine='scan', and appended to a single caltable. Each scan group is
        intended to comprise scans taken closely together in time (typically
        alternated with the diffgain reference scans), and these groups are
        typically done before and after the science target scans. In practice,
        scan groups are identified as scans with IDs separated by less than 4.

        The resulting caltable is immediately registered with the callibrary in
        the local task context (to ensure pre-apply in the subsequent
        computation of residual offset). For final acceptance of this caltable
        in the top-level callibrary, the CalApplication is updated to ensure the
        resulting caltable will be applied to the science target or check
        source.

        Returns:
            GaincalResults for the diffgain on-source phase solutions caltable.
        """
        # Determine scan groups for diffgain on-source intent.
        scan_groups = self._get_scan_groups(intent='DIFFGAINSRC')

        # Exit early if no scan groups could be identified.
        if not scan_groups:
            LOG.warning(f"{self.inputs.ms.basename}: no scan groups found for diffgain on-source intent.")
            return None

        # Run gaincal for the first scan group.
        phasecal_result = self._do_gaincal(combine='scan', intent='DIFFGAINSRC', scan=scan_groups[0])

        # If there are multiple scan groups, run gaincal for all remaining
        # scan groups, appending their solutions to the same caltable.
        if len(scan_groups) > 1:
            caltable = phasecal_result.inputs['caltable']
            for scan_group in scan_groups[1:]:
                self._do_gaincal(caltable=caltable, combine='scan', intent='DIFFGAINSRC', scan=scan_group, append=True)

        # If a caltable was created, then create a modified CalApplication to
        # correctly register the caltable.
        if phasecal_result.pool:
            # Prior to registering this caltable into the local context, set
            # overrides in the CalApplication:
            calapp_overrides = {
                'calwt': False,
                'intent': 'DIFFGAINSRC',
            }
            modified_calapp = callibrary.copy_calapplication(phasecal_result.pool[0], **calapp_overrides)
            phasecal_result.pool[0] = modified_calapp
            phasecal_result.final[0] = modified_calapp

            # Register the diffgain on-source phase solutions into local context
            # to ensure that these are used in pre-apply when computing the
            # residual phase offsets (diagnostic) caltable.
            phasecal_result.accept(self.inputs.context)

            # Update the CalApplication again, this time to ensure this diffgain
            # phase offsets caltable will be applied to the science target
            # and/or check source.
            calapp_overrides = {
                'calwt': False,
                'intent': 'TARGET,CHECK',
            }
            modified_calapp = callibrary.copy_calapplication(phasecal_result.pool[0], **calapp_overrides)
            phasecal_result.pool[0] = modified_calapp
            phasecal_result.final[0] = modified_calapp

        return phasecal_result

    def _do_phasecal_for_diffgain_residual_offsets(self) -> common.GaincalResults:
        """
        Compute residual phase offsets caltable for the diffgain on-source intent.

        Returns:
            GaincalResults for the diffgain on-source residual phase solutions
            caltable.
        """
        # Run 2nd gaincal for the diffgain on-source intent, this time with the
        # phase solutions for diffgain on-source intent in pre-apply, to assess
        # the residual phase offsets.
        phasecal_result = self._do_gaincal(intent='DIFFGAINSRC')

        return phasecal_result

    def _get_scan_groups(self, intent: str) -> List[str]:
        """
        Return list of scan groups associated with given intent, where each
        group are scans separated by less than 4 in ID.

        E.g., if the scans for given intent are:
          '5,7,9,11,79,81,83'
        Then this will return:
          ['5,7,9,11', '79,81,83']

        Returns:
            List of groups of scans.
        """
        # Get IDs of DIFFGAIN scans for given intent.
        dg_scanids = sorted(scan.id for scan in self.inputs.ms.get_scans(scan_intent=intent))

        # Group scans separated in ID by less than 4.
        scan_groups = [','.join(map(str, group))
                       for group in np.split(dg_scanids, np.where(np.diff(dg_scanids) > 3)[0] + 1)]

        return scan_groups
