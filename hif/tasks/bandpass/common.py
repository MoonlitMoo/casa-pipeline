import copy
import os
from typing import List

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.basetask as basetask
import pipeline.infrastructure.vdp as vdp
from pipeline.h.heuristics import caltable as bcaltable
from pipeline.hif.tasks.common import commoncalinputs as commoncalinputs
from pipeline.infrastructure.callibrary import CalApplication, CalFrom, CalToArgs
from pipeline.infrastructure.launcher import Context

LOG = infrastructure.get_logger(__name__)


class VdpCommonBandpassInputs(commoncalinputs.VdpCommonCalibrationInputs):
    """
    CommonBandpassInputs is the base class for defines inputs that are common
    to all pipeline bandpass calibration tasks.

    CommonBandpassInputs should be considered an abstract class. Refer to the
    specializations that inherit from CommonBandpassInputs for concrete
    implementations.
    """
    combine = vdp.VisDependentProperty(default='scan')
    solint = vdp.VisDependentProperty(default='inf')
    solnorm = vdp.VisDependentProperty(default=True)

    @vdp.VisDependentProperty
    def caltable(self):
        namer = bcaltable.BandpassCaltable()
        casa_args = self._get_task_args(ignore=('caltable',))
        return namer.calculate(output_dir=self.output_dir, stage=self.context.stage, **casa_args)

    @vdp.VisDependentProperty
    def intent(self):
        # if the spw was set, look to see which intents were observed in that
        # spectral window and return the intent based on our order of
        # preference: BANDPASS, AMPLITUDE, PHASE
        preferred_intents = ('BANDPASS', 'PHASE', 'AMPLITUDE')
        if self.spw:
            for spw in self.ms.get_spectral_windows(self.spw):
                for intent in preferred_intents:
                    if intent in spw.intents:
                        if intent != preferred_intents[0]:
                            LOG.warning('%s spw %s: %s not present, %s used instead' %
                                        (os.path.basename(self.vis), spw.id,
                                         preferred_intents[0], intent))
                        return intent

        # spw was not set, so look through the spectral windows
        for intent in preferred_intents:
            for spw in self.ms.spectral_windows:
                if intent in spw.intents:
                    if intent != preferred_intents[0]:
                        LOG.warning('%s %s: %s not present, %s used instead' %
                                    (os.path.basename(self.vis), spw.id, preferred_intents[0],
                                     intent))
                    return intent

        # current fallback - return an empty intent
        return ''

    @intent.convert
    def intent(self, value):
        if isinstance(value, list):
            value = [str(v).replace('*', '') for v in value]
        if isinstance(value, str):
            value = value.replace('*', '')
        return value


class BandpassResults(basetask.Results):
    """
    BandpassResults is the results class common to all pipeline bandpass
    calibration tasks.
    """

    def __init__(self, 
                 final: List[CalApplication]=None, 
                 pool: List[CalApplication]=None, 
                 # TODO preceding was intended to hold child results but it
                 # does not appear to be used anywhere. I suspect it can be
                 # removed.
                 preceding: List[basetask.Results]=None,
                 applies_adopted: bool=False,
                 unregister_existing: bool=False):
        """
        Construct and return a new BandpassResults.

        Set applies_adopted to True if the bandpass calibration is adopted
        from another measurement set. This can be the case for sessions,
        where a bandpass calibrator is shared between multiple EBs. This
        flag is for presentation logic and does not affect the calibration
        itself.

        :param final: calibrations to be applied by this task (optional)
        :param pool: calibrations assessed by this task (optional)
        :param preceding: DEPRECATED results from worker tasks executed by
            this task (optional)
        :param applies_adopted: True if this Results applies a bandpass
            caltable generated from another measurement set
        :param unregister_existing: True if existing bandpass calibrations
            should be unregistered before registering new calibration
        """
        if final is None:
            final = []
        if pool is None:
            pool = []
        if preceding is None:
            preceding = []

        super(BandpassResults, self).__init__()
        self.pool: List[CalApplication] = []
        self.final: List[CalApplication] = []
        self.preceding: List[basetask.Results] = []
        self.error = set()
        self.qa = {}
        self.applies_adopted: bool = applies_adopted
        self.unregister_existing: bool = unregister_existing

        # defensive programming: deepcopy the CalApplications as they're
        # adopted just in case the caller updates them after this object is
        # constructed.
        self.pool.extend(copy.deepcopy(pool))
        self.final.extend(copy.deepcopy(final))
        self.preceding.extend(copy.deepcopy(preceding))

        # PIPE-1624: Bandpass phaseup caltable is saved off so it can be used in the Phase RMS structure function analysis in spwphaseup.
        self.phaseup_caltable_for_phase_rms = []

    def merge_with_context(self, context: Context):
        """
        See :method:`~pipeline.api.Results.merge_with_context`
        """
        if not self.final:
            LOG.error('No results to merge')
            return

        # PIPE-1624: Add caltable name to the ms so it can be used later by the Phase RMS structure function analysis.
        if self.phaseup_caltable_for_phase_rms:
            vis = os.path.basename(self.inputs['vis'])
            ms = context.observing_run.get_ms(vis)
            ms.phaseup_caltable_for_phase_rms = self.phaseup_caltable_for_phase_rms

        # If requested, unregister old bandpass calibrations before 
        # registering this one
        if self.unregister_existing:
            # Identify the MS to process.
            vis: str = os.path.basename(self.inputs['vis'])

            # predicate function to match bandpass caltables for this MS
            def bandpass_matcher(calto: CalToArgs, calfrom: CalFrom) -> bool:
                calto_vis = {os.path.basename(v) for v in calto.vis}

                # Standard caltable filenames contain task identifiers,
                # caltable type identifiers, etc. We can use this to identify
                # caltables created by this task. As an extra check we also 
                # check the caltable type
                do_delete = 'bandpass' in calfrom.gaintable and 'bandpass' in calfrom.caltype and vis in calto_vis
                if do_delete:
                    LOG.info(f'Unregistering previous bandpass calibrations for {vis}')
                return do_delete

            context.callibrary.unregister_calibrations(bandpass_matcher)

        for calapp in self.final:
            LOG.debug(f'Adding calibration to callibrary:\n{calapp.calto}\n{calapp.calfrom}')
            context.callibrary.add(calapp.calto, calapp.calfrom)

    def __str__(self):
        s = 'BandpassResults:\n'
        for calapp in self.final:
            s += f'\tBest caltable for spw #{calapp.spw} in {os.path.basename(calapp.vis)} is {calapp.gaintable}\n'
        return s
