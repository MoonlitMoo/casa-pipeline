import copy
import os
import shutil

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.basetask as basetask
import pipeline.infrastructure.callibrary as callibrary
import pipeline.infrastructure.vdp as vdp
from pipeline.h.heuristics import caltable as pcaltable
from pipeline.hif.tasks.common import commoncalinputs
from pipeline.infrastructure import casa_tasks

LOG = infrastructure.get_logger(__name__)


class PolcalWorkerResults(basetask.Results):
    """
    PolcalWorkerResults is the results class for the pipeline polcal calibration
    task.
    """

    def __init__(self, final=None, pool=None, polcal_returns=None):
        """
        Construct and return a new PolcalWorkerResults.

        PolcalWorkerResults can be initialised with an optional list of
        CalApplications detailing which calibrations, from a pool of candidate
        calibrations (pool), are considered the best to apply (final).

        :param final: the calibrations selected as the best to apply
        :type final: list of :class:`~pipeline.infrastructure.callibrary.CalApplication`
        :param pool: the pool of all calibrations evaluated by the task
        :type pool: list of :class:`~pipeline.infrastructure.callibrary.CalApplication`
        """
        if final is None:
            final = []
        if pool is None:
            pool = []
        if polcal_returns is None:
            polcal_returns = []

        super().__init__()
        self.pool = pool
        self.final = final
        self.polcal_returns = polcal_returns
        self.error = set()

    def merge_with_context(self, context, to_field=None, to_intent=None):
        if not self.final:
            LOG.error('No results to merge')
            return

        for calapp in self.final:
            calto = self._get_calto(calapp.calto, to_field, to_intent)

            LOG.debug(f'Adding calibration to callibrary:\n{calto}\n{calapp.calfrom}')
            context.callibrary.add(calto, calapp.calfrom)

    def _get_calto(self, calto, to_field, to_intent):
        """
        Prepare and return the CalTo to be used for results merging.
        """
        # Do not modify the CalTo directly, as the original values should be
        # preserved for subsequent applications. The CalLibrary makes a
        # defensive copy of the CalFrom, so we do not need to protect that
        # object ourselves.
        calto_copy = copy.deepcopy(calto)

        # When dividing a multi-vis task up into single-vis tasks, the
        # to_field and to_intent parameters are resolved down to single-vis
        # scope accordingly. Therefore, we can use the to_field and to_intent
        # values directly as they should already be appropriate for the target
        # measurement set specified in this result.

        # Give the astronomer a chance to override the destination field and
        # intents, so that the reduction does not need to be repeated just to
        # change how the caltable should be applied.
        if to_field is not None:
            calto_copy.field = to_field
        if to_intent is not None:
            calto_copy.intent = to_intent

        return calto_copy

    def __repr__(self):
        s = 'PolcalWorkerResults:\n'
        for calapp in self.final:
            s += f'\t{os.path.basename(calapp.vis)}: calibration application for table {calapp.gaintable}\n'
        return s


class PolcalWorkerInputs(commoncalinputs.VdpCommonCalibrationInputs):
    @vdp.VisDependentProperty
    def caltable(self):
        namer = pcaltable.PolcalCaltable()
        casa_args = self._get_task_args(ignore=('caltable',))
        return namer.calculate(output_dir=self.output_dir, stage=self.context.stage, **casa_args)

    @vdp.VisDependentProperty
    def intent(self):
        return 'POLARIZATION,POLANGLE,POLLEAKAGE'

    @intent.convert
    def intent(self, value):
        if isinstance(value, list):
            value = [str(v).replace('*', '') for v in value]
        if isinstance(value, str):
            value = value.replace('*', '')
        return value

    def __init__(self, context, output_dir=None, vis=None, caltable=None, intent=None, field=None, spw=None,
                 refant=None, antenna=None, minblperant=None, selectdata=None, uvrange=None, scan=None, solint=None,
                 combine=None, preavg=None, minsnr=None, poltype=None, smodel=None, append=None):
        super().__init__(context, output_dir=output_dir, vis=vis, intent=intent, field=field, spw=spw, refant=refant,
                         antenna=antenna, minblperant=minblperant, selectdata=selectdata, uvrange=uvrange)

        # Polcal input parameters
        self.caltable = caltable
        self.scan = scan
        self.solint = solint
        self.combine = combine
        self.preavg = preavg
        self.minsnr = minsnr
        self.poltype = poltype
        self.smodel = smodel
        self.append = append


class PolcalWorker(basetask.StandardTaskTemplate):
    Inputs = PolcalWorkerInputs

    def prepare(self):
        inputs = self.inputs

        # Set caltable to itself to generate a permanent caltable name.
        inputs.caltable = inputs.caltable

        # Delete a pre-existing table if append it not set. This is to
        # replicate the somewhat convoluted old filenamer "dry_run" logic
        # (see PIPE-1688).
        if os.path.exists(inputs.caltable) and not inputs.append:
            LOG.debug(f'Deleting existing calibration table {inputs.caltable}')
            shutil.rmtree(inputs.caltable, ignore_errors=True)

        # Retrieve original Spw input, to attach to final CalApplication.
        origin = [callibrary.CalAppOrigin(task=PolcalWorker, inputs=inputs.to_casa_args())]
        orig_spw = inputs.spw

        # Retrieve the on-the-fly calibration state for the data selection.
        calto = callibrary.get_calto_from_inputs(inputs)
        calstate = inputs.context.callibrary.get_calstate(calto)

        jobs = []
        # If no on-the-fly calibration is applicable for the data selection
        # then generate a single polcal job based on inputs.
        if not calstate.merged():
            args = inputs.to_casa_args()
            jobs.append(casa_tasks.polcal(**args))
        # Otherwise, generate a separate polcal job for each data selection
        # for which the CalLibrary has a separate entry of CalFrom/CalTo.
        else:
            for calto, calfroms in calstate.merged().items():
                # Update inputs based on CalTo.
                inputs.spw = calto.spw
                inputs.field = calto.field
                inputs.intent = calto.intent
                inputs.antenna = calto.antenna

                # Convert to CASA task arguments.
                args = inputs.to_casa_args()

                # Set the on-the-fly calibration state for the data selection.
                calapp = callibrary.CalApplication(calto, calfroms)
                args['gaintable'] = calapp.gaintable
                args['gainfield'] = calapp.gainfield
                args['spwmap'] = calapp.spwmap
                args['interp'] = calapp.interp

                jobs.append(casa_tasks.polcal(**args))

                # Append subsequent output to the same caltable.
                inputs.append = True

        # execute the jobs
        polcal_returns = []
        for job in jobs:
            polcal_returns.append(self._executor.execute(job))

        # create the data selection target defining which data this caltable
        # should calibrate
        calto = callibrary.CalTo(vis=inputs.vis, spw=orig_spw)

        # create the calfrom object describing which data should be selected
        # from this caltable when applied to other data. Set the table name
        # (mandatory) and gainfield (to conform to suggested script
        # standard), leaving spwmap, interp, etc. at their default values.
        calfrom = callibrary.CalFrom(inputs.caltable, caltype='polarization', gainfield='nearest')
        calapp = callibrary.CalApplication(calto, calfrom, origin)

        result = PolcalWorkerResults(pool=[calapp], polcal_returns=polcal_returns)

        return result

    def analyse(self, result):
        # Check that the caltable was actually generated
        on_disk = [table for table in result.pool if table.exists()]
        result.final[:] = on_disk
        missing = [table for table in result.pool if table not in on_disk]
        result.error.clear()
        result.error.update(missing)
        return result
