import os
import shutil

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.basetask as basetask
import pipeline.infrastructure.callibrary as callibrary
from pipeline.infrastructure import casa_tasks
from . import common

LOG = infrastructure.get_logger(__name__)


class GaincalWorkerInputs(common.VdpCommonGaincalInputs):
    def __init__(self, context, output_dir=None, vis=None, caltable=None, field=None, spw=None, antenna=None,
                 uvrange=None, intent=None, selectdata=None, gaintype=None, smodel=None, calmode=None, solint=None,
                 combine=None, refant=None, refantmode=None, minblperant=None, minsnr=None, solnorm=None, append=None,
                 scan=None, splinetime=None, npointaver=None, phasewrap=None, opacity=None, preavg=None, parang=None):

        # Standard parameters
        self.context = context
        self.vis = vis
        self.output_dir = output_dir

        # Gaincal calibration inputs parameters
        self.field = field
        self.spw = spw
        self.antenna = antenna
        self.uvrange = uvrange
        self.intent = intent
        self.selectdata = selectdata
        self.refant = refant
        self.refantmode = refantmode
        self.minblperant = minblperant
        self.opacity = opacity
        self.calmode = calmode

        # Task parameters
        self.gaintype = gaintype
        self.scan = scan
        self.smodel = smodel
        self.solint = solint
        self.combine = combine
        self.minsnr = minsnr
        self.solnorm = solnorm
        self.append = append
        self.preavg = preavg
        self.parang = parang
        self.splinetime = splinetime
        self.npointaver = npointaver
        self.phasewrap = phasewrap

        # The caltable (input parameter)
        self.caltable = caltable


class GaincalWorker(basetask.StandardTaskTemplate):
    """
    GaincalWorker performs a simple gain calibration exactly as specified by
    the inputs, with no analysis or parameter refinement.

    As this task has no analysis, it is not expected to be used in an
    interactive session. The use-case for this task is as a worker task for
    higher-level tasks.
    """
    Inputs = GaincalWorkerInputs

    def prepare(self):
        # create a local variable for the inputs associated with this instance
        inputs = self.inputs

        # make a note of the current inputs state before we start fiddling
        # with it. This origin will be attached to the final CalApplication.
        origin = [callibrary.CalAppOrigin(task=GaincalWorker,
                                          inputs=inputs.to_casa_args())]

        # make fast the caltable name by manually setting it to its current
        # value. This makes the caltable name permanent, so we can
        # subsequently append calibrations for each spectral window to the one
        # table
        inputs.caltable = inputs.caltable

        # Delete a pre-existing table if append it not set. This is to
        # replicate the somewhat convoluted old filenamer "dry_run" logic
        # (see PIPE-1688).
        if os.path.exists(inputs.caltable) and not inputs.append:
            LOG.debug(f'Deleting existing calibration table {inputs.caltable}')
            shutil.rmtree(inputs.caltable, ignore_errors=True)

        # make a memo of the original spw input. These are the spws we will
        # apply the resultant caltable to
        orig_spw = inputs.spw
        orig_antenna = inputs.antenna

        # Retrieve the on-the-fly calibration state for the data selection.
        calto = callibrary.get_calto_from_inputs(inputs)
        calstate = inputs.context.callibrary.get_calstate(calto)

        jobs = []
        # If no on-the-fly calibration is applicable for the data selection
        # then generate a single gaincal job based on inputs.
        if not calstate.merged():
            args = inputs.to_casa_args()
            jobs.append(casa_tasks.gaincal(**args))
        # Otherwise, generate a separate gaincal job for each data selection
        # for which the CalLibrary has a separate entry of CalFrom/CalTo.
        else:
            for calto, calfroms in calstate.merged().items():
                # arrange a gaincal job for the data selection
                if ':' in inputs.spw or '~' in inputs.spw:
                    inputs.spw = orig_spw
                else:
                    inputs.spw = calto.spw
                inputs.field = calto.field
                # PIPE-1490: restrict intent to selection set by CalTo.
                inputs.intent = calto.intent
                # kludge to handle more esoteric 'antenna syntax'
                LOG.todo('handle & and * in antenna selection')
                if '&' in inputs.antenna or '*' in inputs.antenna:
                    inputs.antenna = orig_antenna
                else:
                    inputs.antenna = calto.antenna

                args = inputs.to_casa_args()

                # set the on-the-fly calibration state for the data selection
                calapp = callibrary.CalApplication(calto, calfroms)
                args['gaintable'] = calapp.gaintable
                args['gainfield'] = calapp.gainfield
                args['spwmap']    = calapp.spwmap
                args['interp']    = calapp.interp

                jobs.append(casa_tasks.gaincal(**args))

                # append subsequent output to the same caltable
                inputs.append = True

        # execute the jobs
        for job in jobs:
            self._executor.execute(job)

        # create the data selection target defining which data this caltable
        # should calibrate
        calto = callibrary.CalTo(vis=inputs.vis, spw=orig_spw)

        # create the calfrom object describing which data should be selected
        # from this caltable when applied to other data. Set the table name
        # name (mandatory) and gainfield (to conform to suggested script
        # standard), leaving spwmap, interp, etc. at their default values.
        calfrom = callibrary.CalFrom(inputs.caltable, caltype='gaincal',
                                     gainfield='nearest')

        calapp = callibrary.CalApplication(calto, calfrom, origin)

        result = common.GaincalResults(pool=[calapp])

        return result

    def analyse(self, result):
        # With no best caltable to find, our task is simply to set the one
        # caltable as the best result

        # double-check that the caltable was actually generated
        on_disk = [table for table in result.pool if table.exists()]
        result.final[:] = on_disk

        missing = [table for table in result.pool if table not in on_disk]

        result.error.clear()
        result.error.update(missing)

        return result
