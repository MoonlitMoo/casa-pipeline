import pipeline.h.tasks.applycal.applycal as applycal
import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.vdp as vdp
from pipeline.infrastructure import task_registry

__all__ = ['IFApplycal',
           'SerialIFApplycal',
           'IFApplycalInputs']

LOG = infrastructure.get_logger(__name__)


class IFApplycalInputs(applycal.ApplycalInputs):
    flagdetailedsum = vdp.VisDependentProperty(default=True)

    # Override h_applycal default, adding polarisation (PIPE-600) and diffgain
    # (PIPE-2088) to calibrator intents.
    intent = vdp.VisDependentProperty(default='TARGET,PHASE,BANDPASS,AMPLITUDE,CHECK,DIFFGAINREF,DIFFGAINSRC,'
                                              'POLARIZATION,POLANGLE,POLLEAKAGE')

    def __init__(self, context, output_dir=None, vis=None, field=None, spw=None, antenna=None, intent=None, parang=None,
                 applymode=None, flagbackup=None, flagsum=None, flagdetailedsum=None,
                 parallel=None):
        super().__init__(context, output_dir=output_dir, vis=vis, field=field, spw=spw,
                         antenna=antenna, intent=intent, parang=parang, applymode=applymode,
                         flagbackup=flagbackup, flagsum=flagsum, flagdetailedsum=flagdetailedsum,
                         parallel=parallel)


class SerialIFApplycal(applycal.SerialApplycal):
    Inputs = IFApplycalInputs

    def __init__(self, inputs):
        super().__init__(inputs)


@task_registry.set_equivalent_casa_task('hif_applycal')
@task_registry.set_casa_commands_comment('Calibrations are applied to the data. Final flagging summaries are computed')
class IFApplycal(applycal.Applycal):
    Inputs = IFApplycalInputs
    Task = SerialIFApplycal
