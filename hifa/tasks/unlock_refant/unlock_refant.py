import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.basetask as basetask
import pipeline.infrastructure.vdp as vdp
from pipeline.infrastructure import task_registry
from pipeline.infrastructure.launcher import Context

__all__ = [
    'UnlockRefAnt',
    'UnlockRefAntInputs',
    'UnlockRefAntResults',
]

LOG = infrastructure.get_logger(__name__)


class UnlockRefAntInputs(vdp.StandardInputs):
    def to_casa_args(self):
        # does not use CASA tasks
        raise NotImplementedError

    def __init__(self, context, vis=None, output_dir=None):
        self.context = context
        self.vis = vis
        self.output_dir = output_dir


class UnlockRefAntResults(basetask.Results):
    def __init__(self, vis: str):
        super().__init__()
        self._vis = vis

    def merge_with_context(self, context: Context):
        if self._vis is None:
            LOG.error('No results to merge')
            return

        ms = context.observing_run.get_ms(name=self._vis)
        if ms:
            LOG.debug('Unlocking refant for %s', ms.basename)
            ms.reference_antenna_locked = False

    def __str__(self):
        return 'Unlock reference antenna results: refant list unlocked'

    def __repr__(self):
        return f'UnlockRefAntResults({self._vis})'


@task_registry.set_equivalent_casa_task('hifa_unlock_refant')
@task_registry.set_casa_commands_comment(
    'The reference antenna list for all measurement sets is unlocked to allow modification'
)
class UnlockRefAnt(basetask.StandardTaskTemplate):
    Inputs = UnlockRefAntInputs

    def prepare(self, **parameters):
        return UnlockRefAntResults(self.inputs.vis)

    def analyse(self, results):
        return results
