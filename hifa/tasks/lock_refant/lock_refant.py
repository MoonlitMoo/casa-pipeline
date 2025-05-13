# import os

import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.basetask as basetask
import pipeline.infrastructure.vdp as vdp
from pipeline.infrastructure import task_registry
from pipeline.infrastructure.launcher import Context

__all__ = [
    'LockRefAnt',
    'LockRefAntInputs',
    'LockRefAntResults',
]

LOG = infrastructure.get_logger(__name__)


class LockRefAntInputs(vdp.StandardInputs):
    def to_casa_args(self):
        # refant does not use CASA tasks
        raise NotImplementedError

    def __init__(self, context, vis=None, output_dir=None):
        self.context = context
        self.vis = vis
        self.output_dir = output_dir


class LockRefAntResults(basetask.Results):
    def __init__(self, vis: str):
        super().__init__()
        self._vis = vis

    def merge_with_context(self, context: Context):
        if self._vis is None:
            LOG.error('No results to merge')
            return

        ms = context.observing_run.get_ms(name=self._vis)
        if ms:
            LOG.debug('Locking refant for %s', ms.basename)
            ms.reference_antenna_locked = True

    def __str__(self):
        return 'Lock reference antenna results: refant list locked'

    def __repr__(self):
        return f'LockRefAntResults({self._vis})'


@task_registry.set_equivalent_casa_task('hifa_lock_refant')
@task_registry.set_casa_commands_comment(
    'The reference antenna list for all measurement sets is locked to prevent further modification'
)
class LockRefAnt(basetask.StandardTaskTemplate):
    Inputs = LockRefAntInputs

    def prepare(self, **parameters):
        return LockRefAntResults(self.inputs.vis)

    def analyse(self, results):
        return results
