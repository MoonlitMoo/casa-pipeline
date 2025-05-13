import pipeline.hif.tasks.antpos.antpos as antpos
import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.vdp as vdp
from pipeline.infrastructure import task_registry

__all__ = [
    'ALMAAntpos',
    'ALMAAntposInputs'
]

LOG = infrastructure.get_logger(__name__)


class ALMAAntposInputs(antpos.AntposInputs):
    """
    ALMAAntposInputs defines the inputs for the ALMAAntpos pipeline task.
    """
    # These are ALMA specific settings and override the defaults in
    # the base class.

    # Force the offset input to be from a file
    hm_antpos = vdp.VisDependentProperty(default='file')
    threshold = vdp.VisDependentProperty(default=1.0)

    def __init__(self, context, output_dir=None, vis=None, caltable=None, hm_antpos=None, antposfile=None, antenna=None,
                 offsets=None, threshold=None):
        super(ALMAAntposInputs, self).__init__(
            context, output_dir=output_dir, vis=vis, caltable=caltable, hm_antpos=hm_antpos, antposfile=antposfile,
            antenna=antenna, offsets=offsets)
        self.threshold = threshold

    def __str__(self):
        s = 'AlmaAntposInputs:\n'
        s += '\tvis: %s\n' % self.vis
        s += '\tcaltable: %s\n' % self.caltable
        s += '\thm_antpos: %s\n' % self.hm_antpos
        s += '\tantposfile: %s\n' % self.antposfile
        s += '\tantenna: %s\n' % self.antenna
        s += '\toffsets: %s\n' % self.offsets
        s += '\tthreshold: %s\n' % self.threshold
        return s

@task_registry.set_equivalent_casa_task('hifa_antpos')
class ALMAAntpos(antpos.Antpos):
    Inputs = ALMAAntposInputs
