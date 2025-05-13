import pipeline.hif.tasks.antpos.antpos as antpos
import pipeline.infrastructure as infrastructure
import pipeline.infrastructure.vdp as vdp
from pipeline.infrastructure import task_registry

__all__ = [
    'VLAAntpos',
    'VLAAntposInputs'
]

LOG = infrastructure.get_logger(__name__)


class VLAAntposInputs(antpos.AntposInputs):
    """
    VLAAntposInputs defines the inputs for the priorcals pipeline task.
    """
    # These are VLA specific settings and override the defaults in
    # the base class.

    ant_pos_time_limit = vdp.VisDependentProperty(default=150)

    def __init__(self, context, output_dir=None, vis=None,
                 ant_pos_time_limit=None):
        super().__init__(
            context, output_dir=output_dir, vis=vis)
        # Antenna position time limit, requires CASA>=6.6.1-5. PIPE-2052
        self.ant_pos_time_limit = ant_pos_time_limit

    def to_casa_args(self):
        gencal_args = super().to_casa_args()
        gencal_args['ant_pos_time_limit'] = self.ant_pos_time_limit
        return gencal_args


class VLAAntpos(antpos.Antpos):
    Inputs = VLAAntposInputs
