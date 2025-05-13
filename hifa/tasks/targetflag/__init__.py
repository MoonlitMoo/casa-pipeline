import pipeline.infrastructure.renderer.qaadapter as qaadapter
import pipeline.infrastructure.renderer.weblog as weblog

from . import qa
from . import renderer
from . import targetflag
from .targetflag import Targetflag

__all__ = [
    'Targetflag'
]

qaadapter.registry.register_to_calibration_topic(targetflag.TargetflagResults)

weblog.add_renderer(Targetflag,
                    renderer.T2_4MDetailsTargetflagRenderer(),
                    group_by=weblog.UNGROUPED)
