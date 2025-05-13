import pipeline.infrastructure.renderer.qaadapter as qaadapter
import pipeline.infrastructure.renderer.weblog as weblog

from . import qa
from . import renderer
from . import polcalflag
from .polcalflag import Polcalflag

__all__ = [
    'Polcalflag'
]

qaadapter.registry.register_to_calibration_topic(polcalflag.PolcalflagResults)

weblog.add_renderer(Polcalflag,
                    renderer.T2_4MDetailsPolcalflagRenderer(),
                    group_by=weblog.UNGROUPED)
