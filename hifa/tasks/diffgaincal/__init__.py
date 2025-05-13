import pipeline.infrastructure.renderer.qaadapter as qaadapter
import pipeline.infrastructure.renderer.weblog as weblog

from . import diffgaincal
from . import qa
from . import renderer
from .diffgaincal import DiffGaincal

qaadapter.registry.register_to_calibration_topic(diffgaincal.DiffGaincalResults)

weblog.add_renderer(DiffGaincal,
                    renderer.T2_4MDetailsDiffgaincalRenderer(),
                    group_by=weblog.UNGROUPED)
