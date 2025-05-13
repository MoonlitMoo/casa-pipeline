"""atmcor - Single dish ATM correction task."""
import pipeline.infrastructure.renderer.qaadapter as qaadapter
import pipeline.infrastructure.renderer.weblog as weblog

from . import qa

from .atmcor import SerialSDATMCorrection
from .atmcor import SDATMCorrectionResults
from .renderer import T2_4MDetailsSingleDishATMCorRenderer

SDATMCorrection = SerialSDATMCorrection

qaadapter.registry.register_to_calibration_topic(SDATMCorrectionResults)

weblog.add_renderer(
    SDATMCorrection,
    T2_4MDetailsSingleDishATMCorRenderer(always_rerender=False),
    group_by=weblog.UNGROUPED
)
