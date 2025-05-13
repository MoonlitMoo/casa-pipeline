"""Sky calibration stage."""
import pipeline.infrastructure.renderer.qaadapter as qaadapter
import pipeline.infrastructure.renderer.weblog as weblog

from .skycal import SerialSDSkyCal
from .skycal import SDSkyCal
from . import skycal
from . import qa
from . import renderer

qaadapter.registry.register_to_calibration_topic(skycal.SDSkyCalResults)

weblog.add_renderer(SerialSDSkyCal, renderer.T2_4MDetailsSingleDishSkyCalRenderer(), group_by=weblog.UNGROUPED)
weblog.add_renderer(SDSkyCal, renderer.T2_4MDetailsSingleDishSkyCalRenderer(), group_by=weblog.UNGROUPED)
