import pipeline.infrastructure.renderer.qaadapter as qaadapter
import pipeline.infrastructure.renderer.weblog as weblog

from .importdata import VLAImportData, SerialVLAImportData
from . import importdata
from . import renderer
from . import qa

qaadapter.registry.register_to_dataset_topic(importdata.VLAImportDataResults)

weblog.add_renderer(VLAImportData, renderer.T2_4MDetailsVLAImportDataRenderer(), group_by=weblog.UNGROUPED)
weblog.add_renderer(SerialVLAImportData, renderer.T2_4MDetailsVLAImportDataRenderer(), group_by=weblog.UNGROUPED)
