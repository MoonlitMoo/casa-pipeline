import pipeline.infrastructure.renderer.qaadapter as qaadapter
import pipeline.infrastructure.renderer.weblog as weblog
from . import qa
from .almaimportdata import ALMAImportData, SerialALMAImportData
from . import almaimportdata
from .renderer import T2_4MDetailsALMAImportDataRenderer

qaadapter.registry.register_to_dataset_topic(almaimportdata.ALMAImportDataResults)

weblog.add_renderer(ALMAImportData, T2_4MDetailsALMAImportDataRenderer(), group_by=weblog.UNGROUPED)
weblog.add_renderer(SerialALMAImportData, T2_4MDetailsALMAImportDataRenderer(), group_by=weblog.UNGROUPED)
