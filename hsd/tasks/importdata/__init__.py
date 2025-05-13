# import pipeline.hif.tasks.importdata.renderer as super_renderer
import pipeline.infrastructure.renderer.weblog as weblog
from . import qa
from . import renderer
from .importdata import SerialSDImportData
from .importdata import SDImportData
from .importdata import SDImportDataResults
import pipeline.infrastructure.renderer.qaadapter as qaadapter

qaadapter.registry.register_to_dataset_topic(SDImportDataResults)

importdata_renderer = renderer.T2_4MDetailsSingleDishImportDataRenderer(
    uri='hsd_importdata.mako'
)
weblog.add_renderer(SerialSDImportData, importdata_renderer,
                    group_by=weblog.UNGROUPED)
weblog.add_renderer(SDImportData, importdata_renderer,
                    group_by=weblog.UNGROUPED)
