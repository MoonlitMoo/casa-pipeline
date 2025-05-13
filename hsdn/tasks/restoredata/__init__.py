import pipeline.infrastructure.renderer.weblog as weblog
from . import csvfilereader
from . import renderer
from .restoredata import NRORestoreData

weblog.add_renderer(NRORestoreData, renderer.T2_4MDetailsNRORestoreDataRenderer(description='Restore Calibrated Data'),
                    group_by=weblog.UNGROUPED)
