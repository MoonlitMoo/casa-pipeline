import pipeline.infrastructure.renderer.basetemplates as basetemplates
import pipeline.infrastructure.renderer.weblog as weblog
import pipeline.infrastructure.renderer.qaadapter as qaadapter
from . import qa
from .restoredata import RestoreData

qaadapter.registry.register_to_dataset_topic(restoredata.RestoreDataResults)

weblog.add_renderer(RestoreData, basetemplates.T2_4MDetailsDefaultRenderer(description='Restore Calibrated Data'),
                    group_by=weblog.UNGROUPED)
