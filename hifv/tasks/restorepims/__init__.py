import pipeline.infrastructure.renderer.basetemplates as basetemplates
import pipeline.infrastructure.renderer.qaadapter as qaadapter
import pipeline.infrastructure.renderer.weblog as weblog

from .restorepims import Restorepims
from . import restorepims

#qaadapter.registry.register_to_dataset_topic(restorepims.RestorepimsResults)

weblog.add_renderer(Restorepims,
                    basetemplates.T2_4MDetailsDefaultRenderer(uri='restorepims.mako',
                                                              description='Restorepims'),
                    group_by=weblog.UNGROUPED)
