import pipeline.infrastructure.renderer.basetemplates as basetemplates
import pipeline.infrastructure.renderer.qaadapter as qaadapter
import pipeline.infrastructure.renderer.weblog as weblog

from .fixpointing import Fixpointing
from . import fixpointing

#qaadapter.registry.register_to_dataset_topic(fixpointing.FixpointingResults)

weblog.add_renderer(Fixpointing, basetemplates.T2_4MDetailsDefaultRenderer(uri='fixpointing.mako',
                         description='Fixpointing'), group_by=weblog.UNGROUPED)