import pipeline.infrastructure.renderer.basetemplates as basetemplates
# import pipeline.infrastructure.renderer.qaadapter as qaadapter
import pipeline.infrastructure.renderer.weblog as weblog

from .analyzealpha import Analyzealpha
from . import analyzealpha

# qaadapter.registry.register_to_dataset_topic(analyzealpha.AnalyzealphaResults)

weblog.add_renderer(Analyzealpha,
                    basetemplates.T2_4MDetailsDefaultRenderer(uri='analyzealpha.mako', description='Analyzealpha'),
                    group_by=weblog.UNGROUPED)
