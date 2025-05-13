import pipeline.infrastructure.renderer.basetemplates as basetemplates
# import pipeline.infrastructure.pipelineqa as pipelineqa
# import pipeline.infrastructure.renderer.qaadapter as qaadapter
import pipeline.infrastructure.renderer.weblog as weblog

from .hanning import Hanning
from . import hanning
from . import renderer

# from . import qa

# pipelineqa.registry.add_handler(qa.HanningQAHandler())
# pipelineqa.registry.add_handler(qa.HanningListQAHandler())
# qaadapter.registry.register_to_dataset_topic(hanning.HanningResults)

weblog.add_renderer(Hanning,
                    renderer.T2_4DetailsHanningRenderer(uri='hanning.mako',
                                                              description='VLA Hanning Smoothing'),
                    group_by=weblog.UNGROUPED)
