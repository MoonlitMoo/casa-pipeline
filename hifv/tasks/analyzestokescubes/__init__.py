import pipeline.infrastructure.renderer.qaadapter as qaadapter
import pipeline.infrastructure.renderer.weblog as weblog

from .analyzestokescubes import Analyzestokescubes
from . import renderer

#qaadapter.registry.register_to_dataset_topic(analyzestokescubes.AnalyzestokescubesResults)

weblog.add_renderer(Analyzestokescubes,
                    renderer.T2_4MDetailsAnalyzestokesCubeRenderer(uri='analyzestokescubes.mako',
                                                                   description='Analyzestokescubes'),
                    group_by=weblog.UNGROUPED)
