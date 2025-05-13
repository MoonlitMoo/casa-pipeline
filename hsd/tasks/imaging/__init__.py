import pipeline.infrastructure.renderer.qaadapter as qaadapter
import pipeline.infrastructure.renderer.weblog as weblog

from .imaging import SDImaging
from . import imaging
from . import resultobjects
from . import renderer
from . import qa

qaadapter.registry.register_to_imaging_topic(resultobjects.SDImagingResults)

weblog.add_renderer(SDImaging, renderer.T2_4MDetailsSingleDishImagingRenderer(always_rerender=False),
                    group_by=weblog.UNGROUPED)
