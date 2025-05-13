import pipeline.infrastructure.renderer.qaadapter as qaadapter
import pipeline.infrastructure.renderer.weblog as weblog
from . import qa
from . import referenceantenna
from . import renderer
from .referenceantenna import RefAnt, SerialRefAnt

qaadapter.registry.register_to_miscellaneous_topic(referenceantenna.RefAntResults)

weblog.add_renderer(SerialRefAnt, renderer.T2_4MDetailsRefantRenderer(), group_by=weblog.UNGROUPED)
weblog.add_renderer(RefAnt, renderer.T2_4MDetailsRefantRenderer(), group_by=weblog.UNGROUPED)
