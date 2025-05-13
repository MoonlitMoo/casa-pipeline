import pipeline.infrastructure.renderer.qaadapter as qaadapter
import pipeline.infrastructure.renderer.weblog as weblog
from . import mstransform
from . import qa
from . import renderer
from .mstransform import VlaMstransform

qaadapter.registry.register_to_dataset_topic(mstransform.VlaMstransformResults)

weblog.add_renderer(VlaMstransform, renderer.T2_4MDetailsVlaMstransformRenderer(), group_by=weblog.UNGROUPED)
