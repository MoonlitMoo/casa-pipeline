import pipeline.infrastructure.renderer.basetemplates as basetemplates
import pipeline.infrastructure.renderer.qaadapter as qaadapter
import pipeline.infrastructure.renderer.weblog as weblog

from . import qa, renderer, statwt
from .statwt import Statwt

qaadapter.registry.register_to_dataset_topic(statwt.StatwtResults)

weblog.add_renderer(Statwt,
                    renderer.T2_4MDetailsstatwtRenderer(uri='statwt.mako', description='Reweight visibilities'),
                    group_by=weblog.UNGROUPED)
