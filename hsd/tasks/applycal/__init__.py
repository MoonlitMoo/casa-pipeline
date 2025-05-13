import pipeline.infrastructure.renderer.qaadapter as qaadapter
import pipeline.infrastructure.renderer.weblog as weblog
from . import renderer
from .applycal import SDApplycal, SerialSDApplycal, SDApplycalResults

qaadapter.registry.register_to_flagging_topic(SDApplycalResults)

weblog.add_renderer(SDApplycal, renderer.T2_4MDetailsSDApplycalRenderer(always_rerender=False),
                    group_by=weblog.UNGROUPED)
weblog.add_renderer(SerialSDApplycal, renderer.T2_4MDetailsSDApplycalRenderer(always_rerender=False),
                    group_by=weblog.UNGROUPED)
