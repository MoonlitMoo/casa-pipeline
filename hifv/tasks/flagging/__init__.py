import pipeline.infrastructure.renderer.weblog as weblog
import pipeline.infrastructure.renderer.qaadapter as qaadapter
import pipeline.infrastructure.renderer.basetemplates as basetemplates

from . import checkflag
from . import targetflag
from . import renderer
from . import flagtargetsdata

from . import qa

from .flagdetervla import FlagDeterVLA
from .checkflag import Checkflag
from .targetflag import Targetflag
from .flagcal import Flagcal
from .flagtargetsdata import Flagtargetsdata

qaadapter.registry.register_to_dataset_topic(checkflag.CheckflagResults)
qaadapter.registry.register_to_dataset_topic(targetflag.TargetflagResults)
qaadapter.registry.register_to_dataset_topic(flagdetervla.FlagDeterVLAResults)


# Use locally defined renderer for VLA deterministic flagging.
weblog.add_renderer(FlagDeterVLA, renderer.T2_4MDetailsFlagDeterVLARenderer(), group_by=weblog.UNGROUPED)

weblog.add_renderer(Checkflag, renderer.T2_4MDetailscheckflagRenderer(), group_by=weblog.UNGROUPED)
weblog.add_renderer(Targetflag,
                    basetemplates.T2_4MDetailsDefaultRenderer(uri='vlatargetflag.mako', description='Targetflag'),
                    group_by=weblog.UNGROUPED)
weblog.add_renderer(Flagcal, basetemplates.T2_4MDetailsDefaultRenderer(uri='flagcal.mako', description='Flagcal'),
                    group_by=weblog.UNGROUPED)


weblog.add_renderer(Flagtargetsdata,
                    renderer.T2_4MDetailsFlagtargetsdataRenderer(description='VLA Flagtargetsdata'),
                    group_by=weblog.UNGROUPED)

