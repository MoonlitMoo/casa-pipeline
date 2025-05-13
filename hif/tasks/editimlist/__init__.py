import pipeline.infrastructure.renderer.weblog as weblog

from . import renderer
from .editimlist import Editimlist

weblog.add_renderer(Editimlist,
                    renderer.T2_4MDetailsEditimlistRenderer(uri='editimlist.mako', description='Editimlist',
                                                            always_rerender=False),
                    group_by=weblog.UNGROUPED,)
