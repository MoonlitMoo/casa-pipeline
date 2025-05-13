from pipeline.infrastructure.renderer import weblog
from pipeline.infrastructure.renderer.basetemplates import T2_4MDetailsDefaultRenderer
from .unlock_refant import UnlockRefAnt

weblog.add_renderer(UnlockRefAnt,
                    T2_4MDetailsDefaultRenderer(uri='unlockrefant.mako',
                                                description='Allow modifications to refant list'),
                    group_by=weblog.UNGROUPED)
