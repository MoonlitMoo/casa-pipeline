from pipeline.infrastructure.renderer import weblog
from .lock_refant import LockRefAnt
from .renderer import T2_4MDetailsLockRefantRenderer

weblog.add_renderer(LockRefAnt, T2_4MDetailsLockRefantRenderer(), group_by=weblog.UNGROUPED)
