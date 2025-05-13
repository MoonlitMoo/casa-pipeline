import pipeline.infrastructure.renderer.weblog as weblog
from . import qa
from .renderer import T2_4MDetailsTsysflagContaminationRenderer
from .tsysflagcontamination import TsysFlagContamination

weblog.add_renderer(
    TsysFlagContamination,
    T2_4MDetailsTsysflagContaminationRenderer(),
    group_by=weblog.UNGROUPED,
)
