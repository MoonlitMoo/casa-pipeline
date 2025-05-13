import pipeline.infrastructure.renderer.weblog as weblog

from . import renderer
from .selfcal import Selfcal

weblog.add_renderer(Selfcal, renderer.T2_4MDetailsselfcalRenderer(), group_by=weblog.UNGROUPED)
