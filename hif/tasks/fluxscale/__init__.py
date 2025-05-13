from .fluxscale import Fluxscale

import pipeline.infrastructure.renderer.weblog as weblog
import pipeline.infrastructure.renderer.basetemplates as basetemplates

weblog.add_renderer(Fluxscale, 
                    basetemplates.T2_4MDetailsDefaultRenderer(uri='fluxscale.mako',
                                                              description='Compute calibrator source fluxes'),
                    group_by='session')
