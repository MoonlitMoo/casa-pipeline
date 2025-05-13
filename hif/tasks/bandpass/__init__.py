import pipeline.infrastructure.renderer.qaadapter as qaadapter
import pipeline.infrastructure.renderer.weblog as weblog
from . import common
from . import qa
from . import renderer
from .bandpassmode import BandpassMode
from .channelbandpass import ChannelBandpass
from .phcorbandpass import PhcorBandpass

qaadapter.registry.register_to_calibration_topic(common.BandpassResults)

weblog.add_renderer(PhcorBandpass, 
                    renderer.T2_4MDetailsBandpassRenderer(),
                    group_by='session')
