import pipeline.infrastructure.renderer.weblog as weblog

from .makermsimages import Makermsimages
from . import makermsimages
from . import renderer


def _get_imaging_mode(context, result):
    """Check the last selected imaging mode and pick the custom renderer for VLASS-SE-CUBE."""
    if hasattr(context, 'imaging_mode') and context.imaging_mode == 'VLASS-SE-CUBE':
        return 'VLASS-CUBE'
    else:
        return None


weblog.add_renderer(Makermsimages,
                    renderer.T2_4MDetailsMakermsimagesRenderer(description='Makermsimages'),
                    group_by=weblog.UNGROUPED)


weblog.add_renderer(Makermsimages,
                    renderer.T2_4MDetailsMakermsimagesVlassCubeRenderer(description='Makermsimages'),
                    group_by=weblog.UNGROUPED,
                    key='VLASS-CUBE',
                    key_fn=_get_imaging_mode)
